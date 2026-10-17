// Package eventstore provides a generic KurrentDB-backed event store for
// event-sourced aggregates. It centralizes the four mechanics that every
// module's per-aggregate store used to hand-roll:
//
//   - AppendToStream domain events pulled from the aggregate.
//   - ReadStream a single aggregate's stream back as typed domain events.
//   - Save / load aggregate snapshots, optionally auto-snapshotted every N
//     events to bound replay cost.
//
// This is the building-block replacement for the per-module event-store
// pattern. Each module still owns the domain shapes — NewFromEvents(id,
// events), Apply(event), and MarshalState — but the KurrentDB plumbing is
// shared here.
//
// The generic Store is configured per aggregate type via StoreConfig, which
// carries the event Registry used to decode events back from KurrentDB. The
// caller (a module's store) registers every concrete event struct into that
// registry at composition time.
package eventstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aliighahremanii/buildingblocks/application/concurrency"
	"github.com/aliighahremanii/buildingblocks/application/correlation"
	apperrors "github.com/aliighahremanii/buildingblocks/application/errors"
	domainevents "github.com/aliighahremanii/buildingblocks/domain/events"
	bbesdb "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/registry"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/snapshot"
	"github.com/aliighahremanii/buildingblocks/infrastructure/metrics"
	"github.com/aliighahremanii/buildingblocks/infrastructure/tracing"
)

// Aggregate is the constraint every event-sourced aggregate must satisfy to be
// savable through the generic Store. PullDomainEvents drains recorded events
// from the aggregate after a behavior method ran; the Store appends them to
// KurrentDB.
//
// Aggregate reconstruction (NewFromEvents / Apply / MarshalState) is NOT part
// of this interface — those stay module-specific because the typed aggregate
// must be reconstructed into a concrete *country.Country etc., which a generic
// store cannot do without reflection.
type Aggregate interface {
	PullDomainEvents() []domainevents.Event
}

// MarshalStateFunc returns the serialized aggregate state for snapshotting.
// Modules supply this so the generic store can write snapshots without
// depending on a concrete aggregate type. The returned bytes are opaque to
// the store; they are typically the result of the aggregate's MarshalState
// method wrapped in the shared snapshot envelope.
type MarshalStateFunc[T any] func(t *T) ([]byte, error)

// StoreConfig describes one event-sourced aggregate type's store wiring.
//
// AggregateType is the stream-id prefix (e.g. "country"); stream names are
// produced by bbesdb.StreamName(AggregateType, aggregateID).
//
// Registry maps KurrentDB event-type strings to fresh concrete event structs
// so ReadStream can decode stored events back into typed domainevents.Event
// values without a per-module switch.
//
// Snapshots, when non-nil, enables snapshotting. SnapshotEvery is the stream
// revision interval at which a snapshot is auto-saved after an append (0
// disables auto-snapshotting; the caller must invoke SaveSnapshot itself).
type StoreConfig struct {
	AggregateType string
	Registry      *registry.Registry
	Snapshots     *snapshot.Store
	SnapshotEvery uint64
}

// Store is a KurrentDB-backed event store for a single aggregate type. It is
// safe for concurrent use — the underlying *kurrentdb.Client is goroutine-safe
// and Store carries no mutable state.
type Store struct {
	client *kurrentdb.Client
	config StoreConfig
}

// New constructs a generic event Store for one aggregate type. The same
// *kurrentdb.Client may be shared across many Stores (one per aggregate type)
// in a module's composition root.
func New(client *kurrentdb.Client, config StoreConfig) *Store {
	return &Store{client: client, config: config}
}

// Save appends events to the aggregate's KurrentDB stream under the given
// optimistic-concurrency [concurrency.Expected]. A nil/empty event slice is a
// no-op so callers can hand the freshly-pulled events straight through without
// a length guard; in that case nextRevision is returned as 0 and the stream
// state is left untouched.
//
// expected.Kind selects the KurrentDB StreamState lookahead:
//
//   - [concurrency.ExpectedNoStream]    → kurrentdb.NoStream{}    (Register)
//   - [concurrency.ExpectedAtRevision]  → kurrentdb.Revision(N)   (Update)
//   - [concurrency.ExpectedAny]         → kurrentdb.Any{}         (migration)
//
// When the stream state does not match the expectation, KurrentDB rejects the
// append with a stream-revision-conflict error. Save translates that into a
// typed [apperrors.ConcurrencyError] (carrying the aggregate type, id, and
// the expected/actual revisions when the server reports them) so command
// handlers and the API layer can distinguish a legitimate business conflict
// (HTTP 409, do not blindly retry) from a transient infrastructure failure
// (HTTP 500, safe to retry).
//
// nextRevision is the stream revision the append landed at, suitable for
// seeding the next Save's expectedrevision or for snapshot placement. It is
// 0 when events was empty.
//
// Auto-snapshotting is intentionally not performed here (see StoreConfig
// docs); callers that want a snapshot saved on this write call SaveSnapshot
// immediately after Save, passing the returned nextRevision.
//
// The events slice is the result of Aggregate.PullDomainEvents(); the Store
// does not pull from the aggregate itself, which keeps it decoupled from the
// concrete aggregate type.
func (s *Store) Save(ctx context.Context, aggregateID string, expected concurrency.Expected, events []domainevents.Event) (nextRevision uint64, err error) {
	if len(events) == 0 {
		return 0, nil
	}

	// Trace the append so latency and conflict rates are visible per
	// aggregate type. Attributes mirror the metrics labels so a trace
	// and a metric alert join on the same dimensions.
	ctx, span := tracing.Start(ctx, "eventstore.Save")
	defer func() {
		if span != nil {
			if err != nil {
				span.RecordError(err)
			}
			span.End()
		}
	}()
	if span != nil {
		span.SetAttributes(
			attribute.String("aggregate_type", s.config.AggregateType),
			attribute.Int("events_count", len(events)),
			attribute.String("aggregate_id", aggregateID),
		)
		tracing.SetCorrelationAttributes(span, ctx)
	}

	streamName := bbesdb.StreamName(s.config.AggregateType, aggregateID)
	meta, hasMeta := resolveMetadata(ctx)
	eventData := make([]kurrentdb.EventData, len(events))
	for i, e := range events {
		var ed kurrentdb.EventData
		var err error
		if hasMeta {
			ed, err = bbesdb.ToEventDataWithMetadata(e, meta)
		} else {
			ed, err = bbesdb.ToEventData(e)
		}
		if err != nil {
			return 0, err
		}
		eventData[i] = ed
	}

	opts := kurrentdb.AppendToStreamOptions{StreamState: toKurrentStreamState(expected)}
	res, err := s.client.AppendToStream(ctx, streamName, opts, eventData...)
	if err != nil {
		mapped := mapAppendError(err, streamName, s.config.AggregateType, aggregateID, expected)
		var concErr apperrors.ConcurrencyError
		if errors.As(mapped, &concErr) {
			metrics.EventAppends.WithLabelValues(s.config.AggregateType, "conflict").Inc()
			metrics.ConcurrencyConflicts.WithLabelValues(s.config.AggregateType).Inc()
		} else {
			metrics.EventAppends.WithLabelValues(s.config.AggregateType, "failure").Inc()
		}
		return 0, mapped
	}
	metrics.EventAppends.WithLabelValues(s.config.AggregateType, "success").Inc()
	if res != nil {
		return res.NextExpectedVersion, nil
	}
	return 0, nil
}

// resolveMetadata extracts the correlation/causation/user identifiers from
// ctx and translates them into a KurrentDB EventMetadata envelope. The
// boolean is false when ctx carries no correlation Context — in that case
// Save falls back to ToEventData so background workers, projection
// subscriptions, and tests that never set up correlation are unaffected.
//
// Exported test seam: resolveMetadata is the pure function behind Save's
// metadata decision. It is tested directly because Save delegates to the
// live kurrentdb client (which cannot be exercised without a real
// EventStoreDB connection).
func resolveMetadata(ctx context.Context) (bbesdb.EventMetadata, bool) {
	c, ok := correlation.FromContext(ctx)
	if !ok {
		return bbesdb.EventMetadata{}, false
	}
	return bbesdb.EventMetadata{
		CorrelationID: c.CorrelationID,
		CausationID:   c.CausationID,
		UserID:        c.UserID,
	}, true
}

// toKurrentStreamState translates the application-layer concurrency
// expectation to the KurrentDB client's native StreamState. The mapping is
// 1:1 and is the ONLY place in the codebase that knows how Expected maps to
// KurrentDB wire values, so changing the underlying event store requires
// touching only this function.
func toKurrentStreamState(expected concurrency.Expected) kurrentdb.StreamState {
	switch expected.Kind {
	case concurrency.ExpectedNoStream:
		return kurrentdb.NoStream{}
	case concurrency.ExpectedAtRevision:
		return kurrentdb.Revision(expected.Revision)
	default:
		return kurrentdb.Any{}
	}
}

// mapAppendError inspects a KurrentDB append error and returns:
//
//   - a typed [apperrors.ConcurrencyError] when it is an
//     optimistic-concurrency conflict (stream-revision mismatch), or
//   - a typed [apperrors.ConflictError] when the target stream was
//     hard-deleted (tombstoned) — a permanent state KurrentDB rejects any
//     append to, including with a NoStream expectation, or
//   - the original error wrapped with the stream name for any other failure.
//
// Distinguishing the three is critical: a business conflict must surface as
// HTTP 409 (no retry), while an infrastructure failure must surface as HTTP
// 500 (safe to retry).
//
// KurrentDB returns conflicts as *kurrentdb.Error with Code ==
// ErrorCodeStreamRevisionConflict or ErrorCodeWrongExpectedVersion, and the
// inner cause may be a *kurrentdb.StreamRevisionConflictError carrying the
// expected/actual revisions. We surface both via errors.As.
//
// The conflict→ConcurrencyError translation lives in [buildConcurrencyError]
// and the tombstone→ConflictError translation in
// [buildPermanentlyDeletedConflictError] so both can be unit-tested without a
// live *kurrentdb.Error (whose fields are unexported and therefore impossible
// to construct outside the kurrentdb package). mapAppendError itself is
// exercised end-to-end by the live KurrentDB integration tests (see
// tombstone_integration_test.go).
func mapAppendError(err error, streamName, aggregateType, aggregateID string, expected concurrency.Expected) error {
	var esErr *kurrentdb.Error
	if errors.As(err, &esErr) {
		if esErr.Code() == kurrentdb.ErrorCodeStreamRevisionConflict ||
			esErr.Code() == kurrentdb.ErrorCodeWrongExpectedVersion {
			actualRev := int64(-1)
			var srce *kurrentdb.StreamRevisionConflictError
			if errors.As(err, &srce) {
				if ar, ok := srce.ActualRevision.(kurrentdb.StreamRevision); ok {
					actualRev = int64(ar.Value)
				} else if _, ok := srce.ActualRevision.(kurrentdb.NoStream); ok {
					actualRev = -1
				}
			}
			return buildConcurrencyError(aggregateType, aggregateID, expected, actualRev)
		}
		// A hard-deleted (tombstoned) stream rejects every append — even a
		// Register's NoStream expectation can never succeed against it. That
		// is an actionable business conflict, not an infrastructure failure:
		// the identity is permanently unusable and must be re-registered
		// fresh (D127).
		if classifyStreamAbsence(esErr.Code()) == streamHardDeleted {
			return buildPermanentlyDeletedConflictError(aggregateType)
		}
	}
	return fmt.Errorf("eventstore append %s: %w", streamName, err)
}

// streamAbsenceClass is the tri-state classification of a KurrentDB error
// code for aggregate-absence semantics. It mirrors the purge tooling's
// stream-state vocabulary (cmd/admin purge_discover.go mapReadStreamState):
// absent, hard-deleted, or operational (everything else).
type streamAbsenceClass int

const (
	// streamNotAbsent means the error carries no absence signal — transport,
	// authorization, timeout, or server errors, or no KurrentDB error at
	// all. It must propagate unchanged and never be mistaken for "no
	// aggregate".
	streamNotAbsent streamAbsenceClass = iota
	// streamAbsent means ErrorCodeResourceNotFound: the stream was never
	// created. Soft-truncated streams also read this way; both states may
	// be (re-)created with a NoStream append.
	streamAbsent
	// streamHardDeleted means ErrorCodeStreamDeleted or
	// ErrorCodeStreamTombstoned: the stream was permanently deleted and
	// KurrentDB rejects any append to it forever — the identity cannot be
	// re-created in place.
	streamHardDeleted
)

// classifyStreamAbsence maps a KurrentDB error code to its absence class.
//
// It is a pure function over the extracted code — the unit-testable seam for
// the read-path classification, in the same style as buildConcurrencyError:
// *kurrentdb.Error's fields are unexported, so tests cannot construct a live
// SDK error and instead drive the mapping by code. The code extraction
// (errors.As against *kurrentdb.Error) is pinned end-to-end by the live
// KurrentDB tombstone integration tests (tombstone_integration_test.go).
func classifyStreamAbsence(code kurrentdb.ErrorCode) streamAbsenceClass {
	switch code {
	case kurrentdb.ErrorCodeResourceNotFound:
		return streamAbsent
	case kurrentdb.ErrorCodeStreamDeleted, kurrentdb.ErrorCodeStreamTombstoned:
		return streamHardDeleted
	default:
		return streamNotAbsent
	}
}

// classifyStreamReadError classifies a stream-read error chain
// (wrapping-aware) for aggregate absence. Both absence classes —
// never-created and hard-deleted — mean the aggregate does not exist; the
// distinction matters only for callers that must decide whether the stream
// may be (re-)created (see StreamPermanentlyDeleted).
func classifyStreamReadError(err error) streamAbsenceClass {
	code, ok := kurrentdbErrorCode(err)
	if !ok {
		return streamNotAbsent
	}
	return classifyStreamAbsence(code)
}

// kurrentdbErrorCode extracts the SDK error code from err's chain. ok is
// false when err carries no *kurrentdb.Error anywhere in the chain.
func kurrentdbErrorCode(err error) (kurrentdb.ErrorCode, bool) {
	var esErr *kurrentdb.Error
	if !errors.As(err, &esErr) {
		return 0, false
	}
	return esErr.Code(), true
}

// buildPermanentlyDeletedConflictError constructs the typed application-layer
// error for an append rejected because the target stream was hard-deleted
// (tombstoned). Pure function over its inputs for the same testability
// reason as [buildConcurrencyError]; the message is the shared
// apperrors.PermanentlyDeletedConflictMessage sentence so every boundary
// (append mapping and module-level pre-checks) surfaces one identical,
// actionable, non-leaking client contract.
func buildPermanentlyDeletedConflictError(aggregateType string) apperrors.ConflictError {
	return apperrors.NewConflictError(aggregateType, apperrors.PermanentlyDeletedConflictMessage)
}

// buildConcurrencyError constructs the typed application-layer error from the
// pieces that mapAppendError extracted from the underlying event-store
// envelope. It is exported so tests can verify the translation without
// constructing a *kurrentdb.Error (whose Code field is unexported).
//
// expected is the application-layer expectation the caller placed on the
// failed Save. actualRevision is the stream revision the event store
// reported at append time; -1 means the server did not report a value or
// reported the stream as not-existing.
func buildConcurrencyError(aggregateType, aggregateID string, expected concurrency.Expected, actualRevision int64) apperrors.ConcurrencyError {
	expectedRev := int64(-1)
	if expected.IsAtRevision() {
		expectedRev = int64(expected.Revision)
	}
	return apperrors.NewConcurrencyError(aggregateType, aggregateID, expectedRev, actualRevision)
}

// ReadStream reads every event from the aggregate's KurrentDB stream and
// decodes each into a typed domainevents.Event via the configured Registry.
// Events are returned in stream order (forwards from stream start).
// lastRevision is the stream revision of the most-recent event in the stream
// (0 for an empty stream); callers pass it to [Store.Save] as
// concurrency.AtRevision(lastRevision) for optimistic-concurrency control on
// the next append.
//
// For aggregates with long histories, callers should first call LoadSnapshot,
// reconstruct the aggregate from the snapshot state, and then replay only the
// events since the snapshot revision via a stream-position-scoped read. This
// method reads the whole stream and is intended for short streams or for the
// "no snapshot" bootstrap path.
//
// Each decoded event is enriched with stream metadata via
// [domainevents.Enrich] before it is returned. Enrichment stamps the
// identity/timing fields (name, aggregateID, aggregateType, occurredAt) that
// are not carried in the JSON payload but are present in the event-store
// envelope — without this step, Apply methods that read
// [domainevents.Event.AggregateID] or [domainevents.Event.OccurredAt] would
// see empty values, corrupting aggregate state on rehydration.
func (s *Store) ReadStream(ctx context.Context, aggregateID string) (events []domainevents.Event, lastRevision uint64, err error) {
	streamName := bbesdb.StreamName(s.config.AggregateType, aggregateID)

	stream, err := s.client.ReadStream(ctx, streamName, kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Forwards,
		From:      kurrentdb.Start{},
	}, 10000)
	if err != nil {
		return nil, 0, fmt.Errorf("eventstore read stream %s: %w", streamName, err)
	}
	defer stream.Close()

	var collected []domainevents.Event
	for {
		re, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("eventstore recv %s: %w", streamName, err)
		}
		if re.Event == nil {
			continue
		}
		evt, err := s.decodeAndEnrich(re.Event, aggregateID)
		if err != nil {
			return nil, 0, err
		}
		collected = append(collected, evt)
		lastRevision = re.Event.EventNumber
	}
	return collected, lastRevision, nil
}

// StreamPermanentlyDeleted reports whether the aggregate's stream was
// hard-deleted (tombstoned) — a permanent state from which the identity can
// never be re-created: KurrentDB rejects every append to a tombstoned stream,
// including with a NoStream expectation.
//
// The probe reads the stream head backwards (a single event) and classifies
// the outcome with the same tri-state vocabulary as LoadAggregate:
//
//   - StreamDeleted / StreamTombstoned → true, nil (hard-deleted)
//   - ResourceNotFound → false, nil (ordinary absence: never created, or a
//     soft-truncated stream — both may be (re-)created)
//   - any other error (transport, authorization, timeout, server) →
//     returned unchanged, wrapped with the stream name; the caller must
//     NEVER treat an operational failure as either verdict
//
// io.EOF from the head read means the stream read successfully but holds no
// events — that cannot be a tombstoned stream, so it also classifies as
// false, nil.
//
// The KurrentDB SDK surfaces absence codes on the initial ReadStream call or
// on the first Recv depending on server version (reads.go maps
// ReadResp_StreamNotFound_ and stream-deleted trailers there); both paths
// are classified, mirroring cmd/admin purge_discover.go StreamState.
func (s *Store) StreamPermanentlyDeleted(ctx context.Context, aggregateID string) (bool, error) {
	streamName := bbesdb.StreamName(s.config.AggregateType, aggregateID)
	classifyProbe := func(err error) (bool, error) {
		switch classifyStreamReadError(err) {
		case streamHardDeleted:
			return true, nil
		case streamAbsent:
			return false, nil
		default:
			return false, fmt.Errorf("eventstore stream-state probe %s: %w", streamName, err)
		}
	}

	stream, err := s.client.ReadStream(ctx, streamName, kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Backwards,
		From:      kurrentdb.End{},
	}, 1)
	if err != nil {
		return classifyProbe(err)
	}
	defer stream.Close()
	if _, err := stream.Recv(); err != nil && !errors.Is(err, io.EOF) {
		return classifyProbe(err)
	}
	return false, nil
}

// decodeAndEnrich unmarshals a recorded event's payload via the configured
// Registry and stamps the result with stream metadata via
// [domainevents.Enrich]. It is the testable single-event core of ReadStream:
// callers (and tests) can exercise it without standing up a live KurrentDB
// subscription by constructing a *kurrentdb.RecordedEvent directly.
//
// aggregateID is the caller-supplied stream id (UUID) rather than
// re.StreamID because the recorded event's StreamID includes the
// aggregate-type prefix (e.g. "country-<uuid>") and the caller already knows
// the bare UUID. The aggregate type is sourced from StoreConfig.AggregateType
// so the decoded event reports the same type that NewBase stamped at write
// time.
func (s *Store) decodeAndEnrich(re *kurrentdb.RecordedEvent, aggregateID string) (domainevents.Event, error) {
	evt, err := s.config.Registry.Unmarshal(re.EventType, re.Data)
	if err != nil {
		return nil, fmt.Errorf("eventstore unmarshal %s/%s: %w", re.StreamID, re.EventType, err)
	}
	domainevents.Enrich(
		evt,
		re.EventType,           // name — the event type string NewBase used
		aggregateID,            // aggregateID — bare UUID, no prefix
		s.config.AggregateType, // aggregateType — same as NewBase used
		re.CreatedDate,         // occurredAt — KurrentDB commit timestamp
	)
	return evt, nil
}

// LoadSnapshot attempts to load a snapshot for the aggregate. When none
// exists, found is false and the other returns are zero values; callers branch
// on found rather than inspecting the error.
//
// If no snapshot store was configured (StoreConfig.Snapshots == nil), this
// returns found=false with a nil error, so callers can write one code path
// regardless of whether snapshotting is enabled.
func (s *Store) LoadSnapshot(ctx context.Context, aggregateID string) (state []byte, version uint64, found bool, err error) {
	if s.config.Snapshots == nil {
		return nil, 0, false, nil
	}
	return s.config.Snapshots.Load(ctx, s.config.AggregateType, aggregateID)
}

// ReadTail reads events AFTER fromRevision (exclusive) — i.e. the events
// appended since a snapshot was taken. Used by LoadAggregate to bound
// replay cost on long-lived streams: the snapshot reconstructs the bulk
// of the state, ReadTail replays only the recent events.
//
// Events are returned in stream order (forwards from fromRevision+1).
// lastRevision is the stream revision of the most-recent event in the
// returned slice (0 when the slice is empty — meaning the snapshot was
// up-to-date).
//
// Each decoded event is enriched with stream metadata via
// [domainevents.Enrich] exactly like [ReadStream].
func (s *Store) ReadTail(ctx context.Context, aggregateID string, fromRevision uint64) (events []domainevents.Event, lastRevision uint64, err error) {
	streamName := bbesdb.StreamName(s.config.AggregateType, aggregateID)

	// ReadTail reads from fromRevision+1 (exclusive of the snapshot
	// revision itself). KurrentDB's Revision(N) is inclusive — it
	// starts reading AT event N — so we pass N+1 to skip the snapshot
	// event and read only the newer tail.
	stream, err := s.client.ReadStream(ctx, streamName, kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Forwards,
		From:      kurrentdb.Revision(fromRevision + 1),
	}, 10000)
	if err != nil {
		return nil, 0, fmt.Errorf("eventstore read tail %s@%d: %w", streamName, fromRevision, err)
	}
	defer stream.Close()

	var collected []domainevents.Event
	for {
		re, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, fmt.Errorf("eventstore recv tail %s: %w", streamName, err)
		}
		if re.Event == nil {
			continue
		}
		evt, err := s.decodeAndEnrich(re.Event, aggregateID)
		if err != nil {
			return nil, 0, err
		}
		collected = append(collected, evt)
		lastRevision = re.Event.EventNumber
	}
	return collected, lastRevision, nil
}

// SaveSnapshot stores a snapshot for the aggregate at the given stream
// version. state is the module-supplied serialized aggregate state (typically
// the result of the aggregate's MarshalState method).
//
// If no snapshot store was configured (StoreConfig.Snapshots == nil), this is
// a no-op returning nil, so callers can call it unconditionally.
func (s *Store) SaveSnapshot(ctx context.Context, aggregateID string, version uint64, state []byte) error {
	if s.config.Snapshots == nil {
		return nil
	}
	return s.config.Snapshots.Save(ctx, s.config.AggregateType, aggregateID, version, state)
}

// GetIDFunc returns the KurrentDB stream id (the aggregate's UUID) for an
// aggregate of type T. Modules supply this so the generic store can compute the
// stream name without depending on a concrete aggregate type.
type GetIDFunc[T any] func(t *T) string

// SaveAggregate appends events to the aggregate's KurrentDB stream, resolving
// the stream id via idFunc. It is a typed convenience over Store.Save: callers
// hand the aggregate, its already-pulled domain events, and the
// optimistic-concurrency expectation straight through, and the store looks up
// the id and returns the next stream revision.
//
// expected is the application-layer concurrency expectation that the calling
// handler derived from the prior ByID load: concurrency.NoStream() for
// Register-style handlers, concurrency.AtRevision(rev) for Update-style
// handlers. The Store translates it to the underlying event store's native
// primitive (see Store.Save).
//
// Snapshotting is NOT performed here; see [SaveAggregateWithSnapshot] for the
// variant that auto-snapshots when the configured SnapshotEvery threshold is
// hit.
//
// SaveAggregate is a top-level generic function rather than a method because Go
// does not permit methods to introduce their own type parameters.
func SaveAggregate[T any](s *Store, ctx context.Context, t *T, expected concurrency.Expected, events []domainevents.Event, idFunc GetIDFunc[T]) (uint64, error) {
	return s.Save(ctx, idFunc(t), expected, events)
}

// SaveAggregateWithSnapshot is SaveAggregate plus automatic snapshotting.
// After a successful append, if StoreConfig.SnapshotEvery > 0 and the
// post-append nextRevision is a non-zero multiple of SnapshotEvery, the
// aggregate is serialized via marshalState and saved as a snapshot at
// nextRevision.
//
// Snapshot write failures are returned to the caller. A successful append
// followed by a failed snapshot write does NOT roll back the append (the
// events are durable in KurrentDB); the caller sees the snapshot error
// and the next Save will simply re-attempt the snapshot at the next
// threshold. Projections and read models are unaffected because they
// read from KurrentDB, not from snapshots.
//
// SnapshotEvery == 0 disables auto-snapshotting; the function behaves
// identically to SaveAggregate. This is the path tests use when they
// want to assert on event-replay semantics without a snapshot.
//
// marshalState may be nil — in that case the function behaves like
// SaveAggregate (no snapshot). This is a convenience for stores that
// conditionally enable snapshotting based on configuration.
func SaveAggregateWithSnapshot[T any](
	s *Store,
	ctx context.Context,
	t *T,
	expected concurrency.Expected,
	events []domainevents.Event,
	idFunc GetIDFunc[T],
	marshalState MarshalStateFunc[T],
) (uint64, error) {
	nextRevision, err := SaveAggregate(s, ctx, t, expected, events, idFunc)
	if err != nil {
		return nextRevision, err
	}

	// Auto-snapshot when configured and the threshold is hit. Skip the
	// snapshot if the events slice was empty (Save was a no-op; there is
	// nothing new to snapshot and nextRevision is 0).
	if len(events) == 0 {
		metrics.SnapshotSkipsBecauseNoNewEvents.WithLabelValues(s.config.AggregateType).Inc()
		return nextRevision, nil
	}
	if s.config.Snapshots == nil ||
		s.config.SnapshotEvery == 0 ||
		marshalState == nil {
		return nextRevision, nil
	}

	// Auto-snapshot when the event count (nextRevision+1, since KurrentDB
	// revisions are 0-indexed — the 5th event lands at revision 4) is a
	// non-zero multiple of SnapshotEvery. The `len(events) > 0` guard
	// above already ensures we don't snapshot an empty/no-op save.
	if (nextRevision+1)%s.config.SnapshotEvery == 0 {
		state, stateErr := marshalState(t)
		if stateErr != nil {
			return nextRevision, fmt.Errorf("snapshot marshal at rev %d: %w", nextRevision, stateErr)
		}
		if snapErr := s.SaveSnapshot(ctx, idFunc(t), nextRevision, state); snapErr != nil {
			// Return the snapshot error but keep nextRevision so the
			// caller knows the append succeeded.
			return nextRevision, fmt.Errorf("snapshot save at rev %d: %w", nextRevision, snapErr)
		}
		metrics.SnapshotWrites.WithLabelValues(s.config.AggregateType).Inc()
	}

	return nextRevision, nil
}

// RehydrateFunc reconstructs a typed aggregate of type T from a snapshot and a
// tail of domain events read since the snapshot. id is the aggregate's stream
// id (UUID); snapshot is the serialized snapshot state (nil when no snapshot
// exists, in which case the tail is the full stream); snapshotVersion is the
// stream revision the snapshot was taken at (0 when there is no snapshot). tail
// is never nil: it is the events read from the stream, possibly empty.
//
// A module's rehydrate closure typically:
//   - If snapshot == nil: return NewFromEvents(id, tail) (or an error when the
//     tail is empty — the aggregate does not exist).
//   - Otherwise: UnmarshalState(snapshot), then Apply each tail event.
type RehydrateFunc[T any] func(id string, snapshot []byte, snapshotVersion uint64, tail []domainevents.Event) (*T, error)

// LoadAggregate reconstructs a typed aggregate of type T from KurrentDB. It
// loads a snapshot (if any), reads the event stream, then delegates to
// rehydrate to rebuild the concrete aggregate. This is the generic
// replacement for the per-store ByID pattern; modules supply only the
// rehydration closure that knows how to call NewFromEvents / UnmarshalState /
// Apply on their concrete aggregate type.
//
// The returned revision is the stream revision the aggregate was loaded at.
// Update-style command handlers pass it to Save as
// concurrency.AtRevision(revision) for optimistic-concurrency control; a
// concurrent append between LoadAggregate and Save will fail at the event
// store and surface as apperrors.ConcurrencyError (HTTP 409).
//
// When no snapshot is found, snapshot is passed to rehydrate as nil so the
// closure can branch on "full replay" vs "snapshot + tail" without a separate
// found flag.
//
// LoadAggregate is a top-level generic function rather than a method because Go
// does not permit methods to introduce their own type parameters.
func LoadAggregate[T any](s *Store, ctx context.Context, id string, rehydrate RehydrateFunc[T]) (*T, uint64, error) {
	start := time.Now()
	// Trace the full rehydration path so replay latency is visible per
	// aggregate type. The span wraps the existing metrics histogram so
	// the trace and the metric alert join on the same dimensions.
	ctx, span := tracing.Start(ctx, "eventstore.LoadAggregate")
	defer func() {
		if span != nil {
			span.End()
		}
	}()
	if span != nil {
		span.SetAttributes(
			attribute.String("aggregate_type", s.config.AggregateType),
			attribute.String("aggregate_id", id),
		)
		tracing.SetCorrelationAttributes(span, ctx)
	}
	defer func() {
		metrics.AggregateReplayDuration.WithLabelValues(s.config.AggregateType).Observe(time.Since(start).Seconds())
	}()

	state, version, found, err := s.LoadSnapshot(ctx, id)
	if err != nil {
		if span != nil {
			span.RecordError(err)
		}
		return nil, 0, fmt.Errorf("eventstore snapshot load %s: %w", id, err)
	}

	var events []domainevents.Event
	var lastRevision uint64

	if found {
		// Snapshot hit: read only the tail (events appended since the
		// snapshot). This bounds replay cost on long-lived streams to
		// the events appended in the most recent burst rather than
		// the full history. The rehydrate closure receives the snapshot
		// state and the tail; it knows how to Apply the tail onto the
		// unmarshaled snapshot.
		events, lastRevision, err = s.ReadTail(ctx, id, version)
		if err != nil {
			if span != nil {
				span.RecordError(err)
			}
			// Same absence classification as the full-replay path below: a
			// Postgres snapshot may survive while the stream is hard-deleted
			// (a purge tombstones streams, not snapshot rows). The write
			// model is the system of record, so stream absence — never
			// created OR hard-deleted — wins and the aggregate reads as
			// absent (D127). Operational errors stay raw.
			if classifyStreamReadError(err) != streamNotAbsent {
				return nil, 0, apperrors.NewNotFoundError(s.config.AggregateType, id)
			}
			return nil, 0, fmt.Errorf("eventstore read tail %s@%d: %w", id, version, err)
		}
		// If the tail is empty, the snapshot is up-to-date; lastRevision
		// stays at version so the next Save expects exactly version.
		if lastRevision == 0 {
			lastRevision = version
		}
	} else {
		// Snapshot miss: full-stream replay via ReadStream. This is the
		// bootstrap path (no snapshot yet) and the recovery path
		// (snapshot store down or snapshot corrupted).
		state = nil
		events, lastRevision, err = s.ReadStream(ctx, id)
		if err != nil {
			if span != nil {
				span.RecordError(err)
			}
			// Aggregate absence — never-created (ResourceNotFound) OR
			// hard-deleted (StreamDeleted/StreamTombstoned) — is the same
			// typed NotFoundError as a stream that never existed: there is
			// nothing to replay either way. A tombstoned stream MUST NOT
			// surface as a raw (masked 500) error (D127); operational
			// errors still do.
			if classifyStreamReadError(err) != streamNotAbsent {
				return nil, 0, apperrors.NewNotFoundError(s.config.AggregateType, id)
			}
			return nil, 0, fmt.Errorf("eventstore read stream %s: %w", id, err)
		}
	}
	// events_count is known only after the read; set it once here rather
	// than guessing before the snapshot/tail branch resolves.
	if span != nil {
		span.SetAttributes(attribute.Int("events_count", len(events)))
	}

	// The returned revision is what the next Save must expect. Whether
	// the snapshot was used or not, lastRevision is the highest
	// event-number the aggregate has seen.
	agg, err := rehydrate(id, state, version, events)
	if err != nil {
		if span != nil {
			span.RecordError(err)
		}
		return nil, 0, err
	}
	return agg, lastRevision, nil
}
