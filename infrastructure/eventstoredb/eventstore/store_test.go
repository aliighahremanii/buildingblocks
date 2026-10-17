package eventstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"github.com/aliighahremanii/buildingblocks/application/concurrency"
	"github.com/aliighahremanii/buildingblocks/application/correlation"
	apperrors "github.com/aliighahremanii/buildingblocks/application/errors"
	domainevents "github.com/aliighahremanii/buildingblocks/domain/events"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/registry"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/snapshot"
)

// Integration tests (against a live KurrentDB instance) live alongside the
// modules that wire a real client. Here we only verify construction wiring,
// which is all that can be exercised without a live store.

func TestStoreNewReturnsNonNil(t *testing.T) {
	var client *kurrentdb.Client // nil is fine — we only assert the constructor wires it.
	s := New(client, StoreConfig{
		AggregateType: "country",
		Registry:      registry.New(),
		Snapshots:     nil,
		SnapshotEvery: 50,
	})
	if s == nil {
		t.Fatal("New() returned nil")
	}
}

func TestStoreNewWithSnapshots(t *testing.T) {
	var client *kurrentdb.Client
	s := New(client, StoreConfig{
		AggregateType: "country",
		Registry:      registry.New(),
		Snapshots:     snapshot.New(nil),
		SnapshotEvery: 50,
	})
	if s == nil {
		t.Fatal("New() returned nil")
	}
}

// --- decodeAndEnrich (read-path metadata stamping) ---

// stubEvent mirrors the shape used in registry tests: it embeds Base (so it
// satisfies domainevents.Event via promoted methods) and carries a single
// payload field that survives JSON round-trip.
type stubEvent struct {
	domainevents.Base
	Payload string `json:"payload"`
}

func newStubEventFactory() func() domainevents.Event {
	return func() domainevents.Event { return &stubEvent{} }
}

// TestDecodeAndEnrichStampsMetadataFromRecordedEvent is the regression test
// for the critical rehydration bug: after JSON decode, every identity and
// timing field on Base reads as a zero value because they are unexported and
// therefore not serialized. decodeAndEnrich must stamp them from the
// surrounding event-store envelope so Apply methods see the original values.
//
// This test does NOT require a live KurrentDB: it constructs a
// *kurrentdb.RecordedEvent by hand and exercises the per-event helper that
// ReadStream delegates to.
func TestDecodeAndEnrichStampsMetadataFromRecordedEvent(t *testing.T) {
	t.Parallel()

	// Build the store with a real registry that knows about our stub event.
	reg := registry.New()
	reg.Register("stub.occurred", newStubEventFactory())

	var client *kurrentdb.Client // nil — decodeAndEnrich does not touch the client.
	s := New(client, StoreConfig{
		AggregateType: "stub",
		Registry:      reg,
	})

	// Simulate the wire payload: a JSON blob exactly as KurrentDB would
	// return it. Note the absence of any identity/timing keys — only
	// schemaVersion and the payload survive serialization.
	payload, err := json.Marshal(&stubEvent{
		Base:    domainevents.NewBase("ignored-at-write", "ignored-id", "ignored-type", time.Time{}),
		Payload: "hello",
	})
	if err != nil {
		t.Fatalf("marshal stub: %v", err)
	}

	committedAt := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	re := &kurrentdb.RecordedEvent{
		EventID:     uuid.New(),
		EventType:   "stub.occurred",
		StreamID:    "stub-uuid-123",
		EventNumber: 7,
		CreatedDate: committedAt,
		Data:        payload,
	}

	evt, err := s.decodeAndEnrich(re, "uuid-123")
	if err != nil {
		t.Fatalf("decodeAndEnrich: %v", err)
	}

	stub, ok := evt.(*stubEvent)
	if !ok {
		t.Fatalf("decoded type = %T, want *stubEvent", evt)
	}

	if stub.EventName() != "stub.occurred" {
		t.Fatalf("EventName = %q, want %q", stub.EventName(), "stub.occurred")
	}
	if stub.AggregateID() != "uuid-123" {
		t.Fatalf("AggregateID = %q, want %q (bare UUID, no prefix)", stub.AggregateID(), "uuid-123")
	}
	if stub.AggregateType() != "stub" {
		t.Fatalf("AggregateType = %q, want %q (from StoreConfig)", stub.AggregateType(), "stub")
	}
	if !stub.OccurredAt().Equal(committedAt) {
		t.Fatalf("OccurredAt = %v, want %v (from RecordedEvent.CreatedDate)",
			stub.OccurredAt(), committedAt)
	}
	if stub.Payload != "hello" {
		t.Fatalf("Payload = %q, want %q", stub.Payload, "hello")
	}
}

// TestDecodeAndEnrichPropagatesUnmarshalErrors verifies that a malformed
// payload surfaces as an error wrapping the event type, rather than being
// silently dropped. This protects operators from invisible projection skips
// when a corrupt event appears in a stream.
func TestDecodeAndEnrichPropagatesUnmarshalErrors(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	reg.Register("stub.occurred", newStubEventFactory())

	s := New(nil, StoreConfig{AggregateType: "stub", Registry: reg})

	re := &kurrentdb.RecordedEvent{
		EventType: "stub.occurred",
		StreamID:  "stub-uuid",
		Data:      []byte(`{not-json`),
	}

	_, err := s.decodeAndEnrich(re, "uuid")
	if err == nil {
		t.Fatal("decodeAndEnrich on malformed JSON: want error, got nil")
	}
}

// TestDecodeAndEnrichRejectsUnknownEventType verifies that an event type the
// Registry does not know produces a hard error rather than a silent skip.
// Silent skipping of unknown event types is the most dangerous failure mode
// in event sourcing because it permanently diverges the read model with no
// operator-visible signal.
func TestDecodeAndEnrichRejectsUnknownEventType(t *testing.T) {
	t.Parallel()

	s := New(nil, StoreConfig{
		AggregateType: "stub",
		Registry:      registry.New(), // empty registry — no event types registered
	})

	re := &kurrentdb.RecordedEvent{
		EventType: "does.not.exist",
		StreamID:  "stub-uuid",
		Data:      []byte(`{}`),
	}

	_, err := s.decodeAndEnrich(re, "uuid")
	if err == nil {
		t.Fatal("decodeAndEnrich on unknown event type: want error, got nil")
	}
}

// --- mapAppendError & toKurrentStreamState (optimistic concurrency) ---

// TestToKurrentStreamStateCoversAllExpectedKinds verifies that the
// application-layer Expected maps 1:1 to the correct kurrentdb StreamState.
// This is the single source of truth for the mapping; if it drifts, OCC
// silently breaks.
func TestToKurrentStreamStateCoversAllExpectedKinds(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		expected concurrency.Expected
		check    func(kurrentdb.StreamState) bool
		wantKind string
	}{
		{"NoStream", concurrency.NoStream(), func(s kurrentdb.StreamState) bool {
			_, ok := s.(kurrentdb.NoStream)
			return ok
		}, "kurrentdb.NoStream"},
		{"AtRevision(7)", concurrency.AtRevision(7), func(s kurrentdb.StreamState) bool {
			r, ok := s.(kurrentdb.StreamRevision)
			return ok && r.Value == 7
		}, "kurrentdb.StreamRevision{7}"},
		{"AtRevision(0)", concurrency.AtRevision(0), func(s kurrentdb.StreamState) bool {
			r, ok := s.(kurrentdb.StreamRevision)
			return ok && r.Value == 0
		}, "kurrentdb.StreamRevision{0}"},
		{"Any", concurrency.Any(), func(s kurrentdb.StreamState) bool {
			_, ok := s.(kurrentdb.Any)
			return ok
		}, "kurrentdb.Any"},
		{"zero-value", concurrency.Expected{}, func(s kurrentdb.StreamState) bool {
			_, ok := s.(kurrentdb.Any)
			return ok
		}, "kurrentdb.Any (zero value)"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := toKurrentStreamState(tc.expected)
			if !tc.check(got) {
				t.Fatalf("toKurrentStreamState(%v) = %T, want %s", tc.expected, got, tc.wantKind)
			}
		})
	}
}

// TestBuildConcurrencyErrorAtRevision is the critical contract test for the
// optimistic-concurrency fix: when KurrentDB rejects an append with a stream
// revision conflict, the error surfaced to the application layer MUST be a
// typed apperrors.ConcurrencyError so the transport can map it to HTTP 409
// and the client can decide to reload and retry. Any other error type would
// either be silently retried (data corruption risk) or mapped to HTTP 500
// (spurious infrastructure alert).
//
// mapAppendError does the live-error classification (errors.As against
// *kurrentdb.Error, whose fields are unexported — impossible to construct
// outside the kurrentdb package). The translation from the parsed pieces
// into the application-layer error lives in buildConcurrencyError, which is
// a pure function we can unit-test here. The full chain is exercised by the
// live KurrentDB integration tests in P0.5.
func TestBuildConcurrencyErrorAtRevision(t *testing.T) {
	t.Parallel()

	got := buildConcurrencyError("country", "uuid-1", concurrency.AtRevision(5), 7)

	var cce apperrors.ConcurrencyError
	if !errors.As(got, &cce) {
		t.Fatalf("errors.As(got, &ConcurrencyError) = false; got %T: %v", got, got)
	}
	if cce.AggregateType != "country" {
		t.Fatalf("AggregateType = %q, want %q", cce.AggregateType, "country")
	}
	if cce.AggregateID != "uuid-1" {
		t.Fatalf("AggregateID = %q, want %q", cce.AggregateID, "uuid-1")
	}
	if cce.ExpectedRevision != 5 {
		t.Fatalf("ExpectedRevision = %d, want 5", cce.ExpectedRevision)
	}
	if cce.ActualRevision != 7 {
		t.Fatalf("ActualRevision = %d, want 7", cce.ActualRevision)
	}
}

// TestBuildConcurrencyErrorNoStreamExpectedEncodesAsNegativeOne verifies
// that a NoStream expectation (Register handlers) is encoded as -1 in the
// surfaced ConcurrencyError. This lets operators distinguish "Register
// conflict — stream already exists" from an AtRevision(0) update conflict
// where revision 0 would be a legitimate value.
func TestBuildConcurrencyErrorNoStreamExpectedEncodesAsNegativeOne(t *testing.T) {
	t.Parallel()

	got := buildConcurrencyError("item", "uuid", concurrency.NoStream(), -1)

	var cce apperrors.ConcurrencyError
	if !errors.As(got, &cce) {
		t.Fatalf("errors.As failed; got %T", got)
	}
	if cce.ExpectedRevision != -1 {
		t.Fatalf("ExpectedRevision = %d, want -1 (NoStream)", cce.ExpectedRevision)
	}
}

// TestBuildConcurrencyErrorAnyExpectedEncodesAsNegativeOne verifies the
// zero-value (Any) expectation is also encoded as -1, matching the NoStream
// convention. Any is rare in production code (handlers must specify NoStream
// or AtRevision per the Expected doc) but the encoding must not surprise
// operators who see it in logs.
func TestBuildConcurrencyErrorAnyExpectedEncodesAsNegativeOne(t *testing.T) {
	t.Parallel()

	got := buildConcurrencyError("item", "uuid", concurrency.Any(), -1)

	var cce apperrors.ConcurrencyError
	if !errors.As(got, &cce) {
		t.Fatalf("errors.As failed; got %T", got)
	}
	if cce.ExpectedRevision != -1 {
		t.Fatalf("ExpectedRevision = %d, want -1 (Any)", cce.ExpectedRevision)
	}
}

// TestMapAppendErrorWrapsNonKurrentErrors verifies that errors that are not
// *kurrentdb.Error at all (e.g. gRPC transport failures) are still wrapped
// with the stream name for diagnosis rather than swallowed or misclassified
// as conflicts.
func TestMapAppendErrorWrapsNonKurrentErrors(t *testing.T) {
	t.Parallel()

	plainErr := errors.New("grpc: connection refused")
	got := mapAppendError(plainErr, "country-uuid-x", "country", "uuid-x", concurrency.AtRevision(1))

	if got == nil {
		t.Fatal("got = nil")
	}
	if !strings.Contains(got.Error(), "country-uuid-x") {
		t.Fatalf("error should contain stream name: got %q", got.Error())
	}
	if !errors.Is(got, plainErr) {
		t.Fatalf("errors.Is(got, plainErr) = false; underlying cause should be preserved")
	}
}

// --- stream-absence classification (D127) ---

// TestClassifyStreamAbsenceCoversAllCodes exhaustively pins the tri-state
// read classification — the unit-testable seam for the tombstoned-stream fix
// (D127). *kurrentdb.Error's fields are unexported, so tests drive the
// mapping by code exactly like buildConcurrencyError; the live KurrentDB
// tombstone integration tests (tombstone_integration_test.go) pin the code
// extraction end-to-end.
//
// The classification mirrors the purge tooling's tri-state vocabulary
// (cmd/admin purge_discover.go mapReadStreamState): ResourceNotFound →
// absent; StreamDeleted/StreamTombstoned → hard-deleted; everything else →
// operational (must propagate, never be mistaken for absence).
func TestClassifyStreamAbsenceCoversAllCodes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		code kurrentdb.ErrorCode
		want streamAbsenceClass
	}{
		{"ResourceNotFound", kurrentdb.ErrorCodeResourceNotFound, streamAbsent},
		{"StreamDeleted", kurrentdb.ErrorCodeStreamDeleted, streamHardDeleted},
		{"StreamTombstoned", kurrentdb.ErrorCodeStreamTombstoned, streamHardDeleted},
		// Operational classes must NEVER classify as absence — treating a
		// transport/authorization/timeout/server failure as "no aggregate"
		// would mask real outages as 404s.
		{"Unknown", kurrentdb.ErrorCodeUnknown, streamNotAbsent},
		{"Unauthenticated", kurrentdb.ErrorCodeUnauthenticated, streamNotAbsent},
		{"AccessDenied", kurrentdb.ErrorCodeAccessDenied, streamNotAbsent},
		{"DeadlineExceeded", kurrentdb.ErrorCodeDeadlineExceeded, streamNotAbsent},
		{"Unavailable", kurrentdb.ErrorUnavailable, streamNotAbsent},
		{"NotLeader", kurrentdb.ErrorCodeNotLeader, streamNotAbsent},
		{"InternalServer", kurrentdb.ErrorCodeInternalServer, streamNotAbsent},
		{"WrongExpectedVersion", kurrentdb.ErrorCodeWrongExpectedVersion, streamNotAbsent},
		{"StreamRevisionConflict", kurrentdb.ErrorCodeStreamRevisionConflict, streamNotAbsent},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyStreamAbsence(tc.code); got != tc.want {
				t.Fatalf("classifyStreamAbsence(%v) = %d, want %d", tc.code, got, tc.want)
			}
		})
	}
}

// TestClassifyStreamReadErrorNonKurrentErrors verifies the wrapping-aware
// classifier's negative paths: errors that carry no *kurrentdb.Error —
// plain, wrapped, and even typed application errors — classify as
// operational and must propagate. (Positive absence classification is pinned
// by the live tombstone integration tests.)
func TestClassifyStreamReadErrorNonKurrentErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
	}{
		{"plain error", errors.New("grpc: connection refused")},
		{"wrapped plain error", fmt.Errorf("eventstore recv stream-x: %w", errors.New("socket reset"))},
		{"double-wrapped plain error", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", errors.New("dial tcp: refused")))},
		{"typed NotFoundError", apperrors.NewNotFoundError("country", "id")},
		{"typed ConflictError", apperrors.NewConflictError("country", "boom")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyStreamReadError(tc.err); got != streamNotAbsent {
				t.Fatalf("classifyStreamReadError(%v) = %d, want %d (operational)", tc.err, got, streamNotAbsent)
			}
		})
	}
}

// TestBuildPermanentlyDeletedConflictError verifies the append-path
// translation for hard-deleted (tombstoned) streams: a typed, actionable
// ConflictError carrying the shared stable sentence, with the aggregate type
// as the problem code — and explicitly NOT a ConcurrencyError, so the
// OCC branches (which retry/compensate differently) can never mistake a
// tombstone for a revision race (D127).
func TestBuildPermanentlyDeletedConflictError(t *testing.T) {
	t.Parallel()

	got := buildPermanentlyDeletedConflictError("truckbijakslot")

	var ce apperrors.ConflictError
	if !errors.As(got, &ce) {
		t.Fatalf("errors.As(got, &ConflictError) = false; got %T: %v", got, got)
	}
	if ce.Code != "truckbijakslot" {
		t.Fatalf("Code = %q, want %q", ce.Code, "truckbijakslot")
	}
	if ce.Message != apperrors.PermanentlyDeletedConflictMessage {
		t.Fatalf("Message = %q, want the shared stable sentence %q", ce.Message, apperrors.PermanentlyDeletedConflictMessage)
	}
	if strings.Contains(ce.Message, "stream") || strings.Contains(ce.Message, "eventstore") {
		t.Fatalf("Message must not leak store internals: %q", ce.Message)
	}

	var cce apperrors.ConcurrencyError
	if errors.As(got, &cce) {
		t.Fatalf("permanently-deleted conflict must not be a ConcurrencyError; got %T", got)
	}
}

// --- resolveMetadata (H8 correlation/causation propagation) ---

// TestResolveMetadataAbsentReturnsFalse verifies the fallback path: when the
// context carries no correlation Context (background workers, projection
// subscriptions, tests without the middleware), Save must fall back to the
// metadata-less ToEventData so those callers are unaffected by the audit
// change.
func TestResolveMetadataAbsentReturnsFalse(t *testing.T) {
	t.Parallel()

	_, ok := resolveMetadata(context.Background())
	if ok {
		t.Fatal("resolveMetadata(background): ok=true, want false")
	}
}

// TestResolveMetadataPresentMapsFields verifies the happy path: a correlation
// Context in ctx is translated 1:1 into a KurrentDB EventMetadata envelope
// so Save can stamp correlation/causation/user IDs onto every written event.
func TestResolveMetadataPresentMapsFields(t *testing.T) {
	t.Parallel()

	ctx := correlation.WithContext(context.Background(), correlation.Context{
		CorrelationID: "corr-9",
		CausationID:   "cause-9",
		CommandID:     "cmd-9",
		UserID:        "user-9",
	})

	meta, ok := resolveMetadata(ctx)
	if !ok {
		t.Fatal("resolveMetadata: ok=false, want true")
	}
	if meta.CorrelationID != "corr-9" {
		t.Fatalf("CorrelationID = %q, want %q", meta.CorrelationID, "corr-9")
	}
	if meta.CausationID != "cause-9" {
		t.Fatalf("CausationID = %q, want %q", meta.CausationID, "cause-9")
	}
	if meta.UserID != "user-9" {
		t.Fatalf("UserID = %q, want %q", meta.UserID, "user-9")
	}
}
