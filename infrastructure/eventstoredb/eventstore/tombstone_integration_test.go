package eventstore

// Live KurrentDB integration tests for tombstoned-stream classification
// (D127): a hard-deleted (tombstoned) stream is PERMANENTLY unusable —
// KurrentDB rejects every append to it, including with a NoStream
// expectation — and the generic Store must classify reads of it as typed
// aggregate absence (NotFoundError), appends as the typed actionable
// ConflictError, and the StreamPermanentlyDeleted probe as true. Before D127
// all three surfaced as raw errors: a production bijak issuance against a
// purge-tombstoned truckbijakslot stream masked as an unlogged 500.
//
// These tests pin the *kurrentdb.Error code extraction end-to-end (the SDK's
// error fields are unexported, so the unit suite can only drive the pure
// classification by code). They auto-skip when KURRENTDB_URL is unset.
// Scratch streams use unique names; a tombstone marker is permanent but
// inert — the purge tooling creates the same shape.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"github.com/aliighahremanii/buildingblocks/application/concurrency"
	apperrors "github.com/aliighahremanii/buildingblocks/application/errors"
	domainevents "github.com/aliighahremanii/buildingblocks/domain/events"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/kurrenttest"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/registry"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/snapshot"
	"github.com/aliighahremanii/buildingblocks/infrastructure/postgres/postgrestest"
)

// newTombstoneTestStore builds a Store wired with a stub registry for the
// stub event used by these tests. No snapshots — the full-replay path.
func newTombstoneTestStore(client *kurrentdb.Client) *Store {
	reg := registry.New()
	reg.Register("stub.tombstone", func() domainevents.Event { return &stubSnapshotEvent{} })
	return New(client, StoreConfig{
		AggregateType: "stubtomb",
		Registry:      reg,
	})
}

// appendRawStubEvent appends one schema-valid stub event to the stream via
// the raw client so the test controls stream lifecycle exactly (Save is what
// is being asserted, not used for setup).
func appendRawStubEvent(t *testing.T, client *kurrentdb.Client, streamName string) {
	t.Helper()
	_, err := client.AppendToStream(context.Background(), streamName,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}},
		kurrentdb.EventData{
			EventID:     uuid.New(),
			EventType:   "stub.tombstone",
			ContentType: kurrentdb.ContentTypeJson,
			Data:        []byte(`{"schemaVersion":1}`),
		})
	if err != nil {
		t.Fatalf("seed append %s: %v", streamName, err)
	}
}

// tombstoneStream hard-deletes the stream, mirroring the purge tooling's
// Tombstone call (cmd/admin purge_discover.go).
func tombstoneStream(t *testing.T, client *kurrentdb.Client, streamName string) {
	t.Helper()
	if _, err := client.TombstoneStream(context.Background(), streamName, kurrentdb.TombstoneStreamOptions{}); err != nil {
		t.Fatalf("tombstone %s: %v", streamName, err)
	}
}

// loadStub is the minimal RehydrateFunc for LoadAggregate over the stub
// registry entry.
func loadStub(id string, snapshot []byte, _ uint64, tail []domainevents.Event) (*stubAggregate, error) {
	if snapshot == nil && len(tail) == 0 {
		return nil, apperrors.NewNotFoundError("stubtomb", id)
	}
	return &stubAggregate{id: id}, nil
}

// TestLiveTombstonedStreamReadsAsTypedNotFound proves the primary fix: after
// a hard delete, LoadAggregate (full-replay path, no snapshot) returns the
// same typed NotFoundError as a never-created stream — never the raw
// [ErrorCodeStreamDeleted] error that masked as a 500 in production.
func TestLiveTombstonedStreamReadsAsTypedNotFound(t *testing.T) {
	client := kurrenttest.OpenClient(t)
	store := newTombstoneTestStore(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := kurrenttest.UniqueStreamName(t, "stubtomb")
	appendRawStubEvent(t, client, "stubtomb-"+id)
	tombstoneStream(t, client, "stubtomb-"+id)

	_, _, err := LoadAggregate(store, ctx, id, loadStub)
	var nf apperrors.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("LoadAggregate on tombstoned stream: err=%v (%T), want typed NotFoundError", err, err)
	}
	if nf.Resource != "stubtomb" {
		t.Fatalf("NotFoundError.Resource = %q, want %q", nf.Resource, "stubtomb")
	}
}

// TestLiveTombstonedStreamAppendMapsToTypedConflict proves the write-path
// fix: appending to a tombstoned stream — even with a Register's NoStream
// expectation, which the server rejects permanently — surfaces as the typed
// actionable ConflictError with the shared stable sentence, never a raw
// wrapped error (masked 500) and never a ConcurrencyError (which callers
// treat as a retryable OCC race).
func TestLiveTombstonedStreamAppendMapsToTypedConflict(t *testing.T) {
	client := kurrenttest.OpenClient(t)
	store := newTombstoneTestStore(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := kurrenttest.UniqueStreamName(t, "stubtomb")
	appendRawStubEvent(t, client, "stubtomb-"+id)
	tombstoneStream(t, client, "stubtomb-"+id)

	agg := &stubAggregate{id: id}
	agg.events = []domainevents.Event{&stubSnapshotEvent{
		Base: domainevents.NewBase("stub.tombstone", id, "stubtomb", time.Now()),
	}}
	_, err := store.Save(ctx, id, concurrency.NoStream(), agg.events)
	if err == nil {
		t.Fatal("Save onto tombstoned stream: err=nil, want typed ConflictError")
	}
	var ce apperrors.ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("Save on tombstoned stream: err=%v (%T), want typed ConflictError", err, err)
	}
	if ce.Code != "stubtomb" {
		t.Fatalf("ConflictError.Code = %q, want %q", ce.Code, "stubtomb")
	}
	if ce.Message != apperrors.PermanentlyDeletedConflictMessage {
		t.Fatalf("ConflictError.Message = %q, want shared stable sentence %q", ce.Message, apperrors.PermanentlyDeletedConflictMessage)
	}
	var cce apperrors.ConcurrencyError
	if errors.As(err, &cce) {
		t.Fatalf("tombstone append must not classify as ConcurrencyError; got %T", err)
	}
}

// TestLiveStreamPermanentlyDeletedProbe pins the checker's tri-state over a
// live server: tombstoned → true; ordinary absent (never created) → false
// with nil error. Both absence shapes read as the same NotFoundError through
// ByID, so the probe is the ONLY way the issue workflow can avoid burning a
// bijak number against a permanently unusable guard stream.
func TestLiveStreamPermanentlyDeletedProbe(t *testing.T) {
	client := kurrenttest.OpenClient(t)
	store := newTombstoneTestStore(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tombstonedID := kurrenttest.UniqueStreamName(t, "stubtomb")
	appendRawStubEvent(t, client, "stubtomb-"+tombstonedID)
	tombstoneStream(t, client, "stubtomb-"+tombstonedID)

	gone, err := store.StreamPermanentlyDeleted(ctx, tombstonedID)
	if err != nil {
		t.Fatalf("probe tombstoned: %v", err)
	}
	if !gone {
		t.Fatal("probe on tombstoned stream = false, want true")
	}

	freshID := kurrenttest.UniqueStreamName(t, "stubtomb")
	gone, err = store.StreamPermanentlyDeleted(ctx, freshID)
	if err != nil {
		t.Fatalf("probe never-created: %v", err)
	}
	if gone {
		t.Fatal("probe on never-created stream = true, want false (ordinary absence)")
	}

	// A live, readable stream also probes false (control case).
	liveID := kurrenttest.UniqueStreamName(t, "stubtomb")
	appendRawStubEvent(t, client, "stubtomb-"+liveID)
	gone, err = store.StreamPermanentlyDeleted(ctx, liveID)
	if err != nil {
		t.Fatalf("probe live stream: %v", err)
	}
	if gone {
		t.Fatal("probe on live stream = true, want false")
	}
}

// TestLiveTombstoneClassificationPreservesHealthyStream is the
// existing-behavior guard: with the classification in place, a healthy
// stream still round-trips (append → load) and a never-created stream still
// reads as typed NotFoundError.
func TestLiveTombstoneClassificationPreservesHealthyStream(t *testing.T) {
	client := kurrenttest.OpenClient(t)
	store := newTombstoneTestStore(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := kurrenttest.UniqueStreamName(t, "stubtomb")
	appendRawStubEvent(t, client, "stubtomb-"+id)

	agg, _, err := LoadAggregate(store, ctx, id, loadStub)
	if err != nil {
		t.Fatalf("load healthy stream: %v", err)
	}
	if agg.id != id {
		t.Fatalf("loaded id = %q, want %q", agg.id, id)
	}

	missingID := kurrenttest.UniqueStreamName(t, "stubtomb")
	_, _, err = LoadAggregate(store, ctx, missingID, loadStub)
	var nf apperrors.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("load never-created stream: err=%v (%T), want typed NotFoundError", err, err)
	}
}

// TestLiveTombstonedStreamWithSnapshotReadsAsTypedNotFound covers the
// snapshot/tail read path: a purge tombstones KurrentDB streams but not the
// Postgres snapshot rows, so a snapshot HIT followed by a hard-deleted
// stream must still classify as aggregate absence — the write model is the
// system of record. Before D127 this drift shape surfaced as a raw error
// from ReadTail. Requires live KurrentDB + Postgres; auto-skips otherwise.
func TestLiveTombstonedStreamWithSnapshotReadsAsTypedNotFound(t *testing.T) {
	client := kurrenttest.OpenClient(t)
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_snapshots")

	reg := registry.New()
	reg.Register("stub.tombstone", func() domainevents.Event { return &stubSnapshotEvent{} })
	store := New(client, StoreConfig{
		AggregateType: "stubtomb",
		Registry:      reg,
		Snapshots:     snapshot.New(pool),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := kurrenttest.UniqueStreamName(t, "stubtomb")
	appendRawStubEvent(t, client, "stubtomb-"+id)
	if err := store.SaveSnapshot(ctx, id, 1, []byte(`{"state":1,"id":"`+id+`"}`)); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}
	tombstoneStream(t, client, "stubtomb-"+id)

	_, _, err := LoadAggregate(store, ctx, id, loadStub)
	var nf apperrors.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("LoadAggregate on tombstoned stream with surviving snapshot: err=%v (%T), want typed NotFoundError", err, err)
	}
}
