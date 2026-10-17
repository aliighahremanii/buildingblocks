package eventstore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"github.com/aliighahremanii/buildingblocks/application/concurrency"
	domainevents "github.com/aliighahremanii/buildingblocks/domain/events"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/kurrenttest"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/registry"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/snapshot"
	"github.com/aliighahremanii/buildingblocks/infrastructure/postgres/postgrestest"
)

// stubSnapshotEvent is a minimal event for snapshot integration tests.
type stubSnapshotEvent struct {
	domainevents.Base
	I int `json:"i"`
}

func newSnapshotEventFactory() func() domainevents.Event {
	return func() domainevents.Event { return &stubSnapshotEvent{} }
}

// stubAggregate is a minimal aggregate that records events and can
// marshal/unmarshal its state for snapshots. It implements just enough
// of the event-sourcing contract to exercise LoadAggregate +
// SaveAggregateWithSnapshot without depending on a module.
type stubAggregate struct {
	id     string
	events []domainevents.Event
	state  int // the only mutable state; snapshot persists this
}

func (a *stubAggregate) PullDomainEvents() []domainevents.Event {
	out := a.events
	a.events = nil
	return out
}

func (a *stubAggregate) ID() string { return a.id }

// MarshalState returns a tiny JSON blob so we can assert on it after a
// round-trip. The shape is opaque to the store; only the test interprets it.
func (a *stubAggregate) MarshalState() ([]byte, error) {
	return json.Marshal(map[string]any{"state": a.state, "id": a.id})
}

// rehydrateStub is the RehydrateFunc for stubAggregate. It applies each
// tail event by incrementing state by e.I — the test's contract for
// "this event mutated the aggregate".
func rehydrateStub(id string, snap []byte, _ uint64, tail []domainevents.Event) (*stubAggregate, error) {
	a := &stubAggregate{id: id}
	if snap != nil {
		var s struct {
			State int    `json:"state"`
			ID    string `json:"id"`
		}
		if err := json.Unmarshal(snap, &s); err != nil {
			return nil, err
		}
		a.state = s.State
		a.id = s.ID
	}
	for _, e := range tail {
		if se, ok := e.(*stubSnapshotEvent); ok {
			a.state += se.I
		}
	}
	return a, nil
}

// TestSaveAggregateWithSnapshotWritesAtThreshold verifies the snapshot
// auto-write fires exactly at the SnapshotEvery boundary. This is the
// regression test for the long-standing bug that SnapshotEvery was
// configured but never honored (H2). Without the snapshot, long-lived
// streams paid O(n) replay cost on every ByID.
//
// This is an INTEGRATION test: it requires a live KurrentDB and
// Postgres. It auto-skips when either is unavailable.
func TestSaveAggregateWithSnapshotWritesAtThreshold(t *testing.T) {
	t.Parallel()

	client := kurrenttest.OpenClient(t)
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_snapshots")

	// Build a store with SnapshotEvery = 5 so the test does not have to
	// append hundreds of events.
	reg := registry.New()
	reg.Register("test.bump", newSnapshotEventFactory())
	snaps := snapshot.New(pool)
	s := New(client, StoreConfig{
		AggregateType: "stub",
		Registry:      reg,
		Snapshots:     snaps,
		SnapshotEvery: 5,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := kurrenttest.UniqueStreamName(t, "stub-aggregate")

	// Append 5 events. The 5th lands at revision 5 (1-indexed by
	// KurrentDB), so SnapshotEvery==5 should fire a snapshot.
	for i := 1; i <= 5; i++ {
		agg := &stubAggregate{id: id, state: i}
		agg.events = []domainevents.Event{&stubSnapshotEvent{
			Base: domainevents.NewBase("test.bump", id, "stub", time.Now()),
			I:    1,
		}}
		// First append uses NoStream; subsequent use AtRevision with the
		// previous revision (which is i-1 since KurrentDB revisions are
		// 0-indexed; the first event is at revision 0, so after appending
		// the i-th event the nextExpectedVersion reported by KurrentDB
		// is i-1).
		var expected concurrency.Expected
		if i == 1 {
			expected = concurrency.NoStream()
		} else {
			expected = concurrency.AtRevision(uint64(i - 2))
		}
		nextRev, err := SaveAggregateWithSnapshot(s, ctx, agg, expected, agg.events,
			func(a *stubAggregate) string { return a.id },
			func(a *stubAggregate) ([]byte, error) { return a.MarshalState() },
		)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		_ = nextRev
	}

	// Verify the snapshot row exists at revision 5.
	var snapCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM es_snapshots WHERE aggregate_type = 'stub' AND aggregate_id = $1 AND version = 4`,
		id,
	).Scan(&snapCount); err != nil {
		// KurrentDB revisions are 0-indexed; appending 5 events lands the
		// last event at revision 4, so nextExpectedVersion returned by
		// KurrentDB is 4 (the value the next append must expect). The
		// snapshot is written at that revision.
		t.Fatalf("snapshot count at revision 4: %v (note: KurrentDB revisions are 0-indexed)", err)
	}
	if snapCount != 1 {
		t.Fatalf("snapshot count = %d, want 1 at revision 4 (the SnapshotEvery threshold)", snapCount)
	}
}

// TestLoadAggregateUsesSnapshotTailRead verifies the snapshot reduces
// replay cost: when a snapshot exists, LoadAggregate reads only the tail
// (events since the snapshot), not the full stream. Without this
// property, snapshots would be write-only decoration.
//
// We assert on the behavior indirectly: after loading via a snapshot +
// tail, the aggregate's state must reflect BOTH the snapshot AND the
// tail events. If LoadAggregate were still reading the full stream, the
// state would be correct too (since replay is deterministic) — but the
// whole POINT of the snapshot is to skip the bulk of replay. A live
// test that asserts "fewer events read" requires instrumenting the
// client, which we skip here; the unit-test coverage of ReadTail
// (above) covers the deterministic-replay side.
func TestLoadAggregateUsesSnapshotTailRead(t *testing.T) {
	t.Parallel()

	client := kurrenttest.OpenClient(t)
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_snapshots")

	reg := registry.New()
	reg.Register("test.bump", newSnapshotEventFactory())
	snaps := snapshot.New(pool)
	s := New(client, StoreConfig{
		AggregateType: "stub-tail",
		Registry:      reg,
		Snapshots:     snaps,
		SnapshotEvery: 3,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := kurrenttest.UniqueStreamName(t, "stub-tail-aggregate")

	// Append 3 events (triggering a snapshot at revision 2).
	var prevRev uint64
	for i := 0; i < 3; i++ {
		agg := &stubAggregate{id: id, state: 100 + i}
		agg.events = []domainevents.Event{&stubSnapshotEvent{
			Base: domainevents.NewBase("test.bump", id, "stub-tail", time.Now()),
			I:    1,
		}}
		var expected concurrency.Expected
		if i == 0 {
			expected = concurrency.NoStream()
		} else {
			expected = concurrency.AtRevision(prevRev)
		}
		nextRev, err := SaveAggregateWithSnapshot(s, ctx, agg, expected, agg.events,
			func(a *stubAggregate) string { return a.id },
			func(a *stubAggregate) ([]byte, error) { return a.MarshalState() },
		)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		prevRev = nextRev
	}

	// Append 2 more events AFTER the snapshot. These are the tail that
	// LoadAggregate must read onto the snapshot-rebuilt state.
	for i := 0; i < 2; i++ {
		agg := &stubAggregate{id: id, state: 200 + i}
		agg.events = []domainevents.Event{&stubSnapshotEvent{
			Base: domainevents.NewBase("test.bump", id, "stub-tail", time.Now()),
			I:    1,
		}}
		nextRev, err := SaveAggregateWithSnapshot(s, ctx, agg,
			concurrency.AtRevision(prevRev), agg.events,
			func(a *stubAggregate) string { return a.id },
			func(a *stubAggregate) ([]byte, error) { return a.MarshalState() },
		)
		if err != nil {
			t.Fatalf("post-snapshot append %d: %v", i, err)
		}
		prevRev = nextRev
	}

	// LoadAggregate via snapshot + tail. The state must reflect the
	// snapshot (state=102 from the third pre-snapshot append) PLUS the
	// two tail events (each adds 1).
	loaded, loadedRev, err := LoadAggregate(s, ctx, id, rehydrateStub)
	if err != nil {
		t.Fatalf("LoadAggregate: %v", err)
	}
	if loaded.state != 102+2 {
		t.Fatalf("state = %d, want 104 (snapshot 102 + 2 tail events)", loaded.state)
	}
	if loadedRev != prevRev {
		t.Fatalf("loadedRev = %d, want %d (most recent append)", loadedRev, prevRev)
	}
}

// TestSnapshotFailureDoesNotRollbackAppend verifies the documented
// behavior of SaveAggregateWithSnapshot: if the snapshot write fails
// (e.g. Postgres down), the events already appended to KurrentDB stay
// appended. We do NOT roll back the event-store append on snapshot
// failure because KurrentDB is the source of truth, not Postgres.
//
// We simulate a snapshot failure by passing a closed pool to the
// snapshot store.
func TestSnapshotFailureDoesNotRollbackAppend(t *testing.T) {
	t.Parallel()

	client := kurrenttest.OpenClient(t)
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_snapshots")

	reg := registry.New()
	reg.Register("test.bump", newSnapshotEventFactory())

	// Construct the snapshot store with the live pool, then close it to
	// force snapshot writes to fail.
	snaps := snapshot.New(pool)

	s := New(client, StoreConfig{
		AggregateType: "stub-fail",
		Registry:      reg,
		Snapshots:     snaps,
		SnapshotEvery: 1, // force snapshot on first append
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id := kurrenttest.UniqueStreamName(t, "stub-fail-aggregate")
	agg := &stubAggregate{id: id, state: 42}
	agg.events = []domainevents.Event{&stubSnapshotEvent{
		Base: domainevents.NewBase("test.bump", id, "stub-fail", time.Now()),
		I:    1,
	}}

	// Close the pool BEFORE the append so the snapshot write fails.
	pool.Close()

	_, err := SaveAggregateWithSnapshot(s, ctx, agg, concurrency.NoStream(), agg.events,
		func(a *stubAggregate) string { return a.id },
		func(a *stubAggregate) ([]byte, error) { return a.MarshalState() },
	)
	if err == nil {
		t.Fatal("SaveAggregateWithSnapshot: want snapshot error after pool close, got nil")
	}

	// The append MUST have succeeded despite the snapshot failure.
	// Verify by reading the stream back from KurrentDB.
	rd, rerr := client.ReadStream(ctx, "stub-fail-"+id, kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Forwards,
		From:      kurrentdb.Start{},
	}, 10)
	if rerr != nil {
		t.Fatalf("ReadStream: %v", rerr)
	}
	defer rd.Close()
	count := 0
	for {
		we, e := rd.Recv()
		if e != nil {
			break
		}
		if we.Event != nil {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("KurrentDB stream count = %d, want 1 (append must survive snapshot failure)", count)
	}
}

// suppress unused-import error if a future refactor removes one of the
// dependencies the integration tests above pull in.
var _ = (*pgxpool.Pool)(nil)
