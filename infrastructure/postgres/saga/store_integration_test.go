// Integration tests for the Postgres-backed saga Store. They auto-skip
// when POSTGRES_DSN is unset so unit-only CI environments continue to
// pass, and exercise the real concurrency and timeout-sweep contracts
// against the shared `sagas` / `saga_timeouts` tables created by
// migration 20260726000500_sagas.
//
// Each test takes the postgrestest integration lock and truncates the
// saga tables in t.Cleanup so it leaves nothing behind.
package saga

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aliighahremanii/buildingblocks/infrastructure/postgres/postgrestest"
	bbsaga "github.com/aliighahremanii/buildingblocks/infrastructure/saga"
)

func newStoreOrSkip(t *testing.T) *Store {
	t.Helper()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	t.Cleanup(func() {
		postgrestest.TruncateTables(t, pool, "saga_timeouts", "sagas")
	})
	return New(pool)
}

func TestStoreSaveLoadRoundTrip(t *testing.T) {
	store := newStoreOrSkip(t)
	ctx := context.Background()

	in := bbsaga.Instance{
		ID:            "sample1.processpending-00000000-0000-0000-0000-000000000001",
		Type:          "sample1.processpending",
		Status:        bbsaga.StatusRunning,
		State:         []byte(`{"itemID":"00000000-0000-0000-0000-000000000001","startedAt":"2026-07-26T10:00:00Z","status":"running"}`),
		CorrelationID: "corr-1",
		CausationID:   "caus-1",
		Version:       0,
	}
	if err := store.Save(ctx, in); err != nil {
		t.Fatalf("save new: %v", err)
	}

	got, found, err := store.Load(ctx, in.ID)
	if err != nil || !found {
		t.Fatalf("load after save: found=%v err=%v", found, err)
	}
	if got.Type != in.Type || got.Status != in.Status ||
		got.CorrelationID != in.CorrelationID || got.CausationID != in.CausationID ||
		got.Version != 0 {
		t.Fatalf("load round-trip mismatch:\n got=%+v\n want=%+v", got, in)
	}
	// JSONB reorders object keys canonically, so compare the parsed
	// payloads rather than the raw bytes.
	if !jsonEqual(t, got.State, in.State) {
		t.Fatalf("state round-trip mismatch:\n got=%s\n want=%s", got.State, in.State)
	}

	// Transition with version bump.
	in.Status = bbsaga.StatusCompleted
	in.Version = 1
	if err := store.Save(ctx, in); err != nil {
		t.Fatalf("save transition: %v", err)
	}
	got, _, _ = store.Load(ctx, in.ID)
	if got.Status != bbsaga.StatusCompleted || got.Version != 1 {
		t.Fatalf("transition not stored: %+v", got)
	}
}

func TestStoreSaveVersionConflict(t *testing.T) {
	store := newStoreOrSkip(t)
	ctx := context.Background()

	in := bbsaga.Instance{
		ID:      "conflict-1",
		Type:    "t",
		Status:  bbsaga.StatusRunning,
		Version: 0,
	}
	if err := store.Save(ctx, in); err != nil {
		t.Fatalf("initial save: %v", err)
	}

	// Stale save: caller still believes version 0 but the row already
	// exists at version 0, so replaying version 0 (expected prior = -1)
	// must conflict.
	err := store.Save(ctx, bbsaga.Instance{
		ID:      in.ID,
		Type:    "t",
		Status:  bbsaga.StatusRunning,
		Version: 0,
	})
	if !errors.Is(err, bbsaga.ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict on stale save, got %v", err)
	}

	// Correct optimistic transition: version 0 -> 1.
	in.Status = bbsaga.StatusCompleted
	in.Version = 1
	if err := store.Save(ctx, in); err != nil {
		t.Fatalf("save after conflict should succeed: %v", err)
	}

	// Save-again at version 1 (stored is now 1, expected prior must be 0)
	// while caller keeps stale version 0: conflict.
	err = store.Save(ctx, bbsaga.Instance{
		ID:      in.ID,
		Type:    "t",
		Status:  bbsaga.StatusFailed,
		Version: 1, // caller thinks this is a fresh write but row is at v1
	})
	if !errors.Is(err, bbsaga.ErrConcurrencyConflict) {
		t.Fatalf("expected conflict when stored version <= caller version, got %v", err)
	}
}

func TestStoreSaveUpsertReplacesFields(t *testing.T) {
	store := newStoreOrSkip(t)
	ctx := context.Background()
	id := "upsert-1"

	// Create at v0 with running status and correlation "a".
	in := bbsaga.Instance{ID: id, Type: "t", Status: bbsaga.StatusRunning, CorrelationID: "a", Version: 0}
	if err := store.Save(ctx, in); err != nil {
		t.Fatal(err)
	}

	// Transition v0->v1, change state and correlation.
	in.Status = bbsaga.StatusCompleted
	in.State = []byte(`{"done":true}`)
	in.CorrelationID = "b"
	in.Version = 1
	if err := store.Save(ctx, in); err != nil {
		t.Fatal(err)
	}

	got, _, _ := store.Load(ctx, id)
	if got.CorrelationID != "b" || !jsonEqual(t, got.State, []byte(`{"done":true}`)) {
		t.Fatalf("upsert did not replace fields: %+v", got)
	}
}

func TestScheduleTimeout_UpsertsIdempotently(t *testing.T) {
	store := newStoreOrSkip(t)
	ctx := context.Background()
	id := "timeout-upsert-1"

	if err := store.ScheduleTimeout(ctx, id, "", time.Now().Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.ScheduleTimeout(ctx, id, "", time.Now().Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	// Re-scheduling the same (saga_id, kind) must not double-insert.
	got, _, _ := store.Load(ctx, id)
	_ = got // instance row never got created; that's fine
	var n int
	if err := store.pool.QueryRow(ctx, "SELECT count(*) FROM saga_timeouts WHERE saga_id = $1", id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 timeout row, got %d", n)
	}
}

func TestListDueTimeouts_ReturnsDueOnly(t *testing.T) {
	store := newStoreOrSkip(t)
	ctx := context.Background()
	now := time.Now()

	dueID := "due-1"
	futureID := "future-1"
	if err := store.ScheduleTimeout(ctx, dueID, "", now.Add(-1*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.ScheduleTimeout(ctx, futureID, "", now.Add(1*time.Hour)); err != nil {
		t.Fatal(err)
	}

	ids, err := store.ListDueTimeouts(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != dueID {
		t.Fatalf("expected only dueID, got %v", ids)
	}
}

func TestDeleteTimeout_RemovesRow(t *testing.T) {
	store := newStoreOrSkip(t)
	ctx := context.Background()
	id := "delete-1"

	if err := store.ScheduleTimeout(ctx, id, "", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteTimeout(ctx, id, ""); err != nil {
		t.Fatalf("delete: %v", err)
	}
	ids, err := store.ListDueTimeouts(ctx, time.Now().Add(time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, got := range ids {
		if got == id {
			t.Fatalf("row should be deleted, found %s", id)
		}
	}
}

// jsonEqual reports whether two JSON payloads encode the same value.
// Postgres JSONB stores object keys in canonical (sorted) order, so a
// raw byte comparison of a round-tripped payload would spuriously fail
// whenever the Go-side marshaler emits keys in struct-field order.
func jsonEqual(t *testing.T, a, b []byte) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatalf("jsonEqual: parse a: %v", err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatalf("jsonEqual: parse b: %v", err)
	}
	return reflect.DeepEqual(av, bv)
}
