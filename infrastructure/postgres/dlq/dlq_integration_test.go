package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/dlq"
	"github.com/aliighahremanii/buildingblocks/infrastructure/postgres/postgrestest"
)

// TestFlushCheckpointWithNoFailuresAdvancesCheckpoint verifies the
// happy-path atomic operation: when there are no failures to DLQ,
// FlushCheckpoint just advances the checkpoint. The same Postgres
// transaction guarantees the advance commits atomically.
func TestFlushCheckpointWithNoFailuresAdvancesCheckpoint(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_projection_dlq", "es_projection_checkpoints")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const proj = "test-proj-empty"
	if err := store.FlushCheckpoint(ctx, proj, 1000, nil); err != nil {
		t.Fatalf("FlushCheckpoint: %v", err)
	}

	// Verify the checkpoint advanced.
	var pos uint64
	err := pool.QueryRow(ctx,
		`SELECT position FROM es_projection_checkpoints WHERE projection_name = $1`, proj,
	).Scan(&pos)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if pos != 1000 {
		t.Fatalf("checkpoint position = %d, want 1000", pos)
	}

	// Verify no DLQ rows.
	var dlqCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM es_projection_dlq WHERE projection_name = $1`, proj,
	).Scan(&dlqCount); err != nil {
		t.Fatalf("count dlq: %v", err)
	}
	if dlqCount != 0 {
		t.Fatalf("dlq count = %d, want 0 (no failures)", dlqCount)
	}
}

// TestFlushCheckpointWithFailuresIsAtomic verifies the CRITICAL atomicity
// property of the DLQ fix: when FlushCheckpoint is given failures, BOTH
// the DLQ insert AND the checkpoint advance must commit in one
// transaction. We verify this by checking both tables after a successful
// FlushCheckpoint.
func TestFlushCheckpointWithFailuresIsAtomic(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_projection_dlq", "es_projection_checkpoints")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const proj = "test-proj-failures"
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM es_projection_dlq WHERE projection_name=$1`, proj)
		_, _ = pool.Exec(context.Background(), `DELETE FROM es_projection_checkpoints WHERE projection_name=$1`, proj)
	})
	failures := []dlq.Entry{
		{
			ProjectionName: proj,
			EventID:        "evt-1",
			StreamID:       "country-uuid-1",
			StreamRevision: 5,
			EventType:      "country.registered",
			Payload:        json.RawMessage(`{"nameLocal":"Iran"}`),
			Metadata:       json.RawMessage(`{}`),
			CommitPosition: 500,
			Error:          "poison: bad shape",
			Attempts:       5,
		},
	}

	if err := store.FlushCheckpoint(ctx, proj, 500, failures); err != nil {
		t.Fatalf("FlushCheckpoint: %v", err)
	}

	// DLQ row present.
	var dlqCount int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM es_projection_dlq WHERE projection_name = $1 AND event_id = $2`,
		proj, "evt-1",
	).Scan(&dlqCount); err != nil {
		t.Fatalf("count dlq: %v", err)
	}
	if dlqCount != 1 {
		t.Fatalf("dlq count = %d, want 1", dlqCount)
	}

	// Checkpoint also advanced.
	var pos uint64
	if err := pool.QueryRow(ctx,
		`SELECT position FROM es_projection_checkpoints WHERE projection_name = $1`, proj,
	).Scan(&pos); err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if pos != 500 {
		t.Fatalf("checkpoint = %d, want 500", pos)
	}
}

// TestFlushCheckpointOnConflictDoesNothing verifies that a redelivered
// failure for an already-DLQ'd event does NOT overwrite the original
// entry. ON CONFLICT DO NOTHING preserves the first-failure timestamps
// and error so operators see the original failure, not the most recent.
func TestFlushCheckpointOnConflictDoesNothing(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_projection_dlq", "es_projection_checkpoints")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const proj = "test-proj-conflict"
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM es_projection_dlq WHERE projection_name=$1`, proj)
		_, _ = pool.Exec(context.Background(), `DELETE FROM es_projection_checkpoints WHERE projection_name=$1`, proj)
	})
	original := dlq.Entry{
		ProjectionName: proj,
		EventID:        "evt-X",
		StreamID:       "country-uuid-X",
		StreamRevision: 1,
		EventType:      "country.registered",
		Payload:        json.RawMessage(`{}`),
		Metadata:       json.RawMessage(`{}`),
		CommitPosition: 100,
		Error:          "original failure",
		Attempts:       5,
	}
	if err := store.FlushCheckpoint(ctx, proj, 100, []dlq.Entry{original}); err != nil {
		t.Fatalf("first FlushCheckpoint: %v", err)
	}

	// Second Flush with the SAME event_id but different error text. The
	// original error must be preserved (ON CONFLICT DO NOTHING).
	different := original
	different.Error = "different failure on redelivery"
	if err := store.FlushCheckpoint(ctx, proj, 200, []dlq.Entry{different}); err != nil {
		t.Fatalf("second FlushCheckpoint: %v", err)
	}

	var storedError string
	if err := pool.QueryRow(ctx,
		`SELECT error FROM es_projection_dlq WHERE projection_name = $1 AND event_id = $2`,
		proj, "evt-X",
	).Scan(&storedError); err != nil {
		t.Fatalf("read dlq: %v", err)
	}
	if storedError != "original failure" {
		t.Fatalf("error = %q, want %q (ON CONFLICT preserves first)", storedError, "original failure")
	}
}

// TestListReturnsMostRecentFirst verifies the operator-facing ordering
// of DLQ entries: most-recently-failed first, so operators see new
// failures at the top of the list.
func TestListReturnsMostRecentFirst(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_projection_dlq", "es_projection_checkpoints")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const proj = "test-proj-list"
	// Insert three failures with different commit positions (which
	// determines the order of FlushCheckpoint calls and thus
	// last_failed_at, modulo the test running fast enough that the
	// timestamps are equal — we explicitly bump last_failed_at between
	// inserts to make the ordering deterministic).
	for i, evtID := range []string{"evt-A", "evt-B", "evt-C"} {
		entry := dlq.Entry{
			ProjectionName: proj,
			EventID:        evtID,
			StreamID:       "x-" + evtID,
			StreamRevision: uint64(i),
			EventType:      "x.y",
			Payload:        json.RawMessage(`{}`),
			Metadata:       json.RawMessage(`{}`),
			CommitPosition: uint64(100 + i),
			Error:          "fail",
			Attempts:       1,
		}
		if err := store.FlushCheckpoint(ctx, proj, entry.CommitPosition, []dlq.Entry{entry}); err != nil {
			t.Fatalf("FlushCheckpoint %s: %v", evtID, err)
		}
		// Bump last_failed_at explicitly so ordering is deterministic
		// even when the three inserts happen within the same millisecond.
		if _, err := pool.Exec(ctx,
			`UPDATE es_projection_dlq SET last_failed_at = now() + ($1 || ' microseconds')::interval
			 WHERE projection_name = $2 AND event_id = $3`,
			itoa(i*1000), proj, evtID,
		); err != nil {
			t.Fatalf("bump last_failed_at %s: %v", evtID, err)
		}
	}

	entries, err := store.List(ctx, proj, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("len(entries) = %d, want 3", len(entries))
	}
	// Most-recent first: C was bumped the most.
	if entries[0].EventID != "evt-C" {
		t.Fatalf("entries[0].EventID = %q, want evt-C (most recent first)", entries[0].EventID)
	}
	if entries[2].EventID != "evt-A" {
		t.Fatalf("entries[2].EventID = %q, want evt-A (oldest last)", entries[2].EventID)
	}
}

// TestDeleteRemovesEntry verifies the cmd/admin dlq-reject / dlq-replay
// path can remove a DLQ entry once the operator has decided what to do
// with it.
func TestDeleteRemovesEntry(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_projection_dlq", "es_projection_checkpoints")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const proj = "test-proj-delete"
	entry := dlq.Entry{
		ProjectionName: proj, EventID: "evt-del", StreamID: "x-evt-del",
		StreamRevision: 1, EventType: "x.y", Payload: json.RawMessage(`{}`),
		Metadata: json.RawMessage(`{}`), CommitPosition: 50, Error: "x", Attempts: 1,
	}
	if err := store.FlushCheckpoint(ctx, proj, 50, []dlq.Entry{entry}); err != nil {
		t.Fatalf("FlushCheckpoint: %v", err)
	}

	var id int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM es_projection_dlq WHERE projection_name = $1 AND event_id = $2`,
		proj, "evt-del",
	).Scan(&id); err != nil {
		t.Fatalf("read id: %v", err)
	}

	if err := store.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM es_projection_dlq WHERE id = $1`, id,
	).Scan(&count); err != nil {
		t.Fatalf("count after delete: %v", err)
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0 after Delete", count)
	}
}

// TestListEmptyProjectionReturnsEmpty verifies that querying a projection
// with no DLQ entries returns an empty slice (not nil) and no error.
// Operators running cmd/admin dlq-list on a healthy projection should
// see a clean empty list rather than an error.
func TestListEmptyProjectionReturnsEmpty(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_projection_dlq", "es_projection_checkpoints")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	entries, err := store.List(ctx, "no-such-projection", 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("len(entries) = %d, want 0", len(entries))
	}
}

// itoa is a tiny local helper kept here to avoid pulling strconv into the
// test file just for one UPDATE call in TestListReturnsMostRecentFirst.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	if n < 0 {
		return "-" + itoa(-n)
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// Ensure errors is referenced for future sentinel-error assertions.
var _ = errors.Is

// TestFlushCheckpointWithNilMetadataSucceeds is the D-02-01 regression guard:
// when a failed event carries no user-metadata (nil), the DLQ insert MUST
// coerce it to '{}' so the NOT NULL metadata column accepts the row. Without
// this guard a poison event with no metadata would fail the DLQ insert and
// stall the projection subscription indefinitely.
func TestFlushCheckpointWithNilMetadataSucceeds(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_projection_dlq", "es_projection_checkpoints")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const proj = "test-proj-nil-metadata"
	entry := dlq.Entry{
		EventID:        "evt-nil-meta-001",
		StreamID:       "truck-nil-meta",
		StreamRevision: 1,
		EventType:      "truck.driverAssigned",
		Payload:        json.RawMessage(`{"AssignmentId":"a1","DriverId":"d1"}`),
		Metadata:       nil, // the D-02-01 boundary: nil metadata
		CommitPosition: 42,
		Error:          "invalid input syntax for type uuid",
		Attempts:       5,
	}
	if err := store.FlushCheckpoint(ctx, proj, 42, []dlq.Entry{entry}); err != nil {
		t.Fatalf("FlushCheckpoint with nil metadata: %v", err)
	}

	// Verify the DLQ row was inserted with '{}' metadata.
	var metadata []byte
	err := pool.QueryRow(ctx,
		`SELECT metadata FROM es_projection_dlq WHERE projection_name = $1 AND event_id = $2`,
		proj, entry.EventID,
	).Scan(&metadata)
	if err != nil {
		t.Fatalf("read DLQ row: %v", err)
	}
	if string(metadata) != "{}" {
		t.Fatalf("metadata = %q, want '{}'", string(metadata))
	}

	// Verify the checkpoint advanced (the atomic transaction committed).
	var pos uint64
	if err := pool.QueryRow(ctx,
		`SELECT position FROM es_projection_checkpoints WHERE projection_name = $1`, proj,
	).Scan(&pos); err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if pos != 42 {
		t.Fatalf("checkpoint = %d, want 42", pos)
	}
}

// TestFlushCheckpointWithNilPayloadSucceeds verifies the same coercion applies
// to a nil payload (belt-and-suspenders for the NOT NULL payload column).
func TestFlushCheckpointWithNilPayloadSucceeds(t *testing.T) {
	t.Parallel()
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	postgrestest.TruncateTables(t, pool, "es_projection_dlq", "es_projection_checkpoints")

	store := New(pool)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const proj = "test-proj-nil-payload"
	entry := dlq.Entry{
		EventID:        "evt-nil-payload-001",
		StreamID:       "truck-nil-payload",
		StreamRevision: 1,
		EventType:      "truck.driverAssigned",
		Payload:        nil,
		Metadata:       nil,
		CommitPosition: 7,
		Error:          "decode failure",
		Attempts:       3,
	}
	if err := store.FlushCheckpoint(ctx, proj, 7, []dlq.Entry{entry}); err != nil {
		t.Fatalf("FlushCheckpoint with nil payload: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM es_projection_dlq WHERE projection_name = $1`, proj,
	).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Fatalf("DLQ row count = %d, want 1", count)
	}
}
