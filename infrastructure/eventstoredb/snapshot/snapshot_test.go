package snapshot

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Integration tests for the es_snapshots table live with module-level
// integration tests, not here. The unit test below only verifies construction
// wiring, which is all that can be exercised without a live pool.

func TestSnapshotNewReturnsNonNil(t *testing.T) {
	var pool *pgxpool.Pool // nil pool is fine — we only assert the constructor wires it.
	s := New(pool)
	if s == nil {
		t.Fatal("New() returned nil")
	}
}
