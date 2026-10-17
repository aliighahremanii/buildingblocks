// Package snapshot persists event-sourced aggregate snapshots in the SHARED
// Postgres table `es_snapshots` (created by migration
// 20260726000100_es_shared_tables). Every event-sourced module in the system
// shares this single table; rows are partitioned by (aggregate_type,
// aggregate_id) so modules never collide.
package snapshot

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store reads and writes aggregate snapshots in the shared es_snapshots table.
type Store struct {
	pool *pgxpool.Pool
}

// New constructs a snapshot Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Save upserts a snapshot row for (aggregateType, aggregateID). version is the
// KurrentDB stream revision the snapshot corresponds to; state is the
// serialized aggregate state (typically JSON).
func (s *Store) Save(ctx context.Context, aggregateType, aggregateID string, version uint64, state []byte) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO es_snapshots (aggregate_type, aggregate_id, version, state, updated_at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (aggregate_type, aggregate_id)
DO UPDATE SET version = $3, state = $4, updated_at = now()`,
		aggregateType, aggregateID, version, state)
	if err != nil {
		return fmt.Errorf("snapshot save %s-%s: %w", aggregateType, aggregateID, err)
	}
	return nil
}

// Load returns the snapshot state and stream version for (aggregateType,
// aggregateID). When no snapshot exists, found is false and state/version are
// zero values, so callers can branch cleanly without inspecting the error.
func (s *Store) Load(ctx context.Context, aggregateType, aggregateID string) ([]byte, uint64, bool, error) {
	var (
		state   []byte
		version uint64
	)
	err := s.pool.QueryRow(ctx,
		`SELECT state, version FROM es_snapshots WHERE aggregate_type = $1 AND aggregate_id = $2`,
		aggregateType, aggregateID,
	).Scan(&state, &version)
	if err == pgx.ErrNoRows {
		return nil, 0, false, nil
	}
	if err != nil {
		return nil, 0, false, fmt.Errorf("snapshot load %s-%s: %w", aggregateType, aggregateID, err)
	}
	return state, version, true, nil
}
