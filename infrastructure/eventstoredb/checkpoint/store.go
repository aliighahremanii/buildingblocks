// Package checkpoint persists per-projection positions for KurrentDB ($all
// stream) projection workers. Each projection (postgres, meilisearch, ...)
// records the last commit position it has successfully projected so that, on
// restart or after a dropped subscription, it resumes from that position
// instead of replaying the whole stream.
//
// Positions live in the SHARED Postgres table es_projection_checkpoints
// (created by migration 20260726000100_es_shared_tables). Rows are keyed by
// projection_name, which modules typically namespace (e.g.
// "sample1-postgres") so they never collide with projections owned by other
// modules in the same database.
package checkpoint

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store reads and writes projection positions in Postgres.
type Store struct {
	pool *pgxpool.Pool
}

// New constructs a checkpoint Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const getPositionSQL = `
SELECT position FROM es_projection_checkpoints
WHERE projection_name = $1;`

// Get returns the last saved commit position for projectionName. A missing row
// is treated as the start of the stream (position 0) rather than an error, so
// that a fresh projection whose seed row was not applied can still bootstrap.
func (s *Store) Get(ctx context.Context, projectionName string) (uint64, error) {
	var pos uint64
	err := s.pool.QueryRow(ctx, getPositionSQL, projectionName).Scan(&pos)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return pos, nil
}

const savePositionSQL = `
INSERT INTO es_projection_checkpoints (projection_name, position)
VALUES ($1, $2)
ON CONFLICT (projection_name)
DO UPDATE SET position = EXCLUDED.position, updated_at = now();`

// Save upserts the commit position for projectionName. It is idempotent and
// safe to call for projection names that were not pre-seeded by a migration.
func (s *Store) Save(ctx context.Context, projectionName string, position uint64) error {
	_, err := s.pool.Exec(ctx, savePositionSQL, projectionName, position)
	return err
}
