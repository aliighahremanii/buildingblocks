// Package postgres implements the natspub.PositionStore against the
// shared Postgres instance. Positions live in the
// es_nats_published_positions table (migration 20260726000400).
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/natspub"
)

// Store tracks the NATS publisher's last published KurrentDB $all commit
// position.
type Store struct {
	pool *pgxpool.Pool
}

// New constructs a position Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const getPositionSQL = `
SELECT position FROM es_nats_published_positions
WHERE publisher_name = $1;`

// Get returns the last saved commit position for publisherName. A
// missing row is treated as the start of the stream (position 0).
func (s *Store) Get(ctx context.Context, publisherName string) (uint64, error) {
	var pos uint64
	err := s.pool.QueryRow(ctx, getPositionSQL, publisherName).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("natspub position get %s: %w", publisherName, err)
	}
	return pos, nil
}

const savePositionSQL = `
INSERT INTO es_nats_published_positions (publisher_name, position, events_published)
VALUES ($1, $2, $3)
ON CONFLICT (publisher_name)
DO UPDATE SET position = EXCLUDED.position,
              events_published = es_nats_published_positions.events_published + EXCLUDED.events_published,
              updated_at = now();`

// Save upserts the commit position for publisherName. eventsPublished is
// ADDED to the running total (not replaced) so the table's
// events_published column accumulates the lifetime publish count for
// operational observability ("how many events has this publisher
// shipped to NATS in total?").
func (s *Store) Save(ctx context.Context, publisherName string, position uint64, eventsPublished uint64) error {
	_, err := s.pool.Exec(ctx, savePositionSQL, publisherName, position, eventsPublished)
	if err != nil {
		return fmt.Errorf("natspub position save %s: %w", publisherName, err)
	}
	return nil
}

// Compile-time interface check.
var _ natspub.PositionStore = (*Store)(nil)
