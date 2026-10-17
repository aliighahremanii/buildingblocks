// Package saga implements the saga.Store port against the Postgres
// `sagas` and `saga_timeouts` tables created by migration
// 20260726000500_sagas. The Coordinator (internal/buildingblocks/
// infrastructure/saga) and per-module saga implementations depend on
// the Store interface, not this concrete type.
//
// Concurrency:
//   - Save uses an upsert guarded by `WHERE version = $old` so concurrent
//     transitions surface as saga.ErrConcurrencyConflict (the Postgres
//     implementation maps "0 rows affected" to that error).
//   - ListDueTimeouts + DeleteTimeout (the sweep) is guarded by a
//     transaction-scoped advisory lock keyed per saga_id so only one
//     worker fires a given timeout at a time, even when multiple worker
//     processes share the database.
package saga

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	bbsaga "github.com/aliighahremanii/buildingblocks/infrastructure/saga"
)

// Store implements bbsaga.Store against the `sagas` and `saga_timeouts`
// tables in the shared buildingblocks Postgres database. Construct one
// per worker process; all methods are safe for concurrent use across
// goroutines (pgxpool handles connection pooling).
type Store struct {
	pool *pgxpool.Pool
}

// New constructs a saga Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const loadSQL = `
SELECT saga_type, status, state, correlation_id, causation_id, version
FROM sagas
WHERE saga_id = $1;`

// Load returns the instance for sagaID. found is false (with nil error)
// when the row does not exist.
func (s *Store) Load(ctx context.Context, sagaID string) (bbsaga.Instance, bool, error) {
	var (
		in            bbsaga.Instance
		statusStr     string
		correlationID string
		causationID   string
	)
	in.ID = sagaID
	err := s.pool.QueryRow(ctx, loadSQL, sagaID).Scan(
		&in.Type,
		&statusStr,
		&in.State,
		&correlationID,
		&causationID,
		&in.Version,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return bbsaga.Instance{}, false, nil
		}
		return bbsaga.Instance{}, false, fmt.Errorf("saga store load %q: %w", sagaID, err)
	}
	status, ok := bbsaga.ParseStatus(statusStr)
	if !ok {
		return bbsaga.Instance{}, false, fmt.Errorf("saga store load %q: unknown status %q", sagaID, statusStr)
	}
	in.Status = status
	in.CorrelationID = correlationID
	in.CausationID = causationID
	return in, true, nil
}

const saveSQL = `
INSERT INTO sagas (saga_id, saga_type, status, state, correlation_id, causation_id, version)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (saga_id)
DO UPDATE SET
    saga_type      = EXCLUDED.saga_type,
    status         = EXCLUDED.status,
    state          = EXCLUDED.state,
    correlation_id = EXCLUDED.correlation_id,
    causation_id   = EXCLUDED.causation_id,
    version        = EXCLUDED.version,
    updated_at     = now()
WHERE sagas.version = $8;`

// Save upserts the instance with optimistic-concurrency protection.
// For an existing instance it succeeds only when `sagas.version` equals
// in.Version-1; otherwise zero rows are affected and Save returns
// bbsaga.ErrConcurrencyConflict. For a new instance (in.Version == 0)
// it inserts; if the row already exists (via ON CONFLICT) the `WHERE`
// clause (`sagas.version = $8` with $8 = -1) never matches, so the
// conflict also surfaces as ErrConcurrencyConflict.
func (s *Store) Save(ctx context.Context, in bbsaga.Instance) error {
	// The state column is NOT NULL; a nil State is coerced to '{}' so
	// callers that build a fresh Instance without a state payload (e.g.
	// a saga that keeps state only in memory) still persist cleanly.
	// The column DEFAULT '{}' applies only when the column is omitted
	// from the INSERT, not when an explicit NULL is supplied.
	state := in.State
	if len(state) == 0 {
		state = []byte("{}")
	}
	expectedPrior := in.Version - 1
	tag, err := s.pool.Exec(ctx, saveSQL,
		in.ID,
		in.Type,
		in.Status.String(),
		state,
		in.CorrelationID,
		in.CausationID,
		in.Version,
		expectedPrior,
	)
	if err != nil {
		return fmt.Errorf("saga store save %q: %w", in.ID, err)
	}
	if tag.RowsAffected() == 0 {
		return bbsaga.ErrConcurrencyConflict
	}
	return nil
}

const scheduleTimeoutSQL = `
INSERT INTO saga_timeouts (saga_id, due_at, kind)
VALUES ($1, $2, COALESCE(NULLIF($3, ''), ''))
ON CONFLICT (saga_id, kind)
DO UPDATE SET due_at = EXCLUDED.due_at;`

// ScheduleTimeout arms (or re-arms) the reminder of the given kind for
// sagaID. An empty kind is normalized to "" to match the column default.
func (s *Store) ScheduleTimeout(ctx context.Context, sagaID, kind string, dueAt time.Time) error {
	_, err := s.pool.Exec(ctx, scheduleTimeoutSQL, sagaID, dueAt.UTC(), kind)
	if err != nil {
		return fmt.Errorf("saga store schedule timeout %q: %w", sagaID, err)
	}
	return nil
}

const listDueTimeoutsSQL = `
SELECT saga_id
FROM saga_timeouts
WHERE due_at <= $1
ORDER BY due_at ASC
LIMIT $2;`

// ListDueTimeouts returns saga IDs whose reminder is due by `before`,
// newest-last, capped at limit.
func (s *Store) ListDueTimeouts(ctx context.Context, before time.Time, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, listDueTimeoutsSQL, before.UTC(), limit)
	if err != nil {
		return nil, fmt.Errorf("saga store list due timeouts: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("saga store list due timeouts scan: %w", err)
		}
		out = append(out, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("saga store list due timeouts rows: %w", err)
	}
	return out, nil
}

// advisoryLockKeyFor hashes a saga_id into a stable int64 key for
// pg_advisory_xact_lock. Two callers that race on the same saga_id take
// the same key, so only one acquires the lock; the other blocks until
// the first's transaction commits. Different saga_ids hash to (with
// overwhelming probability) different keys.
//
// We use FNV-1a for simplicity and zero external deps; the sweep does not
// need a cryptographic hash (it is a contention gate, not a security
// boundary). The result is masked to int63 to stay within the
// pg_advisory_xact_lock(bigint) domain.
func advisoryLockKeyFor(sagaID string) int64 {
	const (
		offset32 uint32 = 2166136261
		prime32  uint32 = 16777619
	)
	var h uint32 = offset32
	for i := 0; i < len(sagaID); i++ {
		h ^= uint32(sagaID[i])
		h *= prime32
	}
	// Spread into the upper 32 bits so different short ids collide less.
	high := uint64(h) << 1 // always even → positive int63
	if high > 1<<62-1 {
		high = high % (1<<62 - 1)
	}
	if high == 0 {
		high = 1
	}
	return int64(high)
}

const deleteTimeoutSQL = `
DELETE FROM saga_timeouts
WHERE saga_id = $1 AND kind = COALESCE(NULLIF($2, ''), '');`

// DeleteTimeout removes the reminder row. It takes a transaction-scoped
// advisory lock keyed on sagaID so two worker processes cannot both fire
// the same timeout and delete-then-rerun it; the second blocks until the
// first commits. The lock is released automatically at transaction end.
func (s *Store) DeleteTimeout(ctx context.Context, sagaID, kind string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("saga store delete timeout begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	lockKey := advisoryLockKeyFor(sagaID)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey); err != nil {
		return fmt.Errorf("saga store delete timeout lock: %w", err)
	}

	if _, err := tx.Exec(ctx, deleteTimeoutSQL, sagaID, kind); err != nil {
		return fmt.Errorf("saga store delete timeout %q: %w", sagaID, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("saga store delete timeout commit: %w", err)
	}
	return nil
}
