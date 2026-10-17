// Package postgres implements the idempotency deduplication store against
// the shared Postgres instance. Records live in the command_idempotency
// table (created by migration 20260726000200_command_idempotency).
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aliighahremanii/buildingblocks/application/idempotency"
)

// Store is the Postgres implementation of [idempotency.Store]. It is safe
// for concurrent use — the underlying *pgxpool.Pool is goroutine-safe and
// the Store carries no mutable state.
type Store struct {
	pool *pgxpool.Pool
}

// New constructs an idempotency Store backed by pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const lookupSQL = `
SELECT idempotency_key, command_kind, request_fingerprint, aggregate_id, response_status, response_body, created_at
FROM command_idempotency
WHERE idempotency_key = $1 AND command_kind = $2;`

// Lookup returns the cached record for (key, commandKind), or found=false
// when no record exists.
func (s *Store) Lookup(ctx context.Context, key, commandKind string) (idempotency.Record, bool, error) {
	row := s.pool.QueryRow(ctx, lookupSQL, key, commandKind)
	var rec idempotency.Record
	var fingerprint *string
	var body []byte
	err := row.Scan(&rec.Key, &rec.CommandKind, &fingerprint, &rec.AggregateID, &rec.ResponseStatus, &body, &rec.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return idempotency.Record{}, false, nil
	}
	if err != nil {
		return idempotency.Record{}, false, fmt.Errorf("idempotency lookup %s/%s: %w", commandKind, key, err)
	}
	if fingerprint != nil {
		rec.RequestFingerprint = *fingerprint
	}
	rec.ResponseBody = json.RawMessage(body)
	return rec, true, nil
}

const insertSQL = `
INSERT INTO command_idempotency (idempotency_key, command_kind, request_fingerprint, aggregate_id, response_status, response_body)
VALUES ($1, $2, $3, $4, $5, $6);`

// Insert caches a record. A duplicate (key, commandKind) returns
// [idempotency.ErrConflict] so the middleware can fetch the existing record.
func (s *Store) Insert(ctx context.Context, record idempotency.Record) error {
	_, err := s.pool.Exec(ctx, insertSQL,
		record.Key, record.CommandKind, record.RequestFingerprint, record.AggregateID,
		record.ResponseStatus, []byte(record.ResponseBody),
	)
	if err == nil {
		return nil
	}
	// pgx returns a pgx PgError with code 23505 for unique-violation. We
	// detect it by string-match rather than a typed assertion so the
	// middleware does not need to import pgx directly.
	if isUniqueViolation(err) {
		return idempotency.ErrConflict
	}
	return fmt.Errorf("idempotency insert %s/%s: %w", record.CommandKind, record.Key, err)
}

// isUniqueViolation reports whether err is a Postgres unique-violation
// (SQLSTATE 23505). Kept as a helper so the detection can evolve (typed
// pgconn.PgError, errors.Is, etc.) without touching the call site.
func isUniqueViolation(err error) bool {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "23505"
	}
	return false
}
