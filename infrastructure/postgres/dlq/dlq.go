// Package postgres implements the projection dead-letter queue against the
// shared Postgres instance. DLQ entries live in the es_projection_dlq
// table (created by migration 20260726000300_projection_dlq).
//
// The critical operation is FlushCheckpoint, which writes DLQ entries AND
// advances the projection's checkpoint in ONE Postgres transaction. This
// guarantees the projection never advances past a failed event without
// also recording the failure for operator action.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/dlq"
)

// Store is the Postgres implementation of [dlq.Store].
//
// It depends on the same pgxpool as the checkpoint store; the
// FlushCheckpoint transaction writes to BOTH es_projection_dlq AND
// es_projection_checkpoints in one tx, so the two writes commit or roll
// back together.
type Store struct {
	pool *pgxpool.Pool
}

// New constructs a DLQ Store backed by pool. The same pool MUST back the
// checkpoint store, otherwise FlushCheckpoint cannot span both tables in
// one transaction.
func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// FlushCheckpoint atomically inserts all failures as DLQ entries AND
// advances the checkpoint for projectionName to commitPosition. Both
// writes commit in a single Postgres transaction; on any error neither
// takes effect.
func (s *Store) FlushCheckpoint(ctx context.Context, projectionName string, commitPosition uint64, failures []dlq.Entry) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("dlq flush begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	// 1. Insert DLQ entries. Use ON CONFLICT DO NOTHING so a redelivered
	//    failure for an already-DLQ'd event does not abort the
	//    transaction. The operator's view of the original failure (error,
	//    attempts, timestamps) is preserved.
	for _, f := range failures {
		// Coerce nil metadata/payload to empty JSON so the NOT NULL columns
		// receive '{}' rather than SQL NULL. Events with no user-metadata
		// (e.g. test-borne or early-version events) would otherwise violate
		// the NOT NULL constraint and the DLQ insert would fail, leaving the
		// projection stalled on the poison event indefinitely (D-02-01).
		metadata := []byte(f.Metadata)
		if len(metadata) == 0 {
			metadata = []byte("{}")
		}
		payload := []byte(f.Payload)
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		_, err = tx.Exec(ctx, insertDLQSQL,
			projectionName,
			f.EventID,
			f.StreamID,
			f.StreamRevision,
			f.EventType,
			payload,
			metadata,
			f.CommitPosition,
			f.Error,
			f.Attempts,
		)
		if err != nil {
			return fmt.Errorf("dlq insert %s/%s: %w", projectionName, f.EventID, err)
		}
	}

	// 2. Advance the checkpoint. Same upsert shape as checkpoint.Store.Save
	//    but executed inside this transaction.
	_, err = tx.Exec(ctx, upsertCheckpointSQL, projectionName, commitPosition)
	if err != nil {
		return fmt.Errorf("dlq flush checkpoint %s: %w", projectionName, err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("dlq flush commit %s: %w", projectionName, err)
	}
	return nil
}

const insertDLQSQL = `
INSERT INTO es_projection_dlq
    (projection_name, event_id, stream_id, stream_revision, event_type,
     payload, metadata, commit_position, error, attempts)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (projection_name, event_id) DO NOTHING;`

const upsertCheckpointSQL = `
INSERT INTO es_projection_checkpoints (projection_name, position)
VALUES ($1, $2)
ON CONFLICT (projection_name)
DO UPDATE SET position = EXCLUDED.position, updated_at = now();`

const listDLQSQL = `
SELECT id, projection_name, event_id, stream_id, stream_revision, event_type,
       payload, metadata, commit_position, error, attempts, first_failed_at, last_failed_at
FROM es_projection_dlq
WHERE projection_name = $1
ORDER BY last_failed_at DESC
LIMIT $2;`

// List returns the DLQ entries for projectionName, most-recent first.
func (s *Store) List(ctx context.Context, projectionName string, limit int) ([]dlq.Entry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, listDLQSQL, projectionName, limit)
	if err != nil {
		return nil, fmt.Errorf("dlq list %s: %w", projectionName, err)
	}
	defer rows.Close()

	var out []dlq.Entry
	for rows.Next() {
		var e dlq.Entry
		var payload, metadata []byte
		if err := rows.Scan(
			&e.ID, &e.ProjectionName, &e.EventID, &e.StreamID, &e.StreamRevision,
			&e.EventType, &payload, &metadata, &e.CommitPosition, &e.Error,
			&e.Attempts, &e.FirstFailedAt, &e.LastFailedAt,
		); err != nil {
			return nil, fmt.Errorf("dlq list scan %s: %w", projectionName, err)
		}
		e.Payload = payload
		e.Metadata = metadata
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dlq list rows %s: %w", projectionName, err)
	}
	return out, nil
}

const deleteDLQSQL = `DELETE FROM es_projection_dlq WHERE id = $1;`

// Delete removes a DLQ entry by ID. Used by cmd/admin after a successful
// manual replay or after an audited rejection.
func (s *Store) Delete(ctx context.Context, id int64) error {
	_, err := s.pool.Exec(ctx, deleteDLQSQL, id)
	if err != nil {
		return fmt.Errorf("dlq delete %d: %w", id, err)
	}
	return nil
}

// Compile-time interface check.
var _ dlq.Store = (*Store)(nil)
