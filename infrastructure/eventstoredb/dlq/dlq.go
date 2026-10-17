// Package dlq provides the dead-letter queue for projection workers.
//
// When a projection's ProjectFunc returns an error after exhausting its
// bounded in-process retries, the failed event is routed here. The DLQ
// row preserves the original payload and metadata so operators can
// inspect, replay, or reject it without touching the event store. A row's
// presence means the projection has EXPLICITLY decided to skip this event
// (after N retries) and the checkpoint has been advanced past it; an
// audited decision has been made.
//
// The critical atomicity property is: the DLQ insert AND the checkpoint
// advance happen in ONE Postgres transaction (see [Store.FlushCheckpoint]).
// A crash between them cannot lose the failed event nor advance the
// projection past it.
package dlq

import (
	"context"
	"encoding/json"
	"time"
)

// Entry is one failed event awaiting operator action.
type Entry struct {
	// ID is the auto-incremented row id; operators reference it in
	// cmd/admin dlq-replay/reject.
	ID int64
	// ProjectionName is the projection that failed (e.g.
	// "sample1-postgres"), matching the checkpoint key.
	ProjectionName string
	// EventID is the KurrentDB EventID (UUID) of the failed event.
	EventID string
	// StreamID is the full KurrentDB stream id (e.g. "item-<uuid>").
	StreamID string
	// StreamRevision is the event's position within its stream.
	StreamRevision uint64
	// EventType is the KurrentDB event type (e.g. "item.registered").
	EventType string
	// Payload is the raw event payload as it was received from KurrentDB.
	Payload json.RawMessage
	// Metadata is the user-metadata blob attached to the event (often
	// carries correlationId / causationId when the publisher is wired).
	Metadata json.RawMessage
	// CommitPosition is the KurrentDB $all commit position of the event.
	// Recorded so operators can correlate with the event-store log.
	CommitPosition uint64
	// Error is the last error the ProjectFunc returned before the event
	// was DLQ'd. Operators use it to diagnose.
	Error string
	// Attempts is how many times the ProjectFunc was tried before DLQ'ing.
	Attempts int
	// FirstFailedAt is when the first failure was observed.
	FirstFailedAt time.Time
	// LastFailedAt is when the most recent failure was observed.
	LastFailedAt time.Time
}

// Store is the port for the dead-letter queue. The Postgres implementation
// lives in the infrastructure layer; the projection worker programs
// against this interface.
type Store interface {
	// FlushCheckpoint atomically:
	//  1. inserts one DLQ entry for each failed event in failures, AND
	//  2. advances the checkpoint for projectionName to commitPosition.
	//
	// Both writes happen in ONE Postgres transaction so a crash between
	// them cannot leave the system in an inconsistent state. If the
	// transaction fails, neither write takes effect: the failed events
	// remain in the projection's redelivery window and the checkpoint is
	// not advanced.
	//
	// failures may be empty, in which case FlushCheckpoint just advances
	// the checkpoint (matching the pre-DLQ behavior of the worker).
	FlushCheckpoint(ctx context.Context, projectionName string, commitPosition uint64, failures []Entry) error

	// List returns the DLQ entries for projectionName, ordered by
	// LastFailedAt descending (most recent first). limit caps the result;
	// 0 means a default of 100.
	List(ctx context.Context, projectionName string, limit int) ([]Entry, error)

	// Delete removes a DLQ entry by ID. Used by cmd/admin dlq-replay after
	// a successful manual replay, and by cmd/admin dlq-reject to record
	// an audited skip.
	Delete(ctx context.Context, id int64) error
}
