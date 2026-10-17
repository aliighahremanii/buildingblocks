// Package idempotency provides durable deduplication of externally-retriable
// commands via an optional `Idempotency-Key` HTTP header.
//
// # Why this exists
//
// Network failures, mobile flakiness, and load-balancer double-dispatch can
// cause a client to submit the same command twice. Without deduplication,
// every command that mints a server-side identifier (Register, Open Account,
// Place Order) would create a duplicate aggregate on retry. Event-store
// optimistic concurrency (concurrency.NoStream on Register) catches the
// duplicate at the append, but surfaces it as HTTP 409 — the client cannot
// tell "I'm conflicting with my own retry" from "another client raced me to
// this ID". The Idempotency-Key middleware provides the cleaner contract: a
// retried request with the same key returns the EXACT cached response the
// first attempt produced, so the client treats the retry as success.
//
// # Scope
//
// Deduplication is OPTIONAL: a request without an Idempotency-Key header
// bypasses the cache entirely. A request with the header gets the full
// dedup contract:
//
//   - First request with key K: run the handler, cache the response, return it.
//   - Second request with key K: short-circuit, return the cached response.
//   - Crash between handler-success and cache-write: the retry re-executes
//     the handler; the handler's own optimistic-concurrency controls catch
//     the duplicate safely (NoStream on Register returns 409; AtRevision
//     on Update replays the same domain event or no-op).
//
// # Storage
//
// The dedup state lives in the same Postgres instance as the projection
// checkpoints and snapshots. It is NOT transactional with the KurrentDB
// append (KurrentDB is not XA) — by design, the cached-response write
// happens AFTER the append commits, and the worst-case failure mode
// (crash between append and cache-write) degrades gracefully to 409 +
// client-GET rather than a duplicate effect.
//
// # Key scope
//
// The natural key is (idempotency_key, command_kind). Two different
// commands with the same key (e.g. a Register and a Rename both sending
// "abc-123") get separate cache entries — sharing a key across command
// types would be a client bug, and we surface it rather than silently
// deduping across handlers.
package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Record is one cached command outcome. The middleware writes one after a
// successful handler execution and reads one on every header-bearing
// request before deciding whether to run the handler.
type Record struct {
	// Key is the client-supplied Idempotency-Key header value.
	Key string
	// CommandKind identifies the handler this record belongs to, e.g.
	// "sample1.item.register". Two different commands reusing the same
	// Idempotency-Key get separate records.
	CommandKind string
	// RequestFingerprint identifies the raw request body that produced this
	// response. Empty values are legacy records created before fingerprints
	// were introduced and remain replayable during migration.
	RequestFingerprint string
	// AggregateID is the business identifier the command targeted or
	// created. Recorded for operator traceability ("show me every command
	// that touched this country").
	AggregateID string
	// ResponseStatus is the HTTP status the original handler produced
	// (typically 200, 201, or 204).
	ResponseStatus int
	// ResponseBody is the JSON body the original handler produced, if any.
	// 204 No Content responses store an empty object here so the column's
	// NOT NULL constraint is satisfied.
	ResponseBody json.RawMessage
	// CreatedAt is when the record was cached. Useful for retention policy
	// (records older than N days may be purged).
	CreatedAt time.Time
}

// ErrConflict is returned by [Store.Insert] when a record with the same
// (Key, CommandKind) already exists. The caller fetches the existing record
// via [Store.Lookup] and replays it.
//
// ErrConflict is distinct from "not found" (which Lookup signals via
// found=false) so the middleware can distinguish "I lost a parallel-write
// race" from "no record exists yet" without inspecting the error string.
var ErrConflict = errors.New("idempotency: record already exists")

// Store is the port for the durable idempotency cache. The Postgres
// implementation lives in the infrastructure layer; the middleware (and
// tests) program against this interface.
type Store interface {
	// Lookup returns the cached record for (key, commandKind), or
	// found=false when no record exists. A found record is authoritative:
	// the middleware replays ResponseStatus + ResponseBody without
	// re-running the handler.
	Lookup(ctx context.Context, key, commandKind string) (Record, bool, error)

	// Insert caches a record. The (key, commandKind) pair must be unique;
	// a duplicate returns [ErrConflict] so the caller can fetch the
	// existing record via Lookup. Insert is NOT transactional with the
	// KurrentDB append — see the package doc for the failure-mode
	// analysis.
	Insert(ctx context.Context, record Record) error
}
