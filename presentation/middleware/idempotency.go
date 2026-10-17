// Package middleware provides the HTTP-side adapter for the
// [idempotency.Store]: when a request carries an Idempotency-Key header,
// the middleware deduplicates it across retries.
//
// The middleware is HTTP-aware rather than command-handler-aware because
// the dedup contract is about the WIRE response (status + body), not about
// the application-layer Result. A retried POST /items with the same key
// must return the EXACT same JSON body and status the first attempt
// produced — including the assigned aggregate ID — so the client treats
// the retry as a no-op success rather than a new aggregate.
//
// # Flow
//
//  1. Read Idempotency-Key header. If absent, run the handler normally.
//  2. If present, Look up (key, commandKind) in the dedup store.
//     - Found: short-circuit, write the cached status + body, return.
//     - Not found: continue.
//  3. Run the inner handler, capturing the status + body it produced.
//  4. Insert (key, commandKind, aggregateID, status, body) into the store.
//     - Success: return the response.
//     - ErrConflict: a parallel request won the race; re-Lookup and replay
//     the winner's response.
//
// # Failure modes
//
//   - Crash between handler success and Insert: no cached record. The
//     retry re-runs the handler. The handler's own OCC controls (NoStream
//     on Register, AtRevision on Update) ensure the second run is safe.
//   - Parallel duplicate requests: the Insert's PRIMARY KEY constraint
//     guarantees exactly one winner; the loser gets ErrConflict and replays
//     the winner's response.
package middleware

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	apperrors "github.com/aliighahremanii/buildingblocks/application/errors"
	"github.com/aliighahremanii/buildingblocks/application/idempotency"
	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

// HeaderName is the HTTP header that opts a request into the idempotency
// contract. RFC draft (ietf-httpapi-idempotency-key-header) uses the same
// name; we follow the de-facto industry convention.
const HeaderName = "Idempotency-Key"

// HandlerResult is the minimal view the middleware needs of the inner
// handler's outcome. The wrapping web.Handler produces this struct, the
// middleware caches it, and replays it on duplicate Key.
type HandlerResult struct {
	// AggregateID is the business identifier the command targeted or
	// created. Empty for commands that do not produce one. Cached so the
	// operator can later query "every command that touched this ID".
	AggregateID string
	// Status is the HTTP status the inner handler chose to write.
	Status int
	// Body is the JSON body the inner handler chose to write. nil for
	// 204 No Content responses.
	Body any
}

// HandlerFunc is the inner handler shape. It receives the web.Context and
// returns the HandlerResult the middleware should both write to the wire
// and cache for replay. Errors returned here flow through the standard
// transport problem-mapping; the middleware does not cache error responses
// (only successful commands are idempotent — an errored command is by
// definition in an unknown state and the client must be free to retry).
type HandlerFunc func(ctx web.Context) (HandlerResult, error)

// New returns a web.HandlerFunc that wraps inner with the idempotency
// contract. store may be nil — in that case the middleware is a transparent
// pass-through (used by tests and by deployments that opt out of the
// dedup table). commandKind is the namespaced handler name
// ("sample1.item.register", etc.) and forms part of the dedup natural key.
func New(store idempotency.Store, commandKind string, inner HandlerFunc) web.HandlerFunc {
	return func(ctx web.Context) error {
		key := ctx.Header(HeaderName)
		if key == "" || store == nil {
			// No opt-in (or no store): run the handler normally.
			result, err := inner(ctx)
			if err != nil {
				return err
			}
			return writeFresh(ctx, result)
		}

		fingerprint, err := requestFingerprint(ctx)
		if err != nil {
			return fmt.Errorf("idempotency request fingerprint: %w", err)
		}

		// Opt-in: dedupe by (key, commandKind), and reject a changed body.
		if record, found, err := store.Lookup(ctx.Context(), key, commandKind); err != nil {
			// Lookup failures do NOT block the request: the worst case is
			// we run the handler twice (the very thing the dedup is
			// supposed to prevent), but the handler's own OCC controls
			// make that safe. Log and fall through.
			slog.Error("idempotency: lookup failed; running handler without dedup",
				"commandKind", commandKind, "key", key, "err", err)
		} else if found {
			if record.RequestFingerprint != "" && record.RequestFingerprint != fingerprint {
				return apperrors.NewConflictError("idempotency-key", "request body differs from the original request")
			}
			return writeCached(ctx, record)
		}

		result, err := inner(ctx)
		if err != nil {
			// Do not cache errors. The command is in an unknown state and
			// the client must be free to retry.
			return err
		}

		// Serialize the body once so the initial response and the cached
		// record carry byte-identical bodies (R1-01). ctx.JSON uses
		// json.Encoder.Encode which appends a newline; json.Marshal does
		// not. Sharing the same serialized bytes guarantees the retried
		// response matches the original wire response exactly.
		bodyBytes := serializeBody(result.Body)
		record := idempotency.Record{
			Key:                key,
			CommandKind:        commandKind,
			RequestFingerprint: fingerprint,
			AggregateID:        result.AggregateID,
			ResponseStatus:     result.Status,
			ResponseBody:       bodyBytes,
		}

		// Cache the successful result. A parallel winner means we have to
		// replay their response, not ours.
		if err := store.Insert(ctx.Context(), record); err != nil {
			if errors.Is(err, idempotency.ErrConflict) {
				// Parallel race loser. The winner's response is now in the
				// store; re-Lookup and replay it so both clients see the
				// same outcome.
				if winner, found, lerr := store.Lookup(ctx.Context(), key, commandKind); lerr == nil && found {
					if winner.RequestFingerprint != "" && winner.RequestFingerprint != fingerprint {
						return apperrors.NewConflictError("idempotency-key", "request body differs from the original request")
					}
					slog.Info("idempotency: parallel race lost, replaying winner",
						"commandKind", commandKind, "key", key)
					return writeCached(ctx, winner)
				}
				// Extremely unlikely: Insert said conflict but Lookup
				// returned not-found. Fall through to writing our own
				// result; both clients will see the same status+body.
				slog.Error("idempotency: conflict but lookup missed", "commandKind", commandKind, "key", key)
			} else {
				// Insert failed for some other reason. We have already run
				// the handler; write the response we got so the client is
				// not penalized for a store failure.
				slog.Error("idempotency: insert failed; responding uncached",
					"commandKind", commandKind, "key", key, "err", err)
			}
		}

		return writeBytes(ctx, result.Status, bodyBytes)
	}
}

func requestFingerprint(ctx web.Context) (string, error) {
	req := ctx.Request()
	if req == nil || req.Body == nil {
		return fingerprintBytes(nil), nil
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return "", err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	return fingerprintBytes(body), nil
}

func fingerprintBytes(body []byte) string {
	sum := sha256.Sum256(body)
	return fmt.Sprintf("%x", sum[:])
}

// serializeBody marshals body with json.Marshal, returning {} on nil body or
// marshal failure. The same bytes are used for both the wire response and the
// cached record so retries see byte-identical bodies (R1-01).
func serializeBody(body any) []byte {
	if body == nil {
		return []byte(`{}`)
	}
	if raw, err := json.Marshal(body); err == nil {
		return raw
	}
	return []byte(`{}`)
}

// writeBytes writes a pre-serialized JSON body to the wire. Used for both the
// initial response and cached replay so they produce identical bytes. The body
// is written directly via ResponseWriter rather than ctx.JSON, which would
// re-marshal and append a trailing newline (R1-01).
func writeBytes(ctx web.Context, status int, body []byte) error {
	if status == http.StatusNoContent {
		return ctx.NoContent(status)
	}
	ctx.SetHeader("Content-Type", "application/json")
	ctx.ResponseWriter().WriteHeader(status)
	_, err := ctx.ResponseWriter().Write(body)
	return err
}

// writeFresh serializes a non-cached HandlerResult and writes it to the wire.
// Used when no idempotency store is configured.
func writeFresh(ctx web.Context, result HandlerResult) error {
	return writeBytes(ctx, result.Status, serializeBody(result.Body))
}

// writeCached replays a cached idempotency.Record. It delegates to writeBytes
// so the cached replay uses the exact same serialization path as the original
// response — byte-identical output (R1-01).
func writeCached(ctx web.Context, record idempotency.Record) error {
	return writeBytes(ctx, record.ResponseStatus, record.ResponseBody)
}

// IsConcurrencyError reports whether err is a ConcurrencyError. Register
// handlers (which use concurrency.NoStream) treat such an error as "the
// stream already exists, which means a previous attempt created it" and
// translate it into a Lookup-and-replay so the client sees a 201 (success)
// on its retry rather than an opaque 409.
//
// The helper is exported here rather than in the errors package so the
// idempotency-aware handlers can call it without every handler importing
// both packages.
func IsConcurrencyError(err error) bool {
	var cce apperrors.ConcurrencyError
	return errors.As(err, &cce)
}
