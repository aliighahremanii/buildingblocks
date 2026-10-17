// Package natspub provides the KurrentDB → NATS-JetStream publisher
// projection worker.
//
// # Architecture (Approach C — $all subscription)
//
// The publisher is structurally identical to the other projection workers:
// it subscribes to KurrentDB's $all stream with a stream-prefix filter,
// reads each recorded event, and writes it to a sink. The sink here is a
// NATS JetStream context rather than Postgres/Meilisearch.
//
// This approach avoids the dual-write hazard entirely: KurrentDB's $all
// stream IS the outbox. There is no Postgres outbox table to keep in
// sync with the KurrentDB append; the publisher simply tails $all and
// republishes. A crash between KurrentDB append and NATS publish leaves
// the publisher's position behind; on restart it resumes from the saved
// position and republishes the unflushed tail.
//
// # Pre-subscription JetStream validation (D114)
//
// Before ANY KurrentDB subscription is opened, the configured JetStream
// stream is validated read-only (a StreamInfo lookup — this process never
// creates or mutates streams): the stream must exist and its subject
// filters must cover every emittable subject family
// "<SubjectPrefix>.<aggregateType>.>". A misconfigured stream (most
// commonly subjects "<prefix>.*", which matches exactly one token and
// therefore none of the published subjects) otherwise makes every publish
// fail with nats.ErrNoResponders and the worker crash-loop through $all
// subscriptions. While validation fails, the worker parks in the
// validation gate with bounded, context-cancellable backoff and one
// actionable, non-duplicated log entry per condition — the durable
// position never advances, so no events are lost or skipped.
//
// # Exactly-once delivery
//
// JetStream deduplicates messages by the `Nats-Msg-Id` header within the
// stream's duplicate window (default 2 minutes). The publisher sets
// Nats-Msg-Id to the KurrentDB EventID (a UUID), which is globally
// unique and stable across replays. A consumer that receives the same
// event twice (because the publisher crashed and replayed) sees the
// same Nats-Msg-Id and can dedupe on its side.
//
// # Position tracking
//
// The publisher's progress (the last KurrentDB $all commit position it
// has published) lives in the `es_nats_published_positions` table. The
// position is updated AFTER a successful PublishAsync ack, so a crash
// between publish and position-update leaves the position behind; on
// restart the publisher replays from the saved position, JetStream
// deduplicates the re-published events via Nats-Msg-Id, and the
// position advances past them. A FAILED position save ends the
// subscription attempt with the durable position preserved (a failed
// durable boundary is never superseded); the next attempt resumes from
// the persisted position and the replayed overlap is deduplicated. The
// `events_published` counter accumulates the EXACT number of published
// events because each save accounts only the delta since the previous
// successful save.
//
// At the END of an attempt (stream end, drop, or a batch save that raced
// shutdown) the unsaved tail is flushed on a bounded context DETACHED
// from the run context (D131 — the projection workers' terminal-flush
// pattern): the run context is typically ALREADY cancelled when the
// attempt ends on graceful shutdown, and flushing through it would
// always fail and silently lose the tail to replay-after-restart —
// duplicates beyond the JetStream dedup window.
//
// # Bounded subscription establishment (D136)
//
// SubscribeToAll used to be called SYNCHRONOUSLY on the pinned client. The
// pinned client's connection acquisition is NOT context-cancellable (D107),
// and a call in flight when the generation's client is poisoned parks
// FOREVER on the SDK's unbuffered channel send (D135's parked-sender
// mechanism). One such parked call during a KurrentDB outage outlasting the
// client's discovery budget wedged the publisher's Run — and with it the
// whole worker host: the supervisor cancelled the poisoned generation,
// waited for the generation to drain, and the drain never completed (the
// AR-210 R2 defect — no logs, metrics dark, no recovery even after the
// server returned). The publisher now uses the same bounded-establishment
// treatment as the projection workers: SubscribeToAll runs on a goroutine
// raced against an establishment timeout, with a generation-cancel escape
// on every wait. An abandoned establishment call is drained by ONE
// goroutine that closes any late subscription — bounded accumulation, the
// same honest guarantee as projection.Worker.subscribe.
//
// # Subject naming
//
// Events are published to subjects of the form:
//
//	yardflow.<aggregateType>.<eventType>
//
// e.g. `yardflow.country.registered`, `yardflow.location.terminal_opened`. The
// aggregateType is derived from the stream-id prefix (everything before
// the first '-'); consumers subscribe with wildcards like `yardflow.country.>`
// or `yardflow.>`. Note that `yardflow.*` matches only two-token subjects and
// therefore none of the published subjects — stream subject filters and
// consumer subscriptions covering the whole family must use `yardflow.>`.
package natspub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aliighahremanii/buildingblocks/infrastructure/metrics"
	"github.com/aliighahremanii/buildingblocks/infrastructure/tracing"
)

// Publisher is the sink-side interface the worker depends on. The
// concrete implementation wraps a nats.JetStreamContext; tests substitute
// a fake that records published messages without a real NATS server.
//
// PublishAsync semantics:
//   - subject is the NATS subject (e.g. "yardflow.country.registered").
//   - data is the event payload bytes (the KurrentDB event Data field).
//   - msgID is the Nats-Msg-Id header value (the KurrentDB EventID).
//     Used by JetStream for dedup.
//   - Returns nil on successful queueing (the ack may arrive
//     asynchronously; the worker tracks position only after a successful
//     return, accepting that a crash between return and position-save
//     will cause a duplicate publish that JetStream dedups).
type Publisher interface {
	PublishAsync(ctx context.Context, subject string, data []byte, msgID string) error
}

// PositionStore tracks the publisher's last published KurrentDB $all
// commit position. The Postgres implementation lives in
// infrastructure/postgres/natspub.
type PositionStore interface {
	Get(ctx context.Context, publisherName string) (uint64, error)
	Save(ctx context.Context, publisherName string, position uint64, eventsPublished uint64) error
}

// WorkerConfig configures one publisher.
type WorkerConfig struct {
	// Name is the publisher key under which the position is stored
	// (e.g. "yardflow-nats-publisher"). Allows multiple publishers (one per
	// NATS stream) to track positions independently.
	Name string
	// Prefixes is the list of KurrentDB stream-id prefixes the
	// server-side filter will yield. Use module prefixes (e.g.
	// ["country-", "province-", "location-", "item-", "note-"]) to
	// publish every domain event; narrower filters publish a subset.
	Prefixes []string
	// SubjectPrefix is prepended to every NATS subject. Default "yardflow".
	// Use a non-default prefix when multiple TMS instances publish to a
	// shared NATS and you want subject-name isolation.
	SubjectPrefix string
	// BatchSize controls how many events are published between position
	// saves. Zero falls back to the default (50).
	BatchSize int
	// Transform, when non-nil, switches the worker to 0..N batch-emission
	// mode (R3-F1): each committed source event derives a batch of 0..N
	// contract messages (each with its own subject and Nats-Msg-Id) via a
	// DETERMINISTIC pure function; the durable position advances past a
	// source event only after EVERY message of its batch is published
	// (whole-source-event redrive — see transform.go). nil keeps the raw
	// one-message republish mode byte-identical.
	Transform EventTransform
}

// subscription is exactly what the worker needs from a live KurrentDB
// subscription; *kurrentdb.Subscription implements it. It lives here (where
// it is consumed) so the bounded-establishment machinery is unit-testable
// without a live server, mirroring projection.recvStream.
type subscription interface {
	Recv() *kurrentdb.SubscriptionEvent
	Close() error
}

// subscribeFunc opens a subscription. The worker's default implementation
// calls the shared client; tests override the unexported field.
type subscribeFunc func(client *kurrentdb.Client, ctx context.Context, opts kurrentdb.SubscribeToAllOptions) (subscription, error)

// subscribeToAll is the production SubscribeToAll adapter.
func subscribeToAll(client *kurrentdb.Client, ctx context.Context, opts kurrentdb.SubscribeToAllOptions) (subscription, error) {
	return client.SubscribeToAll(ctx, opts)
}

// Worker subscribes to KurrentDB $all and republishes matching events to
// NATS JetStream. It is safe to construct once and Run from a single
// goroutine (typically the worker process).
type Worker struct {
	client    *kurrentdb.Client
	positions PositionStore
	publisher Publisher
	validator Validator
	config    WorkerConfig

	// runAttempt is the per-cycle subscription attempt; New binds it to
	// runOnce. It is a field so Run-loop tests can substitute a
	// deterministic attempt outcome (see worker_run_test.go) without a
	// live KurrentDB.
	runAttempt func(ctx context.Context) (uint64, error)

	// subscribeF opens the subscription inside runOnce. New binds it to
	// the production SubscribeToAll adapter; tests override it to park,
	// delay, or fail deterministically (D136 bounded-establishment
	// coverage).
	subscribeF subscribeFunc

	// establishmentTimeout bounds the SubscribeToAll confirmation phase
	// (D136 — see the package doc). Tests shorten it; production uses the
	// package default.
	establishmentTimeout time.Duration

	// initialDelay/maxDelay bound the reconnect backoff. Tests shorten
	// them; production uses the package defaults.
	initialDelay time.Duration
	maxDelay     time.Duration

	// flushTimeout bounds the terminal position flush (D131). Tests
	// shorten it; production uses the package default.
	flushTimeout time.Duration

	// failureLog state — touched only from Run's goroutine.
	validationLog failureLog
	publishLog    failureLog
	reconnectLog  failureLog
}

// New constructs a NATS publisher worker. validator may be nil (validation
// is skipped — construction sites that have a JetStream context MUST pass
// one; nil is only for tests and non-JetStream sinks), and must perform its
// checks read-only.
func New(client *kurrentdb.Client, positions PositionStore, publisher Publisher, validator Validator, config WorkerConfig) *Worker {
	if config.SubjectPrefix == "" {
		config.SubjectPrefix = "yardflow"
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 50
	}
	return &Worker{
		client:               client,
		positions:            positions,
		publisher:            publisher,
		validator:            validator,
		config:               config,
		runAttempt:           nil, // bound below (needs the constructed receiver)
		subscribeF:           subscribeToAll,
		establishmentTimeout: defaultEstablishmentTimeout,
		initialDelay:         defaultInitialRetryDelay,
		maxDelay:             defaultMaxRetryDelay,
		flushTimeout:         defaultFlushTimeout,
	}
}

// bindRunAttempt wires the default subscription attempt. Split from New
// only because the closure needs the constructed receiver.
func (w *Worker) bindRunAttempt() {
	if w.runAttempt == nil {
		w.runAttempt = w.runOnce
	}
}

const (
	// Backoff bounds for the reconnect loop (D114): consecutive
	// unproductive attempts double the delay from defaultInitialRetryDelay
	// up to defaultMaxRetryDelay; a productive attempt resets it to the
	// initial floor (quick retry, still bounded — no hot loop). The delay
	// is always waited on a context-cancellable select.
	defaultInitialRetryDelay = time.Second
	defaultMaxRetryDelay     = 30 * time.Second
	// defaultFlushTimeout bounds the terminal position flush (D131).
	// The flush runs on a bounded context DETACHED from the (typically
	// already cancelled) run context — mirroring the projection
	// workers' terminal checkpoint flush — so graceful shutdown still
	// persists the published-but-unsaved tail, but cannot hang process
	// exit forever either.
	defaultFlushTimeout = 10 * time.Second
	// defaultEstablishmentTimeout bounds the SubscribeToAll confirmation
	// phase (D136 — see the package doc). The OBSERVED wait is what keeps
	// the worker host's generation drain bounded: the pinned client's
	// connection acquisition is not context-cancellable, so an
	// establishment call in flight at a client poison parks forever; every
	// wait in the bounded-establishment path therefore also carries a
	// generation-cancel escape. The generous default tolerates slow node
	// discovery in clustered deployments while still converting a wedged
	// establishment into a retryable failure instead of an indefinite
	// hang.
	defaultEstablishmentTimeout = 30 * time.Second
	// reminderEvery is the dedupe cadence: an identical failure is logged
	// in full once, then briefly reminded every reminderEvery-th
	// occurrence so long-running outages stay visible without spam.
	reminderEvery         = 10
	streamFilterMaxWindow = 1000
)

// failureLog deduplicates repeated identical failures: the first occurrence
// logs in full (with the classified guidance), identical occurrences are
// counted silently, and every reminderEvery-th occurrence logs a one-line
// reminder. A changed failure key logs in full again; reset() clears state
// when the condition clears. Not goroutine-safe — owned by Run's goroutine.
type failureLog struct {
	key   string
	count int
}

// log records a failure identified by key. It invokes logFull on the first
// occurrence and every reminderEvery-th occurrence, and reminder (with the
// running count) on other occurrences only if remind is true.
func (f *failureLog) log(key string, logFull func(), remind func(count int)) {
	if key != f.key {
		f.key = key
		f.count = 1
		logFull()
		return
	}
	f.count++
	if f.count%reminderEvery == 0 {
		logFull()
		return
	}
	if remind != nil {
		remind(f.count)
	}
}

// reset clears the dedupe state (the condition cleared).
func (f *failureLog) reset() {
	f.key = ""
	f.count = 0
}

// Run blocks until ctx is cancelled, maintaining a live subscription to
// the $all stream. Subscription drops are logged (deduplicated) and
// retried with bounded exponential backoff. Before ANY KurrentDB
// subscription is opened, the configured JetStream stream is validated
// read-only; while validation fails the worker stays parked in the
// validation gate (bounded backoff, cancellable) rather than crash-looping
// $all reads into a sink that cannot accept events — no position advances,
// so no events are lost or skipped.
func (w *Worker) Run(ctx context.Context) error {
	w.bindRunAttempt()
	delay := time.Duration(0)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Gate on JetStream validation BEFORE touching KurrentDB or the
		// position store: a misconfigured stream must block here (with
		// one actionable log), not fail after the subscription starts.
		if err := w.awaitValidation(ctx); err != nil {
			return err
		}
		published, err := w.runAttempt(ctx)
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err == nil && published == 0:
			// Abnormal: the subscription ended cleanly without publishing
			// anything while the context is still live. Treat it as an
			// unproductive failure (backoff bounds any churn) and surface
			// it deduplicated.
			delay = nextBackoff(delay, w.initialDelay, w.maxDelay)
			w.reconnectLog.log("run:clean-noop", func() {
				slog.Warn("natspub: subscription ended cleanly without publishing; reconnecting",
					"name", w.config.Name)
			}, func(count int) {
				slog.Warn("natspub: subscription still ending cleanly without publishing",
					"name", w.config.Name, "attempts", count)
			})
		case published > 0:
			// Productive attempt (events were published) that still ended
			// — typically a healthy subscription that was dropped; retry
			// quickly but BOUNDED: reset the backoff to the initial floor
			// so repeated productive cycles cannot hot-loop while lag
			// drains fast.
			delay = w.initialDelay
			if err == nil {
				// Ended cleanly after publishing: the condition cleared.
				w.reconnectLog.reset()
			} else {
				w.logReconnectAttempt(err)
			}
		default:
			// Unproductive failure: grow the bounded exponential backoff.
			delay = nextBackoff(delay, w.initialDelay, w.maxDelay)
			w.logReconnectAttempt(err)
		}
		if !sleepCtx(ctx, delay) {
			return ctx.Err()
		}
	}
}

// logReconnectAttempt emits the deduplicated subscription-ended log for an
// attempt that ended with an error.
func (w *Worker) logReconnectAttempt(err error) {
	w.reconnectLog.log(errFingerprint(err), func() {
		slog.Error("natspub: subscription ended, reconnecting",
			"name", w.config.Name, "err", err)
	}, func(count int) {
		slog.Warn("natspub: subscription still failing",
			"name", w.config.Name, "attempts", count, "err", err)
	})
}

// awaitValidation blocks until the JetStream validation passes, the context
// is cancelled, retrying with bounded exponential backoff. Failures are
// classified and logged without duplication: the blocking condition is
// logged once in full (kind + guidance), with periodic reminders while it
// persists, and a recovery after failures is logged as a state change.
func (w *Worker) awaitValidation(ctx context.Context) error {
	if w.validator == nil {
		return nil
	}
	attempt := 0
	delay := time.Duration(0)
	for {
		err := w.validator.Validate(ctx)
		if err == nil {
			if attempt > 0 {
				w.validationLog.reset()
				slog.Info("natspub: JetStream validation recovered; opening KurrentDB subscription",
					"name", w.config.Name, "attempts", attempt)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		attempt++
		var ne *Error
		if errors.As(err, &ne) {
			w.validationLog.log("validate:"+string(ne.Kind)+":"+ne.Detail, func() {
				slog.Error("natspub: JetStream validation failed; KurrentDB subscription NOT opened (position unchanged, no events lost)",
					"name", w.config.Name,
					"kind", string(ne.Kind),
					"stream", ne.Stream,
					"guidance", ne.Guidance,
					"err", err)
			}, func(count int) {
				slog.Warn("natspub: JetStream validation still failing",
					"name", w.config.Name, "attempts", count, "err", err)
			})
		} else {
			// Unclassified validator failure — still blocking, logged
			// deduplicated without fabricated guidance.
			w.validationLog.log("validate:unclassified", func() {
				slog.Error("natspub: JetStream validation failed (unclassified); KurrentDB subscription NOT opened",
					"name", w.config.Name, "err", err)
			}, func(count int) {
				slog.Warn("natspub: JetStream validation still failing",
					"name", w.config.Name, "attempts", count, "err", err)
			})
		}
		delay = nextBackoff(delay, w.initialDelay, w.maxDelay)
		if !sleepCtx(ctx, delay) {
			return ctx.Err()
		}
	}
}

// nextBackoff grows the current backoff delay by doubling, clamped to max.
// Zero (first failure) starts at initial.
func nextBackoff(current, initial, max time.Duration) time.Duration {
	if current <= 0 {
		return initial
	}
	next := current * 2
	if next > max {
		return max
	}
	return next
}

// sleepCtx waits for d unless ctx is done first. It reports whether the
// full delay elapsed; false means the context was cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// errFingerprint derives the reconnect-dedupe key from an error: classified
// natspub errors dedupe by kind (a persistent condition repeats with the
// same kind even when the wrapped transport text varies), everything else
// by its message.
func errFingerprint(err error) string {
	var ne *Error
	if errors.As(err, &ne) {
		return "run:" + string(ne.Kind)
	}
	return "run:" + err.Error()
}

// runOnce opens a subscription, publishes events until it drops, and
// returns how many events were published this attempt plus the cause of the
// end (or nil for a clean end). published lets Run distinguish a productive
// attempt (retry immediately) from an unproductive failure (back off).
func (w *Worker) runOnce(ctx context.Context) (uint64, error) {
	pos, err := w.positions.Get(ctx, w.config.Name)
	if err != nil {
		return 0, fmt.Errorf("load position %s: %w", w.config.Name, err)
	}

	opts := kurrentdb.SubscribeToAllOptions{
		From: kurrentdb.Position{Commit: pos, Prepare: pos},
		Filter: &kurrentdb.SubscriptionFilter{
			Type:     kurrentdb.StreamFilterType,
			Prefixes: w.config.Prefixes,
		},
		MaxSearchWindow:    streamFilterMaxWindow,
		CheckpointInterval: streamFilterMaxWindow,
	}
	// Bounded establishment (D136): the synchronous SubscribeToAll could
	// park forever on a poisoned client and wedge the generation drain
	// (see the package doc). handle owns the establishment context's
	// release; the stream is closed BEFORE the context is cancelled (a
	// cancelled stream context would kill a healthy live stream).
	handle, err := w.subscribe(ctx, opts)
	if err != nil {
		return 0, fmt.Errorf("subscribe %s: %w", w.config.Name, err)
	}
	defer handle.release()
	defer handle.stream.Close()
	stream := handle.stream

	var (
		processed       int
		lastCommit      = pos
		savedCommit     = pos  // the commit boundary already durably saved this attempt
		eventsPublished uint64 // total published this attempt (Run's productivity signal)
		unsaved         uint64 // published since the last SUCCESSFUL save (the exact delta to account)
	)
	for {
		sel := stream.Recv()
		if sel == nil {
			// Stream ended (a server-side end, or the subscription
			// closing under graceful shutdown): persist the unsaved tail
			// on the terminal flush's bounded context DETACHED from ctx
			// — ctx may already be cancelled (D131), and flushing
			// through it would always fail and silently lose the tail to
			// replay. A failed flush ends the attempt with the durable
			// position preserved — the next attempt replays from it and
			// JetStream dedups the overlap.
			if err := w.flushTerminal(lastCommit, unsaved); err != nil {
				return eventsPublished, err
			}
			// Transform mode only: a silent tail (zero-message source
			// events past the last batch boundary) advances the position
			// without any publish delta — persist the moved boundary so
			// the durable cursor does not trail it (the raw mode never
			// reaches here with a moved boundary: unsaved>0 already
			// flushed).
			if lastCommit != savedCommit {
				if err := w.flushPositionOnly(lastCommit); err != nil {
					return eventsPublished, err
				}
			}
			return eventsPublished, nil
		}
		switch {
		case sel.EventAppeared != nil:
			ev := sel.EventAppeared.Event
			if ev == nil {
				continue
			}
			// Derive the event's 0..N batch (raw mode: exactly one
			// message; transform mode: the deterministic contract batch —
			// R3-F1, see transform.go). A transform error ends the
			// attempt with the position preserved — the event is never
			// skipped.
			batch, derr := w.deriveBatch(ev)
			if derr != nil {
				w.publishLog.log("transform:"+errFingerprint(derr), func() {
					slog.Error("natspub: derive batch failed; stopping subscription (position not advanced — no event is skipped)",
						"name", w.config.Name,
						"event_id", ev.EventID,
						"event_type", ev.EventType,
						"err", derr)
				}, func(count int) {
					slog.Warn("natspub: derive batch still failing",
						"name", w.config.Name, "attempts", count, "err", derr)
				})
				return eventsPublished, derr
			}
			// Publish EVERY message of the batch before the position may
			// advance past this source event (whole-source-event redrive:
			// a failure mid-batch stops the attempt with the cursor at
			// the previous fully-published batch boundary; redrive
			// republishes the whole batch and JetStream dedups the
			// overlap by Nats-Msg-Id).
			for _, msg := range batch {
				// Trace each publish so end-to-end event latency and publish
				// failure rates are visible per subject. The span is tight
				// around the publish call (not the whole event-handling branch)
				// so the trace reflects publish time, not position-save time.
				publishCtx, span := tracing.Start(ctx, "nats.publish")
				if span != nil {
					span.SetAttributes(
						attribute.String("subject", msg.Subject),
						attribute.String("msg_id", msg.MsgID),
					)
				}
				if err := w.publisher.PublishAsync(publishCtx, msg.Subject, msg.Data, msg.MsgID); err != nil {
					// Publish failure: stop the subscription so we resume
					// from the last durably saved position on restart. We do
					// NOT skip the event (or the rest of its batch) — NATS
					// publication is the system's outbox contract, and
					// skipping would silently lose the event from every
					// downstream consumer. Events published after the last
					// successful save are intentionally NOT flushed here:
					// their save would advance the position only after a
					// publish that DID succeed, but the retry replays them
					// anyway and JetStream dedups by Nats-Msg-Id, so leaving
					// them unsaved is the no-loss choice.
					if span != nil {
						span.RecordError(err)
						span.End()
					}
					metrics.NATSPublishes.WithLabelValues("failure").Inc()
					logAttrs := []any{
						"name", w.config.Name,
						"subject", msg.Subject,
						"event_id", ev.EventID,
						"err", err,
					}
					var ne *Error
					if errors.As(err, &ne) {
						logAttrs = append(logAttrs, "kind", string(ne.Kind))
						if ne.Guidance != "" {
							logAttrs = append(logAttrs, "guidance", ne.Guidance)
						}
					}
					w.publishLog.log(publishFailureKey(err, msg.Subject), func() {
						slog.Error("natspub: publish failed; stopping subscription (position not advanced — no event is skipped)", logAttrs...)
					}, func(count int) {
						slog.Warn("natspub: publish still failing",
							append([]any{"name", w.config.Name, "attempts", count}, logAttrs...)...)
					})
					return eventsPublished, fmt.Errorf("natspub publish %s: %w", msg.Subject, err)
				}
				if span != nil {
					span.End()
				}
				metrics.NATSPublishes.WithLabelValues("success").Inc()
			}
			// The WHOLE batch is published: the source event may now count
			// as processed and the position may advance past it.
			lastCommit = ev.Position.Commit
			eventsPublished += uint64(len(batch))
			unsaved += uint64(len(batch))
			processed++
			if processed >= w.config.BatchSize {
				// Durable boundary (D114-R1): a failed save ENDS the
				// attempt — never superseded by further reads. The next
				// attempt resumes from the persisted position; already
				// published events are deduplicated by Nats-Msg-Id, and
				// the events_published delta is only accounted by the
				// save that actually succeeded.
				//
				// In transform mode a source event may derive ZERO
				// messages (an event no contract family consumes); the
				// position STILL advances past it — saving a zero-delta
				// boundary when the commit moved (but not when it did
				// not: a no-op save is pure churn). Re-scanning silent
				// events after a restart is harmless (they re-derive
				// nothing) but the durable cursor should not trail the
				// whole silent tail.
				var flushErr error
				if unsaved > 0 {
					flushErr = w.attemptFlush(ctx, lastCommit, unsaved)
				} else {
					// Transform mode only: a silent-tail boundary (the
					// position moved without publishing anything). Save
					// the moved boundary with a zero delta so the durable
					// cursor does not trail the silent tail (re-scanning
					// silent events is harmless but wasteful).
					flushErr = w.attemptPositionSave(ctx, lastCommit)
				}
				if flushErr != nil {
					if ctx.Err() != nil {
						// Shutdown raced the batch save: the failure is a
						// cancellation artifact, NOT necessarily a store
						// failure (the cancelled Exec may or may not have
						// committed server-side). End through the terminal
						// path — the detached bounded flush and its
						// raced-save guard still persist this boundary
						// without double-counting, so shutdown progress
						// is not lost to replay (D131).
						return eventsPublished, w.flushTerminal(lastCommit, unsaved)
					}
					return eventsPublished, flushErr
				}
				savedCommit = lastCommit
				unsaved = 0
				processed = 0
			}
			w.publishLog.reset()
		case sel.SubscriptionDropped != nil:
			// Persist the unsaved tail before ending — on the terminal
			// flush's bounded context DETACHED from ctx (ctx may already
			// be cancelled — D131); a failed flush surfaces as the
			// attempt error so Run retries from the durable position
			// instead of silently proceeding past a failed boundary.
			if err := w.flushTerminal(lastCommit, unsaved); err != nil {
				return eventsPublished, err
			}
			if lastCommit != savedCommit {
				if err := w.flushPositionOnly(lastCommit); err != nil {
					return eventsPublished, err
				}
			}
			if derr := sel.SubscriptionDropped.Error; derr != nil {
				return eventsPublished, fmt.Errorf("subscription dropped %s: %w", w.config.Name, derr)
			}
			return eventsPublished, nil
		}
	}
}

// subscribeResult is the outcome of one establishment call.
type subscribeResult struct {
	stream subscription
	err    error
}

// subHandle pairs a live subscription with the establishment context
// release that must run only after the stream has closed — cancelling the
// establishment context while the stream is alive would kill a healthy
// subscription (the stream's context is its child).
type subHandle struct {
	stream  subscription
	release context.CancelFunc
}

// subscribe opens a subscription with a BOUNDED establishment phase (D136
// — see the package doc), mirroring projection.Worker.subscribe:
//
//   - SubscribeToAll runs on a goroutine raced against
//     w.establishmentTimeout; the result channel is buffered so the
//     goroutine can always deliver and exit.
//   - On timeout the establishment context is cancelled (aborting the
//     half-open attempt) and the call BLOCKS until it returns, so at most
//     ONE establishment call is ever unresolved per attempt: a
//     well-behaved client returns within milliseconds (the late
//     subscription is closed synchronously — no server-side leaks), while
//     a call parked on a poisoned client's unbuffered channel send never
//     returns and is abandoned to a single drain goroutine. Go cannot
//     kill a stuck call; this is the honest bound, not a no-leak claim.
//   - EVERY wait carries a generation-cancel escape, so a parked call can
//     never wedge the worker host's generation drain: Run returns
//     promptly on ctx cancellation (the abandoned call's client
//     generation is retired, never closed — D135).
func (w *Worker) subscribe(ctx context.Context, opts kurrentdb.SubscribeToAllOptions) (*subHandle, error) {
	estCtx, cancel := context.WithCancel(ctx)

	ch := make(chan subscribeResult, 1)
	go func() {
		s, err := w.subscribeF(w.client, estCtx, opts)
		ch <- subscribeResult{stream: s, err: err}
	}()

	timer := time.NewTimer(w.establishmentTimeout)
	defer timer.Stop()

	select {
	case r := <-ch:
		if r.err != nil {
			cancel()
			return nil, r.err
		}
		return &subHandle{stream: r.stream, release: cancel}, nil
	case <-timer.C:
		// Timeout: abort the half-open attempt. The result channel is
		// buffered, so the attempt goroutine can always deliver and exit.
		cancel()
		timeoutErr := fmt.Errorf("subscribe %s: establishment timed out after %s",
			w.config.Name, w.establishmentTimeout)
		// Wait for the aborted call to return (bounded-accumulation
		// wait): no further subscribe attempts are issued from THIS
		// attempt while blocked. Generation cancellation still escapes.
		select {
		case r := <-ch:
			// The call returned after the timeout: close any late
			// subscription SYNCHRONOUSLY (no drain goroutine) so no
			// server-side subscription leaks, then surface the timeout.
			if r.stream != nil {
				_ = r.stream.Close()
			}
			return nil, timeoutErr
		case <-ctx.Done():
			go drainLateSubscription(ch)
			return nil, ctx.Err()
		}
	case <-ctx.Done():
		cancel()
		go drainLateSubscription(ch)
		return nil, ctx.Err()
	}
}

// drainLateSubscription consumes the single buffered result of an
// establishment attempt that outlived its caller (generation cancellation
// raced it) and closes any subscription it produced. Spawned at most once
// per unresolved attempt by subscribe's cancel escape paths; a call parked
// on a poisoned client never delivers and the goroutine stays inert,
// reclaimed at process exit (D135 semantics).
func drainLateSubscription(ch <-chan subscribeResult) {
	if r, ok := <-ch; ok && r.stream != nil {
		_ = r.stream.Close()
	}
}

// publishFailureKey derives the publish-dedupe key: classified natspub
// errors dedupe by kind+subject (the blocking condition is the stream
// configuration for that subject family), transport noise dedupes by the
// full subject.
func publishFailureKey(err error, subject string) string {
	var ne *Error
	if errors.As(err, &ne) {
		return "publish:" + string(ne.Kind) + ":" + subject
	}
	return "publish:" + subject
}

// attemptFlush persists the events published since the last SUCCESSFUL
// save (unsaved is the exact delta — PositionStore.Save ADDS it to the
// running total, so passing an attempt-total here would double-count) and
// advances the NATSPosition metric ONLY after the save succeeded. A zero
// delta is a no-op (no useless write, no metric churn). A failure is
// returned to the caller, which MUST end the attempt: the durable
// position stays at the last successful save, the next attempt resumes
// from it, and the republished overlap is deduplicated by Nats-Msg-Id
// (D114-R1 durable-boundary invariant — a failed boundary is never
// superseded).
func (w *Worker) attemptFlush(ctx context.Context, commitPosition uint64, unsaved uint64) error {
	if unsaved == 0 {
		return nil
	}
	if err := w.positions.Save(ctx, w.config.Name, commitPosition, unsaved); err != nil {
		return fmt.Errorf("natspub position save %s: %w", w.config.Name, err)
	}
	metrics.NATSPosition.Set(float64(commitPosition))
	return nil
}

// attemptPositionSave persists a commit boundary with a ZERO publish
// delta — the transform mode's silent-tail boundary (zero-message source
// events moved the cursor but published nothing). The raw mode never
// reaches it (each processed event publishes exactly one message); it is
// deliberately separate from attemptFlush, whose zero-delta no-op
// contract is pinned (no useless write, no metric churn).
func (w *Worker) attemptPositionSave(ctx context.Context, commitPosition uint64) error {
	if err := w.positions.Save(ctx, w.config.Name, commitPosition, 0); err != nil {
		return fmt.Errorf("natspub position save %s: %w", w.config.Name, err)
	}
	metrics.NATSPosition.Set(float64(commitPosition))
	return nil
}

// flushTerminal persists the events published since the last SUCCESSFUL
// save at the END of a subscription attempt (stream end, drop, or a
// batch save that raced shutdown). It runs on a bounded context DETACHED
// from the run context — the projection workers' terminal-flush pattern
// (D131): the run context is typically ALREADY cancelled when the
// attempt ends on graceful shutdown, and flushing through it would
// always fail with context.Canceled and silently lose the
// published-but-unsaved tail to replay-after-restart (duplicates beyond
// the JetStream dedup window; the C05 E2E defect).
//
// Raced-save guard: the store's Save ADDS events_published, so blindly
// re-saving a delta whose previous attempt's fate is unknown — a batch
// save that raced shutdown may have committed server-side even though
// the worker observed context.Canceled — could double-count the
// accumulating counter. The authoritative position is therefore re-read
// on the SAME detached context first: if it already covers the boundary,
// the delta is durably accounted and no save happens (only the metric
// mirrors it); otherwise the boundary is flushed as the FIRST save for
// that delta. The read is authoritative under the position store's
// single-writer-per-publisher-name contract.
//
// A zero delta is a no-op (no read, no write, no metric churn). A
// failure is logged — the durable position is preserved, the tail
// replays on resume, and JetStream dedups within its window — and
// returned so Run classifies the attempt end; a cancelled run context
// makes Run discard the return value after the log line, matching the
// projection worker's shutdown semantics.
func (w *Worker) flushTerminal(commitPosition uint64, unsaved uint64) error {
	if unsaved == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), w.flushTimeout)
	defer cancel()
	persisted, err := w.positions.Get(ctx, w.config.Name)
	if err != nil {
		slog.Error("natspub: terminal position flush could not re-read the durable position; the unsaved tail replays on resume (JetStream dedups)",
			"name", w.config.Name, "commit", commitPosition, "unsaved", unsaved, "err", err)
		return fmt.Errorf("natspub terminal flush re-read %s: %w", w.config.Name, err)
	}
	if persisted >= commitPosition {
		// The boundary is already durable (a raced batch save committed
		// server-side) and its delta is accounted; the accumulating
		// counter forbids a second save of the same delta. Only mirror
		// the confirmed position into the metric.
		metrics.NATSPosition.Set(float64(commitPosition))
		return nil
	}
	if err := w.attemptFlush(ctx, commitPosition, unsaved); err != nil {
		slog.Error("natspub: terminal position flush failed; durable position preserved (the unsaved tail replays on resume and JetStream dedups)",
			"name", w.config.Name, "commit", commitPosition, "unsaved", unsaved, "err", err)
		return err
	}
	return nil
}

// flushPositionOnly persists a commit boundary whose events published
// NOTHING new (transform mode: trailing zero-message source events). It
// re-reads the authoritative position first (the same raced-save guard as
// flushTerminal), saves a ZERO delta when the boundary moved, and is a
// pure no-op (no read, no write) when it did not — preserving the idle
// no-op contract for the raw mode, where unsaved == 0 implies the position
// never moved.
func (w *Worker) flushPositionOnly(commitPosition uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.flushTimeout)
	defer cancel()
	persisted, err := w.positions.Get(ctx, w.config.Name)
	if err != nil {
		// The silent tail rescans on resume (harmless: those events
		// re-derive nothing); surface the failure for attempt
		// classification.
		return fmt.Errorf("natspub terminal position re-read %s: %w", w.config.Name, err)
	}
	if persisted >= commitPosition {
		return nil
	}
	return w.attemptPositionSave(ctx, commitPosition)
}

// subjectFor derives the NATS subject from the KurrentDB stream id and
// event type. The stream id is "<aggregateType>-<uuid>"; the subject is
// "<prefix>.<aggregateType>.<eventType>". e.g.
// "country-abc-123" + "country.registered" → "yardflow.country.country.registered"
//
// The aggregateType appears twice in the example above because
// KurrentDB's eventType already carries it as a prefix. We could strip
// the duplicate, but keeping it makes wildcard subscriptions
// (`yardflow.country.>`) match both stream-typed and event-typed consumers.
// Tests assert on the exact shape.
func subjectFor(prefix, streamID, eventType string) string {
	aggType := streamID
	if i := strings.Index(streamID, "-"); i > 0 {
		aggType = streamID[:i]
	}
	return fmt.Sprintf("%s.%s.%s", prefix, aggType, eventType)
}
