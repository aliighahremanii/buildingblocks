// Package projection provides a generic KurrentDB -> arbitrary-sink
// projection worker. It centralizes the subscription loop that every
// module-specific projection worker (masterdata-postgres,
// operations-postgres, fleet-meilisearch, ...) would otherwise hand-roll:
//
//   - Subscribe to the FULL $all with a BOUNDED establishment phase (see
//     below). No server-side stream filter is sent: the filtered-read path
//     (FilteredReadAllEventsForward) is unreliable on KurrentDB 26.0.3 once
//     tombstoned streams exist, so the worker reads everything and applies
//     the prefix filter in-process (see matchesPrefix).
//   - Resume from the last checkpointed commit position (re-read from the
//     checkpoint store on every (re)subscribe; the shared KurrentDB client
//     is never closed or recreated).
//   - Hand each recorded event to a caller-supplied ProjectFunc.
//   - Retry transient failures up to MaxAttempts; on exhaustion, route
//     the event to the dead-letter queue AND advance the checkpoint in a
//     single atomic Postgres transaction.
//   - On subscription drop or worker shutdown, flush the checkpoint with
//     a bounded context that is NOT derived from the (possibly already
//     cancelled) run context, so shutdown-time progress is never lost to
//     a cancelled context. A FAILED checkpoint save ends the subscription
//     attempt immediately and the terminal flush may only persist the
//     last CONFIRMED position — a failed durable boundary is never
//     superseded by a later save, and the worker resumes from the
//     persisted checkpoint.
//
// The sink-specific work (unmarshal the event, write it to Postgres /
// Meilisearch) stays in the module as a ProjectFunc. This package has no
// knowledge of any module's aggregates or events.
//
// # Per-subscription boundary and lifecycle
//
// Each subscription attempt is owned by a subscriptionRunner: one receive
// goroutine pumps SubscriptionEvents from the KurrentDB subscription into
// a channel; the worker loop consumes them. The runner's Close is
// idempotent and bounded: it closes the individual KurrentDB subscription
// (which cancels only that stream's context — the shared client stays
// alive), waits for the receive goroutine to exit, and only then releases
// the establishment context. A worker therefore never leaks a receive
// goroutine or a server-side subscription behind a dropped attempt.
//
// The worker exposes a lifecycle state (idle -> connecting -> running ->
// down -> connecting -> ... ; stopped after Run returns) used by the
// worker host's /ready endpoint and the
// yardflow_projection_subscription_up metric. Readiness ownership is
// EXPLICIT: workers never self-register — the host owns a Registry
// (readiness.go) and injects it through the module worker sets, which
// register the workers they actually run. Only workers registered with
// ReadinessRequired gate /ready; optional infrastructure workers (e.g.
// Meilisearch projections) report their state but never gate readiness,
// and the NATS publisher (a separate package) is never registered.
//
// # Bounded subscription establishment
//
// The pinned client (KurrentDB-Client-Go v1.4.1) blocks in
// SubscribeToAll until the server confirms the subscription. A deadline
// context cannot bound ONLY that phase: the live stream's context is a
// child of the context passed to SubscribeToAll, so a deadline that fires
// after establishment would later kill a healthy subscription. The worker
// instead runs SubscribeToAll on a goroutine and races it against an
// establishment timer. On timeout the establishment context is cancelled
// (aborting the half-open attempt) and subscribe then BLOCKS until the
// call returns, so at most ONE establishment call is ever unresolved: a
// well-behaved client returns within milliseconds (late subscriptions are
// closed synchronously — no server-side subscription leaks), while a
// client that ignores cancellation WEDGES the worker (no further
// attempts, state down) until the call returns or the process exits.
// Go cannot kill a stuck call; this is the honest bound, not a no-leak
// claim.
//
// # Inactivity watchdog (probe-backed)
//
// A $all subscription sitting at the head of the stream is legitimately
// silent: the server sends no periodic messages, and CaughtUp notifications
// are server-version dependent (D107), so matching-event inactivity alone
// CANNOT distinguish "idle" from "wedged" and must never be treated as
// failure. The worker therefore pairs silence with a PROBE: when a
// catching-up subscription delivers nothing for StallTimeout, the watchdog
// reads the $all head with a bounded, independent RPC (see watchStall and
// probeHeadViaClient). If the head is at or behind the last processed
// position the subscription is healthy but idle — the watchdog disarms but
// KEEPS WATCHING on a backed-off schedule (idleRecheck, default 5m), so a
// wedge AT the head with later writes (neither delivered nor announced via
// FellBehind) is still detected the next time the head advances. If a
// successful probe proves undelivered events exist beyond lastCommit, the
// read is genuinely wedged and the worker closes only the subscription
// stream, forcing Run to resubscribe from the persisted checkpoint; an
// INCONCLUSIVE probe (timeout / transient error) is never immediate proof
// of a stall — the watchdog requires maxInconclusiveProbes consecutive
// failures first. This is what made the tombstone-triggered KurrentDB read
// wedge (InvalidReadException behind tombstoned streams) self-healing:
// previously it froze every projection silently until a manual restart.
// While the receive loop is inside ProjectFunc the watchdog skips, so a
// slow-but-healthy sink is never mistaken for a stalled read. Transport
// death stays bounded by the pinned client's gRPC keepalive (~20s) and
// surfaces to Recv as a drop without any application watchdog.
// See docs/runbooks/projection-subscription-recovery.md and docs/decisions.md
// (D109, superseding the D107 no-watchdog stance for this deployment).
//
// # Poison-event handling
//
// A "poison event" is one whose ProjectFunc returns an error on every
// retry. The worker retries up to MaxAttempts (default 5) with exponential
// backoff. If all attempts fail, the event is routed to the DLQ AND the
// checkpoint advances past it in a single transaction. This guarantees:
//
//   - The projection cannot get permanently stuck on one event.
//   - The failed event is preserved for operator inspection/replay.
//   - The checkpoint-and-DLQ-write cannot become inconsistent across
//     crashes (they commit in one Postgres tx).
//
// The worker returns from Run only when ctx is cancelled. Subscription
// drops are logged and the subscription is retried after a short delay,
// resuming from the last flushed checkpoint.
package projection

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"go.opentelemetry.io/otel/attribute"

	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/checkpoint"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/dlq"
	"github.com/aliighahremanii/buildingblocks/infrastructure/metrics"
	"github.com/aliighahremanii/buildingblocks/infrastructure/tracing"
)

const (
	defaultBatchSize      = 50
	defaultReconnectDelay = 2 * time.Second

	// defaultMaxAttempts is the number of times ProjectFunc is retried
	// before the event is routed to the DLQ. The first call counts, so
	// defaultMaxAttempts=5 means 1 initial attempt + 4 retries. The
	// default is intentionally modest: most poison events are
	// data-dependent bugs that no amount of retrying will fix, and each
	// retry attempt blocks the projection's forward progress.
	defaultMaxAttempts = 5

	// Retry-delay defaults. Tests pass overrides via Worker.retryBaseDelay
	// and Worker.retryMaxDelay so they can shorten backoff without mutating
	// package globals (which would race under t.Parallel).
	defaultRetryBaseDelay = 500 * time.Millisecond
	defaultRetryMaxDelay  = 30 * time.Second

	// defaultEstablishmentTimeout bounds the SubscribeToAll confirmation
	// phase. Establishment against a healthy server completes in
	// milliseconds; the generous default tolerates slow node discovery in
	// clustered deployments while still converting a wedged establishment
	// into a retryable failure instead of an indefinite hang.
	defaultEstablishmentTimeout = 30 * time.Second

	// defaultFlushTimeout bounds the terminal checkpoint flush. It runs on
	// a detached context (NOT derived from the cancelled run context) so
	// shutdown-time progress still persists, but cannot hang shutdown
	// forever either.
	defaultFlushTimeout = 10 * time.Second

	// defaultStallTimeout is how long a catching-up subscription may be
	// silent (no events and no server notifications) before the watchdog
	// treats it as a wedged read and forces a resubscribe. KurrentDB 26.0.3
	// can stall a live filtered read silently (InvalidReadException behind
	// tombstoned streams): the subscription stays "up" but delivers nothing
	// forever. Once the subscription reports CaughtUp, silence is normal on
	// an idle $all and the watchdog stays quiet.
	defaultStallTimeout = 60 * time.Second

	// stallCheckInterval is how often the stall watchdog re-evaluates.
	stallCheckInterval = 5 * time.Second

	// stallProbeTimeout is the OBSERVED bound on a head probe: the
	// watchdog waits at most this long for the probe result before
	// treating it as inconclusive. It is not a hard cap on the underlying
	// call — the pinned client's getConnectionHandle is not
	// context-cancellable (D107) — so the probe runs on a goroutine with
	// at most ONE parked probe per worker (see watchStall). A healthy
	// KurrentDB answers a single-event backward read in milliseconds.
	stallProbeTimeout = 5 * time.Second

	// idleRecheckInterval is how often the watchdog re-probes after it has
	// verified (or been told by CaughtUp) that the subscription is at the
	// head. A wedge at the head with later writes would otherwise be
	// permanent: the subscription receives neither the new events nor a
	// FellBehind. The recheck is deliberately longer than stallTimeout —
	// an idle $all at head is the common healthy state.
	idleRecheckInterval = 5 * time.Minute

	// maxInconclusiveProbes is how many consecutive failed/inconclusive
	// head probes (timeout, transient error) are required before the
	// watchdog treats the read as stalled. A single transient diagnostic
	// failure must not disrupt an otherwise usable subscription (with
	// seven workers probing independently, one slow server moment would
	// otherwise churn them all). Only a SUCCESSFUL probe that proves
	// head > lastCommit is immediate proof of lag.
	maxInconclusiveProbes = 3

	// pumpExitTimeout bounds how long runner.Close waits for the receive
	// goroutine to exit after the stream is closed. Per the pinned client
	// (v1.4.1), cancelling the stream context unblocks a pending Recv, so
	// this is defense-in-depth rather than an expected wait.
	pumpExitTimeout = 5 * time.Second
)

// State is the projection worker's lifecycle state. It is exported for
// operational surfaces (the worker host's /ready endpoint and tests).
type State string

const (
	// StateIdle: constructed but Run has not been called yet.
	StateIdle State = "idle"
	// StateConnecting: a subscription establishment attempt is in flight.
	StateConnecting State = "connecting"
	// StateRunning: the subscription is live and events are being received.
	StateRunning State = "running"
	// StateDown: the last attempt ended (drop or failure); the worker will
	// retry after the reconnect delay.
	StateDown State = "down"
	// StateStopped: Run returned (worker context cancelled).
	StateStopped State = "stopped"
)

// ProjectFunc handles a single recorded event pulled from the $all
// subscription. It is the hook where a module does its sink-specific write.
//
// Returning an error triggers the worker's bounded-retry-then-DLQ logic:
// the event is retried up to MaxAttempts; on exhaustion it is routed to
// the DLQ and the checkpoint advances past it atomically. Returning nil
// advances the checkpoint normally on the next flush.
//
// ProjectFunc implementations MUST be idempotent so that replay after a
// crash, dropped subscription, or DLQ replay never produces duplicates.
type ProjectFunc func(ctx context.Context, event *kurrentdb.RecordedEvent) error

// recvStream is the per-subscription boundary: exactly the two operations
// the worker needs from a live KurrentDB subscription. *kurrentdb.Subscription
// implements it; tests substitute fakes so the receive/goroutine/lifecycle
// machinery is unit-testable without a live server.
type recvStream interface {
	Recv() *kurrentdb.SubscriptionEvent
	Close() error
}

// subscribeFunc opens a subscription. The worker's default implementation
// calls the shared client; tests override the unexported field.
type subscribeFunc func(client *kurrentdb.Client, ctx context.Context, opts kurrentdb.SubscribeToAllOptions) (recvStream, error)

// checkpointer is the checkpoint-store seam consumed by the worker.
// *checkpoint.Store implements it. The interface lives here (where it is
// consumed) so unit tests can drive the full Run loop with a fake store.
type checkpointer interface {
	Get(ctx context.Context, projectionName string) (uint64, error)
	Save(ctx context.Context, projectionName string, position uint64) error
}

// WorkerConfig describes one projection's subscription wiring.
//
// Name is the checkpoint key under which the worker stores its last
// processed commit position (e.g. "operations-postgres"). Because the
// checkpoint table is shared across modules, name SHOULD be namespaced by
// module.
//
// Prefixes is the list of KurrentDB stream-id prefixes this worker
// projects (e.g. ["country-", "province-", "location-"]). The worker
// subscribes to the FULL $all (see subscribe — the server-side
// filtered-read path is unreliable on KurrentDB 26.0.3 once tombstoned
// streams exist) and applies the prefix match in-process; events in
// streams not matching any prefix advance the checkpoint but are never
// delivered to ProjectFunc. It MUST be non-empty (an empty prefix list
// filters out every stream).
//
// BatchSize controls how many SUCCESSFUL events are projected between
// checkpoint writes. A lower value bounds replay work after a crash; a
// higher value reduces checkpoint churn. Zero falls back to
// defaultBatchSize.
//
// MaxAttempts controls the bounded retry count before an event is routed
// to the DLQ. Zero falls back to defaultMaxAttempts. A value of 1 means
// "no retries" — the first failure immediately DLQs the event.
//
// EstablishmentTimeout bounds the SubscribeToAll confirmation phase (see
// the package doc). Zero or negative falls back to
// defaultEstablishmentTimeout.
//
// FlushTimeout bounds the terminal checkpoint flush that runs on a
// detached context when an attempt ends. Zero or negative falls back to
// defaultFlushTimeout.
//
// ReadinessRequired marks the worker as gating the worker host's /ready
// endpoint. Set it for projections whose sink is required infrastructure
// (the Postgres read-model projections). Optional-infrastructure
// projections (e.g. Meilisearch) report their state for observability but
// MUST NOT gate readiness; the NATS publisher is a separate worker type
// and never registers.
type WorkerConfig struct {
	Name      string
	Prefixes  []string
	BatchSize int

	// MaxAttempts caps the number of ProjectFunc calls per event before
	// DLQ'ing it. <=0 falls back to defaultMaxAttempts.
	MaxAttempts int

	// EstablishmentTimeout bounds subscription establishment. <=0 falls
	// back to defaultEstablishmentTimeout.
	EstablishmentTimeout time.Duration

	// FlushTimeout bounds the terminal checkpoint flush. <=0 falls back
	// to defaultFlushTimeout.
	FlushTimeout time.Duration

	// StallTimeout is how long a catching-up subscription may be silent
	// before the watchdog forces a resubscribe (see the package consts).
	// <=0 falls back to defaultStallTimeout. Once CaughtUp, silence is
	// normal and the watchdog stays quiet.
	StallTimeout time.Duration

	// ReadinessRequired: when true, this worker gates /ready.
	ReadinessRequired bool
}

// Validate rejects structurally invalid configuration at construction
// time so a miswired projection fails fast instead of silently doing
// nothing (or filtering out every stream).
func (c WorkerConfig) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return errors.New("projection: WorkerConfig.Name is required (the checkpoint key)")
	}
	if len(c.Prefixes) == 0 {
		return errors.New("projection: WorkerConfig.Prefixes is required (an empty prefix list filters out every stream)")
	}
	for _, p := range c.Prefixes {
		if p == "" {
			return errors.New("projection: WorkerConfig.Prefixes contains an empty string, which would match every stream; use the module's all-domain-prefixes helper instead")
		}
	}
	return nil
}

// Worker subscribes to KurrentDB $all and projects matching events
// through ProjectFunc. It is safe to construct once and Run from a single
// goroutine (typically the worker process's per-projection runner).
// Status may be read concurrently from other goroutines (/ready, metrics).
type Worker struct {
	client     *kurrentdb.Client
	checks     checkpointer
	dlqs       dlq.Store
	config     WorkerConfig
	project    ProjectFunc
	subscribeF subscribeFunc

	// retryBaseDelay / retryMaxDelay bound the exponential backoff between
	// ProjectFunc retries. Defaults are set in New from the package
	// consts; tests override them per-instance to keep unit-test latency
	// at microseconds while production keeps conservative multi-second
	// backoff. They live on the struct (not as package globals) so
	// parallel tests do not race on shared mutable state.
	retryBaseDelay time.Duration
	retryMaxDelay  time.Duration

	// reconnectDelay is the wait between subscription attempts after a
	// drop or failed establishment. Overridable per-instance for the same
	// reason as the retry delays.
	reconnectDelay time.Duration

	// stallTimeout is how long a catching-up subscription may be silent
	// before the watchdog forces a resubscribe (defaultStallTimeout).
	// Atomic so tests can shorten it mid-run while the watchdog reads it
	// every tick (see watchStall).
	stallTimeout atomic.Int64

	// stallInterval is the watchdog's re-evaluation period. Defaults to
	// stallCheckInterval; tests shorten it.
	stallInterval time.Duration

	// probeHead resolves the current $all head commit position with a
	// bounded read, so the watchdog can distinguish "idle at head"
	// (healthy silence) from "wedged behind head" (undelivered events).
	// Defaults to probeHeadViaClient; tests inject fakes.
	probeHead func(ctx context.Context) (uint64, bool)

	// idleRecheck is how often the watchdog re-probes after verifying the
	// head (defaults to idleRecheckInterval). Tests shorten it.
	idleRecheck time.Duration

	// probeTimeout is the observed bound on a head probe (defaults to
	// stallProbeTimeout). Tests shorten it.
	probeTimeout time.Duration

	// probePending is the per-worker single-flight gate for head probes:
	// the pinned client's connection acquisition is not context-cancellable
	// (D107), so a probe can park inside getConnectionHandle during wedged
	// client discovery. The gate guarantees at most ONE parked probe
	// goroutine per worker for the process lifetime (until the client call
	// returns), instead of one per watchdog tick.
	probePending atomic.Bool

	mu      sync.Mutex
	state   State
	lastErr error
}

// New constructs a projection worker. project is invoked for every
// recorded event matching config.Prefixes. dlqs may be nil — in that case
// failed events are retried indefinitely (the projection stalls on
// permanent failures rather than silently dropping them). Production
// deployments should always supply a non-nil dlqs.
//
// The worker does NOT register itself anywhere: readiness ownership is
// explicit — the composition layer (the worker host, via a Registry
// injected through ModuleDeps and the module worker sets) registers the
// workers it actually runs. See readiness.go.
func New(client *kurrentdb.Client, checks *checkpoint.Store, dlqs dlq.Store, config WorkerConfig, project ProjectFunc) (*Worker, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if config.BatchSize <= 0 {
		config.BatchSize = defaultBatchSize
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = defaultMaxAttempts
	}
	if config.EstablishmentTimeout <= 0 {
		config.EstablishmentTimeout = defaultEstablishmentTimeout
	}
	if config.FlushTimeout <= 0 {
		config.FlushTimeout = defaultFlushTimeout
	}
	if config.StallTimeout <= 0 {
		config.StallTimeout = defaultStallTimeout
	}
	worker := &Worker{
		client:         client,
		checks:         checks,
		dlqs:           dlqs,
		config:         config,
		project:        project,
		subscribeF:     subscribeToAll,
		retryBaseDelay: defaultRetryBaseDelay,
		retryMaxDelay:  defaultRetryMaxDelay,
		reconnectDelay: defaultReconnectDelay,
		stallInterval:  stallCheckInterval,
		idleRecheck:    idleRecheckInterval,
		probeTimeout:   stallProbeTimeout,
		state:          StateIdle,
	}
	worker.stallTimeout.Store(int64(config.StallTimeout))
	return worker, nil
}

// subscribeToAll is the default subscribe implementation over the shared
// client. *kurrentdb.Subscription satisfies recvStream.
func subscribeToAll(client *kurrentdb.Client, ctx context.Context, opts kurrentdb.SubscribeToAllOptions) (recvStream, error) {
	return client.SubscribeToAll(ctx, opts)
}

// Status returns the worker's current lifecycle state for operational
// surfaces. It is safe for concurrent use.
func (w *Worker) Status() WorkerStatus {
	w.mu.Lock()
	defer w.mu.Unlock()
	return WorkerStatus{
		Name:      w.config.Name,
		Required:  w.config.ReadinessRequired,
		State:     w.state,
		LastError: errString(w.lastErr),
	}
}

func (w *Worker) setState(s State, err error) {
	w.mu.Lock()
	w.state = s
	w.lastErr = err
	w.mu.Unlock()
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// Run blocks until ctx is cancelled, maintaining a live subscription to
// the $all stream. Subscription drops are logged and the subscription is
// retried after a short delay, resuming from the last checkpointed
// position on the SAME shared client. The only non-context error path is
// Run returning because ctx was cancelled; every per-attempt failure is
// retried forever.
func (w *Worker) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			w.setState(StateStopped, err)
			return err
		}
		w.setState(StateConnecting, nil)
		err := w.runAttempt(ctx)
		if ctx.Err() != nil {
			// The attempt ended because the worker is shutting down;
			// runAttempt already performed the terminal flush on a
			// detached bounded context.
			w.setState(StateStopped, ctx.Err())
			return ctx.Err()
		}
		w.setState(StateDown, err)
		select {
		case <-ctx.Done():
			w.setState(StateStopped, ctx.Err())
			return ctx.Err()
		case <-time.After(w.reconnectDelay):
		}
	}
}

// runAttempt performs one subscribe -> receive -> drop cycle. It returns
// the cause of the attempt's end (nil for a clean server-side end).
// Before returning it ALWAYS closes the individual subscription, waits
// for the receive goroutine, and flushes the checkpoint with a bounded
// context detached from ctx (ctx may already be cancelled — the terminal
// flush must still persist).
func (w *Worker) runAttempt(ctx context.Context) error {
	pos, err := w.checks.Get(ctx, w.config.Name)
	if err != nil {
		return fmt.Errorf("load checkpoint %s: %w", w.config.Name, err)
	}
	// The metric reflects only PERSISTED progress: the loaded checkpoint
	// is by definition confirmed storage.
	w.setCheckpointMetric(pos)

	w.setState(StateConnecting, nil)
	handle, err := w.subscribe(ctx, pos)
	if err != nil {
		return err
	}
	r := newSubscriptionRunner(handle)
	r.start()
	// Stall watchdog: while catching up, a subscription that delivers
	// nothing for stallTimeout is a wedged server-side read (see
	// watchStall). It closes only the stream — NOT the runner — so the
	// pump's Recv unblocks, the terminal event flows through r.events, and
	// the receiveLoop ends this attempt cleanly for Run to resubscribe.
	watchdogDone := make(chan struct{})
	go w.watchStall(ctx, r, watchdogDone)
	defer close(watchdogDone)
	defer func() {
		if cerr := r.close(); cerr != nil {
			slog.Error("projection: subscription cleanup problem",
				"name", w.config.Name, "err", cerr)
		}
	}()

	w.setState(StateRunning, nil)
	metrics.ProjectionSubscriptionUp.WithLabelValues(w.config.Name).Set(1)

	end := w.receiveLoop(ctx, r, pos)

	metrics.ProjectionReconnects.WithLabelValues(w.config.Name).Inc()
	metrics.ProjectionSubscriptionUp.WithLabelValues(w.config.Name).Set(0)

	// Terminal checkpoint flush: bounded, and detached from ctx so a
	// cancelled worker context cannot lose the progress of the last
	// (unflushed) batch. end.persist is the ONLY position the terminal
	// flush may write: on a normal end it is the last projected commit
	// (every event through it succeeded), but after a FAILED checkpoint
	// save it is the last position whose save CONFIRMED — the failed
	// durable boundary is never superseded by a later terminal write.
	if ferr := w.flushDetached(end.persist); ferr != nil {
		slog.Error("projection: terminal checkpoint flush failed",
			"name", w.config.Name, "commit", end.persist, "err", ferr)
	}
	return end.err
}

// attemptEnd describes how a receive loop ended and what the terminal
// flush is allowed to persist.
//
// persist is the highest commit position this attempt is PERMITTED to
// persist on shutdown: the last projected commit on a normal end, or the
// last CONFIRMED save when the attempt ended because a checkpoint save
// failed (the failed boundary must not be superseded).
type attemptEnd struct {
	persist uint64
	err     error
}

// receiveLoop consumes subscription events until the subscription ends or
// ctx is cancelled. It returns an attemptEnd: the position the terminal
// flush may persist and the end cause.
//
// Durable-boundary invariant (review HIGH): a FAILED checkpoint save
// ENDS the attempt immediately. The worker does not keep projecting past
// an unconfirmed boundary, and the returned persist target is the last
// position whose save CONFIRMED (this attempt's successful saves plus the
// atomic DLQ advances) — never a position whose save failed. Run then
// reconnects and resumes from the PERSISTED checkpoint, so the
// projected-but-uncheckpointed window is bounded to one batch and a
// failed save can never be silently superseded by a later terminal flush.
func (w *Worker) receiveLoop(ctx context.Context, r *subscriptionRunner, startPos uint64) attemptEnd {
	lastCommit := startPos
	r.lastCommit.Store(startPos)
	// persisted is the highest position CONFIRMED by storage in this
	// attempt (the loaded checkpoint, successful batch saves, and atomic
	// DLQ advances). It is the fallback persist target on flush failure.
	persisted := startPos
	processed := 0
	for {
		select {
		case <-ctx.Done():
			return attemptEnd{persist: lastCommit, err: ctx.Err()}
		case sel := <-r.events:
			if sel == nil || sel.SubscriptionDropped != nil {
				if sel != nil && sel.SubscriptionDropped != nil && sel.SubscriptionDropped.Error != nil {
					return attemptEnd{persist: lastCommit, err: fmt.Errorf("subscription dropped %s: %w", w.config.Name, sel.SubscriptionDropped.Error)}
				}
				return attemptEnd{persist: lastCommit, err: nil}
			}
			if sel.EventAppeared == nil || sel.EventAppeared.Event == nil {
				// CheckPointReached / CaughtUp / FellBehind notifications.
				// CaughtUp arms/disarms the stall watchdog: once the
				// subscription has reached the head, silence is normal on
				// an idle $all; while still catching up, silence means a
				// wedged read.
				if sel.CaughtUp != nil {
					r.caughtUp.Store(true)
				}
				if sel.FellBehind != nil {
					r.caughtUp.Store(false)
				}
				// The worker owns its own checkpointing; continue.
				continue
			}
			ev := sel.EventAppeared.Event

			// The watchdog skips evaluation while a projector (or poison
			// handler) is in flight: a slow-but-healthy sink must never
			// look like a stalled read. Only a pump stuck in Recv — no
			// delivery at all — is a stall candidate.
			r.processing.Store(true)

			if w.matchesPrefix(ev.StreamID) {
				if perr := w.projectWithRetry(ctx, ev); perr != nil {
					// All retries exhausted: route to DLQ atomically with
					// checkpoint advance. We do NOT silently advance past
					// the event without recording the failure.
					if err := w.handlePoison(ctx, ev, perr); err != nil {
						// DLQ write failed (Postgres down, etc.). We
						// cannot safely advance — that would lose the
						// event. Stop the subscription so it resumes from
						// lastCommit on restart.
						metrics.ProjectionEvents.WithLabelValues(w.config.Name, "failure").Inc()
						slog.Error("projection: DLQ write failed; stopping subscription to preserve event",
							"name", w.config.Name,
							"event", ev.EventType,
							"stream", ev.StreamID,
							"event_id", ev.EventID,
							"err", err)
						return attemptEnd{persist: lastCommit, err: err}
					}
					// DLQ write succeeded and the checkpoint advanced past
					// ev atomically; the metric reflects that persisted
					// advance.
					lastCommit = ev.Position.Commit
					persisted = lastCommit
					w.setCheckpointMetric(lastCommit)
					processed = 0
					r.lastCommit.Store(lastCommit)
					r.lastActivity.Store(time.Now().UnixNano())
					r.lastEvent.Store(time.Now().UnixNano())
					r.processing.Store(false)
					continue
				}

				metrics.ProjectionEvents.WithLabelValues(w.config.Name, "success").Inc()
			}

			// Every event — matching or not — advances the checkpoint:
			// with an unfiltered subscription the projection must never
			// re-read what it already passed.
			lastCommit = ev.Position.Commit
			processed++
			if processed >= w.config.BatchSize {
				if err := w.flushCheckpoint(ctx, lastCommit); err != nil {
					if ctx.Err() != nil {
						// Shutdown raced the flush: the failure is a
						// cancellation artifact, NOT a store failure.
						// End through the shutdown path — the terminal
						// flush (detached, bounded) persists lastCommit.
						return attemptEnd{persist: lastCommit, err: ctx.Err()}
					}
					// The save FAILED on a live context: the durable
					// boundary did not move. End the attempt now; the
					// terminal flush may only persist `persisted` (the
					// last CONFIRMED position), and Run reconnects from
					// the persisted checkpoint. Continuing here would
					// grow the projected-but-uncheckpointed window
					// without bound and let a later terminal flush
					// supersede this failed boundary.
					slog.Error("projection: checkpoint save failed; stopping subscription to resume from the persisted checkpoint",
						"name", w.config.Name,
						"commit", lastCommit,
						"err", err)
					return attemptEnd{persist: persisted, err: fmt.Errorf("checkpoint save %s at %d: %w", w.config.Name, lastCommit, err)}
				}
				persisted = lastCommit
				processed = 0
			}
			r.lastCommit.Store(lastCommit)
			r.lastActivity.Store(time.Now().UnixNano())
			r.lastEvent.Store(time.Now().UnixNano())
			r.processing.Store(false)
		}
	}
}

// subscribe opens a subscription with a BOUNDED establishment phase (see
// the package doc): SubscribeToAll runs on a goroutine raced against the
// establishment timeout. The returned handle owns the establishment
// context's cancel, which the runner releases after the stream closes —
// a deadline context cannot be used here because it would later kill the
// healthy live stream.
//
// Bounded-accumulation semantics (review MEDIUM): Go cannot kill a stuck
// call, so this function makes a stronger, HONEST guarantee instead of
// "no leaks": at most ONE establishment call (plus at most one
// result-drain goroutine, only if shutdown races it) is ever unresolved.
// On timeout the establishment context is cancelled and subscribe BLOCKS
// until the call returns: a well-behaved client (the pinned client — gRPC
// respects cancellation) returns within milliseconds and the retry loop
// proceeds; a client that ignores cancellation WEDGES this worker — no
// further subscribe attempts are issued, the state reports down with the
// timeout error, and the single late result (if it ever arrives) is
// closed synchronously. There is NO per-retry drain goroutine.
func (w *Worker) subscribe(ctx context.Context, pos uint64) (*subscriptionHandle, error) {
	estCtx, cancel := context.WithCancel(ctx)

	opts := kurrentdb.SubscribeToAllOptions{
		From: kurrentdb.Position{Commit: pos, Prepare: pos},
		// NOTE: deliberately NO server-side filter. The filtered-read path
		// (FilteredReadAllEventsForward) on KurrentDB 26.0.3 fails silently
		// once tombstoned streams exist (InvalidReadException -> a live
		// subscription that never delivers). The worker reads the full $all
		// and applies config.Prefixes in-process (receiveLoop -> matchesPrefix),
		// which also keeps the checkpoint advancing past non-matching events.
	}

	type result = subscribeResult
	ch := make(chan result, 1)
	go func() {
		s, err := w.subscribeF(w.client, estCtx, opts)
		ch <- result{stream: s, err: err}
	}()

	timer := time.NewTimer(w.config.EstablishmentTimeout)
	defer timer.Stop()

	select {
	case r := <-ch:
		if r.err != nil {
			cancel()
			return nil, fmt.Errorf("subscribe %s: %w", w.config.Name, r.err)
		}
		return &subscriptionHandle{stream: r.stream, cancel: cancel}, nil
	case <-timer.C:
		// Timeout: abort the half-open attempt. The result channel is
		// buffered, so the attempt goroutine can always deliver and
		// exit even if nobody ever reads.
		cancel()
		timeoutErr := fmt.Errorf("subscribe %s: establishment timed out after %s",
			w.config.Name, w.config.EstablishmentTimeout)
		// Wait for the aborted call to return (bounded-accumulation
		// wait). While blocked here the worker issues NO further
		// subscribe calls — state down, readiness not ready.
		w.setState(StateDown, timeoutErr)
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
			// Shutdown raced the unresolved call: hand the single
			// result-drain to ONE goroutine and let Run exit. This is
			// the only goroutine subscribe ever spawns, so at most one
			// drain can be parked per worker — and only for a call that
			// ignores both its own context and worker shutdown.
			go drainLateSubscription(ch)
			return nil, fmt.Errorf("subscribe %s: %w (establishment call still unresolved)", w.config.Name, ctx.Err())
		}
	case <-ctx.Done():
		cancel()
		go drainLateSubscription(ch)
		return nil, fmt.Errorf("subscribe %s: %w", w.config.Name, ctx.Err())
	}
}

// drainLateSubscription consumes the single buffered result of an
// establishment attempt that outlived its caller (worker shutdown raced
// it) and closes any subscription it produced. Spawned at most once per
// unresolved attempt by subscribe's shutdown escape paths.
func drainLateSubscription(ch <-chan subscribeResult) {
	if r, ok := <-ch; ok && r.stream != nil {
		_ = r.stream.Close()
	}
}

// subscribeResult is the outcome of one establishment call.
type subscribeResult struct {
	stream recvStream
	err    error
}

// subscriptionHandle pairs a live subscription with the establishment
// context cancel that must not fire while the stream is alive.
type subscriptionHandle struct {
	stream recvStream
	cancel context.CancelFunc
}

// subscriptionRunner owns exactly one subscription's receive goroutine.
// start spawns the pump; close terminates it deterministically:
//
//  1. signal the pump to stop,
//  2. close the individual KurrentDB subscription (cancels only that
//     stream's context; the shared client is untouched),
//  3. wait (bounded) for the pump goroutine to exit — no leaks,
//  4. release the establishment context.
//
// close is idempotent and safe to call exactly once from the defer in
// runAttempt.
type subscriptionRunner struct {
	handle    *subscriptionHandle
	events    chan *kurrentdb.SubscriptionEvent
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error

	// lastActivity is the UnixNano of the most recent Recv result; the
	// stall watchdog uses it to detect a wedged server-side read while
	// catching up. lastEvent is the UnixNano of the most recent ORDINARY
	// event delivery (EventAppeared — set by receiveLoop, never by
	// notifications or subscription start): it is the "a burst may have
	// started" signal that invalidates a prior at-head observation.
	// caughtUp is the watchdog's disarm flag: set by receiveLoop from
	// CaughtUp/FellBehind notifications and by the watchdog itself when a
	// head probe verifies the subscription is at the head (an idle $all at
	// head is normal silence).
	lastActivity atomic.Int64
	lastEvent    atomic.Int64
	caughtUp     atomic.Bool
	// lastCommit mirrors the receive loop's last processed position so the
	// watchdog's head probe can compare it against the $all head.
	lastCommit atomic.Uint64
	// processing is true while the receive loop is inside ProjectFunc (or
	// the poison handler); the watchdog skips while it is set, so a slow
	// but healthy sink is never mistaken for a stalled read.
	processing atomic.Bool
}

func newSubscriptionRunner(handle *subscriptionHandle) *subscriptionRunner {
	r := &subscriptionRunner{
		handle: handle,
		events: make(chan *kurrentdb.SubscriptionEvent),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	r.lastActivity.Store(time.Now().UnixNano())
	return r
}

func (r *subscriptionRunner) start() {
	go r.pump()
}

// pump forwards Recv results until the subscription ends (Recv returns
// nil or a drop event) or the runner is closed. The events channel is
// unbuffered so projection backpressure still reaches the server, exactly
// like the previous inline-Recv loop.
func (r *subscriptionRunner) pump() {
	defer close(r.done)
	for {
		ev := r.handle.stream.Recv()
		r.lastActivity.Store(time.Now().UnixNano())
		terminal := ev == nil || ev.SubscriptionDropped != nil
		select {
		case r.events <- ev:
		case <-r.stop:
			return
		}
		if terminal {
			return
		}
	}
}

// close shuts the subscription down and waits for the pump goroutine.
// Returns an error only if the pump failed to exit in time (which per the
// pinned client should not happen — cancelling the stream context
// unblocks Recv).
func (r *subscriptionRunner) close() error {
	r.closeOnce.Do(func() {
		close(r.stop)
		r.closeErr = r.handle.stream.Close()
		select {
		case <-r.done:
		case <-time.After(pumpExitTimeout):
			r.closeErr = fmt.Errorf("projection: receive goroutine did not exit within %s", pumpExitTimeout)
		}
		// The stream is closed and the pump has exited; the establishment
		// context can now be released.
		r.handle.cancel()
	})
	return r.closeErr
}

// matchesPrefix reports whether an event's stream belongs to this
// projection. The worker subscribes to the full $all (the server-side
// filtered-read path is unreliable once tombstoned streams exist — see
// subscribe) and applies config.Prefixes here. Non-matching events still
// advance the checkpoint (receiveLoop), so they are never re-read after a
// resubscribe; the sinks additionally no-op foreign streams as
// defense-in-depth.
func (w *Worker) matchesPrefix(streamID string) bool {
	for _, p := range w.config.Prefixes {
		if strings.HasPrefix(streamID, p) {
			return true
		}
	}
	return false
}

// watchStall forces a resubscribe when a catching-up subscription goes
// silent while undelivered events exist, and keeps watching after the head
// is verified. A KurrentDB read can wedge server-side (InvalidReadException
// behind tombstoned streams): the subscription stays live (gRPC channel
// healthy), Recv never returns, and without this the worker would report
// "running" forever while the checkpoint never advances.
//
// Silence alone cannot distinguish "idle at head" from "wedged behind
// head" (D107 — CaughtUp notifications are server-version dependent, so
// they cannot be relied on). The watchdog therefore PROBES: on a stall
// candidate it reads the $all head with a bounded, independent RPC
// (probeHeadViaClient — a different read task than the wedged
// subscription stream). Probe outcomes:
//
//   - ok && head > lastCommit: PROVEN lag — undelivered events exist.
//     Close ONLY the stream (never the runner — the stop branch can
//     deadlock the pump without delivering the terminal event); the
//     terminal event flows through r.events, the receiveLoop ends the
//     attempt, and Run resubscribes from the persisted checkpoint.
//   - ok && head <= lastCommit: healthy idle at head. The watchdog
//     DISARMS but does NOT disengage permanently: it re-probes every
//     idleRecheck (default 5m), so a wedge AT the head with later writes
//     (which the subscription then neither delivers nor announces via
//     FellBehind) is still detected the next time the head advances past
//     the last processed position.
//   - !ok (timeout / transient error): INCONCLUSIVE. A single failed
//     diagnostic read must not disrupt an otherwise usable subscription —
//     with several workers probing independently, one slow server moment
//     would churn them all. The watchdog requires maxInconclusiveProbes
//     consecutive failures before treating the read as stalled; only a
//     SUCCESSFUL probe that proves head > lastCommit is immediate proof.
//
// While the receive loop is inside ProjectFunc (processing) the watchdog
// skips: a slow but healthy sink is not a stalled read. CaughtUp
// notifications disarm (same as a probe-verified head); FellBehind
// re-arms immediately.
func (w *Worker) watchStall(ctx context.Context, r *subscriptionRunner, done <-chan struct{}) {
	interval := w.stallInterval
	if interval <= 0 {
		interval = stallCheckInterval
	}
	recheck := w.idleRecheck
	if recheck <= 0 {
		recheck = idleRecheckInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// idleSince, when non-zero, means the watchdog is disarmed: the head
	// was verified (probe) or announced (CaughtUp). inconclusive counts
	// consecutive failed probes while armed.
	var idleSince time.Time
	var inconclusive int

	closeAndReturn := func(reason string) {
		slog.Warn("projection: subscription stalled; forcing resubscribe",
			"name", w.config.Name,
			"reason", reason,
			"last_commit", r.lastCommit.Load())
		_ = r.handle.stream.Close()
	}

	// boundedProbe runs the head probe on a goroutine and waits at most
	// probeTimeout for the result. The pinned client's connection
	// acquisition (getConnectionHandle) is NOT context-cancellable (D107),
	// so during wedged client discovery the probe call itself can park
	// indefinitely; the single-flight gate (w.probePending) bounds that to
	// at most ONE parked probe goroutine per worker for the process
	// lifetime, and the watchdog loop itself never blocks (it observes
	// ctx.Done/done and keeps evaluating). A parked probe that eventually
	// returns releases the gate; its result lands in the buffered channel
	// and is discarded.
	boundedProbe := func(ctx context.Context) (uint64, bool) {
		if !w.probePending.CompareAndSwap(false, true) {
			return 0, false // a probe is already in flight or parked
		}
		ch := make(chan probeResult, 1)
		go func() {
			head, ok := w.probe(ctx)
			w.probePending.Store(false)
			ch <- probeResult{head: head, ok: ok}
		}()
		timeout := w.probeTimeout
		if timeout <= 0 {
			timeout = stallProbeTimeout
		}
		select {
		case res := <-ch:
			return res.head, res.ok
		case <-time.After(timeout):
			return 0, false
		case <-ctx.Done():
			return 0, false
		case <-done:
			return 0, false
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			if r.processing.Load() {
				continue
			}
			// CaughtUp disarms; FellBehind re-arms immediately.
			if r.caughtUp.Load() {
				if idleSince.IsZero() {
					idleSince = time.Now()
				}
			} else if !idleSince.IsZero() {
				idleSince = time.Time{}
				inconclusive = 0
			}

			if idleSince.IsZero() {
				// ARMED: only a probe-verified lag may close the stream.
				idle := time.Since(time.Unix(0, r.lastActivity.Load()))
				if idle <= time.Duration(w.stallTimeout.Load()) {
					continue
				}
				head, ok := boundedProbe(ctx)
				if ok && head > r.lastCommit.Load() {
					closeAndReturn("behind head: undelivered events exist")
					return
				}
				if ok {
					// Healthy idle at head: disarm, re-evaluate later.
					r.caughtUp.Store(true)
					idleSince = time.Now()
					inconclusive = 0
					continue
				}
				inconclusive++
				if inconclusive >= maxInconclusiveProbes {
					closeAndReturn("head probe failed repeatedly")
					return
				}
				continue
			}

			// DISARMED: a fresh ORDINARY event delivery invalidates the
			// prior at-head observation — a burst may have started and the
			// read may wedge before reaching the new head. Re-arm so a
			// behind-head stall is detected after stallTimeout, not after
			// idleRecheck. (Notifications and subscription start never set
			// lastEvent, so CaughtUp-driven disarm is not self-defeating.)
			if time.Since(time.Unix(0, r.lastEvent.Load())) < time.Duration(w.stallTimeout.Load()) {
				r.caughtUp.Store(false)
				idleSince = time.Time{}
				inconclusive = 0
				continue
			}
			// Periodic backed-off re-evaluation. A wedge at the head with
			// later writes would otherwise be permanent; a transient probe
			// failure must not disturb a healthy subscription, so
			// inconclusive probes just reschedule.
			if time.Since(idleSince) < recheck {
				continue
			}
			head, ok := boundedProbe(ctx)
			if ok && head > r.lastCommit.Load() {
				closeAndReturn("head advanced while disarmed: subscription missed writes")
				return
			}
			// Still at head, or inconclusive: keep watching, later.
			idleSince = time.Now()
		}
	}
}

// probe resolves the current $all head commit position with a bounded
// read. Tests inject fakes via w.probeHead; production uses
// probeHeadViaClient. The result is classified by watchStall: a successful
// probe proving head > lastCommit is the only immediate proof of lag.
func (w *Worker) probe(ctx context.Context) (uint64, bool) {
	if w.probeHead != nil {
		return w.probeHead(ctx)
	}
	return w.probeHeadViaClient(ctx)
}

// probeResult carries a head-probe outcome from the probe goroutine to the
// watchdog's bounded wait.
type probeResult struct {
	head uint64
	ok   bool
}

// probeHeadViaClient reads the newest $all event backwards from the end.
// It is an independent RPC from the subscription stream, so it answers
// even when the subscription's own server-side read task is wedged.
// Returns (headCommit, true); a healthy-but-empty log yields (0, true);
// any error yields (0, false) — INCONCLUSIVE: watchStall classifies a
// failed probe as inconclusive (repeated failures while armed act; while
// disarmed it merely reschedules). The observed wait bound is
// stallProbeTimeout via boundedProbe — the underlying call itself can
// park in the pinned client's non-cancellable connection acquisition,
// bounded at one parked probe per worker (see watchStall).
func (w *Worker) probeHeadViaClient(ctx context.Context) (uint64, bool) {
	if w.client == nil {
		return 0, false
	}
	probeCtx, cancel := context.WithTimeout(ctx, stallProbeTimeout)
	defer cancel()
	stream, err := w.client.ReadAll(probeCtx, kurrentdb.ReadAllOptions{
		Direction: kurrentdb.Backwards,
		From:      kurrentdb.End{},
	}, 1)
	if err != nil {
		return 0, false
	}
	defer stream.Close()
	ev, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return 0, true // empty $all: the head is position 0
	}
	if err != nil {
		return 0, false
	}
	if ev == nil || ev.Event == nil {
		return 0, false
	}
	return ev.Event.Position.Commit, true
}

// projectWithRetry calls w.project up to MaxAttempts times with
// exponential backoff. It returns nil on the first success; non-nil if
// every attempt failed (the caller routes the event to the DLQ).
//
// The retry loop blocks the subscription: this is intentional. We MUST
// not advance past a failed event, and we MUST not skip it silently.
// Blocking forces the failure to either clear (transient) or exhaust
// retries (permanent → DLQ).
func (w *Worker) projectWithRetry(ctx context.Context, ev *kurrentdb.RecordedEvent) error {
	// Trace each projection attempt so per-projection / per-event-type
	// latency and poison-event rates are visible. The span wraps the
	// retry loop: a single trace reflects the total time spent (including
	// backoff) rather than per-attempt fragments.
	ctx, span := tracing.Start(ctx, "projection.project")
	defer func() {
		if span != nil {
			span.End()
		}
	}()
	if span != nil {
		span.SetAttributes(
			attribute.String("projection", w.config.Name),
			attribute.String("event_type", ev.EventType),
			attribute.String("event_id", ev.EventID.String()),
		)
	}

	var lastErr error
	delay := w.retryBaseDelay
	for attempt := 1; attempt <= w.config.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			if span != nil {
				span.RecordError(err)
			}
			return err
		}
		lastErr = w.project(ctx, ev)
		if lastErr == nil {
			return nil
		}
		slog.Warn("projection: project attempt failed",
			"name", w.config.Name,
			"event", ev.EventType,
			"stream", ev.StreamID,
			"event_id", ev.EventID,
			"attempt", attempt,
			"max", w.config.MaxAttempts,
			"err", lastErr)

		if attempt < w.config.MaxAttempts {
			select {
			case <-ctx.Done():
				if span != nil {
					span.RecordError(ctx.Err())
				}
				return ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
			if delay > w.retryMaxDelay {
				delay = w.retryMaxDelay
			}
		}
	}
	// All attempts exhausted: record the final error on the span so the
	// trace surfaces the DLQ-bound failure reason alongside the metrics.
	if span != nil {
		span.RecordError(lastErr)
	}
	return lastErr
}

// handlePoison routes ev to the DLQ atomically with a checkpoint advance.
// On success, the projection's checkpoint is at ev.Position.Commit and
// the DLQ row is visible to operators.
//
// If the worker has no DLQ store (dlqs == nil), handlePoison returns the
// original error without advancing — the caller surfaces it and the
// attempt ends, stopping the subscription. This is intentional: a
// deployment that opts out of the DLQ cannot silently drop poison events.
func (w *Worker) handlePoison(ctx context.Context, ev *kurrentdb.RecordedEvent, projectErr error) error {
	if w.dlqs == nil {
		// No DLQ configured: do NOT advance. The caller will propagate
		// the error and the attempt will end, stopping the projection
		// at this position. Operators must investigate.
		return fmt.Errorf("poison event without DLQ (cannot advance): %s/%s %s: %w",
			w.config.Name, ev.StreamID, ev.EventType, projectErr)
	}

	entry := dlq.Entry{
		ProjectionName: w.config.Name,
		EventID:        ev.EventID.String(),
		StreamID:       ev.StreamID,
		StreamRevision: ev.EventNumber,
		EventType:      ev.EventType,
		Payload:        append([]byte(nil), ev.Data...),
		Metadata:       append([]byte(nil), ev.UserMetadata...),
		CommitPosition: ev.Position.Commit,
		Error:          projectErr.Error(),
		Attempts:       w.config.MaxAttempts,
	}

	if err := w.dlqs.FlushCheckpoint(ctx, w.config.Name, ev.Position.Commit, []dlq.Entry{entry}); err != nil {
		return fmt.Errorf("dlq flush %s/%s: %w", w.config.Name, ev.EventID, err)
	}

	slog.Error("projection: event routed to DLQ after exhausting retries",
		"name", w.config.Name,
		"event", ev.EventType,
		"stream", ev.StreamID,
		"event_id", ev.EventID,
		"attempts", w.config.MaxAttempts,
		"err", projectErr)
	metrics.ProjectionEvents.WithLabelValues(w.config.Name, "dlq").Inc()
	return nil
}

// flushCheckpoint persists the checkpoint (and any pending DLQ entries in
// one atomic transaction when a DLQ store is configured). The
// ProjectionCheckpointPosition metric advances ONLY after the storage
// save is confirmed — a failed save leaves the metric at the last
// persisted value so it always mirrors durable state.
func (w *Worker) flushCheckpoint(ctx context.Context, commitPosition uint64) error {
	var err error
	if w.dlqs != nil {
		err = w.dlqs.FlushCheckpoint(ctx, w.config.Name, commitPosition, nil)
	} else {
		err = w.checks.Save(ctx, w.config.Name, commitPosition)
	}
	if err != nil {
		return err
	}
	w.setCheckpointMetric(commitPosition)
	return nil
}

// flushDetached is the terminal checkpoint flush: it runs on a bounded
// context detached from the worker context, because the worker context is
// typically ALREADY cancelled when the attempt ends — flushing through it
// would always fail and silently lose the progress of the last unflushed
// batch to replay-after-restart instead of persistence.
func (w *Worker) flushDetached(commitPosition uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), w.config.FlushTimeout)
	defer cancel()
	return w.flushCheckpoint(ctx, commitPosition)
}

// setCheckpointMetric mirrors a CONFIRMED persisted commit position into
// the ProjectionCheckpointPosition gauge. Never call it speculatively.
func (w *Worker) setCheckpointMetric(commit uint64) {
	metrics.ProjectionCheckpointPosition.WithLabelValues(w.config.Name).Set(float64(commit))
}

// errPoisonEventWithoutDLQ is the sentinel logged when a worker with no
// DLQ store hits a poison event — a clear operator signal that the
// projection stopped rather than silently dropped the event.
var errPoisonEventWithoutDLQ = errors.New("projection: poison event without DLQ store; subscription stopped")

// Registry registration errors (readiness.go).
var (
	errDuplicateWorkerRegistration = errors.New("projection: worker already registered in this readiness registry")
	errRegisterNilWorker           = errors.New("projection: cannot register a nil worker")
)
