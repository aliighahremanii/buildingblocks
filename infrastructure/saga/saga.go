// Package saga provides a general-purpose saga framework that drives
// long-running, multi-step workflows from KurrentDB domain events.
//
// A saga is an event-driven state machine whose durable state survives
// worker restarts. The [Coordinator] subscribes to the KurrentDB $all
// stream and dispatches each domain event to every registered [Saga]
// that subscribed to that event type. The saga implementation loads (or
// creates) its [Instance] from a [Store], advances the state, and saves
// it back with optimistic-concurrency protection (the Save upsert fails
// when the stored version no longer matches the loaded version).
//
// Sagas MUST be idempotent: because the Coordinator resumes from the
// start of the stream on every restart, an event may be redelivered. A
// saga that is already in (or past) the state the event would transition
// it to MUST treat the event as a no-op. This is the saga equivalent of
// a projection's idempotent upsert.
//
// Durability: every transition is persisted to the sagas table before
// the Coordinator moves on, so a crash between events loses at most one
// in-flight React — and only if it had not yet called Save. The next
// delivery (after restart) re-runs React, which is idempotent.
//
// Timeouts: a saga can schedule a reminder via Store.ScheduleTimeout.
// Coordinator.RunTimeoutSweep polls the due rows and fires the saga's
// Timeout callback, then deletes the row. This is how a saga implements
// "if X does not happen within Y, do Z".
package saga

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// Status is the lifecycle state of a saga instance.
type Status int

const (
	// StatusPending is the initial state of a freshly created instance
	// that has not yet started running. Most sagas move straight to
	// StatusRunning on their triggering event.
	StatusPending Status = iota
	// StatusRunning means the saga is actively waiting for further events
	// or a timeout to complete its workflow.
	StatusRunning
	// StatusCompleted is terminal: the workflow succeeded. Further events
	// for this instance are idempotent no-ops.
	StatusCompleted
	// StatusFailed is terminal: the workflow could not complete. A human
	// or an out-of-band process must intervene.
	StatusFailed
)

// String returns the lower-case status name stored in the sagas table
// (status column). It is the single source of truth for the on-disk
// representation; the Postgres store round-trips through String/Parse.
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusRunning:
		return "running"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// ParseStatus decodes a status string produced by Status.String. An
// unrecognized value yields StatusPending and ok=false so callers can
// surface unknown-state rows rather than silently downgrading them.
func ParseStatus(s string) (Status, bool) {
	switch s {
	case "pending":
		return StatusPending, true
	case "running":
		return StatusRunning, true
	case "completed":
		return StatusCompleted, true
	case "failed":
		return StatusFailed, true
	default:
		return StatusPending, false
	}
}

// Instance is the durable state of one saga execution. State is opaque
// JSONB owned by the saga implementation; the framework never inspects
// it. Version is the optimistic-concurrency guard: Save(Instance) succeeds
// only when the stored version equals Version-1 (for an existing row) or
// the row does not exist (for Version == 0).
type Instance struct {
	ID            string
	Type          string
	Status        Status
	State         []byte
	CorrelationID string
	CausationID   string
	Version       int64
}

// ErrConcurrencyConflict is returned by Store.Save when the stored
// version no longer matches the version the caller loaded. This means
// another worker transitioned the saga concurrently; the caller should
// reload and retry (React is idempotent, so retrying is safe).
var ErrConcurrencyConflict = errors.New("saga: concurrency conflict")

// Store is the application-layer port for saga persistence. The
// Postgres implementation lives at
// internal/buildingblocks/infrastructure/postgres/saga.
//
// All methods MUST be safe for concurrent use across goroutines (the
// Coordinator's React dispatch and timeout sweep run concurrently).
type Store interface {
	// Load returns the instance for sagaID. found is false (with nil
	// error) when no such instance exists — the caller treats this as
	// "create a new instance".
	Load(ctx context.Context, sagaID string) (in Instance, found bool, err error)

	// Save upserts the instance with optimistic-concurrency protection.
	// For an existing instance (Version > 0) it succeeds only when the
	// stored version equals Version-1, otherwise it returns
	// ErrConcurrencyConflict. For a new instance (Version == 0) it
	// inserts; if the row already exists it returns
	// ErrConcurrencyConflict. On success the stored version is the
	// Instance.Version the caller supplied.
	Save(ctx context.Context, in Instance) error

	// ScheduleTimeout arms (or re-arms) a reminder of the given kind for
	// sagaID, firing at dueAt. Re-scheduling the same (sagaID, kind)
	// overwrites due_at, so this is idempotent.
	ScheduleTimeout(ctx context.Context, sagaID, kind string, dueAt time.Time) error

	// ListDueTimeouts returns up to limit saga IDs whose due_at has
	// passed, ordered by due_at ascending. The Coordinator's sweep uses
	// it to drive Timeout callbacks.
	ListDueTimeouts(ctx context.Context, before time.Time, limit int) ([]string, error)

	// DeleteTimeout removes the (sagaID, kind) reminder row. It is a
	// no-op (nil error) when the row does not exist.
	DeleteTimeout(ctx context.Context, sagaID, kind string) error
}

// Saga is the interface every saga implementation satisfies. The type
// parameter T is a phantom carried so an implementation can advertise
// its concrete state type; none of the methods reference T, so a single
// concrete type satisfies Saga[any] regardless of its T. The
// Coordinator registers sagas as Saga[any].
//
// React and Timeout own their Store interactions: they Load/Create the
// relevant Instance, advance state, and Save. This keeps the framework
// oblivious to how a saga derives its id from an event.
type Saga[T any] interface {
	// React handles a single domain event from the $all stream. It MUST
	// be idempotent: an event redelivered after a restart or a timeout
	// replay must not corrupt state or double-fire side effects. Returning
	// an error logs the failure; the Coordinator does NOT advance past
	// the event in a way that would lose it (re-delivery will retry).
	React(ctx context.Context, event *kurrentdb.RecordedEvent) error

	// Timeout handles a fired reminder. The Coordinator loads the
	// instance and attaches it to ctx via WithInstance before calling
	// Timeout, so the implementation can recover its state without a
	// separate Load. Returning an error leaves the timeout row in place
	// (the sweep logs and tries again next cycle) only if the caller does
	// not delete it; the sweep always deletes the row after a successful
	// (nil-error) Timeout.
	Timeout(ctx context.Context, kind string) error

	// SagaType returns the stable type discriminator stored in
	// sagas.saga_type and used by the timeout sweep to dispatch to the
	// right implementation. It should be module-namespaced
	// (e.g. "sample1.processpending").
	SagaType() string
}

// instanceKey is the context key used to hand the Coordinator-loaded
// Instance to a saga's Timeout callback without widening the Timeout
// signature.
type instanceKey struct{}

// WithInstance attaches in to ctx so a saga's Timeout can recover the
// instance the sweep loaded on its behalf. React does not need this —
// it derives its id from the event and Load/Save directly.
func WithInstance(ctx context.Context, in Instance) context.Context {
	return context.WithValue(ctx, instanceKey{}, in)
}

// InstanceFromContext recovers an Instance attached by WithInstance.
// ok is false when no instance is attached (e.g. Timeout called outside
// the Coordinator's sweep).
func InstanceFromContext(ctx context.Context) (Instance, bool) {
	in, ok := ctx.Value(instanceKey{}).(Instance)
	return in, ok
}

const (
	defaultReconnectDelay = 2 * time.Second
	defaultSweepInterval  = 30 * time.Second
	defaultSweepLimit     = 100
)

// registeredSaga is one entry in the Coordinator's registry.
type registeredSaga struct {
	saga       Saga[any]
	eventTypes map[string]struct{}
}

// Coordinator drives a set of registered sagas from the KurrentDB $all
// stream and runs the periodic timeout sweep. Construct it with
// NewCoordinator, Register each saga, then run Run and RunTimeoutSweep
// as concurrent goroutines (typically both in the worker process).
//
// The zero value is NOT usable; always use NewCoordinator.
type Coordinator struct {
	client         *kurrentdb.Client
	store          Store
	clock          func() time.Time
	retryBaseDelay time.Duration

	mu    sync.RWMutex
	sagas []registeredSaga
}

// NewCoordinator constructs a Coordinator. client may be nil in tests
// that exercise only dispatch/sweep seams directly. clock defaults to
// time.Now when nil. retryBaseDelay bounds the backoff between failed
// subscription reconnects; <=0 falls back to defaultReconnectDelay.
func NewCoordinator(client *kurrentdb.Client, store Store, clock func() time.Time, retryBaseDelay time.Duration) *Coordinator {
	if clock == nil {
		clock = time.Now
	}
	if retryBaseDelay <= 0 {
		retryBaseDelay = defaultReconnectDelay
	}
	return &Coordinator{
		client:         client,
		store:          store,
		clock:          clock,
		retryBaseDelay: retryBaseDelay,
	}
}

// Register adds saga to the dispatch set for the given event types. An
// event of a registered type is delivered to every saga that subscribed
// to it, in registration order. Register is safe to call before Run
// starts; calling it concurrently with Run is not supported (sagas are
// registered once at startup).
func (c *Coordinator) Register(saga Saga[any], eventTypes []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := make(map[string]struct{}, len(eventTypes))
	for _, et := range eventTypes {
		m[et] = struct{}{}
	}
	c.sagas = append(c.sagas, registeredSaga{saga: saga, eventTypes: m})
}

// matches returns the sagas subscribed to eventType. It takes the read
// lock so it is safe to call concurrently with Register (though in
// practice Register runs only at startup).
func (c *Coordinator) matches(eventType string) []Saga[any] {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var out []Saga[any]
	for _, rs := range c.sagas {
		if _, ok := rs.eventTypes[eventType]; ok {
			out = append(out, rs.saga)
		}
	}
	return out
}

// sagaForType returns the registered saga whose SagaType() matches t, or
// nil. Used by the timeout sweep to dispatch to the right implementation.
func (c *Coordinator) sagaForType(t string) Saga[any] {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, rs := range c.sagas {
		if rs.saga.SagaType() == t {
			return rs.saga
		}
	}
	return nil
}

// dispatch is the per-event fan-out: it delivers event to every saga
// subscribed to event.EventType. It is the testable seam beneath Run —
// tests call it directly to avoid needing a live KurrentDB subscription.
//
// System events (streams beginning with "$", e.g. "$stats" and
// projection internals) are skipped: sagas react to domain events only.
func (c *Coordinator) dispatch(ctx context.Context, event *kurrentdb.RecordedEvent) {
	if event == nil {
		return
	}
	if strings.HasPrefix(event.StreamID, "$") {
		return
	}
	for _, s := range c.matches(event.EventType) {
		if err := s.React(ctx, event); err != nil {
			slog.Error("saga: React failed",
				"saga_type", s.SagaType(),
				"event", event.EventType,
				"stream", event.StreamID,
				"err", err)
		}
	}
}

// Run blocks until ctx is cancelled, maintaining a live subscription to
// the KurrentDB $all stream. Each domain event is handed to dispatch.
//
// The subscription is unfiltered (sagas may react to any event type);
// event-type filtering happens in dispatch against each saga's
// registered types. Subscription drops are logged and retried after a
// short delay.
//
// Resumption: the subscription starts from the beginning of the stream
// on every (re)connect. This is safe because sagas are idempotent — an
// already-completed instance treats its triggering event as a no-op.
// The saga instances themselves (and the sagas table) are the durable
// progress marker; no separate checkpoint is required.
func (c *Coordinator) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("saga: subscription ended, reconnecting", "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.retryBaseDelay):
		}
	}
}

// runOnce opens a subscription and dispatches events until the stream
// drops. It returns the drop cause (nil for a clean end).
func (c *Coordinator) runOnce(ctx context.Context) error {
	if c.client == nil {
		// No client (typical in tests that drive dispatch directly).
		// Block until cancelled so Run behaves as a long-lived loop.
		<-ctx.Done()
		return ctx.Err()
	}
	// Start from the beginning of $all. Sagas are idempotent, so
	// redelivered events are no-ops against already-completed instances.
	// No Filter — sagas may react to any event type; system events
	// ($-prefixed) are skipped in dispatch.
	opts := kurrentdb.SubscribeToAllOptions{
		From: kurrentdb.Start{},
	}
	stream, err := c.client.SubscribeToAll(ctx, opts)
	if err != nil {
		return fmt.Errorf("saga: subscribe to $all: %w", err)
	}
	defer stream.Close()

	for {
		sel := stream.Recv()
		if sel == nil {
			return nil
		}
		switch {
		case sel.EventAppeared != nil:
			c.dispatch(ctx, sel.EventAppeared.Event)
		case sel.SubscriptionDropped != nil:
			if derr := sel.SubscriptionDropped.Error; derr != nil {
				return fmt.Errorf("saga: subscription dropped: %w", derr)
			}
			return nil
		}
	}
}

// RunTimeoutSweep blocks until ctx is cancelled, polling ListDueTimeouts
// every interval and firing each due saga's Timeout callback. interval
// <=0 falls back to defaultSweepInterval. The sweep is designed to run
// as a goroutine alongside Run; the two share the Store but do not
// otherwise coordinate (the saga's own React/Timeout implementations
// are responsible for tolerating concurrent transitions via the Store's
// optimistic-concurrency guard).
func (c *Coordinator) RunTimeoutSweep(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = defaultSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.sweepTimeouts(ctx); err != nil {
				slog.Error("saga: timeout sweep failed", "err", err)
			}
		}
	}
}

// sweepTimeouts is one sweep pass: list due reminders, fire each saga's
// Timeout, and delete the reminder row on success. It is the testable
// seam beneath RunTimeoutSweep.
func (c *Coordinator) sweepTimeouts(ctx context.Context) error {
	ids, err := c.store.ListDueTimeouts(ctx, c.clock(), defaultSweepLimit)
	if err != nil {
		return fmt.Errorf("list due timeouts: %w", err)
	}
	for _, sagaID := range ids {
		if err := c.fireTimeout(ctx, sagaID); err != nil {
			slog.Error("saga: fire timeout failed",
				"saga_id", sagaID, "err", err)
		}
	}
	return nil
}

// fireTimeout loads the instance for sagaID, dispatches to the matching
// saga's Timeout, and deletes the reminder row. A missing instance or an
// unknown saga type still deletes the row so a stale reminder cannot
// wedge the sweep.
func (c *Coordinator) fireTimeout(ctx context.Context, sagaID string) error {
	inst, found, err := c.store.Load(ctx, sagaID)
	if err != nil {
		return fmt.Errorf("load %s: %w", sagaID, err)
	}
	if !found {
		// Stale reminder for a saga that no longer exists (purged table,
		// manual cleanup, etc.). Drop the row so the sweep can advance.
		_ = c.store.DeleteTimeout(ctx, sagaID, "")
		return nil
	}

	s := c.sagaForType(inst.Type)
	if s == nil {
		// No registered saga owns this type (e.g. the saga was removed in
		// a deploy but old rows remain). Drop the row rather than retry
		// forever.
		_ = c.store.DeleteTimeout(ctx, sagaID, "")
		return nil
	}

	// Hand the loaded instance to Timeout via context so the
	// implementation can recover its state without a second Load.
	if err := s.Timeout(WithInstance(ctx, inst), ""); err != nil {
		return err
	}

	if err := c.store.DeleteTimeout(ctx, sagaID, ""); err != nil {
		return fmt.Errorf("delete timeout %s: %w", sagaID, err)
	}
	return nil
}
