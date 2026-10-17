package projection

// lifecycle_test.go — unit coverage for the projection-resilience
// machinery that does not need a live KurrentDB:
//
//   - configuration validation and defaults (New)
//   - bounded subscription establishment (timeout aborts, cancels the
//     establishment context, retries, and closes a subscription that
//     arrives after the timeout)
//   - per-subscription runner close (receive goroutine exits — no leaks)
//   - lifecycle state transitions
//   - terminal checkpoint flush on worker-context cancellation uses a
//     bounded context that is NOT the cancelled run context (regression:
//     the old code flushed through the cancelled ctx and always lost the
//     last unflushed batch)
//   - checkpoint metric advances ONLY after confirmed storage saves
//   - resubscribe from the PERSISTED checkpoint on the SAME client after
//     an explicit drop
//   - D107-R1 review regressions: a FAILED batch save stops the attempt
//     and the terminal flush never supersedes the failed boundary; a
//     never-returning (cancellation-ignoring) subscribe call bounds
//     goroutine growth to ONE unresolved attempt.

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/checkpoint"
	"github.com/aliighahremanii/buildingblocks/infrastructure/metrics"
)

// --- fakes -----------------------------------------------------------------

// recordedSave captures one checkpoint Save call. ctxErr is the state of
// the context AT SAVE TIME — the terminal-flush regression test asserts it
// is nil even though the worker's run context is already cancelled.
type recordedSave struct {
	name   string
	pos    uint64
	ctxErr error
}

type fakeCheckpointer struct {
	mu       sync.Mutex
	pos      map[string]uint64
	saves    []recordedSave
	failSave bool
	getCalls int
	// onSave, when non-nil, is invoked after each Save attempt (outside
	// the lock) with the attempted save; tests use it to flip failSave at
	// exact moments.
	onSave func(recordedSave)
}

func newFakeCheckpointer() *fakeCheckpointer {
	return &fakeCheckpointer{pos: map[string]uint64{}}
}

func (f *fakeCheckpointer) Get(_ context.Context, name string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	return f.pos[name], nil
}

func (f *fakeCheckpointer) Save(ctx context.Context, name string, position uint64) error {
	f.mu.Lock()
	rec := recordedSave{name: name, pos: position, ctxErr: ctx.Err()}
	f.mu.Unlock()
	err := func() error {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.saves = append(f.saves, rec)
		if f.failSave {
			return errors.New("checkpoint store down")
		}
		f.pos[name] = position
		return nil
	}()
	f.mu.Lock()
	hook := f.onSave
	f.mu.Unlock()
	if hook != nil {
		hook(rec)
	}
	return err
}

func (f *fakeCheckpointer) saveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.saves)
}

func (f *fakeCheckpointer) lastSave() recordedSave {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.saves) == 0 {
		return recordedSave{}
	}
	return f.saves[len(f.saves)-1]
}

func (f *fakeCheckpointer) position(name string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pos[name]
}

func (f *fakeCheckpointer) setFailSave(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failSave = fail
}

// fakeStream mimics the pinned client's *Subscription semantics:
//   - Recv blocks until an event is fed, the stream ends (channel closed),
//     or Close is called (then it returns a SubscriptionDropped event).
//   - Close is idempotent.
type fakeStream struct {
	events    chan *kurrentdb.SubscriptionEvent
	closed    chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	closes    int
}

func newFakeStream() *fakeStream {
	return &fakeStream{
		events: make(chan *kurrentdb.SubscriptionEvent),
		closed: make(chan struct{}),
	}
}

// send feeds an event to Recv. It blocks until the pump consumes it,
// mirroring live backpressure.
func (f *fakeStream) send(ev *kurrentdb.SubscriptionEvent) {
	select {
	case f.events <- ev:
	case <-f.closed:
	}
}

// end delivers a terminal nil event (clean stream end).
func (f *fakeStream) end() {
	f.send(nil)
}

// drop delivers a terminal drop event.
func (f *fakeStream) drop(err error) {
	f.send(&kurrentdb.SubscriptionEvent{
		SubscriptionDropped: &kurrentdb.SubscriptionDropped{Error: err},
	})
}

func (f *fakeStream) Recv() *kurrentdb.SubscriptionEvent {
	select {
	case ev, ok := <-f.events:
		if !ok {
			return &kurrentdb.SubscriptionEvent{
				SubscriptionDropped: &kurrentdb.SubscriptionDropped{},
			}
		}
		return ev
	case <-f.closed:
		return &kurrentdb.SubscriptionEvent{
			SubscriptionDropped: &kurrentdb.SubscriptionDropped{
				Error: errors.New("subscription has been dropped"),
			},
		}
	}
}

func (f *fakeStream) Close() error {
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.closes++
		f.mu.Unlock()
		close(f.closed)
	})
	return nil
}

func (f *fakeStream) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes
}

// waitClosed blocks until Close has been called or fails the test.
func (f *fakeStream) waitClosed(t *testing.T, within time.Duration) bool {
	t.Helper()
	deadline := time.After(within)
	for {
		if f.closeCount() > 0 {
			return true
		}
		select {
		case <-deadline:
			return false
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func appearedEvent(commit uint64) *kurrentdb.SubscriptionEvent {
	return &kurrentdb.SubscriptionEvent{
		EventAppeared: &kurrentdb.ResolvedEvent{
			Event: &kurrentdb.RecordedEvent{
				EventType: "test.event",
				StreamID:  "t-stream",
				Position:  kurrentdb.Position{Commit: commit, Prepare: commit},
			},
		},
	}
}

// subscribeRecorder records every subscribe call the worker makes so tests
// can assert establishment options, the client identity, and call order.
type subscribeRecorder struct {
	mu      sync.Mutex
	calls   []subscribeCall
	streams []*fakeStream // streams to hand out, one per call (nil = block on ctx)
	block   int           // first N calls block until their ctx is cancelled
}

type subscribeCall struct {
	client *kurrentdb.Client
	from   uint64
	filter *kurrentdb.SubscriptionFilter // nil = unfiltered $all subscription
	ctxErr error                         // state of the establishment ctx when the call returned
}

// commitOf extracts the commit position from the AllPosition option the
// worker passed (always a concrete kurrentdb.Position in this worker).
func commitOf(from kurrentdb.AllPosition) uint64 {
	if p, ok := from.(kurrentdb.Position); ok {
		return p.Commit
	}
	return 0
}

// subscribe implements subscribeFunc: per call it either hands out the
// next scripted fakeStream or (for the first `block` calls, or when the
// script is exhausted) blocks until the establishment context is
// cancelled — emulating an establishment that never confirms.
func (r *subscribeRecorder) subscribe(client *kurrentdb.Client, ctx context.Context, opts kurrentdb.SubscribeToAllOptions) (recvStream, error) {
	r.mu.Lock()
	idx := len(r.calls)
	var stream *fakeStream
	if idx < len(r.streams) {
		stream = r.streams[idx]
	}
	shouldBlock := idx < r.block || stream == nil
	r.calls = append(r.calls, subscribeCall{client: client, from: commitOf(opts.From), filter: opts.Filter})
	r.mu.Unlock()

	if shouldBlock {
		<-ctx.Done()
		r.mu.Lock()
		r.calls[idx].ctxErr = ctx.Err()
		r.mu.Unlock()
		return nil, ctx.Err()
	}
	return stream, nil
}

func (r *subscribeRecorder) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *subscribeRecorder) call(i int) subscribeCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls[i]
}

// waitForCalls blocks until the recorder has seen at least n subscribe
// calls (the deterministic signal that a drop was processed and the
// worker resubscribed).
func (r *subscribeRecorder) waitForCalls(t *testing.T, n int, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		if r.callCount() >= n {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("subscribe calls = %d, want >= %d within %s", r.callCount(), n, within)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// newTestWorker builds a Worker with the production defaults but a fake
// checkpointer and scriptable subscribe, mirroring what New would produce
// (struct literal so no readiness-registry pollution between unit tests).
func newTestWorker(name string, checks *fakeCheckpointer, sub *subscribeRecorder, project ProjectFunc) *Worker {
	w := &Worker{
		client:         nil, // subscribeRecorder pins the identity it receives
		checks:         checks,
		config:         WorkerConfig{Name: name, Prefixes: []string{"t-"}, BatchSize: 1000, MaxAttempts: 1, EstablishmentTimeout: 20 * time.Millisecond, FlushTimeout: time.Second},
		project:        project,
		subscribeF:     sub.subscribe,
		retryBaseDelay: time.Millisecond,
		retryMaxDelay:  2 * time.Millisecond,
		reconnectDelay: 5 * time.Millisecond,
		// Watchdog effectively disabled for lifecycle tests; the dedicated
		// watchdog tests construct workers with short stallTimeout +
		// stallInterval and an injected probeHead.
		stallInterval: time.Hour,
		idleRecheck:   time.Hour,
		state:         StateIdle,
	}
	w.stallTimeout.Store(int64(time.Hour))
	return w
}

// --- configuration ---------------------------------------------------------

func TestNewValidatesConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		config WorkerConfig
		want   string
	}{
		{"empty name", WorkerConfig{Prefixes: []string{"x-"}}, "Name is required"},
		{"blank name", WorkerConfig{Name: "  ", Prefixes: []string{"x-"}}, "Name is required"},
		{"empty prefixes", WorkerConfig{Name: "x"}, "Prefixes is required"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w, err := New(nil, checkpoint.New(nil), nil, tc.config, noOpProject)
			if err == nil {
				t.Fatalf("New: want error containing %q, got nil worker", tc.want)
			}
			if w != nil {
				t.Fatalf("New: want nil worker on error, got %v", w)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New error = %q, want it to contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestNewDefaultsTimeouts(t *testing.T) {
	t.Parallel()

	w, err := New(nil, checkpoint.New(nil), nil, WorkerConfig{
		Name: "x", Prefixes: []string{"x-"},
		// Timeouts intentionally zero/negative — defaults must apply.
		EstablishmentTimeout: 0,
		FlushTimeout:         -1,
	}, noOpProject)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if w.config.EstablishmentTimeout != defaultEstablishmentTimeout {
		t.Fatalf("EstablishmentTimeout = %s, want default %s", w.config.EstablishmentTimeout, defaultEstablishmentTimeout)
	}
	if w.config.FlushTimeout != defaultFlushTimeout {
		t.Fatalf("FlushTimeout = %s, want default %s", w.config.FlushTimeout, defaultFlushTimeout)
	}
}

// --- bounded establishment -------------------------------------------------

// TestRunEstablishmentTimeoutAbortsAndRetries proves a wedged
// SubscribeToAll (never confirms) cannot hang the worker: the
// establishment timeout aborts the attempt, the establishment context is
// cancelled (the in-flight subscribe returns), and the worker retries.
func TestRunEstablishmentTimeoutAbortsAndRetries(t *testing.T) {
	t.Parallel()

	rec := &subscribeRecorder{block: 1 << 30} // every call blocks until ctx cancelled
	w := newTestWorker("bounded-establishment", newFakeCheckpointer(), rec, noOpProject)
	w.config.EstablishmentTimeout = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait for at least two attempts — the timeout must not end Run, only
	// the attempt.
	deadline := time.After(3 * time.Second)
	for {
		if rec.callCount() >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("subscribe attempts = %d, want >= 2 (timeout must abort and retry)", rec.callCount())
		case <-time.After(2 * time.Millisecond):
		}
	}

	// The aborted attempt's establishment context must have been cancelled
	// (the blocking subscribe returned through ctx.Done), otherwise each
	// attempt would leak a goroutine.
	if c := rec.call(0); c.ctxErr == nil {
		t.Fatal("first aborted attempt: establishment ctx was not cancelled")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
	if s := w.Status(); s.State != StateStopped {
		t.Fatalf("state after Run = %s, want stopped", s.State)
	}
}

// TestRunEstablishmentTimeoutClosesLateSubscription covers the race where
// the server confirms the subscription just AFTER the establishment
// timeout fired: the late result must be Closed so no server-side
// subscription leaks.
func TestRunEstablishmentTimeoutClosesLateSubscription(t *testing.T) {
	t.Parallel()

	late := newFakeStream()
	// Script: call 0 hands out `late` — but only after sleeping past the
	// establishment timeout (the wrapper below adds the delay). Later
	// calls block on script exhaustion (stream == nil).
	rec := &subscribeRecorder{streams: []*fakeStream{late}}
	delay := 15 * time.Millisecond
	inner := rec.subscribe
	w := newTestWorker("late-subscription", newFakeCheckpointer(), rec, noOpProject)
	w.config.EstablishmentTimeout = 5 * time.Millisecond
	w.subscribeF = func(client *kurrentdb.Client, ctx context.Context, opts kurrentdb.SubscribeToAllOptions) (recvStream, error) {
		time.Sleep(delay)
		return inner(client, ctx, opts)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	if !late.waitClosed(t, 3*time.Second) {
		t.Fatal("late-arriving subscription was not closed after establishment timeout")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// --- runner close / goroutine hygiene --------------------------------------

// TestRunnerCloseStopsPumpWithoutLeak proves the per-subscription boundary
// closes cleanly: Close terminates the pump goroutine (done channel
// closes) even while Recv is blocked, and Recv unblocks (the pinned client
// unblocks Recv by cancelling the stream context — the fake mirrors that
// via its closed channel).
func TestRunnerCloseStopsPumpWithoutLeak(t *testing.T) {
	t.Parallel()

	stream := newFakeStream()
	r := newSubscriptionRunner(&subscriptionHandle{stream: stream, cancel: func() {}})
	r.start()

	// Let the pump enter a blocked Recv.
	time.Sleep(10 * time.Millisecond)

	cancelled := make(chan struct{})
	go func() {
		stream.waitClosed(t, time.Second)
		close(cancelled)
	}()

	if err := r.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	select {
	case <-r.done:
		// pump exited — no leak
	case <-time.After(time.Second):
		t.Fatal("pump goroutine did not exit after close")
	}
	// Second close must be a safe no-op (idempotent).
	if err := r.close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// --- lifecycle -------------------------------------------------------------

// TestRunLifecycleTransitions drives one full cycle: connecting -> running
// -> down -> connecting -> running, and verifies the subscription-up
// metric follows the live subscription.
func TestRunLifecycleTransitions(t *testing.T) {
	t.Parallel()

	name := "lifecycle-transitions"
	s1, s2 := newFakeStream(), newFakeStream()
	rec := &subscribeRecorder{streams: []*fakeStream{s1, s2}}
	projected := make(chan uint64, 4)
	w := newTestWorker(name, newFakeCheckpointer(), rec, func(_ context.Context, ev *kurrentdb.RecordedEvent) error {
		projected <- ev.Position.Commit
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// running once subscription 1 is live.
	waitForState(t, w, StateRunning, 2*time.Second)
	if got := testutil.ToFloat64(metrics.ProjectionSubscriptionUp.WithLabelValues(name)); got != 1 {
		t.Fatalf("subscription_up = %f during running, want 1", got)
	}

	// Deliver one event, then explicitly drop subscription 1.
	s1.send(appearedEvent(10))
	select {
	case c := <-projected:
		if c != 10 {
			t.Fatalf("projected commit = %d, want 10", c)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event not projected")
	}
	s1.drop(errors.New("server closing"))

	// Worker goes down briefly, then reconnects on the second stream.
	rec.waitForCalls(t, 2, 3*time.Second)
	waitForState(t, w, StateRunning, 3*time.Second)

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if got := testutil.ToFloat64(metrics.ProjectionSubscriptionUp.WithLabelValues(name)); got != 0 {
		t.Fatalf("subscription_up = %f after stop, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.ProjectionReconnects.WithLabelValues(name)); got < 1 {
		t.Fatalf("subscription_reconnects = %f, want >= 1 after one ended attempt", got)
	}
}

func waitForState(t *testing.T, w *Worker, want State, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		if w.Status().State == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("state = %s, want %s (within %s)", w.Status().State, want, within)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// --- terminal flush --------------------------------------------------------

// TestRunTerminalFlushPersistsAfterCancelWithUncancelledContext is the
// regression test for the shutdown-progress bug: previously the terminal
// flush ran on the already-cancelled worker context and always failed,
// silently losing the progress of the last unflushed batch. Now the flush
// runs on a bounded context detached from the run context: the checkpoint
// save must succeed with a NON-cancelled context even though Run's ctx is
// cancelled, and the metric must advance to the flushed position.
func TestRunTerminalFlushPersistsAfterCancelWithUncancelledContext(t *testing.T) {
	t.Parallel()

	name := "terminal-flush-cancel"
	checks := newFakeCheckpointer()
	s := newFakeStream()
	rec := &subscribeRecorder{streams: []*fakeStream{s}}
	projected := make(chan struct{}, 1)
	w := newTestWorker(name, checks, rec, func(_ context.Context, _ *kurrentdb.RecordedEvent) error {
		projected <- struct{}{}
		return nil
	})
	w.config.BatchSize = 1000 // no batch flush — only the terminal flush persists

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	waitForState(t, w, StateRunning, 2*time.Second)

	s.send(appearedEvent(42))
	select {
	case <-projected:
	case <-time.After(2 * time.Second):
		t.Fatal("event not projected")
	}
	if checks.saveCount() != 0 {
		t.Fatalf("saves = %d before cancel, want 0 (batch flush disabled)", checks.saveCount())
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	saves := func() int { return checks.saveCount() }
	if saves() != 1 {
		t.Fatalf("terminal saves = %d, want exactly 1", saves())
	}
	got := checks.lastSave()
	if got.pos != 42 {
		t.Fatalf("terminal save position = %d, want 42 (progress must persist)", got.pos)
	}
	if got.ctxErr != nil {
		t.Fatalf("terminal save ran on a cancelled context (%v) — flush context must be detached from the run context", got.ctxErr)
	}
	if got := testutil.ToFloat64(metrics.ProjectionCheckpointPosition.WithLabelValues(name)); got != 42 {
		t.Fatalf("checkpoint metric = %f after terminal flush, want 42", got)
	}
}

// TestTerminalFlushFailureDoesNotAdvanceMetric proves the checkpoint
// metric only advances after a CONFIRMED storage save: when the terminal
// flush fails, the metric must stay at the last persisted value.
func TestTerminalFlushFailureDoesNotAdvanceMetric(t *testing.T) {
	t.Parallel()

	name := "terminal-flush-fail"
	checks := newFakeCheckpointer()
	checks.pos[name] = 7 // previously persisted position
	s := newFakeStream()
	rec := &subscribeRecorder{streams: []*fakeStream{s}}
	w := newTestWorker(name, checks, rec, noOpProject)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitForState(t, w, StateRunning, 2*time.Second)

	// Metric starts at the persisted checkpoint.
	if got := testutil.ToFloat64(metrics.ProjectionCheckpointPosition.WithLabelValues(name)); got != 7 {
		t.Fatalf("checkpoint metric = %f at start, want 7 (persisted value)", got)
	}

	s.send(appearedEvent(99))
	// Give the receive loop time to process, then make saves fail and
	// cancel so the terminal flush fails.
	time.Sleep(20 * time.Millisecond)
	checks.setFailSave(true)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	if got := testutil.ToFloat64(metrics.ProjectionCheckpointPosition.WithLabelValues(name)); got != 7 {
		t.Fatalf("checkpoint metric = %f after failed terminal flush, want 7 (never advance without confirmed save)", got)
	}
}

// TestBatchFlushMetricAdvancesOnlyAfterConfirmedSave proves the batch-flush
// path has the same metric discipline: success advances, failure does not.
func TestBatchFlushMetricAdvancesOnlyAfterConfirmedSave(t *testing.T) {
	t.Parallel()

	name := "batch-flush-metric"
	checks := newFakeCheckpointer()
	s := newFakeStream()
	rec := &subscribeRecorder{streams: []*fakeStream{s}}
	w := newTestWorker(name, checks, rec, noOpProject)
	w.config.BatchSize = 1 // flush after every event

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitForState(t, w, StateRunning, 2*time.Second)

	// Successful batch flush -> metric advances.
	s.send(appearedEvent(11))
	waitForMetric(t, name, 11, 2*time.Second)

	// Failing batch flush -> the attempt STOPS (durable-boundary
	// invariant) and the metric stays at the last confirmed save.
	checks.setFailSave(true)
	s.send(appearedEvent(22))
	time.Sleep(50 * time.Millisecond)
	if got := testutil.ToFloat64(metrics.ProjectionCheckpointPosition.WithLabelValues(name)); got != 11 {
		t.Fatalf("checkpoint metric = %f after failed batch flush, want 11", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	// The terminal flush also fails (store still down): metric unchanged.
	if got := testutil.ToFloat64(metrics.ProjectionCheckpointPosition.WithLabelValues(name)); got != 11 {
		t.Fatalf("checkpoint metric = %f after failed terminal flush, want 11", got)
	}
}

func waitForMetric(t *testing.T, name string, want float64, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		if got := testutil.ToFloat64(metrics.ProjectionCheckpointPosition.WithLabelValues(name)); got == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("checkpoint metric for %s did not reach %f within %s (now %f)",
				name, want, within, testutil.ToFloat64(metrics.ProjectionCheckpointPosition.WithLabelValues(name)))
		case <-time.After(2 * time.Millisecond):
		}
	}
}

// --- resubscribe semantics -------------------------------------------------

// TestRunResubscribesFromPersistedCheckpointAfterDrop proves the recovery
// contract: after an explicit drop, the worker re-subscribes on the SAME
// client from the PERSISTED checkpoint (not an in-memory position), i.e.
// every attempt re-reads the checkpoint store.
func TestRunResubscribesFromPersistedCheckpointAfterDrop(t *testing.T) {
	t.Parallel()

	name := "resubscribe-persisted"
	checks := newFakeCheckpointer()
	checks.pos[name] = 100 // pre-existing persisted checkpoint
	s1, s2 := newFakeStream(), newFakeStream()
	rec := &subscribeRecorder{streams: []*fakeStream{s1, s2}}
	projected := make(chan uint64, 2)
	w := newTestWorker(name, checks, rec, func(_ context.Context, ev *kurrentdb.RecordedEvent) error {
		projected <- ev.Position.Commit
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitForState(t, w, StateRunning, 2*time.Second)

	// First subscription starts from the persisted checkpoint.
	if c := rec.call(0); c.from != 100 {
		t.Fatalf("first subscribe From = %d, want 100 (persisted)", c.from)
	}

	// Project one event (commit 150), then drop. With BatchSize=1000 the
	// only persist happens in the terminal flush on drop.
	s1.send(appearedEvent(150))
	select {
	case c := <-projected:
		if c != 150 {
			t.Fatalf("projected commit = %d, want 150", c)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event not projected")
	}
	s1.drop(errors.New("explicit drop"))
	rec.waitForCalls(t, 2, 3*time.Second) // resubscribe happened (after the terminal flush)

	// The terminal flush persisted 150 BEFORE the resubscribe, and the
	// second subscribe resumed from the PERSISTED 150.
	if c := rec.call(1); c.from != 150 {
		t.Fatalf("second subscribe From = %d, want 150 (persisted by terminal flush)", c.from)
	}
	if checks.position(name) < 150 {
		t.Fatalf("persisted checkpoint = %d, want >= 150", checks.position(name))
	}

	// Same client across attempts (no client recreation — by design the
	// field is never reassigned; assert identity through the recorder).
	if c0, c1 := rec.call(0), rec.call(1); c0.client != c1.client {
		t.Fatal("subscribe used different clients across attempts (shared client must be reused)")
	}
}

// TestRunDeliversAllEventsInOrder pins event ordering through the
// receive-goroutine channel: the pump must forward events in Recv order.
func TestRunDeliversAllEventsInOrder(t *testing.T) {
	t.Parallel()

	s := newFakeStream()
	rec := &subscribeRecorder{streams: []*fakeStream{s}}
	var mu sync.Mutex
	var got []uint64
	w := newTestWorker("event-order", newFakeCheckpointer(), rec, func(_ context.Context, ev *kurrentdb.RecordedEvent) error {
		mu.Lock()
		got = append(got, ev.Position.Commit)
		mu.Unlock()
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitForState(t, w, StateRunning, 2*time.Second)

	for _, c := range []uint64{1, 2, 3, 4, 5} {
		s.send(appearedEvent(c))
	}

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 5 {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("projected %d events (%v), want 5", n, got)
			mu.Unlock()
		case <-time.After(2 * time.Millisecond):
		}
	}
	mu.Lock()
	for i, c := range []uint64{1, 2, 3, 4, 5} {
		if got[i] != c {
			t.Fatalf("event order = %v, want [1 2 3 4 5]", got)
		}
	}
	mu.Unlock()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// --- review-fix regressions (D107-R1) ---------------------------------------

// TestBatchFlushFailureStopsAttemptAndTerminalFlushDoesNotJump is the
// direct regression for the review HIGH: a batch checkpoint save at
// position 10 FAILS, the store then RECOVERS (the terminal save could
// succeed), but the persisted checkpoint must NOT jump to 10 — the failed
// durable boundary is never superseded. The attempt must also stop and
// restart from the PERSISTED checkpoint (5), not from the advanced
// in-memory position.
func TestBatchFlushFailureStopsAttemptAndTerminalFlushDoesNotJump(t *testing.T) {
	t.Parallel()

	name := "flush-fail-no-jump"
	checks := newFakeCheckpointer()
	checks.pos[name] = 5 // previously persisted durable boundary
	s1, s2 := newFakeStream(), newFakeStream()
	rec := &subscribeRecorder{streams: []*fakeStream{s1, s2}}
	projected := make(chan uint64, 2)
	w := newTestWorker(name, checks, rec, func(_ context.Context, ev *kurrentdb.RecordedEvent) error {
		projected <- ev.Position.Commit
		return nil
	})
	w.config.BatchSize = 1 // flush after every event

	// The store fails ONLY for the save at commit 10; it recovers the
	// moment that save has been attempted, so the subsequent TERMINAL
	// flush (and the retry's saves) COULD succeed.
	checks.mu.Lock()
	checks.failSave = true
	checks.onSave = func(r recordedSave) {
		if r.pos == 10 {
			checks.setFailSave(false)
		}
	}
	checks.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitForState(t, w, StateRunning, 2*time.Second)

	// Event at commit 10 projects fine; its checkpoint save FAILS.
	s1.send(appearedEvent(10))
	select {
	case c := <-projected:
		if c != 10 {
			t.Fatalf("projected commit = %d, want 10", c)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event not projected")
	}

	// The attempt must end (flush failure) and the worker resubscribe —
	// FROM THE PERSISTED CHECKPOINT 5, not from the failed 10.
	rec.waitForCalls(t, 2, 5*time.Second)
	if c := rec.call(1); c.from != 5 {
		t.Fatalf("resubscribe From = %d, want 5 (persisted checkpoint; the failed boundary must not advance it)", c.from)
	}

	// THE regression assertion: the terminal flush after the failed batch
	// save ran while the store had RECOVERED — yet the persisted position
	// must remain 5. A jump to 10 here means a later save superseded the
	// failed durable boundary.
	if got := checks.position(name); got != 5 {
		t.Fatalf("persisted checkpoint = %d, want 5 (terminal flush must not persist the failed boundary)", got)
	}
	if got := testutil.ToFloat64(metrics.ProjectionCheckpointPosition.WithLabelValues(name)); got != 5 {
		t.Fatalf("checkpoint metric = %f, want 5", got)
	}
	// The failed save at 10 must be on record exactly once, and the
	// terminal (recovered) save must have persisted 5.
	failed, terminal := 0, 0
	checks.mu.Lock()
	for _, sv := range checks.saves {
		switch sv.pos {
		case 10:
			failed++
		case 5:
			terminal++
		}
	}
	checks.mu.Unlock()
	if failed != 1 {
		t.Fatalf("saves at 10 = %d, want exactly 1 (the failed attempt)", failed)
	}
	if terminal == 0 {
		t.Fatal("no terminal save of the confirmed position 5 recorded")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// TestNeverReturningSubscribeBoundsGrowth is the direct regression for
// the review MEDIUM: an establishment call that NEVER returns and ignores
// its context must not accumulate goroutines across retries. The worker
// must issue EXACTLY ONE subscribe call and stop retrying (wedged, state
// down) until the call returns — and when it finally returns, normal
// operation resumes. This documents the honest bound (Go cannot kill a
// stuck call), not a no-leak claim.
func TestNeverReturningSubscribeBoundsGrowth(t *testing.T) {
	t.Parallel()

	name := "never-returning-subscribe"
	checks := newFakeCheckpointer()
	w := newTestWorker(name, checks, nil, noOpProject)
	w.config.EstablishmentTimeout = 5 * time.Millisecond
	w.reconnectDelay = 5 * time.Millisecond

	release := make(chan struct{})
	var calls atomic.Int32
	working := newFakeStream()
	w.subscribeF = func(_ *kurrentdb.Client, ctx context.Context, _ kurrentdb.SubscribeToAllOptions) (recvStream, error) {
		// Reference ctx so vet's unused-parameter conventions stay happy
		// while deliberately IGNORING its cancellation on the first call.
		_ = ctx
		if calls.Add(1) == 1 {
			<-release // blocks until the test releases; cancellation ignored
			return nil, errors.New("establishment returned very late")
		}
		return working, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Let the wedge establish: the timeout fires, the call does not
	// return, and retries must NOT accumulate. Sample the goroutine count
	// twice during a window long enough for dozens of would-be retries.
	deadline := time.After(3 * time.Second)
	for {
		if calls.Load() == 1 && w.Status().State == StateDown {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("wedge not established: calls=%d state=%s", calls.Load(), w.Status().State)
		case <-time.After(2 * time.Millisecond):
		}
	}
	time.Sleep(200 * time.Millisecond) // would allow ~40 retries at 5ms delay
	firstSample := runtime.NumGoroutine()
	if got := calls.Load(); got != 1 {
		t.Fatalf("subscribe calls = %d during wedge, want exactly 1 (retries must stop while one call is unresolved)", got)
	}
	time.Sleep(300 * time.Millisecond)
	secondSample := runtime.NumGoroutine()
	if got := calls.Load(); got != 1 {
		t.Fatalf("subscribe calls grew to %d during wedge, want 1", got)
	}
	// Corroborating bound: no goroutine GROWTH during the fully-wedged
	// window (slack 2 for runtime/concurrent-test noise).
	if secondSample-firstSample > 2 {
		t.Fatalf("goroutines grew during wedge: %d -> %d", firstSample, secondSample)
	}

	// Release the stuck call: subscribe returns, the worker retries, and
	// the second attempt hands out a working stream — normal operation
	// resumes and events flow again.
	close(release)
	waitForState(t, w, StateRunning, 5*time.Second)
	if got := calls.Load(); got != 2 {
		t.Fatalf("subscribe calls = %d after release, want 2 (exactly one retry)", got)
	}
	working.send(appearedEvent(77))

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel (wedge must not block shutdown)")
	}
}
