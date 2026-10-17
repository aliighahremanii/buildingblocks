package natspub

// worker_subscribe_test.go — deterministic coverage for the BOUNDED
// subscription establishment (D136), driven through the subscribeF seam
// so no live KurrentDB is needed:
//
//   - a subscribe call that parks forever (the pinned SDK's
//     non-cancellable connection acquisition / unbuffered channel send on
//     a poisoned client — D135's parked-sender mechanism) must NOT wedge
//     Run: generation cancellation still returns promptly. Before D136
//     runOnce called SubscribeToAll SYNCHRONOUSLY, and one parked call
//     during a KurrentDB outage outlasting the client's discovery budget
//     wedged the whole worker host (the supervisor's generation drain
//     never completed: no logs, metrics dark, no recovery even after the
//     server returned — the AR-210 R2 live defect).
//   - an establishment timeout aborts the half-open attempt, surfaces the
//     timeout, and Run retries — with no further subscribe calls issued
//     while the aborted one is unresolved (bounded accumulation).
//   - a LATE subscription returned after the establishment timeout is
//     closed synchronously (no server-side leak).
//   - a late result abandoned by generation cancellation is closed by the
//     single drain goroutine.
//   - a fast subscribe failure propagates unchanged (no timeout wrap).

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// fakeSubStream is an inert subscription; it records Close so the
// late-result paths can prove no server-side leak.
type fakeSubStream struct {
	mu     sync.Mutex
	closed bool
}

func (f *fakeSubStream) Recv() *kurrentdb.SubscriptionEvent { return nil }

func (f *fakeSubStream) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

func (f *fakeSubStream) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// newSubscribeTestWorker builds a Run-capable worker with the REAL
// runOnce attempt (runAttempt left nil so Run binds it) and a shortened
// establishment timeout.
func newSubscribeTestWorker(name string, establishmentTimeout time.Duration) *Worker {
	w := New(nil, &fakePositionStore{}, &fakePublisher{}, nil, WorkerConfig{
		Name:     name,
		Prefixes: []string{"country-"},
	})
	w.establishmentTimeout = establishmentTimeout
	w.initialDelay = 10 * time.Millisecond
	w.maxDelay = 20 * time.Millisecond
	return w
}

// TestRunReturnsWhenEstablishmentParksOnCancel is the deterministic repro
// of the AR-210 R2 wedge: the establishment call is parked FOREVER (a
// non-cancellable SDK call on a poisoned client). Run must still return
// promptly on generation cancellation — the property whose absence let
// the worker host's supervisor wedge in <-errCh with the metrics
// listener dark and no logs.
func TestRunReturnsWhenEstablishmentParksOnCancel(t *testing.T) {
	t.Parallel()

	w := newSubscribeTestWorker("park-on-cancel", 50*time.Millisecond)

	var calls atomic.Int32
	entered := make(chan struct{}, 1)
	w.subscribeF = func(_ *kurrentdb.Client, _ context.Context, _ kurrentdb.SubscribeToAllOptions) (subscription, error) {
		calls.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-make(chan struct{}) // parked forever — non-cancellable SDK park
		return nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()

	// The first establishment call is in flight (parked).
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the establishment call never started")
	}

	// Generation cancellation must return Run promptly DESPITE the park.
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run wedged on the parked establishment call — the D136 defect")
	}

	// Bounded accumulation: exactly ONE subscribe call was ever issued
	// (the parked attempt is abandoned to a single drain goroutine; no
	// retry pile-up).
	if got := calls.Load(); got != 1 {
		t.Fatalf("subscribe calls = %d, want exactly 1", got)
	}
}

// TestSubscribeEstablishmentTimeoutAbortsAndRetries pins the timeout
// path against a well-behaved client: the establishment context is
// cancelled at the timeout, the aborted call returns, the timeout is
// surfaced as the attempt error, and Run retries (a fresh establishment
// context per attempt).
func TestSubscribeEstablishmentTimeoutAbortsAndRetries(t *testing.T) {
	t.Parallel()

	w := newSubscribeTestWorker("est-timeout", 50*time.Millisecond)

	var calls atomic.Int32
	var sawCancelledEstCtx atomic.Bool
	w.subscribeF = func(_ *kurrentdb.Client, ctx context.Context, _ kurrentdb.SubscribeToAllOptions) (subscription, error) {
		calls.Add(1)
		<-ctx.Done() // well-behaved: abort on the establishment cancel
		if errors.Is(ctx.Err(), context.Canceled) {
			sawCancelledEstCtx.Store(true)
		}
		return nil, errors.New("aborted")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()

	// At least two attempts (timeout → error → backoff → retry).
	deadline := time.After(5 * time.Second)
	for calls.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("subscribe calls = %d, want >= 2 (timeout must be retryable)", calls.Load())
		case <-time.After(2 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if !sawCancelledEstCtx.Load() {
		t.Fatal("the establishment context was never cancelled at the timeout")
	}
}

// TestSubscribeLateStreamClosedSynchronously pins the no-leak side of the
// timeout path: a subscription that arrives AFTER the establishment
// timeout is closed synchronously before the timeout error surfaces.
func TestSubscribeLateStreamClosedSynchronously(t *testing.T) {
	t.Parallel()

	w := newSubscribeTestWorker("late-stream", 50*time.Millisecond)
	s := &fakeSubStream{}
	w.subscribeF = func(_ *kurrentdb.Client, ctx context.Context, _ kurrentdb.SubscribeToAllOptions) (subscription, error) {
		<-ctx.Done()                      // the timeout cancelled the attempt
		time.Sleep(20 * time.Millisecond) // late return after the timeout
		return s, nil
	}

	handle, err := w.subscribe(context.Background(), kurrentdb.SubscribeToAllOptions{})
	if err == nil {
		t.Fatal("expected the establishment timeout error")
	}
	if handle != nil {
		t.Fatalf("no handle may be returned on timeout, got %v", handle)
	}
	if !strings.Contains(err.Error(), "establishment timed out") {
		t.Fatalf("err = %v, want the establishment-timeout error", err)
	}
	if !s.isClosed() {
		t.Fatal("the late subscription was not closed — server-side leak")
	}
}

// TestSubscribeDrainsAbandonedResult pins the generation-cancel escape:
// when cancellation races an unresolved establishment call, subscribe
// returns immediately and the single drain goroutine closes whatever the
// call produces late.
func TestSubscribeDrainsAbandonedResult(t *testing.T) {
	t.Parallel()

	w := newSubscribeTestWorker("drain-abandoned", 5*time.Second) // long timeout: cancellation wins
	s := &fakeSubStream{}
	release := make(chan struct{})
	w.subscribeF = func(_ *kurrentdb.Client, _ context.Context, _ kurrentdb.SubscribeToAllOptions) (subscription, error) {
		<-release // ignores both contexts — the SDK park
		return s, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	type res struct {
		handle *subHandle
		err    error
	}
	resc := make(chan res, 1)
	go func() {
		h, err := w.subscribe(ctx, kurrentdb.SubscribeToAllOptions{})
		resc <- res{handle: h, err: err}
	}()

	time.Sleep(20 * time.Millisecond) // let the attempt start
	cancel()
	select {
	case r := <-resc:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("subscribe returned %v, want context.Canceled", r.err)
		}
		if r.handle != nil {
			t.Fatalf("no handle may be returned on cancellation, got %v", r.handle)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("subscribe wedged on the parked establishment call — the D136 defect")
	}

	// The abandoned call eventually delivers; the drain goroutine closes
	// its late subscription.
	close(release)
	deadline := time.After(3 * time.Second)
	for !s.isClosed() {
		select {
		case <-deadline:
			t.Fatal("the drain goroutine did not close the late subscription")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestSubscribeFastFailurePropagates pins the non-timeout path: a client
// that fails the establishment immediately surfaces its error unchanged
// (no timeout wrap, no establishment-context leak — the cancel path
// releases it).
func TestSubscribeFastFailurePropagates(t *testing.T) {
	t.Parallel()

	w := newSubscribeTestWorker("fast-fail", 5*time.Second)
	boom := errors.New("connection refused")
	w.subscribeF = func(_ *kurrentdb.Client, _ context.Context, _ kurrentdb.SubscribeToAllOptions) (subscription, error) {
		return nil, boom
	}

	handle, err := w.subscribe(context.Background(), kurrentdb.SubscribeToAllOptions{})
	if !errors.Is(err, boom) {
		t.Fatalf("subscribe returned %v, want the underlying error", err)
	}
	if handle != nil {
		t.Fatalf("no handle may be returned on failure, got %v", handle)
	}
}
