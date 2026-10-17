package eventstoredb

// clientmanager_test.go — coverage for the host-owned client generation
// manager (D107-R2):
//
//   - unit: singleflight Swap (concurrent callers → exactly one dial),
//     min-swap-interval backoff, terminal-signal lifecycle (idempotent,
//     reset by a successful swap), nil-client safety
//   - live (KURRENTDB_URL): the acceptance-critical path — a REAL client
//     terminally poisoned against a closed port is replaced by Swap, the
//     new generation works against the live server, and the old
//     generation stays terminally closed. This is the deterministic
//     in-process proxy for the E2E volume-backed-restart defect.

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// deadPortConfig returns a config pointing at a guaranteed-closed local
// port (immediate TCP RST — the fast-poison condition from D107).
func deadPortConfig(t *testing.T) Config {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("close reserved port: %v", err)
	}
	return Config{URL: "kurrentdb://127.0.0.1:" + itoa(port) + "?tls=false"}
}

func itoa(p int) string {
	if p == 0 {
		return "0"
	}
	var b []byte
	for p > 0 {
		b = append([]byte{byte('0' + p%10)}, b...)
		p /= 10
	}
	return string(b)
}

func TestClientManagerSwapSingleflightAndBackoff(t *testing.T) {
	t.Parallel()

	// Deterministic coalescing: the dial BLOCKS on a gate. Caller 1 enters
	// the dial; while it is blocked, callers 2..N must coalesce onto it —
	// exactly one dial, everyone observes the same generation.
	var dials atomic.Int32
	genA, genB := &kurrentdb.Client{}, &kurrentdb.Client{}
	dialEntered := make(chan struct{})
	releaseDial := make(chan struct{})
	mgr := NewClientManager(Config{URL: "kurrentdb://unused"}, genA,
		WithDialFunc(func(context.Context, Config) (*kurrentdb.Client, error) {
			if dials.Add(1) == 1 {
				close(dialEntered)
				<-releaseDial
			}
			return genB, nil
		}),
		WithMinSwapInterval(0),
		WithCloseFunc(func(*kurrentdb.Client) {}),
	)

	if mgr.Client() != genA {
		t.Fatal("initial generation not served")
	}

	const callers = 20
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, callers)
	first := make(chan error, 1)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			c, err := mgr.Swap(context.Background())
			if err == nil && c != genB {
				err = errWrongGeneration
			}
			if i == 0 {
				first <- err
			}
			errs <- err
		}(i)
	}
	close(start)

	// Wait until the winning dial is in flight. Because the dial BLOCKS,
	// no swap can complete until released — every other caller must
	// deterministically coalesce on it (a new dial is only possible after
	// the winner finishes, which requires the gate).
	select {
	case <-dialEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("first swap never reached the dial")
	}
	time.Sleep(100 * time.Millisecond) // grace for all callers to reach Swap
	close(releaseDial)

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("coalesced Swap: %v", err)
		}
	}
	if err := <-first; err != nil {
		t.Fatalf("winning Swap: %v", err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials = %d, want exactly 1 (singleflight)", got)
	}
	if mgr.Client() != genB || mgr.Swaps() != 1 {
		t.Fatalf("post-swap state: client-different=%v swaps=%d", mgr.Client() != genB, mgr.Swaps())
	}

	// A swap that fails does not advance the generation, and its coalesced
	// callers observe the same error.
	failDial := atomic.Bool{}
	failDial.Store(true)
	mgr2 := NewClientManager(Config{URL: "kurrentdb://unused"}, genA,
		WithDialFunc(func(context.Context, Config) (*kurrentdb.Client, error) {
			if failDial.Load() {
				return nil, errDialFailed
			}
			return genB, nil
		}),
		WithMinSwapInterval(0),
		WithCloseFunc(func(*kurrentdb.Client) {}),
	)
	if _, err := mgr2.Swap(context.Background()); err == nil {
		t.Fatal("failed dial returned nil error")
	}
	if mgr2.Client() != genA {
		t.Fatal("failed swap replaced the generation")
	}
	if mgr2.Swaps() != 0 {
		t.Fatalf("failed swap counted: %d", mgr2.Swaps())
	}
}

var errWrongGeneration = errString("clientmanager: coalesced swap observed a different generation")
var errDialFailed = errString("clientmanager: dial failed")

type errString string

func (e errString) Error() string { return string(e) }

func TestClientManagerMinSwapIntervalSpacing(t *testing.T) {
	t.Parallel()

	genA, genB, genC := &kurrentdb.Client{}, &kurrentdb.Client{}, &kurrentdb.Client{}
	next := genB
	mgr := NewClientManager(Config{URL: "kurrentdb://unused"}, genA,
		WithDialFunc(func(context.Context, Config) (*kurrentdb.Client, error) {
			c := next
			if c == genB {
				next = genC
			}
			return c, nil
		}),
		WithMinSwapInterval(120*time.Millisecond),
		WithCloseFunc(func(*kurrentdb.Client) {}),
	)

	// First swap: immediate (no previous swap to space from).
	first := time.Now()
	if _, err := mgr.Swap(context.Background()); err != nil {
		t.Fatalf("first swap: %v", err)
	}
	if elapsed := time.Since(first); elapsed > 50*time.Millisecond {
		t.Fatalf("first swap waited %s; min interval must not delay the first swap", elapsed)
	}

	// Second swap: waits out the remainder of the min interval.
	second := time.Now()
	if _, err := mgr.Swap(context.Background()); err != nil {
		t.Fatalf("second swap: %v", err)
	}
	if elapsed := time.Since(second); elapsed < 100*time.Millisecond {
		t.Fatalf("second swap returned after %s; min-swap-interval backoff not honored", elapsed)
	}
	if mgr.Client() != genC || mgr.Swaps() != 2 {
		t.Fatalf("state after two swaps: swaps=%d", mgr.Swaps())
	}
}

func TestClientManagerTerminalSignalLifecycle(t *testing.T) {
	t.Parallel()

	genA := &kurrentdb.Client{}
	mgr := NewClientManager(Config{URL: "kurrentdb://unused"}, genA,
		WithDialFunc(func(context.Context, Config) (*kurrentdb.Client, error) {
			return &kurrentdb.Client{}, nil
		}),
		WithMinSwapInterval(0),
		WithCloseFunc(func(*kurrentdb.Client) {}),
	)

	// Signal is idempotent per generation.
	mgr.SignalTerminal()
	mgr.SignalTerminal()
	select {
	case <-mgr.TerminalCh():
	default:
		t.Fatal("terminal channel not closed after SignalTerminal")
	}

	// A successful swap arms a FRESH signal for the new generation.
	if _, err := mgr.Swap(context.Background()); err != nil {
		t.Fatalf("swap: %v", err)
	}
	select {
	case <-mgr.TerminalCh():
		t.Fatal("terminal channel still closed after successful swap")
	default:
	}
	mgr.SignalTerminal()
	select {
	case <-mgr.TerminalCh():
	default:
		t.Fatal("re-arming after swap failed")
	}

	// Nil client (event store disabled): signalling is a no-op and the
	// channel never closes — the supervisor never fires.
	nilMgr := NewClientManager(Config{}, nil)
	nilMgr.SignalTerminal()
	select {
	case <-nilMgr.TerminalCh():
		t.Fatal("nil-client manager signalled terminal")
	default:
	}
}

// TestClientManagerMonitorSignalsTerminalWithBackoff drives Monitor with
// an injectable check (the pinned client's terminal error is not
// constructible — unexported fields — so the check is scripted) and
// verifies: healthy → terminal → healthy transitions signal exactly once
// per generation, and the probe interval backs off while terminal
// persists (observed via check counts) and resets on recovery.
func TestClientManagerMonitorSignalsTerminalWithBackoff(t *testing.T) {
	t.Parallel()

	genA, genB := &kurrentdb.Client{}, &kurrentdb.Client{}
	terminal := atomic.Bool{}
	swapped := make(chan struct{})
	var checks atomic.Int32

	mgr := NewClientManager(Config{URL: "kurrentdb://unused"}, genA,
		WithDialFunc(func(context.Context, Config) (*kurrentdb.Client, error) {
			return genB, nil
		}),
		WithCheckFunc(func(_ context.Context, c *kurrentdb.Client) bool {
			checks.Add(1)
			return c == genA && terminal.Load()
		}),
		WithMinSwapInterval(0),
		WithCloseFunc(func(*kurrentdb.Client) {}),
	)
	_ = swapped

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Monitor(ctx, 10*time.Millisecond)

	// Healthy phase: checks run at base speed, no signal.
	time.Sleep(60 * time.Millisecond)
	select {
	case <-mgr.TerminalCh():
		t.Fatal("terminal signalled while healthy")
	default:
	}

	// Terminal phase: signal fires (exactly once for the generation).
	terminal.Store(true)
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-mgr.TerminalCh():
			goto signalled
		case <-deadline:
			t.Fatal("monitor did not signal terminal")
		case <-time.After(2 * time.Millisecond):
		}
	}
signalled:
	// Backoff: while terminal persists, probe rate drops (checks stop
	// growing at base speed). Give the base-rate counter ~6 slots worth
	// of time; with backoff the count must stay well below.
	countAtSignal := checks.Load()
	time.Sleep(120 * time.Millisecond) // ≥ 12 base ticks if no backoff
	countAfter := checks.Load()
	if grown := countAfter - countAtSignal; grown > 6 {
		t.Fatalf("probe count grew by %d during persistent terminal (base ticks would be ~12); backoff not applied", grown)
	}

	// Swap (as the supervisor would): fresh generation, fresh signal,
	// check now healthy on genB → base rate resumes and no new signal.
	if _, err := mgr.Swap(context.Background()); err != nil {
		t.Fatalf("swap: %v", err)
	}
	select {
	case <-mgr.TerminalCh():
		t.Fatal("new generation inherited the closed signal")
	default:
	}
}

// The LIVE acceptance regression (real poisoned client + real
// replacement) lives in the kurrenttest package
// (clientmanager_live_test.go): importing kurrenttest from here would be
// an import cycle (kurrenttest imports this package for OpenClient).

// --- D107-R4 review regressions: cancellation during dial + client/error cleanup ---

// closeRecorder captures every client the manager retires, so tests can
// prove the leak-freedom contract (what was closed, how many times).
type closeRecorder struct {
	mu     sync.Mutex
	closed []*kurrentdb.Client
}

func (r *closeRecorder) close(c *kurrentdb.Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = append(r.closed, c)
}

func (r *closeRecorder) countOf(c *kurrentdb.Client) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, closed := range r.closed {
		if closed == c {
			n++
		}
	}
	return n
}

type swapOutcome struct {
	client *kurrentdb.Client
	err    error
}

// TestClientManagerSwapCancelledDuringDialDoesNotPublish is the D107-R4
// review-HIGH regression: the production dial (OpenClient) ignores its
// context, so a caller that gives up mid-dial can still be handed a live
// client when the dial unblocks. The swap must NOT publish that client —
// it must be closed, the swap must fail with ctx.Err(), the old
// generation must stay current, coalesced callers must observe the same
// failure (singleflight intact), and the manager must not wedge.
func TestClientManagerSwapCancelledDuringDialDoesNotPublish(t *testing.T) {
	t.Parallel()

	genA, genB, genC := &kurrentdb.Client{}, &kurrentdb.Client{}, &kurrentdb.Client{}
	rec := &closeRecorder{}
	dialEntered := make(chan struct{})
	releaseDial := make(chan struct{})
	var dials atomic.Int32
	mgr := NewClientManager(Config{URL: "kurrentdb://unused"}, genA,
		WithDialFunc(func(ctx context.Context, _ Config) (*kurrentdb.Client, error) {
			// Mirror the production OpenClient: ctx deliberately ignored.
			_ = ctx
			if dials.Add(1) == 1 {
				close(dialEntered)
				<-releaseDial // the caller gives up while we are stuck here
				return genB, nil
			}
			return genC, nil
		}),
		WithMinSwapInterval(0),
		WithCloseFunc(rec.close),
	)

	// Winner: swap with a context that gets cancelled mid-dial.
	ctx, cancel := context.WithCancel(context.Background())
	winnerDone := make(chan swapOutcome, 1)
	go func() {
		c, err := mgr.Swap(ctx)
		winnerDone <- swapOutcome{client: c, err: err}
	}()
	<-dialEntered

	// Coalesced caller: piles onto the winner's in-flight dial.
	coalescedDone := make(chan swapOutcome, 1)
	go func() {
		c, err := mgr.Swap(context.Background())
		coalescedDone <- swapOutcome{client: c, err: err}
	}()
	time.Sleep(50 * time.Millisecond) // let the coalesced caller block on the singleflight

	// The winner gives up; the dial then returns a VALID client anyway.
	cancel()
	close(releaseDial)

	select {
	case got := <-winnerDone:
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("cancelled swap error = %v, want context.Canceled", got.err)
		}
		if got.client != nil {
			t.Fatal("cancelled swap returned a client; the fresh generation must not be handed to a gone caller")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled winner did not return")
	}

	// The fresh client from the aborted dial was CLOSED exactly once and
	// never published; the old generation stays current and unclosed.
	if got := rec.countOf(genB); got != 1 {
		t.Fatalf("fresh client closed %d times, want exactly 1 (leak or double close)", got)
	}
	if got := rec.countOf(genA); got != 0 {
		t.Fatalf("old generation closed %d times on a FAILED swap, want 0", got)
	}
	if mgr.Client() != genA || mgr.Swaps() != 0 {
		t.Fatalf("state after cancelled swap: current-is-old=%v swaps=%d, want old/0", mgr.Client() == genA, mgr.Swaps())
	}

	// Singleflight intact: the coalesced caller wakes with the SAME
	// failure and the OLD generation (not the aborted fresh one).
	select {
	case got := <-coalescedDone:
		if got.err == nil || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("coalesced caller error = %v, want the winner's context.Canceled", got.err)
		}
		if got.client != genA {
			t.Fatalf("coalesced caller client = %p, want the OLD generation %p", got.client, genA)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("coalesced caller did not wake — singleflight did not complete on the failure path")
	}

	// Not wedged: a subsequent swap on a live context succeeds and
	// publishes the next generation. The old generation is RETIRED, not
	// closed (D135): a handed-out generation must never be closed.
	fresh, err := mgr.Swap(context.Background())
	if err != nil || fresh != genC {
		t.Fatalf("subsequent swap = (%p, %v), want genC success — manager wedged after cancelled swap", fresh, err)
	}
	if mgr.Client() != genC || mgr.Swaps() != 1 {
		t.Fatalf("state after recovery swap: swaps=%d", mgr.Swaps())
	}
	if got := rec.countOf(genA); got != 0 {
		t.Fatalf("old generation closed %d times on the SUCCESSFUL swap, want 0 (retired, never closed — D135)", got)
	}
}

// TestClientManagerSwapClosesClientReturnedWithError pins the other leak
// path of the D107-R4 HIGH: a dial that misbehaves by returning a
// NON-NIL client together with an error must have that client closed
// exactly once, must not publish it, and must leave the manager operable.
func TestClientManagerSwapClosesClientReturnedWithError(t *testing.T) {
	t.Parallel()

	genA, leaked, genC := &kurrentdb.Client{}, &kurrentdb.Client{}, &kurrentdb.Client{}
	rec := &closeRecorder{}
	boom := errors.New("dial exploded")
	fail := atomic.Bool{}
	fail.Store(true)
	mgr := NewClientManager(Config{URL: "kurrentdb://unused"}, genA,
		WithDialFunc(func(context.Context, Config) (*kurrentdb.Client, error) {
			if fail.Load() {
				return leaked, boom // misbehaving dial: client AND error
			}
			return genC, nil
		}),
		WithMinSwapInterval(0),
		WithCloseFunc(rec.close),
	)

	_, err := mgr.Swap(context.Background())
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("swap error = %v, want wrapped dial error", err)
	}
	if got := rec.countOf(leaked); got != 1 {
		t.Fatalf("client-returned-with-error closed %d times, want exactly 1 (leak or double close)", got)
	}
	if mgr.Client() != genA || mgr.Swaps() != 0 {
		t.Fatalf("failed swap mutated state: current-is-old=%v swaps=%d", mgr.Client() == genA, mgr.Swaps())
	}
	if got := rec.countOf(genA); got != 0 {
		t.Fatalf("old generation closed %d times on a failed swap, want 0", got)
	}

	// Recovery: the manager is not wedged; a clean dial publishes. The
	// old generation is RETIRED, never closed (D135).
	fail.Store(false)
	fresh, err := mgr.Swap(context.Background())
	if err != nil || fresh != genC {
		t.Fatalf("recovery swap = (%p, %v), want genC success", fresh, err)
	}
	if got := rec.countOf(genA); got != 0 {
		t.Fatalf("old generation closed %d times on recovery swap, want 0 (retired, never closed — D135)", got)
	}
	if got := rec.countOf(leaked); got != 1 {
		t.Fatalf("leaked client closed again on recovery: %d, want exactly 1 total", got)
	}
}
