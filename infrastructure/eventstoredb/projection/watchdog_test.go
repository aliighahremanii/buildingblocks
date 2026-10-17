package projection

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// Regression coverage for the unfiltered-subscription + probe-backed stall
// watchdog changes (review round 2026-08-28):
//   - non-matching events bypass ProjectFunc but still advance/persist the checkpoint
//   - subscription options carry no server-side filter
//   - a healthy idle subscription at head is NOT restarted (probe disengages)
//   - a genuinely stalled subscription behind head IS restarted
//   - CaughtUp disables and FellBehind re-enables detection
//   - a slow-but-healthy projector is not mistaken for a stalled read
//   - the watchdog exits when an attempt ends (Run returns cleanly)

func appearedEventIn(commit uint64, stream string) *kurrentdb.SubscriptionEvent {
	return &kurrentdb.SubscriptionEvent{
		EventAppeared: &kurrentdb.ResolvedEvent{
			Event: &kurrentdb.RecordedEvent{
				EventType: "test.event",
				StreamID:  stream,
				Position:  kurrentdb.Position{Commit: commit, Prepare: commit},
			},
		},
	}
}

func caughtUpEvent() *kurrentdb.SubscriptionEvent {
	return &kurrentdb.SubscriptionEvent{CaughtUp: &kurrentdb.CaughtUp{}}
}

func fellBehindEvent() *kurrentdb.SubscriptionEvent {
	return &kurrentdb.SubscriptionEvent{FellBehind: &kurrentdb.FellBehind{}}
}

// newWatchdogTestWorker builds a worker with a short stall window and an
// injected head probe, for the watchdog behavior tests.
func newWatchdogTestWorker(probe func(ctx context.Context) (uint64, bool)) (*Worker, *subscribeRecorder, *fakeCheckpointer, *fakeStream) {
	s := newFakeStream()
	rec := &subscribeRecorder{streams: []*fakeStream{s}}
	checks := newFakeCheckpointer()
	w := &Worker{
		client:         nil,
		checks:         checks,
		config:         WorkerConfig{Name: "watchdog", Prefixes: []string{"t-"}, BatchSize: 1000, MaxAttempts: 1, EstablishmentTimeout: 20 * time.Millisecond, FlushTimeout: time.Second},
		project:        noOpProject,
		subscribeF:     rec.subscribe,
		probeHead:      probe,
		retryBaseDelay: time.Millisecond,
		retryMaxDelay:  2 * time.Millisecond,
		reconnectDelay: 5 * time.Millisecond,
		stallInterval:  10 * time.Millisecond,
		idleRecheck:    time.Hour, // default tests never hit the idle re-probe
		state:          StateIdle,
	}
	w.stallTimeout.Store(int64(40 * time.Millisecond))
	return w, rec, checks, s
}

func runWorker(t *testing.T, w *Worker) (context.CancelFunc, chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	waitForState(t, w, StateRunning, 2*time.Second)
	return cancel, done
}

func stopWorker(t *testing.T, cancel context.CancelFunc, done chan error) {
	t.Helper()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel — watchdog or runner leaked")
	}
}

func TestUnfilteredSubscriptionSkipsNonMatchingAndAdvancesCheckpoint(t *testing.T) {
	s := newFakeStream()
	rec := &subscribeRecorder{streams: []*fakeStream{s}}
	checks := newFakeCheckpointer()
	var mu sync.Mutex
	var got []uint64
	w := &Worker{
		client: nil,
		checks: checks,
		config: WorkerConfig{Name: "skip", Prefixes: []string{"t-"}, BatchSize: 1000, MaxAttempts: 1, EstablishmentTimeout: 20 * time.Millisecond, FlushTimeout: time.Second},
		project: func(_ context.Context, ev *kurrentdb.RecordedEvent) error {
			mu.Lock()
			got = append(got, ev.Position.Commit)
			mu.Unlock()
			return nil
		},
		subscribeF:     rec.subscribe,
		retryBaseDelay: time.Millisecond,
		retryMaxDelay:  2 * time.Millisecond,
		reconnectDelay: 5 * time.Millisecond,
		stallInterval:  time.Hour,
		state:          StateIdle,
	}
	w.stallTimeout.Store(int64(time.Hour))
	cancel, done := runWorker(t, w)

	s.send(appearedEvent(1))          // t-stream: matches
	s.send(appearedEventIn(2, "x-1")) // non-matching
	s.send(appearedEventIn(3, "y-2")) // non-matching
	s.send(appearedEvent(4))          // matches
	s.end()                           // clean stream end -> terminal flush

	// Terminal flush must persist the LAST processed position (4) — the
	// non-matching events advanced the checkpoint without projecting.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if checks.lastSave().pos == 4 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("checkpoint not advanced to 4; last save = %+v", checks.lastSave())
		}
		time.Sleep(2 * time.Millisecond)
	}
	mu.Lock()
	if len(got) != 2 || got[0] != 1 || got[1] != 4 {
		t.Fatalf("projected commits = %v, want [1 4] (non-matching events must bypass ProjectFunc)", got)
	}
	mu.Unlock()
	stopWorker(t, cancel, done)
}

func TestSubscribeOptionsCarryNoServerSideFilter(t *testing.T) {
	s := newFakeStream()
	rec := &subscribeRecorder{streams: []*fakeStream{s}}
	checks := newFakeCheckpointer()
	checks.pos["nofilter"] = 42
	w := &Worker{
		client:         nil,
		checks:         checks,
		config:         WorkerConfig{Name: "nofilter", Prefixes: []string{"t-"}, BatchSize: 1000, MaxAttempts: 1, EstablishmentTimeout: 20 * time.Millisecond, FlushTimeout: time.Second},
		project:        noOpProject,
		subscribeF:     rec.subscribe,
		retryBaseDelay: time.Millisecond,
		retryMaxDelay:  2 * time.Millisecond,
		reconnectDelay: 5 * time.Millisecond,
		stallInterval:  time.Hour,
		state:          StateIdle,
	}
	w.stallTimeout.Store(int64(time.Hour))
	cancel, done := runWorker(t, w)

	if c := rec.call(0); c.from != 42 {
		t.Fatalf("subscribe from = %d, want the persisted checkpoint 42", c.from)
	}
	if c := rec.call(0); c.filter != nil {
		t.Fatalf("subscribe carried a server-side filter %+v; the worker must read the full $all and filter in-process", c.filter)
	}
	stopWorker(t, cancel, done)
}

func TestWatchdogDoesNotRestartHealthyIdleSubscriptionAtHead(t *testing.T) {
	// head == lastCommit (0): the subscription is at the head and silent —
	// a healthy idle $all. The watchdog must NOT restart it.
	w, rec, _, _ := newWatchdogTestWorker(func(ctx context.Context) (uint64, bool) {
		return 0, true
	})
	cancel, done := runWorker(t, w)

	// Let several watchdog windows pass on a silent stream.
	time.Sleep(150 * time.Millisecond)
	if n := rec.callCount(); n != 1 {
		t.Fatalf("subscribe calls = %d, want 1 (healthy idle subscription must not be restarted)", n)
	}
	stopWorker(t, cancel, done)
}

func TestWatchdogRestartsStalledSubscriptionBehindHead(t *testing.T) {
	// head 100 > lastCommit 0: undelivered events exist — genuinely stalled.
	w, rec, _, _ := newWatchdogTestWorker(func(ctx context.Context) (uint64, bool) {
		return 100, true
	})
	cancel, done := runWorker(t, w)

	deadline := time.Now().Add(2 * time.Second)
	for rec.callCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("watchdog did not resubscribe a stalled subscription; calls = %d", rec.callCount())
		}
		time.Sleep(2 * time.Millisecond)
	}
	// The watchdog must also exit cleanly when the attempt ends (Run
	// returns without hanging or leaking the watchdog goroutine).
	stopWorker(t, cancel, done)
}

func TestWatchdogCaughtUpDisablesAndFellBehindReEnables(t *testing.T) {
	// probeHead would report "behind" — but CaughtUp must disarm the
	// watchdog, and FellBehind must re-arm it. The stall window starts
	// long so the test can deliver CaughtUp before the first probe would
	// fire, then is shortened mid-run for the FellBehind phase (the
	// watchdog reads w.stallTimeout fresh every tick).
	w, rec, _, s := newWatchdogTestWorker(func(ctx context.Context) (uint64, bool) {
		return 100, true
	})
	w.stallTimeout.Store(int64(200 * time.Millisecond))
	cancel, done := runWorker(t, w)

	s.send(caughtUpEvent())
	time.Sleep(250 * time.Millisecond)
	if n := rec.callCount(); n != 1 {
		t.Fatalf("CaughtUp must disarm the watchdog; subscribe calls = %d, want 1", n)
	}

	s.send(fellBehindEvent())
	w.stallTimeout.Store(int64(30 * time.Millisecond))
	deadline := time.Now().Add(2 * time.Second)
	for rec.callCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("FellBehind must re-arm the watchdog; calls = %d", rec.callCount())
		}
		time.Sleep(2 * time.Millisecond)
	}
	stopWorker(t, cancel, done)
}

func TestWatchdogDoesNotFireDuringSlowProjection(t *testing.T) {
	s := newFakeStream()
	rec := &subscribeRecorder{streams: []*fakeStream{s}}
	w := &Worker{
		client: nil,
		checks: newFakeCheckpointer(),
		config: WorkerConfig{Name: "slow", Prefixes: []string{"t-"}, BatchSize: 1000, MaxAttempts: 1, EstablishmentTimeout: 20 * time.Millisecond, FlushTimeout: time.Second},
		project: func(_ context.Context, ev *kurrentdb.RecordedEvent) error {
			time.Sleep(30 * time.Millisecond) // slower than half the stall window
			return nil
		},
		subscribeF:     rec.subscribe,
		probeHead:      func(ctx context.Context) (uint64, bool) { return 100, true }, // behind: a processing-blind watchdog would fire
		retryBaseDelay: time.Millisecond,
		retryMaxDelay:  2 * time.Millisecond,
		reconnectDelay: 5 * time.Millisecond,
		stallInterval:  10 * time.Millisecond,
		state:          StateIdle,
	}
	w.stallTimeout.Store(int64(40 * time.Millisecond))
	cancel, done := runWorker(t, w)

	// Feed events continuously; each takes 30ms to project and the stall
	// window is 40ms, so a watchdog that ignored in-flight projection
	// would fire mid-stream.
	for i := uint64(1); i <= 6; i++ {
		s.send(appearedEvent(i))
		time.Sleep(15 * time.Millisecond)
	}
	// Still mid-projection (event 5 or 6) — no reconnect may have fired.
	if n := rec.callCount(); n != 1 {
		t.Fatalf("watchdog fired %d reconnects during healthy slow projection; want 1", n)
	}
	stopWorker(t, cancel, done)
}

func TestWatchdogRecoversWedgeAtHeadAfterHeadAdvances(t *testing.T) {
	// The subscription verifies it is at the head (probe: head ==
	// lastCommit == 0) and goes quiet. A LATER write advances $all to 100;
	// the wedged subscription delivers neither the event nor FellBehind.
	// The disarmed watchdog must re-probe periodically and fire.
	var head atomic.Uint64
	w, rec, _, _ := newWatchdogTestWorker(func(ctx context.Context) (uint64, bool) {
		return head.Load(), true
	})
	w.idleRecheck = 40 * time.Millisecond // shorten the backed-off re-evaluation
	cancel, done := runWorker(t, w)

	// Phase 1: at head, idle — must NOT restart.
	time.Sleep(120 * time.Millisecond)
	if n := rec.callCount(); n != 1 {
		t.Fatalf("healthy idle at head restarted; calls = %d, want 1", n)
	}

	// Phase 2: the $all head advances while the subscription stays silent.
	head.Store(100)
	deadline := time.Now().Add(2 * time.Second)
	for rec.callCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("watchdog did not recover a wedge at head after the head advanced; calls = %d", rec.callCount())
		}
		time.Sleep(2 * time.Millisecond)
	}
	stopWorker(t, cancel, done)
}

func TestWatchdogTransientProbeFailureDoesNotFire(t *testing.T) {
	// The first two probes fail transiently; the third succeeds and proves
	// the subscription is at head. A single (or couple of) inconclusive
	// diagnostic reads must not disrupt an otherwise usable subscription.
	probes := 0
	w, rec, _, _ := newWatchdogTestWorker(func(ctx context.Context) (uint64, bool) {
		probes++
		if probes <= 2 {
			return 0, false // inconclusive
		}
		return 0, true // at head: healthy idle
	})
	cancel, done := runWorker(t, w)

	time.Sleep(200 * time.Millisecond)
	if n := rec.callCount(); n != 1 {
		t.Fatalf("transient probe failures restarted the subscription; calls = %d, want 1", n)
	}
	stopWorker(t, cancel, done)
}

func TestWatchdogRepeatedProbeFailureFires(t *testing.T) {
	// The probe keeps failing (server unresponsive to diagnostics): after
	// maxInconclusiveProbes consecutive failures the read is treated as
	// stalled and the subscription is restarted.
	w, rec, _, _ := newWatchdogTestWorker(func(ctx context.Context) (uint64, bool) {
		return 0, false
	})
	cancel, done := runWorker(t, w)

	deadline := time.Now().Add(2 * time.Second)
	for rec.callCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("watchdog did not fire after repeated probe failures; calls = %d", rec.callCount())
		}
		time.Sleep(2 * time.Millisecond)
	}
	stopWorker(t, cancel, done)
}

func TestWatchdogReArmsOnEventAfterCaughtUp(t *testing.T) {
	// CaughtUp disarms the watchdog, but a delivered event invalidates the
	// at-head observation: a burst may have started and the read may wedge
	// before reaching the new head. The stall must then be detected after
	// stallTimeout — NOT after the 5-minute idleRecheck — even though no
	// FellBehind notification ever arrives. stallTimeout starts long so the
	// CaughtUp is delivered before the first probe would fire.
	w, rec, _, s := newWatchdogTestWorker(func(ctx context.Context) (uint64, bool) {
		return 1000, true // always behind the delivered burst event (101): a wedged read would miss these events
	})
	projected := make(chan struct{}, 1)
	w.project = func(context.Context, *kurrentdb.RecordedEvent) error {
		projected <- struct{}{}
		return nil
	}
	w.stallTimeout.Store(int64(200 * time.Millisecond))
	w.idleRecheck = time.Hour // prove the re-arm-on-event, not the recheck, fires
	cancel, done := runWorker(t, w)

	s.send(caughtUpEvent())
	time.Sleep(250 * time.Millisecond)
	if n := rec.callCount(); n != 1 {
		t.Fatalf("CaughtUp must disarm; calls = %d, want 1", n)
	}

	// Burst begins: an ordinary event is delivered (no FellBehind), then
	// the read wedges mid-burst. The watchdog must have re-armed on the
	// event and fire after ~stallTimeout, not wait for idleRecheck.
	s.send(appearedEvent(101))
	select {
	case <-projected:
	case <-time.After(2 * time.Second):
		t.Fatal("ordinary event was not projected before stall check")
	}
	w.stallTimeout.Store(int64(30 * time.Millisecond))
	deadline := time.Now().Add(10 * time.Second)
	for rec.callCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("event delivery did not re-arm the watchdog; calls = %d", rec.callCount())
		}
		time.Sleep(2 * time.Millisecond)
	}
	stopWorker(t, cancel, done)
}

func TestWatchdogBoundedProbeStaysResponsive(t *testing.T) {
	// The pinned client's connection acquisition is not context-cancellable
	// (D107): a probe can park inside getConnectionHandle during wedged
	// client discovery. Deliberately ignore ctx here: the watchdog must
	// observe its own timeout without accumulating probes across reconnects,
	// and Run must stop while the underlying call remains parked.
	release := make(chan struct{})
	var calls atomic.Int32
	var active atomic.Int32
	w, rec, _, _ := newWatchdogTestWorker(func(context.Context) (uint64, bool) {
		calls.Add(1)
		active.Add(1)
		defer active.Add(-1)
		<-release
		return 0, false
	})
	w.probeTimeout = 30 * time.Millisecond
	cancel, done := runWorker(t, w)

	deadline := time.Now().Add(2 * time.Second)
	for rec.callCount() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("watchdog blocked on a parked probe; calls = %d", rec.callCount())
		}
		time.Sleep(2 * time.Millisecond)
	}
	// Let the replacement attempt run through several watchdog ticks. The
	// worker-scoped single-flight gate must keep the still-parked first probe
	// as the only invocation across attempts.
	time.Sleep(100 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("probe calls while first call remains parked = %d, want 1", got)
	}
	if got := active.Load(); got != 1 {
		t.Fatalf("active probes while first call remains parked = %d, want 1", got)
	}

	stopWorker(t, cancel, done)
	if got := active.Load(); got != 1 {
		t.Fatalf("worker shutdown unexpectedly cancelled non-cancellable probe; active = %d, want 1", got)
	}

	// Release the late call after its attempt and worker have ended. Its
	// buffered result must drain without blocking and release the gate so a
	// later worker run can probe again.
	close(release)
	deadline = time.Now().Add(2 * time.Second)
	for active.Load() != 0 || w.probePending.Load() {
		if time.Now().After(deadline) {
			t.Fatalf("late probe did not finish cleanly; active=%d pending=%v", active.Load(), w.probePending.Load())
		}
		time.Sleep(2 * time.Millisecond)
	}

	w.probeHead = func(context.Context) (uint64, bool) {
		calls.Add(1)
		return 0, true
	}
	probeStream := newFakeStream()
	handle := &subscriptionHandle{stream: probeStream, cancel: func() {}}
	runner := newSubscriptionRunner(handle)
	watchdogDone := make(chan struct{})
	watchdogCtx, stopWatchdog := context.WithCancel(context.Background())
	watchdogExited := make(chan struct{})
	go func() {
		w.watchStall(watchdogCtx, runner, watchdogDone)
		close(watchdogExited)
	}()
	deadline = time.Now().Add(2 * time.Second)
	for calls.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("probe gate was not reusable after late completion; calls = %d", calls.Load())
		}
		time.Sleep(2 * time.Millisecond)
	}
	stopWatchdog()
	select {
	case <-watchdogExited:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not stop after reusable probe completed")
	}
}
