package eventstoredb

// clientmanager_parked_sender_test.go — the D135 regression for the
// AR-210 E2E worker crash ("panic: send on closed channel" in the pinned
// KurrentDB client's getConnectionHandle, worker.log goroutine
// releaseOpeningCompletionLoop).
//
// Mechanism under test (pinned SDK v1.4.1, see also
// client_pinned_behavior_test.go):
//
//   - When one node-discovery sequence exhausts its budget, the SDK's
//     connection state machine sets its permanent close flag, answers
//     the ONE call it is processing, and EXITS — leaving the client's
//     UNBUFFERED channel open with no receiver.
//   - Every concurrent RPC that passed the isClosed() flag check just
//     before is now a PARKED SENDER: blocked forever in
//     `client.channel <- msg`, because no receiver will ever come.
//   - Client.Close() closes that channel; closing a channel under
//     blocked senders panics each of them ("send on closed channel") —
//     and a panic in ANY goroutine kills the whole worker process.
//
// Before D135, ClientManager.Swap called Close on the retired
// generation, so a swap during an outage (pollers/projections mid-RPC)
// crashed the worker. The fix retires handed-out generations WITHOUT
// closing them; this test reproduces the exact parked-sender state
// deterministically with the real SDK (no server needed — a closed local
// port poisons the client in ~1s) and proves the swap survives it.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// terminalProbe performs the manager's production terminal check (one
// unary read against a nonexistent stream) and reports whether the
// client answered with the permanent connection-closed poison.
func terminalProbe(c *kurrentdb.Client) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.ReadStream(ctx, probeStream,
		kurrentdb.ReadStreamOptions{Direction: kurrentdb.Forwards, From: kurrentdb.Start{}}, 1)
	return IsTerminalClose(err)
}

// parkReadersOnPoisonedClient deterministically produces PARKED SENDERS
// on a real client: it opens a client against a guaranteed-closed local
// port, fires `cohort` concurrent ReadStream calls, and waits for the
// discovery sequence to exhaust (~1s against immediate RST). The first
// call whose send the state machine consumed triggers the fatal
// discovery and returns its exhaustion error; every other call that
// reached the send statement in that window is parked forever on the now
// receiver-less channel. It returns the poisoned client and the number
// of calls that returned (the rest are parked).
func parkReadersOnPoisonedClient(t *testing.T, cohort int) (*kurrentdb.Client, int) {
	t.Helper()

	client, err := OpenClient(context.Background(), deadPortConfig(t))
	if err != nil {
		t.Fatalf("open client against dead port: %v", err)
	}
	// DELIBERATELY no t.Cleanup close: the parked senders make Close a
	// panic (that is the defect being regression-tested). The client is
	// inert (poisoned) and dies with the test process.

	returns := make(chan error, cohort)
	for i := 0; i < cohort; i++ {
		go func() {
			_, err := client.ReadStream(context.Background(), "yardflow-parked-sender-probe",
				kurrentdb.ReadStreamOptions{Direction: kurrentdb.Forwards, From: kurrentdb.Start{}}, 1)
			returns <- err
		}()
	}

	// Wait for the triggering call to fail: the full discovery sequence
	// (10 attempts) exhausted against the refused port.
	select {
	case err := <-returns:
		if err == nil {
			t.Fatal("triggering read against a closed port unexpectedly succeeded; test premise broken")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("no cohort call returned; the poison sequence never completed — test premise broken")
	}

	// Settle: calls that started after the poison fast-fail immediately;
	// parked senders never return. One second is orders of magnitude
	// more than a fast-fail needs.
	time.Sleep(1 * time.Second)
	returned := 0
forLoop:
	for {
		select {
		case <-returns:
			returned++
		default:
			break forLoop
		}
	}
	return client, returned
}

// TestClientManagerSwapWithParkedSendersDoesNotPanic is THE D135
// regression. It reproduces the AR-210 E2E failure state in-process — a
// terminally poisoned generation with parked senders — and swaps it. A
// panic in any goroutine kills the test binary before the assertions
// run, so reaching them is the proof. It also pins the retirement
// policy: the handed-out poisoned generation is NEVER closed.
func TestClientManagerSwapWithParkedSendersDoesNotPanic(t *testing.T) {
	t.Parallel()

	const cohort = 32
	poisoned, returned := parkReadersOnPoisonedClient(t, cohort)
	parked := cohort - returned
	if parked < 1 {
		t.Fatalf("no parked senders established (returned=%d/%d); cannot reproduce the E2E panic precondition", returned, cohort)
	}
	t.Logf("poisoned generation established: %d parked senders, %d fast-failed returns", parked, returned)

	// Sanity: the poisoned generation answers with the permanent
	// terminal close (the manager's detection condition).
	if !terminalProbe(poisoned) {
		t.Fatal("poisoned client did not answer with the terminal connection-closed error; pinned behavior changed")
	}

	// The manager owns the poisoned generation; the dial is lazy (never
	// contacts a server), so the swap succeeds offline.
	rec := &closeRecorder{}
	mgr := NewClientManager(Config{URL: "kurrentdb://unused"}, poisoned,
		WithDialFunc(func(context.Context, Config) (*kurrentdb.Client, error) {
			return &kurrentdb.Client{}, nil
		}),
		WithMinSwapInterval(0),
		WithCloseFunc(rec.close),
	)

	fresh, err := mgr.Swap(context.Background())
	if err != nil {
		t.Fatalf("Swap: %v", err)
	}
	if fresh == nil || fresh == poisoned {
		t.Fatal("Swap did not produce a fresh generation")
	}
	if mgr.Client() != fresh || mgr.Swaps() != 1 {
		t.Fatalf("manager state wrong after swap: client-fresh=%v swaps=%d", mgr.Client() == fresh, mgr.Swaps())
	}

	// The retirement policy: a handed-out generation is NEVER closed —
	// closing it is exactly what panicked the parked senders in the E2E.
	if got := rec.countOf(poisoned); got != 0 {
		t.Fatalf("retired generation closed %d times; handed-out generations must never be closed (D135)", got)
	}

	// Recovery semantics (unit level): the fresh generation is current,
	// the terminal signal re-armed for it, and the retired generation
	// stays inert-poisoned forever after.
	select {
	case <-mgr.TerminalCh():
		t.Fatal("swap did not re-arm the terminal signal for the new generation")
	default:
	}
	if !terminalProbe(poisoned) {
		t.Fatal("retired generation stopped reporting terminal close")
	}
	// The process survived the swap with the parked senders in place —
	// assertions running here IS the no-panic proof (any parked-sender
	// panic kills the binary before this line).
}

// TestClientManagerMonitorSurvivesParkedProbe is the D135 monitor-side
// regression: the terminal probe itself can park inside the pinned
// client's non-cancellable connection acquisition (a probe that fires
// while the state machine is mid-discovery and whose send is not
// consumed when the machine exits). A synchronous probe would wedge the
// Monitor goroutine forever — terminal detection, the swap, and worker
// recovery would all stop. The bounded probe abandons it and the next
// tick's probe detects the terminal close.
func TestClientManagerMonitorSurvivesParkedProbe(t *testing.T) {
	t.Parallel()

	genA := &kurrentdb.Client{}
	release := make(chan struct{})
	t.Cleanup(func() { close(release) }) // let the parked fake probe return at teardown

	var calls atomic.Int32
	mgr := NewClientManager(Config{URL: "kurrentdb://unused"}, genA,
		WithCheckFunc(func(context.Context, *kurrentdb.Client) bool {
			if calls.Add(1) == 1 {
				<-release // the parked probe: never answers on its own
			}
			return true // the generation is terminal
		}),
		WithMinSwapInterval(0),
		WithProbeTimeout(100*time.Millisecond),
		WithCloseFunc(func(*kurrentdb.Client) {}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go mgr.Monitor(ctx, 10*time.Millisecond)

	// Despite the first probe parking, the monitor must keep probing and
	// signal the terminal close.
	select {
	case <-mgr.TerminalCh():
	case <-time.After(3 * time.Second):
		t.Fatal("monitor wedged on a parked probe; terminal close never detected — no swap, no recovery")
	}
	if got := calls.Load(); got < 2 {
		t.Fatalf("probe calls = %d, want >= 2 (the parked probe must not stop the probe loop)", got)
	}
	cancel()
}
