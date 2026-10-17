package kurrenttest

// clientmanager_live_test.go — the LIVE acceptance regression for the E2E
// projection-resilience defect (D107-R2), runnable only where a real
// KurrentDB is configured (auto-skips otherwise).
//
// E2E defect: a volume-backed docker restart keeps KurrentDB's port
// refusing longer than the pinned v1.4.1 client's fast-RST discovery
// budget, the shared client terminally closes (ErrorCodeConnectionClosed
// forever), and — before D107-R2 — every projection stayed down until the
// worker process was restarted, violating the required same-process
// restart recovery.
//
// This test reproduces the terminal state deterministically (a client
// poisoned against a closed local port) and proves the fix property in
// ONE process: the host-owned ClientManager swaps the poisoned generation
// for a fresh one that is fully functional against the live server, while
// the old generation stays terminally closed. It lives in this package
// (not eventstoredb) because importing kurrenttest from eventstoredb
// would be an import cycle.

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	bbesdb "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb"
)

// readProbe performs one cheap unary read on c and reports whether it
// failed with the terminal connection-closed error (the same check the
// manager's production probe applies).
func readTerminal(c *kurrentdb.Client) (terminal bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rd, err := c.ReadStream(ctx, "yardflow-manager-live-probe",
		kurrentdb.ReadStreamOptions{Direction: kurrentdb.Forwards, From: kurrentdb.Start{}}, 1)
	if err != nil {
		return bbesdb.IsTerminalClose(err), err
	}
	rd.Close()
	return false, nil
}

func TestLiveClientManagerSwapRecoversFromTerminalClose(t *testing.T) {
	_ = OpenClient(t) // live gate + .env loading; skips when unconfigured

	// Deterministically poison a client against a closed port (immediate
	// RST: one discovery sequence exhausts its budget in ~1s and the
	// client closes permanently — the D107 pinned behavior).
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("close reserved port: %v", err)
	}
	deadCfg := bbesdb.Config{URL: "kurrentdb://127.0.0.1:" + portStr(port) + "?tls=false"}

	poisoned, err := bbesdb.OpenClient(context.Background(), deadCfg)
	if err != nil {
		t.Fatalf("open poisoned client: %v", err)
	}
	t.Cleanup(func() { poisoned.Close() })

	// First read: discovery exhausts (~1s) against the refused port and
	// reports the exhaustion as Unavailable (max discovery attempts) —
	// this is the state a volume-backed restart leaves the client in.
	terminal, firstErr := readTerminal(poisoned)
	if firstErr == nil {
		t.Fatal("read against closed port succeeded; test premise broken")
	}
	if terminal {
		t.Logf("first read already terminal: %v", firstErr)
	}
	// Second read: the PERMANENT fast-fail poison — the pinned v1.4.1
	// behavior (connectionStateMachine exits; every later call returns
	// ErrorCodeConnectionClosed).
	if terminal, err := readTerminal(poisoned); !terminal {
		t.Fatalf("second read error = %v, want the permanent terminal close (poison hypothesis changed — re-evaluate D107)", err)
	}

	// The manager starts with the POISONED generation and the LIVE config;
	// the production dial (OpenClient) supplies the replacement.
	mgr := bbesdb.NewClientManager(bbesdb.ConfigFromEnv(), poisoned)
	if mgr.Client() != poisoned {
		t.Fatal("manager did not serve the seeded generation")
	}

	fresh, err := mgr.Swap(context.Background())
	if err != nil {
		t.Fatalf("Swap: %v", err)
	}
	if fresh == nil || fresh == poisoned {
		t.Fatal("Swap did not produce a fresh generation")
	}
	t.Cleanup(func() { fresh.Close() })
	if mgr.Client() != fresh || mgr.Swaps() != 1 {
		t.Fatalf("manager state wrong after swap: client-fresh=%v swaps=%d", mgr.Client() == fresh, mgr.Swaps())
	}
	select {
	case <-mgr.TerminalCh():
		t.Fatal("swap did not re-arm the terminal signal for the new generation")
	default:
	}

	// THE acceptance property, in-process: the new generation is fully
	// functional against the live server...
	terminal, err = readTerminal(fresh)
	if terminal {
		t.Fatalf("fresh generation terminally closed: %v", err)
	}
	if err != nil {
		t.Logf("fresh generation read: %v (non-terminal — server answered)", err)
	}

	// ...while the old generation stays terminally closed (Swap RETIRED it
	// by dropping the reference — D135: a handed-out generation is never
	// closed; closing a poisoned client panics its parked senders).
	if terminal, _ := readTerminal(poisoned); !terminal {
		t.Fatal("old generation no longer reports terminal close after Swap")
	}

	// Live write/read round-trip on the fresh generation: the replacement
	// is not merely probeable but a fully working client.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream := UniqueStreamName(t, "clientmanager-swap")
	if _, err := fresh.AppendToStream(ctx, stream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}},
		kurrentdb.EventData{EventID: uuid.New(), EventType: "manager.swap.probe", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{}`)},
	); err != nil {
		t.Fatalf("append on fresh generation: %v", err)
	}
	rd, err := fresh.ReadStream(ctx, stream, kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Forwards, From: kurrentdb.Start{}}, 1)
	if err != nil {
		t.Fatalf("read on fresh generation: %v", err)
	}
	rd.Close()
}

func portStr(p int) string {
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

// TestLiveClientManagerSwapRecoversWithParkedSenders is the LIVE D135
// acceptance regression: it reproduces the EXACT AR-210 E2E worker-crash
// state in-process — a terminally poisoned generation with PARKED SENDERS
// (RPC goroutines blocked forever in the pinned client's unbuffered
// channel send) — drives the manager's PRODUCTION detection path (Monitor
// + the real unary probe), swaps through the PRODUCTION dial against the
// live server, and proves full recovery: the fresh generation answers
// reads AND completes a write/read round-trip. Before D135, Swap closed
// the retired generation, closing the channel under the parked senders
// and panicking the process ("send on closed channel"); reaching the
// assertions — no panic in any goroutine — is the proof the retirement
// policy holds against the real SDK with a real replacement generation.
func TestLiveClientManagerSwapRecoversWithParkedSenders(t *testing.T) {
	_ = OpenClient(t) // live gate + .env loading; skips when unconfigured

	// Deterministically produce a poisoned client WITH parked senders: a
	// client opened against a guaranteed-closed port, plus a cohort of
	// concurrent reads whose discovery-exhaustion leaves every call that
	// passed the close-flag check parked on the receiver-less channel
	// (mechanism pinned in client_pinned_behavior_test.go and
	// clientmanager_parked_sender_test.go — package eventstoredb).
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("close reserved port: %v", err)
	}
	deadCfg := bbesdb.Config{URL: "kurrentdb://127.0.0.1:" + portStr(port) + "?tls=false"}

	poisoned, err := bbesdb.OpenClient(context.Background(), deadCfg)
	if err != nil {
		t.Fatalf("open poisoned client: %v", err)
	}
	// DELIBERATELY no t.Cleanup close: the parked senders make Close a
	// panic (the defect under regression). The poisoned generation is
	// inert and dies with the test process — exactly the D135 retirement
	// semantics the worker host now applies.

	const cohort = 32
	returns := make(chan error, cohort)
	for i := 0; i < cohort; i++ {
		go func() {
			_, err := poisoned.ReadStream(context.Background(), "yardflow-manager-live-parked-probe",
				kurrentdb.ReadStreamOptions{Direction: kurrentdb.Forwards, From: kurrentdb.Start{}}, 1)
			returns <- err
		}()
	}
	select {
	case err := <-returns:
		if err == nil {
			t.Fatal("triggering read against a closed port unexpectedly succeeded; test premise broken")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("no cohort call returned; the poison sequence never completed — test premise broken")
	}
	time.Sleep(1 * time.Second) // settle: fast-fail stragglers return; parked senders never do
	returned := 0
drainReturns:
	for {
		select {
		case <-returns:
			returned++
		default:
			break drainReturns
		}
	}
	if parked := cohort - returned; parked < 1 {
		t.Fatalf("no parked senders established (returned=%d/%d); cannot reproduce the E2E crash precondition", returned, cohort)
	}
	t.Logf("poisoned generation established: %d parked senders, %d fast-failed returns", cohort-returned, returned)

	// Sanity: the poisoned generation answers the manager's terminal
	// probe with the permanent connection-closed poison.
	if terminal, err := readTerminal(poisoned); !terminal {
		t.Fatalf("poisoned client read = %v (terminal=%v); want the permanent terminal close", err, terminal)
	}

	// The manager owns the poisoned generation and uses the PRODUCTION
	// dial (OpenClient against the live .env URL). Run the PRODUCTION
	// monitor: its real unary probe must detect the terminal close and
	// signal the supervisor channel.
	mgr := bbesdb.NewClientManager(bbesdb.ConfigFromEnv(), poisoned)
	monCtx, monCancel := context.WithCancel(context.Background())
	defer monCancel()
	go mgr.Monitor(monCtx, 50*time.Millisecond)

	select {
	case <-mgr.TerminalCh():
	case <-time.After(10 * time.Second):
		t.Fatal("monitor never signalled the terminal close of the poisoned generation — no swap, no recovery")
	}

	// The supervisor's action: swap. With parked senders in place, this
	// is the exact call that panicked the AR-210 E2E worker before D135.
	fresh, err := mgr.Swap(context.Background())
	if err != nil {
		t.Fatalf("Swap with parked senders: %v", err)
	}
	if fresh == nil || fresh == poisoned {
		t.Fatal("Swap did not produce a fresh generation")
	}
	t.Cleanup(func() { fresh.Close() })
	if mgr.Client() != fresh || mgr.Swaps() != 1 {
		t.Fatalf("manager state wrong after swap: client-fresh=%v swaps=%d", mgr.Client() == fresh, mgr.Swaps())
	}
	select {
	case <-mgr.TerminalCh():
		t.Fatal("swap did not re-arm the terminal signal for the new generation")
	default:
	}

	// THE recovery property, live: the fresh generation is fully
	// functional against the live server (probe + write/read round-trip)
	// while the retired poisoned generation stays terminally closed and
	// its parked senders stay inert — and the process survived the swap.
	terminal, err := readTerminal(fresh)
	if terminal {
		t.Fatalf("fresh generation terminally closed: %v", err)
	}
	if err != nil {
		t.Logf("fresh generation read: %v (non-terminal — server answered)", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	stream := UniqueStreamName(t, "clientmanager-swap-parked")
	if _, err := fresh.AppendToStream(ctx, stream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.NoStream{}},
		kurrentdb.EventData{EventID: uuid.New(), EventType: "manager.swap.parked.probe", ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{}`)},
	); err != nil {
		t.Fatalf("append on fresh generation after parked-sender swap: %v", err)
	}
	rd, err := fresh.ReadStream(ctx, stream, kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Forwards, From: kurrentdb.Start{}}, 1)
	if err != nil {
		t.Fatalf("read on fresh generation after parked-sender swap: %v", err)
	}
	rd.Close()

	if terminal, _ := readTerminal(poisoned); !terminal {
		t.Fatal("retired generation no longer reports terminal close")
	}
	// Assertions running here IS the no-panic proof: any parked-sender
	// panic would have killed the test binary before this line (D135).
}
