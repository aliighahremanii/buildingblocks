package eventstoredb

// client_pinned_behavior_test.go — pins a load-bearing behavior of the
// PINNED KurrentDB client (github.com/kurrent-io/KurrentDB-Client-Go
// v1.4.1) that the projection-resilience design depends on. If a client
// upgrade changes this behavior, this test fails and the resilience
// docs/runbook must be re-evaluated in the same change.
//
// Pinned behavior: when node DISCOVERY exhausts MaxDiscoverAttempts
// (default 10 × DiscoveryInterval 100ms) against an unreachable server,
// the client's connection state machine sets its permanent close flag and
// exits (impl.go connectionStateMachine: `atomic.StoreInt32(closeFlag, 1);
// ...; return`). Every subsequent RPC on the SAME client then fails fast
// with *kurrentdb.Error code ErrorCodeConnectionClosed ("connection is
// closed") — forever, within the process. There is no code path that
// resets the flag.
//
// Consequence for the worker host: the shared client must never face a
// discovery sequence that completes while KurrentDB is unreachable, or
// the whole worker process loses its event-store connection until the
// process is restarted (recreating the shared client is explicitly
// forbidden by the approved plan). See docs/decisions.md D107 and
// docs/runbooks/projection-subscription-recovery.md.

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// TestPinnedClientDiscoveryFailurePermanentlyClosesClient drives one RPC
// against a guaranteed-closed local port (connection refused — discovery
// fails fast per attempt, exhausting all 10 attempts within a couple of
// seconds) and then proves the SECOND RPC on the same client fails with
// the typed connection-closed code rather than retrying discovery.
func TestPinnedClientDiscoveryFailurePermanentlyClosesClient(t *testing.T) {
	// Reserve a port, then close it so nothing listens: dialing it yields
	// an immediate TCP RST (connection refused), not a black hole.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("close reserved port: %v", err)
	}

	client, err := OpenClient(nil, Config{URL: "kurrentdb://127.0.0.1:" + itoaPort(port) + "?tls=false"})
	if err != nil {
		t.Fatalf("OpenClient (lazy, must not dial yet): %v", err)
	}
	t.Cleanup(func() { client.Close() })

	// First RPC drives discovery, which exhausts its attempts against the
	// refused port. Bounded by the client's own discovery loop (10
	// attempts; each fails fast on RST).
	firstErr := func() error {
		start := time.Now()
		rd, err := client.ReadStream(t.Context(), "probe-stream",
			kurrentdb.ReadStreamOptions{Direction: kurrentdb.Forwards, From: kurrentdb.Start{}}, 1)
		if err == nil {
			rd.Close()
			return nil
		}
		t.Logf("first RPC failed after %s (discovery exhaustion): %v", time.Since(start), err)
		return err
	}()
	if firstErr == nil {
		t.Fatal("first RPC against a closed port unexpectedly succeeded; test premise broken")
	}

	// Second RPC on the same client: must fail FAST with the typed
	// connection-closed code — the permanent poison, not another
	// discovery round.
	start := time.Now()
	_, secondErr := client.ReadStream(t.Context(), "probe-stream",
		kurrentdb.ReadStreamOptions{Direction: kurrentdb.Forwards, From: kurrentdb.Start{}}, 1)
	elapsed := time.Since(start)
	if secondErr == nil {
		t.Fatal("second RPC succeeded after discovery failure — client was NOT permanently closed; pinned v1.4.1 behavior changed, re-evaluate the resilience design")
	}
	var esErr *kurrentdb.Error
	if !errors.As(secondErr, &esErr) {
		t.Fatalf("second RPC error type = %T (%v), want *kurrentdb.Error", secondErr, secondErr)
	}
	if esErr.Code() != kurrentdb.ErrorCodeConnectionClosed {
		t.Fatalf("second RPC error code = %v, want ErrorCodeConnectionClosed (permanent poison)", esErr.Code())
	}
	if elapsed > 2*time.Second {
		t.Fatalf("second RPC took %s after poison; the closed-client fast-fail is part of the pinned contract", elapsed)
	}
}

func itoaPort(p int) string {
	if p == 0 {
		return "0"
	}
	var digits []byte
	for p > 0 {
		digits = append([]byte{byte('0' + p%10)}, digits...)
		p /= 10
	}
	return string(digits)
}
