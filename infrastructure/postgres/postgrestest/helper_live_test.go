package postgrestest

// helper_live_test.go — LIVE regression for the D117 shared-database lock as
// re-specified by D192 and its run-3313 follow-up: the advisory lock on
// integrationTestLockKey must be held by ONE GUARD-OWNED session (hijacked
// out of the caller's pool) for the holder's whole test and be genuinely
// free again after the holder's cleanup. Auto-skips via OpenPool's ping
// probe when no Postgres is reachable.
//
// The observers in all subtests are INDEPENDENT pools that never call Lock:
// the in-process integrationTestMu would fake exclusion for a caller inside
// this process — only the cross-process advisory lock may exclude it. The
// assertions pin:
//
//   - while the holder's guard session holds the key, an independent
//     session's pg_try_advisory_lock fails;
//   - pg_locks reports the key as granted to exactly the guard backend pid
//     (read through the unexported lockHolderPID helper — the package's
//     public API is unchanged);
//   - after the holder's LIFO cleanup (same-session unlock → guard session
//     close → cancel → mutex unlock → pool close), the key can be acquired
//     AND unlocked on one observer session — and when parallel-suite
//     contention keeps a FOREIGN package's holder in the way, the pin
//     degrades to its leak-detection core: the guard pid no longer holds
//     the key (a foreign or absent holder is contention; the guard pid
//     holding is the leak);
//   - the run-3313 regression: a caller pool closed INSIDE the holder's
//     body (the eventstore snapshot-failure test's legitimate shape)
//     returns promptly — the guard session is not a pool resource — and
//     the key is still held afterwards, provably by the same guard pid.

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// observerMustSeeKeyHeldByGuardPIN pins, from an independent session that
// never calls Lock, that the key is granted to exactly the guard's backend
// pid: pg_try_advisory_lock must FAIL and pg_locks must report one granted
// row whose pid equals lockHolderPID(). A 64-bit single-key advisory lock is
// stored in pg_locks SPLIT: classid = key >> 32, objid = key & 0xffffffff
// (both oid), so the predicate reconstructs the key instead of binding it
// into the uint32 objid column.
func observerMustSeeKeyHeldByGuardPIN(t *testing.T, observer *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := observer.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire observer session: %v", err)
	}
	defer conn.Release()

	// 1. An independent session must NOT be able to take the key.
	var ok bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", integrationTestLockKey).Scan(&ok); err != nil {
		t.Fatalf("observer try-lock: %v", err)
	}
	if ok {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", integrationTestLockKey)
		t.Fatal("independent pool session acquired the integration lock while the guard session holds it")
	}

	// 2. pg_locks must show the key granted to exactly the guard backend
	// pid (pg_backend_pid of the holder's hijacked guard session).
	rows, err := conn.Query(ctx,
		`SELECT pid FROM pg_locks
		 WHERE locktype='advisory' AND granted
		   AND classid::bigint = ($1::bigint >> 32)
		   AND objid::bigint = ($1::bigint & 4294967295)`,
		integrationTestLockKey)
	if err != nil {
		t.Fatalf("read pg_locks: %v", err)
	}
	defer rows.Close()
	var pids []int
	for rows.Next() {
		var pid int
		if err := rows.Scan(&pid); err != nil {
			t.Fatalf("scan pg_locks pid: %v", err)
		}
		pids = append(pids, pid)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pg_locks: %v", err)
	}
	if len(pids) != 1 {
		t.Fatalf("pg_locks granted rows for the key = %v, want exactly the guard holder", pids)
	}
	if pids[0] != lockHolderPID() {
		t.Fatalf("pg_locks pid = %d, want the guard backend pid %d", pids[0], lockHolderPID())
	}
}

// grantedAdvisoryHolderPID returns the backend pid currently holding the
// integration key (an exclusive advisory lock — at most one granted row), or
// -1 when the key is free at probe time.
func grantedAdvisoryHolderPID(t *testing.T, conn *pgxpool.Conn) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := conn.Query(ctx,
		`SELECT pid FROM pg_locks
		 WHERE locktype='advisory' AND granted
		   AND classid::bigint = ($1::bigint >> 32)
		   AND objid::bigint = ($1::bigint & 4294967295)`,
		integrationTestLockKey)
	if err != nil {
		t.Fatalf("read pg_locks holder pid: %v", err)
	}
	defer rows.Close()
	pid := -1
	for rows.Next() {
		if err := rows.Scan(&pid); err != nil {
			t.Fatalf("scan pg_locks holder pid: %v", err)
		}
		break // exclusive lock: at most one granted row
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate pg_locks holder pid: %v", err)
	}
	return pid
}

// siblingObserverMustProveKeyFreeAndReleasable pins, from an independent
// session that never calls Lock, that the holder's cleanup genuinely gave the
// key up: the pin is that the GUARD's backend pid no longer holds it, and —
// when contention allows a clean claim — that the key can be acquired AND
// unlocked on one sibling session.
//
// Under the focused parallel cascade (-p 8 with several locked packages) a
// FOREIGN package's locked suite may legitimately hold the key far longer
// than any test-local bound. That is contention, NOT a guard leak: the
// guard's release was already boolean-verified in the holder's cleanup and
// its session is closed, so its pid can no longer hold anything. The claim
// poll therefore waits for OUR claim (never steals), and only on expiry
// classifies the current holder by pid: a foreign or absent holder is
// contention (logged, pass); the GUARD pid still holding is the leak (fail).
func siblingObserverMustProveKeyFreeAndReleasable(t *testing.T) {
	t.Helper()
	observer := OpenPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := observer.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire observer session: %v", err)
	}
	defer conn.Release()

	tryLock := func() bool {
		t.Helper()
		var ok bool
		if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", integrationTestLockKey).Scan(&ok); err != nil {
			t.Fatalf("observer try-lock after holder cleanup: %v", err)
		}
		return ok
	}
	claimAndRelease := func() {
		t.Helper()
		// Unlock on the SAME observer session must report ownership.
		var unlocked bool
		if err := conn.QueryRow(ctx, "SELECT pg_advisory_unlock($1)", integrationTestLockKey).Scan(&unlocked); err != nil {
			t.Fatalf("observer unlock: %v", err)
		}
		if !unlocked {
			t.Fatal("pg_advisory_unlock reported the observer session did not hold the key it just acquired")
		}
	}

	// Bounded claim poll: wait for OUR claim, never steal it.
	claimed := false
	deadline := time.Now().Add(10 * time.Second)
	for {
		if tryLock() {
			claimed = true
			break
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if claimed {
		claimAndRelease()
		return
	}

	// The claim bound expired. A short grace poll absorbs the benign race
	// between a failing try-lock and the holder-pid probe (the foreign
	// holder may exit in between).
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tryLock() {
			claimAndRelease()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Classify the current holder. Reading integrationTestLockPID here is
	// race-free in this binary: subtests run sequentially, no Lock call is
	// in flight while the sibling runs (the in-process mutex is free and
	// only Lock writes the var).
	switch pid := grantedAdvisoryHolderPID(t, conn); {
	case pid == lockHolderPID():
		t.Fatalf("the integration lock key is still held by the guard backend pid %d after the holder's cleanup — the same-session unlock did not free it", pid)
	case pid == -1:
		t.Logf("the integration lock key was unclaimable within the bounds yet free at probe time — the guard pid holds nothing (release pin holds); claim proof skipped")
	default:
		t.Logf("the integration lock key is held by foreign backend pid %d (another package's locked suite) after the holder's cleanup — contention, not a guard leak (the guard pid holds nothing); claim proof skipped", pid)
	}
}

func TestLiveLock_HeldByPinnedSessionAndFreeAfterCleanup(t *testing.T) {
	t.Run("holder excludes an independent pool session", func(t *testing.T) {
		holder := OpenPool(t)
		Lock(t, holder) // the LIFO cleanups register on THIS subtest

		observer := OpenPool(t)
		observerMustSeeKeyHeldByGuardPIN(t, observer)
	})

	t.Run("key is acquirable on one observer session after the holder's cleanup", func(t *testing.T) {
		// The holder subtest above has fully unwound by the time this one
		// starts (its cleanups ran: same-session unlock, guard session
		// close, context cancel, mutex unlock, pool close).
		siblingObserverMustProveKeyFreeAndReleasable(t)
	})
}

// TestLiveLock_GuardSurvivesCallerPoolCloseInsideBody is the run-3313
// regression (D192 follow-up): the eventstore snapshot-failure test closes
// its OWN pool INSIDE its body while the D117 guard is held — a legitimate
// test shape that must stay legal. When the guard's session was still a
// resource of that pool, pgxpool.Close blocked in puddle's destructWG.Wait
// for the checked-out guard connection: the body deadlocked for 4m57s until
// the go-test timeout while the key stayed held, expiring every other locked
// package's 120s acquisition bound (the 10-package run-3313 cascade). With
// the guard session HIJACKED out of the pool (the current Lock), the close
// returns promptly, the key stays provably held by the same guard pid, and
// the sibling observer proves the key free again after the holder's cleanup.
func TestLiveLock_GuardSurvivesCallerPoolCloseInsideBody(t *testing.T) {
	t.Run("holder: in-body pool close returns promptly and the guard keeps the key", func(t *testing.T) {
		holder := OpenPool(t)
		Lock(t, holder) // the LIFO cleanups register on THIS subtest

		// The exact eventstore snapshot-test shape: pool.Close() inside the
		// body, run on a goroutine that ONLY signals; the test goroutine
		// selects against a bounded timer (a deadlocked Close must fail the
		// test here, not wedge the package until the go-test timeout).
		closed := make(chan struct{})
		go func() {
			holder.Close()
			close(closed)
		}()
		select {
		case <-closed:
			// The close returned while the guard is held: the guard session
			// is not a resource of the caller's pool.
		case <-time.After(10 * time.Second):
			t.Fatal("holder pool.Close() inside the body did not return within 10s — the guard session is still a resource of the caller's pool (the run-3313 puddle destructWG.Wait deadlock)")
		}

		// An INDEPENDENT observer (never Lock) must still see the key HELD,
		// granted to exactly the recorded guard pid: the guard survived the
		// caller's pool close.
		observer := OpenPool(t)
		observerMustSeeKeyHeldByGuardPIN(t, observer)
	})

	t.Run("sibling observer: key is free and acquirable after the holder's cleanup", func(t *testing.T) {
		// The holder subtest above has fully unwound (same-session unlock,
		// guard session close, context cancel, mutex unlock) — and its
		// in-body pool close already happened inside the body. The key must
		// be genuinely free and releasable on one sibling session.
		siblingObserverMustProveKeyFreeAndReleasable(t)
	})
}
