package postgrestest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	bbpostgres "github.com/aliighahremanii/buildingblocks/infrastructure/postgres"
	"github.com/aliighahremanii/buildingblocks/infrastructure/testenv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const integrationTestLockKey int64 = 42424220260710

var integrationTestMu sync.Mutex

// integrationTestLockPID is the Postgres backend pid of the guard session
// (the hijacked, guard-owned connection — see Lock) that most recently
// acquired integrationTestLockKey. It exists ONLY so the same-package live
// regression (helper_live_test.go) can assert that pg_locks
// reports the guard backend's pid while the lock is held; it is written
// under integrationTestMu and must be READ only while integrationTestMu is
// held (as the regression does, from the same goroutine that called Lock).
// It is not part of the package's public API.
var integrationTestLockPID int

func OpenPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)

	pool, err := bbpostgres.OpenPool(ctx, bbpostgres.ConfigFromEnv())
	if err != nil {
		// Skips when a live Postgres is optional (a plain `go test ./...`),
		// FAILS when the run declared integration tests required — a job that
		// provisioned Postgres must not report green on a skip.
		testenv.SkipOrFail(t, "Postgres integration test skipped: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TruncateTables(t *testing.T, pool *pgxpool.Pool, tables ...string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, table := range tables {
		if _, err := pool.Exec(ctx, "TRUNCATE TABLE "+table+" RESTART IDENTITY CASCADE"); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
}

// TruncateTablesIfExists truncates the listed tables when they exist and is a
// NO-OP for a missing relation. Cleanup paths use it: a fixture table dropped
// by a concurrently running migration suite is not a fixture-integrity
// failure for a test that recreates the table on its next run, and PostgreSQL
// has no `TRUNCATE TABLE IF EXISTS` form (D192). The absence check is the
// typed `to_regclass` probe; a table dropped between the probe and the
// TRUNCATE is tolerated ONLY on the typed SQLSTATE 42P01 (*pgconn.PgError) —
// never rendered error text. Operational errors still fail the test.
//
// Identifier handling is identical to TruncateTables: callers pass literal
// table names, which are interpolated unquoted exactly as the strict variant
// does. Setup-path truncates must keep using TruncateTables — an absent table
// at SETUP is a genuine fixture failure.
func TruncateTablesIfExists(t *testing.T, pool *pgxpool.Pool, tables ...string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, table := range tables {
		// to_regclass resolves to NULL when the relation does not exist.
		var regclass *string
		if err := pool.QueryRow(ctx, "SELECT to_regclass($1)", table).Scan(&regclass); err != nil {
			t.Fatalf("resolve %s: %v", table, err)
		}
		if regclass == nil {
			continue // missing relation: the no-op this variant exists for
		}
		if _, err := pool.Exec(ctx, "TRUNCATE TABLE "+table+" RESTART IDENTITY CASCADE"); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
				continue // dropped between the probe and the truncate — still a no-op
			}
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
}

// Lock serializes shared-database mutations across the suite: the in-process
// mutex covers the tests of one binary; the session-scoped advisory lock on
// integrationTestLockKey covers the concurrently running package binaries
// against one shared Postgres (D117).
//
// The advisory lock is held by ONE GUARD-OWNED SESSION for the entire test:
// the session is acquired from the caller's pool and IMMEDIATELY HIJACKED
// out of it (pgxpool.Conn.Hijack → *pgx.Conn), so the guard's connection is
// no longer a resource of the caller's pool accounting. This is the D192
// follow-up rule from CI run 3313: a test may legitimately close its own
// pool INSIDE its body (the eventstore snapshot-failure test does exactly
// that to force a snapshot write failure), and pgxpool.Close blocks in
// puddle's destructWG.Wait until every checked-out resource is returned — a
// guard connection still owned by the pool deadlocked that body for 4m57s
// while the key stayed held, expiring every other locked package's
// acquisition bound. The hijacked session is closed by Lock's cleanup and is
// never released back to the pool. The session-scoped rule itself (vs a
// pool.Exec lock on an arbitrary pooled connection, whose recycle silently
// DROPS the lock and whose unpinned unlock can land on a different session
// and no-op — both observed in CI run 3312) is unchanged: the lock still
// ends on the SAME session it was acquired on, with the unlock's ownership
// boolean verified. The acquisition is bounded (120s) so a lock leaked by a
// crashed suite cannot wedge this package until the go-test timeout.
//
// Cleanup order (LIFO) is exactly: same-session verified unlock + raw
// session close (the close runs even after an unlock error) →
// acquisition-context cancel → mutex unlock → (OpenPool's cleanups) pool
// close → pool-creation context cancel.
func Lock(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	integrationTestMu.Lock()
	t.Cleanup(integrationTestMu.Unlock)

	// Registered as a cleanup (never deferred) so the hijacked session's
	// acquisition context cannot be canceled while the lock is held; it
	// bounds the acquire and the lock statement only.
	actx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	poolConn, err := pool.Acquire(actx)
	if err != nil {
		t.Fatalf("acquire the connection for the Postgres integration test lock: %v", err)
	}
	// HIJACK: the connection leaves the pool's accounting (puddle marks the
	// resource destroyed and stops waiting for it), so the guard's session
	// survives a caller's in-body pool close. From here the pgxpool.Conn
	// must NEVER be used or released — every statement, and the final
	// close, go through the hijacked *pgx.Conn.
	conn := poolConn.Hijack()

	if _, err := conn.Exec(actx, "SELECT pg_advisory_lock($1)", integrationTestLockKey); err != nil {
		// Acquisition failed (timeout or error). A lock granted by the
		// server in the instant before the client's cancellation landed
		// would leave this session silently holding the key (the run-3313
		// late "you don't own a lock" warning class). Name the current
		// holder first (best-effort, separately bounded, via the caller's
		// pool), then unwind the session: best-effort same-session unlock,
		// unconditional close, original error preserved as primary.
		diag := advisoryLockHolderDiagnostics(pool)
		t.Fatalf("acquire Postgres integration test lock: %v; %s; %s", err, diag, unwindFailedAcquisition(conn))
	}
	// Record the guard backend identity for the same-package regression
	// (see integrationTestLockPID); the lock is held, so we own the var.
	if err := conn.QueryRow(actx, "SELECT pg_backend_pid()").Scan(&integrationTestLockPID); err != nil {
		// Post-hijack failure: best-effort same-session unlock on a fresh
		// bounded context, then an unconditional close — the original
		// error stays primary.
		t.Fatalf("read the guard backend pid for the Postgres integration test lock: %v; %s", err, unwindFailedAcquisition(conn))
	}

	t.Cleanup(func() {
		// Same SESSION as the acquire: the unlock can neither no-op nor
		// leak. Both the statement error and an UNOWNED unlock (the
		// boolean — the session not holding the key it acquired) are
		// reported; neither skips the close.
		uctx, ucancel := context.WithTimeout(context.Background(), 10*time.Second)
		var unlocked bool
		unlockErr := conn.QueryRow(uctx, "SELECT pg_advisory_unlock($1)", integrationTestLockKey).Scan(&unlocked)
		ucancel()
		if unlockErr != nil {
			t.Errorf("release Postgres integration test lock: %v", unlockErr)
		} else if !unlocked {
			t.Errorf("release Postgres integration test lock: pg_advisory_unlock reported the guard session did not own the key")
		}
		// The session is GUARD-owned: it is closed, never released back to
		// the caller's pool — and the close runs even after an unlock
		// error.
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		closeErr := conn.Close(cctx)
		ccancel()
		if closeErr != nil {
			t.Errorf("close the Postgres integration test lock guard session: %v", closeErr)
		}
	})
}

// unwindFailedAcquisition is the post-hijack failure unwind shared by every
// failure AFTER the session was hijacked (advisory-lock error/timeout,
// guard-pid read failure): a best-effort same-session unlock on a fresh
// bounded context — hardening the ambiguous grant-at-cancel race, where the
// server grants the advisory lock in the instant before the client's
// cancellation lands and the session would otherwise keep the key silently
// (the run-3313 late "you don't own a lock of type ExclusiveLock" warning
// class) — followed by an UNCONDITIONAL bounded close of the guard-owned
// session. The returned note describes the outcome for the failure message;
// the caller's original error stays primary.
func unwindFailedAcquisition(conn *pgx.Conn) string {
	uctx, ucancel := context.WithTimeout(context.Background(), 10*time.Second)
	var unlocked bool
	unlockErr := conn.QueryRow(uctx, "SELECT pg_advisory_unlock($1)", integrationTestLockKey).Scan(&unlocked)
	ucancel()
	cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
	closeErr := conn.Close(cctx)
	ccancel()
	switch {
	case unlockErr != nil && closeErr != nil:
		return fmt.Sprintf("unwind: best-effort same-session unlock failed: %v; the guard session close also failed: %v", unlockErr, closeErr)
	case unlockErr != nil:
		return fmt.Sprintf("unwind: best-effort same-session unlock failed: %v; the guard session was closed", unlockErr)
	case !unlocked:
		if closeErr != nil {
			return fmt.Sprintf("unwind: the failed acquisition did not own the key; the guard session close failed: %v", closeErr)
		}
		return "unwind: the failed acquisition did not own the key; the guard session was closed"
	default:
		if closeErr != nil {
			return fmt.Sprintf("unwind: the failed acquisition HAD been granted the key and the same-session unlock released it; the guard session close failed: %v", closeErr)
		}
		return "unwind: the failed acquisition HAD been granted the key and the same-session unlock released it before the session was closed"
	}
}

// advisoryLockHolderDiagnostics is the run-3313 triage aid: when the advisory
// acquisition fails, name WHO holds the key instead of only the local
// context deadline. Best-effort and SEPARATELY bounded (5s, fresh context,
// via the caller's pool — the pool itself is healthy when only the lock
// statement timed out): the granted advisory rows for the SPLIT single-key
// encoding (classid = key>>32, objid = key & 0xffffffff, both oid — see the
// live regression) joined to pg_stat_activity for pid, state, wait, timing,
// and a bounded query text. It never masks or materially delays the primary
// error: every failure collapses into a one-line note.
func advisoryLockHolderDiagnostics(pool *pgxpool.Pool) string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := pool.Query(ctx, `
		SELECT l.pid, a.state, a.wait_event_type, a.wait_event,
		       a.backend_start, a.query_start, left(a.query, 120)
		FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype='advisory' AND l.granted
		  AND l.classid::bigint = ($1::bigint >> 32)
		  AND l.objid::bigint = ($1::bigint & 4294967295)`, integrationTestLockKey)
	if err != nil {
		return fmt.Sprintf("holder diagnostics unavailable: %v", err)
	}
	defer rows.Close()
	deref := func(s *string) string {
		if s == nil {
			return "unknown"
		}
		return *s
	}
	var holders []string
	for rows.Next() {
		var pid int
		var state, waitEventType, waitEvent *string
		var backendStart, queryStart *time.Time
		var query *string
		if err := rows.Scan(&pid, &state, &waitEventType, &waitEvent, &backendStart, &queryStart, &query); err != nil {
			return fmt.Sprintf("holder diagnostics scan failed: %v", err)
		}
		waiting := "not waiting"
		if waitEventType != nil {
			waiting = deref(waitEventType) + "/" + deref(waitEvent)
		}
		holder := fmt.Sprintf("held by backend pid %d (state %s, %s", pid, deref(state), waiting)
		if backendStart != nil {
			holder += fmt.Sprintf(", backend up %s", time.Since(*backendStart).Round(time.Second))
		}
		if queryStart != nil {
			holder += fmt.Sprintf(", current query running %s", time.Since(*queryStart).Round(time.Second))
		}
		if query != nil && *query != "" {
			holder += fmt.Sprintf(", query %q", *query)
		}
		holder += ")"
		holders = append(holders, holder)
	}
	if err := rows.Err(); err != nil {
		return fmt.Sprintf("holder diagnostics iteration failed: %v", err)
	}
	if len(holders) == 0 {
		return "holder diagnostics: no granted advisory row for the key at probe time (the holder may have exited between the timeout and this probe)"
	}
	return "holder diagnostics: " + strings.Join(holders, "; ")
}

// lockHolderPID returns the backend pid of the guard session that most
// recently acquired integrationTestLockKey (see integrationTestLockPID). It
// is a minimal unexported helper so the same-package live regression can pin
// the pg_locks identity WITHOUT changing the package's public API.
func lockHolderPID() int {
	return integrationTestLockPID
}
