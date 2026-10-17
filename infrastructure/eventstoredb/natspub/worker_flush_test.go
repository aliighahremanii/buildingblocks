package natspub

// worker_flush_test.go — direct regression coverage for the terminal
// position flush (D131): the C05 E2E defect was that on graceful
// cancellation the terminal flush ran through the ALREADY-CANCELLED run
// context, the Postgres save returned context.Canceled, Run swallowed
// the error, and `es_nats_published_positions` stayed empty — the
// restart replayed from the old position and duplicates escaped the
// JetStream dedup window. The fix follows the projection workers'
// detached bounded terminal-flush pattern: the terminal flush runs on
// context.Background() + a timeout, and a raced batch save is guarded
// against double-counting the accumulating events_published counter.

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aliighahremanii/buildingblocks/infrastructure/metrics"
)

// newFlushWorker builds a worker with the recording store wired in.
func newFlushWorker(store *fakePositionStore) *Worker {
	return New(nil, store, &fakePublisher{}, nil, WorkerConfig{
		Name:     "flush-terminal",
		Prefixes: []string{"x-"},
	})
}

// TestFlushTerminalPersistsAfterCancelledRunContext is THE unit-level
// defect regression, with the defect mechanism contrasted directly:
// with the run context already cancelled (graceful shutdown), flushing
// through the run context fails with context.Canceled and persists
// nothing (what the C05 E2E observed — the position row stayed empty),
// while the terminal flush runs DETACHED and its Save observes a live
// context, persists the exact boundary + delta, and advances the
// position metric.
func TestFlushTerminalPersistsAfterCancelledRunContext(t *testing.T) {
	// The metric is a global label-less gauge: no t.Parallel here.
	store := &fakePositionStore{honorCtx: true}
	w := newFlushWorker(store)
	w.flushTimeout = 2 * time.Second

	runCtx, cancel := context.WithCancel(context.Background())
	cancel() // graceful shutdown already happened

	// Defect contrast — the OLD behavior (flush through the run ctx):
	// the ctx-honoring store fails exactly like real Postgres does on a
	// cancelled context, and nothing is persisted.
	if err := w.attemptFlush(runCtx, 4242, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("attemptFlush through the cancelled run ctx = %v, want context.Canceled (the defect mechanism)", err)
	}
	if store.saveCalls != 1 || store.pos != 0 || store.published != 0 {
		t.Fatalf("state after cancelled-ctx flush: calls=%d pos=%d published=%d, want 1/0/0 (nothing persists)", store.saveCalls, store.pos, store.published)
	}

	// The fix — the terminal flush detached from the run context.
	before := promtestutil.ToFloat64(metrics.NATSPosition)
	if err := w.flushTerminal(4242, 3); err != nil {
		t.Fatalf("flushTerminal: %v", err)
	}
	if store.saveCalls != 2 {
		t.Fatalf("save calls = %d, want 2 (failed cancelled-ctx save + detached terminal save)", store.saveCalls)
	}
	if store.lastSaveCtxErr != nil {
		t.Fatalf("terminal save ran on a cancelled context (%v) — the flush context must be detached from the run context", store.lastSaveCtxErr)
	}
	if store.lastGetCtxErr != nil {
		t.Fatalf("terminal flush's guard re-read ran on a cancelled context (%v) — the flush context must be detached", store.lastGetCtxErr)
	}
	if store.pos != 4242 || store.published != 3 {
		t.Fatalf("persisted pos=%d published=%d, want 4242/3 (exact boundary + exact delta)", store.pos, store.published)
	}
	if got := promtestutil.ToFloat64(metrics.NATSPosition); got != 4242 {
		t.Fatalf("NATSPosition = %v after terminal flush, want 4242 (advanced only by the confirmed save; was %v)", got, before)
	}
}

// TestFlushTerminalZeroDeltaIsNoOp pins that an idle shutdown (nothing
// published since the last save — the common graceful-cancel case)
// performs no read, no write, and no metric churn.
func TestFlushTerminalZeroDeltaIsNoOp(t *testing.T) {
	t.Parallel()

	store := &fakePositionStore{honorCtx: true}
	w := newFlushWorker(store)

	if err := w.flushTerminal(4242, 0); err != nil {
		t.Fatalf("zero-delta terminal flush must be a no-op, got %v", err)
	}
	if store.getCalls != 0 || store.saveCalls != 0 {
		t.Fatalf("guard reads = %d, saves = %d, want 0/0 (no side effects on an idle shutdown)", store.getCalls, store.saveCalls)
	}
}

// TestFlushTerminalRacedSaveCommittedSkipsDoubleCount pins the
// raced-save guard: a batch save that raced shutdown committed
// server-side (the worker observed context.Canceled but the row moved),
// so the terminal flush must NOT save again — the store ADDS
// events_published, and a second save of the same delta would
// double-count the accumulating counter (the D114-R1 exact-delta
// contract). Only the metric mirrors the confirmed boundary.
func TestFlushTerminalRacedSaveCommittedSkipsDoubleCount(t *testing.T) {
	// Global metric assertions: no t.Parallel here.
	store := &fakePositionStore{honorCtx: true, pos: 4242, published: 3} // the raced batch save committed
	w := newFlushWorker(store)

	if err := w.flushTerminal(4242, 3); err != nil {
		t.Fatalf("flushTerminal after a committed raced save: %v", err)
	}
	if store.saveCalls != 0 {
		t.Fatalf("saves = %d, want 0 — the boundary is already durable; a re-save would double-count events_published", store.saveCalls)
	}
	if store.published != 3 {
		t.Fatalf("accumulated counter = %d, want exactly 3 (never double-counted)", store.published)
	}
	if got := promtestutil.ToFloat64(metrics.NATSPosition); got != 4242 {
		t.Fatalf("NATSPosition = %v, want 4242 (metric mirrors the confirmed boundary)", got)
	}
}

// TestFlushTerminalRacedSaveNotCommittedFlushes pins the other side of
// the raced-save race: the batch save did NOT commit (the usual case —
// pgx aborts the statement on the cancelled context), so the guard's
// re-read observes the old position and the terminal flush persists the
// boundary as the FIRST save for that delta (exactly once).
func TestFlushTerminalRacedSaveNotCommittedFlushes(t *testing.T) {
	t.Parallel()

	store := &fakePositionStore{honorCtx: true, pos: 4000, published: 1} // raced save did not commit
	w := newFlushWorker(store)

	if err := w.flushTerminal(4242, 3); err != nil {
		t.Fatalf("flushTerminal after an uncommitted raced save: %v", err)
	}
	if store.saveCalls != 1 {
		t.Fatalf("saves = %d, want exactly 1 (the delta's first and only save)", store.saveCalls)
	}
	if store.pos != 4242 || store.published != 4 {
		t.Fatalf("persisted pos=%d published=%d, want 4242/4 (1 prior + 3 flushed exactly once)", store.pos, store.published)
	}
}

// TestFlushTerminalReReadFailurePropagatesAndLogs: if the guard's
// detached re-read itself fails (Postgres down at shutdown), the flush
// must surface and LOG the failure (never swallow it silently) and not
// attempt a blind save — the durable position is preserved, the tail
// replays on resume, JetStream dedups within its window.
func TestFlushTerminalReReadFailurePropagatesAndLogs(t *testing.T) {
	logs := captureLogs(t) // mutates the default logger: no t.Parallel

	store := &fakePositionStore{honorCtx: true, getErr: errors.New("pg down")}
	w := newFlushWorker(store)

	err := w.flushTerminal(4242, 3)
	if err == nil || !errors.Is(err, store.getErr) {
		t.Fatalf("flushTerminal re-read failure = %v, want the store error wrapped", err)
	}
	if !strings.Contains(err.Error(), "natspub terminal flush re-read") {
		t.Fatalf("error must identify the re-read boundary, got %q", err.Error())
	}
	if store.saveCalls != 0 {
		t.Fatalf("saves = %d, want 0 — a failed re-read must not fall through to a blind save", store.saveCalls)
	}
	if !strings.Contains(logs.String(), "terminal position flush could not re-read") {
		t.Fatalf("the re-read failure must be logged, got:\n%s", logs.String())
	}
}

// TestFlushTerminalSaveFailurePropagatesAndLogs: a failed terminal save
// is logged (the swallowed-error half of the C05 defect — Run discards
// the attempt error on a cancelled run context, so the log is the only
// shutdown-time operator signal) and the position metric never advances
// without a confirmed save.
func TestFlushTerminalSaveFailurePropagatesAndLogs(t *testing.T) {
	logs := captureLogs(t) // mutates the default logger: no t.Parallel

	store := &fakePositionStore{honorCtx: true, saveErr: errors.New("pg down")}
	w := newFlushWorker(store)

	before := promtestutil.ToFloat64(metrics.NATSPosition)
	err := w.flushTerminal(4242, 3)
	if err == nil || !errors.Is(err, store.saveErr) {
		t.Fatalf("flushTerminal save failure = %v, want the store error wrapped", err)
	}
	if !strings.Contains(err.Error(), "natspub position save") {
		t.Fatalf("error must be the wrapped save error, got %q", err.Error())
	}
	if !strings.Contains(logs.String(), "terminal position flush failed") {
		t.Fatalf("the flush failure must be logged, got:\n%s", logs.String())
	}
	if got := promtestutil.ToFloat64(metrics.NATSPosition); got != before {
		t.Fatalf("NATSPosition moved to %v after a FAILED terminal save; want unchanged %v", got, before)
	}
}

// TestFlushTerminalBoundedByTimeout proves the flush context is BOUNDED:
// a store call that never completes is released by the timeout, so a
// wedged shutdown cannot hang process exit forever (the projection
// workers' defaultFlushTimeout bound, mirrored here).
func TestFlushTerminalBoundedByTimeout(t *testing.T) {
	store := &fakePositionStore{honorCtx: true, blockOnSave: true}
	w := newFlushWorker(store)
	w.flushTimeout = 50 * time.Millisecond

	start := time.Now()
	err := w.flushTerminal(4242, 3)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("flushTerminal must fail when the store never completes")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("flushTerminal error = %v, want context.DeadlineExceeded (the flush timeout)", err)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("flushTerminal took %s; the flush timeout must bound it", elapsed)
	}
	if store.saveCalls != 1 || store.lastSaveCtxErr != context.DeadlineExceeded {
		t.Fatalf("blocked save observed calls=%d ctxErr=%v, want the deadline to release it", store.saveCalls, store.lastSaveCtxErr)
	}
}

// TestWorkerNewSetsFlushTimeoutDefault pins the constructor default and
// its test override point.
func TestWorkerNewSetsFlushTimeoutDefault(t *testing.T) {
	t.Parallel()

	w := New(nil, &fakePositionStore{}, &fakePublisher{}, nil, WorkerConfig{Name: "t", Prefixes: []string{"x-"}})
	if w.flushTimeout != defaultFlushTimeout {
		t.Fatalf("flushTimeout = %s, want default %s", w.flushTimeout, defaultFlushTimeout)
	}
	if defaultFlushTimeout != 10*time.Second {
		t.Fatalf("defaultFlushTimeout drifted: %s (mirrors the projection worker's bound)", defaultFlushTimeout)
	}
}
