package natspub

// worker_run_test.go — Run-loop behavior coverage (D114): the validation
// gate blocks BEFORE any KurrentDB subscription is opened, retries with
// bounded context-cancellable backoff, logs failures without duplication,
// and the backoff sequence is bounded.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aliighahremanii/buildingblocks/infrastructure/metrics"
)

// syncedBuffer is a mutex-guarded bytes.Buffer: the worker goroutine
// writes log records through the default logger while the test goroutine
// polls String() — unsynchronized access there is a data race under
// -race (same lesson as D109-R2's test-side race).
type syncedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncedBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncedBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// captureLogs swaps the default slog logger for one writing records to a
// buffer and restores it afterwards. Tests using it MUST NOT run in
// parallel (they mutate process-global logging state).
func captureLogs(t *testing.T) *syncedBuffer {
	t.Helper()
	buf := &syncedBuffer{}
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Writer(buf), &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })
	return buf
}

// countingValidator wraps fakeValidator with an atomic counter so the
// retry count is observable while Run loops.
type countingValidator struct {
	fakeValidator
	calls atomic.Int64
}

func (c *countingValidator) Validate(ctx context.Context) error {
	c.calls.Add(1)
	return c.fakeValidator.Validate(ctx)
}

// TestRunValidationGateBlocksSubscription proves the reliability contract:
// while JetStream validation fails, Run NEVER opens a KurrentDB
// subscription (a nil *kurrentdb.Client would panic the moment the worker
// tried — the gate must keep the loop parked in validation), stays
// cancellable, and returns context.Canceled promptly.
//
// Delays are deliberately slow (25ms+) so the observed attempt count stays
// far below the reminder cadence (10) — the dedupe assertions are then
// deterministic rather than timing-sensitive.
func TestRunValidationGateBlocksSubscription(t *testing.T) {
	logs := captureLogs(t)

	validator := &countingValidator{fakeValidator: fakeValidator{
		err: &Error{
			Op:       "validate",
			Kind:     ErrKindSubjectMismatch,
			Stream:   "yardflow-events",
			Detail:   "stream subjects [yardflow.*] do not cover 3 of 3 subject families",
			Guidance: "re-create the stream with subjects 'yardflow.>'",
		},
	}}
	w := New(nil, &fakePositionStore{}, &fakePublisher{}, validator, WorkerConfig{
		Name:     "gate-test",
		Prefixes: []string{"country-", "bol-", "truck-"},
	})
	w.initialDelay = 25 * time.Millisecond
	w.maxDelay = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()

	// Wait until several validation attempts happened, then cancel.
	deadline := time.Now().Add(5 * time.Second)
	for validator.calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	calls := validator.calls.Load()
	if calls < 3 {
		t.Fatalf("validator calls = %d, want >= 3 (retry loop must run)", calls)
	}
	if calls >= reminderEvery {
		t.Fatalf("test timing drifted: %d attempts reached the reminder cadence; raise initialDelay", calls)
	}
	if strings.Contains(logs.String(), "subscription ended") {
		t.Fatalf("worker must not reach the subscription path while validation fails:\n%s", logs.String())
	}

	// Non-duplicative logging: exactly ONE full validation-failure ERROR
	// (below the reminder cadence), carrying the kind and guidance.
	fullFailures := strings.Count(logs.String(), "JetStream validation failed")
	if fullFailures != 1 {
		t.Fatalf("full validation-failure logs = %d, want exactly 1:\n%s", fullFailures, logs.String())
	}
	if !strings.Contains(logs.String(), `guidance="re-create the stream`) {
		t.Fatalf("the failure log must carry the actionable guidance:\n%s", logs.String())
	}
	if !strings.Contains(logs.String(), "kind=subject_mismatch") {
		t.Fatalf("the failure log must carry the error kind:\n%s", logs.String())
	}
}

// TestRunValidationUnclassifiedFailureStillGates verifies an unclassified
// validator error also blocks (defensively) and is logged deduplicated.
// The full-log assertion counts the unique ERROR message text, not a
// shared word, so WARN reminders cannot inflate it.
func TestRunValidationUnclassifiedFailureStillGates(t *testing.T) {
	logs := captureLogs(t)

	validator := &countingValidator{fakeValidator: fakeValidator{
		err: errors.New("transport hiccup"),
	}}
	w := New(nil, &fakePositionStore{}, &fakePublisher{}, validator, WorkerConfig{
		Name:     "gate-test-unclassified",
		Prefixes: []string{"country-"},
	})
	w.initialDelay = 25 * time.Millisecond
	w.maxDelay = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for validator.calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if calls := validator.calls.Load(); calls >= reminderEvery {
		t.Fatalf("test timing drifted: %d attempts reached the reminder cadence; raise initialDelay", calls)
	}
	// Count the exact ERROR message; the WARN reminder uses a different
	// message, so only full logs are counted.
	full := strings.Count(logs.String(),
		"JetStream validation failed (unclassified); KurrentDB subscription NOT opened")
	if full != 1 {
		t.Fatalf("unclassified full failure logs = %d, want 1:\n%s", full, logs.String())
	}
	if strings.Contains(logs.String(), "subscription ended") {
		t.Fatalf("worker must not subscribe while validation fails:\n%s", logs.String())
	}
}

// TestRunNilValidatorSkipsGate verifies nil validator is safe (no
// dereference) — the loop proceeds straight to the subscription attempt,
// which fails fast on the nil client only AFTER the gate, proving gate
// order. The nil-client subscription attempt would panic; instead Run is
// cancelled during the position-load/backoff cycle. We assert only that
// validation is skipped and Run stays cancellable.
func TestRunNilValidatorSkipsGate(t *testing.T) {
	logs := captureLogs(t)

	w := New(nil, &fakePositionStore{getErr: errors.New("pg down")}, &fakePublisher{}, nil, WorkerConfig{
		Name:     "nil-validator-test",
		Prefixes: []string{"country-"},
	})
	w.initialDelay = 25 * time.Millisecond
	w.maxDelay = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "subscription ended") && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if strings.Contains(logs.String(), "JetStream validation failed") {
		t.Fatalf("nil validator must not produce validation logs:\n%s", logs.String())
	}
}

// TestRunBackoffPolicyByAttemptOutcome pins the three reconnect-policy
// classes deterministically via the runAttempt seam (no live KurrentDB):
//
//   - unproductive failure → delay GROWS (bounded exponential)
//   - productive attempt (published > 0) that still ends → delay resets to
//     the initial floor every time (quick retry, never zero — no hot loop)
//   - clean end without publishing → treated as unproductive (grows) and
//     surfaced via the deduped clean-noop warning
func TestRunBackoffPolicyByAttemptOutcome(t *testing.T) {
	cases := []struct {
		name      string
		published uint64
		err       error
		// wantFloor: every inter-attempt gap must be ≥ initialDelay.
		wantFloor bool
		// wantGrowth: gaps must be non-decreasing (backoff grows).
		wantGrowth bool
		// wantLog: message fragment expected in the logs.
		wantLog string
	}{
		{"unproductive failure grows", 0, errors.New("subscribe: boom"), true, true, "subscription ended"},
		{"productive attempt resets to floor", 5, errors.New("subscription dropped"), true, false, "subscription ended"},
		{"clean noop is abnormal", 0, nil, true, true, "ended cleanly without publishing"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLogs(t)

			const attempts = 4
			var mu sync.Mutex
			var starts []time.Time
			w := New(nil, &fakePositionStore{}, &fakePublisher{}, nil, WorkerConfig{
				Name:     "backoff-" + tc.name,
				Prefixes: []string{"country-"},
			})
			w.initialDelay = 25 * time.Millisecond
			w.maxDelay = 80 * time.Millisecond
			w.runAttempt = func(ctx context.Context) (uint64, error) {
				mu.Lock()
				starts = append(starts, time.Now())
				mu.Unlock()
				return tc.published, tc.err
			}

			ctx, cancel := context.WithCancel(context.Background())
			errc := make(chan error, 1)
			go func() { errc <- w.Run(ctx) }()

			deadline := time.Now().Add(5 * time.Second)
			for {
				mu.Lock()
				n := len(starts)
				mu.Unlock()
				if n >= attempts || time.Now().After(deadline) {
					break
				}
				time.Sleep(2 * time.Millisecond)
			}
			cancel()
			select {
			case err := <-errc:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Run returned %v, want context.Canceled", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after cancel")
			}

			mu.Lock()
			defer mu.Unlock()
			if len(starts) < attempts {
				t.Fatalf("attempts = %d, want ≥ %d", len(starts), attempts)
			}
			prev := time.Duration(0)
			for i := 1; i < len(starts); i++ {
				gap := starts[i].Sub(starts[i-1])
				if tc.wantFloor && gap < w.initialDelay {
					t.Fatalf("gap %d→%d = %s < floor %s (hot loop)",
						i-1, i, gap, w.initialDelay)
				}
				if tc.wantGrowth && i > 1 && gap < prev {
					t.Fatalf("gap %d→%d = %s shrank below previous %s (backoff must grow)",
						i-1, i, gap, prev)
				}
				prev = gap
			}
			if !strings.Contains(logs.String(), tc.wantLog) {
				t.Fatalf("logs must contain %q:\n%s", tc.wantLog, logs.String())
			}
		})
	}
}

// TestAttemptFlushExactDeltaAndFailure pins the durable-boundary contract
// (D114-R1): Save receives the EXACT unsaved delta (never an attempt
// total — the store ADDS the value, so an attempt-total would
// double-count across mid-batch flushes), a zero delta performs no save,
// and a failed save propagates as the attempt-ending error.
func TestAttemptFlushExactDeltaAndFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("saves exact delta", func(t *testing.T) {
		t.Parallel()
		store := &fakePositionStore{}
		w := New(nil, store, &fakePublisher{}, nil, WorkerConfig{Name: "flush", Prefixes: []string{"x-"}})

		if err := w.attemptFlush(ctx, 100, 7); err != nil {
			t.Fatalf("save: %v", err)
		}
		if store.pos != 100 || store.published != 7 {
			t.Fatalf("saved pos=%d published=%d, want 100/7", store.pos, store.published)
		}
	})

	t.Run("zero delta is a no-op", func(t *testing.T) {
		t.Parallel()
		store := &fakePositionStore{}
		w := New(nil, store, &fakePublisher{}, nil, WorkerConfig{Name: "flush", Prefixes: []string{"x-"}})

		if err := w.attemptFlush(ctx, 100, 0); err != nil {
			t.Fatalf("zero delta must be a no-op, got %v", err)
		}
		if store.saveCalls != 0 {
			t.Fatalf("save calls = %d, want 0", store.saveCalls)
		}
	})

	t.Run("save failure propagates as attempt-ending error", func(t *testing.T) {
		t.Parallel()
		store := &fakePositionStore{saveErr: errors.New("pg down")}
		w := New(nil, store, &fakePublisher{}, nil, WorkerConfig{Name: "flush", Prefixes: []string{"x-"}})

		err := w.attemptFlush(ctx, 100, 7)
		if err == nil || !strings.Contains(err.Error(), "natspub position save flush") {
			t.Fatalf("expected wrapped save error, got %v", err)
		}
		if !errors.Is(err, store.saveErr) {
			t.Fatal("errors.Is must reach the store error")
		}
	})
}

// TestRunPositionMetricOnlyAfterSuccessfulSave asserts the metric
// contract via the exported gauge: NATSPosition advances only on
// successful saves. (The gauge is global and label-free; the value is
// read through the prometheus testutil.)
func TestRunPositionMetricOnlyAfterSuccessfulSave(t *testing.T) {
	store := &fakePositionStore{saveErr: errors.New("pg down")}
	w := New(nil, store, &fakePublisher{}, nil, WorkerConfig{Name: "metric", Prefixes: []string{"x-"}})

	before := promtestutil.ToFloat64(metrics.NATSPosition)
	if err := w.attemptFlush(context.Background(), 4242, 1); err == nil {
		t.Fatal("expected save failure")
	}
	if got := promtestutil.ToFloat64(metrics.NATSPosition); got != before {
		t.Fatalf("NATSPosition moved to %v after a FAILED save; want unchanged %v", got, before)
	}

	store.saveErr = nil
	if err := w.attemptFlush(context.Background(), 4242, 1); err != nil {
		t.Fatalf("save: %v", err)
	}
	if got := promtestutil.ToFloat64(metrics.NATSPosition); got != 4242 {
		t.Fatalf("NATSPosition = %v after successful save, want 4242", got)
	}
}

// TestNextBackoff pins the bounded exponential sequence.
func TestNextBackoff(t *testing.T) {
	t.Parallel()

	const initial = 100 * time.Millisecond
	const max = time.Second
	cases := []struct {
		current, want time.Duration
	}{
		{0, initial},
		{initial, 200 * time.Millisecond},
		{200 * time.Millisecond, 400 * time.Millisecond},
		{400 * time.Millisecond, 800 * time.Millisecond},
		{800 * time.Millisecond, max},
		{max, max},             // capped
		{5 * time.Second, max}, // above cap stays capped
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.current.String(), func(t *testing.T) {
			t.Parallel()
			if got := nextBackoff(tc.current, initial, max); got != tc.want {
				t.Fatalf("nextBackoff(%s) = %s, want %s", tc.current, got, tc.want)
			}
		})
	}
}

// TestSleepCtxCancellable verifies the backoff wait honors cancellation.
func TestSleepCtxCancellable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if sleepCtx(ctx, time.Minute) {
		t.Fatal("sleepCtx must report cancellation, not sleep")
	}
	if !sleepCtx(context.Background(), 0) {
		t.Fatal("zero sleep on live context must succeed immediately")
	}
}

// TestFailureLogDedupe pins the non-duplicative log contract: first
// occurrence in full, silence for repeats, reminder every reminderEvery-th,
// full again on key change, reset clears.
func TestFailureLogDedupe(t *testing.T) {
	t.Parallel()

	var f failureLog
	full, reminders := 0, 0
	logFull := func() { full++ }
	remind := func(int) { reminders++ }

	for i := 0; i < reminderEvery-1; i++ {
		f.log("same", logFull, remind)
	}
	if full != 1 || reminders != reminderEvery-2 {
		t.Fatalf("after %d identical failures: full=%d reminders=%d, want 1/%d",
			reminderEvery-1, full, reminders, reminderEvery-2)
	}

	f.log("same", logFull, remind) // reminderEvery-th → full again
	if full != 2 {
		t.Fatalf("reminderEvery-th occurrence must log in full: full=%d", full)
	}

	f.log("changed", logFull, remind) // key change → full immediately
	if full != 3 {
		t.Fatalf("key change must log in full: full=%d", full)
	}

	f.reset()
	f.log("changed", logFull, remind) // reset makes the next a first occurrence
	if full != 4 {
		t.Fatalf("after reset the occurrence must log in full: full=%d", full)
	}

	// nil remind callback must not panic.
	f2 := failureLog{}
	f2.log("x", func() {}, nil)
	f2.log("x", func() {}, nil)
}

// TestPublishFailureKeyAndFingerprint pins the dedupe keys.
func TestPublishFailureKeyAndFingerprint(t *testing.T) {
	t.Parallel()

	classified := &Error{Op: "publish", Kind: ErrKindNoResponders, Subject: "yardflow.bol.bol.registered"}
	if got := publishFailureKey(classified, "yardflow.bol.bol.registered"); got != "publish:no_responders:yardflow.bol.bol.registered" {
		t.Fatalf("publishFailureKey = %q", got)
	}
	plain := errors.New("boom")
	if got := publishFailureKey(plain, "s"); got != "publish:s" {
		t.Fatalf("publishFailureKey plain = %q", got)
	}
	wrapped := fmt.Errorf("natspub publish s: %w", classified)
	if got := errFingerprint(wrapped); got != "run:no_responders" {
		t.Fatalf("errFingerprint classified = %q", got)
	}
	if got := errFingerprint(plain); got != "run:boom" {
		t.Fatalf("errFingerprint plain = %q", got)
	}
}
