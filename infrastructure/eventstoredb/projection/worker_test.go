package projection

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/checkpoint"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/dlq"
)

// no-opProject is a ProjectFunc that does nothing — used to satisfy New's
// signature without exercising projection logic.
var noOpProject ProjectFunc = func(_ context.Context, _ *kurrentdb.RecordedEvent) error {
	return nil
}

// Integration tests (against a live KurrentDB instance) live alongside the
// modules that wire a real client. Here we only verify construction wiring
// and the retry-then-DLQ logic that does not require a live subscription.

func TestWorkerNewReturnsNonNil(t *testing.T) {
	var client *kurrentdb.Client // nil is fine — we only assert the constructor wires it.
	w, err := New(client, checkpoint.New(nil), nil, WorkerConfig{
		Name:      "sample1-postgres",
		Prefixes:  []string{"item-"},
		BatchSize: 50,
	}, noOpProject)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if w == nil {
		t.Fatal("New() returned nil")
	}
}

func TestWorkerNewDefaultsBatchSize(t *testing.T) {
	var client *kurrentdb.Client
	w, err := New(client, checkpoint.New(nil), nil, WorkerConfig{
		Name:     "x",
		Prefixes: []string{"x-"},
		// BatchSize intentionally zero — should fall back to the default.
	}, noOpProject)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if w.config.BatchSize != defaultBatchSize {
		t.Fatalf("BatchSize = %d, want default %d", w.config.BatchSize, defaultBatchSize)
	}
}

// TestWorkerNewDefaultsMaxAttempts verifies the retry count defaults to
// defaultMaxAttempts when the caller leaves it at zero. Without this, a
// misconfigured projection could either never retry (MaxAttempts=1) or
// retry forever (MaxAttempts=0 misinterpreted as "no limit").
func TestWorkerNewDefaultsMaxAttempts(t *testing.T) {
	t.Parallel()

	w, err := New(nil, checkpoint.New(nil), nil, WorkerConfig{
		Name:     "x",
		Prefixes: []string{"x-"},
	}, noOpProject)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if w.config.MaxAttempts != defaultMaxAttempts {
		t.Fatalf("MaxAttempts = %d, want default %d", w.config.MaxAttempts, defaultMaxAttempts)
	}
}

// --- projectWithRetry ---

// TestProjectWithRetryReturnsNilOnFirstSuccess verifies that a
// transiently-failing ProjectFunc does NOT need all attempts to succeed —
// the first success wins and the worker continues. This is the
// happy-path for "the SQL was briefly blocked" style failures.
func TestProjectWithRetryReturnsNilOnFirstSuccess(t *testing.T) {
	t.Parallel()

	var calls int
	w := &Worker{
		config: WorkerConfig{Name: "t", MaxAttempts: 5},
		project: func(_ context.Context, _ *kurrentdb.RecordedEvent) error {
			calls++
			return nil // succeeds immediately
		},
	}

	err := w.projectWithRetry(context.Background(), &kurrentdb.RecordedEvent{})
	if err != nil {
		t.Fatalf("projectWithRetry: %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

// TestProjectWithRetrySucceedsOnLaterAttempt verifies the retry-after-
// failure contract: an eventual success is honored and the worker does
// NOT route the event to the DLQ.
func TestProjectWithRetrySucceedsOnLaterAttempt(t *testing.T) {
	t.Parallel()

	var calls int
	w := &Worker{
		config:         WorkerConfig{Name: "t", MaxAttempts: 5},
		retryBaseDelay: time.Microsecond,
		retryMaxDelay:  10 * time.Microsecond,
		project: func(_ context.Context, _ *kurrentdb.RecordedEvent) error {
			calls++
			if calls < 3 {
				return errors.New("transient")
			}
			return nil
		},
	}

	err := w.projectWithRetry(context.Background(), &kurrentdb.RecordedEvent{})
	if err != nil {
		t.Fatalf("projectWithRetry: %v", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3 (succeeds on third attempt)", calls)
	}
}

// TestProjectWithRetryReturnsLastError verifies that when every attempt
// fails, the last error is returned (so the caller can include it in the
// DLQ row). The retry loop must not silently swallow the failure.
func TestProjectWithRetryReturnsLastError(t *testing.T) {
	t.Parallel()

	finalErr := errors.New("permanent poison")
	var calls int
	w := &Worker{
		config:         WorkerConfig{Name: "t", MaxAttempts: 3},
		retryBaseDelay: time.Microsecond,
		retryMaxDelay:  10 * time.Microsecond,
		project: func(_ context.Context, _ *kurrentdb.RecordedEvent) error {
			calls++
			return finalErr
		},
	}

	err := w.projectWithRetry(context.Background(), &kurrentdb.RecordedEvent{})
	if !errors.Is(err, finalErr) {
		t.Fatalf("projectWithRetry err = %v, want %v", err, finalErr)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3 (MaxAttempts)", calls)
	}
}

// TestProjectWithRetryContextCancelStops verifies that a cancelled
// context aborts the retry loop rather than blocking indefinitely.
// Without this property, a worker shutdown would hang waiting for the
// retry delay to elapse.
func TestProjectWithRetryContextCancelStops(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	w := &Worker{
		config:         WorkerConfig{Name: "t", MaxAttempts: 5},
		retryBaseDelay: time.Hour, // absurdly long to force ctx-cancel path
		retryMaxDelay:  2 * time.Hour,
		project: func(_ context.Context, _ *kurrentdb.RecordedEvent) error {
			return errors.New("transient")
		},
	}

	// Cancel after the first attempt fails.
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := w.projectWithRetry(ctx, &kurrentdb.RecordedEvent{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// --- handlePoison ---

// fakeDLQStore is an in-memory dlq.Store for unit-testing handlePoison.
type fakeDLQStore struct {
	flushCalls  int
	flushName   string
	flushCommit uint64
	flushFail   []dlq.Entry
	flushErr    error
}

func (f *fakeDLQStore) FlushCheckpoint(_ context.Context, name string, commit uint64, failures []dlq.Entry) error {
	f.flushCalls++
	f.flushName = name
	f.flushCommit = commit
	f.flushFail = append([]dlq.Entry{}, failures...)
	return f.flushErr
}
func (f *fakeDLQStore) List(_ context.Context, _ string, _ int) ([]dlq.Entry, error) {
	return nil, nil
}
func (f *fakeDLQStore) Delete(_ context.Context, _ int64) error { return nil }

// TestHandlePoisonFlushesAtomically is the headline contract test for the
// DLQ fix: a poison event MUST cause the worker to flush the DLQ entry
// AND the checkpoint advance in one call to dlqs.FlushCheckpoint. The
// implementation guarantees both writes commit in a single Postgres
// transaction; here we verify the worker at least ATTEMPTS the atomic
// flush (rather than two separate calls).
func TestHandlePoisonFlushesAtomically(t *testing.T) {
	t.Parallel()

	fake := &fakeDLQStore{}
	w := &Worker{
		config: WorkerConfig{Name: "test-projection", MaxAttempts: 3},
		dlqs:   fake,
	}

	ev := &kurrentdb.RecordedEvent{
		EventID:      uuid.New(),
		EventType:    "country.registered",
		StreamID:     "country-uuid-1",
		EventNumber:  5,
		Data:         []byte(`{"nameLocal":"Iran"}`),
		UserMetadata: []byte(`{}`),
		Position:     kurrentdb.Position{Commit: 999, Prepare: 998},
	}

	projectErr := errors.New("poison: malformed payload")
	if err := w.handlePoison(context.Background(), ev, projectErr); err != nil {
		t.Fatalf("handlePoison: %v", err)
	}

	if fake.flushCalls != 1 {
		t.Fatalf("FlushCheckpoint called %d times, want 1 (atomic)", fake.flushCalls)
	}
	if fake.flushName != "test-projection" {
		t.Fatalf("flush projection name = %q, want %q", fake.flushName, "test-projection")
	}
	if fake.flushCommit != 999 {
		t.Fatalf("flush commit = %d, want 999 (the event's commit position)", fake.flushCommit)
	}
	if len(fake.flushFail) != 1 {
		t.Fatalf("flush failures = %d entries, want 1", len(fake.flushFail))
	}
	entry := fake.flushFail[0]
	if entry.ProjectionName != "test-projection" {
		t.Fatalf("entry ProjectionName = %q", entry.ProjectionName)
	}
	if entry.EventType != "country.registered" {
		t.Fatalf("entry EventType = %q", entry.EventType)
	}
	if entry.StreamID != "country-uuid-1" {
		t.Fatalf("entry StreamID = %q", entry.StreamID)
	}
	if entry.StreamRevision != 5 {
		t.Fatalf("entry StreamRevision = %d, want 5", entry.StreamRevision)
	}
	if entry.CommitPosition != 999 {
		t.Fatalf("entry CommitPosition = %d, want 999", entry.CommitPosition)
	}
	if entry.Attempts != 3 {
		t.Fatalf("entry Attempts = %d, want 3 (MaxAttempts)", entry.Attempts)
	}
	if entry.Error != projectErr.Error() {
		t.Fatalf("entry Error = %q, want %q", entry.Error, projectErr.Error())
	}
}

func TestPoisonThenValidEventContinuesWorkerFlow(t *testing.T) {
	t.Parallel()

	fake := &fakeDLQStore{}
	poisonErr := errors.New("malformed loading payload")
	w := &Worker{
		config: WorkerConfig{Name: "operations-postgres", MaxAttempts: 1},
		dlqs:   fake,
		project: func(_ context.Context, ev *kurrentdb.RecordedEvent) error {
			if ev.EventType == "bijak.loaded" {
				return poisonErr
			}
			return nil
		},
	}

	poison := &kurrentdb.RecordedEvent{
		EventID:     uuid.New(),
		EventType:   "bijak.loaded",
		StreamID:    "bijak-poison",
		EventNumber: 1,
		Position:    kurrentdb.Position{Commit: 10, Prepare: 9},
	}
	if err := w.projectWithRetry(context.Background(), poison); err == nil {
		t.Fatal("poison projection unexpectedly succeeded")
	} else if err := w.handlePoison(context.Background(), poison, poisonErr); err != nil {
		t.Fatalf("handle poison: %v", err)
	}

	valid := &kurrentdb.RecordedEvent{
		EventID:     uuid.New(),
		EventType:   "bijak.loadingCompleted",
		StreamID:    "bijak-valid",
		EventNumber: 2,
		Position:    kurrentdb.Position{Commit: 11, Prepare: 10},
	}
	if err := w.projectWithRetry(context.Background(), valid); err != nil {
		t.Fatalf("valid event after poison: %v", err)
	}
	if fake.flushCalls != 1 || fake.flushCommit != 10 || len(fake.flushFail) != 1 {
		t.Fatalf("DLQ flush = calls %d commit %d entries %d, want 1/10/1", fake.flushCalls, fake.flushCommit, len(fake.flushFail))
	}
}

// TestHandlePoisonWithoutDLQDoesNotAdvance verifies that a worker with
// no DLQ store CANNOT silently advance past a poison event. The
// previous behavior (log-and-advance) lost events permanently; the fix
// is to return an error and stop the subscription so operators
// investigate.
//
// Deployments that want to opt back into log-and-advance must configure
// a DLQ store (and then explicitly drain or reject entries they don't
// care about).
func TestHandlePoisonWithoutDLQDoesNotAdvance(t *testing.T) {
	t.Parallel()

	w := &Worker{
		config: WorkerConfig{Name: "test-no-dlq", MaxAttempts: 3},
		dlqs:   nil, // no DLQ store
	}

	ev := &kurrentdb.RecordedEvent{
		EventID:   uuid.New(),
		EventType: "country.registered",
		StreamID:  "country-uuid",
		Position:  kurrentdb.Position{Commit: 100, Prepare: 99},
	}
	projectErr := errors.New("permanent")

	err := w.handlePoison(context.Background(), ev, projectErr)
	if err == nil {
		t.Fatal("handlePoison without DLQ: want error, got nil (must NOT silently advance)")
	}
}

// TestHandlePoisonFlushFailurePropagates verifies that when the DLQ
// store itself fails (e.g. Postgres down), handlePoison returns the
// error so runOnce stops the subscription rather than advancing past
// the event. This is the fail-safe property: we never silently lose an
// event even when the recovery mechanism is down.
func TestHandlePoisonFlushFailurePropagates(t *testing.T) {
	t.Parallel()

	flushErr := errors.New("postgres connection refused")
	fake := &fakeDLQStore{flushErr: flushErr}
	w := &Worker{
		config: WorkerConfig{Name: "t", MaxAttempts: 3},
		dlqs:   fake,
	}

	ev := &kurrentdb.RecordedEvent{
		EventID:   uuid.New(),
		EventType: "x.y",
		StreamID:  "x-uuid",
		Position:  kurrentdb.Position{Commit: 1, Prepare: 1},
	}

	err := w.handlePoison(context.Background(), ev, errors.New("poison"))
	if !errors.Is(err, flushErr) {
		t.Fatalf("err = %v, want wrapped flushErr", err)
	}
}

// TestProjectWithRetryRespectsMaxAttemptsBoundary verifies the
// off-by-one: MaxAttempts=N means exactly N calls, not N-1 retries plus
// the initial call (=N+1 total). This guards against a regression that
// would silently under- or over-retry.
func TestProjectWithRetryRespectsMaxAttemptsBoundary(t *testing.T) {
	t.Parallel()

	cases := []int{1, 2, 5}
	for _, max := range cases {
		max := max
		t.Run(itoa(max), func(t *testing.T) {
			t.Parallel()
			var calls int
			w := &Worker{
				config:         WorkerConfig{Name: "t", MaxAttempts: max},
				retryBaseDelay: time.Microsecond,
				retryMaxDelay:  10 * time.Microsecond,
				project: func(_ context.Context, _ *kurrentdb.RecordedEvent) error {
					calls++
					return errors.New("fail")
				},
			}
			_ = w.projectWithRetry(context.Background(), &kurrentdb.RecordedEvent{})
			if calls != max {
				t.Fatalf("calls = %d, want exactly %d (MaxAttempts boundary)", calls, max)
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var sign string
	if n < 0 {
		sign = "-"
		n = -n
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return sign + string(digits)
}

func TestMatchesPrefix(t *testing.T) {
	tests := []struct {
		name     string
		prefixes []string
		stream   string
		want     bool
	}{
		{"matching", []string{"bol-", "order-"}, "bol-04d3462e", true},
		{"second prefix", []string{"bol-", "order-"}, "order-9f1a", true},
		{"non-matching", []string{"bol-", "order-"}, "country-7", false},
		{"system stream", []string{"bol-", "order-"}, "$streams", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Worker{config: WorkerConfig{Prefixes: tt.prefixes}}
			if got := w.matchesPrefix(tt.stream); got != tt.want {
				t.Fatalf("matchesPrefix(%q) = %v, want %v", tt.stream, got, tt.want)
			}
		})
	}
}

func TestValidateRejectsEmptyPrefixString(t *testing.T) {
	if err := (WorkerConfig{Name: "x", Prefixes: []string{"bol-", ""}}).Validate(); err == nil {
		t.Fatal("Validate() accepted a prefix list containing an empty string (would match every stream)")
	}
	if err := (WorkerConfig{Name: "x", Prefixes: []string{"bol-"}}).Validate(); err != nil {
		t.Fatalf("Validate() rejected a valid prefix list: %v", err)
	}
}
