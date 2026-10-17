package projection

// integration_test.go — LIVE integration coverage (real KurrentDB +
// real Postgres) for the projection worker's resilience contract. Tests
// auto-skip when KURRENTDB_URL / POSTGRES_DSN are not configured, per the
// repo integration-test policy; the integration CI job runs them against
// ephemeral services.
//
// Covered (each against the real server and stores):
//
//   - events flow through a live subscription and the checkpoint persists
//   - a NEW worker Run resumes from the persisted checkpoint without
//     reprojecting already-persisted events
//   - cancelling the worker context persists unflushed progress through
//     the terminal flush (bounded, detached context)
//   - an explicit subscription drop (Close) leads to a same-client
//     resubscribe FROM THE PERSISTED CHECKPOINT, and the resubscribed
//     worker projects subsequent events
//
// The pinned-client poison semantics (discovery exhaustion permanently
// closing the shared client) are pinned deterministically WITHOUT a live
// server by client_pinned_behavior_test.go in the parent package.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	bbesdb "github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/checkpoint"
	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/kurrenttest"
	pgdlq "github.com/aliighahremanii/buildingblocks/infrastructure/postgres/dlq"
	"github.com/aliighahremanii/buildingblocks/infrastructure/postgres/postgrestest"
)

// liveEnv opens the live KurrentDB client and Postgres pool, serializes
// DB access, and returns the checkpoint/DLQ stores. The client is opened
// FIRST: kurrenttest.OpenClient loads the repo .env (walking up from the
// test dir), which also populates POSTGRES_DSN for the dev stack before
// postgrestest.OpenPool reads it.
func liveEnv(t *testing.T) (*kurrentdb.Client, *checkpoint.Store, *pgdlq.Store) {
	t.Helper()
	client := kurrenttest.OpenClient(t)
	pool := postgrestest.OpenPool(t)
	postgrestest.Lock(t, pool)
	checks := checkpoint.New(pool)
	dlqs := pgdlq.New(pool)
	return client, checks, dlqs
}

func cleanupProjectionRows(t *testing.T, checks *checkpoint.Store, name string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = checks.Save(ctx, name, 0)
	})
}

// tailCommit returns the current $all tail commit position so tests can
// seed a checkpoint immediately BEFORE appending their events — the
// subscription then starts near the tail instead of scanning the whole
// history (the dev database carries a long $all).
func tailCommit(t *testing.T, client *kurrentdb.Client) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rd, err := client.ReadAll(ctx, kurrentdb.ReadAllOptions{
		Direction: kurrentdb.Backwards,
		From:      kurrentdb.End{},
	}, 1)
	if err != nil {
		t.Fatalf("ReadAll tail: %v", err)
	}
	defer rd.Close()
	we, err := rd.Recv()
	if err != nil || we.Event == nil {
		t.Fatalf("read tail event: event=%v err=%v", we, err)
	}
	return we.Event.Position.Commit
}

// appendLive appends one event to prefix-main and returns its commit
// position.
func appendLive(t *testing.T, client *kurrentdb.Client, stream, eventType string) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := client.AppendToStream(ctx, stream,
		kurrentdb.AppendToStreamOptions{StreamState: kurrentdb.Any{}},
		kurrentdb.EventData{EventID: uuid.New(), EventType: eventType, ContentType: kurrentdb.ContentTypeJson, Data: []byte(`{}`)})
	if err != nil {
		t.Fatalf("append %s: %v", eventType, err)
	}
	return res.CommitPosition
}

// runWorkerAsync starts worker.Run in the background and returns a stop
// function that cancels it and waits (once) for Run to return; the
// registered cleanup performs the same stop if the test did not, without
// double-waiting on the single-value done channel.
func runWorkerAsync(t *testing.T, w *Worker) (stop func() error) {
	t.Helper()
	ctx, cancelCtx := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	var once sync.Once
	var runErr error
	stop = func() error {
		once.Do(func() {
			cancelCtx()
			select {
			case runErr = <-done:
			case <-time.After(20 * time.Second):
				buf := make([]byte, 1<<20)
				n := runtime.Stack(buf, true)
				fmt.Fprintf(os.Stderr, "RUN-DID-NOT-RETURN worker=%s state=%s err=%q\n%s\n",
					w.config.Name, w.Status().State, w.Status().LastError, buf[:n])
				runErr = fmt.Errorf("worker Run did not return after cancel")
			}
		})
		return runErr
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func waitForCount(t *testing.T, counter *atomic.Int64, want int64, within time.Duration, what string) {
	t.Helper()
	deadline := time.After(within)
	for {
		if counter.Load() >= want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("%s: projected %d events, want >= %d within %s", what, counter.Load(), want, within)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func waitState(t *testing.T, w *Worker, want State, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		if w.Status().State == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("worker state = %s, want %s within %s", w.Status().State, want, within)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestLiveWorkerProjectsEventsAndPersistsCheckpoint covers the base live
// contract: a fresh worker subscribes from a seeded checkpoint position,
// receives only the events appended after it, projects them, and the
// checkpoint store persists progress at least up to the last projected
// event's commit position.
func TestLiveWorkerProjectsEventsAndPersistsCheckpoint(t *testing.T) {
	client, checks, dlqs := liveEnv(t)
	name := "live-proj-" + uuid.NewString()[:8]
	prefix := "testliveproj-" + uuid.NewString()[:8]
	stream := prefix + "-main"
	cleanupProjectionRows(t, checks, name)

	// Seed the checkpoint at the current tail so the subscription starts
	// immediately before our events.
	tail := tailCommit(t, client)
	if err := checks.Save(context.Background(), name, tail); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	var projected atomic.Int64
	w, err := New(client, checks, dlqs, WorkerConfig{
		Name: name, Prefixes: []string{prefix}, BatchSize: 1, MaxAttempts: 2,
	}, func(_ context.Context, ev *kurrentdb.RecordedEvent) error {
		if ev.StreamID == stream {
			projected.Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	stop := runWorkerAsync(t, w)
	waitState(t, w, StateRunning, 20*time.Second)

	last := uint64(0)
	for i := 0; i < 3; i++ {
		last = appendLive(t, client, stream, fmt.Sprintf("live.evt.%d", i))
	}
	waitForCount(t, &projected, 3, 30*time.Second, "project events")

	if err := stop(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}

	persisted, err := checks.Get(context.Background(), name)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if persisted < last {
		t.Fatalf("persisted checkpoint = %d, want >= %d (last projected commit)", persisted, last)
	}
}

// TestLiveWorkerResumesWithoutReprojecting proves the restart-resume
// contract against the real server: after one worker run persisted
// progress, a SECOND worker process (same checkpoint key, same client)
// resumes from the persisted checkpoint and never reprojects the events
// the first run already consumed.
func TestLiveWorkerResumesWithoutReprojecting(t *testing.T) {
	client, checks, dlqs := liveEnv(t)
	name := "live-resume-" + uuid.NewString()[:8]
	prefix := "testliveresume-" + uuid.NewString()[:8]
	stream := prefix + "-main"
	cleanupProjectionRows(t, checks, name)

	tail := tailCommit(t, client)
	if err := checks.Save(context.Background(), name, tail); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	mkWorker := func() (*Worker, *atomic.Int64) {
		var count atomic.Int64
		w, err := New(client, checks, dlqs, WorkerConfig{
			Name: name, Prefixes: []string{prefix}, BatchSize: 1, MaxAttempts: 2,
		}, func(_ context.Context, ev *kurrentdb.RecordedEvent) error {
			if ev.StreamID == stream {
				count.Add(1)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return w, &count
	}

	// Run 1: three events; remember the third append's commit position —
	// the durable boundary run 2 must resume from.
	w1, count1 := mkWorker()
	stop1 := runWorkerAsync(t, w1)
	waitState(t, w1, StateRunning, 20*time.Second)
	run1Last := uint64(0)
	for i := 0; i < 3; i++ {
		run1Last = appendLive(t, client, stream, fmt.Sprintf("resume.one.%d", i))
	}
	waitForCount(t, count1, 3, 30*time.Second, "run 1")
	if err := stop1(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run 1 exit: %v", err)
	}

	// Run 2 must not start before the persisted checkpoint has reached the
	// third run-1 commit: the batch/terminal flush can still be in flight
	// after Run returns, and a run 2 resuming from a stale checkpoint
	// reprojects the run-1 events (CI run 3080). Poll the store until the
	// durable position passes the target instead of sleeping a fixed
	// settling window.
	deadline := time.After(10 * time.Second)
	for {
		persisted, err := checks.Get(context.Background(), name)
		if err != nil {
			t.Fatalf("read checkpoint after run 1: %v", err)
		}
		if persisted >= run1Last {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("run-1 checkpoint = %d, want >= %d (third run-1 commit) within 10s", persisted, run1Last)
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Run 2: fresh worker, same checkpoint key — two more events.
	w2, count2 := mkWorker()
	stop2 := runWorkerAsync(t, w2)
	waitState(t, w2, StateRunning, 20*time.Second)
	for i := 0; i < 2; i++ {
		appendLive(t, client, stream, fmt.Sprintf("resume.two.%d", i))
	}
	waitForCount(t, count2, 2, 30*time.Second, "run 2")

	// Settle: run 2 must NEVER see the three run-1 events (resume from the
	// persisted checkpoint is exclusive of already-consumed positions).
	time.Sleep(2 * time.Second)
	if got := count2.Load(); got != 2 {
		t.Fatalf("run 2 projected %d events, want exactly 2 (no reprocessing of run-1 events)", got)
	}
	if err := stop2(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("run 2 exit: %v", err)
	}
}

// TestLiveTerminalFlushPersistsUnflushedProgress proves the shutdown-flush
// contract against the real stores: with batching too large to flush
// incrementally, cancelling the worker context persists the projected
// progress anyway (through the bounded detached-context flush), so a
// restart resumes at the last projected event rather than the last batch.
func TestLiveTerminalFlushPersistsUnflushedProgress(t *testing.T) {
	client, checks, dlqs := liveEnv(t)
	name := "live-terminal-" + uuid.NewString()[:8]
	prefix := "testliveterm-" + uuid.NewString()[:8]
	stream := prefix + "-main"
	cleanupProjectionRows(t, checks, name)

	tail := tailCommit(t, client)
	if err := checks.Save(context.Background(), name, tail); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	var projected atomic.Int64
	w, err := New(client, checks, dlqs, WorkerConfig{
		Name: name, Prefixes: []string{prefix}, BatchSize: 10000, MaxAttempts: 2,
	}, func(_ context.Context, ev *kurrentdb.RecordedEvent) error {
		if ev.StreamID == stream {
			projected.Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	stop := runWorkerAsync(t, w)
	waitState(t, w, StateRunning, 20*time.Second)

	last := uint64(0)
	for i := 0; i < 2; i++ {
		last = appendLive(t, client, stream, fmt.Sprintf("term.%d", i))
	}
	waitForCount(t, &projected, 2, 30*time.Second, "project before cancel")

	// No batch flush has run (batch is huge): the checkpoint must still be
	// the seeded tail, then the terminal flush must persist to `last`.
	if got, _ := checks.Get(context.Background(), name); got != tail {
		t.Fatalf("checkpoint = %d before cancel, want seeded tail %d (no batch flush expected)", got, tail)
	}

	if err := stop(); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}

	persisted, err := checks.Get(context.Background(), name)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	if persisted < last {
		t.Fatalf("persisted checkpoint = %d after terminal flush, want >= %d — shutdown progress was lost", persisted, last)
	}
}

// TestLiveWorkerResubscribesAfterExplicitDrop proves the drop-recovery
// contract end to end on the real server: closing the live subscription
// (the pinned client returns a drop to a blocked Recv) makes the worker
// resubscribe on the SAME shared client FROM THE PERSISTED CHECKPOINT,
// and the resubscribed worker projects events appended after the drop.
func TestLiveWorkerResubscribesAfterExplicitDrop(t *testing.T) {
	client, checks, dlqs := liveEnv(t)
	name := "live-drop-" + uuid.NewString()[:8]
	prefix := "testlivedrop-" + uuid.NewString()[:8]
	stream := prefix + "-main"
	cleanupProjectionRows(t, checks, name)

	tail := tailCommit(t, client)
	if err := checks.Save(context.Background(), name, tail); err != nil {
		t.Fatalf("seed checkpoint: %v", err)
	}

	var projected atomic.Int64
	w, err := New(client, checks, dlqs, WorkerConfig{
		Name: name, Prefixes: []string{prefix}, BatchSize: 1, MaxAttempts: 2,
	}, func(_ context.Context, ev *kurrentdb.RecordedEvent) error {
		if ev.StreamID == stream {
			projected.Add(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Capture each live subscription the worker opens; record the From
	// position and allow the test to close them explicitly.
	var mu sync.Mutex
	var subs []*kurrentdb.Subscription
	var froms []uint64
	inner := w.subscribeF
	w.subscribeF = func(c *kurrentdb.Client, ctx context.Context, opts kurrentdb.SubscribeToAllOptions) (recvStream, error) {
		s, err := inner(c, ctx, opts)
		if err != nil {
			return nil, err
		}
		live, ok := s.(*kurrentdb.Subscription)
		if !ok {
			return nil, fmt.Errorf("expected *kurrentdb.Subscription, got %T", s)
		}
		mu.Lock()
		subs = append(subs, live)
		froms = append(froms, commitOf(opts.From))
		mu.Unlock()
		return s, nil
	}

	stop := runWorkerAsync(t, w)
	waitState(t, w, StateRunning, 20*time.Second)

	appendLive(t, client, stream, "drop.before")
	waitForCount(t, &projected, 1, 30*time.Second, "event before drop")

	// Persisted checkpoint is now at the projected event (batch size 1).
	persistedBefore, err := checks.Get(context.Background(), name)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}

	// Explicit drop: close the live subscription out from under the
	// worker — the same signal a server-side drop produces.
	mu.Lock()
	first := subs[0]
	mu.Unlock()
	if err := first.Close(); err != nil {
		t.Fatalf("explicit subscription close: %v", err)
	}

	// The worker must resubscribe (same client) and stay functional.
	waitState(t, w, StateRunning, 30*time.Second)
	appendLive(t, client, stream, "drop.after")
	waitForCount(t, &projected, 2, 30*time.Second, "event after drop")

	mu.Lock()
	defer mu.Unlock()
	if len(subs) < 2 {
		t.Fatalf("subscriptions opened = %d, want >= 2 after explicit drop", len(subs))
	}
	if froms[1] < persistedBefore {
		t.Fatalf("resubscribe From = %d, want >= persisted checkpoint %d", froms[1], persistedBefore)
	}
	if subs[0] == subs[1] {
		t.Fatal("worker reused the closed subscription object instead of opening a new one")
	}
	_ = stop()
}

// TestLiveEstablishmentFailureAgainstUnreachableServer pins the failure
// path against a real (unreachable) endpoint: the bounded establishment
// either fails fast (transport) or times out; either way the worker
// keeps retrying and stays observably down — never hangs, never exits.
func TestLiveEstablishmentFailureAgainstUnreachableServer(t *testing.T) {
	_, checks, dlqs := liveEnv(t) // live Postgres; the bad client below is the endpoint under test
	name := "live-unreachable-" + uuid.NewString()[:8]
	cleanupProjectionRows(t, checks, name)

	// A second client pointed at a guaranteed-closed port.
	badClient, err := bbesdb.OpenClient(context.Background(), bbesdb.Config{
		URL: "kurrentdb://127.0.0.1:1?tls=false",
	})
	if err != nil {
		t.Fatalf("open bad client: %v", err)
	}
	t.Cleanup(func() { badClient.Close() })

	w, err := New(badClient, checks, dlqs, WorkerConfig{
		Name: name, Prefixes: []string{"nonsense-"}, BatchSize: 10, MaxAttempts: 2,
		EstablishmentTimeout: 2 * time.Second,
	}, func(_ context.Context, _ *kurrentdb.RecordedEvent) error { return nil })
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// The worker must be observable as down/connecting (never running).
	deadline := time.After(8 * time.Second)
	sawDown := false
	for !sawDown {
		if s := w.Status(); s.State == StateDown || s.State == StateConnecting {
			if s.State == StateDown {
				sawDown = true
			}
		}
		select {
		case <-deadline:
			if !sawDown {
				t.Fatalf("worker state = %s, want an observed 'down' against the unreachable server", w.Status().State)
			}
		case <-time.After(50 * time.Millisecond):
		case err := <-done:
			t.Fatalf("Run exited early with %v; it must keep retrying", err)
		}
	}

	// Cancellation still ends Run promptly (no hang in establishment).
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel while server unreachable")
	}
}
