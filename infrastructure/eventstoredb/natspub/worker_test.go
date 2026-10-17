package natspub

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// fakePublisher records every PublishAsync call so tests can assert on
// the subject, data, and msgID the worker produced.
type fakePublisher struct {
	mu        sync.Mutex
	published []publishedMsg
	failWith  error
	// failOnMsgID, when non-empty, fails ONLY the publish carrying that
	// Nats-Msg-Id (the mid-batch failure injection of transform_test.go).
	failOnMsgID string
}

type publishedMsg struct {
	subject string
	data    []byte
	msgID   string
}

func (f *fakePublisher) PublishAsync(_ context.Context, subject string, data []byte, msgID string) error {
	f.mu.Lock()
	if f.failOnMsgID != "" && msgID == f.failOnMsgID {
		f.mu.Unlock()
		return errors.New("injected mid-batch publish failure for " + msgID)
	}
	if f.failWith != nil {
		f.mu.Unlock()
		return f.failWith
	}
	// Copy data so the test sees a stable snapshot even if the caller
	// reuses the underlying buffer.
	d := make([]byte, len(data))
	copy(d, data)
	f.published = append(f.published, publishedMsg{subject: subject, data: d, msgID: msgID})
	f.mu.Unlock()
	return nil
}

func (f *fakePublisher) snapshot() []publishedMsg {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]publishedMsg, len(f.published))
	copy(out, f.published)
	return out
}

// fakePositionStore is an in-memory PositionStore for worker tests.
type fakePositionStore struct {
	mu        sync.Mutex
	pos       uint64
	published uint64
	getErr    error
	saveErr   error
	saveCalls int
	// getCalls counts Get invocations (the terminal flush re-reads the
	// authoritative position before saving — D131).
	getCalls int
	// lastSaveCtxErr/lastGetCtxErr capture ctx.Err() at call time so
	// regression tests can prove WHERE a flush actually ran — the
	// terminal flush must run on a context DETACHED from an
	// already-cancelled run context.
	lastSaveCtxErr error
	lastGetCtxErr  error
	// honorCtx simulates the real Postgres store's context behavior:
	// when true, Get/Save return the context error instead of touching
	// state if their ctx is already cancelled — exactly what pgx does
	// on a cancelled context (the C05 E2E defect mechanism).
	honorCtx bool
	// blockOnSave simulates a store call that never completes until its
	// context is done (then returns the context error): used to prove
	// the terminal flush's timeout bound.
	blockOnSave bool
}

func (f *fakePositionStore) Get(ctx context.Context, _ string) (uint64, error) {
	f.mu.Lock()
	f.getCalls++
	f.lastGetCtxErr = ctx.Err()
	honor, err := f.honorCtx, f.getErr
	pos := f.pos
	f.mu.Unlock()
	if honor && ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return pos, err
}

func (f *fakePositionStore) Save(ctx context.Context, _ string, position uint64, eventsPublished uint64) error {
	f.mu.Lock()
	f.saveCalls++
	f.lastSaveCtxErr = ctx.Err()
	honor, block := f.honorCtx, f.blockOnSave
	err := f.saveErr
	f.mu.Unlock()
	if block {
		<-ctx.Done()
		f.mu.Lock()
		f.lastSaveCtxErr = ctx.Err()
		f.mu.Unlock()
		return ctx.Err()
	}
	if honor && ctx.Err() != nil {
		return ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pos = position
	f.published += eventsPublished
	return err
}

// TestSubjectForDerivesAggregateTypeFromStreamID verifies the NATS
// subject derivation. Subjects drive consumer wildcards, so the shape
// is part of the public contract with downstream consumers.
func TestSubjectForDerivesAggregateTypeFromStreamID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		prefix, streamID, eventType, want string
	}{
		{"yardflow", "country-uuid-1", "country.registered", "yardflow.country.country.registered"},
		{"yardflow", "location-uuid-2", "location.terminal_opened", "yardflow.location.location.terminal_opened"},
		{"yardflow", "item-uuid-3", "item.processed", "yardflow.item.item.processed"},
		{"custom", "note-uuid-4", "note.renamed", "custom.note.note.renamed"},
		// No prefix on stream id (defensive — should not happen in
		// practice but must not panic).
		{"yardflow", "nouuid", "x.y", "yardflow.nouuid.x.y"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.streamID, func(t *testing.T) {
			t.Parallel()
			got := subjectFor(tc.prefix, tc.streamID, tc.eventType)
			if got != tc.want {
				t.Fatalf("subjectFor(%q,%q,%q) = %q, want %q",
					tc.prefix, tc.streamID, tc.eventType, got, tc.want)
			}
		})
	}
}

// TestWorkerNewSetsDefaults verifies the constructor fills in defaults
// for SubjectPrefix and BatchSize so callers can omit them.
func TestWorkerNewSetsDefaults(t *testing.T) {
	t.Parallel()

	w := New(nil, &fakePositionStore{}, &fakePublisher{}, nil, WorkerConfig{
		Name:     "test",
		Prefixes: []string{"x-"},
	})
	if w.config.SubjectPrefix != "yardflow" {
		t.Fatalf("SubjectPrefix = %q, want default %q", w.config.SubjectPrefix, "yardflow")
	}
	if w.config.BatchSize != 50 {
		t.Fatalf("BatchSize = %d, want default 50", w.config.BatchSize)
	}
	if w.initialDelay != defaultInitialRetryDelay || w.maxDelay != defaultMaxRetryDelay {
		t.Fatalf("backoff defaults = %s/%s, want %s/%s",
			w.initialDelay, w.maxDelay, defaultInitialRetryDelay, defaultMaxRetryDelay)
	}
}

// TestWorkerNewHonorsExplicitConfig verifies explicit config is NOT
// overwritten by defaults.
func TestWorkerNewHonorsExplicitConfig(t *testing.T) {
	t.Parallel()

	w := New(nil, &fakePositionStore{}, &fakePublisher{}, nil, WorkerConfig{
		Name:          "test",
		Prefixes:      []string{"x-"},
		SubjectPrefix: "custom",
		BatchSize:     10,
	})
	if w.config.SubjectPrefix != "custom" {
		t.Fatalf("SubjectPrefix = %q, want %q", w.config.SubjectPrefix, "custom")
	}
	if w.config.BatchSize != 10 {
		t.Fatalf("BatchSize = %d, want 10", w.config.BatchSize)
	}
}

// TestRunOncePublishesMatchingEvents and the no-skip publish-failure
// contract are covered LIVE (real KurrentDB + NATS + Postgres, gated on
// their env vars) by TestLiveWorkerPublishesAndAdvancesPosition and
// TestLiveWorkerValidationGateBlocksOnStarSubjects in worker_live_test.go:
// appended events are republished with Nats-Msg-Id = KurrentDB EventID,
// the durable position advances, and a misconfigured stream blocks in the
// validation gate without publishing or advancing anything.
func TestRunOncePublishesMatchingEvents(t *testing.T) {
	t.Skip("covered live by TestLiveWorkerPublishesAndAdvancesPosition (worker_live_test.go)")
}

func TestRunOnceStopsOnPublishFailure(t *testing.T) {
	t.Skip("covered live by TestLiveWorkerPublishesAndAdvancesPosition and the natsadapter no-responder classification tests")
}

// ensure unused imports are referenced for future expansion
var (
	_ = errors.New
	_ = kurrentdb.Start{}
)
