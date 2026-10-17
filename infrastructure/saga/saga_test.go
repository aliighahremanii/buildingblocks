package saga

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"
)

// fakeStore is an in-memory Store used to exercise the Coordinator's
// dispatch and sweep seams without Postgres. Save enforces the same
// optimistic-concurrency rule as the real implementation so the unit
// tests prove the contract rather than a stub.
type fakeStore struct {
	mu            sync.Mutex
	instances     map[string]Instance
	timeouts      map[string]time.Time // saga_id -> due_at (kind always "")
	saves         int
	scheduleCalls int
	deleteCalls   int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		instances: make(map[string]Instance),
		timeouts:  make(map[string]time.Time),
	}
}

func (s *fakeStore) Load(_ context.Context, sagaID string) (Instance, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.instances[sagaID]
	return in, ok, nil
}

func (s *fakeStore) Save(_ context.Context, in Instance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, exists := s.instances[in.ID]
	if exists {
		// Existing row: stored version must equal the version we are
		// replacing (in.Version - 1).
		if existing.Version != in.Version-1 {
			return ErrConcurrencyConflict
		}
	} else {
		// New row: caller must supply Version 0.
		if in.Version != 0 {
			return ErrConcurrencyConflict
		}
	}
	s.instances[in.ID] = in
	s.saves++
	return nil
}

func (s *fakeStore) ScheduleTimeout(_ context.Context, sagaID, _ string, dueAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.timeouts[sagaID] = dueAt
	s.scheduleCalls++
	return nil
}

func (s *fakeStore) ListDueTimeouts(_ context.Context, before time.Time, limit int) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for id, due := range s.timeouts {
		if !due.After(before) {
			out = append(out, id)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *fakeStore) DeleteTimeout(_ context.Context, sagaID, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.timeouts, sagaID)
	s.deleteCalls++
	return nil
}

// fakeSaga is a minimal Saga[any] that records every React/Timeout call
// and persists its state through the injected Store, exactly as a real
// saga implementation would. It demonstrates the load-transition-save
// pattern the framework expects.
type fakeSaga struct {
	store        Store
	mu           sync.Mutex
	reactCalls   []string // event types seen
	timeoutCalls int
	sagaType     string
}

func (f *fakeSaga) SagaType() string { return f.sagaType }

func (f *fakeSaga) React(ctx context.Context, event *kurrentdb.RecordedEvent) error {
	f.mu.Lock()
	f.reactCalls = append(f.reactCalls, event.EventType)
	f.mu.Unlock()

	sagaID := "fake-" + event.StreamID
	inst, found, err := f.store.Load(ctx, sagaID)
	if err != nil {
		return err
	}
	if found && inst.Status == StatusCompleted {
		// Idempotent: already done.
		return nil
	}
	if !found {
		inst = Instance{
			ID:     sagaID,
			Type:   f.sagaType,
			Status: StatusRunning,
			// Version 0 — Save inserts a new row.
		}
	} else {
		// Transition to completed on the second event of this type.
		inst.Status = StatusCompleted
		inst.Version++ // load-at-N, save-at-N+1
	}
	return f.store.Save(ctx, inst)
}

func (f *fakeSaga) Timeout(ctx context.Context, _ string) error {
	f.mu.Lock()
	f.timeoutCalls++
	f.mu.Unlock()
	in, ok := InstanceFromContext(ctx)
	if !ok {
		return nil
	}
	in.Status = StatusCompleted
	in.Version++ // load-at-N, save-at-N+1
	return f.store.Save(ctx, in)
}

// recordedEvent is a minimal *kurrentdb.RecordedEvent stand-in for unit
// tests. We construct it via pointer to the real type so dispatch's nil
// and StreamID checks exercise the production path.
func recordedEvent(eventType, streamID string) *kurrentdb.RecordedEvent {
	return &kurrentdb.RecordedEvent{
		EventType: eventType,
		StreamID:  streamID,
	}
}

func TestStatusStringRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []Status{StatusPending, StatusRunning, StatusCompleted, StatusFailed}
	for _, s := range cases {
		got, ok := ParseStatus(s.String())
		if !ok {
			t.Fatalf("ParseStatus(%q) not ok", s.String())
		}
		if got != s {
			t.Fatalf("round trip %d -> %q -> %d", s, s.String(), got)
		}
	}
	if _, ok := ParseStatus("bogus"); ok {
		t.Fatal("ParseStatus should reject unknown values")
	}
}

func TestRegister_ReactsOnlyToRegisteredEventTypes(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	c := NewCoordinator(nil, store, time.Now, 0)
	sg := &fakeSaga{store: store, sagaType: "fake"}
	c.Register(sg, []string{"item.registered", "item.processed"})

	c.dispatch(context.Background(), recordedEvent("item.registered", "item-1"))
	c.dispatch(context.Background(), recordedEvent("country.created", "country-1")) // not registered

	sg.mu.Lock()
	defer sg.mu.Unlock()
	if len(sg.reactCalls) != 1 {
		t.Fatalf("expected 1 React call, got %d (%v)", len(sg.reactCalls), sg.reactCalls)
	}
	if sg.reactCalls[0] != "item.registered" {
		t.Fatalf("expected item.registered, got %q", sg.reactCalls[0])
	}
}

func TestDispatch_SkipsSystemEvents(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	c := NewCoordinator(nil, store, time.Now, 0)
	sg := &fakeSaga{store: store, sagaType: "fake"}
	c.Register(sg, []string{"$stats"})

	c.dispatch(context.Background(), recordedEvent("$stats", "$stats-1"))

	sg.mu.Lock()
	defer sg.mu.Unlock()
	if len(sg.reactCalls) != 0 {
		t.Fatalf("system events must be skipped, got %d React calls", len(sg.reactCalls))
	}
}

func TestDispatch_FansOutToMultipleSagas(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	c := NewCoordinator(nil, store, time.Now, 0)
	sg1 := &fakeSaga{store: store, sagaType: "fake1"}
	sg2 := &fakeSaga{store: store, sagaType: "fake2"}
	c.Register(sg1, []string{"item.registered"})
	c.Register(sg2, []string{"item.registered"})

	c.dispatch(context.Background(), recordedEvent("item.registered", "item-1"))

	for _, sg := range []*fakeSaga{sg1, sg2} {
		sg.mu.Lock()
		if len(sg.reactCalls) != 1 {
			t.Fatalf("%s: expected 1 React call, got %d", sg.sagaType, len(sg.reactCalls))
		}
		sg.mu.Unlock()
	}
}

func TestReact_IncrementVersionOnSave(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	c := NewCoordinator(nil, store, time.Now, 0)
	sg := &fakeSaga{store: store, sagaType: "fake"}
	c.Register(sg, []string{"item.registered"})

	// First delivery: creates the instance at version 0.
	c.dispatch(context.Background(), recordedEvent("item.registered", "item-1"))

	inst, found, err := store.Load(context.Background(), "fake-item-1")
	if err != nil || !found {
		t.Fatalf("expected instance saved, found=%v err=%v", found, err)
	}
	if inst.Version != 0 {
		t.Fatalf("expected version 0 after create, got %d", inst.Version)
	}
	if inst.Status != StatusRunning {
		t.Fatalf("expected running status after create, got %s", inst.Status)
	}

	// Second delivery: transitions to completed, bumps version to 1.
	c.dispatch(context.Background(), recordedEvent("item.registered", "item-1"))

	inst, _, _ = store.Load(context.Background(), "fake-item-1")
	if inst.Version != 1 {
		t.Fatalf("expected version 1 after transition, got %d", inst.Version)
	}
	if inst.Status != StatusCompleted {
		t.Fatalf("expected completed status, got %s", inst.Status)
	}

	// Third delivery: idempotent no-op (already completed), no save bump.
	c.dispatch(context.Background(), recordedEvent("item.registered", "item-1"))
	inst, _, _ = store.Load(context.Background(), "fake-item-1")
	if inst.Version != 1 {
		t.Fatalf("idempotent re-delivery must not bump version, got %d", inst.Version)
	}
}

func TestSave_ConcurrencyConflict(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	ctx := context.Background()
	in := Instance{ID: "x", Type: "t", Status: StatusRunning, Version: 0}
	if err := store.Save(ctx, in); err != nil {
		t.Fatalf("initial save: %v", err)
	}
	// Stale save: caller still thinks version is 0 but stored is already 0.
	// Re-saving version 0 must conflict because the row exists.
	err := store.Save(ctx, Instance{ID: "x", Type: "t", Status: StatusRunning, Version: 0})
	if !errors.Is(err, ErrConcurrencyConflict) {
		t.Fatalf("expected ErrConcurrencyConflict, got %v", err)
	}
}

func TestSweepTimeouts_FiresTimeoutAndDeletesRow(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	c := NewCoordinator(nil, store, time.Now, 0)
	sg := &fakeSaga{store: store, sagaType: "fake"}
	c.Register(sg, []string{"item.registered"})

	// Seed an instance and a due timeout for it.
	inst := Instance{ID: "fake-item-1", Type: "fake", Status: StatusRunning, Version: 0}
	if err := store.Save(context.Background(), inst); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Minute)
	if err := store.ScheduleTimeout(context.Background(), "fake-item-1", "", past); err != nil {
		t.Fatal(err)
	}

	if err := c.sweepTimeouts(context.Background()); err != nil {
		t.Fatalf("sweepTimeouts: %v", err)
	}

	sg.mu.Lock()
	defer sg.mu.Unlock()
	if sg.timeoutCalls != 1 {
		t.Fatalf("expected 1 timeout call, got %d", sg.timeoutCalls)
	}
	if store.deleteCalls != 1 {
		t.Fatalf("expected timeout row deleted, deleteCalls=%d", store.deleteCalls)
	}
	inst2, _, _ := store.Load(context.Background(), "fake-item-1")
	if inst2.Status != StatusCompleted {
		t.Fatalf("expected timeout to complete the saga, got %s", inst2.Status)
	}
}

func TestSweepTimeouts_DropsStaleReminderForMissingInstance(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	c := NewCoordinator(nil, store, time.Now, 0)
	c.Register(&fakeSaga{store: store, sagaType: "fake"}, []string{"item.registered"})

	// A timeout row whose saga instance was never persisted (purged).
	if err := store.ScheduleTimeout(context.Background(), "ghost", "", time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := c.sweepTimeouts(context.Background()); err != nil {
		t.Fatalf("sweep must not fail on missing instance: %v", err)
	}
	if store.deleteCalls != 1 {
		t.Fatalf("stale row should be deleted, deleteCalls=%d", store.deleteCalls)
	}
}

func TestRunTimeoutSweep_StopsOnContextCancel(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	c := NewCoordinator(nil, store, time.Now, 0)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.RunTimeoutSweep(ctx, 5*time.Millisecond) }()
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected context error on cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("RunTimeoutSweep did not stop on cancel")
	}
}
