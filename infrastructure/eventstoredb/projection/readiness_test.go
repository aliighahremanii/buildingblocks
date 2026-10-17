package projection

// readiness_test.go — unit coverage for the host-owned readiness
// Registry (D107-R1): explicit registration, duplicate rejection,
// deterministic snapshots, the gating rule (only ReadinessRequired
// workers gate Ready(); optional workers report but never gate; an empty
// registry is vacuously ready), and — the review-fix regressions — that
// readiness ownership has NO process-global state: a partially-built
// composition leaves phantoms only in ITS registry (discarded with it),
// and two independent registries never observe each other's workers.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/kurrent-io/KurrentDB-Client-Go/kurrentdb"

	"github.com/aliighahremanii/buildingblocks/infrastructure/eventstoredb/checkpoint"
)

func mustNewWorker(t *testing.T, name string, required bool) *Worker {
	t.Helper()
	w, err := New(nil, checkpoint.New(nil), nil, WorkerConfig{
		Name: name, Prefixes: []string{"t-"}, ReadinessRequired: required,
	}, noOpProject)
	if err != nil {
		t.Fatalf("New %s: %v", name, err)
	}
	return w
}

func TestRegistryGatingRule(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	req := mustNewWorker(t, "reg-test-required", true)
	opt := mustNewWorker(t, "reg-test-optional", false)
	if err := reg.Register(req); err != nil {
		t.Fatalf("Register required: %v", err)
	}
	if err := reg.Register(opt); err != nil {
		t.Fatalf("Register optional: %v", err)
	}

	// Required worker idle (constructed, never Run) -> not ready.
	ready, statuses := reg.Ready()
	if ready {
		t.Fatal("Ready() = true with a required worker idle, want false")
	}
	rs := findStatus(t, statuses, "reg-test-required")
	if !rs.Required || rs.State != StateIdle {
		t.Fatalf("required status = %+v, want {Required:true State:idle}", rs)
	}

	// Optional worker down must NOT gate: with the required worker
	// running, Ready() must be true even though the optional one is down.
	opt.setState(StateDown, context.Canceled)
	req.setState(StateRunning, nil)
	ready, statuses = reg.Ready()
	if !ready {
		t.Fatal("Ready() = false with required running and only an optional worker down, want true")
	}
	os := findStatus(t, statuses, "reg-test-optional")
	if os.Required || os.State != StateDown {
		t.Fatalf("optional status = %+v, want {Required:false State:down}", os)
	}

	// Required worker down again -> not ready.
	req.setState(StateDown, nil)
	if ready, _ := reg.Ready(); ready {
		t.Fatal("Ready() = true with required worker down, want false")
	}
}

// TestRegistryEmptyIsVacuouslyReady pins the empty-registry contract: a
// worker process with no registered required workers (nothing required
// to do) is ready. This is what makes the NATS-only or
// optional-infrastructure-only configurations honest.
func TestRegistryEmptyIsVacuouslyReady(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	ready, statuses := reg.Ready()
	if !ready {
		t.Fatal("empty registry Ready() = false, want true (vacuous)")
	}
	if len(statuses) != 0 {
		t.Fatalf("empty registry statuses = %+v, want none", statuses)
	}
}

// TestRegistryRejectsDuplicateAndNil pins registration discipline: the
// same worker twice is a composition error (two owners believe they run
// it), and nil is rejected outright.
func TestRegistryRejectsDuplicateAndNil(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	w := mustNewWorker(t, "reg-test-dup", true)
	if err := reg.Register(w); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := reg.Register(w); !errors.Is(err, errDuplicateWorkerRegistration) {
		t.Fatalf("second Register err = %v, want errDuplicateWorkerRegistration", err)
	}
	if err := reg.Register(nil); !errors.Is(err, errRegisterNilWorker) {
		t.Fatalf("Register(nil) err = %v, want errRegisterNilWorker", err)
	}
	if _, statuses := reg.Ready(); len(statuses) != 1 {
		t.Fatalf("statuses = %d entries after rejected registrations, want 1", len(statuses))
	}
}

// TestRegistryPartialConstructionLeavesNoGlobalPhantom is the review-fix
// regression (MEDIUM): simulating a partially-built composition — worker
// A registered, then worker B's construction FAILS and the composition is
// discarded — must leave NO trace in a fresh composition's registry. The
// discarded registry may keep its (idle, honestly not-ready) entries, but
// nothing process-global survives it.
func TestRegistryPartialConstructionLeavesNoGlobalPhantom(t *testing.T) {
	t.Parallel()

	// Composition attempt 1: register one worker, then "fail" before the
	// second is registered; the whole registry is discarded with it.
	failed := NewRegistry()
	orphan := mustNewWorker(t, "reg-test-orphan", true)
	if err := failed.Register(orphan); err != nil {
		t.Fatalf("Register orphan: %v", err)
	}
	// (Composition fails here; `failed` and `orphan` go out of scope in
	// the caller. Nothing to unregister — there is no global state.)

	// Composition attempt 2: fresh registry, fresh workers. It must see
	// ONLY its own worker — the orphan from attempt 1 must be invisible,
	// and readiness must be governed solely by attempt 2's workers.
	fresh := NewRegistry()
	w2 := mustNewWorker(t, "reg-test-fresh", true)
	if err := fresh.Register(w2); err != nil {
		t.Fatalf("Register fresh: %v", err)
	}
	w2.setState(StateRunning, nil)

	ready, statuses := fresh.Ready()
	if !ready {
		t.Fatal("fresh registry not ready with its required worker running")
	}
	for _, s := range statuses {
		if s.Name == "reg-test-orphan" {
			t.Fatal("fresh registry observed the discarded composition's worker — registry state is NOT instance-scoped")
		}
	}

	// The discarded registry's own view remains honest (its orphan is
	// idle → not ready) but that view dies with the registry.
	if ready, _ := failed.Ready(); ready {
		t.Fatal("discarded registry reports ready with an idle required worker — inconsistent")
	}
}

// TestRepeatedHostConstructionIsIndependent is the second review-fix
// regression (MEDIUM): two host constructions in one process (e.g.
// re-embedding the worker host in tooling or tests) must yield
// independent registries — an old, stopped worker set can never gate the
// new host's readiness, and worker names may repeat across hosts.
func TestRepeatedHostConstructionIsIndependent(t *testing.T) {
	t.Parallel()

	host1 := NewRegistry()
	old1 := mustNewWorker(t, "reg-test-repeated", true)
	if err := host1.Register(old1); err != nil {
		t.Fatalf("Register host1: %v", err)
	}
	old1.setState(StateRunning, nil)

	// Reconstruct: host 2 builds its own worker with the SAME projection
	// name; the old worker set is shut down (stopped).
	old1.setState(StateStopped, context.Canceled)
	host2 := NewRegistry()
	new1 := mustNewWorker(t, "reg-test-repeated", true)
	if err := host2.Register(new1); err != nil {
		t.Fatalf("Register host2: %v", err)
	}

	// Host 2's verdict must depend only on ITS worker: idle → not ready;
	// running → ready — never on the old stopped worker.
	if ready, _ := host2.Ready(); ready {
		t.Fatal("host2 ready while its own worker is idle")
	}
	new1.setState(StateRunning, nil)
	if ready, statuses := host2.Ready(); !ready {
		t.Fatalf("host2 not ready with its worker running; statuses=%+v", statuses)
	}
	if _, statuses := host2.Ready(); len(statuses) != 1 {
		t.Fatalf("host2 statuses = %d entries, want 1 (its own worker only)", len(statuses))
	}
	// Host 1's stale registry still reports its own (stopped) worker and
	// is not ready — but only host 1 (discarded) would ever read it.
	if ready, _ := host1.Ready(); ready {
		t.Fatal("stale host1 registry reports ready with a stopped required worker")
	}
}

// TestRegistrySnapshotIsSortedAndJSONShaped pins the /ready body shape:
// name-sorted, stable field names.
func TestRegistrySnapshotIsSortedAndJSONShaped(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	if err := reg.Register(mustNewWorker(t, "reg-json-b", true)); err != nil {
		t.Fatalf("Register b: %v", err)
	}
	if err := reg.Register(mustNewWorker(t, "reg-json-a", false)); err != nil {
		t.Fatalf("Register a: %v", err)
	}

	statuses := reg.Readiness()
	if len(statuses) != 2 || statuses[0].Name != "reg-json-a" || statuses[1].Name != "reg-json-b" {
		t.Fatalf("snapshot not name-sorted: %+v", statuses)
	}

	encoded, err := json.Marshal(statuses[1])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"name":"reg-json-b","required":true,"state":"idle"}`
	if string(encoded) != want {
		t.Fatalf("WorkerStatus JSON = %s, want %s", encoded, want)
	}
}

// TestStatusCarriesLastError proves the readiness body surfaces the last
// failure reason for operator diagnosis.
func TestStatusCarriesLastError(t *testing.T) {
	t.Parallel()

	w := mustNewWorker(t, "reg-lasterr", true)
	if s := w.Status(); s.LastError != "" {
		t.Fatalf("LastError = %q on fresh worker, want empty", s.LastError)
	}
	w.setState(StateDown, errors.New("sentinel"))
	if s := w.Status(); s.LastError != "sentinel" {
		t.Fatalf("LastError = %q, want sentinel", s.LastError)
	}
}

// TestRegistryGenerationReplaceByName pins the D107-R2 generation-swap
// semantics: after a client-generation swap the module worker sets
// construct FRESH workers with the SAME projection names; registering
// them REPLACES the old entries (by name) so /ready is governed by the
// current generation only. Until the replacement registers, the old
// (stopped) worker keeps readiness honestly failing.
func TestRegistryGenerationReplaceByName(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	gen1 := mustNewWorker(t, "reg-gen-same-name", true)
	if err := reg.Register(gen1); err != nil {
		t.Fatalf("Register gen1: %v", err)
	}
	gen1.setState(StateRunning, nil)
	if ready, _ := reg.Ready(); !ready {
		t.Fatal("generation 1 running must be ready")
	}

	// Generation swap: the host cancels the old generation (worker goes
	// stopped), then the new worker registers under the same name.
	gen1.setState(StateStopped, context.Canceled)
	if ready, _ := reg.Ready(); ready {
		t.Fatal("stopped old generation must keep readiness failing until replacement registers")
	}

	gen2 := mustNewWorker(t, "reg-gen-same-name", true)
	if err := reg.Register(gen2); err != nil {
		t.Fatalf("Register gen2 (same name, new pointer): %v", err)
	}
	if _, statuses := reg.Ready(); len(statuses) != 1 {
		t.Fatalf("statuses = %+v, want exactly ONE entry after generation replacement", statuses)
	}
	if ready, _ := reg.Ready(); ready {
		t.Fatal("new generation idle must not be ready")
	}
	gen2.setState(StateRunning, nil)
	if ready, statuses := reg.Ready(); !ready {
		t.Fatalf("new generation running must be ready; statuses=%+v", statuses)
	}

	// The OLD generation's state changes no longer affect the registry.
	gen1.setState(StateRunning, nil)
	if ready, statuses := reg.Ready(); !ready || len(statuses) != 1 {
		t.Fatalf("old worker state must not influence the registry: ready=%v statuses=%+v", ready, statuses)
	}

	// A DIFFERENT name still appends (no accidental replacement).
	other := mustNewWorker(t, "reg-gen-other-name", false)
	if err := reg.Register(other); err != nil {
		t.Fatalf("Register other: %v", err)
	}
	if _, statuses := reg.Ready(); len(statuses) != 2 {
		t.Fatalf("statuses = %d entries, want 2", len(statuses))
	}
}

func findStatus(t *testing.T, statuses []WorkerStatus, name string) WorkerStatus {
	t.Helper()
	for _, s := range statuses {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("status for %q not found in snapshot: %+v", name, statuses)
	return WorkerStatus{}
}

// Compile-time: the test file exercises the kurrentdb import through
// noOpProject's signature.
var _ = func(_ context.Context, _ *kurrentdb.RecordedEvent) error { return nil }
