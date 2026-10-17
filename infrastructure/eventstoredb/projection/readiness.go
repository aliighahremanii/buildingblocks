// readiness.go — the projection-readiness Registry.
//
// The worker host exposes GET /ready on the metrics listener. Readiness
// must aggregate the ACTUAL projection workers running in this process —
// but the workers are constructed deep inside each module's RunAll, so
// the host cannot reach them without either threading a collector through
// every module's public API or scanning for modules (both forbidden by
// repo policy).
//
// Ownership model (review fix, D107-R1): the Registry is an ordinary
// struct OWNED by the worker host. NewWorker creates exactly one, injects
// it through ModuleDeps.Readiness into each module worker runner, the
// module worker sets explicitly Register() the workers they construct and
// run, and the host's /ready handler reads that same instance. There is
// NO process-global state: constructing a second host (tests, embedded
// tooling) yields an independent registry, and a partially-constructed
// host that fails and is discarded takes its registry with it — no
// phantom required workers can outlive the host that owned them. Workers
// themselves never self-register, so a worker constructed but never
// registered (unit tests, retired sample modules) is invisible to
// readiness.
//
// Gating rule: only workers registered with
// WorkerConfig.ReadinessRequired gate readiness — the Postgres read-model
// projections whose sink is required infrastructure.
// Optional-infrastructure projections (Meilisearch) report their state in
// the readiness body for observability but never flip /ready by
// themselves, and the NATS publisher (natspub — a different worker type)
// is never registered. A registry with zero required workers is
// vacuously ready (e.g. a worker process with no Postgres configured has
// nothing required to do).
package projection

import (
	"sort"
	"sync"
)

// WorkerStatus is a point-in-time readiness snapshot of one projection
// worker. JSON tags shape the /ready response body.
type WorkerStatus struct {
	Name      string `json:"name"`
	Required  bool   `json:"required"`
	State     State  `json:"state"`
	LastError string `json:"last_error,omitempty"`
}

// Registry aggregates the readiness of the projection workers one host
// actually runs. Construct one per host (the worker host does); Register
// each worker at composition time. All methods are safe for concurrent
// use.
type Registry struct {
	mu      sync.Mutex
	workers []*Worker
}

// NewRegistry constructs an empty readiness registry.
func NewRegistry() *Registry {
	return &Registry{}
}

// Register adds w to the registry.
//
// Registering the same *Worker twice is a composition error (two owners
// believe they run it) and is rejected.
//
// Registering a DIFFERENT worker under an already-registered NAME is the
// supported client-generation-swap path (D107-R2): the worker host
// restarts module runners after swapping the terminally-closed KurrentDB
// client, and the new runner constructs fresh workers with the same
// projection names. The new worker REPLACES the old entry at the moment
// of registration — until then the old (stopped) worker keeps /ready
// honestly failing. The host guarantees the old generation's context is
// cancelled before the replacement registers.
func (r *Registry) Register(w *Worker) error {
	if w == nil {
		return errRegisterNilWorker
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, existing := range r.workers {
		if existing == w {
			return errDuplicateWorkerRegistration
		}
		if existing.config.Name == w.config.Name {
			r.workers[i] = w
			return nil
		}
	}
	r.workers = append(r.workers, w)
	return nil
}

// Readiness returns a deterministic (name-sorted) snapshot of every
// registered worker.
func (r *Registry) Readiness() []WorkerStatus {
	r.mu.Lock()
	workers := make([]*Worker, len(r.workers))
	copy(workers, r.workers)
	r.mu.Unlock()

	statuses := make([]WorkerStatus, 0, len(workers))
	for _, w := range workers {
		statuses = append(statuses, w.Status())
	}
	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].Name < statuses[j].Name
	})
	return statuses
}

// Ready reports whether every registered ReadinessRequired projection
// worker has a live subscription. Workers in any state other than running
// (idle, connecting, down, stopped) are not ready — an honest flap during
// a transient reconnect is preferable to reporting ready while a required
// projection is not consuming.
func (r *Registry) Ready() (bool, []WorkerStatus) {
	statuses := r.Readiness()
	ready := true
	for _, s := range statuses {
		if s.Required && s.State != StateRunning {
			ready = false
		}
	}
	return ready, statuses
}
