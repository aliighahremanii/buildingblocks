// Package concurrency provides the application-layer abstraction for optimistic
// concurrency control on event-sourced aggregate streams.
//
// Every event store that supports atomic append-with-version exposes a "stream
// state" expectation: a no-stream expectation (the stream must not exist), an
// at-revision expectation (the stream must be at exactly revision N), or an
// any-state expectation (no concurrency check). The exact wire representation
// is event-store-specific (KurrentDB's StreamState, NATS-JS's
// Nats-Expected-Stream / Nats-Expected-Sequence, etc.), but the application
// layer needs a single, infrastructure-agnostic way to express the expectation.
//
// [Expected] is that abstraction. Command handlers carry an Expected value
// from the load step through the save step so that the infrastructure adapter
// (eventstore.Store.Save) can translate it to the underlying event store's
// native concurrency primitive.
//
// The three constructors are intentionally named for the use case rather than
// the wire encoding, so command-handler code reads as business intent:
//
//   - [NoStream]    → Register-style handlers ("this aggregate must be new").
//   - [AtRevision]  → Update-style handlers ("I loaded this at revision N;
//     fail if anyone committed since").
//   - [Any]         → Migration / bootstrap only ("don't check"). Production
//     command handlers MUST specify NoStream or AtRevision.
package concurrency

// Expected is the optimistic-concurrency expectation that an
// event-sourced command handler places on a Save. It is the
// application-layer counterpart to the event store's native "expected stream
// state": NoStream (Register), AtRevision (Update), or Any (migration only).
//
// The zero value is Any (no concurrency check). New code MUST set this
// explicitly — production command handlers should never rely on the zero
// value because silently unguarded appends are the source of lost updates.
type Expected struct {
	Kind     ExpectedKind
	Revision uint64 // only meaningful when Kind == ExpectedAtRevision
}

// ExpectedKind enumerates the three concurrency expectations.
type ExpectedKind int

const (
	// ExpectedAny disables the concurrency check. Reserved for migration and
	// bootstrap code that predates optimistic concurrency. Production command
	// handlers should not use ExpectedAny: an unguarded append silently
	// accepts concurrent updates and loses the losing writer's decision.
	ExpectedAny ExpectedKind = iota
	// ExpectedNoStream asserts that the stream must not yet exist. Used by
	// Register-style handlers to detect duplicate aggregate creation: a
	// retried Register that already succeeded will fail the no-stream
	// expectation, surfacing as a ConcurrencyError rather than silently
	// creating a second stream.
	ExpectedNoStream
	// ExpectedAtRevision asserts that the stream must be at exactly
	// Expected.Revision. Used by Update-style handlers that loaded the
	// aggregate at this revision: any concurrent append advances the stream
	// past this revision, failing the save and surfacing as a
	// ConcurrencyError rather than silently overwriting the concurrent
	// writer's decision.
	ExpectedAtRevision
)

// NoStream constructs the expectation used by Register-style handlers: the
// stream must not yet exist. A retried Register that already succeeded will
// fail this expectation at the event store, surfacing as a ConcurrencyError
// the API maps to HTTP 409.
func NoStream() Expected { return Expected{Kind: ExpectedNoStream} }

// AtRevision constructs the expectation used by Update-style handlers: the
// stream must be at exactly revision. Callers pass the revision returned by
// the immediately-preceding ByID load, so any concurrent append between load
// and save advances the stream past this revision and fails the save.
func AtRevision(revision uint64) Expected {
	return Expected{Kind: ExpectedAtRevision, Revision: revision}
}

// Any constructs the expectation that disables the concurrency check. It
// exists for migration and bootstrap only; new production code must use
// [NoStream] or [AtRevision].
func Any() Expected { return Expected{Kind: ExpectedAny} }

// IsNoStream reports whether this expectation is the no-stream expectation.
// Convenience for infrastructure adapters that switch on Kind.
func (e Expected) IsNoStream() bool { return e.Kind == ExpectedNoStream }

// IsAtRevision reports whether this expectation is the at-revision expectation.
func (e Expected) IsAtRevision() bool { return e.Kind == ExpectedAtRevision }

// IsAny reports whether this expectation disables the concurrency check.
func (e Expected) IsAny() bool { return e.Kind == ExpectedAny }
