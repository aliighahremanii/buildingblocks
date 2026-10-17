package concurrency

import (
	"testing"
)

func TestNoStreamConstructsExpectedNoStream(t *testing.T) {
	t.Parallel()
	e := NoStream()
	if !e.IsNoStream() {
		t.Fatalf("NoStream().IsNoStream() = false, want true")
	}
	if e.IsAtRevision() {
		t.Fatalf("NoStream().IsAtRevision() = true, want false")
	}
	if e.IsAny() {
		t.Fatalf("NoStream().IsAny() = true, want false")
	}
	if e.Revision != 0 {
		t.Fatalf("NoStream().Revision = %d, want 0 (unused)", e.Revision)
	}
	if e.Kind != ExpectedNoStream {
		t.Fatalf("Kind = %v, want ExpectedNoStream", e.Kind)
	}
}

func TestAtRevisionConstructsExpectedAtRevision(t *testing.T) {
	t.Parallel()
	e := AtRevision(42)
	if !e.IsAtRevision() {
		t.Fatalf("AtRevision(42).IsAtRevision() = false, want true")
	}
	if e.IsNoStream() {
		t.Fatalf("AtRevision(42).IsNoStream() = true, want false")
	}
	if e.Revision != 42 {
		t.Fatalf("Revision = %d, want 42", e.Revision)
	}
	if e.Kind != ExpectedAtRevision {
		t.Fatalf("Kind = %v, want ExpectedAtRevision", e.Kind)
	}
}

func TestAtRevisionZeroIsStillAtRevision(t *testing.T) {
	t.Parallel()
	// Revision 0 is a valid KurrentDB stream state ("first event has not been
	// written yet" == revision 0 == NoStream semantically, but the type
	// preserves the caller's intent). The Store adapter decides how to map
	// AtRevision(0) at the wire level.
	e := AtRevision(0)
	if !e.IsAtRevision() {
		t.Fatalf("AtRevision(0).IsAtRevision() = false, want true")
	}
	if e.Revision != 0 {
		t.Fatalf("Revision = %d, want 0", e.Revision)
	}
}

func TestAnyConstructsExpectedAny(t *testing.T) {
	t.Parallel()
	e := Any()
	if !e.IsAny() {
		t.Fatalf("Any().IsAny() = false, want true")
	}
	if e.IsNoStream() {
		t.Fatalf("Any().IsNoStream() = true, want false")
	}
	if e.IsAtRevision() {
		t.Fatalf("Any().IsAtRevision() = true, want false")
	}
	if e.Kind != ExpectedAny {
		t.Fatalf("Kind = %v, want ExpectedAny", e.Kind)
	}
}

func TestExpectedZeroValueIsAny(t *testing.T) {
	t.Parallel()
	// The zero value is Any (no concurrency check). This documents the
	// footgun noted in the Expected doc comment: production handlers MUST
	// set Expected explicitly rather than relying on the zero value.
	var e Expected
	if !e.IsAny() {
		t.Fatalf("zero-value Expected.IsAny() = false, want true (zero value is Any)")
	}
}

func TestExpectedKindValuesAreDistinct(t *testing.T) {
	t.Parallel()
	// The Kind enum must have distinct values so a switch on Kind is
	// exhaustive and unambiguous. This guards against a refactor that
	// accidentally aliases two cases.
	if ExpectedAny == ExpectedNoStream {
		t.Fatalf("ExpectedAny == ExpectedNoStream")
	}
	if ExpectedAny == ExpectedAtRevision {
		t.Fatalf("ExpectedAny == ExpectedAtRevision")
	}
	if ExpectedNoStream == ExpectedAtRevision {
		t.Fatalf("ExpectedNoStream == ExpectedAtRevision")
	}
}
