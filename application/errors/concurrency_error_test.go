package errors

import (
	"errors"
	"testing"
)

func TestNewConcurrencyErrorPopulatesFields(t *testing.T) {
	t.Parallel()
	ce := NewConcurrencyError("country", "uuid-abc", int64(5), int64(7))
	if ce.Code != ConcurrencyConflictCode {
		t.Fatalf("Code = %q, want %q", ce.Code, ConcurrencyConflictCode)
	}
	if ce.Message != ConcurrencyConflictMessage {
		t.Fatalf("Message = %q, want %q", ce.Message, ConcurrencyConflictMessage)
	}
	if ce.AggregateType != "country" {
		t.Fatalf("AggregateType = %q", ce.AggregateType)
	}
	if ce.AggregateID != "uuid-abc" {
		t.Fatalf("AggregateID = %q", ce.AggregateID)
	}
	if ce.ExpectedRevision != 5 {
		t.Fatalf("ExpectedRevision = %d, want 5", ce.ExpectedRevision)
	}
	if ce.ActualRevision != 7 {
		t.Fatalf("ActualRevision = %d, want 7", ce.ActualRevision)
	}
}

func TestConcurrencyErrorIsErrorsAsTarget(t *testing.T) {
	t.Parallel()
	// errors.As must match a ConcurrencyError target so the transport layer
	// can detect OCC conflicts via errors.As(err, &cce). This is the contract
	// WriteProblemError relies on to map ConcurrencyError → HTTP 409.
	ce := NewConcurrencyError("country", "uuid", 1, 2)
	wrapped := wrapSomehow(ce)

	var target ConcurrencyError
	if !errors.As(wrapped, &target) {
		t.Fatalf("errors.As(wrapped, &ConcurrencyError) = false, want true")
	}
	if target.AggregateID != "uuid" {
		t.Fatalf("unwrapped AggregateID = %q, want %q", target.AggregateID, "uuid")
	}
}

func TestConcurrencyErrorMessageIsNonEmpty(t *testing.T) {
	t.Parallel()
	// An empty message would surface as an empty HTTP response body, which
	// is opaque to clients. The default Message constant must be actionable.
	ce := NewConcurrencyError("x", "y", 0, 0)
	if ce.Error() == "" {
		t.Fatal("Error() is empty")
	}
	if ce.Message == "" {
		t.Fatalf("Message constant is empty")
	}
}

func TestConcurrencyConflictCodeAndMessageAreStable(t *testing.T) {
	t.Parallel()
	// Problem-response codes are part of the public API contract: clients
	// switch on them. Lock the wire values so a refactor cannot silently
	// break clients.
	if ConcurrencyConflictCode != "ConcurrencyConflict" {
		t.Fatalf("ConcurrencyConflictCode = %q, want %q", ConcurrencyConflictCode, "ConcurrencyConflict")
	}
	if ConcurrencyConflictMessage == "" {
		t.Fatalf("ConcurrencyConflictMessage is empty")
	}
}

// wrapSomehow wraps err in a fmt.Errorf("%w") chain to verify errors.As
// traversal works through typical wrapping the runtime produces.
func wrapSomehow(err error) error {
	return &wrappedErr{cause: err}
}

type wrappedErr struct{ cause error }

func (w *wrappedErr) Error() string { return "wrapped: " + w.cause.Error() }
func (w *wrappedErr) Unwrap() error { return w.cause }
