package errors

import (
	"errors"
	"testing"
)

func TestValidationError(t *testing.T) {
	t.Parallel()

	ve := NewValidationError(Field("Name", "name is required"), Field("Code", "code is required"))
	if len(ve.Errors) != 2 {
		t.Fatalf("expected 2 field errors, got %d", len(ve.Errors))
	}
	if ve.Errors[0].Code != "Name" || ve.Errors[0].Message != "name is required" {
		t.Fatalf("first field error: %+v", ve.Errors[0])
	}
	msg := ve.Error()
	if msg != "name is required; code is required" {
		t.Fatalf("Error() = %q", msg)
	}
}

func TestValidationErrorSingleField(t *testing.T) {
	t.Parallel()
	ve := NewValidationError(Field("X", "x failed"))
	if ve.Error() != "x failed" {
		t.Fatalf("single field Error() = %q", ve.Error())
	}
}

func TestNotFoundError(t *testing.T) {
	t.Parallel()

	withID := NewNotFoundError("country", "abc-123")
	if withID.Resource != "country" || withID.ID != "abc-123" {
		t.Fatalf("fields mismatch: %+v", withID)
	}
	if got := withID.Error(); got != "country with id abc-123 not found" {
		t.Fatalf("Error() = %q", got)
	}

	withoutID := NewNotFoundError("country", "")
	if got := withoutID.Error(); got != "country not found" {
		t.Fatalf("Error() without ID = %q", got)
	}
}

func TestConflictError(t *testing.T) {
	t.Parallel()
	ce := NewConflictError("DuplicateCode", "country already exists")
	if ce.Code != "DuplicateCode" || ce.Message != "country already exists" {
		t.Fatalf("fields mismatch: %+v", ce)
	}
	if ce.Error() != "country already exists" {
		t.Fatalf("Error() = %q", ce.Error())
	}
}

func TestUnauthorizedError(t *testing.T) {
	t.Parallel()
	ue := NewUnauthorizedError("BadToken", "token expired")
	if ue.Code != "BadToken" || ue.Message != "token expired" {
		t.Fatalf("fields mismatch: %+v", ue)
	}
	if ue.Error() != "token expired" {
		t.Fatalf("Error() = %q", ue.Error())
	}

	are := NewAuthenticationRequiredError()
	if are.Code != AuthenticationRequiredCode || are.Message != AuthenticationRequiredMessage {
		t.Fatalf("auth required fields mismatch: %+v", are)
	}
}

func TestForbiddenError(t *testing.T) {
	t.Parallel()
	fe := NewForbiddenError("insufficient role")
	if fe.Message != "insufficient role" {
		t.Fatalf("fields mismatch: %+v", fe)
	}
	if fe.Error() != "insufficient role" {
		t.Fatalf("Error() = %q", fe.Error())
	}
}

func TestTooManyRequestsError(t *testing.T) {
	t.Parallel()
	tmr := NewTooManyRequestsError("RateLimit", "too many requests")
	if tmr.Code != "RateLimit" || tmr.Message != "too many requests" {
		t.Fatalf("fields mismatch: %+v", tmr)
	}
	if tmr.Error() != "too many requests" {
		t.Fatalf("Error() = %q", tmr.Error())
	}
}

func TestNotFoundMessage(t *testing.T) {
	t.Parallel()
	if got := NotFoundMessage("country"); got != "country not found." {
		t.Fatalf("NotFoundMessage = %q", got)
	}
}

func TestErrorTypeAssertions(t *testing.T) {
	t.Parallel()
	var err error

	err = NewValidationError(Field("X", "fail"))
	var ve ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("ValidationError should match via errors.As")
	}

	err = NewNotFoundError("x", "y")
	var nfe NotFoundError
	if !errors.As(err, &nfe) {
		t.Fatalf("NotFoundError should match via errors.As")
	}

	err = NewConflictError("X", "y")
	var ce ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("ConflictError should match via errors.As")
	}
}
