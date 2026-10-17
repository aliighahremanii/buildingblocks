package errors

import "strings"

// FieldError is a single validation failure with an optional code.
type FieldError struct {
	Code    string
	Message string
}

// ValidationError collects one or more field-level validation failures.
type ValidationError struct {
	Errors []FieldError
}

// NewValidationError constructs a ValidationError from one or more FieldErrors.
func NewValidationError(fields ...FieldError) ValidationError {
	return ValidationError{Errors: fields}
}

// Field is a convenience constructor for a FieldError.
func Field(code, message string) FieldError {
	return FieldError{Code: code, Message: message}
}

func (e ValidationError) Error() string {
	msgs := make([]string, len(e.Errors))
	for i, fe := range e.Errors {
		msgs[i] = fe.Message
	}
	return strings.Join(msgs, "; ")
}
