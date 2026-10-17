package errors

// TooManyRequestsError represents a 429 response.
type TooManyRequestsError struct {
	Code    string
	Message string
}

func NewTooManyRequestsError(code, message string) TooManyRequestsError {
	return TooManyRequestsError{Code: code, Message: message}
}

func (e TooManyRequestsError) Error() string { return e.Message }
