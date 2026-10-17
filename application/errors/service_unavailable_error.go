package errors

// ServiceUnavailableError represents a 503 Service Unavailable response. It is
// used to signal that a write surface is temporarily disabled while a
// maintenance operation (e.g. the BOL allocation backfill) is in progress.
type ServiceUnavailableError struct {
	Code    string
	Message string
}

func NewServiceUnavailableError(code, message string) ServiceUnavailableError {
	return ServiceUnavailableError{Code: code, Message: message}
}

func (e ServiceUnavailableError) Error() string { return e.Message }
