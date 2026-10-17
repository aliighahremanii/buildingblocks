package errors

const (
	AuthenticationRequiredCode    = "Unauthorized"
	AuthenticationRequiredMessage = "Authentication is required."
)

// UnauthorizedError represents a 401 Unauthorized response.
type UnauthorizedError struct {
	Code    string
	Message string
}

func NewUnauthorizedError(code, message string) UnauthorizedError {
	return UnauthorizedError{Code: code, Message: message}
}

func NewAuthenticationRequiredError() UnauthorizedError {
	return NewUnauthorizedError(AuthenticationRequiredCode, AuthenticationRequiredMessage)
}

func (e UnauthorizedError) Error() string { return e.Message }
