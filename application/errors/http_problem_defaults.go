package errors

const (
	BadRequestCode            = "BadRequest"
	InvalidRequestBodyMessage = "Invalid request body."
	ForbiddenCode             = "Forbidden"
	ValidationErrorCode       = "ValidationError"
	ValidationErrorMessage    = "One or more validation errors occurred."
	NotFoundCode              = "NotFound"
	InternalErrorCode         = "InternalError"
	InternalErrorMessage      = "An unexpected error occurred."
)

func NotFoundMessage(resource string) string {
	return resource + " not found."
}
