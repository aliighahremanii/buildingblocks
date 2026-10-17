package errors

type NotFoundError struct {
	Resource string
	ID       string
}

func NewNotFoundError(resource string, id string) NotFoundError {
	return NotFoundError{
		Resource: resource,
		ID:       id,
	}
}

func (e NotFoundError) Error() string {
	if e.ID == "" {
		return e.Resource + " not found"
	}

	return e.Resource + " with id " + e.ID + " not found"
}
