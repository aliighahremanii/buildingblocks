package web

import (
	"context"
	"errors"
	"net/http"
)

// BindJSONError wraps a client request-body decoding error. HTTP adapters
// return this error so presentation middleware can consistently map invalid
// JSON to a bad-request response without importing a concrete adapter.
type BindJSONError struct {
	Err error
}

func (e *BindJSONError) Error() string { return e.Err.Error() }
func (e *BindJSONError) Unwrap() error { return e.Err }

func NewBindJSONError(err error) error {
	if err == nil {
		return nil
	}
	var bindErr *BindJSONError
	if errors.As(err, &bindErr) {
		return err
	}
	return &BindJSONError{Err: err}
}

type Context interface {
	Context() context.Context

	Method() string
	Path() string

	Query(name string) string
	Param(name string) string

	Header(name string) string
	SetHeader(name string, value string)

	BindJSON(target any) error

	JSON(status int, value any) error
	Text(status int, value string) error
	NoContent(status int) error

	// ResponseWriter and Request expose the underlying net/http pair for
	// handlers that cannot be expressed as a BindJSON/JSON round-trip — e.g.
	// the gqlgen GraphQL server, websocket upgrades, or streaming responses.
	// Use of these accessors opts out of the convenience methods above; callers
	// are responsible for writing the response themselves.
	ResponseWriter() http.ResponseWriter
	Request() *http.Request
}
