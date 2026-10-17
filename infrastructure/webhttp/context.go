package webhttp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

var _ web.Context = (*httpContext)(nil)

type httpContext struct {
	responseWriter http.ResponseWriter
	request        *http.Request
}

func newContext(responseWriter http.ResponseWriter, request *http.Request) *httpContext {
	return &httpContext{
		responseWriter: responseWriter,
		request:        request,
	}
}

func (httpContext *httpContext) Context() context.Context {
	return httpContext.request.Context()
}

func (httpContext *httpContext) Method() string {
	return httpContext.request.Method
}

func (httpContext *httpContext) Path() string {
	return httpContext.request.URL.Path
}

func (httpContext *httpContext) Query(name string) string {
	return httpContext.request.URL.Query().Get(name)
}

func (httpContext *httpContext) Param(name string) string {
	return httpContext.request.PathValue(name)
}

func (httpContext *httpContext) Header(name string) string {
	return httpContext.request.Header.Get(name)
}

func (httpContext *httpContext) SetHeader(name string, value string) {
	httpContext.responseWriter.Header().Set(name, value)
}

func (httpContext *httpContext) BindJSON(target any) error {
	defer httpContext.request.Body.Close()

	decoder := json.NewDecoder(httpContext.request.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(target); err != nil {
		return web.NewBindJSONError(err)
	}
	// Reject trailing data after the first JSON value (R1-02/R2-01). A body
	// like {"name":"X"}garbage, {"name":"X"}{"name":"Y"}, or
	// {"name":"X"}] must be rejected. decoder.More() only checks for the
	// next element within the current array/object and returns false for
	// unmatched delimiters like ] or }, so it cannot serve as a top-level
	// completeness check. Instead, attempt a second decode and require
	// io.EOF — any other result means there is trailing data.
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return web.NewBindJSONError(errors.New("unexpected trailing data in JSON body"))
	}
	return nil
}

func (httpContext *httpContext) JSON(status int, value any) error {
	httpContext.responseWriter.Header().Set("Content-Type", "application/json")
	httpContext.responseWriter.WriteHeader(status)

	return json.NewEncoder(httpContext.responseWriter).Encode(value)
}

func (httpContext *httpContext) Text(status int, value string) error {
	httpContext.responseWriter.Header().Set("Content-Type", "text/plain; charset=utf-8")
	httpContext.responseWriter.WriteHeader(status)

	_, err := httpContext.responseWriter.Write([]byte(value))
	return err
}

func (httpContext *httpContext) NoContent(status int) error {
	httpContext.responseWriter.WriteHeader(status)
	return nil
}

func (httpContext *httpContext) ResponseWriter() http.ResponseWriter {
	return httpContext.responseWriter
}

func (httpContext *httpContext) Request() *http.Request {
	return httpContext.request
}
