package middleware

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/aliighahremanii/buildingblocks/application/correlation"
	apperrors "github.com/aliighahremanii/buildingblocks/application/errors"
	"github.com/aliighahremanii/buildingblocks/infrastructure/webhttp"
	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

type testProblemResponse struct {
	Code    string             `json:"code"`
	Message string             `json:"message"`
	Errors  []testProblemField `json:"errors,omitempty"`
}

type testProblemField struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type fakeContext struct {
	ctx context.Context
}

func (f fakeContext) Context() context.Context { return f.ctx }
func (fakeContext) Method() string             { return "GET" }
func (fakeContext) Path() string               { return "/" }
func (fakeContext) Query(string) string        { return "" }
func (fakeContext) Param(string) string        { return "" }
func (fakeContext) Header(string) string       { return "" }
func (fakeContext) SetHeader(string, string)   {}
func (fakeContext) BindJSON(any) error         { return nil }
func (fakeContext) JSON(int, any) error        { return nil }
func (fakeContext) Text(int, string) error     { return nil }
func (fakeContext) NoContent(int) error        { return nil }

func TestWriteProblemErrorMapsValidationError(t *testing.T) {
	t.Parallel()

	response := executeWriteProblemError(t, apperrors.NewValidationError(
		apperrors.Field("Email.Invalid", "email is invalid"),
		apperrors.Field("Password.Required", "password is required"),
	))

	assertProblemResponse(t, response, http.StatusUnprocessableEntity, testProblemResponse{
		Code:    apperrors.ValidationErrorCode,
		Message: apperrors.ValidationErrorMessage,
		Errors: []testProblemField{
			{Code: "Email.Invalid", Message: "email is invalid"},
			{Code: "Password.Required", Message: "password is required"},
		},
	})
}

func TestWriteProblemErrorMapsUnauthorizedError(t *testing.T) {
	t.Parallel()

	response := executeWriteProblemError(t, apperrors.NewAuthenticationRequiredError())

	assertProblemResponse(t, response, http.StatusUnauthorized, testProblemResponse{
		Code:    apperrors.AuthenticationRequiredCode,
		Message: apperrors.AuthenticationRequiredMessage,
	})
}

func TestWriteProblemErrorMapsNotFoundError(t *testing.T) {
	t.Parallel()

	response := executeWriteProblemError(t, apperrors.NewNotFoundError("sample item", "id-1"))

	assertProblemResponse(t, response, http.StatusNotFound, testProblemResponse{
		Code:    apperrors.NotFoundCode,
		Message: apperrors.NotFoundMessage("sample item"),
	})
}

func TestWriteProblemErrorMapsInternalError(t *testing.T) {
	t.Parallel()

	response := executeWriteProblemError(t, errors.New("boom"))

	assertProblemResponse(t, response, http.StatusInternalServerError, testProblemResponse{
		Code:    apperrors.InternalErrorCode,
		Message: apperrors.InternalErrorMessage,
	})
}

// recordingFallbackContext is a web.Context that captures the JSON status
// and body written through it, so a test can assert both the masked client
// response AND that the underlying error never leaks into it.
type recordingFallbackContext struct {
	ctx    context.Context
	method string
	path   string
	status int
	body   []byte
}

func (r *recordingFallbackContext) Context() context.Context { return r.ctx }
func (r *recordingFallbackContext) Method() string           { return r.method }
func (r *recordingFallbackContext) Path() string             { return r.path }
func (r *recordingFallbackContext) Query(string) string      { return "" }
func (r *recordingFallbackContext) Param(string) string      { return "" }
func (r *recordingFallbackContext) Header(string) string     { return "" }
func (r *recordingFallbackContext) SetHeader(string, string) {}
func (r *recordingFallbackContext) BindJSON(any) error       { return nil }
func (r *recordingFallbackContext) JSON(status int, value any) error {
	r.status = status
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	r.body = body
	return nil
}
func (r *recordingFallbackContext) Text(int, string) error { return nil }
func (r *recordingFallbackContext) NoContent(int) error    { return nil }
func (r *recordingFallbackContext) ResponseWriter() http.ResponseWriter {
	return nil
}
func (r *recordingFallbackContext) Request() *http.Request { return nil }

// TestWriteProblemErrorFallbackLogsCauseWithoutClientLeak is the D127
// observability regression: the unclassified fallback branch writes a
// GENERIC 500 problem to the client (no leak of the underlying error or any
// sensitive internals) but logs the cause server-side with the underlying
// error plus the request's method, path, and correlation metadata — the
// production tombstoned-stream incident was undiagnosable precisely because
// this branch never logged.
//
// Deliberately NOT t.Parallel(): the test swaps the process-global
// slog.Default. Parallel tests in this package exercise the fallback path
// (TestWriteProblemErrorMapsInternalError et al.), which reads
// slog.Default() concurrently — a SetDefault racing those reads trips -race.
// Serial top-level tests never overlap parallel tests in the same test
// binary, so the global swap is confined to this test's execution window.
func TestWriteProblemErrorFallbackLogsCauseWithoutClientLeak(t *testing.T) {
	// Capture slog output by swapping the default logger for the test.
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	ctx := &recordingFallbackContext{
		ctx: correlation.WithContext(context.Background(), correlation.Context{
			CorrelationID: "corr-tombstone-1",
			CommandID:     "cmd-tombstone-1",
		}),
		method: http.MethodPost,
		path:   "/api/operations/v1/bijaks",
	}
	underlying := errors.New("eventstore recv truckbijakslot-x: [ErrorCodeStreamDeleted] stream is deleted")

	err := WriteProblemError(ctx, underlying, testResponse, testField)
	if err != nil {
		t.Fatalf("WriteProblemError returned error: %v", err)
	}

	// The client sees ONLY the generic masked problem — the raw store error
	// must not leak into the body.
	if ctx.status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", ctx.status, http.StatusInternalServerError)
	}
	var clientBody testProblemResponse
	if err := json.Unmarshal(ctx.body, &clientBody); err != nil {
		t.Fatalf("decode client body: %v", err)
	}
	if clientBody.Code != apperrors.InternalErrorCode || clientBody.Message != apperrors.InternalErrorMessage {
		t.Fatalf("client body = %+v, want the generic masked InternalError problem", clientBody)
	}
	if strings.Contains(string(ctx.body), "ErrorCodeStreamDeleted") || strings.Contains(string(ctx.body), "truckbijakslot") {
		t.Fatalf("client body leaks the underlying error: %s", ctx.body)
	}

	// The server log carries the cause and the request/correlation metadata.
	logged := buf.String()
	for _, want := range []string{
		"ErrorCodeStreamDeleted",    // the underlying error
		"corr-tombstone-1",          // correlation id
		"cmd-tombstone-1",           // command id
		http.MethodPost,             // method
		"/api/operations/v1/bijaks", // path
	} {
		if !strings.Contains(logged, want) {
			t.Fatalf("fallback log missing %q; log=%q", want, logged)
		}
	}
}

// TestWriteProblemErrorFallbackLogsCauseWithoutCorrelation verifies the
// fallback logging works when the context carries no correlation Context
// (background workers, direct handler tests) — the error, method, and path
// are still logged.
//
// Deliberately NOT t.Parallel(): see the global slog.Default note on
// TestWriteProblemErrorFallbackLogsCauseWithoutClientLeak.
func TestWriteProblemErrorFallbackLogsCauseWithoutCorrelation(t *testing.T) {
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelError})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	ctx := &recordingFallbackContext{
		ctx:    context.Background(),
		method: http.MethodGet,
		path:   "/health",
	}

	if err := WriteProblemError(ctx, errors.New("disk on fire"), testResponse, testField); err != nil {
		t.Fatalf("WriteProblemError returned error: %v", err)
	}

	logged := buf.String()
	if !strings.Contains(logged, "disk on fire") {
		t.Fatalf("fallback log missing the underlying error; log=%q", logged)
	}
	if strings.Contains(logged, "correlation_id") {
		t.Fatalf("fallback log must omit absent correlation metadata; log=%q", logged)
	}
}

// TestWriteProblemErrorMapsJSONSyntaxError verifies that a malformed JSON body
// wrapped in BindJSONError (as the webhttp adapter produces) maps to HTTP 400
// Bad Request, not 500 Internal Error (D-01/R2-03).
func TestWriteProblemErrorMapsJSONSyntaxError(t *testing.T) {
	t.Parallel()

	// Trigger a real json.SyntaxError by decoding invalid JSON, then wrap it
	// in BindJSONError as the webhttp adapter does.
	dec := json.NewDecoder(strings.NewReader("not-json"))
	var v map[string]any
	rawErr := dec.Decode(&v)
	if rawErr == nil {
		t.Fatal("expected a decode error from invalid JSON")
	}
	err := &web.BindJSONError{Err: rawErr}

	response := executeWriteProblemError(t, err)

	assertProblemResponse(t, response, http.StatusBadRequest, testProblemResponse{
		Code:    apperrors.BadRequestCode,
		Message: apperrors.InvalidRequestBodyMessage,
	})
}

// TestWriteProblemErrorMapsJSONTypeMismatchError verifies that a type-mismatched
// JSON value wrapped in BindJSONError maps to HTTP 400 Bad Request (D-01/R2-03).
func TestWriteProblemErrorMapsJSONTypeMismatchError(t *testing.T) {
	t.Parallel()

	err := &web.BindJSONError{
		Err: &json.UnmarshalTypeError{
			Value:  "number",
			Type:   reflect.TypeOf(""),
			Offset: 10,
		},
	}

	response := executeWriteProblemError(t, err)

	assertProblemResponse(t, response, http.StatusBadRequest, testProblemResponse{
		Code:    apperrors.BadRequestCode,
		Message: apperrors.InvalidRequestBodyMessage,
	})
}

// TestWriteProblemErrorMapsTruncatedJSON verifies that io.ErrUnexpectedEOF
// (truncated JSON body) wrapped in BindJSONError maps to 400 (R2-03).
func TestWriteProblemErrorMapsTruncatedJSON(t *testing.T) {
	t.Parallel()

	err := &web.BindJSONError{Err: io.ErrUnexpectedEOF}

	response := executeWriteProblemError(t, err)

	assertProblemResponse(t, response, http.StatusBadRequest, testProblemResponse{
		Code:    apperrors.BadRequestCode,
		Message: apperrors.InvalidRequestBodyMessage,
	})
}

// TestWriteProblemErrorMapsEmptyBody verifies that io.EOF (empty body) wrapped
// in BindJSONError maps to 400 (R2-03).
func TestWriteProblemErrorMapsEmptyBody(t *testing.T) {
	t.Parallel()

	err := &web.BindJSONError{Err: io.EOF}

	response := executeWriteProblemError(t, err)

	assertProblemResponse(t, response, http.StatusBadRequest, testProblemResponse{
		Code:    apperrors.BadRequestCode,
		Message: apperrors.InvalidRequestBodyMessage,
	})
}

// TestWriteProblemErrorMapsConcurrencyError verifies that a domain-level
// optimistic-concurrency conflict (returned by eventstore.Save when another
// command committed first) surfaces as HTTP 409 with the canonical conflict
// problem code. This is the transport-visible end of the OCC fix: a client
// receiving 409 knows to reload the aggregate and re-apply its intent rather
// than blindly retrying, preventing silent lost updates.
func TestWriteProblemErrorMapsConcurrencyError(t *testing.T) {
	t.Parallel()

	// Use a wrapped error to verify errors.As traversal through typical
	// wrapping the runtime produces.
	inner := apperrors.NewConcurrencyError("country", "uuid-1", 5, 7)
	wrapped := wrapForTest(inner)

	response := executeWriteProblemError(t, wrapped)

	assertProblemResponse(t, response, http.StatusConflict, testProblemResponse{
		Code:    apperrors.ConcurrencyConflictCode,
		Message: apperrors.ConcurrencyConflictMessage,
	})
}

// wrapForTest wraps err in a fmt.Errorf("%w") chain to verify errors.As
// traversal works through typical wrapping.
func wrapForTest(err error) error {
	return &testWrappedErr{cause: err}
}

type testWrappedErr struct{ cause error }

func (w *testWrappedErr) Error() string { return "wrapped: " + w.cause.Error() }
func (w *testWrappedErr) Unwrap() error { return w.cause }

func executeWriteProblemError(t *testing.T, err error) *httptest.ResponseRecorder {
	t.Helper()

	mux := http.NewServeMux()
	router := webhttp.NewRouter(mux)
	router.Get("/test", func(ctx web.Context) error {
		return WriteProblemError(ctx, err, testResponse, testField)
	})

	request := httptest.NewRequest(http.MethodGet, "/test", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	return response
}

func testResponse(code string, message string, fields []testProblemField) testProblemResponse {
	return testProblemResponse{Code: code, Message: message, Errors: fields}
}

func testField(code string, message string) testProblemField {
	return testProblemField{Code: code, Message: message}
}

func assertProblemResponse(t *testing.T, response *httptest.ResponseRecorder, expectedStatus int, expected testProblemResponse) {
	t.Helper()

	if response.Code != expectedStatus {
		t.Fatalf("expected status %d, got %d", expectedStatus, response.Code)
	}

	var actual testProblemResponse
	if err := json.Unmarshal(response.Body.Bytes(), &actual); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}

	if actual.Code != expected.Code {
		t.Fatalf("expected code %q, got %q", expected.Code, actual.Code)
	}
	if actual.Message != expected.Message {
		t.Fatalf("expected message %q, got %q", expected.Message, actual.Message)
	}
	if len(actual.Errors) != len(expected.Errors) {
		t.Fatalf("expected %d field errors, got %d", len(expected.Errors), len(actual.Errors))
	}
	for i := range expected.Errors {
		if actual.Errors[i] != expected.Errors[i] {
			t.Fatalf("expected field error %+v, got %+v", expected.Errors[i], actual.Errors[i])
		}
	}
}
