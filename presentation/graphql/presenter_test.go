package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gqlgraphql "github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

// testLogger builds a slog logger writing into buf so tests can assert
// server-side logging without touching the process-global default.
func testLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, nil))
}

// clientInputError is a stand-in for a host's client-caused error type
// (e.g. the host read layer's ClientSearchError): its message names only
// the rejected input.
type clientInputError struct{ msg string }

func (e *clientInputError) Error() string { return e.msg }

// stubFailingSchema emits one resolver-style error through
// gqlgraphql.AddError — exactly what gqlgen's generated resolvers do — so
// presenter composition tests exercise the production error path (executor
// installs the presenter; AddError presents through it).
type stubFailingSchema struct {
	stubSchema
	err error
}

func (s stubFailingSchema) Exec(ctx context.Context) gqlgraphql.ResponseHandler {
	return func(ctx context.Context) *gqlgraphql.Response {
		gqlgraphql.AddError(ctx, s.err)
		return &gqlgraphql.Response{Data: []byte(`null`)}
	}
}

// decodeGraphQLErrors parses the error list of a GraphQL JSON envelope.
func decodeGraphQLErrors(t *testing.T, body string) []struct {
	Message    string `json:"message"`
	Extensions struct {
		Code string `json:"code"`
	} `json:"extensions"`
} {
	t.Helper()
	var envelope struct {
		Errors []struct {
			Message    string `json:"message"`
			Extensions struct {
				Code string `json:"code"`
			} `json:"extensions"`
		} `json:"errors"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("response is not a GraphQL envelope: %v (body=%q)", err, body)
	}
	if len(envelope.Errors) != 1 {
		t.Fatalf("want exactly one error, got %d in %q", len(envelope.Errors), body)
	}
	return envelope.Errors
}

func TestSafeErrorPresenter_NilErrorReturnsNil(t *testing.T) {
	t.Parallel()

	if got := SafeErrorPresenter(nil, nil)(context.Background(), nil); got != nil {
		t.Fatalf("nil error must present nil, got %v", got)
	}
}

func TestSafeErrorPresenter_ClientErrorPreservedAsBadUserInput(t *testing.T) {
	t.Parallel()

	cause := &clientInputError{msg: "limit must be between 1 and 50, got 99"}
	present := SafeErrorPresenter(func(err error) bool {
		var e *clientInputError
		return errors.As(err, &e)
	}, nil)

	for name, err := range map[string]error{
		"direct":   cause,
		"wrapped":  fmt.Errorf("colors: %w", cause),
		"gqlerror": gqlerror.WrapPath(nil, cause),
	} {
		got := present(context.Background(), err)
		if got == nil {
			t.Fatalf("%s: presented nil", name)
		}
		if !strings.Contains(got.Message, "limit must be between 1 and 50") {
			t.Fatalf("%s: client message must be preserved verbatim, got %q", name, got.Message)
		}
		if code, ok := extensionCode(got); !ok || code != CodeBadUserInput {
			t.Fatalf("%s: code = %q (ok=%v), want %q", name, code, ok, CodeBadUserInput)
		}
	}
}

func TestSafeErrorPresenter_CodedErrorsPassThroughUntouched(t *testing.T) {
	t.Parallel()

	present := SafeErrorPresenter(nil, nil)

	for _, code := range []string{
		"GRAPHQL_VALIDATION_FAILED",
		"GRAPHQL_PARSE_FAILED",
		"COMPLEXITY_LIMIT_EXCEEDED",
		"SOME_DEVELOPER_CODE",
	} {
		original := gqlerror.Errorf("detailed protocol message for %s", code)
		original.Extensions = map[string]any{"code": code}
		got := present(context.Background(), original)
		if got != original {
			t.Fatalf("code %s: coded errors must pass through as the SAME error", code)
		}
		if got.Message != original.Message {
			t.Fatalf("code %s: message must be untouched", code)
		}
	}
}

func TestSafeErrorPresenter_UncodedErrorsMaskedAndLogged(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	present := SafeErrorPresenter(nil, testLogger(&logBuf))

	leaky := fmt.Errorf("search masterdata_color: connection refused to meili:7700")
	got := present(context.Background(), leaky)
	if got == nil {
		t.Fatal("presented nil")
	}
	if got.Message != MaskedInternalErrorMessage {
		t.Fatalf("message = %q, want masked %q", got.Message, MaskedInternalErrorMessage)
	}
	if code, ok := extensionCode(got); !ok || code != CodeInternalServerError {
		t.Fatalf("code = %q (ok=%v), want %q", code, ok, CodeInternalServerError)
	}
	// The real error is server-side evidence only.
	if !strings.Contains(logBuf.String(), "connection refused to meili:7700") {
		t.Fatalf("masked error must be logged server-side, log=%q", logBuf.String())
	}
	if !strings.Contains(logBuf.String(), "masked at boundary") {
		t.Fatalf("log must identify the masking event, log=%q", logBuf.String())
	}
}

func TestSafeErrorPresenter_ClassifierNeverSeesNil(t *testing.T) {
	t.Parallel()

	var classified int
	present := SafeErrorPresenter(func(error) bool { classified++; return false }, nil)
	_ = present(context.Background(), errors.New("boom"))
	if classified != 1 {
		t.Fatalf("classifier calls = %d, want 1", classified)
	}
	// nil short-circuits before classification.
	_ = present(context.Background(), nil)
	if classified != 1 {
		t.Fatalf("nil error must not reach the classifier, calls = %d", classified)
	}
}

// TestNewHandler_PresenterCompositionThroughExecutor proves the presenter
// is installed on the gqlgen server and runs on the real resolver-error
// path: a resolver error presented through gqlgraphql.AddError (what
// generated code calls) is masked in the HTTP envelope, while a classified
// client error keeps its message with BAD_USER_INPUT.
func TestNewHandler_PresenterCompositionThroughExecutor(t *testing.T) {
	t.Parallel()

	if stubSchemaErr != nil {
		t.Fatalf("stub schema failed to load: %v", stubSchemaErr)
	}

	post := func(t *testing.T, handler http.Handler) string {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ ok }"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer t")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d (GraphQL errors answer 200), body: %s", rec.Code, rec.Body.String())
		}
		return rec.Body.String()
	}

	t.Run("internal error masked", func(t *testing.T) {
		t.Parallel()
		var logBuf bytes.Buffer
		handler := NewHandler(HandlerConfig{
			Schema:      stubFailingSchema{err: errors.New("meili: dial tcp 10.0.0.9:7700: connection refused")},
			Auth:        func(next http.Handler) http.Handler { return next },
			ErrorLogger: testLogger(&logBuf),
		})
		body := post(t, handler)
		errs := decodeGraphQLErrors(t, body)
		if errs[0].Message != MaskedInternalErrorMessage {
			t.Fatalf("message = %q, want masked", errs[0].Message)
		}
		if errs[0].Extensions.Code != CodeInternalServerError {
			t.Fatalf("code = %q, want %q", errs[0].Extensions.Code, CodeInternalServerError)
		}
		if strings.Contains(body, "10.0.0.9") {
			t.Fatalf("masked error leaked infrastructure detail: %s", body)
		}
		if !strings.Contains(logBuf.String(), "connection refused") {
			t.Fatalf("masked error must be logged server-side, log=%q", logBuf.String())
		}
	})

	t.Run("client error preserved", func(t *testing.T) {
		t.Parallel()
		handler := NewHandler(HandlerConfig{
			Schema: stubFailingSchema{err: &clientInputError{msg: "offset must be >= 0, got -1"}},
			Auth:   func(next http.Handler) http.Handler { return next },
			IsClientError: func(err error) bool {
				var e *clientInputError
				return errors.As(err, &e)
			},
		})
		body := post(t, handler)
		errs := decodeGraphQLErrors(t, body)
		if errs[0].Message != "offset must be >= 0, got -1" {
			t.Fatalf("message = %q, want the client message verbatim", errs[0].Message)
		}
		if errs[0].Extensions.Code != CodeBadUserInput {
			t.Fatalf("code = %q, want %q", errs[0].Extensions.Code, CodeBadUserInput)
		}
	})
}

// TestNewHandler_GraphQLValidationErrorsPassThrough proves gqlgen's own
// protocol errors keep their detailed client-feedback messages through the
// presenter: an unknown field stays GRAPHQL_VALIDATION_FAILED and a syntax
// error stays GRAPHQL_PARSE_FAILED, neither masked as internal.
func TestNewHandler_GraphQLValidationErrorsPassThrough(t *testing.T) {
	t.Parallel()

	if stubSchemaErr != nil {
		t.Fatalf("stub schema failed to load: %v", stubSchemaErr)
	}

	handler := NewHandler(HandlerConfig{
		Schema: stubSchema{},
		Auth:   func(next http.Handler) http.Handler { return next },
	})

	cases := []struct {
		name    string
		query   string
		wantMsg string
		wantCod string
	}{
		{
			name:    "unknown field",
			query:   `{ nope }`,
			wantMsg: `Cannot query field "nope"`,
			wantCod: "GRAPHQL_VALIDATION_FAILED",
		},
		{
			name:    "syntax error",
			query:   `{ ok `,
			wantMsg: `Expected Name`,
			wantCod: "GRAPHQL_PARSE_FAILED",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			payload, err := json.Marshal(map[string]string{"query": tc.query})
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer t")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			errs := decodeGraphQLErrors(t, rec.Body.String())
			if !strings.Contains(errs[0].Message, tc.wantMsg) {
				t.Fatalf("message = %q, must keep the detailed protocol message %q", errs[0].Message, tc.wantMsg)
			}
			if errs[0].Extensions.Code != tc.wantCod {
				t.Fatalf("code = %q, want %q", errs[0].Extensions.Code, tc.wantCod)
			}
		})
	}
}
