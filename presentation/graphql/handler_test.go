package graphql

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gqlgraphql "github.com/99designs/gqlgen/graphql"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
)

// stubSchema is a minimal ExecutableSchema so handler-composition tests do
// not depend on a real generated schema. Exec returns a handler writing a
// canned GraphQL response; Schema serves a one-field SDL so gqlgen's
// operation validation accepts `{ ok }`.
type stubSchema struct{}

var stubQuerySchema, stubSchemaErr = gqlparser.LoadSchema(&ast.Source{
	Name: "stub.graphql", Input: "type Query { ok: Boolean! }",
})

func (stubSchema) Schema() *ast.Schema { return stubQuerySchema }

func (stubSchema) Complexity(_ context.Context, _, _ string, _ int, _ map[string]any) (int, bool) {
	return 1, true
}

func (stubSchema) Exec(_ context.Context) gqlgraphql.ResponseHandler {
	return func(ctx context.Context) *gqlgraphql.Response {
		return &gqlgraphql.Response{Data: []byte(`{"ok":true}`)}
	}
}

func TestNewHandler_CustomAuthWrapperReplacesMockAuth(t *testing.T) {
	t.Parallel()

	if stubSchemaErr != nil {
		t.Fatalf("stub schema failed to load: %v", stubSchemaErr)
	}

	var wrapperRan bool
	handler := NewHandler(HandlerConfig{
		Schema: stubSchema{},
		Auth: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				wrapperRan = true
				if auth := r.Header.Get("Authorization"); auth == "" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				next.ServeHTTP(w, r)
			})
		},
	})
	// Prove the wrapper is OUTSIDE the pipeline: without a token the request
	// must be rejected before gqlgen (mock auth would behave identically for
	// a missing token with AllowAnonymous=false, but the wrapper's distinct
	// 401 body shape — empty, not the GraphQL envelope — distinguishes it).
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ ok }"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if !wrapperRan {
		t.Fatal("custom Auth wrapper never ran")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 from the custom wrapper", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ ok }"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer anything")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 with token (mock auth must be bypassed: any non-empty token passes)", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"ok"`) {
		t.Fatalf("unexpected body %q", rec.Body.String())
	}
}

func TestNewHandler_MockAuthStillDefaultWithoutWrapper(t *testing.T) {
	t.Parallel()

	handler := NewHandler(HandlerConfig{Schema: stubSchema{}})
	// Default mock auth with AllowAnonymous=false: no token -> 401 with the
	// mock envelope.
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ ok }"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (default mock auth preserved)", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "unauthorized") {
		t.Fatalf("unexpected body %q", rec.Body.String())
	}
}

func TestNewHandler_BodyLimitComposedBeforeGqlgen(t *testing.T) {
	t.Parallel()

	handler := NewHandler(HandlerConfig{
		Schema:       stubSchema{},
		Auth:         func(next http.Handler) http.Handler { return next },
		MaxBodyBytes: 32,
	})
	req := httptest.NewRequest(http.MethodPost, "/graphql", nil)
	req.ContentLength = 33
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 from composed body limit", rec.Code)
	}
}

func TestNewHandler_NilSchemaPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("NewHandler with nil Schema must panic")
		}
	}()
	_ = NewHandler(HandlerConfig{})
}

// TestNewHandler_MultipartRejected pins the query-only transport surface:
// the gqlgen server registers ONLY Options/GET/POST. The multipart
// transport (mutation file uploads) is deliberately not registered, so a
// multipart request must be rejected by gqlgen with 400 "transport not
// supported" — never parsed or executed.
func TestNewHandler_MultipartRejected(t *testing.T) {
	t.Parallel()

	if stubSchemaErr != nil {
		t.Fatalf("stub schema failed to load: %v", stubSchemaErr)
	}

	handler := NewHandler(HandlerConfig{
		Schema: stubSchema{},
		Auth:   func(next http.Handler) http.Handler { return next },
	})

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	_ = mw.WriteField("query", `{ ok }`)
	_ = mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/graphql", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer t")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (multipart transport not registered)", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "transport not supported") {
		t.Fatalf("unexpected rejection body: %s", rec.Body.String())
	}
}
