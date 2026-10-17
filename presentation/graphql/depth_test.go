package graphql

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQueryDepthCountsNesting(t *testing.T) {
	cases := []struct {
		query string
		want  int
	}{
		{``, 0},
		{`{ searchCountries { total } }`, 2},
		{`{ searchCountries { items { id } } }`, 3},
		{`query { searchCountries { items { n { n2 { id } } } } }`, 5},
		{`{ a { b { c } } d }`, 3},
	}
	for _, tc := range cases {
		got, err := QueryDepth(tc.query)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tc.query, err)
		}
		if got != tc.want {
			t.Errorf("QueryDepth(%q) = %d, want %d", tc.query, got, tc.want)
		}
	}
}

func TestQueryDepthRejectsMalformed(t *testing.T) {
	if _, err := QueryDepth(`{ broken `); err == nil {
		t.Fatalf("expected parse error for malformed query")
	}
}

func TestSelectionDepthEmpty(t *testing.T) {
	if got := SelectionDepth(nil); got != 0 {
		t.Fatalf("SelectionDepth(nil) = %d, want 0", got)
	}
}

func TestWithDepthLimitAllowsShallow(t *testing.T) {
	called := false
	h := WithDepthLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}), 10)

	body := gqlJSON(`{ searchCountries { total } }`)
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatalf("expected inner handler to be called")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
}

func TestWithDepthLimitAllowsAtExactBoundary(t *testing.T) {
	const maxDepth = 3
	called := false
	h := WithDepthLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}), maxDepth)

	// depth 3 == maxDepth: must pass (only strictly greater is rejected).
	body := gqlJSON(`{ searchCountries { items { id } } }`)
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatalf("inner handler must be called when depth == maxDepth")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 at boundary, got %d (body=%q)", rec.Code, rec.Body.String())
	}
}

func TestWithDepthLimitRejectsDeep(t *testing.T) {
	const maxDepth = 10
	called := false
	h := WithDepthLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}), maxDepth)

	nesting := maxDepth + 3
	inner := "id"
	for i := 0; i < nesting; i++ {
		inner = "n { " + inner + " }"
	}
	body := gqlJSON("query { searchCountries { items " + inner + " } }")
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if called {
		t.Fatalf("inner handler must NOT be called for deep query")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (body=%q)", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "depth") {
		t.Fatalf("error body should mention depth, got %q", rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected json content-type")
	}
}

func TestWithDepthLimitRejectsDeepQueryParamOnGet(t *testing.T) {
	const maxDepth = 2
	called := false
	h := WithDepthLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}), maxDepth)

	// depth 4 via GET ?query= must be rejected before the handler runs.
	req := httptest.NewRequest(http.MethodGet, "/graphql?query={a{b{c{d}}}}", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if called {
		t.Fatalf("inner handler must NOT be called for deep GET query")
	}
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (body=%q)", rec.Code, rec.Body.String())
	}
}

func TestWithDepthLimitAllowsShallowQueryParamOnGet(t *testing.T) {
	called := false
	h := WithDepthLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}), 10)

	req := httptest.NewRequest(http.MethodGet, "/graphql?query={a{b}}", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatalf("inner handler must be called for shallow GET query")
	}
}

func TestWithDepthLimitSkipsNonGraphQLMethod(t *testing.T) {
	called := false
	h := WithDepthLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}), 10)

	req := httptest.NewRequest(http.MethodDelete, "/graphql", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !called {
		t.Fatalf("non-POST/GET must bypass depth check")
	}
}

func TestWithDepthLimitSkipsNonGraphQLContentType(t *testing.T) {
	called := false
	h := WithDepthLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}), 1)

	// A POST with text/plain is not GraphQL-shaped; the limiter must pass it
	// through untouched rather than reject it.
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`not json`))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if !called {
		t.Fatalf("non-GraphQL content type must bypass depth check")
	}
}

func TestWithDepthLimitRestoresBodyForDownstream(t *testing.T) {
	const maxDepth = 10
	var downstreamBody string
	h := WithDepthLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		downstreamBody = string(raw)
	}), maxDepth)

	original := gqlJSON(`{ searchCountries { total } }`)
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(original))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if downstreamBody != original {
		t.Fatalf("body not restored for downstream reader:\n got: %q\nwant: %q", downstreamBody, original)
	}
}

func TestIsGraphQLContentType(t *testing.T) {
	cases := map[string]bool{
		"application/json":            true,
		"application/json; charset=x": true,
		"application/graphql":         true,
		"application/graphql+json":    true,
		"APPLICATION/JSON":            true,
		"text/plain":                  false,
		"":                            false,
	}
	for ct, want := range cases {
		if got := isGraphQLContentType(ct); got != want {
			t.Errorf("isGraphQLContentType(%q) = %v, want %v", ct, got, want)
		}
	}
}

func TestReadAndRestoreBodyRewiresRequest(t *testing.T) {
	original := `{"query":"{ __typename }"}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(original))
	data, err := readAndRestoreBody(req)
	if err != nil {
		t.Fatalf("read error: %v", err)
	}
	if string(data) != original {
		t.Fatalf("read data mismatch: %q", data)
	}
	restored, err := readAndRestoreBody(req)
	if err != nil {
		t.Fatalf("second read error: %v", err)
	}
	if string(restored) != original {
		t.Fatalf("body not restored: %q", restored)
	}
}

func TestReadAndRestoreBodyNilBody(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Body = nil
	data, err := readAndRestoreBody(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data != nil {
		t.Fatalf("expected nil data for nil body, got %v", data)
	}
}

func TestExtractQueryField(t *testing.T) {
	cases := []struct {
		body string
		want string
	}{
		{`{"query":"{ a }"}`, "{ a }"},
		{`{"query":"  { a }  "}`, "{ a }"},
		{`{"variables":{}}`, ""},
		{`not json`, ""},
		{`   `, ""},
	}
	for _, tc := range cases {
		if got := extractQueryField([]byte(tc.body)); got != tc.want {
			t.Errorf("extractQueryField(%q) = %q, want %q", tc.body, got, tc.want)
		}
	}
}

func TestSanitizeMessage(t *testing.T) {
	cases := map[string]string{
		`plain`:           `plain`,
		`has "quote"`:     `has \"quote\"`,
		"new\nline":       `new\nline`,
		"back\\slash":     `back\\slash`,
		"tab\tand\rcr":    `tab\tand\rcr`,
		"control\x01char": "controlchar",
	}
	for in, want := range cases {
		if got := sanitizeMessage(in); got != want {
			t.Errorf("sanitizeMessage(%q) = %q, want %q", in, got, want)
		}
	}
}

// gqlJSON builds a GraphQL JSON request body carrying the given query.
func gqlJSON(query string) string {
	b, _ := json.Marshal(map[string]any{"query": query})
	return string(b)
}
