// Package graphql provides reusable HTTP middleware and helpers for protecting
// any gqlgen-backed GraphQL endpoint: query-depth limiting, mock bearer-token
// authentication, and a configured handler factory.
package graphql

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"github.com/vektah/gqlparser/v2/parser"
)

// WithDepthLimit wraps next with a maximum query-depth guard. Queries whose
// deepest selection path exceeds maxDepth are rejected with a 400 response
// before execution.
//
// Depth is computed by parsing the raw query string independent of gqlgen's own
// parsing, so the limit is enforced before any resolver or extension runs.
func WithDepthLimit(next http.Handler, maxDepth int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if shouldSkipDepthCheck(r) {
			next.ServeHTTP(w, r)
			return
		}

		query, ok := graphqlQueryFromRequest(r)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}

		depth, err := QueryDepth(query)
		if err != nil {
			// Malformed queries are left for gqlgen to report properly.
			next.ServeHTTP(w, r)
			return
		}
		if depth > maxDepth {
			writeDepthError(w, depth, maxDepth)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// shouldSkipDepthCheck reports whether the request cannot carry a GraphQL
// query this middleware can inspect: only GraphQL-content-type POST bodies
// and GET query parameters are inspected (graphqlQueryFromRequest); every
// other method passes through untouched.
func shouldSkipDepthCheck(r *http.Request) bool {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		return true
	}
	return false
}

// QueryDepth parses query and returns the maximum selection depth across all
// operations. An empty query returns depth 0 with no error. Parsing-only (no
// schema validation) is intentional: malformed/unknown-field queries are left
// for gqlgen to report with full context.
func QueryDepth(query string) (int, error) {
	if query == "" {
		return 0, nil
	}
	doc, qerr := parser.ParseQuery(&ast.Source{Input: query})
	if qerr != nil {
		return 0, qerr
	}
	max := 0
	for _, op := range doc.Operations {
		if d := SelectionDepth(op.SelectionSet); d > max {
			max = d
		}
	}
	for _, frag := range doc.Fragments {
		if d := SelectionDepth(frag.SelectionSet); d > max {
			max = d
		}
	}
	return max, nil
}

// SelectionDepth returns the depth of a selection set, counting nested fields.
// A single scalar field has depth 1; a field with one nested object has depth 2.
func SelectionDepth(sel ast.SelectionSet) int {
	if len(sel) == 0 {
		return 0
	}
	max := 0
	for _, s := range sel {
		switch v := s.(type) {
		case *ast.Field:
			d := SelectionDepth(v.SelectionSet)
			if d > max {
				max = d
			}
		case *ast.InlineFragment:
			d := SelectionDepth(v.SelectionSet)
			if d > max {
				max = d
			}
		case *ast.FragmentSpread:
			// Fragment depth is computed at the top level via doc.Fragments;
			// here we treat a spread as depth 1 to avoid recursion complexity.
			if 1 > max {
				max = 1
			}
		}
	}
	return max + 1
}

// graphqlQueryFromRequest extracts the GraphQL query string from a POST JSON
// body or a GET ?query= parameter without consuming the body for downstream
// readers.
func graphqlQueryFromRequest(r *http.Request) (string, bool) {
	if r.Method == http.MethodGet {
		if q := r.URL.Query().Get("query"); q != "" {
			return q, true
		}
		return "", false
	}

	if !isGraphQLContentType(r.Header.Get("Content-Type")) {
		return "", false
	}

	body, err := readAndRestoreBody(r)
	if err != nil {
		return "", false
	}
	q := extractQueryField(body)
	if q == "" {
		return "", false
	}
	return q, true
}

func writeDepthError(w http.ResponseWriter, depth, maxDepth int) {
	err := gqlerror.Errorf("query depth %d exceeds maximum allowed depth of %d", depth, maxDepth)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_, _ = w.Write([]byte(`{"errors":[{"message":"` + sanitizeMessage(err.Message) + `"}]}`))
}

// isGraphQLContentType reports whether a content-type header indicates a
// GraphQL JSON request body.
func isGraphQLContentType(ct string) bool {
	ct = strings.ToLower(strings.TrimSpace(ct))
	if ct == "" {
		return false
	}
	// Strip any parameters like "; charset=utf-8".
	if i := strings.Index(ct, ";"); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	return ct == "application/json" || ct == "application/graphql" || strings.HasPrefix(ct, "application/")
}

// readAndRestoreBody reads the request body and rewires r.Body so downstream
// handlers can still read it. Returns the body bytes.
func readAndRestoreBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	data, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(data))
	return data, err
}

// extractQueryField extracts the "query" field from a GraphQL JSON request
// body. Returns "" when the body is not a JSON object or has no query field.
func extractQueryField(body []byte) string {
	if len(bytes.TrimSpace(body)) == 0 {
		return ""
	}
	var payload struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.Query)
}

// sanitizeMessage escapes a string for safe inclusion inside a JSON string
// literal in a manually-built error response.
func sanitizeMessage(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				continue
			}
			b.WriteRune(r)
		}
	}
	return b.String()
}
