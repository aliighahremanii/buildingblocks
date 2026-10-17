package graphql

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWithBodyLimit_RejectsDeclaredOversizedBody(t *testing.T) {
	t.Parallel()

	var reached bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})

	handler := WithBodyLimit(next, 64)

	req := httptest.NewRequest(http.MethodPost, "/graphql", nil)
	req.ContentLength = 65 // declared too large
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if reached {
		t.Fatal("downstream handler must not be reached for an oversized body")
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	var body struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not a GraphQL error envelope: %v (body=%q)", err, rec.Body.String())
	}
	if len(body.Errors) != 1 || !strings.HasPrefix(body.Errors[0].Message, "request body exceeds") {
		t.Fatalf("unexpected error envelope: %+v", body.Errors)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q, want application/json", ct)
	}
}

func TestWithBodyLimit_AllowsBodyWithinLimit(t *testing.T) {
	t.Parallel()

	var body string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		body = string(data)
		w.WriteHeader(http.StatusOK)
	})

	handler := WithBodyLimit(next, 128)

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ ok }"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if body != `{"query":"{ ok }"}` {
		t.Fatalf("downstream body = %q", body)
	}
}

func TestWithBodyLimit_UndeclaredLengthCappedByMaxBytesReader(t *testing.T) {
	t.Parallel()

	var readErr error
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, readErr = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	handler := WithBodyLimit(next, 16)

	// ContentLength is -1 for a request whose body length is unknown.
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(strings.Repeat("x", 64)))
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if readErr == nil {
		t.Fatal("reading a 64-byte body under a 16-byte cap must fail via MaxBytesReader")
	}
	var maxBytesErr *http.MaxBytesError
	if !errors.As(readErr, &maxBytesErr) {
		t.Fatalf("read error must be *http.MaxBytesError, got %T: %v", readErr, readErr)
	}
}

func TestWithBodyLimit_NonPositiveLimitFallsBackToDefault(t *testing.T) {
	t.Parallel()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := WithBodyLimit(next, 0)

	// A 2 MiB declared body exceeds the 1 MiB default.
	req := httptest.NewRequest(http.MethodPost, "/graphql", nil)
	req.ContentLength = 2 << 20
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413 (default 1 MiB cap)", rec.Code)
	}
}
