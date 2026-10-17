package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aliighahremanii/buildingblocks/application/correlation"
	"github.com/aliighahremanii/buildingblocks/presentation/auth"
	"github.com/aliighahremanii/buildingblocks/presentation/middleware"
)

func TestWithActorMetadataStampsUserIDWhenAuthenticated(t *testing.T) {
	t.Parallel()

	var got correlation.Context
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req = req.WithContext(auth.WithAuthenticatedUser(req.Context(), "user-123"))
	// Pre-seed a correlation context (as WithCorrelationID would).
	req = req.WithContext(correlation.WithContext(req.Context(), correlation.Context{
		CorrelationID: "corr-1",
		CausationID:   "cause-1",
		CommandID:     "cmd-1",
	}))

	h := middleware.WithActorMetadata(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, _ = correlation.FromContext(r.Context())
	}))
	h.ServeHTTP(rec, req)

	if got.UserID != "user-123" {
		t.Fatalf("UserID = %q, want %q", got.UserID, "user-123")
	}
	if got.CorrelationID != "corr-1" {
		t.Fatalf("CorrelationID = %q, want %q", got.CorrelationID, "corr-1")
	}
	if got.CausationID != "cause-1" {
		t.Fatalf("CausationID = %q, want %q", got.CausationID, "cause-1")
	}
	if got.CommandID != "cmd-1" {
		t.Fatalf("CommandID = %q, want %q", got.CommandID, "cmd-1")
	}
}

func TestWithActorMetadataLeavesUserIDEmptyWhenAnonymous(t *testing.T) {
	t.Parallel()

	var got correlation.Context
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req = req.WithContext(correlation.WithContext(req.Context(), correlation.Context{
		CorrelationID: "corr-2",
	}))

	h := middleware.WithActorMetadata(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, _ = correlation.FromContext(r.Context())
	}))
	h.ServeHTTP(rec, req)

	if got.UserID != "" {
		t.Fatalf("anonymous UserID = %q, want empty", got.UserID)
	}
	if got.CorrelationID != "corr-2" {
		t.Fatalf("CorrelationID not preserved: %q", got.CorrelationID)
	}
}

func TestWithActorMetadataMintsCorrelationWhenAbsent(t *testing.T) {
	t.Parallel()

	var got correlation.Context
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req = req.WithContext(auth.WithAuthenticatedUser(req.Context(), "u-9"))

	h := middleware.WithActorMetadata(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, _ = correlation.FromContext(r.Context())
	}))
	h.ServeHTTP(rec, req)

	if got.UserID != "u-9" {
		t.Fatalf("UserID = %q, want u-9", got.UserID)
	}
	if got.CorrelationID == "" {
		t.Fatal("CorrelationID should have been minted")
	}
	if got.CommandID == "" {
		t.Fatal("CommandID should have been minted")
	}
	if got.CausationID == "" {
		t.Fatal("CausationID should have been minted")
	}
}

// TestUnauthenticatedResponseCarriesCorrelationID verifies the §5.1 requirement
// that all responses, including unauthenticated ones, contain a Correlation ID.
// Because WithCorrelationID is the outermost middleware, even a request that
// never authenticates reaches the actor middleware with a correlation context
// already attached; the response therefore can carry the header downstream.
func TestUnauthenticatedResponseCarriesCorrelationID(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	// Simulate full host order: correlation first (outer), then actor (inner).
	outer := middleware.WithCorrelationID(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	outer.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if rec.Header().Get(middleware.CorrelationHeaderName) == "" {
		t.Fatal("unauthenticated response missing X-Correlation-ID header")
	}
}
