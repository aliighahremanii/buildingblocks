package graphql

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMockUserFromToken(t *testing.T) {
	cases := []struct {
		token string
		want  string
	}{
		{"", ""},
		{"   ", ""},
		{"dev-token", "dev-token"},
		{"header.payload.signature", "payload"},
		{"a.b.c.d", "a.b.c.d"}, // not exactly 3 parts → whole token
	}
	for _, tc := range cases {
		got := mockUserFromToken(tc.token)
		if got.ID != tc.want {
			t.Errorf("mockUserFromToken(%q).ID = %q, want %q", tc.token, got.ID, tc.want)
		}
		if got.Roles != nil {
			t.Errorf("mockUserFromToken(%q).Roles = %v, want nil", tc.token, got.Roles)
		}
	}
}

func TestBearerToken(t *testing.T) {
	cases := []struct {
		header string
		want   string
	}{
		{"", ""},
		{"Bearer abc123", "abc123"},
		{"Bearer   spaced  ", "spaced"},
		{"bearer lowercase", "bearer lowercase"}, // only "Bearer " exact prefix is stripped
		{"rawtoken", "rawtoken"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if tc.header != "" {
			req.Header.Set("Authorization", tc.header)
		}
		if got := bearerToken(req); got != tc.want {
			t.Errorf("bearerToken(%q) = %q, want %q", tc.header, got, tc.want)
		}
	}
}

func TestUserFromContextAbsent(t *testing.T) {
	if u, ok := UserFromContext(context.Background()); ok {
		t.Fatalf("expected ok=false on bare context, got %+v", u)
	}
}

func TestWithUserRoundTrip(t *testing.T) {
	ctx := WithUser(context.Background(), User{ID: "u1", Roles: []string{"admin"}})
	u, ok := UserFromContext(ctx)
	if !ok {
		t.Fatalf("expected user present after WithUser")
	}
	if u.ID != "u1" {
		t.Fatalf("ID = %q, want u1", u.ID)
	}
	if len(u.Roles) != 1 || u.Roles[0] != "admin" {
		t.Fatalf("Roles = %v, want [admin]", u.Roles)
	}
}

func TestWithMockAuthAllowsAuthenticatedWhenAllowAnonymousFalse(t *testing.T) {
	var gotUser User
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		u, ok := UserFromContext(r.Context())
		gotUser = u
		_ = ok
		w.WriteHeader(http.StatusOK)
	})
	h := WithMockAuth(inner, false)

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer header.payload.sig")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatalf("inner handler must be called for authenticated request")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if gotUser.ID != "payload" {
		t.Fatalf("context user ID = %q, want payload", gotUser.ID)
	}
}

func TestWithMockAuthRejectsMissingTokenWhenAllowAnonymousFalse(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})
	h := WithMockAuth(inner, false)

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if called {
		t.Fatalf("inner handler must NOT be called without a token in production mode")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	if rec.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("expected WWW-Authenticate: Bearer")
	}
	if !strings.Contains(rec.Body.String(), "unauthorized") {
		t.Fatalf("error body should mention unauthorized, got %q", rec.Body.String())
	}
}

func TestWithMockAuthAnonymousPassesThroughWhenAllowed(t *testing.T) {
	userPresent := false
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		_, ok := UserFromContext(r.Context())
		userPresent = ok
		w.WriteHeader(http.StatusOK)
	})
	h := WithMockAuth(inner, true)

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !called {
		t.Fatalf("inner handler must be called for anonymous dev access")
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if userPresent {
		t.Fatalf("no user must be on context for anonymous access")
	}
}

func TestWithMockAuthAnonymousDevStillAuthenticatesToken(t *testing.T) {
	var gotUser User
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, _ = UserFromContext(r.Context())
	})
	h := WithMockAuth(inner, true)

	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer dev-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if gotUser.ID != "dev-token" {
		t.Fatalf("context user ID = %q, want dev-token", gotUser.ID)
	}
}

func TestWithMockAuthEmptyBearerRejectedWhenNotAnonymous(t *testing.T) {
	called := false
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	})
	h := WithMockAuth(inner, false)

	// "Bearer " with only whitespace after the scheme must be treated as no
	// token and therefore rejected in production mode.
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer   ")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if called {
		t.Fatalf("inner handler must NOT be called for empty bearer")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}
