package graphql

import (
	"context"
	"net/http"
	"strings"
)

// User represents the authenticated principal established from a JWT/OIDC
// token. It is stored on the request context by WithMockAuth and retrieved by
// UserFromContext.
//
// The mock implementation (WithMockAuth) populates only ID; Roles is left nil
// and will be populated once real OIDC/JWT verification is plugged in.
type User struct {
	ID    string
	Roles []string
}

// userCtxKey is the context key under which the authenticated user is stored on
// the request context. It is unexported so callers must go through
// UserFromContext / WithUser.
type userCtxKey struct{}

// WithUser returns ctx with user stored under the GraphQL user context key. It
// is the integration point a real OIDC/JWT middleware uses to stash the
// verified principal.
func WithUser(ctx context.Context, user User) context.Context {
	return context.WithValue(ctx, userCtxKey{}, user)
}

// UserFromContext extracts the authenticated user from ctx. The bool result is
// false when no user has been set on the context (e.g. anonymous dev access).
func UserFromContext(ctx context.Context) (User, bool) {
	u, ok := ctx.Value(userCtxKey{}).(User)
	return u, ok
}

// WithMockAuth wraps next with mock bearer-token authentication.
//
// Behavior:
//   - A request carrying `Authorization: Bearer <token>` with a non-empty token
//     is accepted; a best-effort user id is derived from the token and placed on
//     the request context.
//   - When allowAnonymous is true, a request without a token is allowed through
//     with no user on the context (anonymous dev access).
//   - When allowAnonymous is false, a request without a token is rejected with
//     401.
//
// This is a phase-2 mock: it does not validate a JWT signature. It exists to
// carve out the place real OIDC/JWT verification will plug in and to keep the
// GraphQL context carrying a user identity today.
func WithMockAuth(next http.Handler, allowAnonymous bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := mockUserFromToken(bearerToken(r))
		if user.ID == "" {
			if !allowAnonymous {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("WWW-Authenticate", "Bearer")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"errors":[{"message":"unauthorized"}]}`))
				return
			}
			// Anonymous dev access: pass through without a user on the context.
			next.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithUser(r.Context(), user)))
	})
}

// bearerToken extracts the raw token from the Authorization header, stripping
// the "Bearer " scheme prefix when present.
func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if h == "" {
		return ""
	}
	const scheme = "Bearer "
	if strings.HasPrefix(h, scheme) {
		return strings.TrimSpace(h[len(scheme):])
	}
	return strings.TrimSpace(h)
}

// mockUserFromToken derives a best-effort User from a bearer token. For the dev
// mock any non-empty token is accepted; if the token is a dot-separated
// JWT-shaped string, the subject segment is taken from the payload middle
// segment, otherwise the raw token is used as the id. This never validates a
// signature.
func mockUserFromToken(token string) User {
	token = strings.TrimSpace(token)
	if token == "" {
		return User{}
	}
	parts := strings.Split(token, ".")
	if len(parts) == 3 {
		return User{ID: parts[1]}
	}
	return User{ID: token}
}
