package webhttp

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/aliighahremanii/buildingblocks/presentation/auth"
)

func TestWithRequiredBearerAuthenticationRejectsMissingAndInvalidTokens(t *testing.T) {
	authenticator := NewStaticBearerAuthenticator(map[string]Principal{"valid": {UserID: "user-1"}})
	handler := WithRequiredBearerAuthentication(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("protected handler must not run")
	}), authenticator)
	for _, header := range []string{"", "Bearer invalid"} {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.Header.Set("Authorization", header)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized || response.Header().Get("WWW-Authenticate") != "Bearer" {
			t.Fatalf("header=%q status=%d authenticate=%q", header, response.Code, response.Header().Get("WWW-Authenticate"))
		}
	}
}

func TestJWTBearerAuthenticatorValidatesIssuerAudienceExpiryAndClaims(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	publicPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})
	authenticator, err := NewJWTBearerAuthenticator(JWTAuthenticatorConfig{PublicKey: publicPEM, Issuer: "https://issuer.example", Audience: "yardflow"})
	if err != nil {
		t.Fatal(err)
	}
	claims := jwt.MapClaims{"sub": "user-1", "roles": []string{"Admin", "Manager"}, "iss": "https://issuer.example", "aud": "yardflow", "exp": time.Now().Add(time.Hour).Unix()}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	raw, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	principal, ok, err := authenticator.AuthenticateBearerToken(t.Context(), raw)
	if err != nil || !ok || principal.UserID != "user-1" || len(principal.Roles) != 2 {
		t.Fatalf("principal=%+v ok=%v err=%v", principal, ok, err)
	}
	claims["aud"] = "other"
	token = jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	raw, _ = token.SignedString(privateKey)
	if _, ok, _ := authenticator.AuthenticateBearerToken(t.Context(), raw); ok {
		t.Fatal("wrong audience token must be rejected")
	}
}

func TestWithBearerAuthenticationPopulatesPrincipal(t *testing.T) {
	t.Parallel()

	authenticator := NewStaticBearerAuthenticator(map[string]Principal{"token-1": {UserID: "admin", Roles: []string{"Admin"}}})
	handler := WithBearerAuthentication(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userID, ok := auth.UserID(r.Context())
		if !ok || userID != "admin" {
			t.Fatalf("userID = %q, %v", userID, ok)
		}
		if !auth.HasRole(r.Context(), "Admin") {
			t.Fatal("Admin role missing from context")
		}
	}), authenticator)

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer token-1")
	handler.ServeHTTP(httptest.NewRecorder(), request)
}

func TestWithBearerAuthenticationLeavesInvalidTokenUnauthenticated(t *testing.T) {
	t.Parallel()

	authenticator := NewStaticBearerAuthenticator(map[string]Principal{"token-1": {UserID: "admin"}})
	handler := WithBearerAuthentication(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth.IsAuthenticated(r.Context()) {
			t.Fatal("invalid bearer token should not authenticate")
		}
	}), authenticator)

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer wrong")
	handler.ServeHTTP(httptest.NewRecorder(), request)
}

func TestNewStaticBearerAuthenticatorFromSpec(t *testing.T) {
	t.Parallel()

	authenticator := NewStaticBearerAuthenticatorFromSpec("admin-token=admin:Admin,Manager; operator-token=operator:Manager")
	if authenticator == nil {
		t.Fatal("authenticator missing")
	}
	principal, ok, err := authenticator.AuthenticateBearerToken(t.Context(), "admin-token")
	if err != nil || !ok {
		t.Fatalf("AuthenticateBearerToken ok = %v, err = %v", ok, err)
	}
	if principal.UserID != "admin" || len(principal.Roles) != 2 || principal.Roles[0] != "Admin" || principal.Roles[1] != "Manager" {
		t.Fatalf("principal = %#v", principal)
	}
}

func TestBearerTokenParser(t *testing.T) {
	t.Parallel()

	if token, ok := bearerToken("Bearer abc"); !ok || token != "abc" {
		t.Fatalf("bearerToken valid = %q, %v", token, ok)
	}
	for _, header := range []string{"", "Basic abc", "Bearer", "Bearer   "} {
		if token, ok := bearerToken(header); ok || token != "" {
			t.Fatalf("bearerToken(%q) = %q, %v", header, token, ok)
		}
	}
}
