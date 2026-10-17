package webhttp

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/aliighahremanii/buildingblocks/presentation/auth"
)

type Principal struct {
	UserID string
	Roles  []string
}

type BearerAuthenticator interface {
	AuthenticateBearerToken(ctx context.Context, token string) (Principal, bool, error)
}

func WithBearerAuthentication(next http.Handler, authenticator BearerAuthenticator) http.Handler {
	if authenticator == nil {
		return next
	}
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		token, ok := bearerToken(request.Header.Get("Authorization"))
		if !ok {
			next.ServeHTTP(responseWriter, request)
			return
		}

		principal, authenticated, err := authenticator.AuthenticateBearerToken(request.Context(), token)
		if err == nil && authenticated && strings.TrimSpace(principal.UserID) != "" {
			request = request.WithContext(auth.WithAuthenticatedPrincipal(request.Context(), principal.UserID, principal.Roles))
		}
		next.ServeHTTP(responseWriter, request)
	})
}

// WithRequiredBearerAuthentication rejects missing, invalid, and unverifiable
// tokens before a request reaches application routes.
func WithRequiredBearerAuthentication(next http.Handler, authenticator BearerAuthenticator) http.Handler {
	return http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		if authenticator == nil {
			writeUnauthorized(responseWriter)
			return
		}
		token, ok := bearerToken(request.Header.Get("Authorization"))
		if !ok {
			writeUnauthorized(responseWriter)
			return
		}
		principal, authenticated, err := authenticator.AuthenticateBearerToken(request.Context(), token)
		if err != nil || !authenticated || strings.TrimSpace(principal.UserID) == "" {
			writeUnauthorized(responseWriter)
			return
		}
		request = request.WithContext(auth.WithAuthenticatedPrincipal(request.Context(), principal.UserID, principal.Roles))
		next.ServeHTTP(responseWriter, request)
	})
}

func writeUnauthorized(responseWriter http.ResponseWriter) {
	responseWriter.Header().Set("Content-Type", "application/problem+json")
	responseWriter.Header().Set("WWW-Authenticate", "Bearer")
	responseWriter.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(responseWriter).Encode(map[string]string{
		"code": "Unauthorized", "message": "A valid bearer token is required.",
	})
}

func bearerToken(header string) (string, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || strings.TrimSpace(parts[1]) == "" {
		return "", false
	}
	return parts[1], true
}

type StaticBearerAuthenticator struct {
	principals map[string]Principal
}

type JWTAuthenticatorConfig struct {
	PublicKey  []byte
	Issuer     string
	Audience   string
	UserClaim  string
	RolesClaim string
}

type JWTBearerAuthenticator struct {
	key        any
	methods    []string
	issuer     string
	audience   string
	userClaim  string
	rolesClaim string
}

func NewJWTBearerAuthenticator(config JWTAuthenticatorConfig) (*JWTBearerAuthenticator, error) {
	if len(config.PublicKey) == 0 || strings.TrimSpace(config.Issuer) == "" || strings.TrimSpace(config.Audience) == "" {
		return nil, errors.New("JWT public key, issuer, and audience are required")
	}
	key, methods, err := parseJWTVerificationKey(config.PublicKey)
	if err != nil {
		return nil, err
	}
	userClaim := strings.TrimSpace(config.UserClaim)
	if userClaim == "" {
		userClaim = "sub"
	}
	rolesClaim := strings.TrimSpace(config.RolesClaim)
	if rolesClaim == "" {
		rolesClaim = "roles"
	}
	return &JWTBearerAuthenticator{key: key, methods: methods, issuer: config.Issuer, audience: config.Audience, userClaim: userClaim, rolesClaim: rolesClaim}, nil
}

func parseJWTVerificationKey(publicKey []byte) (any, []string, error) {
	if key, err := jwt.ParseRSAPublicKeyFromPEM(publicKey); err == nil {
		return key, []string{"RS256", "RS384", "RS512", "PS256", "PS384", "PS512"}, nil
	}
	if key, err := jwt.ParseECPublicKeyFromPEM(publicKey); err == nil {
		return key, []string{"ES256", "ES384", "ES512"}, nil
	}
	if key, err := jwt.ParseEdPublicKeyFromPEM(publicKey); err == nil {
		return key, []string{"EdDSA"}, nil
	}
	return nil, nil, errors.New("JWT public key must be RSA, ECDSA, or Ed25519 PEM")
}

func (a *JWTBearerAuthenticator) AuthenticateBearerToken(_ context.Context, rawToken string) (Principal, bool, error) {
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(rawToken, claims, func(token *jwt.Token) (any, error) {
		switch a.key.(type) {
		case *rsa.PublicKey, *ecdsa.PublicKey, ed25519.PublicKey:
			return a.key, nil
		default:
			return nil, errors.New("unsupported JWT verification key")
		}
	}, jwt.WithValidMethods(a.methods), jwt.WithIssuer(a.issuer), jwt.WithAudience(a.audience), jwt.WithExpirationRequired())
	if err != nil || !token.Valid {
		return Principal{}, false, nil
	}
	userID := strings.TrimSpace(fmt.Sprint(claims[a.userClaim]))
	if userID == "" || userID == "<nil>" {
		return Principal{}, false, nil
	}
	return Principal{UserID: userID, Roles: claimStrings(claims[a.rolesClaim])}, true, nil
}

func claimStrings(value any) []string {
	switch typed := value.(type) {
	case string:
		return strings.FieldsFunc(typed, func(r rune) bool { return r == ',' || r == ' ' })
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if value := strings.TrimSpace(fmt.Sprint(item)); value != "" {
				out = append(out, value)
			}
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	default:
		return nil
	}
}

func NewStaticBearerAuthenticator(principals map[string]Principal) *StaticBearerAuthenticator {
	copied := make(map[string]Principal, len(principals))
	for token, principal := range principals {
		if strings.TrimSpace(token) == "" || strings.TrimSpace(principal.UserID) == "" {
			continue
		}
		copied[token] = Principal{UserID: strings.TrimSpace(principal.UserID), Roles: append([]string(nil), principal.Roles...)}
	}
	return &StaticBearerAuthenticator{principals: copied}
}

func (a *StaticBearerAuthenticator) AuthenticateBearerToken(ctx context.Context, token string) (Principal, bool, error) {
	principal, ok := a.principals[token]
	if !ok {
		return Principal{}, false, nil
	}
	return Principal{UserID: principal.UserID, Roles: append([]string(nil), principal.Roles...)}, true, nil
}

func NewStaticBearerAuthenticatorFromSpec(spec string) *StaticBearerAuthenticator {
	principals := map[string]Principal{}
	for _, entry := range strings.Split(spec, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			continue
		}
		token := strings.TrimSpace(parts[0])
		principalSpec := strings.TrimSpace(parts[1])
		principalParts := strings.SplitN(principalSpec, ":", 2)
		userID := strings.TrimSpace(principalParts[0])
		roles := []string{}
		if len(principalParts) == 2 {
			for _, role := range strings.Split(principalParts[1], ",") {
				role = strings.TrimSpace(role)
				if role != "" {
					roles = append(roles, role)
				}
			}
		}
		if token != "" && userID != "" {
			principals[token] = Principal{UserID: userID, Roles: roles}
		}
	}
	if len(principals) == 0 {
		return nil
	}
	return NewStaticBearerAuthenticator(principals)
}
