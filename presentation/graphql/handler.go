package graphql

import (
	"log/slog"
	"net/http"
	"time"

	gqlgraphql "github.com/99designs/gqlgen/graphql"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	defaultMaxDepth      = 10
	defaultMaxComplexity = 1000
	defaultTimeout       = 10 * time.Second
)

// HandlerConfig configures a GraphQL HTTP handler built by NewHandler. Zero
// values for numeric and duration fields fall back to documented defaults.
type HandlerConfig struct {
	// Schema is the gqlgen executable schema to serve. Required.
	Schema gqlgraphql.ExecutableSchema

	// MaxDepth is the maximum allowed selection depth. Zero means 10.
	MaxDepth int

	// MaxComplexity is the gqlgen complexity limit. Zero means 1000.
	MaxComplexity int

	// Timeout is the per-request execution timeout. Zero means 10s.
	Timeout time.Duration

	// MaxBodyBytes is the request-body cap. Zero (or negative) means 1 MiB.
	MaxBodyBytes int64

	// AllowAnonymous permits requests with no bearer token (development).
	// When false, missing tokens are rejected with 401. Ignored when Auth
	// is set — the wrapper owns the policy.
	AllowAnonymous bool

	// EnableIntrospection enables gqlgen's introspection extension
	// (development). When false, introspection is disabled.
	EnableIntrospection bool

	// Auth optionally replaces the built-in mock auth middleware with a
	// caller-supplied wrapper. Production hosts that already enforce bearer
	// authentication on the surrounding chain pass a passthrough wrapper
	// (return next) so authentication is enforced exactly once, by the
	// platform's authenticator — never by the phase-2 mock. Nil keeps the
	// mock behavior (WithMockAuth with AllowAnonymous).
	Auth func(http.Handler) http.Handler

	// IsClientError optionally classifies resolver errors as client-caused.
	// The error's message must be safe for clients (it names only the
	// rejected input). Classified errors are presented verbatim under the
	// stable BAD_USER_INPUT code; uncoded, unclassified errors are masked
	// as INTERNAL_SERVER_ERROR and logged server-side. Nil classifies
	// nothing. See SafeErrorPresenter.
	IsClientError func(error) bool

	// ErrorLogger receives masked internal errors server-side. Nil uses
	// slog.Default().
	ErrorLogger *slog.Logger
}

// NewHandler creates an http.Handler with all GraphQL protections applied:
// bearer-token authentication (mock by default, caller-supplied in
// production), query depth limit, request-body limit, gqlgen complexity
// limit, a per-request timeout, the safe error presenter (BAD_USER_INPUT
// preservation, GraphQL protocol-error pass-through, INTERNAL_SERVER_ERROR
// masking), and an optional introspection toggle.
//
// The middleware order is (outermost first):
//
//	Auth -> WithDepthLimit -> WithBodyLimit -> http.TimeoutHandler -> gqlgen server
//
// so auth runs before the query body is inspected, the depth guard runs
// before the timed gqlgen execution, and the body cap runs before gqlgen
// decodes anything.
//
// The gqlgen server accepts ONLY the query transports: OPTIONS, GET, and
// JSON POST. The multipart transport is deliberately NOT registered — it
// exists for mutations with file uploads, and this handler serves
// query-only read layers; a multipart request is rejected by gqlgen with
// 400 "transport not supported".
func NewHandler(config HandlerConfig) http.Handler {
	if config.Schema == nil {
		panic("graphql.NewHandler: HandlerConfig.Schema is required")
	}

	maxDepth := config.MaxDepth
	if maxDepth == 0 {
		maxDepth = defaultMaxDepth
	}
	maxComplexity := config.MaxComplexity
	if maxComplexity == 0 {
		maxComplexity = defaultMaxComplexity
	}
	timeout := config.Timeout
	if timeout == 0 {
		timeout = defaultTimeout
	}

	srv := handler.New(config.Schema)
	srv.AddTransport(transport.Options{})
	srv.AddTransport(transport.GET{})
	srv.AddTransport(transport.POST{})

	srv.SetQueryCache(lru.New[*ast.QueryDocument](1000))
	srv.SetParserTokenLimit(2000)
	srv.SetErrorPresenter(SafeErrorPresenter(config.IsClientError, config.ErrorLogger))

	srv.Use(extension.FixedComplexityLimit(maxComplexity))
	if config.EnableIntrospection {
		srv.Use(extension.Introspection{})
	}

	h := http.TimeoutHandler(srv, timeout, `{"errors":[{"message":"request timeout"}]}`)
	h = WithBodyLimit(h, config.MaxBodyBytes)
	h = WithDepthLimit(h, maxDepth)
	if config.Auth != nil {
		h = config.Auth(h)
	} else {
		h = WithMockAuth(h, config.AllowAnonymous)
	}
	return h
}
