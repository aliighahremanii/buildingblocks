package middleware

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"github.com/aliighahremanii/buildingblocks/application/correlation"
	"github.com/aliighahremanii/buildingblocks/presentation/web"
)

// CorrelationHeaderName is the HTTP header that carries the correlation ID.
// When a client supplies it, the same value propagates to every event written
// while serving the request; when absent the middleware mints a fresh UUID so
// every request has one.
const CorrelationHeaderName = "X-Correlation-ID"

// CorrelationID returns a web.Middleware that reads the X-Correlation-ID
// header from the request (minting a fresh UUID when absent), mints a fresh
// command ID, and stores both in the request context so downstream command
// handlers and the event store can stamp them onto written events.
//
// The command ID doubles as the causation ID for the events written by this
// command: a single inbound HTTP command directly causes its events.
func CorrelationID() web.Middleware {
	return func(next web.HandlerFunc) web.HandlerFunc {
		return func(ctx web.Context) error {
			c := newContextFromID(ctx.Header(CorrelationHeaderName))
			return next(enrichWithContext(ctx, c))
		}
	}
}

// WithCorrelationID wraps an http.Handler with the same correlation logic as
// CorrelationID but at the net/http layer. This is the form used to wrap the
// API host's ServeMux globally — module routes are registered individually
// and web.Router has no global Use, so host-level wiring must happen above
// the mux.
func WithCorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := newContextFromID(r.Header.Get(CorrelationHeaderName))
		// Echo the correlation ID back on every response (including 401/500)
		// so callers can trace a request across services and logs. §5.1.
		w.Header().Set(CorrelationHeaderName, c.CorrelationID)
		r = r.WithContext(correlation.WithContext(r.Context(), c))
		next.ServeHTTP(w, r)
	})
}

// newContextFromID builds a correlation.Context from the inbound correlation
// ID, minting a fresh UUID when the client did not supply one. The command ID
// is always fresh; it doubles as the causation ID.
func newContextFromID(correlationID string) correlation.Context {
	if correlationID == "" {
		correlationID = uuid.New().String()
	}
	commandID := uuid.New().String()
	return correlation.Context{
		CorrelationID: correlationID,
		CausationID:   commandID,
		CommandID:     commandID,
	}
}

// correlatedContext wraps a web.Context and overrides Context() to return a
// context carrying the correlation Context. All other methods delegate to
// the inner web.Context.
//
// This cannot use interface embedding because web.Context declares a
// Context() method, which collides with the embedded field name "Context".
// Explicit delegation avoids the ambiguity.
type correlatedContext struct {
	inner web.Context
	ctx   context.Context
}

func (c correlatedContext) Context() context.Context            { return c.ctx }
func (c correlatedContext) Method() string                      { return c.inner.Method() }
func (c correlatedContext) Path() string                        { return c.inner.Path() }
func (c correlatedContext) Query(name string) string            { return c.inner.Query(name) }
func (c correlatedContext) Param(name string) string            { return c.inner.Param(name) }
func (c correlatedContext) Header(name string) string           { return c.inner.Header(name) }
func (c correlatedContext) SetHeader(name, value string)        { c.inner.SetHeader(name, value) }
func (c correlatedContext) BindJSON(target any) error           { return c.inner.BindJSON(target) }
func (c correlatedContext) JSON(status int, value any) error    { return c.inner.JSON(status, value) }
func (c correlatedContext) Text(status int, value string) error { return c.inner.Text(status, value) }
func (c correlatedContext) NoContent(status int) error          { return c.inner.NoContent(status) }
func (c correlatedContext) ResponseWriter() http.ResponseWriter { return c.inner.ResponseWriter() }
func (c correlatedContext) Request() *http.Request              { return c.inner.Request() }

// enrichWithContext returns a web.Context whose Context() carries c.
func enrichWithContext(ctx web.Context, c correlation.Context) web.Context {
	return correlatedContext{inner: ctx, ctx: correlation.WithContext(ctx.Context(), c)}
}
