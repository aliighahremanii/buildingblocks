// Package correlation propagates the audit-trail identifiers required by
// audit finding H8 (correlation/causation/command-ID propagation) across the
// request → command-handler → event-store boundary.
//
// The correlation ID ties together every event written while serving one
// inbound request. The causation ID identifies the command that directly
// caused an event; for a single-command request it equals the command ID.
// The command ID is a fresh UUID minted per inbound command by the HTTP
// middleware.
package correlation

import "context"

// Context carries the audit identifiers for one inbound command.
type Context struct {
	CorrelationID string
	CausationID   string
	CommandID     string
	UserID        string
}

// contextKey is an unexported type so callers cannot construct matching keys
// outside this package; WithContext and FromContext are the only accessors.
type contextKey struct{}

var key contextKey

// WithContext returns a copy of parent that carries c. Retrieve it via
// FromContext.
func WithContext(parent context.Context, c Context) context.Context {
	return context.WithValue(parent, key, c)
}

// FromContext returns the correlation Context stored in ctx and whether one
// was present. When ok is false the returned Context is the zero value.
func FromContext(ctx context.Context) (Context, bool) {
	c, ok := ctx.Value(key).(Context)
	return c, ok
}

// CommandIDFromContext returns the command ID stored in ctx, or "" when no
// correlation Context is present.
func CommandIDFromContext(ctx context.Context) string {
	c, ok := FromContext(ctx)
	if !ok {
		return ""
	}
	return c.CommandID
}
