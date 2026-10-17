package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/aliighahremanii/buildingblocks/application/correlation"
)

// TracerName is the single instrumented tracer name used across yardflow. Centralizing
// it ensures every span is attributed to the same tracer and lets collectors
// filter yardflow spans by instrumentation scope.
const TracerName = "yardflow"

// tracer returns the global tracer. It is a package-level helper (not a
// cached *trace.Tracer) so Init/SetTracerProvider swapping at startup is
// observed immediately by callers; caching a tracer at package init would
// pin whichever provider happened to be first.
func tracer() trace.Tracer {
	return otel.Tracer(TracerName)
}

// Start begins a new span named name on the global yardflow tracer and returns a
// context carrying it. The returned span MUST be End()ed (typically via
// `defer span.End()`) or the span is dropped, leaking a record in the
// in-memory exporter buffer.
//
// Callers do not import the otel package directly; this helper is the only
// tracing import most code paths need.
func Start(ctx context.Context, name string) (context.Context, trace.Span) {
	return tracer().Start(ctx, name)
}

// SpanFromContext returns the active span on ctx, or nil when tracing is
// disabled (no span was started). Returning nil — rather than the SDK's
// noopSpan — lets callers cheaply skip attribute work they would otherwise
// perform unconditionally on the no-op span.
func SpanFromContext(ctx context.Context) trace.Span {
	span := trace.SpanFromContext(ctx)
	if span == nil {
		return nil
	}
	// SpanFromContext never returns nil for the SDK, but it returns a
	// non-nil noopSpan when no real span is active. Detect that by
	// checking SpanContext().IsValid(): a noopSpan carries an invalid
	// SpanContext, a real span carries a valid one.
	if !span.SpanContext().IsValid() {
		return nil
	}
	return span
}

// SetCorrelationAttributes stamps the span with the correlation/causation/
// command IDs from ctx, when present. High-value write paths (eventstore
// Save / LoadAggregate) call this so trace backends can fan out a trace by
// correlation ID and cross-reference the audit trail recorded in
// KurrentDB event metadata.
//
// Empty values are skipped so operators do not see noise like
// correlation_id="" when background workers fire without an inbound
// correlation Context.
func SetCorrelationAttributes(span trace.Span, ctx context.Context) {
	if span == nil {
		return
	}
	c, ok := correlation.FromContext(ctx)
	if !ok {
		return
	}
	attrs := make([]attribute.KeyValue, 0, 3)
	if c.CorrelationID != "" {
		attrs = append(attrs, attribute.String("correlation_id", c.CorrelationID))
	}
	if c.CausationID != "" {
		attrs = append(attrs, attribute.String("causation_id", c.CausationID))
	}
	if c.CommandID != "" {
		attrs = append(attrs, attribute.String("command_id", c.CommandID))
	}
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
}
