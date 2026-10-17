package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/aliighahremanii/buildingblocks/application/correlation"
)

// TestSetCorrelationAttributesAsserts that the correlation IDs from ctx
// are stamped onto the span as attributes when present, and that empty
// values are skipped to avoid noisy correlation_id="" attributes.
//
// Subtests run sequentially (not t.Parallel): they share the in-memory
// exporter and tracer provider set up by the parent, so parallel
// subtests would race on exp.Reset()/GetSpans().
func TestSetCorrelationAttributesAsserts(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp), // synchronous so we can assert immediately
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	original := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(original) })

	t.Run("all_three_present", func(t *testing.T) {
		exp.Reset()

		ctx := correlation.WithContext(context.Background(), correlation.Context{
			CorrelationID: "corr-7",
			CausationID:   "cause-7",
			CommandID:     "cmd-7",
		})
		_, span := Start(ctx, "with-corr")
		SetCorrelationAttributes(span, ctx)
		span.End()

		stubs := exp.GetSpans()
		if len(stubs) != 1 {
			t.Fatalf("spans = %d, want 1", len(stubs))
		}
		attrs := attrMap(stubs[0].Attributes)
		assertAttr(t, attrs, "correlation_id", "corr-7")
		assertAttr(t, attrs, "causation_id", "cause-7")
		assertAttr(t, attrs, "command_id", "cmd-7")
	})

	t.Run("some_empty_skipped", func(t *testing.T) {
		exp.Reset()

		// CausationID and CommandID empty: only correlation_id should
		// be stamped.
		ctx := correlation.WithContext(context.Background(), correlation.Context{
			CorrelationID: "corr-8",
		})
		_, span := Start(ctx, "partial-corr")
		SetCorrelationAttributes(span, ctx)
		span.End()

		stubs := exp.GetSpans()
		if len(stubs) != 1 {
			t.Fatalf("spans = %d, want 1", len(stubs))
		}
		attrs := attrMap(stubs[0].Attributes)
		assertAttr(t, attrs, "correlation_id", "corr-8")
		if _, ok := attrs["causation_id"]; ok {
			t.Errorf("causation_id should be absent, got %q", attrs["causation_id"])
		}
		if _, ok := attrs["command_id"]; ok {
			t.Errorf("command_id should be absent, got %q", attrs["command_id"])
		}
	})

	t.Run("no_correlation_context_is_noop", func(t *testing.T) {
		exp.Reset()

		// Plain background context carries no correlation Context.
		ctx := context.Background()
		_, span := Start(ctx, "no-corr")
		SetCorrelationAttributes(span, ctx)
		span.End()

		stubs := exp.GetSpans()
		if len(stubs) != 1 {
			t.Fatalf("spans = %d, want 1", len(stubs))
		}
		for _, a := range stubs[0].Attributes {
			if key := string(a.Key); key == "correlation_id" || key == "causation_id" || key == "command_id" {
				t.Errorf("unexpected correlation attribute %q=%v on span with no correlation ctx", key, a.Value.AsString())
			}
		}
	})

	t.Run("nil_span_is_safe", func(t *testing.T) {
		// SpanFromContext returns nil when no real span is active.
		// SetCorrelationAttributes(nil, ctx) must not panic.
		ctx := correlation.WithContext(context.Background(), correlation.Context{CorrelationID: "x"})
		SetCorrelationAttributes(nil, ctx) // must not panic
	})
}

// attrMap converts a slice of attributes into a lookup map keyed by the
// attribute key. Used to assert specific attributes exist with the right
// value without depending on attribute ordering.
func attrMap(attrs []attribute.KeyValue) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, a := range attrs {
		m[string(a.Key)] = a.Value.AsString()
	}
	return m
}

// assertAttr fails the test if the attribute at key does not equal want.
func assertAttr(t *testing.T, m map[string]string, key, want string) {
	t.Helper()
	got, ok := m[key]
	if !ok {
		t.Errorf("attribute %q missing; want %q", key, want)
		return
	}
	if got != want {
		t.Errorf("attribute %q = %q, want %q", key, got, want)
	}
}
