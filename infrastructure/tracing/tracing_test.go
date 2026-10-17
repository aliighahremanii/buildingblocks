package tracing

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// TestConfigFromEnvDefaults asserts the documented defaults when neither
// OTEL_* env var is set. The env vars are read once at call time (not
// cached at package init), so unsetting + calling reproduces the default
// path deterministically.
//
// Not parallel: t.Setenv mutates the process env, which the testing
// package forbids under t.Parallel().
func TestConfigFromEnvDefaults(t *testing.T) {
	t.Setenv(envServiceName, "")
	t.Setenv(envOTLPEndpoint, "")

	cfg := ConfigFromEnv()

	if cfg.ServiceName != defaultServiceNm {
		t.Fatalf("ServiceName = %q, want %q", cfg.ServiceName, defaultServiceNm)
	}
	if cfg.OTLPEndpoint != "" {
		t.Fatalf("OTLPEndpoint = %q, want empty (no-op)", cfg.OTLPEndpoint)
	}
}

// TestConfigFromEnvOverrides asserts the env vars are honored when set.
//
// Not parallel: t.Setenv mutates the process env.
func TestConfigFromEnvOverrides(t *testing.T) {
	t.Setenv(envServiceName, "yardflow-api")
	t.Setenv(envOTLPEndpoint, "otel-collector:4317")

	cfg := ConfigFromEnv()

	if cfg.ServiceName != "yardflow-api" {
		t.Fatalf("ServiceName = %q, want %q", cfg.ServiceName, "yardflow-api")
	}
	if cfg.OTLPEndpoint != "otel-collector:4317" {
		t.Fatalf("OTLPEndpoint = %q, want %q", cfg.OTLPEndpoint, "otel-collector:4317")
	}
}

// TestInitEmptyEndpointIsNoop verifies the documented "tracing disabled"
// path: Init installs a noop TracerProvider, Start works without panic,
// and the span carries an invalid SpanContext (the noop sentinel).
// The returned shutdown closure must be a cheap no-op and idempotent.
//
// NOT parallel: this test and the other Init/Start tests below all swap
// the process-global otel.TracerProvider via otel.SetTracerProvider.
// Running them in parallel races on that global and produces flaky
// "noop span has valid SpanContext" failures.
func TestInitEmptyEndpointIsNoop(t *testing.T) {
	// Snapshot and restore the global provider so this test does not
	// pollute other tests (including the real-tracer test) via the
	// process-global otel.SetTracerProvider.
	original := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(original) })

	shutdown, err := Init(context.Background(), Config{ServiceName: "test", OTLPEndpoint: ""})
	if err != nil {
		t.Fatalf("Init noop: unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	// Start must not panic and must yield a span carrying an invalid
	// SpanContext (the noop span's signature).
	_, span := Start(context.Background(), "noop-test")
	if span == nil {
		t.Fatal("Start returned nil span; want a (noop) trace.Span")
	}
	if span.SpanContext().IsValid() {
		t.Fatalf("noop span SpanContext valid; want invalid: %+v", span.SpanContext())
	}
	span.End()

	// Idempotent shutdown: calling twice must not error.
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown (should be no-op): %v", err)
	}
}

// TestInitWithFakeEndpointReturnsRealTracer verifies Init wires a real
// (non-noop) TracerProvider when OTLPEndpoint is non-empty. The OTLP gRPC
// exporter dials lazily, so an unreachable endpoint does not fail Init.
// The shutdown closure must close the provider cleanly.
//
// We do NOT assert that spans are exported (that would require a live
// collector); instead we assert that a started span carries a valid
// SpanContext — the reliable signature of a real, sampling tracer — and
// that shutdown returns nil.
//
// NOT parallel: see TestInitEmptyEndpointIsNoop (process-global provider).
func TestInitWithFakeEndpointReturnsRealTracer(t *testing.T) {
	original := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(original) })

	shutdown, err := Init(context.Background(), Config{
		ServiceName:  "yardflow-test",
		OTLPEndpoint: "localhost:0", // unreachable but valid host:port
	})
	if err != nil {
		t.Fatalf("Init real: unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(context.Background()) })

	_, span := Start(context.Background(), "real-test")
	if span == nil {
		t.Fatal("Start returned nil span; want a real trace.Span")
	}
	if !span.SpanContext().IsValid() {
		t.Fatalf("real span SpanContext invalid; want valid: %+v", span.SpanContext())
	}
	span.End()

	// Shutdown flushes pending spans (none delivered, since endpoint is
	// unreachable) and closes the gRPC connection. It must not leak.
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// TestStartProducesRealSpanWithInMemoryExporter is the positive control
// for span content: bypass Init (which is hardcoded to the OTLP gRPC
// exporter) and wire a TracerProvider whose spans land in an in-memory
// exporter. This verifies our Start wrapper and SpanFromContext helper
// see exactly what the SDK records.
//
// NOT parallel: see TestInitEmptyEndpointIsNoop (process-global provider).
func TestStartProducesRealSpanWithInMemoryExporter(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		// SyncSpanProcessor would let us assert synchronously, but
		// BatchSpanProcessor + a forced Shutdown flush is the realistic
		// production-shaped path and exercises the same Start wrapper.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	original := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(original) })

	ctx, span := Start(context.Background(), "in-memory-test")
	if !span.SpanContext().IsValid() {
		t.Fatalf("span SpanContext invalid; want valid: %+v", span.SpanContext())
	}

	// SpanFromContext on the span-started context should return the
	// same non-nil span.
	if got := SpanFromContext(ctx); got == nil {
		t.Fatal("SpanFromContext returned nil for a real span context")
	}

	span.End()

	// Force the batch processor to flush so the exporter sees the span.
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}

	stubs := exp.GetSpans()
	if len(stubs) != 1 {
		t.Fatalf("exported spans = %d, want 1", len(stubs))
	}
	if stubs[0].Name != "in-memory-test" {
		t.Fatalf("span name = %q, want %q", stubs[0].Name, "in-memory-test")
	}
}

// TestSpanFromContextNilWhenNoSpan covers the nil-return contract:
// SpanFromContext on a background context returns nil (no real span
// active), not the SDK's noopSpan. This is the helper's distinguishing
// behavior vs. trace.SpanFromContext.
//
// NOT parallel: see TestInitEmptyEndpointIsNoop (process-global provider).
func TestSpanFromContextNilWhenNoSpan(t *testing.T) {
	// Use a noop provider explicitly so we are in a known state
	// regardless of test ordering.
	original := otel.GetTracerProvider()
	otel.SetTracerProvider(trace.NewNoopTracerProvider())
	t.Cleanup(func() { otel.SetTracerProvider(original) })

	if got := SpanFromContext(context.Background()); got != nil {
		t.Fatalf("SpanFromContext(background) = %v, want nil", got)
	}
}
