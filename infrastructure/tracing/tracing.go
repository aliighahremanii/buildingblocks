// Package tracing initializes and exposes the OpenTelemetry tracer for the
// whole yardflow process (API + worker). It centralizes audit finding H8's tracing
// plumbing so callers depend on this building block rather than wiring the
// OTel SDK directly.
//
// Tracing is strictly opt-in: when OTEL_EXPORTER_OTLP_ENDPOINT is unset, Init
// installs a no-op TracerProvider so every tracer.Start call in the codebase
// is safe (records nothing, exports nothing). This keeps local development,
// unit tests, and tracer-unaware deployments free of OTel infrastructure
// while still allowing high-value code paths to instrument themselves
// unconditionally.
package tracing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
)

// Env var names follow the OpenTelemetry specification defaults so standard
// OTel tooling (collectors, sidecars) configures yardflow without bespoke docs.
const (
	envServiceName   = "OTEL_SERVICE_NAME"
	envOTLPEndpoint  = "OTEL_EXPORTER_OTLP_ENDPOINT"
	defaultServiceNm = "yardflow"
)

// Config carries the tracer wiring knobs. It is intentionally tiny: most
// OTel tuning (sampling, batching, TLS) is left at SDK defaults so the
// operational surface stays small. Add fields here only when a knob needs
// to vary per-process.
type Config struct {
	// ServiceName identifies the emitting process in trace backends
	// (e.g. "yardflow-api", "yardflow-worker"). Defaults to "yardflow".
	ServiceName string

	// OTLPEndpoint is the OTLP gRPC target
	// (e.g. "otel-collector:4317"). Empty means "no export": Init
	// installs a no-op TracerProvider and Start calls become free.
	OTLPEndpoint string
}

// ConfigFromEnv reads Config from the standard OTEL_* environment
// variables. When OTEL_EXPORTER_OTLP_ENDPOINT is empty the returned
// Config disables tracing (no export) — callers do not need to branch,
// Init handles the no-op path.
func ConfigFromEnv() Config {
	return Config{
		ServiceName:  defaultString(envServiceName, defaultServiceNm),
		OTLPEndpoint: os.Getenv(envOTLPEndpoint),
	}
}

// Init wires the global TracerProvider per cfg and returns a shutdown
// closure the caller MUST defer from process startup (typically the host's
// Close). When cfg.OTLPEndpoint is empty, Init installs a no-op provider
// and the returned closure is a cheap no-op; every tracer.Start call site
// remains valid and records nothing.
//
// The shutdown closure flushes pending spans and releases the gRPC
// connection. It logs (does not return) flush errors: a flush failure
// means a few spans were not delivered, not that the process is broken.
// The closure is idempotent.
func Init(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	if cfg.OTLPEndpoint == "" {
		// Install the explicit noop provider so a previously-installed
		// real provider is replaced by a noop when tracing is turned
		// off between runs. tracer().Start is then a cheap no-op and
		// exports nothing.
		otel.SetTracerProvider(trace.NewNoopTracerProvider())
		slog.Info("tracing: disabled (no OTEL_EXPORTER_OTLP_ENDPOINT)")
		return func(context.Context) error { return nil }, nil
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint),
		// The endpoint is expected to be an in-cluster address
		// (e.g. "otel-collector:4317"); TLS is off by design. Cluster
		// traffic is already encrypted at the mesh layer, and enabling
		// TLS by default would block the common local-dev path.
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("tracing: create otlp exporter: %w", err)
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			attribute.String(string(semconv.ServiceNameKey), cfg.ServiceName),
		),
	)
	if err != nil {
		// A merge failure on the resource is non-fatal for tracing;
		// fall back to Default() so spans still flow, just without the
		// explicit service.name override.
		slog.Warn("tracing: resource merge failed; using default resource", "err", err)
		res = resource.Default()
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		// ParentBased(AlwaysSample) is the SDK default; state it
		// explicitly so a future SDK upgrade cannot silently change
		// sampling behavior.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)

	slog.Info("tracing: enabled", "endpoint", cfg.OTLPEndpoint, "service", cfg.ServiceName)

	var closed bool
	return func(ctx context.Context) error {
		if closed {
			return nil
		}
		closed = true
		if err := tp.Shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("tracing: shutdown flush failed", "err", err)
			return err
		}
		return nil
	}, nil
}

// defaultString returns the env var value or def when unset/blank. Centralizing
// the trim+fallback keeps ConfigFromEnv readable and the empty-string rule
// consistent ("unset" and "blank" both mean "use the default").
func defaultString(envVar, def string) string {
	if v := os.Getenv(envVar); v != "" {
		return v
	}
	return def
}
