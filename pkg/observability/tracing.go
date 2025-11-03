package observability

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tracingEnabled atomic.Bool

// EnableTracing toggles OpenTelemetry span emission for shared helpers.
func EnableTracing(enabled bool) {
	tracingEnabled.Store(enabled)
}

// TracingEnabled reports whether spans should be emitted.
func TracingEnabled() bool {
	return tracingEnabled.Load()
}

// StartSpan creates a tracer span when tracing is enabled and returns the context/span pair.
// When tracing is disabled, it returns the original context and a no-op span.
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if !TracingEnabled() {
		return ctx, trace.SpanFromContext(ctx)
	}
	tracer := otel.Tracer("github.com/bubustack/bobrapet")
	return tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}
