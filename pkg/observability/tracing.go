/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package observability

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var (
	tracingEnabled          atomic.Bool
	tracePropagationEnabled atomic.Bool
)

// EnableTracing toggles OpenTelemetry span emission for shared helpers.
func EnableTracing(enabled bool) {
	tracingEnabled.Store(enabled)
	refreshTracerProvider()
}

// EnableTracePropagation toggles whether OTEL propagators should inject trace context.
func EnableTracePropagation(enabled bool) {
	tracePropagationEnabled.Store(enabled)
	if enabled {
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
	} else {
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	}
}

// TracePropagationEnabled reports whether OTEL trace propagation is enabled.
func TracePropagationEnabled() bool {
	return tracePropagationEnabled.Load()
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
