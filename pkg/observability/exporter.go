package observability

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

var (
	tracerMu              sync.Mutex
	tracerShutdown        func(context.Context) error
	tracerStarted         bool
	tracerServiceName     string
	tracerInitTimeout     = 10 * time.Second
	tracerShutdownTimeout = 5 * time.Second
)

// ConfigureTracing sets the default tracing service name and init/shutdown timeouts.
//
// Notes:
//   - serviceName is used when InitTracerProvider is called without a name.
//   - initTimeout bounds exporter initialization; shutdownTimeout bounds flush/close.
//   - Zero or negative timeout values leave the current defaults unchanged.
func ConfigureTracing(serviceName string, initTimeout, shutdownTimeout time.Duration) {
	tracerMu.Lock()
	defer tracerMu.Unlock()
	if serviceName != "" {
		tracerServiceName = serviceName
	}
	if initTimeout > 0 {
		tracerInitTimeout = initTimeout
	}
	if shutdownTimeout > 0 {
		tracerShutdownTimeout = shutdownTimeout
	}
}

// InitTracerProvider configures an OTLP trace exporter and tracer provider.
// It is safe to call multiple times; initialization happens once per process
// lifetime unless the provider is explicitly shut down.
func InitTracerProvider(ctx context.Context, serviceName string) (func(context.Context) error, error) {
	tracerMu.Lock()
	defer tracerMu.Unlock()

	if tracerStarted {
		return tracerShutdown, nil
	}
	if !TracingEnabled() {
		return nil, nil
	}
	if shouldSkipTracingExport() {
		return nil, nil
	}
	if serviceName == "" {
		serviceName = tracerServiceName
	}

	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}

	res, err := buildResource(ctx, serviceName)
	if err != nil {
		return nil, err
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(provider)
	tracerShutdown = provider.Shutdown
	tracerStarted = true
	return tracerShutdown, nil
}

// ShutdownTracerProvider flushes and closes the configured tracer provider.
// It is safe to call multiple times.
func ShutdownTracerProvider(ctx context.Context) error {
	tracerMu.Lock()
	shutdown := tracerShutdown
	started := tracerStarted
	tracerMu.Unlock()

	if !started || shutdown == nil {
		return nil
	}

	if err := shutdown(ctx); err != nil {
		return err
	}

	tracerMu.Lock()
	tracerShutdown = nil
	tracerStarted = false
	tracerMu.Unlock()
	return nil
}

func refreshTracerProvider() {
	if !TracingEnabled() {
		shutdownWithTimeout()
		return
	}

	serviceName, timeout := tracerInitSettings()
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if _, err := InitTracerProvider(ctx, serviceName); err != nil {
		otel.Handle(err)
	}
}

func shutdownWithTimeout() {
	timeout := tracerShutdownSettings()
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if err := ShutdownTracerProvider(ctx); err != nil {
		otel.Handle(err)
	}
}

func tracerInitSettings() (string, time.Duration) {
	tracerMu.Lock()
	defer tracerMu.Unlock()
	return tracerServiceName, tracerInitTimeout
}

func tracerShutdownSettings() time.Duration {
	tracerMu.Lock()
	defer tracerMu.Unlock()
	return tracerShutdownTimeout
}

func shouldSkipTracingExport() bool {
	val := strings.TrimSpace(strings.ToLower(os.Getenv("OTEL_TRACES_EXPORTER")))
	return val == "none"
}

func buildResource(ctx context.Context, serviceName string) (*resource.Resource, error) {
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithHost(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
	)
	if err != nil && !errors.Is(err, resource.ErrPartialResource) {
		return res, err
	}

	if serviceName == "" || hasServiceName(res) {
		return res, err
	}
	fallback := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceNameKey.String(serviceName))
	merged, mergeErr := resource.Merge(res, fallback)
	if mergeErr != nil {
		return res, mergeErr
	}
	return merged, err
}

func hasServiceName(res *resource.Resource) bool {
	if res == nil {
		return false
	}
	iter := res.Iter()
	for iter.Next() {
		kv := iter.Attribute()
		if kv.Key == semconv.ServiceNameKey {
			return kv.Value.AsString() != ""
		}
	}
	return false
}
