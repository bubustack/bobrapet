package storage

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

var (
	meterOnce sync.Once
	meter     metric.Meter = metricnoop.NewMeterProvider().Meter("github.com/bubustack/bobrapet/pkg/storage")

	hydrationSizeHist   metric.Int64Histogram
	dehydrationSizeHist metric.Int64Histogram
)

func initMeter() {
	meterOnce.Do(func() {
		meter = otel.Meter("github.com/bubustack/bobrapet/pkg/storage")
		var err error
		hydrationSizeHist, err = meter.Int64Histogram(
			"bobrapet.storage.hydration.size_bytes",
			metric.WithUnit("By"),
			metric.WithDescription("Size in bytes of hydrated payloads"),
		)
		_ = err

		dehydrationSizeHist, err = meter.Int64Histogram(
			"bobrapet.storage.dehydration.size_bytes",
			metric.WithUnit("By"),
			metric.WithDescription("Size in bytes of dehydrated payloads"),
		)
		_ = err
	})
}

func recordHydrationSize(ctx context.Context, sizeBytes int64, stepRunID string) {
	initMeter()
	if hydrationSizeHist == nil {
		return
	}
	attrs := []attribute.KeyValue{}
	if stepRunID != "" {
		attrs = append(attrs, attribute.String("steprun_id", stepRunID))
	}
	hydrationSizeHist.Record(ctx, sizeBytes, metric.WithAttributes(attrs...))
}

func recordDehydrationSize(ctx context.Context, sizeBytes int64, stepRunID string) {
	initMeter()
	if dehydrationSizeHist == nil {
		return
	}
	attrs := []attribute.KeyValue{}
	if stepRunID != "" {
		attrs = append(attrs, attribute.String("steprun_id", stepRunID))
	}
	dehydrationSizeHist.Record(ctx, sizeBytes, metric.WithAttributes(attrs...))
}
