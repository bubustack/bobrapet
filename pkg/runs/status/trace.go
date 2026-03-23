package status

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

func ensureTraceInfo(ctx context.Context, target **runsv1alpha1.TraceInfo) {
	if target == nil {
		return
	}
	if *target != nil && (*target).TraceID != "" {
		return
	}
	sc := trace.SpanFromContext(ctx).SpanContext()
	if !sc.IsValid() {
		return
	}
	sampled := sc.TraceFlags().IsSampled()
	*target = &runsv1alpha1.TraceInfo{
		TraceID: sc.TraceID().String(),
		SpanID:  sc.SpanID().String(),
		Sampled: &sampled,
	}
}
