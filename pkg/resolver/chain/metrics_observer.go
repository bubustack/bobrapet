package chain

import (
	"context"
	"time"

	"github.com/bubustack/bobrapet/pkg/metrics"
)

// NewMetricsObserver wires resolver stage events into controller metrics.
func NewMetricsObserver(layer string) Observer {
	if layer == "" {
		layer = unknownValue
	}
	return &metricsObserver{layer: layer}
}

type metricsObserver struct {
	layer string
}

func (m *metricsObserver) StageStarted(context.Context, Stage) {}

func (m *metricsObserver) StageCompleted(
	_ context.Context,
	stage Stage,
	outcome StageOutcome,
	_ error,
	duration time.Duration,
) {
	metrics.RecordResolverStage(m.layer, stage.Name, stage.Mode.String(), string(outcome), duration)
}
