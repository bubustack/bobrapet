package reconcile

import (
	"context"
	"time"

	"github.com/bubustack/bobrapet/pkg/metrics"
)

// StartControllerReconcile applies an optional timeout to ctx and returns a
// closure that records controller reconcile metrics and cancels the context.
// Callers should `defer finish(err)` so metrics see the final error value.
func StartControllerReconcile(
	ctx context.Context,
	controller string,
	timeout time.Duration,
) (context.Context, func(error)) {
	ctx, cancel := WithTimeout(ctx, timeout)
	start := time.Now()
	return ctx, func(err error) {
		cancel()
		metrics.RecordControllerReconcile(controller, time.Since(start), err)
	}
}
