package reconcile

import (
	"context"
	"time"
)

// WithTimeout wraps ctx with a timeout when timeout > 0.
// Returns the original ctx with a no-op cancel when timeout <= 0.
//
// This is shared across controllers to consistently bound reconcile duration.
func WithTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}
