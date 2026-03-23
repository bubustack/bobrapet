package reconcile

import (
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

// JitteredRequeueDelay returns a jittered delay bounded by maxDelay.
//
// The function treats non-positive values as "unset" and falls back to the
// provided fallback durations. When maxDelay is smaller than baseDelay, maxDelay
// is clamped up to baseDelay so callers never receive a delay larger than maxDelay.
func JitteredRequeueDelay(baseDelay, maxDelay, fallbackBase, fallbackMax time.Duration) time.Duration {
	if baseDelay <= 0 {
		baseDelay = fallbackBase
	}
	if maxDelay <= 0 {
		maxDelay = fallbackMax
	}
	if maxDelay < baseDelay {
		maxDelay = baseDelay
	}

	delay := min(wait.Jitter(baseDelay, 0.5), maxDelay)
	return delay
}
