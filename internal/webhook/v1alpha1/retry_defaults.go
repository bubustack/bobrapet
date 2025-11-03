package v1alpha1

import (
	"time"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
)

// ResolveRetryPolicy applies controller defaults to a retry policy when it is omitted.
func ResolveRetryPolicy(cfg *config.ControllerConfig, policy *bubuv1alpha1.RetryPolicy) *bubuv1alpha1.RetryPolicy {
	if policy == nil {
		policy = &bubuv1alpha1.RetryPolicy{}
	}

	if policy.MaxRetries == nil {
		maxRetries := int32(3)
		if cfg != nil && cfg.MaxRetries > 0 {
			maxRetries = int32(cfg.MaxRetries)
		}
		policy.MaxRetries = &maxRetries
	}

	if policy.Delay == nil || *policy.Delay == "" {
		delay := time.Second
		if cfg != nil && cfg.ExponentialBackoffBase > 0 {
			delay = cfg.ExponentialBackoffBase
		}
		delayStr := delay.String()
		policy.Delay = &delayStr
	}

	if policy.Backoff == nil {
		strategy := enums.BackoffStrategyExponential
		policy.Backoff = &strategy
	}

	return policy
}
