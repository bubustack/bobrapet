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

package kubeutil

import (
	"context"
	"fmt"
	"time"
)

// RetryConfig specifies how Retry executes the provided function.
type RetryConfig struct {
	// MaxAttempts controls how many times fn is invoked before giving up.
	// Values <= 0 default to a single attempt.
	MaxAttempts int
	// InitialDelay determines how long Retry waits before the second attempt.
	// Values <= 0 default to 250 milliseconds.
	InitialDelay time.Duration
	// Multiplier controls the exponential backoff multiplier applied after each wait.
	// Values <= 0 default to 1 (no backoff).
	Multiplier float64
}

// Retry invokes fn until it succeeds, the context is canceled, or MaxAttempts
// is reached. The retryable callback decides whether a returned error should
// trigger another attempt.
//
// Arguments:
//   - ctx context.Context: context for cancellation.
//   - cfg RetryConfig: retry configuration.
//   - fn func(context.Context) error: the function to retry.
//   - retryable func(error) bool: determines if an error is retryable.
//
// Returns:
//   - error: nil on success, or the last error after exhausting retries.
func Retry(
	ctx context.Context,
	cfg RetryConfig,
	fn func(context.Context) error,
	retryable func(error) bool,
) error {
	attempts := cfg.MaxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	delay := cfg.InitialDelay
	if delay <= 0 {
		delay = 250 * time.Millisecond
	}
	multiplier := cfg.Multiplier
	if multiplier <= 0 {
		multiplier = 1
	}

	for attempt := 1; attempt <= attempts; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if retryable == nil || !retryable(err) || attempt == attempts {
			return fmt.Errorf("retry attempt %d failed: %w", attempt, err)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("retry canceled: %w", ctx.Err())
		case <-time.After(delay):
		}
		delay = time.Duration(float64(delay) * multiplier)
	}
	return fmt.Errorf("retry aborted after %d attempts", attempts)
}

// Periodic executes fn every interval until the context is canceled. Returning
// an error from fn stops the loop early and surfaces the error to the caller.
//
// Arguments:
//   - ctx context.Context: context for cancellation.
//   - interval time.Duration: how often to execute fn.
//   - fn func(context.Context) error: the function to execute periodically.
//
// Returns:
//   - error: nil when context is canceled, or the error from fn.
func Periodic(
	ctx context.Context,
	interval time.Duration,
	fn func(context.Context) error,
) error {
	if interval <= 0 {
		return nil
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := fn(ctx); err != nil {
				return err
			}
		}
	}
}
