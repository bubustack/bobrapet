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

package v1alpha1

import (
	"time"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
)

// DefaultRetryDelay is the default delay between retries when not specified.
const DefaultRetryDelay = time.Second

// ResolveRetryPolicy applies controller defaults to a retry policy when it is omitted.
//
// Behavior:
//   - If policy is nil, creates a new empty RetryPolicy.
//   - Sets MaxRetries to cfg.MaxRetries (or 3 if cfg is nil or MaxRetries < 0).
//   - Sets Delay to DefaultRetryDelay (1s) if nil or empty.
//   - Sets Backoff strategy to Exponential if nil.
//
// Arguments:
//   - cfg *config.ControllerConfig: controller configuration; may be nil.
//   - policy *bubuv1alpha1.RetryPolicy: policy to resolve; may be nil.
//
// Returns:
//   - Non-nil *bubuv1alpha1.RetryPolicy with all fields populated.
func ResolveRetryPolicy(cfg *config.ControllerConfig, policy *bubuv1alpha1.RetryPolicy) *bubuv1alpha1.RetryPolicy {
	if policy == nil {
		policy = &bubuv1alpha1.RetryPolicy{}
	}

	if policy.MaxRetries == nil {
		maxRetries := int32(3)
		if cfg != nil && cfg.MaxRetries >= 0 {
			maxRetries = int32(cfg.MaxRetries)
		}
		policy.MaxRetries = &maxRetries
	}

	if policy.Delay == nil || *policy.Delay == "" {
		delayStr := DefaultRetryDelay.String()
		policy.Delay = &delayStr
	}

	if policy.Backoff == nil {
		strategy := enums.BackoffStrategyExponential
		policy.Backoff = &strategy
	}

	return policy
}
