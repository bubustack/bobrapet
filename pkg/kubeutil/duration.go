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

import "time"

// PositiveDurationOrDefault returns candidate when it is greater than zero,
// otherwise it falls back to the provided default value.
//
// Arguments:
//   - candidate time.Duration: the preferred duration.
//   - fallback time.Duration: the default duration to use if candidate <= 0.
//
// Returns:
//   - time.Duration: candidate if positive, otherwise fallback.
func PositiveDurationOrDefault(candidate, fallback time.Duration) time.Duration {
	if candidate > 0 {
		return candidate
	}
	return fallback
}

// FirstPositiveDuration returns the first positive duration from the provided list.
//
// Arguments:
//   - values ...time.Duration: durations to check in order.
//
// Returns:
//   - time.Duration: the first positive value, or 0 if none are positive.
func FirstPositiveDuration(values ...time.Duration) time.Duration {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}
