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

import "github.com/bubustack/core/runtime/naming"

// ComposeName builds a DNS-1123 compliant resource name from the provided parts.
//
// Arguments:
//   - parts ...string: name segments to join with hyphens.
//
// Returns:
//   - string: DNS-1123 compliant name (max 63 characters).
//
// Behavior:
//   - Joins parts with hyphens.
//   - When the result exceeds 63 characters, truncates and appends a deterministic
//     FNV-1a hash suffix for uniqueness.
//   - Results are stable across reconciliations for the same inputs.
func ComposeName(parts ...string) string {
	return naming.ComposeDNS1123(parts...)
}
