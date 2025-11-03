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
	"fmt"
	"hash/fnv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

const maxDNS1123Length = validation.DNS1123LabelMaxLength

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
	base := strings.Join(parts, "-")
	if len(base) <= maxDNS1123Length {
		return base
	}

	hash := fnv.New32a()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	suffix := fmt.Sprintf("%08x", hash.Sum32())

	prefixLen := maxDNS1123Length - len(suffix) - 1
	if prefixLen < 1 {
		prefixLen = maxDNS1123Length - len(suffix)
	}
	if prefixLen < 1 {
		if len(suffix) > maxDNS1123Length {
			return suffix[:maxDNS1123Length]
		}
		return suffix
	}

	prefix := base[:prefixLen]
	prefix = strings.TrimSuffix(prefix, "-")
	if len(prefix) == 0 {
		prefix = base[:prefixLen]
		prefix = strings.Trim(prefix, "-")
		if len(prefix) == 0 {
			prefix = "resource"
		}
	}

	result := fmt.Sprintf("%s-%s", prefix, suffix)
	if len(result) > maxDNS1123Length {
		result = result[:maxDNS1123Length]
		result = strings.TrimSuffix(result, "-")
		if len(result) == 0 {
			if len(suffix) > maxDNS1123Length {
				return suffix[:maxDNS1123Length]
			}
			return suffix
		}
	}
	return result
}
