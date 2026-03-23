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

package transport

import (
	"github.com/bubustack/bobrapet/api/v1alpha1"
)

// StepNeedsHubRouting returns true if the step requires hub-based routing
// for per-packet template evaluation. Steps with runtime configuration must go
// through the hub; steps without runtime config can use direct P2P connections.
func StepNeedsHubRouting(step *v1alpha1.Step) bool {
	if step == nil {
		return false
	}
	// Steps with Runtime field need hub routing for per-packet template evaluation
	if step.Runtime != nil && len(step.Runtime.Raw) > 0 {
		return true
	}
	// Steps with If conditions need hub routing for conditional evaluation
	if step.If != nil && *step.If != "" {
		return true
	}
	return false
}

// CanUseDirectConnection returns true if the connection between two steps
// can bypass the hub and use a direct P2P gRPC connection.
func CanUseDirectConnection(upstream, downstream *v1alpha1.Step) bool {
	if upstream == nil || downstream == nil {
		return false
	}
	// Downstream step must not need hub routing
	return !StepNeedsHubRouting(downstream)
}
