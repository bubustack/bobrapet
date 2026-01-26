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

// Package annotations provides utilities for managing Kubernetes object annotations.
//
// This package consolidates annotation parsing, modification, and patching logic
// used across BubuStack controllers. It provides a consistent approach to:
//
//   - Trigger token management for StepRuns and StoryRuns
//   - Annotation set operations (parse, merge, deduplicate)
//   - Conflict-safe annotation patching
//
// # Trigger Tokens
//
// The [EnsureStepRunTriggerTokens] function adds trigger tokens to a StepRun's
// annotations, handling conflict retries automatically:
//
//	added, err := annotations.EnsureStepRunTriggerTokens(ctx, client, key, "token1", "token2")
//	if err != nil {
//	    return err
//	}
//	for _, token := range added {
//	    // Process newly added token
//	}
//
// # Token Parsing
//
// The [ParseTriggerTokens] function parses comma-separated trigger token strings
// into deduplicated sets:
//
//	tokens := annotations.ParseTriggerTokens("token1,token2,token1")
//	// tokens = {"token1", "token2"}
//
// This package follows Kubernetes controller-runtime conventions and integrates
// with the standard client-go retry mechanisms.
package annotations
