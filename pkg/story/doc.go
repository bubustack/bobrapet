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

// Package story provides utilities for building and validating Story resources.
//
// This package consolidates story-related functionality used by controllers,
// webhooks, and SDK helpers for consistent Story resource management.
//
// # Building Stories
//
// Use the fluent Builder API to construct Story resources programmatically:
//
//	story, err := story.New("my-story", "default").
//	    Pattern(enums.StreamingPattern).
//	    AddEngramStep("step-1", refs.EngramReference{Name: "my-engram"}, nil, nil).
//	    Build()
//
// The builder validates step names, dependencies, and loop configurations
// before returning the constructed Story.
//
// # Validating Loop Steps
//
// Loop steps require special validation for their template and policy fields:
//
//	errs := story.ValidateLoopStep(&step)
//	if len(errs) > 0 {
//	    // Handle validation errors
//	}
//
// Loop validation checks:
//   - Required 'with' block with template.ref and template.name
//   - Positive batchSize/concurrency values when set
//   - Valid execution.loop policy values
package story
