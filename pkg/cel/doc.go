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

// Package cel provides CEL (Common Expression Language) evaluation for BubuStack.
//
// This package implements CEL expression evaluation for step conditions, with
// block resolution, and data transformations. It includes caching for compiled
// expressions to improve performance.
//
// # Evaluator
//
// The [Evaluator] is the main entry point for CEL operations:
//
//	evaluator, err := cel.New(logger, cel.Config{
//	    EvaluationTimeout:   5 * time.Second,
//	    MaxExpressionLength: 10000,
//	})
//	defer evaluator.Close()
//
// # When Conditions
//
// Evaluate step conditions:
//
//	result, err := evaluator.EvaluateWhenCondition(ctx, "steps.process.output.count > 10", vars)
//	if result {
//	    // Step should execute
//	}
//
// # With Block Resolution
//
// Resolve with block expressions:
//
//	resolved, err := evaluator.ResolveWithInputs(ctx, withBlock, vars)
//
// Supports template interpolation:
//
//	"{{ steps.process.output.file }}-suffix"  // GitHub Actions style
//	"{{ inputs.bucket }}/{{ inputs.key }}"    // Multiple interpolations
//
// # Compilation Cache
//
// The [CompilationCache] caches compiled CEL programs:
//
//	cache := cel.NewCompilationCache(config, env, logger)
//	cached, err := cache.CompileAndCache(ctx, expression, "when")
//	result, err := cache.Evaluate(ctx, expression, "filter", vars)
//
// Cache features:
//   - LRU eviction when max size is reached
//   - TTL-based expiration
//   - Cache hit/miss metrics
//
// # Custom Functions
//
// Custom CEL functions are available:
//   - len(): Enhanced length function for various types
//   - range(): Builds integer sequences (range(n) or range(start, end))
//   - sort_by(), dedupe(), group_by(): List operations
//   - sum_by(), count_by(): Aggregation operations
//   - rename_keys(): Map transformations
//   - now(), format_time(): Date/time operations
//
// # Variables
//
// Standard variables available in expressions:
//   - inputs: Story inputs from trigger or API
//   - steps: Outputs from other steps
//   - packet: Current packet metadata (streaming only)
package cel
