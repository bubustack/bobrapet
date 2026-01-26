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

// Package resolver provides configuration resolution utilities.
//
// This package contains the chain subpackage which implements a stage-based
// pipeline executor for applying layered configuration resolution.
//
// # Configuration Resolution
//
// BubuStack uses a layered configuration model where settings are applied
// in order of precedence:
//
//  1. EngramTemplate defaults
//  2. Engram overrides
//  3. Story defaults
//  4. StepRun overrides
//
// The chain.Executor implements this pattern by running stages that each
// apply one layer of configuration.
//
// # Subpackages
//
// chain: Stage-based pipeline executor
//
//	executor := chain.NewExecutor(logger)
//	executor.Add(chain.Stage{Name: "template", Fn: applyTemplate})
//	executor.Run(ctx)
//
// See the chain package documentation for detailed usage.
package resolver
