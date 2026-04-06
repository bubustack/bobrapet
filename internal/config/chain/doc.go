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

// Package chain provides a pipeline executor for configuration resolution.
//
// This package implements a stage-based execution pipeline used by the
// configuration resolver to apply layered configuration from templates,
// engrams, stories, and step runs.
//
// # Executor
//
// The [Executor] runs a series of stages with configurable error handling:
//
//	executor := chain.NewExecutor(logger.WithName("resolver")).
//	    WithObserver(chain.NewMetricsObserver("engram"))
//
//	executor.
//	    Add(chain.Stage{Name: "template", Mode: chain.ModeFailFast, Fn: applyTemplate}).
//	    Add(chain.Stage{Name: "engram", Mode: chain.ModeLogAndSkip, Fn: applyEngram}).
//	    Add(chain.Stage{Name: "story", Mode: chain.ModeLogAndSkip, Fn: applyStory})
//
//	if err := executor.Run(ctx); err != nil {
//	    return err
//	}
//
// # Error Modes
//
// Two error handling modes are supported:
//
//   - ModeFailFast: Stops execution when a stage returns an error
//   - ModeLogAndSkip: Logs the error and continues to the next stage
//
// # Observers
//
// Observers are notified of stage completion for metrics and logging:
//
//	observer := chain.NewMetricsObserver("layer_name")
//	executor.WithObserver(observer)
//
// The [MetricsObserver] records Prometheus metrics for stage durations
// and outcomes.
//
// # Stage Outcomes
//
// Stages produce one of three outcomes:
//
//   - StageSuccess: Stage completed without error
//   - StageError: Stage failed with ModeFailFast
//   - StageSkipped: Stage failed with ModeLogAndSkip (execution continued)
package chain
