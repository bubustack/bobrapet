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

// Package runs provides utilities for StoryRun and StepRun management.
//
// This package contains subpackages that provide specialized functionality
// for run lifecycle management, including identity, inputs, status patching,
// and validation.
//
// # Subpackages
//
// childrun: Child StepRun spec generation and management
//
//	spec := childrun.BuildSpec(storyRun, step, index)
//
// identity: ServiceAccount identity management for engram runners
//
//	runner := identity.NewEngramRunner(storyRunName)
//	saName := runner.ServiceAccountName()
//	subject := runner.Subject(namespace)
//
// inputs: Input resolution and parsing for StoryRuns
//
//	inputs, err := inputs.Parse(storyRun.Spec.Inputs)
//
// list: StepRun listing and filtering utilities
//
//	stepRuns, err := list.StepRunsForStoryRun(ctx, client, storyRun)
//
// loop: Loop step limit calculation and enforcement
//
//	limits := loop.ComputeLimits(step, config)
//
// status: Status patching for StoryRun and StepRun
//
//	status.PatchStoryRunPhase(ctx, client, storyRun, enums.PhaseSucceeded)
//	status.PatchStepRunPhase(ctx, client, stepRun, enums.PhaseRunning)
//
// validation: Downstream target validation for streaming
//
//	err := validation.ValidateDownstreamTargets(targets)
//	err := validation.ValidateGRPCEndpoint(endpoint)
package runs
