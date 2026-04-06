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

// Package logging provides structured logging utilities for BubuStack controllers.
//
// This package contains a [ControllerLogger] that provides consistent structured
// logging across all controllers, with context-aware logging for StoryRuns,
// StepRuns, and other BubuStack resources.
//
// # Controller Logger
//
// The [ControllerLogger] wraps logr.Logger with resource-aware methods:
//
//	log := logging.NewControllerLogger(ctx, "StoryRunController")
//	log = log.WithStoryRun(storyRun)
//	log.Info("Processing story run")
//	log.Error(err, "Failed to reconcile")
//
// # Resource Context
//
// Add resource context to loggers:
//
//	log.WithStoryRun(storyRun)    // Adds storyrun, namespace, story, phase
//	log.WithStepRun(stepRun)      // Adds steprun, namespace, storyrun
//	log.WithEngram(engram)        // Adds engram, namespace, template
//	log.WithImpulse(impulse)      // Adds impulse, namespace, story
//	log.WithTransport(transport)  // Adds transport, namespace
//
// # Logging Methods
//
// Standard logging methods:
//
//	log.Info("message", "key", "value")
//	log.Error(err, "message", "key", "value")
//	log.Debug("debug message")  // Only emitted at V(1) or higher
//	log.V(level).Info("verbose message")
//
// # Feature Flags
//
// Feature flags control logging behavior through [features.go]:
//
//	logging.SetFeatureEnabled("detailed-metrics", true)
//	if logging.IsFeatureEnabled("detailed-metrics") { ... }
//
// This package integrates with controller-runtime's logging infrastructure
// and the standard logr interface.
package logging
