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

// Package metrics provides Prometheus metrics for BubuStack controllers.
//
// This package defines and registers all Prometheus metrics used by the operator,
// including counters, histograms, and gauges for tracking controller operations,
// resource lifecycles, and transport activities.
//
// # Controller Metrics
//
// StoryRun and StepRun lifecycle metrics:
//
//	metrics.RecordStoryRunMetrics("default", "my-story", "Succeeded", 300*time.Second)
//	metrics.RecordStepRunComplete("default", "my-story", "step-1", "Succeeded", 30*time.Second)
//
// Controller reconcile metrics:
//
//	metrics.RecordControllerReconcile("StoryRun", duration, err)
//
// # CEL Metrics
//
// CEL expression evaluation metrics:
//
//	metrics.RecordCELEvaluation("when", duration, nil)
//	metrics.RecordCELCacheHit("compilation")
//
// # Transport Metrics
//
// Transport and binding metrics:
//
//	metrics.RecordTransportBindingReady("default", "binding-1", true)
//	metrics.RecordTransportPacketSent("grpc", 1024)
//
// # Cleanup Metrics
//
// Resource cleanup tracking:
//
//	metrics.RecordStoryRunCleanup("default", WindowRetention, 5)
//	metrics.RecordStoryRunCleanedUpTotal("default")
//
// # Registration
//
// Metrics are automatically registered with the controller-runtime metrics registry.
// The [Enable] function must be called at startup to activate metric recording.
package metrics
