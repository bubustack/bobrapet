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

// Package observability provides interfaces and utilities for logging and tracing.
//
// This package defines standard interfaces for structured logging and distributed
// tracing that can be implemented by different backends. It allows shared packages
// to emit observability data without depending on specific implementations.
//
// # Logger Interface
//
// The [Logger] interface defines methods for template evaluation observability:
//
//	type Logger interface {
//	    CacheHit(expression, expressionType string)
//	    EvaluationStart(expression, expressionType string)
//	    EvaluationError(err error, expression, expressionType string, duration time.Duration)
//	    EvaluationSuccess(expression, expressionType string, duration time.Duration, result any)
//	}
//
// # Tracing
//
// The [StartSpan] function creates OpenTelemetry spans for distributed tracing:
//
//	ctx, span := observability.StartSpan(ctx, "reconcile", attribute.String("step", stepName))
//	defer span.End()
//
// # Integration
//
// This package integrates with:
//   - OpenTelemetry for distributed tracing
//   - Prometheus for metrics (via pkg/metrics)
//   - Standard logr for structured logging
//
// Implementations can be swapped at runtime for testing or different deployment
// environments.
package observability
