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

package logging

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
)

// LoggerKey is used for storing logger in context
type LoggerKey struct{}

// ControllerLogger provides structured logging for controllers
type ControllerLogger struct {
	logger logr.Logger
}

// NewControllerLogger creates a new structured logger
func NewControllerLogger(ctx context.Context, controller string) *ControllerLogger {
	logger := log.FromContext(ctx).WithName(controller)
	return &ControllerLogger{logger: logger}
}

// WithStoryRun returns a logger with StoryRun context
func (cl *ControllerLogger) WithStoryRun(srun *runsv1alpha1.StoryRun) *ControllerLogger {
	logger := cl.logger.WithValues(
		"storyrun", srun.Name,
		"namespace", srun.Namespace,
		"story", srun.Spec.StoryRef,
		"phase", srun.Status.Phase,
	)
	return &ControllerLogger{logger: logger}
}

// WithStepRun returns a logger with StepRun context
func (cl *ControllerLogger) WithStepRun(steprun *runsv1alpha1.StepRun) *ControllerLogger {
	logger := cl.logger.WithValues(
		"steprun", steprun.Name,
		"namespace", steprun.Namespace,
		"storyrun", steprun.Spec.StoryRunRef,
		"step", steprun.Spec.StepID,
		"engram", steprun.Spec.EngramRef,
		"phase", steprun.Status.Phase,
	)
	return &ControllerLogger{logger: logger}
}

// WithStory returns a logger with Story context
func (cl *ControllerLogger) WithStory(story *bubushv1alpha1.Story) *ControllerLogger {
	logger := cl.logger.WithValues(
		"story", story.Name,
		"namespace", story.Namespace,
		"steps", len(story.Spec.Steps),
	)
	return &ControllerLogger{logger: logger}
}

// WithEngram returns a logger with Engram context
func (cl *ControllerLogger) WithEngram(engram *bubushv1alpha1.Engram) *ControllerLogger {
	logger := cl.logger.WithValues(
		"engram", engram.Name,
		"namespace", engram.Namespace,
	)
	return &ControllerLogger{logger: logger}
}

// WithImpulse returns a logger with Impulse context
func (cl *ControllerLogger) WithImpulse(impulse *bubushv1alpha1.Impulse) *ControllerLogger {
	logger := cl.logger.WithValues(
		"impulse", impulse.Name,
		"namespace", impulse.Namespace,
		"story", impulse.Spec.StoryRef,
	)
	return &ControllerLogger{logger: logger}
}

// WithDuration adds duration to the logger context
func (cl *ControllerLogger) WithDuration(duration time.Duration) *ControllerLogger {
	logger := cl.logger.WithValues("duration", duration.String())
	return &ControllerLogger{logger: logger}
}

// WithError adds error to the logger context
func (cl *ControllerLogger) WithError(err error) *ControllerLogger {
	if err == nil {
		return cl
	}
	logger := cl.logger.WithValues("error", err.Error())
	return &ControllerLogger{logger: logger}
}

// WithValues adds custom key-value pairs to the logger
func (cl *ControllerLogger) WithValues(keysAndValues ...any) *ControllerLogger {
	logger := cl.logger.WithValues(keysAndValues...)
	return &ControllerLogger{logger: logger}
}

// Info logs an info message
func (cl *ControllerLogger) Info(msg string, keysAndValues ...any) {
	cl.logger.Info(msg, keysAndValues...)
}

// Error logs an error message
func (cl *ControllerLogger) Error(err error, msg string, keysAndValues ...any) {
	cl.logger.Error(err, msg, keysAndValues...)
}

// V returns a logger for a specific verbosity level
func (cl *ControllerLogger) V(level int) logr.Logger {
	return cl.logger.V(level)
}

// ReconcileLogger provides specialized logging for reconcile operations
type ReconcileLogger struct {
	*ControllerLogger
	startTime time.Time
}

// NewReconcileLogger creates a logger for reconcile operations
func NewReconcileLogger(ctx context.Context, controller string) *ReconcileLogger {
	return &ReconcileLogger{
		ControllerLogger: NewControllerLogger(ctx, controller),
		startTime:        time.Now(),
	}
}

// ReconcileStart logs the start of reconciliation
func (rl *ReconcileLogger) ReconcileStart(msg string, keysAndValues ...any) {
	rl.Info("Reconcile started: "+msg, keysAndValues...)
}

// ReconcileSuccess logs successful reconciliation
func (rl *ReconcileLogger) ReconcileSuccess(msg string, keysAndValues ...any) {
	duration := time.Since(rl.startTime)
	allValues := append(keysAndValues, "duration", duration.String())
	rl.Info("Reconcile succeeded: "+msg, allValues...)
}

// ReconcileError logs reconciliation error
func (rl *ReconcileLogger) ReconcileError(err error, msg string, keysAndValues ...any) {
	duration := time.Since(rl.startTime)
	allValues := append(keysAndValues, "duration", duration.String())
	rl.Error(err, "Reconcile failed: "+msg, allValues...)
}

// ReconcileRequeue logs reconciliation requeue
func (rl *ReconcileLogger) ReconcileRequeue(msg string, after time.Duration, keysAndValues ...any) {
	duration := time.Since(rl.startTime)
	allValues := append(keysAndValues, "duration", duration.String(), "requeue_after", after.String())
	rl.Info("Reconcile requeue: "+msg, allValues...)
}

// StepLogger provides specialized logging for step operations
type StepLogger struct {
	*ControllerLogger
}

// NewStepLogger creates a logger for step operations
func NewStepLogger(ctx context.Context, steprun *runsv1alpha1.StepRun) *StepLogger {
	cl := NewControllerLogger(ctx, "steprun").WithStepRun(steprun)
	return &StepLogger{ControllerLogger: cl}
}

// StepStart logs step execution start
func (sl *StepLogger) StepStart(msg string, keysAndValues ...any) {
	sl.Info("Step started: "+msg, keysAndValues...)
}

// StepProgress logs step execution progress
func (sl *StepLogger) StepProgress(msg string, keysAndValues ...any) {
	sl.V(1).Info("Step progress: "+msg, keysAndValues...)
}

// StepSuccess logs step execution success
func (sl *StepLogger) StepSuccess(msg string, duration time.Duration, keysAndValues ...any) {
	allValues := append(keysAndValues, "duration", duration.String())
	sl.Info("Step succeeded: "+msg, allValues...)
}

// StepError logs step execution error
func (sl *StepLogger) StepError(err error, msg string, keysAndValues ...any) {
	sl.Error(err, "Step failed: "+msg, keysAndValues...)
}

// StepRetry logs step retry
func (sl *StepLogger) StepRetry(attempt int, reason string, keysAndValues ...any) {
	allValues := append(keysAndValues, "attempt", attempt, "reason", reason)
	sl.Info("Step retry", allValues...)
}

// CELLogger implements the observability.Logger interface for CEL evaluation logging.
type CELLogger struct {
	log logr.Logger
}

// NewCELLogger creates a new CELLogger from a base logger.
func NewCELLogger(baseLogger logr.Logger) *CELLogger {
	return &CELLogger{
		log: baseLogger.WithName("cel"),
	}
}

// CacheHit logs a cache hit event.
func (l *CELLogger) CacheHit(expression, expressionType string) {
	l.log.V(1).Info("CEL cache hit", "expression", expression, "type", expressionType)
}

// EvaluationStart logs the start of a CEL evaluation
func (l *CELLogger) EvaluationStart(expression, expressionType string) {
	l.log.V(1).Info("CEL evaluation started", "expression", expression, "type", expressionType)
}

// EvaluationError logs a CEL evaluation error
func (l *CELLogger) EvaluationError(err error, expression, expressionType string, duration time.Duration) {
	l.log.Error(
		err,
		"CEL evaluation failed",
		"expression",
		expression,
		"type",
		expressionType,
		"duration",
		duration.String(),
	)
}

// EvaluationSuccess logs a successful CEL evaluation
func (l *CELLogger) EvaluationSuccess(expression, expressionType string, duration time.Duration, result any) {
	l.log.V(1).Info(
		"CEL evaluation succeeded",
		"expression", expression,
		"type", expressionType,
		"duration", duration.String(),
		"result", result,
	)
}

// CleanupLogger provides specialized logging for cleanup operations
type CleanupLogger struct {
	*ControllerLogger
}

// NewCleanupLogger creates a logger for cleanup operations
func NewCleanupLogger(ctx context.Context, resourceType string) *CleanupLogger {
	cl := NewControllerLogger(ctx, "cleanup").WithValues("resource_type", resourceType)
	return &CleanupLogger{ControllerLogger: cl}
}

// CleanupStart logs cleanup start
func (cl *CleanupLogger) CleanupStart(resourceName string, keysAndValues ...any) {
	allValues := append(keysAndValues, "resource", resourceName)
	cl.Info("Cleanup started", allValues...)
}

// CleanupSuccess logs cleanup success
func (cl *CleanupLogger) CleanupSuccess(resourceName string, duration time.Duration, keysAndValues ...any) {
	allValues := append(keysAndValues, "resource", resourceName, "duration", duration.String())
	cl.Info("Cleanup succeeded", allValues...)
}

// CleanupError logs cleanup error
func (cl *CleanupLogger) CleanupError(err error, resourceName string, keysAndValues ...any) {
	allValues := append(keysAndValues, "resource", resourceName)
	cl.Error(err, "Cleanup failed", allValues...)
}
