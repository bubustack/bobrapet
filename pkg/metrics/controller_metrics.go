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

package metrics

import (
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// StoryRun metrics
	StoryRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_storyruns_total",
			Help: "Total number of StoryRuns processed",
		},
		[]string{"namespace", "story", "phase"},
	)

	StoryRunDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobrapet_storyrun_duration_seconds",
			Help:    "Duration of StoryRun execution",
			Buckets: []float64{1, 5, 15, 30, 60, 300, 600, 1800, 3600},
		},
		[]string{"namespace", "story", "phase"},
	)

	StoryRunStepsActive = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobrapet_storyrun_steps_active",
			Help: "Number of currently active steps in StoryRuns",
		},
		[]string{"namespace", "story"},
	)

	StoryRunStepsCompleted = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobrapet_storyrun_steps_completed",
			Help: "Number of completed steps in StoryRuns",
		},
		[]string{"namespace", "story"},
	)

	// StepRun metrics
	StepRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_stepruns_total",
			Help: "Total number of StepRuns processed",
		},
		[]string{"namespace", "engram", "phase"},
	)

	StepRunDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobrapet_steprun_duration_seconds",
			Help:    "Duration of StepRun execution",
			Buckets: []float64{0.1, 0.5, 1, 5, 15, 30, 60, 300},
		},
		[]string{"namespace", "engram", "phase"},
	)

	StepRunRetries = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_steprun_retries_total",
			Help: "Total number of StepRun retries",
		},
		[]string{"namespace", "engram", "reason"},
	)

	// Controller metrics
	ControllerReconcileTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_controller_reconcile_total",
			Help: "Total number of reconciles by controller",
		},
		[]string{"controller", "result"},
	)

	ControllerReconcileDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobrapet_controller_reconcile_duration_seconds",
			Help:    "Duration of controller reconcile operations",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"controller"},
	)

	ControllerReconcileErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_controller_reconcile_errors_total",
			Help: "Total number of reconcile errors",
		},
		[]string{"controller", "error_type"},
	)

	// CEL evaluation metrics
	CELEvaluationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobrapet_cel_evaluation_duration_seconds",
			Help:    "Duration of CEL expression evaluation",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1},
		},
		[]string{"expression_type"},
	)

	CELEvaluationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_cel_evaluation_total",
			Help: "Total number of CEL expression evaluations",
		},
		[]string{"expression_type", "result"},
	)

	CELCacheHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_cel_cache_hits_total",
			Help: "Total number of CEL compilation cache hits",
		},
		[]string{"cache_type"},
	)

	// Resource cleanup metrics
	ResourceCleanupTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_resource_cleanup_total",
			Help: "Total number of resource cleanup operations",
		},
		[]string{"resource_type", "result"},
	)

	ResourceCleanupDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobrapet_resource_cleanup_duration_seconds",
			Help:    "Duration of resource cleanup operations",
			Buckets: []float64{0.1, 0.5, 1, 5, 10, 30},
		},
		[]string{"resource_type"},
	)

	// Job execution metrics
	JobExecutionTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_job_execution_total",
			Help: "Total number of Job executions",
		},
		[]string{"namespace", "image", "result"},
	)

	JobExecutionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobrapet_job_execution_duration_seconds",
			Help:    "Duration of Job execution",
			Buckets: []float64{1, 5, 15, 30, 60, 300, 600, 1800},
		},
		[]string{"namespace", "image"},
	)

	// Quota violation metrics
	QuotaViolationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_quota_violation_total",
			Help: "Total number of quota violations",
		},
		[]string{"namespace", "resource_type", "violation_type"},
	)

	// Resource quota metrics
	ResourceQuotaUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobrapet_resource_quota_usage",
			Help: "Current resource quota usage",
		},
		[]string{"namespace", "resource_type"},
	)

	ResourceQuotaLimit = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobrapet_resource_quota_limit",
			Help: "Resource quota limits",
		},
		[]string{"namespace", "resource_type"},
	)

	// gRPC Transport (bobravoz-grpc) metrics
	GRPCStreamRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_grpc_stream_requests_total",
			Help: "Total number of streaming RPC requests by status code",
		},
		[]string{"method", "code"},
	)

	GRPCStreamDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobravoz_grpc_stream_duration_seconds",
			Help:    "Duration of streaming RPCs in seconds",
			Buckets: []float64{.1, .5, 1, 5, 10, 30, 60, 300},
		},
		[]string{"method"},
	)

	GRPCMessagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_grpc_messages_received_total",
			Help: "Total messages received on streaming RPCs",
		},
		[]string{"storyrun", "step"},
	)

	GRPCMessagesSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_grpc_messages_sent_total",
			Help: "Total messages sent on streaming RPCs",
		},
		[]string{"storyrun", "step"},
	)

	GRPCMessagesDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobravoz_grpc_messages_dropped_total",
			Help: "Total messages dropped due to buffer overflow or errors",
		},
		[]string{"storyrun", "step", "reason"},
	)
)

func init() {
	metrics.Registry.MustRegister(
		StoryRunsTotal,
		StoryRunDuration,
		StoryRunStepsActive,
		StoryRunStepsCompleted,
		StepRunsTotal,
		StepRunDuration,
		StepRunRetries,
		ControllerReconcileTotal,
		ControllerReconcileDuration,
		ControllerReconcileErrors,
		CELEvaluationDuration,
		CELEvaluationTotal,
		CELCacheHits,
		ResourceCleanupTotal,
		ResourceCleanupDuration,
		JobExecutionTotal,
		JobExecutionDuration,
		QuotaViolationTotal,
		ResourceQuotaUsage,
		ResourceQuotaLimit,
		// gRPC Transport metrics
		GRPCStreamRequests,
		GRPCStreamDuration,
		GRPCMessagesReceived,
		GRPCMessagesSent,
		GRPCMessagesDropped,
	)
}

// Helper functions to record metrics with consistent labels

// RecordStoryRunMetrics records metrics for StoryRun operations
func RecordStoryRunMetrics(namespace, story, phase string, duration time.Duration) {
	StoryRunsTotal.WithLabelValues(namespace, story, phase).Inc()
	if duration > 0 {
		StoryRunDuration.WithLabelValues(namespace, story, phase).Observe(duration.Seconds())
	}
}

// RecordStepRunMetrics records metrics for StepRun operations
func RecordStepRunMetrics(namespace, engram, phase string, duration time.Duration) {
	StepRunsTotal.WithLabelValues(namespace, engram, phase).Inc()
	if duration > 0 {
		StepRunDuration.WithLabelValues(namespace, engram, phase).Observe(duration.Seconds())
	}
}

// RecordStepRunDuration records duration metrics for StepRun operations (alias for consistency)
func RecordStepRunDuration(namespace, storyRunRef, stepName, phase string, duration time.Duration) {
	// Use engram field as step identifier for metrics consistency
	RecordStepRunMetrics(namespace, stepName, phase, duration)
}

// RecordControllerReconcile records controller reconcile metrics
func RecordControllerReconcile(controller string, duration time.Duration, err error) {
	result := "success"
	if err != nil {
		result = "error"
		ControllerReconcileErrors.WithLabelValues(controller, classifyError(err)).Inc()
	}

	ControllerReconcileTotal.WithLabelValues(controller, result).Inc()
	ControllerReconcileDuration.WithLabelValues(controller).Observe(duration.Seconds())
}

// RecordCELEvaluation records CEL evaluation metrics
func RecordCELEvaluation(expressionType string, duration time.Duration, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}

	CELEvaluationTotal.WithLabelValues(expressionType, result).Inc()
	CELEvaluationDuration.WithLabelValues(expressionType).Observe(duration.Seconds())
}

// RecordCELCacheHit records CEL cache hit
func RecordCELCacheHit(cacheType string) {
	CELCacheHits.WithLabelValues(cacheType).Inc()
}

// RecordResourceCleanup records resource cleanup metrics
func RecordResourceCleanup(resourceType string, duration time.Duration, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}

	ResourceCleanupTotal.WithLabelValues(resourceType, result).Inc()
	ResourceCleanupDuration.WithLabelValues(resourceType).Observe(duration.Seconds())
}

// RecordJobExecution records Job execution metrics
func RecordJobExecution(namespace, image, result string, duration time.Duration) {
	JobExecutionTotal.WithLabelValues(namespace, image, result).Inc()
	if duration > 0 {
		JobExecutionDuration.WithLabelValues(namespace, image).Observe(duration.Seconds())
	}
}

// UpdateStoryRunStepsGauge updates the active/completed steps gauge
func UpdateStoryRunStepsGauge(namespace, story string, active, completed int) {
	StoryRunStepsActive.WithLabelValues(namespace, story).Set(float64(active))
	StoryRunStepsCompleted.WithLabelValues(namespace, story).Set(float64(completed))
}

// classifyError classifies errors for metrics labeling
func classifyError(err error) string {
	if err == nil {
		return "none"
	}

	errStr := err.Error()
	switch {
	case contains(errStr, "not found"):
		return "not_found"
	case contains(errStr, "conflict"):
		return "conflict"
	case contains(errStr, "timeout"):
		return "timeout"
	case contains(errStr, "connection"):
		return "connection"
	case contains(errStr, "permission") || contains(errStr, "forbidden"):
		return "permission"
	case contains(errStr, "validation"):
		return "validation"
	default:
		return "unknown"
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || (len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
			indexOf(s, substr) >= 0)))
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// RecordQuotaViolation records quota violation metrics
func RecordQuotaViolation(namespace, resourceType, violationType string) {
	QuotaViolationTotal.WithLabelValues(namespace, resourceType, violationType).Inc()
}

// UpdateResourceQuotaUsage updates current resource usage
func UpdateResourceQuotaUsage(namespace, resourceType string, usage float64) {
	ResourceQuotaUsage.WithLabelValues(namespace, resourceType).Set(usage)
}

// UpdateResourceQuotaLimit updates resource quota limits
func UpdateResourceQuotaLimit(namespace, resourceType string, limit float64) {
	ResourceQuotaLimit.WithLabelValues(namespace, resourceType).Set(limit)
}

// RecordStorageOperation records storage provider operations (placeholder for storage package)
func RecordStorageOperation(provider, operation string, duration time.Duration, err error) {
	// This would be implemented properly in the storage package
	// For now, just record as a generic operation
	result := "success"
	if err != nil {
		result = "error"
	}

	// Use existing controller metrics as placeholder
	ControllerReconcileTotal.WithLabelValues(fmt.Sprintf("storage-%s", provider), result).Inc()
	ControllerReconcileDuration.WithLabelValues(fmt.Sprintf("storage-%s", provider)).Observe(duration.Seconds())
}

// RecordArtifactOperation records artifact management operations
func RecordArtifactOperation(operation string, duration time.Duration, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}
	// Use existing controller metrics for artifact operations
	ControllerReconcileTotal.WithLabelValues("artifact-manager", result).Inc()
	ControllerReconcileDuration.WithLabelValues("artifact-manager").Observe(duration.Seconds())
}

// RecordCleanupOperation records cleanup operations
func RecordCleanupOperation(resourceType, namespace string, deletedCount int, duration time.Duration, err error) {
	result := "success"
	if err != nil {
		result = "error"
	}

	// Use existing controller metrics for cleanup operations
	ControllerReconcileTotal.WithLabelValues(fmt.Sprintf("cleanup-%s", resourceType), result).Inc()
	ControllerReconcileDuration.WithLabelValues(fmt.Sprintf("cleanup-%s", resourceType)).Observe(duration.Seconds())

	// Could add specific cleanup metrics here if needed:
	// CleanupOperationsTotal.WithLabelValues(resourceType, namespace, result).Inc()
	// CleanupDeletedResourcesTotal.WithLabelValues(resourceType, namespace).Add(float64(deletedCount))
}

// gRPC Transport metric helpers

// RecordGRPCStreamRequest records a streaming RPC request with its result code
func RecordGRPCStreamRequest(method, code string) {
	GRPCStreamRequests.WithLabelValues(method, code).Inc()
}

// RecordGRPCStreamDuration records the duration of a streaming RPC
func RecordGRPCStreamDuration(method string, durationSeconds float64) {
	GRPCStreamDuration.WithLabelValues(method).Observe(durationSeconds)
}

// RecordGRPCMessageReceived records a message received on a stream
func RecordGRPCMessageReceived(storyRun, step string) {
	GRPCMessagesReceived.WithLabelValues(storyRun, step).Inc()
}

// RecordGRPCMessageSent records a message sent on a stream
func RecordGRPCMessageSent(storyRun, step string) {
	GRPCMessagesSent.WithLabelValues(storyRun, step).Inc()
}

// RecordGRPCMessageDropped records a dropped message with a reason
func RecordGRPCMessageDropped(storyRun, step, reason string) {
	GRPCMessagesDropped.WithLabelValues(storyRun, step, reason).Inc()
}
