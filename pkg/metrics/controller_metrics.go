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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	resultSuccess = "success"
	resultError   = "error"
)

// StoryRunWindow labels identify which cleanup window a gauge value tracks.
const (
	WindowChildTTL  = "child_ttl"
	WindowRetention = "retention"
)

var (
	metricsEnabled atomic.Bool

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

	StoryRunWindowSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobrapet_storyrun_window_seconds",
			Help: "Configured child TTL and retention windows applied to StoryRuns",
		},
		[]string{"namespace", "story", "window"},
	)

	StoryRunQueueAgeSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobrapet_storyrun_queue_age_seconds",
			Help:    "Time StoryRuns spend queued due to scheduling or priority ordering.",
			Buckets: []float64{1, 5, 15, 30, 60, 300, 600, 1800, 3600},
		},
		[]string{"namespace", "queue"},
	)

	StoryRunQueueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobrapet_storyrun_queue_depth",
			Help: "Number of steps queued by scheduling limits per queue.",
		},
		[]string{"namespace", "storyrun", "queue"},
	)

	ImpulseThrottledTriggersTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobrapet_impulse_throttled_triggers_total",
			Help: "Number of triggers delayed by per-Impulse throttling, sourced from Impulse status.",
		},
		[]string{"namespace", "impulse"},
	)

	StoryRunDependentsDeletedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_storyrun_dependents_deleted_total",
			Help: "Total number of StoryRun dependents deleted during cleanup, partitioned by resource type.",
		},
		[]string{"namespace", "storyrun", "resource"},
	)

	StoryRunRBACOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_storyrun_rbac_operations_total",
			Help: "Number of StoryRun RBAC reconciliation operations by resource and controller result",
		},
		[]string{"resource", "operation"},
	)

	StoryDirtyMarksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_story_dirty_marks_total",
			Help: "Total number of dirty flags set for Stories",
		},
		[]string{"namespace", "story", "reason"},
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

	StepRunCacheLookupsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_steprun_cache_lookups_total",
			Help: "Total number of StepRun cache lookups partitioned by hit/miss result.",
		},
		[]string{"namespace", "engram", "result"},
	)

	StepRunChildrenCreatedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_child_stepruns_created_total",
			Help: "Total number of child StepRuns created per StoryRun/Engram.",
		},
		[]string{"namespace", "storyrun", "engram"},
	)

	DownstreamTargetMutationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_downstream_target_mutations_total",
			Help: "Total number of downstream target mutations applied to StepRuns",
		},
		[]string{"namespace", "step", "action", "hashes"},
	)
	TransportBindingReadFallbacksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_transport_binding_read_fallbacks_total",
			Help: "Total number of binding env read fallbacks triggered while waiting for " +
				"API readers to observe TransportBindings",
		},
		[]string{"namespace", "reader"},
	)

	ResolverStageTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_resolver_stage_total",
			Help: "Total number of resolver chain stages executed, labeled by layer, stage, mode, and outcome",
		},
		[]string{"layer", "stage", "mode", "outcome"},
	)

	ResolverStageDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobrapet_resolver_stage_duration_seconds",
			Help:    "Duration of resolver chain stages",
			Buckets: []float64{0.001, 0.01, 0.05, 0.1, 0.5, 1, 2},
		},
		[]string{"layer", "stage"},
	)

	ResolverServiceAccountFallbacksTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_resolver_service_account_fallback_total",
			Help: "Number of times the config resolver defaulted the StepRun ServiceAccount to the per-StoryRun runner",
		},
		[]string{"storyrun"},
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

	TransportBindingOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_transport_binding_operations_total",
			Help: "Total number of TransportBinding operations performed by a controller",
		},
		[]string{"controller", "result", "mutated", "binding_alias"},
	)

	TransportBindingOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobrapet_transport_binding_operation_duration_seconds",
			Help:    "Duration of TransportBinding CreateOrUpdate calls",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5},
		},
		[]string{"controller", "mutated", "binding_alias"},
	)

	TransportBindingAnnotationSanitizeFailuresTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_transport_binding_annotation_sanitize_failures_total",
			Help: "Counts sanitized binding annotation failures keyed by namespace, StoryRun, and Step.",
		},
		[]string{"namespace", "story_run", "step"},
	)

	ControllerMapperFailures = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_controller_mapper_failures_total",
			Help: "Total number of controller watch mapper failures",
		},
		[]string{"controller", "mapper"},
	)

	ControllerIndexFallbacks = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_controller_index_fallback_total",
			Help: "Total number of times controllers had to fall back to full namespace scans " +
				"because a field index lookup failed",
		},
		[]string{"controller", "index"},
	)

	TriggerBackfillLoopsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_trigger_backfill_loops_total",
			Help: "Number of bounded trigger backfill iterations executed",
		},
		[]string{"controller", "namespace", "resource", "pending"},
	)

	TriggerBackfillMarkedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_trigger_backfill_marked_total",
			Help: "Total number of child resources annotated during trigger backfill",
		},
		[]string{"controller", "namespace", "resource"},
	)

	DAGIterationSteps = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bobrapet_dag_iteration_steps",
			Help:    "Ready/skipped step counts per DAG iteration",
			Buckets: []float64{0, 1, 2, 5, 10, 20, 40},
		},
		[]string{"controller", "type"},
	)

	SubStoryRefreshTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bobrapet_substory_refresh_total",
			Help: "Total number of DAG sub-story refreshes triggered by synchronous executeStory completions",
		},
		[]string{"controller"},
	)
)

func init() {
	metricsEnabled.Store(true)
	metrics.Registry.MustRegister(
		StoryRunsTotal,
		StoryRunDuration,
		StoryRunStepsActive,
		StoryRunStepsCompleted,
		StoryRunWindowSeconds,
		StoryRunQueueAgeSeconds,
		StoryRunQueueDepth,
		ImpulseThrottledTriggersTotal,
		StoryRunDependentsDeletedTotal,
		StoryRunRBACOperationsTotal,
		StoryDirtyMarksTotal,
		StepRunsTotal,
		StepRunDuration,
		StepRunRetries,
		StepRunCacheLookupsTotal,
		StepRunChildrenCreatedTotal,
		DownstreamTargetMutationsTotal,
		TransportBindingReadFallbacksTotal,
		ResolverStageTotal,
		ResolverStageDuration,
		ResolverServiceAccountFallbacksTotal,
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
		TransportBindingOperationsTotal,
		TransportBindingOperationDuration,
		TransportBindingAnnotationSanitizeFailuresTotal,
		ControllerMapperFailures,
		ControllerIndexFallbacks,
		TriggerBackfillLoopsTotal,
		TriggerBackfillMarkedTotal,
		DAGIterationSteps,
		SubStoryRefreshTotal,
	)
}

// Enable toggles emission of controller metrics at runtime.
func Enable(enabled bool) {
	metricsEnabled.Store(enabled)
}

func shouldRecord() bool {
	return metricsEnabled.Load()
}

// Helper functions to record metrics with consistent labels

// RecordStoryRunMetrics records metrics for StoryRun operations
func RecordStoryRunMetrics(namespace, story, phase string, duration time.Duration) {
	if !shouldRecord() {
		return
	}
	StoryRunsTotal.WithLabelValues(namespace, story, phase).Inc()
	if duration > 0 {
		StoryRunDuration.WithLabelValues(namespace, story, phase).Observe(duration.Seconds())
	}
}

// RecordStoryRunWindowSeconds captures the configured TTL/retention windows for StoryRuns.
func RecordStoryRunWindowSeconds(namespace, story, window string, seconds float64) {
	if !shouldRecord() {
		return
	}
	if seconds < 0 {
		seconds = 0
	}
	StoryRunWindowSeconds.WithLabelValues(namespace, story, window).Set(seconds)
}

// RecordStoryRunQueueAge observes how long a StoryRun has been queued.
func RecordStoryRunQueueAge(namespace, queue string, age time.Duration) {
	if !shouldRecord() {
		return
	}
	if namespace == "" {
		namespace = UnknownLabelValue
	}
	if queue == "" {
		queue = UnknownLabelValue
	}
	if age < 0 {
		age = 0
	}
	StoryRunQueueAgeSeconds.WithLabelValues(namespace, queue).Observe(age.Seconds())
}

// RecordStoryRunQueueDepth records the number of steps queued for a queue.
func RecordStoryRunQueueDepth(namespace, storyRun, queue string, depth int) {
	if !shouldRecord() {
		return
	}
	if namespace == "" {
		namespace = UnknownLabelValue
	}
	if storyRun == "" {
		storyRun = UnknownLabelValue
	}
	if queue == "" {
		queue = UnknownLabelValue
	}
	if depth < 0 {
		depth = 0
	}
	StoryRunQueueDepth.WithLabelValues(namespace, storyRun, queue).Set(float64(depth))
}

// RecordImpulseThrottledTriggers records throttled trigger counts from Impulse status.
func RecordImpulseThrottledTriggers(namespace, impulse string, count int64) {
	if !shouldRecord() {
		return
	}
	if namespace == "" {
		namespace = UnknownLabelValue
	}
	if impulse == "" {
		impulse = UnknownLabelValue
	}
	if count < 0 {
		count = 0
	}
	ImpulseThrottledTriggersTotal.WithLabelValues(namespace, impulse).Set(float64(count))
}

// RecordStoryDirtyMark increments the metric tracking why a Story was marked dirty.
func RecordStoryDirtyMark(namespace, story, reason string) {
	if !shouldRecord() {
		return
	}
	StoryDirtyMarksTotal.WithLabelValues(namespace, story, reason).Inc()
}

// RecordStoryRunDependentCleanup counts StoryRun dependent deletions keyed by resource type.
func RecordStoryRunDependentCleanup(namespace, storyRun, resource string, count int) {
	if !shouldRecord() || count <= 0 {
		return
	}
	if namespace == "" {
		namespace = UnknownLabelValue
	}
	if storyRun == "" {
		storyRun = UnknownLabelValue
	}
	if resource == "" {
		resource = UnknownResourceValue
	}
	StoryRunDependentsDeletedTotal.WithLabelValues(namespace, storyRun, resource).Add(float64(count))
}

// RecordStoryRunRBACOperation counts RBAC reconciliation changes keyed by operation result.
func RecordStoryRunRBACOperation(resource, operation string) {
	if !shouldRecord() {
		return
	}
	if operation == "" {
		operation = "unchanged"
	}
	StoryRunRBACOperationsTotal.WithLabelValues(resource, operation).Inc()
}

// RecordStepRunMetrics records metrics for StepRun operations
func RecordStepRunMetrics(namespace, engram, phase string, duration time.Duration) {
	if !shouldRecord() {
		return
	}
	StepRunsTotal.WithLabelValues(namespace, engram, phase).Inc()
	if duration > 0 {
		StepRunDuration.WithLabelValues(namespace, engram, phase).Observe(duration.Seconds())
	}
}

// RecordStepRunCacheLookup increments the StepRun cache lookup counter.
func RecordStepRunCacheLookup(namespace, engram, result string) {
	if !shouldRecord() {
		return
	}
	if namespace == "" {
		namespace = UnknownLabelValue
	}
	if engram == "" {
		engram = UnknownLabelValue
	}
	if result == "" {
		result = UnknownResourceValue
	}
	StepRunCacheLookupsTotal.WithLabelValues(namespace, engram, result).Inc()
}

// RecordDownstreamTargetMutation counts downstream target updates keyed by action and hash summary.
func RecordDownstreamTargetMutation(namespace, step, action, hashSummary string) {
	if !shouldRecord() {
		return
	}
	if action == "" {
		action = UnknownResourceValue
	}
	if hashSummary == "" {
		hashSummary = EmptyHashSummary
	}
	DownstreamTargetMutationsTotal.WithLabelValues(namespace, step, action, hashSummary).Inc()
}

// RecordChildStepRunCreated increments the counter tracking newly created child StepRuns.
func RecordChildStepRunCreated(namespace, storyRun, engram string) {
	if !shouldRecord() {
		return
	}
	if namespace == "" {
		namespace = UnknownLabelValue
	}
	if storyRun == "" {
		storyRun = UnknownLabelValue
	}
	if engram == "" {
		engram = UnknownLabelValue
	}
	StepRunChildrenCreatedTotal.WithLabelValues(namespace, storyRun, engram).Inc()
}

// RecordTransportBindingReadFallback counts binding env read fallbacks keyed by namespace and reader type.
func RecordTransportBindingReadFallback(namespace, reader string) {
	if !shouldRecord() {
		return
	}
	if namespace == "" {
		namespace = UnknownLabelValue
	}
	if reader == "" {
		reader = "cached_client"
	}
	TransportBindingReadFallbacksTotal.WithLabelValues(namespace, reader).Inc()
}

// RecordResolverStage tracks resolver-chain stage execution telemetry.
func RecordResolverStage(layer, stage, mode, outcome string, duration time.Duration) {
	if !shouldRecord() {
		return
	}
	if layer == "" {
		layer = UnknownResourceValue
	}
	if stage == "" {
		stage = UnknownResourceValue
	}
	if mode == "" {
		mode = UnknownResourceValue
	}
	if outcome == "" {
		outcome = resultSuccess
	}
	ResolverStageTotal.WithLabelValues(layer, stage, mode, outcome).Inc()
	if duration > 0 {
		ResolverStageDuration.WithLabelValues(layer, stage).Observe(duration.Seconds())
	}
}

// RecordResolverServiceAccountFallback counts resolver runs that default the ServiceAccount name.
func RecordResolverServiceAccountFallback(storyRun string) {
	if !shouldRecord() {
		return
	}
	if storyRun == "" {
		storyRun = UnknownLabelValue
	}
	ResolverServiceAccountFallbacksTotal.WithLabelValues(storyRun).Inc()
}

// RecordTriggerBackfill captures bounded trigger backfill activity for controllers such as
// Engram and Story.
func RecordTriggerBackfill(controller, namespace, resource string, marked int, pending bool) {
	if !shouldRecord() {
		return
	}
	pendingLabel := strconv.FormatBool(pending)
	TriggerBackfillLoopsTotal.WithLabelValues(controller, namespace, resource, pendingLabel).Inc()
	if marked > 0 {
		TriggerBackfillMarkedTotal.WithLabelValues(controller, namespace, resource).Add(float64(marked))
	}
}

// RecordDAGIteration captures ready/skipped counts for DAG loop iterations.
func RecordDAGIteration(controller string, readyCount, skippedCount int) {
	if !shouldRecord() {
		return
	}
	if readyCount >= 0 {
		DAGIterationSteps.WithLabelValues(controller, "ready").Observe(float64(readyCount))
	}
	if skippedCount >= 0 {
		DAGIterationSteps.WithLabelValues(controller, "skipped").Observe(float64(skippedCount))
	}
}

// RecordSubStoryRefresh increments the counter tracking DAG sub-story refresh passes.
func RecordSubStoryRefresh(controller string) {
	if !shouldRecord() {
		return
	}
	SubStoryRefreshTotal.WithLabelValues(controller).Inc()
}

// RecordStepRunDuration records duration metrics for StepRun operations (alias for consistency)
func RecordStepRunDuration(namespace, storyRunRef, stepName, phase string, duration time.Duration) {
	// Use engram field as step identifier for metrics consistency
	RecordStepRunMetrics(namespace, stepName, phase, duration)
}

// RecordControllerReconcile records controller reconcile metrics
func RecordControllerReconcile(controller string, duration time.Duration, err error) {
	if !shouldRecord() {
		return
	}
	result := resultSuccess
	if err != nil {
		result = resultError
		ControllerReconcileErrors.WithLabelValues(controller, classifyError(err)).Inc()
	}

	ControllerReconcileTotal.WithLabelValues(controller, result).Inc()
	ControllerReconcileDuration.WithLabelValues(controller).Observe(duration.Seconds())
}

// RecordCELEvaluation records CEL evaluation metrics
func RecordCELEvaluation(expressionType string, duration time.Duration, err error) {
	if !shouldRecord() {
		return
	}
	result := resultSuccess
	if err != nil {
		result = resultError
	}

	CELEvaluationTotal.WithLabelValues(expressionType, result).Inc()
	CELEvaluationDuration.WithLabelValues(expressionType).Observe(duration.Seconds())
}

// RecordCELCacheHit records CEL cache hit
func RecordCELCacheHit(cacheType string) {
	if !shouldRecord() {
		return
	}
	CELCacheHits.WithLabelValues(cacheType).Inc()
}

// RecordResourceCleanup records resource cleanup metrics
func RecordResourceCleanup(resourceType string, duration time.Duration, err error) {
	if !shouldRecord() {
		return
	}
	result := resultSuccess
	if err != nil {
		result = resultError
	}

	ResourceCleanupTotal.WithLabelValues(resourceType, result).Inc()
	if duration > 0 {
		ResourceCleanupDuration.WithLabelValues(resourceType).Observe(duration.Seconds())
	}
}

// RecordTransportBindingOperation records metrics for TransportBinding mutations.
func RecordTransportBindingOperation(controller, bindingAlias string, mutated bool, duration time.Duration, err error) {
	if !shouldRecord() {
		return
	}
	if bindingAlias == "" {
		bindingAlias = UnknownLabelValue
	}
	result := resultSuccess
	if err != nil {
		result = resultError
	}

	mutatedLabel := strconv.FormatBool(mutated)
	TransportBindingOperationsTotal.WithLabelValues(controller, result, mutatedLabel, bindingAlias).Inc()
	if duration > 0 {
		TransportBindingOperationDuration.WithLabelValues(controller, mutatedLabel, bindingAlias).Observe(duration.Seconds())
	}
}

// RecordTransportBindingAnnotationSanitizeFailure increments the counter when
// Engram binding annotations fail sanitization.
func RecordTransportBindingAnnotationSanitizeFailure(namespace, storyRun, step string) {
	if !shouldRecord() {
		return
	}
	if namespace == "" {
		namespace = UnknownLabelValue
	}
	if storyRun == "" {
		storyRun = UnknownLabelValue
	}
	if step == "" {
		step = UnknownLabelValue
	}
	TransportBindingAnnotationSanitizeFailuresTotal.WithLabelValues(namespace, storyRun, step).Inc()
}

// RecordControllerMapperFailure records mapper failures for watch handlers.
func RecordControllerMapperFailure(controller, mapper string) {
	if !shouldRecord() {
		return
	}
	ControllerMapperFailures.WithLabelValues(controller, mapper).Inc()
}

// RecordControllerIndexFallback records when a controller cannot use a configured field index.
func RecordControllerIndexFallback(controller, index string) {
	if !shouldRecord() {
		return
	}
	ControllerIndexFallbacks.WithLabelValues(controller, index).Inc()
}

// RecordJobExecution records Job execution metrics
func RecordJobExecution(namespace, image, result string, duration time.Duration) {
	if !shouldRecord() {
		return
	}
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
		return UnknownResourceValue
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
	if !shouldRecord() {
		return
	}
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
	if !shouldRecord() {
		return
	}
	// This would be implemented properly in the storage package
	// For now, just record as a generic operation
	result := resultSuccess
	if err != nil {
		result = resultError
	}

	// Use existing controller metrics as placeholder
	ControllerReconcileTotal.WithLabelValues(fmt.Sprintf("storage-%s", provider), result).Inc()
	ControllerReconcileDuration.WithLabelValues(fmt.Sprintf("storage-%s", provider)).Observe(duration.Seconds())
}

// RecordArtifactOperation records artifact management operations
func RecordArtifactOperation(operation string, duration time.Duration, err error) {
	if !shouldRecord() {
		return
	}
	result := resultSuccess
	if err != nil {
		result = resultError
	}
	// Use existing controller metrics for artifact operations
	ControllerReconcileTotal.WithLabelValues("artifact-manager", result).Inc()
	ControllerReconcileDuration.WithLabelValues("artifact-manager").Observe(duration.Seconds())
}

// RecordCleanupOperation records cleanup operations
func RecordCleanupOperation(resourceType, namespace string, deletedCount int, duration time.Duration, err error) {
	if !shouldRecord() {
		return
	}
	result := resultSuccess
	if err != nil {
		result = resultError
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
	if !shouldRecord() {
		return
	}
	GRPCStreamRequests.WithLabelValues(method, code).Inc()
}

// RecordGRPCStreamDuration records the duration of a streaming RPC
func RecordGRPCStreamDuration(method string, durationSeconds float64) {
	if !shouldRecord() {
		return
	}
	GRPCStreamDuration.WithLabelValues(method).Observe(durationSeconds)
}

// RecordGRPCMessageReceived records a message received on a stream
func RecordGRPCMessageReceived(storyRun, step string) {
	if !shouldRecord() {
		return
	}
	GRPCMessagesReceived.WithLabelValues(storyRun, step).Inc()
}

// RecordGRPCMessageSent records a message sent on a stream
func RecordGRPCMessageSent(storyRun, step string) {
	if !shouldRecord() {
		return
	}
	GRPCMessagesSent.WithLabelValues(storyRun, step).Inc()
}

// RecordGRPCMessageDropped records a dropped message with a reason
func RecordGRPCMessageDropped(storyRun, step, reason string) {
	if !shouldRecord() {
		return
	}
	GRPCMessagesDropped.WithLabelValues(storyRun, step, reason).Inc()
}
