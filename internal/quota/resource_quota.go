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

package quota

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bubustack/bobrapet/internal/logging"
	"github.com/bubustack/bobrapet/internal/metrics"
)

// ResourceQuota manages resource limits and quotas via namespace annotations
type ResourceQuota struct {
	client client.Client
}

// NewResourceQuota creates a new resource quota manager
func NewResourceQuota(client client.Client) *ResourceQuota {
	return &ResourceQuota{client: client}
}

// QuotaLimits holds all the quota limits for a namespace
type QuotaLimits struct {
	// Story/StoryRun limits
	MaxActiveStoryRuns     int
	MaxStoriesPerNamespace int
	MaxStepsPerStory       int

	// Loop execution limits
	MaxLoopIterations int
	LoopBatchSize     int
	MaxConcurrency    int

	// Step execution limits
	MaxActiveStepRuns      int
	MaxStepRunsPerStoryRun int
	StepExecutionTimeout   time.Duration

	// Resource limits
	MaxCPUPerStepRun     string
	MaxMemoryPerStepRun  string
	MaxStoragePerStepRun string

	// Feature flags
	ParallelLoopsEnabled   bool
	TelemetryEnabled       bool
	ArtifactStorageEnabled bool

	// Dead Letter Queue settings
	DLQEnabled    bool
	DLQMaxRetries int
}

// DefaultQuotaLimits returns sensible defaults
func DefaultQuotaLimits() QuotaLimits {
	return QuotaLimits{
		MaxActiveStoryRuns:     100,
		MaxStoriesPerNamespace: 1000,
		MaxStepsPerStory:       1000,
		MaxLoopIterations:      10000,
		LoopBatchSize:          100,
		MaxConcurrency:         10,
		MaxActiveStepRuns:      500,
		MaxStepRunsPerStoryRun: 10000,
		StepExecutionTimeout:   30 * time.Minute,
		MaxCPUPerStepRun:       "1000m",
		MaxMemoryPerStepRun:    "1Gi",
		MaxStoragePerStepRun:   "10Gi",
		ParallelLoopsEnabled:   false,
		TelemetryEnabled:       false,
		ArtifactStorageEnabled: false,
		DLQEnabled:             false,
		DLQMaxRetries:          3,
	}
}

// GetQuotaLimits retrieves quota limits from namespace annotations
func (rq *ResourceQuota) GetQuotaLimits(ctx context.Context, namespace string) (QuotaLimits, error) {
	limits := DefaultQuotaLimits()

	var ns corev1.Namespace
	if err := rq.client.Get(ctx, client.ObjectKey{Name: namespace}, &ns); err != nil {
		// If namespace doesn't exist or can't be read, return defaults
		return limits, nil
	}

	annotations := ns.Annotations
	if annotations == nil {
		return limits, nil
	}

	// Parse Story/StoryRun limits
	if val, exists := annotations["bobrapet.bubu.sh/max-active-storyruns"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			limits.MaxActiveStoryRuns = parsed
		}
	}

	if val, exists := annotations["bobrapet.bubu.sh/max-stories"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			limits.MaxStoriesPerNamespace = parsed
		}
	}

	if val, exists := annotations["bobrapet.bubu.sh/max-steps-per-story"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			limits.MaxStepsPerStory = parsed
		}
	}

	// Parse loop execution limits
	if val, exists := annotations["bobrapet.bubu.sh/max-loop-iterations"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			limits.MaxLoopIterations = parsed
		}
	}

	if val, exists := annotations["bobrapet.bubu.sh/loop-batch-size"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 && parsed <= 1000 {
			limits.LoopBatchSize = parsed
		}
	}

	if val, exists := annotations["bobrapet.bubu.sh/max-concurrency"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 && parsed <= 50 {
			limits.MaxConcurrency = parsed
		}
	}

	// Parse step execution limits
	if val, exists := annotations["bobrapet.bubu.sh/max-active-stepruns"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			limits.MaxActiveStepRuns = parsed
		}
	}

	if val, exists := annotations["bobrapet.bubu.sh/max-stepruns-per-storyrun"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			limits.MaxStepRunsPerStoryRun = parsed
		}
	}

	if val, exists := annotations["bobrapet.bubu.sh/step-execution-timeout"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil && parsed > 0 {
			limits.StepExecutionTimeout = parsed
		}
	}

	// Parse resource limits (strings, validated elsewhere)
	if val, exists := annotations["bobrapet.bubu.sh/max-cpu-per-steprun"]; exists {
		limits.MaxCPUPerStepRun = val
	}

	if val, exists := annotations["bobrapet.bubu.sh/max-memory-per-steprun"]; exists {
		limits.MaxMemoryPerStepRun = val
	}

	if val, exists := annotations["bobrapet.bubu.sh/max-storage-per-steprun"]; exists {
		limits.MaxStoragePerStepRun = val
	}

	// Parse feature flags
	if val, exists := annotations["bobrapet.bubu.sh/parallel-loops"]; exists {
		limits.ParallelLoopsEnabled = val == "true"
	}

	if val, exists := annotations["bobrapet.bubu.sh/telemetry"]; exists {
		limits.TelemetryEnabled = val == "true"
	}

	if val, exists := annotations["bobrapet.bubu.sh/artifact-storage"]; exists {
		limits.ArtifactStorageEnabled = val == "true"
	}

	// Parse DLQ settings
	if val, exists := annotations["bobrapet.bubu.sh/dlq-enabled"]; exists {
		limits.DLQEnabled = val == "true"
	}

	if val, exists := annotations["bobrapet.bubu.sh/dlq-max-retries"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed >= 0 && parsed <= 10 {
			limits.DLQMaxRetries = parsed
		}
	}

	return limits, nil
}

// CheckStoryRunQuota validates if a new StoryRun can be created
func (rq *ResourceQuota) CheckStoryRunQuota(ctx context.Context, namespace string) error {
	limits, err := rq.GetQuotaLimits(ctx, namespace)
	if err != nil {
		return err
	}

	// Count active StoryRuns in namespace
	activeCount, err := rq.countActiveStoryRuns(ctx, namespace)
	if err != nil {
		return err
	}

	if activeCount >= limits.MaxActiveStoryRuns {
		metrics.RecordQuotaViolation(namespace, "storyrun", "max_active_exceeded")
		return fmt.Errorf("namespace %s has reached maximum active StoryRuns: %d >= %d",
			namespace, activeCount, limits.MaxActiveStoryRuns)
	}

	return nil
}

// CheckStepRunQuota validates if a new StepRun can be created
func (rq *ResourceQuota) CheckStepRunQuota(ctx context.Context, namespace, storyRunRef string) error {
	limits, err := rq.GetQuotaLimits(ctx, namespace)
	if err != nil {
		return err
	}

	// Count active StepRuns for this StoryRun
	stepRunCount, err := rq.countStepRunsForStoryRun(ctx, namespace, storyRunRef)
	if err != nil {
		return err
	}

	if stepRunCount >= limits.MaxStepRunsPerStoryRun {
		metrics.RecordQuotaViolation(namespace, "steprun", "max_per_storyrun_exceeded")
		return fmt.Errorf("storyRun %s has reached maximum StepRuns: %d >= %d",
			storyRunRef, stepRunCount, limits.MaxStepRunsPerStoryRun)
	}

	// Count total active StepRuns in namespace
	totalStepRuns, err := rq.countActiveStepRuns(ctx, namespace)
	if err != nil {
		return err
	}

	if totalStepRuns >= limits.MaxActiveStepRuns {
		metrics.RecordQuotaViolation(namespace, "steprun", "max_active_exceeded")
		return fmt.Errorf("namespace %s has reached maximum active StepRuns: %d >= %d",
			namespace, totalStepRuns, limits.MaxActiveStepRuns)
	}

	return nil
}

// CheckStoryQuota validates if a new Story can be created
func (rq *ResourceQuota) CheckStoryQuota(ctx context.Context, namespace string, stepCount int) error {
	limits, err := rq.GetQuotaLimits(ctx, namespace)
	if err != nil {
		return err
	}

	// Check steps per story limit
	if stepCount > limits.MaxStepsPerStory {
		metrics.RecordQuotaViolation(namespace, "story", "max_steps_exceeded")
		return fmt.Errorf("Story exceeds maximum steps: %d > %d", stepCount, limits.MaxStepsPerStory)
	}

	// Count total Stories in namespace
	storyCount, err := rq.countStoriesInNamespace(ctx, namespace)
	if err != nil {
		return err
	}

	if storyCount >= limits.MaxStoriesPerNamespace {
		metrics.RecordQuotaViolation(namespace, "story", "max_stories_exceeded")
		return fmt.Errorf("namespace %s has reached maximum Stories: %d >= %d",
			namespace, storyCount, limits.MaxStoriesPerNamespace)
	}

	return nil
}

// Helper methods to count resources
func (rq *ResourceQuota) countActiveStoryRuns(ctx context.Context, namespace string) (int, error) {
	// Use field selector to efficiently count only active StoryRuns
	// This would need to be implemented with proper indexing
	return 0, nil // Placeholder - implement with your indexing strategy
}

func (rq *ResourceQuota) countStepRunsForStoryRun(ctx context.Context, namespace, storyRunRef string) (int, error) {
	// Count StepRuns for specific StoryRun
	return 0, nil // Placeholder - implement with your indexing strategy
}

func (rq *ResourceQuota) countActiveStepRuns(ctx context.Context, namespace string) (int, error) {
	// Count all active StepRuns in namespace
	return 0, nil // Placeholder - implement with your indexing strategy
}

func (rq *ResourceQuota) countStoriesInNamespace(ctx context.Context, namespace string) (int, error) {
	// Count all Stories in namespace
	return 0, nil // Placeholder - implement with your indexing strategy
}

// LogQuotaUsage logs current quota usage for observability
func (rq *ResourceQuota) LogQuotaUsage(ctx context.Context, namespace string) {
	limits, err := rq.GetQuotaLimits(ctx, namespace)
	if err != nil {
		return
	}

	logger := logging.NewControllerLogger(ctx, "quota")
	logger.Info("Current quota limits for namespace",
		"namespace", namespace,
		"max_active_storyruns", limits.MaxActiveStoryRuns,
		"max_stories", limits.MaxStoriesPerNamespace,
		"max_steps_per_story", limits.MaxStepsPerStory,
		"max_loop_iterations", limits.MaxLoopIterations,
		"parallel_enabled", limits.ParallelLoopsEnabled,
		"dlq_enabled", limits.DLQEnabled)
}
