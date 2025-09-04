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

package lifecycle

import (
	"context"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/internal/logging"
	"github.com/bubustack/bobrapet/internal/metrics"
)

// CleanupManager handles TTL-based cleanup of runs and associated resources
type CleanupManager struct {
	client client.Client
}

// CleanupPolicy defines the cleanup strategy for a namespace
type CleanupPolicy struct {
	// TTL for successful runs (default: 7 days)
	SuccessfulRunsTTL time.Duration
	// TTL for failed runs (default: 30 days, keep longer for debugging)
	FailedRunsTTL time.Duration
	// TTL for running/pending runs (default: 24 hours, cleanup stuck runs)
	RunningRunsTTL time.Duration
	// Maximum number of runs to keep per Story (default: 100)
	MaxRunsPerStory int
	// Whether to cleanup artifacts when cleaning up runs
	CleanupArtifacts bool
	// Whether to cleanup associated Jobs
	CleanupJobs bool
}

// DefaultCleanupPolicy returns sensible defaults
func DefaultCleanupPolicy() CleanupPolicy {
	return CleanupPolicy{
		SuccessfulRunsTTL: 7 * 24 * time.Hour,  // 7 days
		FailedRunsTTL:     30 * 24 * time.Hour, // 30 days
		RunningRunsTTL:    24 * time.Hour,      // 24 hours
		MaxRunsPerStory:   100,
		CleanupArtifacts:  true,
		CleanupJobs:       true,
	}
}

// NewCleanupManager creates a new cleanup manager
func NewCleanupManager(client client.Client) *CleanupManager {
	return &CleanupManager{
		client: client,
	}
}

// GetCleanupPolicy extracts cleanup policy from namespace annotations
func (c *CleanupManager) GetCleanupPolicy(ctx context.Context, namespace string) (CleanupPolicy, error) {
	// Start with defaults
	policy := DefaultCleanupPolicy()

	// Get namespace to read annotations
	var ns corev1.Namespace
	if err := c.client.Get(ctx, types.NamespacedName{Name: namespace}, &ns); err != nil {
		return policy, fmt.Errorf("failed to get namespace %s: %w", namespace, err)
	}

	// Override defaults with annotations
	if ns.Annotations != nil {
		if val, ok := ns.Annotations["bobrapet.bubu.sh/cleanup-successful-ttl"]; ok {
			if duration, err := time.ParseDuration(val); err == nil {
				policy.SuccessfulRunsTTL = duration
			}
		}
		if val, ok := ns.Annotations["bobrapet.bubu.sh/cleanup-failed-ttl"]; ok {
			if duration, err := time.ParseDuration(val); err == nil {
				policy.FailedRunsTTL = duration
			}
		}
		if val, ok := ns.Annotations["bobrapet.bubu.sh/cleanup-running-ttl"]; ok {
			if duration, err := time.ParseDuration(val); err == nil {
				policy.RunningRunsTTL = duration
			}
		}
		if val, ok := ns.Annotations["bobrapet.bubu.sh/cleanup-max-runs"]; ok {
			if maxRuns, err := strconv.Atoi(val); err == nil && maxRuns > 0 {
				policy.MaxRunsPerStory = maxRuns
			}
		}
		if val, ok := ns.Annotations["bobrapet.bubu.sh/cleanup-artifacts"]; ok {
			policy.CleanupArtifacts = val == "true"
		}
		if val, ok := ns.Annotations["bobrapet.bubu.sh/cleanup-jobs"]; ok {
			policy.CleanupJobs = val == "true"
		}
	}

	return policy, nil
}

// CleanupStoryRuns performs TTL-based cleanup of StoryRuns in a namespace
func (c *CleanupManager) CleanupStoryRuns(ctx context.Context, namespace string) error {
	logger := logging.NewControllerLogger(ctx, "cleanup-manager").
		WithValues("namespace", namespace, "resource", "storyrun")

	startTime := time.Now()

	// Get cleanup policy for this namespace
	policy, err := c.GetCleanupPolicy(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to get cleanup policy: %w", err)
	}

	// List all StoryRuns in the namespace
	var storyRuns runsv1alpha1.StoryRunList
	if err := c.client.List(ctx, &storyRuns, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("failed to list story runs: %w", err)
	}

	logger.Info("Starting StoryRun cleanup",
		"totalRuns", len(storyRuns.Items),
		"policy", fmt.Sprintf("success:%v, failed:%v, running:%v, max:%d",
			policy.SuccessfulRunsTTL, policy.FailedRunsTTL, policy.RunningRunsTTL, policy.MaxRunsPerStory))

	deletedCount := 0

	// Group runs by story for max count enforcement
	runsByStory := make(map[string][]runsv1alpha1.StoryRun)
	for _, run := range storyRuns.Items {
		story := run.Spec.StoryRef
		runsByStory[story] = append(runsByStory[story], run)
	}

	// Process each story's runs
	for storyName, runs := range runsByStory {
		storyDeletedCount, err := c.cleanupStoryRunsForStory(ctx, namespace, storyName, runs, policy)
		if err != nil {
			logger.Error(err, "Failed to cleanup runs for story", "story", storyName)
			continue
		}
		deletedCount += storyDeletedCount
	}

	// Record metrics
	duration := time.Since(startTime)
	metrics.RecordCleanupOperation("storyrun", namespace, deletedCount, duration, nil)

	logger.Info("StoryRun cleanup completed",
		"deletedCount", deletedCount,
		"duration", duration)

	return nil
}

// cleanupStoryRunsForStory cleans up runs for a specific story
func (c *CleanupManager) cleanupStoryRunsForStory(ctx context.Context, namespace, storyName string, runs []runsv1alpha1.StoryRun, policy CleanupPolicy) (int, error) {
	logger := logging.NewControllerLogger(ctx, "cleanup-manager").
		WithValues("namespace", namespace, "story", storyName)

	now := time.Now()
	deletedCount := 0
	toDelete := []runsv1alpha1.StoryRun{}

	// First pass: identify runs to delete based on TTL
	for _, run := range runs {
		shouldDelete := false
		reason := ""

		// Get the relevant timestamp for TTL calculation
		var relevantTime time.Time
		if run.Status.FinishedAt != nil {
			relevantTime = run.Status.FinishedAt.Time
		} else if run.Status.StartedAt != nil {
			relevantTime = run.Status.StartedAt.Time
		} else {
			relevantTime = run.CreationTimestamp.Time
		}

		// Apply TTL based on phase
		switch run.Status.Phase {
		case "Succeeded":
			if now.Sub(relevantTime) > policy.SuccessfulRunsTTL {
				shouldDelete = true
				reason = fmt.Sprintf("successful run TTL exceeded (age: %v)", now.Sub(relevantTime))
			}
		case "Failed", "Error":
			if now.Sub(relevantTime) > policy.FailedRunsTTL {
				shouldDelete = true
				reason = fmt.Sprintf("failed run TTL exceeded (age: %v)", now.Sub(relevantTime))
			}
		case "Running", "Pending", "":
			if now.Sub(relevantTime) > policy.RunningRunsTTL {
				shouldDelete = true
				reason = fmt.Sprintf("running run TTL exceeded (age: %v)", now.Sub(relevantTime))
			}
		}

		if shouldDelete {
			logger.V(1).Info("Marking run for deletion", "run", run.Name, "reason", reason)
			toDelete = append(toDelete, run)
		}
	}

	// Second pass: enforce max count limit (keep most recent)
	if len(runs)-len(toDelete) > policy.MaxRunsPerStory {
		// Sort remaining runs by creation time (newest first)
		remaining := []runsv1alpha1.StoryRun{}
		for _, run := range runs {
			shouldKeep := true
			for _, del := range toDelete {
				if del.Name == run.Name {
					shouldKeep = false
					break
				}
			}
			if shouldKeep {
				remaining = append(remaining, run)
			}
		}

		// Sort by creation time (newest first)
		for i := 0; i < len(remaining)-1; i++ {
			for j := i + 1; j < len(remaining); j++ {
				if remaining[i].CreationTimestamp.Before(&remaining[j].CreationTimestamp) {
					remaining[i], remaining[j] = remaining[j], remaining[i]
				}
			}
		}

		// Mark excess runs for deletion
		if len(remaining) > policy.MaxRunsPerStory {
			for i := policy.MaxRunsPerStory; i < len(remaining); i++ {
				logger.V(1).Info("Marking run for deletion due to count limit",
					"run", remaining[i].Name,
					"position", i+1,
					"limit", policy.MaxRunsPerStory)
				toDelete = append(toDelete, remaining[i])
			}
		}
	}

	// Actually delete the runs
	for _, run := range toDelete {
		if err := c.deleteStoryRun(ctx, &run, policy); err != nil {
			logger.Error(err, "Failed to delete StoryRun", "run", run.Name)
			continue
		}
		deletedCount++
		logger.V(1).Info("Deleted StoryRun", "run", run.Name)
	}

	return deletedCount, nil
}

// CleanupStepRuns performs TTL-based cleanup of StepRuns in a namespace
func (c *CleanupManager) CleanupStepRuns(ctx context.Context, namespace string) error {
	logger := logging.NewControllerLogger(ctx, "cleanup-manager").
		WithValues("namespace", namespace, "resource", "steprun")

	startTime := time.Now()

	// Get cleanup policy for this namespace
	policy, err := c.GetCleanupPolicy(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to get cleanup policy: %w", err)
	}

	// List all StepRuns in the namespace
	var stepRuns runsv1alpha1.StepRunList
	if err := c.client.List(ctx, &stepRuns, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("failed to list step runs: %w", err)
	}

	logger.Info("Starting StepRun cleanup", "totalRuns", len(stepRuns.Items))

	deletedCount := 0
	now := time.Now()

	for _, run := range stepRuns.Items {
		shouldDelete := false

		// Get the relevant timestamp (StepRun uses string timestamps)
		var relevantTime time.Time
		if run.Status.FinishedAt != "" {
			if t, err := time.Parse(time.RFC3339, run.Status.FinishedAt); err == nil {
				relevantTime = t
			} else {
				relevantTime = run.CreationTimestamp.Time
			}
		} else if run.Status.StartedAt != "" {
			if t, err := time.Parse(time.RFC3339, run.Status.StartedAt); err == nil {
				relevantTime = t
			} else {
				relevantTime = run.CreationTimestamp.Time
			}
		} else {
			relevantTime = run.CreationTimestamp.Time
		}

		// Apply TTL based on phase
		switch run.Status.Phase {
		case "Succeeded":
			shouldDelete = now.Sub(relevantTime) > policy.SuccessfulRunsTTL
		case "Failed", "Error":
			shouldDelete = now.Sub(relevantTime) > policy.FailedRunsTTL
		case "Running", "Pending", "":
			shouldDelete = now.Sub(relevantTime) > policy.RunningRunsTTL
		}

		if shouldDelete {
			if err := c.deleteStepRun(ctx, &run, policy); err != nil {
				logger.Error(err, "Failed to delete StepRun", "run", run.Name)
				continue
			}
			deletedCount++
			logger.V(1).Info("Deleted StepRun", "run", run.Name, "age", now.Sub(relevantTime))
		}
	}

	// Record metrics
	duration := time.Since(startTime)
	metrics.RecordCleanupOperation("steprun", namespace, deletedCount, duration, nil)

	logger.Info("StepRun cleanup completed",
		"deletedCount", deletedCount,
		"duration", duration)

	return nil
}

// deleteStoryRun deletes a StoryRun and optionally its associated resources
func (c *CleanupManager) deleteStoryRun(ctx context.Context, run *runsv1alpha1.StoryRun, policy CleanupPolicy) error {
	// TODO: If policy.CleanupArtifacts, clean up artifacts for this StoryRun
	// TODO: If policy.CleanupJobs, clean up associated Jobs

	return c.client.Delete(ctx, run)
}

// deleteStepRun deletes a StepRun and optionally its associated resources
func (c *CleanupManager) deleteStepRun(ctx context.Context, run *runsv1alpha1.StepRun, policy CleanupPolicy) error {
	// TODO: If policy.CleanupArtifacts, clean up artifacts for this StepRun
	// TODO: If policy.CleanupJobs, clean up associated Job

	return c.client.Delete(ctx, run)
}

// CleanupNamespace performs complete cleanup for a namespace
func (c *CleanupManager) CleanupNamespace(ctx context.Context, namespace string) error {
	logger := logging.NewControllerLogger(ctx, "cleanup-manager").
		WithValues("namespace", namespace)

	logger.Info("Starting namespace cleanup")

	// Cleanup StepRuns first (child resources)
	if err := c.CleanupStepRuns(ctx, namespace); err != nil {
		logger.Error(err, "Failed to cleanup StepRuns")
		return fmt.Errorf("failed to cleanup step runs: %w", err)
	}

	// Then cleanup StoryRuns (parent resources)
	if err := c.CleanupStoryRuns(ctx, namespace); err != nil {
		logger.Error(err, "Failed to cleanup StoryRuns")
		return fmt.Errorf("failed to cleanup story runs: %w", err)
	}

	logger.Info("Namespace cleanup completed")
	return nil
}

// GetCleanupStats returns cleanup statistics for a namespace
func (c *CleanupManager) GetCleanupStats(ctx context.Context, namespace string) (*CleanupStats, error) {
	stats := &CleanupStats{
		Namespace: namespace,
	}

	// Count StoryRuns by phase
	var storyRuns runsv1alpha1.StoryRunList
	if err := c.client.List(ctx, &storyRuns, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("failed to list story runs: %w", err)
	}

	for _, run := range storyRuns.Items {
		stats.TotalStoryRuns++
		switch run.Status.Phase {
		case "Succeeded":
			stats.SuccessfulStoryRuns++
		case "Failed", "Error":
			stats.FailedStoryRuns++
		default:
			stats.RunningStoryRuns++
		}
	}

	// Count StepRuns by phase
	var stepRuns runsv1alpha1.StepRunList
	if err := c.client.List(ctx, &stepRuns, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("failed to list step runs: %w", err)
	}

	for _, run := range stepRuns.Items {
		stats.TotalStepRuns++
		switch run.Status.Phase {
		case "Succeeded":
			stats.SuccessfulStepRuns++
		case "Failed", "Error":
			stats.FailedStepRuns++
		default:
			stats.RunningStepRuns++
		}
	}

	return stats, nil
}

// CleanupStats represents cleanup statistics
type CleanupStats struct {
	Namespace           string `json:"namespace"`
	TotalStoryRuns      int    `json:"totalStoryRuns"`
	SuccessfulStoryRuns int    `json:"successfulStoryRuns"`
	FailedStoryRuns     int    `json:"failedStoryRuns"`
	RunningStoryRuns    int    `json:"runningStoryRuns"`
	TotalStepRuns       int    `json:"totalStepRuns"`
	SuccessfulStepRuns  int    `json:"successfulStepRuns"`
	FailedStepRuns      int    `json:"failedStepRuns"`
	RunningStepRuns     int    `json:"runningStepRuns"`
}
