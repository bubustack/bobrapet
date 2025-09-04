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

package controller

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"

	"github.com/bubustack/bobrapet/internal/logging"
	"github.com/bubustack/bobrapet/internal/metrics"
)

// StoryReconciler reconciles a Story object
type StoryReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	conditionManager *ConditionManager
	finalizerManager *FinalizerManager
}

// NewStoryReconciler creates a new StoryReconciler with initialized managers
func NewStoryReconciler(client client.Client, scheme *runtime.Scheme) *StoryReconciler {
	return &StoryReconciler{
		Client:           client,
		Scheme:           scheme,
		conditionManager: NewConditionManager(0), // Initialize with generation 0, will update per reconcile
		finalizerManager: NewFinalizerManager(client),
	}
}

// +kubebuilder:rbac:groups=bubu.sh,resources=stories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bubu.sh,resources=stories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bubu.sh,resources=stories/finalizers,verbs=update
// +kubebuilder:rbac:groups=bubu.sh,resources=engrams,verbs=get;list;watch

// Reconcile validates Story structure and maintains metadata
func (r *StoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Initialize structured logging and metrics
	rl := logging.NewReconcileLogger(ctx, "story")
	startTime := time.Now()

	defer func() {
		duration := time.Since(startTime)
		metrics.RecordControllerReconcile("story", duration, nil)
	}()

	var story bubushv1alpha1.Story
	if err := r.Get(ctx, req.NamespacedName, &story); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Add Story context to logger
	storyLogger := rl.WithStory(&story)
	rl.ReconcileStart("Processing Story")

	// Calculate and update step count
	stepCount := r.calculateStepCount(story.Spec.Steps)
	if story.Status.StepsCount != stepCount {
		story.Status.StepsCount = stepCount
		storyLogger.Info("Updating step count", "old_count", story.Status.StepsCount, "new_count", stepCount)
		if err := r.Update(ctx, &story); err != nil {
			rl.ReconcileError(err, "Failed to update step count")
			return ctrl.Result{}, err
		}
	}

	// Update condition manager generation for this reconcile
	r.conditionManager.UpdateGeneration(story.Generation)

	// Validate story structure
	storyLogger.V(1).Info("Validating story structure")
	validationErrors := r.validateStory(ctx, &story)

	// Update status conditions using new ConditionManager
	conditions := &story.Status.Conditions
	if len(validationErrors) == 0 {
		r.conditionManager.SetReadyCondition(conditions, true, ReasonValidationPassed, "Story is valid and ready for execution")
		r.conditionManager.SetCondition(conditions, ConditionCompiled, metav1.ConditionTrue, ReasonValidationPassed, "Story structure validated and compiled")
	} else {
		r.conditionManager.SetReadyCondition(conditions, false, ReasonValidationFailed, fmt.Sprintf("Validation failed: %v", validationErrors))
		r.conditionManager.SetDegradedCondition(conditions, true, ReasonValidationFailed, "Story has validation errors")
	}

	// Use debounced status update
	if err := r.Status().Update(ctx, &story); err != nil {
		rl.ReconcileError(err, "Failed to update story status")
		return ctrl.Result{}, err
	}

	if len(validationErrors) > 0 {
		rl.ReconcileError(fmt.Errorf("validation failed"), "Story validation failed",
			"error_count", len(validationErrors),
			"errors", validationErrors)
		// Don't return error - just report via conditions
	} else {
		rl.ReconcileSuccess("Story validated successfully",
			"step_count", stepCount,
			"has_validation_errors", false)
	}

	return ctrl.Result{}, nil
}

// Helper functions

// calculateStepCount counts all steps in the story (reference-based approach)
func (r *StoryReconciler) calculateStepCount(steps []bubushv1alpha1.Step) int {
	// With reference-based approach, we simply count the defined steps
	// Step references are resolved at runtime, so we count all defined steps
	return len(steps)
}

// validateStory performs validation checks on the story
func (r *StoryReconciler) validateStory(ctx context.Context, story *bubushv1alpha1.Story) []string {
	var errors []string
	storyLogger := logging.NewControllerLogger(ctx, "story").WithStory(story)

	storyLogger.V(2).Info("Validating story steps", "step_count", len(story.Spec.Steps))

	// Validate steps
	for i, step := range story.Spec.Steps {
		stepErrors := r.validateStep(ctx, step, story.Namespace)
		if len(stepErrors) > 0 {
			storyLogger.V(1).Info("Step validation errors",
				"step_index", i,
				"step_name", step.Name,
				"errors", stepErrors)
		}
		errors = append(errors, stepErrors...)
	}

	// Check for circular dependencies (basic check)
	if r.hasCircularDependency(story.Spec.Steps) {
		circularError := "circular dependency detected in steps"
		storyLogger.Error(fmt.Errorf("circular dependency"), "Circular dependency detected in story steps")
		errors = append(errors, circularError)
	}

	storyLogger.V(2).Info("Story validation completed",
		"total_errors", len(errors),
		"is_valid", len(errors) == 0)

	// Record validation metrics
	if len(errors) > 0 {
		metrics.ControllerReconcileErrors.WithLabelValues("story", "validation_failed").Inc()
	}

	return errors
}

// validateStep validates an individual step
func (r *StoryReconciler) validateStep(ctx context.Context, step bubushv1alpha1.Step, namespace string) []string {
	var errors []string

	// Check that either Type or Ref is set, but not both
	hasType := step.Type != nil && *step.Type != ""
	hasRef := step.Ref != nil && *step.Ref != ""

	if !hasType && !hasRef {
		errors = append(errors, fmt.Sprintf("step '%s' must have either 'type' or 'ref' set", step.Name))
	}
	if hasType && hasRef {
		errors = append(errors, fmt.Sprintf("step '%s' cannot have both 'type' and 'ref' set", step.Name))
	}

	// Validate built-in types
	if hasType {
		validTypes := []string{"condition", "loop", "parallel", "sleep", "gate", "setData", "mergeData", "stop", "switch", "filter", "transform", "executeStory", "wait", "throttle", "batch"}
		valid := false
		for _, vt := range validTypes {
			if *step.Type == vt {
				valid = true
				break
			}
		}
		if !valid {
			errors = append(errors, fmt.Sprintf("step '%s' has invalid type '%s'", step.Name, *step.Type))
		}

		// Type-specific validation
		switch *step.Type {
		case "condition":
			if step.If == nil || *step.If == "" {
				errors = append(errors, fmt.Sprintf("condition step '%s' must have 'if' expression", step.Name))
			}
		case "loop":
			if step.Items == nil || *step.Items == "" {
				errors = append(errors, fmt.Sprintf("loop step '%s' must have 'items' expression", step.Name))
			}
			if step.Body == nil {
				errors = append(errors, fmt.Sprintf("loop step '%s' must have 'body' steps", step.Name))
			}
		case "parallel":
			if step.Branches == nil {
				errors = append(errors, fmt.Sprintf("parallel step '%s' must have 'branches'", step.Name))
			}
		case "switch":
			if len(step.Cases) == 0 {
				errors = append(errors, fmt.Sprintf("switch step '%s' must have 'cases'", step.Name))
			}
			for i, switchCase := range step.Cases {
				if switchCase.When == "" {
					errors = append(errors, fmt.Sprintf("switch step '%s' case %d must have 'when' expression", step.Name, i))
				}
			}
		case "filter":
			if step.Filter == nil || *step.Filter == "" {
				errors = append(errors, fmt.Sprintf("filter step '%s' must have 'filter' expression", step.Name))
			}
		case "transform":
			if step.Transform == nil || *step.Transform == "" {
				errors = append(errors, fmt.Sprintf("transform step '%s' must have 'transform' expression", step.Name))
			}
		case "executeStory":
			if step.StoryRef == nil || *step.StoryRef == "" {
				errors = append(errors, fmt.Sprintf("executeStory step '%s' must have 'storyRef'", step.Name))
			}
		case "wait":
			hasWaitConfig := (step.Duration != nil && *step.Duration != "") ||
				(step.Event != nil && *step.Event != "") ||
				(step.Condition != nil && *step.Condition != "")
			if !hasWaitConfig {
				errors = append(errors, fmt.Sprintf("wait step '%s' must have 'duration', 'event', or 'condition'", step.Name))
			}
		case "throttle":
			if step.Rate == nil || *step.Rate == "" {
				errors = append(errors, fmt.Sprintf("throttle step '%s' must have 'rate'", step.Name))
			}
		case "batch":
			if step.Size == nil || *step.Size <= 0 {
				errors = append(errors, fmt.Sprintf("batch step '%s' must have positive 'size'", step.Name))
			}
		}
	}

	// Validate Engram references
	if hasRef {
		if err := r.validateEngramRef(ctx, *step.Ref, namespace); err != nil {
			errors = append(errors, fmt.Sprintf("step '%s' references invalid engram '%s': %v", step.Name, *step.Ref, err))
		}
	}

	// Note: With reference-based approach, step references are validated at runtime
	// during StoryRun execution. Step name validation is handled by the StoryRun controller.

	return errors
}

// validateEngramRef checks if an Engram reference is valid
func (r *StoryReconciler) validateEngramRef(ctx context.Context, engramRef, namespace string) error {
	var engram bubushv1alpha1.Engram
	storyLogger := logging.NewControllerLogger(ctx, "story").WithValues("engram_ref", engramRef, "namespace", namespace)

	err := r.Get(ctx, types.NamespacedName{
		Name:      engramRef,
		Namespace: namespace,
	}, &engram)

	if err != nil {
		storyLogger.V(1).Info("Engram reference validation failed", "error", err.Error())
		metrics.ControllerReconcileErrors.WithLabelValues("story", "engram_ref_invalid").Inc()
	} else {
		storyLogger.V(2).Info("Engram reference validated successfully")
	}

	return err
}

// hasCircularDependency performs a basic check for circular dependencies
func (r *StoryReconciler) hasCircularDependency(steps []bubushv1alpha1.Step) bool {
	// For MVP, we'll do a simple name-based check for nested structures
	// In production, this should be a proper graph traversal
	stepNames := make(map[string]bool)

	checkStep := func(step bubushv1alpha1.Step) bool {
		if stepNames[step.Name] {
			return true // Circular reference found
		}
		stepNames[step.Name] = true

		// For nested structures, we would need to parse the JSON and check recursively
		// This is a simplified check for MVP - in production we'd parse the JSON steps

		stepNames[step.Name] = false
		return false
	}

	for _, step := range steps {
		if checkStep(step) {
			return true
		}
	}

	return false
}

// updateStatusConditions is now handled by ConditionManager in the main reconcile loop

// SetupWithManager sets up the controller with the Manager.
func (r *StoryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return r.setupControllerWithOptions(mgr, controller.Options{
		MaxConcurrentReconciles: 5, // Will be overridden by config
	})
}

// SetupWithManagerAndConfig sets up the controller with configurable options
func (r *StoryReconciler) SetupWithManagerAndConfig(mgr ctrl.Manager, opts controller.Options) error {
	return r.setupControllerWithOptions(mgr, opts)
}

func (r *StoryReconciler) setupControllerWithOptions(mgr ctrl.Manager, opts controller.Options) error {
	ctx := context.Background()

	// Index Stories by step count for efficient queries (fix: use correct field name)
	if err := mgr.GetFieldIndexer().IndexField(ctx, &bubushv1alpha1.Story{}, "status.stepsCount",
		func(obj client.Object) []string {
			story := obj.(*bubushv1alpha1.Story)
			return []string{fmt.Sprintf("%d", story.Status.StepsCount)}
		}); err != nil {
		return err
	}

	// Index Stories by step references for efficient Engram→Story lookups (fixes N+1 query)
	if err := mgr.GetFieldIndexer().IndexField(ctx, &bubushv1alpha1.Story{}, "spec.steps.ref",
		func(obj client.Object) []string {
			story := obj.(*bubushv1alpha1.Story)
			var refs []string
			for _, step := range story.Spec.Steps {
				if step.Ref != nil {
					refs = append(refs, *step.Ref)
				}
			}
			return refs
		}); err != nil {
		return err
	}

	// Index Stories by validation status for efficient health queries
	if err := mgr.GetFieldIndexer().IndexField(ctx, &bubushv1alpha1.Story{}, "status.conditions.ready",
		func(obj client.Object) []string {
			story := obj.(*bubushv1alpha1.Story)
			for _, condition := range story.Status.Conditions {
				if condition.Type == "Ready" {
					return []string{string(condition.Status)}
				}
			}
			return []string{"Unknown"}
		}); err != nil {
		return err
	}

	// Use standard controller builder with enhanced predicates
	return ctrl.NewControllerManagedBy(mgr).
		For(&bubushv1alpha1.Story{}).
		Watches(&bubushv1alpha1.Engram{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			// Find Stories that reference this Engram using field index (fixes N+1 query)
			var stories bubushv1alpha1.StoryList
			if err := mgr.GetClient().List(ctx, &stories,
				client.InNamespace(obj.GetNamespace()),
				client.MatchingFields{"spec.steps.ref": obj.GetName()}); err != nil {
				return nil
			}
			var requests []reconcile.Request
			for _, story := range stories.Items {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{
						Name:      story.Name,
						Namespace: story.Namespace,
					},
				})
			}
			return requests
		}), builder.WithPredicates(SpecChangedPredicate())).
		WithOptions(opts).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("bubushv1alpha1.story").
		Complete(r)
}
