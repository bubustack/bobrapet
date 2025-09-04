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

package runs

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/cel"
	"github.com/bubustack/bobrapet/internal/config"
	parentcontroller "github.com/bubustack/bobrapet/internal/controller"
	"github.com/bubustack/bobrapet/internal/logging"
	"github.com/bubustack/bobrapet/internal/metrics"
)

// StoryRunReconciler reconciles a StoryRun object
type StoryRunReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	CELCache         *cel.CompilationCache
	conditionManager *parentcontroller.ConditionManager
	finalizerManager *parentcontroller.FinalizerManager
	// reconcilePattern removed
	referenceValidator *parentcontroller.ReferenceValidator
}

const (
	// StoryRunFinalizer is the finalizer name for StoryRun cleanup
	StoryRunFinalizer = "runs.bubu.sh/storyrun-cleanup"

	// MaxJSONSize limits the size of JSON payloads to prevent memory exhaustion
	MaxJSONSize = 1 * 1024 * 1024 // 1MB limit
	// MaxStepsPerLevel limits the number of steps at any nesting level
	MaxStepsPerLevel = 1000
)

// SpecChangedPredicate filters events to only reconcile on meaningful changes
// (generation, annotations, labels) and ignores status-only updates
func SpecChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true // Always process creates
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Reconcile on generation changes (spec updates)
			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}

			// Reconcile on annotation changes (for manual triggers, sleep timers, etc.)
			if !reflect.DeepEqual(e.ObjectOld.GetAnnotations(), e.ObjectNew.GetAnnotations()) {
				return true
			}

			// Reconcile on label changes (for selectors, tenant changes, etc.)
			if !reflect.DeepEqual(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels()) {
				return true
			}

			// Ignore status-only updates
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true // Always process deletes for finalizer handling
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return true // Process generic events (e.g., external triggers)
		},
	}
}

// Helper function to parse JSON step arrays from flexible schema
func (r *StoryRunReconciler) parseStepsFromJSON(jsonSteps *apiextensionsv1.JSON) ([]bubushv1alpha1.Step, error) {
	if jsonSteps == nil {
		return nil, nil
	}

	// Check size before parsing to prevent memory exhaustion
	if len(jsonSteps.Raw) > MaxJSONSize {
		return nil, fmt.Errorf("JSON payload too large: %d bytes > %d limit", len(jsonSteps.Raw), MaxJSONSize)
	}

	var steps []bubushv1alpha1.Step
	if err := json.Unmarshal(jsonSteps.Raw, &steps); err != nil {
		return nil, fmt.Errorf("failed to parse steps from JSON: %w", err)
	}

	// Validate step count to prevent resource exhaustion
	if len(steps) > MaxStepsPerLevel {
		return nil, fmt.Errorf("too many steps: %d > %d limit", len(steps), MaxStepsPerLevel)
	}

	return steps, nil
}

// RBAC
// +kubebuilder:rbac:groups=runs.bubu.sh,resources=storyruns,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=runs.bubu.sh,resources=storyruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=runs.bubu.sh,resources=stepruns,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=bubu.sh,resources=stories,verbs=get;list;watch
// +kubebuilder:rbac:groups=bubu.sh,resources=engrams,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *StoryRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Initialize structured logging and metrics
	rl := logging.NewReconcileLogger(ctx, "storyrun")
	startTime := time.Now()

	if config.IsTelemetryEnabled() {
		var spanCtx context.Context
		var span oteltrace.Span
		spanCtx, span = otel.Tracer("bobrapet/controller/storyrun").Start(ctx, "Reconcile StoryRun")
		ctx = spanCtx
		span.SetAttributes(
			attribute.String("resource", "StoryRun"),
			attribute.String("name", req.Name),
			attribute.String("namespace", req.Namespace),
		)
		defer span.End()
	}

	defer func() {
		duration := time.Since(startTime)
		metrics.RecordControllerReconcile("storyrun", duration, nil)
	}()

	var srun runsv1alpha1.StoryRun
	if err := r.Get(ctx, req.NamespacedName, &srun); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Add StoryRun context to logger
	storyLogger := rl.WithStoryRun(&srun)
	if config.IsTelemetryEnabled() {
		oteltrace.SpanFromContext(ctx).SetAttributes(
			attribute.String("story", srun.Spec.StoryRef),
			attribute.String("namespace", srun.Namespace),
			attribute.String("name", srun.Name),
			attribute.String("phase", srun.Status.Phase),
		)
	}
	rl.ReconcileStart("Processing StoryRun")

	// Initialize managers if needed
	if r.conditionManager == nil {
		r.conditionManager = parentcontroller.NewConditionManager(srun.Generation)
	}
	if r.finalizerManager == nil {
		r.finalizerManager = parentcontroller.NewFinalizerManager(r.Client)
	}
	// ReconcilePattern removed - using direct reconcile logic
	if r.referenceValidator == nil {
		r.referenceValidator = parentcontroller.NewReferenceValidator(r.Client)
	}

	// Add finalizer if missing
	if !r.finalizerManager.HasFinalizer(&srun, parentcontroller.StoryRunFinalizer) {
		r.finalizerManager.AddFinalizer(&srun, parentcontroller.StoryRunFinalizer)
		return ctrl.Result{}, r.Update(ctx, &srun)
	}

	// Handle deletion
	if srun.DeletionTimestamp != nil {
		conditions := &srun.Status.Conditions
		r.conditionManager.SetTerminatingCondition(conditions, true, parentcontroller.ReasonDeletionRequested, "StoryRun is being deleted")
		return r.handleCleanup(ctx, &srun)
	}

	// Terminal?
	storyLogger.Info("Checking terminal phase", "current_phase", srun.Status.Phase)
	if srun.Status.Phase == "Succeeded" || srun.Status.Phase == "Failed" || srun.Status.Phase == "Canceled" {
		rl.ReconcileSuccess("StoryRun in terminal phase")
		// Record final metrics
		if srun.Status.StartedAt != nil && srun.Status.FinishedAt != nil {
			start, _ := time.Parse(time.RFC3339, srun.Status.StartedAt.Format(time.RFC3339))
			end, _ := time.Parse(time.RFC3339, srun.Status.FinishedAt.Format(time.RFC3339))
			duration := end.Sub(start)
			metrics.RecordStoryRunMetrics(srun.Namespace, srun.Spec.StoryRef, srun.Status.Phase, duration)
		}
		return ctrl.Result{}, nil
	}

	// Load Story
	storyLogger.Info("Loading Story", "storyRef", srun.Spec.StoryRef)
	var story bubushv1alpha1.Story
	if err := r.Get(ctx, types.NamespacedName{Name: srun.Spec.StoryRef, Namespace: srun.Namespace}, &story); err != nil {
		srun.Status.Phase = "Failed"
		srun.Status.Message = fmt.Sprintf("story not found: %v", err)
		rl.ReconcileError(err, "Failed to load Story")
		metrics.RecordStoryRunMetrics(srun.Namespace, srun.Spec.StoryRef, "Failed", 0)
		_ = r.Status().Update(ctx, &srun)
		return ctrl.Result{}, nil
	}
	storyLogger.Info("Story loaded successfully", "steps_count", len(story.Spec.Steps))

	// Init
	if srun.Status.Phase == "" {
		srun.Status.Phase = "Running"
		if srun.Status.StartedAt == nil {
			now := metav1.NewTime(time.Now())
			srun.Status.StartedAt = &now
		}
		storyLogger.Info("Initializing StoryRun execution")
	}

	// Snapshot status for efficient comparison
	oldStatus := srun.Status.DeepCopy()

	completed := set(srun.Status.Completed)
	running := set(srun.Status.Active)

	// Update metrics gauges
	metrics.UpdateStoryRunStepsGauge(srun.Namespace, srun.Spec.StoryRef, len(running), len(completed))

	// ALWAYS check status of active steps first to sync with StepRun states
	storyLogger.Info("Updating active steps status", "active_count", len(srun.Status.Active))
	modified, err := r.updateActiveStepsStatus(ctx, &srun)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Recalculate after status update
	completed = set(srun.Status.Completed)
	running = set(srun.Status.Active)
	storyLogger.Info("Status after update", "completed_count", len(completed), "running_count", len(running))

	// Find all ready steps (dependencies satisfied, not completed, not running)
	storyLogger.Info("Finding ready steps", "total_steps", len(story.Spec.Steps))
	readySteps := r.findReadySteps(story.Spec.Steps, completed, running)
	storyLogger.Info("Ready steps found", "ready_count", len(readySteps))

	if len(readySteps) == 0 {
		// Check if we're done or stuck
		if len(completed) == len(story.Spec.Steps) {
			srun.Status.Phase = "Succeeded"
			srun.Status.Message = "all steps completed"
			finishedAt := metav1.NewTime(time.Now())
			srun.Status.FinishedAt = &finishedAt
			rl.ReconcileSuccess("All steps completed successfully",
				"total_steps", len(story.Spec.Steps),
				"completed_steps", len(completed))
		} else if len(running) == 0 {
			srun.Status.Phase = "Failed"
			srun.Status.Message = "workflow stuck - no steps can proceed"
			finishedAt := metav1.NewTime(time.Now())
			srun.Status.FinishedAt = &finishedAt
			rl.ReconcileError(fmt.Errorf("workflow stuck"), "No steps can proceed",
				"total_steps", len(story.Spec.Steps),
				"completed_steps", len(completed),
				"running_steps", len(running))
		}
		// Update status with retry
		if err := r.updateStatusWithRetry(ctx, &srun, 3); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		// Only requeue for waiting states
		if len(running) > 0 {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, nil
	}

	// Execute all ready steps in parallel
	storyLogger.Info("Ready steps check", "ready_steps_count", len(readySteps), "completed_count", len(completed), "running_count", len(running))
	if len(readySteps) > 0 {
		storyLogger.Info("Executing ready steps", "ready_steps", len(readySteps))
		for _, step := range readySteps {
			storyLogger.Info("Executing step", "step", step.Name, "type", getStepType(step))
			if err := r.executeStep(ctx, &srun, step); err != nil {
				rl.ReconcileError(err, "Failed to execute step", "step", step.Name)
				return ctrl.Result{}, err
			}
		}
	}

	// Batched status update if changed
	if r.hasStatusChanged(*oldStatus, srun.Status) || modified {
		if err := r.updateStatusWithRetry(ctx, &srun, 3); err != nil {
			return ctrl.Result{RequeueAfter: time.Duration(2<<2) * time.Second}, err
		}
	}

	// Only requeue when necessary (use current running state after updates)
	currentRunning := set(srun.Status.Active)
	if len(readySteps) == 0 && len(currentRunning) > 0 {
		// If any sleep step is pending, compute shortest wake time
		if srun.Annotations != nil {
			var nextWake *time.Time
			for k, v := range srun.Annotations {
				if strings.HasPrefix(k, "runs.bubu.sh/sleep.") && strings.HasSuffix(k, ".until") {
					if t, err := time.Parse(time.RFC3339, v); err == nil {
						if nextWake == nil || t.Before(*nextWake) {
							nextWake = &t
						}
					}
				}
			}
			if nextWake != nil {
				now := time.Now()
				if nextWake.After(now) {
					return ctrl.Result{RequeueAfter: nextWake.Sub(now)}, nil
				}
				return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
			}
		}
		// Only requeue if waiting for running steps - longer interval
		rl.ReconcileRequeue("Waiting for active steps to complete", 30*time.Second,
			"active_steps", len(currentRunning))
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	// No automatic requeue - rely on events from StepRun changes
	rl.ReconcileSuccess("Reconciliation completed", "ready_steps_executed", len(readySteps))
	return ctrl.Result{}, nil
}

// findReadySteps returns steps that are ready to execute (dependencies satisfied)
func (r *StoryRunReconciler) findReadySteps(steps []bubushv1alpha1.Step, completed, running map[string]bool) []*bubushv1alpha1.Step {
	var ready []*bubushv1alpha1.Step

	for i := range steps {
		step := &steps[i]

		// Skip if already completed or running
		if completed[step.Name] || running[step.Name] {
			continue
		}

		// Check if all dependencies are satisfied
		dependenciesMet := true
		for _, dep := range step.Needs {
			if !completed[dep] {
				dependenciesMet = false
				break
			}
		}

		if dependenciesMet {
			ready = append(ready, step)
		}
	}

	return ready
}

// executeStep executes a single step (built-in or external)
func (r *StoryRunReconciler) executeStep(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubushv1alpha1.Step) error {
	// Add to active list
	srun.Status.Active = append(srun.Status.Active, step.Name)

	// Handle built-in steps
	if step.Type != nil {
		return r.executeBuiltinStep(ctx, srun, step)
	}

	// Handle external engram steps
	if step.Ref != nil {
		return r.executeEngramStep(ctx, srun, step)
	}

	return fmt.Errorf("step %s has neither type nor ref", step.Name)
}

// executeBuiltinStep handles built-in step types
func (r *StoryRunReconciler) executeBuiltinStep(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubushv1alpha1.Step) error {
	// For built-ins that complete immediately (sleep, stop, etc.)
	switch *step.Type {
	case "sleep":
		// Non-blocking sleep using annotation and controller requeue
		var d time.Duration = 2 * time.Second
		if step.With != nil && len(step.With.Raw) > 0 {
			var cfg struct {
				Duration string `json:"duration"`
			}
			if err := json.Unmarshal(step.With.Raw, &cfg); err == nil {
				if parsed, err2 := time.ParseDuration(cfg.Duration); err2 == nil {
					d = parsed
				}
			}
		}
		if srun.Annotations == nil {
			srun.Annotations = map[string]string{}
		}
		key := fmt.Sprintf("runs.bubu.sh/sleep.%s.until", step.Name)
		if val, ok := srun.Annotations[key]; ok {
			if t, err := time.Parse(time.RFC3339, val); err == nil {
				if time.Now().After(t) || time.Now().Equal(t) {
					// Wake up
					delete(srun.Annotations, key)
					r.markStepCompleted(srun, step.Name)
					return nil
				}
				// Still sleeping; remain active
				return nil
			}
		}
		// Set wake time annotation and persist to trigger requeue
		wake := time.Now().Add(d).Format(time.RFC3339)
		srun.Annotations[key] = wake
		if err := r.Update(ctx, srun); err != nil {
			return err
		}
		return nil
	case "stop":
		mode := "return"
		if step.Mode != nil {
			mode = *step.Mode
		}
		switch mode {
		case "fail":
			srun.Status.Phase = "Failed"
		case "cancel":
			srun.Status.Phase = "Canceled"
		default:
			srun.Status.Phase = "Succeeded"
		}
		r.markStepCompleted(srun, step.Name)
		return nil
	default:
		// For complex built-ins (condition, loop, parallel), handle separately
		return r.executeComplexBuiltin(ctx, srun, step)
	}
}

// executeEngramStep handles external engram execution
func (r *StoryRunReconciler) executeEngramStep(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubushv1alpha1.Step) error {
	stepName := fmt.Sprintf("%s-%s", srun.Name, step.Name)

	// Render templated input if present
	var renderedInput *runtime.RawExtension
	if step.With != nil {
		ri, err := r.renderTemplates(ctx, srun, step.With, nil)
		if err != nil {
			return fmt.Errorf("failed to render templates for step %s: %w", step.Name, err)
		}
		renderedInput = ri
	}

	// Create StepRun if it doesn't exist
	var stepRun runsv1alpha1.StepRun
	if err := r.Get(ctx, types.NamespacedName{Name: stepName, Namespace: srun.Namespace}, &stepRun); err != nil {
		trueVar := true
		stepRun = runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      stepName,
				Namespace: srun.Namespace,
				Labels: map[string]string{
					"storyrun": srun.Name,
					"step":     step.Name,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: srun.APIVersion,
					Kind:       srun.Kind,
					Name:       srun.Name,
					UID:        srun.UID,
					Controller: &trueVar,
				}},
			},
			Spec: runsv1alpha1.StepRunSpec{
				StoryRunRef:      srun.Name,
				StepID:           step.Name,
				EngramRef:        *step.Ref,
				Input:            renderedInput,
				IdempotencyToken: generateIdempotencyToken(srun.Name, step.Name),
			},
		}
		return r.Create(ctx, &stepRun)
	}

	return nil
}

// batchCreateStepRuns creates multiple StepRuns efficiently
func (r *StoryRunReconciler) batchCreateStepRuns(ctx context.Context, srun *runsv1alpha1.StoryRun, steps []*bubushv1alpha1.Step) error {
	var stepRuns []runsv1alpha1.StepRun

	// Prepare all StepRuns
	for _, step := range steps {
		if step.Ref == nil {
			continue // Skip built-in steps
		}

		stepName := fmt.Sprintf("%s-%s", srun.Name, step.Name)
		stepRun := runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{Name: stepName, Namespace: srun.Namespace},
			Spec: runsv1alpha1.StepRunSpec{
				StoryRunRef: srun.Name,
				StepID:      step.Name,
				EngramRef:   *step.Ref,
				Input:       step.With,

				IdempotencyToken: generateIdempotencyToken(srun.Name, step.Name),
			},
		}
		stepRuns = append(stepRuns, stepRun)
	}

	// Batch create all StepRuns
	for _, stepRun := range stepRuns {
		if err := r.Create(ctx, &stepRun); err != nil {
			return fmt.Errorf("failed to create StepRun %s: %w", stepRun.Name, err)
		}
	}

	return nil
}

// executeComplexBuiltin handles complex built-in steps (condition, loop, parallel, etc.)
func (r *StoryRunReconciler) executeComplexBuiltin(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubushv1alpha1.Step) error {
	switch *step.Type {
	case "condition":
		if step.If == nil {
			r.markStepCompleted(srun, step.Name)
			return nil
		}
		ok, err := r.evaluateConditionExpression(ctx, srun, *step.If, srun.Spec.Inputs)
		if err != nil {
			return err
		}
		var next []bubushv1alpha1.Step
		if ok {
			if thenSteps, err := r.parseStepsFromJSON(step.Then); err == nil {
				next = thenSteps
			}
		} else {
			if elseSteps, err := r.parseStepsFromJSON(step.Else); err == nil {
				next = elseSteps
			}
		}
		for i := range next {
			child := next[i]
			child.Name = fmt.Sprintf("%s-%s", step.Name, child.Name)
			if child.Type != nil {
				if err := r.executeBuiltinStep(ctx, srun, &child); err != nil {
					return err
				}
			} else if child.Ref != nil {
				if err := r.executeEngramStep(ctx, srun, &child); err != nil {
					return err
				}
			}
		}
		r.markStepCompleted(srun, step.Name)
		return nil
	case "switch":
		for _, c := range step.Cases {
			ok, err := r.evaluateConditionExpression(ctx, srun, c.When, srun.Spec.Inputs)
			if err != nil {
				return err
			}
			if ok {
				steps, err := r.parseStepsFromJSON(c.Steps)
				if err != nil {
					return err
				}
				for i := range steps {
					child := steps[i]
					child.Name = fmt.Sprintf("%s-%s", step.Name, child.Name)
					if child.Type != nil {
						if err := r.executeBuiltinStep(ctx, srun, &child); err != nil {
							return err
						}
					} else if child.Ref != nil {
						if err := r.executeEngramStep(ctx, srun, &child); err != nil {
							return err
						}
					}
				}
				r.markStepCompleted(srun, step.Name)
				return nil
			}
		}
		if step.Default != nil {
			steps, err := r.parseStepsFromJSON(step.Default)
			if err != nil {
				return err
			}
			for i := range steps {
				child := steps[i]
				child.Name = fmt.Sprintf("%s-%s", step.Name, child.Name)
				if child.Type != nil {
					if err := r.executeBuiltinStep(ctx, srun, &child); err != nil {
						return err
					}
				} else if child.Ref != nil {
					if err := r.executeEngramStep(ctx, srun, &child); err != nil {
						return err
					}
				}
			}
		}
		r.markStepCompleted(srun, step.Name)
		return nil
	case "parallel":
		branches, err := r.parseStepsFromJSON(step.Branches)
		if err == nil {
			for i := range branches {
				child := branches[i]
				child.Name = fmt.Sprintf("%s-%s", step.Name, child.Name)
				if child.Type != nil {
					_ = r.executeBuiltinStep(ctx, srun, &child)
				} else if child.Ref != nil {
					_ = r.executeEngramStep(ctx, srun, &child)
				}
			}
		}
		r.markStepCompleted(srun, step.Name)
		return nil
	case "loop":
		return r.executeLoopStep(ctx, srun, step)
	case "gate":
		if srun.Annotations == nil {
			srun.Annotations = map[string]string{}
		}
		key := fmt.Sprintf("runs.bubu.sh/gate.%s", step.Name)
		val := srun.Annotations[key]
		if val == "approved" {
			// If already approved, complete immediately
			r.markStepCompleted(srun, step.Name)
			return nil
		}
		if val == "denied" {
			r.markStepFailed(srun, step.Name, "gate denied")
			return nil
		}
		// Set to waiting and persist metadata update
		srun.Annotations[key] = "waiting"
		if err := r.Update(ctx, srun); err != nil {
			return err
		}
		// Remain active until annotation is updated to approved/denied
		return nil
	default:
		return fmt.Errorf("complex builtin %s not implemented", *step.Type)
	}
}

// updateActiveStepsStatus checks status of all active steps efficiently
func (r *StoryRunReconciler) updateActiveStepsStatus(ctx context.Context, srun *runsv1alpha1.StoryRun) (bool, error) {
	var needsUpdate bool

	storyLogger := logging.NewControllerLogger(ctx, "storyrun")

	// Handle gate approvals/denials via annotations
	for _, stepName := range srun.Status.Active {
		if srun.Annotations != nil {
			key := fmt.Sprintf("runs.bubu.sh/gate.%s", stepName)
			if val, ok := srun.Annotations[key]; ok {
				if val == "approved" {
					storyLogger.Info("Gate approved", "step", stepName)
					r.markStepCompleted(srun, stepName)
					needsUpdate = true
					continue
				}
				if val == "denied" {
					storyLogger.Info("Gate denied", "step", stepName)
					r.markStepFailed(srun, stepName, "gate denied")
					needsUpdate = true
					continue
				}
			}
		}
	}

	// Check each active step with backing StepRun
	for _, stepName := range srun.Status.Active {
		stepRunName := fmt.Sprintf("%s-%s", srun.Name, stepName)
		var stepRun runsv1alpha1.StepRun
		if err := r.Get(ctx, types.NamespacedName{Name: stepRunName, Namespace: srun.Namespace}, &stepRun); err != nil {
			storyLogger.V(1).Info("StepRun not found", "step", stepName, "stepRunName", stepRunName, "error", err)
			continue // StepRun not found, might still be creating or builtin
		}
		storyLogger.Info("Found StepRun", "step", stepName, "phase", stepRun.Status.Phase)
		if stepRun.Status.Phase == "Succeeded" {
			storyLogger.Info("Marking step completed", "step", stepName)
			r.markStepCompleted(srun, stepName)
			needsUpdate = true
		} else if stepRun.Status.Phase == "Failed" {
			storyLogger.Info("Marking step failed", "step", stepName)
			r.markStepFailed(srun, stepName, "StepRun failed")
			needsUpdate = true
		}
	}

	storyLogger.Info("Active steps status update completed", "needsUpdate", needsUpdate, "activeSteps", len(srun.Status.Active))
	return needsUpdate, nil
}

// markStepCompleted moves step from active to completed
func (r *StoryRunReconciler) markStepCompleted(srun *runsv1alpha1.StoryRun, stepName string) {
	// Remove from active
	active := []string{}
	for _, name := range srun.Status.Active {
		if name != stepName {
			active = append(active, name)
		}
	}
	srun.Status.Active = active

	// Add to completed
	srun.Status.Completed = append(srun.Status.Completed, stepName)
}

// markStepFailed handles step failure
func (r *StoryRunReconciler) markStepFailed(srun *runsv1alpha1.StoryRun, stepName, reason string) {
	// Remove from active
	active := []string{}
	for _, name := range srun.Status.Active {
		if name != stepName {
			active = append(active, name)
		}
	}
	srun.Status.Active = active

	// Mark workflow as failed
	srun.Status.Phase = "Failed"
	srun.Status.Message = fmt.Sprintf("step %s failed: %s", stepName, reason)
}

// clearPVClaimRef clears the claimRef from a PersistentVolume to make it available for reuse
func (r *StoryRunReconciler) clearPVClaimRef(ctx context.Context, pvName string) error {
	var pv corev1.PersistentVolume
	if err := r.Get(ctx, client.ObjectKey{Name: pvName}, &pv); err != nil {
		if errors.IsNotFound(err) {
			// PV doesn't exist, nothing to clear
			return nil
		}
		return fmt.Errorf("failed to get PV %s: %w", pvName, err)
	}

	// Only clear claimRef if PV is in Released state
	if pv.Status.Phase != corev1.VolumeReleased {
		return nil
	}

	// Clear the claimRef
	patch := []byte(`{"spec":{"claimRef":null}}`)
	if err := r.Patch(ctx, &pv, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("failed to clear claimRef for PV %s: %w", pvName, err)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StoryRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return r.setupControllerWithOptions(mgr, controller.Options{
		MaxConcurrentReconciles: 8, // Will be overridden by config
	})
}

// SetupWithManagerAndConfig sets up the controller with configurable options
func (r *StoryRunReconciler) SetupWithManagerAndConfig(mgr ctrl.Manager, opts controller.Options) error {
	ctx := context.Background()

	// Index StoryRuns by referenced Story for efficient reverse lookups
	if err := mgr.GetFieldIndexer().IndexField(ctx, &runsv1alpha1.StoryRun{}, "spec.storyRef",
		func(obj client.Object) []string {
			return []string{obj.(*runsv1alpha1.StoryRun).Spec.StoryRef}
		}); err != nil {
		return err
	}

	// Index StepRuns by StoryRun reference for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(ctx, &runsv1alpha1.StepRun{}, "spec.storyRunRef",
		func(obj client.Object) []string {
			return []string{obj.(*runsv1alpha1.StepRun).Spec.StoryRunRef}
		}); err != nil {
		return err
	}

	// Index StepRuns by phase for efficient status queries
	if err := mgr.GetFieldIndexer().IndexField(ctx, &runsv1alpha1.StepRun{}, "status.phase",
		func(obj client.Object) []string {
			return []string{obj.(*runsv1alpha1.StepRun).Status.Phase}
		}); err != nil {
		return err
	}

	// Index StepRuns by Engram reference for impact analysis
	if err := mgr.GetFieldIndexer().IndexField(ctx, &runsv1alpha1.StepRun{}, "spec.engramRef",
		func(obj client.Object) []string {
			ref := obj.(*runsv1alpha1.StepRun).Spec.EngramRef
			if ref == "" {
				return []string{}
			}
			return []string{ref}
		}); err != nil {
		return err
	}

	return r.setupControllerWithOptions(mgr, opts)
}

func (r *StoryRunReconciler) setupControllerWithOptions(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StoryRun{}).
		Owns(&runsv1alpha1.StepRun{}). // Watch StepRun changes (event-driven)
		WithOptions(opts).
		WithEventFilter(predicate.Or(
			SpecChangedPredicate(),
			// Process child StepRun changes to update StoryRun status
			predicate.Funcs{
				UpdateFunc: func(e event.UpdateEvent) bool {
					// Always process updates to catch status changes from StepRuns
					return true
				},
				DeleteFunc: func(e event.DeleteEvent) bool {
					return true // Process deletion for finalizer cleanup
				},
				CreateFunc: func(e event.CreateEvent) bool {
					return true // Process all creates
				},
			},
		)).
		Named("runs-storyrun").
		Complete(r)
}

// Helper functions
func set(ss []string) map[string]bool {
	m := map[string]bool{}
	for _, s := range ss {
		m[s] = true
	}
	return m
}

func jobForStep(step runsv1alpha1.StepRun, image string) *batchv1.Job {
	var inputBytes []byte
	if step.Spec.Input != nil {
		inputBytes = step.Spec.Input.Raw
	} else {
		inputBytes = []byte("{}")
	}
	backoff := int32(0)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      step.Name + "-job",
			Namespace: step.Namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "engram",
						Image: image,
						Env: []corev1.EnvVar{
							{Name: "ENGRAM_INPUT", Value: string(inputBytes)},
							{Name: "ENGRAM_RUN_ID", Value: step.Name},
							{Name: "ENGRAM_NAMESPACE", Value: step.Namespace},
							{Name: "ENGRAM_DEADLINE", Value: step.Spec.Deadline},
							{Name: "ENGRAM_IDEMPOTENCY_TOKEN", Value: step.Spec.IdempotencyToken},
						},
						SecurityContext: &corev1.SecurityContext{
							RunAsNonRoot:             &[]bool{true}[0],
							AllowPrivilegeEscalation: &[]bool{false}[0],
							ReadOnlyRootFilesystem:   &[]bool{true}[0],
							Capabilities: &corev1.Capabilities{
								Drop: []corev1.Capability{"ALL"},
							},
						},
					}},
				},
			},
		},
	}
}

// Helper functions for ABI contract
func generateIdempotencyToken(storyRunName, stepName string) string {
	data := fmt.Sprintf("%s-%s", storyRunName, stepName)
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash)[:16] // Use first 16 chars of hash
}

func (r *StoryRunReconciler) resolveImage(ctx context.Context, namespace, engramRef, fallback string) string {
	if fallback != "" {
		return fallback
	}
	if engramRef == "" {
		return "ghcr.io/bubustack/engram-default:latest"
	}
	var e bubushv1alpha1.Engram
	if err := r.Get(ctx, types.NamespacedName{Name: engramRef, Namespace: namespace}, &e); err == nil {
		if e.Spec.Engine != nil && e.Spec.Engine.TemplateRef != "" {
			// Resolve from EngramTemplate when available; fallback to convention
			return "ghcr.io/bubustack/" + e.Spec.Engine.TemplateRef + ":latest"
		}
	}
	return "ghcr.io/bubustack/engram-default:latest"
}

// evaluateConditionExpression evaluates a CEL expression against StoryRun inputs
func (r *StoryRunReconciler) evaluateConditionExpression(ctx context.Context, srun *runsv1alpha1.StoryRun, expression string, inputs *runtime.RawExtension) (bool, error) {
	expr := r.preprocessExpression(expression)
	vars := r.buildCELVars(ctx, srun, inputs, nil)

	// Use cached evaluation if cache is available
	if r.CELCache != nil {
		return r.CELCache.EvaluateCondition(ctx, expr, vars)
	}

	// Fallback to direct evaluation if no cache
	env, err := EnhancedCELEnvironment()
	if err != nil {
		return false, err
	}
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return false, issues.Err()
	}
	prg, err := env.Program(ast)
	if err != nil {
		return false, err
	}
	out, _, err := prg.Eval(vars)
	if err != nil {
		return false, err
	}
	if b, ok := out.Value().(bool); ok {
		return b, nil
	}
	return false, fmt.Errorf("expression did not evaluate to boolean")
}

// evaluateTransformExpression evaluates a CEL expression and returns the result
func (r *StoryRunReconciler) evaluateTransformExpression(ctx context.Context, srun *runsv1alpha1.StoryRun, expression string, inputs *runtime.RawExtension) (interface{}, error) {
	expr := r.preprocessExpression(expression)
	vars := r.buildCELVars(ctx, srun, inputs, nil)

	// Use cached evaluation if cache is available
	if r.CELCache != nil {
		return r.CELCache.EvaluateTransform(ctx, expr, vars)
	}

	// Fallback to direct evaluation if no cache
	env, err := EnhancedCELEnvironment()
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	out, _, err := prg.Eval(vars)
	if err != nil {
		return nil, err
	}
	return out.Value(), nil
}

// buildStepOutputsContext creates a context with all completed step outputs for CEL expressions
func (r *StoryRunReconciler) buildStepOutputsContext(ctx context.Context, srun *runsv1alpha1.StoryRun) map[string]interface{} {
	outputs := make(map[string]interface{})

	// Get all StepRuns for this StoryRun to access their outputs
	var stepRuns runsv1alpha1.StepRunList
	if err := r.List(ctx, &stepRuns, client.InNamespace(srun.Namespace),
		client.MatchingFields{"spec.storyRunRef": srun.Name}); err != nil {
		return outputs
	}

	// Build outputs map from completed StepRuns
	for _, stepRun := range stepRuns.Items {
		if stepRun.Status.Phase == "Succeeded" && stepRun.Status.Output != nil {
			var output map[string]interface{}
			if err := json.Unmarshal(stepRun.Status.Output.Raw, &output); err == nil {
				outputs[stepRun.Name] = map[string]interface{}{
					"output": output,
					"status": stepRun.Status.Phase,
				}
			}
		}
	}

	return outputs
}

// resolveItemsFromInputs is a minimal MVP helper to support items: "{{ inputs.emails }}" -> []interface{}
func resolveItemsFromInputs(step *bubushv1alpha1.Step, inputs *runtime.RawExtension) ([]interface{}, error) {
	if step.Items == nil {
		return nil, fmt.Errorf("no items expression")
	}
	expr := strings.TrimSpace(*step.Items)
	if strings.HasPrefix(expr, "{{") && strings.HasSuffix(expr, "}}") {
		expr = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(expr, "{{"), "}}"))
	}
	// Only support inputs.<field> for MVP
	if !strings.HasPrefix(expr, "inputs.") {
		return nil, fmt.Errorf("unsupported items expression: %s", expr)
	}
	var in map[string]interface{}
	if inputs != nil && len(inputs.Raw) > 0 {
		if err := json.Unmarshal(inputs.Raw, &in); err != nil {
			return nil, err
		}
	}
	if in == nil {
		in = map[string]interface{}{}
	}
	key := strings.TrimPrefix(expr, "inputs.")
	val, ok := in[key]
	if !ok {
		return nil, fmt.Errorf("inputs key not found: %s", key)
	}
	switch v := val.(type) {
	case []interface{}:
		return v, nil
	default:
		return nil, fmt.Errorf("items must resolve to array, got %T", v)
	}
}

func (r *StoryRunReconciler) preprocessExpression(expression string) string {
	expr := strings.TrimSpace(expression)
	// strip optional {{ }} or ${}
	if (strings.HasPrefix(expr, "{{") && strings.HasSuffix(expr, "}}")) ||
		(strings.HasPrefix(expr, "${") && strings.HasSuffix(expr, "}")) {
		expr = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(expr, "${"), "{{"), "}}"))
	}
	// allow starting dot like .inputs.foo
	if strings.HasPrefix(expr, ".inputs") || strings.HasPrefix(expr, ".steps") || strings.HasPrefix(expr, ".item") {
		expr = strings.TrimPrefix(expr, ".")
	}
	return expr
}

func (r *StoryRunReconciler) buildCELVars(ctx context.Context, srun *runsv1alpha1.StoryRun, inputs *runtime.RawExtension, extra map[string]interface{}) map[string]interface{} {
	var in map[string]interface{}
	if inputs != nil && len(inputs.Raw) > 0 {
		_ = json.Unmarshal(inputs.Raw, &in)
	}
	if in == nil {
		in = map[string]interface{}{}
	}
	stepOutputs := r.buildStepOutputsContext(ctx, srun)
	vars := map[string]interface{}{
		"inputs": in,
		"steps":  stepOutputs,
		"now":    time.Now(),
		"run": map[string]interface{}{
			"name":      srun.Name,
			"namespace": srun.Namespace,
			"startedAt": srun.Status.StartedAt,
		},
	}
	for k, v := range extra {
		vars[k] = v
	}
	return vars
}

func (r *StoryRunReconciler) renderTemplates(ctx context.Context, srun *runsv1alpha1.StoryRun, raw *runtime.RawExtension, extra map[string]interface{}) (*runtime.RawExtension, error) {
	if raw == nil || len(raw.Raw) == 0 {
		return raw, nil
	}
	var data interface{}
	if err := json.Unmarshal(raw.Raw, &data); err != nil {
		return nil, err
	}
	rendered, err := r.renderValue(ctx, srun, data, extra)
	if err != nil {
		return nil, err
	}
	bytes, err := json.Marshal(rendered)
	if err != nil {
		return nil, err
	}
	return &runtime.RawExtension{Raw: bytes}, nil
}

func (r *StoryRunReconciler) renderValue(ctx context.Context, srun *runsv1alpha1.StoryRun, v interface{}, extra map[string]interface{}) (interface{}, error) {
	switch t := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(t))
		for k, val := range t {
			rv, err := r.renderValue(ctx, srun, val, extra)
			if err != nil {
				return nil, err
			}
			out[k] = rv
		}
		return out, nil
	case []interface{}:
		arr := make([]interface{}, 0, len(t))
		for _, val := range t {
			rv, err := r.renderValue(ctx, srun, val, extra)
			if err != nil {
				return nil, err
			}
			arr = append(arr, rv)
		}
		return arr, nil
	case string:
		s := strings.TrimSpace(t)
		if (strings.HasPrefix(s, "{{") && strings.HasSuffix(s, "}}")) || (strings.HasPrefix(s, "${") && strings.HasSuffix(s, "}")) {
			res, err := r.evaluateTemplateString(ctx, srun, s, srun.Spec.Inputs, extra)
			if err != nil {
				return nil, err
			}
			return res, nil
		}
		return t, nil
	default:
		return v, nil
	}
}

func (r *StoryRunReconciler) evaluateTemplateString(ctx context.Context, srun *runsv1alpha1.StoryRun, expression string, inputs *runtime.RawExtension, extra map[string]interface{}) (interface{}, error) {
	expr := r.preprocessExpression(expression)
	vars := r.buildCELVars(ctx, srun, inputs, extra)

	// Use cached evaluation if cache is available
	if r.CELCache != nil {
		return r.CELCache.Evaluate(ctx, expr, "template", vars)
	}

	// Fallback to direct evaluation if no cache
	env, err := EnhancedCELEnvironment()
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	out, _, err := prg.Eval(vars)
	if err != nil {
		return nil, err
	}
	return out.Value(), nil
}

// ✅ PERFORMANCE FIX: Proper error handling with retry logic
func (r *StoryRunReconciler) updateStatusWithRetry(ctx context.Context, srun *runsv1alpha1.StoryRun, maxRetries int) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if err := r.Status().Update(ctx, srun); err != nil {
			lastErr = err
			if i < maxRetries-1 {
				// Exponential backoff: 100ms, 200ms, 400ms...
				time.Sleep(time.Duration(100*(1<<uint(i))) * time.Millisecond)
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to update StoryRun status after %d retries: %w", maxRetries, lastErr)
}

// ✅ PERFORMANCE FIX: Efficient status comparison without JSON marshaling
func (r *StoryRunReconciler) hasStatusChanged(old, new runsv1alpha1.StoryRunStatus) bool {
	if old.Phase != new.Phase {
		return true
	}
	if old.Message != new.Message {
		return true
	}
	if !slices.Equal(old.Active, new.Active) {
		return true
	}
	if !slices.Equal(old.Completed, new.Completed) {
		return true
	}
	if old.StartedAt != new.StartedAt {
		return true
	}
	if old.FinishedAt != new.FinishedAt {
		return true
	}
	// Compare conditions by checking length and key fields
	if len(old.Conditions) != len(new.Conditions) {
		return true
	}
	for i, oldCond := range old.Conditions {
		newCond := new.Conditions[i]
		if oldCond.Type != newCond.Type ||
			oldCond.Status != newCond.Status ||
			oldCond.Reason != newCond.Reason {
			return true
		}
	}
	return false
}

// getStepType returns the type of a step for logging
func getStepType(step *bubushv1alpha1.Step) string {
	if step.Type != nil {
		return *step.Type
	}
	if step.Ref != nil {
		return "engram:" + *step.Ref
	}
	return "unknown"
}

// executeLoopStep handles loop execution with batching and optional parallelism
func (r *StoryRunReconciler) executeLoopStep(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubushv1alpha1.Step) error {
	// Evaluate items via CEL (expects array)
	if step.Items == nil {
		r.markStepCompleted(srun, step.Name)
		return nil
	}

	val, err := r.evaluateTransformExpression(ctx, srun, *step.Items, srun.Spec.Inputs)
	if err != nil {
		return err
	}

	arr, ok := val.([]interface{})
	if !ok {
		return fmt.Errorf("loop items must evaluate to array, got %T", val)
	}

	// Parse body once
	body, err := r.parseStepsFromJSON(step.Body)
	if err != nil {
		return err
	}

	// Check for resource quotas and apply limits
	maxIterations := r.getMaxLoopIterations(srun.Namespace)
	if len(arr) > maxIterations {
		return fmt.Errorf("loop exceeds maximum iterations: %d > %d", len(arr), maxIterations)
	}

	// Determine batch size and parallelism
	batchSize := r.getLoopBatchSize(srun.Namespace)
	enableParallel := r.isParallelExecutionEnabled(srun.Namespace, step)

	// Process in batches to avoid memory pressure
	for i := 0; i < len(arr); i += batchSize {
		end := i + batchSize
		if end > len(arr) {
			end = len(arr)
		}

		batch := arr[i:end]
		if enableParallel {
			err = r.executeLoopBatchParallel(ctx, srun, step, body, batch, i)
		} else {
			err = r.executeLoopBatchSequential(ctx, srun, step, body, batch, i)
		}

		if err != nil {
			return err
		}

		// Yield control between batches to avoid blocking reconcile loop
		if i+batchSize < len(arr) {
			// We need to update the StoryRun and requeue to continue processing
			if updateErr := r.Update(ctx, srun); updateErr != nil {
				return updateErr
			}
			return fmt.Errorf("requeue needed for batch processing")
		}
	}

	r.markStepCompleted(srun, step.Name)
	return nil
}

// executeLoopBatchSequential processes a batch of loop items sequentially
func (r *StoryRunReconciler) executeLoopBatchSequential(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubushv1alpha1.Step, body []bubushv1alpha1.Step, batch []interface{}, offset int) error {
	for idx, it := range batch {
		globalIdx := offset + idx
		for i := range body {
			child := body[i]
			child.Name = fmt.Sprintf("%s-%d-%s", step.Name, globalIdx, child.Name)

			// Render child.With with {item,index}
			if child.With != nil {
				ctxVars := map[string]interface{}{"item": it, "index": globalIdx}
				rw, err := r.renderTemplates(ctx, srun, child.With, ctxVars)
				if err == nil {
					child.With = rw
				}
			}

			if child.Type != nil {
				if err := r.executeBuiltinStep(ctx, srun, &child); err != nil {
					return err
				}
			} else if child.Ref != nil {
				if err := r.executeEngramStep(ctx, srun, &child); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// executeLoopBatchParallel processes a batch of loop items in parallel
func (r *StoryRunReconciler) executeLoopBatchParallel(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubushv1alpha1.Step, body []bubushv1alpha1.Step, batch []interface{}, offset int) error {
	// Use errgroup for parallel execution with proper error handling
	g, gctx := errgroup.WithContext(ctx)

	// Limit parallelism to avoid overwhelming the cluster
	maxConcurrency := r.getMaxConcurrency(srun.Namespace)
	semaphore := make(chan struct{}, maxConcurrency)

	for idx, it := range batch {
		// Capture loop variables
		idx, it := idx, it
		globalIdx := offset + idx

		g.Go(func() error {
			// Acquire semaphore
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			for i := range body {
				child := body[i]
				child.Name = fmt.Sprintf("%s-%d-%s", step.Name, globalIdx, child.Name)

				// Render child.With with {item,index}
				if child.With != nil {
					ctxVars := map[string]interface{}{"item": it, "index": globalIdx}
					rw, err := r.renderTemplates(gctx, srun, child.With, ctxVars)
					if err == nil {
						child.With = rw
					}
				}

				if child.Type != nil {
					if err := r.executeBuiltinStep(gctx, srun, &child); err != nil {
						return err
					}
				} else if child.Ref != nil {
					if err := r.executeEngramStep(gctx, srun, &child); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}

	return g.Wait()
}

// Configuration helpers for loop execution
func (r *StoryRunReconciler) getMaxLoopIterations(namespace string) int {
	// Check namespace annotation for custom limit
	if ns, err := r.getNamespace(context.Background(), namespace); err == nil {
		if limit, exists := ns.Annotations["bobrapet.bubu.sh/max-loop-iterations"]; exists {
			if val, err := strconv.Atoi(limit); err == nil && val > 0 {
				return val
			}
		}
	}
	return 10000 // Default limit
}

func (r *StoryRunReconciler) getLoopBatchSize(namespace string) int {
	if ns, err := r.getNamespace(context.Background(), namespace); err == nil {
		if size, exists := ns.Annotations["bobrapet.bubu.sh/loop-batch-size"]; exists {
			if val, err := strconv.Atoi(size); err == nil && val > 0 && val <= 1000 {
				return val
			}
		}
	}
	return 100 // Default batch size
}

func (r *StoryRunReconciler) isParallelExecutionEnabled(namespace string, step *bubushv1alpha1.Step) bool {
	// For now, check only namespace-level setting since Step doesn't have Annotations field
	// In the future, this could be extended to check step metadata or embedded configurations

	// Check namespace-level default
	if ns, err := r.getNamespace(context.Background(), namespace); err == nil {
		if parallel, exists := ns.Annotations["bobrapet.bubu.sh/parallel-loops"]; exists {
			return parallel == "true"
		}
	}
	return false // Default to sequential
}

func (r *StoryRunReconciler) getMaxConcurrency(namespace string) int {
	if ns, err := r.getNamespace(context.Background(), namespace); err == nil {
		if concurrency, exists := ns.Annotations["bobrapet.bubu.sh/max-concurrency"]; exists {
			if val, err := strconv.Atoi(concurrency); err == nil && val > 0 && val <= 50 {
				return val
			}
		}
	}
	return 10 // Default concurrency
}

func (r *StoryRunReconciler) getNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	var ns corev1.Namespace
	if err := r.Get(ctx, client.ObjectKey{Name: name}, &ns); err != nil {
		return nil, err
	}
	return &ns, nil
}

// handleCleanup performs cleanup when StoryRun is being deleted
func (r *StoryRunReconciler) handleCleanup(ctx context.Context, srun *runsv1alpha1.StoryRun) (ctrl.Result, error) {
	cl := logging.NewCleanupLogger(ctx, "storyrun")
	cl.CleanupStart(srun.Name, "namespace", srun.Namespace)
	startTime := time.Now()

	defer func() {
		duration := time.Since(startTime)
		metrics.RecordResourceCleanup("storyrun", duration, nil)
	}()

	// Cleanup owned StepRuns - use indexed field for efficiency
	var stepRuns runsv1alpha1.StepRunList
	if err := r.List(ctx, &stepRuns,
		client.InNamespace(srun.Namespace),
		client.MatchingFields{"spec.storyRunRef": srun.Name}); err != nil {
		cl.CleanupError(err, srun.Name, "failed_to_list_stepruns")
		return ctrl.Result{}, err
	}

	cl.Info("Found StepRuns to cleanup", "steprun_count", len(stepRuns.Items))

	// Delete all StepRuns belonging to this StoryRun
	for _, stepRun := range stepRuns.Items {
		if stepRun.DeletionTimestamp == nil {
			if err := r.Delete(ctx, &stepRun); err != nil {
				cl.CleanupError(err, stepRun.Name, "failed_to_delete_steprun")
				return ctrl.Result{}, err
			}
			cl.V(1).Info("Deleted StepRun", "steprun", stepRun.Name)
		}
	}

	// Wait for all StepRuns to be fully deleted before removing finalizer
	if len(stepRuns.Items) > 0 {
		cl.Info("Waiting for StepRuns to be deleted", "remaining", len(stepRuns.Items))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Keep PVCs alive for reuse - only clean up data inside them
	var pvcs corev1.PersistentVolumeClaimList
	if err := r.List(ctx, &pvcs,
		client.InNamespace(srun.Namespace),
		client.MatchingLabels{"bobrapet.bubu.sh/storyrun": srun.Name}); err != nil {
		cl.CleanupError(err, "Failed to list PVCs for data cleanup")
		return ctrl.Result{}, err
	}

	cl.Info("Found PVCs for data cleanup", "pvc_count", len(pvcs.Items))

	// Clean up data inside PVCs but keep PVCs bound to avoid PV Released state
	for _, pvc := range pvcs.Items {
		if pvc.Status.Phase == corev1.ClaimBound {
			cl.Info("PVC data cleanup - keeping PVC alive to avoid binding conflicts", "pvc_name", pvc.Name)
			// TODO: Add data cleanup job if needed (engrams should handle their own cleanup)
		}
	}

	// Cleanup any annotations that might reference external resources
	if srun.Annotations != nil {
		for key := range srun.Annotations {
			if strings.HasPrefix(key, "runs.bubu.sh/") {
				cl.V(1).Info("Cleaning up annotation", "key", key)
				// Could trigger cleanup of external resources here
			}
		}
	}

	// Remove the finalizer to allow deletion using FinalizerManager
	if r.finalizerManager == nil {
		r.finalizerManager = parentcontroller.NewFinalizerManager(r.Client)
	}
	r.finalizerManager.RemoveFinalizer(srun, parentcontroller.StoryRunFinalizer)
	if err := r.Update(ctx, srun); err != nil {
		cl.CleanupError(err, srun.Name, "failed_to_remove_finalizer")
		return ctrl.Result{}, err
	}

	duration := time.Since(startTime)
	cl.CleanupSuccess(srun.Name, duration)
	return ctrl.Result{}, nil
}
