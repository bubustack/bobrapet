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
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/patch"
)

const (
	// StoryRunFinalizer is the finalizer for StoryRun resources
	StoryRunFinalizer = "storyrun.bubustack.io/finalizer"
	// DefaultMaxInlineInputsSize is the default maximum size for inline inputs (5 KB).
	// This provides a safe fallback if webhooks are disabled and no operator config is set.
	// It's a balance between preventing etcd bloat and allowing reasonable input sizes.
	DefaultMaxInlineInputsSize = 5 * 1024
)

// StoryRunReconciler reconciles a StoryRun object
type StoryRunReconciler struct {
	config.ControllerDependencies
	rbacManager   *RBACManager
	dagReconciler *DAGReconciler
	Recorder      record.EventRecorder
}

//+kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=runs.bubustack.io,resources=storyruns/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=create;get;watch;list
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=create;get;watch;list
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=create;get;watch;list
//+kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns,verbs=get;list;watch;create;update;patch
//+kubebuilder:rbac:groups=bubustack.io,resources=stories,verbs=get;list;watch
//+kubebuilder:rbac:groups=bubustack.io,resources=engrams,verbs=get;list;watch

func (r *StoryRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	log := logging.NewReconcileLogger(ctx, "storyrun").WithValues("storyrun", req.NamespacedName)
	startTime := time.Now()
	defer func() {
		metrics.RecordControllerReconcile("storyrun", time.Since(startTime), err)
	}()

	// Bound reconcile duration
	timeout := r.ControllerDependencies.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var srun runsv1alpha1.StoryRun
	if err := r.ControllerDependencies.Client.Get(ctx, req.NamespacedName, &srun); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Validate input size to prevent oversized objects when webhooks are disabled.
	maxBytes := r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize
	if maxBytes == 0 {
		maxBytes = DefaultMaxInlineInputsSize // Fallback to a safe default, mirroring webhook logic.
	}
	if srun.Spec.Inputs != nil && len(srun.Spec.Inputs.Raw) > maxBytes {
		if srun.Status.Phase != enums.PhaseFailed {
			log.Info("StoryRun spec.inputs is too large, failing run", "size", len(srun.Spec.Inputs.Raw), "maxSize", maxBytes)
			failMsg := fmt.Sprintf("spec.inputs size %d bytes exceeds maximum of %d", len(srun.Spec.Inputs.Raw), maxBytes)
			if err := r.setStoryRunPhase(ctx, &srun, enums.PhaseFailed, failMsg); err != nil {
				log.Error(err, "Failed to set StoryRun status to Failed due to oversized inputs")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil // Stop reconciliation for this oversized object.
	}

	// Handle deletion first
	if !srun.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &srun)
	}

	// Add finalizer if it doesn't exist
	if !controllerutil.ContainsFinalizer(&srun, StoryRunFinalizer) {
		beforePatch := srun.DeepCopy()
		controllerutil.AddFinalizer(&srun, StoryRunFinalizer)
		if err := r.Patch(ctx, &srun, client.MergeFrom(beforePatch)); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
	}

	if err := r.rbacManager.Reconcile(ctx, &srun); err != nil {
		log.Error(err, "Failed to reconcile RBAC")
		// We can choose to fail hard here or just log and continue.
		// For now, let's fail hard as RBAC is critical for engrams to run.
		return ctrl.Result{}, err
	}

	if srun.Status.Phase == enums.PhaseSucceeded || srun.Status.Phase == enums.PhaseFailed {
		return ctrl.Result{}, nil
	}

	story, err := r.getStoryForRun(ctx, &srun)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("Story not found for StoryRun, will requeue", "story", srun.Spec.StoryRef.Name)
			// Emit event for user visibility (guard recorder for tests)
			if r.Recorder != nil {
				r.Recorder.Event(&srun, "Warning", conditions.ReasonStoryNotFound, fmt.Sprintf("Waiting for Story '%s'", srun.Spec.StoryRef.ToNamespacedName(&srun).String()))
			}
			statusErr := patch.RetryableStatusPatch(ctx, r.ControllerDependencies.Client, &srun, func(obj client.Object) {
				sr := obj.(*runsv1alpha1.StoryRun)
				if sr.Status.Phase == "" {
					sr.Status.Phase = enums.PhasePending
				}
				cm := conditions.NewConditionManager(sr.Generation)
				message := fmt.Sprintf("Waiting for Story '%s' to be created", srun.Spec.StoryRef.ToNamespacedName(&srun).String())
				cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonStoryNotFound, message)
				sr.Status.Message = message
			})
			if statusErr != nil {
				log.Error(statusErr, "Failed to update StoryRun status while waiting for Story")
				return ctrl.Result{}, statusErr
			}
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}

		// For other errors, fail the run
		log.Error(err, "Failed to get Story for StoryRun")
		if updateErr := r.setStoryRunPhase(ctx, &srun, enums.PhaseFailed, fmt.Sprintf("failed to get story: %v", err)); updateErr != nil {
			log.Error(updateErr, "Failed to set StoryRun status to Failed")
			return ctrl.Result{}, updateErr // Return the error from the status update
		}
		return ctrl.Result{}, nil // Do not requeue, as this is a terminal state
	}

	if story.Spec.Pattern == enums.StreamingPattern {
		if _, err := r.reconcileStreamingStoryRun(ctx, &srun, story); err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				log.Info("Reconciliation timed out, returning error to trigger failure rate limiter")
				return ctrl.Result{}, fmt.Errorf("reconcile timed out: %w", ctx.Err())
			}
			log.Error(err, "Failed to reconcile streaming story run")
			if updateErr := r.setStoryRunPhase(ctx, &srun, enums.PhaseFailed, fmt.Sprintf("failed to reconcile streaming story: %v", err)); updateErr != nil {
				log.Error(updateErr, "Failed to set StoryRun status to Failed")
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, nil
		}
	}

	if srun.Status.Phase == "" {
		err := r.setStoryRunPhase(ctx, &srun, enums.PhaseRunning, "Starting StoryRun execution")
		if err != nil {
			log.Error(err, "Failed to update StoryRun status to Running")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Re-evaluate the entire DAG based on the current state.
	return r.dagReconciler.Reconcile(ctx, &srun, story)
}

func (r *StoryRunReconciler) reconcileStreamingStoryRun(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubushv1alpha1.Story) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun").WithValues("storyrun", srun.Name)

	if story.Spec.StreamingStrategy == enums.StreamingStrategyPerStoryRun {
		log.Info("Reconciling streaming StoryRun with PerStoryRun strategy")

		// For each step in the story, create a dedicated Engram owned by this StoryRun
		for _, step := range story.Spec.Steps {
			if step.Ref == nil {
				continue
			}

			// Get the original engram definition
			originalEngram := &bubushv1alpha1.Engram{}
			originalEngramKey := step.Ref.ToNamespacedName(story)
			if err := r.ControllerDependencies.Client.Get(ctx, originalEngramKey, originalEngram); err != nil {
				if errors.IsNotFound(err) {
					return ctrl.Result{}, fmt.Errorf("step '%s' references engram '%s' which does not exist", step.Name, originalEngram.Name)
				}
				return ctrl.Result{}, fmt.Errorf("failed to get engram for step '%s': %w", step.Name, err)
			}

			// Create or update the run-specific engram
			runEngram := &bubushv1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-%s", srun.Name, step.Name),
					Namespace: srun.Namespace,
				},
			}

			op, err := controllerutil.CreateOrUpdate(ctx, r.ControllerDependencies.Client, runEngram, func() error {
				// Copy the spec from the original engram
				runEngram.Spec = *originalEngram.Spec.DeepCopy()
				// Ensure the mode is a long-running one
				if runEngram.Spec.Mode != enums.WorkloadModeDeployment && runEngram.Spec.Mode != enums.WorkloadModeStatefulSet {
					log.Info("Engram for streaming step is not in a long-running mode, defaulting to 'deployment'", "step", step.Name, "originalMode", runEngram.Spec.Mode)
					runEngram.Spec.Mode = enums.WorkloadModeDeployment
				}
				// Set the StoryRun as the owner
				return controllerutil.SetControllerReference(srun, runEngram, r.ControllerDependencies.Scheme)
			})

			if err != nil {
				log.Error(err, "Failed to create or update run-specific Engram", "step", step.Name)
				return ctrl.Result{}, err
			}
			if op != controllerutil.OperationResultNone {
				log.Info("Reconciled run-specific engram", "engram", runEngram.Name, "operation", op)
			}
		}
	}

	// For PerStory strategy, we don't need to do anything. The engrams are expected to be running.
	// We can now mark the StoryRun as running.
	if srun.Status.Phase == enums.PhasePending {
		if err := r.setStoryRunPhase(ctx, srun, enums.PhaseRunning, "Streaming StoryRun is active"); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *StoryRunReconciler) reconcileDelete(ctx context.Context, srun *runsv1alpha1.StoryRun) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun").WithValues("storyrun", srun.Name)
	log.Info("Reconciling deletion for StoryRun")

	// List all StepRuns for this StoryRun
	var stepRunList runsv1alpha1.StepRunList
	if err := r.ControllerDependencies.Client.List(ctx, &stepRunList, client.InNamespace(srun.Namespace), client.MatchingLabels{"bubustack.io/storyrun": srun.Name}); err != nil {
		log.Error(err, "Failed to list StepRuns for cleanup")
		return ctrl.Result{}, err
	}

	// For a simple MVP, we will just delete the StepRuns.
	// A more advanced implementation might try to gracefully cancel them.
	for _, sr := range stepRunList.Items {
		if sr.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, &sr); err != nil {
				log.Error(err, "Failed to delete StepRun during cleanup", "stepRun", sr.Name)
				return ctrl.Result{}, err
			}
			log.Info("Deleted child StepRun", "stepRun", sr.Name)
		}
	}

	// Once all children are gone, remove the finalizer.
	if len(stepRunList.Items) == 0 {
		if controllerutil.ContainsFinalizer(srun, StoryRunFinalizer) {
			patch := client.MergeFrom(srun.DeepCopy())
			controllerutil.RemoveFinalizer(srun, StoryRunFinalizer)
			// Use Patch for atomicity, avoiding race conditions with other updaters.
			// This is safer than Update for modifying metadata like finalizers.
			if err := r.Patch(ctx, srun, patch); err != nil {
				log.Error(err, "Failed to remove finalizer")
				return ctrl.Result{}, err
			}
			log.Info("Removed finalizer")
		}
	} else {
		// If children still exist, requeue to check on them later.
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

func (r *StoryRunReconciler) getStoryForRun(ctx context.Context, srun *runsv1alpha1.StoryRun) (*bubushv1alpha1.Story, error) {
	var story bubushv1alpha1.Story
	key := srun.Spec.StoryRef.ToNamespacedName(srun)
	if err := r.ControllerDependencies.Client.Get(ctx, key, &story); err != nil {
		return nil, err
	}
	return &story, nil
}

func (r *StoryRunReconciler) setStoryRunPhase(ctx context.Context, srun *runsv1alpha1.StoryRun, phase enums.Phase, message string) error {
	return patch.RetryableStatusPatch(ctx, r.ControllerDependencies.Client, srun, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StoryRun)
		sr.Status.Phase = phase
		sr.Status.Message = message
		sr.Status.ObservedGeneration = sr.Generation

		cm := conditions.NewConditionManager(sr.Generation)

		switch phase {
		case enums.PhaseRunning:
			if sr.Status.StartedAt == nil {
				now := metav1.Now()
				sr.Status.StartedAt = &now
			}
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonReconciling, message)
		case enums.PhaseSucceeded:
			if sr.Status.FinishedAt == nil {
				now := metav1.Now()
				sr.Status.FinishedAt = &now
				if sr.Status.StartedAt != nil {
					duration := now.Sub(sr.Status.StartedAt.Time)
					sr.Status.Duration = duration.String()
					metrics.RecordStoryRunMetrics(sr.Namespace, sr.Spec.StoryRef.Name, string(phase), duration)
				}
			}
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonCompleted, message)
		case enums.PhaseFailed:
			if sr.Status.FinishedAt == nil {
				now := metav1.Now()
				sr.Status.FinishedAt = &now
				if sr.Status.StartedAt != nil {
					duration := now.Sub(sr.Status.StartedAt.Time)
					sr.Status.Duration = duration.String()
					metrics.RecordStoryRunMetrics(sr.Namespace, sr.Spec.StoryRef.Name, string(phase), duration)
				}
			}
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonExecutionFailed, message)
		}
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *StoryRunReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	r.rbacManager = NewRBACManager(mgr.GetClient(), mgr.GetScheme())
	stepExecutor := NewStepExecutor(mgr.GetClient(), mgr.GetScheme(), &r.ControllerDependencies.CELEvaluator)
	r.dagReconciler = NewDAGReconciler(mgr.GetClient(), &r.ControllerDependencies.CELEvaluator, stepExecutor, r.ControllerDependencies.ConfigResolver)
	r.Recorder = mgr.GetEventRecorderFor("storyrun-controller")

	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StoryRun{}).
		WithOptions(opts).
		Owns(&runsv1alpha1.StepRun{}).
		Complete(r)
}
