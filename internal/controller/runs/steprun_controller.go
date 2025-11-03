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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/podspec"
	rec "github.com/bubustack/bobrapet/pkg/reconcile"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	runsinputs "github.com/bubustack/bobrapet/pkg/runs/inputs"
	runstatus "github.com/bubustack/bobrapet/pkg/runs/status"
	runsvalidation "github.com/bubustack/bobrapet/pkg/runs/validation"
	transportutil "github.com/bubustack/bobrapet/pkg/transport"
	transportbinding "github.com/bubustack/bobrapet/pkg/transport/binding"
	"github.com/bubustack/bobrapet/pkg/workload"
	"github.com/bubustack/core/contracts"
	coreenv "github.com/bubustack/core/runtime/env"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
)

const (
	// StepRunFinalizer is the finalizer for StepRun resources
	StepRunFinalizer = "steprun.bubustack.io/finalizer"

	eventReasonJobPatched                            = "JobStatusPatched"
	eventReasonJobFallback                           = "JobStatusFallback"
	eventReasonJobSucceeded                          = "JobCompleted"
	eventReasonTransportBindingAnnotated             = "TransportBindingAnnotated"
	eventReasonTransportBindingMergeFailed           = "TransportBindingMergeFailed"
	eventReasonTransportBindingCodecFailed           = "TransportBindingCodecRejected"
	eventReasonDownstreamTargetsRejected             = "DownstreamTargetsRejected"
	eventReasonDownstreamTargetsUpdated              = "DownstreamTargetsUpdated"
	eventReasonDownstreamTargetsCleared              = "DownstreamTargetsCleared"
	eventReasonTransportBindingAnnotationInvalid     = "TransportBindingAnnotationInvalid"
	eventReasonTransportBindingPending               = "TransportBindingPending"
	eventReasonTransportBindingDecodeFailed          = "TransportBindingDecodeFailed"
	eventReasonTransportBindingAnnotationPatchFailed = "TransportBindingAnnotationPatchFailed"
	eventReasonJobCelBlocked                         = "JobCelBlocked"
	eventReasonTransportConditionUpdated             = "TransportConditionUpdated"
)

var errTransportBindingPending = fmt.Errorf("transport binding not yet observed in informer cache")

const (
	executionModeHybrid    = "hybrid"
	executionModeStreaming = "streaming"
)

type jobResources struct {
	envVars      []corev1.EnvVar
	envFrom      []corev1.EnvFromSource
	volumes      []corev1.Volume
	volumeMounts []corev1.VolumeMount
}

// StepRunReconciler reconciles a StepRun object
type StepRunReconciler struct {
	config.ControllerDependencies
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams,verbs=get;list;watch
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=core,resources=pods/log,verbs=get
// +kubebuilder:rbac:groups=transportutil.bubustack.io,resources=transportbindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=transportutil.bubustack.io,resources=transportbindings/status,verbs=get;update;patch

// Reconcile is the main entry point for StepRun reconciliation.
//
// Behavior:
//   - Applies optional reconcile timeout from operator config.
//   - Fetches StepRun and ignores NotFound.
//   - Handles deletion if DeletionTimestamp set.
//   - Ensures finalizer is present.
//   - Routes to reconcileNormal for non-terminal phases.
//
// Arguments:
//   - ctx context.Context: for API calls and cancellation.
//   - req ctrl.Request: the reconcile request with StepRun name.
//
// Returns:
//   - ctrl.Result: may include requeue or delay.
//   - error: non-nil on failures.
func (r *StepRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	baseLogger := logging.NewReconcileLogger(ctx, "steprun").WithValues("steprun", req.NamespacedName)

	timeout := time.Duration(0)
	if r.ConfigResolver != nil && r.ConfigResolver.GetOperatorConfig() != nil {
		timeout = r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	}
	if timeout > 0 {
		baseLogger.V(1).Info("Applying reconcile timeout", "timeout", timeout.String())
	}

	return rec.Run(ctx, req, rec.RunOptions{
		Controller: "steprun",
		Timeout:    timeout,
	}, rec.RunHooks[*runsv1alpha1.StepRun]{
		Get: func(ctx context.Context, req ctrl.Request) (*runsv1alpha1.StepRun, error) {
			var stepRun runsv1alpha1.StepRun
			if err := r.Get(ctx, req.NamespacedName, &stepRun); err != nil {
				return nil, err
			}
			return &stepRun, nil
		},
		HandleDeletion: func(ctx context.Context, step *runsv1alpha1.StepRun) (bool, ctrl.Result, error) {
			return rec.ShortCircuitDeletion(ctx, step, func(ctx context.Context) (ctrl.Result, error) {
				return r.reconcileDelete(ctx, step)
			})
		},
		EnsureFinalizer: func(ctx context.Context, step *runsv1alpha1.StepRun) error {
			stepLogger := baseLogger.WithStepRun(step)
			added, err := kubeutil.EnsureFinalizer(ctx, r.Client, step, StepRunFinalizer)
			if err != nil {
				stepLogger.Error(err, "Failed to add StepRun finalizer")
				return err
			}
			if added {
				stepLogger.Info("Added StepRun finalizer")
			}
			return nil
		},
		HandleNormal: func(ctx context.Context, step *runsv1alpha1.StepRun) (ctrl.Result, error) {
			stepLogger := baseLogger.WithStepRun(step)
			return rec.RunPipeline(ctx, step, rec.PipelineHooks[*runsv1alpha1.StepRun, struct{}]{
				Prepare: func(ctx context.Context, step *runsv1alpha1.StepRun) (struct{}, *ctrl.Result, error) {
					if step.Status.Phase.IsTerminal() {
						if _, err := r.cleanupRunScopedRuntime(ctx, step); err != nil {
							stepLogger.Error(err, "Failed to cleanup realtime runtime for terminal StepRun")
							return struct{}{}, nil, err
						}
						return struct{}{}, &ctrl.Result{}, nil
					}
					if step.Status.Phase == "" {
						if err := r.setStepRunPhase(ctx, step, enums.PhasePending, "StepRun created and awaiting processing"); err != nil {
							stepLogger.Error(err, "Failed to update StepRun status to Pending")
							return struct{}{}, nil, err
						}
						return struct{}{}, &ctrl.Result{Requeue: true}, nil
					}
					return struct{}{}, nil, nil
				},
				Ensure: func(ctx context.Context, step *runsv1alpha1.StepRun, _ struct{}) (ctrl.Result, error) {
					return r.reconcileNormal(ctx, step)
				},
			})
		},
	})
}

// reconcileNormal handles normal (non-deletion) StepRun reconciliation.
//
// Behavior:
//   - Fetches referenced Engram and EngramTemplate.
//   - Determines execution mode (job, deployment, statefulset).
//   - Routes to appropriate execution handler.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to reconcile.
//
// Returns:
//   - ctrl.Result: reconcile result with possible requeue.
//   - error: non-nil on failures.
func (r *StepRunReconciler) reconcileNormal(ctx context.Context, step *runsv1alpha1.StepRun) (ctrl.Result, error) {
	stepLogger := logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step)

	engram, err := r.getEngramForStep(ctx, step)
	if err != nil {
		if errors.IsNotFound(err) {
			stepLogger.Info("Engram referenced by StepRun not found, setting phase to Blocked")
			// Emit event for user visibility (guard recorder for tests)
			if r.Recorder != nil {
				r.Recorder.Event(step, "Warning", conditions.ReasonReferenceNotFound, fmt.Sprintf("Engram '%s' not found", step.Spec.EngramRef.Name))
			}
			err := r.setStepRunPhase(ctx, step, enums.PhaseBlocked, fmt.Sprintf("Engram '%s' not found", step.Spec.EngramRef.Name))
			return ctrl.Result{}, err // Stop reconciliation, wait for watch to trigger
		}
		stepLogger.Error(err, "Failed to get Engram for StepRun")
		return ctrl.Result{}, err
	}

	// Default to "job" mode if not specified
	mode := enums.WorkloadModeJob
	if engram != nil && engram.Spec.Mode != "" {
		mode = engram.Spec.Mode
	}

	stepLogger.Info("Determined execution mode", "mode", mode)

	switch mode {
	case enums.WorkloadModeDeployment, enums.WorkloadModeStatefulSet:
		story, err := r.getStoryForStep(ctx, step)
		if err != nil {
			stepLogger.Error(err, "Failed to load Story for realtime StepRun")
			return ctrl.Result{}, err
		}
		stepSpec := findStoryStep(story, step.Spec.StepID)
		if stepSpec == nil {
			err := fmt.Errorf("story %s does not define step %s", story.Name, step.Spec.StepID)
			stepLogger.Error(err, "Streaming StepRun references missing step")
			return ctrl.Result{}, err
		}
		engramTemplate, err := r.getEngramTemplateForEngram(ctx, engram)
		if err != nil {
			if errors.IsNotFound(err) {
				stepLogger.Info("EngramTemplate referenced by Engram not found, setting phase to Blocked")
				err := r.setStepRunPhase(ctx, step, enums.PhaseBlocked, fmt.Sprintf("EngramTemplate '%s' not found", engram.Spec.TemplateRef.Name))
				return ctrl.Result{}, err
			}
			stepLogger.Error(err, "Failed to get EngramTemplate for Engram")
			return ctrl.Result{}, err
		}
		return r.reconcileRunScopedRealtimeStep(ctx, step, story, stepSpec, engram, engramTemplate)
	default:
		engramTemplate, err := r.getEngramTemplateForEngram(ctx, engram)
		if err != nil {
			if errors.IsNotFound(err) {
				stepLogger.Info("EngramTemplate referenced by Engram not found, setting phase to Blocked")
				err := r.setStepRunPhase(ctx, step, enums.PhaseBlocked, fmt.Sprintf("EngramTemplate '%s' not found", engram.Spec.TemplateRef.Name))
				return ctrl.Result{}, err // Stop reconciliation, wait for watch to trigger
			}
			stepLogger.Error(err, "Failed to get EngramTemplate for Engram")
			return ctrl.Result{}, err
		}
		return r.reconcileJobExecution(ctx, step, engram, engramTemplate)
	}
}

// reconcileJobExecution ensures a Job exists for the StepRun and monitors it.
//
// Behavior:
//   - Creates Job if not found, handling CEL evaluation blocks.
//   - Marks StepRun as Running after successful Job creation.
//   - Defers steady-state monitoring to handleJobStatus.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to reconcile.
//   - engram *v1alpha1.Engram: the referenced Engram.
//   - engramTemplate *catalogv1alpha1.EngramTemplate: the template.
//
// Returns:
//   - ctrl.Result: reconcile result with possible requeue.
//   - error: non-nil on failures.
func (r *StepRunReconciler) reconcileJobExecution(ctx context.Context, step *runsv1alpha1.StepRun, engram *v1alpha1.Engram, engramTemplate *catalogv1alpha1.EngramTemplate) (ctrl.Result, error) {
	stepLogger := logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step)

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, job)
	if err != nil {
		if errors.IsNotFound(err) {
			stepLogger.Info("Job not found, creating a new one")
			newJob, createErr := r.createJobForStep(ctx, step, engram, engramTemplate)
			if createErr != nil {
				if evalBlocked, ok := createErr.(*cel.ErrEvaluationBlocked); ok {
					stepLogger.Info("CEL evaluation blocked, requeueing", "reason", evalBlocked.Reason)
					if r.Recorder != nil {
						r.Recorder.Eventf(
							step,
							corev1.EventTypeNormal,
							eventReasonJobCelBlocked,
							"CEL evaluation blocked while creating Job: %s",
							evalBlocked.Reason,
						)
					}
					return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
				}
				stepLogger.Error(createErr, "Failed to create Job for StepRun")
				return ctrl.Result{}, createErr
			}
			if err := r.Create(ctx, newJob); err != nil {
				if errors.IsAlreadyExists(err) {
					stepLogger.Info("Job already exists after create attempt; reconciling existing Job status")
					existing := &batchv1.Job{}
					if getErr := r.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, existing); getErr != nil {
						stepLogger.Error(getErr, "Failed to fetch Job after AlreadyExists error")
						return ctrl.Result{}, getErr
					}
					return r.handleJobStatus(ctx, step, existing)
				}
				stepLogger.Error(err, "Failed to create Job in cluster")
				return ctrl.Result{}, err
			}
			stepLogger.Info("Created Job for StepRun", "jobName", newJob.Name, "jobUID", string(newJob.GetUID()))
			if err := r.setStepRunPhase(ctx, step, enums.PhaseRunning, "Job created for StepRun"); err != nil {
				stepLogger.Error(err, "Failed to update StepRun status to Running")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
		stepLogger.Error(err, "Failed to get Job for StepRun")
		return ctrl.Result{}, err
	}

	return r.handleJobStatus(ctx, step, job)
}

// setStepRunPhase updates the StepRun phase and conditions via conflict-retrying kubeutil.
//
// Behavior:
//   - Updates parent Engram status for terminal phases (best-effort).
//   - Delegates to runstatus.PatchStepRunPhase for actual kubeutil.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to update.
//   - phase enums.Phase: the new phase.
//   - message string: message for the phase transition.
//
// Returns:
//   - error: non-nil on patch failure.
func (r *StepRunReconciler) setStepRunPhase(ctx context.Context, step *runsv1alpha1.StepRun, phase enums.Phase, message string) error {
	// If the phase is terminal, also update the parent Engram's status.
	if phase.IsTerminal() {
		if err := r.updateEngramStatus(ctx, step, phase); err != nil {
			// Log the error but don't fail the StepRun phase transition.
			// The Engram status update is best-effort.
			logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step).Error(err, "Failed to update parent engram status")
		}
	}

	return runstatus.PatchStepRunPhase(ctx, r.Client, step, phase, message)
}

// updateEngramStatus increments parent Engram execution counters on terminal phases.
//
// Behavior:
//   - Skips StepRuns without an EngramRef.
//   - Fetches the Engram and applies retryable status kubeutil.
//   - Updates LastExecutionTime, TotalExecutions, and FailedExecutions.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the completed StepRun.
//   - phase enums.Phase: the terminal phase.
//
// Returns:
//   - error: non-nil on fetch or patch failure.
func (r *StepRunReconciler) updateEngramStatus(ctx context.Context, step *runsv1alpha1.StepRun, phase enums.Phase) error {
	if step.Spec.EngramRef.Name == "" {
		return nil // No engram to update
	}

	engram := &v1alpha1.Engram{}
	engramKey := client.ObjectKey{Namespace: step.Namespace, Name: step.Spec.EngramRef.Name}
	if err := r.Get(ctx, engramKey, engram); err != nil {
		return fmt.Errorf("failed to get parent engram %s for status update: %w", step.Spec.EngramRef.Name, err)
	}

	return kubeutil.RetryableStatusPatch(ctx, r.Client, engram, func(obj client.Object) {
		e := obj.(*v1alpha1.Engram)
		now := metav1.Now()
		e.Status.LastExecutionTime = &now
		e.Status.TotalExecutions++
		if phase == enums.PhaseFailed {
			e.Status.FailedExecutions++
		}
	})
}

// getEngramForStep fetches the Engram referenced by the StepRun.
//
// Behavior:
//   - Returns error if EngramRef is missing or empty.
//   - Uses refs.LoadNamespacedReference for fetching.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun with the reference.
//
// Returns:
//   - *v1alpha1.Engram: the fetched Engram.
//   - error: non-nil on missing ref or fetch failure.
func (r *StepRunReconciler) getEngramForStep(ctx context.Context, step *runsv1alpha1.StepRun) (*v1alpha1.Engram, error) {
	if step.Spec.EngramRef == nil || strings.TrimSpace(step.Spec.EngramRef.Name) == "" {
		return nil, fmt.Errorf("steprun %s has no engram reference (.spec.engramRef.name)", step.Name)
	}
	engram, key, err := refs.LoadNamespacedReference(
		ctx,
		r.Client,
		step,
		step.Spec.EngramRef,
		func() *v1alpha1.Engram { return &v1alpha1.Engram{} },
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("get engram %s: %w", key.String(), err)
	}
	return engram, nil
}

// getEngramTemplateForEngram fetches the cluster-scoped EngramTemplate.
//
// Behavior:
//   - Returns error if engram is nil or templateRef.name is empty.
//   - Templates are cluster-scoped (no namespace).
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - engram *v1alpha1.Engram: supplies Spec.TemplateRef.Name.
//
// Returns:
//   - *catalogv1alpha1.EngramTemplate: the fetched template.
//   - error: non-nil on fetch failure.
func (r *StepRunReconciler) getEngramTemplateForEngram(ctx context.Context, engram *v1alpha1.Engram) (*catalogv1alpha1.EngramTemplate, error) {
	if engram == nil {
		return nil, fmt.Errorf("engram is nil: cannot fetch template")
	}
	name := strings.TrimSpace(engram.Spec.TemplateRef.Name)
	if name == "" {
		return nil, fmt.Errorf("engram %s missing spec.templateRef.name", engram.Name)
	}
	template, err := refs.LoadClusterObject(
		ctx,
		r.Client,
		name,
		func() *catalogv1alpha1.EngramTemplate { return &catalogv1alpha1.EngramTemplate{} },
	)
	if err != nil {
		return nil, fmt.Errorf("get EngramTemplate %s: %w", name, err)
	}
	return template, nil
}

// reconcileDelete handles StepRun deletion and resource cleanup.
//
// Behavior:
//   - Deletes owned Job if it exists.
//   - Cleans up realtime runtime resources.
//   - Removes finalizer when cleanup is complete.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun being deleted.
//
// Returns:
//   - ctrl.Result: with requeue if cleanup pending.
//   - error: non-nil on failures.
func (r *StepRunReconciler) reconcileDelete(ctx context.Context, step *runsv1alpha1.StepRun) (ctrl.Result, error) {
	stepLogger := logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step)
	stepLogger.Info("Reconciling deletion for StepRun")

	if !controllerutil.ContainsFinalizer(step, StepRunFinalizer) {
		return ctrl.Result{}, nil
	}

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, job)

	if err != nil && !errors.IsNotFound(err) {
		stepLogger.Error(err, "Failed to get Job for cleanup check")
		return ctrl.Result{}, err
	}

	// If the job exists and is not being deleted, delete it.
	if err == nil && job.DeletionTimestamp.IsZero() {
		stepLogger.Info("Deleting owned Job")
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
			stepLogger.Error(err, "Failed to delete Job during cleanup")
			return ctrl.Result{}, err
		}
		// Requeue to wait for the job to be fully deleted.
		return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
	}

	// If the job is not found, it's safe to remove the finalizer.
	if errors.IsNotFound(err) {
		cleaned, cleanupErr := r.cleanupRunScopedRuntime(ctx, step)
		if cleanupErr != nil {
			stepLogger.Error(cleanupErr, "Failed to cleanup realtime runtime during deletion")
			return ctrl.Result{}, cleanupErr
		}
		if !cleaned {
			stepLogger.Info("Waiting for realtime runtime resources to be deleted before removing finalizer")
			return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
		}

		stepLogger.Info("Owned resources are deleted, removing finalizer")
		beforePatch := step.DeepCopy()
		controllerutil.RemoveFinalizer(step, StepRunFinalizer)
		if err := r.Patch(ctx, step, client.MergeFrom(beforePatch)); err != nil {
			stepLogger.Error(err, "Failed to remove finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// If the job still exists (is being deleted), requeue.
	stepLogger.Info("Owned Job is still terminating, waiting for it to be deleted")
	return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
}

// createJobForStep builds a Job resource for executing the StepRun.
//
// Behavior:
//   - Prepares execution context with inputs and config.
//   - Builds job spec with proper pod template.
//   - Sets owner reference to StepRun.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StepRun: the StepRun owner.
//   - engram *v1alpha1.Engram: the Engram configuration.
//   - engramTemplate *catalogv1alpha1.EngramTemplate: the template.
//
// Returns:
//   - *batchv1.Job: the constructed Job.
//   - error: non-nil on failures.
func (r *StepRunReconciler) createJobForStep(ctx context.Context, srun *runsv1alpha1.StepRun, engram *v1alpha1.Engram, engramTemplate *catalogv1alpha1.EngramTemplate) (*batchv1.Job, error) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(srun)
	operatorCfg := r.ConfigResolver.GetOperatorConfig()
	runtimeEnvCfg := toRuntimeEnvConfig(operatorCfg.Controller.Engram.EngramControllerConfig)

	story, storyRun, resolvedConfig, inputBytes, stepTimeout, downstreamTargets, dependentSteps, err := r.prepareExecutionContext(ctx, srun, engram, engramTemplate, stepLogger)
	if err != nil {
		return nil, err
	}
	if err := r.ensureStorageSecret(ctx, srun.Namespace, resolvedConfig, stepLogger); err != nil {
		return nil, err
	}

	if err := r.ensureDownstreamTargets(ctx, srun, downstreamTargets, dependentSteps, stepLogger); err != nil {
		return nil, err
	}

	executionMode := "batch"
	if len(downstreamTargets) > 0 {
		executionMode = "hybrid"
	}

	storageTimeout := podspec.FormatStorageTimeout(resolvedConfig, runtimeEnvCfg.DefaultStorageTimeoutSeconds)
	resources, err := r.assembleJobResources(ctx, jobResourceInputs{
		srun:           srun,
		story:          story,
		storyRun:       storyRun,
		engram:         engram,
		engramTemplate: engramTemplate,
		resolvedConfig: resolvedConfig,
		runtimeEnvCfg:  runtimeEnvCfg,
		inputBytes:     inputBytes,
		stepTimeout:    stepTimeout,
		executionMode:  executionMode,
		stepLogger:     stepLogger,
	})
	if err != nil {
		return nil, err
	}

	activeDeadlineSeconds := int64((stepTimeout + 2*time.Minute).Seconds())
	envVars := resources.envVars

	job := r.buildJobSpec(srun, resolvedConfig, envVars, resources.envFrom, resources.volumes, resources.volumeMounts, activeDeadlineSeconds, storageTimeout)
	if err := controllerutil.SetControllerReference(srun, job, r.Scheme); err != nil {
		return nil, err
	}
	_ = storyRun // storyRun currently unused beyond inputs; keep for future enhancements
	return job, nil
}

type jobResourceInputs struct {
	srun           *runsv1alpha1.StepRun
	story          *v1alpha1.Story
	storyRun       *runsv1alpha1.StoryRun
	engram         *v1alpha1.Engram
	engramTemplate *catalogv1alpha1.EngramTemplate
	resolvedConfig *config.ResolvedExecutionConfig
	runtimeEnvCfg  coreenv.Config
	inputBytes     []byte
	stepTimeout    time.Duration
	executionMode  string
	stepLogger     *logging.ControllerLogger
}

func (r *StepRunReconciler) assembleJobResources(ctx context.Context, in jobResourceInputs) (jobResources, error) {
	resources := jobResources{}
	secretArtifacts := podspec.BuildSecretArtifacts(in.engramTemplate.Spec.SecretSchema, in.resolvedConfig.Secrets)

	envVars := r.buildBaseEnvVars(in.srun, in.story, in.inputBytes, in.stepTimeout, in.executionMode, in.resolvedConfig, in.resolvedConfig.DebugLogs)
	envVars = append(envVars, coreenv.BuildGRPCTuningEnv(in.runtimeEnvCfg)...)
	envVars = append(envVars, secretArtifacts.EnvVars...)
	envVars = append(envVars, corev1.EnvVar{Name: contracts.EngramNameEnv, Value: in.engram.Name})

	volumes := append([]corev1.Volume(nil), secretArtifacts.Volumes...)
	volumeMounts := append([]corev1.VolumeMount(nil), secretArtifacts.VolumeMounts...)
	envFrom := append([]corev1.EnvFromSource(nil), secretArtifacts.EnvFrom...)

	if hasS3(in.resolvedConfig) {
		in.stepLogger.Info("Configuring pod for S3 object storage access", "bucket", in.resolvedConfig.Storage.S3.Bucket)
	}

	if tlsSecret := getTLSSecretName(&in.engram.ObjectMeta); tlsSecret != "" {
		if err := r.configureTLSEnvAndMounts(ctx, in.engram.Namespace, tlsSecret, &volumes, &volumeMounts, &envVars); err != nil {
			in.stepLogger.Error(err, "Failed to configure TLS for StepRun job", "secret", tlsSecret)
		}
	}

	startedAt := metav1.Now()
	if in.srun.Status.StartedAt != nil {
		startedAt = *in.srun.Status.StartedAt
	}
	r.addStartedAtEnv(&envVars, startedAt)

	configBytes, err := r.evaluateStepConfig(ctx, in.srun, in.storyRun)
	if err != nil {
		return jobResources{}, fmt.Errorf("failed to evaluate step config: %w", err)
	}
	envVars = append(envVars, buildConfigEnvVars(configBytes)...)

	envVars, err = appendManifestEnvVars(in.srun, envVars)
	if err != nil {
		return jobResources{}, err
	}

	resources.envVars = envVars
	resources.envFrom = envFrom
	resources.volumes = volumes
	resources.volumeMounts = volumeMounts
	return resources, nil
}

func hasS3(resolved *config.ResolvedExecutionConfig) bool {
	return resolved != nil && resolved.Storage != nil && resolved.Storage.S3 != nil
}

func (r *StepRunReconciler) ensureStorageSecret(ctx context.Context, targetNamespace string, resolved *config.ResolvedExecutionConfig, logger *logging.ControllerLogger) error {
	if !hasS3(resolved) || resolved.Storage.S3.Authentication.SecretRef == nil {
		return nil
	}
	sourceNamespace := r.ConfigResolver.ConfigNamespace()
	if sourceNamespace == "" || sourceNamespace == targetNamespace {
		return nil
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	secretName := resolved.Storage.S3.Authentication.SecretRef.Name
	if err := kubeutil.EnsureSecretCopy(ctx, reader, r.Client, sourceNamespace, targetNamespace, secretName); err != nil {
		logger.Error(err, "Failed to sync storage secret", "secret", secretName, "sourceNamespace", sourceNamespace)
		return err
	}
	logger.V(1).Info("Synced storage secret into workload namespace", "secret", secretName, "sourceNamespace", sourceNamespace)
	return nil
}

func appendManifestEnvVars(srun *runsv1alpha1.StepRun, envVars []corev1.EnvVar) ([]corev1.EnvVar, error) {
	if srun == nil || len(srun.Spec.RequestedManifest) == 0 {
		return envVars, nil
	}
	manifestBytes, err := json.Marshal(srun.Spec.RequestedManifest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal manifest spec: %w", err)
	}
	envVars = append(envVars, corev1.EnvVar{
		Name:  contracts.ManifestSpecEnv,
		Value: string(manifestBytes),
	})
	return envVars, nil
}

// prepareExecutionContext gathers all context needed for Job/Deployment creation.
//
// Behavior:
//   - Fetches Story, StoryRun, and resolves execution config.
//   - Resolves inputs and computes step timeout.
//   - Computes downstream targets for realtime routing.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StepRun: the StepRun.
//   - engram *v1alpha1.Engram: the Engram configuration.
//   - engramTemplate *catalogv1alpha1.EngramTemplate: the template.
//   - stepLogger *logging.ControllerLogger: for logging.
//
// Returns:
//   - All execution context values, or error on failure.
func (r *StepRunReconciler) prepareExecutionContext(ctx context.Context, srun *runsv1alpha1.StepRun, engram *v1alpha1.Engram, engramTemplate *catalogv1alpha1.EngramTemplate, stepLogger *logging.ControllerLogger) (*v1alpha1.Story, *runsv1alpha1.StoryRun, *config.ResolvedExecutionConfig, []byte, time.Duration, []runsv1alpha1.DownstreamTarget, []string, error) {
	story, err := r.getStoryForStep(ctx, srun)
	if err != nil {
		return nil, nil, nil, nil, 0, nil, nil, fmt.Errorf("failed to get story for step: %w", err)
	}
	storyRun, err := r.getParentStoryRun(ctx, srun)
	if err != nil {
		return nil, nil, nil, nil, 0, nil, nil, fmt.Errorf("failed to get parent storyrun: %w", err)
	}
	resolvedConfig, err := r.ConfigResolver.ResolveExecutionConfig(ctx, srun, story, engram, engramTemplate, nil)
	if err != nil {
		return nil, nil, nil, nil, 0, nil, nil, fmt.Errorf("failed to resolve execution config for step '%s': %w", srun.Name, err)
	}
	stepLogger.Info("Resolved ServiceAccountName", "sa", resolvedConfig.ServiceAccountName)
	inputBytes, err := r.resolveRunScopedInputs(ctx, srun, storyRun)
	if err != nil {
		return nil, nil, nil, nil, 0, nil, nil, err
	}
	stepTimeout := r.computeStepTimeout(srun, story, stepLogger)
	downstreamTargets, dependentSteps, err := r.computeDownstreamTargets(ctx, story, srun)
	if err != nil {
		return nil, nil, nil, nil, 0, nil, nil, err
	}
	return story, storyRun, resolvedConfig, inputBytes, stepTimeout, downstreamTargets, dependentSteps, nil
}

// computeStepTimeout selects the effective timeout for a StepRun.
//
// Behavior:
//   - Starts from Controller.DefaultStepTimeout (or 30m if unset).
//   - Overrides with StepRun.Spec.Timeout if valid.
//   - Finally honors Story.Spec.Policy.Timeouts.Step when present.
//   - Logs invalid duration strings before falling back.
//
// Arguments:
//   - srun *runsv1alpha1.StepRun: supplies optional per-step timeout.
//   - story *v1alpha1.Story: provides policy-level overrides.
//   - stepLogger *logging.ControllerLogger: for logging timeout selection.
//
// Returns:
//   - time.Duration: the computed timeout.
func (r *StepRunReconciler) computeStepTimeout(srun *runsv1alpha1.StepRun, story *v1alpha1.Story, stepLogger *logging.ControllerLogger) time.Duration {
	timeout := r.baseStepTimeout(story, srun)

	if srun != nil && srun.Spec.Timeout != "" {
		if parsed, err := parsePositiveDuration(srun.Spec.Timeout); err == nil {
			return parsed
		} else {
			stepLogger.Error(err, "Invalid step timeout on StepRun, using defaults", "rawTimeout", srun.Spec.Timeout)
		}
	}

	if policyTimeout, hasPolicy, err := parseStoryPolicyTimeout(story); hasPolicy {
		if err != nil {
			stepLogger.Error(err, "Invalid step timeout in Story policy, using default")
		} else {
			stepLogger.Info("Using Story-level step timeout", "timeout", policyTimeout)
			return policyTimeout
		}
	}

	return timeout
}

func (r *StepRunReconciler) baseStepTimeout(story *v1alpha1.Story, srun *runsv1alpha1.StepRun) time.Duration {
	controllerConfig := r.ConfigResolver.GetOperatorConfig().Controller
	timeout := controllerConfig.DefaultStepTimeout

	if story != nil && srun != nil {
		if storyStep := findStoryStep(story, srun.Spec.StepID); storyStep != nil {
			switch storyStep.Type {
			case enums.StepTypeGate:
				if controllerConfig.ApprovalDefaultTimeout > 0 {
					timeout = controllerConfig.ApprovalDefaultTimeout
				}
			case enums.StepTypeWait:
				if controllerConfig.ExternalDataTimeout > 0 {
					timeout = controllerConfig.ExternalDataTimeout
				}
			case enums.StepTypeCondition:
				if controllerConfig.ConditionalTimeout > 0 {
					timeout = controllerConfig.ConditionalTimeout
				}
			}
		}
	}

	if timeout == 0 {
		timeout = 30 * time.Minute
	}
	return timeout
}

func parsePositiveDuration(raw string) (time.Duration, error) {
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse duration: %w", err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	return parsed, nil
}

func parseStoryPolicyTimeout(story *v1alpha1.Story) (time.Duration, bool, error) {
	if story == nil || story.Spec.Policy == nil || story.Spec.Policy.Timeouts == nil || story.Spec.Policy.Timeouts.Step == nil {
		return 0, false, nil
	}
	duration, err := parsePositiveDuration(*story.Spec.Policy.Timeouts.Step)
	if err != nil {
		return 0, true, err
	}
	return duration, true, nil
}

// computeDownstreamTargets finds realtime Engrams that depend on this StepRun.
//
// Behavior:
//   - Uses buildDependencyGraphs to find dependent steps.
//   - Resolves each step's Engram and filters to Deployment/StatefulSet modes.
//   - Constructs `name.namespace.svc:<port>` endpoints.
//   - Deduplicates before returning DownstreamTarget entries.
//
// Arguments:
//   - ctx context.Context: for Engram GET calls.
//   - story *v1alpha1.Story: supplies the step graph.
//   - srun *runsv1alpha1.StepRun: provides step ID and namespace.
//
// Returns:
//   - []runsv1alpha1.DownstreamTarget: unique GRPC endpoints.
//   - []string: dependent step names.
//   - error: non-nil for unrecoverable API failures.
func (r *StepRunReconciler) computeDownstreamTargets(ctx context.Context, story *v1alpha1.Story, srun *runsv1alpha1.StepRun) ([]runsv1alpha1.DownstreamTarget, []string, error) {
	if story == nil || srun == nil {
		return nil, nil, nil
	}

	_, dependents := buildDependencyGraphs(story.Spec.Steps)
	dependentSet := dependents[srun.Spec.StepID]
	if len(dependentSet) == 0 {
		return nil, nil, nil
	}

	stepIndex := make(map[string]*v1alpha1.Step, len(story.Spec.Steps))
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		stepIndex[step.Name] = step
	}

	port := r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort
	dependentNames := make([]string, 0, len(dependentSet))
	for name := range dependentSet {
		dependentNames = append(dependentNames, name)
	}
	sort.Strings(dependentNames)

	log := logging.NewControllerLogger(ctx, "steprun-hybrid").WithStepRun(srun)
	seenEndpoints := make(map[string]struct{})
	targets := make([]runsv1alpha1.DownstreamTarget, 0, len(dependentNames))

	for _, depName := range dependentNames {
		depStep := stepIndex[depName]
		if depStep == nil || depStep.Ref == nil {
			continue
		}

		depEngram := &v1alpha1.Engram{}
		key := depStep.Ref.ToNamespacedName(srun)
		if err := r.Get(ctx, key, depEngram); err != nil {
			if errors.IsNotFound(err) {
				log.Info("Skipping downstream target; referenced engram not found", "dependentStep", depName, "engram", key.Name)
				continue
			}
			return nil, nil, fmt.Errorf("failed to resolve downstream engram '%s' for step '%s': %w", key.Name, depName, err)
		}

		mode := depEngram.Spec.Mode
		if mode == "" {
			mode = enums.WorkloadModeJob
		}
		if mode != enums.WorkloadModeDeployment && mode != enums.WorkloadModeStatefulSet {
			continue
		}

		endpoint := fmt.Sprintf("%s.%s.svc:%d", depEngram.Name, depEngram.Namespace, port)
		if _, exists := seenEndpoints[endpoint]; exists {
			continue
		}
		seenEndpoints[endpoint] = struct{}{}

		targets = append(targets, runsv1alpha1.DownstreamTarget{
			GRPCTarget: &runsv1alpha1.GRPCTarget{Endpoint: endpoint},
		})
	}

	return targets, dependentNames, nil
}

// ensureDownstreamTargets validates and patches StepRun downstream targets.
//
// Behavior:
//   - Validates desired targets via pkg/runs/validation.
//   - Patches StepRun.Spec.DownstreamTargets if changed.
//   - Emits events with hashed endpoints.
//   - Records mutation metrics.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StepRun: the StepRun to update.
//   - desired []runsv1alpha1.DownstreamTarget: target list.
//   - dependentSteps []string: names of dependent steps.
//   - logger *logging.ControllerLogger: for logging.
//
// Returns:
//   - error: non-nil on validation or patch failure.
func (r *StepRunReconciler) ensureDownstreamTargets(ctx context.Context, srun *runsv1alpha1.StepRun, desired []runsv1alpha1.DownstreamTarget, dependentSteps []string, logger *logging.ControllerLogger) error {
	stepSummary := strings.Join(dependentSteps, ",")
	if stepSummary == "" {
		stepSummary = metrics.EmptyHashSummary
	}
	if err := runsvalidation.ValidateDownstreamTargets(desired); err != nil {
		logger.Error(err, "Refusing to update downstream targets due to validation failure")
		if r.Recorder != nil {
			r.Recorder.Eventf(
				srun,
				corev1.EventTypeWarning,
				eventReasonDownstreamTargetsRejected,
				"Rejected %d downstream target(s) for dependent steps [%s]: %v",
				len(desired),
				stepSummary,
				err,
			)
		}
		return err
	}
	desiredHashes := summarizeDownstreamTargetHashes(desired)
	if reflect.DeepEqual(srun.Spec.DownstreamTargets, desired) {
		return nil
	}

	before := srun.DeepCopy()
	prevHashes := summarizeDownstreamTargetHashes(before.Spec.DownstreamTargets)
	formatHashes := func(hashes []string) string {
		if len(hashes) == 0 {
			return metrics.EmptyHashSummary
		}
		return strings.Join(hashes, "|")
	}
	if len(desired) == 0 {
		srun.Spec.DownstreamTargets = nil
	} else {
		srun.Spec.DownstreamTargets = desired
	}

	if err := r.Patch(ctx, srun, client.MergeFrom(before)); err != nil {
		logger.Error(err, "Failed to update StepRun downstream targets")
		return err
	}
	action := "updated"
	if len(desired) == 0 {
		action = "cleared"
	}
	hashLabel := formatHashes(desiredHashes)
	if len(desired) == 0 {
		hashLabel = formatHashes(prevHashes)
	}
	metrics.RecordDownstreamTargetMutation(srun.Namespace, srun.Spec.StepID, action, hashLabel)

	if len(desired) > 0 {
		logger.Info("Updated StepRun downstream targets", "count", len(desired), "endpointHashes", desiredHashes)
		hashText := strings.Join(desiredHashes, ",")
		if hashText == "" {
			hashText = metrics.EmptyHashSummary
		}
		if r.Recorder != nil {
			r.Recorder.Eventf(
				srun,
				corev1.EventTypeNormal,
				eventReasonDownstreamTargetsUpdated,
				"Updated %d downstream target(s) for dependent steps [%s] (hashes=%s)",
				len(desired),
				stepSummary,
				hashText,
			)
		}
	} else {
		logger.Info("Cleared StepRun downstream targets", "previousEndpointHashes", prevHashes)
		hashText := strings.Join(prevHashes, ",")
		if hashText == "" {
			hashText = metrics.EmptyHashSummary
		}
		if r.Recorder != nil {
			r.Recorder.Eventf(
				srun,
				corev1.EventTypeNormal,
				eventReasonDownstreamTargetsCleared,
				"Cleared StepRun downstream targets for dependent steps [%s] (previous hashes=%s)",
				stepSummary,
				hashText,
			)
		}
	}
	return nil
}

// summarizeDownstreamTargetHashes generates short SHA256 hashes for logging.
//
// Behavior:
//   - Returns nil for empty targets.
//   - Hashes GRPCTarget.Endpoint or Terminate.StopMode.
//   - Returns 8-char hex prefix for each target.
//
// Arguments:
//   - targets []runsv1alpha1.DownstreamTarget: targets to hash.
//
// Returns:
//   - []string: hash prefixes for each target.
func summarizeDownstreamTargetHashes(targets []runsv1alpha1.DownstreamTarget) []string {
	if len(targets) == 0 {
		return nil
	}
	hashes := make([]string, 0, len(targets))
	for _, target := range targets {
		var payload string
		switch {
		case target.GRPCTarget != nil:
			payload = strings.TrimSpace(target.GRPCTarget.Endpoint)
		case target.Terminate != nil:
			payload = string(target.Terminate.StopMode)
		}
		if payload == "" {
			continue
		}
		sum := sha256.Sum256([]byte(payload))
		hashes = append(hashes, fmt.Sprintf("%.8x", sum[:4]))
	}
	return hashes
}

// buildBaseEnvVars constructs the base environment variables for step execution.
//
// Behavior:
//   - Sets story/step/run context variables.
//   - Sets runtime config from operator config.
//   - Includes downward API variables for pod metadata.
//
// Arguments:
//   - srun *runsv1alpha1.StepRun: the StepRun.
//   - story *v1alpha1.Story: the parent Story.
//   - inputBytes []byte: serialized inputs.
//   - stepTimeout time.Duration: the timeout value.
//   - executionMode string: job/hybrid mode.
//   - debugEnabled bool: whether debug logs are enabled.
//
// Returns:
//   - []corev1.EnvVar: the environment variables.
func (r *StepRunReconciler) buildBaseEnvVars(srun *runsv1alpha1.StepRun, story *v1alpha1.Story, inputBytes []byte, stepTimeout time.Duration, executionMode string, resolvedConfig *config.ResolvedExecutionConfig, debugEnabled bool) []corev1.EnvVar {
	storyName := ""
	if story != nil {
		storyName = story.Name
	}
	operatorCfg := r.ConfigResolver.GetOperatorConfig()
	runtimeEnvCfg := toRuntimeEnvConfig(operatorCfg.Controller.Engram.EngramControllerConfig)
	maxInlineSize := runtimeEnvCfg.DefaultMaxInlineSize
	if resolvedConfig != nil {
		maxInlineSize = resolvedConfig.MaxInlineSize
	}

	envVars := []corev1.EnvVar{
		{Name: contracts.StoryNameEnv, Value: storyName},
		{Name: contracts.StoryRunIDEnv, Value: srun.Spec.StoryRunRef.Name},
		{Name: contracts.StepNameEnv, Value: srun.Spec.StepID},
		{Name: contracts.StepRunNameEnv, Value: srun.Name},
		{Name: contracts.StepRunNamespaceEnv, Value: srun.Namespace},
		{Name: contracts.InputsEnv, Value: string(inputBytes)},
		{Name: contracts.ExecutionModeEnv, Value: executionMode},
		{Name: contracts.HybridBridgeEnv, Value: strconv.FormatBool(executionMode == executionModeHybrid)},
		{Name: contracts.GRPCPortEnv, Value: fmt.Sprintf("%d", runtimeEnvCfg.DefaultGRPCPort)},
		{Name: contracts.MaxInlineSizeEnv, Value: fmt.Sprintf("%d", maxInlineSize)},
		{Name: contracts.StorageTimeoutEnv, Value: fmt.Sprintf("%ds", runtimeEnvCfg.DefaultStorageTimeoutSeconds)},
		{Name: contracts.StepTimeoutEnv, Value: stepTimeout.String()},
		{Name: contracts.MaxRecursionDepthEnv, Value: "64"},
		{Name: contracts.DebugEnv, Value: strconv.FormatBool(debugEnabled)},
		// Downward API: expose pod metadata to containers via env
		{Name: contracts.PodNameEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: contracts.PodNamespaceEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: contracts.ServiceAccountNameEnv, ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.serviceAccountName"}}},
	}
	return envVars
}

// addStartedAtEnv appends BUBU_STARTED_AT to env vars via core helper.
//
// Behavior:
//   - Delegates to coreenv.AppendStartedAtEnv for RFC3339Nano timestamp.
//
// Arguments:
//   - envVars *[]corev1.EnvVar: slice to append to.
//   - startedAt metav1.Time: the start timestamp.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Appends to envVars slice.
func (r *StepRunReconciler) addStartedAtEnv(envVars *[]corev1.EnvVar, startedAt metav1.Time) {
	coreenv.AppendStartedAtEnv(envVars, startedAt)
}

// buildJobSpec constructs a Job manifest for the StepRun.
//
// Behavior:
//   - Builds pod template with resolved config and env vars.
//   - Applies storage env to container.
//   - Sets labels for informer selectors.
//
// Arguments:
//   - srun *runsv1alpha1.StepRun: the StepRun owner.
//   - resolvedConfig *config.ResolvedExecutionConfig: resolved settings.
//   - envVars []corev1.EnvVar: environment variables.
//   - envFrom []corev1.EnvFromSource: env sources.
//   - volumes []corev1.Volume: pod volumes.
//   - volumeMounts []corev1.VolumeMount: container mounts.
//   - activeDeadlineSeconds int64: job timeout.
//   - storageTimeout string: formatted storage timeout.
//
// Returns:
//   - *batchv1.Job: the constructed Job.
func (r *StepRunReconciler) buildJobSpec(
	srun *runsv1alpha1.StepRun,
	resolvedConfig *config.ResolvedExecutionConfig,
	envVars []corev1.EnvVar,
	envFrom []corev1.EnvFromSource,
	volumes []corev1.Volume,
	volumeMounts []corev1.VolumeMount,
	activeDeadlineSeconds int64,
	storageTimeout string,
) *batchv1.Job {
	operatorCfg := r.ConfigResolver.GetOperatorConfig()
	podTemplate := r.buildRuntimePodTemplate(
		resolvedConfig,
		operatorCfg,
		map[string]string{contracts.StepRunLabelKey: srun.Name},
		nil,
		envVars,
		envFrom,
		volumes,
		volumeMounts,
		nil,
		resolvedConfig.RestartPolicy,
	)

	if len(podTemplate.Spec.Containers) > 0 {
		podspec.ApplyStorageEnv(resolvedConfig, &podTemplate.Spec, &podTemplate.Spec.Containers[0], storageTimeout)
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      srun.Name,
			Namespace: srun.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "bobrapet",
				"app.kubernetes.io/component": "engram",
				contracts.StepRunLabelKey:     srun.Name,
				contracts.StoryRunLegacyLabel: srun.Spec.StoryRunRef.Name,
			},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			BackoffLimit:            &resolvedConfig.BackoffLimit,
			TTLSecondsAfterFinished: &resolvedConfig.TTLSecondsAfterFinished,
			Template:                podTemplate,
		},
	}
}

// buildRuntimePodTemplate constructs PodTemplateSpec for jobs and deployments.
//
// Behavior:
//   - Delegates to podspec.Build for shared pod template construction.
//   - Sets container name to "engram" for StepRun workloads.
//   - Applies termination grace period from operator config.
//
// Arguments:
//   - resolvedConfig *config.ResolvedExecutionConfig: resolved settings.
//   - operatorCfg *config.OperatorConfig: operator config.
//   - labels map[string]string: pod labels.
//   - annotations map[string]string: pod annotations.
//   - envVars []corev1.EnvVar: environment variables.
//   - envFrom []corev1.EnvFromSource: env sources.
//   - volumes []corev1.Volume: pod volumes.
//   - volumeMounts []corev1.VolumeMount: container mounts.
//   - ports []corev1.ContainerPort: container ports.
//   - restartPolicy corev1.RestartPolicy: pod restart policy.
//
// Returns:
//   - corev1.PodTemplateSpec: the constructed template.
func (r *StepRunReconciler) buildRuntimePodTemplate(
	resolvedConfig *config.ResolvedExecutionConfig,
	operatorCfg *config.OperatorConfig,
	labels map[string]string,
	annotations map[string]string,
	envVars []corev1.EnvVar,
	envFrom []corev1.EnvFromSource,
	volumes []corev1.Volume,
	volumeMounts []corev1.VolumeMount,
	ports []corev1.ContainerPort,
	restartPolicy corev1.RestartPolicy,
) corev1.PodTemplateSpec {
	return podspec.Build(podspec.Config{
		ContainerName:             "engram",
		Labels:                    labels,
		Annotations:               annotations,
		EnvVars:                   envVars,
		EnvFrom:                   envFrom,
		Volumes:                   volumes,
		VolumeMounts:              volumeMounts,
		Ports:                     ports,
		RestartPolicy:             restartPolicy,
		TerminationGracePeriodSec: int64(operatorCfg.Controller.Engram.EngramControllerConfig.DefaultTerminationGracePeriodSeconds),
		ResolvedConfig:            resolvedConfig,
	})
}

// getStoryRunInputs fetches initial inputs from the parent StoryRun.
//
// Behavior:
//   - Delegates to runsinputs.DecodeStoryRunInputs.
//
// Arguments:
//   - _ context.Context: unused.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//
// Returns:
//   - map[string]any: decoded inputs.
//   - error: non-nil on decode failure.
func (r *StepRunReconciler) getStoryRunInputs(_ context.Context, storyRun *runsv1alpha1.StoryRun) (map[string]any, error) {
	return runsinputs.DecodeStoryRunInputs(storyRun)
}

// getParentStoryRun fetches the StoryRun that owns this StepRun.
//
// Behavior:
//   - Validates storyRunRef.name is present.
//   - Uses refs.LoadNamespacedReference for fetch.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - srun *runsv1alpha1.StepRun: the StepRun.
//
// Returns:
//   - *runsv1alpha1.StoryRun: the parent StoryRun.
//   - error: non-nil on missing ref or fetch failure.
func (r *StepRunReconciler) getParentStoryRun(ctx context.Context, srun *runsv1alpha1.StepRun) (*runsv1alpha1.StoryRun, error) {
	if strings.TrimSpace(srun.Spec.StoryRunRef.Name) == "" {
		return nil, fmt.Errorf("steprun %s missing spec.storyRunRef.name", srun.Name)
	}
	storyRun, key, err := refs.LoadNamespacedReference(
		ctx,
		r.Client,
		srun,
		&srun.Spec.StoryRunRef,
		func() *runsv1alpha1.StoryRun { return &runsv1alpha1.StoryRun{} },
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("get parent StoryRun %s: %w", key.String(), err)
	}
	return storyRun, nil
}

// handleJobStatus reconciles StepRun status with observed Job status.
//
// Behavior:
//   - Lets SDK patches win for terminal phases.
//   - Applies fallback updates when SDK fails to write.
//   - Extracts exit codes for granular failure handling.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to update.
//   - job *batchv1.Job: the observed Job.
//
// Returns:
//   - ctrl.Result: reconcile result.
//   - error: non-nil on patch failures.
func (r *StepRunReconciler) handleJobStatus(ctx context.Context, step *runsv1alpha1.StepRun, job *batchv1.Job) (ctrl.Result, error) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(step).WithValues("job_name", job.Name)

	r.ensureRetriesPatched(ctx, step, job.Status.Failed, stepLogger)

	switch {
	case job.Status.Succeeded > 0:
		return ctrl.Result{}, r.handleJobSucceeded(ctx, step, stepLogger)
	case job.Status.Failed > 0:
		return ctrl.Result{}, r.handleJobFailed(ctx, step, job, stepLogger)
	default:
		return ctrl.Result{}, nil
	}
}

func (r *StepRunReconciler) handleJobSucceeded(ctx context.Context, step *runsv1alpha1.StepRun, logger *logging.ControllerLogger) error {
	logger.Info("Job succeeded. Verifying StepRun status.")
	if step.Status.Phase != enums.PhaseSucceeded {
		logger.Info("Job pod exited 0, but Engram SDK did not patch StepRun status to Succeeded. Controller will mark as Succeeded as a fallback.")
		successMsg := "StepRun pod completed successfully; final status updated by controller."
		if err := r.setStepRunPhase(ctx, step, enums.PhaseSucceeded, successMsg); err != nil {
			logger.Error(err, "Failed to update StepRun status to Succeeded")
			return err
		}
		if r.Recorder != nil {
			r.Recorder.Event(step, corev1.EventTypeNormal, eventReasonJobPatched, "StepRun job succeeded but controller patched status because the SDK had not updated it")
		}
	}
	if r.Recorder != nil {
		r.Recorder.Event(step, corev1.EventTypeNormal, eventReasonJobSucceeded, "StepRun job completed successfully")
	}
	return nil
}

func (r *StepRunReconciler) handleJobFailed(ctx context.Context, step *runsv1alpha1.StepRun, job *batchv1.Job, logger *logging.ControllerLogger) error {
	exitCode := r.extractPodExitCode(ctx, job)

	if r.stepStatusPatchedBySDK(step) {
		logger.Info("Job failed but SDK already updated StepRun status; preserving SDK's error details",
			"podExitCode", exitCode,
			"currentPhase", step.Status.Phase)
		r.ensureExitCodePatched(ctx, step, exitCode, logger)
		return nil
	}

	return r.applyFailureFallback(ctx, step, exitCode, logger)
}

func (r *StepRunReconciler) stepStatusPatchedBySDK(step *runsv1alpha1.StepRun) bool {
	return step.Status.Phase == enums.PhaseFailed || step.Status.Phase == enums.PhaseTimeout
}

func (r *StepRunReconciler) ensureExitCodePatched(ctx context.Context, step *runsv1alpha1.StepRun, exitCode int, logger *logging.ControllerLogger) {
	if exitCode == 0 || step.Status.ExitCode != 0 {
		return
	}
	if err := kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		sr.Status.ExitCode = int32(exitCode)
		sr.Status.ExitClass = classifyExitCode(exitCode)
	}); err != nil {
		logger.Error(err, "Failed to patch exit code into StepRun status")
	}
}

func (r *StepRunReconciler) ensureRetriesPatched(ctx context.Context, step *runsv1alpha1.StepRun, failedCount int32, logger *logging.ControllerLogger) {
	if failedCount == step.Status.Retries {
		return
	}
	if err := kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		sr.Status.Retries = failedCount
	}); err != nil {
		logger.Error(err, "Failed to patch retries into StepRun status")
	}
}

func (r *StepRunReconciler) applyFailureFallback(ctx context.Context, step *runsv1alpha1.StepRun, exitCode int, logger *logging.ControllerLogger) error {
	logger.Info("Job failed and SDK did not update status; applying fallback status", "podExitCode", exitCode)

	phase := enums.PhaseFailed
	message := "Job execution failed. Check pod logs for details."
	if exitCode == 124 {
		phase = enums.PhaseTimeout
		message = "Job execution timed out. Check step timeout configuration and pod logs."
	}

	if err := r.setStepRunPhase(ctx, step, phase, message); err != nil {
		logger.Error(err, "Failed to update StepRun status after job failure")
		return err
	}

	if err := kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StepRun)
		sr.Status.LastFailureMsg = message
		if exitCode != 0 {
			sr.Status.ExitCode = int32(exitCode)
			sr.Status.ExitClass = classifyExitCode(exitCode)
		}
	}); err != nil {
		logger.Error(err, "Failed to patch StepRun failure metadata after job failure")
		return err
	}

	if r.Recorder != nil {
		eventMsg := fmt.Sprintf("StepRun job failed (exit=%d); controller patched status because the SDK had not updated it", exitCode)
		r.Recorder.Event(step, corev1.EventTypeWarning, eventReasonJobFallback, eventMsg)
	}
	return nil
}

// extractPodExitCode retrieves exit code from the Job's failed pod.
//
// Behavior:
//   - Lists pods owned by the Job.
//   - Returns exit code from most recent failed pod.
//   - Returns 0 if pod or exit code cannot be determined.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - job *batchv1.Job: the Job to inspect.
//
// Returns:
//   - int: the exit code (0 if unknown).
func (r *StepRunReconciler) extractPodExitCode(ctx context.Context, job *batchv1.Job) int {
	// List pods owned by this Job
	var podList corev1.PodList
	if err := kubeutil.ListByLabels(ctx, r.Client, job.Namespace, map[string]string{"job-name": job.Name}, &podList); err != nil {
		return 0
	}

	// Find the most recent failed pod
	for i := len(podList.Items) - 1; i >= 0; i-- {
		pod := &podList.Items[i]
		if pod.Status.Phase == corev1.PodFailed {
			// Check container statuses for exit code
			for _, cs := range pod.Status.ContainerStatuses {
				if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
					return int(cs.State.Terminated.ExitCode)
				}
			}
		}
	}
	return 0
}

// reconcileRunScopedRealtimeStep loads the parent StoryRun, resolves runtime
// config, ensures the per-step TransportBinding, and reconciles the run-scoped
// Service + Deployment before updating StepRun status/conditions based on
// binding/deployment readiness (internal/controller/runs/steprun_controller.go:1090-1197).
// Behavior:
//   - Fetches the StoryRun + execution config, skips pre-evaluating realtime inputs, and computes the step/storage
//     timeouts plus the deterministic binding name (lines 1092-1114).
//   - Calls ensureRunTransportBinding; paused vs blocked phases and the next requeue delay depend on whether the
//     binding is pending or failed (lines 1115-1131).
//   - Resolves the binding env payload, writes sanitized Engram annotations, and decodes the payload to determine
//     insecure transport allowances before reconciling the Service and Deployment (lines 1133-1159,1484-1504).
//   - Reads TransportBinding + Deployment readiness, sets the StepRun phase and transport Ready condition, and
//     requeues until both report Ready (lines 1160-1194).
//
// Args:
//   - ctx context.Context: propagate cancellation to client calls.
//   - step *runsv1alpha1.StepRun: resource receiving status updates.
//   - story *v1alpha1.Story: optional Story spec used when building env vars.
//   - stepSpec *v1alpha1.Step: defines realtime mode specifics passed into binding/workload builders.
//   - engram *v1alpha1.Engram: influences binding permissions and insecure transport checks.
//   - template *catalogv1alpha1.EngramTemplate: forwarded to workload builders.
//
// Returns:
//   - ctrl.Result: requeues with nextRequeueDelay until dependencies report ready; returns empty result once ready.
//   - error: bubbled up from binding/service/deployment reconciliation or status patches.
func (r *StepRunReconciler) reconcileRunScopedRealtimeStep(ctx context.Context, step *runsv1alpha1.StepRun, story *v1alpha1.Story, stepSpec *v1alpha1.Step, engram *v1alpha1.Engram, template *catalogv1alpha1.EngramTemplate) (ctrl.Result, error) {
	rtx, err := r.buildRealtimeRuntimeContext(ctx, step, story, stepSpec, engram, template)
	if err != nil {
		return ctrl.Result{}, err
	}

	binding, result, err := r.ensureRealtimeBinding(ctx, step, story, stepSpec, rtx)
	if result != nil || err != nil {
		if result != nil {
			return *result, err
		}
		return ctrl.Result{}, err
	}

	envValue, bindingInfo := r.prepareBindingEnv(ctx, step, binding, rtx)
	if err := r.ensureEngramTransportAnnotation(ctx, engram, envValue, step); err != nil {
		rtx.logger.Error(err, "Failed to annotate engram with transport binding")
	}

	allowInsecure := transportutil.AllowInsecureTransport(engram, bindingInfo)
	if err := r.ensureRealtimeService(ctx, step, story, rtx); err != nil {
		return ctrl.Result{}, err
	}

	return r.ensureRealtimeDeployment(ctx, step, story, engram, template, binding, envValue, bindingInfo, allowInsecure, rtx)
}

type realtimeRuntimeContext struct {
	logger         *logging.ControllerLogger
	storyRun       *runsv1alpha1.StoryRun
	resolvedCfg    *config.ResolvedExecutionConfig
	stepTimeout    time.Duration
	storageTimeout string
	bindingName    string
}

func (r *StepRunReconciler) buildRealtimeRuntimeContext(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	stepSpec *v1alpha1.Step,
	engram *v1alpha1.Engram,
	template *catalogv1alpha1.EngramTemplate,
) (*realtimeRuntimeContext, error) {
	logger := logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step).WithValues("mode", "run-scoped-realtime")

	storyRun, err := r.getParentStoryRun(ctx, step)
	if err != nil {
		logger.Error(err, "Failed to load parent StoryRun")
		return nil, err
	}

	resolvedCfg, err := r.ConfigResolver.ResolveExecutionConfig(ctx, step, story, engram, template, stepSpec)
	if err != nil {
		logger.Error(err, "Failed to resolve execution config for realtime StepRun")
		return nil, err
	}

	stepTimeout := r.computeStepTimeout(step, story, logger)
	runtimeEnvCfg := toRuntimeEnvConfig(r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig)
	storageTimeout := podspec.FormatStorageTimeout(resolvedCfg, runtimeEnvCfg.DefaultStorageTimeoutSeconds)
	bindingName := transportbinding.Name(step.Spec.StoryRunRef.Name, step.Spec.StepID)

	return &realtimeRuntimeContext{
		logger:         logger,
		storyRun:       storyRun,
		resolvedCfg:    resolvedCfg,
		stepTimeout:    stepTimeout,
		storageTimeout: storageTimeout,
		bindingName:    bindingName,
	}, nil
}

func (r *StepRunReconciler) ensureRealtimeBinding(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	stepSpec *v1alpha1.Step,
	rtx *realtimeRuntimeContext,
) (*transportv1alpha1.TransportBinding, *ctrl.Result, error) {
	binding, err := r.ensureRunTransportBinding(ctx, step, rtx.storyRun, story, stepSpec, rtx.bindingName)
	switch err {
	case nil:
		return binding, nil, nil
	case errTransportBindingPending:
		r.handleBindingPending(ctx, step, rtx)
		res := ctrl.Result{RequeueAfter: r.nextRequeueDelay()}
		return nil, &res, nil
	default:
		r.handleBindingFailure(ctx, step, err, rtx)
		res := ctrl.Result{RequeueAfter: r.nextRequeueDelay()}
		return nil, &res, err
	}
}

func (r *StepRunReconciler) handleBindingPending(ctx context.Context, step *runsv1alpha1.StepRun, rtx *realtimeRuntimeContext) {
	rtx.logger.V(1).Info("Transport binding not observed yet; waiting for informer cache", "binding", rtx.bindingName)
	if err := r.setStepRunPhase(ctx, step, enums.PhasePaused, "Waiting for transport binding to become available"); err != nil {
		rtx.logger.Error(err, "Failed to update StepRun status while waiting for transport binding")
	}
	if r.Recorder != nil {
		r.Recorder.Eventf(
			step,
			corev1.EventTypeNormal,
			eventReasonTransportBindingPending,
			"Waiting for transport binding %s to become visible",
			rtx.bindingName,
		)
	}
}

func (r *StepRunReconciler) handleBindingFailure(ctx context.Context, step *runsv1alpha1.StepRun, err error, rtx *realtimeRuntimeContext) {
	rtx.logger.Error(err, "Failed to ensure transport binding for run-scoped realtime step")
	statusErr := r.setStepRunPhase(ctx, step, enums.PhaseBlocked, fmt.Sprintf("Transport binding error: %v", err))
	if statusErr != nil {
		rtx.logger.Error(statusErr, "Failed to update StepRun status after transport binding failure")
	}
}

func (r *StepRunReconciler) prepareBindingEnv(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	binding *transportv1alpha1.TransportBinding,
	rtx *realtimeRuntimeContext,
) (string, *transportpb.BindingInfo) {
	bindingEnvValue, bindingResolved := r.resolveBindingEnvValue(ctx, step.Namespace, binding.Name)
	if !bindingResolved {
		rtx.logger.V(1).Info("Falling back to binding name for env payload", "binding", rtx.bindingName)
	}
	bindingInfo, decodeErr := transportutil.DecodeBindingInfo(bindingEnvValue)
	if decodeErr != nil {
		rtx.logger.Info("Failed to decode transport binding info; env overrides may be incomplete", "binding", rtx.bindingName, "error", decodeErr)
		if r.Recorder != nil {
			r.Recorder.Eventf(
				step,
				corev1.EventTypeWarning,
				eventReasonTransportBindingDecodeFailed,
				"Failed to decode env payload for binding %s: %v",
				rtx.bindingName,
				decodeErr,
			)
		}
	}
	return bindingEnvValue, bindingInfo
}

func (r *StepRunReconciler) ensureRealtimeService(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	rtx *realtimeRuntimeContext,
) error {
	service := r.desiredRunScopedService(step, story, rtx.resolvedCfg)
	if _, err := r.reconcileRunScopedService(ctx, step, service); err != nil {
		rtx.logger.Error(err, "Failed to reconcile Service for realtime StepRun")
		return err
	}
	return nil
}

func (r *StepRunReconciler) ensureRealtimeDeployment(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	engram *v1alpha1.Engram,
	template *catalogv1alpha1.EngramTemplate,
	binding *transportv1alpha1.TransportBinding,
	bindingEnvValue string,
	bindingInfo *transportpb.BindingInfo,
	allowInsecure bool,
	rtx *realtimeRuntimeContext,
) (ctrl.Result, error) {
	deployment, err := r.desiredRunScopedDeployment(ctx, step, story, rtx.storyRun, engram, template, rtx.resolvedCfg, rtx.storageTimeout, rtx.stepTimeout, bindingEnvValue, bindingInfo, allowInsecure)
	if err != nil {
		rtx.logger.Error(err, "Failed to construct Deployment for realtime StepRun")
		return ctrl.Result{}, err
	}
	currentDeployment, err := r.reconcileRunScopedDeployment(ctx, step, deployment)
	if err != nil {
		rtx.logger.Error(err, "Failed to reconcile Deployment for realtime StepRun", "deployment", deployment.Name)
		return ctrl.Result{}, err
	}

	bindingReady, _, bindingMsg := transportbinding.ReadyState(binding)
	deploymentReady, deploymentMsg := deploymentReadyState(currentDeployment)

	phase, message, conditionMsg := deriveRealtimePhase(bindingReady, bindingMsg, deploymentReady, deploymentMsg)
	if err := r.setStepRunPhase(ctx, step, phase, message); err != nil {
		rtx.logger.Error(err, "Failed to update StepRun phase for realtime execution")
		return ctrl.Result{}, err
	}
	if err := r.setTransportCondition(ctx, step, bindingReady, conditionMsg); err != nil {
		rtx.logger.Error(err, "Failed to update transport condition on StepRun")
		return ctrl.Result{}, err
	}

	if !bindingReady || !deploymentReady {
		return ctrl.Result{RequeueAfter: r.nextRequeueDelay()}, nil
	}

	rtx.logger.Info("Run-scoped realtime runtime is ready", "deployment", currentDeployment.Name)
	return ctrl.Result{}, nil
}

func deriveRealtimePhase(bindingReady bool, bindingMsg string, deploymentReady bool, deploymentMsg string) (enums.Phase, string, string) {
	if !bindingReady {
		if bindingMsg == "" {
			bindingMsg = "transport binding is not ready"
		}
		return enums.PhasePaused, bindingMsg, bindingMsg
	}
	if !deploymentReady {
		if deploymentMsg == "" {
			deploymentMsg = "waiting for realtime deployment readiness"
		}
		return enums.PhaseRunning, deploymentMsg, bindingMsg
	}
	return enums.PhaseRunning, "Realtime runtime is ready", bindingMsg
}

// resolveRunScopedInputs merges and evaluates inputs for the StepRun.
//
// Behavior:
//   - Merges StoryRun inputs, prior step outputs, and step input block.
//   - Evaluates with CEL to produce final JSON payload.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun with input spec.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//
// Returns:
//   - []byte: marshaled resolved inputs.
//   - error: non-nil on resolution or marshal failures.
func (r *StepRunReconciler) resolveRunScopedInputs(ctx context.Context, step *runsv1alpha1.StepRun, storyRun *runsv1alpha1.StoryRun) ([]byte, error) {
	storyInputs, err := r.getStoryRunInputs(ctx, storyRun)
	if err != nil {
		return nil, fmt.Errorf("failed to get storyrun inputs: %w", err)
	}
	stepOutputs, err := getPriorStepOutputs(ctx, r.Client, storyRun, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get prior step outputs: %w", err)
	}

	withBlock := map[string]any{}
	if step.Spec.Input != nil && len(step.Spec.Input.Raw) > 0 {
		if err := json.Unmarshal(step.Spec.Input.Raw, &withBlock); err != nil {
			return nil, fmt.Errorf("failed to unmarshal step input: %w", err)
		}
	}

	vars := map[string]any{
		"inputs": storyInputs,
		"steps":  stepOutputs,
	}
	resolvedInputs, err := r.CELEvaluator.ResolveWithInputs(ctx, withBlock, vars)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve step inputs with CEL: %w", err)
	}
	inputBytes, err := json.Marshal(resolvedInputs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal resolved step inputs: %w", err)
	}
	return inputBytes, nil
}

// cleanupRunScopedRuntime deletes realtime resources after TTL expiry.
//
// Behavior:
//   - Deletes Deployment, StatefulSet, Service, and TransportBinding.
//   - Returns true when all resources are fully deleted.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to clean up.
//
// Returns:
//   - bool: true if cleanup is complete.
//   - error: non-nil on deletion failures.
func (r *StepRunReconciler) cleanupRunScopedRuntime(ctx context.Context, step *runsv1alpha1.StepRun) (bool, error) {
	logger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(step)
	targets := []realtimeCleanupTarget{
		{
			Object:      &appsv1.Deployment{},
			Key:         types.NamespacedName{Name: step.Name, Namespace: step.Namespace},
			Description: "deployment",
			DeleteOpts:  []client.DeleteOption{client.PropagationPolicy(metav1.DeletePropagationBackground)},
		},
		{
			Object:      &appsv1.StatefulSet{},
			Key:         types.NamespacedName{Name: step.Name, Namespace: step.Namespace},
			Description: "statefulset",
			DeleteOpts:  []client.DeleteOption{client.PropagationPolicy(metav1.DeletePropagationBackground)},
		},
		{
			Object:      &corev1.Service{},
			Key:         types.NamespacedName{Name: step.Name, Namespace: step.Namespace},
			Description: "service",
		},
		{
			Object:      &transportv1alpha1.TransportBinding{},
			Key:         types.NamespacedName{Name: transportbinding.Name(step.Spec.StoryRunRef.Name, step.Spec.StepID), Namespace: step.Namespace},
			Description: "transport binding",
		},
	}

	cleaned := true
	for _, target := range targets {
		done, err := r.ensureRealtimeResourceDeleted(ctx, logger, target)
		if err != nil {
			return false, err
		}
		if !done {
			cleaned = false
		}
	}
	return cleaned, nil
}

type realtimeCleanupTarget struct {
	Object      client.Object
	Key         types.NamespacedName
	Description string
	DeleteOpts  []client.DeleteOption
}

func (r *StepRunReconciler) ensureRealtimeResourceDeleted(
	ctx context.Context,
	logger *logging.ControllerLogger,
	target realtimeCleanupTarget,
) (bool, error) {
	obj := target.Object
	if err := r.Get(ctx, target.Key, obj); err != nil {
		if errors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}

	if !obj.GetDeletionTimestamp().IsZero() {
		return false, nil
	}

	logger.Info("Deleting realtime resource after TTL expiry",
		"resource", target.Description,
		"name", target.Key.Name,
	)
	if err := r.Delete(ctx, obj, target.DeleteOpts...); err != nil && !errors.IsNotFound(err) {
		logger.Error(err, "Failed to delete realtime resource", "resource", target.Description, "name", target.Key.Name)
		return false, err
	}
	return false, nil
}

// deploymentReadyState checks if deployment has desired ready replicas.
//
// Behavior:
//   - Returns false if deployment is nil.
//   - Compares ReadyReplicas against Spec.Replicas.
//
// Arguments:
//   - deployment *appsv1.Deployment: the deployment to check.
//
// Returns:
//   - bool: true if ready.
//   - string: status message.
func deploymentReadyState(deployment *appsv1.Deployment) (bool, string) {
	if deployment == nil {
		return false, "deployment does not exist"
	}
	desired := int32(1)
	if deployment.Spec.Replicas != nil {
		desired = *deployment.Spec.Replicas
	}
	if deployment.Status.ReadyReplicas < desired {
		return false, fmt.Sprintf("ready replicas %d/%d", deployment.Status.ReadyReplicas, desired)
	}
	return true, "deployment ready"
}

// setTransportCondition patches StepRun with transport readiness condition.
//
// Behavior:
//   - Updates ConditionTransportReady via conditions.NewConditionManager.
//   - Emits events for significant readiness changes.
//   - Mirrors ObservedGeneration for consistency.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun to update.
//   - ready bool: whether transport is ready.
//   - message string: status message.
//
// Returns:
//   - error: non-nil on patch failure.
func (r *StepRunReconciler) setTransportCondition(ctx context.Context, step *runsv1alpha1.StepRun, ready bool, message string) error {
	var (
		eventReady   bool
		eventMessage string
		emitEvent    bool
	)

	if err := kubeutil.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		s := obj.(*runsv1alpha1.StepRun)
		cm := conditions.NewConditionManager(s.Generation)
		status := metav1.ConditionFalse
		reason := conditions.ReasonAwaitingTransport
		msg := message
		if ready {
			status = metav1.ConditionTrue
			reason = conditions.ReasonTransportReady
			if msg == "" {
				msg = "transport binding ready"
			}
		} else if msg == "" {
			msg = "transport binding not ready"
		}

		prev := conditions.GetCondition(s.Status.Conditions, conditions.ConditionTransportReady)
		if prev == nil || prev.Status != status || prev.Reason != reason || prev.Message != msg {
			emitEvent = true
			eventReady = ready
			eventMessage = msg
		}

		cm.SetCondition(&s.Status.Conditions, conditions.ConditionTransportReady, status, reason, msg)
		s.Status.ObservedGeneration = s.Generation
	}); err != nil {
		return err
	}

	if emitEvent && r.Recorder != nil {
		state := "not ready"
		if eventReady {
			state = "ready"
		}
		r.Recorder.Eventf(
			step,
			corev1.EventTypeNormal,
			eventReasonTransportConditionUpdated,
			"Marked transport %s (%s)",
			state,
			eventMessage,
		)
	}

	return nil
}

// resolveBindingEnvValue fetches TransportBinding and returns encoded env value.
//
// Behavior:
//   - Prefers APIReader over cached client.
//   - Delegates to transportutil.ResolveBindingEnvValue.
//   - Falls back to binding name on failures.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - namespace string: binding namespace.
//   - bindingName string: binding name.
//
// Returns:
//   - string: encoded env value or fallback name.
//   - bool: true if binding was successfully read.
func (r *StepRunReconciler) resolveBindingEnvValue(ctx context.Context, namespace, bindingName string) (string, bool) {
	reader := client.Reader(r.Client)
	readerLabel := "cached_client"
	if r.APIReader != nil {
		reader = r.APIReader
		readerLabel = "api_reader"
	}

	value := transportutil.ResolveBindingEnvValue(ctx, reader, namespace, bindingName)
	if value == "" || value == bindingName {
		metrics.RecordTransportBindingReadFallback(namespace, readerLabel)
		return bindingName, false
	}
	return value, true
}

// ensureRunTransportBinding creates/updates TransportBinding for realtime StepRun.
//
// Behavior:
//   - Resolves Story transport and merges settings.
//   - Creates or updates TransportBinding with owner references.
//   - Records mutation metrics.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun owner.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *v1alpha1.Story: the Story definition.
//   - stepSpec *v1alpha1.Step: the step definition.
//   - bindingName string: name for the TransportBinding.
//
// Returns:
//   - *transportv1alpha1.TransportBinding: the created/updated binding.
//   - error: non-nil on failures.
func (r *StepRunReconciler) ensureRunTransportBinding(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	storyRun *runsv1alpha1.StoryRun,
	story *v1alpha1.Story,
	stepSpec *v1alpha1.Step,
	bindingName string,
) (*transportv1alpha1.TransportBinding, error) {
	ctxData, err := r.buildTransportBindingContext(ctx, step, story, stepSpec, bindingName)
	if err != nil {
		return nil, err
	}

	binding := newTransportBindingResource(step.Namespace, bindingName)
	start := time.Now()
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, binding, func() error {
		return r.applyTransportBindingSpec(step, storyRun, binding, ctxData)
	})
	duration := time.Since(start)
	mutated := result == controllerutil.OperationResultCreated || result == controllerutil.OperationResultUpdated
	metrics.RecordTransportBindingOperation("steprun", ctxData.bindingAlias, mutated, duration, err)
	if err != nil {
		ctxData.logger.Error(err, "Failed to ensure transport binding", "operationResult", result)
		return nil, err
	}
	ctxData.logger.V(1).Info("Ensured transport binding",
		"operationResult", result,
		"mutated", mutated,
		"duration", duration,
		"bindingAlias", ctxData.bindingAlias,
	)

	return r.fetchBindingForReadiness(ctx, step, binding, ctxData.logger)
}

type transportBindingContext struct {
	logger       *logging.ControllerLogger
	transportObj *transportv1alpha1.Transport
	driver       string
	settings     []byte
	defaultPort  int32
	bindingAlias string
}

func (r *StepRunReconciler) buildTransportBindingContext(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	stepSpec *v1alpha1.Step,
	bindingName string,
) (*transportBindingContext, error) {
	logger := logging.NewControllerLogger(ctx, "steprun-binding").
		WithStepRun(step).
		WithValues("binding", bindingName)

	if story == nil {
		return nil, fmt.Errorf("story reference is required for realtime steps")
	}
	if stepSpec == nil {
		return nil, fmt.Errorf("story step definition is required for realtime steps")
	}

	transportObj, decl, err := transportutil.ResolveStoryTransport(ctx, r.Client, story, stepSpec.Transport)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve transport %q for story %s: %w", stepSpec.Transport, story.Name, err)
	}
	if transportObj == nil {
		return nil, fmt.Errorf("story %s does not declare any transports", story.Name)
	}

	driver := strings.TrimSpace(transportObj.Spec.Driver)
	if driver == "" {
		return nil, fmt.Errorf("transport %s is missing driver identifier", transportObj.Name)
	}
	logger = logger.WithValues("transportDriver", driver)

	settingsBytes, err := transportutil.MergeSettings(transportObj.Spec.DefaultSettings, decl.Settings)
	if err != nil {
		logger.Error(err, "Failed to merge transport settings", "transport", transportObj.Name)
		r.recordTransportBindingWarning(step, eventReasonTransportBindingMergeFailed, fmt.Sprintf("Failed to merge settings for transport %s: %v", transportObj.Name, err))
		return nil, fmt.Errorf("failed to merge transport settings: %w", err)
	}

	defaultPort, err := r.resolveConnectorPort(logger)
	if err != nil {
		return nil, err
	}

	bindingAlias := bindingName
	if step.Namespace != "" {
		bindingAlias = fmt.Sprintf("%s/%s", step.Namespace, bindingName)
	}

	return &transportBindingContext{
		logger:       logger,
		transportObj: transportObj,
		driver:       driver,
		settings:     settingsBytes,
		defaultPort:  defaultPort,
		bindingAlias: bindingAlias,
	}, nil
}

func (r *StepRunReconciler) resolveConnectorPort(logger *logging.ControllerLogger) (int32, error) {
	cfgResolver := r.ConfigResolver
	if cfgResolver == nil {
		err := fmt.Errorf("config resolver is not initialized")
		logger.Error(err, "Cannot determine connector endpoint port")
		return 0, err
	}
	operatorCfg := cfgResolver.GetOperatorConfig()
	if operatorCfg == nil {
		err := fmt.Errorf("operator config is not loaded")
		logger.Error(err, "Cannot determine connector endpoint port")
		return 0, err
	}
	return int32(operatorCfg.Controller.Engram.EngramControllerConfig.DefaultGRPCPort), nil
}

func newTransportBindingResource(namespace, name string) *transportv1alpha1.TransportBinding {
	return &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func (r *StepRunReconciler) applyTransportBindingSpec(
	step *runsv1alpha1.StepRun,
	storyRun *runsv1alpha1.StoryRun,
	binding *transportv1alpha1.TransportBinding,
	ctxData *transportBindingContext,
) error {
	binding.Spec.TransportRef = ctxData.transportObj.Name
	binding.Spec.Driver = ctxData.driver
	binding.Spec.StepName = step.Spec.StepID
	binding.Spec.EngramName = step.Name
	binding.Spec.StoryRunRef = &refs.StoryRunReference{
		ObjectReference: refs.ObjectReference{
			Name:      storyRun.Name,
			Namespace: ptr.To(step.Namespace),
		},
		UID: ptr.To(storyRun.UID),
	}

	if err := transportutil.PopulateBindingMedia(binding, ctxData.transportObj); err != nil {
		ctxData.logger.Error(err, "Failed to populate binding media", "transport", ctxData.transportObj.Name)
		r.recordTransportBindingWarning(step, eventReasonTransportBindingCodecFailed, fmt.Sprintf("Transport %s rejected media configuration: %v", ctxData.transportObj.Name, err))
		return err
	}
	if err := transportutil.ValidateCodecSupport(binding, ctxData.transportObj); err != nil {
		ctxData.logger.Error(err, "Failed to validate binding codec support", "transport", ctxData.transportObj.Name)
		r.recordTransportBindingWarning(step, eventReasonTransportBindingCodecFailed, fmt.Sprintf("Transport %s rejected codec settings: %v", ctxData.transportObj.Name, err))
		return err
	}

	if len(ctxData.settings) > 0 {
		binding.Spec.RawSettings = &runtime.RawExtension{Raw: append([]byte(nil), ctxData.settings...)}
	} else {
		binding.Spec.RawSettings = nil
	}

	binding.Spec.ConnectorEndpoint = transportutil.LocalConnectorEndpoint(ctxData.defaultPort)

	return controllerutil.SetControllerReference(step, binding, r.Scheme)
}

func (r *StepRunReconciler) fetchBindingForReadiness(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	binding *transportv1alpha1.TransportBinding,
	logger *logging.ControllerLogger,
) (*transportv1alpha1.TransportBinding, error) {
	var refreshed transportv1alpha1.TransportBinding
	reader := r.APIReader
	readerLabel := "apiReader"
	if reader == nil {
		reader = r.Client
		readerLabel = "client"
	}
	if err := reader.Get(ctx, types.NamespacedName{Name: binding.Name, Namespace: binding.Namespace}, &refreshed); err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Transport binding not yet visible to reader; waiting before evaluating readiness", "reader", readerLabel)
			if r.Recorder != nil {
				r.Recorder.Eventf(
					step,
					corev1.EventTypeNormal,
					eventReasonTransportBindingPending,
					"Transport binding %s not yet visible via %s reader",
					binding.Name,
					readerLabel,
				)
			}
			return nil, errTransportBindingPending
		}
		return nil, err
	}
	return &refreshed, nil
}

// recordTransportBindingWarning emits a warning event for transport binding issues.
//
// Behavior:
//   - Returns early if recorder or step is nil.
//   - Emits warning event with the given reason and message.
//
// Arguments:
//   - step *runsv1alpha1.StepRun: the event target.
//   - reason string: event reason.
//   - message string: event message.
//
// Returns:
//   - None.
func (r *StepRunReconciler) recordTransportBindingWarning(step *runsv1alpha1.StepRun, reason, message string) {
	if r.Recorder == nil || step == nil {
		return
	}
	r.Recorder.Event(step, corev1.EventTypeWarning, reason, message)
}

// ensureEngramTransportAnnotation patches Engram with transport binding annotation.
//
// Behavior:
//   - Trims and sanitizes the binding value.
//   - Patches Engram.Annotations[contracts.TransportBindingAnnotation] if changed.
//   - Emits events for invalid input or successful annotation.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - engram *v1alpha1.Engram: the Engram to annotate.
//   - bindingValue string: the raw binding value.
//   - step *runsv1alpha1.StepRun: for context in events.
//
// Returns:
//   - error: non-nil on patch failure.
func (r *StepRunReconciler) ensureEngramTransportAnnotation(ctx context.Context, engram *v1alpha1.Engram, bindingValue string, step *runsv1alpha1.StepRun) error {
	if engram == nil {
		return nil
	}
	bindingValue = strings.TrimSpace(bindingValue)
	if bindingValue == "" {
		return nil
	}
	meta := newTransportAnnotationMeta(ctx, engram, step)
	sanitized := coretransport.SanitizeBindingAnnotationValue(bindingValue)
	if sanitized == "" {
		r.handleAnnotationSanitizeFailure(engram, meta)
		return nil
	}
	if !annotationNeedsUpdate(engram, sanitized) {
		return nil
	}
	return r.patchTransportAnnotation(ctx, engram, sanitized, meta)
}

type transportAnnotationMeta struct {
	logger         *logging.ControllerLogger
	storyRunName   string
	stepIdentifier string
	stepRunName    string
}

func newTransportAnnotationMeta(ctx context.Context, engram *v1alpha1.Engram, step *runsv1alpha1.StepRun) transportAnnotationMeta {
	meta := transportAnnotationMeta{
		logger: logging.NewControllerLogger(ctx, "steprun-binding").
			WithValues("engram", engram.Name, "namespace", engram.Namespace),
		storyRunName:   metrics.UnknownLabelValue,
		stepIdentifier: metrics.UnknownLabelValue,
		stepRunName:    metrics.UnknownLabelValue,
	}
	if step == nil {
		meta.logger = meta.logger.WithValues("storyRun", meta.storyRunName, "stepID", meta.stepIdentifier, "stepRun", meta.stepRunName)
		return meta
	}

	if step.Spec.StoryRunRef.Name != "" {
		meta.storyRunName = step.Spec.StoryRunRef.Name
	}
	if step.Spec.StepID != "" {
		meta.stepIdentifier = step.Spec.StepID
	} else if step.Labels[contracts.ParentStepLabel] != "" {
		meta.stepIdentifier = step.Labels[contracts.ParentStepLabel]
	}
	if step.Name != "" {
		meta.stepRunName = step.Name
	}
	meta.logger = meta.logger.WithStepRun(step).WithValues("storyRun", meta.storyRunName, "stepID", meta.stepIdentifier)
	return meta
}

func (r *StepRunReconciler) handleAnnotationSanitizeFailure(engram *v1alpha1.Engram, meta transportAnnotationMeta) {
	meta.logger.Info("Skipping transport annotation; unable to sanitize binding value")
	metrics.RecordTransportBindingAnnotationSanitizeFailure(engram.Namespace, meta.storyRunName, meta.stepIdentifier)
	meta.logger.V(2).Info("Transport binding annotation sanitize failure", "stepRun", meta.stepRunName)
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(
		engram,
		corev1.EventTypeWarning,
		eventReasonTransportBindingAnnotationInvalid,
		"Failed to sanitize transport binding for Engram %s/%s (storyRun=%s, step=%s, stepRun=%s)",
		engram.Namespace,
		engram.Name,
		meta.storyRunName,
		meta.stepIdentifier,
		meta.stepRunName,
	)
}

func annotationNeedsUpdate(engram *v1alpha1.Engram, sanitized string) bool {
	return engram.Annotations == nil || engram.Annotations[contracts.TransportBindingAnnotation] != sanitized
}

func (r *StepRunReconciler) patchTransportAnnotation(
	ctx context.Context,
	engram *v1alpha1.Engram,
	sanitized string,
	meta transportAnnotationMeta,
) error {
	patch := client.MergeFrom(engram.DeepCopy())
	if engram.Annotations == nil {
		engram.Annotations = make(map[string]string, 1)
	}
	engram.Annotations[contracts.TransportBindingAnnotation] = sanitized
	if err := r.Patch(ctx, engram, patch); err != nil {
		meta.logger.V(1).Info("Failed to patch transport binding annotation", "sanitized", sanitized, "error", err)
		if r.Recorder != nil {
			r.Recorder.Eventf(
				engram,
				corev1.EventTypeWarning,
				eventReasonTransportBindingAnnotationPatchFailed,
				"Failed to record transport binding %s on Engram %s/%s (storyRun=%s, step=%s, stepRun=%s): %v",
				sanitized,
				engram.Namespace,
				engram.Name,
				meta.storyRunName,
				meta.stepIdentifier,
				meta.stepRunName,
				err,
			)
		}
		return err
	}
	meta.logger.V(1).Info("Annotated Engram with transport binding", "sanitized", sanitized)
	if r.Recorder != nil {
		r.Recorder.Eventf(
			engram,
			corev1.EventTypeNormal,
			eventReasonTransportBindingAnnotated,
			"Recorded transport binding %s on Engram %s/%s (storyRun=%s, step=%s, stepRun=%s)",
			sanitized,
			engram.Namespace,
			engram.Name,
			meta.storyRunName,
			meta.stepIdentifier,
			meta.stepRunName,
		)
	}
	return nil
}

// desiredRunScopedService renders the Service manifest that fronts a realtime
// StepRun, mirroring Impulse metadata precedence (controller defaults <
// execution-config overrides < Story overrides).
//
// Arguments:
//   - step *runsv1alpha1.StepRun: supplies namespace/name, labels, and owner identity.
//   - story *v1alpha1.Story: optional Story pointer whose labels/annotations should propagate.
//   - resolvedCfg *config.ResolvedExecutionConfig: provides Service labels, annotations,
//     and ports resolved from Engram templates.
//
// Returns:
//   - *corev1.Service: synthesized Service manifest passed to reconcileRunScopedService.
//
// Side Effects:
//   - None; helper only constructs and returns a Service struct.
func (r *StepRunReconciler) desiredRunScopedService(step *runsv1alpha1.StepRun, story *v1alpha1.Story, resolvedCfg *config.ResolvedExecutionConfig) *corev1.Service {
	selector := runsidentity.SelectorLabels(step.Spec.StoryRunRef.Name)
	selector[contracts.StepRunLabelKey] = step.Name
	if story != nil {
		selector[contracts.StoryLabelKey] = story.Name
	}

	labels := kubeutil.MergeStringMaps(
		map[string]string{
			"app.kubernetes.io/name":       "bobrapet-realtime-runtime",
			"app.kubernetes.io/managed-by": "steprun-controller",
		},
		resolvedCfg.ServiceLabels,
	)
	if labels == nil {
		labels = map[string]string{}
	}

	annotations := kubeutil.MergeStringMaps(resolvedCfg.ServiceAnnotations)

	ports := kubeutil.CloneServicePorts(resolvedCfg.ServicePorts)
	if len(ports) == 0 {
		defaultPort := int32(r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort)
		ports = []corev1.ServicePort{{
			Name:       "grpc",
			Protocol:   corev1.ProtocolTCP,
			Port:       defaultPort,
			TargetPort: intstr.FromString("grpc"),
		}}
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        step.Name,
			Namespace:   step.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Selector: kubeutil.MergeStringMaps(selector),
			Ports:    ports,
		},
	}
}

// reconcileRunScopedService ensures the per-StepRun Service exists and matches
// the desired spec by delegating drift detection to kubeutil.EnsureService.
//
// Arguments:
//   - ctx context.Context: reconciliation context forwarded to kubeutil.EnsureService.
//   - owner *runsv1alpha1.StepRun: controller owner reference applied to the Service.
//   - desired *corev1.Service: manifest generated by desiredRunScopedService.
//
// Returns:
//   - *corev1.Service: reconciled Service fetched/created by kubeutil.EnsureService.
//   - error: create/update errors from kubeutil.EnsureService.
//
// Side Effects:
//   - Issues Get/Create/Patch calls inside kubeutil.EnsureService and logs create/update events.
func (r *StepRunReconciler) reconcileRunScopedService(ctx context.Context, owner *runsv1alpha1.StepRun, desired *corev1.Service) (*corev1.Service, error) {
	service, result, err := kubeutil.EnsureService(ctx, r.Client, r.Scheme, owner, desired)
	if err != nil {
		return nil, err
	}
	switch result {
	case controllerutil.OperationResultCreated:
		logging.NewControllerLogger(ctx, "steprun-service").WithStepRun(owner).Info("Created Service for realtime StepRun", "service", desired.Name)
	case controllerutil.OperationResultUpdated:
		logging.NewControllerLogger(ctx, "steprun-service").WithStepRun(owner).Info("Updated Service for realtime StepRun", "service", desired.Name)
	}
	return service, nil
}

// desiredRunScopedDeployment builds a Deployment manifest for realtime StepRun.
//
// Behavior:
//   - Constructs pod template with env vars, volumes, and security context.
//   - Applies transport binding and storage configuration.
//   - Returns the Deployment for reconciliation.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun owner.
//   - story *v1alpha1.Story: the parent Story.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - engram *v1alpha1.Engram: the Engram configuration.
//   - template *catalogv1alpha1.EngramTemplate: the template.
//   - resolvedCfg *config.ResolvedExecutionConfig: resolved settings.
//   - storageTimeout string: formatted storage timeout.
//   - stepTimeout time.Duration: step execution timeout.
//   - bindingEnv string: transport binding env value.
//   - bindingInfo *transportpb.BindingInfo: binding details.
//   - allowInsecure bool: whether to allow insecure connections.
//
// Returns:
//   - *appsv1.Deployment: the constructed Deployment.
//   - error: non-nil on failures.
func (r *StepRunReconciler) desiredRunScopedDeployment(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	storyRun *runsv1alpha1.StoryRun,
	engram *v1alpha1.Engram,
	template *catalogv1alpha1.EngramTemplate,
	resolvedCfg *config.ResolvedExecutionConfig,
	storageTimeout string,
	stepTimeout time.Duration,
	bindingEnv string,
	bindingInfo *transportpb.BindingInfo,
	allowInsecure bool,
) (*appsv1.Deployment, error) {
	operatorCfg := r.ConfigResolver.GetOperatorConfig()
	runtimeEnvCfg := toRuntimeEnvConfig(operatorCfg.Controller.Engram.EngramControllerConfig)
	defaultPort := transportutil.EffectiveServicePort(resolvedCfg, int32(operatorCfg.Controller.Engram.EngramControllerConfig.DefaultGRPCPort))

	labels, podLabels := buildRealtimeLabels(step, story, engram)
	podAnnotations := buildRealtimeAnnotations(step, story, bindingEnv)

	baseEnv, err := r.buildRealtimeBaseEnv(ctx, step, story, storyRun, engram, bindingEnv, bindingInfo, allowInsecure, stepTimeout, runtimeEnvCfg, operatorCfg)
	if err != nil {
		return nil, err
	}
	secretArtifacts := podspec.BuildSecretArtifacts(template.Spec.SecretSchema, resolvedCfg.Secrets)
	envForDeployment, volumes, volumeMounts := composeRealtimeArtifacts(baseEnv, secretArtifacts)

	podSpec := r.buildRuntimePodTemplate(
		resolvedCfg,
		operatorCfg,
		podLabels,
		podAnnotations,
		envForDeployment,
		secretArtifacts.EnvFrom,
		volumes,
		volumeMounts,
		[]corev1.ContainerPort{{
			Name:          "grpc",
			ContainerPort: defaultPort,
		}},
		corev1.RestartPolicyAlways,
	)

	podspec.ApplyStorageEnv(resolvedCfg, &podSpec.Spec, &podSpec.Spec.Containers[0], storageTimeout)

	if err := r.ensureRealtimeTLS(ctx, allowInsecure, step.Namespace, engram, operatorCfg, &podSpec); err != nil {
		return nil, err
	}

	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      step.Name,
			Namespace: step.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{contracts.StepRunLabelKey: step.Name}},
			Template: podSpec,
		},
	}, nil
}

func buildRealtimeLabels(step *runsv1alpha1.StepRun, story *v1alpha1.Story, engram *v1alpha1.Engram) (map[string]string, map[string]string) {
	labels := runsidentity.SelectorLabels(step.Spec.StoryRunRef.Name)
	labels["app.kubernetes.io/name"] = "bobrapet-realtime-runtime"
	labels["app.kubernetes.io/managed-by"] = "steprun-controller"
	labels[contracts.StepRunLabelKey] = step.Name
	labels[contracts.StepLabelKey] = step.Spec.StepID
	if engram != nil {
		labels[contracts.EngramLabelKey] = engram.Name
	}
	if story != nil {
		labels[contracts.StoryLabelKey] = story.Name
	}

	podLabels := make(map[string]string, len(labels))
	for k, v := range labels {
		podLabels[k] = v
	}
	return labels, podLabels
}

func buildRealtimeAnnotations(step *runsv1alpha1.StepRun, story *v1alpha1.Story, bindingEnv string) map[string]string {
	annotations := map[string]string{
		contracts.StoryRunAnnotation: step.Spec.StoryRunRef.Name,
		contracts.StepAnnotation:     step.Spec.StepID,
	}
	if story != nil {
		annotations[contracts.StoryAnnotation] = story.Name
	}
	if bindingEnv != "" {
		annotations[contracts.TransportBindingAnnotation] = bindingEnv
	}
	return annotations
}

func (r *StepRunReconciler) buildRealtimeBaseEnv(
	ctx context.Context,
	step *runsv1alpha1.StepRun,
	story *v1alpha1.Story,
	storyRun *runsv1alpha1.StoryRun,
	engram *v1alpha1.Engram,
	bindingEnv string,
	bindingInfo *transportpb.BindingInfo,
	allowInsecure bool,
	stepTimeout time.Duration,
	runtimeEnvCfg coreenv.Config,
	operatorCfg *config.OperatorConfig,
) ([]corev1.EnvVar, error) {
	meta := realtimeMetadata(step, story, storyRun, engram)
	baseEnv := coreenv.BuildBaseEnv(meta, runtimeEnvCfg, operatorCfg.Controller.TracePropagationEnabled)

	startedAt := metav1.Now()
	if step.Status.StartedAt != nil {
		startedAt = *step.Status.StartedAt
	}
	coreenv.AppendStartedAtEnv(&baseEnv, startedAt)

	configBytes, err := r.evaluateStepConfigForRealtime(ctx, step, storyRun)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate step config for realtime deployment: %w", err)
	}
	baseEnv = append(baseEnv, buildConfigEnvVars(configBytes)...)

	storyInputsBytes, err := r.getStoryRunInputsBytes(ctx, storyRun)
	if err != nil {
		return nil, fmt.Errorf("failed to get story inputs: %w", err)
	}
	if len(storyInputsBytes) == 0 {
		storyInputsBytes = []byte("{}")
	}
	baseEnv = append(baseEnv, corev1.EnvVar{Name: contracts.InputsEnv, Value: string(storyInputsBytes)})

	if transportsJSON := marshalStoryTransports(story); transportsJSON != "" {
		baseEnv = append(baseEnv, corev1.EnvVar{Name: contracts.TransportsEnv, Value: transportsJSON})
	}
	if bindingEnv != "" {
		baseEnv = append(baseEnv, corev1.EnvVar{Name: contracts.TransportBindingEnv, Value: bindingEnv})
	}
	baseEnv = coretransport.AppendTransportMetadataEnv(baseEnv, bindingInfo)
	baseEnv = coretransport.AppendTransportHeartbeatEnv(baseEnv, operatorCfg.Controller.Transport.HeartbeatInterval)
	baseEnv = coretransport.AppendBindingEnvOverrides(baseEnv, bindingInfo)
	baseEnv = transportutil.ApplyTransportSecurityEnv(baseEnv, allowInsecure)
	if stepTimeout > 0 {
		baseEnv = append(baseEnv, corev1.EnvVar{Name: contracts.StepTimeoutEnv, Value: stepTimeout.String()})
	}
	return baseEnv, nil
}

func realtimeMetadata(step *runsv1alpha1.StepRun, story *v1alpha1.Story, storyRun *runsv1alpha1.StoryRun, engram *v1alpha1.Engram) coreenv.Metadata {
	storyName := ""
	if story != nil {
		storyName = story.Name
	}
	workloadMode := ""
	if engram != nil {
		workloadMode = string(engram.Spec.Mode)
	}
	return coreenv.Metadata{
		StoryName:        storyName,
		StoryRunName:     storyRun.Name,
		StepName:         step.Spec.StepID,
		StepRunName:      step.Name,
		StepRunNamespace: step.Namespace,
		WorkloadMode:     workloadMode,
	}
}

func marshalStoryTransports(story *v1alpha1.Story) string {
	if story == nil {
		return ""
	}
	if len(story.Status.Transports) > 0 {
		if payload, err := transportutil.MarshalStoryTransportStatuses(story.Status.Transports); err == nil {
			return payload
		}
	}
	if payload, err := transportutil.MarshalStoryTransports(story.Spec.Transports); err == nil {
		return payload
	}
	return ""
}

func composeRealtimeArtifacts(baseEnv []corev1.EnvVar, artifacts podspec.SecretArtifacts) ([]corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount) {
	envVars := append([]corev1.EnvVar{}, baseEnv...)
	envVars = append(envVars, artifacts.EnvVars...)

	volumeMounts := append([]corev1.VolumeMount(nil), artifacts.VolumeMounts...)
	volumes := append([]corev1.Volume(nil), artifacts.Volumes...)

	return envVars, volumes, volumeMounts
}

func (r *StepRunReconciler) ensureRealtimeTLS(
	ctx context.Context,
	allowInsecure bool,
	namespace string,
	engram *v1alpha1.Engram,
	operatorCfg *config.OperatorConfig,
	podSpec *corev1.PodTemplateSpec,
) error {
	if allowInsecure {
		return nil
	}
	secretName := transportutil.ResolveTLSSecretName(engram, operatorCfg.Controller.Transport.GRPC.DefaultTLSSecret)
	if secretName == "" {
		return fmt.Errorf("transport TLS secret not configured and insecure transport disabled")
	}
	return r.configureTLSEnvAndMounts(ctx, namespace, secretName, &podSpec.Spec.Volumes, &podSpec.Spec.Containers[0].VolumeMounts, &podSpec.Spec.Containers[0].Env)
}

// reconcileRunScopedDeployment ensures Deployment exists and matches desired spec.
//
// Behavior:
//   - Delegates to workload.Ensure for create/update.
//   - Checks PodTemplate and Replicas for drift.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - owner *runsv1alpha1.StepRun: the owner reference.
//   - desired *appsv1.Deployment: the desired Deployment spec.
//
// Returns:
//   - *appsv1.Deployment: the reconciled Deployment.
//   - error: non-nil on failures.
func (r *StepRunReconciler) reconcileRunScopedDeployment(ctx context.Context, owner *runsv1alpha1.StepRun, desired *appsv1.Deployment) (*appsv1.Deployment, error) {
	key := types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}
	logger := logging.NewControllerLogger(ctx, "steprun-workload").WithStepRun(owner).Logr()

	return workload.Ensure(ctx, workload.EnsureOptions[*appsv1.Deployment]{
		Client:         r.Client,
		Scheme:         r.Scheme,
		Owner:          owner,
		NamespacedName: key,
		Logger:         logger,
		WorkloadType:   "Deployment",
		NewEmpty:       func() *appsv1.Deployment { return &appsv1.Deployment{} },
		BuildDesired: func() (*appsv1.Deployment, error) {
			return desired.DeepCopy(), nil
		},
		NeedsUpdate: func(current, desired *appsv1.Deployment) bool {
			return workload.PodTemplateChanged(&current.Spec.Template, &desired.Spec.Template) ||
				!reflect.DeepEqual(current.Spec.Replicas, desired.Spec.Replicas)
		},
		ApplyUpdate: func(current, desired *appsv1.Deployment) {
			current.Spec.Template = desired.Spec.Template
			current.Spec.Replicas = desired.Spec.Replicas
		},
	})
}

// classifyExitCode maps container exit codes to ExitClass for retry logic.
//
// Behavior:
//   - Returns Success for 0, Retry for signals/timeouts.
//   - Returns Terminal for application errors.
//   - Returns RateLimited for 429.
//
// Arguments:
//   - code int: the exit code.
//
// Returns:
//   - enums.ExitClass: the classification.
func classifyExitCode(code int) enums.ExitClass {
	switch {
	case code == 0:
		return enums.ExitClassSuccess
	case code == 124:
		// GNU timeout convention; SDK uses this for BUBU_STEP_TIMEOUT
		// Timeouts are often retryable (e.g., increase timeout or retry during lower load)
		return enums.ExitClassRetry
	case code == 137:
		// SIGKILL - often OOMKilled or evicted; retryable with more resources
		return enums.ExitClassRetry
	case code == 143:
		// SIGTERM - graceful shutdown; typically retryable
		return enums.ExitClassRetry
	case code == 125, code == 126, code == 127:
		// Command not found / not executable - terminal error
		return enums.ExitClassTerminal
	case code == 429:
		// HTTP 429 Too Many Requests (if exit code is HTTP status) - rate limited
		return enums.ExitClassRateLimited
	case code >= 1 && code <= 127:
		// Application errors - default to terminal (don't retry logic errors)
		// Operators can override this based on LastFailureMsg analysis
		return enums.ExitClassTerminal
	case code >= 128 && code <= 255:
		// Signal termination - often retryable (transient issues)
		return enums.ExitClassRetry
	default:
		// Unknown exit code - mark as terminal to avoid infinite retries
		return enums.ExitClassTerminal
	}
}

// evaluateStepConfigForRealtime evaluates 'with' for realtime steps (inputs only).
//
// Behavior:
//   - Evaluates only 'inputs.*' references.
//   - Leaves 'steps.*' unevaluated (for runtime hub).
//   - Returns raw config if steps references are found.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//
// Returns:
//   - []byte: marshaled evaluated config.
//   - error: non-nil on evaluation failures.
func (r *StepRunReconciler) evaluateStepConfigForRealtime(ctx context.Context, step *runsv1alpha1.StepRun, storyRun *runsv1alpha1.StoryRun) ([]byte, error) {
	stepName := ""
	if step != nil {
		stepName = step.Name
	}
	storyName := ""
	if storyRun != nil {
		storyName = storyRun.Name
	}
	log := logr.FromContextOrDiscard(ctx).WithValues("step", stepName, "story", storyName)
	log.Info("CEL: evaluating 'with' block for realtime step (inputs.* only)")

	if step == nil || step.Spec.Input == nil || len(step.Spec.Input.Raw) == 0 {
		log.Info("CEL: returning empty config (no input)")
		return []byte("{}"), nil
	}

	// Get story trigger inputs for CEL context
	// Note: Step outputs are NOT available - they're per-packet in realtime mode
	storyInputs, err := r.getStoryRunInputs(ctx, storyRun)
	if err != nil {
		return nil, fmt.Errorf("failed to get story inputs for realtime config evaluation: %w", err)
	}

	// Parse config from StepRun.Spec.Input (Engram.Spec.With + Step.With)
	configMap := map[string]any{}
	if err := json.Unmarshal(step.Spec.Input.Raw, &configMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal step config: %w", err)
	}

	// Evaluate with inputs.* context only
	vars := map[string]any{
		"inputs": storyInputs,
	}
	log.Info("CEL: evaluating with inputs context", "configKeys", len(configMap), "hasInputs", len(storyInputs) > 0)

	// Evaluate - expressions referencing steps.* will fail (expected)
	evaluatedConfig, err := r.CELEvaluator.ResolveWithInputs(ctx, configMap, vars)
	if err != nil {
		// If error mentions steps, it belongs in 'runtime' field, not 'with'
		if strings.Contains(err.Error(), "no such key: steps") {
			log.Info("CEL: config references steps data - should use 'runtime' field instead")
			// For now, return raw config and let hub handle it (migration compatibility)
			return step.Spec.Input.Raw, nil
		}
		log.Error(err, "CEL: evaluation failed")
		return nil, fmt.Errorf("failed to evaluate 'with' config for realtime step: %w", err)
	}
	log.Info("CEL: evaluation succeeded")

	// Marshal evaluated config
	configBytes, err := json.Marshal(evaluatedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal evaluated config: %w", err)
	}

	log.Info("CEL: returning evaluated static config", "size", len(configBytes))
	return configBytes, nil
}

// evaluateStepConfig evaluates CEL expressions in merged config for batch steps.
//
// Behavior:
//   - Merges story inputs and prior step outputs.
//   - Evaluates all CEL expressions in config.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun.
//   - storyRun *runsv1alpha1.StoryRun: the parent StoryRun.
//
// Returns:
//   - []byte: marshaled evaluated config.
//   - error: non-nil on evaluation failures.
func (r *StepRunReconciler) evaluateStepConfig(ctx context.Context, step *runsv1alpha1.StepRun, storyRun *runsv1alpha1.StoryRun) ([]byte, error) {
	stepName := ""
	if step != nil {
		stepName = step.Name
	}
	storyName := ""
	if storyRun != nil {
		storyName = storyRun.Name
	}
	log := logr.FromContextOrDiscard(ctx).WithValues("step", stepName, "story", storyName)
	hasInput := step != nil && step.Spec.Input != nil
	inputSize := 0
	if hasInput {
		inputSize = len(step.Spec.Input.Raw)
	}
	log.Info("CEL: evaluateStepConfig called", "hasInput", hasInput, "inputSize", inputSize)

	if !hasInput || len(step.Spec.Input.Raw) == 0 {
		log.Info("CEL: returning empty config (no input)")
		return []byte("{}"), nil
	}

	// Get story inputs for CEL context
	storyInputs, err := r.getStoryRunInputs(ctx, storyRun)
	if err != nil {
		return nil, fmt.Errorf("failed to get story inputs for config evaluation: %w", err)
	}

	// Get prior step outputs for CEL context
	stepOutputs, err := getPriorStepOutputs(ctx, r.Client, storyRun, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get step outputs for config evaluation: %w", err)
	}

	// Parse merged config from StepRun.Spec.Input (Engram.Spec.With + Step.With)
	mergedConfig := map[string]any{}
	if err := json.Unmarshal(step.Spec.Input.Raw, &mergedConfig); err != nil {
		return nil, fmt.Errorf("failed to unmarshal step config: %w", err)
	}

	// Evaluate CEL expressions
	vars := map[string]any{
		"inputs": storyInputs,
		"steps":  stepOutputs,
	}
	log.Info("CEL: before evaluation", "configKeys", len(mergedConfig), "hasInputs", len(storyInputs) > 0)
	evaluatedConfig, err := r.CELEvaluator.ResolveWithInputs(ctx, mergedConfig, vars)
	if err != nil {
		log.Error(err, "CEL: evaluation failed")
		return nil, fmt.Errorf("failed to evaluate CEL in step config: %w", err)
	}
	log.Info("CEL: evaluation succeeded")

	// Marshal evaluated config
	configBytes, err := json.Marshal(evaluatedConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal evaluated config: %w", err)
	}

	log.Info("CEL: returning evaluated config", "size", len(configBytes))
	return configBytes, nil
}

// getStoryRunInputsBytes returns StoryRun inputs as marshaled JSON.
//
// Behavior:
//   - Delegates to runsinputs.MarshalStoryRunInputs.
//
// Arguments:
//   - ctx context.Context: unused.
//   - storyRun *runsv1alpha1.StoryRun: the StoryRun.
//
// Returns:
//   - []byte: marshaled inputs.
//   - error: non-nil on marshal failure.
func (r *StepRunReconciler) getStoryRunInputsBytes(ctx context.Context, storyRun *runsv1alpha1.StoryRun) ([]byte, error) {
	_ = ctx
	return runsinputs.MarshalStoryRunInputs(storyRun)
}

// buildConfigEnvVars creates BUBU_CONFIG env var from config bytes.
//
// Behavior:
//   - Delegates to kubeutil.ConfigEnvVars.
//
// Arguments:
//   - configBytes []byte: the config JSON.
//
// Returns:
//   - []corev1.EnvVar: the config env vars.
func buildConfigEnvVars(configBytes []byte) []corev1.EnvVar {
	return kubeutil.ConfigEnvVars(configBytes)
}

// nextRequeueDelay calculates jittered requeue delay from operator config.
//
// Behavior:
//   - Uses operator config for base/max delay.
//   - Falls back to 5s-30s if config unavailable.
//
// Arguments:
//   - None.
//
// Returns:
//   - time.Duration: the requeue delay with jitter.
func (r *StepRunReconciler) nextRequeueDelay() time.Duration {
	const fallbackBase = 5 * time.Second
	const fallbackMax = 30 * time.Second

	operatorCfg := r.ConfigResolver.GetOperatorConfig()
	if operatorCfg == nil {
		return rec.JitteredRequeueDelay(0, 0, fallbackBase, fallbackMax)
	}
	return rec.JitteredRequeueDelay(operatorCfg.Controller.RequeueBaseDelay, operatorCfg.Controller.RequeueMaxDelay, fallbackBase, fallbackMax)
}

// findStoryStep finds a step by name in the Story spec.
//
// Behavior:
//   - Returns nil if story is nil.
//   - Iterates steps and returns matching step.
//
// Arguments:
//   - story *v1alpha1.Story: the Story to search.
//   - stepID string: the step name to find.
//
// Returns:
//   - *v1alpha1.Step: the found step, or nil.
func findStoryStep(story *v1alpha1.Story, stepID string) *v1alpha1.Step {
	if story == nil {
		return nil
	}
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if step.Name == stepID {
			return step
		}
	}
	return nil
}

// getStoryForStep fetches the Story referenced by the StepRun's parent StoryRun.
//
// Behavior:
//   - Gets parent StoryRun first.
//   - Loads Story via StoryRef.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - step *runsv1alpha1.StepRun: the StepRun.
//
// Returns:
//   - *v1alpha1.Story: the parent Story.
//   - error: non-nil on fetch failure.
func (r *StepRunReconciler) getStoryForStep(ctx context.Context, step *runsv1alpha1.StepRun) (*v1alpha1.Story, error) {
	storyRun, err := r.getParentStoryRun(ctx, step)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(storyRun.Spec.StoryRef.Name) == "" {
		return nil, fmt.Errorf("storyrun %s missing spec.storyRef.name", storyRun.Name)
	}
	story, key, err := refs.LoadNamespacedReference(
		ctx,
		r.Client,
		storyRun,
		&storyRun.Spec.StoryRef,
		func() *v1alpha1.Story { return &v1alpha1.Story{} },
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("get parent Story %s: %w", key.String(), err)
	}
	return story, nil
}

// SetupWithManager configures the controller with watches and predicates.
//
// Behavior:
//   - Watches StepRun, Job, Deployment, Service, TransportBinding.
//   - Maps Engram and EngramTemplate changes to affected StepRuns.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//   - opts controller.Options: controller options.
//
// Returns:
//   - error: non-nil on setup failure.
func (r *StepRunReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	r.Recorder = mgr.GetEventRecorderFor("steprun-controller")

	// Filter to only reconcile on generation changes (spec updates), not status updates
	generationPredicate := predicate.GenerationChangedPredicate{}

	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StepRun{}, builder.WithPredicates(generationPredicate)).
		WithOptions(opts).
		Owns(&batchv1.Job{}, builder.WithPredicates(generationPredicate)).
		Owns(&appsv1.Deployment{}, builder.WithPredicates(generationPredicate)).
		Owns(&corev1.Service{}, builder.WithPredicates(generationPredicate)).
		Owns(&transportv1alpha1.TransportBinding{}, builder.WithPredicates(generationPredicate)).
		Watches(&v1alpha1.Engram{}, handler.EnqueueRequestsFromMapFunc(r.mapEngramToStepRuns), builder.WithPredicates(generationPredicate)).
		Watches(&catalogv1alpha1.EngramTemplate{}, handler.EnqueueRequestsFromMapFunc(r.mapEngramTemplateToStepRuns), builder.WithPredicates(generationPredicate)).
		Complete(r)
}

// mapEngramToStepRuns finds StepRuns referencing a given Engram.
//
// Behavior:
//   - Uses field index to find matching StepRuns.
//   - Returns reconcile requests for affected StepRuns.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - obj client.Object: the Engram that changed.
//
// Returns:
//   - []reconcile.Request: requests for affected StepRuns.
func (r *StepRunReconciler) mapEngramToStepRuns(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logging.NewReconcileLogger(ctx, "steprun-mapper").WithValues("engram", obj.GetName())
	var stepRuns runsv1alpha1.StepRunList
	indexKey := fmt.Sprintf("%s/%s", obj.GetNamespace(), obj.GetName())
	reqs, err := refs.EnqueueByField(ctx, r.Client, &stepRuns, contracts.IndexStepRunEngramRef, indexKey)
	if err != nil {
		log.Error(err, "failed to list stepruns for engram")
		return nil
	}

	return reqs
}

// mapEngramTemplateToStepRuns finds StepRuns affected by EngramTemplate change.
//
// Behavior:
//   - Finds Engrams using the template.
//   - Finds non-terminal StepRuns using those Engrams.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - obj client.Object: the EngramTemplate that changed.
//
// Returns:
//   - []reconcile.Request: requests for affected StepRuns.
func (r *StepRunReconciler) mapEngramTemplateToStepRuns(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logging.NewReconcileLogger(ctx, "steprun-mapper").WithValues("engram-template", obj.GetName())

	// 1. Find all Engrams that reference this EngramTemplate.
	var engrams v1alpha1.EngramList
	// Note: We list across all namespaces because Engrams in any namespace can reference a cluster-scoped template.
	if err := r.List(ctx, &engrams, client.MatchingFields{contracts.IndexEngramTemplateRef: obj.GetName()}); err != nil {
		log.Error(err, "failed to list engrams for engramtemplate")
		return nil
	}

	if len(engrams.Items) == 0 {
		return nil
	}

	// 2. For each of those Engrams, find all the StepRuns that reference them.
	var allRequests []reconcile.Request
	for _, engram := range engrams.Items {
		var stepRuns runsv1alpha1.StepRunList
		indexKey := fmt.Sprintf("%s/%s", engram.GetNamespace(), engram.GetName())
		if err := r.List(ctx, &stepRuns, client.MatchingFields{contracts.IndexStepRunEngramRef: indexKey}); err != nil {
			log.Error(err, "failed to list stepruns for engram", "engram", engram.GetName())
			continue // Continue to the next engram
		}

		for _, item := range stepRuns.Items {
			if item.Status.Phase.IsTerminal() {
				continue
			}
			allRequests = append(allRequests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      item.GetName(),
					Namespace: item.GetNamespace(),
				},
			})
		}
	}

	return allRequests
}

// configureTLSEnvAndMounts adds TLS volume, mounts, and env vars.
//
// Behavior:
//   - Fetches TLS secret.
//   - Adds volume if not exists.
//   - Adds volume mount if not exists.
//   - Adds TLS-related env vars.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - namespace string: secret namespace.
//   - secretName string: TLS secret name.
//   - volumes *[]corev1.Volume: target volumes slice.
//   - volumeMounts *[]corev1.VolumeMount: target mounts slice.
//   - envVars *[]corev1.EnvVar: target env vars slice.
//
// Returns:
//   - error: non-nil on fetch failure.
func (r *StepRunReconciler) configureTLSEnvAndMounts(
	ctx context.Context,
	namespace, secretName string,
	volumes *[]corev1.Volume,
	volumeMounts *[]corev1.VolumeMount,
	envVars *[]corev1.EnvVar,
) error {
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: secretName}, &secret); err != nil {
		return err
	}

	const volumeName = "engram-tls"
	if !volumeExists(*volumes, volumeName) {
		*volumes = append(*volumes, corev1.Volume{
			Name: volumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: secretName},
			},
		})
	}

	const mountPath = "/var/run/tls"
	if !volumeMountExists(*volumeMounts, volumeName, mountPath) {
		*volumeMounts = append(*volumeMounts, corev1.VolumeMount{
			Name:      volumeName,
			MountPath: mountPath,
			ReadOnly:  true,
		})
	}

	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCTLSCertFileEnv, Value: mountPath + "/tls.crt"})
	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCTLSKeyFileEnv, Value: mountPath + "/tls.key"})
	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCAFileEnv, Value: mountPath + "/ca.crt"})
	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCClientCertFileEnv, Value: mountPath + "/tls.crt"})
	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCClientKeyFileEnv, Value: mountPath + "/tls.key"})
	appendEnvIfMissing(envVars, corev1.EnvVar{Name: contracts.GRPCRequireTLSEnv, Value: "true"})

	return nil
}

// toRuntimeEnvConfig converts EngramControllerConfig to coreenv.Config.
//
// Behavior:
//   - Maps all GRPC and runtime settings to core env config struct.
//
// Arguments:
//   - cfg config.EngramControllerConfig: the source config.
//
// Returns:
//   - coreenv.Config: the mapped config.
func toRuntimeEnvConfig(cfg config.EngramControllerConfig) coreenv.Config {
	return coreenv.Config{
		DefaultGRPCPort:                       cfg.DefaultGRPCPort,
		DefaultGRPCHeartbeatIntervalSeconds:   cfg.DefaultGRPCHeartbeatIntervalSeconds,
		DefaultMaxInlineSize:                  cfg.DefaultMaxInlineSize,
		DefaultStorageTimeoutSeconds:          cfg.DefaultStorageTimeoutSeconds,
		DefaultGracefulShutdownTimeoutSeconds: cfg.DefaultGracefulShutdownTimeoutSeconds,
		DefaultMaxRecvMsgBytes:                cfg.DefaultMaxRecvMsgBytes,
		DefaultMaxSendMsgBytes:                cfg.DefaultMaxSendMsgBytes,
		DefaultDialTimeoutSeconds:             cfg.DefaultDialTimeoutSeconds,
		DefaultChannelBufferSize:              cfg.DefaultChannelBufferSize,
		DefaultReconnectMaxRetries:            cfg.DefaultReconnectMaxRetries,
		DefaultReconnectBaseBackoffMillis:     cfg.DefaultReconnectBaseBackoffMillis,
		DefaultReconnectMaxBackoffSeconds:     cfg.DefaultReconnectMaxBackoffSeconds,
		DefaultHangTimeoutSeconds:             cfg.DefaultHangTimeoutSeconds,
		DefaultMessageTimeoutSeconds:          cfg.DefaultMessageTimeoutSeconds,
	}
}

// volumeExists checks if a volume with the given name exists.
//
// Behavior:
//   - Iterates volumes and returns true if name matches.
//
// Arguments:
//   - volumes []corev1.Volume: the volumes to search.
//   - name string: the volume name to find.
//
// Returns:
//   - bool: true if found.
func volumeExists(volumes []corev1.Volume, name string) bool {
	for i := range volumes {
		if volumes[i].Name == name {
			return true
		}
	}
	return false
}

// volumeMountExists checks if a mount with the given name and path exists.
//
// Behavior:
//   - Iterates mounts and returns true if name and path match.
//
// Arguments:
//   - mounts []corev1.VolumeMount: the mounts to search.
//   - name string: the mount name.
//   - mountPath string: the mount path.
//
// Returns:
//   - bool: true if found.
func volumeMountExists(mounts []corev1.VolumeMount, name, mountPath string) bool {
	for i := range mounts {
		if mounts[i].Name == name && mounts[i].MountPath == mountPath {
			return true
		}
	}
	return false
}

// appendEnvIfMissing appends an env var only if not already present.
//
// Behavior:
//   - Checks existing env vars by name.
//   - Appends only if name not found.
//
// Arguments:
//   - envVars *[]corev1.EnvVar: target slice to modify.
//   - envVar corev1.EnvVar: the env var to add.
//
// Returns:
//   - None.
//
// Side Effects:
//   - May append to envVars slice.
func appendEnvIfMissing(envVars *[]corev1.EnvVar, envVar corev1.EnvVar) {
	for _, existing := range *envVars {
		if existing.Name == envVar.Name {
			return
		}
	}
	*envVars = append(*envVars, envVar)
}

// getTLSSecretName extracts TLS secret name from object annotations.
//
// Behavior:
//   - Returns empty if obj is nil.
//   - Checks EngramTLSSecretAnnotation first.
//   - Falls back to TLSSecretAnnotation.
//
// Arguments:
//   - obj *metav1.ObjectMeta: the object with annotations.
//
// Returns:
//   - string: the TLS secret name, or empty.
func getTLSSecretName(obj *metav1.ObjectMeta) string {
	if obj == nil {
		return ""
	}
	if v, ok := obj.Annotations[contracts.EngramTLSSecretAnnotation]; ok && v != "" {
		return v
	}
	if v, ok := obj.Annotations[contracts.TLSSecretAnnotation]; ok && v != "" {
		return v
	}
	return ""
}
