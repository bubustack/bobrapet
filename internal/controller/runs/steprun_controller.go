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
	"encoding/json"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/patch"
)

const (
	// StepRunFinalizer is the finalizer for StepRun resources
	StepRunFinalizer = "steprun.bubustack.io/finalizer"
)

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
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods/log,verbs=get

func (r *StepRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	log := logging.NewReconcileLogger(ctx, "steprun").WithValues("steprun", req.NamespacedName)
	startTime := time.Now()
	defer func() {
		metrics.RecordControllerReconcile("steprun", time.Since(startTime), err)
	}()

	// Bound reconcile duration
	timeout := r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var stepRun runsv1alpha1.StepRun
	if err := r.Get(ctx, req.NamespacedName, &stepRun); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	stepLogger := log.WithStepRun(&stepRun)

	if !stepRun.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &stepRun)
	}

	// Ensure finalizer present for cleanup semantics
	if !controllerutil.ContainsFinalizer(&stepRun, StepRunFinalizer) {
		beforePatch := stepRun.DeepCopy()
		controllerutil.AddFinalizer(&stepRun, StepRunFinalizer)
		if err := r.Patch(ctx, &stepRun, client.MergeFrom(beforePatch)); err != nil {
			stepLogger.Error(err, "Failed to add StepRun finalizer")
			return ctrl.Result{}, err
		}
	}

	if stepRun.Status.Phase.IsTerminal() {
		return ctrl.Result{}, nil
	}

	if stepRun.Status.Phase == "" {
		err := r.setStepRunPhase(ctx, &stepRun, enums.PhasePending, "StepRun created and awaiting processing")
		if err != nil {
			stepLogger.Error(err, "Failed to update StepRun status to Pending")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	return r.reconcileNormal(ctx, &stepRun)
}

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
		// Real-time execution is managed by the RealtimeEngramReconciler.
		// This controller's job is done for this StepRun.
		stepLogger.Info("StepRun is for a real-time Engram, reconciliation is handled by RealtimeEngramReconciler.")
		// We could potentially update the StepRun status here to reflect that it has been "processed".
		return ctrl.Result{}, nil
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

func (r *StepRunReconciler) reconcileJobExecution(ctx context.Context, step *runsv1alpha1.StepRun, engram *v1alpha1.Engram, engramTemplate *catalogv1alpha1.EngramTemplate) (ctrl.Result, error) {
	stepLogger := logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step)

	job := &batchv1.Job{}
	err := r.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, job)
	if err != nil {
		if errors.IsNotFound(err) {
			stepLogger.Info("Job not found, creating a new one")
			newJob, err := r.createJobForStep(ctx, step, engram, engramTemplate)
			if err != nil {
				// Handle CEL evaluation blockage by requeueing
				if evalBlocked, ok := err.(*cel.ErrEvaluationBlocked); ok {
					stepLogger.Info("CEL evaluation blocked, requeueing", "reason", evalBlocked.Reason)
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				}
				stepLogger.Error(err, "Failed to create Job for StepRun")
				return ctrl.Result{}, err
			}
			if err := r.Create(ctx, newJob); err != nil {
				stepLogger.Error(err, "Failed to create Job in cluster")
				return ctrl.Result{}, err
			}
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

// setStepRunPhase is a centralized method for updating the phase and conditions of a StepRun.
// It uses a conflict-retrying patch to apply status updates safely.
func (r *StepRunReconciler) setStepRunPhase(ctx context.Context, step *runsv1alpha1.StepRun, phase enums.Phase, message string) error {
	// If the phase is terminal, also update the parent Engram's status.
	if phase.IsTerminal() {
		if err := r.updateEngramStatus(ctx, step, phase); err != nil {
			// Log the error but don't fail the StepRun phase transition.
			// The Engram status update is best-effort.
			logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step).Error(err, "Failed to update parent engram status")
		}
	}

	return patch.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
		s := obj.(*runsv1alpha1.StepRun)
		s.Status.Phase = phase
		s.Status.ObservedGeneration = s.Generation

		cm := conditions.NewConditionManager(s.Generation)

		switch phase {
		case enums.PhasePending:
			// Only set StartedAt if not already set (preserve operator's BUBU_STARTED_AT timestamp)
			// This prevents timing drift between controller and SDK duration calculations
			if s.Status.StartedAt == nil {
				s.Status.StartedAt = &metav1.Time{Time: time.Now()}
			}
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonReconciling, message)
		case enums.PhaseBlocked:
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonSchedulingFailed, message)
			s.Status.LastFailureMsg = message
		case enums.PhaseRunning:
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonReconciling, message)
		case enums.PhaseSucceeded:
			if s.Status.FinishedAt == nil {
				now := metav1.Now()
				s.Status.FinishedAt = &now
				if s.Status.StartedAt != nil {
					duration := now.Sub(s.Status.StartedAt.Time)
					s.Status.Duration = duration.String()
					metrics.RecordStepRunMetrics(s.Namespace, s.Spec.EngramRef.Name, string(phase), duration)
				}
			}
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonCompleted, message)
		case enums.PhaseFailed:
			if s.Status.FinishedAt == nil {
				now := metav1.Now()
				s.Status.FinishedAt = &now
				if s.Status.StartedAt != nil {
					duration := now.Sub(s.Status.StartedAt.Time)
					s.Status.Duration = duration.String()
					metrics.RecordStepRunMetrics(s.Namespace, s.Spec.EngramRef.Name, string(phase), duration)
				}
			}
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonExecutionFailed, message)
		}
	})
}

func (r *StepRunReconciler) updateEngramStatus(ctx context.Context, step *runsv1alpha1.StepRun, phase enums.Phase) error {
	if step.Spec.EngramRef.Name == "" {
		return nil // No engram to update
	}

	engram := &v1alpha1.Engram{}
	engramKey := client.ObjectKey{Namespace: step.Namespace, Name: step.Spec.EngramRef.Name}
	if err := r.Get(ctx, engramKey, engram); err != nil {
		return fmt.Errorf("failed to get parent engram %s for status update: %w", step.Spec.EngramRef.Name, err)
	}

	return patch.RetryableStatusPatch(ctx, r.Client, engram, func(obj client.Object) {
		e := obj.(*v1alpha1.Engram)
		now := metav1.Now()
		e.Status.LastExecutionTime = &now
		e.Status.TotalExecutions++
		if phase == enums.PhaseFailed {
			e.Status.FailedExecutions++
		}
	})
}

func (r *StepRunReconciler) getEngramForStep(ctx context.Context, step *runsv1alpha1.StepRun) (*v1alpha1.Engram, error) {
	if step.Spec.EngramRef.Name == "" {
		return nil, fmt.Errorf("steprun %s has no engram reference (.spec.engramRef.name)", step.Name)
	}
	var engram v1alpha1.Engram
	key := step.Spec.EngramRef.ToNamespacedName(step)
	if err := r.Get(ctx, key, &engram); err != nil {
		return nil, err
	}
	return &engram, nil
}

func (r *StepRunReconciler) getEngramTemplateForEngram(ctx context.Context, engram *v1alpha1.Engram) (*catalogv1alpha1.EngramTemplate, error) {
	var engramTemplate catalogv1alpha1.EngramTemplate
	// EngramTemplates are cluster-scoped, so we must not provide a namespace.
	key := client.ObjectKey{Name: engram.Spec.TemplateRef.Name}
	if err := r.Get(ctx, key, &engramTemplate); err != nil {
		return nil, err
	}
	return &engramTemplate, nil
}

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
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// If the job is not found, it's safe to remove the finalizer.
	if errors.IsNotFound(err) {
		stepLogger.Info("Owned Job is deleted, removing finalizer")
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
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *StepRunReconciler) createJobForStep(ctx context.Context, srun *runsv1alpha1.StepRun, engram *v1alpha1.Engram, engramTemplate *catalogv1alpha1.EngramTemplate) (*batchv1.Job, error) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(srun)

	story, storyRun, resolvedConfig, inputBytes, stepTimeout, err := r.prepareExecutionContext(ctx, srun, engram, engramTemplate, stepLogger)
	if err != nil {
		return nil, err
	}

	secretEnvVars, volumes, volumeMounts := r.setupSecrets(ctx, resolvedConfig, engramTemplate)
	envVars := r.buildBaseEnvVars(srun, story, inputBytes, stepTimeout)
	envVars = r.appendGRPCTuningEnv(envVars)
	envVars = r.appendStorageEnv(envVars, resolvedConfig, stepLogger)

	activeDeadlineSeconds := int64((stepTimeout + 2*time.Minute).Seconds())
	startedAt := metav1.Now()
	if srun.Status.StartedAt != nil {
		startedAt = *srun.Status.StartedAt
	}
	r.addStartedAtEnv(&envVars, startedAt)
	envVars = append(envVars, secretEnvVars...)
	if engram.Spec.With != nil && len(engram.Spec.With.Raw) > 0 {
		envVars = append(envVars, corev1.EnvVar{Name: "BUBU_CONFIG", Value: string(engram.Spec.With.Raw)})
	}

	job := r.buildJobSpec(srun, resolvedConfig, envVars, volumes, volumeMounts, activeDeadlineSeconds)
	if err := controllerutil.SetControllerReference(srun, job, r.Scheme); err != nil {
		return nil, err
	}
	_ = storyRun // storyRun currently unused beyond inputs; keep for future enhancements
	return job, nil
}

func (r *StepRunReconciler) prepareExecutionContext(ctx context.Context, srun *runsv1alpha1.StepRun, engram *v1alpha1.Engram, engramTemplate *catalogv1alpha1.EngramTemplate, stepLogger *logging.ControllerLogger) (*v1alpha1.Story, *runsv1alpha1.StoryRun, *config.ResolvedExecutionConfig, []byte, time.Duration, error) {
	story, err := r.getStoryForStep(ctx, srun)
	if err != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("failed to get story for step: %w", err)
	}
	storyRun, err := r.getParentStoryRun(ctx, srun)
	if err != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("failed to get parent storyrun: %w", err)
	}
	resolvedConfig, err := r.ConfigResolver.ResolveExecutionConfig(ctx, srun, story, engram, engramTemplate)
	if err != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("failed to resolve execution config for step '%s': %w", srun.Name, err)
	}
	stepLogger.Info("Resolved ServiceAccountName", "sa", resolvedConfig.ServiceAccountName)
	storyRunInputs, err := r.getStoryRunInputs(ctx, storyRun)
	if err != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("failed to get storyrun inputs: %w", err)
	}
	stepOutputs, err := getPriorStepOutputs(ctx, r.Client, storyRun, nil)
	if err != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("failed to get prior step outputs: %w", err)
	}
	with := map[string]any{}
	if srun.Spec.Input != nil {
		if err := json.Unmarshal(srun.Spec.Input.Raw, &with); err != nil {
			return nil, nil, nil, nil, 0, fmt.Errorf("failed to unmarshal step 'with' block: %w", err)
		}
	}
	vars := map[string]any{"inputs": storyRunInputs, "steps": stepOutputs}
	resolvedInputs, err := r.CELEvaluator.ResolveWithInputs(ctx, with, vars)
	if err != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("failed to resolve inputs with CEL: %w", err)
	}
	inputBytes, err := json.Marshal(resolvedInputs)
	if err != nil {
		return nil, nil, nil, nil, 0, fmt.Errorf("failed to marshal resolved inputs: %w", err)
	}
	stepTimeout := r.computeStepTimeout(story, stepLogger)
	return story, storyRun, resolvedConfig, inputBytes, stepTimeout, nil
}

func (r *StepRunReconciler) computeStepTimeout(story *v1alpha1.Story, stepLogger *logging.ControllerLogger) time.Duration {
	stepTimeout := r.ConfigResolver.GetOperatorConfig().Controller.DefaultStepTimeout
	if stepTimeout == 0 {
		stepTimeout = 30 * time.Minute
	}
	if story.Spec.Policy != nil && story.Spec.Policy.Timeouts != nil && story.Spec.Policy.Timeouts.Step != nil {
		if parsedTimeout, err := time.ParseDuration(*story.Spec.Policy.Timeouts.Step); err == nil && parsedTimeout > 0 {
			stepTimeout = parsedTimeout
			stepLogger.Info("Using Story-level step timeout", "timeout", stepTimeout)
		} else if err != nil {
			stepLogger.Error(err, "Invalid step timeout in Story policy, using default", "rawTimeout", *story.Spec.Policy.Timeouts.Step, "default", stepTimeout)
		}
	}
	return stepTimeout
}

func (r *StepRunReconciler) buildBaseEnvVars(srun *runsv1alpha1.StepRun, story *v1alpha1.Story, inputBytes []byte, stepTimeout time.Duration) []corev1.EnvVar {
	envVars := []corev1.EnvVar{
		{Name: "BUBU_STORY_NAME", Value: story.Name},
		{Name: "BUBU_STORYRUN_ID", Value: srun.Spec.StoryRunRef.Name},
		{Name: "BUBU_STEP_NAME", Value: srun.Spec.StepID},
		{Name: "BUBU_STEPRUN_NAME", Value: srun.Name},
		{Name: "BUBU_STEPRUN_NAMESPACE", Value: srun.Namespace},
		{Name: "BUBU_INPUTS", Value: string(inputBytes)},
		{Name: "BUBU_EXECUTION_MODE", Value: "batch"},
		{Name: "BUBU_GRPC_PORT", Value: fmt.Sprintf("%d", r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort)},
		{Name: "BUBU_MAX_INLINE_SIZE", Value: fmt.Sprintf("%d", r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize)},
		{Name: "BUBU_STORAGE_TIMEOUT", Value: fmt.Sprintf("%ds", r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultStorageTimeoutSeconds)},
		{Name: "BUBU_STEP_TIMEOUT", Value: stepTimeout.String()},
	}
	return envVars
}

func (r *StepRunReconciler) appendGRPCTuningEnv(envVars []corev1.EnvVar) []corev1.EnvVar {
	engramConfig := r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig
	return append(envVars,
		corev1.EnvVar{Name: "BUBU_GRPC_MAX_RECV_BYTES", Value: fmt.Sprintf("%d", engramConfig.DefaultMaxRecvMsgBytes)},
		corev1.EnvVar{Name: "BUBU_GRPC_MAX_SEND_BYTES", Value: fmt.Sprintf("%d", engramConfig.DefaultMaxSendMsgBytes)},
		corev1.EnvVar{Name: "BUBU_GRPC_CLIENT_MAX_RECV_BYTES", Value: fmt.Sprintf("%d", engramConfig.DefaultMaxRecvMsgBytes)},
		corev1.EnvVar{Name: "BUBU_GRPC_CLIENT_MAX_SEND_BYTES", Value: fmt.Sprintf("%d", engramConfig.DefaultMaxSendMsgBytes)},
		corev1.EnvVar{Name: "BUBU_GRPC_DIAL_TIMEOUT", Value: fmt.Sprintf("%ds", engramConfig.DefaultDialTimeoutSeconds)},
		corev1.EnvVar{Name: "BUBU_GRPC_CHANNEL_BUFFER_SIZE", Value: fmt.Sprintf("%d", engramConfig.DefaultChannelBufferSize)},
		corev1.EnvVar{Name: "BUBU_GRPC_RECONNECT_MAX_RETRIES", Value: fmt.Sprintf("%d", engramConfig.DefaultReconnectMaxRetries)},
		corev1.EnvVar{Name: "BUBU_GRPC_RECONNECT_BASE_BACKOFF", Value: fmt.Sprintf("%dms", engramConfig.DefaultReconnectBaseBackoffMillis)},
		corev1.EnvVar{Name: "BUBU_GRPC_RECONNECT_MAX_BACKOFF", Value: fmt.Sprintf("%ds", engramConfig.DefaultReconnectMaxBackoffSeconds)},
		corev1.EnvVar{Name: "BUBU_GRPC_HANG_TIMEOUT", Value: fmt.Sprintf("%ds", engramConfig.DefaultHangTimeoutSeconds)},
		corev1.EnvVar{Name: "BUBU_GRPC_MESSAGE_TIMEOUT", Value: fmt.Sprintf("%ds", engramConfig.DefaultMessageTimeoutSeconds)},
		corev1.EnvVar{Name: "BUBU_GRPC_CHANNEL_SEND_TIMEOUT", Value: fmt.Sprintf("%ds", engramConfig.DefaultMessageTimeoutSeconds)},
	)
}

func (r *StepRunReconciler) appendStorageEnv(envVars []corev1.EnvVar, resolvedConfig *config.ResolvedExecutionConfig, stepLogger *logging.ControllerLogger) []corev1.EnvVar {
	if storagePolicy := resolvedConfig.Storage; storagePolicy != nil && storagePolicy.S3 != nil {
		s3Config := storagePolicy.S3
		stepLogger.Info("Configuring pod for S3 object storage access", "bucket", s3Config.Bucket)
		envVars = append(envVars,
			corev1.EnvVar{Name: "BUBU_STORAGE_PROVIDER", Value: "s3"},
			corev1.EnvVar{Name: "BUBU_STORAGE_S3_BUCKET", Value: s3Config.Bucket},
		)
		if s3Config.Region != "" {
			envVars = append(envVars, corev1.EnvVar{Name: "BUBU_STORAGE_S3_REGION", Value: s3Config.Region})
		}
		if s3Config.Endpoint != "" {
			envVars = append(envVars, corev1.EnvVar{Name: "BUBU_STORAGE_S3_ENDPOINT", Value: s3Config.Endpoint})
		}
	}
	return envVars
}

func (r *StepRunReconciler) addStartedAtEnv(envVars *[]corev1.EnvVar, startedAt metav1.Time) {
	*envVars = append(*envVars, corev1.EnvVar{Name: "BUBU_STARTED_AT", Value: startedAt.Format(time.RFC3339Nano)})
}

func (r *StepRunReconciler) buildJobSpec(srun *runsv1alpha1.StepRun, resolvedConfig *config.ResolvedExecutionConfig, envVars []corev1.EnvVar, volumes []corev1.Volume, volumeMounts []corev1.VolumeMount, activeDeadlineSeconds int64) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      srun.Name,
			Namespace: srun.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "bobrapet",
				"app.kubernetes.io/component": "engram",
				"bubustack.io/steprun":        srun.Name,
				"bubustack.io/story-run":      srun.Spec.StoryRunRef.Name,
			},
		},
		Spec: batchv1.JobSpec{
			ActiveDeadlineSeconds:   &activeDeadlineSeconds,
			BackoffLimit:            &resolvedConfig.BackoffLimit,
			TTLSecondsAfterFinished: &resolvedConfig.TTLSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"bubustack.io/steprun": srun.Name},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:                 resolvedConfig.RestartPolicy,
					ServiceAccountName:            resolvedConfig.ServiceAccountName,
					AutomountServiceAccountToken:  &resolvedConfig.AutomountServiceAccountToken,
					TerminationGracePeriodSeconds: &r.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultTerminationGracePeriodSeconds,
					SecurityContext:               resolvedConfig.ToPodSecurityContext(),
					Volumes:                       volumes,
					Containers: []corev1.Container{{
						Name:            "engram",
						Image:           resolvedConfig.Image,
						ImagePullPolicy: resolvedConfig.ImagePullPolicy,
						Resources:       resolvedConfig.Resources,
						SecurityContext: resolvedConfig.ToContainerSecurityContext(),
						LivenessProbe:   resolvedConfig.LivenessProbe,
						ReadinessProbe:  resolvedConfig.ReadinessProbe,
						StartupProbe:    resolvedConfig.StartupProbe,
						Env:             envVars,
						VolumeMounts:    volumeMounts,
					}},
				},
			},
		},
	}
}

// getStoryRunInputs fetches the initial inputs from the parent StoryRun.
func (r *StepRunReconciler) getStoryRunInputs(_ context.Context, storyRun *runsv1alpha1.StoryRun) (map[string]any, error) {
	if storyRun.Spec.Inputs == nil {
		return make(map[string]any), nil
	}

	var inputs map[string]any
	if err := json.Unmarshal(storyRun.Spec.Inputs.Raw, &inputs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal storyrun inputs: %w", err)
	}
	return inputs, nil
}

func (r *StepRunReconciler) getParentStoryRun(ctx context.Context, srun *runsv1alpha1.StepRun) (*runsv1alpha1.StoryRun, error) {
	var storyRun runsv1alpha1.StoryRun
	key := types.NamespacedName{Name: srun.Spec.StoryRunRef.Name, Namespace: srun.Namespace}
	if err := r.Get(ctx, key, &storyRun); err != nil {
		return nil, err
	}
	return &storyRun, nil
}

func (r *StepRunReconciler) handleJobStatus(ctx context.Context, step *runsv1alpha1.StepRun, job *batchv1.Job) (ctrl.Result, error) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(step).WithValues("job_name", job.Name)

	if job.Status.Succeeded > 0 {
		stepLogger.Info("Job succeeded. Verifying StepRun status.")
		// If the SDK has not updated the status yet, the controller should take over.
		if step.Status.Phase != enums.PhaseSucceeded {
			stepLogger.Info("Job pod exited 0, but Engram SDK did not patch StepRun status to Succeeded. Controller will mark as Succeeded as a fallback.")
			successMsg := "StepRun pod completed successfully; final status updated by controller."
			// Use the original reconcile context. If it has timed out, the patch will fail
			// and the entire reconcile will be retried, which is the correct and safe behavior.
			// Using a detached context here would break the transactional nature of the reconcile loop.
			if err := r.setStepRunPhase(ctx, step, enums.PhaseSucceeded, successMsg); err != nil {
				stepLogger.Error(err, "Failed to update StepRun status to Succeeded")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if job.Status.Failed > 0 {
		// Extract exit code from pod for granular retry policies (e.g., retry on timeout=124 but not validation errors)
		exitCode := r.extractPodExitCode(ctx, job)

		// Check if SDK already patched status (won the race)
		// SDK writes Phase=Failed with detailed error context (timeout details, dehydration errors, etc.)
		if step.Status.Phase == enums.PhaseFailed {
			stepLogger.Info("Job failed but SDK already updated StepRun status; preserving SDK's error details",
				"podExitCode", exitCode)

			// Enhance status with exit code if SDK didn't provide it (SDK may crash before os.Exit)
			if exitCode != 0 && step.Status.ExitCode == 0 {
				// Use the original reconcile context. If it has timed out, the patch will fail
				// and the entire reconcile will be retried. This ensures the exit code is
				// eventually recorded without risking a race condition from a detached context.
				if err := patch.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
					sr := obj.(*runsv1alpha1.StepRun)
					sr.Status.ExitCode = int32(exitCode)
					// Optionally set ExitClass for semantic categorization
					sr.Status.ExitClass = classifyExitCode(exitCode)
				}); err != nil {
					stepLogger.Error(err, "Failed to patch exit code into StepRun status")
				}
			}
			return ctrl.Result{}, nil
		}

		// SDK didn't write status yet (crashed before patching or no RBAC)
		// Operator patches with generic message as fallback
		stepLogger.Info("Job failed and SDK did not update status; applying fallback Failed status",
			"podExitCode", exitCode)
		// Use the original reconcile context. If it has timed out, the patch will fail and
		// the entire reconcile will be retried, which is the correct and safe behavior for
		// ensuring this terminal state is eventually recorded.
		if err := patch.RetryableStatusPatch(ctx, r.Client, step, func(obj client.Object) {
			sr := obj.(*runsv1alpha1.StepRun)
			sr.Status.Phase = enums.PhaseFailed
			sr.Status.LastFailureMsg = "Job execution failed. Check pod logs for details."
			if exitCode != 0 {
				sr.Status.ExitCode = int32(exitCode)
				sr.Status.ExitClass = classifyExitCode(exitCode)
			}
			now := metav1.Now()
			sr.Status.FinishedAt = &now
			if sr.Status.StartedAt != nil {
				sr.Status.Duration = now.Sub(sr.Status.StartedAt.Time).String()
			}
		}); err != nil {
			stepLogger.Error(err, "Failed to patch StepRun status after job failure")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// extractPodExitCode retrieves the exit code from the Job's failed pod.
// Returns 0 if the pod or exit code cannot be determined.
func (r *StepRunReconciler) extractPodExitCode(ctx context.Context, job *batchv1.Job) int {
	// List pods owned by this Job
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(job.Namespace), client.MatchingLabels{"job-name": job.Name}); err != nil {
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

// classifyExitCode provides semantic categorization of exit codes for retry policies.
// Maps container exit codes to ExitClass for intelligent retry logic.
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

func (r *StepRunReconciler) getStoryForStep(ctx context.Context, step *runsv1alpha1.StepRun) (*v1alpha1.Story, error) {
	storyRun := &runsv1alpha1.StoryRun{}
	storyRunKey := types.NamespacedName{Name: step.Spec.StoryRunRef.Name, Namespace: step.Namespace}
	if err := r.Get(ctx, storyRunKey, storyRun); err != nil {
		return nil, fmt.Errorf("failed to get parent StoryRun %s: %w", step.Spec.StoryRunRef.Name, err)
	}
	story := &v1alpha1.Story{}
	storyKey := storyRun.Spec.StoryRef.ToNamespacedName(storyRun)
	if err := r.Get(ctx, storyKey, story); err != nil {
		return nil, fmt.Errorf("failed to get parent Story %s: %w", storyRun.Spec.StoryRef.Name, err)
	}
	return story, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StepRunReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	r.Recorder = mgr.GetEventRecorderFor("steprun-controller")
	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StepRun{}).
		WithOptions(opts).
		Owns(&batchv1.Job{}).
		Watches(&v1alpha1.Engram{}, handler.EnqueueRequestsFromMapFunc(r.mapEngramToStepRuns)).
		Watches(&catalogv1alpha1.EngramTemplate{}, handler.EnqueueRequestsFromMapFunc(r.mapEngramTemplateToStepRuns)).
		Complete(r)
}

// mapEngramToStepRuns finds all StepRuns that reference a given Engram.
func (r *StepRunReconciler) mapEngramToStepRuns(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logging.NewReconcileLogger(ctx, "steprun-mapper").WithValues("engram", obj.GetName())
	var stepRuns runsv1alpha1.StepRunList
	if err := r.List(ctx, &stepRuns, client.InNamespace(obj.GetNamespace()), client.MatchingFields{"spec.engramRef": obj.GetName()}); err != nil {
		log.Error(err, "failed to list stepruns for engram")
		return nil
	}

	requests := make([]reconcile.Request, len(stepRuns.Items))
	for i, item := range stepRuns.Items {
		requests[i] = reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      item.GetName(),
				Namespace: item.GetNamespace(),
			},
		}
	}
	return requests
}

// mapEngramTemplateToStepRuns finds all StepRuns that are indirectly affected by an EngramTemplate change.
// It does this by first finding all Engrams that use the template, and then finding all StepRuns
// that use those Engrams.
func (r *StepRunReconciler) mapEngramTemplateToStepRuns(ctx context.Context, obj client.Object) []reconcile.Request {
	log := logging.NewReconcileLogger(ctx, "steprun-mapper").WithValues("engram-template", obj.GetName())

	// 1. Find all Engrams that reference this EngramTemplate.
	var engrams v1alpha1.EngramList
	// Note: We list across all namespaces because Engrams in any namespace can reference a cluster-scoped template.
	if err := r.List(ctx, &engrams, client.MatchingFields{"spec.templateRef.name": obj.GetName()}); err != nil {
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
		if err := r.List(ctx, &stepRuns, client.InNamespace(engram.GetNamespace()), client.MatchingFields{"spec.engramRef": engram.GetName()}); err != nil {
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

// setupSecrets resolves the secret mappings from the Engram and prepares the necessary
// volumes, volume mounts, and environment variables for the pod.
func (r *StepRunReconciler) setupSecrets(_ context.Context, resolvedConfig *config.ResolvedExecutionConfig, engramTemplate *catalogv1alpha1.EngramTemplate) ([]corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount) {
	var envVars []corev1.EnvVar
	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount
	// Note: we no longer return EnvFromSource; secrets are either mounted as files or injected via explicit env vars

	if resolvedConfig.Secrets == nil || engramTemplate.Spec.SecretSchema == nil {
		return envVars, volumes, volumeMounts
	}

	for logicalName, actualSecretName := range resolvedConfig.Secrets {
		secretDef, ok := engramTemplate.Spec.SecretSchema[logicalName]
		if !ok {
			// Engram is trying to map a secret that the template doesn't define.
			// We should probably log this, but it's not a fatal error for the reconcile.
			continue
		}

		sdkSecretKey := fmt.Sprintf("BUBU_SECRET_%s", logicalName)

		switch secretDef.MountType {
		case enums.SecretMountTypeFile:
			// 1. Create the Volume pointing to the actual k8s Secret
			volumeName := fmt.Sprintf("secret-%s", logicalName)
			volumes = append(volumes, corev1.Volume{
				Name: volumeName,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: actualSecretName,
					},
				},
			})

			// 2. Create the VolumeMount to mount it into the container
			mountPath := secretDef.MountPath
			if mountPath == "" {
				mountPath = fmt.Sprintf("/etc/bubu/secrets/%s", logicalName) // Default mount path
			}
			volumeMounts = append(volumeMounts, corev1.VolumeMount{
				Name:      volumeName,
				MountPath: mountPath,
				ReadOnly:  true,
			})

			// 3. Add the env var for the SDK to discover it
			envVars = append(envVars, corev1.EnvVar{
				Name:  sdkSecretKey,
				Value: fmt.Sprintf("file:%s", mountPath),
			})

		case enums.SecretMountTypeEnv:
			// For each key defined in the template, create an EnvVar that sources
			// its value directly from the specified Kubernetes secret.
			for _, key := range secretDef.ExpectedKeys {
				prefix := secretDef.EnvPrefix
				if prefix == "" {
					prefix = fmt.Sprintf("%s_", logicalName) // Default prefix
				}
				envVars = append(envVars, corev1.EnvVar{
					Name: prefix + key,
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: actualSecretName,
							},
							Key: key,
						},
					},
				})
			}

			// Add the discovery env var for the SDK. The SDK will now find the
			// environment variables directly, as they are populated at the same time.
			sdkValue := fmt.Sprintf("env:%s", secretDef.EnvPrefix)
			if secretDef.EnvPrefix == "" {
				sdkValue = fmt.Sprintf("env:%s_", logicalName)
			}
			envVars = append(envVars, corev1.EnvVar{
				Name:  sdkSecretKey,
				Value: sdkValue,
			})
		}
	}

	return envVars, volumes, volumeMounts
}
