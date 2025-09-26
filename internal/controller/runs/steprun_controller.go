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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/patch"
)

const (
	// StepRunFinalizer is the finalizer for StepRun resources
	StepRunFinalizer = "steprun.bubu.sh/finalizer"
)

// StepRunReconciler reconciles a StepRun object
type StepRunReconciler struct {
	config.ControllerDependencies
}

//+kubebuilder:rbac:groups=runs.bubu.sh,resources=stepruns,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=runs.bubu.sh,resources=stepruns/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=runs.bubu.sh,resources=stepruns/finalizers,verbs=update
//+kubebuilder:rbac:groups=bubu.sh,resources=engrams,verbs=get;list;watch
//+kubebuilder:rbac:groups=bubu.sh,resources=engrams/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

func (r *StepRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "steprun").WithValues("steprun", req.NamespacedName)

	var step runsv1alpha1.StepRun
	if err := r.ControllerDependencies.Client.Get(ctx, req.NamespacedName, &step); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	stepLogger := log.WithStepRun(&step)

	if !step.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &step)
	}

	if step.Status.Phase.IsTerminal() {
		return ctrl.Result{}, nil
	}

	if step.Status.Phase == "" {
		err := r.setStepRunPhase(ctx, &step, enums.PhasePending, "StepRun created and awaiting processing")
		if err != nil {
			stepLogger.Error(err, "Failed to update StepRun status to Pending")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	return r.reconcileNormal(ctx, &step)
}

func (r *StepRunReconciler) reconcileNormal(ctx context.Context, step *runsv1alpha1.StepRun) (ctrl.Result, error) {
	stepLogger := logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step)

	engram, err := r.getEngramForStep(ctx, step)
	if err != nil {
		if errors.IsNotFound(err) {
			stepLogger.Info("Engram referenced by StepRun not found, setting phase to Blocked")
			err := r.setStepRunPhase(ctx, step, enums.PhaseBlocked, fmt.Sprintf("Engram '%s' not found", step.Spec.EngramRef.Name))
			return ctrl.Result{}, err // Stop reconciliation, wait for watch to trigger
		}
		stepLogger.Error(err, "Failed to get Engram for StepRun")
		return ctrl.Result{}, err
	}

	// Default to "job" mode if not specified
	mode := enums.WorkloadModeJob
	if engram != nil && engram.Spec.Mode != "" { //nolint:staticcheck
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
	err := r.ControllerDependencies.Client.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, job)
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
			if err := r.ControllerDependencies.Client.Create(ctx, newJob); err != nil {
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

func (r *StepRunReconciler) setStepRunFailed(ctx context.Context, step *runsv1alpha1.StepRun, message string) error {
	return r.setStepRunPhase(ctx, step, enums.PhaseFailed, message)
}

func (r *StepRunReconciler) setStepRunBlocked(ctx context.Context, step *runsv1alpha1.StepRun, phase enums.Phase, message string) error {
	return r.setStepRunPhase(ctx, step, phase, message)
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

	return patch.RetryableStatusPatch(ctx, r.ControllerDependencies.Client, step, func(obj client.Object) {
		s := obj.(*runsv1alpha1.StepRun)
		s.Status.Phase = phase
		s.Status.ObservedGeneration = s.Generation

		cm := conditions.NewConditionManager(s.Generation)

		switch phase {
		case enums.PhasePending:
			s.Status.StartedAt = &metav1.Time{Time: time.Now()}
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, "Pending", message)
		case enums.PhaseBlocked:
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, "Blocked", message)
			s.Status.LastFailureMsg = message
		case enums.PhaseRunning:
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, "Running", message)
		case enums.PhaseSucceeded:
			now := metav1.Now()
			s.Status.FinishedAt = &now
			if s.Status.StartedAt != nil {
				s.Status.Duration = now.Sub(s.Status.StartedAt.Time).String()
			}
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, "Succeeded", message)
		case enums.PhaseFailed:
			now := metav1.Now()
			s.Status.FinishedAt = &now
			if s.Status.StartedAt != nil {
				s.Status.Duration = now.Sub(s.Status.StartedAt.Time).String()
			}
			s.Status.LastFailureMsg = message
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, "Failed", message)
		}
	})
}

func (r *StepRunReconciler) updateEngramStatus(ctx context.Context, step *runsv1alpha1.StepRun, phase enums.Phase) error {
	if step.Spec.EngramRef.Name == "" {
		return nil // No engram to update
	}

	engram := &v1alpha1.Engram{}
	engramKey := client.ObjectKey{Namespace: step.Namespace, Name: step.Spec.EngramRef.Name}
	if err := r.ControllerDependencies.Client.Get(ctx, engramKey, engram); err != nil {
		return fmt.Errorf("failed to get parent engram %s for status update: %w", step.Spec.EngramRef.Name, err)
	}

	return patch.RetryableStatusPatch(ctx, r.ControllerDependencies.Client, engram, func(obj client.Object) {
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
	if err := r.ControllerDependencies.Client.Get(ctx, key, &engram); err != nil {
		return nil, err
	}
	return &engram, nil
}

func (r *StepRunReconciler) getEngramTemplateForEngram(ctx context.Context, engram *v1alpha1.Engram) (*catalogv1alpha1.EngramTemplate, error) {
	var engramTemplate catalogv1alpha1.EngramTemplate
	// EngramTemplates are cluster-scoped, so we must not provide a namespace.
	key := client.ObjectKey{Name: engram.Spec.TemplateRef.Name}
	if err := r.ControllerDependencies.Client.Get(ctx, key, &engramTemplate); err != nil {
		return nil, err
	}
	return &engramTemplate, nil
}

func (r *StepRunReconciler) reconcileDelete(ctx context.Context, step *runsv1alpha1.StepRun) (ctrl.Result, error) {
	stepLogger := logging.NewReconcileLogger(ctx, "steprun").WithStepRun(step)
	stepLogger.Info("Reconciling deletion for StepRun")

	job := &batchv1.Job{}
	err := r.ControllerDependencies.Client.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, job)
	if err != nil && !errors.IsNotFound(err) {
		stepLogger.Error(err, "Failed to get Job for cleanup")
		return ctrl.Result{}, err
	}
	if err == nil && job.DeletionTimestamp.IsZero() {
		if err := r.ControllerDependencies.Client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
			stepLogger.Error(err, "Failed to delete Job during cleanup")
			return ctrl.Result{}, err
		}
	}

	if controllerutil.ContainsFinalizer(step, StepRunFinalizer) {
		controllerutil.RemoveFinalizer(step, StepRunFinalizer)
		if err := r.ControllerDependencies.Client.Update(ctx, step); err != nil {
			stepLogger.Error(err, "Failed to remove finalizer")
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

func (r *StepRunReconciler) createJobForStep(ctx context.Context, srun *runsv1alpha1.StepRun, engram *v1alpha1.Engram, engramTemplate *catalogv1alpha1.EngramTemplate) (*batchv1.Job, error) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(srun)

	// Fetch the parent Story to provide the full context for the resolver.
	story, err := r.getStoryForStep(ctx, srun)
	if err != nil {
		return nil, fmt.Errorf("failed to get story for step: %w", err)
	}

	// Resolve the final configuration using the hierarchical resolver.
	resolvedConfig, err := r.ConfigResolver.ResolveExecutionConfig(ctx, srun, story, engram, engramTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve execution config for step '%s': %w", srun.Name, err)
	}

	stepLogger.Info("Resolved ServiceAccountName", "sa", resolvedConfig.ServiceAccountName)

	// Get the StoryRun's initial inputs
	storyRunInputs, err := r.getStoryRunInputs(ctx, srun)
	if err != nil {
		return nil, fmt.Errorf("failed to get storyrun inputs: %w", err)
	}

	// Resolve inputs using the shared CEL evaluator
	stepOutputs, err := r.getPriorStepOutputs(ctx, srun)
	if err != nil {
		return nil, fmt.Errorf("failed to get prior step outputs: %w", err)
	}

	var with map[string]interface{}
	if srun.Spec.Input != nil {
		if err := json.Unmarshal(srun.Spec.Input.Raw, &with); err != nil {
			return nil, fmt.Errorf("failed to unmarshal step 'with' block: %w", err)
		}
	}

	vars := map[string]interface{}{
		"inputs": storyRunInputs,
		"steps":  stepOutputs,
	}
	resolvedInputs, err := r.ControllerDependencies.CELEvaluator.ResolveWithInputs(with, vars)
	if err != nil {
		// This is where we detect if an upstream output is not ready.
		// The CEL evaluator will return a compile error for an undeclared reference.
		return nil, fmt.Errorf("failed to resolve inputs with CEL: %w", err)
	}

	inputBytes, err := json.Marshal(resolvedInputs)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal resolved inputs: %w", err)
	}

	// Setup secrets
	secretEnvVars, volumes, volumeMounts, envFromSources, err := r.setupSecrets(ctx, resolvedConfig, engramTemplate)
	if err != nil {
		return nil, fmt.Errorf("failed to setup secrets for step '%s': %w", srun.Name, err)
	}

	// Base environment variables
	envVars := []corev1.EnvVar{
		{Name: "BUBU_STEPRUN_NAME", Value: srun.Name},
		{Name: "BUBU_STEPRUN_NAMESPACE", Value: srun.Namespace},
		{Name: "BUBU_INPUTS", Value: string(inputBytes)},
		{Name: "BOBRAPET_EXECUTION_MODE", Value: "batch"},
		{Name: "BUBU_GRPC_PORT", Value: fmt.Sprintf("%d", r.ControllerDependencies.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort)},
		{Name: "BUBU_MAX_INLINE_SIZE", Value: fmt.Sprintf("%d", r.ControllerDependencies.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize)},
	}
	envVars = append(envVars, secretEnvVars...)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      srun.Name,
			Namespace: srun.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "bobrapet",
				"app.kubernetes.io/component": "engram",
				"bubu.sh/steprun":             srun.Name,
				"bubu.sh/story-run":           srun.Spec.StoryRunRef.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &resolvedConfig.BackoffLimit,
			TTLSecondsAfterFinished: &resolvedConfig.TTLSecondsAfterFinished,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"bubu.sh/steprun": srun.Name,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy:                resolvedConfig.RestartPolicy,
					ServiceAccountName:           resolvedConfig.ServiceAccountName,
					AutomountServiceAccountToken: &[]bool{true}[0],
					Containers: []corev1.Container{{
						Name:            "engram",
						Image:           resolvedConfig.Image,
						ImagePullPolicy: resolvedConfig.ImagePullPolicy,
						Resources:       resolvedConfig.Resources,
					}},
				},
			},
		},
	}

	// Handle object storage configuration
	if storagePolicy := resolvedConfig.Storage; storagePolicy != nil && storagePolicy.S3 != nil {
		s3Config := storagePolicy.S3
		stepLogger.Info("Configuring pod for S3 object storage access", "bucket", s3Config.Bucket)

		// Add environment variables for the SDK
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

		// Handle secret-based authentication
		if auth := &s3Config.Authentication; auth.SecretRef != nil {
			secretName := auth.SecretRef.Name
			stepLogger.Info("Using S3 secret reference for authentication", "secretName", secretName)
			envFromSources = append(envFromSources, corev1.EnvFromSource{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
				},
			})
		}
	}

	// Final assembly of pod spec
	job.Spec.Template.Spec.Volumes = volumes
	job.Spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
	job.Spec.Template.Spec.Containers[0].Env = envVars
	job.Spec.Template.Spec.Containers[0].EnvFrom = envFromSources

	if err := controllerutil.SetControllerReference(srun, job, r.Scheme); err != nil {
		return nil, err
	}

	return job, nil
}

// getPriorStepOutputs fetches the outputs of all previously completed steps in the same StoryRun.
func (r *StepRunReconciler) getPriorStepOutputs(ctx context.Context, srun *runsv1alpha1.StepRun) (map[string]interface{}, error) {
	outputs := make(map[string]interface{})

	// 1. Fetch the parent StoryRun.
	var storyRun runsv1alpha1.StoryRun
	err := r.ControllerDependencies.Client.Get(ctx, types.NamespacedName{Name: srun.Spec.StoryRunRef.Name, Namespace: srun.Namespace}, &storyRun)
	if err != nil {
		// If the StoryRun isn't found, we can't get outputs, but it's not a fatal error for the StepRun itself.
		return outputs, client.IgnoreNotFound(err)
	}

	// 2. Iterate through its status field containing step outputs.
	if storyRun.Status.StepOutputs == nil {
		return outputs, nil
	}

	// 3. Unmarshal the raw JSON outputs into a map[string]interface{}.
	for stepID, rawOutput := range storyRun.Status.StepOutputs {
		if rawOutput == nil {
			continue
		}
		var outputData map[string]interface{}
		if err := json.Unmarshal(rawOutput.Raw, &outputData); err != nil {
			// Log the error but don't fail the entire step execution.
			// A single malformed output shouldn't prevent other steps from running.
			logging.NewControllerLogger(ctx, "steprun").WithStepRun(srun).Error(err, "Failed to unmarshal output from prior step", "priorStepID", stepID)
			continue
		}

		// The CEL expressions expect to access data via `steps.step-name.outputs.field`.
		// We must wrap the direct output data in a map with an "outputs" key.
		stepContext := map[string]interface{}{
			"outputs": outputData,
		}

		// Provide both the real step ID and a normalized alias (hyphens -> underscores)
		outputs[stepID] = stepContext
		alias := normalizeStepIdentifier(stepID)
		if alias != stepID {
			if _, exists := outputs[alias]; !exists {
				outputs[alias] = stepContext
			}
		}
	}

	// 4. Return the map.
	return outputs, nil
}

// getStoryRunInputs fetches the initial inputs from the parent StoryRun.
func (r *StepRunReconciler) getStoryRunInputs(ctx context.Context, srun *runsv1alpha1.StepRun) (map[string]interface{}, error) {
	var storyRun runsv1alpha1.StoryRun
	err := r.ControllerDependencies.Client.Get(ctx, types.NamespacedName{Name: srun.Spec.StoryRunRef.Name, Namespace: srun.Namespace}, &storyRun)
	if err != nil {
		// If the StoryRun isn't found, we can't get inputs. This is a fatal error for input resolution.
		return nil, err
	}

	if storyRun.Spec.Inputs == nil {
		return make(map[string]interface{}), nil
	}

	var inputs map[string]interface{}
	if err := json.Unmarshal(storyRun.Spec.Inputs.Raw, &inputs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal storyrun inputs: %w", err)
	}
	return inputs, nil
}

func (r *StepRunReconciler) handleJobStatus(ctx context.Context, step *runsv1alpha1.StepRun, job *batchv1.Job) (ctrl.Result, error) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(step).WithValues("job_name", job.Name)

	if job.Status.Succeeded > 0 {
		stepLogger.Info("Job succeeded. Waiting for Engram to update StepRun status.")
		return ctrl.Result{}, nil
	}

	if job.Status.Failed > 0 {
		stepLogger.Info("Job failed. The Engram pod is expected to update the StepRun status with failure details.")
		if err := r.setStepRunPhase(ctx, step, enums.PhaseFailed, "Job execution failed. Check pod logs for details."); err != nil {
			stepLogger.Error(err, "Failed to forcefully update StepRun status after job failure")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StepRun{}).
		WithOptions(opts).
		Owns(&batchv1.Job{}).
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
	if err := r.List(ctx, &engrams, client.MatchingFields{"spec.templateRef": obj.GetName()}); err != nil {
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
func (r *StepRunReconciler) setupSecrets(_ context.Context, resolvedConfig *config.ResolvedExecutionConfig, engramTemplate *catalogv1alpha1.EngramTemplate) ([]corev1.EnvVar, []corev1.Volume, []corev1.VolumeMount, []corev1.EnvFromSource, error) {
	var envVars []corev1.EnvVar
	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount
	var envFromSources []corev1.EnvFromSource

	if resolvedConfig.Secrets == nil || engramTemplate.Spec.SecretSchema == nil {
		return envVars, volumes, volumeMounts, envFromSources, nil
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

	return envVars, volumes, volumeMounts, envFromSources, nil
}
