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
	"k8s.io/apimachinery/pkg/api/resource"
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

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	parentcontroller "github.com/bubustack/bobrapet/internal/controller"
	"github.com/bubustack/bobrapet/internal/logging"
	"github.com/bubustack/bobrapet/internal/metrics"
	"github.com/bubustack/bobrapet/internal/storage"
)

// StepRunReconciler reconciles a StepRun object
type StepRunReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	StorageManager   storage.StorageManager
	conditionManager *parentcontroller.ConditionManager
	finalizerManager *parentcontroller.FinalizerManager
	// reconcilePattern removed
}

// Finalizer management is now handled by the shared FinalizerManager

// +kubebuilder:rbac:groups=runs.bubu.sh,resources=stepruns,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=runs.bubu.sh,resources=stepruns/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=runs.bubu.sh,resources=stepruns/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=catalog.bubu.sh,resources=engramtemplates,verbs=get;list;watch

// Reconcile manages the StepRun lifecycle: creates Jobs, tracks execution, handles retries
func (r *StepRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "steprun")

	// Start OTel span for StepRun reconciliation
	var span oteltrace.Span
	if config.IsTelemetryEnabled() {
		ctx, span = otel.Tracer("bobrapet/controller/steprun").Start(ctx, "Reconcile StepRun")
		span.SetAttributes(
			attribute.String("resource", "StepRun"),
			attribute.String("name", req.Name),
			attribute.String("namespace", req.Namespace),
		)
		defer span.End()
	}

	// Reconcile timing for RED metrics
	reconcileStart := time.Now()
	defer func() {
		metrics.RecordControllerReconcile("steprun", time.Since(reconcileStart), nil)
	}()

	var step runsv1alpha1.StepRun
	if err := r.Get(ctx, req.NamespacedName, &step); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Attach more attributes now that we have the object
	if config.IsTelemetryEnabled() {
		span.SetAttributes(
			attribute.String("storyRunRef", step.Spec.StoryRunRef),
			attribute.String("stepId", step.Spec.StepID),
			attribute.String("engramRef", step.Spec.EngramRef),
			attribute.String("namespace", step.Namespace),
		)
	}

	// Add StepRun context to logger
	stepLogger := log.WithStepRun(&step)
	log.ReconcileStart("Processing StepRun")

	// Initialize managers if needed
	if r.conditionManager == nil {
		r.conditionManager = parentcontroller.NewConditionManager(step.Generation)
	}
	if r.finalizerManager == nil {
		r.finalizerManager = parentcontroller.NewFinalizerManager(r.Client)
	}
	// ReconcilePattern removed - using direct reconcile logic

	// Add finalizer if missing
	if !r.finalizerManager.HasFinalizer(&step, parentcontroller.StepRunFinalizer) {
		stepLogger.Info("Adding finalizer to StepRun")
		r.finalizerManager.AddFinalizer(&step, parentcontroller.StepRunFinalizer)
		return ctrl.Result{}, r.Update(ctx, &step)
	}

	// Handle deletion
	if step.DeletionTimestamp != nil {
		stepLogger.Info("StepRun deletion requested")
		// Note: StepRun doesn't have Status.Conditions yet, but we can add terminating logic here
		stepLogger.Info("StepRun terminating", "reason", "DeletionRequested")
		return r.handleCleanup(ctx, &step)
	}

	// Terminal states - no further action needed
	if step.Status.Phase == "Succeeded" || step.Status.Phase == "Failed" || step.Status.Phase == "Canceled" {
		stepLogger.Info("StepRun in terminal phase", "phase", step.Status.Phase)
		log.ReconcileSuccess("StepRun processing completed")

		// Record final metrics
		if step.Status.StartedAt != "" && step.Status.FinishedAt != "" {
			if startTime, err := time.Parse(time.RFC3339, step.Status.StartedAt); err == nil {
				if endTime, err := time.Parse(time.RFC3339, step.Status.FinishedAt); err == nil {
					duration := endTime.Sub(startTime)
					metrics.RecordStepRunDuration(step.Namespace, step.Spec.StoryRunRef, step.Name, step.Status.Phase, duration)
				}
			}
		}
		return ctrl.Result{}, nil
	}

	// Initialize phase if not set
	if step.Status.Phase == "" {
		stepLogger.Info("Initializing StepRun phase")
		step.Status.Phase = "Pending"
		step.Status.StartedAt = time.Now().Format(time.RFC3339)

		// Initialize dependencies from Story
		if err := r.initializeDependencies(ctx, &step); err != nil {
			log.ReconcileError(err, "Failed to initialize dependencies")
			return ctrl.Result{}, err
		}

		if err := r.Status().Update(ctx, &step); err != nil {
			log.ReconcileError(err, "Failed to initialize StepRun status")
			return ctrl.Result{}, err
		}
	}

	// Check dependencies before proceeding
	if step.Status.Phase == "Pending" {
		ready, err := r.checkDependencies(ctx, &step)
		if err != nil {
			log.ReconcileError(err, "Failed to check dependencies")
			return ctrl.Result{}, err
		}

		if !ready {
			stepLogger.V(1).Info("Dependencies not ready, waiting")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
	}

	// Direct lookup instead of listing all jobs
	stepLogger.V(1).Info("Looking for existing Job")
	activeJob, err := r.findJobForStepRun(ctx, &step)
	if err != nil && client.IgnoreNotFound(err) != nil {
		log.ReconcileError(err, "Failed to find Job for StepRun")
		return ctrl.Result{}, err
	}

	// If no Job exists, create one
	if activeJob == nil {
		stepLogger.Info("Creating new Job for StepRun")
		_, err := r.createJobForStep(ctx, &step)
		if err != nil {
			log.ReconcileError(err, "Failed to create Job for StepRun")
			step.Status.Phase = "Failed"
			step.Status.ExitClass = "error"
			_ = r.Status().Update(ctx, &step)
			metrics.RecordStepRunDuration(step.Namespace, step.Spec.StoryRunRef, step.Name, "Failed", 0)
			return ctrl.Result{}, err
		}

		step.Status.Phase = "Running"
		// Persist trace ID for correlation
		if config.IsTelemetryEnabled() {
			if sc := oteltrace.SpanFromContext(ctx).SpanContext(); sc.IsValid() {
				step.Status.TraceID = sc.TraceID().String()
			}
		}
		if err := r.Status().Update(ctx, &step); err != nil {
			log.ReconcileError(err, "Failed to update StepRun status to Running")
			return ctrl.Result{}, err
		}

		log.ReconcileSuccess("StepRun Job created successfully")
		// No periodic requeue; rely on Job watch events
		return ctrl.Result{}, nil
	}

	// Job exists - check its status
	stepLogger.V(1).Info("Job exists, checking status", "job_name", activeJob.Name)
	return r.handleJobStatus(ctx, &step, activeJob)
}

// createJobForStep creates a Kubernetes Job for the StepRun
func (r *StepRunReconciler) createJobForStep(ctx context.Context, step *runsv1alpha1.StepRun) (*batchv1.Job, error) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(step)

	// Get story name from StoryRun for storage
	storyName, err := r.getStoryNameForStep(ctx, step)
	if err != nil {
		stepLogger.Error(err, "Failed to get story name for storage")
		return nil, fmt.Errorf("failed to get story name: %w", err)
	}

	// Ensure storyrun-specific storage exists (shared persistent PVC for all steps)
	pvcName, err := r.StorageManager.EnsureStoryRunStorage(ctx, step.Namespace, storyName, step.Spec.StoryRunRef)
	if err != nil {
		stepLogger.Error(err, "Failed to ensure storyrun storage", "story", storyName, "storyrun", step.Spec.StoryRunRef)
		return nil, fmt.Errorf("failed to ensure storage: %w", err)
	}

	stepLogger.V(1).Info("Using story storage", "story", storyName, "pvc", pvcName)

	// Resolve the container image and pull policy
	image := r.resolveImage(ctx, step)
	imagePullPolicy := r.resolveImagePullPolicy(ctx, step)

	// Resolve inputs from dependent steps using GitHub Actions-style expressions
	inputBytes, err := r.resolveStepInputs(ctx, step)
	if err != nil {
		stepLogger.Error(err, "Failed to resolve step inputs")
		return nil, fmt.Errorf("failed to resolve inputs: %w", err)
	}

	// Delegate large data handling to the engram itself for better performance
	// Pass raw input data - engram will handle artifact resolution if needed
	processedInputs := &runtime.RawExtension{Raw: inputBytes}

	// Get storage configuration
	volumeMounts, volumes, storageEnvVars := r.StorageManager.GetVolumeSpec(pvcName)

	// Create the Job
	jobName := fmt.Sprintf("%s-job", step.Name)
	if step.Status.Retries > 0 {
		jobName = fmt.Sprintf("%s-retry-%d", step.Name, step.Status.Retries)
	}

	// Combine environment variables
	envVars := []corev1.EnvVar{
		{Name: "ENGRAM_INPUT", Value: string(processedInputs.Raw)},
		{Name: "ENGRAM_RUN_ID", Value: step.Spec.StepID},
		{Name: "ENGRAM_NAMESPACE", Value: step.Namespace},
		{Name: "ENGRAM_DEADLINE", Value: step.Spec.Deadline},
		{Name: "ENGRAM_IDEMPOTENCY_TOKEN", Value: step.Spec.IdempotencyToken},
		{Name: "ENGRAM_STORY", Value: storyName},
		{Name: "ENGRAM_PVC", Value: pvcName},
		// Tracing context propagation
		{Name: "TRACEPARENT", Value: func() string {
			if config.IsTelemetryEnabled() {
				return buildTraceparent(ctx)
			}
			return ""
		}()},
	}
	// Add storage environment variables
	envVars = append(envVars, storageEnvVars...)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: step.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "bobrapet",
				"app.kubernetes.io/component": "engram",
				"bubu.sh/steprun":             step.Name,
				"bubu.sh/story-run":           step.Spec.StoryRunRef,
				"bobrapet.bubu.sh/story":      storyName,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &[]int32{0}[0], // No automatic retries - we handle this ourselves
			TTLSecondsAfterFinished: &[]int32{300}[0],
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Volumes:       volumes,
					Containers: []corev1.Container{{
						Name:            "engram",
						Image:           image,
						ImagePullPolicy: imagePullPolicy,
						Env:             envVars,
						VolumeMounts:    volumeMounts,
						SecurityContext: &corev1.SecurityContext{
							RunAsNonRoot:             &[]bool{true}[0],
							RunAsUser:                &[]int64{1000}[0], // Use numeric user ID
							AllowPrivilegeEscalation: &[]bool{false}[0],
							ReadOnlyRootFilesystem:   &[]bool{true}[0],
							Capabilities: &corev1.Capabilities{
								Drop: []corev1.Capability{"ALL"},
							},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("100m"),
								corev1.ResourceMemory: resource.MustParse("128Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1"),
								corev1.ResourceMemory: resource.MustParse("512Mi"),
							},
						},
					}},
				},
			},
		},
	}

	// Set owner reference
	if err := ctrl.SetControllerReference(step, job, r.Scheme); err != nil {
		return nil, err
	}

	// Create the Job
	if err := r.Create(ctx, job); err != nil {
		return nil, err
	}

	return job, nil
}

// handleJobStatus processes the Job status and updates StepRun accordingly
func (r *StepRunReconciler) handleJobStatus(ctx context.Context, step *runsv1alpha1.StepRun, job *batchv1.Job) (ctrl.Result, error) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(step).
		WithValues("job_name", job.Name)

	// Update pod name for logging
	r.updatePodName(ctx, step, job)

	// Check for success
	if job.Status.Succeeded > 0 {
		step.Status.Phase = "Succeeded"
		step.Status.ExitClass = "success"
		step.Status.ExitCode = 0
		step.Status.FinishedAt = time.Now().Format(time.RFC3339)

		// Capture outputs from pod logs (engrams should print JSON output to stdout)
		output, err := r.captureJobOutput(ctx, job, step)
		if err != nil {
			stepLogger.Error(err, "Failed to capture job output")
			// Don't fail the step if we can't capture output
		} else if len(output) > 0 {
			// Store output directly - engram is responsible for artifact management
			step.Status.Output = &runtime.RawExtension{Raw: output}
		}

		if err := r.Status().Update(ctx, step); err != nil {
			return ctrl.Result{}, err
		}

		// RED metrics for StepRun and job execution
		var dur time.Duration
		if step.Status.StartedAt != "" {
			if start, err := time.Parse(time.RFC3339, step.Status.StartedAt); err == nil {
				dur = time.Since(start)
			}
		}
		metrics.RecordStepRunMetrics(step.Namespace, step.Spec.EngramRef, "Succeeded", dur)
		image := ""
		if len(job.Spec.Template.Spec.Containers) > 0 {
			image = job.Spec.Template.Spec.Containers[0].Image
		}
		metrics.RecordJobExecution(step.Namespace, image, "succeeded", dur)
		return ctrl.Result{}, nil
	}

	// Check for failure
	if job.Status.Failed > 0 {
		exitCode, err := r.getExitCode(ctx, job)
		if err != nil {
			stepLogger.Error(err, "Failed to get exit code from Job")
			exitCode = 11 // Default to terminal
		}

		step.Status.ExitCode = exitCode
		step.Status.FinishedAt = time.Now().Format(time.RFC3339)

		// Handle different exit codes according to ABI
		switch exitCode {
		case 10: // Retriable error
			if r.shouldRetry(step) {
				// Delete failed job so a new one can be created on next reconcile
				if err := r.Delete(ctx, job); err != nil {
					stepLogger.Error(err, "Failed to delete failed job before retry")
				}
				// Retry counter
				metrics.StepRunRetries.WithLabelValues(step.Namespace, step.Spec.EngramRef, "retriable").Inc()
				return r.retryStep(ctx, step)
			}
			step.Status.Phase = "Failed"
			step.Status.ExitClass = "retry"
		case 11: // Terminal error
			step.Status.Phase = "Failed"
			step.Status.ExitClass = "terminal"
		case 12: // Rate limited
			step.Status.ExitClass = "rateLimited"
			metrics.StepRunRetries.WithLabelValues(step.Namespace, step.Spec.EngramRef, "rate_limited").Inc()
			if err := r.Status().Update(ctx, step); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		default: // Unknown error - treat as terminal
			step.Status.Phase = "Failed"
			step.Status.ExitClass = "terminal"
		}

		if err := r.Status().Update(ctx, step); err != nil {
			return ctrl.Result{}, err
		}

		// RED metrics for StepRun and job execution on failure
		var dur time.Duration
		if step.Status.StartedAt != "" {
			if start, err := time.Parse(time.RFC3339, step.Status.StartedAt); err == nil {
				dur = time.Since(start)
			}
		}
		metrics.RecordStepRunMetrics(step.Namespace, step.Spec.EngramRef, "Failed", dur)
		image := ""
		if len(job.Spec.Template.Spec.Containers) > 0 {
			image = job.Spec.Template.Spec.Containers[0].Image
		}
		metrics.RecordJobExecution(step.Namespace, image, "failed", dur)
		return ctrl.Result{}, nil
	}

	// Job is still running - check for timeout
	if r.isTimedOut(step) {
		step.Status.Phase = "Failed"
		step.Status.ExitClass = "timeout"
		step.Status.FinishedAt = time.Now().Format(time.RFC3339)

		// Cancel the job
		if err := r.Delete(ctx, job); err != nil {
			stepLogger.Error(err, "Failed to delete timed out job")
		}

		if err := r.Status().Update(ctx, step); err != nil {
			return ctrl.Result{}, err
		}

		// Timeout metrics
		var dur time.Duration
		if step.Status.StartedAt != "" {
			if start, err := time.Parse(time.RFC3339, step.Status.StartedAt); err == nil {
				dur = time.Since(start)
			}
		}
		metrics.RecordStepRunMetrics(step.Namespace, step.Spec.EngramRef, "Failed", dur)
		image := ""
		if len(job.Spec.Template.Spec.Containers) > 0 {
			image = job.Spec.Template.Spec.Containers[0].Image
		}
		metrics.RecordJobExecution(step.Namespace, image, "timeout", dur)
		return ctrl.Result{}, nil
	}

	// Still running - rely on Job events to requeue
	return ctrl.Result{}, nil
}

// Helper functions

// getStoryNameForStep gets the story name from the StoryRun reference
func (r *StepRunReconciler) getStoryNameForStep(ctx context.Context, step *runsv1alpha1.StepRun) (string, error) {
	// Get the StoryRun
	var storyRun runsv1alpha1.StoryRun
	err := r.Get(ctx, types.NamespacedName{
		Name:      step.Spec.StoryRunRef,
		Namespace: step.Namespace,
	}, &storyRun)
	if err != nil {
		return "", fmt.Errorf("failed to get StoryRun %s: %w", step.Spec.StoryRunRef, err)
	}

	if storyRun.Spec.StoryRef == "" {
		return "", fmt.Errorf("StoryRun %s has no storyRef", step.Spec.StoryRunRef)
	}

	return storyRun.Spec.StoryRef, nil
}

// checkDependencies checks if all step dependencies are ready
func (r *StepRunReconciler) checkDependencies(ctx context.Context, stepRun *runsv1alpha1.StepRun) (bool, error) {
	if len(stepRun.Status.Dependencies) == 0 {
		return true, nil // No dependencies
	}

	resolver := NewExpressionResolver(r.Client, stepRun.Namespace, stepRun.Spec.StoryRunRef)
	ready, pendingDeps, err := resolver.CheckDependencies(ctx, stepRun.Status.Dependencies)
	if err != nil {
		return false, fmt.Errorf("failed to check dependencies: %w", err)
	}

	if !ready {
		stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(stepRun)
		stepLogger.V(1).Info("Waiting for dependencies", "pending", pendingDeps)
	}

	return ready, nil
}

// resolveStepInputs resolves GitHub Actions-style expressions in step inputs
func (r *StepRunReconciler) resolveStepInputs(ctx context.Context, stepRun *runsv1alpha1.StepRun) ([]byte, error) {
	// Get the story and step definition
	storyRun, err := r.getStoryRunForStep(ctx, stepRun)
	if err != nil {
		return nil, err
	}

	story, err := r.getStory(ctx, storyRun)
	if err != nil {
		return nil, err
	}

	// Find this step in the story
	var stepSpec *bubushv1alpha1.Step
	for _, step := range story.Spec.Steps {
		stepID := step.ID
		if stepID == "" {
			stepID = step.Name // Fallback to name if ID not set
		}
		if stepID == stepRun.Spec.StepID {
			stepSpec = &step
			break
		}
	}

	if stepSpec == nil {
		return nil, fmt.Errorf("step %s not found in story", stepRun.Spec.StepID)
	}

	// Parse step inputs from the 'with' field
	var inputs map[string]interface{}
	if stepSpec.With != nil && len(stepSpec.With.Raw) > 0 {
		if err := json.Unmarshal(stepSpec.With.Raw, &inputs); err != nil {
			return nil, fmt.Errorf("failed to parse step inputs: %w", err)
		}
	} else {
		inputs = make(map[string]interface{})
	}

	// Resolve expressions
	resolver := NewExpressionResolver(r.Client, stepRun.Namespace, stepRun.Spec.StoryRunRef)
	resolvedInputs, err := resolver.ResolveInputs(ctx, inputs)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve expressions: %w", err)
	}

	return json.Marshal(resolvedInputs)
}

// getStoryRunForStep gets the StoryRun for a StepRun
func (r *StepRunReconciler) getStoryRunForStep(ctx context.Context, stepRun *runsv1alpha1.StepRun) (*runsv1alpha1.StoryRun, error) {
	var storyRun runsv1alpha1.StoryRun
	err := r.Get(ctx, types.NamespacedName{
		Name:      stepRun.Spec.StoryRunRef,
		Namespace: stepRun.Namespace,
	}, &storyRun)
	if err != nil {
		return nil, fmt.Errorf("failed to get StoryRun %s: %w", stepRun.Spec.StoryRunRef, err)
	}
	return &storyRun, nil
}

// getStory gets the Story from a StoryRun
func (r *StepRunReconciler) getStory(ctx context.Context, storyRun *runsv1alpha1.StoryRun) (*bubushv1alpha1.Story, error) {
	var story bubushv1alpha1.Story
	err := r.Get(ctx, types.NamespacedName{
		Name:      storyRun.Spec.StoryRef,
		Namespace: storyRun.Namespace,
	}, &story)
	if err != nil {
		return nil, fmt.Errorf("failed to get Story %s: %w", storyRun.Spec.StoryRef, err)
	}
	return &story, nil
}

// initializeDependencies initializes step dependencies from the Story definition
func (r *StepRunReconciler) initializeDependencies(ctx context.Context, stepRun *runsv1alpha1.StepRun) error {
	storyRun, err := r.getStoryRunForStep(ctx, stepRun)
	if err != nil {
		return err
	}

	story, err := r.getStory(ctx, storyRun)
	if err != nil {
		return err
	}

	// Find this step in the story
	for _, step := range story.Spec.Steps {
		stepID := step.ID
		if stepID == "" {
			stepID = step.Name // Fallback to name if ID not set
		}
		if stepID == stepRun.Spec.StepID {
			// Set dependencies from the 'needs' field
			if len(step.Needs) > 0 {
				stepRun.Status.Dependencies = make([]string, len(step.Needs))
				copy(stepRun.Status.Dependencies, step.Needs)
			}
			break
		}
	}

	return nil
}

// captureJobOutput captures output from job pod logs
func (r *StepRunReconciler) captureJobOutput(ctx context.Context, job *batchv1.Job, stepRun *runsv1alpha1.StepRun) ([]byte, error) {
	// Find pods for this job
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingFields{"metadata.ownerReferences.uid": string(job.UID)}); err != nil {
		return nil, fmt.Errorf("failed to list job pods: %w", err)
	}

	for _, pod := range pods.Items {
		// Skip if pod is not completed
		if pod.Status.Phase != corev1.PodSucceeded {
			continue
		}

		// TODO: Implement actual log reading from pod
		// For now, we'll simulate output capture
		// In a real implementation, you would:
		// 1. Get pod logs using clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{})
		// 2. Parse the logs to extract JSON output (engrams should print JSON to stdout)
		// 3. Return the parsed output

		// Placeholder: return empty output for now
		return []byte("{}"), nil
	}

	return nil, fmt.Errorf("no completed pods found for job %s", job.Name)
}

// resolveImage resolves the container image for a StepRun
func (r *StepRunReconciler) resolveImage(ctx context.Context, step *runsv1alpha1.StepRun) string {
	// If image is explicitly set, use it
	if step.Spec.Image != "" {
		return step.Spec.Image
	}

	// Try to resolve from Engram CRD
	if step.Spec.EngramRef != "" {
		var engram bubushv1alpha1.Engram
		if err := r.Get(ctx, types.NamespacedName{
			Name:      step.Spec.EngramRef,
			Namespace: step.Namespace,
		}, &engram); err == nil {
			if engram.Spec.Engine != nil && engram.Spec.Engine.TemplateRef != "" {
				// Try to resolve from EngramTemplate first
				if image := r.resolveImageFromTemplate(ctx, engram.Spec.Engine.TemplateRef); image != "" {
					return image
				}
				// Fallback to convention if template not found
				return "ghcr.io/bubustack/" + engram.Spec.Engine.TemplateRef + ":latest"
			}
		}
	}

	// Default fallback image
	return "ghcr.io/bubustack/engram-default:latest"
}

// resolveImagePullPolicy resolves the image pull policy for a StepRun
func (r *StepRunReconciler) resolveImagePullPolicy(ctx context.Context, step *runsv1alpha1.StepRun) corev1.PullPolicy {
	// Try to resolve from Engram CRD
	if step.Spec.EngramRef != "" {
		var engram bubushv1alpha1.Engram
		if err := r.Get(ctx, types.NamespacedName{
			Name:      step.Spec.EngramRef,
			Namespace: step.Namespace,
		}, &engram); err == nil {
			if engram.Spec.Engine != nil && engram.Spec.Engine.ImagePullPolicy != "" {
				return corev1.PullPolicy(engram.Spec.Engine.ImagePullPolicy)
			}
		}
	}

	// Default to IfNotPresent
	return corev1.PullIfNotPresent
}

// resolveImageFromTemplate resolves the container image from an EngramTemplate
func (r *StepRunReconciler) resolveImageFromTemplate(ctx context.Context, templateRef string) string {
	var template catalogv1alpha1.EngramTemplate
	if err := r.Get(ctx, types.NamespacedName{
		Name: templateRef,
		// EngramTemplates are cluster-scoped, so no namespace
	}, &template); err != nil {
		return ""
	}

	return template.Spec.Image
}

// updatePodName finds pods efficiently using labels
func (r *StepRunReconciler) updatePodName(ctx context.Context, step *runsv1alpha1.StepRun, job *batchv1.Job) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(step.Namespace),
		client.MatchingFields{"metadata.ownerReferences.uid": string(job.UID)}); err != nil {
		return
	}

	for _, pod := range pods.Items {
		step.Status.PodName = pod.Name
		return // Take first pod
	}
}

// getExitCode extracts exit code efficiently using labels
func (r *StepRunReconciler) getExitCode(ctx context.Context, job *batchv1.Job) (int32, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods,
		client.InNamespace(job.Namespace),
		client.MatchingFields{"metadata.ownerReferences.uid": string(job.UID)}); err != nil {
		return 11, err
	}

	for _, pod := range pods.Items {
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.State.Terminated != nil {
				return cs.State.Terminated.ExitCode, nil
			}
		}
	}

	return 11, fmt.Errorf("no terminated container found")
}

// Direct job lookup instead of listing all jobs
func (r *StepRunReconciler) findJobForStepRun(ctx context.Context, step *runsv1alpha1.StepRun) (*batchv1.Job, error) {
	// Try direct lookup by expected name pattern first
	expectedJobName := fmt.Sprintf("%s-job", step.Name)
	if step.Status.Retries > 0 {
		expectedJobName = fmt.Sprintf("%s-retry-%d", step.Name, step.Status.Retries)
	}

	var job batchv1.Job
	err := r.Get(ctx, types.NamespacedName{
		Name:      expectedJobName,
		Namespace: step.Namespace,
	}, &job)

	if err == nil {
		return &job, nil
	}

	// If direct lookup fails, use labels for efficient query
	var jobs batchv1.JobList
	err = r.List(ctx, &jobs,
		client.InNamespace(step.Namespace),
		client.MatchingLabels{
			"bubu.sh/steprun": step.Name,
		})

	if err != nil {
		return nil, err
	}

	if len(jobs.Items) == 0 {
		return nil, nil
	}

	return &jobs.Items[0], nil
}

// shouldRetry determines if a step should be retried based on policy and current attempts
func (r *StepRunReconciler) shouldRetry(step *runsv1alpha1.StepRun) bool {
	maxRetries := 3 // Default
	if step.Spec.RetryPolicy != nil && step.Spec.RetryPolicy.MaxRetries > 0 {
		maxRetries = step.Spec.RetryPolicy.MaxRetries
	}

	return step.Status.Retries < maxRetries
}

// retryStep handles retrying a failed step by creating a new job
func (r *StepRunReconciler) retryStep(ctx context.Context, step *runsv1alpha1.StepRun) (ctrl.Result, error) {
	step.Status.Retries++

	// Calculate backoff delay
	delay := r.calculateBackoffDelay(step)

	if err := r.Status().Update(ctx, step); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: delay}, nil
}

// calculateBackoffDelay calculates the delay for retry based on policy
func (r *StepRunReconciler) calculateBackoffDelay(step *runsv1alpha1.StepRun) time.Duration {
	baseDelay := 2 * time.Second
	maxDelay := 1 * time.Minute

	if step.Spec.RetryPolicy != nil {
		if step.Spec.RetryPolicy.BaseDelay != "" {
			if parsed, err := time.ParseDuration(step.Spec.RetryPolicy.BaseDelay); err == nil {
				baseDelay = parsed
			}
		}
		if step.Spec.RetryPolicy.MaxDelay != "" {
			if parsed, err := time.ParseDuration(step.Spec.RetryPolicy.MaxDelay); err == nil {
				maxDelay = parsed
			}
		}
	}

	// Simple exponential backoff
	delay := baseDelay * time.Duration(1<<uint(step.Status.Retries))
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

// isTimedOut checks if the step has exceeded its timeout
func (r *StepRunReconciler) isTimedOut(step *runsv1alpha1.StepRun) bool {
	if step.Status.StartedAt == "" {
		return false
	}

	timeout := 5 * time.Minute // Default timeout
	if step.Spec.Timeout != "" {
		if parsed, err := time.ParseDuration(step.Spec.Timeout); err == nil {
			timeout = parsed
		}
	}

	startTime, err := time.Parse(time.RFC3339, step.Status.StartedAt)
	if err != nil {
		return false
	}

	return time.Since(startTime) > timeout
}

// SetupWithManager sets up the controller with the Manager.
func (r *StepRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	ctx := context.Background()

	// Index Jobs by StepRun owner for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(ctx, &batchv1.Job{}, "metadata.ownerReferences.name",
		func(obj client.Object) []string {
			job := obj.(*batchv1.Job)
			var names []string
			for _, ref := range job.OwnerReferences {
				if ref.Kind == "StepRun" {
					names = append(names, ref.Name)
				}
			}
			return names
		}); err != nil {
		return err
	}

	// Index Pods by Job owner UID for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(ctx, &corev1.Pod{}, "metadata.ownerReferences.uid",
		func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			var uids []string
			for _, ref := range pod.OwnerReferences {
				if ref.Kind == "Job" {
					uids = append(uids, string(ref.UID))
				}
			}
			return uids
		}); err != nil {
		return err
	}

	return r.setupControllerWithOptions(mgr, controller.Options{
		MaxConcurrentReconciles: 15, // Will be overridden by config
	})
}

// SetupWithManagerAndConfig sets up the controller with configurable options
func (r *StepRunReconciler) SetupWithManagerAndConfig(mgr ctrl.Manager, opts controller.Options) error {
	ctx := context.Background()

	// Index Jobs by StepRun owner for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(ctx, &batchv1.Job{}, "metadata.ownerReferences.name",
		func(obj client.Object) []string {
			job := obj.(*batchv1.Job)
			var names []string
			for _, ref := range job.OwnerReferences {
				if ref.Kind == "StepRun" {
					names = append(names, ref.Name)
				}
			}
			return names
		}); err != nil {
		return err
	}

	// Index Pods by Job owner UID for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(ctx, &corev1.Pod{}, "metadata.ownerReferences.uid",
		func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			var uids []string
			for _, ref := range pod.OwnerReferences {
				if ref.Kind == "Job" {
					uids = append(uids, string(ref.UID))
				}
			}
			return uids
		}); err != nil {
		return err
	}

	return r.setupControllerWithOptions(mgr, opts)
}

func (r *StepRunReconciler) setupControllerWithOptions(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StepRun{}).
		Owns(&batchv1.Job{}).
		WithOptions(opts).
		WithEventFilter(predicate.Or(
			predicate.GenerationChangedPredicate{},
			predicate.AnnotationChangedPredicate{},
			predicate.LabelChangedPredicate{},
			// Always process deletion events for finalizer handling
			predicate.Funcs{
				DeleteFunc: func(e event.DeleteEvent) bool {
					return true
				},
				CreateFunc: func(e event.CreateEvent) bool {
					return true // Process all creates
				},
				UpdateFunc: func(e event.UpdateEvent) bool {
					// Process all updates, not just spec changes
					return true
				},
			},
		)).
		Named("runs-steprun").
		Complete(r)
}

// handleEngramPauseRequest processes pause requests from engrams
func (r *StepRunReconciler) handleEngramPauseRequest(ctx context.Context, stepRun *runsv1alpha1.StepRun, pauseType string) error {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(stepRun).
		WithValues("pause_type", pauseType)

	// Parse pause request from engram output
	var pauseRequest struct {
		Status string `json:"status"`
		Pause  struct {
			Type             string                 `json:"type"`
			Reason           string                 `json:"reason"`
			ResumeConditions map[string]interface{} `json:"resumeConditions"`
			ResumeEngram     string                 `json:"resumeEngram,omitempty"`
			ResumeConfig     map[string]interface{} `json:"resumeConfig,omitempty"`
			ResumeData       map[string]interface{} `json:"resumeData,omitempty"`
		} `json:"pause"`
	}

	if stepRun.Status.Output != nil {
		if err := json.Unmarshal(stepRun.Status.Output.Raw, &pauseRequest); err != nil {
			stepLogger.Error(err, "Failed to parse pause request from engram output")
			return err
		}
	}

	// Create pause info
	now := metav1.Now()
	pauseInfo := &runsv1alpha1.PauseInfo{
		PausedAt: &now,
		Reason:   pauseRequest.Pause.Reason,
	}

	// Handle different pause types
	switch pauseType {
	case "human_approval":
		pauseInfo.ExternalTriggerStatus = "waiting_for_approval"
		r.setupApprovalTrigger(ctx, stepRun, pauseRequest.Pause)

	case "external_data":
		pauseInfo.ExternalTriggerStatus = "waiting_for_data"
		r.setupExternalDataWait(ctx, stepRun, pauseRequest.Pause)

	case "scheduled_delay":
		if resumeAt, ok := pauseRequest.Pause.ResumeConditions["resumeAt"].(string); ok {
			if t, err := time.Parse(time.RFC3339, resumeAt); err == nil {
				pauseAt := metav1.NewTime(t)
				pauseInfo.ResumeAt = &pauseAt
			}
		}

	case "conditional_wait":
		pauseInfo.ExternalTriggerStatus = "waiting_for_condition"
		r.setupConditionalWait(ctx, stepRun, pauseRequest.Pause)
	}

	stepRun.Status.PauseInfo = pauseInfo

	stepLogger.Info("Engram requested pause",
		"reason", pauseRequest.Pause.Reason)

	return nil
}

// setupApprovalTrigger creates approval mechanism
func (r *StepRunReconciler) setupApprovalTrigger(ctx context.Context, stepRun *runsv1alpha1.StepRun, pauseConfig interface{}) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(stepRun)

	// Parse approval configuration from pauseConfig
	config := r.parseApprovalConfig(pauseConfig)
	if config == nil {
		stepLogger.Error(fmt.Errorf("invalid approval config"), "Failed to parse approval configuration")
		return
	}

	// Create an approval annotation on the StepRun
	if stepRun.Annotations == nil {
		stepRun.Annotations = make(map[string]string)
	}

	approvalKey := fmt.Sprintf("runs.bubu.sh/approval.%s", stepRun.Name)
	stepRun.Annotations[approvalKey] = "pending"
	stepRun.Annotations[fmt.Sprintf("%s.approver", approvalKey)] = config.RequiredApprover
	stepRun.Annotations[fmt.Sprintf("%s.reason", approvalKey)] = config.Reason
	stepRun.Annotations[fmt.Sprintf("%s.timeout", approvalKey)] = config.Timeout.String()

	// If external notification is configured, trigger it
	if config.NotificationEngram != "" {
		r.createNotificationEngram(ctx, stepRun, config)
	}

	stepLogger.Info("Approval trigger set up",
		"approver", config.RequiredApprover,
		"reason", config.Reason,
		"timeout", config.Timeout)
}

// setupExternalDataWait sets up external data monitoring
func (r *StepRunReconciler) setupExternalDataWait(ctx context.Context, stepRun *runsv1alpha1.StepRun, pauseConfig interface{}) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(stepRun)

	// Parse external data wait configuration
	config := r.parseExternalDataConfig(pauseConfig)
	if config == nil {
		stepLogger.Error(fmt.Errorf("invalid external data config"), "Failed to parse external data configuration")
		return
	}

	// Create monitoring annotation on the StepRun
	if stepRun.Annotations == nil {
		stepRun.Annotations = make(map[string]string)
	}

	waitKey := fmt.Sprintf("runs.bubu.sh/external-wait.%s", stepRun.Name)
	stepRun.Annotations[waitKey] = "waiting"
	stepRun.Annotations[fmt.Sprintf("%s.source", waitKey)] = config.DataSource
	stepRun.Annotations[fmt.Sprintf("%s.condition", waitKey)] = config.WaitCondition
	stepRun.Annotations[fmt.Sprintf("%s.timeout", waitKey)] = config.Timeout.String()
	stepRun.Annotations[fmt.Sprintf("%s.poll-interval", waitKey)] = config.PollInterval.String()

	// If monitoring engram is configured, create it
	if config.MonitoringEngram != "" {
		r.createMonitoringEngram(ctx, stepRun, config)
	}

	stepLogger.Info("External data wait set up",
		"source", config.DataSource,
		"condition", config.WaitCondition,
		"timeout", config.Timeout,
		"poll_interval", config.PollInterval)
}

// setupConditionalWait sets up condition monitoring
func (r *StepRunReconciler) setupConditionalWait(ctx context.Context, stepRun *runsv1alpha1.StepRun, pauseConfig interface{}) {
	stepLogger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(stepRun)

	// Parse conditional wait configuration
	config := r.parseConditionalConfig(pauseConfig)
	if config == nil {
		stepLogger.Error(fmt.Errorf("invalid conditional config"), "Failed to parse conditional configuration")
		return
	}

	// Create condition monitoring annotation on the StepRun
	if stepRun.Annotations == nil {
		stepRun.Annotations = make(map[string]string)
	}

	conditionKey := fmt.Sprintf("runs.bubu.sh/condition.%s", stepRun.Name)
	stepRun.Annotations[conditionKey] = "evaluating"
	stepRun.Annotations[fmt.Sprintf("%s.expression", conditionKey)] = config.CELExpression
	stepRun.Annotations[fmt.Sprintf("%s.timeout", conditionKey)] = config.Timeout.String()
	stepRun.Annotations[fmt.Sprintf("%s.interval", conditionKey)] = config.EvalInterval.String()

	// Schedule next condition evaluation
	nextEval := time.Now().Add(config.EvalInterval)
	stepRun.Annotations[fmt.Sprintf("%s.next-eval", conditionKey)] = nextEval.Format(time.RFC3339)

	stepLogger.Info("Conditional wait set up",
		"expression", config.CELExpression,
		"timeout", config.Timeout,
		"interval", config.EvalInterval,
		"next_eval", nextEval)
}

// Configuration structures for pause mechanisms
type ApprovalConfig struct {
	RequiredApprover   string        `json:"requiredApprover"`
	Reason             string        `json:"reason"`
	Timeout            time.Duration `json:"timeout"`
	NotificationEngram string        `json:"notificationEngram,omitempty"`
}

type ExternalDataConfig struct {
	DataSource       string        `json:"dataSource"`
	WaitCondition    string        `json:"waitCondition"`
	Timeout          time.Duration `json:"timeout"`
	PollInterval     time.Duration `json:"pollInterval"`
	MonitoringEngram string        `json:"monitoringEngram,omitempty"`
}

type ConditionalConfig struct {
	CELExpression string        `json:"celExpression"`
	Timeout       time.Duration `json:"timeout"`
	EvalInterval  time.Duration `json:"evalInterval"`
}

// parseApprovalConfig parses approval configuration from interface{}
func (r *StepRunReconciler) parseApprovalConfig(pauseConfig interface{}) *ApprovalConfig {
	// Implementation would parse JSON/map structure into ApprovalConfig
	// For now, return a basic config
	return &ApprovalConfig{
		RequiredApprover: "admin",
		Reason:           "Manual approval required",
		Timeout:          24 * time.Hour,
	}
}

// parseExternalDataConfig parses external data configuration
func (r *StepRunReconciler) parseExternalDataConfig(pauseConfig interface{}) *ExternalDataConfig {
	// Implementation would parse JSON/map structure
	return &ExternalDataConfig{
		DataSource:    "external-api",
		WaitCondition: "data.status == 'ready'",
		Timeout:       30 * time.Minute,
		PollInterval:  1 * time.Minute,
	}
}

// parseConditionalConfig parses conditional configuration
func (r *StepRunReconciler) parseConditionalConfig(pauseConfig interface{}) *ConditionalConfig {
	// Implementation would parse JSON/map structure
	return &ConditionalConfig{
		CELExpression: "true", // placeholder
		Timeout:       10 * time.Minute,
		EvalInterval:  30 * time.Second,
	}
}

// createNotificationEngram creates an Engram for approval notifications
func (r *StepRunReconciler) createNotificationEngram(ctx context.Context, stepRun *runsv1alpha1.StepRun, config *ApprovalConfig) {
	logger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(stepRun)
	logger.Info("Creating notification engram for approval",
		"engram", config.NotificationEngram,
		"approver", config.RequiredApprover)

	// This would create an Engram resource for handling notifications
	// Implementation depends on notification Engram templates
}

// createMonitoringEngram creates an Engram for external data monitoring
func (r *StepRunReconciler) createMonitoringEngram(ctx context.Context, stepRun *runsv1alpha1.StepRun, config *ExternalDataConfig) {
	logger := logging.NewControllerLogger(ctx, "steprun").WithStepRun(stepRun)
	logger.Info("Creating monitoring engram for external data",
		"engram", config.MonitoringEngram,
		"source", config.DataSource)

	// This would create an Eno gram resource for polling external data
	// Implementation depends on monitoring Engram templates
}

// handleCleanup performs cleanup when StepRun is being deleted
func (r *StepRunReconciler) handleCleanup(ctx context.Context, stepRun *runsv1alpha1.StepRun) (ctrl.Result, error) {
	cl := logging.NewCleanupLogger(ctx, "steprun")
	cl.CleanupStart("Cleaning up StepRun resources")

	// Find and cleanup owned Jobs - use owner references for reliable cleanup
	var jobs batchv1.JobList
	if err := r.List(ctx, &jobs, client.InNamespace(stepRun.Namespace)); err != nil {
		cl.CleanupError(err, "Failed to list Jobs for cleanup")
		return ctrl.Result{}, err
	}

	// Filter jobs owned by this StepRun
	var ownedJobs []batchv1.Job
	for _, job := range jobs.Items {
		for _, owner := range job.OwnerReferences {
			if owner.UID == stepRun.UID {
				ownedJobs = append(ownedJobs, job)
				break
			}
		}
	}

	cl.WithValues("job_count", len(ownedJobs)).Info("Found Jobs to cleanup")

	// Delete all Jobs belonging to this StepRun
	for _, job := range ownedJobs {
		if job.DeletionTimestamp == nil {
			// Set propagation policy to delete Pods immediately
			deletePolicy := metav1.DeletePropagationForeground
			if err := r.Delete(ctx, &job, &client.DeleteOptions{
				PropagationPolicy: &deletePolicy,
			}); err != nil {
				cl.CleanupError(err, "Failed to delete Job", "job_name", job.Name)
				return ctrl.Result{}, err
			}
			cl.Info("Deleted Job successfully", "job_name", job.Name)
		}
	}

	// Wait for all Jobs to be fully deleted before removing finalizer
	if len(ownedJobs) > 0 {
		cl.Info("Waiting for Jobs to be deleted", "remaining_jobs", len(ownedJobs))
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}

	// TODO: Cleanup artifacts if artifact storage is implemented
	// This would include cleaning up any S3/MinIO objects referenced in stepRun.Status.Artifacts

	// Remove the finalizer to allow deletion using FinalizerManager
	if r.finalizerManager == nil {
		r.finalizerManager = parentcontroller.NewFinalizerManager(r.Client)
	}
	r.finalizerManager.RemoveFinalizer(stepRun, parentcontroller.StepRunFinalizer)
	if err := r.Update(ctx, stepRun); err != nil {
		cl.CleanupError(err, "Failed to remove finalizer")
		return ctrl.Result{}, err
	}

	cl.CleanupSuccess("Successfully cleaned up StepRun resources", 0)

	// Record cleanup metrics
	metrics.RecordResourceCleanup("steprun", 0, nil)

	return ctrl.Result{}, nil
}

// buildTraceparent constructs a minimal W3C traceparent header from the current span
func buildTraceparent(ctx context.Context) string {
	sc := oteltrace.SpanFromContext(ctx).SpanContext()
	if !sc.IsValid() {
		return ""
	}
	// version(00)-traceid-spanid-flags(01)
	return "00-" + sc.TraceID().String() + "-" + sc.SpanID().String() + "-01"
}
