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
	"regexp"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	"github.com/bubustack/bobrapet/pkg/patch"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"

	refs "github.com/bubustack/bobrapet/pkg/refs"
)

const (
	// StoryRunFinalizer is the finalizer for StoryRun resources
	StoryRunFinalizer = "storyrun.bubu.sh/finalizer"
)

// StoryRunReconciler reconciles a StoryRun object
type StoryRunReconciler struct {
	config.ControllerDependencies
}

//+kubebuilder:rbac:groups=runs.bubu.sh,resources=storyruns,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=runs.bubu.sh,resources=storyruns/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=runs.bubu.sh,resources=storyruns/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=create;get;watch;list
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=create;get;watch;list;bind;escalate
//+kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=create;get;watch;list
//+kubebuilder:rbac:groups=runs.bubu.sh,resources=stepruns,verbs=get;list;watch;create;update;patch
//+kubebuilder:rbac:groups=bubu.sh,resources=stories,verbs=get;list;watch
//+kubebuilder:rbac:groups=bubu.sh,resources=engrams,verbs=get;list;watch

func (r *StoryRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun").WithValues("storyrun", req.NamespacedName)

	var srun runsv1alpha1.StoryRun
	if err := r.ControllerDependencies.Client.Get(ctx, req.NamespacedName, &srun); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion first
	if !srun.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &srun)
	}

	// Add finalizer if it doesn't exist
	if !controllerutil.ContainsFinalizer(&srun, StoryRunFinalizer) {
		controllerutil.AddFinalizer(&srun, StoryRunFinalizer)
		if err := r.Update(ctx, &srun); err != nil {
			log.Error(err, "Failed to add finalizer")
			return ctrl.Result{}, err
		}
	}

	if err := r.ensureEngramRBAC(ctx, &srun); err != nil {
		log.Error(err, "Failed to ensure Engram RBAC")
		return ctrl.Result{}, err
	}

	if srun.Status.Phase == enums.PhaseSucceeded || srun.Status.Phase == enums.PhaseFailed {
		return ctrl.Result{}, nil
	}

	story, err := r.getStoryForRun(ctx, &srun)
	if err != nil {
		err := r.setStoryRunPhase(ctx, &srun, enums.PhaseFailed, fmt.Sprintf("failed to get story: %v", err))
		return ctrl.Result{}, err
	}

	if srun.Status.Phase == "" {
		err := r.setStoryRunPhase(ctx, &srun, enums.PhaseRunning, "Starting StoryRun execution")
		if err != nil {
			log.Error(err, "Failed to update StoryRun status to Running")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// List all StepRuns for this StoryRun to determine the current state of the DAG
	var stepRunList runsv1alpha1.StepRunList
	if err := r.ControllerDependencies.Client.List(ctx, &stepRunList, client.InNamespace(srun.Namespace), client.MatchingLabels{"bubu.sh/storyrun": srun.Name}); err != nil {
		log.Error(err, "Failed to list StepRuns")
		return ctrl.Result{}, err
	}

	// Build the current state from the list of StepRuns
	completedSteps := make(map[string]bool)
	runningSteps := make(map[string]bool)
	failedSteps := make(map[string]bool)
	needsStatusUpdate := false

	if srun.Status.StepOutputs == nil {
		srun.Status.StepOutputs = make(map[string]*runtime.RawExtension)
	}

	for _, sr := range stepRunList.Items {
		stepID := sr.Spec.StepID
		if sr.Status.Phase == enums.PhaseSucceeded {
			completedSteps[stepID] = true
			if sr.Status.Output != nil {
				// Check if this is a new, un-persisted output
				if _, exists := srun.Status.StepOutputs[stepID]; !exists {
					srun.Status.StepOutputs[stepID] = sr.Status.Output
					log.Info("Found new output from successful StepRun", "stepID", stepID)
					needsStatusUpdate = true
				}
			}
		} else if sr.Status.Phase == enums.PhaseFailed || sr.Status.Phase == enums.PhaseCanceled {
			failedSteps[stepID] = true
		} else if sr.Status.Phase == enums.PhaseRunning || sr.Status.Phase == enums.PhasePending {
			runningSteps[stepID] = true
		}
	}

	if needsStatusUpdate {
		err := patch.RetryableStatusPatch(ctx, r.ControllerDependencies.Client, &srun, func(obj client.Object) {
			sr := obj.(*runsv1alpha1.StoryRun)
			if sr.Status.StepOutputs == nil {
				sr.Status.StepOutputs = make(map[string]*runtime.RawExtension)
			}
			// This re-iterates, but it's inside the patch function, which is correct.
			for _, stepRun := range stepRunList.Items {
				if stepRun.Status.Phase == enums.PhaseSucceeded && stepRun.Status.Output != nil {
					stepID := stepRun.Spec.StepID
					if _, exists := sr.Status.StepOutputs[stepID]; !exists {
						sr.Status.StepOutputs[stepID] = stepRun.Status.Output
					}
				}
			}
		})

		if err != nil {
			log.Error(err, "Failed to update StoryRun status with new outputs")
			return ctrl.Result{}, err
		}
		log.Info("Successfully updated StoryRun status with new outputs, requeueing for DAG re-evaluation.")
		return ctrl.Result{Requeue: true}, nil
	}

	// Check failure policy
	if len(failedSteps) > 0 && r.shouldFailFast(story) {
		log.Info("A step failed and fail-fast policy is enabled. Failing StoryRun.")
		err := r.setStoryRunPhase(ctx, &srun, enums.PhaseFailed, fmt.Sprintf("step %s failed", getFirstKey(failedSteps)))
		return ctrl.Result{}, err
	}

	// Build dependency graphs
	dependencies, dependents := r.buildDependencyGraphs(story.Spec.Steps)

	// Find steps that are ready to run
	readySteps := r.findReadySteps(story.Spec.Steps, completedSteps, runningSteps, dependencies)

	// Launch ready steps
	for _, step := range readySteps {
		if err := r.executeEngramStep(ctx, &srun, story, step, dependents); err != nil {
			log.Error(err, "Failed to execute step", "step", step.Name)
			return ctrl.Result{}, err
		}
	}

	// Check for completion
	if len(completedSteps) == len(story.Spec.Steps) && len(failedSteps) == 0 {
		log.Info("All steps completed successfully.")
		err := r.setStoryRunPhase(ctx, &srun, enums.PhaseSucceeded, "All steps completed successfully")
		return ctrl.Result{}, err
	}

	// If no steps are ready and none are running, but not all are complete, we are stuck.
	if len(readySteps) == 0 && len(runningSteps) == 0 && len(completedSteps) < len(story.Spec.Steps) {
		log.Info("Workflow is stuck. No steps are running or ready to run.")
		err := r.setStoryRunPhase(ctx, &srun, enums.PhaseFailed, "Workflow stuck: circular dependency or unresolved step failure.")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *StoryRunReconciler) reconcileDelete(ctx context.Context, srun *runsv1alpha1.StoryRun) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun").WithValues("storyrun", srun.Name)
	log.Info("Reconciling deletion for StoryRun")

	// List all StepRuns for this StoryRun
	var stepRunList runsv1alpha1.StepRunList
	if err := r.ControllerDependencies.Client.List(ctx, &stepRunList, client.InNamespace(srun.Namespace), client.MatchingLabels{"bubu.sh/storyrun": srun.Name}); err != nil {
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
			controllerutil.RemoveFinalizer(srun, StoryRunFinalizer)
			if err := r.Update(ctx, srun); err != nil {
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

func (r *StoryRunReconciler) shouldFailFast(story *bubushv1alpha1.Story) bool {
	if story.Spec.Policy != nil &&
		story.Spec.Policy.Retries != nil &&
		story.Spec.Policy.Retries.ContinueOnStepFailure != nil {
		return !*story.Spec.Policy.Retries.ContinueOnStepFailure
	}
	return true // Default to failing fast
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
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, "Running", message)
		case enums.PhaseSucceeded:
			if sr.Status.FinishedAt == nil {
				now := metav1.Now()
				sr.Status.FinishedAt = &now
				if sr.Status.StartedAt != nil {
					sr.Status.Duration = now.Sub(sr.Status.StartedAt.Time).String()
				}
			}
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, "Succeeded", message)
		case enums.PhaseFailed:
			if sr.Status.FinishedAt == nil {
				now := metav1.Now()
				sr.Status.FinishedAt = &now
				if sr.Status.StartedAt != nil {
					sr.Status.Duration = now.Sub(sr.Status.StartedAt.Time).String()
				}
			}
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, "Failed", message)
		}
	})
}

func (r *StoryRunReconciler) buildDependencyGraphs(steps []bubushv1alpha1.Step) (map[string]map[string]bool, map[string]map[string]bool) {
	dependencies := make(map[string]map[string]bool)
	dependents := make(map[string]map[string]bool)
	// Support both dot and bracket notation; allow hyphens or underscores in IDs
	stepNameRegex := regexp.MustCompile(`steps\.([a-zA-Z0-9_\-]+)\.|steps\s*\[\s*['\"]([a-zA-Z0-9_\-]+)['\"]\s*\]`)

	// Build a map of normalized aliases (underscores) back to real step names
	aliasToReal := make(map[string]string)
	for _, s := range steps {
		alias := normalizeStepIdentifier(s.Name)
		if alias != s.Name {
			aliasToReal[alias] = s.Name
		}
	}

	for _, step := range steps {
		if dependencies[step.Name] == nil {
			dependencies[step.Name] = make(map[string]bool)
		}
		if dependents[step.Name] == nil {
			dependents[step.Name] = make(map[string]bool)
		}

		if step.If != nil {
			matches := stepNameRegex.FindAllStringSubmatch(*step.If, -1)
			for _, match := range matches {
				var depName string
				if len(match) > 2 && match[2] != "" {
					depName = match[2]
				} else if len(match) > 1 {
					depName = match[1]
				}
				if depName == "" {
					continue
				}
				// Map normalized alias back to real step name if needed
				if real, ok := aliasToReal[depName]; ok {
					depName = real
				}
				dependencies[step.Name][depName] = true
				if dependents[depName] == nil {
					dependents[depName] = make(map[string]bool)
				}
				dependents[depName][step.Name] = true
			}
		}
	}
	return dependencies, dependents
}

func (r *StoryRunReconciler) findReadySteps(steps []bubushv1alpha1.Step, completed, running map[string]bool, dependencies map[string]map[string]bool) []*bubushv1alpha1.Step {
	var ready []*bubushv1alpha1.Step
	for i := range steps {
		step := &steps[i]
		if completed[step.Name] || running[step.Name] {
			continue
		}

		deps := dependencies[step.Name]
		allDepsMet := true
		for dep := range deps {
			if !completed[dep] {
				allDepsMet = false
				break
			}
		}

		if allDepsMet {
			ready = append(ready, step)
		}
	}
	return ready
}

func (r *StoryRunReconciler) executeEngramStep(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubushv1alpha1.Story, step *bubushv1alpha1.Step, dependents map[string]map[string]bool) error {
	stepName := fmt.Sprintf("%s-%s", srun.Name, step.Name)
	var stepRun runsv1alpha1.StepRun
	err := r.ControllerDependencies.Client.Get(ctx, types.NamespacedName{Name: stepName, Namespace: srun.Namespace}, &stepRun)

	if err != nil && errors.IsNotFound(err) {
		downstreamTargets := r.findDownstreamTargets(ctx, srun, story, step.Name, dependents)

		// Get the engram to merge the 'with' blocks
		var engram bubushv1alpha1.Engram
		if step.Ref != nil {
			if err := r.ControllerDependencies.Client.Get(ctx, types.NamespacedName{Name: step.Ref.Name, Namespace: srun.Namespace}, &engram); err != nil {
				return fmt.Errorf("failed to get engram '%s' for step '%s': %w", step.Ref.Name, step.Name, err)
			}
		}

		mergedWith, err := r.mergeWithBlocks(engram.Spec.With, step.With)
		if err != nil {
			return fmt.Errorf("failed to merge 'with' blocks for step '%s': %w", step.Name, err)
		}

		stepRun = runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      stepName,
				Namespace: srun.Namespace,
				Labels: map[string]string{
					"bubu.sh/storyrun":   srun.Name,
					"bubu.sh/story-name": story.Name,
				},
			},
			Spec: runsv1alpha1.StepRunSpec{
				StoryRunRef: refs.StoryRunReference{
					ObjectReference: refs.ObjectReference{Name: srun.Name},
				},
				StepID:            step.Name,
				EngramRef:         step.Ref,
				Input:             mergedWith,
				DownstreamTargets: downstreamTargets,
			},
		}
		if err := controllerutil.SetControllerReference(srun, &stepRun, r.ControllerDependencies.Scheme); err != nil {
			return err
		}
		return r.ControllerDependencies.Client.Create(ctx, &stepRun)
	}
	return err // Return other errors or nil if found
}

// mergeWithBlocks combines the 'with' blocks from an Engram and a Step.
// The Step's values take precedence.
func (r *StoryRunReconciler) mergeWithBlocks(engramWith, stepWith *runtime.RawExtension) (*runtime.RawExtension, error) {
	if engramWith == nil {
		return stepWith, nil
	}
	if stepWith == nil {
		return engramWith, nil
	}

	var engramMap, stepMap map[string]interface{}
	if err := json.Unmarshal(engramWith.Raw, &engramMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal engram 'with' block: %w", err)
	}
	if err := json.Unmarshal(stepWith.Raw, &stepMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal step 'with' block: %w", err)
	}

	// Start with the engram's values and overwrite with the step's values.
	for k, v := range stepMap {
		engramMap[k] = v
	}

	mergedBytes, err := json.Marshal(engramMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal merged 'with' block: %w", err)
	}

	return &runtime.RawExtension{Raw: mergedBytes}, nil
}

func (r *StoryRunReconciler) findDownstreamTargets(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubushv1alpha1.Story, stepName string, dependents map[string]map[string]bool) []runsv1alpha1.DownstreamTarget {
	var targets []runsv1alpha1.DownstreamTarget
	nextSteps := dependents[stepName]

	if len(nextSteps) == 0 {
		return []runsv1alpha1.DownstreamTarget{{
			Terminate: &runsv1alpha1.TerminateTarget{StopMode: enums.StopModeSuccess},
		}}
	}

	for nextStepName := range nextSteps {
		var stepSpec *bubushv1alpha1.Step
		for i := range story.Spec.Steps {
			if story.Spec.Steps[i].Name == nextStepName {
				stepSpec = &story.Spec.Steps[i]
				break
			}
		}

		if stepSpec == nil || stepSpec.Ref == nil {
			continue
		}

		var nextEngram bubushv1alpha1.Engram
		engramKey := types.NamespacedName{Namespace: srun.Namespace, Name: stepSpec.Ref.Name}
		if err := r.ControllerDependencies.Client.Get(ctx, engramKey, &nextEngram); err != nil {
			continue
		}

		if nextEngram.Spec.Mode != "" &&
			(nextEngram.Spec.Mode == enums.WorkloadModeDeployment || nextEngram.Spec.Mode == enums.WorkloadModeStatefulSet) {
			endpoint := fmt.Sprintf("engram-%s.%s.svc:9000", stepSpec.Ref.Name, srun.Namespace)
			targets = append(targets, runsv1alpha1.DownstreamTarget{
				GRPCTarget: &runsv1alpha1.GRPCTarget{Endpoint: endpoint},
			})
		}
	}

	if len(targets) == 0 {
		return []runsv1alpha1.DownstreamTarget{{
			Terminate: &runsv1alpha1.TerminateTarget{StopMode: enums.StopModeSuccess},
		}}
	}
	return targets
}

func getFirstKey(m map[string]bool) string {
	for k := range m {
		return k
	}
	return ""
}

// StepState holds the status of a single step.
type StepState struct {
	Phase      enums.Phase
	IsTerminal bool
	Output     *runtime.RawExtension
}

func (r *StoryRunReconciler) ensureEngramRBAC(ctx context.Context, storyRun *runsv1alpha1.StoryRun) error {
	log := logging.NewReconcileLogger(ctx, "storyrun")

	story, err := r.getStoryForRun(ctx, storyRun)
	if err != nil {
		// If the story isn't found, we can't determine the storage policy, but we can still proceed
		// with the basic RBAC setup. The error will be handled in the main reconcile loop.
		log.Error(err, "Could not get parent story for RBAC setup, proceeding without storage policy")
	}

	saName := fmt.Sprintf("%s-engram-runner", storyRun.Name)
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: storyRun.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.ControllerDependencies.Client, sa, func() error {
		if sa.Annotations == nil {
			sa.Annotations = make(map[string]string)
		}

		if story != nil && story.Spec.Policy != nil && story.Spec.Policy.Storage != nil && story.Spec.Policy.Storage.S3 != nil &&
			story.Spec.Policy.Storage.S3.Authentication.ServiceAccountAnnotations != nil {
			log.Info("Applying S3 ServiceAccount annotations from StoragePolicy")
			for k, v := range story.Spec.Policy.Storage.S3.Authentication.ServiceAccountAnnotations {
				sa.Annotations[k] = v
			}
		}

		return controllerutil.SetOwnerReference(storyRun, sa, r.ControllerDependencies.Scheme)
	})

	if err != nil {
		return fmt.Errorf("failed to create or update ServiceAccount: %w", err)
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: storyRun.Namespace,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"runs.bubu.sh"},
				Resources: []string{"stepruns"},
				Verbs:     []string{"get", "watch"},
			},
			{
				APIGroups: []string{"runs.bubu.sh"},
				Resources: []string{"stepruns/status"},
				Verbs:     []string{"patch", "update"},
			},
		},
	}
	if err := controllerutil.SetOwnerReference(storyRun, role, r.ControllerDependencies.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on Role: %w", err)
	}
	if err := r.ControllerDependencies.Client.Create(ctx, role); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create Role: %w", err)
	} else if err == nil {
		log.Info("Created Role for Engram runner", "role", role.Name)
	}

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: storyRun.Namespace,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      saName,
				Namespace: storyRun.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     saName,
		},
	}
	if err := controllerutil.SetOwnerReference(storyRun, rb, r.ControllerDependencies.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on RoleBinding: %w", err)
	}
	if err := r.ControllerDependencies.Client.Create(ctx, rb); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create RoleBinding: %w", err)
	} else if err == nil {
		log.Info("Created RoleBinding for Engram runner", "roleBinding", rb.Name)
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StoryRunReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&runsv1alpha1.StoryRun{}).
		WithOptions(opts).
		Owns(&runsv1alpha1.StepRun{}).
		Complete(r)
}
