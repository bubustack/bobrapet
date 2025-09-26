package runs

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/patch"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DAGReconciler is responsible for the core workflow orchestration of a StoryRun.
type DAGReconciler struct {
	client.Client
	CEL            *cel.Evaluator
	StepExecutor   *StepExecutor
	ConfigResolver *config.Resolver
}

// NewDAGReconciler creates a new DAGReconciler.
func NewDAGReconciler(client client.Client, cel *cel.Evaluator, stepExecutor *StepExecutor, configResolver *config.Resolver) *DAGReconciler {
	return &DAGReconciler{
		Client:         client,
		CEL:            cel,
		StepExecutor:   stepExecutor,
		ConfigResolver: configResolver,
	}
}

// Reconcile orchestrates the execution of the StoryRun's DAG.
func (r *DAGReconciler) Reconcile(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag").WithValues("storyrun", srun.Name)

	// 1. Initialize Status
	if srun.Status.StepStates == nil {
		srun.Status.StepStates = make(map[string]runsv1alpha1.StepState)
	}
	if srun.Status.StepOutputs == nil {
		srun.Status.StepOutputs = make(map[string]*runtime.RawExtension)
	}

	// 2. Sync State from Child Resources (StepRuns)
	if err := r.syncStateFromStepRuns(ctx, srun); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Check on Running Synchronous Sub-Stories
	r.checkSyncSubStories(ctx, srun, story)

	// 4. Build State Maps for DAG Traversal
	completedSteps, runningSteps, failedSteps := buildStateMaps(srun.Status.StepStates)

	// 5. Check for StoryRun Completion or Failure
	if len(failedSteps) > 0 && r.shouldFailFast(story) {
		return ctrl.Result{}, r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, "A step failed and fail-fast policy is enabled.")
	}
	if len(completedSteps) == len(story.Spec.Steps) {
		return ctrl.Result{}, r.setStoryRunPhase(ctx, srun, enums.PhaseSucceeded, "All steps completed successfully.")
	}

	// 6. Find and Launch Ready Steps
	readySteps, err := r.findAndLaunchReadySteps(ctx, srun, story, completedSteps, runningSteps)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 7. Persist Status Changes
	err = patch.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StoryRun)
		sr.Status.StepStates = srun.Status.StepStates
		sr.Status.StepOutputs = srun.Status.StepOutputs
	})
	if err != nil {
		log.Error(err, "Failed to patch StoryRun status")
		return ctrl.Result{}, err
	}

	if len(readySteps) > 0 {
		return ctrl.Result{Requeue: true}, nil // Requeue immediately to check on newly launched steps
	}

	return ctrl.Result{}, nil
}

func (r *DAGReconciler) syncStateFromStepRuns(ctx context.Context, srun *runsv1alpha1.StoryRun) error {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-sync")
	var stepRunList runsv1alpha1.StepRunList
	if err := r.List(ctx, &stepRunList, client.InNamespace(srun.Namespace), client.MatchingLabels{"bubu.sh/storyrun": srun.Name}); err != nil {
		log.Error(err, "Failed to list StepRuns")
		return err
	}

	for _, sr := range stepRunList.Items {
		stepID := sr.Spec.StepID
		currentState := srun.Status.StepStates[stepID]

		if currentState.Phase != sr.Status.Phase {
			log.Info("Syncing StepRun status to StoryRun", "step", stepID, "oldPhase", currentState.Phase, "newPhase", sr.Status.Phase)
			srun.Status.StepStates[stepID] = runsv1alpha1.StepState{
				Phase:   sr.Status.Phase,
				Message: sr.Status.LastFailureMsg,
			}
			if sr.Status.Phase == enums.PhaseSucceeded && sr.Status.Output != nil {
				srun.Status.StepOutputs[stepID] = sr.Status.Output
			}
		}
	}
	return nil
}

func (r *DAGReconciler) checkSyncSubStories(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-substory")
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		state, exists := srun.Status.StepStates[step.Name]
		if !exists || state.Phase != enums.PhaseRunning {
			continue
		}

		if step.Type == enums.StepTypeExecuteStory && step.With != nil {
			var withConfig struct {
				WaitForCompletion bool `json:"waitForCompletion"`
			}
			if err := json.Unmarshal(step.With.Raw, &withConfig); err != nil {
				log.Error(err, "Failed to parse 'with' for running executeStory, marking as failed", "step", step.Name)
				srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: "invalid 'with' block"}
				continue
			}

			if !withConfig.WaitForCompletion {
				continue
			}

			subRun := &runsv1alpha1.StoryRun{}
			subRunKey := types.NamespacedName{Name: state.SubStoryRunName, Namespace: srun.Namespace}
			err := r.Get(ctx, subRunKey, subRun)
			if err != nil {
				log.Error(err, "Failed to get sub-StoryRun for sync step, marking as failed", "step", step.Name)
				srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: "Failed to get sub-StoryRun"}
				continue
			}

			if subRun.Status.Phase.IsTerminal() {
				log.Info("Sub-StoryRun completed", "step", step.Name, "subPhase", subRun.Status.Phase)
				srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: subRun.Status.Phase, Message: subRun.Status.Message}
				if subRun.Status.Phase == enums.PhaseSucceeded && subRun.Status.Output != nil {
					srun.Status.StepOutputs[step.Name] = subRun.Status.Output
				}
			}
		}
	}
}

func (r *DAGReconciler) findAndLaunchReadySteps(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, completedSteps, runningSteps map[string]bool) ([]*bubuv1alpha1.Step, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-launcher")
	dependencies, _ := buildDependencyGraphs(story.Spec.Steps)
	storyRunInputs, _ := getStoryRunInputs(srun)

	priorStepOutputs := make(map[string]interface{})
	for stepName, rawOutput := range srun.Status.StepOutputs {
		if rawOutput == nil {
			continue
		}
		var outputData interface{}
		if err := json.Unmarshal(rawOutput.Raw, &outputData); err != nil {
			log.Error(err, "Failed to unmarshal step output for CEL, will be unavailable", "step", stepName)
			continue
		}
		priorStepOutputs[stepName] = outputData
	}

	readySteps, err := r.findReadySteps(story.Spec.Steps, completedSteps, runningSteps, dependencies, storyRunInputs, priorStepOutputs)
	if err != nil {
		log.Error(err, "Failed to find ready steps")
		return nil, err
	}

	for _, step := range readySteps {
		if err := r.StepExecutor.Execute(ctx, srun, story, step); err != nil {
			log.Error(err, "Failed to execute step", "step", step.Name)
			srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: err.Error()}
			// Propagate the error up to the main reconcile loop to trigger backoff
			return nil, fmt.Errorf("failed to execute step %s: %w", step.Name, err)
		}
		srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseRunning}
	}

	return readySteps, nil
}

func (r *DAGReconciler) findReadySteps(steps []bubuv1alpha1.Step, completed, running map[string]bool, dependencies map[string]map[string]bool, storyRunInputs, priorStepOutputs map[string]interface{}) ([]*bubuv1alpha1.Step, error) {
	var ready []*bubuv1alpha1.Step
	vars := map[string]interface{}{
		"inputs": storyRunInputs,
		"steps":  priorStepOutputs,
	}

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
			if step.If != nil && *step.If != "" {
				result, err := r.CEL.EvaluateWhenCondition(*step.If, vars)
				if err != nil {
					// This indicates a potentially recoverable error (e.g., variable not yet available).
					// We log it but don't fail the entire reconcile.
					log := logging.NewReconcileLogger(context.Background(), "storyrun-if-eval")
					log.Info("CEL 'if' condition blocked or failed evaluation, step not ready", "step", step.Name, "reason", err)
					continue
				}
				if !result {
					continue
				}
			}
			ready = append(ready, step)
		}
	}
	return ready, nil
}

// buildDependencyGraphs and its helpers remain as pure functions.
func buildDependencyGraphs(steps []bubuv1alpha1.Step) (map[string]map[string]bool, map[string]map[string]bool) {
	dependencies := make(map[string]map[string]bool)
	dependents := make(map[string]map[string]bool)
	stepNameRegex := regexp.MustCompile(`steps\.([a-zA-Z0-9_\-]+)\.|steps\s*\[\s*['"]([a-zA-Z0-9_\-]+)['"]\s*\]`)

	// Build a map of normalized aliases (underscores) back to real step names
	aliasToReal := make(map[string]string)
	for _, s := range steps {
		alias := normalizeStepIdentifier(s.Name)
		if alias != s.Name {
			aliasToReal[alias] = s.Name
		}
	}

	for i := range steps {
		step := &steps[i]
		if dependencies[step.Name] == nil {
			dependencies[step.Name] = make(map[string]bool)
		}
		if dependents[step.Name] == nil {
			dependents[step.Name] = make(map[string]bool)
		}

		// 1. Explicit dependencies from 'needs'
		for _, depName := range step.Needs {
			addDependency(step.Name, depName, dependencies, dependents)
		}

		// 2. Implicit dependencies from 'if' condition
		if step.If != nil {
			findAndAddDeps(*step.If, step.Name, stepNameRegex, aliasToReal, dependencies, dependents)
		}

		// 3. Implicit dependencies from 'with' block for engram and executeStory steps
		var withBlock *runtime.RawExtension
		if step.Ref != nil && step.With != nil { // Engram step
			withBlock = step.With
		} else if step.Type == enums.StepTypeExecuteStory && step.With != nil {
			withBlock = step.With
		}

		if withBlock != nil {
			findAndAddDeps(string(withBlock.Raw), step.Name, stepNameRegex, aliasToReal, dependencies, dependents)
		}
	}
	return dependencies, dependents
}

func findAndAddDeps(expression, stepName string, regex *regexp.Regexp, aliasMap map[string]string, dependencies, dependents map[string]map[string]bool) {
	matches := regex.FindAllStringSubmatch(expression, -1)
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
		if real, ok := aliasMap[depName]; ok {
			depName = real
		}
		addDependency(stepName, depName, dependencies, dependents)
	}
}

func addDependency(stepName, depName string, dependencies, dependents map[string]map[string]bool) {
	dependencies[stepName][depName] = true
	if dependents[depName] == nil {
		dependents[depName] = make(map[string]bool)
	}
	dependents[depName][stepName] = true
}

// buildStateMaps, getStoryRunInputs, shouldFailFast, setStoryRunPhase are also pure helpers.
func buildStateMaps(states map[string]runsv1alpha1.StepState) (completed, running, failed map[string]bool) {
	completed = make(map[string]bool)
	running = make(map[string]bool)
	failed = make(map[string]bool)

	for name, state := range states {
		if state.Phase == enums.PhaseSucceeded {
			completed[name] = true
		} else if state.Phase == enums.PhaseRunning || state.Phase == enums.PhasePending {
			running[name] = true
		} else if state.Phase.IsTerminal() { // Failed, Canceled, etc.
			failed[name] = true
		}
	}
	return
}

func getStoryRunInputs(srun *runsv1alpha1.StoryRun) (map[string]interface{}, error) {
	if srun.Spec.Inputs == nil {
		return make(map[string]interface{}), nil
	}
	var inputs map[string]interface{}
	if err := json.Unmarshal(srun.Spec.Inputs.Raw, &inputs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal storyrun inputs: %w", err)
	}
	return inputs, nil
}

func (r *DAGReconciler) shouldFailFast(story *bubuv1alpha1.Story) bool {
	if story.Spec.Policy != nil &&
		story.Spec.Policy.Retries != nil &&
		story.Spec.Policy.Retries.ContinueOnStepFailure != nil {
		return !*story.Spec.Policy.Retries.ContinueOnStepFailure
	}
	return true // Default to failing fast
}

func (r *DAGReconciler) setStoryRunPhase(ctx context.Context, srun *runsv1alpha1.StoryRun, phase enums.Phase, message string) error {
	return patch.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
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

// normalizeStepIdentifier replaces characters not allowed in CEL identifiers with underscores.
func normalizeStepIdentifier(name string) string {
	return regexp.MustCompile(`[^a-zA-Z0-9_]`).ReplaceAllString(name, "_")
}
