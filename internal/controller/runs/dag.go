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
func NewDAGReconciler(k8sClient client.Client, celEval *cel.Evaluator, stepExecutor *StepExecutor, configResolver *config.Resolver) *DAGReconciler {
	return &DAGReconciler{
		Client:         k8sClient,
		CEL:            celEval,
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

	// 2. Sync State from Child Resources (StepRuns)
	stepRunList, err := r.syncStateFromStepRuns(ctx, srun)
	if err != nil {
		return ctrl.Result{}, err
	}
	log.Info("Synced StepRuns", "count", len(stepRunList.Items))

	// Loop to traverse the DAG as far as possible in one reconciliation cycle.
	// The loop is bounded by the number of steps to prevent infinite cycles in case of bugs.
	priorStepOutputs, err := getPriorStepOutputs(ctx, r.Client, srun, stepRunList)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get prior step outputs: %w", err)
	}
	for i := 0; i < len(story.Spec.Steps)+1; i++ { // +1 to allow one final check after all steps are processed
		// Sync state from synchronous sub-stories
		if updated := r.checkSyncSubStories(ctx, srun, story); updated {
			// If a sub-story status was synced, we must re-fetch the outputs
			// Inefficient to re-list all step runs, but acceptable for now as sub-stories are less common.
			// A future improvement would be to only fetch the single sub-story's output and merge it in.
			stepRunList, err = r.syncStateFromStepRuns(ctx, srun)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to re-sync step runs after sub-story sync: %w", err)
			}
			priorStepOutputs, err = getPriorStepOutputs(ctx, r.Client, srun, stepRunList)
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("failed to re-fetch prior step outputs after sub-story sync: %w", err)
			}
		}

		// Build state maps for DAG traversal
		completedSteps, runningSteps, failedSteps := buildStateMaps(srun.Status.StepStates)

		// 5. Check for StoryRun Completion or Failure
		if len(failedSteps) > 0 && r.shouldFailFast(story) {
			return ctrl.Result{}, r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, "A step failed and fail-fast policy is enabled.")
		}
		if len(completedSteps) == len(story.Spec.Steps) {
			if err := r.finalizeSuccessfulRun(ctx, srun, story); err != nil {
				log.Error(err, "Failed to finalize successful story run")
				// Try to set phase to failed, but the finalization error is the root cause.
				_ = r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, "Failed to evaluate final output template.")
				return ctrl.Result{}, err // Return the evaluation error
			}
			return ctrl.Result{}, nil // Finalization handles setting the Succeeded phase
		}

		// 6. Persist current status BEFORE launching new steps so downstream resolvers can see prior outputs.
		// This prevents a race where a newly created StepRun's reconciler fetches the
		// parent StoryRun before its status is updated with the latest completed steps.
		if err := patch.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
			sr := obj.(*runsv1alpha1.StoryRun)
			sr.Status.StepStates = srun.Status.StepStates
		}); err != nil {
			log.Error(err, "Failed to patch StoryRun status before launching steps")
			return ctrl.Result{}, err
		}

		// 7. Find and Launch Ready Steps
		readySteps, skippedSteps, err := r.findAndLaunchReadySteps(ctx, srun, story, completedSteps, runningSteps, priorStepOutputs)
		if err != nil {
			return ctrl.Result{}, err
		}

		// 8. Persist Status Changes (including skipped steps)
		err = patch.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
			sr := obj.(*runsv1alpha1.StoryRun)
			// Merge the maps to ensure we don't lose state
			for k, v := range srun.Status.StepStates {
				sr.Status.StepStates[k] = v
			}
		})
		if err != nil {
			log.Error(err, "Failed to patch StoryRun status")
			return ctrl.Result{}, err
		}

		if len(readySteps) == 0 && len(skippedSteps) == 0 {
			break // DAG is stable, no more progress can be made in this cycle.
		}

		// After launching, immediately sync state again and refresh prior outputs
		// to allow the next level of the DAG to be processed in this cycle.
		stepRunList, err = r.syncStateFromStepRuns(ctx, srun)
		if err != nil {
			return ctrl.Result{}, err
		}
		priorStepOutputs, err = getPriorStepOutputs(ctx, r.Client, srun, stepRunList)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *DAGReconciler) syncStateFromStepRuns(ctx context.Context, srun *runsv1alpha1.StoryRun) (*runsv1alpha1.StepRunList, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-sync")
	var stepRunList runsv1alpha1.StepRunList
	if err := r.List(ctx, &stepRunList, client.InNamespace(srun.Namespace), client.MatchingLabels{"bubustack.io/storyrun": srun.Name}); err != nil {
		log.Error(err, "Failed to list StepRuns")
		return nil, err
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
			// DEPRECATED: Do not propagate StepRun outputs into the parent StoryRun.
			// This was causing a storage hazard where the StoryRun could exceed etcd's
			// size limit. Step outputs are now resolved on-demand from StepRun objects.
		}
	}
	return &stepRunList, nil
}

func (r *DAGReconciler) checkSyncSubStories(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) bool {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-substory")
	var statusUpdated bool
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
				statusUpdated = true
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
				statusUpdated = true
				continue
			}

			if subRun.Status.Phase.IsTerminal() {
				log.Info("Sub-StoryRun completed", "step", step.Name, "subPhase", subRun.Status.Phase)
				newState := runsv1alpha1.StepState{Phase: subRun.Status.Phase, Message: subRun.Status.Message}
				if srun.Status.StepStates[step.Name] != newState {
					srun.Status.StepStates[step.Name] = newState
					statusUpdated = true
				}
				// Manually copy sub-story output into parent's step state.
				// This is still risky if the sub-story output is large, but acceptable for now
				// as it's less common than step outputs. A future enhancement should offload this.
				// Note: sub-story outputs are not propagated to StoryRun to avoid large status payloads.
				// They are resolved on-demand by downstream evaluators using the sub-StoryRun object.
			}
		}
	}
	return statusUpdated
}

func (r *DAGReconciler) findAndLaunchReadySteps(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, completedSteps, runningSteps map[string]bool, priorStepOutputs map[string]any) ([]*bubuv1alpha1.Step, []*bubuv1alpha1.Step, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-launcher")
	dependencies, _ := buildDependencyGraphs(story.Spec.Steps)
	storyRunInputs, _ := getStoryRunInputs(srun)

	vars := map[string]any{
		"inputs": storyRunInputs,
		"steps":  priorStepOutputs,
	}

	readySteps, skippedSteps := r.findReadySteps(ctx, story.Spec.Steps, completedSteps, runningSteps, dependencies, vars)

	// Handle skipped steps by updating their status.
	for _, step := range skippedSteps {
		if _, exists := srun.Status.StepStates[step.Name]; !exists || srun.Status.StepStates[step.Name].Phase != enums.PhaseSkipped {
			log.Info("Marking step as Skipped", "step", step.Name)
			srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseSkipped, Message: "Skipped due to 'if' condition"}
		}
	}

	for _, step := range readySteps {
		if err := r.StepExecutor.Execute(ctx, srun, story, step, vars); err != nil {
			log.Error(err, "Failed to execute step", "step", step.Name)
			srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: err.Error()}
			// Propagate the error up to the main reconcile loop to trigger backoff
			return nil, nil, fmt.Errorf("failed to execute step %s: %w", step.Name, err)
		}
		srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseRunning}
	}

	return readySteps, skippedSteps, nil
}

// getPriorStepOutputs fetches the outputs of all previously completed steps in the same StoryRun.
// It serves as the single source of truth for step outputs within the DAG reconciler.
func getPriorStepOutputs(ctx context.Context, c client.Client, srun *runsv1alpha1.StoryRun, stepRunList *runsv1alpha1.StepRunList) (map[string]any, error) {
	log := logging.NewReconcileLogger(ctx, "dag-output-resolver")
	outputs := make(map[string]any)

	// The StoryRun's status.stepOutputs field is deprecated and no longer used.
	// Instead, we always resolve outputs by listing the child StepRun objects,
	// which is the single source of truth.
	if stepRunList == nil {
		var newList runsv1alpha1.StepRunList
		if err := c.List(ctx, &newList, client.InNamespace(srun.Namespace),
			client.MatchingLabels{"bubustack.io/storyrun": srun.Name}); err != nil {
			log.Error(err, "Failed to list StepRuns for output resolution")
			return nil, err
		}
		stepRunList = &newList
	}

	for i := range stepRunList.Items {
		sr := &stepRunList.Items[i]
		if _, exists := outputs[sr.Spec.StepID]; exists {
			continue // An output for this step has already been resolved.
		}

		if sr.Status.Phase == enums.PhaseSucceeded && sr.Status.Output != nil {
			var outputData map[string]any
			if err := json.Unmarshal(sr.Status.Output.Raw, &outputData); err != nil {
				log.Error(err, "Failed to unmarshal output from prior StepRun during fallback", "step", sr.Spec.StepID)
				continue
			}
			outputs[sr.Spec.StepID] = map[string]any{"outputs": outputData}
		}
	}

	// Also resolve outputs from completed synchronous sub-stories.
	for stepID, state := range srun.Status.StepStates {
		if state.Phase == enums.PhaseSucceeded && state.SubStoryRunName != "" {
			if _, exists := outputs[stepID]; exists {
				continue // Already populated, maybe from a StepRun (shouldn't happen for substories)
			}

			subRun := &runsv1alpha1.StoryRun{}
			subRunKey := types.NamespacedName{Name: state.SubStoryRunName, Namespace: srun.Namespace}
			if err := c.Get(ctx, subRunKey, subRun); err != nil {
				log.Error(err, "Failed to get sub-StoryRun for output resolution", "subStoryRun", state.SubStoryRunName)
				continue // Don't fail the whole reconcile, just skip this output
			}

			if subRun.Status.Output != nil {
				var outputData map[string]any
				if err := json.Unmarshal(subRun.Status.Output.Raw, &outputData); err != nil {
					log.Error(err, "Failed to unmarshal output from sub-StoryRun", "step", stepID)
					continue
				}
				outputs[stepID] = map[string]any{"outputs": outputData}
			}
		}
	}

	// Add normalized aliases for CEL expressions.
	for stepID, stepContext := range outputs {
		alias := normalizeStepIdentifier(stepID)
		if _, exists := outputs[alias]; !exists {
			outputs[alias] = stepContext
		}
	}

	return outputs, nil
}

func (r *DAGReconciler) findReadySteps(ctx context.Context, steps []bubuv1alpha1.Step, completed, running map[string]bool, dependencies map[string]map[string]bool, vars map[string]any) ([]*bubuv1alpha1.Step, []*bubuv1alpha1.Step) {
	var ready, skipped []*bubuv1alpha1.Step

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
				result, err := r.CEL.EvaluateWhenCondition(ctx, *step.If, vars)
				if err != nil {
					// This indicates a potentially recoverable error (e.g., variable not yet available).
					// We log it but don't fail the entire reconcile.
					log := logging.NewControllerLogger(ctx, "storyrun-if-eval")
					log.Info("CEL 'if' condition blocked or failed evaluation, step not ready", "step", step.Name, "reason", err)
					continue
				}
				if !result {
					skipped = append(skipped, step)
					continue
				}
			}
			ready = append(ready, step)
		}
	}
	return ready, skipped
}

// finalizeSuccessfulRun evaluates the Story's output template and sets the final status.
func (r *DAGReconciler) finalizeSuccessfulRun(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) error {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-finalize").WithValues("storyrun", srun.Name)

	// If there's no output template, just mark as succeeded.
	if story.Spec.Output == nil {
		return r.setStoryRunPhase(ctx, srun, enums.PhaseSucceeded, "All steps completed successfully.")
	}

	// Evaluate the output template
	log.Info("Evaluating story output template")
	storyRunInputs, err := getStoryRunInputs(srun)
	if err != nil {
		return fmt.Errorf("failed to get storyrun inputs for output evaluation: %w", err)
	}
	priorStepOutputs, err := getPriorStepOutputs(ctx, r.Client, srun, nil)
	if err != nil {
		return fmt.Errorf("failed to get prior step outputs for output evaluation: %w", err)
	}

	vars := map[string]any{
		"inputs": storyRunInputs,
		"steps":  priorStepOutputs,
	}

	var outputTemplate map[string]any
	if err := json.Unmarshal(story.Spec.Output.Raw, &outputTemplate); err != nil {
		return fmt.Errorf("failed to unmarshal story output template: %w", err)
	}

	resolvedOutput, err := r.CEL.ResolveWithInputs(ctx, outputTemplate, vars)
	if err != nil {
		return fmt.Errorf("failed to evaluate story output template with CEL: %w", err)
	}

	outputBytes, err := json.Marshal(resolvedOutput)
	if err != nil {
		return fmt.Errorf("failed to marshal resolved story output: %w", err)
	}

	// Patch the output into the status and set the final phase
	return patch.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StoryRun)
		sr.Status.Output = &runtime.RawExtension{Raw: outputBytes}

		// Now set the final phase and other completion fields
		sr.Status.Phase = enums.PhaseSucceeded
		sr.Status.Message = "All steps completed successfully."
		sr.Status.ObservedGeneration = sr.Generation

		if sr.Status.FinishedAt == nil {
			now := metav1.Now()
			sr.Status.FinishedAt = &now
			if sr.Status.StartedAt != nil {
				sr.Status.Duration = now.Sub(sr.Status.StartedAt.Time).String()
			}
		}

		cm := conditions.NewConditionManager(sr.Generation)
		cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonCompleted, "All steps completed successfully.")
	})
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
		if state.Phase == enums.PhaseSucceeded || state.Phase == enums.PhaseSkipped {
			completed[name] = true
		} else if state.Phase == enums.PhaseRunning || state.Phase == enums.PhasePending {
			running[name] = true
		} else if state.Phase.IsTerminal() { // Failed, Canceled, etc.
			failed[name] = true
		}
	}
	return
}

func getStoryRunInputs(srun *runsv1alpha1.StoryRun) (map[string]any, error) {
	if srun.Spec.Inputs == nil {
		return make(map[string]any), nil
	}
	var inputs map[string]any
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
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonReconciling, message)
		case enums.PhaseSucceeded:
			if sr.Status.FinishedAt == nil {
				now := metav1.Now()
				sr.Status.FinishedAt = &now
				if sr.Status.StartedAt != nil {
					sr.Status.Duration = now.Sub(sr.Status.StartedAt.Time).String()
				}
			}
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonCompleted, message)
		case enums.PhaseFailed:
			if sr.Status.FinishedAt == nil {
				now := metav1.Now()
				sr.Status.FinishedAt = &now
				if sr.Status.StartedAt != nil {
					sr.Status.Duration = now.Sub(sr.Status.StartedAt.Time).String()
				}
			}
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonExecutionFailed, message)
		}
	})
}

// normalizeStepIdentifier replaces characters not allowed in CEL identifiers with underscores.
func normalizeStepIdentifier(name string) string {
	return regexp.MustCompile(`[^a-zA-Z0-9_]`).ReplaceAllString(name, "_")
}
