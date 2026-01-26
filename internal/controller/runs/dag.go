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
	"sort"
	"strings"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	rec "github.com/bubustack/bobrapet/pkg/reconcile"
	runsinputs "github.com/bubustack/bobrapet/pkg/runs/inputs"
	runslist "github.com/bubustack/bobrapet/pkg/runs/list"
	runsstatus "github.com/bubustack/bobrapet/pkg/runs/status"
	"github.com/bubustack/bobrapet/pkg/storage"
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

type dagPipelineState struct {
	log          *logging.ControllerLogger
	story        *bubuv1alpha1.Story
	stepRunList  *runsv1alpha1.StepRunList
	priorOutputs map[string]any
	readyCount   int
	skippedCount int
	needsPersist bool
}

func (d *dagPipelineState) recordActivity(ready, skipped int) {
	d.readyCount += ready
	d.skippedCount += skipped
	if ready > 0 || skipped > 0 {
		d.needsPersist = true
	}
}

// DAGReconcilerDeps bundles the collaborators required to construct a DAGReconciler.
type DAGReconcilerDeps struct {
	Client         client.Client
	CELEvaluator   *cel.Evaluator
	StepExecutor   *StepExecutor
	ConfigResolver *config.Resolver
}

// validate ensures all required dependencies are non-nil before construction.
//
// Behavior:
//   - Checks each field in DAGReconcilerDeps for nil.
//   - Panics with a descriptive message listing all missing dependencies.
//
// Arguments:
//   - None (method receiver provides the struct to validate).
//
// Returns:
//   - None; panics on failure to fail fast during setup.
//
// Side Effects:
//   - Panics if any dependency is nil, preventing misconfigured controllers.
func (d DAGReconcilerDeps) validate() {
	var missing []string
	if d.Client == nil {
		missing = append(missing, "client")
	}
	if d.CELEvaluator == nil {
		missing = append(missing, "cel evaluator")
	}
	if d.StepExecutor == nil {
		missing = append(missing, "step executor")
	}
	if d.ConfigResolver == nil {
		missing = append(missing, "config resolver")
	}
	if len(missing) > 0 {
		panic(fmt.Sprintf("NewDAGReconciler missing dependencies: %s", strings.Join(missing, ", ")))
	}
}

// NewDAGReconciler returns a DAGReconciler backed by the controller-runtime client,
// CEL evaluator, StepExecutor, and config resolver supplied by SetupWithManager
// (internal/controller/runs/dag.go:47-63; internal/controller/runs/storyrun_controller.go:536-549).
// Behavior:
//   - Reuses the manager client so DAG helpers can List/Get StepRuns via the cache.
//   - Shares the StepExecutor and CEL evaluator with StoryRun reconciliation to keep
//     execution semantics aligned.
//
// Arguments:
//   - deps DAGReconcilerDeps: groups the cache-aware client, CEL evaluator, StepExecutor, and config resolver.
//
// Returns:
//   - *DAGReconciler ready for registration via `SetupWithManager`.
//
// Side Effects:
//   - None; simply packages existing collaborators.
//
// Notes / Gotchas:
//   - All arguments should be non-nil; nil collaborators will panic when DAG
//     reconciliation dereferences them.
func NewDAGReconciler(deps DAGReconcilerDeps) *DAGReconciler {
	deps.validate()
	return &DAGReconciler{
		Client:         deps.Client,
		CEL:            deps.CELEvaluator,
		StepExecutor:   deps.StepExecutor,
		ConfigResolver: deps.ConfigResolver,
	}
}

// Reconcile orchestrates the execution of the StoryRun's DAG.
//
// Behavior:
//   - Initializes StepStates map if nil.
//   - Syncs StepRun phases into StoryRun.Status.StepStates.
//   - Collects prior step outputs for CEL evaluation.
//   - Iterates the DAG: refreshes sub-stories, checks completion/failure,
//     persists state, and launches ready steps until no progress is made.
//   - Updates running duration and requeues while the StoryRun is active.
//
// Arguments:
//   - ctx context.Context: propagated to all client calls and helpers.
//   - srun *runsv1alpha1.StoryRun: the StoryRun being orchestrated.
//   - story *bubuv1alpha1.Story: the Story definition containing step DAG.
//
// Returns:
//   - ctrl.Result: contains RequeueAfter when StoryRun is still running.
//   - error: non-nil on sync/launch/persist failures.
//
// Side Effects:
//   - Creates/updates StepRuns via StepExecutor.
//   - Patches StoryRun status (StepStates, Duration, Phase).
//   - Emits DAG iteration metrics.
func (r *DAGReconciler) Reconcile(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) (ctrl.Result, error) {
	hooks := rec.PipelineHooks[*runsv1alpha1.StoryRun, *dagPipelineState]{
		Prepare: func(ctx context.Context, sr *runsv1alpha1.StoryRun) (*dagPipelineState, *ctrl.Result, error) {
			state := &dagPipelineState{
				log:   logging.NewReconcileLogger(ctx, "storyrun-dag").WithValues("storyrun", sr.Name),
				story: story,
			}

			ensureStepStatesInitialized(sr)

			list, err := r.syncStateFromStepRuns(ctx, sr)
			if err != nil {
				return nil, nil, err
			}
			state.log.Info("Synced StepRuns", "count", len(list.Items))
			state.stepRunList = list

			prior, err := getPriorStepOutputs(ctx, r.Client, sr, list)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get prior step outputs: %w", err)
			}
			state.priorOutputs = prior

			return state, nil, nil
		},
		Ensure: func(ctx context.Context, sr *runsv1alpha1.StoryRun, state *dagPipelineState) (ctrl.Result, error) {
			if err := r.runDagIterations(ctx, sr, state); err != nil {
				return ctrl.Result{}, err
			}
			return r.buildDagResult(sr), nil
		},
		Finalize: func(ctx context.Context, sr *runsv1alpha1.StoryRun, state *dagPipelineState, result ctrl.Result, ensureErr error) (ctrl.Result, error) {
			if ensureErr != nil {
				return result, ensureErr
			}

			r.recordDagMetrics(state)

			if state != nil && state.needsPersist {
				if err := r.persistMergedStates(ctx, sr); err != nil {
					state.log.Error(err, "Failed to patch StoryRun status after DAG traversal")
					return result, err
				}
			}

			if sr.Status.Phase == enums.PhaseRunning {
				if err := r.updateRunningDuration(ctx, sr); err != nil && state != nil && state.log != nil {
					state.log.Error(err, "Failed to update running duration")
				}
				result.RequeueAfter = r.runningRequeueDelay()
			}

			return result, nil
		},
	}

	return rec.RunPipeline(ctx, srun, hooks)
}

func (r *DAGReconciler) runDagIterations(ctx context.Context, srun *runsv1alpha1.StoryRun, state *dagPipelineState) error {
	if state == nil || state.story == nil {
		return fmt.Errorf("dag pipeline state is not initialized")
	}

	for i := 0; i < len(state.story.Spec.Steps)+1; i++ {
		var err error
		state.stepRunList, state.priorOutputs, err = r.refreshAfterSubStoriesIfNeeded(ctx, srun, state.story, state.stepRunList, state.priorOutputs)
		if err != nil {
			return err
		}

		completed, running, failed := buildStateMaps(state.story, srun.Status.StepStates)
		done, err := r.checkCompletionOrFailure(ctx, srun, state.story, failed, completed, state.log)
		if err != nil {
			return err
		}
		if done {
			return nil
		}

		ready, skipped, expanded, err := r.findAndLaunchReadySteps(ctx, srun, state.story, completed, running, state.priorOutputs)
		if err != nil {
			return err
		}
		state.recordActivity(len(ready), len(skipped))
		if expanded {
			state.needsPersist = true
		}

		if len(ready) == 0 && len(skipped) == 0 {
			break
		}

		state.stepRunList, err = r.syncStateFromStepRuns(ctx, srun)
		if err != nil {
			return err
		}
		state.priorOutputs, err = getPriorStepOutputs(ctx, r.Client, srun, state.stepRunList)
		if err != nil {
			return fmt.Errorf("failed to get prior step outputs: %w", err)
		}
	}
	return nil
}

func (r *DAGReconciler) recordDagMetrics(state *dagPipelineState) {
	if state == nil {
		return
	}
	if state.readyCount == 0 && state.skippedCount == 0 {
		return
	}
	metrics.RecordDAGIteration("storyrun-dag", state.readyCount, state.skippedCount)
}

func (r *DAGReconciler) buildDagResult(_ *runsv1alpha1.StoryRun) ctrl.Result {
	return ctrl.Result{}
}

// updateRunningDuration patches Status.Duration for running StoryRuns by
// recomputing time.Since(Status.StartedAt) and writing it via
// kubeutil.RetryableStatusPatch (internal/controller/runs/dag.go:124-145).
// Behavior:
//   - Runs only when PhaseRunning and StartedAt are populated.
//   - Computes time.Since(...).Round(time.Second) and stores the formatted string.
//   - Returns the patch error so callers can decide whether to surface/log it.
//
// Arguments:
//   - ctx context.Context: propagated into kubeutil.RetryableStatusPatch.
//   - srun *runsv1alpha1.StoryRun: provides phase/start timestamps and receives the duration update.
//
// Returns:
//   - error: nil on success, or any error returned by kubeutil.RetryableStatusPatch.
//
// Side Effects:
//   - Issues a status-only update on the StoryRun resource.
//
// Notes / Gotchas:
//   - Helper skips the patch when the rounded duration is unchanged to avoid API churn.
func (r *DAGReconciler) updateRunningDuration(ctx context.Context, srun *runsv1alpha1.StoryRun) error {
	if srun.Status.Phase != enums.PhaseRunning || srun.Status.StartedAt == nil {
		return nil
	}
	nextDuration := time.Since(srun.Status.StartedAt.Time).Round(time.Second).String()
	if srun.Status.Duration == nextDuration {
		return nil
	}
	return kubeutil.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StoryRun)
		if sr.Status.Phase != enums.PhaseRunning || sr.Status.StartedAt == nil {
			return
		}
		updated := time.Since(sr.Status.StartedAt.Time).Round(time.Second).String()
		if sr.Status.Duration == updated {
			return
		}
		sr.Status.Duration = updated
	})
}

// refreshAfterSubStoriesIfNeeded reruns syncStateFromStepRuns and
// getPriorStepOutputs when checkSyncSubStories observes new sub-story
// completions, otherwise it returns the previously fetched list and output
// map unchanged (internal/controller/runs/dag.go:83-88,154-167,270-410).
// Arguments:
//   - ctx context.Context: forwarded into checkSyncSubStories, syncStateFromStepRuns,
//     and getPriorStepOutputs.
//   - srun/story: identify the StoryRun and its DAG so executeStory steps can be examined.
//   - stepRunList/prior: cached data to return when no refresh is required.
//
// Returns:
//   - Updated (*StepRunList, map[string]any) when sub-stories changed, or the originals otherwise.
//   - error when either the StepRun re-sync or output rebuild fails.
//
// Side Effects:
//   - checkSyncSubStories may mutate Status.StepStates for executeStory steps.
func (r *DAGReconciler) refreshAfterSubStoriesIfNeeded(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, stepRunList *runsv1alpha1.StepRunList, prior map[string]any) (*runsv1alpha1.StepRunList, map[string]any, error) {
	if updated := r.checkSyncSubStories(ctx, srun, story); !updated {
		return stepRunList, prior, nil
	}
	metrics.RecordSubStoryRefresh("storyrun-dag")
	lst, err := r.syncStateFromStepRuns(ctx, srun)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to re-sync step runs after sub-story sync: %w", err)
	}
	out, err := getPriorStepOutputs(ctx, r.Client, srun, lst)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to re-fetch prior step outputs after sub-story sync: %w", err)
	}
	return lst, out, nil
}

// checkCompletionOrFailure inspects the completed/failed step maps and either
// triggers fail-fast handling or finalizes the StoryRun when all steps are done
// (internal/controller/runs/dag.go:89-181).
// Behavior:
//   - If any steps failed and the Story's fail-fast policy is true, it sets the
//     StoryRun phase to Failed and stops the loop.
//   - If every step completed, it runs finalizeSuccessfulRun and returns whether the
//     DAG finished.
//
// Arguments:
//   - ctx context.Context: forwarded into status helpers.
//   - srun/story: StoryRun being reconciled and its Story definition.
//   - failed/completed maps: derived from buildStateMaps.
//   - log *logging.ControllerLogger: used to surface finalization failures.
//
// Returns:
//   - bool: true when the DAG reached a terminal condition.
//   - error: any error returned by finalizeSuccessfulRun or the subsequent phase kubeutil.
//
// Side Effects:
//   - Writes StoryRun status via setStoryRunPhase / finalizeSuccessfulRun.
//
// Notes / Gotchas:
//   - Caller should stop iterating when the returned bool is true, even if an error
//     is also returned.
func (r *DAGReconciler) checkCompletionOrFailure(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, failed, completed map[string]bool, log *logging.ControllerLogger) (bool, error) {
	if len(failed) > 0 && r.shouldFailFast(story) {
		reason := "A step failed and fail-fast policy is enabled."
		for name := range failed {
			reason = fmt.Sprintf("Step %s failed and fail-fast policy is enabled.", name)
			break
		}
		return true, r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, reason)
	}
	if len(completed) == len(story.Spec.Steps) {
		if err := r.finalizeSuccessfulRun(ctx, srun, story); err != nil {
			log.Error(err, "Failed to finalize successful story run")
			_ = r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, "Failed to evaluate final output template.")
			return true, err
		}
		return true, nil
	}
	return false, nil
}

// persistMergedStates re-reads StoryRun status and merges locally computed StepStates
// without clobbering terminal phases that may have landed via another reconcile pass.
//
// Behavior:
//   - Re-fetches StoryRun status to get the latest cluster state.
//   - Merges local StepStates, preserving terminal phases from the cluster.
//   - Updates progress counters via updateStepProgress.
//
// Arguments:
//   - ctx context.Context: propagated to the patch helper.
//   - srun *runsv1alpha1.StoryRun: the StoryRun with local state to merge.
//
// Returns:
//   - error: nil on success, or the patch error.
//
// Side Effects:
//   - Updates StoryRun status subresource.
//   - Emits step progress metrics.
func (r *DAGReconciler) persistMergedStates(ctx context.Context, srun *runsv1alpha1.StoryRun) error {
	return kubeutil.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
		sr := obj.(*runsv1alpha1.StoryRun)
		if sr.Status.StepStates == nil {
			sr.Status.StepStates = make(map[string]runsv1alpha1.StepState, len(srun.Status.StepStates))
		}
		// Only merge states that haven't been updated by concurrent reconciles.
		// Preserve terminal phases (Succeeded, Failed, Timeout, etc.) from the freshly fetched object
		// to avoid overwriting step completions that occurred during this reconcile iteration.
		for k, v := range srun.Status.StepStates {
			existingState, exists := sr.Status.StepStates[k]
			// If the step reached a terminal state in the cluster, don't overwrite it.
			if exists && existingState.Phase.IsTerminal() && !v.Phase.IsTerminal() {
				continue
			}
			sr.Status.StepStates[k] = v
		}
		r.updateStepProgress(sr)
	})
}

// runningRequeueDelay returns a jittered requeue duration for running StoryRuns.
//
// Behavior:
//   - Uses ConfigResolver to get operator-configured requeue delays.
//   - Falls back to 5s base / 30s max when config is unavailable.
//   - Applies jitter via rec.JitteredRequeueDelay.
//
// Arguments:
//   - None (uses receiver's ConfigResolver).
//
// Returns:
//   - time.Duration: the computed requeue delay with jitter.
func (r *DAGReconciler) runningRequeueDelay() time.Duration {
	const fallbackBase = 5 * time.Second
	const fallbackMax = 30 * time.Second

	if r.ConfigResolver == nil {
		return rec.JitteredRequeueDelay(0, 0, fallbackBase, fallbackMax)
	}
	cfg := r.ConfigResolver.GetOperatorConfig()
	if cfg == nil {
		return rec.JitteredRequeueDelay(0, 0, fallbackBase, fallbackMax)
	}
	return rec.JitteredRequeueDelay(cfg.Controller.RequeueBaseDelay, cfg.Controller.RequeueMaxDelay, fallbackBase, fallbackMax)
}

// updateStepProgress summarizes StepStates into StoryRun counters and emits metrics.
//
// Behavior:
//   - Calls summarizeStepProgress to count active/completed/failed/skipped steps.
//   - Updates Status.StepsComplete, Status.StepsFailed, Status.StepsSkipped.
//   - Emits gauge metrics via metrics.UpdateStoryRunStepsGauge.
//
// Arguments:
//   - sr *runsv1alpha1.StoryRun: the StoryRun to update in memory.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates StoryRun status counters.
//   - Emits Prometheus gauge updates.
func (r *DAGReconciler) updateStepProgress(sr *runsv1alpha1.StoryRun) {
	active, completed, failed, skipped := summarizeStepProgress(sr.Status.StepStates)
	sr.Status.StepsComplete = int32(completed)
	sr.Status.StepsFailed = int32(failed)
	sr.Status.StepsSkipped = int32(skipped)
	if storyName := sr.Spec.StoryRef.Name; storyName != "" {
		metrics.UpdateStoryRunStepsGauge(sr.Namespace, storyName, active, completed)
	}
}

// summarizeStepProgress tallies StepStates by phase category.
//
// Behavior:
//   - Iterates all StepStates and increments counters based on Phase.
//   - Running → active, Succeeded → completed, Failed → failed, Skipped → skipped.
//
// Arguments:
//   - states map[string]runsv1alpha1.StepState: the StepStates to summarize.
//
// Returns:
//   - active int: count of Running steps.
//   - completed int: count of Succeeded steps.
//   - failed int: count of Failed steps.
//   - skipped int: count of Skipped steps.
func summarizeStepProgress(states map[string]runsv1alpha1.StepState) (active, completed, failed, skipped int) {
	for _, state := range states {
		switch state.Phase {
		case enums.PhaseRunning:
			active++
		case enums.PhaseSucceeded:
			completed++
		case enums.PhaseFailed:
			failed++
		case enums.PhaseSkipped:
			skipped++
		}
	}
	return
}

// syncStateFromStepRuns lists all StepRuns for the StoryRun and mirrors their phases
// into Status.StepStates.
//
// Behavior:
//   - Initializes StepStates if nil.
//   - Lists StepRuns via runslist.StepRunsByStoryRun using field indexes.
//   - Skips StepRuns with empty Phase (invalid/initializing).
//   - Updates StepStates when phase changes, logging transitions.
//
// Arguments:
//   - ctx context.Context: propagated to list operations.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//
// Returns:
//   - *runsv1alpha1.StepRunList: the listed StepRuns.
//   - error: nil on success, or the list error.
//
// Side Effects:
//   - Mutates srun.Status.StepStates in memory.
//   - Logs phase transitions.
func (r *DAGReconciler) syncStateFromStepRuns(ctx context.Context, srun *runsv1alpha1.StoryRun) (*runsv1alpha1.StepRunList, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-sync")
	ensureStepStatesInitialized(srun)
	var stepRunList runsv1alpha1.StepRunList
	if err := runslist.StepRunsByStoryRun(ctx, r.Client, srun.Namespace, srun.Name, nil, &stepRunList); err != nil {
		log.Error(err, "Failed to list StepRuns")
		return nil, err
	}

	for _, sr := range stepRunList.Items {
		stepID := sr.Spec.StepID
		currentState, exists := srun.Status.StepStates[stepID]

		// Skip if StepRun phase is empty (invalid/initializing state)
		// Empty phases fail Kubernetes validation and cause reconciliation storms
		if sr.Status.Phase == "" {
			continue
		}

		// Only sync if this is a new step OR the phase has changed
		if !exists || currentState.Phase != sr.Status.Phase {
			oldPhase := enums.Phase("")
			if exists {
				oldPhase = currentState.Phase
			}
			log.Info("Syncing StepRun status to StoryRun", "step", stepID, "oldPhase", oldPhase, "newPhase", sr.Status.Phase)
			srun.Status.StepStates[stepID] = runsv1alpha1.StepState{
				Phase:   sr.Status.Phase,
				Message: sr.Status.LastFailureMsg,
			}
		}
	}
	return &stepRunList, nil
}

// checkSyncSubStories polls executeStory steps waiting for completion and copies
// their terminal phase/message into the parent StepState.
//
// Behavior:
//   - Iterates Story steps looking for running executeStory types with waitForCompletion.
//   - Fetches the child StoryRun and checks if it reached a terminal phase.
//   - Updates parent StepState when the child completes.
//
// Arguments:
//   - ctx context.Context: propagated to GET calls.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *bubuv1alpha1.Story: the Story definition.
//
// Returns:
//   - bool: true if any StepState was updated (caller should refresh).
//
// Side Effects:
//   - Mutates srun.Status.StepStates in memory.
//   - Logs errors and completions.
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

// findAndLaunchReadySteps builds dependency graphs, finds ready steps, and launches them.
//
// Behavior:
//   - Builds dependency/dependent graphs from Story steps.
//   - Decodes StoryRun inputs for CEL evaluation.
//   - Merges prior step outputs with inputs for variable context.
//   - Calls findReadySteps to identify ready and skipped steps.
//   - Marks skipped steps and launches ready steps via StepExecutor.
//   - Expands running loop steps.
//
// Arguments:
//   - ctx context.Context: propagated to executor and helpers.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *bubuv1alpha1.Story: the Story definition with step DAG.
//   - completedSteps, runningSteps map[string]bool: current state buckets.
//   - priorStepOutputs map[string]any: outputs from completed steps for CEL.
//
// Returns:
//   - []*bubuv1alpha1.Step: steps that were launched.
//   - []*bubuv1alpha1.Step: steps that were skipped.
//   - error: nil on success, or execution errors.
//
// Side Effects:
//   - Creates/updates StepRuns via StepExecutor.
//   - Mutates srun.Status.StepStates for skipped steps.
func (r *DAGReconciler) findAndLaunchReadySteps(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, completedSteps, runningSteps map[string]bool, priorStepOutputs map[string]any) ([]*bubuv1alpha1.Step, []*bubuv1alpha1.Step, bool, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-launcher")
	ensureStepStatesInitialized(srun)
	dependencies, _ := buildDependencyGraphs(story.Spec.Steps)
	storyRunInputs, _ := getStoryRunInputs(srun)

	vars := map[string]any{
		"inputs": storyRunInputs,
		"steps":  priorStepOutputs,
	}

	readySteps, skippedSteps := r.findReadySteps(ctx, story, story.Spec.Steps, srun.Status.StepStates, completedSteps, runningSteps, dependencies, vars)

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
			return nil, nil, false, fmt.Errorf("failed to execute step %s: %w", step.Name, err)
		}
		if state, ok := srun.Status.StepStates[step.Name]; !ok || state.Phase == "" {
			srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseRunning}
		}
	}

	expanded, err := r.expandRunningLoopSteps(ctx, srun, story, vars)
	if err != nil {
		return nil, nil, false, err
	}

	return readySteps, skippedSteps, expanded, nil
}

// expandRunningLoopSteps re-executes running loop steps to launch additional iterations.
//
// Behavior:
//   - Iterates Story steps looking for running loop types with initialized Loop state.
//   - Re-calls StepExecutor.Execute to launch additional loop iterations.
//   - Returns on first execution error.
//
// Arguments:
//   - ctx context.Context: propagated to executor.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *bubuv1alpha1.Story: the Story definition.
//   - vars map[string]any: variable context for CEL evaluation.
//
// Returns:
//   - error: nil on success, or execution errors.
//
// Side Effects:
//   - Creates additional StepRuns via StepExecutor.
func (r *DAGReconciler) expandRunningLoopSteps(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, vars map[string]any) (bool, error) {
	if story == nil {
		return false, nil
	}
	expanded := false
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if step.Type != enums.StepTypeLoop {
			continue
		}
		state, exists := srun.Status.StepStates[step.Name]
		if !exists || state.Loop == nil || state.Phase != enums.PhaseRunning {
			continue
		}
		if err := r.StepExecutor.Execute(ctx, srun, story, step, vars); err != nil {
			return expanded, fmt.Errorf("failed to expand loop step %s: %w", step.Name, err)
		}
		expanded = true
	}
	return expanded, nil
}

// getPriorStepOutputs aggregates outputs from completed StepRuns and sub-stories.
//
// Behavior:
//   - Ensures a StepRunList is available (uses cached list or fetches fresh).
//   - Collects outputs from succeeded StepRuns via collectOutputsFromStepRuns.
//   - Collects outputs from synchronous sub-stories via collectOutputsFromSubStories.
//   - Adds normalized alias keys for CEL expressions.
//
// Arguments:
//   - ctx context.Context: propagated to list/get operations.
//   - c client.Client: Kubernetes client for fetching resources.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - stepRunList *runsv1alpha1.StepRunList: optional cached list (nil triggers fresh fetch).
//
// Returns:
//   - map[string]any: aggregated outputs keyed by step name.
//   - error: nil on success, or list/fetch errors.
func getPriorStepOutputs(ctx context.Context, c client.Client, srun *runsv1alpha1.StoryRun, stepRunList *runsv1alpha1.StepRunList) (map[string]any, error) {
	log := logging.NewReconcileLogger(ctx, "dag-output-resolver")
	outputs := make(map[string]any)

	// Ensure we have a list of StepRuns to inspect
	lst, err := ensureStepRunList(ctx, c, srun, stepRunList, log)
	if err != nil {
		return nil, err
	}

	// Collect outputs from StepRuns and synchronous sub-stories
	collectOutputsFromStepRuns(lst, outputs, log)
	collectOutputsFromSubStories(ctx, c, srun, outputs, log)

	// Add normalized aliases for CEL expressions
	addAliasKeys(outputs)

	return outputs, nil
}

// ensureStepRunList returns a non-nil StepRunList scoped to the StoryRun.
//
// Behavior:
//   - Returns the provided list if non-nil.
//   - Otherwise lists StepRuns via runslist.StepRunsByStoryRun.
//
// Arguments:
//   - ctx context.Context: propagated to list operations.
//   - c client.Client: Kubernetes client.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - stepRunList *runsv1alpha1.StepRunList: optional cached list.
//   - log *logging.ReconcileLogger: for error logging.
//
// Returns:
//   - *runsv1alpha1.StepRunList: the cached or freshly fetched list.
//   - error: nil on success, or list errors.
func ensureStepRunList(
	ctx context.Context,
	c client.Client,
	srun *runsv1alpha1.StoryRun,
	stepRunList *runsv1alpha1.StepRunList,
	log *logging.ReconcileLogger,
) (*runsv1alpha1.StepRunList, error) {
	if stepRunList != nil {
		return stepRunList, nil
	}
	var newList runsv1alpha1.StepRunList
	if err := runslist.StepRunsByStoryRun(ctx, c, srun.Namespace, srun.Name, nil, &newList); err != nil {
		log.Error(err, "Failed to list StepRuns for output resolution")
		return nil, err
	}
	return &newList, nil
}

// collectOutputsFromStepRuns extracts outputs from succeeded StepRuns into outputs map.
//
// Behavior:
//   - Iterates StepRuns and populates outputs/signals for succeeded phases.
//   - Unmarshals Status.Output via populateStepOutputs.
//   - Applies manifest placeholders for CEL evaluation.
//   - Merges signals into the step context.
//
// Arguments:
//   - stepRunList *runsv1alpha1.StepRunList: the StepRuns to process.
//   - outputs map[string]any: destination map keyed by step ID.
//   - log *logging.ReconcileLogger: for error and info logging.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates the outputs map with step contexts.
func collectOutputsFromStepRuns(stepRunList *runsv1alpha1.StepRunList, outputs map[string]any, log *logging.ReconcileLogger) {
	for i := range stepRunList.Items {
		sr := &stepRunList.Items[i]
		stepID := sr.Spec.StepID

		var stepContext map[string]any
		if existing, exists := outputs[stepID]; exists {
			if cast, ok := existing.(map[string]any); ok {
				stepContext = cast
			}
		}
		if stepContext == nil {
			stepContext = make(map[string]any)
		}

		updated := false

		if sr.Status.Phase == enums.PhaseSucceeded {
			if outputData, ok := populateStepOutputs(
				sr.Status.Output,
				stepID,
				stepContext,
				log,
				"Failed to unmarshal output from prior StepRun during fallback",
			); ok {
				logStepOutputSummary(log, sr, outputData)
				if len(sr.Status.Manifest) > 0 {
					stepContext["manifest"] = sr.Status.Manifest
					applyManifestPlaceholders(outputData, sr.Status.Manifest)
				}
				updated = true
			}
		}
		if len(sr.Status.Signals) > 0 {
			signals := make(map[string]any)
			for key, raw := range sr.Status.Signals {
				if len(raw.Raw) == 0 {
					continue
				}
				var data any
				if err := json.Unmarshal(raw.Raw, &data); err != nil {
					log.Error(err, "Failed to unmarshal signal from StepRun", "step", sr.Spec.StepID, "signal", key)
					continue
				}
				insertSignalValue(signals, key, data)
			}
			if len(signals) > 0 {
				if existingSignals, ok := stepContext["signals"].(map[string]any); ok {
					mergeSignalMaps(existingSignals, signals)
				} else {
					stepContext["signals"] = signals
				}
				updated = true
			}
		}
		if updated {
			outputs[stepID] = stepContext
			log.Info("Collected StepRun context for DAG inputs",
				"step", stepID,
				"phase", sr.Status.Phase,
				"signals", len(sr.Status.Signals) > 0,
				"outputsCaptured", stepContext["outputs"] != nil)
		}
	}
}

// mergeSignalMaps recursively merges src into dst, preserving nested map structure.
//
// Behavior:
//   - Returns immediately if either map is nil.
//   - Recursively merges nested maps.
//   - Overwrites non-map values.
//
// Arguments:
//   - dst map[string]any: destination map to merge into.
//   - src map[string]any: source map to merge from.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates dst in place.
func mergeSignalMaps(dst, src map[string]any) {
	if dst == nil || src == nil {
		return
	}
	for k, v := range src {
		if existing, ok := dst[k].(map[string]any); ok {
			if vMap, ok := v.(map[string]any); ok {
				mergeSignalMaps(existing, vMap)
				continue
			}
		}
		dst[k] = v
	}
}

// walkOrCreateDottedPath traverses/creates nested maps for a dotted path.
//
// Behavior:
//   - Splits path on '.' and traverses/creates intermediate maps.
//   - Returns the parent map and final segment for assignment.
//   - Optionally trims whitespace and replaces non-map values.
//
// Arguments:
//   - root map[string]any: starting map for traversal.
//   - path string: dotted path like "a.b.c".
//   - trim bool: whether to trim whitespace from segments.
//   - replaceNonMap bool: whether to replace non-map values with new maps.
//
// Returns:
//   - map[string]any: the parent map for the final segment.
//   - string: the final segment name.
//   - bool: true if path was valid and traversable.
func walkOrCreateDottedPath(root map[string]any, path string, trim bool, replaceNonMap bool) (map[string]any, string, bool) {
	if trim {
		path = strings.TrimSpace(path)
	}
	if path == "" {
		return nil, "", false
	}

	segments := strings.Split(path, ".")
	current := root
	for i, segment := range segments {
		if trim {
			segment = strings.TrimSpace(segment)
		}
		if segment == "" {
			continue
		}
		if i == len(segments)-1 {
			return current, segment, true
		}

		if next, ok := current[segment].(map[string]any); ok {
			current = next
			continue
		}

		if _, exists := current[segment]; exists && !replaceNonMap {
			return nil, "", false
		}

		next := make(map[string]any)
		current[segment] = next
		current = next
	}
	return nil, "", false
}

// insertSignalValue stores a dotted signal key inside a nested map structure,
// allocating intermediate map[string]any nodes as needed.
//
// Behavior:
//   - Trims whitespace and returns early when the key is empty.
//   - Splits the key on '.', skipping blank segments so malformed paths collapse safely.
//   - Reuses or allocates intermediate maps until the final segment is reached, then writes value in place.
//
// Arguments:
//   - root map[string]any: destination map that will be mutated in place; callers must pass a non-nil map.
//   - key string: dotted path whose segments are trimmed before insertion.
//   - value any: payload stored at the last segment without cloning.
func insertSignalValue(root map[string]any, key string, value any) {
	current, leaf, ok := walkOrCreateDottedPath(root, key, true, true)
	if !ok {
		return
	}
	current[leaf] = value
}

// collectOutputsFromSubStories extracts outputs from completed synchronous sub-stories.
//
// Behavior:
//   - Iterates StepStates looking for succeeded steps with SubStoryRunName.
//   - Fetches each sub-StoryRun and decodes its output.
//   - Skips steps that already have outputs populated.
//
// Arguments:
//   - ctx context.Context: propagated to GET calls.
//   - c client.Client: Kubernetes client for fetching sub-StoryRuns.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - outputs map[string]any: destination map keyed by step ID.
//   - log *logging.ReconcileLogger: for error logging.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates the outputs map with sub-story contexts.
func collectOutputsFromSubStories(ctx context.Context, c client.Client, srun *runsv1alpha1.StoryRun, outputs map[string]any, log *logging.ReconcileLogger) {
	for stepID, state := range srun.Status.StepStates {
		if state.Phase != enums.PhaseSucceeded || state.SubStoryRunName == "" {
			continue
		}
		if _, exists := outputs[stepID]; exists {
			continue
		}
		subRun := &runsv1alpha1.StoryRun{}
		subRunKey := types.NamespacedName{Name: state.SubStoryRunName, Namespace: srun.Namespace}
		if err := c.Get(ctx, subRunKey, subRun); err != nil {
			log.Error(err, "Failed to get sub-StoryRun for output resolution", "subStoryRun", state.SubStoryRunName)
			continue
		}
		stepContext := make(map[string]any)
		if _, ok := populateStepOutputs(
			subRun.Status.Output,
			stepID,
			stepContext,
			log,
			"Failed to unmarshal output from sub-StoryRun",
		); ok {
			outputs[stepID] = stepContext
		}
	}
}

// populateStepOutputs decodes a RawExtension into standard step output keys.
//
// Behavior:
//   - Returns early if raw is nil or empty.
//   - Unmarshals JSON into stepContext["outputs"] and stepContext["output"].
//   - Logs errors with the provided error message.
//
// Arguments:
//   - raw *runtime.RawExtension: the serialized output to decode.
//   - stepID string: step identifier for logging.
//   - stepContext map[string]any: destination map for output keys.
//   - log *logging.ReconcileLogger: for error logging.
//   - errMsg string: error message prefix for logging.
//
// Returns:
//   - map[string]any: the decoded output data.
//   - bool: true if decoding succeeded.
func populateStepOutputs(
	raw *runtime.RawExtension,
	stepID string,
	stepContext map[string]any,
	log *logging.ReconcileLogger,
	errorMessage string,
) (map[string]any, bool) {
	if stepContext == nil || raw == nil || len(raw.Raw) == 0 {
		return nil, false
	}
	if _, exists := stepContext["outputs"]; exists {
		return nil, false
	}

	var outputData map[string]any
	if err := json.Unmarshal(raw.Raw, &outputData); err != nil {
		if log != nil {
			log.Error(err, errorMessage, "step", stepID)
		}
		return nil, false
	}

	stepContext["outputs"] = outputData
	stepContext["output"] = outputData
	return outputData, true
}

// applyManifestPlaceholders synthesizes placeholder values from manifest metadata.
//
// Behavior:
//   - Iterates manifest entries and applies samples, lengths, or exists markers.
//   - Handles root manifest metadata separately via applyRootManifestMetadata.
//   - Skips array-indexed paths (containing '[').
//
// Arguments:
//   - outputs map[string]any: destination map for placeholders.
//   - manifest map[string]runsv1alpha1.StepManifestData: manifest metadata.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates outputs map with placeholder values.
func applyManifestPlaceholders(outputs map[string]any, manifest map[string]runsv1alpha1.StepManifestData) {
	if outputs == nil || len(manifest) == 0 {
		return
	}

	for path, data := range manifest {
		if path == "" || path == manifestRootPath {
			applyRootManifestMetadata(outputs, data)
			continue
		}
		if strings.Contains(path, "[") {
			continue
		}

		if refPath, ok := storageRefFromOutputs(outputs); ok &&
			data.Length == nil &&
			data.Hash == "" &&
			data.Type == "" &&
			data.Sample == nil {
			ensurePathValue(outputs, path, map[string]any{
				storage.StorageRefKey:  refPath,
				storage.StoragePathKey: path,
			})
			continue
		}

		if sample, ok := decodeManifestSample(data.Sample); ok {
			ensurePathValue(outputs, path, sample)
			continue
		}

		if data.Hash != "" || data.Type != "" {
			meta := map[string]any{}
			if data.Hash != "" {
				meta[cel.ManifestHashKey] = data.Hash
			}
			if data.Type != "" {
				meta[cel.ManifestTypeKey] = data.Type
			}
			if len(meta) > 0 {
				ensurePathValue(outputs, path, meta)
				continue
			}
		}

		if data.Length != nil {
			length := int(*data.Length)
			if length < 0 {
				length = 0
			}
			placeholder := make([]any, length)
			ensurePathValue(outputs, path, placeholder)
			continue
		}

		if data.Exists != nil && *data.Exists {
			ensurePathValue(outputs, path, map[string]any{})
		}
	}
}

func storageRefFromOutputs(outputs map[string]any) (string, bool) {
	if outputs == nil {
		return "", false
	}
	refRaw, ok := outputs[storage.StorageRefKey]
	if !ok {
		return "", false
	}
	refPath, ok := refRaw.(string)
	if !ok || refPath == "" {
		return "", false
	}
	return refPath, true
}

// applyRootManifestMetadata writes root-level manifest metadata to outputs.
//
// Behavior:
//   - Writes cel.ManifestLengthKey when data.Length is non-nil.
//
// Arguments:
//   - outputs map[string]any: destination map.
//   - data runsv1alpha1.StepManifestData: root manifest metadata.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates outputs map.
func applyRootManifestMetadata(outputs map[string]any, data runsv1alpha1.StepManifestData) {
	if outputs == nil {
		return
	}
	if data.Length != nil {
		outputs[cel.ManifestLengthKey] = *data.Length
	}
}

// decodeManifestSample unmarshals a manifest sample RawExtension.
//
// Behavior:
//   - Returns early if raw is nil or empty.
//   - Unmarshals JSON into any type.
//
// Arguments:
//   - raw *runtime.RawExtension: the sample to decode.
//
// Returns:
//   - any: the decoded sample value.
//   - bool: true if decoding succeeded.
func decodeManifestSample(raw *runtime.RawExtension) (any, bool) {
	if raw == nil || len(raw.Raw) == 0 {
		return nil, false
	}
	var out any
	if err := json.Unmarshal(raw.Raw, &out); err != nil {
		return nil, false
	}
	return out, true
}

// ensurePathValue inserts a value at a dotted manifest path inside the outputs map
// without overwriting existing leaves.
//
// Behavior:
//   - Splits the path on '.' and skips blank segments so malformed keys collapse safely.
//   - Allocates intermediate map[string]any nodes as needed, but aborts if a segment already holds a non-map value.
//   - Writes the provided value only when the final segment is missing, preserving real StepRun outputs.
//
// Arguments:
//   - root map[string]any: mutable outputs map that applyManifestPlaceholders builds before inserting placeholders.
//   - path string: dotted manifest key taken from StepManifestData entries.
//   - value any: sample, placeholder array, or object that should appear at the target path.
func ensurePathValue(root map[string]any, path string, value any) {
	current, leaf, ok := walkOrCreateDottedPath(root, path, false, false)
	if !ok {
		return
	}
	if _, exists := current[leaf]; !exists {
		current[leaf] = value
	}
}

// logStepOutputSummary logs output keys and size when verbose logging is enabled.
//
// Behavior:
//   - Returns early if step output logging is disabled.
//   - Collects and sorts output keys for readable logging.
//   - Logs step ID, byte size, and key list.
//
// Arguments:
//   - log *logging.ReconcileLogger: logger instance.
//   - sr *runsv1alpha1.StepRun: the StepRun with output.
//   - output map[string]any: the decoded output map.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Logs info message when enabled.
func logStepOutputSummary(log *logging.ReconcileLogger, sr *runsv1alpha1.StepRun, output map[string]any) {
	if !logging.StepOutputLoggingEnabled() || log == nil || sr == nil || sr.Status.Output == nil {
		return
	}
	keys := make([]string, 0, len(output))
	for k := range output {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	log.Info("Captured step output", "step", sr.Spec.StepID, "sizeBytes", len(sr.Status.Output.Raw), "keys", keys)
}

// addAliasKeys duplicates keys using normalized aliases for CEL convenience.
//
// Behavior:
//   - Iterates outputs and creates normalized aliases via normalizeStepIdentifier.
//   - Skips if alias already exists to preserve original data.
//
// Arguments:
//   - outputs map[string]any: the outputs map to augment.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates outputs map with alias entries.
func addAliasKeys(outputs map[string]any) {
	for stepID, stepContext := range outputs {
		alias := normalizeStepIdentifier(stepID)
		if _, exists := outputs[alias]; !exists {
			outputs[alias] = stepContext
		}
	}
}

// findReadySteps identifies steps whose dependencies and conditions are satisfied.
//
// Behavior:
//   - Skips completed or running steps.
//   - Checks all dependencies are met (completed or streaming-satisfied).
//   - Evaluates CEL "if" conditions when present.
//   - Separates ready steps from skipped steps (condition evaluated to false).
//
// Arguments:
//   - ctx context.Context: propagated to CEL evaluation.
//   - story *bubuv1alpha1.Story: Story definition for streaming checks.
//   - steps []bubuv1alpha1.Step: all steps in the Story.
//   - stepStates map[string]runsv1alpha1.StepState: current step phases.
//   - completed, running map[string]bool: state buckets.
//   - dependencies map[string]map[string]bool: step → dependencies map.
//   - vars map[string]any: variable context for CEL.
//
// Returns:
//   - []*bubuv1alpha1.Step: steps ready to execute.
//   - []*bubuv1alpha1.Step: steps skipped due to false conditions.
func (r *DAGReconciler) findReadySteps(
	ctx context.Context,
	story *bubuv1alpha1.Story,
	steps []bubuv1alpha1.Step,
	stepStates map[string]runsv1alpha1.StepState,
	completed, running map[string]bool,
	dependencies map[string]map[string]bool,
	vars map[string]any,
) ([]*bubuv1alpha1.Step, []*bubuv1alpha1.Step) {
	var ready, skipped []*bubuv1alpha1.Step

	for i := range steps {
		step := &steps[i]
		if completed[step.Name] || running[step.Name] {
			continue
		}

		deps := dependencies[step.Name]
		allDepsMet := true
		for dep := range deps {
			if completed[dep] {
				continue
			}
			depState := stepStates[dep]
			if r.dependencySatisfiedForStreaming(story, depState) {
				continue
			}
			allDepsMet = false
			break
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
//
// Behavior:
//   - If no output template exists, marks StoryRun as Succeeded immediately.
//   - Collects inputs and prior step outputs for CEL evaluation.
//   - Unmarshals and evaluates the output template via CEL.
//   - Enforces a 1 MiB output size limit, setting Degraded condition if exceeded.
//   - Patches StoryRun with output, phase, conditions, and completion timestamps.
//
// Arguments:
//   - ctx context.Context: propagated to CEL and patch helpers.
//   - srun *runsv1alpha1.StoryRun: the StoryRun to finalize.
//   - story *bubuv1alpha1.Story: the Story definition with output template.
//
// Returns:
//   - error: nil on success, or evaluation/patch errors.
//
// Side Effects:
//   - Patches StoryRun status with output and completion fields.
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

	// BLOCKER MITIGATION: Prevent writing unbounded data to etcd.
	const maxOutputBytes = 1024 * 1024 // 1 MiB
	if len(outputBytes) > maxOutputBytes {
		log.Error(fmt.Errorf("resolved story output is too large"), "output size exceeds maximum limit", "size", len(outputBytes), "limit", maxOutputBytes)
		return kubeutil.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
			sr := obj.(*runsv1alpha1.StoryRun)
			cm := conditions.NewConditionManager(sr.Generation)
			reason := conditions.ReasonOutputResolutionFailed
			msg := fmt.Sprintf("Failed to record story output: size of %d bytes exceeds maximum allowed size of %d bytes. The story succeeded, but its final output could not be stored in the StoryRun status.", len(outputBytes), maxOutputBytes)
			cm.SetCondition(&sr.Status.Conditions, conditions.ConditionDegraded, metav1.ConditionTrue, reason, msg)

			// Still mark the story as Succeeded, but with a clear warning.
			sr.Status.Phase = enums.PhaseSucceeded
			sr.Status.Message = "All steps completed successfully, but the final output was too large to store."
			sr.Status.ObservedGeneration = sr.Generation
			if sr.Status.FinishedAt == nil {
				now := metav1.Now()
				sr.Status.FinishedAt = &now
				if sr.Status.StartedAt != nil {
					sr.Status.Duration = now.Sub(sr.Status.StartedAt.Time).Round(time.Second).String()
				}
			}
		})
	}

	// Patch the output into the status and set the final phase
	return kubeutil.RetryableStatusPatch(ctx, r.Client, srun, func(obj client.Object) {
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
				sr.Status.Duration = now.Sub(sr.Status.StartedAt.Time).Round(time.Second).String()
			}
		}

		cm := conditions.NewConditionManager(sr.Generation)
		cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonCompleted, "All steps completed successfully.")
	})
}

// buildDependencyGraphs builds adjacency maps for step dependencies.
//
// Behavior:
//   - Scans explicit "needs" dependencies.
//   - Scans implicit dependencies from "if" and "with" CEL expressions.
//   - Builds both forward (dependencies) and reverse (dependents) maps.
//   - Maps normalized aliases back to real step names.
//
// Arguments:
//   - steps []bubuv1alpha1.Step: all steps in the Story.
//
// Returns:
//   - map[string]map[string]bool: step → set of steps it depends on.
//   - map[string]map[string]bool: step → set of steps that depend on it.
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

// findAndAddDeps scans an expression for step references and records dependencies.
//
// Behavior:
//   - Uses regex to find "steps.name" or "steps['name']" patterns.
//   - Maps normalized aliases back to real step names.
//   - Calls addDependency for each found reference.
//
// Arguments:
//   - expression string: CEL expression to scan.
//   - stepName string: the step that has this expression (depends on others).
//   - regex *regexp.Regexp: pattern for matching step references.
//   - aliasMap map[string]string: normalized alias → real name.
//   - dependencies, dependents map[string]map[string]bool: adjacency maps to populate.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates dependencies and dependents maps.
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

// addDependency records a bidirectional dependency edge.
//
// Behavior:
//   - Adds depName to dependencies[stepName].
//   - Adds stepName to dependents[depName].
//   - Initializes dependents[depName] if nil.
//
// Arguments:
//   - stepName string: the dependent step.
//   - depName string: the step being depended on.
//   - dependencies, dependents map[string]map[string]bool: adjacency maps.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates both maps.
func addDependency(stepName, depName string, dependencies, dependents map[string]map[string]bool) {
	dependencies[stepName][depName] = true
	if dependents[depName] == nil {
		dependents[depName] = make(map[string]bool)
	}
	dependents[depName][stepName] = true
}

// buildStateMaps buckets StepStates into completed, running, and failed sets.
//
// Behavior:
//   - Succeeded and Skipped → completed.
//   - Running and Pending → running.
//   - Other terminal phases → failed.
//   - Ignores StepStates not present in the Story spec (loop template iterations, etc.).
//
// Arguments:
//   - story *bubuv1alpha1.Story: Story definition to scope step names.
//   - states map[string]runsv1alpha1.StepState: current step states.
//
// Returns:
//   - completed map[string]bool: steps that succeeded or were skipped.
//   - running map[string]bool: steps currently executing.
//   - failed map[string]bool: steps that failed terminally.
func buildStateMaps(story *bubuv1alpha1.Story, states map[string]runsv1alpha1.StepState) (completed, running, failed map[string]bool) {
	completed = make(map[string]bool)
	running = make(map[string]bool)
	failed = make(map[string]bool)

	allowed := make(map[string]struct{})
	if story != nil {
		for i := range story.Spec.Steps {
			allowed[story.Spec.Steps[i].Name] = struct{}{}
		}
	}

	for name, state := range states {
		if len(allowed) > 0 {
			if _, ok := allowed[name]; !ok {
				continue
			}
		}
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

// dependencySatisfiedForStreaming checks if a dependency is satisfied for streaming Stories.
//
// Behavior:
//   - Returns false for non-streaming Stories (batch mode requires completion).
//   - For streaming, Pending/Running/Paused deps are considered satisfied.
//   - Terminal deps are satisfied only if Succeeded.
//
// Arguments:
//   - story *bubuv1alpha1.Story: the Story definition.
//   - depState runsv1alpha1.StepState: the dependency step's current state.
//
// Returns:
//   - bool: true if the dependency is satisfied for streaming execution.
func (r *DAGReconciler) dependencySatisfiedForStreaming(story *bubuv1alpha1.Story, depState runsv1alpha1.StepState) bool {
	if story == nil || story.Spec.Pattern != enums.StreamingPattern {
		return false
	}
	if depState.Phase == "" {
		return false
	}
	if depState.Phase.IsTerminal() {
		return depState.Phase == enums.PhaseSucceeded
	}
	switch depState.Phase {
	case enums.PhasePending, enums.PhaseRunning, enums.PhasePaused:
		return true
	default:
		return false
	}
}

// getStoryRunInputs decodes StoryRun inputs via the shared runsinputs helper.
//
// Behavior:
//   - Delegates to runsinputs.DecodeStoryRunInputs.
//
// Arguments:
//   - srun *runsv1alpha1.StoryRun: the StoryRun with spec.inputs.
//
// Returns:
//   - map[string]any: the decoded inputs.
//   - error: nil on success, or decoding errors.
func getStoryRunInputs(srun *runsv1alpha1.StoryRun) (map[string]any, error) {
	return runsinputs.DecodeStoryRunInputs(srun)
}

// shouldFailFast checks if the Story should abort on first step failure.
//
// Behavior:
//   - Reads story.Spec.Policy.Retries.ContinueOnStepFailure.
//   - Returns true (fail fast) when unset or when ContinueOnStepFailure is false.
//
// Arguments:
//   - story *bubuv1alpha1.Story: the Story definition.
//
// Returns:
//   - bool: true if the Story should fail fast on step failures.
func (r *DAGReconciler) shouldFailFast(story *bubuv1alpha1.Story) bool {
	if story.Spec.Policy != nil &&
		story.Spec.Policy.Retries != nil &&
		story.Spec.Policy.Retries.ContinueOnStepFailure != nil {
		return !*story.Spec.Policy.Retries.ContinueOnStepFailure
	}
	return true // Default to failing fast
}

// setStoryRunPhase delegates to the shared status helper for consistent status semantics.
//
// Behavior:
//   - Calls runsstatus.PatchStoryRunPhase to update phase, message, and timestamps.
//
// Arguments:
//   - ctx context.Context: propagated to the patch helper.
//   - srun *runsv1alpha1.StoryRun: the StoryRun to update.
//   - phase enums.Phase: the target phase.
//   - message string: status message.
//
// Returns:
//   - error: nil on success, or patch errors.
//
// Side Effects:
//   - Updates StoryRun status subresource.
func (r *DAGReconciler) setStoryRunPhase(ctx context.Context, srun *runsv1alpha1.StoryRun, phase enums.Phase, message string) error {
	return runsstatus.PatchStoryRunPhase(ctx, r.Client, srun, phase, message)
}

// normalizeStepIdentifier replaces characters not allowed in CEL identifiers.
//
// Behavior:
//   - Delegates to sanitizeStepIdentifier for consistent aliasing.
//   - Replaces non-alphanumeric characters with underscores.
//
// Arguments:
//   - name string: the step name to normalize.
//
// Returns:
//   - string: the normalized CEL-safe identifier.
func normalizeStepIdentifier(name string) string {
	return sanitizeStepIdentifier(name)
}
