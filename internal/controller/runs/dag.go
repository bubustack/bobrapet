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
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/internal/setup"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	rec "github.com/bubustack/bobrapet/pkg/reconcile"
	runslist "github.com/bubustack/bobrapet/pkg/runs/list"
	runsstatus "github.com/bubustack/bobrapet/pkg/runs/status"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/bobrapet/pkg/templatesafety"
	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/templating"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DAGReconciler is responsible for the core workflow orchestration of a StoryRun.
type DAGReconciler struct {
	client.Client
	TemplateEvaluator *templating.Evaluator
	StepExecutor      *StepExecutor
	ConfigResolver    *config.Resolver
}

const sleepCompletedMsg = "Sleep completed."
const stepTimersAnnotationKey = "runs.bubustack.io/step-timers"
const stepTimersSoftLimitBytes = 200 * 1024

type stepTimerState struct {
	SleepUntil    *metav1.Time `json:"sleepUntil,omitempty"`
	WaitTimeoutAt *metav1.Time `json:"waitTimeoutAt,omitempty"`
	GateTimeoutAt *metav1.Time `json:"gateTimeoutAt,omitempty"`
}

type stepTimerStore struct {
	items map[string]stepTimerState
	dirty bool
}

type dagPipelineState struct {
	log             *logging.ControllerLogger
	story           *bubuv1alpha1.Story
	stepRunList     *runsv1alpha1.StepRunList
	priorOutputs    map[string]any
	readyCount      int
	skippedCount    int
	needsPersist    bool
	minPollInterval time.Duration
	timers          *stepTimerStore
}

type dagPhase string

const (
	dagPhaseMain         dagPhase = "main"
	dagPhaseCompensation dagPhase = "compensation"
	dagPhaseFinally      dagPhase = "finally"
)

type stepDependencyPolicy struct {
	allowFailedDependencies bool
	skipOnFailedDependency  bool
}

const (
	concurrencyQueuedMessagePrefix       = "Queued due to story concurrency limit"
	queueConcurrencyQueuedMessagePrefix  = "Queued due to queue concurrency limit"
	globalConcurrencyQueuedMessagePrefix = "Queued due to global concurrency limit"
	priorityQueuedMessagePrefix          = "Queued due to higher-priority work"
)

func loadStepTimers(srun *runsv1alpha1.StoryRun, log *logging.ControllerLogger) *stepTimerStore {
	store := &stepTimerStore{items: map[string]stepTimerState{}}
	if srun == nil {
		return store
	}
	raw := strings.TrimSpace(srun.GetAnnotations()[stepTimersAnnotationKey])
	if raw == "" {
		return store
	}
	if err := json.Unmarshal([]byte(raw), &store.items); err != nil {
		if log != nil {
			log.Info("Failed to parse step timer annotation; ignoring", "error", err.Error())
		}
		store.items = map[string]stepTimerState{}
	}
	return store
}

func (s *stepTimerStore) ensureSleepUntil(step string, startedAt time.Time, duration time.Duration) *metav1.Time {
	if s == nil || duration <= 0 {
		return nil
	}
	state := s.items[step]
	if state.SleepUntil != nil && !state.SleepUntil.IsZero() {
		return state.SleepUntil
	}
	until := metav1.NewTime(startedAt.Add(duration))
	state.SleepUntil = &until
	s.items[step] = state
	s.dirty = true
	return state.SleepUntil
}

func (s *stepTimerStore) ensureWaitTimeoutAt(step string, startedAt time.Time, timeout time.Duration) *metav1.Time {
	if s == nil || timeout <= 0 {
		return nil
	}
	state := s.items[step]
	if state.WaitTimeoutAt != nil && !state.WaitTimeoutAt.IsZero() {
		return state.WaitTimeoutAt
	}
	until := metav1.NewTime(startedAt.Add(timeout))
	state.WaitTimeoutAt = &until
	s.items[step] = state
	s.dirty = true
	return state.WaitTimeoutAt
}

func (s *stepTimerStore) ensureGateTimeoutAt(step string, startedAt time.Time, timeout time.Duration) *metav1.Time {
	if s == nil || timeout <= 0 {
		return nil
	}
	state := s.items[step]
	if state.GateTimeoutAt != nil && !state.GateTimeoutAt.IsZero() {
		return state.GateTimeoutAt
	}
	until := metav1.NewTime(startedAt.Add(timeout))
	state.GateTimeoutAt = &until
	s.items[step] = state
	s.dirty = true
	return state.GateTimeoutAt
}

func (s *stepTimerStore) pruneTerminal(srun *runsv1alpha1.StoryRun) {
	if s == nil || srun == nil || len(s.items) == 0 {
		return
	}
	if len(srun.Status.StepStates) == 0 {
		return
	}
	removed := false
	for step := range s.items {
		state, ok := srun.Status.StepStates[step]
		if ok && state.Phase.IsTerminal() {
			delete(s.items, step)
			removed = true
		}
	}
	if removed {
		s.dirty = true
	}
}

func (d *dagPipelineState) recordActivity(ready, skipped int) {
	d.readyCount += ready
	d.skippedCount += skipped
	if ready > 0 || skipped > 0 {
		d.needsPersist = true
	}
}

func (d *dagPipelineState) setPollInterval(interval time.Duration) {
	if d == nil || interval <= 0 {
		return
	}
	if d.minPollInterval == 0 || interval < d.minPollInterval {
		d.minPollInterval = interval
	}
}

// DAGReconcilerDeps bundles the collaborators required to construct a DAGReconciler.
type DAGReconcilerDeps struct {
	Client            client.Client
	TemplateEvaluator *templating.Evaluator
	StepExecutor      *StepExecutor
	ConfigResolver    *config.Resolver
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
	if d.TemplateEvaluator == nil {
		missing = append(missing, "template evaluator")
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
// Template evaluator, StepExecutor, and config resolver supplied by SetupWithManager
// (internal/controller/runs/dag.go:47-63; internal/controller/runs/storyrun_controller.go:536-549).
// Behavior:
//   - Reuses the manager client so DAG helpers can List/Get StepRuns via the cache.
//   - Shares the StepExecutor and template evaluator with StoryRun reconciliation to keep
//     execution semantics aligned.
//
// Arguments:
//   - deps DAGReconcilerDeps: groups the cache-aware client, template evaluator, StepExecutor, and config resolver.
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
		Client:            deps.Client,
		TemplateEvaluator: deps.TemplateEvaluator,
		StepExecutor:      deps.StepExecutor,
		ConfigResolver:    deps.ConfigResolver,
	}
}

// Reconcile orchestrates the execution of the StoryRun's DAG.
//
// Behavior:
//   - Initializes StepStates map if nil.
//   - Syncs StepRun phases into StoryRun.Status.StepStates.
//   - Collects prior step outputs for template evaluation.
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
//
//nolint:gocyclo // complex by design
func (r *DAGReconciler) Reconcile(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) (ctrl.Result, error) {
	ctx = withStoryMaxRecursionDepth(ctx, r.ConfigResolver, story)
	hooks := rec.PipelineHooks[*runsv1alpha1.StoryRun, *dagPipelineState]{
		Prepare: func(ctx context.Context, sr *runsv1alpha1.StoryRun) (*dagPipelineState, *ctrl.Result, error) {
			state := &dagPipelineState{
				log:   logging.NewReconcileLogger(ctx, "storyrun-dag").WithValues("storyrun", sr.Name),
				story: story,
			}

			ensureStepStatesInitialized(sr)
			state.timers = loadStepTimers(sr, state.log)

			list, syncChanged, err := r.syncStateFromStepRuns(ctx, sr)
			if err != nil {
				return nil, nil, err
			}
			if syncChanged {
				state.needsPersist = true
			}
			state.log.V(1).Info("Synced StepRuns", "count", len(list.Items))
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
			if state != nil && state.timers != nil {
				if err := r.persistStepTimers(ctx, sr, state.timers, state.log); err != nil {
					state.log.Error(err, "Failed to patch StoryRun step timers")
					return result, err
				}
			}

			if sr.Status.Phase == enums.PhaseRunning {
				if err := r.updateRunningDuration(ctx, sr); err != nil && state != nil && state.log != nil {
					state.log.Error(err, "Failed to update running duration")
				}
				requeue := r.runningRequeueDelay()
				if state != nil && state.minPollInterval > 0 {
					requeue = max(state.minPollInterval, config.MinRequeueDelay)
				}
				result.RequeueAfter = requeue
			}

			return result, nil
		},
	}

	return rec.RunPipeline(ctx, srun, hooks)
}

//nolint:gocyclo // complex by design
func (r *DAGReconciler) runDagIterations(ctx context.Context, srun *runsv1alpha1.StoryRun, state *dagPipelineState) error {
	if state == nil || state.story == nil {
		return fmt.Errorf("dag pipeline state is not initialized")
	}

	allSteps := allStorySteps(state.story)
	if graphErr := validateRuntimeDependencyGraph(allSteps); graphErr != nil {
		message := fmt.Sprintf("Invalid story dependency graph: %v", graphErr)
		statusErr := runsstatus.BuildStoryRunError(conditions.ReasonInvalidConfiguration, message, nil, srun.Status.StepStates)
		srun.Status.Error = statusErr
		return r.setStoryRunPhaseWithError(ctx, srun, enums.PhaseFailed, conditions.ReasonInvalidConfiguration, message, statusErr)
	}
	for i := 0; i < len(allSteps)+1; i++ {
		if timedOut, err := r.enforceStoryTimeout(ctx, srun, state.story, state.log); err != nil {
			return err
		} else if timedOut {
			return nil
		}
		var err error
		var subStoriesUpdated bool
		state.stepRunList, state.priorOutputs, subStoriesUpdated, err = r.refreshAfterSubStoriesIfNeeded(ctx, srun, allSteps, state.stepRunList, state.priorOutputs)
		if err != nil {
			return err
		}
		if subStoriesUpdated {
			state.needsPersist = true
		}

		if r.checkSyncGates(ctx, srun, state.story, allSteps, state) {
			state.needsPersist = true
		}
		if r.checkSyncSleepSteps(ctx, srun, state.story, allSteps, state) {
			state.needsPersist = true
		}
		if r.checkSyncWaitSteps(ctx, srun, state.story, allSteps, state) {
			state.needsPersist = true
		}
		if r.checkSyncParallelSteps(ctx, srun, allSteps, state.stepRunList) {
			state.needsPersist = true
		}

		mainCompleted, mainRunning, mainFailed, _ := buildStateMaps(state.story.Spec.Steps, srun.Status.StepStates)
		clearConcurrencyQueuedSteps(mainRunning, srun.Status.StepStates)
		if r.shouldFailFast(state.story) && len(mainFailed) > 0 {
			if markFailFastSkipped(srun, state.story, mainCompleted, mainRunning) {
				state.needsPersist = true
			}
			mainCompleted, mainRunning, mainFailed, _ = buildStateMaps(state.story.Spec.Steps, srun.Status.StepStates)
			clearConcurrencyQueuedSteps(mainRunning, srun.Status.StepStates)
		}
		mainDone := stepsTerminal(len(state.story.Spec.Steps), mainCompleted, mainFailed)

		// For realtime stories, topology termination (Degraded condition) means
		// main phase is done — realtime steps run indefinitely until the hub signals
		// all have disconnected.
		if !mainDone && state.story.Spec.Pattern.IsRealtime() {
			if conditions.IsDegraded(srun.Status.Conditions) {
				for _, c := range srun.Status.Conditions {
					if c.Type == conditions.ConditionDegraded &&
						c.Status == metav1.ConditionTrue &&
						c.Reason == conditions.ReasonTopologyTerminated {
						mainDone = true
						// Mark all non-terminal streaming main steps as Failed so
						// compensation/finally phases can evaluate them.
						for i := range state.story.Spec.Steps {
							step := &state.story.Spec.Steps[i]
							if stepState, ok := srun.Status.StepStates[step.Name]; ok {
								if !stepState.Phase.IsTerminal() {
									stepState.Phase = enums.PhaseFailed
									stepState.Message = "realtime topology terminated" //nolint:goconst
									srun.Status.StepStates[step.Name] = stepState
									mainFailed[step.Name] = true
									state.needsPersist = true
								}
							}
						}
						// Rebuild state maps after marking failures.
						_, mainRunning, mainFailed, _ = buildStateMaps(state.story.Spec.Steps, srun.Status.StepStates)
						clearConcurrencyQueuedSteps(mainRunning, srun.Status.StepStates)
						break
					}
				}
			}
		}

		if mainDone && len(mainFailed) == 0 && len(state.story.Spec.Compensations) > 0 {
			if markCompensationsSkipped(srun, state.story) {
				state.needsPersist = true
			}
		}

		compCompleted, _, compFailed, _ := buildStateMaps(state.story.Spec.Compensations, srun.Status.StepStates)
		finalCompleted, _, finalFailed, _ := buildStateMaps(state.story.Spec.Finally, srun.Status.StepStates)

		compDone := stepsTerminal(len(state.story.Spec.Compensations), compCompleted, compFailed)
		finalDone := stepsTerminal(len(state.story.Spec.Finally), finalCompleted, finalFailed)

		if r.syncAllowedFailures(srun, allSteps) {
			state.needsPersist = true
		}

		var phase dagPhase
		switch {
		case !mainDone:
			phase = dagPhaseMain
		case len(mainFailed) > 0 && len(state.story.Spec.Compensations) > 0 && !compDone:
			phase = dagPhaseCompensation
		case len(state.story.Spec.Finally) > 0 && !finalDone:
			phase = dagPhaseFinally
		default:
			if err := r.finalizeStoryRun(ctx, srun, state.story, mainFailed, compFailed, finalFailed, state.log); err != nil {
				return err
			}
			return nil
		}

		completed, running, _, _ := buildStateMaps(allSteps, srun.Status.StepStates)
		clearConcurrencyQueuedSteps(running, srun.Status.StepStates)
		depPolicy := stepDependencyPolicy{
			allowFailedDependencies: phase != dagPhaseMain,
			skipOnFailedDependency:  phase == dagPhaseMain && !r.shouldFailFast(state.story),
		}
		var stepList []bubuv1alpha1.Step
		switch phase {
		case dagPhaseCompensation:
			stepList = state.story.Spec.Compensations
		case dagPhaseFinally:
			stepList = state.story.Spec.Finally
		default:
			stepList = state.story.Spec.Steps
		}

		// Refresh step list and prior outputs before evaluating readiness so step "if"
		// conditions see the latest completed step outputs (e.g. fetch-feed.body).
		syncChanged := false
		state.stepRunList, syncChanged, err = r.syncStateFromStepRuns(ctx, srun)
		if err != nil {
			return err
		}
		if syncChanged {
			state.needsPersist = true
		}
		state.priorOutputs, err = getPriorStepOutputs(ctx, r.Client, srun, state.stepRunList)
		if err != nil {
			return fmt.Errorf("failed to get prior step outputs: %w", err)
		}

		ready, skipped, queued, err := r.findAndLaunchReadySteps(ctx, srun, state.story, stepList, completed, running, state.priorOutputs, depPolicy, &state.needsPersist)
		if err != nil {
			return err
		}
		state.recordActivity(len(ready), len(skipped))
		if queued > 0 {
			state.needsPersist = true
		}

		if len(ready) == 0 && len(skipped) == 0 {
			break
		}
	}
	return nil
}

func (r *DAGReconciler) enforceStoryTimeout(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, log *logging.ControllerLogger) (bool, error) {
	if srun == nil || story == nil || story.Spec.Policy == nil || story.Spec.Policy.Timeouts == nil || story.Spec.Policy.Timeouts.Story == nil {
		return false, nil
	}
	raw := strings.TrimSpace(*story.Spec.Policy.Timeouts.Story)
	if raw == "" {
		return false, nil
	}
	if srun.Status.StartedAt == nil {
		return false, nil
	}
	timeout, err := parsePositiveDuration(raw)
	if err != nil {
		if log != nil {
			log.Error(err, "Invalid story timeout; ignoring", "timeout", raw)
		}
		return false, nil
	}
	elapsed := time.Since(srun.Status.StartedAt.Time)
	if elapsed < timeout {
		return false, nil
	}
	message := fmt.Sprintf("StoryRun exceeded timeout of %s.", timeout.String())
	srun.Status.Phase = enums.PhaseTimeout
	srun.Status.Message = message
	statusErr := runsstatus.BuildStoryRunError(conditions.ReasonTimedOut, message, nil, srun.Status.StepStates)
	srun.Status.Error = statusErr
	if err := r.setStoryRunPhaseWithError(ctx, srun, enums.PhaseTimeout, conditions.ReasonTimedOut, message, statusErr); err != nil {
		return true, err
	}
	if log != nil {
		log.Info("StoryRun timed out", "timeout", timeout.String(), "elapsed", elapsed.String())
	}
	return true, nil
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
//   - srun/steps: identify the StoryRun and its steps so executeStory steps can be examined.
//   - stepRunList/prior: cached data to return when no refresh is required.
//
// Returns:
//   - Updated (*StepRunList, map[string]any) when sub-stories changed, or the originals otherwise.
//   - bool indicating whether checkSyncSubStories changed any StepState and callers should persist status.
//   - error when either the StepRun re-sync or output rebuild fails.
//
// Side Effects:
//   - checkSyncSubStories may mutate Status.StepStates for executeStory steps.
func (r *DAGReconciler) refreshAfterSubStoriesIfNeeded(ctx context.Context, srun *runsv1alpha1.StoryRun, steps []bubuv1alpha1.Step, stepRunList *runsv1alpha1.StepRunList, prior map[string]any) (*runsv1alpha1.StepRunList, map[string]any, bool, error) {
	if updated := r.checkSyncSubStories(ctx, srun, steps); !updated {
		return stepRunList, prior, false, nil
	}
	metrics.RecordSubStoryRefresh("storyrun-dag")
	lst, _, err := r.syncStateFromStepRuns(ctx, srun)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to re-sync step runs after sub-story sync: %w", err)
	}
	out, err := getPriorStepOutputs(ctx, r.Client, srun, lst)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to re-fetch prior step outputs after sub-story sync: %w", err)
	}
	return lst, out, true, nil
}

// checkCompletionOrFailure inspects the completed/failed step maps and either
// triggers fail-fast handling or finalizes the StoryRun when all steps are done
// (internal/controller/runs/dag.go:89-181).
// Behavior:
//   - If any steps failed and the Story's fail-fast policy is true, it sets the
//     StoryRun phase to Failed and stops the loop.
//   - If every step is terminal, it finalizes the StoryRun. Failed steps yield a Failed
//     StoryRun (unless fail-fast already handled it).
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
func (r *DAGReconciler) finalizeStoryRun(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	mainFailed, compFailed, finalFailed map[string]bool,
	log *logging.ControllerLogger,
) error {
	switch {
	case len(finalFailed) > 0:
		stepName := firstFailedStepName(finalFailed)
		message := fmt.Sprintf("Finally step %s failed.", stepName)
		statusErr := runsstatus.BuildStoryRunError(conditions.ReasonCleanupFailed, message, finalFailed, srun.Status.StepStates)
		srun.Status.Error = statusErr
		return r.setStoryRunPhaseWithError(ctx, srun, enums.PhaseFailed, conditions.ReasonCleanupFailed, message, statusErr)
	case len(compFailed) > 0:
		stepName := firstFailedStepName(compFailed)
		message := fmt.Sprintf("Compensation step %s failed.", stepName)
		statusErr := runsstatus.BuildStoryRunError(conditions.ReasonCompensationFailed, message, compFailed, srun.Status.StepStates)
		srun.Status.Error = statusErr
		return r.setStoryRunPhaseWithError(ctx, srun, enums.PhaseFailed, conditions.ReasonCompensationFailed, message, statusErr)
	case len(mainFailed) > 0:
		if len(story.Spec.Compensations) > 0 {
			message := "Story failed; compensations completed."
			statusErr := runsstatus.BuildStoryRunError(conditions.ReasonCompensated, message, mainFailed, srun.Status.StepStates)
			srun.Status.Error = statusErr
			return r.setStoryRunPhaseWithError(ctx, srun, enums.PhaseCompensated, conditions.ReasonCompensated, message, statusErr)
		}
		stepName := firstFailedStepName(mainFailed)
		message := fmt.Sprintf("Story completed with failed step: %s.", stepName)
		statusErr := runsstatus.BuildStoryRunError(conditions.ReasonDependencyFailed, message, mainFailed, srun.Status.StepStates)
		srun.Status.Error = statusErr
		return r.setStoryRunPhaseWithError(ctx, srun, enums.PhaseFailed, conditions.ReasonDependencyFailed, message, statusErr)
	default:
		if err := r.finalizeSuccessfulRun(ctx, srun, story); err != nil {
			var blocked *templating.ErrEvaluationBlocked
			if errors.As(err, &blocked) {
				if log != nil {
					log.V(1).Info("Story output evaluation blocked; waiting for materialize", "reason", blocked.Reason)
				}
				return err
			}
			var offloaded *templating.ErrOffloadedDataUsage
			if errors.As(err, &offloaded) {
				policy := resolveOffloadedPolicy(r.ConfigResolver)
				if shouldBlockOffloaded(policy) || shouldResolveAllOffloaded(policy) {
					if log != nil {
						log.Info("Story output requires offloaded data; waiting for hydration", "reason", offloaded.Reason)
					}
					return err
				}
			}
			if log != nil {
				log.Error(err, "Failed to finalize successful story run")
			}
			statusErr := runsstatus.BuildStoryRunError(conditions.ReasonOutputResolutionFailed, err.Error(), nil, srun.Status.StepStates)
			srun.Status.Error = statusErr
			_ = r.setStoryRunPhaseWithError(ctx, srun, enums.PhaseFailed, conditions.ReasonOutputResolutionFailed, "Failed to evaluate final output template.", statusErr)
			return err
		}
		return nil
	}
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
			sr.Status.StepStates[k] = mergeStepState(existingState, v)
		}
		r.updateStepProgress(sr)
	})
}

func (r *DAGReconciler) persistStepTimers(ctx context.Context, srun *runsv1alpha1.StoryRun, store *stepTimerStore, log *logging.ControllerLogger) error {
	if srun == nil || store == nil {
		return nil
	}
	store.pruneTerminal(srun)
	if !store.dirty {
		return nil
	}
	if len(store.items) == 0 {
		return r.patchStoryRunAnnotations(ctx, srun, func(ann map[string]string) (bool, error) {
			if _, ok := ann[stepTimersAnnotationKey]; !ok {
				return false, nil
			}
			delete(ann, stepTimersAnnotationKey)
			return true, nil
		})
	}
	raw, err := json.Marshal(store.items)
	if err != nil {
		return err
	}
	payload := string(raw)
	if log != nil && len(payload) >= stepTimersSoftLimitBytes {
		log.Info("Step timer annotation size approaching metadata limits",
			"sizeBytes", len(payload),
			"limitBytes", stepTimersSoftLimitBytes)
	}
	return r.patchStoryRunAnnotations(ctx, srun, func(ann map[string]string) (bool, error) {
		if ann[stepTimersAnnotationKey] == payload {
			return false, nil
		}
		ann[stepTimersAnnotationKey] = payload
		return true, nil
	})
}

func (r *DAGReconciler) patchStoryRunAnnotations(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	update func(map[string]string) (bool, error),
) error {
	if srun == nil {
		return nil
	}
	key := client.ObjectKeyFromObject(srun)
	return retry.OnError(retry.DefaultRetry, apierrors.IsConflict, func() error {
		current := &runsv1alpha1.StoryRun{}
		if err := r.Get(ctx, key, current); err != nil {
			return err
		}
		original := current.DeepCopy()
		annotations := current.GetAnnotations()
		if annotations == nil {
			annotations = map[string]string{}
		}
		changed, err := update(annotations)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		current.SetAnnotations(annotations)
		return r.Patch(ctx, current, client.MergeFrom(original))
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
		case enums.PhaseRunning, enums.PhasePaused:
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
//   - bool: true when the in-memory StoryRun StepStates changed.
//   - error: nil on success, or the list error.
//
// Side Effects:
//   - Mutates srun.Status.StepStates in memory.
//   - Logs phase transitions.
func (r *DAGReconciler) syncStateFromStepRuns(ctx context.Context, srun *runsv1alpha1.StoryRun) (*runsv1alpha1.StepRunList, bool, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-sync")
	ensureStepStatesInitialized(srun)
	var stepRunList runsv1alpha1.StepRunList
	if err := runslist.StepRunsByStoryRun(ctx, r.Client, srun.Namespace, srun.Name, nil, &stepRunList); err != nil {
		log.Error(err, "Failed to list StepRuns")
		return nil, false, err
	}
	updated := false

	for _, sr := range stepRunList.Items {
		stepID := sr.Spec.StepID
		currentState, exists := srun.Status.StepStates[stepID]

		// Skip if StepRun phase is empty (invalid/initializing state)
		// Empty phases fail Kubernetes validation and cause reconciliation storms
		if sr.Status.Phase == "" {
			continue
		}

		phaseChanged := !exists || currentState.Phase != sr.Status.Phase
		timeChanged := (currentState.StartedAt == nil && sr.Status.StartedAt != nil) ||
			(currentState.FinishedAt == nil && sr.Status.FinishedAt != nil)
		messageChanged := sr.Status.LastFailureMsg != "" && sr.Status.LastFailureMsg != currentState.Message

		if phaseChanged || timeChanged || messageChanged {
			oldPhase := enums.Phase("")
			if exists {
				oldPhase = currentState.Phase
			}
			if phaseChanged {
				log.Info("Syncing StepRun status to StoryRun", "step", stepID, "oldPhase", oldPhase, "newPhase", sr.Status.Phase)
			}
			desired := runsv1alpha1.StepState{
				Phase:      sr.Status.Phase,
				Message:    sr.Status.LastFailureMsg,
				StartedAt:  sr.Status.StartedAt,
				FinishedAt: sr.Status.FinishedAt,
			}
			srun.Status.StepStates[stepID] = mergeStepState(currentState, desired)
			updated = true
		}
	}
	return &stepRunList, updated, nil
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
//   - steps []bubuv1alpha1.Step: steps to inspect (may include cleanup steps).
//
// Returns:
//   - bool: true if any StepState was updated (caller should refresh).
//
// Side Effects:
//   - Mutates srun.Status.StepStates in memory.
//   - Logs errors and completions.
func (r *DAGReconciler) checkSyncSubStories(ctx context.Context, srun *runsv1alpha1.StoryRun, steps []bubuv1alpha1.Step) bool {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-substory")
	var statusUpdated bool
	for i := range steps {
		step := &steps[i]
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
				state := runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: "invalid 'with' block"}
				state = ensureStepStateTimes(state, metav1.Now())
				srun.Status.StepStates[step.Name] = state
				statusUpdated = true
				continue
			}

			if !withConfig.WaitForCompletion {
				continue
			}

			childName := strings.TrimSpace(state.SubStoryRunName)
			if childName == "" {
				log.Info("Waiting for executeStory child StoryRun assignment", "step", step.Name)
				continue
			}

			subRun := &runsv1alpha1.StoryRun{}
			subRunKey := types.NamespacedName{Name: childName, Namespace: srun.Namespace}
			err := r.Get(ctx, subRunKey, subRun)
			if err != nil {
				if apierrors.IsNotFound(err) {
					log.Info("Waiting for executeStory child StoryRun to appear", "step", step.Name, "childStoryRun", childName)
				} else {
					log.Error(err, "Failed to fetch executeStory child StoryRun, will retry", "step", step.Name, "childStoryRun", childName)
				}
				continue
			}

			if subRun.Status.Phase.IsTerminal() {
				log.Info("Sub-StoryRun completed", "step", step.Name, "subPhase", subRun.Status.Phase)
				newState := runsv1alpha1.StepState{Phase: subRun.Status.Phase, Message: subRun.Status.Message}
				newState = ensureStepStateTimes(newState, metav1.Now())
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

type waitConfig struct {
	until        string
	timeout      *time.Duration
	pollInterval time.Duration
	onTimeout    string
}

type sleepConfig struct {
	duration time.Duration
}

type gateConfig struct {
	timeout      *time.Duration
	pollInterval time.Duration
	onTimeout    string
}

//nolint:gocyclo // complex by design
func (r *DAGReconciler) checkSyncParallelSteps(ctx context.Context, srun *runsv1alpha1.StoryRun, steps []bubuv1alpha1.Step, stepRuns *runsv1alpha1.StepRunList) bool {
	if srun == nil || len(steps) == 0 || stepRuns == nil {
		return false
	}
	if len(srun.Status.PrimitiveChildren) == 0 {
		return false
	}
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-parallel")
	ensureStepStatesInitialized(srun)

	stepRunByName := make(map[string]*runsv1alpha1.StepRun, len(stepRuns.Items))
	for i := range stepRuns.Items {
		sr := &stepRuns.Items[i]
		stepRunByName[sr.Name] = sr
	}

	updated := false
	now := metav1.Now()

	for i := range steps {
		step := &steps[i]
		if step.Type != enums.StepTypeParallel {
			continue
		}
		state, exists := srun.Status.StepStates[step.Name]
		if !exists || state.Phase.IsTerminal() {
			continue
		}
		childNames := srun.Status.PrimitiveChildren[step.Name]
		if len(childNames) == 0 {
			continue
		}

		allowFailure := map[string]bool{}
		if step.With != nil && len(step.With.Raw) > 0 {
			branches, err := parseParallelBranches(step)
			if err != nil {
				log.Error(err, "Failed to parse parallel branches", "step", step.Name)
			} else {
				for i := range branches {
					child := &branches[i]
					if child.AllowFailure != nil && *child.AllowFailure {
						allowFailure[child.Name] = true
					}
				}
			}
		}

		allDone := true
		var failedBranches []string
		allowedFailures := 0
		for _, childName := range childNames {
			child := stepRunByName[childName]
			if child == nil || child.Status.Phase == "" || !child.Status.Phase.IsTerminal() {
				allDone = false
				continue
			}
			if child.Status.Phase == enums.PhaseSucceeded || child.Status.Phase == enums.PhaseSkipped {
				continue
			}
			if allowFailure[child.Spec.StepID] {
				allowedFailures++
				continue
			}
			failedBranches = append(failedBranches, child.Spec.StepID)
		}

		if !allDone {
			continue
		}

		next := state
		switch {
		case len(failedBranches) > 0:
			next.Phase = enums.PhaseFailed
			next.Message = fmt.Sprintf("Parallel branches failed: %s", strings.Join(failedBranches, ", "))
		case allowedFailures > 0:
			next.Phase = enums.PhaseSucceeded
			next.Message = "Parallel branches completed with allowed failures."
		default:
			next.Phase = enums.PhaseSucceeded
			next.Message = "Parallel branches completed."
		}
		next = ensureStepStateTimes(next, now)
		srun.Status.StepStates[step.Name] = next
		updated = true
	}
	return updated
}

func parseParallelBranches(step *bubuv1alpha1.Step) ([]bubuv1alpha1.Step, error) {
	if step == nil || step.With == nil || len(step.With.Raw) == 0 {
		return nil, fmt.Errorf("parallel step '%s' missing 'with' configuration", step.Name)
	}
	type parallelWith struct {
		Steps []bubuv1alpha1.Step `json:"steps"`
	}
	var cfg parallelWith
	if err := json.Unmarshal(step.With.Raw, &cfg); err != nil {
		return nil, fmt.Errorf("parallel step '%s' invalid 'with' block: %w", step.Name, err)
	}
	return cfg.Steps, nil
}

//nolint:gocyclo // complex by design
func (r *DAGReconciler) checkSyncSleepSteps(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, steps []bubuv1alpha1.Step, state *dagPipelineState) bool {
	if srun == nil || story == nil {
		return false
	}
	ensureStepStatesInitialized(srun)
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-sleep")
	now := metav1.Now()
	updated := false

	for i := range steps {
		step := &steps[i]
		if step.Type != enums.StepTypeSleep {
			continue
		}
		current, exists := srun.Status.StepStates[step.Name]
		if !exists || current.Phase.IsTerminal() {
			continue
		}
		if current.Phase != enums.PhasePaused && current.Phase != enums.PhaseRunning && current.Phase != enums.PhasePending {
			continue
		}

		cfg, cfgErr := parseSleepConfig(step)
		if cfgErr != nil {
			log.Error(cfgErr, "Invalid sleep step configuration", "step", step.Name)
			next := runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: cfgErr.Error()}
			next = ensureStepStateTimes(next, now)
			if next != current {
				srun.Status.StepStates[step.Name] = next
				updated = true
			}
			continue
		}

		next := current
		if cfg.duration <= 0 {
			next.Phase = enums.PhaseSucceeded
			next.Message = sleepCompletedMsg
		} else {
			if next.StartedAt == nil {
				next.StartedAt = &now
			}
			sleepUntil := next.StartedAt.Add(cfg.duration)
			if state != nil && state.timers != nil {
				if stored := state.timers.ensureSleepUntil(step.Name, next.StartedAt.Time, cfg.duration); stored != nil {
					sleepUntil = stored.Time
				}
			}
			remaining := sleepUntil.Sub(now.Time)
			if remaining <= 0 {
				next.Phase = enums.PhaseSucceeded
				next.Message = sleepCompletedMsg
			} else {
				next.Phase = enums.PhasePaused
				if next.Message == "" || next.Message == sleepCompletedMsg {
					next.Message = fmt.Sprintf("Sleeping for %s.", cfg.duration.String())
				}
				if state != nil {
					state.setPollInterval(remaining)
				}
			}
		}

		next = ensureStepStateTimes(next, now)
		if next != current {
			srun.Status.StepStates[step.Name] = next
			updated = true
		}
	}

	return updated
}

//nolint:gocyclo // complex by design
func (r *DAGReconciler) checkSyncWaitSteps(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, steps []bubuv1alpha1.Step, state *dagPipelineState) bool {
	if srun == nil || story == nil {
		return false
	}
	if r.TemplateEvaluator == nil {
		return false
	}
	ensureStepStatesInitialized(srun)
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-wait")

	inputs, err := getStoryRunInputs(ctx, srun, story)
	if err != nil {
		log.Error(err, "Failed to decode StoryRun inputs for wait evaluation")
		return false
	}
	prior := map[string]any{}
	if state != nil && state.priorOutputs != nil {
		prior = state.priorOutputs
	}
	vars := map[string]any{
		"inputs": inputs,
		"steps":  prior,
	}

	updated := false
	now := metav1.Now()

	for i := range steps {
		step := &steps[i]
		if step.Type != enums.StepTypeWait {
			continue
		}
		current, exists := srun.Status.StepStates[step.Name]
		if !exists || current.Phase.IsTerminal() {
			continue
		}
		if current.Phase != enums.PhasePaused && current.Phase != enums.PhaseRunning && current.Phase != enums.PhasePending {
			continue
		}

		cfg, cfgErr := parseWaitConfig(step)
		if cfgErr != nil {
			log.Error(cfgErr, "Invalid wait step configuration", "step", step.Name)
			next := runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: cfgErr.Error()}
			next = ensureStepStateTimes(next, now)
			if next != current {
				srun.Status.StepStates[step.Name] = next
				updated = true
			}
			continue
		}
		if err := templatesafety.ValidateTemplateString(cfg.until); err != nil {
			next := runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: err.Error()}
			next = ensureStepStateTimes(next, now)
			if next != current {
				srun.Status.StepStates[step.Name] = next
				updated = true
			}
			continue
		}

		result := false
		var evalErr error
		if offloaded := detectOffloadedOutputRefs(cfg.until, prior); offloaded != nil {
			policy := resolveOffloadedPolicy(r.ConfigResolver)
			resolveAll := shouldResolveAllOffloaded(policy) ||
				(story != nil && storyForcesControllerResolve(story.GetAnnotations()))
			if resolveAll {
				result, evalErr = resolveConditionInProcess(ctx, r.TemplateEvaluator, cfg.until, vars)
			} else if shouldBlockOffloaded(policy) {
				materialized, matErr := resolveMaterialize(ctx, r.Client, r.Scheme(), r.ConfigResolver, srun, materializePurposeWait, step.Name, "", materializeModeCondition, cfg.until, vars)
				if matErr != nil {
					evalErr = matErr
				} else if boolVal, ok := materialized.(bool); ok {
					result = boolVal
				} else {
					evalErr = fmt.Errorf("materialize result must be bool, got %T", materialized)
				}
			} else {
				evalErr = offloaded
			}
		} else {
			result, evalErr = r.TemplateEvaluator.EvaluateCondition(ctx, cfg.until, vars)
		}
		if evalErr != nil {
			var blocked *templating.ErrEvaluationBlocked
			if errors.As(evalErr, &blocked) {
				log.Info("Wait condition blocked; waiting for materialize", "step", step.Name, "reason", blocked.Reason)
				result = false
				evalErr = nil
			}
		}
		if evalErr != nil {
			var offloaded *templating.ErrOffloadedDataUsage
			if errors.As(evalErr, &offloaded) {
				policy := resolveOffloadedPolicy(r.ConfigResolver)
				resolveAll := shouldResolveAllOffloaded(policy) ||
					(story != nil && storyForcesControllerResolve(story.GetAnnotations()))
				if resolveAll {
					result, evalErr = resolveConditionInProcess(ctx, r.TemplateEvaluator, cfg.until, vars)
				} else if shouldBlockOffloaded(policy) {
					log.Info("Wait condition requires offloaded data; waiting for hydration", "step", step.Name, "reason", offloaded.Reason)
					result = false
					evalErr = nil
				} else {
					next := runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: offloaded.Error()}
					next = ensureStepStateTimes(next, now)
					if next != current {
						srun.Status.StepStates[step.Name] = next
						updated = true
					}
					continue
				}
			}
			if evalErr != nil {
				log.Info("Wait condition blocked or failed evaluation, step not ready", "step", step.Name, "reason", evalErr)
				result = false
			}
		}

		next := current
		if result {
			next.Phase = enums.PhaseSucceeded
			next.Message = "Wait condition satisfied."
		} else {
			next.Phase = enums.PhasePaused
			if next.Message == "" || next.Message == "Wait condition satisfied." {
				next.Message = "Waiting for condition."
			}
		}

		waiting := !result
		if waiting {
			if cfg.timeout != nil {
				if next.StartedAt == nil {
					next.StartedAt = &now
				}
				timeoutAt := next.StartedAt.Add(*cfg.timeout)
				if state != nil && state.timers != nil {
					if stored := state.timers.ensureWaitTimeoutAt(step.Name, next.StartedAt.Time, *cfg.timeout); stored != nil {
						timeoutAt = stored.Time
					}
				}
				if !now.Time.Before(timeoutAt) {
					applyTimeoutBehavior("wait", cfg.onTimeout, &next)
					waiting = false
				}
			}
			if waiting && state != nil {
				state.setPollInterval(cfg.pollInterval)
			}
		}

		next = ensureStepStateTimes(next, now)
		if next != current {
			srun.Status.StepStates[step.Name] = next
			updated = true
		}
	}

	return updated
}

//nolint:gocyclo // complex by design
func (r *DAGReconciler) checkSyncGates(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, steps []bubuv1alpha1.Step, state *dagPipelineState) bool {
	if srun == nil || story == nil {
		return false
	}
	ensureStepStatesInitialized(srun)
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-gate")
	now := metav1.Now()

	updated := false
	for i := range steps {
		step := &steps[i]
		if step.Type != enums.StepTypeGate {
			continue
		}
		current, exists := srun.Status.StepStates[step.Name]
		if !exists || current.Phase.IsTerminal() {
			continue
		}
		if current.Phase != enums.PhasePaused && current.Phase != enums.PhaseRunning && current.Phase != enums.PhasePending {
			continue
		}

		cfg, cfgErr := parseGateConfig(step)
		if cfgErr != nil {
			log.Error(cfgErr, "Invalid gate step configuration", "step", step.Name)
			next := runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: cfgErr.Error()}
			next = ensureStepStateTimes(next, now)
			if next != current {
				srun.Status.StepStates[step.Name] = next
				updated = true
			}
			continue
		}

		decision := runsv1alpha1.GateDecisionPending
		status := runsv1alpha1.GateStatus{}
		if srun.Status.Gates != nil {
			if gateStatus, ok := srun.Status.Gates[step.Name]; ok {
				status = gateStatus
				if gateStatus.State != "" {
					decision = gateStatus.State
				}
			}
		}

		next := current
		switch decision {
		case runsv1alpha1.GateDecisionApproved:
			next.Phase = enums.PhaseSucceeded
			next.Message = "Gate approved."
			if status.Message != "" {
				next.Message = status.Message
			}
		case runsv1alpha1.GateDecisionRejected:
			next.Phase = enums.PhaseFailed
			next.Message = "Gate rejected."
			if status.Message != "" {
				next.Message = status.Message
			}
		default:
			next.Phase = enums.PhasePaused
			if next.Message == "" {
				next.Message = "Waiting for gate decision."
			}
			if cfg.timeout != nil {
				if next.StartedAt == nil {
					next.StartedAt = &now
				}
				timeoutAt := next.StartedAt.Add(*cfg.timeout)
				if state != nil && state.timers != nil {
					if stored := state.timers.ensureGateTimeoutAt(step.Name, next.StartedAt.Time, *cfg.timeout); stored != nil {
						timeoutAt = stored.Time
					}
				}
				if !now.Time.Before(timeoutAt) {
					applyTimeoutBehavior("gate", cfg.onTimeout, &next)
				}
			}
		}

		if next.Phase == enums.PhasePaused && state != nil && cfg.pollInterval > 0 {
			state.setPollInterval(cfg.pollInterval)
		}

		next = ensureStepStateTimes(next, now)
		if next != current {
			srun.Status.StepStates[step.Name] = next
			updated = true
		}
	}

	return updated
}

func parseSleepConfig(step *bubuv1alpha1.Step) (sleepConfig, error) {
	if step == nil || step.With == nil || len(step.With.Raw) == 0 {
		return sleepConfig{}, nil
	}
	var raw struct {
		Duration string `json:"duration"`
	}
	if err := json.Unmarshal(step.With.Raw, &raw); err != nil {
		return sleepConfig{}, fmt.Errorf("step '%s' has invalid 'with' for sleep: %w", step.Name, err)
	}
	if strings.TrimSpace(raw.Duration) == "" {
		return sleepConfig{}, nil
	}
	parsed, err := parsePositiveDuration(raw.Duration)
	if err != nil {
		return sleepConfig{}, fmt.Errorf("step '%s' has invalid sleep duration: %w", step.Name, err)
	}
	return sleepConfig{duration: parsed}, nil
}

func parseWaitConfig(step *bubuv1alpha1.Step) (waitConfig, error) {
	if step.With == nil {
		return waitConfig{}, fmt.Errorf("step '%s' of type 'wait' requires a 'with' block", step.Name)
	}
	var raw struct {
		Until        string `json:"until"`
		Timeout      string `json:"timeout,omitempty"`
		PollInterval string `json:"pollInterval,omitempty"`
		OnTimeout    string `json:"onTimeout,omitempty"`
	}
	if err := json.Unmarshal(step.With.Raw, &raw); err != nil {
		return waitConfig{}, fmt.Errorf("step '%s' has invalid 'with' for wait: %w", step.Name, err)
	}
	if strings.TrimSpace(raw.Until) == "" {
		return waitConfig{}, fmt.Errorf("step '%s' of type 'wait' requires 'with.until' to be set", step.Name)
	}
	cfg := waitConfig{until: raw.Until}
	if raw.Timeout != "" {
		parsed, err := parsePositiveDuration(raw.Timeout)
		if err != nil {
			return waitConfig{}, fmt.Errorf("step '%s' has invalid wait timeout: %w", step.Name, err)
		}
		cfg.timeout = &parsed
	}
	if raw.PollInterval != "" {
		parsed, err := parsePositiveDuration(raw.PollInterval)
		if err != nil {
			return waitConfig{}, fmt.Errorf("step '%s' has invalid wait pollInterval: %w", step.Name, err)
		}
		cfg.pollInterval = parsed
	}
	onTimeout, err := normalizeOnTimeout(raw.OnTimeout)
	if err != nil {
		return waitConfig{}, fmt.Errorf("step '%s' has invalid wait onTimeout: %w", step.Name, err)
	}
	cfg.onTimeout = onTimeout
	return cfg, nil
}

func parseGateConfig(step *bubuv1alpha1.Step) (gateConfig, error) {
	if step.With == nil {
		return gateConfig{}, nil
	}
	var raw struct {
		Timeout      string `json:"timeout,omitempty"`
		PollInterval string `json:"pollInterval,omitempty"`
		OnTimeout    string `json:"onTimeout,omitempty"`
	}
	if err := json.Unmarshal(step.With.Raw, &raw); err != nil {
		return gateConfig{}, fmt.Errorf("step '%s' has invalid 'with' for gate: %w", step.Name, err)
	}
	cfg := gateConfig{}
	if raw.Timeout != "" {
		parsed, err := parsePositiveDuration(raw.Timeout)
		if err != nil {
			return gateConfig{}, fmt.Errorf("step '%s' has invalid gate timeout: %w", step.Name, err)
		}
		cfg.timeout = &parsed
	}
	if raw.PollInterval != "" {
		parsed, err := parsePositiveDuration(raw.PollInterval)
		if err != nil {
			return gateConfig{}, fmt.Errorf("step '%s' has invalid gate pollInterval: %w", step.Name, err)
		}
		cfg.pollInterval = parsed
	}
	onTimeout, err := normalizeOnTimeout(raw.OnTimeout)
	if err != nil {
		return gateConfig{}, fmt.Errorf("step '%s' has invalid gate onTimeout: %w", step.Name, err)
	}
	cfg.onTimeout = onTimeout
	return cfg, nil
}

func normalizeOnTimeout(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "fail", "skip":
		return strings.ToLower(strings.TrimSpace(raw)), nil
	default:
		return "", fmt.Errorf("unsupported value %q (expected fail or skip)", raw)
	}
}

func applyTimeoutBehavior(kind, onTimeout string, state *runsv1alpha1.StepState) {
	action := strings.ToLower(strings.TrimSpace(onTimeout))
	if action == "" {
		action = "fail"
	}
	switch action {
	case "skip":
		state.Phase = enums.PhaseSkipped
		state.Message = fmt.Sprintf("%s step timed out and was skipped.", kind)
	default:
		state.Phase = enums.PhaseTimeout
		state.Message = fmt.Sprintf("%s step timed out.", kind)
	}
}

// findAndLaunchReadySteps builds dependency graphs, finds ready steps, and launches them.
//
// Behavior:
//   - Builds dependency/dependent graphs from Story steps.
//   - Decodes StoryRun inputs for template evaluation.
//   - Merges prior step outputs with inputs for variable context.
//   - Calls findReadySteps to identify ready and skipped steps.
//   - Marks skipped steps and launches ready steps via StepExecutor.
//
// Arguments:
//   - ctx context.Context: propagated to executor and helpers.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *bubuv1alpha1.Story: the Story definition with step DAG.
//   - completedSteps, runningSteps map[string]bool: current state buckets.
//   - priorStepOutputs map[string]any: outputs from completed steps for templates.
//   - depPolicy stepDependencyPolicy: dependency handling policy for this phase.
//
// Returns:
//   - []*bubuv1alpha1.Step: steps that were launched.
//   - []*bubuv1alpha1.Step: steps that were skipped.
//   - error: nil on success, or execution errors.
//
// Side Effects:
//   - Creates/updates StepRuns via StepExecutor.
//   - Mutates srun.Status.StepStates for skipped steps.
//
//nolint:gocyclo // complex by design
func (r *DAGReconciler) findAndLaunchReadySteps(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, steps []bubuv1alpha1.Step, completedSteps, runningSteps map[string]bool, priorStepOutputs map[string]any, depPolicy stepDependencyPolicy, needsPersist *bool) ([]*bubuv1alpha1.Step, []*bubuv1alpha1.Step, int, error) {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-launcher")
	ensureStepStatesInitialized(srun)
	dependencies, _ := buildDependencyGraphs(steps)
	storyRunInputs, _ := getStoryRunInputs(ctx, srun, story)

	vars := map[string]any{
		"inputs": storyRunInputs,
		"steps":  priorStepOutputs,
	}

	readySteps, skippedSteps, skippedReasons, unskipped := r.findReadySteps(ctx, srun, story, steps, srun.Status.StepStates, completedSteps, runningSteps, dependencies, vars, depPolicy)
	if needsPersist != nil && len(unskipped) > 0 {
		*needsPersist = true
	}
	queuedCount := 0
	readySteps, queuedSteps, queuedReason, err := r.enforceStoryConcurrency(ctx, srun, story, readySteps)
	if err != nil {
		return nil, nil, 0, err
	}
	if len(queuedSteps) > 0 {
		r.markQueuedSteps(srun, queuedSteps, queuedReason)
		queuedCount += len(queuedSteps)
	}
	readySteps, queuedSteps, queuedReason, err = r.enforceSchedulingLimits(ctx, srun, story, readySteps)
	if err != nil {
		return nil, nil, 0, err
	}
	if len(queuedSteps) > 0 {
		r.markQueuedSteps(srun, queuedSteps, queuedReason)
		queuedCount += len(queuedSteps)
	}
	if story != nil {
		scheduling := resolveSchedulingDecision(story, r.ConfigResolver, nil)
		metrics.RecordStoryRunQueueDepth(srun.Namespace, srun.Name, scheduling.Queue, queuedCount)
	}

	// Handle skipped steps by updating their status.
	for _, step := range skippedSteps {
		if _, exists := srun.Status.StepStates[step.Name]; !exists || srun.Status.StepStates[step.Name].Phase != enums.PhaseSkipped {
			log.Info("Marking step as Skipped", "step", step.Name)
			message := "Skipped due to 'if' condition"
			if reason, ok := skippedReasons[step.Name]; ok && reason != "" {
				message = reason
			}
			state := runsv1alpha1.StepState{Phase: enums.PhaseSkipped, Message: message}
			state = ensureStepStateTimes(state, metav1.Now())
			srun.Status.StepStates[step.Name] = state
		}
	}

	for _, step := range readySteps {
		if err := r.StepExecutor.Execute(ctx, srun, story, step, vars); err != nil {
			var blocked *templating.ErrEvaluationBlocked
			if errors.As(err, &blocked) {
				log.Info("Step execution blocked on template evaluation", "step", step.Name, "reason", blocked.Reason)
				continue
			}
			var offloaded *templating.ErrOffloadedDataUsage
			if errors.As(err, &offloaded) {
				policy := resolveOffloadedPolicy(r.ConfigResolver)
				if shouldBlockOffloaded(policy) || shouldResolveAllOffloaded(policy) {
					log.Info("Step execution requires offloaded data; waiting for hydration", "step", step.Name, "reason", offloaded.Reason)
					continue
				}
			}
			log.Error(err, "Failed to execute step", "step", step.Name)
			state := runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: err.Error()}
			state = ensureStepStateTimes(state, metav1.Now())
			srun.Status.StepStates[step.Name] = state
			// Propagate the error up to the main reconcile loop to trigger backoff
			return nil, nil, queuedCount, fmt.Errorf("failed to execute step %s: %w", step.Name, err)
		}
		if state, ok := srun.Status.StepStates[step.Name]; !ok || state.Phase == "" || isConcurrencyQueued(state) {
			state := runsv1alpha1.StepState{Phase: enums.PhaseRunning}
			state = ensureStepStateTimes(state, metav1.Now())
			srun.Status.StepStates[step.Name] = state
		}
	}

	return readySteps, skippedSteps, queuedCount, nil
}

func (r *DAGReconciler) enforceStoryConcurrency(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, readySteps []*bubuv1alpha1.Step) ([]*bubuv1alpha1.Step, []*bubuv1alpha1.Step, string, error) {
	if srun == nil || story == nil || len(readySteps) == 0 {
		return readySteps, nil, "", nil
	}
	limit := storyConcurrencyLimit(story)
	if limit <= 0 {
		return readySteps, nil, "", nil
	}
	runningCount, err := r.countRunningStepRuns(ctx, srun.Namespace, story.Name)
	if err != nil {
		return nil, nil, "", err
	}
	slots := max(int(limit)-runningCount, 0)
	if slots >= len(readySteps) {
		return readySteps, nil, "", nil
	}
	queued := readySteps[slots:]
	message := fmt.Sprintf("%s (%d running, limit %d)", concurrencyQueuedMessagePrefix, runningCount, limit)
	return readySteps[:slots], queued, message, nil
}

func (r *DAGReconciler) enforceSchedulingLimits(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, readySteps []*bubuv1alpha1.Step) ([]*bubuv1alpha1.Step, []*bubuv1alpha1.Step, string, error) {
	if srun == nil || story == nil || len(readySteps) == 0 {
		return readySteps, nil, "", nil
	}
	scheduling := resolveSchedulingDecision(story, r.ConfigResolver, nil)
	queueLabel := queueLabelValue(scheduling.Queue)
	if blocked, reason, err := r.enforcePriorityOrdering(ctx, srun, scheduling.Queue, queueLabel, scheduling.Priority); err != nil {
		return nil, nil, "", err
	} else if blocked {
		return nil, readySteps, reason, nil
	}

	globalLimit := globalConcurrencyLimit(r.ConfigResolver)
	queueLimit := queueConcurrencyLimit(schedulingConfig(r.ConfigResolver), scheduling.Queue)
	runningGlobal := 0
	runningQueue := 0

	globalSlots := len(readySteps)
	if globalLimit > 0 {
		count, err := r.countRunningStepRunsGlobal(ctx)
		if err != nil {
			return nil, nil, "", err
		}
		runningGlobal = count
		globalSlots = max(int(globalLimit)-runningGlobal, 0)
	}

	queueSlots := len(readySteps)
	if queueLimit > 0 {
		count, err := r.countRunningStepRunsByQueue(ctx, queueLabel)
		if err != nil {
			return nil, nil, "", err
		}
		runningQueue = count
		queueSlots = max(int(queueLimit)-runningQueue, 0)
	}

	slots := minInt(len(readySteps), globalSlots, queueSlots)
	if slots >= len(readySteps) {
		return readySteps, nil, "", nil
	}

	queued := readySteps[slots:]
	reason := ""
	switch {
	case globalLimit > 0 && (queueLimit <= 0 || globalSlots <= queueSlots):
		reason = fmt.Sprintf("%s (%d running, limit %d)", globalConcurrencyQueuedMessagePrefix, runningGlobal, globalLimit)
	case queueLimit > 0:
		reason = fmt.Sprintf("%s (%d running, limit %d)", queueConcurrencyQueuedMessagePrefix, runningQueue, queueLimit)
	default:
		reason = "Queued due to scheduling limits"
	}
	return readySteps[:slots], queued, reason, nil
}

func storyConcurrencyLimit(story *bubuv1alpha1.Story) int32 {
	if story == nil || story.Spec.Policy == nil || story.Spec.Policy.Concurrency == nil {
		return 0
	}
	return *story.Spec.Policy.Concurrency
}

func (r *DAGReconciler) countRunningStepRuns(ctx context.Context, namespace, storyName string) (int, error) {
	if strings.TrimSpace(storyName) == "" {
		return 0, nil
	}
	var stepRuns runsv1alpha1.StepRunList
	if err := r.List(ctx, &stepRuns,
		client.InNamespace(namespace),
		client.MatchingLabels{contracts.StoryNameLabelKey: storyName},
	); err != nil {
		return 0, err
	}
	count := 0
	for i := range stepRuns.Items {
		if stepRuns.Items[i].Status.Phase == enums.PhaseRunning {
			count++
		}
	}
	return count, nil
}

func (r *DAGReconciler) countRunningStepRunsGlobal(ctx context.Context) (int, error) {
	var stepRuns runsv1alpha1.StepRunList
	if err := r.List(ctx, &stepRuns, client.MatchingFields{setup.StepRunPhaseField: string(enums.PhaseRunning)}); err != nil {
		return 0, err
	}
	return len(stepRuns.Items), nil
}

func (r *DAGReconciler) countRunningStepRunsByQueue(ctx context.Context, queueLabel string) (int, error) {
	if strings.TrimSpace(queueLabel) == "" {
		return 0, nil
	}
	var stepRuns runsv1alpha1.StepRunList
	if err := r.List(ctx, &stepRuns,
		client.MatchingLabels{contracts.QueueLabelKey: queueLabel},
	); err != nil {
		return 0, err
	}
	count := 0
	for i := range stepRuns.Items {
		if stepRuns.Items[i].Status.Phase == enums.PhaseRunning {
			count++
		}
	}
	return count, nil
}

func (r *DAGReconciler) enforcePriorityOrdering(ctx context.Context, srun *runsv1alpha1.StoryRun, queueName string, queueLabel string, priority int32) (bool, string, error) {
	if srun == nil || strings.TrimSpace(queueLabel) == "" {
		return false, "", nil
	}
	if strings.TrimSpace(queueName) == "" {
		queueName = defaultQueueName
	}
	var runs runsv1alpha1.StoryRunList
	if err := r.List(ctx, &runs, client.MatchingLabels{contracts.QueueLabelKey: queueLabel}); err != nil {
		return false, "", err
	}
	now := time.Now()
	agingSeconds := queuePriorityAgingSeconds(schedulingConfig(r.ConfigResolver), queueName)
	currentPriority := effectivePriority(priority, storyRunQueuedSince(srun), agingSeconds, now)
	for i := range runs.Items {
		other := &runs.Items[i]
		if other.Name == srun.Name && other.Namespace == srun.Namespace {
			continue
		}
		if other.Status.Phase.IsTerminal() {
			continue
		}
		if !storyRunHasDemand(other) {
			continue
		}
		otherPriority := priorityFromLabels(other.Labels)
		otherEffective := effectivePriority(otherPriority, storyRunQueuedSince(other), agingSeconds, now)
		if otherEffective <= currentPriority {
			continue
		}
		if queuedAt := storyRunQueuedSince(srun); queuedAt != nil {
			metrics.RecordStoryRunQueueAge(srun.Namespace, queueName, now.Sub(*queuedAt))
		}
		return true, priorityQueuedMessagePrefix, nil
	}
	return false, "", nil
}

func effectivePriority(base int32, queuedSince *time.Time, agingSeconds int32, now time.Time) int32 {
	if queuedSince == nil || agingSeconds <= 0 {
		return base
	}
	elapsed := now.Sub(*queuedSince)
	if elapsed <= 0 {
		return base
	}
	steps := int32(elapsed.Seconds()) / agingSeconds
	if steps <= 0 {
		return base
	}
	return base + steps
}

func storyRunQueuedSince(srun *runsv1alpha1.StoryRun) *time.Time {
	if srun == nil || len(srun.Status.StepStates) == 0 {
		return nil
	}
	var earliest *time.Time
	for _, state := range srun.Status.StepStates {
		if !isConcurrencyQueued(state) || state.StartedAt == nil {
			continue
		}
		candidate := state.StartedAt.Time
		if earliest == nil || candidate.Before(*earliest) {
			t := candidate
			earliest = &t
		}
	}
	return earliest
}

func storyRunHasDemand(srun *runsv1alpha1.StoryRun) bool {
	if srun == nil {
		return false
	}
	if srun.Status.Phase == enums.PhaseRunning || srun.Status.Phase == enums.PhasePending {
		return true
	}
	if len(srun.Status.StepStates) == 0 {
		return false
	}
	for _, state := range srun.Status.StepStates {
		if state.Phase == enums.PhaseRunning || isConcurrencyQueued(state) {
			return true
		}
	}
	return false
}

func (r *DAGReconciler) markQueuedSteps(srun *runsv1alpha1.StoryRun, queuedSteps []*bubuv1alpha1.Step, message string) {
	if srun == nil || len(queuedSteps) == 0 {
		return
	}
	ensureStepStatesInitialized(srun)
	now := metav1.Now()
	for _, step := range queuedSteps {
		if step == nil {
			continue
		}
		state := srun.Status.StepStates[step.Name]
		if state.Phase == enums.PhasePending && state.Message == message {
			continue
		}
		state.Phase = enums.PhasePending
		state.Message = message
		state = ensureStepStateTimes(state, now)
		srun.Status.StepStates[step.Name] = state
	}
}

func clearConcurrencyQueuedSteps(running map[string]bool, states map[string]runsv1alpha1.StepState) {
	if len(running) == 0 || len(states) == 0 {
		return
	}
	for name := range running {
		state, ok := states[name]
		if !ok {
			continue
		}
		if isConcurrencyQueued(state) {
			delete(running, name)
		}
	}
}

func isConcurrencyQueued(state runsv1alpha1.StepState) bool {
	if state.Phase != enums.PhasePending {
		return false
	}
	switch {
	case strings.HasPrefix(state.Message, concurrencyQueuedMessagePrefix):
		return true
	case strings.HasPrefix(state.Message, queueConcurrencyQueuedMessagePrefix):
		return true
	case strings.HasPrefix(state.Message, globalConcurrencyQueuedMessagePrefix):
		return true
	case strings.HasPrefix(state.Message, priorityQueuedMessagePrefix):
		return true
	default:
		return false
	}
}

func minInt(values ...int) int {
	if len(values) == 0 {
		return 0
	}
	min := values[0]
	for i := 1; i < len(values); i++ {
		if values[i] < min {
			min = values[i]
		}
	}
	return min
}

// getPriorStepOutputs aggregates outputs from completed StepRuns and sub-stories.
//
// Behavior:
//   - Ensures a StepRunList is available (uses cached list or fetches fresh).
//   - Collects outputs from succeeded StepRuns via collectOutputsFromStepRuns.
//   - Collects outputs from synchronous sub-stories via collectOutputsFromSubStories.
//   - Adds normalized alias keys for template expressions.
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

	// Add normalized aliases for template expressions
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
//   - Prepares step outputs for template evaluation.
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
//
//nolint:gocyclo // complex by design
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

		if sr.Status.Phase == enums.PhaseSucceeded || sr.Status.Phase == enums.PhaseSkipped {
			if sr.Status.Phase == enums.PhaseSucceeded {
				if outputData, ok := populateStepOutputs(
					sr.Status.Output,
					stepID,
					stepContext,
					log,
					"Failed to unmarshal output from prior StepRun during fallback",
				); ok {
					logStepOutputSummary(log, sr, outputData)
				}
			}
			// Always include succeeded/skipped steps in the outputs map with at least an
			// empty output context, so downstream template expressions don't nil-deref
			// when a step completed without output.
			if stepContext["output"] == nil {
				stepContext["output"] = map[string]any{}
			}
			if stepContext["outputs"] == nil {
				stepContext["outputs"] = map[string]any{}
			}
			updated = true
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
			log.V(1).Info("Collected StepRun context for DAG inputs",
				"step", stepID,
				"phase", sr.Status.Phase,
				"signals", len(sr.Status.Signals) > 0,
				"outputsCaptured", stepContext["outputs"] != nil)
		}
	}
}

// hydratePriorOutputs replaces storage refs in step outputs with resolved content
// so "if" / "with" templates see inline values. When storage is disabled or
// hydration fails, outputs are left unchanged (downstream may then block on
// offloaded refs or use materialize).
func hydratePriorOutputs(ctx context.Context, outputs map[string]any, log *logging.ReconcileLogger) {
	if ctx == nil || len(outputs) == 0 {
		return
	}
	manager, err := storage.SharedManager(ctx)
	if err != nil || manager == nil {
		return
	}
	hydratePriorOutputsWithManager(ctx, manager, outputs, log)
}

func hydratePriorOutputsWithManager(ctx context.Context, manager *storage.StorageManager, outputs map[string]any, log *logging.ReconcileLogger) {
	if manager == nil || len(outputs) == 0 {
		return
	}
	for stepID, raw := range outputs {
		if !stepOutputHasStorageRef(outputs, stepID) {
			continue
		}
		stepContext, ok := raw.(map[string]any)
		if !ok || stepContext == nil {
			continue
		}
		outputVal := stepContext["output"]
		if outputVal == nil {
			continue
		}
		hydrated, err := manager.HydrateStorageRefsOnly(ctx, outputVal)
		if err != nil {
			if log != nil {
				log.Error(err, "Failed to hydrate prior step output; templates may see refs", "step", stepID)
			}
			continue
		}
		stepContext["output"] = hydrated
		stepContext["outputs"] = hydrated
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
				if hasSignalSeq(existing) || hasSignalSeq(vMap) {
					if shouldOverwriteSignalValue(existing, vMap) {
						dst[k] = v
					}
					continue
				}
				mergeSignalMaps(existing, vMap)
				continue
			}
		}
		if shouldOverwriteSignalValue(dst[k], v) {
			dst[k] = v
		}
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
	if existing, exists := current[leaf]; exists {
		if !shouldOverwriteSignalValue(existing, value) {
			return
		}
	}
	current[leaf] = value
}

func hasSignalSeq(value any) bool {
	_, ok := extractSignalSeq(value)
	return ok
}

func shouldOverwriteSignalValue(existing any, incoming any) bool {
	existingSeq, okExisting := extractSignalSeq(existing)
	incomingSeq, okIncoming := extractSignalSeq(incoming)
	if okExisting && okIncoming {
		return incomingSeq >= existingSeq
	}
	return true
}

//nolint:gocyclo // complex by design
func extractSignalSeq(value any) (uint64, bool) {
	m, ok := value.(map[string]any)
	if !ok {
		return 0, false
	}
	raw, ok := m["seq"]
	if !ok || raw == nil {
		return 0, false
	}
	switch v := raw.(type) {
	case float64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case float32:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int64:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case int32:
		if v < 0 {
			return 0, false
		}
		return uint64(v), true
	case uint:
		return uint64(v), true
	case uint64:
		return v, true
	case uint32:
		return uint64(v), true
	case json.Number:
		parsed, err := v.Int64()
		if err != nil || parsed < 0 {
			return 0, false
		}
		return uint64(parsed), true
	case string:
		parsed, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
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

// addAliasKeys duplicates keys using normalized aliases for template convenience.
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
//   - Checks all dependencies are met (completed or realtime-satisfied).
//   - Evaluates template "if" conditions when present.
//   - Separates ready steps from skipped steps (condition evaluated to false).
//
// Arguments:
//   - ctx context.Context: propagated to template evaluation.
//   - story *bubuv1alpha1.Story: Story definition for realtime checks.
//   - steps []bubuv1alpha1.Step: all steps in the Story.
//   - stepStates map[string]runsv1alpha1.StepState: current step phases.
//   - completed, running map[string]bool: state buckets.
//   - dependencies map[string]map[string]bool: step → dependencies map.
//   - vars map[string]any: variable context for templates.
//   - depPolicy stepDependencyPolicy: dependency handling policy.
//
// Returns:
//   - []*bubuv1alpha1.Step: steps ready to execute.
//   - []*bubuv1alpha1.Step: steps skipped due to false conditions or failed dependencies.
//   - map[string]string: step name -> skip reason.
//   - []string: step names that were Skipped but re-evaluated true (un-skipped) so the caller can persist.
//
//nolint:gocyclo // complex by design
func (r *DAGReconciler) findReadySteps(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	steps []bubuv1alpha1.Step,
	stepStates map[string]runsv1alpha1.StepState,
	completed, running map[string]bool,
	dependencies map[string]map[string]bool,
	vars map[string]any,
	depPolicy stepDependencyPolicy,
) ([]*bubuv1alpha1.Step, []*bubuv1alpha1.Step, map[string]string, []string) {
	var ready, skipped []*bubuv1alpha1.Step
	skipReasons := make(map[string]string)
	var unskipped []string
	isRealtime := story != nil && story.Spec.Pattern.IsRealtime()

	for i := range steps {
		step := &steps[i]
		if completed[step.Name] || running[step.Name] {
			continue
		}
		if state, ok := stepStates[step.Name]; ok && state.Phase.IsTerminal() {
			// Re-evaluate Skipped steps whose "if" referenced missing output that is now available.
			if state.Phase == enums.PhaseSkipped && step.If != nil && *step.If != "" {
				if !outputRefsMaybeStale(*step.If, completed, vars) {
					var result bool
					var evalErr error
					stepOutputs, _ := vars["steps"].(map[string]any)
					if offloaded := detectOffloadedOutputRefs(*step.If, stepOutputs); offloaded != nil {
						policy := resolveOffloadedPolicy(r.ConfigResolver)
						resolveAll := shouldResolveAllOffloaded(policy) ||
							(story != nil && storyForcesControllerResolve(story.GetAnnotations()))
						if resolveAll {
							result, evalErr = resolveConditionInProcess(ctx, r.TemplateEvaluator, *step.If, vars)
						} else if shouldBlockOffloaded(policy) && srun != nil {
							matVars := map[string]any{"inputs": vars["inputs"], "steps": filterStepsForTemplate(*step.If, stepOutputs)}
							materialized, matErr := resolveMaterialize(ctx, r.Client, r.Scheme(), r.ConfigResolver, srun, materializePurposeIf, step.Name, "", materializeModeCondition, *step.If, matVars)
							if matErr != nil {
								evalErr = matErr
							} else if boolVal, ok := materialized.(bool); ok {
								result = boolVal
							} else {
								evalErr = fmt.Errorf("materialize if result is not bool for step %s", step.Name)
							}
						} else {
							continue
						}
					} else {
						result, evalErr = r.TemplateEvaluator.EvaluateCondition(ctx, *step.If, vars)
					}
					if evalErr != nil {
						var blocked *templating.ErrEvaluationBlocked
						if errors.As(evalErr, &blocked) {
							logging.NewControllerLogger(ctx, "storyrun-if-eval").Info(
								"Re-evaluate Skipped step blocked; waiting for materialize",
								"step", step.Name, "reason", blocked.Reason,
							)
						}
						continue
					}
					if result {
						delete(stepStates, step.Name)
						completed[step.Name] = false
						unskipped = append(unskipped, step.Name)
						logging.NewControllerLogger(ctx, "storyrun-if-eval").Info(
							"Re-evaluated Skipped step; condition now true, re-queuing",
							"step", step.Name,
						)
						// Fall through to dependency/if/with checks and possibly add to ready
					} else {
						continue
					}
				} else {
					continue
				}
			} else {
				continue
			}
		}

		deps := dependencies[step.Name]
		allDepsMet := true
		failedDep := ""
		for dep := range deps {
			if completed[dep] {
				continue
			}
			depState := stepStates[dep]
			if r.dependencySatisfiedForRealtime(story, depState) {
				continue
			}
			if depPolicy.allowFailedDependencies && depState.Phase.IsTerminal() {
				continue
			}
			if depPolicy.skipOnFailedDependency && depState.Phase.IsTerminal() &&
				depState.Phase != enums.PhaseSucceeded &&
				depState.Phase != enums.PhaseSkipped {
				failedDep = dep
				break
			}
			allDepsMet = false
			break
		}

		if failedDep != "" {
			skipped = append(skipped, step)
			skipReasons[step.Name] = fmt.Sprintf("Skipped due to failed dependency: %s", failedDep)
			continue
		}
		if allDepsMet {
			if step.If != nil && *step.If != "" && !isRealtime {
				var result bool
				var err error
				if safetyErr := templatesafety.ValidateTemplateString(*step.If); safetyErr != nil {
					state := runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: safetyErr.Error()}
					state = ensureStepStateTimes(state, metav1.Now())
					stepStates[step.Name] = state
					continue
				}
				stepOutputs, _ := vars["steps"].(map[string]any)
				if offloaded := detectOffloadedOutputRefs(*step.If, stepOutputs); offloaded != nil {
					policy := resolveOffloadedPolicy(r.ConfigResolver)
					resolveAll := shouldResolveAllOffloaded(policy) ||
						(story != nil && storyForcesControllerResolve(story.GetAnnotations()))
					if resolveAll {
						result, err = resolveConditionInProcess(ctx, r.TemplateEvaluator, *step.If, vars)
					} else if shouldBlockOffloaded(policy) && srun != nil {
						materialized, matErr := resolveMaterialize(ctx, r.Client, r.Scheme(), r.ConfigResolver, srun, materializePurposeIf, step.Name, "", materializeModeCondition, *step.If, vars)
						if matErr != nil {
							err = matErr
						} else if boolVal, ok := materialized.(bool); ok {
							result = boolVal
						} else {
							err = fmt.Errorf("materialize result must be bool, got %T", materialized)
						}
					} else {
						logging.NewControllerLogger(ctx, "storyrun-if-eval").Info("Template 'if' references offloaded output but resolution not enabled; set templating.offloaded-data-policy to 'inject' or 'controller'", "step", step.Name, "policy", policy, "reason", offloaded.Reason)
						err = offloaded
					}
				} else {
					result, err = r.TemplateEvaluator.EvaluateCondition(ctx, *step.If, vars)
					// Evaluator can return ErrOffloadedDataUsage when it hits a ref during evaluation
					// (detectOffloadedOutputRefs may have missed it). Handle via in-process or materialization.
					if err != nil {
						var offloaded *templating.ErrOffloadedDataUsage
						if errors.As(err, &offloaded) {
							policy := resolveOffloadedPolicy(r.ConfigResolver)
							resolveAll := shouldResolveAllOffloaded(policy) ||
								(story != nil && storyForcesControllerResolve(story.GetAnnotations()))
							if resolveAll {
								result, err = resolveConditionInProcess(ctx, r.TemplateEvaluator, *step.If, vars)
							} else if shouldBlockOffloaded(policy) && srun != nil {
								materialized, matErr := resolveMaterialize(ctx, r.Client, r.Scheme(), r.ConfigResolver, srun, materializePurposeIf, step.Name, "", materializeModeCondition, *step.If, vars)
								if matErr == nil {
									if boolVal, ok := materialized.(bool); ok {
										result = boolVal
										err = nil
									} else {
										err = fmt.Errorf("materialize result must be bool, got %T", materialized)
									}
								}
							}
						}
					}
				}
				if err != nil {
					log := logging.NewControllerLogger(ctx, "storyrun-if-eval")
					var blocked *templating.ErrEvaluationBlocked
					if errors.As(err, &blocked) {
						log.V(1).Info("Template 'if' condition blocked; waiting for materialize", "step", step.Name, "reason", blocked.Reason)
						continue
					}
					var offloaded *templating.ErrOffloadedDataUsage
					if errors.As(err, &offloaded) {
						policy := resolveOffloadedPolicy(r.ConfigResolver)
						if shouldBlockOffloaded(policy) || shouldResolveAllOffloaded(policy) {
							log.Info("Template 'if' condition requires offloaded data; waiting for resolution", "step", step.Name, "reason", offloaded.Reason)
							continue
						}
						state := runsv1alpha1.StepState{Phase: enums.PhaseFailed, Message: offloaded.Error()}
						state = ensureStepStateTimes(state, metav1.Now())
						stepStates[step.Name] = state
						log.Info("Template 'if' condition requires offloaded data; failing step", "step", step.Name, "reason", offloaded.Reason)
						continue
					}
					// This indicates a potentially recoverable error (e.g., variable not yet available).
					log.Info("Template 'if' condition blocked or failed evaluation, step not ready", "step", step.Name, "reason", err)
					continue
				}
				if err == nil && !result {
					// Do not skip when the condition references a completed step's output
					// that is empty in vars (e.g. controller cache not yet updated after SDK patch).
					if outputRefsMaybeStale(*step.If, completed, vars) {
						logging.NewControllerLogger(ctx, "storyrun-if-eval").Info(
							"Template 'if' evaluated false but referenced step output is empty; waiting for prior outputs",
							"step", step.Name,
						)
						continue
					}
					skipped = append(skipped, step)
					skipReasons[step.Name] = "Skipped due to 'if' condition"
					continue
				}
			}
			// Do not add to ready when the step's "with" block references a completed step's
			// output that is empty in vars (avoids creating a StepRun that stays Pending).
			if withRefsMaybeStale(step, completed, vars) {
				logging.NewControllerLogger(ctx, "storyrun-if-eval").Info(
					"Step 'with' references prior step output that is empty; waiting for prior outputs",
					"step", step.Name,
				)
				continue
			}
			ready = append(ready, step)
		}
	}
	return ready, skipped, skipReasons, unskipped
}

// finalizeSuccessfulRun evaluates the Story's output template and sets the final status.
//
// Behavior:
//   - If no output template exists, marks StoryRun as Succeeded immediately.
//   - Collects inputs and prior step outputs for template evaluation.
//   - Unmarshals and evaluates the output template via templates.
//   - Enforces a 1 MiB output size limit, setting Degraded condition if exceeded.
//   - Patches StoryRun with output, phase, conditions, and completion timestamps.
//
// Arguments:
//   - ctx context.Context: propagated to template and patch helpers.
//   - srun *runsv1alpha1.StoryRun: the StoryRun to finalize.
//   - story *bubuv1alpha1.Story: the Story definition with output template.
//
// Returns:
//   - error: nil on success, or evaluation/patch errors.
//
// Side Effects:
//   - Patches StoryRun status with output and completion fields.
//
//nolint:gocyclo // complex by design
func (r *DAGReconciler) finalizeSuccessfulRun(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) error {
	log := logging.NewReconcileLogger(ctx, "storyrun-dag-finalize").WithValues("storyrun", srun.Name)

	// If there's no output template, just mark as succeeded.
	if story.Spec.Output == nil {
		return r.setStoryRunPhase(ctx, srun, enums.PhaseSucceeded, conditions.ReasonCompleted, successMessage(srun))
	}

	// Evaluate the output template
	log.Info("Evaluating story output template")
	if err := templatesafety.ValidateTemplateJSON(story.Spec.Output.Raw); err != nil {
		return err
	}
	storyRunInputs, err := getStoryRunInputs(ctx, srun, story)
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

	var resolvedOutput map[string]any
	if offloaded := detectOffloadedOutputRefs(string(story.Spec.Output.Raw), priorStepOutputs); offloaded != nil {
		policy := resolveOffloadedPolicy(r.ConfigResolver)
		resolveAll := shouldResolveAllOffloaded(policy) || storyForcesControllerResolve(story.GetAnnotations())
		if resolveAll {
			resolved, ipErr := resolveOffloadedInProcess(ctx, r.TemplateEvaluator, outputTemplate, vars)
			if ipErr != nil {
				return fmt.Errorf("in-process resolution failed for story output: %w", ipErr)
			}
			resolvedOutput = resolved
		} else if shouldBlockOffloaded(policy) {
			filteredVars := map[string]any{"inputs": vars["inputs"], "steps": filterStepsForTemplate(string(story.Spec.Output.Raw), priorStepOutputs)}
			result, err := resolveMaterialize(ctx, r.Client, r.Scheme(), r.ConfigResolver, srun, materializePurposeStoryOutput, "", "", materializeModeObject, outputTemplate, filteredVars)
			if err != nil {
				return err
			}
			materialized, ok := result.(map[string]any)
			if !ok {
				return fmt.Errorf("materialize result must be object, got %T", result)
			}
			resolvedOutput = materialized
		} else {
			return offloaded
		}
	} else {
		out, err := r.TemplateEvaluator.ResolveWithInputs(ctx, outputTemplate, vars)
		if err != nil {
			return fmt.Errorf("failed to evaluate story output template: %w", err)
		}
		resolvedOutput = out
	}

	outputBytes, err := json.Marshal(resolvedOutput)
	if err != nil {
		return fmt.Errorf("failed to marshal resolved story output: %w", err)
	}

	if err := validateJSONOutputBytes(outputBytes, story.Spec.OutputsSchema, "Story outputs"); err != nil {
		log.Error(err, "Story output failed schema validation")
		message := fmt.Sprintf("Story output failed schema validation: %v", err)
		if setErr := r.setStoryRunPhase(ctx, srun, enums.PhaseFailed, conditions.ReasonOutputSchemaFailed, message); setErr != nil {
			return setErr
		}
		return nil
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
			sr.Status.Message = successMessageWithDegradedOutput(sr)
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
		sr.Status.Message = successMessage(sr)
		sr.Status.ObservedGeneration = sr.Generation

		if sr.Status.FinishedAt == nil {
			now := metav1.Now()
			sr.Status.FinishedAt = &now
			if sr.Status.StartedAt != nil {
				sr.Status.Duration = now.Sub(sr.Status.StartedAt.Time).Round(time.Second).String()
			}
		}

		cm := conditions.NewConditionManager(sr.Generation)
		cm.SetCondition(&sr.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonCompleted, successMessage(sr))
	})
}

func successMessage(srun *runsv1alpha1.StoryRun) string {
	if srun == nil || len(srun.Status.AllowedFailures) == 0 {
		return "All steps completed successfully."
	}
	return fmt.Sprintf("Completed with allowed failures: %s.", strings.Join(srun.Status.AllowedFailures, ", "))
}

func successMessageWithDegradedOutput(srun *runsv1alpha1.StoryRun) string {
	if srun == nil || len(srun.Status.AllowedFailures) == 0 {
		return "All steps completed successfully, but the final output was too large to store."
	}
	return fmt.Sprintf("Completed with allowed failures: %s, but the final output was too large to store.", strings.Join(srun.Status.AllowedFailures, ", "))
}

// buildDependencyGraphs builds adjacency maps for step dependencies.
//
// Behavior:
//   - Scans explicit "needs" dependencies.
//   - Scans implicit dependencies from "if" and "with" template expressions.
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
	// Match steps.name, steps["name"], and (index .steps "name") for implicit dependency extraction.
	stepNameRegex := regexp.MustCompile(
		`steps\.([a-zA-Z0-9_\-]+)\.|steps\s*\[\s*['"]([a-zA-Z0-9_\-]+)['"]\s*\]|\(index\s+\.steps\s+["']([a-zA-Z0-9_\-]+)["']\)`,
	)

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

//nolint:gocyclo // complex by design
func validateRuntimeDependencyGraph(steps []bubuv1alpha1.Step) error {
	if len(steps) == 0 {
		return nil
	}

	dependencies, _ := buildDependencyGraphs(steps)
	stepNames := make(map[string]struct{}, len(steps))
	for i := range steps {
		stepNames[steps[i].Name] = struct{}{}
	}

	unknownDeps := make([]string, 0)
	for stepName, deps := range dependencies {
		for depName := range deps {
			if _, ok := stepNames[depName]; !ok {
				unknownDeps = append(unknownDeps, fmt.Sprintf("%s->%s", stepName, depName))
			}
		}
	}
	if len(unknownDeps) > 0 {
		sort.Strings(unknownDeps)
		return fmt.Errorf("unknown step dependencies: %s", strings.Join(unknownDeps, ", "))
	}

	indegree := make(map[string]int, len(stepNames))
	for stepName := range stepNames {
		indegree[stepName] = 0
	}
	for stepName, deps := range dependencies {
		indegree[stepName] = len(deps)
	}

	ready := make([]string, 0, len(stepNames))
	for stepName, degree := range indegree {
		if degree == 0 {
			ready = append(ready, stepName)
		}
	}
	sort.Strings(ready)

	visited := 0
	for len(ready) > 0 {
		current := ready[0]
		ready = ready[1:]
		visited++
		for stepName, deps := range dependencies {
			if !deps[current] {
				continue
			}
			indegree[stepName]--
			delete(deps, current)
			if indegree[stepName] == 0 {
				ready = append(ready, stepName)
				sort.Strings(ready)
			}
		}
	}

	if visited == len(stepNames) {
		return nil
	}

	blocked := make([]string, 0, len(stepNames)-visited)
	for stepName, degree := range indegree {
		if degree > 0 {
			blocked = append(blocked, stepName)
		}
	}
	sort.Strings(blocked)
	return fmt.Errorf("dependency cycle detected involving step(s): %s", strings.Join(blocked, ", "))
}

// outputRefsMaybeStale returns true when the "if" expression references a completed
// step's output but that step has empty output in vars (e.g. controller cache not yet
// updated after SDK patch). Caller should not mark the step skipped in that case.
func outputRefsMaybeStale(ifExpr string, completedSteps map[string]bool, vars map[string]any) bool {
	if ifExpr == "" || len(completedSteps) == 0 {
		return false
	}
	stepOutputs, _ := vars["steps"].(map[string]any)
	if len(stepOutputs) == 0 {
		return false
	}
	stepNameRegex := regexp.MustCompile(
		`steps\.([a-zA-Z0-9_\-]+)\.|steps\s*\[\s*['"]([a-zA-Z0-9_\-]+)['"]\s*\]|\(index\s+\.steps\s+["']([a-zA-Z0-9_\-]+)["']\)`,
	)
	matches := stepNameRegex.FindAllStringSubmatch(ifExpr, -1)
	for _, match := range matches {
		var depName string
		if len(match) > 3 && match[3] != "" {
			depName = match[3]
		} else if len(match) > 2 && match[2] != "" {
			depName = match[2]
		} else if len(match) > 1 && match[1] != "" {
			depName = match[1]
		}
		if depName == "" || !completedSteps[depName] {
			continue
		}
		ctx, _ := stepOutputs[depName].(map[string]any)
		if ctx == nil {
			return true
		}
		output, _ := ctx["output"].(map[string]any)
		if len(output) == 0 {
			return true
		}
	}
	return false
}

// withRefsMaybeStale returns true when the step's "with" block references a completed
// step's output but that step has empty output in vars. Caller should not add the step
// to ready in that case (avoids creating a StepRun that would stay Pending on input resolution).
func withRefsMaybeStale(step *bubuv1alpha1.Step, completed map[string]bool, vars map[string]any) bool {
	if step == nil || len(completed) == 0 {
		return false
	}
	var withBytes []byte
	if step.With != nil {
		withBytes = step.With.Raw
	}
	if len(withBytes) == 0 {
		return false
	}
	return outputRefsMaybeStale(string(withBytes), completed, vars)
}

// findAndAddDeps scans an expression for step references and records dependencies.
//
// Behavior:
//   - Uses regex to find "steps.name", "steps['name']", or "(index .steps \"name\")" patterns.
//   - Maps normalized aliases back to real step names.
//   - Calls addDependency for each found reference.
//
// Arguments:
//   - expression string: template expression to scan.
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
		if len(match) > 3 && match[3] != "" {
			depName = match[3] // (index .steps "name")
		} else if len(match) > 2 && match[2] != "" {
			depName = match[2] // steps["name"]
		} else if len(match) > 1 && match[1] != "" {
			depName = match[1] // steps.name
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

func allStorySteps(story *bubuv1alpha1.Story) []bubuv1alpha1.Step {
	if story == nil {
		return nil
	}
	total := len(story.Spec.Steps) + len(story.Spec.Compensations) + len(story.Spec.Finally)
	steps := make([]bubuv1alpha1.Step, 0, total)
	steps = append(steps, story.Spec.Steps...)
	steps = append(steps, story.Spec.Compensations...)
	steps = append(steps, story.Spec.Finally...)
	return steps
}

func stepsTerminal(total int, completed, failed map[string]bool) bool {
	if total == 0 {
		return true
	}
	return len(completed)+len(failed) == total
}

func markFailFastSkipped(srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, completed, running map[string]bool) bool {
	if srun == nil || story == nil {
		return false
	}
	ensureStepStatesInitialized(srun)
	now := metav1.Now()
	updated := false
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if completed[step.Name] || running[step.Name] {
			continue
		}
		state := srun.Status.StepStates[step.Name]
		if state.Phase.IsTerminal() {
			continue
		}
		state.Phase = enums.PhaseSkipped
		state.Message = "Skipped due to fail-fast policy"
		state = ensureStepStateTimes(state, now)
		srun.Status.StepStates[step.Name] = state
		updated = true
	}
	return updated
}

func markCompensationsSkipped(srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) bool {
	if srun == nil || story == nil {
		return false
	}
	if len(story.Spec.Compensations) == 0 {
		return false
	}

	completed, running, failed, _ := buildStateMaps(story.Spec.Compensations, srun.Status.StepStates)
	ensureStepStatesInitialized(srun)
	now := metav1.Now()
	updated := false
	for i := range story.Spec.Compensations {
		step := &story.Spec.Compensations[i]
		if completed[step.Name] || running[step.Name] || failed[step.Name] {
			continue
		}
		state := srun.Status.StepStates[step.Name]
		if state.Phase.IsTerminal() {
			continue
		}
		state.Phase = enums.PhaseSkipped
		state.Message = "Skipped because story succeeded"
		state = ensureStepStateTimes(state, now)
		srun.Status.StepStates[step.Name] = state
		updated = true
	}
	return updated
}

func firstFailedStepName(failed map[string]bool) string {
	for name := range failed {
		return name
	}
	return ""
}

// buildStateMaps buckets StepStates into completed, running, failed, and allowed-failed sets.
//
// Behavior:
//   - Succeeded and Skipped → completed.
//   - Running, Pending, and Paused → running.
//   - Terminal failures → failed, unless AllowFailure is set (then marked completed + allowedFailures).
//   - Ignores StepStates not present in the provided step list.
func buildStateMaps(steps []bubuv1alpha1.Step, states map[string]runsv1alpha1.StepState) (completed, running, failed, allowedFailures map[string]bool) {
	completed = make(map[string]bool)
	running = make(map[string]bool)
	failed = make(map[string]bool)
	allowedFailures = make(map[string]bool)

	allowed := make(map[string]struct{})
	allowFailure := make(map[string]bool, len(steps))
	for i := range steps {
		allowed[steps[i].Name] = struct{}{}
		if steps[i].AllowFailure != nil && *steps[i].AllowFailure {
			allowFailure[steps[i].Name] = true
		}
	}

	for name, state := range states {
		if _, ok := allowed[name]; !ok {
			continue
		}
		if state.Phase == enums.PhaseSucceeded || state.Phase == enums.PhaseSkipped {
			completed[name] = true
		} else if state.Phase == enums.PhaseRunning || state.Phase == enums.PhasePending || state.Phase == enums.PhasePaused {
			running[name] = true
		} else if state.Phase.IsTerminal() { // Failed, Canceled, etc.
			if allowFailure[name] {
				completed[name] = true
				allowedFailures[name] = true
				continue
			}
			failed[name] = true
		}
	}
	return completed, running, failed, allowedFailures
}

func (r *DAGReconciler) syncAllowedFailures(srun *runsv1alpha1.StoryRun, steps []bubuv1alpha1.Step) bool {
	if srun == nil {
		return false
	}
	allowed := collectAllowedFailures(steps, srun.Status.StepStates)
	if len(allowed) == 0 && len(srun.Status.AllowedFailures) == 0 {
		return false
	}
	if stringSliceEqual(allowed, srun.Status.AllowedFailures) {
		return false
	}
	srun.Status.AllowedFailures = allowed
	return true
}

func collectAllowedFailures(steps []bubuv1alpha1.Step, states map[string]runsv1alpha1.StepState) []string {
	allowFailure := make(map[string]bool, len(steps))
	for i := range steps {
		if steps[i].AllowFailure != nil && *steps[i].AllowFailure {
			allowFailure[steps[i].Name] = true
		}
	}

	var allowed []string
	for name, state := range states {
		if !allowFailure[name] {
			continue
		}
		if state.Phase == enums.PhaseSucceeded || state.Phase == enums.PhaseSkipped {
			continue
		}
		if state.Phase.IsTerminal() {
			allowed = append(allowed, name)
		}
	}
	sort.Strings(allowed)
	return allowed
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// dependencySatisfiedForRealtime checks if a dependency is satisfied for realtime Stories.
//
// Behavior:
//   - Returns false for non-realtime Stories (batch mode requires completion).
//   - For realtime, Pending/Running/Paused deps are considered satisfied.
//   - Terminal deps are satisfied only if Succeeded.
//
// Arguments:
//   - story *bubuv1alpha1.Story: the Story definition.
//   - depState runsv1alpha1.StepState: the dependency step's current state.
//
// Returns:
//   - bool: true if the dependency is satisfied for realtime execution.
func (r *DAGReconciler) dependencySatisfiedForRealtime(story *bubuv1alpha1.Story, depState runsv1alpha1.StepState) bool {
	if story == nil || !story.Spec.Pattern.IsRealtime() {
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

// getStoryRunInputs resolves StoryRun inputs via the shared StoryRun-input helper.
//
// Behavior:
//   - Applies defaults from story.Spec.InputsSchema when present.
//
// Arguments:
//   - ctx context.Context: propagated to storage hydration when StoryRun inputs were offloaded.
//   - srun *runsv1alpha1.StoryRun: the StoryRun with spec.inputs.
//   - story *bubuv1alpha1.Story: the Story providing inputsSchema (optional).
//
// Returns:
//   - map[string]any: the decoded and storage-hydrated inputs.
//   - error: nil on success, or decode/hydration errors.
func getStoryRunInputs(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story) (map[string]any, error) {
	return resolveHydratedStoryRunInputs(ctx, srun, story)
}

// shouldFailFast checks if the Story should abort on first step failure.
//
// Behavior:
//   - Reads story.Spec.Policy.Retries.ContinueOnStepFailure.
//   - Returns false (continue) only when ContinueOnStepFailure is explicitly true.
//   - Defaults to fail-fast (Argo-like) when unset.
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
	return true // Default to fail-fast when policy is unset
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
func (r *DAGReconciler) setStoryRunPhase(ctx context.Context, srun *runsv1alpha1.StoryRun, phase enums.Phase, reason, message string) error {
	return runsstatus.PatchStoryRunPhase(ctx, r.Client, srun, phase, reason, message)
}

func (r *DAGReconciler) setStoryRunPhaseWithError(ctx context.Context, srun *runsv1alpha1.StoryRun, phase enums.Phase, reason, message string, statusErr *runtime.RawExtension) error {
	return runsstatus.PatchStoryRunPhaseWithError(ctx, r.Client, srun, phase, reason, message, statusErr)
}

// normalizeStepIdentifier replaces characters not allowed in template identifiers.
//
// Behavior:
//   - Delegates to sanitizeStepIdentifier for consistent aliasing.
//   - Replaces non-alphanumeric characters with underscores.
//
// Arguments:
//   - name string: the step name to normalize.
//
// Returns:
//   - string: the normalized template-safe identifier.
func normalizeStepIdentifier(name string) string {
	return sanitizeStepIdentifier(name)
}
