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
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/observability"
	rec "github.com/bubustack/bobrapet/pkg/reconcile"
	refs "github.com/bubustack/bobrapet/pkg/refs"
	childrun "github.com/bubustack/bobrapet/pkg/runs/childrun"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	runslist "github.com/bubustack/bobrapet/pkg/runs/list"
	looputil "github.com/bubustack/bobrapet/pkg/runs/loop"
	"github.com/bubustack/core/contracts"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/bubustack/bobrapet/pkg/storage"

	"go.opentelemetry.io/otel/attribute"
)

// StepExecutor is responsible for executing individual steps in a StoryRun.
type StepExecutor struct {
	client.Client
	Scheme         *runtime.Scheme
	CEL            *cel.Evaluator
	ConfigResolver *config.Resolver
	Recorder       record.EventRecorder
}

var (
	manifestLenPattern  = regexp.MustCompile(`len\(\s*steps\.([a-zA-Z0-9_\-]+)\.output((?:\.[a-zA-Z0-9_\-]+|\[[^]]+\])*)\s*\)`)
	manifestSamplePattern = regexp.MustCompile(`sample\(\s*steps\.([a-zA-Z0-9_\-]+)\.output((?:\.[a-zA-Z0-9_\-]+|\[[^]]+\])*)\s*\)`)
	manifestHashPattern   = regexp.MustCompile(`hash_of\(\s*steps\.([a-zA-Z0-9_\-]+)\.output((?:\.[a-zA-Z0-9_\-]+|\[[^]]+\])*)\s*\)`)
	manifestTypePattern   = regexp.MustCompile(`type_of\(\s*steps\.([a-zA-Z0-9_\-]+)\.output((?:\.[a-zA-Z0-9_\-]+|\[[^]]+\])*)\s*\)`)
	manifestPathPattern = regexp.MustCompile(`steps\.([a-zA-Z0-9_\-]+)\.output((?:\.[a-zA-Z0-9_\-]+|\[[^]]+\])*)`)
	stepAliasPattern    = regexp.MustCompile(`steps\.([a-zA-Z0-9_\-]+)`)
)

const (
	manifestRootPath         = "$"
	defaultMaxLoopIterations = 100
)

const (
	eventReasonStepRunCreated           = "ChildStepRunCreated"
	eventReasonStepRunDrift             = "ChildStepRunDrift"
	eventReasonRequestedManifestUpdated = "RequestedManifestUpdated"
	eventReasonSubStoryRunCreated       = "SubStoryRunCreated"
	eventReasonSubStoryRunSpoofed       = "SubStoryRunSpoofed"
)

type loopConfig struct {
	Items       json.RawMessage   `json:"items"`
	Template    bubuv1alpha1.Step `json:"template"`
	BatchSize   *int              `json:"batchSize,omitempty"`
	Concurrency *int              `json:"concurrency,omitempty"`
}

// NewStepExecutor creates a new StepExecutor.
//
// Behavior:
//   - Wraps the provided dependencies for step execution.
//
// Arguments:
//   - k8sClient client.Client: Kubernetes client for CRUD operations.
//   - scheme *runtime.Scheme: scheme for owner references.
//   - celEval *cel.Evaluator: CEL evaluator for expression resolution.
//   - cfgResolver *config.Resolver: configuration resolver.
//   - recorder record.EventRecorder: event recorder for emitting events.
//
// Returns:
//   - *StepExecutor: ready for step execution.
func NewStepExecutor(k8sClient client.Client, scheme *runtime.Scheme, celEval *cel.Evaluator, cfgResolver *config.Resolver, recorder record.EventRecorder) *StepExecutor {
	return &StepExecutor{
		Client:         k8sClient,
		Scheme:         scheme,
		CEL:            celEval,
		ConfigResolver: cfgResolver,
		Recorder:       recorder,
	}
}

// Execute determines the step type and calls the appropriate execution method.
//
// Behavior:
//   - Initializes StepStates if needed.
//   - Dispatches to type-specific executors based on step.Type or step.Ref.
//   - Traces execution with OpenTelemetry spans.
//
// Arguments:
//   - ctx context.Context: propagated to all operations.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *bubuv1alpha1.Story: the Story definition.
//   - step *bubuv1alpha1.Step: the step to execute.
//   - vars map[string]any: variable context for CEL evaluation.
//
// Returns:
//   - error: nil on success, or execution errors.
//
// Side Effects:
//   - Creates/updates StepRuns based on step type.
//   - Updates srun.Status.StepStates for primitive steps.
func (e *StepExecutor) Execute(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, vars map[string]any) (err error) {
	ensureStoryStepStateMap(srun)
	stepType := string(step.Type)
	if stepType == "" {
		stepType = "primitive"
		if step.Ref != nil {
			stepType = "engram"
		}
	}
	ctx, span := observability.StartSpan(ctx, "StepExecutor.Execute",
		attribute.String("namespace", srun.Namespace),
		attribute.String("storyrun", srun.Name),
		attribute.String("story", story.Name),
		attribute.String("step", step.Name),
		attribute.String("step_type", stepType),
		attribute.Int("needs.count", len(step.Needs)),
	)
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
		}
	}()

	// An Engram step is defined by the presence of the 'ref' field.
	if step.Ref != nil {
		return e.executeEngramStep(ctx, srun, story, step)
	}

	// If 'ref' is not present, a 'type' must be specified.
	switch step.Type {
	case enums.StepTypeExecuteStory:
		return e.executeStoryStep(ctx, srun, step, vars)
	case enums.StepTypeLoop:
		return e.executeLoopStep(ctx, srun, story, step, vars)
	case enums.StepTypeParallel:
		return e.executeParallelStep(ctx, srun, story, step, vars)
	case enums.StepTypeCondition, enums.StepTypeSwitch, enums.StepTypeSetData, enums.StepTypeTransform, enums.StepTypeFilter, enums.StepTypeMergeData:
		srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseSucceeded, Message: "Primitive evaluated and outputs are available."}
		return nil
	case enums.StepTypeSleep:
		srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseSucceeded, Message: "Sleep completed."}
		return nil
	case enums.StepTypeStop:
		return e.executeStopStep(ctx, srun, step)
	default:
		return fmt.Errorf("step '%s' has an unsupported type '%s' or is missing a 'ref'", step.Name, step.Type)
	}
}

// executeEngramStep creates or patches a StepRun for an Engram-based step.
//
// Behavior:
//   - Computes deterministic StepRun name via kubeutil.ComposeName.
//   - Extracts timeout and retry overrides from step.Execution.
//   - Creates StepRun if not found, or patches if already exists.
//
// Arguments:
//   - ctx context.Context: propagated to client operations.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *bubuv1alpha1.Story: the Story definition.
//   - step *bubuv1alpha1.Step: the Engram step with ref.
//
// Returns:
//   - error: nil on success, or client errors.
//
// Side Effects:
//   - Creates or patches a StepRun resource.
func (e *StepExecutor) executeEngramStep(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) error {
	stepName := kubeutil.ComposeName(srun.Name, step.Name)
	requestedManifest := e.computeManifestRequests(story, step.Name)
	desiredTimeout, desiredRetry := extractExecutionOverrides(step)

	var stepRun runsv1alpha1.StepRun
	err := e.Get(ctx, types.NamespacedName{Name: stepName, Namespace: srun.Namespace}, &stepRun)
	if errors.IsNotFound(err) {
		return e.createEngramStepRun(ctx, srun, story, step, stepName, desiredTimeout, desiredRetry, requestedManifest)
	}
	if err != nil {
		return err
	}

	return e.patchEngramStepRun(ctx, &stepRun, desiredTimeout, desiredRetry, requestedManifest)
}

// extractExecutionOverrides extracts timeout and retry from step.Execution.
//
// Behavior:
//   - Returns empty/nil when step.Execution is nil.
//   - Deep copies retry policy to prevent mutation.
//
// Arguments:
//   - step *bubuv1alpha1.Step: the step to extract from.
//
// Returns:
//   - string: the timeout duration string, or empty.
//   - *bubuv1alpha1.RetryPolicy: deep copied retry policy, or nil.
func extractExecutionOverrides(step *bubuv1alpha1.Step) (string, *bubuv1alpha1.RetryPolicy) {
	if step.Execution == nil {
		return "", nil
	}

	var timeout string
	if step.Execution.Timeout != nil {
		timeout = *step.Execution.Timeout
	}

	var retry *bubuv1alpha1.RetryPolicy
	if step.Execution.Retry != nil {
		retry = step.Execution.Retry.DeepCopy()
	}
	return timeout, retry
}

// createEngramStepRun fetches the referenced Engram, merges its `with` defaults
// with the step override, stamps Story/StoryRun labels plus StoryRunRef/EngramRef,
// sets the parent StoryRun as controller owner, and creates the child StepRun
// before logging and emitting a ChildStepRunCreated event
// (internal/controller/runs/step_executor.go:157-288).
//
// Behavior:
//   - Validates the step has a non-nil Engram ref, loads the Engram via
//     step.Ref.ToNamespacedName, and merges Engram + step `with` blocks
//     via kubeutil.MergeWithBlocks (internal/controller/runs/step_executor.go:205-219).
//   - Builds the StepRun spec demanded by the Stage-to-stage binding guard
//     (StoryRunRef, Story/StoryRun labels, StepID, EngramRef, timeout, retry,
//     requested manifests, merged input) and applies controller ownership
//     so cleanup + downstream target helpers stay in sync
//     (internal/controller/runs/step_executor.go:221-249; controllers/_duplication_candidates.md:22-30).
//   - Issues the Create call (with event + log) and falls back to a refetch +
//     patch when the child already exists (internal/controller/runs/step_executor.go:243-288).
//
// Args:
//   - ctx/srun/story/step: reconcile context and parent metadata.
//   - stepName: deterministic child name from kubeutil.ComposeName.
//   - timeout/retry/requestedManifest: execution overrides propagated to the child spec.
//
// Returns: error if the Engram fetch, merge, owner reference, Create, or patch fails.
//
// Side Effects: Reads the Engram, creates a child StepRun owned by the StoryRun,
// and emits a Normal event for observability.
func (e *StepExecutor) createEngramStepRun(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	step *bubuv1alpha1.Step,
	stepName string,
	timeout string,
	retry *bubuv1alpha1.RetryPolicy,
	requestedManifest []runsv1alpha1.ManifestRequest,
) error {
	log := logging.NewReconcileLogger(ctx, "step-executor").WithValues("storyrun", srun.Name, "step", stepName)
	if step.Ref == nil {
		return fmt.Errorf("step '%s' is missing a 'ref'", step.Name)
	}

	engramKey := step.Ref.ToNamespacedName(srun)
	var engram bubuv1alpha1.Engram
	if err := e.Get(ctx, engramKey, &engram); err != nil {
		return fmt.Errorf("failed to get engram '%s' for step '%s': %w", step.Ref.Name, step.Name, err)
	}

	log.V(1).Info("Merging Engram defaults with step override",
		"engram", engramKey.String(),
		"storyRun", srun.Name,
		"step", step.Name,
	)
	mergedWith, err := kubeutil.MergeWithBlocks(engram.Spec.With, step.With)
	if err != nil {
		return fmt.Errorf("failed to merge 'with' blocks for step '%s': %w", step.Name, err)
	}
	maxInline := e.maxInlineSizeForStep(step.Execution)
	mergedWith, err = e.maybeOffloadStepRunInput(ctx, srun, stepName, maxInline, mergedWith)
	if err != nil {
		return err
	}

	labels := runsidentity.SelectorLabels(srun.Name)
	labels[contracts.StoryNameLabelKey] = story.Name
	stepRun := runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stepName,
			Namespace: srun.Namespace,
			Labels:    labels,
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: srun.Name},
			},
			StepID:            step.Name,
			EngramRef:         step.Ref,
			Input:             mergedWith,
			Timeout:           timeout,
			Retry:             retry,
			RequestedManifest: requestedManifest,
		},
	}

	if err := controllerutil.SetControllerReference(srun, &stepRun, e.Scheme); err != nil {
		return err
	}
	if err := e.Create(ctx, &stepRun); err != nil {
		if errors.IsAlreadyExists(err) {
			var existing runsv1alpha1.StepRun
			if getErr := e.Get(ctx, types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, &existing); getErr != nil {
				return fmt.Errorf("failed to fetch StepRun '%s' after AlreadyExists: %w", stepRun.Name, getErr)
			}
			if drift := describeStepRunDrift(&existing, srun, story, step); len(drift) > 0 {
				reason := strings.Join(drift, "; ")
				log.Info("Detected immutable StepRun drift", "childStepRun", existing.Name, "drift", reason)
				if e.Recorder != nil {
					e.Recorder.Eventf(
						srun,
						corev1.EventTypeWarning,
						eventReasonStepRunDrift,
						"Refusing to patch StepRun %s because controller-owned references drifted: %s",
						existing.Name,
						reason,
					)
				}
				return fmt.Errorf("detected immutable StepRun drift for %s: %s", existing.Name, reason)
			}
			if patchErr := e.patchEngramStepRun(ctx, &existing, timeout, retry, requestedManifest); patchErr != nil {
				return fmt.Errorf("failed to patch StepRun '%s' after AlreadyExists: %w", stepRun.Name, patchErr)
			}
			log.V(1).Info("Patched existing StepRun after AlreadyExists",
				"childStepRun", stepRun.Name,
				"engram", step.Ref.Name,
				"engramNamespace", engramKey.Namespace)
			return nil
		}
		return err
	}
	metrics.RecordChildStepRunCreated(srun.Namespace, srun.Name, step.Ref.Name)
	log.Info("Created child StepRun for Engram",
		"childStepRun", stepRun.Name,
		"engram", step.Ref.Name,
		"engramNamespace", engramKey.Namespace,
		"operationResult", controllerutil.OperationResultCreated)
	if e.Recorder != nil {
		e.Recorder.Eventf(
			srun,
			corev1.EventTypeNormal,
			eventReasonStepRunCreated,
			"Created StepRun %s for Engram %s/%s",
			stepRun.Name,
			engramKey.Namespace,
			step.Ref.Name,
		)
	}
	return nil
}

// describeStepRunDrift identifies immutable field drift between existing and desired StepRun.
//
// Behavior:
//   - Compares StoryRunRef, StepID, EngramRef against expected values.
//   - Returns list of drift descriptions, empty if no drift.
//
// Arguments:
//   - existing *runsv1alpha1.StepRun: the existing StepRun to check.
//   - srun *runsv1alpha1.StoryRun: expected parent StoryRun.
//   - story *bubuv1alpha1.Story: expected Story (unused but kept for symmetry).
//   - step *bubuv1alpha1.Step: expected step definition.
//
// Returns:
//   - []string: list of drift descriptions.
func describeStepRunDrift(existing *runsv1alpha1.StepRun, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) []string {
	var reasons []string
	if srun != nil {
		if existing.Spec.StoryRunRef.Name != srun.Name {
			reasons = append(reasons, fmt.Sprintf("StoryRunRef=%s expected %s", existing.Spec.StoryRunRef.Name, srun.Name))
		}
		if label := existing.GetLabels()[contracts.StoryRunLabelKey]; label != srun.Name {
			reasons = append(reasons, fmt.Sprintf("%s label=%s expected %s", contracts.StoryRunLabelKey, label, srun.Name))
		}
	}
	if story != nil {
		if label := existing.GetLabels()[contracts.StoryNameLabelKey]; label != story.Name {
			reasons = append(reasons, fmt.Sprintf("%s label=%s expected %s", contracts.StoryNameLabelKey, label, story.Name))
		}
	}
	if step != nil {
		if existing.Spec.StepID != step.Name {
			reasons = append(reasons, fmt.Sprintf("StepID=%s expected %s", existing.Spec.StepID, step.Name))
		}
		if !engramRefsEqual(existing.Spec.EngramRef, step.Ref) {
			reasons = append(reasons, fmt.Sprintf("EngramRef %+v expected %+v", existing.Spec.EngramRef, step.Ref))
		}
	}
	return reasons
}

// engramRefsEqual compares two EngramReference pointers for equality.
//
// Behavior:
//   - Returns true if both nil, false if only one is nil.
//   - Compares Name, Namespace, and UID fields.
//
// Arguments:
//   - a, b *refs.EngramReference: references to compare.
//
// Returns:
//   - bool: true if equal.
func engramRefsEqual(a, b *refs.EngramReference) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Name != b.Name {
		return false
	}
	if !stringPtrEqual(a.Namespace, b.Namespace) {
		return false
	}
	switch {
	case a.UID == nil && b.UID == nil:
		return true
	case a.UID != nil && b.UID != nil:
		return *a.UID == *b.UID
	default:
		return false
	}
}

// stringPtrEqual compares two string pointers for equality.
//
// Behavior:
//   - Returns true if both nil or both have equal values.
//
// Arguments:
//   - a, b *string: pointers to compare.
//
// Returns:
//   - bool: true if equal.
func stringPtrEqual(a, b *string) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a != nil && b != nil:
		return *a == *b
	default:
		return false
	}
}

// patchEngramStepRun reconciles an existing StepRun's timeout, retry policy,
// and requested manifests. It deep-copies the current spec, compares the desired
// values, and issues a MergeFrom patch when any field changed
// (internal/controller/runs/step_executor.go:231-301).
//
// Args:
//   - ctx: reconcile context forwarded to controller-runtime.
//   - stepRun: live StepRun fetched earlier.
//   - timeout/retry/requestedManifest: desired spec fields computed by executeEngramStep.
//
// Returns: nil when no changes are needed or the patch succeeds; otherwise the
// patch error for controller-runtime to retry.
//
// Notes:
//   - Empty requestedManifest slices clear the field so Stories can remove manifest
//     requests without deleting the StepRun (lines 291-299).
//   - Input, EngramRef, and labels remain immutable; those fields still rely on
//     createEngramStepRun + Stage guard coordination (controllers/_duplication_candidates.md:19-21).
func (e *StepExecutor) patchEngramStepRun(
	ctx context.Context,
	stepRun *runsv1alpha1.StepRun,
	timeout string,
	retry *bubuv1alpha1.RetryPolicy,
	requestedManifest []runsv1alpha1.ManifestRequest,
) error {
	log := logging.NewReconcileLogger(ctx, "step-executor").
		WithValues("steprun", stepRun.Name, "step", stepRun.Spec.StepID)
	before := stepRun.DeepCopy()
	timeoutChanged, retryChanged := childrun.UpdateTimeoutAndRetry(stepRun, timeout, retry)
	manifestChanged := childrun.UpdateRequestedManifest(&stepRun.Spec.RequestedManifest, requestedManifest, false)
	needsPatch := timeoutChanged || retryChanged || manifestChanged

	if !needsPatch {
		return nil
	}

	if err := e.Patch(ctx, stepRun, client.MergeFrom(before)); err != nil {
		return err
	}

	if timeoutChanged || retryChanged {
		log.V(1).Info("Updated StepRun timeout/retry settings",
			"timeoutChanged", timeoutChanged,
			"retryChanged", retryChanged,
			"previousTimeout", before.Spec.Timeout,
			"currentTimeout", stepRun.Spec.Timeout,
			"previousRetry", before.Spec.Retry,
			"currentRetry", stepRun.Spec.Retry,
		)
	}

	if manifestChanged {
		prevCount := len(before.Spec.RequestedManifest)
		currCount := len(stepRun.Spec.RequestedManifest)
		log.Info("Updated StepRun requested manifests", "previousCount", prevCount, "currentCount", currCount)
		if e.Recorder != nil {
			state := "updated"
			if currCount == 0 {
				state = "cleared"
			}
			e.Recorder.Eventf(
				stepRun,
				corev1.EventTypeNormal,
				eventReasonRequestedManifestUpdated,
				"Requested manifests %s (previous=%d, current=%d)",
				state,
				prevCount,
				currCount,
			)
		}
	}

	return nil
}

// executeLoopStep handles loop-type steps by expanding iterations as StepRuns.
//
// Behavior:
//   - Parses loop config from step.With (items, template, batchSize, concurrency).
//   - Resolves items via CEL evaluation.
//   - Enforces batch size and concurrency limits from config hierarchy.
//   - Creates StepRun for each iteration up to limits.
//   - Tracks loop progress in StepState.Loop.
//
// Arguments:
//   - ctx context.Context: propagated to client operations.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *bubuv1alpha1.Story: the Story definition.
//   - step *bubuv1alpha1.Step: the loop step.
//   - vars map[string]any: variable context for CEL.
//
// Returns:
//   - error: nil on success, or validation/execution errors.
//
// Side Effects:
//   - Creates child StepRuns for loop iterations.
//   - Updates srun.Status.StepStates with loop progress.
func (e *StepExecutor) executeLoopStep(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, vars map[string]any) error {
	execCtx, err := e.buildLoopExecutionContext(ctx, srun, story, step, vars)
	if err != nil {
		return err
	}

	state := execCtx.state
	loopState := execCtx.loopState

	if execCtx.failed > 0 {
		return e.failLoopStep(srun, step.Name, state, execCtx.failed, execCtx.childNames)
	}

	if execCtx.isCompleted() {
		return e.completeLoopStep(srun, step.Name, state, loopState.Total, execCtx.childNames)
	}

	if execCtx.availableSlots() <= 0 {
		return e.waitForLoopSlots(srun, step.Name, state, execCtx.active, execCtx.childNames)
	}

	if execCtx.remainingIterations() <= 0 {
		return e.awaitLoopCompletion(srun, step.Name, state, execCtx.childNames)
	}

	launchBudget := execCtx.launchBudget()
	if launchBudget <= 0 {
		e.storeLoopState(srun, step.Name, *state, execCtx.childNames)
		return nil
	}

	nextIndex, err := e.launchLoopIterationsFromContext(ctx, execCtx, launchBudget, vars)
	if err != nil {
		return err
	}

	state.Phase = enums.PhaseRunning
	state.Message = execCtx.progressMessage()
	loopState.NextIndex = findNextLoopIndex(execCtx.seenIndices, loopState.Total, nextIndex)
	e.storeLoopState(srun, step.Name, *state, execCtx.childNames)
	return nil
}

type loopExecutionContext struct {
	story       *bubuv1alpha1.Story
	step        *bubuv1alpha1.Step
	srun        *runsv1alpha1.StoryRun
	cfg         loopConfig
	items       []any
	engramWith  *runtime.RawExtension
	limits      looputil.Limits
	state       *runsv1alpha1.StepState
	loopState   *runsv1alpha1.LoopState
	seenIndices map[int]struct{}
	completed   int
	active      int
	failed      int
	childNames  []string
}

func (c *loopExecutionContext) remainingIterations() int {
	return c.loopState.Total - c.loopState.Created
}

func (c *loopExecutionContext) availableSlots() int {
	return c.loopState.MaxConcurrency - c.active
}

func (c *loopExecutionContext) launchBudget() int {
	remaining := c.remainingIterations()
	budget := c.loopState.BatchSize
	if budget <= 0 || budget > remaining {
		budget = remaining
	}
	if slots := c.availableSlots(); slots < budget {
		budget = slots
	}
	return budget
}

func (c *loopExecutionContext) isCompleted() bool {
	return c.loopState.Total == 0 || c.loopState.Completed == c.loopState.Total
}

func (c *loopExecutionContext) progressMessage() string {
	return fmt.Sprintf(
		"Loop step '%s' progress %d/%d created, %d completed",
		c.step.Name,
		c.loopState.Created,
		c.loopState.Total,
		c.loopState.Completed,
	)
}

func (e *StepExecutor) buildLoopExecutionContext(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, vars map[string]any) (*loopExecutionContext, error) {
	cfg, err := parseLoopConfig(step)
	if err != nil {
		return nil, err
	}

	var engramWith *runtime.RawExtension
	if cfg.Template.Ref != nil && cfg.Template.Ref.Name != "" {
		engramKey := cfg.Template.Ref.ToNamespacedName(srun)
		var engram bubuv1alpha1.Engram
		if err := e.Get(ctx, engramKey, &engram); err != nil {
			return nil, fmt.Errorf("failed to get engram '%s' for loop step '%s': %w", cfg.Template.Ref.Name, step.Name, err)
		}
		engramWith = engram.Spec.With
	}

	itemsSpec, err := parseLoopItems(step.Name, cfg)
	if err != nil {
		return nil, err
	}

	resolvedRaw, err := e.resolveLoopItems(ctx, itemsSpec, vars)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve 'items' for loop step '%s': %w", step.Name, err)
	}

	resolvedItems, err := coerceToList(resolvedRaw)
	if err != nil {
		return nil, fmt.Errorf("'items' in loop step '%s' did not resolve to a list: %w", step.Name, err)
	}

	if err := e.enforceLoopIterationLimit(step.Name, len(resolvedItems)); err != nil {
		return nil, err
	}

	limits := e.loopExecutionLimits(story, step, cfg.BatchSize, cfg.Concurrency, len(resolvedItems))
	stateValue := e.ensureLoopStepState(srun, step.Name, len(resolvedItems), limits)
	loopState := stateValue.Loop

	children, err := e.listLoopStepRuns(ctx, srun, step.Name)
	if err != nil {
		return nil, err
	}
	seenIndices, completed, active, failed, childNames := summarizeLoopChildren(children, loopState.Total)
	loopState.Created = len(seenIndices)
	loopState.Completed = completed
	loopState.BatchSize = limits.BatchSize
	loopState.MaxConcurrency = limits.MaxConcurrency
	loopState.NextIndex = findNextLoopIndex(seenIndices, loopState.Total, loopState.NextIndex)

	return &loopExecutionContext{
		story:       story,
		step:        step,
		srun:        srun,
		cfg:         cfg,
		items:       resolvedItems,
		engramWith:  engramWith,
		limits:      limits,
		state:       &stateValue,
		loopState:   loopState,
		seenIndices: seenIndices,
		completed:   completed,
		active:      active,
		failed:      failed,
		childNames:  childNames,
	}, nil
}

func parseLoopConfig(step *bubuv1alpha1.Step) (loopConfig, error) {
	if step.With == nil {
		return loopConfig{}, fmt.Errorf("loop step '%s' is missing a 'with' block", step.Name)
	}
	var cfg loopConfig
	if err := json.Unmarshal(step.With.Raw, &cfg); err != nil {
		return loopConfig{}, fmt.Errorf("failed to parse 'with' for loop step '%s': %w", step.Name, err)
	}
	if cfg.Template.Ref == nil {
		return loopConfig{}, fmt.Errorf("loop step '%s' template is missing an engram ref", step.Name)
	}
	if cfg.Template.Name == "" {
		return loopConfig{}, fmt.Errorf("loop step '%s' template must declare a name", step.Name)
	}
	return cfg, nil
}

func parseLoopItems(stepName string, cfg loopConfig) (any, error) {
	var itemsSpec any
	if err := json.Unmarshal(cfg.Items, &itemsSpec); err != nil {
		return nil, fmt.Errorf("failed to parse 'items' for loop step '%s': %w", stepName, err)
	}
	return itemsSpec, nil
}

func (e *StepExecutor) enforceLoopIterationLimit(stepName string, count int) error {
	maxIterations := defaultMaxLoopIterations
	if e.ConfigResolver != nil {
		if cfg := e.ConfigResolver.GetOperatorConfig(); cfg != nil && cfg.Controller.MaxLoopIterations > 0 {
			maxIterations = cfg.Controller.MaxLoopIterations
		}
	}
	if count > maxIterations {
		return fmt.Errorf("loop step '%s' exceeds maximum of %d iterations", stepName, maxIterations)
	}
	return nil
}

func (e *StepExecutor) failLoopStep(srun *runsv1alpha1.StoryRun, stepName string, state *runsv1alpha1.StepState, failed int, childNames []string) error {
	state.Phase = enums.PhaseFailed
	state.Message = fmt.Sprintf("Loop step '%s' blocked by %d failed iterations", stepName, failed)
	state.Loop = nil
	e.storeLoopState(srun, stepName, *state, childNames)
	return fmt.Errorf("loop step '%s' has %d failed iterations", stepName, failed)
}

func (e *StepExecutor) completeLoopStep(srun *runsv1alpha1.StoryRun, stepName string, state *runsv1alpha1.StepState, total int, childNames []string) error {
	state.Phase = enums.PhaseSucceeded
	state.Message = fmt.Sprintf("Loop step '%s' completed %d iterations", stepName, total)
	state.Loop = nil
	e.storeLoopState(srun, stepName, *state, childNames)
	return nil
}

func (e *StepExecutor) waitForLoopSlots(srun *runsv1alpha1.StoryRun, stepName string, state *runsv1alpha1.StepState, active int, childNames []string) error {
	state.Phase = enums.PhaseRunning
	state.Message = fmt.Sprintf("Loop step '%s' waiting for %d in-flight iterations", stepName, active)
	e.storeLoopState(srun, stepName, *state, childNames)
	return nil
}

func (e *StepExecutor) awaitLoopCompletion(srun *runsv1alpha1.StoryRun, stepName string, state *runsv1alpha1.StepState, childNames []string) error {
	state.Message = fmt.Sprintf("Loop step '%s' awaiting completion", stepName)
	e.storeLoopState(srun, stepName, *state, childNames)
	return nil
}

func (e *StepExecutor) launchLoopIterationsFromContext(ctx context.Context, execCtx *loopExecutionContext, budget int, vars map[string]any) (int, error) {
	nextIndex := execCtx.loopState.NextIndex
	launched := 0
	for launched < budget && nextIndex < execCtx.loopState.Total {
		if _, exists := execCtx.seenIndices[nextIndex]; exists {
			nextIndex++
			continue
		}
		childName, err := e.launchLoopIteration(ctx, execCtx.srun, execCtx.story, execCtx.step, &execCtx.cfg, execCtx.engramWith, execCtx.items[nextIndex], vars, nextIndex, execCtx.loopState.Total)
		if err != nil {
			return nextIndex, err
		}
		execCtx.childNames = appendUnique(execCtx.childNames, childName)
		execCtx.seenIndices[nextIndex] = struct{}{}
		execCtx.loopState.Created++
		launched++
		nextIndex++
	}
	return nextIndex, nil
}

// resolveLoopOverrides collects loop policy overrides from Story and step.
//
// loopExecutionLimits computes effective batch size and concurrency limits.
//
// Behavior:
//   - Starts with controller defaults, applies Story/step overrides.
//   - Applies runtime overrides from loop config.
//   - Clamps values to total and enforces minimum of 1.
//
// Arguments:
//   - story *bubuv1alpha1.Story: Story with optional loop policy.
//   - step *bubuv1alpha1.Step: step with optional loop policy.
//   - batchOverride, concurrencyOverride *int: runtime overrides.
//   - total int: total number of iterations.
//
// Returns:
//   - looputil.Limits: computed batch size and max concurrency.
func (e *StepExecutor) loopExecutionLimits(
	story *bubuv1alpha1.Story,
	step *bubuv1alpha1.Step,
	batchOverride, concurrencyOverride *int,
	total int,
) looputil.Limits {
	ctrlCfg := config.DefaultControllerConfig()
	if e.ConfigResolver != nil {
		if opCfg := e.ConfigResolver.GetOperatorConfig(); opCfg != nil {
			ctrlCfg = &opCfg.Controller
		}
	}

	overrides := looputil.ResolveOverrides(story, step)
	return looputil.ComputeLimits(ctrlCfg, overrides, batchOverride, concurrencyOverride, total)
}

// ensureLoopStepState initializes or updates the StepState for a loop step.
//
// Behavior:
//   - Ensures StepStates map is initialized.
//   - Creates/updates Loop state with total, batch size, and concurrency.
//
// Arguments:
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - stepName string: the loop step name.
//   - total int: total iterations.
//   - limits looputil.Limits: computed execution limits.
//
// Returns:
//   - runsv1alpha1.StepState: the current or initialized state.
//
// Side Effects:
//   - Mutates srun.Status.StepStates.
func (e *StepExecutor) ensureLoopStepState(srun *runsv1alpha1.StoryRun, stepName string, total int, limits looputil.Limits) runsv1alpha1.StepState {
	ensureStepStatesInitialized(srun)
	state, exists := srun.Status.StepStates[stepName]
	if !exists || state.Loop == nil || state.Loop.Total != total {
		state.Phase = enums.PhaseRunning
		state.Message = fmt.Sprintf("Loop step '%s' launching %d iterations", stepName, total)
		state.Loop = &runsv1alpha1.LoopState{
			Total:          total,
			BatchSize:      limits.BatchSize,
			MaxConcurrency: limits.MaxConcurrency,
			NextIndex:      0,
		}
		srun.Status.StepStates[stepName] = state
	}
	return srun.Status.StepStates[stepName]
}

// listLoopStepRuns lists StepRuns for a specific loop step.
//
// Behavior:
//   - Queries StepRuns by StoryRun and ParentStepLabel.
//   - Returns a copy of the items slice.
//
// Arguments:
//   - ctx context.Context: propagated to list operation.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - stepName string: the loop step name.
//
// Returns:
//   - []runsv1alpha1.StepRun: copy of matching StepRuns.
//   - error: nil on success, or list errors.
func (e *StepExecutor) listLoopStepRuns(ctx context.Context, srun *runsv1alpha1.StoryRun, stepName string) ([]runsv1alpha1.StepRun, error) {
	var list runsv1alpha1.StepRunList
	if err := runslist.StepRunsByStoryRun(ctx, e.Client, srun.Namespace, srun.Name, map[string]string{
		contracts.ParentStepLabel: stepName,
	}, &list); err != nil {
		return nil, fmt.Errorf("failed to list loop iterations for step '%s': %w", stepName, err)
	}
	items := make([]runsv1alpha1.StepRun, len(list.Items))
	copy(items, list.Items)
	return items, nil
}

// summarizeLoopChildren analyzes loop iteration StepRuns.
//
// Behavior:
//   - Builds set of seen loop indices.
//   - Counts completed, active, and failed iterations.
//   - Collects child StepRun names.
//
// Arguments:
//   - children []runsv1alpha1.StepRun: the loop iteration StepRuns.
//   - total int: expected total iterations.
//
// Returns:
//   - map[int]struct{}: set of seen indices.
//   - int: completed count.
//   - int: active (non-terminal) count.
//   - int: failed count.
//   - []string: child names.
func summarizeLoopChildren(children []runsv1alpha1.StepRun, total int) (map[int]struct{}, int, int, int, []string) {
	seen := make(map[int]struct{}, len(children))
	completed := 0
	active := 0
	failed := 0
	names := make([]string, 0, len(children))
	for i := range children {
		child := &children[i]
		names = append(names, child.Name)
		if idx, ok := parseLoopIndex(child.Labels); ok && (total <= 0 || idx < total) {
			seen[idx] = struct{}{}
		}
		if child.Status.Phase == enums.PhaseSucceeded {
			completed++
		}
		if !child.Status.Phase.IsTerminal() {
			active++
		} else if child.Status.Phase != enums.PhaseSucceeded {
			failed++
		}
	}
	return seen, completed, active, failed, names
}

// parseLoopIndex extracts the loop index from StepRun labels.
//
// Behavior:
//   - Parses contracts.LoopIndexLabelKey as an integer.
//   - Returns (0, false) if label is missing or non-numeric.
//
// Arguments:
//   - labels map[string]string: StepRun labels.
//
// Returns:
//   - int: the loop index.
//   - bool: true if successfully parsed.
func parseLoopIndex(labels map[string]string) (int, bool) {
	if labels == nil {
		return 0, false
	}
	val, ok := labels[contracts.LoopIndexLabelKey]
	if !ok {
		return 0, false
	}
	idx, err := strconv.Atoi(val)
	if err != nil {
		return 0, false
	}
	return idx, true
}

// findNextLoopIndex finds the next unclaimed loop index.
//
// Behavior:
//   - Starts from 'start' and scans for first index not in 'seen'.
//   - Returns 'total' when all indices are claimed.
//
// Arguments:
//   - seen map[int]struct{}: set of already-used indices.
//   - total int: total number of iterations.
//   - start int: starting index to search from.
//
// Returns:
//   - int: next available index, or 'total' if none.
func findNextLoopIndex(seen map[int]struct{}, total int, start int) int {
	if start < 0 {
		start = 0
	}
	for i := start; i < total; i++ {
		if _, exists := seen[i]; !exists {
			return i
		}
	}
	return total
}

// launchLoopIteration creates a StepRun for a single loop iteration.
//
// Behavior:
//   - Resolves loop template 'with' block with item/index/total context.
//   - Computes manifest requests for the template.
//   - Creates/updates the child StepRun.
//
// Arguments:
//   - ctx context.Context: propagated to client operations.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *bubuv1alpha1.Story: the Story definition.
//   - step *bubuv1alpha1.Step: the loop step.
//   - loopCfg *loopConfig: parsed loop configuration.
//   - item any: the current iteration item.
//   - vars map[string]any: base variable context.
//   - iteration int: current iteration index.
//   - total int: total iterations.
//
// Returns:
//   - string: the created StepRun name.
//   - error: nil on success, or creation errors.
func (e *StepExecutor) launchLoopIteration(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	story *bubuv1alpha1.Story,
	step *bubuv1alpha1.Step,
	loopCfg *loopConfig,
	engramWith *runtime.RawExtension,
	item any,
	vars map[string]any,
	iteration int,
	total int,
) (string, error) {
	loopVars := map[string]any{
		"inputs": vars["inputs"],
		"steps":  vars["steps"],
		"item":   item,
		"index":  iteration,
		"total":  total,
	}
	resolvedWithBytes, err := e.resolveTemplateWith(ctx, loopCfg.Template.With, loopVars)
	if err != nil {
		return "", fmt.Errorf("failed to resolve 'with' for loop iteration %d in step '%s': %w", iteration, step.Name, err)
	}
	if engramWith != nil {
		mergedWith, mergeErr := kubeutil.MergeWithBlocks(engramWith, resolvedWithBytes)
		if mergeErr != nil {
			return "", fmt.Errorf("failed to merge engram defaults for loop iteration %d in step '%s': %w", iteration, step.Name, mergeErr)
		}
		resolvedWithBytes = mergedWith
	}
	childName := kubeutil.ComposeName(srun.Name, step.Name, strconv.Itoa(iteration))
	maxInline := e.maxInlineSizeForStep(loopCfg.Template.Execution)
	resolvedWithBytes, err = e.maybeOffloadStepRunInput(ctx, srun, childName, maxInline, resolvedWithBytes)
	if err != nil {
		return "", err
	}

	requestedManifest := e.computeManifestRequests(story, loopCfg.Template.Name)
	if resolvedWithBytes == nil {
		resolvedWithBytes = &runtime.RawExtension{}
	}
	loopLabels := runsidentity.SelectorLabels(srun.Name)
	loopLabels[contracts.ParentStepLabel] = step.Name
	loopLabels[contracts.LoopIndexLabelKey] = strconv.Itoa(iteration)
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childName,
			Namespace: srun.Namespace,
			Labels:    loopLabels,
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef:       refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: srun.Name}},
			StepID:            loopCfg.Template.Name,
			EngramRef:         loopCfg.Template.Ref,
			Input:             resolvedWithBytes,
			RequestedManifest: requestedManifest,
		},
	}
	if err := e.createOrUpdateChildStepRun(ctx, srun, stepRun); err != nil {
		return "", err
	}
	return childName, nil
}

func (e *StepExecutor) maybeOffloadStepRunInput(ctx context.Context, srun *runsv1alpha1.StoryRun, stepName string, maxInlineBytes int, input *runtime.RawExtension) (*runtime.RawExtension, error) {
	if input == nil || len(input.Raw) == 0 {
		return input, nil
	}

	if maxInlineBytes <= 0 || len(input.Raw) <= maxInlineBytes {
		return input, nil
	}

	return nil, fmt.Errorf("step input exceeds %d bytes; provide a storage reference instead of inlining", maxInlineBytes)
}

func defaultMaxInlineSize(resolver *config.Resolver) int {
	if resolver == nil {
		return storage.DefaultMaxInlineSize
	}
	cfg := resolver.GetOperatorConfig()
	if cfg == nil {
		return storage.DefaultMaxInlineSize
	}
	maxInline := cfg.Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize
	if maxInline < 0 {
		return storage.DefaultMaxInlineSize
	}
	return maxInline
}

func (e *StepExecutor) maxInlineSizeForStep(overrides *bubuv1alpha1.ExecutionOverrides) int {
	if overrides != nil && overrides.MaxInlineSize != nil {
		return *overrides.MaxInlineSize
	}
	return defaultMaxInlineSize(e.ConfigResolver)
}

// storeLoopState persists the StepState and primitive-child names for a loop step.
//
// Behavior:
//   - Ensures StepStates is initialized.
//   - Clears or sets primitive children based on Loop state.
//   - Updates srun.Status.StepStates.
//
// Arguments:
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - stepName string: the loop step name.
//   - state runsv1alpha1.StepState: the state to persist.
//   - childNames []string: names of child StepRuns.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates srun.Status.StepStates and PrimitiveChildren.
func (e *StepExecutor) storeLoopState(srun *runsv1alpha1.StoryRun, stepName string, state runsv1alpha1.StepState, childNames []string) {
	ensureStepStatesInitialized(srun)
	if state.Loop == nil {
		clearPrimitiveChildren(srun, stepName)
	} else {
		setPrimitiveChildren(srun, stepName, childNames)
	}
	srun.Status.StepStates[stepName] = state
}

// appendUnique appends a candidate to values if not already present.
//
// Behavior:
//   - Linear scan for existing value.
//   - Returns original slice if found, else appends.
//
// Arguments:
//   - values []string: existing values.
//   - candidate string: value to add.
//
// Returns:
//   - []string: updated slice.
func appendUnique(values []string, candidate string) []string {
	for _, existing := range values {
		if existing == candidate {
			return values
		}
	}
	return append(values, candidate)
}

// executeParallelStep handles parallel-type steps by launching all branches.
//
// Behavior:
//   - Parses parallel config from step.With.
//   - Creates a StepRun for each parallel branch.
//   - Stores child names in PrimitiveChildren.
//
// Arguments:
//   - ctx context.Context: propagated to client operations.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *bubuv1alpha1.Story: the Story definition.
//   - step *bubuv1alpha1.Step: the parallel step.
//   - vars map[string]any: variable context for CEL.
//
// Returns:
//   - error: nil on success, or creation errors.
//
// Side Effects:
//   - Creates child StepRuns.
//   - Updates srun.Status.StepStates and PrimitiveChildren.
func (e *StepExecutor) executeParallelStep(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, vars map[string]any) error {
	type parallelWith struct {
		Steps []bubuv1alpha1.Step `json:"steps"`
	}
	var parallelCfg parallelWith
	if err := json.Unmarshal(step.With.Raw, &parallelCfg); err != nil {
		return fmt.Errorf("failed to parse 'with' for parallel step '%s': %w", step.Name, err)
	}

	childStepRunNames := make([]string, 0, len(parallelCfg.Steps))
	for _, childStep := range parallelCfg.Steps {
		childStepName := kubeutil.ComposeName(srun.Name, step.Name, childStep.Name)
		childStepRunNames = append(childStepRunNames, childStepName)

		resolvedWithBytes, err := e.resolveTemplateWith(ctx, childStep.With, vars)
		if err != nil {
			return fmt.Errorf("failed to resolve 'with' for parallel branch '%s' in step '%s': %w", childStep.Name, step.Name, err)
		}

		requestedManifest := e.computeManifestRequests(story, childStep.Name)

		childLabels := runsidentity.SelectorLabels(srun.Name)
		childLabels[contracts.ParentStepLabel] = step.Name
		stepRun := &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      childStepName,
				Namespace: srun.Namespace,
				Labels:    childLabels,
			},
			Spec: runsv1alpha1.StepRunSpec{
				StoryRunRef:       refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: srun.Name}},
				StepID:            childStep.Name,
				EngramRef:         childStep.Ref,
				Input:             resolvedWithBytes,
				RequestedManifest: requestedManifest,
			},
		}
		if err := e.createOrUpdateChildStepRun(ctx, srun, stepRun); err != nil {
			return err
		}
	}

	setPrimitiveChildren(srun, step.Name, childStepRunNames)
	srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseRunning, Message: "Parallel block expanded"}
	return nil
}

// resolveLoopItems evaluates loop items via CEL when needed.
//
// Behavior:
//   - For maps: resolves via CEL and extracts "items" or "value" key.
//   - For arrays: returns as-is.
//   - For strings: evaluates as CEL expression.
//   - Returns error for nil items.
//
// Arguments:
//   - ctx context.Context: propagated to CEL evaluation.
//   - raw any: the raw items specification.
//   - vars map[string]any: variable context for CEL.
//
// Returns:
//   - any: the resolved items (typically []any).
//   - error: nil on success, or evaluation errors.
func (e *StepExecutor) resolveLoopItems(ctx context.Context, raw any, vars map[string]any) (any, error) {
	switch typed := raw.(type) {
	case map[string]any:
		resolved, err := e.CEL.ResolveWithInputs(ctx, typed, vars)
		if err != nil {
			return nil, err
		}
		if val, ok := resolved["items"]; ok {
			return val, nil
		}
		if val, ok := resolved["value"]; ok {
			return val, nil
		}
		if len(resolved) == 1 {
			for _, v := range resolved {
				return v, nil
			}
		}
		return resolved, nil
	case []any:
		return typed, nil
	case string:
		resolved, err := e.CEL.ResolveWithInputs(ctx, map[string]any{"value": typed}, vars)
		if err != nil {
			return nil, err
		}
		return resolved["value"], nil
	case nil:
		return nil, fmt.Errorf("'items' cannot be null")
	default:
		return typed, nil
	}
}

// createOrUpdateChildStepRun creates or patches a child StepRun.
//
// Behavior:
//   - Sets StoryRun as controller owner.
//   - Creates if not exists, or patches if spec differs.
//   - Patches: StepID, StoryRunRef, EngramRef, Input, RequestedManifest.
//
// Arguments:
//   - ctx context.Context: propagated to client operations.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - desired *runsv1alpha1.StepRun: the desired StepRun spec.
//
// Returns:
//   - error: nil on success, or client errors.
//
// Side Effects:
//   - Creates or patches a StepRun resource.
func (e *StepExecutor) createOrUpdateChildStepRun(ctx context.Context, srun *runsv1alpha1.StoryRun, desired *runsv1alpha1.StepRun) error {
	if err := controllerutil.SetControllerReference(srun, desired, e.Scheme); err != nil {
		return err
	}

	if err := e.Create(ctx, desired); err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}

		var existing runsv1alpha1.StepRun
		if getErr := e.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, &existing); getErr != nil {
			return getErr
		}

		before := existing.DeepCopy()
		specChanged := rec.UpdateStepRunSpec(&existing, desired)

		ownerBefore := before.GetOwnerReferences()
		if err := controllerutil.SetControllerReference(srun, &existing, e.Scheme); err != nil {
			return err
		}
		ownerChanged := !reflect.DeepEqual(ownerBefore, existing.GetOwnerReferences())

		if specChanged || ownerChanged {
			if patchErr := e.Patch(ctx, &existing, client.MergeFrom(before)); patchErr != nil {
				return patchErr
			}
			logger := logging.NewReconcileLogger(ctx, "step-executor").
				WithValues("namespace", desired.Namespace, "child", desired.Name)
			if srun != nil {
				logger = logger.WithValues("storyrun", srun.Name)
			}
			logger.V(1).Info("Patched existing StepRun to match desired spec", "specChanged", specChanged, "ownerChanged", ownerChanged)
		}

		return nil
	}
	return nil
}

// resolveTemplateWith evaluates a 'with' block via CEL.
//
// Behavior:
//   - Returns nil for nil or empty input.
//   - Unmarshals, evaluates via CEL, and re-marshals.
//
// Arguments:
//   - ctx context.Context: propagated to CEL evaluation.
//   - raw *runtime.RawExtension: the 'with' block to evaluate.
//   - vars map[string]any: variable context for CEL.
//
// Returns:
//   - *runtime.RawExtension: the resolved 'with' block.
//   - error: nil on success, or evaluation errors.
func (e *StepExecutor) resolveTemplateWith(ctx context.Context, raw *runtime.RawExtension, vars map[string]any) (*runtime.RawExtension, error) {
	if raw == nil || len(raw.Raw) == 0 {
		return nil, nil
	}

	withVars := make(map[string]any)
	if err := json.Unmarshal(raw.Raw, &withVars); err != nil {
		return nil, fmt.Errorf("failed to parse 'with' block: %w", err)
	}

	resolved, err := e.CEL.ResolveWithInputs(ctx, withVars, vars)
	if err != nil {
		return nil, err
	}

	encoded, err := json.Marshal(resolved)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal resolved 'with' block: %w", err)
	}

	return &runtime.RawExtension{Raw: encoded}, nil
}

// executeStopStep handles stop-type steps that terminate the StoryRun.
//
// Behavior:
//   - Parses optional phase and message from step.With.
//   - Defaults to Succeeded phase if not specified.
//   - Updates StoryRun phase and message.
//
// Arguments:
//   - ctx context.Context: propagated to logging.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - step *bubuv1alpha1.Step: the stop step.
//
// Returns:
//   - error: nil on success, or parsing errors.
//
// Side Effects:
//   - Mutates srun.Status.Phase, Message, and StepStates.
func (e *StepExecutor) executeStopStep(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubuv1alpha1.Step) error {
	log := logging.NewReconcileLogger(ctx, "step-executor").WithValues("storyrun", srun.Name, "step", step.Name)

	type stopWith struct {
		Phase   enums.Phase `json:"phase"`
		Message string      `json:"message"`
	}
	var stopCfg stopWith

	if step.With != nil {
		if err := json.Unmarshal(step.With.Raw, &stopCfg); err != nil {
			return fmt.Errorf("failed to parse 'with' for stop step '%s': %w", step.Name, err)
		}
	}

	if stopCfg.Phase == "" {
		stopCfg.Phase = enums.PhaseSucceeded
	}
	if stopCfg.Message == "" {
		stopCfg.Message = fmt.Sprintf("Story execution stopped by step '%s' with phase '%s'", step.Name, stopCfg.Phase)
	}

	log.Info("Executing stop step", "phase", stopCfg.Phase, "message", stopCfg.Message)
	srun.Status.Phase = stopCfg.Phase
	srun.Status.Message = stopCfg.Message
	srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: stopCfg.Phase, Message: stopCfg.Message}
	return nil
}

// executeStoryStep handles executeStory-type steps that spawn sub-StoryRuns.
//
// Behavior:
//   - Parses storyRef, waitForCompletion, and inputs from step.With.
//   - Fetches the target Story.
//   - Resolves input expressions via CEL.
//   - Creates/fetches the sub-StoryRun.
//   - Updates parent StepState with sub-run reference.
//
// Arguments:
//   - ctx context.Context: propagated to client and CEL operations.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - step *bubuv1alpha1.Step: the executeStory step.
//   - vars map[string]any: variable context for CEL.
//
// Returns:
//   - error: nil on success, or parsing/creation errors.
//
// Side Effects:
//   - Creates a child StoryRun.
//   - Updates srun.Status.StepStates.
func (e *StepExecutor) executeStoryStep(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubuv1alpha1.Step, vars map[string]any) error {
	log := logging.NewReconcileLogger(ctx, "step-executor").WithValues("storyrun", srun.Name, "step", step.Name)

	ensureStoryStepStateMap(srun)

	cfg, err := parseExecuteStoryConfig(step)
	if err != nil {
		return err
	}

	targetStory, err := e.getTargetStory(ctx, srun, cfg.storyRef, step.Name)
	if err != nil {
		return err
	}

	inputs, err := e.resolveStoryStepInputs(ctx, cfg.rawInputs, vars, step.Name)
	if err != nil {
		return err
	}

	subRun, err := e.ensureSubStoryRun(ctx, srun, step, targetStory, inputs, cfg.waitForCompletion, log)
	if err != nil {
		return err
	}

	updateStoryStepState(srun, step, subRun, cfg.waitForCompletion)
	return nil
}

type executeStoryConfig struct {
	storyRef          *refs.ObjectReference
	waitForCompletion bool
	rawInputs         *runtime.RawExtension
}

// parseExecuteStoryConfig extracts executeStory configuration from step.With.
//
// Behavior:
//   - Requires step.With to be present.
//   - Parses storyRef (required), waitForCompletion (default true), and with block.
//
// Arguments:
//   - step *bubuv1alpha1.Step: the executeStory step.
//
// Returns:
//   - executeStoryConfig: parsed configuration.
//   - error: nil on success, or validation/parsing errors.
func parseExecuteStoryConfig(step *bubuv1alpha1.Step) (executeStoryConfig, error) {
	if step.With == nil {
		return executeStoryConfig{}, fmt.Errorf("story step '%s' requires a 'with' block", step.Name)
	}

	type executeStoryWith struct {
		StoryRef          *refs.ObjectReference `json:"storyRef"`
		WaitForCompletion *bool                 `json:"waitForCompletion,omitempty"`
		With              *runtime.RawExtension `json:"with,omitempty"`
	}

	var raw executeStoryWith
	if err := json.Unmarshal(step.With.Raw, &raw); err != nil {
		return executeStoryConfig{}, fmt.Errorf("failed to unmarshal step 'with' block for story step '%s': %w", step.Name, err)
	}
	if raw.StoryRef == nil || raw.StoryRef.Name == "" {
		return executeStoryConfig{}, fmt.Errorf("story step '%s' is missing a 'storyRef' in its 'with' block", step.Name)
	}
	wait := true
	if raw.WaitForCompletion != nil {
		wait = *raw.WaitForCompletion
	}
	return executeStoryConfig{
		storyRef:          raw.StoryRef,
		waitForCompletion: wait,
		rawInputs:         raw.With,
	}, nil
}

// ensureStoryStepStateMap delegates to ensureStepStatesInitialized for legacy compatibility.
//
// Behavior:
//   - Allocates StepStates map if nil.
//
// Arguments:
//   - srun *runsv1alpha1.StoryRun: the StoryRun to initialize.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates srun.Status.StepStates.
func ensureStoryStepStateMap(srun *runsv1alpha1.StoryRun) {
	ensureStepStatesInitialized(srun)
}

// ensureStepStatesInitialized lazily allocates the StepStates map.
//
// Behavior:
//   - Allocates empty map if nil.
//
// Arguments:
//   - srun *runsv1alpha1.StoryRun: the StoryRun to initialize.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates srun.Status.StepStates.
func ensureStepStatesInitialized(srun *runsv1alpha1.StoryRun) {
	if srun.Status.StepStates == nil {
		srun.Status.StepStates = make(map[string]runsv1alpha1.StepState)
	}
}

// ensurePrimitiveChildrenMap lazily allocates the PrimitiveChildren map.
//
// Behavior:
//   - Allocates empty map if nil.
//
// Arguments:
//   - srun *runsv1alpha1.StoryRun: the StoryRun to initialize.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates srun.Status.PrimitiveChildren.
func ensurePrimitiveChildrenMap(srun *runsv1alpha1.StoryRun) {
	if srun.Status.PrimitiveChildren == nil {
		srun.Status.PrimitiveChildren = make(map[string][]string)
	}
}

// setPrimitiveChildren stores child StepRun names for a step.
//
// Behavior:
//   - Ensures PrimitiveChildren map is initialized.
//   - Overwrites existing entry.
//
// Arguments:
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - stepName string: the step name.
//   - childNames []string: child StepRun names.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates srun.Status.PrimitiveChildren.
func setPrimitiveChildren(srun *runsv1alpha1.StoryRun, stepName string, childNames []string) {
	ensurePrimitiveChildrenMap(srun)
	srun.Status.PrimitiveChildren[stepName] = childNames
}

// clearPrimitiveChildren removes child StepRun names for a step.
//
// Behavior:
//   - No-op if PrimitiveChildren is nil.
//   - Deletes the entry for stepName.
//
// Arguments:
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - stepName string: the step name to clear.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates srun.Status.PrimitiveChildren.
func clearPrimitiveChildren(srun *runsv1alpha1.StoryRun, stepName string) {
	if srun.Status.PrimitiveChildren == nil {
		return
	}
	delete(srun.Status.PrimitiveChildren, stepName)
}

// getTargetStory fetches the target Story for an executeStory step.
//
// Behavior:
//   - Validates ref is non-nil with non-empty name.
//   - Uses refs.LoadNamespacedReference to fetch the Story.
//
// Arguments:
//   - ctx context.Context: propagated to client operations.
//   - srun *runsv1alpha1.StoryRun: provides namespace context.
//   - ref *refs.ObjectReference: the story reference.
//   - stepName string: for error messages.
//
// Returns:
//   - *bubuv1alpha1.Story: the fetched Story.
//   - error: nil on success, or fetch errors.
func (e *StepExecutor) getTargetStory(ctx context.Context, srun *runsv1alpha1.StoryRun, ref *refs.ObjectReference, stepName string) (*bubuv1alpha1.Story, error) {
	if ref == nil || strings.TrimSpace(ref.Name) == "" {
		return nil, fmt.Errorf("story step '%s' missing storyRef name", stepName)
	}
	story, key, err := refs.LoadNamespacedReference(
		ctx,
		e.Client,
		srun,
		ref,
		func() *bubuv1alpha1.Story { return &bubuv1alpha1.Story{} },
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get story '%s' for story step '%s': %w", key.String(), stepName, err)
	}
	return story, nil
}

// resolveStoryStepInputs resolves the inputs for an executeStory step.
//
// Behavior:
//   - Returns nil for nil input.
//   - Delegates to resolveTemplateWith for CEL evaluation.
//
// Arguments:
//   - ctx context.Context: propagated to CEL evaluation.
//   - raw *runtime.RawExtension: the raw inputs.
//   - vars map[string]any: variable context for CEL.
//   - stepName string: for error messages.
//
// Returns:
//   - *runtime.RawExtension: the resolved inputs.
//   - error: nil on success, or resolution errors.
func (e *StepExecutor) resolveStoryStepInputs(ctx context.Context, raw *runtime.RawExtension, vars map[string]any, stepName string) (*runtime.RawExtension, error) {
	if raw == nil {
		return nil, nil
	}
	resolved, err := e.resolveTemplateWith(ctx, raw, vars)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve 'with' block for story step '%s': %w", stepName, err)
	}
	return resolved, nil
}

// ensureSubStoryRun creates or fetches the child StoryRun for an executeStory step.
//
// Behavior:
//   - Uses existing SubStoryRunName from StepState or composes new name.
//   - Copies StoryRef with namespace/UID from target Story.
//   - Sets parent StoryRun as controller owner.
//   - Emits events for creation or ownership warnings.
//
// Arguments:
//   - ctx context.Context: propagated to client operations.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - step *bubuv1alpha1.Step: the executeStory step.
//   - targetStory *bubuv1alpha1.Story: the target Story.
//   - inputs *runtime.RawExtension: resolved inputs for the child.
//   - waitForCompletion bool: whether parent waits for child.
//   - log *logging.ControllerLogger: for logging.
//
// Returns:
//   - *runsv1alpha1.StoryRun: the child StoryRun.
//   - error: nil on success, or creation/validation errors.
//
// Side Effects:
//   - Creates a StoryRun if not exists.
//   - Emits Kubernetes events.
func (e *StepExecutor) ensureSubStoryRun(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	step *bubuv1alpha1.Step,
	targetStory *bubuv1alpha1.Story,
	inputs *runtime.RawExtension,
	waitForCompletion bool,
	log *logging.ControllerLogger,
) (*runsv1alpha1.StoryRun, error) {
	ensureStoryStepStateMap(srun)

	subRunName := kubeutil.ComposeName(srun.Name, step.Name)
	state := srun.Status.StepStates[step.Name]
	if state.SubStoryRunName != "" {
		subRunName = state.SubStoryRunName
	} else {
		state.SubStoryRunName = subRunName
		srun.Status.StepStates[step.Name] = state
	}

	subRunKey := types.NamespacedName{Name: subRunName, Namespace: srun.Namespace}
	subRun := &runsv1alpha1.StoryRun{}
	if err := e.Get(ctx, subRunKey, subRun); err == nil {
		return subRun, nil
	} else if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get sub-StoryRun '%s' for step '%s': %w", subRunName, step.Name, err)
	}

	ns := targetStory.Namespace
	storyRef := refs.StoryReference{
		ObjectReference: refs.ObjectReference{
			Name:      targetStory.Name,
			Namespace: &ns,
		},
		UID: &targetStory.UID,
	}

	newSubRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      subRunName,
			Namespace: srun.Namespace,
			Labels: map[string]string{
				contracts.ParentStoryRunLabel: srun.Name,
				contracts.ParentStepLabel:     step.Name,
			},
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: storyRef,
		},
	}
	if inputs != nil {
		newSubRun.Spec.Inputs = inputs
	}

	if err := controllerutil.SetControllerReference(srun, newSubRun, e.Scheme); err != nil {
		return nil, fmt.Errorf("failed to set owner reference on sub-StoryRun '%s': %w", subRunName, err)
	}

	if err := e.Create(ctx, newSubRun); err != nil {
		if !errors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to create sub-StoryRun '%s': %w", subRunName, err)
		}
		if err := e.Get(ctx, subRunKey, subRun); err != nil {
			return nil, fmt.Errorf("failed to fetch sub-StoryRun '%s' after AlreadyExists: %w", subRunName, err)
		}
		if !metav1.IsControlledBy(subRun, srun) {
			e.recordSpoofedSubStoryEvent(srun, subRunName, "owner reference does not match parent StoryRun")
			return nil, fmt.Errorf("sub-StoryRun '%s' is not owned by StoryRun '%s'", subRunName, srun.Name)
		}
		if targetStory != nil {
			if subRun.Spec.StoryRef.UID == nil || *subRun.Spec.StoryRef.UID != targetStory.UID {
				e.recordSpoofedSubStoryEvent(
					srun,
					subRunName,
					fmt.Sprintf("story UID mismatch (have=%v, want=%v)", subRun.Spec.StoryRef.UID, targetStory.UID),
				)
				return nil, fmt.Errorf(
					"sub-StoryRun '%s' targets Story UID %v; expected %v",
					subRunName,
					subRun.Spec.StoryRef.UID,
					targetStory.UID,
				)
			}
		}
		return subRun, nil
	}
	log.Info("Created sub-StoryRun", "name", subRunName, "waitForCompletion", waitForCompletion)
	if e.Recorder != nil {
		e.Recorder.Eventf(
			srun,
			corev1.EventTypeNormal,
			eventReasonSubStoryRunCreated,
			"Created sub-story run %s for step %s (waitForCompletion=%t)",
			subRunName,
			step.Name,
			waitForCompletion,
		)
	}
	return newSubRun, nil
}

// recordSpoofedSubStoryEvent emits a Warning event for ownership mismatches.
//
// Behavior:
//   - Returns early if Recorder or srun is nil.
//   - Emits SubStoryRunSpoofed event with reason.
//
// Arguments:
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - subRunName string: the sub-StoryRun name.
//   - reason string: description of the ownership issue.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Emits a Kubernetes Warning event.
func (e *StepExecutor) recordSpoofedSubStoryEvent(srun *runsv1alpha1.StoryRun, subRunName, reason string) {
	if e.Recorder == nil || srun == nil {
		return
	}
	e.Recorder.Eventf(
		srun,
		corev1.EventTypeWarning,
		eventReasonSubStoryRunSpoofed,
		"Detected spoofed sub-story run %s: %s",
		subRunName,
		reason,
	)
}

// updateStoryStepState updates the parent StepState based on sub-StoryRun status.
//
// Behavior:
//   - Stores SubStoryRunName in StepState.
//   - If waitForCompletion: mirrors child phase/message or sets Running.
//   - If fire-and-forget: marks as Succeeded immediately.
//
// Arguments:
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - step *bubuv1alpha1.Step: the executeStory step.
//   - subRun *runsv1alpha1.StoryRun: the child StoryRun.
//   - waitForCompletion bool: whether to wait for child completion.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates srun.Status.StepStates.
func updateStoryStepState(srun *runsv1alpha1.StoryRun, step *bubuv1alpha1.Step, subRun *runsv1alpha1.StoryRun, waitForCompletion bool) {
	state := srun.Status.StepStates[step.Name]
	state.SubStoryRunName = subRun.Name

	if waitForCompletion {
		if subRun.Status.Phase.IsTerminal() && subRun.Status.Phase != "" {
			state.Phase = subRun.Status.Phase
			state.Message = subRun.Status.Message
			if state.Message == "" {
				state.Message = fmt.Sprintf("Sub-story run '%s' completed with phase %s", subRun.Name, subRun.Status.Phase)
			}
		} else {
			state.Phase = enums.PhaseRunning
			state.Message = fmt.Sprintf("Waiting for sub-story run '%s' to complete", subRun.Name)
		}
	} else {
		state.Phase = enums.PhaseSucceeded
		state.Message = fmt.Sprintf("Launched sub-story run '%s'", subRun.Name)
	}

	srun.Status.StepStates[step.Name] = state
}

// computeManifestRequests scans Story expressions for manifest path references.
//
// Behavior:
//   - Gathers manifest path operations from step and story level expressions.
//   - Builds ManifestRequest list for the target step.
//
// Arguments:
//   - story *bubuv1alpha1.Story: the Story to scan.
//   - targetStep string: the step to compute manifests for.
//
// Returns:
//   - []runsv1alpha1.ManifestRequest: manifest requests, or nil if none.
func (e *StepExecutor) computeManifestRequests(story *bubuv1alpha1.Story, targetStep string) []runsv1alpha1.ManifestRequest {
	if story == nil || targetStep == "" {
		return nil
	}

	pathOps := gatherManifestPathOperations(story, targetStep)

	if len(pathOps) == 0 {
		return nil
	}

	paths := sortedManifestPaths(pathOps)
	return buildManifestRequests(paths, pathOps)
}

// gatherManifestPathOperations collects manifest path operations from Story.
//
// Behavior:
//   - Scans step-level if/with expressions.
//   - Scans story-level output/policy expressions.
//
// Arguments:
//   - story *bubuv1alpha1.Story: the Story to scan.
//   - targetStep string: the step to match references against.
//
// Returns:
//   - map[string]map[runsv1alpha1.ManifestOperation]struct{}: path → operations map.
func gatherManifestPathOperations(story *bubuv1alpha1.Story, targetStep string) map[string]map[runsv1alpha1.ManifestOperation]struct{} {
	pathOps := make(map[string]map[runsv1alpha1.ManifestOperation]struct{})
	addFromString := func(text string) {
		collectManifestPathsFromString(targetStep, text, pathOps)
	}

	addStepLevelManifestExpressions(story, addFromString)
	addStoryLevelManifestExpressions(story, addFromString)
	return pathOps
}

// addStepLevelManifestExpressions scans step if/with for manifest expressions.
//
// Behavior:
//   - Iterates Story steps and calls add for each expression.
//
// Arguments:
//   - story *bubuv1alpha1.Story: the Story to scan.
//   - add func(string): callback to process each expression.
//
// Returns:
//   - None.
func addStepLevelManifestExpressions(story *bubuv1alpha1.Story, add func(string)) {
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if step.If != nil {
			add(*step.If)
		}
		if step.With != nil && len(step.With.Raw) > 0 {
			add(string(step.With.Raw))
		}
	}
}

// addStoryLevelManifestExpressions scans story output/policy for manifest expressions.
//
// Behavior:
//   - Scans story.Spec.Output and story.Spec.Policy.With.
//
// Arguments:
//   - story *bubuv1alpha1.Story: the Story to scan.
//   - add func(string): callback to process each expression.
//
// Returns:
//   - None.
func addStoryLevelManifestExpressions(story *bubuv1alpha1.Story, add func(string)) {
	if story.Spec.Output != nil && len(story.Spec.Output.Raw) > 0 {
		add(string(story.Spec.Output.Raw))
	}
	if story.Spec.Policy != nil && story.Spec.Policy.With != nil && len(story.Spec.Policy.With.Raw) > 0 {
		add(string(story.Spec.Policy.With.Raw))
	}
}

// sortedManifestPaths extracts and sorts paths from pathOps map.
//
// Behavior:
//   - Collects keys from pathOps and sorts alphabetically.
//
// Arguments:
//   - pathOps map[string]map[runsv1alpha1.ManifestOperation]struct{}: source map.
//
// Returns:
//   - []string: sorted path list.
func sortedManifestPaths(pathOps map[string]map[runsv1alpha1.ManifestOperation]struct{}) []string {
	paths := make([]string, 0, len(pathOps))
	for path := range pathOps {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// buildManifestRequests constructs ManifestRequest slice from paths and operations.
//
// Behavior:
//   - Iterates sorted paths and builds requests with ordered operations.
//
// Arguments:
//   - paths []string: sorted path list.
//   - pathOps map[string]map[runsv1alpha1.ManifestOperation]struct{}: path → operations map.
//
// Returns:
//   - []runsv1alpha1.ManifestRequest: the constructed requests.
func buildManifestRequests(paths []string, pathOps map[string]map[runsv1alpha1.ManifestOperation]struct{}) []runsv1alpha1.ManifestRequest {
	requests := make([]runsv1alpha1.ManifestRequest, 0, len(paths))
	orderedOps := []runsv1alpha1.ManifestOperation{
		runsv1alpha1.ManifestOperationExists,
		runsv1alpha1.ManifestOperationLength,
		runsv1alpha1.ManifestOperationSample,
		runsv1alpha1.ManifestOperationHash,
		runsv1alpha1.ManifestOperationType,
	}
	for _, path := range paths {
		opsSet := pathOps[path]
		ops := make([]runsv1alpha1.ManifestOperation, 0, len(opsSet))
		for _, cand := range orderedOps {
			if _, ok := opsSet[cand]; ok {
				ops = append(ops, cand)
			}
		}
		requests = append(requests, runsv1alpha1.ManifestRequest{Path: path, Operations: ops})
	}
	return requests
}

// collectManifestPathsFromString extracts manifest paths from a text expression.
//
// Behavior:
//   - Replaces step aliases for matching.
//   - Matches len() and direct path patterns for targetStep.
//   - Adds corresponding operations to pathOps.
//
// Arguments:
//   - targetStep string: the step to match references against.
//   - text string: the expression text to scan.
//   - pathOps map[string]map[runsv1alpha1.ManifestOperation]struct{}: destination map.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates pathOps with found paths and operations.
func collectManifestPathsFromString(targetStep, text string, pathOps map[string]map[runsv1alpha1.ManifestOperation]struct{}) {
	if text == "" {
		return
	}

	text = replaceStepAliases(text)

	if text == "" {
		return
	}

	aliasTarget := sanitizeStepIdentifier(targetStep)

	for _, match := range manifestLenPattern.FindAllStringSubmatch(text, -1) {
		if len(match) < 3 {
			continue
		}
		if match[1] != aliasTarget {
			continue
		}
		path := normaliseManifestPath(match[2])
		addManifestOperation(pathOps, path, runsv1alpha1.ManifestOperationExists)
		addManifestOperation(pathOps, path, runsv1alpha1.ManifestOperationLength)
	}

	for _, match := range manifestSamplePattern.FindAllStringSubmatch(text, -1) {
		if len(match) < 3 {
			continue
		}
		if match[1] != aliasTarget {
			continue
		}
		path := normaliseManifestPath(match[2])
		addManifestOperation(pathOps, path, runsv1alpha1.ManifestOperationExists)
		addManifestOperation(pathOps, path, runsv1alpha1.ManifestOperationSample)
	}

	for _, match := range manifestHashPattern.FindAllStringSubmatch(text, -1) {
		if len(match) < 3 {
			continue
		}
		if match[1] != aliasTarget {
			continue
		}
		path := normaliseManifestPath(match[2])
		addManifestOperation(pathOps, path, runsv1alpha1.ManifestOperationExists)
		addManifestOperation(pathOps, path, runsv1alpha1.ManifestOperationHash)
	}

	for _, match := range manifestTypePattern.FindAllStringSubmatch(text, -1) {
		if len(match) < 3 {
			continue
		}
		if match[1] != aliasTarget {
			continue
		}
		path := normaliseManifestPath(match[2])
		addManifestOperation(pathOps, path, runsv1alpha1.ManifestOperationExists)
		addManifestOperation(pathOps, path, runsv1alpha1.ManifestOperationType)
	}

	for _, match := range manifestPathPattern.FindAllStringSubmatch(text, -1) {
		if len(match) < 3 {
			continue
		}
		if match[1] != aliasTarget {
			continue
		}
		path := normaliseManifestPath(match[2])
		addManifestOperation(pathOps, path, runsv1alpha1.ManifestOperationExists)
	}
}

// normaliseManifestPath cleans up a manifest path suffix.
//
// Behavior:
//   - Trims leading dot.
//   - Returns "$" for root path (empty after trim).
//
// Arguments:
//   - suffix string: the path suffix to normalize.
//
// Returns:
//   - string: normalized path.
func normaliseManifestPath(suffix string) string {
	path := strings.TrimPrefix(suffix, ".")
	if path == "" {
		return manifestRootPath
	}
	return path
}

// addManifestOperation adds an operation to the pathOps map for a path.
//
// Behavior:
//   - Returns early if pathOps is nil.
//   - Initializes the operations set if needed.
//   - Adds the operation to the set.
//
// Arguments:
//   - pathOps map[string]map[runsv1alpha1.ManifestOperation]struct{}: destination map.
//   - path string: the manifest path.
//   - op runsv1alpha1.ManifestOperation: the operation to add.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Mutates pathOps.
func addManifestOperation(pathOps map[string]map[runsv1alpha1.ManifestOperation]struct{}, path string, op runsv1alpha1.ManifestOperation) {
	if pathOps == nil {
		return
	}
	ops, ok := pathOps[path]
	if !ok {
		ops = make(map[runsv1alpha1.ManifestOperation]struct{})
		pathOps[path] = ops
	}
	ops[op] = struct{}{}
}

// replaceStepAliases normalizes step identifiers in expressions for matching.
//
// Behavior:
//   - Finds step references via stepAliasPattern.
//   - Replaces non-CEL-safe characters with underscores.
//
// Arguments:
//   - input string: the expression to process.
//
// Returns:
//   - string: expression with normalized step identifiers.
func replaceStepAliases(input string) string {
	return stepAliasPattern.ReplaceAllStringFunc(input, func(match string) string {
		submatches := stepAliasPattern.FindStringSubmatch(match)
		if len(submatches) != 2 {
			return match
		}
		original := submatches[1]
		alias := sanitizeStepIdentifier(original)
		if alias == original {
			return match
		}
		return strings.Replace(match, original, alias, 1)
	})
}

// sanitizeStepIdentifier replaces non-CEL-safe characters with underscores.
//
// Behavior:
//   - Keeps alphanumeric characters and underscores.
//   - Replaces all other characters with underscores.
//
// Arguments:
//   - name string: the step name to sanitize.
//
// Returns:
//   - string: CEL-safe identifier.
func sanitizeStepIdentifier(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// coerceToList converts input to a []any slice for loop iteration.
//
// Behavior:
//   - Returns []any directly if input is already that type.
//   - Converts []string to []any by wrapping each element.
//   - Wraps a single string in a single-element []any.
//   - Returns error for unsupported types.
//
// Arguments:
//   - input any: the value to coerce.
//
// Returns:
//   - []any: the coerced list.
//   - error: non-nil if input cannot be coerced.
func coerceToList(input any) ([]any, error) {
	switch v := input.(type) {
	case []any:
		return v, nil
	case []string:
		var list []any
		for _, item := range v {
			list = append(list, item)
		}
		return list, nil
	case string:
		return []any{v}, nil
	default:
		return nil, fmt.Errorf("input '%v' is not a list or string", input)
	}
}
