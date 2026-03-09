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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	webhookshared "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/observability"
	rec "github.com/bubustack/bobrapet/pkg/reconcile"
	refs "github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/bobrapet/pkg/templatesafety"
	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/templating"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
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
	Scheme            *runtime.Scheme
	TemplateEvaluator *templating.Evaluator
	ConfigResolver    *config.Resolver
	Recorder          record.EventRecorder
}

const (
	eventReasonStepRunCreated     = "ChildStepRunCreated"
	eventReasonStepRunDrift       = "ChildStepRunDrift"
	eventReasonSubStoryRunCreated = "SubStoryRunCreated"
	eventReasonSubStoryRunSpoofed = "SubStoryRunSpoofed"
	eventReasonStepInputNearLimit = "StepInputNearLimit"
)

const stepInputSoftBudgetRatio = 8
const stepInputSoftBudgetDivisor = 10

// NewStepExecutor creates a new StepExecutor.
//
// Behavior:
//   - Wraps the provided dependencies for step execution.
//
// Arguments:
//   - k8sClient client.Client: Kubernetes client for CRUD operations.
//   - scheme *runtime.Scheme: scheme for owner references.
//   - templateEval *templating.Evaluator: template evaluator for expression resolution.
//   - cfgResolver *config.Resolver: configuration resolver.
//   - recorder record.EventRecorder: event recorder for emitting events.
//
// Returns:
//   - *StepExecutor: ready for step execution.
func NewStepExecutor(k8sClient client.Client, scheme *runtime.Scheme, templateEval *templating.Evaluator, cfgResolver *config.Resolver, recorder record.EventRecorder) *StepExecutor {
	return &StepExecutor{
		Client:            k8sClient,
		Scheme:            scheme,
		TemplateEvaluator: templateEval,
		ConfigResolver:    cfgResolver,
		Recorder:          recorder,
	}
}

func (e *StepExecutor) controllerConfig() *config.ControllerConfig {
	if e == nil || e.ConfigResolver == nil {
		return nil
	}
	if resolved := e.ConfigResolver.GetOperatorConfig(); resolved != nil {
		return &resolved.Controller
	}
	return nil
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
//   - vars map[string]any: variable context for template evaluation.
//
// Returns:
//   - error: nil on success, or execution errors.
//
// Side Effects:
//   - Creates/updates StepRuns based on step type.
//   - Updates srun.Status.StepStates for primitive steps.
func (e *StepExecutor) Execute(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, vars map[string]any) (err error) {
	ensureStepStatesInitialized(srun)
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
		return e.executeStoryStep(ctx, srun, story, step, vars)
	case enums.StepTypeParallel:
		return e.executeParallelStep(ctx, srun, story, step, vars)
	case enums.StepTypeCondition:
		markStepState(srun, step.Name, enums.PhaseSucceeded, "Primitive evaluated and outputs are available.")
		return nil
	case enums.StepTypeSleep:
		markStepState(srun, step.Name, enums.PhasePaused, "Sleeping.")
		return nil
	case enums.StepTypeGate:
		markStepState(srun, step.Name, enums.PhasePaused, "Waiting for gate decision.")
		return nil
	case enums.StepTypeWait:
		markStepState(srun, step.Name, enums.PhasePaused, "Waiting for condition.")
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
	desiredTimeout, desiredRetry := extractExecutionOverrides(step)
	desiredRetry = resolveStepRetryPolicy(desiredRetry, story)

	if err := e.validateEngramReference(ctx, story, step); err != nil {
		return err
	}

	var stepRun runsv1alpha1.StepRun
	err := e.Get(ctx, types.NamespacedName{Name: stepName, Namespace: srun.Namespace}, &stepRun)
	if k8serrors.IsNotFound(err) {
		return e.createEngramStepRun(ctx, srun, story, step, stepName, desiredTimeout, desiredRetry)
	}
	if err != nil {
		return err
	}

	storyName := ""
	if story != nil {
		storyName = story.Name
	}
	scheduling := resolveSchedulingDecision(story, e.ConfigResolver, nil)
	return e.patchEngramStepRun(ctx, &stepRun, desiredTimeout, desiredRetry, storyName, scheduling)
}

// mergeTemplateRetryIntoPolicy fetches the EngramTemplate for the engram and merges its
// retry recommendations (RecommendedBaseDelay, RecommendedMaxDelay, RecommendedBackoff) into
// policy when those fields are unset. Returns the merged policy; does not mutate the input.
func (e *StepExecutor) mergeTemplateRetryIntoPolicy(ctx context.Context, policy *bubuv1alpha1.RetryPolicy, engram *bubuv1alpha1.Engram, log *logging.ControllerLogger) *bubuv1alpha1.RetryPolicy {
	if engram == nil {
		return policy
	}
	var template catalogv1alpha1.EngramTemplate
	templateName := types.NamespacedName{Name: engram.Spec.TemplateRef.Name}
	if err := e.Get(ctx, templateName, &template); err != nil {
		log.V(1).Info("Could not fetch EngramTemplate for retry merge; using policy as-is", "template", templateName.Name, "error", err)
		return policy
	}
	if template.Spec.Execution == nil || template.Spec.Execution.Retry == nil {
		return policy
	}
	return config.MergeTemplateRetryIntoPolicy(policy, template.Spec.Execution.Retry)
}

// lookupTemplateGeneration fetches the EngramTemplate referenced by the engram and returns
// its metadata.generation. Returns 0 if the engram is nil, has no template ref, or the
// template cannot be fetched (best-effort; StepRun creation should not fail for this).
func (e *StepExecutor) lookupTemplateGeneration(ctx context.Context, engram *bubuv1alpha1.Engram, log *logging.ControllerLogger) int64 {
	if engram == nil || engram.Spec.TemplateRef.Name == "" {
		return 0
	}
	var tmpl catalogv1alpha1.EngramTemplate
	templateName := types.NamespacedName{Name: engram.Spec.TemplateRef.Name}
	if err := e.Get(ctx, templateName, &tmpl); err != nil {
		log.V(1).Info("Could not fetch EngramTemplate for generation pinning; skipping", "template", templateName.Name, "error", err)
		return 0
	}
	return tmpl.Generation
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

func resolveStepRetryPolicy(stepRetry *bubuv1alpha1.RetryPolicy, story *bubuv1alpha1.Story) *bubuv1alpha1.RetryPolicy {
	if stepRetry != nil {
		return stepRetry
	}
	if story == nil || story.Spec.Policy == nil || story.Spec.Policy.Retries == nil || story.Spec.Policy.Retries.StepRetryPolicy == nil {
		return nil
	}
	return story.Spec.Policy.Retries.StepRetryPolicy.DeepCopy()
}

func (e *StepExecutor) validateEngramReference(ctx context.Context, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) error {
	if step == nil || step.Ref == nil {
		return nil
	}
	if story == nil {
		return fmt.Errorf("step '%s' missing parent story context for engram validation", step.Name)
	}
	targetNamespace := refs.ResolveNamespace(story, &step.Ref.ObjectReference)
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		e.Client,
		e.controllerConfig(),
		story,
		"bubustack.io",
		"Story",
		"bubustack.io",
		"Engram",
		targetNamespace,
		step.Ref.Name,
		"EngramRef",
	); err != nil {
		return fmt.Errorf("step '%s' references engram in namespace '%s': %w", step.Name, targetNamespace, err)
	}
	return nil
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
//     merged input) and applies controller ownership
//     so cleanup + downstream target helpers stay in sync
//     (internal/controller/runs/step_executor.go:221-249; controllers/_duplication_candidates.md:22-30).
//   - Issues the Create call (with event + log) and falls back to a refetch +
//     patch when the child already exists (internal/controller/runs/step_executor.go:243-288).
//
// Args:
//   - ctx/srun/story/step: reconcile context and parent metadata.
//   - stepName: deterministic child name from kubeutil.ComposeName.
//   - timeout/retry: execution overrides propagated to the child spec.
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

	// Merge template retry recommendations (delay, maxDelay, backoff) into policy when unset.
	retry = e.mergeTemplateRetryIntoPolicy(ctx, retry, &engram, log)

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
	if story != nil && story.Name != "" {
		labels[contracts.StoryNameLabelKey] = story.Name
	}
	correlation := correlationIDFromAnnotations(srun)
	if correlation != "" {
		ensureCorrelationLabel(labels, correlation)
	}
	scheduling := resolveSchedulingDecision(story, e.ConfigResolver, nil)
	labels = applySchedulingLabels(labels, scheduling)
	annotations := map[string]string{}
	if correlation != "" {
		annotations[contracts.CorrelationIDAnnotation] = correlation
	}
	stepRun := runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:        stepName,
			Namespace:   srun.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: srun.Name},
			},
			StepID:             step.Name,
			EngramRef:          step.Ref,
			TemplateGeneration: e.lookupTemplateGeneration(ctx, &engram, log),
			Input:              mergedWith,
			Timeout:            timeout,
			Retry:              retry,
		},
	}
	if strings.TrimSpace(stepRun.Spec.IdempotencyKey) == "" {
		stepRun.Spec.IdempotencyKey = runsidentity.StepRunIdempotencyKey(srun.Namespace, srun.Name, step.Name)
	}

	if err := controllerutil.SetControllerReference(srun, &stepRun, e.Scheme); err != nil {
		return err
	}
	if err := e.Create(ctx, &stepRun); err != nil {
		if k8serrors.IsAlreadyExists(err) {
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
			storyName := ""
			if story != nil {
				storyName = story.Name
			}
			if patchErr := e.patchEngramStepRun(ctx, &existing, timeout, retry, storyName, scheduling); patchErr != nil {
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
//   - story *bubuv1alpha1.Story: expected Story definition.
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
		expectedLabel := runsidentity.SelectorLabels(srun.Name)[contracts.StoryRunLabelKey]
		if label := existing.GetLabels()[contracts.StoryRunLabelKey]; label != expectedLabel {
			reasons = append(reasons, fmt.Sprintf("%s label=%s expected %s", contracts.StoryRunLabelKey, label, expectedLabel))
		}
	}
	if story != nil {
		if story.Name != "" {
			if label, ok := existing.GetLabels()[contracts.StoryNameLabelKey]; ok && label != "" && label != story.Name {
				reasons = append(reasons, fmt.Sprintf("%s label=%s expected %s", contracts.StoryNameLabelKey, label, story.Name))
			}
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

// patchEngramStepRun reconciles an existing StepRun's timeout, retry policy, and story label.
// It deep-copies the current spec, compares the desired values, and issues a
// MergeFrom patch when any field changed
// (internal/controller/runs/step_executor.go:231-301).
//
// Args:
//   - ctx: reconcile context forwarded to controller-runtime.
//   - stepRun: live StepRun fetched earlier.
//   - timeout/retry: desired spec fields computed by executeEngramStep.
//   - storyName: desired Story name for labeling (optional).
//
// Returns: nil when no changes are needed or the patch succeeds; otherwise the
// patch error for controller-runtime to retry.
//
// Notes:
//   - Input and EngramRef remain immutable; those fields still rely on
//     createEngramStepRun + Stage guard coordination (controllers/_duplication_candidates.md:19-21).
func (e *StepExecutor) patchEngramStepRun(
	ctx context.Context,
	stepRun *runsv1alpha1.StepRun,
	timeout string,
	retry *bubuv1alpha1.RetryPolicy,
	storyName string,
	scheduling schedulingDecision,
) error {
	log := logging.NewReconcileLogger(ctx, "step-executor").
		WithValues("steprun", stepRun.Name, "step", stepRun.Spec.StepID)
	before := stepRun.DeepCopy()
	labelsChanged, labelErr := ensureStoryNameLabel(stepRun, storyName)
	if labelErr != nil {
		return labelErr
	}
	if stepRun.Labels == nil {
		stepRun.Labels = map[string]string{}
	}
	idempotencyChanged := false
	if strings.TrimSpace(stepRun.Spec.IdempotencyKey) == "" {
		stepRun.Spec.IdempotencyKey = runsidentity.StepRunIdempotencyKey(stepRun.Namespace, stepRun.Spec.StoryRunRef.Name, stepRun.Spec.StepID)
		idempotencyChanged = true
	}
	schedulingChanged := ensureSchedulingLabels(stepRun.Labels, scheduling)
	timeoutChanged, retryChanged := UpdateTimeoutAndRetry(stepRun, timeout, retry)
	needsPatch := timeoutChanged || retryChanged || labelsChanged || schedulingChanged || idempotencyChanged

	if !needsPatch {
		return nil
	}

	if err := e.Patch(ctx, stepRun, client.MergeFrom(before)); err != nil {
		return err
	}

	if timeoutChanged || retryChanged || labelsChanged || schedulingChanged {
		log.V(1).Info("Updated StepRun timeout/retry settings",
			"timeoutChanged", timeoutChanged,
			"retryChanged", retryChanged,
			"previousTimeout", before.Spec.Timeout,
			"currentTimeout", stepRun.Spec.Timeout,
			"previousRetry", before.Spec.Retry,
			"currentRetry", stepRun.Spec.Retry,
			"labelsChanged", labelsChanged,
			"schedulingChanged", schedulingChanged,
		)
	}

	return nil
}

func (e *StepExecutor) maybeOffloadStepRunInput(ctx context.Context, srun *runsv1alpha1.StoryRun, stepName string, maxInlineBytes int, input *runtime.RawExtension) (*runtime.RawExtension, error) {
	if input == nil || len(input.Raw) == 0 {
		return input, nil
	}

	if maxInlineBytes <= 0 || len(input.Raw) <= maxInlineBytes {
		softBudget := maxInlineBytes * stepInputSoftBudgetRatio / stepInputSoftBudgetDivisor
		if softBudget > 0 && len(input.Raw) >= softBudget {
			if e.Recorder != nil {
				e.Recorder.Eventf(
					srun,
					corev1.EventTypeWarning,
					eventReasonStepInputNearLimit,
					"Step input for %s is %d bytes (soft budget %d / max %d). Consider moving static config to ConfigMaps or reducing inline config.",
					stepName,
					len(input.Raw),
					softBudget,
					maxInlineBytes,
				)
			}
		}
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
//   - vars map[string]any: variable context for templates.
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
	scheduling := resolveSchedulingDecision(story, e.ConfigResolver, nil)
	for i := range parallelCfg.Steps {
		childStep := &parallelCfg.Steps[i]
		if childStep.Ref != nil {
			if err := e.validateEngramReference(ctx, story, childStep); err != nil {
				return fmt.Errorf("parallel step '%s' branch '%s' rejected: %w", step.Name, childStep.Name, err)
			}
		}

		childStepName := kubeutil.ComposeName(srun.Name, step.Name, childStep.Name)
		childStepRunNames = append(childStepRunNames, childStepName)

		resolvedWithBytes, err := e.resolveTemplateWith(ctx, srun, materializePurposeParallel, step.Name, childStep.Name, childStep.With, vars)
		if err != nil {
			return fmt.Errorf("failed to resolve 'with' for parallel branch '%s' in step '%s': %w", childStep.Name, step.Name, err)
		}

		childTimeout, childRetry := extractExecutionOverrides(childStep)
		childRetry = resolveStepRetryPolicy(childRetry, story)

		// Look up EngramTemplate generation for version pinning.
		var templateGeneration int64
		if childStep.Ref != nil {
			engramKey := childStep.Ref.ToNamespacedName(srun)
			var childEngram bubuv1alpha1.Engram
			if err := e.Get(ctx, engramKey, &childEngram); err == nil {
				pLog := logging.NewReconcileLogger(ctx, "step-executor").WithValues("storyrun", srun.Name, "step", childStepName)
				templateGeneration = e.lookupTemplateGeneration(ctx, &childEngram, pLog)
			}
		}

		childLabels := runsidentity.SelectorLabels(srun.Name)
		if story != nil && story.Name != "" {
			childLabels[contracts.StoryNameLabelKey] = story.Name
		}
		childLabels = applySchedulingLabels(childLabels, scheduling)
		childLabels[contracts.ParentStepLabel] = step.Name
		stepRun := &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      childStepName,
				Namespace: srun.Namespace,
				Labels:    childLabels,
			},
			Spec: runsv1alpha1.StepRunSpec{
				StoryRunRef:        refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: srun.Name}},
				StepID:             childStep.Name,
				EngramRef:          childStep.Ref,
				TemplateGeneration: templateGeneration,
				Input:              resolvedWithBytes,
				Timeout:            childTimeout,
				Retry:              childRetry,
			},
		}
		if err := e.createOrUpdateChildStepRun(ctx, srun, stepRun); err != nil {
			return err
		}
	}

	setPrimitiveChildren(srun, step.Name, childStepRunNames)
	markStepState(srun, step.Name, enums.PhaseRunning, "Parallel block expanded")
	return nil
}

// createOrUpdateChildStepRun creates or patches a child StepRun.
//
// Behavior:
//   - Sets StoryRun as controller owner.
//   - Creates if not exists, or patches if spec differs.
//   - Patches: StepID, StoryRunRef, EngramRef, Input.
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
		if !k8serrors.IsAlreadyExists(err) {
			return err
		}

		var existing runsv1alpha1.StepRun
		if getErr := e.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, &existing); getErr != nil {
			return getErr
		}

		before := existing.DeepCopy()
		specChanged := rec.UpdateStepRunSpec(&existing, desired)
		labelsChanged := ensureStepRunLabels(&existing, desired)

		ownerBefore := before.GetOwnerReferences()
		if err := controllerutil.SetControllerReference(srun, &existing, e.Scheme); err != nil {
			return err
		}
		ownerChanged := !reflect.DeepEqual(ownerBefore, existing.GetOwnerReferences())

		if specChanged || ownerChanged || labelsChanged {
			if patchErr := e.Patch(ctx, &existing, client.MergeFrom(before)); patchErr != nil {
				return patchErr
			}
			logger := logging.NewReconcileLogger(ctx, "step-executor").
				WithValues("namespace", desired.Namespace, "child", desired.Name)
			if srun != nil {
				logger = logger.WithValues("storyrun", srun.Name)
			}
			logger.V(1).Info("Patched existing StepRun to match desired spec", "specChanged", specChanged, "ownerChanged", ownerChanged, "labelsChanged", labelsChanged)
		}

		return nil
	}
	return nil
}

func ensureStepRunLabels(existing, desired *runsv1alpha1.StepRun) bool {
	if existing == nil || desired == nil {
		return false
	}
	desiredLabels := desired.GetLabels()
	if len(desiredLabels) == 0 {
		return false
	}
	if existing.Labels == nil {
		existing.Labels = map[string]string{}
	}
	changed := false
	for key, value := range desiredLabels {
		if existing.Labels[key] != value {
			existing.Labels[key] = value
			changed = true
		}
	}
	return changed
}

func ensureStoryNameLabel(stepRun *runsv1alpha1.StepRun, storyName string) (bool, error) {
	if stepRun == nil || strings.TrimSpace(storyName) == "" {
		return false, nil
	}
	if stepRun.Labels == nil {
		stepRun.Labels = map[string]string{}
	}
	existing := strings.TrimSpace(stepRun.Labels[contracts.StoryNameLabelKey])
	if existing == "" {
		stepRun.Labels[contracts.StoryNameLabelKey] = storyName
		return true, nil
	}
	if existing != storyName {
		return false, fmt.Errorf("stepRun %s has %s label=%s expected %s", stepRun.Name, contracts.StoryNameLabelKey, existing, storyName)
	}
	return false, nil
}

// resolveTemplateWith evaluates a 'with' block via templates.
//
// Behavior:
//   - Returns nil for nil or empty input.
//   - Unmarshals, evaluates via templates, and re-marshals.
//
// Arguments:
//   - ctx context.Context: propagated to template evaluation.
//   - srun *runsv1alpha1.StoryRun: parent StoryRun (used for materialize steps).
//   - purpose/stepName/childName: identifiers for materialize StepRun naming.
//   - raw *runtime.RawExtension: the 'with' block to evaluate.
//   - vars map[string]any: variable context for templates.
//
// Returns:
//   - *runtime.RawExtension: the resolved 'with' block.
//   - error: nil on success, or evaluation errors.
func (e *StepExecutor) resolveTemplateWith(ctx context.Context, srun *runsv1alpha1.StoryRun, purpose, stepName, childName string, raw *runtime.RawExtension, vars map[string]any) (*runtime.RawExtension, error) {
	if raw == nil || len(raw.Raw) == 0 {
		return nil, nil
	}
	if err := templatesafety.ValidateTemplateJSON(raw.Raw); err != nil {
		return nil, err
	}

	withVars := make(map[string]any)
	if err := json.Unmarshal(raw.Raw, &withVars); err != nil {
		return nil, fmt.Errorf("failed to parse 'with' block: %w", err)
	}

	// Do not inject materialize for "with" blocks: the SDK evaluates them at runtime and can
	// resolve storage refs (it only needs access to the data). Materialize is for controller
	// built-in actions (if, step input/config) where the controller must evaluate the template.
	resolved, err := e.TemplateEvaluator.ResolveWithInputs(ctx, withVars, vars)
	if err != nil {
		// When resolution hits a ref, evaluator returns ErrOffloadedDataUsage. Pass the raw
		// template through so the StepRun gets it and the SDK can resolve and hydrate at runtime.
		var offloaded *templating.ErrOffloadedDataUsage
		if errors.As(err, &offloaded) {
			return raw, nil
		}
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
	markStepState(srun, step.Name, stopCfg.Phase, stopCfg.Message)
	return nil
}

// executeStoryStep handles executeStory-type steps that spawn sub-StoryRuns.
//
// Behavior:
//   - Parses storyRef, waitForCompletion, and inputs from step.With.
//   - Fetches the target Story.
//   - Resolves input expressions via templates.
//   - Creates/fetches the sub-StoryRun.
//   - Updates parent StepState with sub-run reference.
//
// Arguments:
//   - ctx context.Context: propagated to client and template operations.
//   - srun *runsv1alpha1.StoryRun: the parent StoryRun.
//   - story *bubuv1alpha1.Story: the parent Story definition.
//   - step *bubuv1alpha1.Step: the executeStory step.
//   - vars map[string]any: variable context for templates.
//
// Returns:
//   - error: nil on success, or parsing/creation errors.
//
// Side Effects:
//   - Creates a child StoryRun.
//   - Updates srun.Status.StepStates.
func (e *StepExecutor) executeStoryStep(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, vars map[string]any) error {
	log := logging.NewReconcileLogger(ctx, "step-executor").WithValues("storyrun", srun.Name, "step", step.Name)

	ensureStepStatesInitialized(srun)

	cfg, err := parseExecuteStoryConfig(step)
	if err != nil {
		return err
	}

	targetStory, err := e.getTargetStory(ctx, story, cfg.storyRef, step.Name)
	if err != nil {
		return err
	}

	mergedInputs := cfg.rawInputs
	if targetStory.Spec.Policy != nil && targetStory.Spec.Policy.With != nil {
		mergedInputs, err = kubeutil.MergeWithBlocks(targetStory.Spec.Policy.With, cfg.rawInputs)
		if err != nil {
			return fmt.Errorf("failed to merge story policy.with for step '%s': %w", step.Name, err)
		}
	}

	inputs, err := e.resolveStoryStepInputs(ctx, srun, mergedInputs, vars, step.Name)
	if err != nil {
		return err
	}

	parentScheduling := resolveSchedulingDecision(story, e.ConfigResolver, nil)
	subRun, err := e.ensureSubStoryRun(ctx, srun, step, targetStory, inputs, cfg.waitForCompletion, log, parentScheduling)
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

func markStepState(srun *runsv1alpha1.StoryRun, stepName string, phase enums.Phase, message string) {
	if srun == nil {
		return
	}
	ensureStepStatesInitialized(srun)
	now := metav1.Now()
	state := srun.Status.StepStates[stepName]
	state.Phase = phase
	state.Message = message
	state = ensureStepStateTimes(state, now)
	srun.Status.StepStates[stepName] = state
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
//   - story *bubuv1alpha1.Story: provides namespace context.
//   - ref *refs.ObjectReference: the story reference.
//   - stepName string: for error messages.
//
// Returns:
//   - *bubuv1alpha1.Story: the fetched Story.
//   - error: nil on success, or fetch errors.
func (e *StepExecutor) getTargetStory(ctx context.Context, story *bubuv1alpha1.Story, ref *refs.ObjectReference, stepName string) (*bubuv1alpha1.Story, error) {
	if ref == nil || strings.TrimSpace(ref.Name) == "" {
		return nil, fmt.Errorf("story step '%s' missing storyRef name", stepName)
	}
	if story == nil {
		return nil, fmt.Errorf("story step '%s' missing parent story context", stepName)
	}
	targetNamespace := refs.ResolveNamespace(story, ref)
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		e.Client,
		e.controllerConfig(),
		story,
		"bubustack.io",
		"Story",
		"bubustack.io",
		"Story",
		targetNamespace,
		ref.Name,
		"StoryRef",
	); err != nil {
		return nil, fmt.Errorf("step '%s' references Story in namespace '%s': %w", stepName, targetNamespace, err)
	}
	target, key, err := refs.LoadNamespacedReference(
		ctx,
		e.Client,
		story,
		ref,
		func() *bubuv1alpha1.Story { return &bubuv1alpha1.Story{} },
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get story '%s' for story step '%s': %w", key.String(), stepName, err)
	}
	return target, nil
}

// resolveStoryStepInputs resolves the inputs for an executeStory step.
//
// Behavior:
//   - Returns nil for nil input.
//   - Delegates to resolveTemplateWith for template evaluation.
//
// Arguments:
//   - ctx context.Context: propagated to template evaluation.
//   - raw *runtime.RawExtension: the raw inputs.
//   - vars map[string]any: variable context for templates.
//   - stepName string: for error messages.
//
// Returns:
//   - *runtime.RawExtension: the resolved inputs.
//   - error: nil on success, or resolution errors.
func (e *StepExecutor) resolveStoryStepInputs(ctx context.Context, srun *runsv1alpha1.StoryRun, raw *runtime.RawExtension, vars map[string]any, stepName string) (*runtime.RawExtension, error) {
	if raw == nil {
		return nil, nil
	}
	resolved, err := e.resolveTemplateWith(ctx, srun, materializePurposeExecute, stepName, "", raw, vars)
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
	parentScheduling schedulingDecision,
) (*runsv1alpha1.StoryRun, error) {
	ensureStepStatesInitialized(srun)

	subRunName := kubeutil.ComposeName(srun.Name, step.Name)
	state := srun.Status.StepStates[step.Name]
	if state.SubStoryRunName != "" {
		subRunName = state.SubStoryRunName
	} else {
		state.SubStoryRunName = subRunName
		srun.Status.StepStates[step.Name] = state
	}

	ns := targetStory.Namespace
	storyRef := refs.StoryReference{
		ObjectReference: refs.ObjectReference{
			Name:      targetStory.Name,
			Namespace: &ns,
		},
		UID: &targetStory.UID,
	}

	scheduling := resolveSchedulingDecision(targetStory, e.ConfigResolver, &parentScheduling)
	labels := map[string]string{
		contracts.ParentStoryRunLabel: srun.Name,
		contracts.ParentStepLabel:     step.Name,
	}
	labels = applySchedulingLabels(labels, scheduling)
	newSubRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      subRunName,
			Namespace: srun.Namespace,
			Labels:    labels,
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: storyRef,
		},
	}
	if inputs != nil {
		newSubRun.Spec.Inputs = inputs
	}

	subRunKey := types.NamespacedName{Name: subRunName, Namespace: srun.Namespace}
	subRun := &runsv1alpha1.StoryRun{}
	if err := e.Get(ctx, subRunKey, subRun); err == nil {
		if !metav1.IsControlledBy(subRun, srun) {
			e.recordSpoofedSubStoryEvent(srun, subRunName, "owner reference does not match parent StoryRun")
			return nil, fmt.Errorf("sub-StoryRun '%s' is not owned by StoryRun '%s'", subRunName, srun.Name)
		}
		if !subStoryRunMatchesDesired(subRun, newSubRun) {
			e.recordSpoofedSubStoryEvent(srun, subRunName, "spec does not match desired sub-story run")
			return nil, fmt.Errorf("sub-StoryRun '%s' spec does not match expected StoryRef/inputs", subRunName)
		}
		return subRun, nil
	} else if !k8serrors.IsNotFound(err) {
		return nil, fmt.Errorf("failed to get sub-StoryRun '%s' for step '%s': %w", subRunName, step.Name, err)
	}

	if err := controllerutil.SetControllerReference(srun, newSubRun, e.Scheme); err != nil {
		return nil, fmt.Errorf("failed to set owner reference on sub-StoryRun '%s': %w", subRunName, err)
	}

	if err := e.Create(ctx, newSubRun); err != nil {
		if !k8serrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("failed to create sub-StoryRun '%s': %w", subRunName, err)
		}
		if err := e.Get(ctx, subRunKey, subRun); err != nil {
			return nil, fmt.Errorf("failed to fetch sub-StoryRun '%s' after AlreadyExists: %w", subRunName, err)
		}
		if !metav1.IsControlledBy(subRun, srun) {
			e.recordSpoofedSubStoryEvent(srun, subRunName, "owner reference does not match parent StoryRun")
			return nil, fmt.Errorf("sub-StoryRun '%s' is not owned by StoryRun '%s'", subRunName, srun.Name)
		}
		if !subStoryRunMatchesDesired(subRun, newSubRun) {
			e.recordSpoofedSubStoryEvent(srun, subRunName, "spec does not match desired sub-story run")
			return nil, fmt.Errorf("sub-StoryRun '%s' spec does not match expected StoryRef/inputs", subRunName)
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

func subStoryRunMatchesDesired(existing, desired *runsv1alpha1.StoryRun) bool {
	if existing == nil || desired == nil {
		return false
	}
	if !storyRefMatches(existing.Spec.StoryRef, desired.Spec.StoryRef) {
		return false
	}
	return inputsEqual(existing.Spec.Inputs, desired.Spec.Inputs)
}

func storyRefMatches(existing, desired refs.StoryReference) bool {
	if existing.Name != desired.Name {
		return false
	}
	if !sameNamespace(existing.Namespace, desired.Namespace) {
		return false
	}
	if strings.TrimSpace(desired.Version) != "" && desired.Version != existing.Version {
		return false
	}
	if desired.UID != nil {
		if existing.UID == nil || *existing.UID != *desired.UID {
			return false
		}
	}
	return true
}

func sameNamespace(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func inputsEqual(a, b *runtime.RawExtension) bool {
	if a == nil || len(a.Raw) == 0 {
		return b == nil || len(b.Raw) == 0
	}
	if b == nil || len(b.Raw) == 0 {
		return false
	}
	var left any
	if err := json.Unmarshal(a.Raw, &left); err != nil {
		return bytes.Equal(bytes.TrimSpace(a.Raw), bytes.TrimSpace(b.Raw))
	}
	var right any
	if err := json.Unmarshal(b.Raw, &right); err != nil {
		return bytes.Equal(bytes.TrimSpace(a.Raw), bytes.TrimSpace(b.Raw))
	}
	return reflect.DeepEqual(left, right)
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

	state = ensureStepStateTimes(state, metav1.Now())
	srun.Status.StepStates[step.Name] = state
}

// sanitizeStepIdentifier replaces non-template-safe characters with underscores.
//
// Behavior:
//   - Keeps alphanumeric characters and underscores.
//   - Replaces all other characters with underscores.
//
// Arguments:
//   - name string: the step name to sanitize.
//
// Returns:
//   - string: template-safe identifier.
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
