package runs

import (
	"bytes"
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
	"github.com/bubustack/bobrapet/internal/controller/mergeutil"
	"github.com/bubustack/bobrapet/internal/controller/naming"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/observability"
	refs "github.com/bubustack/bobrapet/pkg/refs"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"go.opentelemetry.io/otel/attribute"
)

// StepExecutor is responsible for executing individual steps in a StoryRun.
type StepExecutor struct {
	client.Client
	Scheme         *runtime.Scheme
	CEL            *cel.Evaluator
	ConfigResolver *config.Resolver
}

var (
	manifestLenPattern  = regexp.MustCompile(`len\(\s*steps\.([a-zA-Z0-9_\-]+)\.output((?:\.[a-zA-Z0-9_\-]+|\[[^]]+\])*)\s*\)`)
	manifestPathPattern = regexp.MustCompile(`steps\.([a-zA-Z0-9_\-]+)\.output((?:\.[a-zA-Z0-9_\-]+|\[[^]]+\])*)`)
	stepAliasPattern    = regexp.MustCompile(`steps\.([a-zA-Z0-9_\-]+)`)
)

const (
	manifestRootPath         = "$"
	defaultMaxLoopIterations = 100
)

// NewStepExecutor creates a new StepExecutor.
func NewStepExecutor(k8sClient client.Client, scheme *runtime.Scheme, celEval *cel.Evaluator, cfgResolver *config.Resolver) *StepExecutor {
	return &StepExecutor{Client: k8sClient, Scheme: scheme, CEL: celEval, ConfigResolver: cfgResolver}
}

// Execute determines the step type and calls the appropriate execution method.
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

func (e *StepExecutor) executeEngramStep(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) error {
	stepName := naming.Compose(srun.Name, step.Name)
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
	if step.Ref == nil {
		return fmt.Errorf("step '%s' is missing a 'ref'", step.Name)
	}

	var engram bubuv1alpha1.Engram
	if err := e.Get(ctx, step.Ref.ToNamespacedName(srun), &engram); err != nil {
		return fmt.Errorf("failed to get engram '%s' for step '%s': %w", step.Ref.Name, step.Name, err)
	}

	mergedWith, err := mergeutil.MergeWithBlocks(engram.Spec.With, step.With)
	if err != nil {
		return fmt.Errorf("failed to merge 'with' blocks for step '%s': %w", step.Name, err)
	}

	stepRun := runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stepName,
			Namespace: srun.Namespace,
			Labels: map[string]string{
				"bubustack.io/storyrun":   srun.Name,
				"bubustack.io/story-name": story.Name,
			},
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
	return e.Create(ctx, &stepRun)
}

func (e *StepExecutor) patchEngramStepRun(
	ctx context.Context,
	stepRun *runsv1alpha1.StepRun,
	timeout string,
	retry *bubuv1alpha1.RetryPolicy,
	requestedManifest []runsv1alpha1.ManifestRequest,
) error {
	before := stepRun.DeepCopy()
	needsPatch := false

	if stepRun.Spec.Timeout != timeout {
		stepRun.Spec.Timeout = timeout
		needsPatch = true
	}

	if !reflect.DeepEqual(stepRun.Spec.Retry, retry) {
		if retry == nil {
			stepRun.Spec.Retry = nil
		} else {
			stepRun.Spec.Retry = retry.DeepCopy()
		}
		needsPatch = true
	}

	if len(requestedManifest) > 0 && !manifestRequestsEqual(stepRun.Spec.RequestedManifest, requestedManifest) {
		stepRun.Spec.RequestedManifest = requestedManifest
		needsPatch = true
	}

	if !needsPatch {
		return nil
	}

	return e.Patch(ctx, stepRun, client.MergeFrom(before))
}

func (e *StepExecutor) executeLoopStep(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, vars map[string]any) error {
	type loopWith struct {
		Items    json.RawMessage   `json:"items"`
		Template bubuv1alpha1.Step `json:"template"`
	}
	var loopCfg loopWith
	if err := json.Unmarshal(step.With.Raw, &loopCfg); err != nil {
		return fmt.Errorf("failed to parse 'with' for loop step '%s': %w", step.Name, err)
	}

	var itemsSpec any
	if err := json.Unmarshal(loopCfg.Items, &itemsSpec); err != nil {
		return fmt.Errorf("failed to parse 'items' for loop step '%s': %w", step.Name, err)
	}

	resolvedItemsRaw, err := e.resolveLoopItems(ctx, itemsSpec, vars)
	if err != nil {
		return fmt.Errorf("failed to resolve 'items' for loop step '%s': %w", step.Name, err)
	}

	resolvedItems, err := coerceToList(resolvedItemsRaw)
	if err != nil {
		return fmt.Errorf("'items' in loop step '%s' did not resolve to a list: %w", step.Name, err)
	}

	maxIterations := defaultMaxLoopIterations
	if e.ConfigResolver != nil {
		if cfg := e.ConfigResolver.GetOperatorConfig(); cfg != nil && cfg.Controller.MaxLoopIterations > 0 {
			maxIterations = cfg.Controller.MaxLoopIterations
		}
	}
	if len(resolvedItems) > maxIterations {
		return fmt.Errorf("loop step '%s' exceeds maximum of %d iterations", step.Name, maxIterations)
	}

	childStepRunNames := make([]string, 0, len(resolvedItems))
	for i, item := range resolvedItems {
		childStepName := naming.Compose(srun.Name, step.Name, strconv.Itoa(i))
		childStepRunNames = append(childStepRunNames, childStepName)

		loopVars := map[string]any{
			"inputs": vars["inputs"],
			"steps":  vars["steps"],
			"item":   item,
			"index":  i,
		}

		resolvedWithBytes, err := e.resolveTemplateWith(ctx, loopCfg.Template.With, loopVars)
		if err != nil {
			return fmt.Errorf("failed to resolve 'with' for loop iteration %d in step '%s': %w", i, step.Name, err)
		}

		requestedManifest := e.computeManifestRequests(story, loopCfg.Template.Name)

		stepRun := &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      childStepName,
				Namespace: srun.Namespace,
				Labels:    map[string]string{"bubustack.io/storyrun": srun.Name, "bubustack.io/parent-step": step.Name},
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
			return err
		}
	}

	if srun.Status.PrimitiveChildren == nil {
		srun.Status.PrimitiveChildren = make(map[string][]string)
	}
	srun.Status.PrimitiveChildren[step.Name] = childStepRunNames
	srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseRunning, Message: "Loop expanded"}
	return nil
}

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
		childStepName := naming.Compose(srun.Name, step.Name, childStep.Name)
		childStepRunNames = append(childStepRunNames, childStepName)

		resolvedWithBytes, err := e.resolveTemplateWith(ctx, childStep.With, vars)
		if err != nil {
			return fmt.Errorf("failed to resolve 'with' for parallel branch '%s' in step '%s': %w", childStep.Name, step.Name, err)
		}

		requestedManifest := e.computeManifestRequests(story, childStep.Name)

		stepRun := &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      childStepName,
				Namespace: srun.Namespace,
				Labels:    map[string]string{"bubustack.io/storyrun": srun.Name, "bubustack.io/parent-step": step.Name},
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

	if srun.Status.PrimitiveChildren == nil {
		srun.Status.PrimitiveChildren = make(map[string][]string)
	}
	srun.Status.PrimitiveChildren[step.Name] = childStepRunNames
	srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseRunning, Message: "Parallel block expanded"}
	return nil
}

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
		updated := false

		if existing.Spec.StepID != desired.Spec.StepID {
			existing.Spec.StepID = desired.Spec.StepID
			updated = true
		}
		if !reflect.DeepEqual(existing.Spec.StoryRunRef, desired.Spec.StoryRunRef) {
			existing.Spec.StoryRunRef = desired.Spec.StoryRunRef
			updated = true
		}
		if !reflect.DeepEqual(existing.Spec.EngramRef, desired.Spec.EngramRef) {
			if desired.Spec.EngramRef == nil {
				existing.Spec.EngramRef = nil
			} else {
				clone := *desired.Spec.EngramRef
				existing.Spec.EngramRef = &clone
			}
			updated = true
		}
		if !rawExtensionsEqual(existing.Spec.Input, desired.Spec.Input) {
			existing.Spec.Input = cloneRawExtension(desired.Spec.Input)
			updated = true
		}
		if !manifestRequestsEqual(existing.Spec.RequestedManifest, desired.Spec.RequestedManifest) {
			existing.Spec.RequestedManifest = copyManifestRequests(desired.Spec.RequestedManifest)
			updated = true
		}

		ownerBefore := before.GetOwnerReferences()
		if err := controllerutil.SetControllerReference(srun, &existing, e.Scheme); err != nil {
			return err
		}
		ownerChanged := !reflect.DeepEqual(ownerBefore, existing.GetOwnerReferences())

		if updated || ownerChanged {
			if patchErr := e.Patch(ctx, &existing, client.MergeFrom(before)); patchErr != nil {
				return patchErr
			}
		}

		return nil
	}
	return nil
}

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

func ensureStoryStepStateMap(srun *runsv1alpha1.StoryRun) {
	if srun.Status.StepStates == nil {
		srun.Status.StepStates = make(map[string]runsv1alpha1.StepState)
	}
}

func (e *StepExecutor) getTargetStory(ctx context.Context, srun *runsv1alpha1.StoryRun, ref *refs.ObjectReference, stepName string) (*bubuv1alpha1.Story, error) {
	var targetStory bubuv1alpha1.Story
	if err := e.Get(ctx, ref.ToNamespacedName(srun), &targetStory); err != nil {
		return nil, fmt.Errorf("failed to get story '%s' for story step '%s': %w", ref.Name, stepName, err)
	}
	return &targetStory, nil
}

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

func (e *StepExecutor) ensureSubStoryRun(
	ctx context.Context,
	srun *runsv1alpha1.StoryRun,
	step *bubuv1alpha1.Step,
	targetStory *bubuv1alpha1.Story,
	inputs *runtime.RawExtension,
	waitForCompletion bool,
	log *logging.ControllerLogger,
) (*runsv1alpha1.StoryRun, error) {
	subRunName := naming.Compose(srun.Name, step.Name)
	if state, ok := srun.Status.StepStates[step.Name]; ok && state.SubStoryRunName != "" {
		subRunName = state.SubStoryRunName
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
				"bubustack.io/parent-storyrun": srun.Name,
				"bubustack.io/parent-step":     step.Name,
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
		return subRun, nil
	}

	log.Info("Created sub-StoryRun", "name", subRunName, "waitForCompletion", waitForCompletion)
	return newSubRun, nil
}

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

func gatherManifestPathOperations(story *bubuv1alpha1.Story, targetStep string) map[string]map[runsv1alpha1.ManifestOperation]struct{} {
	pathOps := make(map[string]map[runsv1alpha1.ManifestOperation]struct{})
	addFromString := func(text string) {
		collectManifestPathsFromString(targetStep, text, pathOps)
	}

	addStepLevelManifestExpressions(story, addFromString)
	addStoryLevelManifestExpressions(story, addFromString)
	return pathOps
}

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

func addStoryLevelManifestExpressions(story *bubuv1alpha1.Story, add func(string)) {
	if story.Spec.Output != nil && len(story.Spec.Output.Raw) > 0 {
		add(string(story.Spec.Output.Raw))
	}
	if story.Spec.Policy != nil && story.Spec.Policy.With != nil && len(story.Spec.Policy.With.Raw) > 0 {
		add(string(story.Spec.Policy.With.Raw))
	}
}

func sortedManifestPaths(pathOps map[string]map[runsv1alpha1.ManifestOperation]struct{}) []string {
	paths := make([]string, 0, len(pathOps))
	for path := range pathOps {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func buildManifestRequests(paths []string, pathOps map[string]map[runsv1alpha1.ManifestOperation]struct{}) []runsv1alpha1.ManifestRequest {
	requests := make([]runsv1alpha1.ManifestRequest, 0, len(paths))
	orderedOps := []runsv1alpha1.ManifestOperation{runsv1alpha1.ManifestOperationExists, runsv1alpha1.ManifestOperationLength}
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

func normaliseManifestPath(suffix string) string {
	path := strings.TrimPrefix(suffix, ".")
	if path == "" {
		return manifestRootPath
	}
	return path
}

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

func (e *StepExecutor) mergeWithBlocks(engramWith, stepWith *runtime.RawExtension) (*runtime.RawExtension, error) {
	return mergeutil.MergeWithBlocks(engramWith, stepWith)
}

func manifestRequestsEqual(a, b []runsv1alpha1.ManifestRequest) bool {
	if len(a) != len(b) {
		return false
	}

	toMap := func(reqs []runsv1alpha1.ManifestRequest) map[string]map[runsv1alpha1.ManifestOperation]struct{} {
		out := make(map[string]map[runsv1alpha1.ManifestOperation]struct{}, len(reqs))
		for _, req := range reqs {
			set := make(map[runsv1alpha1.ManifestOperation]struct{}, len(req.Operations))
			for _, op := range req.Operations {
				set[op] = struct{}{}
			}
			out[req.Path] = set
		}
		return out
	}

	mapA := toMap(a)
	mapB := toMap(b)

	if len(mapA) != len(mapB) {
		return false
	}

	for path, opsA := range mapA {
		opsB, ok := mapB[path]
		if !ok {
			return false
		}
		if len(opsA) != len(opsB) {
			return false
		}
		for op := range opsA {
			if _, ok := opsB[op]; !ok {
				return false
			}
		}
	}
	return true
}

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

func rawExtensionsEqual(a, b *runtime.RawExtension) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return bytes.Equal(a.Raw, b.Raw)
	}
}

func cloneRawExtension(src *runtime.RawExtension) *runtime.RawExtension {
	if src == nil {
		return nil
	}
	clone := &runtime.RawExtension{}
	if len(src.Raw) > 0 {
		clone.Raw = append([]byte(nil), src.Raw...)
	}
	clone.Object = src.Object
	return clone
}

func copyManifestRequests(src []runsv1alpha1.ManifestRequest) []runsv1alpha1.ManifestRequest {
	if len(src) == 0 {
		return nil
	}

	out := make([]runsv1alpha1.ManifestRequest, len(src))
	for i := range src {
		out[i].Path = src[i].Path
		if len(src[i].Operations) > 0 {
			ops := make([]runsv1alpha1.ManifestOperation, len(src[i].Operations))
			copy(ops, src[i].Operations)
			out[i].Operations = ops
		}
	}
	return out
}
