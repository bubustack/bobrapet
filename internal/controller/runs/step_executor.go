package runs

import (
	"context"
	"encoding/json"
	"fmt"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/cel"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	refs "github.com/bubustack/bobrapet/pkg/refs"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// StepExecutor is responsible for executing individual steps in a StoryRun.
type StepExecutor struct {
	client.Client
	Scheme *runtime.Scheme
	CEL    *cel.Evaluator
}

// NewStepExecutor creates a new StepExecutor.
func NewStepExecutor(client client.Client, scheme *runtime.Scheme, cel *cel.Evaluator) *StepExecutor {
	return &StepExecutor{Client: client, Scheme: scheme, CEL: cel}
}

// Execute determines the step type and calls the appropriate execution method.
func (e *StepExecutor) Execute(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, vars map[string]interface{}) error {
	// An Engram step is defined by the presence of the 'ref' field.
	if step.Ref != nil {
		return e.executeEngramStep(ctx, srun, story, step)
	}

	// If 'ref' is not present, a 'type' must be specified.
	switch step.Type {
	case enums.StepTypeExecuteStory:
		return e.executeStoryStep(ctx, srun, step)
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
	stepName := fmt.Sprintf("%s-%s", srun.Name, step.Name)
	var stepRun runsv1alpha1.StepRun
	err := e.Client.Get(ctx, types.NamespacedName{Name: stepName, Namespace: srun.Namespace}, &stepRun)

	if err != nil && errors.IsNotFound(err) {
		if step.Ref == nil {
			return fmt.Errorf("step '%s' is missing a 'ref'", step.Name)
		}

		// Get the engram to merge the 'with' blocks
		var engram bubuv1alpha1.Engram
		if err := e.Client.Get(ctx, step.Ref.ToNamespacedName(srun), &engram); err != nil {
			return fmt.Errorf("failed to get engram '%s' for step '%s': %w", step.Ref.Name, step.Name, err)
		}

		mergedWith, err := e.mergeWithBlocks(engram.Spec.With, step.With)
		if err != nil {
			return fmt.Errorf("failed to merge 'with' blocks for step '%s': %w", step.Name, err)
		}

		stepRun = runsv1alpha1.StepRun{
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
				StepID:    step.Name,
				EngramRef: step.Ref,
				Input:     mergedWith,
			},
		}
		if err := controllerutil.SetControllerReference(srun, &stepRun, e.Scheme); err != nil {
			return err
		}
		return e.Client.Create(ctx, &stepRun)
	}
	return err // Return other errors or nil if found
}

func (e *StepExecutor) executeLoopStep(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, vars map[string]interface{}) error {
	type loopWith struct {
		Items    json.RawMessage   `json:"items"`
		Template bubuv1alpha1.Step `json:"template"`
	}
	var config loopWith
	if err := json.Unmarshal(step.With.Raw, &config); err != nil {
		return fmt.Errorf("failed to parse 'with' for loop step '%s': %w", step.Name, err)
	}

	var itemsMap interface{}
	if err := json.Unmarshal(config.Items, &itemsMap); err != nil {
		// fallback to string
		var str string
		if err := json.Unmarshal(config.Items, &str); err != nil {
			return fmt.Errorf("failed to parse 'items' for loop step '%s': %w", step.Name, err)
		}
		itemsMap = map[string]interface{}{"value": str}
	}

	resolvedItemsRaw, err := e.CEL.ResolveWithInputs(ctx, itemsMap.(map[string]interface{}), vars)
	if err != nil {
		return fmt.Errorf("failed to resolve 'items' for loop step '%s': %w", step.Name, err)
	}

	resolvedItems, err := coerceToList(resolvedItemsRaw)
	if err != nil {
		return fmt.Errorf("'items' in loop step '%s' did not resolve to a list: %w", step.Name, err)
	}

	const maxIterations = 100
	if len(resolvedItems) > maxIterations {
		return fmt.Errorf("loop step '%s' exceeds maximum of %d iterations", step.Name, maxIterations)
	}

	var childStepRunNames []string
	for i, item := range resolvedItems {
		childStepName := fmt.Sprintf("%s-%s-%d", srun.Name, step.Name, i)
		childStepRunNames = append(childStepRunNames, childStepName)

		loopVars := map[string]interface{}{
			"inputs": vars["inputs"],
			"steps":  vars["steps"],
			"item":   item,
			"index":  i,
		}

		var withMap interface{}
		if config.Template.With != nil {
			if err := json.Unmarshal(config.Template.With.Raw, &withMap); err != nil {
				return fmt.Errorf("failed to parse 'with' for loop template in step '%s': %w", step.Name, err)
			}
		}

		resolvedWith, err := e.CEL.ResolveWithInputs(ctx, withMap.(map[string]interface{}), loopVars)
		if err != nil {
			return fmt.Errorf("failed to resolve 'with' for loop iteration %d in step '%s': %w", i, step.Name, err)
		}
		resolvedWithBytes, _ := json.Marshal(resolvedWith)

		stepRun := &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      childStepName,
				Namespace: srun.Namespace,
				Labels:    map[string]string{"bubustack.io/storyrun": srun.Name, "bubustack.io/parent-step": step.Name},
			},
			Spec: runsv1alpha1.StepRunSpec{
				StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: srun.Name}},
				StepID:      config.Template.Name,
				EngramRef:   config.Template.Ref,
				Input:       &runtime.RawExtension{Raw: resolvedWithBytes},
			},
		}
		if err := controllerutil.SetControllerReference(srun, stepRun, e.Scheme); err != nil {
			return err
		}
		if err := e.Client.Create(ctx, stepRun); err != nil && !errors.IsAlreadyExists(err) {
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

func (e *StepExecutor) executeParallelStep(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, vars map[string]interface{}) error {
	type parallelWith struct {
		Steps []bubuv1alpha1.Step `json:"steps"`
	}
	var config parallelWith
	if err := json.Unmarshal(step.With.Raw, &config); err != nil {
		return fmt.Errorf("failed to parse 'with' for parallel step '%s': %w", step.Name, err)
	}

	var childStepRunNames []string
	for _, childStep := range config.Steps {
		childStepName := fmt.Sprintf("%s-%s-%s", srun.Name, step.Name, childStep.Name)
		childStepRunNames = append(childStepRunNames, childStepName)

		var withMap interface{}
		if childStep.With != nil {
			if err := json.Unmarshal(childStep.With.Raw, &withMap); err != nil {
				return fmt.Errorf("failed to parse 'with' for parallel branch '%s' in step '%s': %w", childStep.Name, step.Name, err)
			}
		}

		resolvedWith, err := e.CEL.ResolveWithInputs(ctx, withMap.(map[string]interface{}), vars)
		if err != nil {
			return fmt.Errorf("failed to resolve 'with' for parallel branch '%s' in step '%s': %w", childStep.Name, step.Name, err)
		}
		resolvedWithBytes, _ := json.Marshal(resolvedWith)

		stepRun := &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      childStepName,
				Namespace: srun.Namespace,
				Labels:    map[string]string{"bubustack.io/storyrun": srun.Name, "bubustack.io/parent-step": step.Name},
			},
			Spec: runsv1alpha1.StepRunSpec{
				StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: srun.Name}},
				StepID:      childStep.Name,
				EngramRef:   childStep.Ref,
				Input:       &runtime.RawExtension{Raw: resolvedWithBytes},
			},
		}
		if err := controllerutil.SetControllerReference(srun, stepRun, e.Scheme); err != nil {
			return err
		}
		if err := e.Client.Create(ctx, stepRun); err != nil && !errors.IsAlreadyExists(err) {
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

func (e *StepExecutor) executeStopStep(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubuv1alpha1.Step) error {
	log := logging.NewReconcileLogger(ctx, "step-executor").WithValues("storyrun", srun.Name, "step", step.Name)

	type stopWith struct {
		Phase   enums.Phase `json:"phase"`
		Message string      `json:"message"`
	}
	var config stopWith

	if step.With != nil {
		if err := json.Unmarshal(step.With.Raw, &config); err != nil {
			return fmt.Errorf("failed to parse 'with' for stop step '%s': %w", step.Name, err)
		}
	}

	if config.Phase == "" {
		config.Phase = enums.PhaseSucceeded
	}
	if config.Message == "" {
		config.Message = fmt.Sprintf("Story execution stopped by step '%s' with phase '%s'", step.Name, config.Phase)
	}

	log.Info("Executing stop step", "phase", config.Phase, "message", config.Message)
	srun.Status.Phase = config.Phase
	srun.Status.Message = config.Message
	return nil
}

func (e *StepExecutor) executeStoryStep(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubuv1alpha1.Step) error {
	log := logging.NewReconcileLogger(ctx, "step-executor").WithValues("storyrun", srun.Name)

	type executeStoryWith struct {
		StoryRef *refs.ObjectReference `json:"storyRef"`
	}

	var with executeStoryWith
	if step.With != nil {
		if err := json.Unmarshal(step.With.Raw, &with); err != nil {
			return fmt.Errorf("failed to unmarshal step 'with' block for story step '%s': %w", step.Name, err)
		}
	}

	if with.StoryRef == nil {
		return fmt.Errorf("story step '%s' is missing a 'storyRef' in 'with' block", step.Name)
	}

	var story bubuv1alpha1.Story
	if err := e.Client.Get(ctx, with.StoryRef.ToNamespacedName(srun), &story); err != nil {
		return fmt.Errorf("failed to get story '%s' for story step '%s': %w", with.StoryRef.Name, step.Name, err)
	}

	log.Info("Executing story step", "story", story.Name)

	srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseSucceeded, Message: "Sub-story execution is not yet implemented."}

	return nil
}

func (e *StepExecutor) mergeWithBlocks(engramWith, stepWith *runtime.RawExtension) (*runtime.RawExtension, error) {
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

	for k, v := range stepMap {
		engramMap[k] = v
	}

	mergedBytes, err := json.Marshal(engramMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal merged 'with' block: %w", err)
	}

	return &runtime.RawExtension{Raw: mergedBytes}, nil
}

func coerceToList(input interface{}) ([]interface{}, error) {
	switch v := input.(type) {
	case []interface{}:
		return v, nil
	case []string:
		var list []interface{}
		for _, item := range v {
			list = append(list, item)
		}
		return list, nil
	case string:
		return []interface{}{v}, nil
	default:
		return nil, fmt.Errorf("input '%v' is not a list or string", input)
	}
}
