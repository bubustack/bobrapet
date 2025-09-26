package runs

import (
	"context"
	"encoding/json"
	"fmt"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
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
}

// NewStepExecutor creates a new StepExecutor.
func NewStepExecutor(client client.Client, scheme *runtime.Scheme) *StepExecutor {
	return &StepExecutor{Client: client, Scheme: scheme}
}

// Execute determines the step type and calls the appropriate execution method.
func (e *StepExecutor) Execute(ctx context.Context, srun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, step *bubuv1alpha1.Step) error {
	// An Engram step is defined by the presence of the 'ref' field.
	if step.Ref != nil {
		return e.executeEngramStep(ctx, srun, story, step)
	}

	// If 'ref' is not present, a 'type' must be specified.
	switch step.Type {
	case enums.StepTypeExecuteStory:
		return e.executeStoryStep(ctx, srun, step)
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
					"bubu.sh/storyrun":   srun.Name,
					"bubu.sh/story-name": story.Name,
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

func (e *StepExecutor) executeStoryStep(ctx context.Context, srun *runsv1alpha1.StoryRun, step *bubuv1alpha1.Step) error {
	log := logging.NewReconcileLogger(ctx, "step-executor").WithValues("storyrun", srun.Name)

	type executeStoryWith struct {
		StoryRef          refs.StoryReference   `json:"storyRef"`
		WaitForCompletion bool                  `json:"waitForCompletion"`
		Inputs            *runtime.RawExtension `json:"inputs"`
	}
	var config executeStoryWith

	if step.With == nil {
		return fmt.Errorf("step '%s' is type 'executeStory' but is missing 'with' block", step.Name)
	}
	if err := json.Unmarshal(step.With.Raw, &config); err != nil {
		return fmt.Errorf("failed to parse 'with' for executeStory step '%s': %w", step.Name, err)
	}

	subStoryRunName := fmt.Sprintf("%s-%s", srun.Name, step.Name)
	subStoryRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      subStoryRunName,
			Namespace: srun.Namespace,
			Labels:    map[string]string{"bubu.sh/parent-storyrun": srun.Name},
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: config.StoryRef,
			Inputs:   config.Inputs,
		},
	}

	if err := controllerutil.SetControllerReference(srun, subStoryRun, e.Scheme); err != nil {
		return err
	}

	if err := e.Client.Create(ctx, subStoryRun); err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	// Record the name of the sub-run in the parent's step state.
	srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{
		Phase:           enums.PhaseRunning,
		SubStoryRunName: subStoryRunName,
	}

	log.Info("Created sub-StoryRun", "subStoryRun", subStoryRun.Name, "waitForCompletion", config.WaitForCompletion)

	// If we are NOT waiting, we can consider the step complete immediately.
	if !config.WaitForCompletion {
		srun.Status.StepStates[step.Name] = runsv1alpha1.StepState{Phase: enums.PhaseSucceeded}
	}

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
