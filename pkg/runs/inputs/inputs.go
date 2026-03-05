package inputs

import (
	"encoding/json"
	"fmt"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

// DecodeStoryRunInputs returns the StoryRun's spec.inputs as a map, defaulting to
// an empty map when the field is nil.
func DecodeStoryRunInputs(storyRun *runsv1alpha1.StoryRun) (map[string]any, error) {
	if storyRun == nil {
		return nil, fmt.Errorf("nil StoryRun provided")
	}
	if storyRun.Spec.Inputs == nil {
		return make(map[string]any), nil
	}

	var inputs map[string]any
	if err := json.Unmarshal(storyRun.Spec.Inputs.Raw, &inputs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal storyrun inputs: %w", err)
	}
	return inputs, nil
}

// MarshalStoryRunInputs encodes spec.inputs into canonical JSON bytes so callers
// can inject the payload into env vars or template contexts.
func MarshalStoryRunInputs(storyRun *runsv1alpha1.StoryRun) ([]byte, error) {
	inputs, err := DecodeStoryRunInputs(storyRun)
	if err != nil {
		return nil, err
	}
	return json.Marshal(inputs)
}
