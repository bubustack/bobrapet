package inputs

import (
	"encoding/json"
	"fmt"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
)

const maxSchemaDefaultDepth = 32

// ResolveStoryRunInputs returns StoryRun inputs with schema defaults applied.
// Defaults are pulled from Story.Spec.InputsSchema "default" values.
func ResolveStoryRunInputs(story *bubuv1alpha1.Story, storyRun *runsv1alpha1.StoryRun) (map[string]any, error) {
	inputs, err := DecodeStoryRunInputs(storyRun)
	if err != nil {
		return nil, err
	}
	if story == nil || story.Spec.InputsSchema == nil || len(story.Spec.InputsSchema.Raw) == 0 {
		return inputs, nil
	}
	resolved, err := ApplySchemaDefaults(story.Spec.InputsSchema.Raw, inputs)
	if err != nil {
		return nil, err
	}
	return resolved, nil
}

// ResolveStoryRunInputsBytes marshals resolved StoryRun inputs with schema defaults applied.
func ResolveStoryRunInputsBytes(story *bubuv1alpha1.Story, storyRun *runsv1alpha1.StoryRun) ([]byte, error) {
	inputs, err := ResolveStoryRunInputs(story, storyRun)
	if err != nil {
		return nil, err
	}
	return json.Marshal(inputs)
}

// ApplySchemaDefaults applies JSON Schema "default" values to the provided input map.
// The returned map is a deep copy of the input with defaults applied.
func ApplySchemaDefaults(schemaRaw []byte, input map[string]any) (map[string]any, error) {
	if len(schemaRaw) == 0 {
		return cloneJSONMap(input)
	}
	var schema map[string]any
	if err := json.Unmarshal(schemaRaw, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}
	resolved, err := cloneJSONMap(input)
	if err != nil {
		return nil, err
	}
	applyDefaultsToObject(schema, resolved, 0)
	return resolved, nil
}

func applyDefaultsToObject(schema map[string]any, obj map[string]any, depth int) bool {
	if schema == nil || obj == nil || depth > maxSchemaDefaultDepth {
		return false
	}
	changed := false
	if allOf, ok := schema["allOf"].([]any); ok {
		for _, entry := range allOf {
			if sub, ok := entry.(map[string]any); ok {
				if applyDefaultsToObject(sub, obj, depth+1) {
					changed = true
				}
			}
		}
	}
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return changed
	}
	for key, raw := range props {
		propSchema, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		val, exists := obj[key]
		if !exists {
			if def, ok := propSchema["default"]; ok {
				obj[key] = cloneJSONValue(def)
				changed = true
				continue
			}
			if schemaHasPropertyDefaults(propSchema, depth+1) {
				child := make(map[string]any)
				if applyDefaultsToObject(propSchema, child, depth+1) {
					obj[key] = child
					changed = true
				}
			}
			continue
		}
		if nested, ok := val.(map[string]any); ok {
			if applyDefaultsToObject(propSchema, nested, depth+1) {
				changed = true
			}
		}
	}
	return changed
}

func schemaHasPropertyDefaults(schema map[string]any, depth int) bool {
	if schema == nil || depth > maxSchemaDefaultDepth {
		return false
	}
	if schema["type"] != nil && schema["type"] != "object" {
		return false
	}
	if allOf, ok := schema["allOf"].([]any); ok {
		for _, entry := range allOf {
			if sub, ok := entry.(map[string]any); ok {
				if schemaHasPropertyDefaults(sub, depth+1) {
					return true
				}
			}
		}
	}
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	for _, raw := range props {
		propSchema, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if _, ok := propSchema["default"]; ok {
			return true
		}
		if schemaHasPropertyDefaults(propSchema, depth+1) {
			return true
		}
	}
	return false
}

func cloneJSONMap(input map[string]any) (map[string]any, error) {
	if input == nil {
		return make(map[string]any), nil
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("failed to clone input map: %w", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("failed to clone input map: %w", err)
	}
	if out == nil {
		out = make(map[string]any)
	}
	return out, nil
}

func cloneJSONValue(value any) any {
	raw, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return value
	}
	return out
}
