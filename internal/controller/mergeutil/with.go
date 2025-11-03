package mergeutil

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
)

// MergeWithBlocks merges two runtime.RawExtension values representing JSON "with" blocks.
// Values from the stepWith overlay the engramWith while preserving nested structures.
func MergeWithBlocks(engramWith, stepWith *runtime.RawExtension) (*runtime.RawExtension, error) {
	if engramWith == nil {
		return stepWith, nil
	}
	if stepWith == nil {
		return engramWith, nil
	}

	var engramMap, stepMap map[string]any
	if err := json.Unmarshal(engramWith.Raw, &engramMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal engram 'with' block: %w", err)
	}
	if err := json.Unmarshal(stepWith.Raw, &stepMap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal step 'with' block: %w", err)
	}

	merged := deepMergeJSONMaps(engramMap, stepMap)

	mergedBytes, err := json.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal merged 'with' block: %w", err)
	}

	return &runtime.RawExtension{Raw: mergedBytes}, nil
}

func deepMergeJSONMaps(base, overlay map[string]any) map[string]any {
	if base == nil && overlay == nil {
		return nil
	}
	out := make(map[string]any)
	for k, v := range base {
		out[k] = deepCloneJSONValue(v)
	}
	for k, v := range overlay {
		if existing, ok := out[k]; ok {
			out[k] = mergeJSONValue(existing, v)
			continue
		}
		out[k] = deepCloneJSONValue(v)
	}
	return out
}

func mergeJSONValue(base, overlay any) any {
	baseMap, baseOK := toStringAnyMap(base)
	overlayMap, overlayOK := toStringAnyMap(overlay)
	if baseOK && overlayOK {
		return deepMergeJSONMaps(baseMap, overlayMap)
	}
	return deepCloneJSONValue(overlay)
}

func deepCloneJSONValue(value any) any {
	switch v := value.(type) {
	case map[string]any:
		return deepMergeJSONMaps(v, nil)
	case []any:
		out := make([]any, len(v))
		for i := range v {
			out[i] = deepCloneJSONValue(v[i])
		}
		return out
	case string, float64, bool, nil:
		return v
	default:
		// For numbers unmarshalled as json.Number or other interfaces, fall back to JSON marshal/unmarshal clone.
		bytes, err := json.Marshal(v)
		if err != nil {
			return v
		}
		var clone any
		if err := json.Unmarshal(bytes, &clone); err != nil {
			return v
		}
		return clone
	}
}

func toStringAnyMap(value any) (map[string]any, bool) {
	switch v := value.(type) {
	case map[string]any:
		return v, true
	case map[any]any:
		out := make(map[string]any, len(v))
		for key, val := range v {
			strKey := fmt.Sprintf("%v", key)
			out[strKey] = val
		}
		return out, true
	default:
		return nil, false
	}
}
