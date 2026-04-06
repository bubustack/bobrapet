package inputs

import (
	"encoding/json"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestApplySchemaDefaults(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"region": map[string]any{
				"type":    "string",
				"default": "us-east-1",
			},
			"nested": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"enabled": map[string]any{
						"type":    "boolean",
						"default": true,
					},
				},
			},
		},
	}
	schemaRaw, err := json.Marshal(schema)
	require.NoError(t, err)

	resolved, err := ApplySchemaDefaults(schemaRaw, map[string]any{})
	require.NoError(t, err)
	require.Equal(t, "us-east-1", resolved["region"])
	require.Equal(t, map[string]any{"enabled": true}, resolved["nested"])

	resolved, err = ApplySchemaDefaults(schemaRaw, map[string]any{"region": "eu"})
	require.NoError(t, err)
	require.Equal(t, "eu", resolved["region"])
}

func TestApplySchemaDefaultsInvalidSchema(t *testing.T) {
	_, err := ApplySchemaDefaults([]byte("{not-json}"), map[string]any{})
	require.Error(t, err)
}

func TestResolveStoryRunInputsAppliesDefaults(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"mode": map[string]any{
				"type":    "string",
				"default": "batch",
			},
		},
	}
	schemaRaw, err := json.Marshal(schema)
	require.NoError(t, err)

	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			InputsSchema: &runtime.RawExtension{Raw: schemaRaw},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		Spec: runsv1alpha1.StoryRunSpec{
			Inputs: &runtime.RawExtension{Raw: []byte(`{}`)},
		},
	}

	inputs, err := ResolveStoryRunInputs(story, storyRun)
	require.NoError(t, err)
	require.Equal(t, "batch", inputs["mode"])
}
