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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
)

func TestResolveNestedPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		data   map[string]any
		path   []string
		expect any
	}{
		{
			name:   "resolves top-level key",
			data:   map[string]any{"foo": "bar"},
			path:   []string{"foo"},
			expect: "bar",
		},
		{
			name: "resolves nested key",
			data: map[string]any{
				"steps": map[string]any{
					"fetch": map[string]any{
						"output": map[string]any{
							"body": "hello",
						},
					},
				},
			},
			path:   []string{"steps", "fetch", "output", "body"},
			expect: "hello",
		},
		{
			name: "returns nil for missing key",
			data: map[string]any{
				"steps": map[string]any{
					"fetch": map[string]any{},
				},
			},
			path:   []string{"steps", "fetch", "output", "body"},
			expect: nil,
		},
		{
			name:   "returns nil for non-map intermediate",
			data:   map[string]any{"steps": "not-a-map"},
			path:   []string{"steps", "fetch"},
			expect: nil,
		},
		{
			name:   "returns nil for empty path on empty map",
			data:   map[string]any{},
			path:   []string{"missing"},
			expect: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := resolveNestedPath(tt.data, tt.path)
			assert.Equal(t, tt.expect, result)
		})
	}
}

func TestCheckRequiresContext(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	story := &v1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-story",
			Namespace: "default",
		},
		Spec: v1alpha1.StorySpec{
			Steps: []v1alpha1.Step{
				{Name: "fetch", Type: enums.StepTypeCondition},
				{
					Name:     "process",
					Type:     enums.StepTypeCondition,
					Needs:    []string{"fetch"},
					Requires: []string{"steps.fetch.output.body"},
				},
			},
		},
	}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "test-story"}},
		},
	}

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-step",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID:      "process",
			StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: "test-run"}},
		},
	}

	t.Run("returns error when required path is nil", func(t *testing.T) {
		t.Parallel()
		client := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(story, storyRun, stepRun).
			Build()

		reconciler := &StepRunReconciler{
			ControllerDependencies: config.ControllerDependencies{
				Client: client,
				Scheme: scheme,
			},
		}

		vars := map[string]any{
			"inputs": map[string]any{},
			"steps": map[string]any{
				"fetch": map[string]any{
					"output": map[string]any{},
				},
			},
		}

		err := reconciler.checkRequiresContext(context.Background(), stepRun, vars)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "required context")
		assert.Contains(t, err.Error(), "steps.fetch.output.body")
	})

	t.Run("passes when required path is present", func(t *testing.T) {
		t.Parallel()
		client := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(story, storyRun, stepRun).
			Build()

		reconciler := &StepRunReconciler{
			ControllerDependencies: config.ControllerDependencies{
				Client: client,
				Scheme: scheme,
			},
		}

		vars := map[string]any{
			"inputs": map[string]any{},
			"steps": map[string]any{
				"fetch": map[string]any{
					"output": map[string]any{
						"body": "some-data",
					},
				},
			},
		}

		err := reconciler.checkRequiresContext(context.Background(), stepRun, vars)
		require.NoError(t, err)
	})

	t.Run("passes when step has no requires", func(t *testing.T) {
		t.Parallel()
		noReqStory := &v1alpha1.Story{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "no-req-story",
				Namespace: "default",
			},
			Spec: v1alpha1.StorySpec{
				Steps: []v1alpha1.Step{
					{Name: "simple", Type: enums.StepTypeCondition},
				},
			},
		}
		noReqStoryRun := &runsv1alpha1.StoryRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "no-req-run",
				Namespace: "default",
			},
			Spec: runsv1alpha1.StoryRunSpec{
				StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "no-req-story"}},
			},
		}
		noReqStep := &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "no-req-step",
				Namespace: "default",
			},
			Spec: runsv1alpha1.StepRunSpec{
				StepID:      "simple",
				StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: "no-req-run"}},
			},
		}
		client := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(noReqStory, noReqStoryRun, noReqStep).
			Build()

		reconciler := &StepRunReconciler{
			ControllerDependencies: config.ControllerDependencies{
				Client: client,
				Scheme: scheme,
			},
		}

		err := reconciler.checkRequiresContext(context.Background(), noReqStep, map[string]any{})
		require.NoError(t, err)
	})
}
