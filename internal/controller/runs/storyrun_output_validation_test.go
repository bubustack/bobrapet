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
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/templating"
)

func TestFinalizeSuccessfulRunRejectsInvalidOutput(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))

	outputSchema, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"foo": map[string]any{"type": "string"},
		},
		"required": []any{"foo"},
	})
	require.NoError(t, err)

	outputTemplate, err := json.Marshal(map[string]any{
		"foo": 123,
	})
	require.NoError(t, err)

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "story",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "step1",
				Ref: &refs.EngramReference{
					ObjectReference: refs.ObjectReference{Name: "engram"},
				},
			}},
			Output:        &runtime.RawExtension{Raw: outputTemplate},
			OutputsSchema: &runtime.RawExtension{Raw: outputSchema},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: story.Name},
			},
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase: enums.PhaseRunning,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(storyRun).
		WithObjects(story, storyRun).
		Build()

	eval, err := templating.New(templating.Config{})
	require.NoError(t, err)
	t.Cleanup(eval.Close)

	reconciler := &DAGReconciler{
		Client:            client,
		TemplateEvaluator: eval,
	}

	ctx := context.Background()
	err = reconciler.finalizeSuccessfulRun(ctx, storyRun, story)
	require.NoError(t, err)

	var updated runsv1alpha1.StoryRun
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: storyRun.Name, Namespace: storyRun.Namespace}, &updated))
	require.Equal(t, enums.PhaseFailed, updated.Status.Phase)
	require.Contains(t, updated.Status.Message, "schema")
}
