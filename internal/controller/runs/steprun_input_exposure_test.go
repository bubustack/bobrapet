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

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/refs"
)

func TestResolveRunScopedInputsDoesNotPersistSpecInput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "input-exposure",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "input-step",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
			Input: &runtime.RawExtension{
				Raw: []byte(`{"secret":"value"}`),
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: "story"},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(stepRun).
		WithObjects(stepRun.DeepCopy(), storyRun).
		Build()

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:            client,
			Scheme:            scheme,
			TemplateEvaluator: newTestEvaluator(t),
		},
	}

	originalRaw := append([]byte(nil), stepRun.Spec.Input.Raw...)
	inputSchema := &runtime.RawExtension{
		Raw: []byte(`{
			"type":"object",
			"properties":{
				"secret":{"type":"string"},
				"mode":{"type":"string","default":"safe"}
			}
		}`),
	}

	resolvedBytes, err := reconciler.resolveRunScopedInputs(ctx, stepRun, storyRun, nil, inputSchema, "Step inputs")
	require.NoError(t, err)
	require.JSONEq(t, `{"secret":"value","mode":"safe"}`, string(resolvedBytes))

	var persisted runsv1alpha1.StepRun
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, &persisted))
	require.JSONEq(t, string(originalRaw), string(persisted.Spec.Input.Raw))
}
