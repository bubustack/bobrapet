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

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/refs"
)

func TestHandleJobSucceededRejectsInvalidOutput(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))

	outputSchema, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			"foo": map[string]any{"type": "string"},
		},
		"required": []any{"foo"},
	})
	require.NoError(t, err)

	invalidOutput, err := json.Marshal(map[string]any{
		"foo": 123,
	})
	require.NoError(t, err)

	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "template",
		},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			OutputSchema: &runtime.RawExtension{Raw: outputSchema},
		},
	}
	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "engram",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{
				Name: template.Name,
			},
		},
	}
	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "step",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
			EngramRef: &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: engram.Name},
			},
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase:  enums.PhaseRunning,
			Output: &runtime.RawExtension{Raw: invalidOutput},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(step).
		WithObjects(step, engram, template).
		Build()

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: client,
			Scheme: scheme,
		},
	}

	ctx := context.Background()
	logger := logging.NewControllerLogger(ctx, "test").WithStepRun(step)
	_, err = reconciler.handleJobSucceeded(ctx, step, logger)
	require.NoError(t, err)

	var updated runsv1alpha1.StepRun
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &updated))
	require.Equal(t, enums.PhaseFailed, updated.Status.Phase)
}
