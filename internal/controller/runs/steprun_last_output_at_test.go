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
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
)

func TestApplyCacheHitSetsLastOutputAt(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))

	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-cache",
			Namespace: "default",
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhasePending,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(step).
		WithObjects(step).
		Build()

	manager, err := config.NewOperatorConfigManager(cl, "default", "bobrapet-operator-config")
	require.NoError(t, err)
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	before := time.Now().Add(-time.Second)
	output := json.RawMessage(`{"result":"cached"}`)
	err = reconciler.applyCacheHit(context.Background(), step, output)
	require.NoError(t, err)

	var updated runsv1alpha1.StepRun
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Name:      step.Name,
		Namespace: step.Namespace,
	}, &updated))

	require.Equal(t, enums.PhaseSucceeded, updated.Status.Phase, "phase should be Succeeded after cache hit")
	require.NotNil(t, updated.Status.LastOutputAt, "LastOutputAt should be set after cache hit")
	require.False(t, updated.Status.LastOutputAt.Time.Before(before), "LastOutputAt should be recent")
	require.NotNil(t, updated.Status.Output, "Output should be set")
	require.JSONEq(t, `{"result":"cached"}`, string(updated.Status.Output.Raw))
}

func TestApplyCacheHitEmptyOutputSkipsLastOutputAt(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))

	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-cache-empty",
			Namespace: "default",
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhasePending,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(step).
		WithObjects(step).
		Build()

	manager, err := config.NewOperatorConfigManager(cl, "default", "bobrapet-operator-config")
	require.NoError(t, err)
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	err = reconciler.applyCacheHit(context.Background(), step, nil)
	require.NoError(t, err)

	var updated runsv1alpha1.StepRun
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{
		Name:      step.Name,
		Namespace: step.Namespace,
	}, &updated))

	require.Equal(t, enums.PhaseSucceeded, updated.Status.Phase, "phase should be Succeeded even with empty output")
	require.Nil(t, updated.Status.LastOutputAt, "LastOutputAt should not be set when output is empty")
}
