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
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/refs"
)

func TestStepStatusPatchedBySDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		phase enums.Phase
		want  bool
	}{
		{name: "Failed is recognized as SDK-patched", phase: enums.PhaseFailed, want: true},
		{name: "Timeout is recognized as SDK-patched", phase: enums.PhaseTimeout, want: true},
		{name: "Succeeded is recognized as SDK-patched", phase: enums.PhaseSucceeded, want: true},
		{name: "Running is not SDK-patched", phase: enums.PhaseRunning, want: false},
		{name: "Pending is not SDK-patched", phase: enums.PhasePending, want: false},
	}

	reconciler := &StepRunReconciler{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			step := &runsv1alpha1.StepRun{
				Status: runsv1alpha1.StepRunStatus{
					Phase: tt.phase,
				},
			}
			require.Equal(t, tt.want, reconciler.stepStatusPatchedBySDK(step))
		})
	}
}

// noRetryPolicy returns a RetryPolicy with MaxRetries=0 to prevent
// ResolveRetryPolicy from defaulting to 3 retries.
func noRetryPolicy() *bubuv1alpha1.RetryPolicy {
	zero := int32(0)
	return &bubuv1alpha1.RetryPolicy{MaxRetries: &zero}
}

// TestHandleJobFailedPreservesSDKSucceeded verifies that when the SDK has
// already patched a StepRun to Succeeded (e.g. Engram finished work before
// the container was killed), the controller's handleJobFailed path does NOT
// overwrite the status with Failed. This is the core race-condition
// regression test.
func TestHandleJobFailedPreservesSDKSucceeded(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))

	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-sdk-succeeded",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "step",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
			EngramRef: &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: "engram"},
			},
			Retry: noRetryPolicy(),
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhaseSucceeded, // SDK already patched success
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      step.Name,
			Namespace: step.Namespace,
		},
		Status: batchv1.JobStatus{
			Failed: 1,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(step).
		WithObjects(step, job).
		Build()

	manager := config.NewOperatorConfigManager(client, "default", "bobrapet-operator-config")
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         client,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(client, manager),
		},
	}

	ctx := context.Background()
	logger := logging.NewControllerLogger(ctx, "test").WithStepRun(step)
	res, err := reconciler.handleJobFailed(ctx, step, job, logger)
	require.NoError(t, err)
	require.Zero(t, res.RequeueAfter, "should not requeue when SDK already patched Succeeded")

	// Verify the phase was preserved as Succeeded — NOT overwritten to Failed.
	var updated runsv1alpha1.StepRun
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &updated))
	require.Equal(t, enums.PhaseSucceeded, updated.Status.Phase,
		"controller must not overwrite SDK's Succeeded status when Job reports failure")
}

// TestHandleJobFailedPreservesSDKFailed verifies the existing behavior:
// when the SDK patched Failed with its own error details, the controller
// preserves that status rather than applying its own fallback.
func TestHandleJobFailedPreservesSDKFailed(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))

	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-sdk-failed",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "step",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
			EngramRef: &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: "engram"},
			},
			Retry: noRetryPolicy(),
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase:          enums.PhaseFailed, // SDK already patched failure
			LastFailureMsg: "SDK-reported: out of memory",
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      step.Name,
			Namespace: step.Namespace,
		},
		Status: batchv1.JobStatus{
			Failed: 1,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(step).
		WithObjects(step, job).
		Build()

	manager := config.NewOperatorConfigManager(client, "default", "bobrapet-operator-config")
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         client,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(client, manager),
		},
	}

	ctx := context.Background()
	logger := logging.NewControllerLogger(ctx, "test").WithStepRun(step)
	res, err := reconciler.handleJobFailed(ctx, step, job, logger)
	require.NoError(t, err)
	require.Zero(t, res.RequeueAfter)

	var updated runsv1alpha1.StepRun
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &updated))
	require.Equal(t, enums.PhaseFailed, updated.Status.Phase)
	require.Equal(t, "SDK-reported: out of memory", updated.Status.LastFailureMsg,
		"controller must preserve SDK's failure message, not overwrite with generic one")
}

// TestHandleJobFailedPreservesSDKTimeout verifies that when the SDK patched
// Timeout, the controller preserves it.
func TestHandleJobFailedPreservesSDKTimeout(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))

	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-sdk-timeout",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "step",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
			EngramRef: &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: "engram"},
			},
			Retry: noRetryPolicy(),
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhaseTimeout, // SDK already patched timeout
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      step.Name,
			Namespace: step.Namespace,
		},
		Status: batchv1.JobStatus{
			Failed: 1,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(step).
		WithObjects(step, job).
		Build()

	manager := config.NewOperatorConfigManager(client, "default", "bobrapet-operator-config")
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         client,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(client, manager),
		},
	}

	ctx := context.Background()
	logger := logging.NewControllerLogger(ctx, "test").WithStepRun(step)
	res, err := reconciler.handleJobFailed(ctx, step, job, logger)
	require.NoError(t, err)
	require.Zero(t, res.RequeueAfter)

	var updated runsv1alpha1.StepRun
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &updated))
	require.Equal(t, enums.PhaseTimeout, updated.Status.Phase,
		"controller must preserve SDK's Timeout status")
}

// TestHandleJobFailedAppliesFallbackForRunningPhase verifies that when the
// SDK has NOT patched the status (phase is still Running), the controller
// correctly applies its own failure fallback.
func TestHandleJobFailedAppliesFallbackForRunningPhase(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))

	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-no-sdk-patch",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "step",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
			EngramRef: &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: "engram"},
			},
			Retry: noRetryPolicy(),
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhaseRunning, // SDK did not patch — still Running
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      step.Name,
			Namespace: step.Namespace,
		},
		Status: batchv1.JobStatus{
			Failed: 1,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(step).
		WithObjects(step, job).
		Build()

	manager := config.NewOperatorConfigManager(client, "default", "bobrapet-operator-config")
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         client,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(client, manager),
		},
	}

	ctx := context.Background()
	logger := logging.NewControllerLogger(ctx, "test").WithStepRun(step)
	_, err := reconciler.handleJobFailed(ctx, step, job, logger)
	require.NoError(t, err)

	var updated runsv1alpha1.StepRun
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &updated))
	require.Equal(t, enums.PhaseFailed, updated.Status.Phase,
		"controller must apply failure fallback when SDK has not patched status")
}

// TestHandleJobStatusRoutesSucceededJobCorrectly verifies that when
// Kubernetes reports Job.Status.Succeeded > 0, the controller calls
// handleJobSucceeded (not handleJobFailed), ensuring no race window for
// successful jobs. EngramRef is nil so output validation is skipped.
func TestHandleJobStatusRoutesSucceededJobCorrectly(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))

	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-job-succeeded",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "step",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
			// EngramRef nil — skips output schema validation.
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhaseRunning,
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      step.Name,
			Namespace: step.Namespace,
		},
		Status: batchv1.JobStatus{
			Succeeded: 1,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(step).
		WithObjects(step, job).
		Build()

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: client,
			Scheme: scheme,
		},
	}

	ctx := context.Background()
	_, err := reconciler.handleJobStatus(ctx, step, job)
	require.NoError(t, err)

	var updated runsv1alpha1.StepRun
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &updated))
	require.Equal(t, enums.PhaseSucceeded, updated.Status.Phase,
		"Job with Succeeded > 0 must route to handleJobSucceeded and mark step Succeeded")
}

// TestHandleJobSucceededSkipsWhenSDKAlreadyPatched verifies that
// handleJobSucceeded does not re-patch when the SDK has already set
// Succeeded (the `step.Status.Phase != enums.PhaseSucceeded` guard).
func TestHandleJobSucceededSkipsWhenSDKAlreadyPatched(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))

	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-already-succeeded",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "step",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
			// EngramRef nil — skips output schema validation.
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase: enums.PhaseSucceeded, // SDK already patched
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(step).
		WithObjects(step).
		Build()

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: client,
			Scheme: scheme,
		},
	}

	ctx := context.Background()
	logger := logging.NewControllerLogger(ctx, "test").WithStepRun(step)
	_, err := reconciler.handleJobSucceeded(ctx, step, logger)
	require.NoError(t, err)

	var updated runsv1alpha1.StepRun
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &updated))
	require.Equal(t, enums.PhaseSucceeded, updated.Status.Phase)
}
