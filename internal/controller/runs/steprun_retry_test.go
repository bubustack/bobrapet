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
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

func TestComputeRetryDelayBackoffStrategies(t *testing.T) {
	t.Parallel()

	delay := "1s"
	exponential := enums.BackoffStrategyExponential
	linear := enums.BackoffStrategyLinear
	constant := enums.BackoffStrategyConstant

	policy := &bubuv1alpha1.RetryPolicy{
		Delay:   &delay,
		Backoff: &exponential,
	}

	require.Equal(t, time.Second, computeRetryDelay(policy, 1, nil))
	require.Equal(t, 2*time.Second, computeRetryDelay(policy, 2, nil))

	policy.Backoff = &linear
	require.Equal(t, 3*time.Second, computeRetryDelay(policy, 3, nil))

	policy.Backoff = &constant
	require.Equal(t, time.Second, computeRetryDelay(policy, 3, nil))
}

func TestComputeRetryDelayMaxDelayCaps(t *testing.T) {
	t.Parallel()

	delay := "1s"
	maxDelay := "1500ms"
	exponential := enums.BackoffStrategyExponential

	policy := &bubuv1alpha1.RetryPolicy{
		Delay:    &delay,
		MaxDelay: &maxDelay,
		Backoff:  &exponential,
	}

	require.Equal(t, time.Second, computeRetryDelay(policy, 1, nil))
	require.Equal(t, 1500*time.Millisecond, computeRetryDelay(policy, 2, nil))
}

func TestComputeRetryDelayJitter(t *testing.T) {
	t.Parallel()

	delay := "2s"
	jitter := int32(50)
	constant := enums.BackoffStrategyConstant

	policy := &bubuv1alpha1.RetryPolicy{
		Delay:   &delay,
		Jitter:  &jitter,
		Backoff: &constant,
	}

	got := computeRetryDelay(policy, 1, nil)
	require.GreaterOrEqual(t, got, 2*time.Second)
	require.LessOrEqual(t, got, 3*time.Second)
}

func TestHandleJobFailedSchedulesRetry(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))

	maxRetries := int32(2)
	delay := "1s"
	backoff := enums.BackoffStrategyConstant

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
				ObjectReference: refs.ObjectReference{Name: "engram"},
			},
			Retry: &bubuv1alpha1.RetryPolicy{
				MaxRetries: &maxRetries,
				Delay:      &delay,
				Backoff:    &backoff,
			},
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase:     enums.PhaseRunning,
			ExitClass: enums.ExitClassRetry,
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

	manager, err := config.NewOperatorConfigManager(client, "default", "bobrapet-operator-config")
	require.NoError(t, err)
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
	require.Greater(t, res.RequeueAfter, time.Duration(0))

	var updated runsv1alpha1.StepRun
	require.NoError(t, client.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &updated))
	require.Equal(t, enums.PhasePending, updated.Status.Phase)
	require.Equal(t, int32(1), updated.Status.Retries)
	require.NotNil(t, updated.Status.NextRetryAt)

	var deleted batchv1.Job
	err = client.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &deleted)
	require.True(t, apierrors.IsNotFound(err))
}

func TestReconcileJobExecutionRetryDueDeletesJob(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))

	retryAt := metav1.NewTime(time.Now().Add(-time.Minute))
	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step",
			Namespace: "default",
		},
		Status: runsv1alpha1.StepRunStatus{
			NextRetryAt: &retryAt,
		},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      step.Name,
			Namespace: step.Namespace,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(step).
		WithObjects(step, job).
		Build()

	manager, err := config.NewOperatorConfigManager(client, "default", "bobrapet-operator-config")
	require.NoError(t, err)
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         client,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(client, manager),
		},
	}

	res, err := reconciler.reconcileJobExecution(context.Background(), step, nil, nil)
	require.NoError(t, err)
	require.Greater(t, res.RequeueAfter, time.Duration(0))

	var deleted batchv1.Job
	err = client.Get(context.Background(), types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &deleted)
	require.True(t, apierrors.IsNotFound(err))
}

func TestReconcileJobExecutionRetryDueWithActivePods(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	retryAt := metav1.NewTime(time.Now().Add(-time.Minute))
	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step",
			Namespace: "default",
		},
		Status: runsv1alpha1.StepRunStatus{
			NextRetryAt: &retryAt,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "active-pod",
			Namespace: step.Namespace,
			Labels: map[string]string{
				"job-name": step.Name,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(step).
		WithObjects(step, pod).
		Build()

	manager, err := config.NewOperatorConfigManager(client, "default", "bobrapet-operator-config")
	require.NoError(t, err)
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         client,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(client, manager),
		},
	}

	res, err := reconciler.reconcileJobExecution(context.Background(), step, nil, nil)
	require.NoError(t, err)
	require.Greater(t, res.RequeueAfter, time.Duration(0))

	var updated runsv1alpha1.StepRun
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &updated))
	require.Nil(t, updated.Status.NextRetryAt)

	var job batchv1.Job
	err = client.Get(context.Background(), types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &job)
	require.True(t, apierrors.IsNotFound(err))
}

func TestClassifyExitCode_UnknownForSentinel(t *testing.T) {
	t.Parallel()
	require.Equal(t, enums.ExitClassUnknown, classifyExitCode(-1))
}

func TestRetryableExitClass_UnknownIsRetryable(t *testing.T) {
	t.Parallel()
	require.True(t, retryableExitClass(enums.ExitClassUnknown))
}

func TestClassifyExitCode_SuccessStillZero(t *testing.T) {
	t.Parallel()
	// Ensure 0 is still success (not accidentally caught by -1 change)
	require.Equal(t, enums.ExitClassSuccess, classifyExitCode(0))
}

func TestScheduleRetryIfNeeded_UnknownExitConsumesBudget(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))

	maxRetries := int32(1)
	delay := "1s"
	backoff := enums.BackoffStrategyConstant
	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unknown-retry-step",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			Retry: &bubuv1alpha1.RetryPolicy{
				MaxRetries: &maxRetries,
				Delay:      &delay,
				Backoff:    &backoff,
			},
		},
		Status: runsv1alpha1.StepRunStatus{
			Phase:   enums.PhaseRunning,
			Retries: 0,
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

	ctx := context.Background()
	logger := logging.NewControllerLogger(ctx, "test").WithStepRun(step)

	scheduled, res, err := reconciler.scheduleRetryIfNeeded(ctx, step, nil, -1, enums.ExitClassUnknown, "temporary infrastructure issue", logger)
	require.NoError(t, err)
	require.True(t, scheduled)
	require.Greater(t, res.RequeueAfter, time.Duration(0))

	var updated runsv1alpha1.StepRun
	require.NoError(t, cl.Get(ctx, types.NamespacedName{Name: step.Name, Namespace: step.Namespace}, &updated))
	require.Equal(t, int32(1), updated.Status.Retries)
	require.NotNil(t, updated.Status.NextRetryAt)

	scheduled, _, err = reconciler.scheduleRetryIfNeeded(ctx, &updated, nil, -1, enums.ExitClassUnknown, "temporary infrastructure issue", logger)
	require.NoError(t, err)
	require.False(t, scheduled)
}
