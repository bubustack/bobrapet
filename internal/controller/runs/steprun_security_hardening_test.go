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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
	coreenv "github.com/bubustack/core/runtime/env"
)

func TestBuildJobSpecForcesServiceAccountTokenAutomount(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bobrapet-operator-config",
				Namespace: "default",
			},
			Data: map[string]string{},
		}).
		Build()

	manager, err := config.NewOperatorConfigManager(cl, "default", "bobrapet-operator-config")
	require.NoError(t, err)
	require.NoError(t, manager.LoadInitial(context.Background()))

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step",
			Namespace: "default",
		},
	}
	resolved := &config.ResolvedExecutionConfig{
		Image:                        "ghcr.io/bubustack/test:latest",
		ServiceAccountName:           "managed-step-runner",
		AutomountServiceAccountToken: false,
		RestartPolicy:                corev1.RestartPolicyNever,
	}

	job := reconciler.buildJobSpec(stepRun, resolved, nil, nil, nil, nil, 30, "")

	require.NotNil(t, job.Spec.Template.Spec.AutomountServiceAccountToken)
	require.True(t, *job.Spec.Template.Spec.AutomountServiceAccountToken)
	require.Equal(t, "managed-step-runner", job.Spec.Template.Spec.ServiceAccountName)
}

func TestEnsureStorageSecret_AllowsConfiguredDefaultCrossNamespaceCopy(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	sourceSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storage-auth",
			Namespace: "operator-system",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"accessKey": []byte("access"),
		},
	}
	operatorConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bobrapet-operator-config",
			Namespace: "operator-system",
		},
		Data: map[string]string{
			contracts.KeyStorageS3AuthSecret: "storage-auth",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sourceSecret, operatorConfig).
		Build()

	manager, err := config.NewOperatorConfigManager(cl, "operator-system", "bobrapet-operator-config")
	require.NoError(t, err)
	require.NoError(t, manager.LoadInitial(context.Background()))

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			APIReader:      cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	resolved := &config.ResolvedExecutionConfig{
		Storage: &v1alpha1.StoragePolicy{
			S3: &v1alpha1.S3StorageProvider{
				Authentication: v1alpha1.S3Authentication{
					SecretRef: &corev1.LocalObjectReference{Name: "storage-auth"},
				},
			},
		},
	}

	err = reconciler.ensureStorageSecret(context.Background(), "tenant-a", resolved, logging.NewControllerLogger(context.Background(), "test"))
	require.NoError(t, err)

	var copied corev1.Secret
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: "storage-auth", Namespace: "tenant-a"}, &copied))
	require.Equal(t, sourceSecret.Type, copied.Type)
	require.Equal(t, sourceSecret.Data["accessKey"], copied.Data["accessKey"])
}

func TestEnsureStorageSecret_DeniesArbitraryCrossNamespaceCopy(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "bobrapet-operator-config",
				Namespace: "operator-system",
			},
			Data: map[string]string{
				contracts.KeyStorageS3AuthSecret: "storage-auth",
			},
		}).
		Build()

	manager, err := config.NewOperatorConfigManager(cl, "operator-system", "bobrapet-operator-config")
	require.NoError(t, err)
	require.NoError(t, manager.LoadInitial(context.Background()))

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			APIReader:      cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	resolved := &config.ResolvedExecutionConfig{
		Storage: &v1alpha1.StoragePolicy{
			S3: &v1alpha1.S3StorageProvider{
				Authentication: v1alpha1.S3Authentication{
					SecretRef: &corev1.LocalObjectReference{Name: "other-secret"},
				},
			},
		},
	}

	err = reconciler.ensureStorageSecret(context.Background(), "tenant-a", resolved, logging.NewControllerLogger(context.Background(), "test"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "cross-namespace storage secret sync denied")

	var copied corev1.Secret
	err = cl.Get(context.Background(), types.NamespacedName{Name: "other-secret", Namespace: "tenant-a"}, &copied)
	require.True(t, apierrors.IsNotFound(err))
}

func TestAssembleJobResources_FailsClosedWhenTLSSecretMissing(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	manager, err := config.NewOperatorConfigManager(cl, "default", "bobrapet-operator-config")
	require.NoError(t, err)
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			APIReader:      cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "step",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
		},
	}
	engram := &v1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "engram",
			Namespace: "default",
			Annotations: map[string]string{
				contracts.EngramTLSSecretAnnotation: "missing-tls-secret",
			},
		},
	}
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template"},
	}

	_, err = reconciler.assembleJobResources(context.Background(), jobResourceInputs{
		srun:           stepRun,
		engram:         engram,
		engramTemplate: template,
		resolvedConfig: &config.ResolvedExecutionConfig{},
		runtimeEnvCfg:  coreenv.Config{},
		inputBytes:     []byte("{}"),
		stepTimeout:    time.Second,
		executionMode:  "batch",
		stepLogger:     logging.NewControllerLogger(context.Background(), "test").WithStepRun(stepRun),
	})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "failed to configure TLS for StepRun job"))
}

func TestAssembleJobResourcesUsesSecretForTriggerData(t *testing.T) {
	t.Skip("jobResources.podAnnotations not yet implemented")
	t.Parallel()

	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "bobrapet-operator-config", Namespace: "default"},
			Data:       map[string]string{},
		}).
		Build()

	manager, err := config.NewOperatorConfigManager(cl, "default", "bobrapet-operator-config")
	require.NoError(t, err)
	require.NoError(t, manager.LoadInitial(ctx))

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step",
			Namespace: "default",
			UID:       types.UID("step-uid"),
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "step",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
		},
	}
	engram := &v1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "engram",
			Namespace: "default",
		},
	}
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template"},
	}
	inputBytes := []byte(`{"hello":"world"}`)

	resources, err := reconciler.assembleJobResources(ctx, jobResourceInputs{
		srun:           stepRun,
		engram:         engram,
		engramTemplate: template,
		resolvedConfig: &config.ResolvedExecutionConfig{},
		runtimeEnvCfg:  coreenv.Config{},
		inputBytes:     inputBytes,
		stepTimeout:    time.Second,
		executionMode:  "batch",
		stepLogger:     logging.NewControllerLogger(ctx, "test").WithStepRun(stepRun),
	})
	require.NoError(t, err)

	triggerEnv := findEnvVar(resources.envVars, contracts.TriggerDataEnv)
	require.NotNil(t, triggerEnv)
	require.Empty(t, triggerEnv.Value)
	require.NotNil(t, triggerEnv.ValueFrom)
	require.NotNil(t, triggerEnv.ValueFrom.SecretKeyRef)
	require.Equal(t, triggerDataSecretName(stepRun), triggerEnv.ValueFrom.SecretKeyRef.Name)
	require.Equal(t, triggerDataSecretKey, triggerEnv.ValueFrom.SecretKeyRef.Key)
	require.Equal(t, map[string]string{triggerDataHashAnnotation: hashTriggerDataPayload(inputBytes)}, resources.podAnnotations)

	var secret corev1.Secret
	require.NoError(t, cl.Get(ctx, types.NamespacedName{Name: triggerDataSecretName(stepRun), Namespace: stepRun.Namespace}, &secret))
	require.JSONEq(t, string(inputBytes), string(secret.Data[triggerDataSecretKey]))

	owner := metav1.GetControllerOf(&secret)
	require.NotNil(t, owner)
	require.Equal(t, "StepRun", owner.Kind)
	require.Equal(t, stepRun.Name, owner.Name)
}

func findEnvVar(envVars []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range envVars {
		if envVars[i].Name == name {
			return &envVars[i]
		}
	}
	return nil
}
