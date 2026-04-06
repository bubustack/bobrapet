/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/core/contracts"
)

func TestImpulseEnsureStorageSecretAllowsConfiguredDefaultCrossNamespaceCopy(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	sourceSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storage-auth",
			Namespace: "operator-system",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"accessKey": []byte("access")},
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

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sourceSecret, operatorConfig).
		Build()

	manager, err := config.NewOperatorConfigManager(client, "operator-system", "bobrapet-operator-config")
	require.NoError(t, err)
	require.NoError(t, manager.LoadInitial(context.Background()))

	reconciler := &ImpulseReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         client,
			APIReader:      client,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(client, manager),
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
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: "storage-auth", Namespace: "tenant-a"}, &copied))
	require.Equal(t, sourceSecret.Data["accessKey"], copied.Data["accessKey"])
}

func TestImpulseEnsureStorageSecretDeniesArbitraryCrossNamespaceCopy(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	operatorConfig := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bobrapet-operator-config",
			Namespace: "operator-system",
		},
		Data: map[string]string{
			contracts.KeyStorageS3AuthSecret: "storage-auth",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(operatorConfig).
		Build()

	manager, err := config.NewOperatorConfigManager(client, "operator-system", "bobrapet-operator-config")
	require.NoError(t, err)
	require.NoError(t, manager.LoadInitial(context.Background()))

	reconciler := &ImpulseReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         client,
			APIReader:      client,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(client, manager),
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
	err = client.Get(context.Background(), types.NamespacedName{Name: "other-secret", Namespace: "tenant-a"}, &copied)
	require.True(t, apierrors.IsNotFound(err))
}
