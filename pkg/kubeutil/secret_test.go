/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0
*/

package kubeutil

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
)

func TestEnsureSecretCopyDeniesCrossNamespaceWithoutAuthorization(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	source := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storage-auth",
			Namespace: "operator-system",
		},
		Data: map[string][]byte{"accessKey": []byte("access")},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(source).
		Build()

	err := EnsureSecretCopy(context.Background(), client, client, "operator-system", "tenant-a", "storage-auth")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not explicitly authorized")

	var copied corev1.Secret
	err = client.Get(context.Background(), types.NamespacedName{Name: "storage-auth", Namespace: "tenant-a"}, &copied)
	require.True(t, apierrors.IsNotFound(err))
}

func TestEnsureSecretCopyAllowsAuthorizedCrossNamespaceCopy(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	source := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storage-auth",
			Namespace: "operator-system",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"accessKey": []byte("access")},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(source).
		Build()

	err := EnsureSecretCopy(
		context.Background(),
		client,
		client,
		"operator-system",
		"tenant-a",
		"storage-auth",
		AllowSecretCopyNames("storage-auth"),
	)
	require.NoError(t, err)

	var copied corev1.Secret
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: "storage-auth", Namespace: "tenant-a"}, &copied)) //nolint:lll
	require.Equal(t, source.Type, copied.Type)
	require.Equal(t, source.Data["accessKey"], copied.Data["accessKey"])
}

func TestEnsureSecretCopyRejectsUnmanagedTargetSecretCollision(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	source := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storage-auth",
			Namespace: "operator-system",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"accessKey": []byte("access")},
	}
	target := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storage-auth",
			Namespace: "tenant-a",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"accessKey": []byte("stale")},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(source, target).
		Build()

	err := EnsureSecretCopy(
		context.Background(),
		client,
		client,
		"operator-system",
		"tenant-a",
		"storage-auth",
		AllowSecretCopyNames("storage-auth"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "existing target secret is not controller-managed")

	var unchanged corev1.Secret
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: "storage-auth", Namespace: "tenant-a"}, &unchanged)) //nolint:lll
	require.Equal(t, []byte("stale"), unchanged.Data["accessKey"])
}

func TestEnsureSecretCopyRejectsManagedTargetFromUnexpectedSource(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	source := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storage-auth",
			Namespace: "operator-system",
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"accessKey": []byte("access")},
	}
	target := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storage-auth",
			Namespace: "tenant-a",
			Annotations: map[string]string{
				managedStorageSecretAnnotation:       "true",
				managedStorageSecretSourceAnnotation: "other-system/storage-auth",
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{"accessKey": []byte("stale")},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(source, target).
		Build()

	err := EnsureSecretCopy(
		context.Background(),
		client,
		client,
		"operator-system",
		"tenant-a",
		"storage-auth",
		AllowSecretCopyNames("storage-auth"),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected \"operator-system/storage-auth\"")

	var unchanged corev1.Secret
	nn := types.NamespacedName{Name: "storage-auth", Namespace: "tenant-a"}
	require.NoError(t, client.Get(context.Background(), nn, &unchanged))
	require.Equal(t, []byte("stale"), unchanged.Data["accessKey"])
}
