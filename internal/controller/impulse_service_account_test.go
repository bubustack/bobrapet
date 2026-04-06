package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/logging"
)

func TestEnsureImpulseServiceAccountRejectsUnmanagedCollision(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	impulse := &v1alpha1.Impulse{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-impulse",
			Namespace: "default",
			UID:       types.UID("impulse-uid"),
		},
	}
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-impulse-impulse-runner",
			Namespace: impulse.Namespace,
			Annotations: map[string]string{
				"preserve.example/key": "keep",
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		Build()

	reconciler := &ImpulseReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: client,
			Scheme: scheme,
		},
	}
	resolved := &config.ResolvedExecutionConfig{}

	managed, err := reconciler.ensureImpulseServiceAccount(context.Background(), impulse, resolved, logging.NewControllerLogger(context.Background(), "test"))
	require.False(t, managed)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing to adopt existing ServiceAccount")

	var sa corev1.ServiceAccount
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: existing.Name, Namespace: existing.Namespace}, &sa))
	require.Equal(t, "keep", sa.Annotations["preserve.example/key"])
}

func TestEnsureImpulseServiceAccountAllowsControllerOwnedServiceAccount(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	impulse := &v1alpha1.Impulse{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-impulse",
			Namespace: "default",
			UID:       types.UID("impulse-uid"),
		},
	}
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-impulse-impulse-runner",
			Namespace: impulse.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1alpha1.GroupVersion.String(),
				Kind:       "Impulse",
				Name:       impulse.Name,
				UID:        impulse.UID,
				Controller: ptrTo(true),
			}},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existing).
		Build()

	reconciler := &ImpulseReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: client,
			Scheme: scheme,
		},
	}
	resolved := &config.ResolvedExecutionConfig{}

	managed, err := reconciler.ensureImpulseServiceAccount(context.Background(), impulse, resolved, logging.NewControllerLogger(context.Background(), "test"))
	require.True(t, managed)
	require.NoError(t, err)
	require.Equal(t, "demo-impulse-impulse-runner", resolved.ServiceAccountName)
	require.True(t, resolved.AutomountServiceAccountToken)
}

func TestEnsureImpulseServiceAccountPreservesCustomServiceAccountAndAutomount(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	impulse := &v1alpha1.Impulse{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-impulse",
			Namespace: "default",
			UID:       types.UID("impulse-uid"),
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	reconciler := &ImpulseReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: client,
			Scheme: scheme,
		},
	}
	resolved := &config.ResolvedExecutionConfig{
		ServiceAccountName: "pod-crash-runner",
	}

	managed, err := reconciler.ensureImpulseServiceAccount(context.Background(), impulse, resolved, logging.NewControllerLogger(context.Background(), "test"))
	require.False(t, managed)
	require.NoError(t, err)
	require.Equal(t, "pod-crash-runner", resolved.ServiceAccountName)
	require.True(t, resolved.AutomountServiceAccountToken)
}

func ptrTo(v bool) *bool {
	return &v
}
