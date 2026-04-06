package runs

import (
	"context"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type staleStepRunClient struct {
	client.Client
	staleKey types.NamespacedName
}

func (c *staleStepRunClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if key == c.staleKey {
		if _, ok := obj.(*runsv1alpha1.StepRun); ok {
			return apierrors.NewNotFound(schema.GroupResource{
				Group:    runsv1alpha1.GroupVersion.Group,
				Resource: "stepruns",
			}, key.Name)
		}
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func TestEffectClaimReconcileUsesAPIReaderBeforeDeletingOnCacheMiss(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))

	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "step-effect-live",
			Namespace: "default",
			UID:       types.UID("step-effect-live-uid"),
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun-live"},
			},
			StepID: "deliver",
		},
	}
	claim := &runsv1alpha1.EffectClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      runsidentity.DeriveEffectClaimName(step.Namespace, step.Name, "deliver-webhook"),
			Namespace: step.Namespace,
		},
		Spec: runsv1alpha1.EffectClaimSpec{
			StepRunRef: refs.StepRunReference{
				ObjectReference: refs.ObjectReference{Name: step.Name},
				UID:             &step.UID,
			},
			EffectKey:            "deliver-webhook",
			HolderIdentity:       "worker-a",
			LeaseDurationSeconds: 30,
		},
	}

	backingClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(claim).
		WithObjects(step, claim).
		Build()

	reconciler := &EffectClaimReconciler{
		Client: &staleStepRunClient{
			Client:   backingClient,
			staleKey: types.NamespacedName{Name: step.Name, Namespace: step.Namespace},
		},
		APIReader: backingClient,
		Scheme:    scheme,
	}

	_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKeyFromObject(claim),
	})
	require.NoError(t, err)

	var updated runsv1alpha1.EffectClaim
	require.NoError(t, backingClient.Get(context.Background(), client.ObjectKeyFromObject(claim), &updated))
	require.Equal(t, runsv1alpha1.EffectClaimPhaseReserved, updated.Status.Phase)
	require.True(t, hasStepRunOwnerReference(updated.OwnerReferences, step), "effect claim should remain attached to the live StepRun recovered via APIReader")
}

func TestEffectClaimReconcileDeletesCrossNamespaceClaim(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))

	otherNamespace := "other" //nolint:goconst
	claim := &runsv1alpha1.EffectClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      runsidentity.DeriveEffectClaimName(otherNamespace, "step-cross-ns", "deliver-webhook"),
			Namespace: "default",
		},
		Spec: runsv1alpha1.EffectClaimSpec{
			StepRunRef: refs.StepRunReference{
				ObjectReference: refs.ObjectReference{
					Name:      "step-cross-ns",
					Namespace: &otherNamespace,
				},
			},
			EffectKey: "deliver-webhook",
		},
	}

	backingClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(claim).
		WithObjects(claim).
		Build()

	reconciler := &EffectClaimReconciler{
		Client:    backingClient,
		APIReader: backingClient,
		Scheme:    scheme,
	}

	_, err := reconciler.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKeyFromObject(claim),
	})
	require.NoError(t, err)

	var remaining runsv1alpha1.EffectClaim
	err = backingClient.Get(context.Background(), client.ObjectKeyFromObject(claim), &remaining)
	require.True(t, apierrors.IsNotFound(err), "cross-namespace EffectClaim should be deleted instead of processed")
}
