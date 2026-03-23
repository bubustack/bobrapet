package runs

import (
	"context"
	"strings"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	identity "github.com/bubustack/core/runtime/identity"
)

func TestRBACManagerReconcileCopiesStorageAnnotations(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add StepRun scheme: %v", err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add Story scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add rbac scheme: %v", err)
	}

	annotations := map[string]string{
		"eks.amazonaws.com/role-arn": "arn:aws:iam::123:role/runner",
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-story",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.StorySpec{
			Policy: &bubuv1alpha1.StoryPolicy{
				Storage: &bubuv1alpha1.StoragePolicy{
					S3: &bubuv1alpha1.S3StorageProvider{
						Authentication: bubuv1alpha1.S3Authentication{
							ServiceAccountAnnotations: annotations,
						},
					},
				},
			},
		},
	}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-storyrun",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{
					Name: story.Name,
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(story, storyRun).
		Build()

	manager := &RBACManager{
		Client: client,
		Scheme: scheme,
	}

	if err := manager.Reconcile(context.Background(), storyRun); err != nil {
		t.Fatalf("rbac reconcile failed: %v", err)
	}

	saName := identity.StoryRunEngramRunnerServiceAccount(storyRun.Name)
	var sa corev1.ServiceAccount
	if err := client.Get(context.Background(), types.NamespacedName{Name: saName, Namespace: storyRun.Namespace}, &sa); err != nil {
		t.Fatalf("failed to fetch reconciled service account: %v", err)
	}

	for key, val := range annotations {
		if sa.Annotations[key] != val {
			t.Fatalf("expected annotation %s=%s, got %s", key, val, sa.Annotations[key])
		}
	}
	if _, ok := sa.Annotations[managedStorageAnnotationKey]; !ok {
		t.Fatalf("expected managed annotation marker to be set")
	}

	expectedKeys := sets.NewString()
	for key := range annotations {
		expectedKeys.Insert(key)
	}
	if got := sets.NewString(strings.Split(sa.Annotations[managedStorageAnnotationKey], ",")...); !got.Equal(expectedKeys) {
		t.Fatalf("expected managed keys %v, got %v", expectedKeys.List(), got.List())
	}
}
