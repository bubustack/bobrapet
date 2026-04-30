package config

import (
	"context"
	"strings"
	"testing"

	"github.com/bubustack/core/contracts"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestNewOperatorConfigManagerReturnsConstructorError(t *testing.T) {
	t.Parallel()

	manager, err := NewOperatorConfigManager(nil, "default", "cfg")
	if err == nil {
		t.Fatal("expected constructor error")
	}
	if manager != nil {
		t.Fatalf("expected nil manager on constructor error, got %#v", manager)
	}
	if !strings.Contains(err.Error(), "Client must be provided") {
		t.Fatalf("expected wrapped client validation error, got %v", err)
	}
}

func TestMustNewOperatorConfigManagerPanicsOnConstructorError(t *testing.T) {
	t.Parallel()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic from must constructor")
		}
		if !strings.Contains(recovered.(error).Error(), "Client must be provided") {
			t.Fatalf("expected wrapped client validation error, got %v", recovered)
		}
	}()

	_ = MustNewOperatorConfigManager(nil, "default", "cfg")
}

func TestOperatorConfigManagerLoadInitialUsesAPIReaderAdapter(t *testing.T) {
	t.Parallel()

	scheme := testCoreV1Scheme(t)
	cachedClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	apiReader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(testOperatorConfigMap("cfg", map[string]string{
			contracts.KeyServiceAccountName: "api-reader-sa",
		})).
		Build()

	manager, err := NewOperatorConfigManager(cachedClient, "default", "cfg", apiReader)
	if err != nil {
		t.Fatalf("NewOperatorConfigManager returned error: %v", err)
	}
	if err := manager.LoadInitial(context.Background()); err != nil {
		t.Fatalf("LoadInitial returned error: %v", err)
	}

	if got := manager.GetControllerConfig().ServiceAccountName; got != "api-reader-sa" {
		t.Fatalf("expected APIReader config to be loaded, got service account %q", got)
	}
}

func TestOperatorConfigManagerReconcileAdaptsControllerRuntimeRequest(t *testing.T) {
	t.Parallel()

	scheme := testCoreV1Scheme(t)
	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(testOperatorConfigMap("cfg", map[string]string{
			contracts.KeyServiceAccountName: "reconciled-sa",
		})).
		Build()

	manager, err := NewOperatorConfigManager(client, "default", "cfg")
	if err != nil {
		t.Fatalf("NewOperatorConfigManager returned error: %v", err)
	}
	_, err = manager.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "cfg"},
	})
	if err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}

	if got := manager.GetControllerConfig().ServiceAccountName; got != "reconciled-sa" {
		t.Fatalf("expected reconciled config to be applied, got service account %q", got)
	}
}

func TestOperatorConfigManagerPredicateMatchesConfiguredConfigMap(t *testing.T) {
	t.Parallel()

	manager, err := NewOperatorConfigManager(fake.NewClientBuilder().WithScheme(testCoreV1Scheme(t)).Build(), "default", "cfg")
	if err != nil {
		t.Fatalf("NewOperatorConfigManager returned error: %v", err)
	}
	predicate := manager.configMapPredicate()
	watched := testOperatorConfigMap("cfg", nil)
	other := testOperatorConfigMap("other", nil)

	if !predicate.Create(event.CreateEvent{Object: watched}) {
		t.Fatal("expected create event for configured ConfigMap to match")
	}
	if !predicate.Update(event.UpdateEvent{ObjectOld: other, ObjectNew: watched}) {
		t.Fatal("expected update event with configured ConfigMap as new object to match")
	}
	if !predicate.Delete(event.DeleteEvent{Object: watched}) {
		t.Fatal("expected delete event for configured ConfigMap to match")
	}
	if predicate.Create(event.CreateEvent{Object: other}) {
		t.Fatal("expected create event for other ConfigMap to be ignored")
	}
	if predicate.Generic(event.GenericEvent{Object: watched}) {
		t.Fatal("expected generic event to be ignored")
	}
}

func testCoreV1Scheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 scheme: %v", err)
	}
	return scheme
}

func testOperatorConfigMap(name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
		},
		Data: data,
	}
}
