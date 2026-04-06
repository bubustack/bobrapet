package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
)

func TestSyncImpulseTriggerStats(t *testing.T) {
	ctx := context.Background()
	fixtures := newImpulseStatsFixtures(t)

	requireNoError(t, fixtures.reconciler.syncImpulseTriggerStats(ctx, fixtures.impulse), "first sync")

	updated := fetchImpulse(t, fixtures.client, fixtures.impulse)
	assertImpulseCounters(t, &updated, 2, 1, 1)
	assertTimeEqual(t, updated.Status.LastTrigger, fixtures.failed.Status.StartedAt, "last trigger")
	assertTimeEqual(t, updated.Status.LastSuccess, fixtures.succeeded.Status.FinishedAt, "last success")

	requireNoError(t, fixtures.client.Delete(ctx, fixtures.succeeded), "delete succeeded run")

	refreshed := fetchImpulse(t, fixtures.client, fixtures.impulse)
	requireNoError(t, fixtures.reconciler.syncImpulseTriggerStats(ctx, &refreshed), "second sync")

	monotonic := fetchImpulse(t, fixtures.client, fixtures.impulse)
	assertImpulseCounters(t, &monotonic, 1, 0, 1)
	assertTimeEqual(t, monotonic.Status.LastSuccess, fixtures.succeeded.Status.FinishedAt, "last success persists after deletion")
}

type impulseStatsFixtures struct {
	client     client.Client
	reconciler *ImpulseReconciler
	impulse    *bubushv1alpha1.Impulse
	succeeded  *runsv1alpha1.StoryRun
	failed     *runsv1alpha1.StoryRun
}

func newImpulseStatsFixtures(t *testing.T) *impulseStatsFixtures {
	t.Helper()

	scheme := runtime.NewScheme()
	requireNoError(t, bubushv1alpha1.AddToScheme(scheme), "add impulse scheme")
	requireNoError(t, runsv1alpha1.AddToScheme(scheme), "add storyrun scheme")

	now := time.Now().UTC()
	impulse := &bubushv1alpha1.Impulse{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "voice-hook",
			Namespace: "livekit-demo",
		},
	}

	succeeded, failed, other := buildStoryRunFixtures(now, impulse)

	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&bubushv1alpha1.Impulse{}).
		WithRuntimeObjects(impulse, succeeded, failed, other).
		WithIndex(&runsv1alpha1.StoryRun{}, contracts.IndexStoryRunImpulseRef, func(obj client.Object) []string {
			run := obj.(*runsv1alpha1.StoryRun)
			if run.Spec.ImpulseRef == nil || run.Spec.ImpulseRef.Name == "" {
				return nil
			}
			ns := refs.ResolveNamespace(run, &run.Spec.ImpulseRef.ObjectReference)
			return []string{refs.NamespacedKey(ns, run.Spec.ImpulseRef.Name)}
		})

	cl := builder.Build()
	reconciler := &ImpulseReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: cl,
			Scheme: scheme,
		},
	}
	return &impulseStatsFixtures{
		client:     cl,
		reconciler: reconciler,
		impulse:    impulse,
		succeeded:  succeeded,
		failed:     failed,
	}
}

func buildStoryRunFixtures(now time.Time, impulse *bubushv1alpha1.Impulse) (*runsv1alpha1.StoryRun, *runsv1alpha1.StoryRun, *runsv1alpha1.StoryRun) {
	succeeded := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run-success",
			Namespace:         impulse.Namespace,
			CreationTimestamp: metav1.NewTime(now.Add(-2 * time.Minute)),
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
			ImpulseRef: &refs.ImpulseReference{
				ObjectReference: refs.ObjectReference{Name: impulse.Name},
			},
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase:      enums.PhaseSucceeded,
			StartedAt:  &metav1.Time{Time: now.Add(-90 * time.Second)},
			FinishedAt: &metav1.Time{Time: now.Add(-30 * time.Second)},
			TriggerTokens: []string{
				contracts.StoryRunTriggerTokenImpulse,
				contracts.StoryRunTriggerTokenImpulseSuccess,
			},
		},
	}

	failed := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run-failed",
			Namespace:         impulse.Namespace,
			CreationTimestamp: metav1.NewTime(now.Add(-1 * time.Minute)),
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
			ImpulseRef: &refs.ImpulseReference{
				ObjectReference: refs.ObjectReference{Name: impulse.Name},
			},
		},
		Status: runsv1alpha1.StoryRunStatus{
			Phase:     enums.PhaseFailed,
			StartedAt: &metav1.Time{Time: now.Add(-45 * time.Second)},
			TriggerTokens: []string{
				contracts.StoryRunTriggerTokenImpulse,
				contracts.StoryRunTriggerTokenImpulseFailed,
			},
		},
	}

	other := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "run-other",
			Namespace:         impulse.Namespace,
			CreationTimestamp: metav1.NewTime(now.Add(-30 * time.Second)),
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story"}},
			ImpulseRef: &refs.ImpulseReference{
				ObjectReference: refs.ObjectReference{Name: "different"},
			},
		},
	}
	return succeeded, failed, other
}

func fetchImpulse(t *testing.T, cl client.Client, impulse *bubushv1alpha1.Impulse) bubushv1alpha1.Impulse {
	t.Helper()
	var out bubushv1alpha1.Impulse
	key := types.NamespacedName{Name: impulse.Name, Namespace: impulse.Namespace}
	requireNoError(t, cl.Get(context.Background(), key, &out), "get impulse")
	return out
}

func assertImpulseCounters(t *testing.T, impulse *bubushv1alpha1.Impulse, triggers, successes, failures int64) {
	t.Helper()
	assertInt64Equal(t, impulse.Status.TriggersReceived, triggers, "triggers received")
	assertInt64Equal(t, impulse.Status.StoriesLaunched, successes, "stories launched")
	assertInt64Equal(t, impulse.Status.FailedTriggers, failures, "failed triggers")
}

func assertInt64Equal(t *testing.T, got, want int64, field string) {
	t.Helper()
	if got != want {
		t.Fatalf("expected %s=%d, got %d", field, want, got)
	}
}

func assertTimeEqual(t *testing.T, got, want *metav1.Time, field string) {
	t.Helper()
	if want == nil {
		return
	}
	if got == nil {
		t.Fatalf("expected %s to equal %s, got %v", field, want, got)
	}
	if got.Equal(want) {
		return
	}
	if got.Truncate(time.Second).Equal(want.Truncate(time.Second)) {
		return
	}
	t.Fatalf("expected %s to equal %s, got %v", field, want, got)
}

func requireNoError(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}
