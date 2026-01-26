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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/refs"
	transportbinding "github.com/bubustack/bobrapet/pkg/transport/binding"
	"k8s.io/utils/ptr"
)

func TestRunScopedRealtimeStepCreatesResources(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	story := &v1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "story",
			Namespace: "default",
		},
		Spec: v1alpha1.StorySpec{
			Pattern: enums.StreamingPattern,
			Transports: []v1alpha1.StoryTransport{{
				Name:         "primary",
				TransportRef: "demo-transport",
			}},
			Steps: []v1alpha1.Step{{
				Name:      "streaming",
				Transport: "primary",
				Ref: &refs.EngramReference{
					ObjectReference: refs.ObjectReference{Name: "base-engram"},
				},
			}},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sr",
			Namespace: "default",
			UID:       types.UID("run-uid"),
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: story.Name},
			},
		},
	}
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kubeutil.ComposeName(storyRun.Name, "streaming"),
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: storyRun.Name},
			},
			StepID: "streaming",
			EngramRef: &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: "base-engram"},
			},
		},
	}
	engram := &v1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-engram",
			Namespace: "default",
		},
		Spec: v1alpha1.EngramSpec{
			Mode: enums.WorkloadModeDeployment,
			Transport: &v1alpha1.EngramTransportSpec{
				TLS: &v1alpha1.EngramTLSSpec{
					UseDefaultTLS: ptr.To(false),
				},
			},
			TemplateRef: refs.EngramTemplateReference{
				Name: "template",
			},
		},
	}
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "template",
		},
	}
	transport := &transportv1alpha1.Transport{
		ObjectMeta: metav1.ObjectMeta{
			Name: "demo-transport",
		},
		Spec: transportv1alpha1.TransportSpec{
			Driver: "demo",
			SupportedAudio: []transportv1alpha1.AudioCodec{{
				Name: "pcm16",
			}},
			SupportedBinary: []string{"application/json"},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(stepRun).
		WithObjects(
			story,
			storyRun,
			stepRun.DeepCopy(),
			engram,
			template,
			transport,
		).
		Build()

	manager := config.NewOperatorConfigManager(client, "default", "bobrapet-operator-config")
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         client,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(client, manager),
		},
	}

	res, err := reconciler.reconcileRunScopedRealtimeStep(context.Background(), stepRun.DeepCopy(), story, &story.Spec.Steps[0], engram, template)
	require.NoError(t, err)
	require.Greater(t, res.RequeueAfter, time.Duration(0))

	var updated runsv1alpha1.StepRun
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, &updated))
	require.Equal(t, enums.PhasePaused, updated.Status.Phase)

	service := &corev1.Service{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, service))
	require.True(t, metav1.IsControlledBy(service, stepRun))

	deployment := &appsv1.Deployment{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, deployment))
	require.True(t, metav1.IsControlledBy(deployment, stepRun))

	binding := &transportv1alpha1.TransportBinding{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: transportbinding.Name(storyRun.Name, "streaming"), Namespace: stepRun.Namespace}, binding))
	require.True(t, metav1.IsControlledBy(binding, stepRun))
	require.Equal(t, stepRun.Spec.StepID, binding.Spec.StepName)
	require.Equal(t, stepRun.Name, binding.Spec.EngramName)

	transportCondition := conditions.GetCondition(updated.Status.Conditions, conditions.ConditionTransportReady)
	require.NotNil(t, transportCondition)
	require.Equal(t, metav1.ConditionFalse, transportCondition.Status)
}

func TestRunScopedRealtimeStepTransitionsToRunning(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	story := buildStreamingStory()
	storyRun := buildStoryRun()
	stepRun := buildStreamingStepRun(storyRun.Name)
	engram := buildBaseEngram()
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{
			Name: "template",
		},
	}
	transport := buildStreamingTransport()

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(stepRun, &transportv1alpha1.TransportBinding{}, &appsv1.Deployment{}).
		WithObjects(
			story,
			storyRun,
			stepRun.DeepCopy(),
			engram,
			template,
			transport,
		).
		Build()

	manager := config.NewOperatorConfigManager(client, "default", "config")
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         client,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(client, manager),
		},
	}

	_, err := reconciler.reconcileRunScopedRealtimeStep(context.Background(), stepRun.DeepCopy(), story, &story.Spec.Steps[0], engram, template)
	require.NoError(t, err)

	// Mark binding and deployment as ready.
	binding := &transportv1alpha1.TransportBinding{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: transportbinding.Name(storyRun.Name, "streaming"), Namespace: stepRun.Namespace}, binding))
	binding.Status.Conditions = []metav1.Condition{{
		Type:   conditions.ConditionReady,
		Status: metav1.ConditionTrue,
	}}
	require.NoError(t, client.Status().Update(context.Background(), binding))

	deployment := &appsv1.Deployment{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, deployment))
	deployment.Status.ReadyReplicas = 1
	require.NoError(t, client.Status().Update(context.Background(), deployment))

	// Reconcile again.
	loadedStepRun := &runsv1alpha1.StepRun{}
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, loadedStepRun))

	res, err := reconciler.reconcileRunScopedRealtimeStep(context.Background(), loadedStepRun, story, &story.Spec.Steps[0], engram, template)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), res.RequeueAfter)

	var updated runsv1alpha1.StepRun
	require.NoError(t, client.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, &updated))
	require.Equal(t, enums.PhaseRunning, updated.Status.Phase)
	transportCondition := conditions.GetCondition(updated.Status.Conditions, conditions.ConditionTransportReady)
	require.NotNil(t, transportCondition)
	require.Equal(t, metav1.ConditionTrue, transportCondition.Status)
}

func buildStreamingStepRun(storyRunName string) *runsv1alpha1.StepRun {
	return &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kubeutil.ComposeName(storyRunName, "streaming"),
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: storyRunName},
			},
			StepID: "streaming",
			EngramRef: &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: "base-engram"},
			},
		},
	}
}

func buildStreamingStory() *v1alpha1.Story {
	return &v1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "story",
			Namespace: "default",
		},
		Spec: v1alpha1.StorySpec{
			Pattern: enums.StreamingPattern,
			Transports: []v1alpha1.StoryTransport{{
				Name:         "primary",
				TransportRef: "demo-transport",
			}},
			Steps: []v1alpha1.Step{{
				Name:      "streaming",
				Transport: "primary",
				Ref: &refs.EngramReference{
					ObjectReference: refs.ObjectReference{Name: "base-engram"},
				},
			}},
		},
	}
}

func buildStoryRun() *runsv1alpha1.StoryRun {
	return &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "storyrun",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: "story"},
			},
		},
	}
}

func buildBaseEngram() *v1alpha1.Engram {
	return &v1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "base-engram",
			Namespace: "default",
		},
		Spec: v1alpha1.EngramSpec{
			Mode: enums.WorkloadModeDeployment,
			Transport: &v1alpha1.EngramTransportSpec{
				TLS: &v1alpha1.EngramTLSSpec{UseDefaultTLS: ptr.To(false)},
			},
			TemplateRef: refs.EngramTemplateReference{Name: "template"},
		},
	}
}

func buildStreamingTransport() *transportv1alpha1.Transport {
	return &transportv1alpha1.Transport{
		ObjectMeta: metav1.ObjectMeta{
			Name: "demo-transport",
		},
		Spec: transportv1alpha1.TransportSpec{
			Driver: "demo",
			SupportedAudio: []transportv1alpha1.AudioCodec{{
				Name: "pcm16",
			}},
			SupportedBinary: []string{"application/json"},
		},
	}
}
