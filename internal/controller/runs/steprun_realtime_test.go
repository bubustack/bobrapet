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
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	"github.com/bubustack/core/contracts"
	"github.com/bubustack/tractatus/envelope"
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
			Pattern: enums.RealtimePattern,
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
					SecretRef: &corev1.LocalObjectReference{Name: "engram-tls"},
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

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(stepRun).
		WithObjects(
			story,
			storyRun,
			stepRun.DeepCopy(),
			engram,
			template,
			transport,
			buildRealtimeTLSSecret(),
		).
		Build()

	manager, _ := config.NewOperatorConfigManager(cl, "default", "bobrapet-operator-config")
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	res, err := reconciler.reconcileRunScopedRealtimeStep(context.Background(), stepRun.DeepCopy(), story, &story.Spec.Steps[0], engram, template)
	require.NoError(t, err)
	require.Greater(t, res.RequeueAfter, time.Duration(0))

	var updated runsv1alpha1.StepRun
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, &updated))
	require.Equal(t, enums.PhasePaused, updated.Status.Phase)

	service := &corev1.Service{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, service))
	require.True(t, metav1.IsControlledBy(service, stepRun))

	deployment := &appsv1.Deployment{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, deployment))
	require.True(t, metav1.IsControlledBy(deployment, stepRun))

	binding := &transportv1alpha1.TransportBinding{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: transportbinding.Name(storyRun.Name, "streaming"), Namespace: stepRun.Namespace}, binding))
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

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(stepRun, &transportv1alpha1.TransportBinding{}, &appsv1.Deployment{}).
		WithObjects(
			story,
			storyRun,
			stepRun.DeepCopy(),
			engram,
			template,
			transport,
			buildRealtimeTLSSecret(),
		).
		Build()

	manager, _ := config.NewOperatorConfigManager(cl, "default", "config")
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	_, err := reconciler.reconcileRunScopedRealtimeStep(context.Background(), stepRun.DeepCopy(), story, &story.Spec.Steps[0], engram, template)
	require.NoError(t, err)

	// Mark binding and deployment as ready.
	binding := &transportv1alpha1.TransportBinding{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: transportbinding.Name(storyRun.Name, "streaming"), Namespace: stepRun.Namespace}, binding))
	binding.Status.Conditions = []metav1.Condition{{
		Type:   conditions.ConditionReady,
		Status: metav1.ConditionTrue,
	}}
	require.NoError(t, cl.Status().Update(context.Background(), binding))

	deployment := &appsv1.Deployment{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, deployment))
	deployment.Status.ReadyReplicas = 1
	require.NoError(t, cl.Status().Update(context.Background(), deployment))

	// Reconcile again.
	loadedStepRun := &runsv1alpha1.StepRun{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, loadedStepRun))

	res, err := reconciler.reconcileRunScopedRealtimeStep(context.Background(), loadedStepRun, story, &story.Spec.Steps[0], engram, template)
	require.NoError(t, err)
	require.Equal(t, time.Duration(0), res.RequeueAfter)

	var updated runsv1alpha1.StepRun
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, &updated))
	require.Equal(t, enums.PhaseRunning, updated.Status.Phase)
	transportCondition := conditions.GetCondition(updated.Status.Conditions, conditions.ConditionTransportReady)
	require.NotNil(t, transportCondition)
	require.Equal(t, metav1.ConditionTrue, transportCondition.Status)
}

func TestRunScopedRealtimeStepDeploymentIncludesTransportKinds(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	story := buildStreamingStory()
	story.Status.Transports = []v1alpha1.StoryTransportStatus{{
		Name:         "primary",
		TransportRef: "demo-transport",
		Mode:         enums.TransportModeHot,
		ModeReason:   "streaming-default",
	}}
	storyRun := buildStoryRun()
	stepRun := buildStreamingStepRun(storyRun.Name)
	engram := buildBaseEngram()
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template"},
	}
	transport := buildStreamingTransport()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(stepRun).
		WithObjects(
			story,
			storyRun,
			stepRun.DeepCopy(),
			engram,
			template,
			transport,
			buildRealtimeTLSSecret(),
		).
		Build()

	manager, err := config.NewOperatorConfigManager(cl, "default", "config")
	require.NoError(t, err)
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			APIReader:      cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	_, err = reconciler.reconcileRunScopedRealtimeStep(context.Background(), stepRun.DeepCopy(), story, &story.Spec.Steps[0], engram, template)
	require.NoError(t, err)

	deployment := &appsv1.Deployment{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, deployment))

	var payload string
	for _, env := range deployment.Spec.Template.Spec.Containers[0].Env {
		if env.Name == contracts.TransportsEnv {
			payload = env.Value
			break
		}
	}

	require.NotEmpty(t, payload)

	var descriptors []envelope.TransportDescriptor
	require.NoError(t, json.Unmarshal([]byte(payload), &descriptors))
	require.Len(t, descriptors, 1)
	require.Equal(t, "primary", descriptors[0].Name)
	require.Equal(t, "demo-provider", descriptors[0].Kind)
	require.Equal(t, string(enums.TransportModeHot), descriptors[0].Mode)
}

func TestRunScopedRealtimeStepDeploymentAutomountsServiceAccountToken(t *testing.T) {
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
		ObjectMeta: metav1.ObjectMeta{Name: "template"},
	}
	transport := buildStreamingTransport()

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(stepRun).
		WithObjects(
			story,
			storyRun,
			stepRun.DeepCopy(),
			engram,
			template,
			transport,
			buildRealtimeTLSSecret(),
		).
		Build()

	manager, err := config.NewOperatorConfigManager(cl, "default", "config")
	require.NoError(t, err)
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         cl,
			APIReader:      cl,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(cl, manager),
		},
	}

	_, err = reconciler.reconcileRunScopedRealtimeStep(context.Background(), stepRun.DeepCopy(), story, &story.Spec.Steps[0], engram, template)
	require.NoError(t, err)

	deployment := &appsv1.Deployment{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, deployment))
	require.NotNil(t, deployment.Spec.Template.Spec.AutomountServiceAccountToken)
	require.True(t, *deployment.Spec.Template.Spec.AutomountServiceAccountToken)
	require.NotEmpty(t, deployment.Spec.Template.Spec.ServiceAccountName)
}

func TestRunScopedRealtimeStepStablePausedReconcileDoesNotPatchDeployment(t *testing.T) {
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
	stepRun.CreationTimestamp = metav1.NewTime(time.Date(2026, time.March, 29, 18, 0, 0, 0, time.UTC))
	engram := buildBaseEngram()
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template"},
	}
	transport := buildStreamingTransport()

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(stepRun, &transportv1alpha1.TransportBinding{}).
		WithObjects(
			story,
			storyRun,
			stepRun.DeepCopy(),
			engram,
			template,
			transport,
			buildRealtimeTLSSecret(),
		).
		Build()
	countingClient := &deploymentPatchCountingClient{Client: baseClient}

	manager, err := config.NewOperatorConfigManager(countingClient, "default", "config")
	require.NoError(t, err)
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         countingClient,
			APIReader:      countingClient,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(countingClient, manager),
		},
	}

	ctx := context.Background()
	_, err = reconciler.reconcileRunScopedRealtimeStep(ctx, stepRun.DeepCopy(), story, &story.Spec.Steps[0], engram, template)
	require.NoError(t, err)
	require.Equal(t, 0, countingClient.DeploymentPatchCount())

	for range 3 {
		loaded := &runsv1alpha1.StepRun{}
		require.NoError(t, countingClient.Get(ctx, types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, loaded))
		res, err := reconciler.reconcileRunScopedRealtimeStep(ctx, loaded, story, &story.Spec.Steps[0], engram, template)
		require.NoError(t, err)
		require.Greater(t, res.RequeueAfter, time.Duration(0))
	}

	deployment := &appsv1.Deployment{}
	require.NoError(t, countingClient.Get(ctx, types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, deployment))
	require.Equal(t, 0, countingClient.DeploymentPatchCount())
	require.Equal(t, stepRun.CreationTimestamp.UTC().Format(time.RFC3339Nano), envValueFor(deployment.Spec.Template.Spec.Containers[0].Env, contracts.StartedAtEnv))
}

func TestRunScopedRealtimeStepRepeatedReconcileIgnoresAPIDefaults(t *testing.T) {
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
	stepRun.CreationTimestamp = metav1.NewTime(time.Date(2026, time.March, 29, 18, 0, 0, 0, time.UTC))
	engram := buildBaseEngram()
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template"},
	}
	transport := buildStreamingTransport()

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(stepRun, &transportv1alpha1.TransportBinding{}).
		WithObjects(
			story,
			storyRun,
			stepRun.DeepCopy(),
			engram,
			template,
			transport,
			buildRealtimeTLSSecret(),
		).
		Build()
	countingClient := &deploymentPatchCountingClient{Client: baseClient}

	manager, err := config.NewOperatorConfigManager(countingClient, "default", "config")
	require.NoError(t, err)
	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:         countingClient,
			APIReader:      countingClient,
			Scheme:         scheme,
			ConfigResolver: config.NewResolver(countingClient, manager),
		},
	}

	ctx := context.Background()
	_, err = reconciler.reconcileRunScopedRealtimeStep(ctx, stepRun.DeepCopy(), story, &story.Spec.Steps[0], engram, template)
	require.NoError(t, err)

	deployment := &appsv1.Deployment{}
	deploymentKey := types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}
	require.NoError(t, countingClient.Get(ctx, deploymentKey, deployment))
	simulateAPIServerRealtimeDeploymentDefaults(deployment)
	require.NoError(t, countingClient.Update(ctx, deployment))

	service := &corev1.Service{}
	serviceKey := types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}
	require.NoError(t, countingClient.Get(ctx, serviceKey, service))
	simulateAPIServerRealtimeServiceDefaults(service)
	require.NoError(t, countingClient.Update(ctx, service))

	loaded := &runsv1alpha1.StepRun{}
	require.NoError(t, countingClient.Get(ctx, deploymentKey, loaded))
	initialConnectorGeneration := loaded.Annotations[connectorGenerationAnnotation]
	countingClient.ResetPatchCounts()

	for range 3 {
		current := &runsv1alpha1.StepRun{}
		require.NoError(t, countingClient.Get(ctx, deploymentKey, current))
		res, err := reconciler.reconcileRunScopedRealtimeStep(ctx, current, story, &story.Spec.Steps[0], engram, template)
		require.NoError(t, err)
		require.Greater(t, res.RequeueAfter, time.Duration(0))
	}

	require.Equal(t, 0, countingClient.DeploymentPatchCount())
	require.Equal(t, 0, countingClient.ServicePatchCount())
	require.Equal(t, 0, countingClient.StepRunPatchCount())

	reloaded := &runsv1alpha1.StepRun{}
	require.NoError(t, countingClient.Get(ctx, deploymentKey, reloaded))
	require.Equal(t, initialConnectorGeneration, reloaded.Annotations[connectorGenerationAnnotation])
}

type deploymentPatchCountingClient struct {
	client.Client
	mu                   sync.Mutex
	deploymentPatchCount int
	servicePatchCount    int
	stepRunPatchCount    int
}

func (c *deploymentPatchCountingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c.mu.Lock()
	switch obj.(type) {
	case *appsv1.Deployment:
		c.deploymentPatchCount++
	case *corev1.Service:
		c.servicePatchCount++
	case *runsv1alpha1.StepRun:
		c.stepRunPatchCount++
	}
	c.mu.Unlock()
	return c.Client.Patch(ctx, obj, patch, opts...)
}

func (c *deploymentPatchCountingClient) DeploymentPatchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deploymentPatchCount
}

func (c *deploymentPatchCountingClient) ServicePatchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servicePatchCount
}

func (c *deploymentPatchCountingClient) StepRunPatchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stepRunPatchCount
}

func (c *deploymentPatchCountingClient) ResetPatchCounts() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deploymentPatchCount = 0
	c.servicePatchCount = 0
	c.stepRunPatchCount = 0
}

func envValueFor(envs []corev1.EnvVar, name string) string {
	for _, env := range envs {
		if env.Name == name {
			return env.Value
		}
	}
	return ""
}

func simulateAPIServerRealtimeDeploymentDefaults(deployment *appsv1.Deployment) {
	if deployment == nil {
		return
	}
	for i := range deployment.Spec.Template.Spec.Containers {
		container := &deployment.Spec.Template.Spec.Containers[i]
		for j := range container.Ports {
			if container.Ports[j].Protocol == "" {
				container.Ports[j].Protocol = corev1.ProtocolTCP
			}
		}
		if container.TerminationMessagePath == "" {
			container.TerminationMessagePath = corev1.TerminationMessagePathDefault
		}
		if container.TerminationMessagePolicy == "" {
			container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
		}
	}
	for i := range deployment.Spec.Template.Spec.Volumes {
		if secret := deployment.Spec.Template.Spec.Volumes[i].Secret; secret != nil && secret.DefaultMode == nil {
			mode := corev1.SecretVolumeSourceDefaultMode
			secret.DefaultMode = &mode
		}
	}
}

func simulateAPIServerRealtimeServiceDefaults(service *corev1.Service) {
	if service == nil {
		return
	}
	if service.Spec.Type == "" {
		service.Spec.Type = corev1.ServiceTypeClusterIP
	}
	if service.Spec.SessionAffinity == "" {
		service.Spec.SessionAffinity = corev1.ServiceAffinityNone
	}
	if service.Spec.InternalTrafficPolicy == nil {
		policy := corev1.ServiceInternalTrafficPolicyCluster
		service.Spec.InternalTrafficPolicy = &policy
	}
	for i := range service.Spec.Ports {
		if service.Spec.Ports[i].Protocol == "" {
			service.Spec.Ports[i].Protocol = corev1.ProtocolTCP
		}
	}
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
			Pattern: enums.RealtimePattern,
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
				TLS: &v1alpha1.EngramTLSSpec{SecretRef: &corev1.LocalObjectReference{Name: "engram-tls"}},
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
			Provider: "demo-provider",
			Driver:   "demo",
			SupportedAudio: []transportv1alpha1.AudioCodec{{
				Name: "pcm16",
			}},
			SupportedBinary: []string{"application/json"},
		},
	}
}

func buildRealtimeTLSSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "engram-tls",
			Namespace: "default",
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			"tls.crt": []byte("crt"),
			"tls.key": []byte("key"),
			"ca.crt":  []byte("ca"),
		},
	}
}
