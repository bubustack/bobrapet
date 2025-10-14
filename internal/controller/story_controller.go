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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/patch"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/refs"
	"k8s.io/apimachinery/pkg/api/errors"
)

const (
	// StoryFinalizer is the name of the finalizer used by the Story controller.
	StoryFinalizer = "story.bubustack.io/finalizer"
)

type executeStoryWith struct {
	StoryRef refs.StoryReference `json:"storyRef"`
}

// StoryReconciler reconciles a Story object
type StoryReconciler struct {
	config.ControllerDependencies
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=bubustack.io,resources=stories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bubustack.io,resources=stories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bubustack.io,resources=stories/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *StoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	log := log.FromContext(ctx)
	startTime := time.Now()
	defer func() {
		metrics.RecordControllerReconcile("story", time.Since(startTime), err)
	}()

	// Bound reconcile duration
	timeout := r.ControllerDependencies.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var story bubuv1alpha1.Story
	if err := r.ControllerDependencies.Client.Get(ctx, req.NamespacedName, &story); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion first.
	if !story.ObjectMeta.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &story)
	}

	// Add finalizer if it doesn't exist.
	if !controllerutil.ContainsFinalizer(&story, StoryFinalizer) {
		patch := client.MergeFrom(story.DeepCopy())
		controllerutil.AddFinalizer(&story, StoryFinalizer)
		if err := r.Patch(ctx, &story, patch); err != nil {
			log.Error(err, "Failed to add finalizer to Story")
			return ctrl.Result{}, err
		}
	}

	// Validate that all referenced engrams exist
	validationErr := r.validateEngramReferences(ctx, &story)
	if validationErr != nil {
		// Emit event for user visibility (guard recorder for test environments)
		if r.Recorder != nil {
			r.Recorder.Event(&story, "Warning", conditions.ReasonReferenceNotFound, validationErr.Error())
		}
	}

	// Update the status based on validation.
	err = patch.RetryableStatusPatch(ctx, r.ControllerDependencies.Client, &story, func(obj client.Object) {
		s := obj.(*bubuv1alpha1.Story)
		s.Status.StepsTotal = int32(len(s.Spec.Steps))
		s.Status.ObservedGeneration = s.Generation

		cm := conditions.NewConditionManager(s.Generation)
		if validationErr != nil {
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonValidationFailed, validationErr.Error())
		} else {
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonValidationPassed, "All Engram references are valid.")
		}
	})

	if err != nil {
		return ctrl.Result{}, err
	}

	// If validation failed, do not time-requeue; rely on engram/watch events to trigger reconcile.
	if validationErr != nil {
		return ctrl.Result{}, nil
	}

	// Handle streaming pattern infrastructure for PerStory strategy
	if story.Spec.Pattern == enums.StreamingPattern && story.Spec.StreamingStrategy == enums.StreamingStrategyPerStory {
		return r.reconcilePerStoryStreaming(ctx, &story)
	}

	return ctrl.Result{}, nil
}

func (r *StoryReconciler) reconcileDelete(ctx context.Context, story *bubuv1alpha1.Story) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling deletion for Story")

	if controllerutil.ContainsFinalizer(story, StoryFinalizer) {
		// Clean up external resources associated with the Story, such as Deployments and Services.
		if err := r.cleanupOwnedResources(ctx, story); err != nil {
			return ctrl.Result{}, err
		}

		// Remove the finalizer from the list and update it.
		patch := client.MergeFrom(story.DeepCopy())
		controllerutil.RemoveFinalizer(story, StoryFinalizer)
		if err := r.Patch(ctx, story, patch); err != nil {
			log.Error(err, "Failed to remove finalizer from Story")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *StoryReconciler) cleanupOwnedResources(ctx context.Context, story *bubuv1alpha1.Story) error {
	log := log.FromContext(ctx)

	// This cleanup logic is only for streaming stories with a PerStory strategy.
	if story.Spec.Pattern != enums.StreamingPattern || story.Spec.StreamingStrategy != enums.StreamingStrategyPerStory {
		return nil
	}

	// Delete Deployments
	for _, step := range story.Spec.Steps {
		if step.Ref != nil {
			deploymentName := fmt.Sprintf("%s-%s", story.Name, step.Name)
			deployment := &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      deploymentName,
					Namespace: story.Namespace,
				},
			}
			if err := r.Delete(ctx, deployment, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil && !errors.IsNotFound(err) {
				log.Error(err, "Failed to delete Deployment for streaming step", "deployment", deploymentName)
				return err
			}

			// Delete Services
			serviceName := fmt.Sprintf("%s-%s", story.Name, step.Name)
			service := &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      serviceName,
					Namespace: story.Namespace,
				},
			}
			if err := r.Delete(ctx, service); err != nil && !errors.IsNotFound(err) {
				log.Error(err, "Failed to delete Service for streaming step", "service", serviceName)
				return err
			}
		}
	}

	log.Info("Successfully cleaned up owned resources")
	return nil
}

func (r *StoryReconciler) reconcilePerStoryStreaming(ctx context.Context, story *bubuv1alpha1.Story) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Info("Reconciling workloads for streaming Story with PerStory strategy")

	for _, step := range story.Spec.Steps {
		if step.Ref == nil {
			continue // This step is not an engram, so nothing to deploy.
		}

		// Fetch the Engram to get its spec
		var engram bubuv1alpha1.Engram
		engramKey := step.Ref.ToNamespacedName(story)
		if err := r.ControllerDependencies.Client.Get(ctx, engramKey, &engram); err != nil {
			log.Error(err, "Failed to get Engram for streaming step", "engram", engramKey.Name)
			return ctrl.Result{}, err // Requeue and try again
		}

		// Resolve template and execution config to build a real deployment
		// Fetch EngramTemplate
		template := &catalogv1alpha1.EngramTemplate{}
		if err := r.ControllerDependencies.Client.Get(ctx, types.NamespacedName{Name: engram.Spec.TemplateRef.Name, Namespace: ""}, template); err != nil {
			log.Error(err, "Failed to get EngramTemplate for streaming step", "engramTemplate", engram.Spec.TemplateRef.Name)
			return ctrl.Result{}, err
		}
		resolved, err := r.ControllerDependencies.ConfigResolver.ResolveExecutionConfig(ctx, nil, story, &engram, template)
		if err != nil {
			log.Error(err, "Failed to resolve execution config for streaming step")
			return ctrl.Result{}, err
		}
		deployment := r.deploymentForStreamingStepWithConfig(story, &step, &engram, resolved)
		if err := r.reconcileOwnedDeployment(ctx, story, deployment); err != nil {
			return ctrl.Result{}, err
		}

		service := r.serviceForStreamingStepWithConfig(story, &step, &engram, resolved)
		if err := r.reconcileOwnedService(ctx, story, service); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *StoryReconciler) reconcileOwnedDeployment(ctx context.Context, owner *bubuv1alpha1.Story, desired *appsv1.Deployment) error {
	log := log.FromContext(ctx)
	existing := &appsv1.Deployment{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if err != nil && errors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(owner, desired, r.Scheme); err != nil {
			return err
		}
		log.Info("Creating Deployment for streaming Story step", "deployment", desired.Name)
		return r.Create(ctx, desired)
	} else if err != nil {
		return err
	}
	// Update if the pod template or replica count differs
	// Preserve cluster-managed fields via fresh GET + merge
	if !reflect.DeepEqual(existing.Spec.Template, desired.Spec.Template) ||
		(existing.Spec.Replicas != nil && desired.Spec.Replicas != nil && *existing.Spec.Replicas != *desired.Spec.Replicas) {
		patch := client.MergeFrom(existing.DeepCopy())
		existing.Spec.Template = desired.Spec.Template
		existing.Spec.Replicas = desired.Spec.Replicas
		log.Info("Patching Deployment for streaming Story step", "deployment", desired.Name)
		return r.Patch(ctx, existing, patch)
	}
	return nil
}

func (r *StoryReconciler) reconcileOwnedService(ctx context.Context, owner *bubuv1alpha1.Story, desired *corev1.Service) error {
	log := log.FromContext(ctx)
	existing := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}, existing)
	if err != nil && errors.IsNotFound(err) {
		if err := controllerutil.SetControllerReference(owner, desired, r.Scheme); err != nil {
			return err
		}
		log.Info("Creating Service for streaming Story step", "service", desired.Name)
		return r.Create(ctx, desired)
	} else if err != nil {
		return err
	}

	// Only patch if there are changes in the fields we manage.
	// We preserve the ClusterIP by modifying the existing object.
	original := existing.DeepCopy()
	needsPatch := false

	if !reflect.DeepEqual(existing.ObjectMeta.Labels, desired.ObjectMeta.Labels) {
		existing.ObjectMeta.Labels = desired.ObjectMeta.Labels
		needsPatch = true
	}

	if !reflect.DeepEqual(existing.ObjectMeta.Annotations, desired.ObjectMeta.Annotations) {
		existing.ObjectMeta.Annotations = desired.ObjectMeta.Annotations
		needsPatch = true
	}

	if !reflect.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) {
		existing.Spec.Ports = desired.Spec.Ports
		needsPatch = true
	}

	if !reflect.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) {
		existing.Spec.Selector = desired.Spec.Selector
		needsPatch = true
	}

	if needsPatch {
		log.Info("Patching Service for streaming Story step", "service", desired.Name)
		return r.Patch(ctx, existing, client.MergeFrom(original))
	}

	return nil
}

func (r *StoryReconciler) deploymentForStreamingStepWithConfig(story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, engram *bubuv1alpha1.Engram, cfg *config.ResolvedExecutionConfig) *appsv1.Deployment {
	name := fmt.Sprintf("%s-%s", story.Name, step.Name)
	labels := map[string]string{
		"app.kubernetes.io/name":       "bobrapet-streaming-engram",
		"app.kubernetes.io/managed-by": "story-controller",
		"bubustack.io/story":           story.Name,
		"bubustack.io/step":            step.Name,
	}
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: story.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: cfg.ServiceAccountName,
					Containers: []corev1.Container{{
						Name:            "engram",
						Image:           cfg.Image,
						ImagePullPolicy: cfg.ImagePullPolicy,
						LivenessProbe:   cfg.LivenessProbe,
						ReadinessProbe:  cfg.ReadinessProbe,
						StartupProbe:    cfg.StartupProbe,
						Ports: []corev1.ContainerPort{{
							Name:          "grpc",
							ContainerPort: int32(r.ControllerDependencies.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort),
						}},
						Env: []corev1.EnvVar{{Name: "BUBU_EXECUTION_MODE", Value: "streaming"}},
					}},
				},
			},
		},
	}
}

func (r *StoryReconciler) serviceForStreamingStepWithConfig(story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, engram *bubuv1alpha1.Engram, cfg *config.ResolvedExecutionConfig) *corev1.Service {
	name := fmt.Sprintf("%s-%s", story.Name, step.Name)
	labels := map[string]string{
		"bubustack.io/story": story.Name,
		"bubustack.io/step":  step.Name,
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: story.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: labels,
			Ports: []corev1.ServicePort{{
				Protocol:   corev1.ProtocolTCP,
				Port:       int32(r.ControllerDependencies.ConfigResolver.GetOperatorConfig().Controller.Engram.EngramControllerConfig.DefaultGRPCPort),
				TargetPort: intstr.FromString("grpc"),
			}},
		},
	}
}

func (r *StoryReconciler) validateEngramReferences(ctx context.Context, story *bubuv1alpha1.Story) error {
	for i, step := range story.Spec.Steps {
		if step.Ref != nil { // This is an Engram step.
			var engram bubuv1alpha1.Engram
			key := step.Ref.ToNamespacedName(story)
			if err := r.ControllerDependencies.Client.Get(ctx, key, &engram); err != nil {
				if errors.IsNotFound(err) {
					return fmt.Errorf("step '%s' references engram '%s' which does not exist in namespace '%s'", step.Name, key.Name, key.Namespace)
				}
				return fmt.Errorf("failed to get engram for step '%s': %w", step.Name, err)
			}
			// If this is a streaming story with a PerStory strategy, the engram must be a long-running type.
			if story.Spec.Pattern == enums.StreamingPattern && story.Spec.StreamingStrategy == enums.StreamingStrategyPerStory {
				if engram.Spec.Mode != enums.WorkloadModeDeployment && engram.Spec.Mode != enums.WorkloadModeStatefulSet {
					return fmt.Errorf("step '%s' references engram '%s' with mode '%s', but streaming stories with a PerStory strategy require 'deployment' or 'statefulset' mode", step.Name, engram.Name, engram.Spec.Mode)
				}
			}
		} else if step.Type == "" { // Not an Engram step, so it must have a Type.
			return fmt.Errorf("step %d ('%s') must have a 'type' or a 'ref'", i, step.Name)
		} else if step.Type == enums.StepTypeExecuteStory {
			if step.With == nil {
				return fmt.Errorf("step %d ('%s') is of type 'executeStory' but has no 'with' block", i, step.Name)
			}
			var with executeStoryWith
			if err := json.Unmarshal(step.With.Raw, &with); err != nil {
				return fmt.Errorf("step %d ('%s') has an invalid 'with' block for 'executeStory': %w", i, step.Name, err)
			}
			var subStory bubuv1alpha1.Story
			key := with.StoryRef.ToNamespacedName(story)
			if err := r.ControllerDependencies.Client.Get(ctx, key, &subStory); err != nil {
				if errors.IsNotFound(err) {
					return fmt.Errorf("step '%s' references story '%s' which does not exist in namespace '%s'", step.Name, key.Name, key.Namespace)
				}
				return fmt.Errorf("failed to get story for step '%s': %w", step.Name, err)
			}
		}

		// Add validation for other primitive types here as they are implemented.
		// For example:
		// if step.Type == enums.StepTypeExecuteStory {
		//   // validate the 'with' block for executeStory
		// }
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *StoryReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	r.Recorder = mgr.GetEventRecorderFor("story-controller")
	mapEngramToStories := func(ctx context.Context, obj client.Object) []reconcile.Request {
		var stories bubuv1alpha1.StoryList
		if err := r.Client.List(ctx, &stories, client.InNamespace(obj.GetNamespace()), client.MatchingFields{"spec.steps.ref.name": obj.GetName()}); err != nil {
			return nil
		}
		reqs := make([]reconcile.Request, 0, len(stories.Items))
		for i := range stories.Items {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Name: stories.Items[i].Name, Namespace: stories.Items[i].Namespace}})
		}
		return reqs
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&bubuv1alpha1.Story{}).
		Watches(&bubuv1alpha1.Engram{}, handler.EnqueueRequestsFromMapFunc(mapEngramToStories)).
		WithOptions(opts).
		Complete(r)
}
