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
	"fmt"
	"time"

	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/patch"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"k8s.io/apimachinery/pkg/api/errors"
)

// StoryReconciler reconciles a Story object
type StoryReconciler struct {
	config.ControllerDependencies
}

// +kubebuilder:rbac:groups=bubu.sh,resources=stories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bubu.sh,resources=stories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bubu.sh,resources=stories/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Story object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.2/pkg/reconcile
// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (r *StoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	var story bubuv1alpha1.Story
	if err := r.ControllerDependencies.Client.Get(ctx, req.NamespacedName, &story); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Validate that all referenced engrams exist
	validationErr := r.validateEngramReferences(ctx, &story)

	// Update the status based on validation.
	err := patch.RetryableStatusPatch(ctx, r.ControllerDependencies.Client, &story, func(obj client.Object) {
		s := obj.(*bubuv1alpha1.Story)
		s.Status.StepsTotal = int32(len(s.Spec.Steps))
		s.Status.ObservedGeneration = s.Generation

		cm := conditions.NewConditionManager(s.Generation)
		if validationErr != nil {
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonReferenceNotFound, validationErr.Error())
		} else {
			cm.SetCondition(&s.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonValidationPassed, "All Engram references are valid.")
		}
	})

	if err != nil {
		return ctrl.Result{}, err
	}

	// If validation failed, we requeue with a delay.
	// This prevents constant requeueing for a story that references a non-existent engram.
	if validationErr != nil {
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	// Handle streaming pattern infrastructure for PerStory strategy
	if story.Spec.Pattern == enums.StreamingPattern && story.Spec.StreamingStrategy == enums.StreamingStrategyPerStory {
		return r.reconcilePerStoryStreaming(ctx, &story)
	}

	return ctrl.Result{}, nil
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

		// We can reuse the logic from the RealtimeEngramReconciler, but we need the full config.
		// A full implementation would require getting the template and resolving the config here.
		// For now, we will create a placeholder deployment. A real implementation would need the config resolver.
		// This is a simplified example; a full implementation needs the resolved execution config.
		deployment := r.deploymentForStreamingStep(story, &step, &engram)
		if err := r.reconcileOwnedDeployment(ctx, story, deployment); err != nil {
			return ctrl.Result{}, err
		}

		service := r.serviceForStreamingStep(story, &step, &engram)
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
	// TODO: Add update logic if the spec differs.
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
	// TODO: Add update logic if the spec differs.
	return nil
}

func (r *StoryReconciler) deploymentForStreamingStep(story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, engram *bubuv1alpha1.Engram) *appsv1.Deployment {
	name := fmt.Sprintf("%s-%s", story.Name, step.Name)
	labels := map[string]string{
		"app.kubernetes.io/name":       "bobrapet-streaming-engram",
		"app.kubernetes.io/managed-by": "story-controller",
		"bubu.sh/story":                story.Name,
		"bubu.sh/step":                 step.Name,
	}
	replicas := int32(1)

	// NOTE: This is a simplified spec. A real implementation would need to fetch the
	// EngramTemplate and use the config resolver to get the correct image, resources, etc.
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
					Containers: []corev1.Container{{
						Name:  "engram",
						Image: "busybox", // Placeholder - should come from resolved config
						Args:  []string{"sleep", "3600"},
					}},
				},
			},
		},
	}
}

func (r *StoryReconciler) serviceForStreamingStep(story *bubuv1alpha1.Story, step *bubuv1alpha1.Step, engram *bubuv1alpha1.Engram) *corev1.Service {
	name := fmt.Sprintf("%s-%s", story.Name, step.Name)
	labels := map[string]string{
		"bubu.sh/story": story.Name,
		"bubu.sh/step":  step.Name,
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
				Port:       80, // Placeholder
				TargetPort: intstr.FromInt(8080),
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&bubuv1alpha1.Story{}).
		WithOptions(opts).
		Complete(r)
}
