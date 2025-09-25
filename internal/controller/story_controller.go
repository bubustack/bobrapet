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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
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

	// Handle streaming pattern infrastructure
	if story.Spec.Pattern == enums.StreamingPattern {
		// Reconcile Deployments/StatefulSets for each step
		log.FromContext(ctx).Info("Reconciling workloads for streaming Story")
		// TODO: Implement the logic to iterate through story.Spec.Steps
		// and create/update a Deployment or StatefulSet for each engram ref.
	}

	return ctrl.Result{}, nil
}

func (r *StoryReconciler) validateEngramReferences(ctx context.Context, story *bubuv1alpha1.Story) error {
	for _, step := range story.Spec.Steps {
		if step.Ref != nil {
			var engram bubuv1alpha1.Engram
			key := step.Ref.ToNamespacedName(story)
			if err := r.ControllerDependencies.Client.Get(ctx, key, &engram); err != nil {
				if errors.IsNotFound(err) {
					return fmt.Errorf("step '%s' references engram '%s' which does not exist in namespace '%s'", step.Name, key.Name, key.Namespace)
				}
				return fmt.Errorf("failed to get engram for step '%s': %w", step.Name, err)
			}
		}
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
