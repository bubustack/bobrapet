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

	"github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/patch"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
)

// EngramReconciler reconciles an Engram object
type EngramReconciler struct {
	config.ControllerDependencies
}

// +kubebuilder:rbac:groups=bubu.sh,resources=engrams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bubu.sh,resources=engrams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bubu.sh,resources=engrams/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Engram object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.1/pkg/reconcile
func (r *EngramReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var engram bubushv1alpha1.Engram
	if err := r.Get(ctx, req.NamespacedName, &engram); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Validate that the referenced template exists
	var template v1alpha1.EngramTemplate
	err := r.Get(ctx, client.ObjectKey{Name: engram.Spec.TemplateRef.Name}, &template)
	validationErr := err

	// Update the status based on validation.
	patchErr := patch.RetryableStatusPatch(ctx, r.Client, &engram, func(obj client.Object) {
		e := obj.(*bubushv1alpha1.Engram)
		e.Status.ObservedGeneration = e.Generation

		cm := conditions.NewConditionManager(e.Generation)
		if validationErr != nil {
			msg := fmt.Sprintf("EngramTemplate '%s' not found.", engram.Spec.TemplateRef.Name)
			if !errors.IsNotFound(validationErr) {
				msg = fmt.Sprintf("Failed to get EngramTemplate: %s", validationErr.Error())
			}
			cm.SetCondition(&e.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonTemplateNotFound, msg)
		} else {
			cm.SetCondition(&e.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonTemplateResolved, "EngramTemplate reference is valid.")
		}
	})

	if patchErr != nil {
		log.Error(patchErr, "Failed to update Engram status")
		return ctrl.Result{}, patchErr
	}

	// If validation failed, requeue with a delay.
	if validationErr != nil {
		return ctrl.Result{RequeueAfter: 1 * time.Minute}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *EngramReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&bubushv1alpha1.Engram{}).
		WithOptions(opts).
		Complete(r)
}
