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
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/patch"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
)

// EngramReconciler reconciles an Engram object
type EngramReconciler struct {
	config.ControllerDependencies
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=bubustack.io,resources=engrams,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.1/pkg/reconcile
func (r *EngramReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	log := logging.NewReconcileLogger(ctx, "engram").WithValues("engram", req.NamespacedName)
	startTime := time.Now()
	defer func() {
		metrics.RecordControllerReconcile("engram", time.Since(startTime), err)
	}()

	// Bound reconcile duration to avoid hanging on slow/unresponsive API calls
	timeout := r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var engram bubushv1alpha1.Engram
	if err := r.Get(ctx, req.NamespacedName, &engram); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Validate that the referenced template exists
	var template v1alpha1.EngramTemplate
	err = r.Get(ctx, client.ObjectKey{Name: engram.Spec.TemplateRef.Name, Namespace: ""}, &template)
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
		if errors.IsNotFound(patchErr) {
			// Object was removed while we were patching status; nothing left to do.
			return ctrl.Result{}, nil
		}
		log.Error(patchErr, "Failed to update Engram status")
		return ctrl.Result{}, patchErr
	}

	// If validation failed, requeue with a delay.
	if validationErr != nil {
		// Emit a warning event for visibility
		r.Recorder.Event(&engram, corev1.EventTypeWarning, conditions.ReasonTemplateNotFound, fmt.Sprintf("EngramTemplate '%s' not found or failed to resolve", engram.Spec.TemplateRef.Name))
		if errors.IsNotFound(validationErr) {
			// If template is not found, we should not requeue with a short delay.
			// Rely on the watch on EngramTemplate to trigger a new reconcile when the template is created.
			log.Info("EngramTemplate not found, will not requeue.")
			return ctrl.Result{}, nil
		}
		log.Error(validationErr, "Validation failed, requeueing with exponential backoff.")
		return ctrl.Result{}, validationErr
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *EngramReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	// Wire event recorder
	r.Recorder = mgr.GetEventRecorderFor("engram-controller")
	mapTemplateToEngrams := func(ctx context.Context, obj client.Object) []reconcile.Request {
		var engrams bubushv1alpha1.EngramList
		// List all engrams in all namespaces that might reference this cluster-scoped template.
		if err := r.List(ctx, &engrams, client.MatchingFields{"spec.templateRef.name": obj.GetName()}); err != nil {
			// In case of error, log it but don't panic the controller.
			// An empty list will be returned, and reconciliation will depend on engram updates.
			log := logging.NewControllerLogger(ctx, "engram-mapper")
			log.Error(err, "failed to list engrams for engramtemplate watch", "engramtemplate", obj.GetName())
			return nil
		}

		reqs := make([]reconcile.Request, 0, len(engrams.Items))
		for i := range engrams.Items {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Name: engrams.Items[i].Name, Namespace: engrams.Items[i].Namespace}})
		}
		return reqs
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&bubushv1alpha1.Engram{}).
		Watches(&v1alpha1.EngramTemplate{}, handler.EnqueueRequestsFromMapFunc(mapTemplateToEngrams)).
		WithOptions(opts).
		Complete(r)
}
