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

package lifecycle

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/bubustack/bobrapet/internal/lifecycle"
	"github.com/bubustack/bobrapet/internal/logging"
)

// CleanupAnnotations defines the standard cleanup-related annotations
var CleanupAnnotations = []string{
	"bobrapet.bubu.sh/cleanup-successful-ttl",
	"bobrapet.bubu.sh/cleanup-failed-ttl",
	"bobrapet.bubu.sh/cleanup-running-ttl",
	"bobrapet.bubu.sh/cleanup-max-runs",
	"bobrapet.bubu.sh/cleanup-artifacts",
	"bobrapet.bubu.sh/cleanup-jobs",
	"bobrapet.bubu.sh/force-cleanup",
}

//+kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

// CleanupReconciler runs periodic cleanup of expired runs
type CleanupReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	CleanupManager *lifecycle.CleanupManager
}

// Reconcile performs periodic cleanup when namespaces are updated
func (r *CleanupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logging.NewControllerLogger(ctx, "cleanup").
		WithValues("namespace", req.Namespace)

	// Skip system namespaces
	// Note: For cluster-scoped Namespace resources, the name is in req.Name, not req.Namespace
	if req.Name == "kube-system" || req.Name == "kube-public" || req.Name == "kube-node-lease" {
		return ctrl.Result{}, nil
	}

	logger.Info("Starting scheduled cleanup")

	// Perform cleanup for this namespace
	// Note: Namespace resources are cluster-scoped, so req.Namespace is empty
	// and the actual namespace name is in req.Name
	if err := r.CleanupManager.CleanupNamespace(ctx, req.Name); err != nil {
		logger.Error(err, "Failed to cleanup namespace")
		return ctrl.Result{RequeueAfter: time.Hour}, err // Retry in 1 hour on error
	}

	logger.Info("Scheduled cleanup completed")

	// Requeue for next cleanup cycle (every 6 hours)
	return ctrl.Result{RequeueAfter: 6 * time.Hour}, nil
}

// SetupWithManager sets up the controller with the Manager
func (r *CleanupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return r.SetupWithManagerAndConfig(mgr, controller.Options{})
}

// SetupWithManagerAndConfig sets up the controller with specific configuration
func (r *CleanupReconciler) SetupWithManagerAndConfig(mgr ctrl.Manager, options controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Namespace{}).
		WithOptions(options).
		WithEventFilter(predicate.Funcs{
			CreateFunc: func(e event.CreateEvent) bool {
				return true
			},
			UpdateFunc: func(e event.UpdateEvent) bool {
				oldNS, oldOk := e.ObjectOld.(*corev1.Namespace)
				newNS, newOk := e.ObjectNew.(*corev1.Namespace)

				if !oldOk || !newOk {
					return false
				}

				// Check if cleanup-related annotations changed
				for _, annotation := range CleanupAnnotations {
					oldVal := ""
					newVal := ""

					if oldNS.Annotations != nil {
						oldVal = oldNS.Annotations[annotation]
					}
					if newNS.Annotations != nil {
						newVal = newNS.Annotations[annotation]
					}

					if oldVal != newVal {
						return true
					}
				}

				return false
			},
			DeleteFunc: func(e event.DeleteEvent) bool {
				return false
			},
		}).
		Complete(r)
}
