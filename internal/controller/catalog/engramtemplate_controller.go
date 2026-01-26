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

package catalog

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
)

// EngramTemplateReconciler reconciles an EngramTemplate object
type EngramTemplateReconciler struct {
	config.ControllerDependencies
}

// +kubebuilder:rbac:groups=catalog.bubustack.io,resources=engramtemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=catalog.bubustack.io,resources=engramtemplates/status,verbs=get;patch;update
// +kubebuilder:rbac:groups=bubustack.io,resources=engrams,verbs=get;list;watch

// Reconcile validates and manages EngramTemplate lifecycle.
//
// Behavior:
//   - Fetches the EngramTemplate and validates required fields (image, version).
//   - Validates JSON schemas (input, output, config) if provided.
//   - Updates status with validation results and usage count.
//   - Records metrics for reconcile duration and errors.
//
// Arguments:
//   - ctx context.Context: for API calls and timeout from ConfigResolver.
//   - req ctrl.Request: identifies the EngramTemplate to reconcile.
//
// Returns:
//   - ctrl.Result with optional RequeueAfter on transient failures.
//   - Error propagated for logging; nil on success or handled errors.
func (r *EngramTemplateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	// Initialize structured logging and metrics
	rl := logging.NewReconcileLogger(ctx, "engramtemplate")
	startTime := time.Now()

	if r.ConfigResolver != nil {
		if timeout := r.ConfigResolver.GetOperatorConfig().Controller.ReconcileTimeout; timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
	}

	defer func() {
		duration := time.Since(startTime)
		metrics.RecordControllerReconcile("engramtemplate", duration, err)
	}()

	var template catalogv1alpha1.EngramTemplate
	if err = r.Get(ctx, req.NamespacedName, &template); err != nil {
		err = client.IgnoreNotFound(err)
		return ctrl.Result{}, err
	}

	// Add template context to logger
	templateLogger := rl.WithValues("template", template.Name, "version", template.Spec.Version)
	rl.ReconcileStart("Processing EngramTemplate")

	// Validate required fields using shared helper with TrimSpace
	if missingField, handled := checkRequiredFields(&template); handled {
		setErrorStatus(&template, fmt.Sprintf("%s is required", missingField))
		rl.ReconcileError(fmt.Errorf("%s missing", missingField), fmt.Sprintf("%s is required for EngramTemplate", missingField))
		if err = r.updateStatus(ctx, &template); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, err
	}

	templateLogger.Info("Validating EngramTemplate", "image", template.Spec.Image, "version", template.Spec.Version)

	// Validate JSON schemas if provided using shared helper that aggregates all errors
	schemaErrors := validateSchemas([]SchemaEntry{
		{Name: "input", Schema: template.Spec.InputSchema},
		{Name: "output", Schema: template.Spec.OutputSchema},
		{Name: "config", Schema: template.Spec.ConfigSchema},
	})
	if len(schemaErrors) > 0 {
		errorMsg := strings.Join(schemaErrors, "; ")
		setErrorStatus(&template, fmt.Sprintf("invalid schema(s): %s", errorMsg))
		rl.ReconcileError(fmt.Errorf("schema validation failed"), errorMsg)
		if updateErr := r.updateStatus(ctx, &template); updateErr != nil {
			templateLogger.Error(updateErr, "failed to update status after schema validation error")
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	templateLogger.V(1).Info("All schemas validated successfully",
		"has_input_schema", template.Spec.InputSchema != nil,
		"has_output_schema", template.Spec.OutputSchema != nil,
		"has_config_schema", template.Spec.ConfigSchema != nil)

	// Update status to ready
	setReadyStatus(&template)

	// List all Engrams that were created from this template (extracted helper)
	if err = r.setUsageCount(ctx, templateLogger.Logr(), &template, req.Name); err != nil {
		templateLogger.Error(err, "Failed to list engrams for template")
	}

	if err = r.updateStatus(ctx, &template); err != nil {
		rl.ReconcileError(err, "Failed to update EngramTemplate status")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}

	rl.ReconcileSuccess("EngramTemplate validated successfully",
		"image", template.Spec.Image,
		"version", template.Spec.Version,
		"has_input_schema", template.Spec.InputSchema != nil,
		"has_output_schema", template.Spec.OutputSchema != nil,
		"has_config_schema", template.Spec.ConfigSchema != nil)
	return ctrl.Result{}, nil
}

// setUsageCount lists Engrams using this template and updates UsageCount.
//
// Behavior:
//   - Lists Engrams via indexed lookup or fallback full list.
//   - Counts matches using aggregateEngramExecutions.
//   - Updates template.Status.UsageCount in memory.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - logger logr.Logger: for V(1) debug logging.
//   - template *catalogv1alpha1.EngramTemplate: receives updated UsageCount.
//   - templateName string: template name to match against.
//
// Returns:
//   - nil on success.
//   - Error if list operation fails.
func (r *EngramTemplateReconciler) setUsageCount(ctx context.Context, logger logr.Logger, template *catalogv1alpha1.EngramTemplate, templateName string) error {
	var engrams bubushv1alpha1.EngramList
	if err := listByTemplateRefNameOrAll(ctx, logger, r.Client, &engrams, templateName); err != nil {
		return err
	}
	template.Status.UsageCount = aggregateEngramExecutions(engrams.Items, templateName)
	return nil
}

// aggregateEngramExecutions counts Engrams whose spec.templateRef.name equals
// the provided template and returns that total as an int32.
//
// Behavior:
//   - Returns 0 immediately when templateName is empty or the caller supplied an
//     empty slice, preventing stale UsageCount values.
//   - Iterates each Engram once and increments the counter whenever
//     items[i].Spec.TemplateRef.Name matches the template.
//
// Arguments:
//   - items []bubushv1alpha1.Engram: Engrams already listed by setUsageCount
//     so reconciliation can reuse the same aggregation for indexed and fallback paths.
//   - templateName string: trimmed template name used to match Engrams.
//
// Returns:
//   - int32: number of Engrams referencing the template.
//
// Side Effects:
//   - None; callers assign the result to template.Status.UsageCount themselves.
//
// Notes / Gotchas:
//   - Keep shared counters (e.g., aggregateImpulseTriggers) in sync so catalog
//     dashboards report adoption consistently across template types.
func aggregateEngramExecutions(items []bubushv1alpha1.Engram, templateName string) int32 {
	return aggregateByTemplateRefName(items, templateName, func(item *bubushv1alpha1.Engram) string {
		return item.Spec.TemplateRef.Name
	})
}

// updateStatus persists the EngramTemplate status prepared by earlier helpers
// using kubeutil.RetryableStatusUpdate.
//
// Behavior:
//   - Clones the current Conditions and ValidationErrors slices plus the
//     ObservedGeneration/ValidationStatus/UsageCount scalars.
//   - Calls kubeutil.RetryableStatusUpdate so conflicts are retried until the
//     provided context expires.
//
// Arguments:
//   - ctx context.Context: controls retries/cancellation.
//   - template *catalogv1alpha1.EngramTemplate: object whose status is written.
//
// Returns:
//   - error: propagated from kubeutil.RetryableStatusUpdate.
//
// Side Effects:
//   - Issues Kubernetes status updates for ObservedGeneration, conditions,
//     ValidationStatus, ValidationErrors, and UsageCount.
//
// Notes / Gotchas:
//   - When adding new status fields, include them in the snapshot so retries do
//     not clobber computed values.
func (r *EngramTemplateReconciler) updateStatus(ctx context.Context, template *catalogv1alpha1.EngramTemplate) error {
	desired := snapshotTemplateStatus(
		template.Status.Conditions,
		template.Status.ValidationErrors,
		template.Status.UsageCount,
		template.Status.ObservedGeneration,
		template.Status.ValidationStatus,
	)
	return kubeutil.RetryableStatusUpdate(ctx, r.Client, template, func(obj client.Object) {
		t := obj.(*catalogv1alpha1.EngramTemplate)
		desired.applyToEngramTemplateStatus(&t.Status)
	})
}

// SetupWithManager sets up the controller with the Manager.
//
// Behavior:
//   - Registers the controller for EngramTemplate resources.
//   - Watches Engram resources to update template usage counts.
//   - Applies GenerationChangedPredicate to skip status-only updates.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//   - opts controller.Options: controller configuration (concurrency, etc.).
//
// Returns:
//   - nil on success.
//   - Error if controller setup fails.
func (r *EngramTemplateReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&catalogv1alpha1.EngramTemplate{}).
		Watches(
			&bubushv1alpha1.Engram{},
			handler.EnqueueRequestsFromMapFunc(r.mapEngramToTemplate),
		).
		WithOptions(opts).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("catalog-engramtemplate").
		Complete(r)
}

// mapEngramToTemplate converts Engram watch events into reconcile requests for
// the referenced EngramTemplate by trimming spec.templateRef.name and enqueuing
// a single NamespacedName with that value.
//
// Behavior:
//   - Ignores events that are not Engrams or whose templateRef name is empty
//     to avoid unnecessary reconciles.
//   - Trims whitespace and returns []reconcile.Request{{NamespacedName: {Name: trimmed}}}
//     for cluster-scoped EngramTemplates.
//
// Arguments:
//   - _ context.Context: unused.
//   - obj client.Object: informer object, expected to be *bubushv1alpha1.Engram.
//
// Returns:
//   - []reconcile.Request: nil when no template should be enqueued, otherwise a
//     single request targeting the referenced template.
//
// Side Effects:
//   - None; used by handler.EnqueueRequestsFromMapFunc in SetupWithManager.
func (r *EngramTemplateReconciler) mapEngramToTemplate(_ context.Context, obj client.Object) []reconcile.Request {
	return enqueueTemplateRequest(obj, func(object client.Object) string {
		engram, ok := object.(*bubushv1alpha1.Engram)
		if !ok {
			return ""
		}
		return engram.Spec.TemplateRef.Name
	})
}
