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
	"slices"
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
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
)

const (
	// ImpulseTemplateFinalizer is the finalizer for ImpulseTemplate resources
	ImpulseTemplateFinalizer = "impulsetemplate.catalog.bubustack.io/finalizer"
)

// ImpulseTemplateReconciler reconciles a ImpulseTemplate object
type ImpulseTemplateReconciler struct {
	config.ControllerDependencies
}

// +kubebuilder:rbac:groups=catalog.bubustack.io,resources=impulsetemplates,verbs=get;list;watch
// +kubebuilder:rbac:groups=catalog.bubustack.io,resources=impulsetemplates/status,verbs=get;patch;update
// +kubebuilder:rbac:groups=bubustack.io,resources=impulses,verbs=get;list;watch

// Reconcile validates and manages ImpulseTemplate lifecycle.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/reconcile
//
// Behavior:
//   - Fetches the ImpulseTemplate and validates required fields (image, version).
//   - Validates supported modes (deployment/statefulset only for impulses).
//   - Validates JSON schemas (context, config) if provided.
//   - Updates status with validation results and usage count.
//   - Records metrics for reconcile duration and errors.
//
// Arguments:
//   - ctx context.Context: for API calls and timeout from ConfigResolver.
//   - req ctrl.Request: identifies the ImpulseTemplate to reconcile.
//
// Returns:
//   - ctrl.Result with optional RequeueAfter on transient failures.
//   - Error propagated for logging; nil on success or handled errors.
func (r *ImpulseTemplateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	// Initialize structured logging and metrics
	rl := logging.NewReconcileLogger(ctx, "impulsetemplate")
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
		metrics.RecordControllerReconcile("impulsetemplate", duration, err)
	}()

	var template catalogv1alpha1.ImpulseTemplate
	if err = r.Get(ctx, req.NamespacedName, &template); err != nil {
		err = client.IgnoreNotFound(err)
		return ctrl.Result{}, err
	}

	// Add template context to logger
	templateLogger := rl.WithValues("template", template.Name, "version", template.Spec.Version)
	rl.ReconcileStart("Processing ImpulseTemplate")

	// Validate required fields using shared helper with TrimSpace
	if missing, handled := checkRequiredFields(&template); handled {
		setErrorStatus(&template, fmt.Sprintf("%s is required", missing))
		rl.ReconcileError(fmt.Errorf("%s missing", missing), fmt.Sprintf("%s is required for ImpulseTemplate", missing))
		if err = r.persistStatus(ctx, &template); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, err
	}

	templateLogger.Info("Validating ImpulseTemplate", "image", template.Spec.Image, "version", template.Spec.Version)

	// Validate supported modes (extracted helper)
	if invalidMode, handled := r.validateSupportedModes(&template); handled {
		setErrorStatus(&template, fmt.Sprintf("invalid supported mode '%s' for impulse template (must be deployment or statefulset)", invalidMode))
		rl.ReconcileError(fmt.Errorf("invalid supported mode: %s", invalidMode), "Invalid supported mode for ImpulseTemplate")
		if err = r.persistStatus(ctx, &template); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, err
	}
	templateLogger.V(1).Info("Supported modes validated", "modes", template.Spec.SupportedModes)

	// Validate JSON schemas if provided using shared helper that aggregates all errors
	schemaErrors := validateSchemas([]SchemaEntry{
		{Name: "context", Schema: template.Spec.ContextSchema},
		{Name: "config", Schema: template.Spec.ConfigSchema},
	})
	if len(schemaErrors) > 0 {
		errorMsg := strings.Join(schemaErrors, "; ")
		setErrorStatus(&template, fmt.Sprintf("invalid schema(s): %s", errorMsg))
		rl.ReconcileError(fmt.Errorf("schema validation failed"), errorMsg)
		if updateErr := r.persistStatus(ctx, &template); updateErr != nil {
			templateLogger.Error(updateErr, "failed to update status after schema validation error")
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	templateLogger.V(1).Info("All schemas validated successfully",
		"has_context_schema", template.Spec.ContextSchema != nil,
		"has_config_schema", template.Spec.ConfigSchema != nil)

	// Update status to ready
	setReadyStatus(&template)

	// List all Impulses that were created from this template
	if err := r.setImpulseTemplateUsage(ctx, templateLogger.Logr(), &template, req.Name); err != nil {
		templateLogger.Error(err, "Failed to compute impulse template usage")
	}

	if err = r.persistStatus(ctx, &template); err != nil {
		rl.ReconcileError(err, "Failed to update ImpulseTemplate status")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}

	rl.ReconcileSuccess("ImpulseTemplate validated successfully",
		"image", template.Spec.Image,
		"version", template.Spec.Version,
		"supported_modes", template.Spec.SupportedModes,
		"has_context_schema", template.Spec.ContextSchema != nil,
		"has_config_schema", template.Spec.ConfigSchema != nil)
	return ctrl.Result{}, nil
}

// persistStatus snapshots the current ImpulseTemplate status fields and writes
// them via kubeutil.RetryableStatusUpdate so Ready/validation data persist even
// when concurrent reconciliation updates occur
//
// Behavior:
//   - Deep-copies Conditions and ValidationErrors plus UsageCount,
//     ObservedGeneration, and ValidationStatus into local variables.
//   - Invokes kubeutil.RetryableStatusUpdate with the caller's context so each
//     retry re-GETs the object, applies the snapshot, and updates the status
//     subresource.
//
// Arguments:
//   - ctx context.Context: propagated to kubeutil.RetryableStatusUpdate to honor
//     reconcile deadlines.
//   - template *catalogv1alpha1.ImpulseTemplate: supplies the key for the retry
//     helper and the status snapshot to persist.
//
// Returns:
//   - error: non-nil when the status update ultimately fails so callers can
//     requeue.
//
// Side Effects:
//   - Issues status GET/UPDATE calls and mutates the API object, but leaves the
//     caller's in-memory snapshot unchanged beyond the initial field copies.
//
// Notes / Gotchas:
//   - Add new status fields to the cloned snapshot when extending the CRD; any
//     fields omitted here will be overwritten during persistence.
func (r *ImpulseTemplateReconciler) persistStatus(ctx context.Context, template *catalogv1alpha1.ImpulseTemplate) error {
	desired := snapshotTemplateStatus(
		template.Status.Conditions,
		template.Status.ValidationErrors,
		template.Status.UsageCount,
		template.Status.ObservedGeneration,
		template.Status.ValidationStatus,
	)
	return kubeutil.RetryableStatusUpdate(ctx, r.Client, template, func(obj client.Object) {
		t := obj.(*catalogv1alpha1.ImpulseTemplate)
		desired.applyToImpulseTemplateStatus(&t.Status)
	})
}

// setImpulseTemplateUsage lists Impulses that reference the provided template
// and updates template.Status.UsageCount before the status patch
//
// Behavior:
//   - Attempts to list Impulses via the spec.templateRef.name field index and
//     computes the count with aggregateImpulseTriggers when the index succeeds
//     without requiring a full list.
//   - Falls back to an unfiltered list when the index lookup fails, then repeats
//     the aggregation.
//
// Arguments:
//   - ctx context.Context: passed to client.List to honor reconcile timeouts
//     and cancellation.
//   - template *catalogv1alpha1.ImpulseTemplate: receives the updated UsageCount
//     in memory.
//   - templateName string: selector key used for both the indexed list and the
//     aggregation comparison.
//
// Returns:
//   - error: nil if at least one list succeeds; fallback list errors propagate so
//     callers can log failures.
//
// Side Effects:
//   - Issues up to two List calls for Impulse objects and mutates
//     template.Status.UsageCount; persistence still relies on persistStatus
//     later in reconciliation.
//
// Notes / Gotchas:
//   - Logs at V(1) when the indexed list fails before falling back.
//
//nolint:logcheck // ctx carries request metadata; logger is the controller's structured logger
func (r *ImpulseTemplateReconciler) setImpulseTemplateUsage(ctx context.Context, logger logr.Logger, template *catalogv1alpha1.ImpulseTemplate, templateName string) error {
	var impulses bubushv1alpha1.ImpulseList
	if err := listByTemplateRefNameOrAll(ctx, logger, r.Client, &impulses, templateName); err != nil {
		return err
	}
	template.Status.UsageCount = aggregateImpulseTriggers(impulses.Items, templateName)
	return nil
}

// aggregateImpulseTriggers counts Impulse objects whose spec.templateRef.name
// matches the provided template and returns that total as an int32
//
// Behavior:
//   - Returns 0 when templateName is empty or the slice is nil.
//   - Performs a single pass over items and increments the count whenever
//     items[i].Spec.TemplateRef.Name equals templateName.
//
// Arguments:
//   - items []bubushv1alpha1.Impulse: result of a List call in
//     setImpulseTemplateUsage.
//   - templateName string: trimmed template name from the reconcile request.
//
// Returns:
//   - int32: number of Impulses referencing the template.
//
// Side Effects:
//   - None; callers assign the returned count to Status.UsageCount themselves.
//
// Notes / Gotchas:
//   - See impulsetemplate_usage_test.go for unit tests that cover the nil-slice
//     and mismatch cases.
func aggregateImpulseTriggers(items []bubushv1alpha1.Impulse, templateName string) int32 {
	return aggregateByTemplateRefName(items, templateName, func(item *bubushv1alpha1.Impulse) string {
		return item.Spec.TemplateRef.Name
	})
}

// SetupWithManager sets up the controller with the Manager.
//
// Behavior:
//   - Delegates to setupControllerWithOptions for actual setup.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//   - opts controller.Options: controller configuration (concurrency, etc.).
//
// Returns:
//   - nil on success.
//   - Error if controller setup fails.
func (r *ImpulseTemplateReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return r.setupControllerWithOptions(mgr, opts)
}

// setupControllerWithOptions configures the controller with specific options.
//
// Behavior:
//   - Registers the controller for ImpulseTemplate resources.
//   - Watches Impulse resources to update template usage counts.
//   - Applies GenerationChangedPredicate to skip status-only updates.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//   - options controller.Options: controller configuration.
//
// Returns:
//   - nil on success.
//   - Error if controller setup fails.
func (r *ImpulseTemplateReconciler) setupControllerWithOptions(mgr ctrl.Manager, options controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&catalogv1alpha1.ImpulseTemplate{}).
		Watches(
			&bubushv1alpha1.Impulse{},
			handler.EnqueueRequestsFromMapFunc(r.mapImpulseToTemplate),
		).
		WithOptions(options).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("catalog-impulsetemplate").
		Complete(r)
}

// mapImpulseToTemplate converts Impulse watch events into reconcile requests for
// the referenced ImpulseTemplate by trimming spec.templateRef.name and enqueuing
// a single NamespacedName with that value.
//
// Behavior:
//   - Ignores events that are not Impulses or whose templateRef name is empty
//     to avoid unnecessary reconciles.
//   - Trims whitespace and returns []reconcile.Request{{NamespacedName: {Name: trimmed}}}
//     for cluster-scoped ImpulseTemplates.
//
// Arguments:
//   - _ context.Context: unused.
//   - obj client.Object: informer object, expected to be *bubushv1alpha1.Impulse.
//
// Returns:
//   - []reconcile.Request: nil when no template should be enqueued, otherwise a
//     single request targeting the referenced template.
//
// Side Effects:
//   - None; used by handler.EnqueueRequestsFromMapFunc in setupControllerWithOptions.
func (r *ImpulseTemplateReconciler) mapImpulseToTemplate(_ context.Context, obj client.Object) []reconcile.Request {
	return enqueueTemplateRequest(obj, func(object client.Object) string {
		impulse, ok := object.(*bubushv1alpha1.Impulse)
		if !ok {
			return ""
		}
		return impulse.Spec.TemplateRef.Name
	})
}

// validateSupportedModes ensures only deployment/statefulset are allowed.
//
// Behavior:
//   - Checks each mode in SupportedModes against allowed values.
//   - Impulses cannot run as jobs (must be always-on).
//
// Arguments:
//   - t *catalogv1alpha1.ImpulseTemplate: the template to validate.
//
// Returns:
//   - invalid enums.WorkloadMode: the first invalid mode found.
//   - handled bool: true if an invalid mode exists.
func (r *ImpulseTemplateReconciler) validateSupportedModes(t *catalogv1alpha1.ImpulseTemplate) (invalid enums.WorkloadMode, handled bool) {
	valid := []enums.WorkloadMode{"deployment", "statefulset"}
	for _, mode := range t.Spec.SupportedModes {
		if !slices.Contains(valid, mode) {
			return mode, true
		}
	}
	return "", false
}
