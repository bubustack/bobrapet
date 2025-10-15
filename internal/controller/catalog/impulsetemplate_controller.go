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
	"encoding/json"
	"fmt"
	"slices"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
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

// +kubebuilder:rbac:groups=catalog.bubustack.io,resources=impulsetemplates,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=catalog.bubustack.io,resources=impulsetemplates/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=catalog.bubustack.io,resources=impulsetemplates/finalizers,verbs=update

// Reconcile validates and manages ImpulseTemplate lifecycle
func (r *ImpulseTemplateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Initialize structured logging and metrics
	rl := logging.NewReconcileLogger(ctx, "impulsetemplate")
	startTime := time.Now()

	defer func() {
		duration := time.Since(startTime)
		metrics.RecordControllerReconcile("impulsetemplate", duration, nil)
	}()

	var template catalogv1alpha1.ImpulseTemplate
	if err := r.Get(ctx, req.NamespacedName, &template); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Add template context to logger
	templateLogger := rl.WithValues("template", template.Name, "version", template.Spec.Version)
	rl.ReconcileStart("Processing ImpulseTemplate")

	// Validate required fields
	if template.Spec.Image == "" {
		r.updateErrorStatus(&template, "image is required")
		rl.ReconcileError(fmt.Errorf("image missing"), "Image is required for ImpulseTemplate")
		if err := r.updateStatusWithRetry(ctx, &template); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	if template.Spec.Version == "" {
		r.updateErrorStatus(&template, "version is required")
		rl.ReconcileError(fmt.Errorf("version missing"), "Version is required for ImpulseTemplate")
		if err := r.updateStatusWithRetry(ctx, &template); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	templateLogger.Info("Validating ImpulseTemplate", "image", template.Spec.Image, "version", template.Spec.Version)

	// Validate supported modes are appropriate for impulses (deployment or statefulset only)
	validImpulseModes := []enums.WorkloadMode{"deployment", "statefulset"}
	for _, mode := range template.Spec.SupportedModes {
		if !slices.Contains(validImpulseModes, mode) {
			r.updateErrorStatus(&template, fmt.Sprintf("invalid supported mode '%s' for impulse template (must be deployment or statefulset)", mode))
			rl.ReconcileError(fmt.Errorf("invalid supported mode: %s", mode), "Invalid supported mode for ImpulseTemplate")
			if err := r.updateStatusWithRetry(ctx, &template); err != nil {
				return ctrl.Result{RequeueAfter: 5 * time.Second}, err
			}
			return ctrl.Result{}, nil
		}
	}
	templateLogger.V(1).Info("Supported modes validated", "modes", template.Spec.SupportedModes)

	// Validate JSON schemas if provided
	if template.Spec.ContextSchema != nil {
		if err := r.validateJSONSchema(template.Spec.ContextSchema.Raw); err != nil {
			r.updateErrorStatus(&template, fmt.Sprintf("invalid context schema: %v", err))
			rl.ReconcileError(err, "Invalid context schema")
			if updateErr := r.updateStatusWithRetry(ctx, &template); updateErr != nil {
				templateLogger.Error(updateErr, "failed to update status after schema validation error")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		templateLogger.V(1).Info("Context schema validated")
	}

	if template.Spec.ConfigSchema != nil {
		if err := r.validateJSONSchema(template.Spec.ConfigSchema.Raw); err != nil {
			r.updateErrorStatus(&template, fmt.Sprintf("invalid config schema: %v", err))
			rl.ReconcileError(err, "Invalid config schema")
			if updateErr := r.updateStatusWithRetry(ctx, &template); updateErr != nil {
				templateLogger.Error(updateErr, "failed to update status after schema validation error")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		templateLogger.V(1).Info("Config schema validated")
	}

	// Update status to ready
	r.updateReadyStatus(&template)

	// List all Impulses that were created from this template
	var impulses bubushv1alpha1.ImpulseList
	if err := r.List(ctx, &impulses, client.MatchingFields{"spec.templateRef.name": req.Name}); err != nil {
		templateLogger.Error(err, "Failed to list impulses for template")
		// We don't fail the reconcile, just log the error. Status will be updated on next reconcile.
	} else {
		template.Status.UsageCount = int32(len(impulses.Items))
	}

	if err := r.updateStatusWithRetry(ctx, &template); err != nil {
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

// validateJSONSchema performs basic JSON schema validation
func (r *ImpulseTemplateReconciler) validateJSONSchema(schemaBytes []byte) error {
	if len(schemaBytes) == 0 {
		return nil
	}

	// Basic JSON validation
	var schema any
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	// Could add more sophisticated JSON Schema validation here
	// For now, we just ensure it's valid JSON
	return nil
}

// updateErrorStatus updates the template status with an error condition
func (r *ImpulseTemplateReconciler) updateErrorStatus(template *catalogv1alpha1.ImpulseTemplate, message string) {
	cm := conditions.NewConditionManager(template.Generation)
	cm.SetCondition(&template.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonValidationFailed, message)
}

// updateReadyStatus updates the template status to ready
func (r *ImpulseTemplateReconciler) updateReadyStatus(template *catalogv1alpha1.ImpulseTemplate) {
	cm := conditions.NewConditionManager(template.Generation)
	cm.SetCondition(&template.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonValidationPassed, "ImpulseTemplate is valid and ready for use")
}

// setCondition updates or appends a condition to the conditions slice
// remove custom setCondition helper; ConditionManager is used for consistency

// updateStatusWithRetry performs status update with retry logic
func (r *ImpulseTemplateReconciler) updateStatusWithRetry(ctx context.Context, template *catalogv1alpha1.ImpulseTemplate) error {
	var lastErr error
	for i := 0; i < 3; i++ {
		if err := r.Status().Update(ctx, template); err != nil {
			lastErr = err
			if i < 2 {
				// Use a timer that respects context cancellation
				sleepDuration := time.Duration(100*(1<<uint(i))) * time.Millisecond
				select {
				case <-time.After(sleepDuration):
					// Continue to next retry
				case <-ctx.Done():
					return fmt.Errorf("context cancelled while waiting to retry status update: %w", ctx.Err())
				}
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to update ImpulseTemplate status after %d retries: %w", 3, lastErr)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ImpulseTemplateReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&catalogv1alpha1.ImpulseTemplate{}).
		WithOptions(opts).
		Complete(r)
}

// SetupWithManagerAndConfig sets up the controller with the Manager using configurable options.
func (r *ImpulseTemplateReconciler) SetupWithManagerAndConfig(mgr ctrl.Manager, options controller.Options) error {
	return r.setupControllerWithOptions(mgr, options)
}

// setupControllerWithOptions configures the controller with specific options
func (r *ImpulseTemplateReconciler) setupControllerWithOptions(mgr ctrl.Manager, options controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&catalogv1alpha1.ImpulseTemplate{}).
		WithOptions(options).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("catalog-impulsetemplate").
		Complete(r)
}
