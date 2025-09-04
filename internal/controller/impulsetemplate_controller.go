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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"

	"github.com/bubustack/bobrapet/internal/logging"
	"github.com/bubustack/bobrapet/internal/metrics"
)

// ImpulseTemplateReconciler reconciles an ImpulseTemplate object
type ImpulseTemplateReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	conditionManager *ConditionManager
	// reconcilePattern removed
}

//+kubebuilder:rbac:groups=catalog.bubu.sh,resources=impulsetemplates,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=catalog.bubu.sh,resources=impulsetemplates/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=catalog.bubu.sh,resources=impulsetemplates/finalizers,verbs=update

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
	templateLogger := rl.WithValues("template", template.Name, "category", getImpulseCategory(template.Spec.UIHints), "version", template.Spec.Version)
	rl.ReconcileStart("Processing ImpulseTemplate")

	// Validate required fields
	if template.Spec.Image == "" {
		r.updateErrorStatus(&template, "image is required")
		rl.ReconcileError(fmt.Errorf("image missing"), "Image is required for ImpulseTemplate")
		if err := r.updateStatusWithRetry(ctx, &template, 3); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	if template.Spec.Version == "" {
		r.updateErrorStatus(&template, "version is required")
		rl.ReconcileError(fmt.Errorf("version missing"), "Version is required for ImpulseTemplate")
		if err := r.updateStatusWithRetry(ctx, &template, 3); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	templateLogger.Info("Validating ImpulseTemplate", "image", template.Spec.Image, "version", template.Spec.Version)

	// Validate supported modes are appropriate for impulses (deployment or statefulset only)
	validImpulseModes := []string{"deployment", "statefulset"}
	for _, mode := range template.Spec.SupportedModes {
		valid := false
		for _, vm := range validImpulseModes {
			if mode == vm {
				valid = true
				break
			}
		}
		if !valid {
			r.updateErrorStatus(&template, fmt.Sprintf("invalid supported mode '%s' for impulse template (must be deployment or statefulset)", mode))
			rl.ReconcileError(fmt.Errorf("invalid supported mode: %s", mode), "Invalid supported mode for ImpulseTemplate")
			if err := r.updateStatusWithRetry(ctx, &template, 3); err != nil {
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
			if updateErr := r.updateStatusWithRetry(ctx, &template, 3); updateErr != nil {
				templateLogger.Error(updateErr, "failed to update status after schema validation error")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		templateLogger.V(1).Info("Context schema validated")
	}

	if template.Spec.MappingSchema != nil {
		if err := r.validateJSONSchema(template.Spec.MappingSchema.Raw); err != nil {
			r.updateErrorStatus(&template, fmt.Sprintf("invalid mapping schema: %v", err))
			rl.ReconcileError(err, "Invalid mapping schema")
			if updateErr := r.updateStatusWithRetry(ctx, &template, 3); updateErr != nil {
				templateLogger.Error(updateErr, "failed to update status after schema validation error")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		templateLogger.V(1).Info("Mapping schema validated")
	}

	// Update status to ready
	r.updateReadyStatus(&template)
	if err := r.updateStatusWithRetry(ctx, &template, 3); err != nil {
		rl.ReconcileError(err, "Failed to update ImpulseTemplate status")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}

	rl.ReconcileSuccess("ImpulseTemplate validated successfully",
		"image", template.Spec.Image,
		"version", template.Spec.Version,
		"supported_modes", template.Spec.SupportedModes,
		"has_context_schema", template.Spec.ContextSchema != nil,
		"has_mapping_schema", template.Spec.MappingSchema != nil)
	return ctrl.Result{}, nil
}

// validateJSONSchema performs basic JSON schema validation
func (r *ImpulseTemplateReconciler) validateJSONSchema(schemaBytes []byte) error {
	if len(schemaBytes) == 0 {
		return nil
	}

	// Basic JSON validation
	var schema interface{}
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	// Could add more sophisticated JSON Schema validation here
	// For now, we just ensure it's valid JSON
	return nil
}

// updateErrorStatus updates the template status with an error condition
func (r *ImpulseTemplateReconciler) updateErrorStatus(template *catalogv1alpha1.ImpulseTemplate, message string) {
	now := metav1.Now()
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "ValidationFailed",
		Message:            message,
		LastTransitionTime: now,
		ObservedGeneration: template.Generation,
	}
	r.setCondition(&template.Status.Conditions, condition)
}

// updateReadyStatus updates the template status to ready
func (r *ImpulseTemplateReconciler) updateReadyStatus(template *catalogv1alpha1.ImpulseTemplate) {
	now := metav1.Now()
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "Valid",
		Message:            "ImpulseTemplate is valid and ready for use",
		LastTransitionTime: now,
		ObservedGeneration: template.Generation,
	}
	r.setCondition(&template.Status.Conditions, condition)
}

// setCondition updates or appends a condition to the conditions slice
func (r *ImpulseTemplateReconciler) setCondition(conditions *[]metav1.Condition, newCondition metav1.Condition) {
	for i, condition := range *conditions {
		if condition.Type == newCondition.Type {
			(*conditions)[i] = newCondition
			return
		}
	}
	*conditions = append(*conditions, newCondition)
}

// updateStatusWithRetry performs status update with retry logic
func (r *ImpulseTemplateReconciler) updateStatusWithRetry(ctx context.Context, template *catalogv1alpha1.ImpulseTemplate, maxRetries int) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if err := r.Status().Update(ctx, template); err != nil {
			lastErr = err
			if i < maxRetries-1 {
				// Exponential backoff: 100ms, 200ms, 400ms...
				time.Sleep(time.Duration(100*(1<<uint(i))) * time.Millisecond)
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to update ImpulseTemplate status after %d retries: %w", maxRetries, lastErr)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ImpulseTemplateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return r.SetupWithManagerAndConfig(mgr, controller.Options{MaxConcurrentReconciles: 3})
}

// SetupWithManagerAndConfig sets up the controller with the Manager using configurable options.
func (r *ImpulseTemplateReconciler) SetupWithManagerAndConfig(mgr ctrl.Manager, options controller.Options) error {
	return r.setupControllerWithOptions(mgr, options)
}

// setupControllerWithOptions configures the controller with specific options
func (r *ImpulseTemplateReconciler) setupControllerWithOptions(mgr ctrl.Manager, options controller.Options) error {
	ctx := context.Background()

	// Index ImpulseTemplates by category for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(ctx, &catalogv1alpha1.ImpulseTemplate{}, "spec.uiHints.category",
		func(obj client.Object) []string {
			template := obj.(*catalogv1alpha1.ImpulseTemplate)
			if template.Spec.UIHints != nil && template.Spec.UIHints.Category != "" {
				return []string{template.Spec.UIHints.Category}
			}
			return []string{"uncategorized"}
		}); err != nil {
		return err
	}

	// Index by version for version-based queries
	if err := mgr.GetFieldIndexer().IndexField(ctx, &catalogv1alpha1.ImpulseTemplate{}, "spec.version",
		func(obj client.Object) []string {
			return []string{obj.(*catalogv1alpha1.ImpulseTemplate).Spec.Version}
		}); err != nil {
		return err
	}

	// Index by supported modes for mode-based queries
	if err := mgr.GetFieldIndexer().IndexField(ctx, &catalogv1alpha1.ImpulseTemplate{}, "spec.supportedModes",
		func(obj client.Object) []string {
			template := obj.(*catalogv1alpha1.ImpulseTemplate)
			if len(template.Spec.SupportedModes) > 0 {
				return template.Spec.SupportedModes
			}
			return []string{"deployment"} // Default mode
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&catalogv1alpha1.ImpulseTemplate{}).
		WithOptions(options).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("impulsetemplate").
		Complete(r)
}

// Helper functions

func getImpulseCategory(hints *catalogv1alpha1.UIHints) string {
	if hints != nil && hints.Category != "" {
		return hints.Category
	}
	return "uncategorized"
}
