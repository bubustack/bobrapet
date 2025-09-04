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

// EngramTemplateReconciler reconciles an EngramTemplate object
type EngramTemplateReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	conditionManager *ConditionManager
	// reconcilePattern removed
}

//+kubebuilder:rbac:groups=catalog.bubu.sh,resources=engramtemplates,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=catalog.bubu.sh,resources=engramtemplates/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=catalog.bubu.sh,resources=engramtemplates/finalizers,verbs=update

// Reconcile validates and manages EngramTemplate lifecycle
func (r *EngramTemplateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Initialize structured logging and metrics
	rl := logging.NewReconcileLogger(ctx, "engramtemplate")
	startTime := time.Now()

	defer func() {
		duration := time.Since(startTime)
		metrics.RecordControllerReconcile("engramtemplate", duration, nil)
	}()

	var template catalogv1alpha1.EngramTemplate
	if err := r.Get(ctx, req.NamespacedName, &template); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Add template context to logger
	templateLogger := rl.WithValues("template", template.Name, "category", getCategory(template.Spec.UIHints), "version", template.Spec.Version)
	rl.ReconcileStart("Processing EngramTemplate")

	// Initialize managers if needed
	if r.conditionManager == nil {
		r.conditionManager = NewConditionManager(template.Generation)
	}
	// ReconcilePattern removed - using direct reconcile logic

	// Validate required fields
	if template.Spec.Image == "" {
		conditions := &template.Status.Conditions
		r.conditionManager.SetReadyCondition(conditions, false, ReasonValidationFailed, "image is required")
		rl.ReconcileError(fmt.Errorf("image missing"), "Image is required for EngramTemplate")
		if err := r.updateStatusWithRetry(ctx, &template, 3); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	if template.Spec.Version == "" {
		r.updateErrorStatus(&template, "version is required")
		rl.ReconcileError(fmt.Errorf("version missing"), "Version is required for EngramTemplate")
		if err := r.updateStatusWithRetry(ctx, &template, 3); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	templateLogger.Info("Validating EngramTemplate", "image", template.Spec.Image, "version", template.Spec.Version)

	// Validate JSON schemas if provided
	if template.Spec.InputSchema != nil {
		if err := r.validateJSONSchema(template.Spec.InputSchema.Raw); err != nil {
			r.updateErrorStatus(&template, fmt.Sprintf("invalid input schema: %v", err))
			rl.ReconcileError(err, "Invalid input schema")
			if updateErr := r.updateStatusWithRetry(ctx, &template, 3); updateErr != nil {
				templateLogger.Error(updateErr, "failed to update status after schema validation error")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		templateLogger.V(1).Info("Input schema validated")
	}

	if template.Spec.OutputSchema != nil {
		if err := r.validateJSONSchema(template.Spec.OutputSchema.Raw); err != nil {
			r.updateErrorStatus(&template, fmt.Sprintf("invalid output schema: %v", err))
			rl.ReconcileError(err, "Invalid output schema")
			if updateErr := r.updateStatusWithRetry(ctx, &template, 3); updateErr != nil {
				templateLogger.Error(updateErr, "failed to update status after schema validation error")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		templateLogger.V(1).Info("Output schema validated")
	}

	// Update status to ready
	r.updateReadyStatus(&template)
	if err := r.updateStatusWithRetry(ctx, &template, 3); err != nil {
		rl.ReconcileError(err, "Failed to update EngramTemplate status")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}

	rl.ReconcileSuccess("EngramTemplate validated successfully",
		"image", template.Spec.Image,
		"version", template.Spec.Version,
		"has_input_schema", template.Spec.InputSchema != nil,
		"has_output_schema", template.Spec.OutputSchema != nil)
	return ctrl.Result{}, nil
}

// validateJSONSchema performs basic JSON schema validation
func (r *EngramTemplateReconciler) validateJSONSchema(schemaBytes []byte) error {
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
func (r *EngramTemplateReconciler) updateErrorStatus(template *catalogv1alpha1.EngramTemplate, message string) {
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
func (r *EngramTemplateReconciler) updateReadyStatus(template *catalogv1alpha1.EngramTemplate) {
	now := metav1.Now()
	condition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "Valid",
		Message:            "EngramTemplate is valid and ready for use",
		LastTransitionTime: now,
		ObservedGeneration: template.Generation,
	}
	r.setCondition(&template.Status.Conditions, condition)
}

// setCondition updates or appends a condition to the conditions slice
func (r *EngramTemplateReconciler) setCondition(conditions *[]metav1.Condition, newCondition metav1.Condition) {
	for i, condition := range *conditions {
		if condition.Type == newCondition.Type {
			(*conditions)[i] = newCondition
			return
		}
	}
	*conditions = append(*conditions, newCondition)
}

// updateStatusWithRetry performs status update with retry logic
func (r *EngramTemplateReconciler) updateStatusWithRetry(ctx context.Context, template *catalogv1alpha1.EngramTemplate, maxRetries int) error {
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
	return fmt.Errorf("failed to update EngramTemplate status after %d retries: %w", maxRetries, lastErr)
}

// SetupWithManager sets up the controller with the Manager.
func (r *EngramTemplateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return r.SetupWithManagerAndConfig(mgr, controller.Options{MaxConcurrentReconciles: 3})
}

// SetupWithManagerAndConfig sets up the controller with the Manager using configurable options.
func (r *EngramTemplateReconciler) SetupWithManagerAndConfig(mgr ctrl.Manager, options controller.Options) error {
	return r.setupControllerWithOptions(mgr, options)
}

// setupControllerWithOptions configures the controller with specific options
func (r *EngramTemplateReconciler) setupControllerWithOptions(mgr ctrl.Manager, options controller.Options) error {
	ctx := context.Background()

	// Index EngramTemplates by category for efficient lookups
	if err := mgr.GetFieldIndexer().IndexField(ctx, &catalogv1alpha1.EngramTemplate{}, "spec.uiHints.category",
		func(obj client.Object) []string {
			template := obj.(*catalogv1alpha1.EngramTemplate)
			if template.Spec.UIHints != nil && template.Spec.UIHints.Category != "" {
				return []string{template.Spec.UIHints.Category}
			}
			return []string{"uncategorized"}
		}); err != nil {
		return err
	}

	// Index by version for version-based queries
	if err := mgr.GetFieldIndexer().IndexField(ctx, &catalogv1alpha1.EngramTemplate{}, "spec.version",
		func(obj client.Object) []string {
			return []string{obj.(*catalogv1alpha1.EngramTemplate).Spec.Version}
		}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&catalogv1alpha1.EngramTemplate{}).
		WithOptions(options).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("engramtemplate").
		Complete(r)
}

// Helper functions

func getCategory(hints *catalogv1alpha1.UIHints) string {
	if hints != nil && hints.Category != "" {
		return hints.Category
	}
	return "uncategorized"
}
