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
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/patch"
)

// EngramTemplateReconciler reconciles an EngramTemplate object
type EngramTemplateReconciler struct {
	config.ControllerDependencies
}

//+kubebuilder:rbac:groups=catalog.bubustack.io,resources=engramtemplates,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=catalog.bubustack.io,resources=engramtemplates/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=catalog.bubustack.io,resources=engramtemplates/finalizers,verbs=update

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
	templateLogger := rl.WithValues("template", template.Name, "version", template.Spec.Version)
	rl.ReconcileStart("Processing EngramTemplate")

	// Validate required fields
	if template.Spec.Image == "" {
		r.updateErrorStatus(&template, "image is required")
		rl.ReconcileError(fmt.Errorf("image missing"), "Image is required for EngramTemplate")
		if err := r.updateStatus(ctx, &template); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		return ctrl.Result{}, nil
	}

	if template.Spec.Version == "" {
		r.updateErrorStatus(&template, "version is required")
		rl.ReconcileError(fmt.Errorf("version missing"), "Version is required for EngramTemplate")
		if err := r.updateStatus(ctx, &template); err != nil {
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
			if updateErr := r.updateStatus(ctx, &template); updateErr != nil {
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
			if updateErr := r.updateStatus(ctx, &template); updateErr != nil {
				templateLogger.Error(updateErr, "failed to update status after schema validation error")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		templateLogger.V(1).Info("Output schema validated")
	}

	if template.Spec.ConfigSchema != nil {
		if err := r.validateJSONSchema(template.Spec.ConfigSchema.Raw); err != nil {
			r.updateErrorStatus(&template, fmt.Sprintf("invalid config schema: %v", err))
			rl.ReconcileError(err, "Invalid config schema")
			if updateErr := r.updateStatus(ctx, &template); updateErr != nil {
				templateLogger.Error(updateErr, "failed to update status after schema validation error")
			}
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		templateLogger.V(1).Info("Config schema validated")
	}

	// Update status to ready
	r.updateReadyStatus(&template)

	// List all Engrams that were created from this template
	var engrams bubushv1alpha1.EngramList
	if err := r.List(ctx, &engrams, client.MatchingFields{"spec.templateRef.name": req.Name}); err != nil {
		templateLogger.Error(err, "Failed to list engrams for template")
		// We don't fail the reconcile, just log the error. Status will be updated on next reconcile.
	} else {
		template.Status.UsageCount = int32(len(engrams.Items))
	}

	if err := r.updateStatus(ctx, &template); err != nil {
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
	cm := conditions.NewConditionManager(template.Generation)
	cm.SetCondition(&template.Status.Conditions, conditions.ConditionReady, metav1.ConditionFalse, conditions.ReasonValidationFailed, message)
}

// updateReadyStatus updates the template status to ready
func (r *EngramTemplateReconciler) updateReadyStatus(template *catalogv1alpha1.EngramTemplate) {
	cm := conditions.NewConditionManager(template.Generation)
	cm.SetCondition(&template.Status.Conditions, conditions.ConditionReady, metav1.ConditionTrue, conditions.ReasonValidationPassed, "EngramTemplate is valid and ready for use")
}

func (r *EngramTemplateReconciler) updateStatus(ctx context.Context, template *catalogv1alpha1.EngramTemplate) error {
	// A more robust implementation would use a merge patch.
	return patch.RetryableStatusPatch(ctx, r.Client, template, func(obj client.Object) {
		t := obj.(*catalogv1alpha1.EngramTemplate)
		// This is a full replacement. A safer method would be to merge fields.
		t.Status.Conditions = template.Status.Conditions
		t.Status.UsageCount = template.Status.UsageCount
	})
}

// SetupWithManager sets up the controller with the Manager.
func (r *EngramTemplateReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&catalogv1alpha1.EngramTemplate{}).
		WithOptions(opts).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("catalog-engramtemplate").
		Complete(r)
}
