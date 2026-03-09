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

package v1alpha1

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bobrapet/pkg/templatesafety"
	"github.com/bubustack/bobrapet/pkg/validation"
	"github.com/bubustack/core/templating"
)

// log is for logging in this package.
var storylog = logf.Log.WithName("story-resource")

// StoryWebhook sets up the webhook for Story in the manager.
type StoryWebhook struct {
	client.Client
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

// SetupWebhookWithManager registers the webhook for Story in the manager.
//
// Behavior:
//   - Stores the manager's client for Engram/Template lookups during validation.
//   - Creates both defaulter and validator webhooks for Story resources.
//   - Injects Config and ConfigManager into both handlers.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//
// Returns:
//   - nil on success.
//   - Error if webhook registration fails.
func (wh *StoryWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	wh.Client = mgr.GetClient()

	return ctrl.NewWebhookManagedBy(mgr).For(&bubushv1alpha1.Story{}).
		WithValidator(&StoryCustomValidator{
			Client:        wh.Client,
			Config:        wh.Config,
			ConfigManager: wh.ConfigManager,
		}).
		WithDefaulter(&StoryCustomDefaulter{
			Config:        wh.Config,
			ConfigManager: wh.ConfigManager,
		}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-bubustack-io-v1alpha1-story,mutating=true,failurePolicy=fail,sideEffects=None,groups=bubustack.io,resources=stories,verbs=create;update,versions=v1alpha1,name=mstory-v1alpha1.kb.io,admissionReviewVersions=v1

// StoryCustomDefaulter struct is responsible for setting default values on the custom resource of the
// Kind Story when those are created or updated.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as it is used only for temporary operations and does not need to be deeply copied.
type StoryCustomDefaulter struct {
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

var _ webhook.CustomDefaulter = &StoryCustomDefaulter{}

// controllerConfig returns the effective controller configuration.
//
// Behavior:
//   - Delegates to ResolveControllerConfig for unified config resolution.
//
// Returns:
//   - Non-nil *config.ControllerConfig from the highest priority source.
func (d *StoryCustomDefaulter) controllerConfig() *config.ControllerConfig {
	return ResolveControllerConfig(storylog, d.ConfigManager, d.Config)
}

// Default implements webhook.CustomDefaulter for Story resources.
//
// Behavior:
//   - Resolves RetryPolicy for Story-level and step-level retries.
//   - Sets MaxRetries, Delay, and Backoff if not specified.
//
// Arguments:
//   - ctx context.Context: unused but required by interface.
//   - obj runtime.Object: expected to be *bubushv1alpha1.Story.
//
// Returns:
//   - nil on success.
//   - Error if obj is not a Story.
func (d *StoryCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	return DefaultResource[*bubushv1alpha1.Story](obj, "Story", storylog, func(story *bubushv1alpha1.Story) error {
		cfg := d.controllerConfig()
		if story.Spec.Policy != nil {
			if story.Spec.Policy.Retries == nil {
				story.Spec.Policy.Retries = &bubushv1alpha1.StoryRetries{}
			}
			story.Spec.Policy.Retries.StepRetryPolicy = ResolveRetryPolicy(cfg, story.Spec.Policy.Retries.StepRetryPolicy)
			if story.Spec.Policy.Retries.ContinueOnStepFailure == nil {
				continueOnFailure := false
				story.Spec.Policy.Retries.ContinueOnStepFailure = &continueOnFailure
			}
		}
		for i := range story.Spec.Steps {
			// Default maxRetries=0 for side-effect steps without explicit retry policy
			step := &story.Spec.Steps[i]
			if step.SideEffects != nil && *step.SideEffects {
				if step.Execution == nil {
					step.Execution = &bubushv1alpha1.ExecutionOverrides{}
				}
				if step.Execution.Retry == nil {
					zero := int32(0)
					step.Execution.Retry = &bubushv1alpha1.RetryPolicy{
						MaxRetries: &zero,
					}
				}
			}
			if story.Spec.Steps[i].Execution != nil {
				story.Spec.Steps[i].Execution.Retry = ResolveRetryPolicy(cfg, story.Spec.Steps[i].Execution.Retry)
			}
		}

		// Collect template step reference warnings
		reachability := buildReachabilityMap(story.Spec.Steps)
		allStepNames := collectStepNames(story.Spec.Steps)
		var warnings []string
		for i := range story.Spec.Steps {
			step := &story.Spec.Steps[i]
			warnings = append(warnings, validateStepTemplateRefs(step, allStepNames, reachability[step.Name])...)
		}
		story.Status.ValidationWarnings = warnings

		return nil
	})
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-bubustack-io-v1alpha1-story,mutating=false,failurePolicy=fail,sideEffects=None,groups=bubustack.io,resources=stories,verbs=create;update,versions=v1alpha1,name=vstory-v1alpha1.kb.io,admissionReviewVersions=v1

// StoryCustomValidator struct is responsible for validating the Story resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type StoryCustomValidator struct {
	Client        client.Client
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

var _ webhook.CustomValidator = &StoryCustomValidator{}

// controllerConfig returns the effective controller configuration.
//
// Behavior:
//   - Delegates to ResolveControllerConfig for unified config resolution.
//
// Returns:
//   - Non-nil *config.ControllerConfig from the highest priority source.
func (v *StoryCustomValidator) controllerConfig() *config.ControllerConfig {
	return ResolveControllerConfig(storylog, v.ConfigManager, v.Config)
}

// ValidateCreate implements webhook.CustomValidator for Story creation.
//
// Behavior:
//   - Type-asserts obj to Story and validates spec.
//   - Uses 5-second timeout for template/Engram lookups.
//   - Validates size limits, steps shape, graph acyclicity, and transports.
//
// Arguments:
//   - ctx context.Context: for API lookups.
//   - obj runtime.Object: expected to be *bubushv1alpha1.Story.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, error if type assertion fails or validation errors exist.
func (v *StoryCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	story, ok := obj.(*bubushv1alpha1.Story)
	if !ok {
		return nil, fmt.Errorf("expected a Story object but got %T", obj)
	}
	storylog.Info("Validation for Story upon creation", "name", story.GetName())
	webhookCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return v.validateStory(webhookCtx, story)
}

// ValidateUpdate implements webhook.CustomValidator for Story updates.
//
// Behavior:
//   - Skips validation if resource is being deleted (DeletionTimestamp set).
//   - Skips validation if spec is unchanged (metadata-only update).
//   - Uses 5-second timeout for template/Engram lookups.
//
// Arguments:
//   - ctx context.Context: for API lookups.
//   - oldObj runtime.Object: previous Story state.
//   - newObj runtime.Object: proposed Story state.
//
// Returns:
//   - nil, nil if validation passes or is skipped.
//   - nil, error if type assertion fails or validation errors exist.
func (v *StoryCustomValidator) ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) (admission.Warnings, error) {
	story, ok := newObj.(*bubushv1alpha1.Story)
	if !ok {
		return nil, fmt.Errorf("expected a Story object for the newObj but got %T", newObj)
	}
	storylog.Info("Validation for Story upon update", "name", story.GetName())

	// Allow metadata-only updates during deletion (e.g., finalizer removal)
	if story.DeletionTimestamp != nil {
		return nil, nil
	}

	// Skip validation if the spec hasn't changed
	if oldStory, ok := oldObj.(*bubushv1alpha1.Story); ok {
		if reflect.DeepEqual(oldStory.Spec, story.Spec) {
			return nil, nil
		}
	}

	webhookCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return v.validateStory(webhookCtx, story)
}

// ValidateDelete implements webhook.CustomValidator for Story deletion.
//
// Behavior:
//   - Always allows deletion (no-op validation).
//   - Exists as scaffold placeholder; delete verb not enabled in annotation.
//
// Arguments:
//   - ctx context.Context: unused.
//   - obj runtime.Object: expected to be *bubushv1alpha1.Story.
//
// Returns:
//   - nil, nil (deletion always allowed).
func (v *StoryCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	return ValidateDeleteResource[*bubushv1alpha1.Story](obj, "Story", storylog, nil)
}

// validateStory performs basic invariants validation for Story specs.
//
// Behavior:
//   - Validates total Story size (< 1 MiB).
//   - Validates output and with block sizes against config limits.
//   - Validates step uniqueness, ref/type exclusivity, and needs references.
//   - Validates executeStory references point to existing Stories.
//   - Validates step graph is acyclic.
//   - Validates transport definitions and step transport references.
//   - Aggregates multiple validation errors for comprehensive feedback.
//
// Arguments:
//   - ctx context.Context: for Engram/Template lookups.
//   - story *bubushv1alpha1.Story: the Story to validate.
//
// Returns:
//   - nil if all validations pass.
//   - Aggregated field.ErrorList containing all validation failures.
func (v *StoryCustomValidator) validateStory(ctx context.Context, story *bubushv1alpha1.Story) (admission.Warnings, error) {
	agg := validation.NewAggregator()
	var warnings admission.Warnings

	// Story-level size validation (fail-fast on critical errors)
	if err := validateStorySize(story); err != nil {
		return nil, err
	}

	cfg := v.controllerConfig()
	maxSize := pickStoryMaxWithSize(cfg)
	if err := validateOutputSize(story, maxSize); err != nil {
		agg.AddFieldError("spec.output", conditions.ReasonValidationFailed, err.Error())
	}

	// Step validation uses aggregator to collect all step errors
	warnings = append(warnings, validateStepsShapeAggregated(ctx, v, story, maxSize, agg)...)

	if err := validateStoryOutputExpressions(story); err != nil {
		agg.AddFieldError("spec.output", conditions.ReasonValidationFailed, err.Error())
	}
	validateStoryPolicyTimeouts(story, agg)
	if err := validateStoryPolicyWith(story, maxSize); err != nil {
		agg.AddFieldError("spec.policy.with", conditions.ReasonValidationFailed, err.Error())
	}
	if story.Spec.InputsSchema != nil && len(story.Spec.InputsSchema.Raw) > 0 {
		if err := ValidateJSONSchemaDefinition(story.Spec.InputsSchema.Raw, "Story inputs"); err != nil {
			agg.AddFieldError("spec.inputsSchema", conditions.ReasonValidationFailed, err.Error())
		}
	}
	if story.Spec.OutputsSchema != nil && len(story.Spec.OutputsSchema.Raw) > 0 {
		if err := ValidateJSONSchemaDefinition(story.Spec.OutputsSchema.Raw, "Story outputs"); err != nil {
			agg.AddFieldError("spec.outputsSchema", conditions.ReasonValidationFailed, err.Error())
		}
	}

	mainNames := collectStepNames(story.Spec.Steps)
	compNames := collectStepNames(story.Spec.Compensations)
	finallyNames := collectStepNames(story.Spec.Finally)
	compAllowed := mergeStepNameSets(mainNames, compNames)
	finallyAllowed := mergeStepNameSets(mainNames, compNames, finallyNames)

	// Compensations and finally blocks are allowed for both batch and streaming stories.
	// For streaming stories, cleanup steps execute as batch jobs after the streaming
	// topology terminates (when the StoryRun's main steps reach a terminal phase).

	if err := validateNeedsExistence(story.Spec.Steps, mainNames); err != nil {
		agg.AddFieldError("spec.steps", conditions.ReasonValidationFailed, err.Error())
	}
	if err := validateNeedsExistence(story.Spec.Compensations, compAllowed); err != nil {
		agg.AddFieldError("spec.compensations", conditions.ReasonValidationFailed, err.Error())
	}
	if err := validateNeedsExistence(story.Spec.Finally, finallyAllowed); err != nil {
		agg.AddFieldError("spec.finally", conditions.ReasonValidationFailed, err.Error())
	}

	if err := validateStepGraphAcyclic("steps", story.Spec.Steps); err != nil {
		agg.AddFieldError("spec.steps", conditions.ReasonValidationFailed, err.Error())
	}
	if err := validateStepGraphAcyclic("compensations", story.Spec.Compensations); err != nil {
		agg.AddFieldError("spec.compensations", conditions.ReasonValidationFailed, err.Error())
	}
	if err := validateStepGraphAcyclic("finally", story.Spec.Finally); err != nil {
		agg.AddFieldError("spec.finally", conditions.ReasonValidationFailed, err.Error())
	}

	// Validate requires paths reference known upstream steps.
	validateRequiresPathsForSlice(story.Spec.Steps, "spec.steps", mainNames, agg)
	validateRequiresPathsForSlice(story.Spec.Compensations, "spec.compensations", compAllowed, agg)
	validateRequiresPathsForSlice(story.Spec.Finally, "spec.finally", finallyAllowed, agg)

	transportByName, err := validateStoryTransportsSpec(story)
	if err != nil {
		agg.AddFieldError("spec.transports", conditions.ReasonValidationFailed, err.Error())
	}

	if err := validateStepTransportRefs(story.Spec.Steps, transportByName); err != nil {
		agg.AddFieldError("spec.steps", conditions.ReasonTransportReferenceInvalid, err.Error())
	}
	if err := validateStepTransportRefs(story.Spec.Compensations, transportByName); err != nil {
		agg.AddFieldError("spec.compensations", conditions.ReasonTransportReferenceInvalid, err.Error())
	}
	if err := validateStepTransportRefs(story.Spec.Finally, transportByName); err != nil {
		agg.AddFieldError("spec.finally", conditions.ReasonTransportReferenceInvalid, err.Error())
	}
	validateStoryTransportRoutingRules(story, agg)
	validateStoryTransportStreaming(story, agg)

	if w, err := v.validateExecuteStoryReferences(ctx, story, story.Spec.Steps); err != nil {
		agg.AddFieldError("spec.steps", conditions.ReasonStoryReferenceInvalid, err.Error())
	} else {
		warnings = append(warnings, w...)
	}
	if w, err := v.validateExecuteStoryReferences(ctx, story, story.Spec.Compensations); err != nil {
		agg.AddFieldError("spec.compensations", conditions.ReasonStoryReferenceInvalid, err.Error())
	} else {
		warnings = append(warnings, w...)
	}
	if w, err := v.validateExecuteStoryReferences(ctx, story, story.Spec.Finally); err != nil {
		agg.AddFieldError("spec.finally", conditions.ReasonStoryReferenceInvalid, err.Error())
	} else {
		warnings = append(warnings, w...)
	}

	if agg.HasErrors() {
		return warnings, agg.ToFieldErrors().ToAggregate()
	}
	return warnings, nil
}

// validateStorySize ensures the total serialized Story size doesn't exceed 1 MiB.
//
// Behavior:
//   - Marshals the Story to JSON to calculate total size.
//   - Rejects Stories larger than 1 MiB to prevent etcd storage issues.
//
// Arguments:
//   - story *bubushv1alpha1.Story: the Story to validate.
//
// Returns:
//   - nil if size is within limits.
//   - Error describing the size violation.
func validateStorySize(story *bubushv1alpha1.Story) error {
	rawStory, err := json.Marshal(story)
	if err != nil {
		return fmt.Errorf("internal error: failed to marshal story for size validation: %w", err)
	}
	const maxTotalStorySizeBytes = 1 * 1024 * 1024 // 1 MiB
	if len(rawStory) > maxTotalStorySizeBytes {
		return fmt.Errorf("story size of %d bytes exceeds maximum allowed size of %d bytes", len(rawStory), maxTotalStorySizeBytes)
	}
	return nil
}

// pickStoryMaxWithSize returns the maximum allowed size for with/output blocks.
//
// Behavior:
//   - Uses cfg.MaxStoryWithBlockSizeBytes if set.
//   - Falls back to DefaultControllerConfig() if cfg is nil or value is unset.
//   - Logs at V(1) when using fallback for observability.
//
// Arguments:
//   - cfg *config.ControllerConfig: may be nil.
//
// Returns:
//   - Maximum byte size for with/output blocks.
func pickStoryMaxWithSize(cfg *config.ControllerConfig) int {
	if cfg == nil {
		storylog.V(1).Info("pickStoryMaxWithSize: config nil, using defaults")
		cfg = config.DefaultControllerConfig()
	}
	maxSize := cfg.MaxStoryWithBlockSizeBytes
	if maxSize <= 0 {
		maxSize = config.DefaultControllerConfig().MaxStoryWithBlockSizeBytes
	}
	return maxSize
}

// validateOutputSize ensures the Story's output block doesn't exceed maxSize bytes.
//
// Arguments:
//   - story *bubushv1alpha1.Story: the Story to validate.
//   - maxSize int: maximum allowed bytes for the output block.
//
// Returns:
//   - nil if output is within limits or absent.
//   - Error describing the size violation.
func validateOutputSize(story *bubushv1alpha1.Story, maxSize int) error {
	if story.Spec.Output != nil && len(story.Spec.Output.Raw) > maxSize {
		return fmt.Errorf("story 'output' block size of %d bytes exceeds maximum allowed size of %d bytes", len(story.Spec.Output.Raw), maxSize)
	}
	return nil
}

// engramCache caches Engrams by their namespaced key to avoid repeated API calls
// when multiple steps reference the same Engram.
type engramCache map[types.NamespacedName]*bubushv1alpha1.Engram

// templateCache caches EngramTemplates by name (cluster-scoped) to avoid repeated
// API calls when multiple Engrams use the same template.
type templateCache map[string]*v1alpha1.EngramTemplate

// validateStepsShapeAggregated validates each step in the Story, collecting all errors.
//
// Behavior:
//   - Ensures step names are unique.
//   - Ensures each step has either ref or type, but not both.
//   - Rejects self-dependency in needs.
//   - Validates with block sizes against maxSize.
//   - For ref steps, validates against Engram/EngramTemplate using caches.
//   - Collects all errors into the aggregator instead of failing on first.
//
// Arguments:
//   - ctx context.Context: for API calls and cancellation.
//   - v *StoryCustomValidator: provides client for Engram/Template lookups.
//   - story *bubushv1alpha1.Story: the Story to validate.
//   - maxSize int: maximum allowed bytes for with blocks.
//   - agg *validation.Aggregator: collects all validation errors.
//
// Side Effects:
//   - Adds validation errors to agg.
func validateStepsShapeAggregated(ctx context.Context, v *StoryCustomValidator, story *bubushv1alpha1.Story, maxSize int, agg *validation.Aggregator) admission.Warnings {
	validator := stepShapeValidator{
		seen:      make(map[string]struct{}),
		engrams:   make(engramCache),
		templates: make(templateCache),
	}

	validateStepSlice(ctx, v, story, story.Spec.Steps, "spec.steps", maxSize, agg, &validator)
	validateStepSlice(ctx, v, story, story.Spec.Compensations, "spec.compensations", maxSize, agg, &validator)
	validateStepSlice(ctx, v, story, story.Spec.Finally, "spec.finally", maxSize, agg, &validator)
	return validator.warnings
}

func validateStepSlice(ctx context.Context, v *StoryCustomValidator, story *bubushv1alpha1.Story, steps []bubushv1alpha1.Step, field string, maxSize int, agg *validation.Aggregator, validator *stepShapeValidator) {
	if validator == nil {
		return
	}
	for i := range steps {
		s := &steps[i]
		stepPath := fmt.Sprintf("%s[%d]", field, i)
		validator.validateStep(ctx, v, story, s, stepPath, maxSize, agg)
	}
}

type stepShapeValidator struct {
	seen      map[string]struct{}
	engrams   engramCache
	templates templateCache
	warnings  admission.Warnings
}

func (sv *stepShapeValidator) validateStep(ctx context.Context, v *StoryCustomValidator, story *bubushv1alpha1.Story, step *bubushv1alpha1.Step, stepPath string, maxSize int, agg *validation.Aggregator) {
	sv.checkName(step, stepPath, agg)
	sv.checkTypeCombination(step, stepPath, agg)
	sv.checkNeeds(step, stepPath, agg)
	sv.checkBatchOnly(story, step, stepPath, agg)
	sv.checkRef(ctx, v, story, step, stepPath, agg)
	sv.checkWithSize(step, stepPath, maxSize, agg)
	sv.checkJSONObjects(step, stepPath, agg)
	sv.checkPrimitive(step, stepPath, agg)
	sv.checkExpressions(story, step, stepPath, agg)
}

func (sv *stepShapeValidator) checkName(step *bubushv1alpha1.Step, stepPath string, agg *validation.Aggregator) {
	if _, exists := sv.seen[step.Name]; exists {
		agg.AddFieldError(stepPath+".name", conditions.ReasonValidationFailed, fmt.Sprintf("duplicate step name '%s'", step.Name))
	}
	sv.seen[step.Name] = struct{}{}
}

func (sv *stepShapeValidator) checkTypeCombination(step *bubushv1alpha1.Step, stepPath string, agg *validation.Aggregator) {
	if step.Ref != nil && step.Type != "" {
		agg.AddFieldError(stepPath, conditions.ReasonValidationFailed, fmt.Sprintf("step '%s' must not set both ref and type", step.Name))
	}
	if step.Ref == nil && step.Type == "" {
		agg.AddFieldError(stepPath, conditions.ReasonValidationFailed, fmt.Sprintf("step '%s' must set either ref or type", step.Name))
	}
}

func (sv *stepShapeValidator) checkNeeds(step *bubushv1alpha1.Step, stepPath string, agg *validation.Aggregator) {
	for _, dep := range step.Needs {
		if dep == step.Name {
			agg.AddFieldError(stepPath+".needs", conditions.ReasonValidationFailed, fmt.Sprintf("step '%s' cannot depend on itself in needs", step.Name))
		}
	}
}

func (sv *stepShapeValidator) checkBatchOnly(story *bubushv1alpha1.Story, step *bubushv1alpha1.Step, stepPath string, agg *validation.Aggregator) {
	if story == nil || step == nil {
		return
	}
	if story.Spec.Pattern != enums.StreamingPattern {
		return
	}
	switch step.Type {
	case enums.StepTypeGate, enums.StepTypeWait:
		agg.AddFieldError(stepPath+".type", conditions.ReasonValidationFailed, fmt.Sprintf("step '%s' of type '%s' is only supported for batch stories", step.Name, step.Type))
	}
}

func (sv *stepShapeValidator) checkRef(ctx context.Context, v *StoryCustomValidator, story *bubushv1alpha1.Story, step *bubushv1alpha1.Step, stepPath string, agg *validation.Aggregator) {
	if step.Ref == nil {
		return
	}
	warns, err := v.validateEngramStepCached(ctx, story, step, sv.engrams, sv.templates)
	sv.warnings = append(sv.warnings, warns...)
	if err != nil {
		agg.AddFieldError(stepPath+".ref", conditions.ReasonEngramReferenceInvalid, err.Error())
	}
}

func (sv *stepShapeValidator) checkWithSize(step *bubushv1alpha1.Step, stepPath string, maxSize int, agg *validation.Aggregator) {
	if step.With == nil {
		return
	}
	if len(step.With.Raw) > maxSize {
		agg.AddFieldError(stepPath+".with", conditions.ReasonValidationFailed, fmt.Sprintf("'with' block size of %d bytes exceeds maximum allowed size of %d bytes", len(step.With.Raw), maxSize))
	}
}

func (sv *stepShapeValidator) checkJSONObjects(step *bubushv1alpha1.Step, stepPath string, agg *validation.Aggregator) {
	if step == nil {
		return
	}
	if step.With != nil && len(step.With.Raw) > 0 {
		trimmed := TrimLeadingSpace(step.With.Raw)
		if err := EnsureJSONObject(stepPath+".with", trimmed); err != nil {
			agg.AddFieldError(stepPath+".with", conditions.ReasonValidationFailed, err.Error())
		}
	}
	if step.Runtime != nil && len(step.Runtime.Raw) > 0 {
		trimmed := TrimLeadingSpace(step.Runtime.Raw)
		if err := EnsureJSONObject(stepPath+".runtime", trimmed); err != nil {
			agg.AddFieldError(stepPath+".runtime", conditions.ReasonValidationFailed, err.Error())
		}
	}
}

func (sv *stepShapeValidator) checkPrimitive(step *bubushv1alpha1.Step, stepPath string, agg *validation.Aggregator) {
	if err := validatePrimitiveShapes(step); err != nil {
		agg.AddFieldError(stepPath, conditions.ReasonValidationFailed, err.Error())
	}
}

func (sv *stepShapeValidator) checkExpressions(story *bubushv1alpha1.Story, step *bubushv1alpha1.Step, stepPath string, agg *validation.Aggregator) {
	if story == nil || step == nil {
		return
	}
	batchScope := batchRuntimeScope()
	staticScope := streamingStaticScope()
	runtimeScope := streamingRuntimeScope()

	if step.If != nil && strings.TrimSpace(*step.If) != "" {
		scope := batchScope
		if story.Spec.Pattern == enums.StreamingPattern {
			scope = runtimeScope
		}
		if err := templatesafety.ValidateTemplateString(*step.If); err != nil {
			agg.AddFieldError(stepPath+".if", conditions.ReasonValidationFailed, err.Error())
		} else if err := templating.ValidateTemplateString(*step.If, scope); err != nil {
			agg.AddFieldError(stepPath+".if", conditions.ReasonValidationFailed, err.Error())
		}
	}

	if step.With != nil && len(step.With.Raw) > 0 {
		scope := batchScope
		if story.Spec.Pattern == enums.StreamingPattern {
			scope = staticScope
		}
		if err := templatesafety.ValidateTemplateJSON(step.With.Raw); err != nil {
			agg.AddFieldError(stepPath+".with", conditions.ReasonValidationFailed, err.Error())
		} else if err := templating.ValidateJSONTemplates(step.With.Raw, scope); err != nil {
			agg.AddFieldError(stepPath+".with", conditions.ReasonValidationFailed, err.Error())
		}
	}

	if step.Runtime != nil && len(step.Runtime.Raw) > 0 {
		runtimeAllowed := story.Spec.Pattern == enums.StreamingPattern
		var warning string
		if !runtimeAllowed {
			runtimeAllowed, warning = sv.runtimeAllowedForBatchStory(story, step)
		}
		if !runtimeAllowed {
			agg.AddFieldError(stepPath+".runtime", conditions.ReasonValidationFailed, "runtime is only supported for streaming stories or bridged realtime steps")
		} else {
			if warning != "" {
				sv.warnings = append(sv.warnings, warning)
			}
			if err := templatesafety.ValidateTemplateJSON(step.Runtime.Raw); err != nil {
				agg.AddFieldError(stepPath+".runtime", conditions.ReasonValidationFailed, err.Error())
			} else if err := templating.ValidateJSONTemplates(step.Runtime.Raw, runtimeScope); err != nil {
				agg.AddFieldError(stepPath+".runtime", conditions.ReasonValidationFailed, err.Error())
			}
		}
	}

	if step.Type == enums.StepTypeWait && step.With != nil && len(step.With.Raw) > 0 {
		var withConfig struct {
			Until string `json:"until"`
		}
		if err := json.Unmarshal(step.With.Raw, &withConfig); err == nil {
			if strings.TrimSpace(withConfig.Until) != "" {
				if err := templatesafety.ValidateTemplateString(withConfig.Until); err != nil {
					agg.AddFieldError(stepPath+".with.until", conditions.ReasonValidationFailed, err.Error())
				} else if err := templating.ValidateTemplateString(withConfig.Until, batchScope); err != nil {
					agg.AddFieldError(stepPath+".with.until", conditions.ReasonValidationFailed, err.Error())
				}
			}
		}
	}
}

func (sv *stepShapeValidator) runtimeAllowedForBatchStory(story *bubushv1alpha1.Story, step *bubushv1alpha1.Step) (bool, string) {
	if story == nil || step == nil {
		return false, ""
	}
	if strings.TrimSpace(step.Transport) != "" {
		return true, ""
	}
	if step.Ref == nil {
		return false, ""
	}
	targetNamespace := refs.ResolveNamespace(story, &step.Ref.ObjectReference)
	engramKey := types.NamespacedName{Name: step.Ref.Name, Namespace: targetNamespace}
	if engram, ok := sv.engrams[engramKey]; ok {
		mode := engram.Spec.Mode
		if mode == "" {
			mode = enums.WorkloadModeJob
		}
		switch mode {
		case enums.WorkloadModeDeployment, enums.WorkloadModeStatefulSet:
			return true, ""
		default:
			return false, ""
		}
	}
	return true, fmt.Sprintf("step '%s': runtime allowed for batch story because engram '%s/%s' was not found to confirm streaming mode", step.Name, engramKey.Namespace, engramKey.Name)
}

// validatePrimitiveShapes validates type-specific step requirements.
//
// Behavior:
//   - For executeStory steps: requires with block with storyRef.name set.
//   - Other step types: no additional validation (pass-through).
//
// Arguments:
//   - s *bubushv1alpha1.Step: the step to validate.
//
// Returns:
//   - nil if step is valid.
//   - Error describing the validation failure.
func validatePrimitiveShapes(s *bubushv1alpha1.Step) error {
	switch s.Type {
	case enums.StepTypeExecuteStory:
		if s.With == nil {
			return fmt.Errorf("step '%s' of type 'executeStory' requires a 'with' block", s.Name)
		}
		var withConfig struct {
			StoryRef struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace,omitempty"`
				Version   string `json:"version,omitempty"`
			} `json:"storyRef"`
		}
		if err := json.Unmarshal(s.With.Raw, &withConfig); err != nil {
			return fmt.Errorf("step '%s' has an invalid 'with' block for type 'executeStory': %w", s.Name, err)
		}
		if withConfig.StoryRef.Name == "" {
			return fmt.Errorf("step '%s' of type 'executeStory' requires 'with.storyRef.name' to be set", s.Name)
		}
	case enums.StepTypeWait:
		if s.With == nil {
			return fmt.Errorf("step '%s' of type 'wait' requires a 'with' block", s.Name)
		}
		var withConfig struct {
			Until        string `json:"until"`
			Timeout      string `json:"timeout,omitempty"`
			PollInterval string `json:"pollInterval,omitempty"`
			OnTimeout    string `json:"onTimeout,omitempty"`
		}
		if err := json.Unmarshal(s.With.Raw, &withConfig); err != nil {
			return fmt.Errorf("step '%s' has an invalid 'with' block for type 'wait': %w", s.Name, err)
		}
		if strings.TrimSpace(withConfig.Until) == "" {
			return fmt.Errorf("step '%s' of type 'wait' requires 'with.until' to be set", s.Name)
		}
		if withConfig.Timeout != "" {
			if _, err := parsePositiveDuration(withConfig.Timeout); err != nil {
				return fmt.Errorf("step '%s' has invalid wait timeout: %w", s.Name, err)
			}
		}
		if withConfig.PollInterval != "" {
			parsed, err := parsePositiveDuration(withConfig.PollInterval)
			if err != nil {
				return fmt.Errorf("step '%s' has invalid wait pollInterval: %w", s.Name, err)
			}
			if parsed < config.MinRequeueDelay {
				return fmt.Errorf("step '%s' has wait pollInterval shorter than %s", s.Name, config.MinRequeueDelay)
			}
		}
		if err := validateOnTimeout(withConfig.OnTimeout); err != nil {
			return fmt.Errorf("step '%s' has invalid wait onTimeout: %w", s.Name, err)
		}
	case enums.StepTypeGate:
		if s.With == nil {
			return nil
		}
		var withConfig struct {
			Timeout      string `json:"timeout,omitempty"`
			PollInterval string `json:"pollInterval,omitempty"`
			OnTimeout    string `json:"onTimeout,omitempty"`
		}
		if err := json.Unmarshal(s.With.Raw, &withConfig); err != nil {
			return fmt.Errorf("step '%s' has an invalid 'with' block for type 'gate': %w", s.Name, err)
		}
		if withConfig.Timeout != "" {
			if _, err := parsePositiveDuration(withConfig.Timeout); err != nil {
				return fmt.Errorf("step '%s' has invalid gate timeout: %w", s.Name, err)
			}
		}
		if withConfig.PollInterval != "" {
			parsed, err := parsePositiveDuration(withConfig.PollInterval)
			if err != nil {
				return fmt.Errorf("step '%s' has invalid gate pollInterval: %w", s.Name, err)
			}
			if parsed < config.MinRequeueDelay {
				return fmt.Errorf("step '%s' has gate pollInterval shorter than %s", s.Name, config.MinRequeueDelay)
			}
		}
		if err := validateOnTimeout(withConfig.OnTimeout); err != nil {
			return fmt.Errorf("step '%s' has invalid gate onTimeout: %w", s.Name, err)
		}
	}
	return nil
}

func batchRuntimeScope() templating.ExpressionScope {
	return templating.NewExpressionScope("batch-runtime", true, true, true, templating.RootInputs, templating.RootSteps)
}

func streamingStaticScope() templating.ExpressionScope {
	return templating.NewExpressionScope("streaming-static", false, false, true, templating.RootInputs)
}

func streamingRuntimeScope() templating.ExpressionScope {
	return templating.NewExpressionScope("streaming-runtime", true, true, true, templating.RootInputs, templating.RootSteps, templating.RootPacket)
}

func storyOutputScope() templating.ExpressionScope {
	return templating.NewExpressionScope("story-output", true, true, true, templating.RootInputs, templating.RootSteps)
}

func validateStoryOutputExpressions(story *bubushv1alpha1.Story) error {
	if story == nil || story.Spec.Output == nil || len(story.Spec.Output.Raw) == 0 {
		return nil
	}
	if err := templatesafety.ValidateTemplateJSON(story.Spec.Output.Raw); err != nil {
		return err
	}
	return templating.ValidateJSONTemplates(story.Spec.Output.Raw, storyOutputScope())
}

func validateStoryPolicyTimeouts(story *bubushv1alpha1.Story, agg *validation.Aggregator) {
	if story == nil || agg == nil || story.Spec.Policy == nil || story.Spec.Policy.Timeouts == nil {
		return
	}
	timeouts := story.Spec.Policy.Timeouts
	if timeouts.Story != nil && strings.TrimSpace(*timeouts.Story) != "" {
		if _, err := parsePositiveDuration(strings.TrimSpace(*timeouts.Story)); err != nil {
			agg.AddFieldError("spec.policy.timeouts.story", conditions.ReasonValidationFailed, err.Error())
		}
	}
	if timeouts.Step != nil && strings.TrimSpace(*timeouts.Step) != "" {
		if _, err := parsePositiveDuration(strings.TrimSpace(*timeouts.Step)); err != nil {
			agg.AddFieldError("spec.policy.timeouts.step", conditions.ReasonValidationFailed, err.Error())
		}
	}
}

func validateStoryPolicyWith(story *bubushv1alpha1.Story, maxSize int) error {
	if story == nil || story.Spec.Policy == nil || story.Spec.Policy.With == nil || len(story.Spec.Policy.With.Raw) == 0 {
		return nil
	}
	trimmed := TrimLeadingSpace(story.Spec.Policy.With.Raw)
	if err := EnsureJSONObject("spec.policy.with", trimmed); err != nil {
		return err
	}
	if len(story.Spec.Policy.With.Raw) > maxSize {
		return fmt.Errorf("story 'policy.with' block size of %d bytes exceeds maximum allowed size of %d bytes", len(story.Spec.Policy.With.Raw), maxSize)
	}
	if err := templatesafety.ValidateTemplateJSON(story.Spec.Policy.With.Raw); err != nil {
		return err
	}
	return templating.ValidateJSONTemplates(story.Spec.Policy.With.Raw, batchRuntimeScope())
}

func parsePositiveDuration(raw string) (time.Duration, error) {
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse duration: %w", err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("duration must be positive")
	}
	return parsed, nil
}

func validateOnTimeout(value string) error {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "fail", "skip":
		return nil
	default:
		return fmt.Errorf("unsupported value %q (expected fail or skip)", value)
	}
}

// validateExecuteStoryReferences validates executeStory step references.
//
// Behavior:
//   - Skips validation if Client is nil (unit test mode).
//   - For each executeStory step, fetches the referenced Story.
//   - Rejects self-references (Story cannot execute itself).
//   - Returns NotFound errors with step context.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - story *bubushv1alpha1.Story: the Story containing executeStory steps.
//   - steps []bubushv1alpha1.Step: the steps to validate.
//
// Returns:
//   - nil if all references are valid.
//   - Error describing the first invalid reference.
func (v *StoryCustomValidator) validateExecuteStoryReferences(ctx context.Context, story *bubushv1alpha1.Story, steps []bubushv1alpha1.Step) (admission.Warnings, error) {
	if v.Client == nil {
		return nil, nil
	}
	cfg := v.controllerConfig()
	var warnings admission.Warnings
	for i := range steps {
		step := &steps[i]
		if step.Type != enums.StepTypeExecuteStory || step.With == nil {
			continue
		}
		var withConfig struct {
			StoryRef struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace,omitempty"`
				Version   string `json:"version,omitempty"`
			} `json:"storyRef"`
		}
		if err := json.Unmarshal(step.With.Raw, &withConfig); err != nil {
			return warnings, fmt.Errorf("step '%s' has an invalid 'with' block for type 'executeStory': %w", step.Name, err)
		}
		targetNamespace := story.Namespace
		if withConfig.StoryRef.Namespace != "" {
			targetNamespace = withConfig.StoryRef.Namespace
		}
		if withConfig.StoryRef.Name == "" {
			continue
		}
		if err := ValidateCrossNamespaceReference(
			ctx,
			v.Client,
			cfg,
			story,
			"bubustack.io",
			"Story",
			"bubustack.io",
			"Story",
			targetNamespace,
			withConfig.StoryRef.Name,
			"StoryRef",
		); err != nil {
			return warnings, fmt.Errorf("step '%s' references Story in namespace '%s': %w", step.Name, targetNamespace, err)
		}
		if targetNamespace == story.Namespace && withConfig.StoryRef.Name == story.Name {
			return warnings, fmt.Errorf("step '%s' of type 'executeStory' cannot reference the same story", step.Name)
		}

		// Story existence is a warning, not a hard error. The referenced Story
		// may be created in the same kubectl apply batch. The controller validates
		// the reference during reconciliation.
		var target bubushv1alpha1.Story
		key := types.NamespacedName{Namespace: targetNamespace, Name: withConfig.StoryRef.Name}
		storylog.V(1).Info("Validating executeStory reference", "step", step.Name, "targetStory", key.String())
		if err := v.Client.Get(ctx, key, &target); err != nil {
			if apierrors.IsNotFound(err) {
				storylog.V(1).Info("Referenced Story not found", "step", step.Name, "targetStory", key.String())
				warnings = append(warnings, fmt.Sprintf("step '%s': referenced Story '%s/%s' not found yet; the controller will retry", step.Name, targetNamespace, withConfig.StoryRef.Name))
				continue
			}
			return warnings, fmt.Errorf("failed to validate executeStory reference for step '%s': %w", step.Name, err)
		}
		if strings.TrimSpace(withConfig.StoryRef.Version) != "" && strings.TrimSpace(target.Spec.Version) != strings.TrimSpace(withConfig.StoryRef.Version) {
			warnings = append(warnings, fmt.Sprintf("step '%s': Story '%s/%s' version mismatch (expected %q, got %q)", step.Name, targetNamespace, withConfig.StoryRef.Name, withConfig.StoryRef.Version, target.Spec.Version))
		}
	}
	return warnings, nil
}

// validateNeedsExistence ensures all step.needs references point to allowed steps.
//
// Behavior:
//   - Checks each step's needs list against the allowed set.
//   - Rejects references to non-existent or disallowed steps.
//
// Arguments:
//   - steps []bubushv1alpha1.Step: steps to validate.
//   - allowed map[string]struct{}: allowed dependency names.
//
// Returns:
//   - nil if all needs references are valid.
//   - Error describing the unknown dependency.
func validateNeedsExistence(steps []bubushv1alpha1.Step, allowed map[string]struct{}) error {
	for i := range steps {
		s := &steps[i]
		for _, dep := range s.Needs {
			if _, ok := allowed[dep]; !ok {
				return fmt.Errorf("step '%s' lists unknown dependency '%s' in needs", s.Name, dep)
			}
		}
	}
	return nil
}

func collectStepNames(steps []bubushv1alpha1.Step) map[string]struct{} {
	names := make(map[string]struct{}, len(steps))
	for i := range steps {
		names[steps[i].Name] = struct{}{}
	}
	return names
}

// validateRequiresPathsForSlice validates requires paths for all steps in a slice.
func validateRequiresPathsForSlice(steps []bubushv1alpha1.Step, field string, allStepNames map[string]struct{}, agg *validation.Aggregator) {
	reachable := buildReachabilityMap(steps)
	for i := range steps {
		s := &steps[i]
		if len(s.Requires) == 0 {
			continue
		}
		stepPath := fmt.Sprintf("%s[%d]", field, i)
		validateRequiresPaths(s, stepPath, allStepNames, reachable[s.Name], agg)
	}
}

// validateRequiresPaths validates that each requires path is well-formed and references
// a step that exists in the DAG and is transitively upstream via needs.
func validateRequiresPaths(step *bubushv1alpha1.Step, stepPath string, allStepNames map[string]struct{}, reachable map[string]struct{}, agg *validation.Aggregator) {
	for i, path := range step.Requires {
		if !strings.HasPrefix(path, "steps.") {
			agg.AddFieldError(fmt.Sprintf("%s.requires[%d]", stepPath, i), conditions.ReasonValidationFailed,
				fmt.Sprintf("requires path must start with 'steps.', got %q", path))
			continue
		}
		parts := strings.SplitN(path, ".", 4) // steps.<name>.output.<key>
		if len(parts) < 3 {
			agg.AddFieldError(fmt.Sprintf("%s.requires[%d]", stepPath, i), conditions.ReasonValidationFailed,
				fmt.Sprintf("requires path must have at least 3 segments (steps.<name>.<field>), got %q", path))
			continue
		}
		stepName := parts[1]
		if _, ok := allStepNames[stepName]; !ok {
			agg.AddFieldError(fmt.Sprintf("%s.requires[%d]", stepPath, i), conditions.ReasonValidationFailed,
				fmt.Sprintf("requires references unknown step %q", stepName))
			continue
		}
		if _, ok := reachable[stepName]; !ok {
			agg.AddFieldError(fmt.Sprintf("%s.requires[%d]", stepPath, i), conditions.ReasonValidationFailed,
				fmt.Sprintf("requires references step %q which is not upstream (add it to needs or needs chain)", stepName))
		}
	}
}

// buildReachabilityMap computes the transitive upstream closure for each step via needs.
func buildReachabilityMap(steps []bubushv1alpha1.Step) map[string]map[string]struct{} {
	direct := make(map[string][]string)
	for _, s := range steps {
		direct[s.Name] = s.Needs
	}
	reachable := make(map[string]map[string]struct{})
	for _, s := range steps {
		visited := make(map[string]struct{})
		queue := make([]string, len(s.Needs))
		copy(queue, s.Needs)
		for len(queue) > 0 {
			curr := queue[0]
			queue = queue[1:]
			if _, ok := visited[curr]; ok {
				continue
			}
			visited[curr] = struct{}{}
			queue = append(queue, direct[curr]...)
		}
		reachable[s.Name] = visited
	}
	return reachable
}

func mergeStepNameSets(sets ...map[string]struct{}) map[string]struct{} {
	merged := make(map[string]struct{})
	for _, set := range sets {
		for name := range set {
			merged[name] = struct{}{}
		}
	}
	return merged
}

// validateStepGraphAcyclic uses Kahn's algorithm to detect dependency cycles.
//
// Behavior:
//   - Builds a dependency graph from step.needs relationships.
//   - Performs topological sort using Kahn's algorithm.
//   - Identifies steps that couldn't be processed (cycle members).
//
// Arguments:
//   - label string: label to include in error messages.
//   - steps []bubushv1alpha1.Step: steps to validate.
//
// Returns:
//   - nil if the graph is acyclic.
//   - Error listing the steps involved in the cycle.
func validateStepGraphAcyclic(label string, steps []bubushv1alpha1.Step) error {
	if len(steps) == 0 {
		return nil
	}

	indegree, edges, err := buildStepDependencyGraph(steps)
	if err != nil {
		return err
	}

	blocked := detectStepGraphCycles(indegree, edges)
	if len(blocked) > 0 {
		sort.Strings(blocked)
		return fmt.Errorf("%s contains a dependency cycle involving step(s): %s", label, strings.Join(blocked, ", "))
	}
	return nil
}

// validateStoryTransportsSpec validates the Story's transport declarations.
//
// Behavior:
//   - Ensures transport names are non-empty and unique.
//   - Ensures each transport has a transportRef.
//   - Builds a map of transport names for step validation.
//
// Arguments:
//   - story *bubushv1alpha1.Story: the Story to validate.
//
// Returns:
//   - Map of valid transport names (for step validation).
//   - Error describing the first validation failure.
func validateStoryTransportsSpec(story *bubushv1alpha1.Story) (map[string]struct{}, error) {
	if len(story.Spec.Transports) == 0 {
		return nil, nil
	}
	names := make(map[string]struct{}, len(story.Spec.Transports))
	for _, decl := range story.Spec.Transports {
		name := strings.TrimSpace(decl.Name)
		if name == "" {
			return nil, fmt.Errorf("story transport entries must define a name")
		}
		if _, exists := names[name]; exists {
			return nil, fmt.Errorf("transport name %q is duplicated; names must be unique per story", name)
		}
		if strings.TrimSpace(decl.TransportRef) == "" {
			return nil, fmt.Errorf("transport '%s' must set transportRef", decl.Name)
		}
		names[name] = struct{}{}
	}
	return names, nil
}

func validateStoryTransportRoutingRules(story *bubushv1alpha1.Story, agg *validation.Aggregator) {
	if story == nil || agg == nil {
		return
	}
	if len(story.Spec.Transports) == 0 {
		return
	}
	allowedSteps := mergeStepNameSets(
		collectStepNames(story.Spec.Steps),
		collectStepNames(story.Spec.Compensations),
		collectStepNames(story.Spec.Finally),
	)
	scope := streamingRuntimeScope()
	for i := range story.Spec.Transports {
		decl := &story.Spec.Transports[i]
		if decl.Streaming == nil || decl.Streaming.Routing == nil || len(decl.Streaming.Routing.Rules) == 0 {
			continue
		}
		for j, rule := range decl.Streaming.Routing.Rules {
			rulePath := fmt.Sprintf("spec.transports[%d].streaming.routing.rules[%d]", i, j)
			if rule.When != nil && strings.TrimSpace(*rule.When) != "" {
				if err := templatesafety.ValidateTemplateString(*rule.When); err != nil {
					agg.AddFieldError(rulePath+".when", conditions.ReasonValidationFailed, err.Error())
				} else if err := templating.ValidateTemplateString(*rule.When, scope); err != nil {
					agg.AddFieldError(rulePath+".when", conditions.ReasonValidationFailed, err.Error())
				}
			}
			if rule.Action != "" && rule.Action != bubushv1alpha1.TransportRoutingRuleAllow && rule.Action != bubushv1alpha1.TransportRoutingRuleDeny {
				agg.AddFieldError(rulePath+".action", conditions.ReasonValidationFailed, fmt.Sprintf("unsupported routing action %q", rule.Action))
			}
			if rule.Target == nil || len(rule.Target.Steps) == 0 {
				continue
			}
			seen := make(map[string]struct{}, len(rule.Target.Steps))
			for k, stepName := range rule.Target.Steps {
				trimmed := strings.TrimSpace(stepName)
				targetPath := fmt.Sprintf("%s.target.steps[%d]", rulePath, k)
				if trimmed == "" {
					agg.AddFieldError(targetPath, conditions.ReasonValidationFailed, "target step name cannot be empty")
					continue
				}
				if _, ok := allowedSteps[trimmed]; !ok {
					agg.AddFieldError(targetPath, conditions.ReasonValidationFailed, fmt.Sprintf("unknown step %q", trimmed))
				}
				if _, ok := seen[trimmed]; ok {
					agg.AddFieldError(targetPath, conditions.ReasonValidationFailed, fmt.Sprintf("duplicate step %q", trimmed))
					continue
				}
				seen[trimmed] = struct{}{}
			}
		}
	}
}

func validateStoryTransportStreaming(story *bubushv1alpha1.Story, agg *validation.Aggregator) {
	if story == nil || agg == nil {
		return
	}
	if len(story.Spec.Transports) == 0 {
		return
	}
	for i := range story.Spec.Transports {
		decl := &story.Spec.Transports[i]
		if decl.Streaming == nil {
			continue
		}
		streaming := decl.Streaming
		streamingPath := fmt.Sprintf("spec.transports[%d].streaming", i)
		if streaming.Backpressure != nil && streaming.Backpressure.Buffer != nil {
			buf := streaming.Backpressure.Buffer
			bufPath := streamingPath + ".backpressure.buffer"
			if buf.MaxMessages != nil && *buf.MaxMessages < 0 {
				agg.AddFieldError(bufPath+".maxMessages", conditions.ReasonValidationFailed, "maxMessages must be non-negative")
			}
			if buf.MaxBytes != nil && *buf.MaxBytes < 0 {
				agg.AddFieldError(bufPath+".maxBytes", conditions.ReasonValidationFailed, "maxBytes must be non-negative")
			}
			if buf.MaxAgeSeconds != nil && *buf.MaxAgeSeconds < 0 {
				agg.AddFieldError(bufPath+".maxAgeSeconds", conditions.ReasonValidationFailed, "maxAgeSeconds must be non-negative")
			}
			if strings.TrimSpace(string(buf.DropPolicy)) != "" &&
				buf.DropPolicy != bubushv1alpha1.BufferDropNewest && buf.DropPolicy != bubushv1alpha1.BufferDropOldest {
				agg.AddFieldError(bufPath+".dropPolicy", conditions.ReasonValidationFailed, "dropPolicy must be drop_newest or drop_oldest")
			}
		}
		if streaming.FlowControl != nil {
			flow := streaming.FlowControl
			flowPath := streamingPath + ".flowControl"
			if strings.TrimSpace(string(flow.Mode)) != "" &&
				flow.Mode != bubushv1alpha1.FlowControlNone && flow.Mode != bubushv1alpha1.FlowControlCredits && flow.Mode != bubushv1alpha1.FlowControlWindow {
				agg.AddFieldError(flowPath+".mode", conditions.ReasonValidationFailed, "mode must be none, credits, or window")
			}
			if flow.InitialCredits != nil {
				if flow.InitialCredits.Messages != nil && *flow.InitialCredits.Messages < 0 {
					agg.AddFieldError(flowPath+".initialCredits.messages", conditions.ReasonValidationFailed, "messages must be non-negative")
				}
				if flow.InitialCredits.Bytes != nil && *flow.InitialCredits.Bytes < 0 {
					agg.AddFieldError(flowPath+".initialCredits.bytes", conditions.ReasonValidationFailed, "bytes must be non-negative")
				}
			}
			if flow.AckEvery != nil {
				if flow.AckEvery.Messages != nil && *flow.AckEvery.Messages < 0 {
					agg.AddFieldError(flowPath+".ackEvery.messages", conditions.ReasonValidationFailed, "messages must be non-negative")
				}
				if flow.AckEvery.Bytes != nil && *flow.AckEvery.Bytes < 0 {
					agg.AddFieldError(flowPath+".ackEvery.bytes", conditions.ReasonValidationFailed, "bytes must be non-negative")
				}
				if flow.AckEvery.MaxDelay != nil && strings.TrimSpace(*flow.AckEvery.MaxDelay) != "" {
					if d, err := time.ParseDuration(strings.TrimSpace(*flow.AckEvery.MaxDelay)); err != nil || d <= 0 {
						agg.AddFieldError(flowPath+".ackEvery.maxDelay", conditions.ReasonValidationFailed, "maxDelay must be a valid positive duration")
					}
				}
			}
			if flow.PauseThreshold != nil && flow.PauseThreshold.BufferPct != nil {
				if *flow.PauseThreshold.BufferPct < 0 || *flow.PauseThreshold.BufferPct > 100 {
					agg.AddFieldError(flowPath+".pauseThreshold.bufferPct", conditions.ReasonValidationFailed, "bufferPct must be between 0 and 100")
				}
			}
			if flow.ResumeThreshold != nil && flow.ResumeThreshold.BufferPct != nil {
				if *flow.ResumeThreshold.BufferPct < 0 || *flow.ResumeThreshold.BufferPct > 100 {
					agg.AddFieldError(flowPath+".resumeThreshold.bufferPct", conditions.ReasonValidationFailed, "bufferPct must be between 0 and 100")
				}
			}
		}
		if streaming.Delivery != nil {
			delivery := streaming.Delivery
			deliveryPath := streamingPath + ".delivery"
			if strings.TrimSpace(string(delivery.Ordering)) != "" &&
				delivery.Ordering != bubushv1alpha1.OrderingNone && delivery.Ordering != bubushv1alpha1.OrderingPerStream && delivery.Ordering != bubushv1alpha1.OrderingPerPartition {
				agg.AddFieldError(deliveryPath+".ordering", conditions.ReasonValidationFailed, "ordering must be none, per_stream, or per_partition")
			}
			if strings.TrimSpace(string(delivery.Semantics)) != "" &&
				delivery.Semantics != bubushv1alpha1.DeliveryBestEffort && delivery.Semantics != bubushv1alpha1.DeliveryAtLeastOnce {
				agg.AddFieldError(deliveryPath+".semantics", conditions.ReasonValidationFailed, "semantics must be best_effort or at_least_once")
			}
			if delivery.Replay != nil {
				replay := delivery.Replay
				if strings.TrimSpace(string(replay.Mode)) != "" &&
					replay.Mode != bubushv1alpha1.ReplayNone && replay.Mode != bubushv1alpha1.ReplayMemory && replay.Mode != bubushv1alpha1.ReplayDurable {
					agg.AddFieldError(deliveryPath+".replay.mode", conditions.ReasonValidationFailed, "mode must be none, memory, or durable")
				}
				if replay.RetentionSeconds != nil && *replay.RetentionSeconds < 0 {
					agg.AddFieldError(deliveryPath+".replay.retentionSeconds", conditions.ReasonValidationFailed, "retentionSeconds must be non-negative")
				}
				if replay.CheckpointInterval != nil && strings.TrimSpace(*replay.CheckpointInterval) != "" {
					if d, err := time.ParseDuration(strings.TrimSpace(*replay.CheckpointInterval)); err != nil || d <= 0 {
						agg.AddFieldError(deliveryPath+".replay.checkpointInterval", conditions.ReasonValidationFailed, "checkpointInterval must be a valid positive duration")
					}
				}
			}
		}
		if streaming.Routing != nil {
			routing := streaming.Routing
			routingPath := streamingPath + ".routing"
			if strings.TrimSpace(string(routing.Mode)) != "" &&
				routing.Mode != bubushv1alpha1.TransportRoutingAuto && routing.Mode != bubushv1alpha1.TransportRoutingHub && routing.Mode != bubushv1alpha1.TransportRoutingP2P {
				agg.AddFieldError(routingPath+".mode", conditions.ReasonValidationFailed, "mode must be auto, hub, or p2p")
			}
			if strings.TrimSpace(string(routing.FanOut)) != "" &&
				routing.FanOut != bubushv1alpha1.TransportFanOutSequential && routing.FanOut != bubushv1alpha1.TransportFanOutParallel {
				agg.AddFieldError(routingPath+".fanOut", conditions.ReasonValidationFailed, "fanOut must be sequential or parallel")
			}
			if routing.MaxDownstreams != nil && *routing.MaxDownstreams <= 0 {
				agg.AddFieldError(routingPath+".maxDownstreams", conditions.ReasonValidationFailed, "maxDownstreams must be >= 1")
			}
		}
		if streaming.Partitioning != nil {
			partitioning := streaming.Partitioning
			partitionPath := streamingPath + ".partitioning"
			if strings.TrimSpace(string(partitioning.Mode)) != "" &&
				partitioning.Mode != bubushv1alpha1.TransportPartitionNone && partitioning.Mode != bubushv1alpha1.TransportPartitionPreserve && partitioning.Mode != bubushv1alpha1.TransportPartitionHash {
				agg.AddFieldError(partitionPath+".mode", conditions.ReasonValidationFailed, "mode must be none, preserve, or hash")
			}
			if partitioning.Key != nil && strings.TrimSpace(*partitioning.Key) == "" {
				agg.AddFieldError(partitionPath+".key", conditions.ReasonValidationFailed, "key must be non-empty when set")
			}
			if partitioning.Partitions != nil && *partitioning.Partitions <= 0 {
				agg.AddFieldError(partitionPath+".partitions", conditions.ReasonValidationFailed, "partitions must be >= 1")
			}
		}
		if streaming.Lifecycle != nil {
			lifecycle := streaming.Lifecycle
			lifecyclePath := streamingPath + ".lifecycle"
			if strings.TrimSpace(string(lifecycle.Strategy)) != "" &&
				lifecycle.Strategy != bubushv1alpha1.TransportUpgradeRolling && lifecycle.Strategy != bubushv1alpha1.TransportUpgradeDrainCutover && lifecycle.Strategy != bubushv1alpha1.TransportUpgradeBlueGreen {
				agg.AddFieldError(lifecyclePath+".strategy", conditions.ReasonValidationFailed, "strategy must be rolling, drain_cutover, or blue_green")
			}
			if lifecycle.DrainTimeoutSeconds != nil && *lifecycle.DrainTimeoutSeconds < 0 {
				agg.AddFieldError(lifecyclePath+".drainTimeoutSeconds", conditions.ReasonValidationFailed, "drainTimeoutSeconds must be non-negative")
			}
			if lifecycle.MaxInFlight != nil && *lifecycle.MaxInFlight < 0 {
				agg.AddFieldError(lifecyclePath+".maxInFlight", conditions.ReasonValidationFailed, "maxInFlight must be non-negative")
			}
		}
		if streaming.Observability != nil && streaming.Observability.Tracing != nil {
			obsPath := streamingPath + ".observability.tracing"
			tracing := streaming.Observability.Tracing
			if tracing.SampleRate != nil && (*tracing.SampleRate < 0 || *tracing.SampleRate > 100) {
				agg.AddFieldError(obsPath+".sampleRate", conditions.ReasonValidationFailed, "sampleRate must be between 0 and 100")
			}
			if tracing.SamplePolicy != nil && strings.TrimSpace(*tracing.SamplePolicy) != "" {
				policy := strings.ToLower(strings.TrimSpace(*tracing.SamplePolicy))
				switch policy {
				case "rate", "random", "always", "never":
				default:
					agg.AddFieldError(obsPath+".samplePolicy", conditions.ReasonValidationFailed, "samplePolicy must be rate, random, always, or never")
				}
			}
		}
		if streaming.Recording != nil {
			recording := streaming.Recording
			recordingPath := streamingPath + ".recording"
			if strings.TrimSpace(string(recording.Mode)) != "" &&
				recording.Mode != bubushv1alpha1.TransportRecordingOff && recording.Mode != bubushv1alpha1.TransportRecordingMetadata && recording.Mode != bubushv1alpha1.TransportRecordingPayload {
				agg.AddFieldError(recordingPath+".mode", conditions.ReasonValidationFailed, "mode must be off, metadata, or payload")
			}
			if recording.SampleRate != nil && (*recording.SampleRate < 0 || *recording.SampleRate > 100) {
				agg.AddFieldError(recordingPath+".sampleRate", conditions.ReasonValidationFailed, "sampleRate must be between 0 and 100")
			}
			if recording.RetentionSeconds != nil && *recording.RetentionSeconds < 0 {
				agg.AddFieldError(recordingPath+".retentionSeconds", conditions.ReasonValidationFailed, "retentionSeconds must be non-negative")
			}
			for j, fieldPath := range recording.RedactFields {
				if strings.TrimSpace(fieldPath) == "" {
					agg.AddFieldError(fmt.Sprintf("%s.redactFields[%d]", recordingPath, j), conditions.ReasonValidationFailed, "redact field cannot be empty")
				}
			}
		}
		if len(decl.Streaming.Lanes) > 0 {
			seenNames := make(map[string]struct{})
			seenKinds := make(map[string]struct{})
			for j, lane := range decl.Streaming.Lanes {
				lanePath := fmt.Sprintf("spec.transports[%d].streaming.lanes[%d]", i, j)
				name := strings.TrimSpace(lane.Name)
				if name == "" {
					agg.AddFieldError(lanePath+".name", conditions.ReasonValidationFailed, "lane name must be set")
				} else {
					if _, ok := seenNames[name]; ok {
						agg.AddFieldError(lanePath+".name", conditions.ReasonValidationFailed, fmt.Sprintf("duplicate lane name %q", name))
					}
					seenNames[name] = struct{}{}
				}
				kind := strings.TrimSpace(string(lane.Kind))
				if kind == "" {
					agg.AddFieldError(lanePath+".kind", conditions.ReasonValidationFailed, "lane kind must be set")
				} else {
					if _, ok := seenKinds[kind]; ok {
						agg.AddFieldError(lanePath+".kind", conditions.ReasonValidationFailed, fmt.Sprintf("duplicate lane kind %q", kind))
					}
					seenKinds[kind] = struct{}{}
				}
				if lane.MaxMessages != nil && *lane.MaxMessages < 0 {
					agg.AddFieldError(lanePath+".maxMessages", conditions.ReasonValidationFailed, "maxMessages must be non-negative")
				}
				if lane.MaxBytes != nil && *lane.MaxBytes < 0 {
					agg.AddFieldError(lanePath+".maxBytes", conditions.ReasonValidationFailed, "maxBytes must be non-negative")
				}
			}
		}
		if decl.Streaming.FanIn != nil {
			fanIn := decl.Streaming.FanIn
			fanInPath := fmt.Sprintf("spec.transports[%d].streaming.fanIn", i)
			mode := strings.TrimSpace(string(fanIn.Mode))
			if mode == string(bubushv1alpha1.TransportFanInQuorum) {
				if fanIn.Quorum == nil || *fanIn.Quorum <= 0 {
					agg.AddFieldError(fanInPath+".quorum", conditions.ReasonValidationFailed, "quorum must be set when mode=quorum")
				}
			} else if fanIn.Quorum != nil {
				agg.AddFieldError(fanInPath+".quorum", conditions.ReasonValidationFailed, "quorum is only valid when mode=quorum")
			}
			if fanIn.TimeoutSeconds != nil && *fanIn.TimeoutSeconds < 0 {
				agg.AddFieldError(fanInPath+".timeoutSeconds", conditions.ReasonValidationFailed, "timeoutSeconds must be non-negative")
			}
			if fanIn.MaxEntries != nil && *fanIn.MaxEntries <= 0 {
				agg.AddFieldError(fanInPath+".maxEntries", conditions.ReasonValidationFailed, "maxEntries must be >= 1")
			}
		}
	}
}

// validateStepTransportRefs ensures step transport references are valid.
//
// Behavior:
//   - Skips steps without a transport field.
//   - Validates transport references against the Story's declared transports.
//   - Rejects references to undeclared transports.
//
// Arguments:
//   - story *bubushv1alpha1.Story: the Story containing steps to validate.
//   - transportByName map[string]struct{}: set of valid transport names.
//
// Returns:
//   - nil if all transport references are valid.
//   - Error describing the invalid reference.
func validateStepTransportRefs(steps []bubushv1alpha1.Step, transportByName map[string]struct{}) error {
	for i := range steps {
		step := &steps[i]
		name := strings.TrimSpace(step.Transport)
		if name == "" {
			continue
		}
		if len(transportByName) == 0 {
			return fmt.Errorf("step '%s' references transport %q, but story does not declare any transports", step.Name, name)
		}
		if _, ok := transportByName[name]; !ok {
			return fmt.Errorf("step '%s' references unknown transport %q", step.Name, name)
		}
	}
	return nil
}

// buildStepDependencyGraph constructs the dependency graph for cycle detection.
//
// Behavior:
//   - Creates indegree map and adjacency list from step.needs relationships.
//   - Rejects self-dependencies and duplicate dependencies.
//
// Arguments:
//   - story *bubushv1alpha1.Story: the Story to analyze.
//
// Returns:
//   - indegree map[string]int: count of incoming edges per step.
//   - edges map[string][]string: adjacency list (dependency -> dependents).
//   - Error if graph construction fails due to invalid dependencies.
func buildStepDependencyGraph(steps []bubushv1alpha1.Step) (map[string]int, map[string][]string, error) {
	indegree := make(map[string]int, len(steps))
	edges := make(map[string][]string, len(steps))
	nameSet := make(map[string]struct{}, len(steps))
	for i := range steps {
		name := steps[i].Name
		indegree[name] = 0
		nameSet[name] = struct{}{}
	}

	for i := range steps {
		step := &steps[i]
		seen := make(map[string]struct{}, len(step.Needs))
		for _, dep := range step.Needs {
			if dep == step.Name {
				return nil, nil, fmt.Errorf("step '%s' cannot depend on itself", step.Name)
			}
			if _, dup := seen[dep]; dup {
				continue
			}
			if _, ok := nameSet[dep]; !ok {
				continue
			}
			seen[dep] = struct{}{}
			edges[dep] = append(edges[dep], step.Name)
			indegree[step.Name]++
		}
	}

	return indegree, edges, nil
}

// detectStepGraphCycles performs topological sort using Kahn's algorithm.
//
// Behavior:
//   - Processes steps with zero indegree (no dependencies).
//   - Decrements indegree of dependents as steps are processed.
//   - Returns steps that couldn't be processed (cycle members).
//
// Arguments:
//   - indegree map[string]int: count of incoming edges per step.
//   - edges map[string][]string: adjacency list (dependency -> dependents).
//
// Returns:
//   - nil if all steps were processed (acyclic).
//   - Slice of step names that are part of a cycle.
func detectStepGraphCycles(indegree map[string]int, edges map[string][]string) []string {
	queue := make([]string, 0, len(indegree))
	for name, deg := range indegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	processed := 0
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		processed++

		for _, child := range edges[current] {
			indegree[child]--
			if indegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	if processed == len(indegree) {
		return nil
	}

	blocked := make([]string, 0, len(indegree))
	for name, deg := range indegree {
		if deg > 0 {
			blocked = append(blocked, name)
		}
	}
	return blocked
}

// mergeWithAndRuntime merges the 'with' and 'runtime' blocks into a single JSON object.
//
// Behavior:
//   - Unmarshals both with and runtime blocks into maps.
//   - Merges runtime into with (runtime takes precedence on overlap).
//   - Returns nil if both blocks are empty.
//
// Arguments:
//   - step *bubushv1alpha1.Step: the step containing with/runtime blocks.
//
// Returns:
//   - Merged JSON bytes.
//   - nil, nil if both blocks are empty.
//   - Error if unmarshaling fails.
func mergeWithAndRuntime(step *bubushv1alpha1.Step) ([]byte, error) {
	withMap := make(map[string]any)
	runtimeMap := make(map[string]any)

	if step.With != nil && len(step.With.Raw) > 0 {
		if err := json.Unmarshal(step.With.Raw, &withMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal 'with' block: %w", err)
		}
	}

	if step.Runtime != nil && len(step.Runtime.Raw) > 0 {
		if err := json.Unmarshal(step.Runtime.Raw, &runtimeMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal 'runtime' block: %w", err)
		}
	}

	// Merge runtime into with (runtime takes precedence if there's overlap)
	maps.Copy(withMap, runtimeMap)

	if len(withMap) == 0 {
		return nil, nil
	}

	return json.Marshal(withMap)
}

// validateEngramStepCached validates a ref-based step using cached lookups.
//
// Behavior:
//   - Fetches Engram using cache to avoid repeated API calls.
//   - Fetches EngramTemplate using cache.
//   - Merges step's with+runtime blocks for validation.
//   - Validates merged config against template's inputSchema.
//
// Arguments:
//   - ctx context.Context: for API calls.
//   - namespace string: default namespace for Engram lookup.
//   - step *bubushv1alpha1.Step: the step to validate.
//   - engrams engramCache: cache of previously fetched Engrams.
//   - templates templateCache: cache of previously fetched EngramTemplates.
//
// Returns:
//   - nil if step is valid.
//   - Error describing the validation failure.
//
// Side Effects:
//   - Populates engrams and templates caches with fetched objects.
func (v *StoryCustomValidator) validateEngramStepCached(
	ctx context.Context,
	story *bubushv1alpha1.Story,
	step *bubushv1alpha1.Step,
	engrams engramCache,
	templates templateCache,
) (admission.Warnings, error) {
	if story == nil {
		return nil, fmt.Errorf("story is required for engram reference validation")
	}
	targetNamespace := refs.ResolveNamespace(story, &step.Ref.ObjectReference)
	if err := ValidateCrossNamespaceReference(
		ctx,
		v.Client,
		v.controllerConfig(),
		story,
		"bubustack.io",
		"Story",
		"bubustack.io",
		"Engram",
		targetNamespace,
		step.Ref.Name,
		"EngramRef",
	); err != nil {
		return nil, fmt.Errorf("step '%s' references engram in namespace '%s': %w", step.Name, targetNamespace, err)
	}

	// Build the Engram key
	engramKey := types.NamespacedName{Name: step.Ref.Name, Namespace: targetNamespace}

	// Engram existence is a warning, not a hard error. Engrams may be created
	// in the same kubectl apply batch as the Story. The controller validates
	// Engram references during reconciliation.
	var warnings admission.Warnings
	engram, ok := engrams[engramKey]
	if !ok {
		engram = &bubushv1alpha1.Engram{}
		if err := v.Client.Get(ctx, engramKey, engram); err != nil {
			if apierrors.IsNotFound(err) {
				warnings = append(warnings, fmt.Sprintf("step '%s': engram '%s' not found yet in namespace '%s'; the controller will retry", step.Name, engramKey.Name, engramKey.Namespace))
				return warnings, nil
			}
			return nil, fmt.Errorf("failed to get engram for step '%s': %w", step.Name, err)
		}
		engrams[engramKey] = engram
	}
	if strings.TrimSpace(step.Ref.Version) != "" && strings.TrimSpace(engram.Spec.Version) != strings.TrimSpace(step.Ref.Version) {
		warnings = append(warnings, fmt.Sprintf("step '%s': engram '%s' version mismatch (expected %q, got %q)", step.Name, engramKey.String(), step.Ref.Version, engram.Spec.Version))
	}

	// EngramTemplate must pre-exist (cluster-scoped, admin-installed).
	templateName := engram.Spec.TemplateRef.Name
	template, ok := templates[templateName]
	if !ok {
		template = &v1alpha1.EngramTemplate{}
		if err := v.Client.Get(ctx, types.NamespacedName{Name: templateName, Namespace: ""}, template); err != nil {
			if apierrors.IsNotFound(err) {
				return warnings, fmt.Errorf("step '%s' references engram '%s' which in turn references EngramTemplate '%s' that was not found", step.Name, engram.Name, templateName)
			}
			return warnings, fmt.Errorf("failed to get EngramTemplate for step '%s': %w", step.Name, err)
		}
		templates[templateName] = template
	}

	// Validate the step's 'with' + 'runtime' blocks against the template's inputSchema.
	if template.Spec.InputSchema != nil && len(template.Spec.InputSchema.Raw) > 0 {
		mergedConfig, err := mergeWithAndRuntime(step)
		if err != nil {
			return warnings, fmt.Errorf("step '%s': failed to merge 'with' and 'runtime' for validation: %w", step.Name, err)
		}
		if len(mergedConfig) == 0 {
			mergedConfig = []byte("{}")
		}
		if err := validateJSONAgainstSchema(mergedConfig, template.Spec.InputSchema.Raw, "EngramTemplate"); err != nil {
			return warnings, fmt.Errorf("step '%s': %w", step.Name, err)
		}
	}

	return warnings, nil
}

// extractTemplateStringsFromJSON walks a JSON value and collects all string values containing "{{".
func extractTemplateStringsFromJSON(raw []byte) []string {
	var node any
	if err := json.Unmarshal(raw, &node); err != nil {
		return nil
	}
	var result []string
	collectTemplateStrings(node, &result)
	return result
}

func collectTemplateStrings(node any, result *[]string) {
	switch typed := node.(type) {
	case map[string]any:
		for _, v := range typed {
			collectTemplateStrings(v, result)
		}
	case []any:
		for _, v := range typed {
			collectTemplateStrings(v, result)
		}
	case string:
		if strings.Contains(typed, "{{") {
			*result = append(*result, typed)
		}
	}
}

// validateStepTemplateRefs checks that template expressions in a step's with/runtime
// fields reference only known and reachable upstream steps.
func validateStepTemplateRefs(step *bubushv1alpha1.Step, allStepNames map[string]struct{}, reachable map[string]struct{}) []string {
	var warnings []string
	var templates []string
	if step.If != nil {
		templates = append(templates, *step.If)
	}
	if step.With != nil && len(step.With.Raw) > 0 {
		templates = append(templates, extractTemplateStringsFromJSON(step.With.Raw)...)
	}
	if step.Runtime != nil && len(step.Runtime.Raw) > 0 {
		templates = append(templates, extractTemplateStringsFromJSON(step.Runtime.Raw)...)
	}
	for _, tpl := range templates {
		refs := templating.ExtractStepReferences(tpl)
		for _, ref := range refs {
			if _, ok := allStepNames[ref]; !ok {
				warnings = append(warnings, fmt.Sprintf("step '%s' references unknown step '%s' in template expression", step.Name, ref))
			} else if _, ok := reachable[ref]; !ok {
				warnings = append(warnings, fmt.Sprintf("step '%s' references step '%s' which is not in its upstream dependency chain", step.Name, ref))
			}
		}
	}
	return warnings
}
