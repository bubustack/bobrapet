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
	storypkg "github.com/bubustack/bobrapet/pkg/story"
	"github.com/bubustack/bobrapet/pkg/validation"
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
		}
		for i := range story.Spec.Steps {
			if story.Spec.Steps[i].Execution != nil {
				story.Spec.Steps[i].Execution.Retry = ResolveRetryPolicy(cfg, story.Spec.Steps[i].Execution.Retry)
			}
		}
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
	return ValidateCreateResource[*bubushv1alpha1.Story](ctx, obj, "Story", storylog, func(ctx context.Context, story *bubushv1alpha1.Story) error {
		webhookCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		return v.validateStory(webhookCtx, story)
	})
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

	if err := v.validateStory(webhookCtx, story); err != nil {
		return nil, err
	}
	return nil, nil
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
func (v *StoryCustomValidator) validateStory(ctx context.Context, story *bubushv1alpha1.Story) error {
	agg := validation.NewAggregator()

	// Story-level size validation (fail-fast on critical errors)
	if err := validateStorySize(story); err != nil {
		return err
	}

	cfg := v.controllerConfig()
	maxSize := pickStoryMaxWithSize(cfg)
	if err := validateOutputSize(story, maxSize); err != nil {
		agg.AddFieldError("spec.output", conditions.ReasonValidationFailed, err.Error())
	}

	// Step validation uses aggregator to collect all step errors
	validateStepsShapeAggregated(ctx, v, story, maxSize, agg)

	if err := validateNeedsExistence(story); err != nil {
		agg.AddFieldError("spec.steps", conditions.ReasonValidationFailed, err.Error())
	}

	if err := validateStepGraphAcyclic(story); err != nil {
		agg.AddFieldError("spec.steps", conditions.ReasonValidationFailed, err.Error())
	}

	transportByName, err := validateStoryTransportsSpec(story)
	if err != nil {
		agg.AddFieldError("spec.transports", conditions.ReasonValidationFailed, err.Error())
	}

	if err := validateStepTransportRefs(story, transportByName); err != nil {
		agg.AddFieldError("spec.steps", conditions.ReasonTransportReferenceInvalid, err.Error())
	}

	if err := v.validateExecuteStoryReferences(ctx, story); err != nil {
		agg.AddFieldError("spec.steps", conditions.ReasonStoryReferenceInvalid, err.Error())
	}

	if agg.HasErrors() {
		return agg.ToFieldErrors().ToAggregate()
	}
	return nil
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
func validateStepsShapeAggregated(ctx context.Context, v *StoryCustomValidator, story *bubushv1alpha1.Story, maxSize int, agg *validation.Aggregator) {
	validator := stepShapeValidator{
		seen:      make(map[string]struct{}),
		engrams:   make(engramCache),
		templates: make(templateCache),
	}

	for i := range story.Spec.Steps {
		s := &story.Spec.Steps[i]
		stepPath := fmt.Sprintf("spec.steps[%d]", i)
		validator.validateStep(ctx, v, story, s, stepPath, maxSize, agg)
	}
}

type stepShapeValidator struct {
	seen      map[string]struct{}
	engrams   engramCache
	templates templateCache
}

func (sv *stepShapeValidator) validateStep(ctx context.Context, v *StoryCustomValidator, story *bubushv1alpha1.Story, step *bubushv1alpha1.Step, stepPath string, maxSize int, agg *validation.Aggregator) {
	sv.checkName(step, stepPath, agg)
	sv.checkTypeCombination(step, stepPath, agg)
	sv.checkNeeds(step, stepPath, agg)
	sv.checkRef(ctx, v, story.Namespace, step, stepPath, agg)
	sv.checkWithSize(step, stepPath, maxSize, agg)
	sv.checkPrimitive(step, stepPath, agg)
	sv.checkLoop(step, stepPath, agg)
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

func (sv *stepShapeValidator) checkRef(ctx context.Context, v *StoryCustomValidator, namespace string, step *bubushv1alpha1.Step, stepPath string, agg *validation.Aggregator) {
	if step.Ref == nil {
		return
	}
	if err := v.validateEngramStepCached(ctx, namespace, step, sv.engrams, sv.templates); err != nil {
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

func (sv *stepShapeValidator) checkPrimitive(step *bubushv1alpha1.Step, stepPath string, agg *validation.Aggregator) {
	if err := validatePrimitiveShapes(step); err != nil {
		agg.AddFieldError(stepPath, conditions.ReasonValidationFailed, err.Error())
	}
}

func (sv *stepShapeValidator) checkLoop(step *bubushv1alpha1.Step, stepPath string, agg *validation.Aggregator) {
	if step.Type != enums.StepTypeLoop {
		return
	}
	for _, loopErr := range storypkg.ValidateLoopStep(step) {
		reason := loopErr.Reason
		if reason == "" {
			reason = conditions.ReasonValidationFailed
		}
		agg.AddFieldError(stepPath+loopErr.Field, reason, loopErr.Message)
	}
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
	if s.Type == enums.StepTypeExecuteStory {
		if s.With == nil {
			return fmt.Errorf("step '%s' of type 'executeStory' requires a 'with' block", s.Name)
		}
		var withConfig struct {
			StoryRef struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace,omitempty"`
			} `json:"storyRef"`
		}
		if err := json.Unmarshal(s.With.Raw, &withConfig); err != nil {
			return fmt.Errorf("step '%s' has an invalid 'with' block for type 'executeStory': %w", s.Name, err)
		}
		if withConfig.StoryRef.Name == "" {
			return fmt.Errorf("step '%s' of type 'executeStory' requires 'with.storyRef.name' to be set", s.Name)
		}
	}
	return nil
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
//
// Returns:
//   - nil if all references are valid.
//   - Error describing the first invalid reference.
func (v *StoryCustomValidator) validateExecuteStoryReferences(ctx context.Context, story *bubushv1alpha1.Story) error {
	if v.Client == nil {
		return nil
	}
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if step.Type != enums.StepTypeExecuteStory || step.With == nil {
			continue
		}
		var withConfig struct {
			StoryRef struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace,omitempty"`
			} `json:"storyRef"`
		}
		if err := json.Unmarshal(step.With.Raw, &withConfig); err != nil {
			return fmt.Errorf("step '%s' has an invalid 'with' block for type 'executeStory': %w", step.Name, err)
		}
		targetNamespace := story.Namespace
		if withConfig.StoryRef.Namespace != "" {
			targetNamespace = withConfig.StoryRef.Namespace
		}
		if withConfig.StoryRef.Name == "" {
			continue
		}
		if targetNamespace == story.Namespace && withConfig.StoryRef.Name == story.Name {
			return fmt.Errorf("step '%s' of type 'executeStory' cannot reference the same story", step.Name)
		}

		var target bubushv1alpha1.Story
		key := types.NamespacedName{Namespace: targetNamespace, Name: withConfig.StoryRef.Name}
		storylog.V(1).Info("Validating executeStory reference", "step", step.Name, "targetStory", key.String())
		if err := v.Client.Get(ctx, key, &target); err != nil {
			if apierrors.IsNotFound(err) {
				storylog.V(1).Info("Referenced Story not found", "step", step.Name, "targetStory", key.String())
				return fmt.Errorf("step '%s' of type 'executeStory' references Story '%s/%s' which does not exist", step.Name, targetNamespace, withConfig.StoryRef.Name)
			}
			return fmt.Errorf("failed to validate executeStory reference for step '%s': %w", step.Name, err)
		}
	}
	return nil
}

// validateNeedsExistence ensures all step.needs references point to existing steps.
//
// Behavior:
//   - Builds a set of all step names.
//   - Checks each step's needs list against the set.
//   - Rejects references to non-existent steps.
//
// Arguments:
//   - story *bubushv1alpha1.Story: the Story to validate.
//
// Returns:
//   - nil if all needs references are valid.
//   - Error describing the unknown dependency.
func validateNeedsExistence(story *bubushv1alpha1.Story) error {
	stepNames := make(map[string]struct{}, len(story.Spec.Steps))
	for i := range story.Spec.Steps {
		stepNames[story.Spec.Steps[i].Name] = struct{}{}
	}
	for i := range story.Spec.Steps {
		s := &story.Spec.Steps[i]
		for _, dep := range s.Needs {
			if _, ok := stepNames[dep]; !ok {
				return fmt.Errorf("step '%s' lists unknown dependency '%s' in needs", s.Name, dep)
			}
		}
	}
	return nil
}

// validateStepGraphAcyclic uses Kahn's algorithm to detect dependency cycles.
//
// Behavior:
//   - Builds a dependency graph from step.needs relationships.
//   - Performs topological sort using Kahn's algorithm.
//   - Identifies steps that couldn't be processed (cycle members).
//
// Arguments:
//   - story *bubushv1alpha1.Story: the Story to validate.
//
// Returns:
//   - nil if the graph is acyclic.
//   - Error listing the steps involved in the cycle.
func validateStepGraphAcyclic(story *bubushv1alpha1.Story) error {
	if len(story.Spec.Steps) == 0 {
		return nil
	}

	indegree, edges, err := buildStepDependencyGraph(story)
	if err != nil {
		return err
	}

	blocked := detectStepGraphCycles(indegree, edges)
	if len(blocked) > 0 {
		sort.Strings(blocked)
		return fmt.Errorf("story contains a dependency cycle involving step(s): %s", strings.Join(blocked, ", "))
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
func validateStepTransportRefs(story *bubushv1alpha1.Story, transportByName map[string]struct{}) error {
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
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
//   - Validates that dependencies are declared before dependents.
//   - Rejects self-dependencies and duplicate dependencies.
//
// Arguments:
//   - story *bubushv1alpha1.Story: the Story to analyze.
//
// Returns:
//   - indegree map[string]int: count of incoming edges per step.
//   - edges map[string][]string: adjacency list (dependency -> dependents).
//   - Error if graph construction fails due to invalid dependencies.
func buildStepDependencyGraph(story *bubushv1alpha1.Story) (map[string]int, map[string][]string, error) {
	indegree := make(map[string]int, len(story.Spec.Steps))
	edges := make(map[string][]string, len(story.Spec.Steps))
	index := make(map[string]int, len(story.Spec.Steps))

	for i := range story.Spec.Steps {
		name := story.Spec.Steps[i].Name
		indegree[name] = 0
		index[name] = i
	}

	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		seen := make(map[string]struct{}, len(step.Needs))
		for _, dep := range step.Needs {
			if dep == step.Name {
				return nil, nil, fmt.Errorf("step '%s' cannot depend on itself", step.Name)
			}
			if depIdx, ok := index[dep]; ok && depIdx >= i {
				return nil, nil, fmt.Errorf("step '%s' dependency '%s' must be declared before the step", step.Name, dep)
			}
			if _, dup := seen[dep]; dup {
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
	withMap := make(map[string]interface{})
	runtimeMap := make(map[string]interface{})

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
	for k, v := range runtimeMap {
		withMap[k] = v
	}

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
	namespace string,
	step *bubushv1alpha1.Step,
	engrams engramCache,
	templates templateCache,
) error {
	// Build the Engram key
	engramKey := types.NamespacedName{Name: step.Ref.Name, Namespace: namespace}
	if step.Ref.Namespace != nil && *step.Ref.Namespace != "" {
		engramKey.Namespace = *step.Ref.Namespace
	}

	// Check cache first
	engram, ok := engrams[engramKey]
	if !ok {
		engram = &bubushv1alpha1.Engram{}
		if err := v.Client.Get(ctx, engramKey, engram); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("step '%s' references engram '%s' which does not exist in namespace '%s'", step.Name, engramKey.Name, engramKey.Namespace)
			}
			return fmt.Errorf("failed to get engram for step '%s': %w", step.Name, err)
		}
		engrams[engramKey] = engram
	}

	// Check template cache
	templateName := engram.Spec.TemplateRef.Name
	template, ok := templates[templateName]
	if !ok {
		template = &v1alpha1.EngramTemplate{}
		if err := v.Client.Get(ctx, types.NamespacedName{Name: templateName, Namespace: ""}, template); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("step '%s' references engram '%s' which in turn references EngramTemplate '%s' that was not found", step.Name, engram.Name, templateName)
			}
			return fmt.Errorf("failed to get EngramTemplate for step '%s': %w", step.Name, err)
		}
		templates[templateName] = template
	}

	// Validate the step's 'with' + 'runtime' blocks against the template's inputSchema.
	if template.Spec.InputSchema != nil && len(template.Spec.InputSchema.Raw) > 0 {
		mergedConfig, err := mergeWithAndRuntime(step)
		if err != nil {
			return fmt.Errorf("step '%s': failed to merge 'with' and 'runtime' for validation: %w", step.Name, err)
		}
		if len(mergedConfig) > 0 {
			if err := validateJSONAgainstSchema(mergedConfig, template.Spec.InputSchema.Raw, "EngramTemplate"); err != nil {
				return fmt.Errorf("step '%s': %w", step.Name, err)
			}
		}
	}

	return nil
}
