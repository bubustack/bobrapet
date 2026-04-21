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
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/controller-runtime/pkg/client"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	webhookshared "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/bobrapet/pkg/storage"
	"k8s.io/apimachinery/pkg/api/errors"
)

// log is for logging in this package.
var storyrunlog = logf.Log.WithName("storyrun-resource")

type StoryRunWebhook struct {
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

// SetupWebhookWithManager registers the StoryRun validating webhook with the manager.
//
// Behavior:
//   - Creates a validating webhook for StoryRun resources.
//   - Uses mgr.GetAPIReader() so admission checks read authoritative API state.
//   - Injects Config and ConfigManager into the validator.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//
// Returns:
//   - nil on success.
//   - Error if webhook registration fails.
func (wh *StoryRunWebhook) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &runsv1alpha1.StoryRun{}).
		WithValidator(&StoryRunCustomValidator{
			Client:        mgr.GetAPIReader(),
			Config:        wh.Config,
			ConfigManager: wh.ConfigManager,
		}).
		Complete()
}

// NOTE: The 'path' attribute must follow a specific pattern and should not be modified directly here.
// Modifying the path for an invalid path can cause API server errors; failing to locate the webhook.
// +kubebuilder:webhook:path=/validate-runs-bubustack-io-v1alpha1-storyrun,mutating=false,failurePolicy=fail,sideEffects=None,groups=runs.bubustack.io,resources=storyruns,verbs=create;update,versions=v1alpha1,name=vstoryrun-v1alpha1.kb.io,admissionReviewVersions=v1

// StoryRunCustomValidator struct is responsible for validating the StoryRun resource
// when it is created, updated, or deleted.
//
// NOTE: The +kubebuilder:object:generate=false marker prevents controller-gen from generating DeepCopy methods,
// as this struct is used only for temporary operations and does not need to be deeply copied.
type StoryRunCustomValidator struct {
	Client        client.Reader
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

var _ admission.Validator[*runsv1alpha1.StoryRun] = &StoryRunCustomValidator{}

// controllerConfig returns the effective controller configuration.
//
// Behavior:
//   - Delegates to webhookshared.ResolveControllerConfig for unified config resolution.
//
// Returns:
//   - Non-nil *config.ControllerConfig from the highest priority source.
func (v *StoryRunCustomValidator) controllerConfig() *config.ControllerConfig {
	return webhookshared.ResolveControllerConfig(storyrunlog, v.ConfigManager, v.Config)
}

// ValidateCreate implements admission.Validator for StoryRun creation.
//
// Behavior:
//   - Validates storyRef.name is set and Story exists.
//   - Validates inputs shape, size, and schema against Story.
//
// Arguments:
//   - ctx context.Context: for Story lookup.
//   - sr *runsv1alpha1.StoryRun: the StoryRun being created.
//
// Returns:
//   - nil, nil if validation passes.
//   - nil, error if validation errors exist.
func (v *StoryRunCustomValidator) ValidateCreate(ctx context.Context, sr *runsv1alpha1.StoryRun) (admission.Warnings, error) {
	storyrunlog.Info("Validation for StoryRun upon creation", "name", sr.GetName())
	story, err := v.validateStoryRun(ctx, sr)
	if err != nil {
		return nil, err
	}
	if err := validateTriggerTokenName(sr); err != nil {
		return nil, err
	}
	if err := validateTriggerInputHashOnCreate(ctx, sr); err != nil {
		return nil, err
	}
	return warnOffloadedInputs(story, sr), nil
}

// ValidateUpdate implements webhook.CustomValidator for StoryRun updates.
//
// Behavior:
//   - Skips validation if resource is being deleted (DeletionTimestamp set).
//   - Rejects updates that decrease observedGeneration.
//   - Skips validation if spec is unchanged (metadata-only update).
//   - Allows a graceful cancel request when only spec.cancelRequested changes
//     from nil/false to true.
//
// Arguments:
//   - ctx context.Context: for Story lookup.
//   - oldObj runtime.Object: previous StoryRun state.
//   - newObj runtime.Object: proposed StoryRun state.
//
// Returns:
//   - nil, nil if validation passes or is skipped.
//   - nil, error if type assertion fails or validation errors exist.
func (v *StoryRunCustomValidator) ValidateUpdate(ctx context.Context, oldSr, newSr *runsv1alpha1.StoryRun) (admission.Warnings, error) {
	// Allow metadata-only updates during deletion (e.g., finalizer removal).
	if newSr.GetDeletionTimestamp() != nil {
		return nil, nil
	}
	storyrunlog.Info("Validation for StoryRun upon update", "name", newSr.GetName())
	if err := ensureStoryRunObservedGenerationMonotonic(oldSr, newSr); err != nil {
		return nil, err
	}
	if err := validateTriggerTokenName(newSr); err != nil {
		return nil, err
	}
	if err := validateTriggerTokenOnUpdate(oldSr, newSr); err != nil {
		return nil, err
	}
	if err := validateTriggerInputHashOnUpdate(ctx, oldSr, newSr); err != nil {
		return nil, err
	}
	if reflect.DeepEqual(oldSr.Spec, newSr.Spec) {
		return nil, nil
	}
	if allowsGracefulCancelRequest(oldSr, newSr) {
		return nil, nil
	}
	return nil, fmt.Errorf("spec is immutable for StoryRun; create a new StoryRun for updated inputs or references")
}

func allowsGracefulCancelRequest(oldSr, newSr *runsv1alpha1.StoryRun) bool {
	if oldSr == nil || newSr == nil {
		return false
	}
	if !allowsCancelRequestedTransition(oldSr.Spec.CancelRequested, newSr.Spec.CancelRequested) {
		return false
	}

	oldSpec := oldSr.Spec
	newSpec := newSr.Spec
	oldSpec.CancelRequested = nil
	newSpec.CancelRequested = nil

	return reflect.DeepEqual(oldSpec, newSpec)
}

func allowsCancelRequestedTransition(oldValue, newValue *bool) bool {
	newRequested := newValue != nil && *newValue
	if !newRequested {
		return false
	}
	oldRequested := oldValue != nil && *oldValue
	return !oldRequested
}

// ValidateDelete implements webhook.CustomValidator for StoryRun deletion.
//
// Behavior:
//   - Always allows deletion (no-op validation).
//   - Exists as scaffold placeholder; delete verb not enabled in annotation.
//
// Arguments:
//   - ctx context.Context: unused.
//   - obj runtime.Object: expected to be *runsv1alpha1.StoryRun.
//
// Returns:
//   - nil, nil (deletion always allowed).
func (v *StoryRunCustomValidator) ValidateDelete(_ context.Context, sr *runsv1alpha1.StoryRun) (admission.Warnings, error) {
	storyrunlog.Info("Validation for StoryRun upon deletion", "name", sr.GetName())
	return nil, nil
}

// validateStoryRun performs basic invariants validation for StoryRun specs.
//
// Behavior:
//   - Validates storyRef.name is set.
//   - Validates referenced Story exists.
//   - Validates inputs shape and size against config limits.
//   - Validates inputs against Story's inputsSchema if defined.
//
// Arguments:
//   - ctx context.Context: for Story lookup.
//   - sr *runsv1alpha1.StoryRun: the StoryRun to validate.
//
// Returns:
//   - nil if all validations pass.
//   - Error describing the first validation failure.
func (v *StoryRunCustomValidator) validateStoryRun(ctx context.Context, sr *runsv1alpha1.StoryRun) (*bubuv1alpha1.Story, error) {
	if err := requireStoryRef(sr); err != nil {
		return nil, err
	}
	cfg := v.controllerConfig()
	story, _, err := fetchStory(ctx, v.Client, cfg, sr)
	if err != nil {
		return nil, err
	}
	if err := validateInputsShapeAndSize(cfg, sr); err != nil {
		return nil, err
	}
	if err := validateInputsSchema(story, sr); err != nil {
		return nil, err
	}
	return story, nil
}

// requireStoryRef ensures the StoryRun has a non-empty storyRef.name.
//
// Arguments:
//   - sr *runsv1alpha1.StoryRun: the StoryRun to validate.
//
// Returns:
//   - nil if storyRef.name is set.
//   - Error if storyRef.name is empty.
func requireStoryRef(sr *runsv1alpha1.StoryRun) error {
	if sr.Spec.StoryRef.Name == "" {
		return fmt.Errorf("spec.storyRef.name is required")
	}
	return nil
}

func validateTriggerTokenName(sr *runsv1alpha1.StoryRun) error {
	if sr == nil {
		return nil
	}
	ann := sr.GetAnnotations()
	if len(ann) == 0 {
		return nil
	}
	token := strings.TrimSpace(ann[runsidentity.StoryRunTriggerTokenAnnotation])
	if token == "" {
		return nil
	}
	if strings.TrimSpace(sr.Name) == "" {
		return fmt.Errorf("metadata.name is required when %s is set", runsidentity.StoryRunTriggerTokenAnnotation)
	}
	if strings.TrimSpace(sr.GenerateName) != "" {
		return fmt.Errorf("metadata.generateName must be empty when %s is set", runsidentity.StoryRunTriggerTokenAnnotation)
	}
	storyNamespace := refs.ResolveNamespace(sr, &sr.Spec.StoryRef.ObjectReference)
	expected := runsidentity.DeriveStoryRunName(storyNamespace, sr.Spec.StoryRef.Name, token)
	if sr.Name != expected {
		return fmt.Errorf("metadata.name must be %q when %s is set (got %q)", expected, runsidentity.StoryRunTriggerTokenAnnotation, sr.Name)
	}
	return nil
}

func validateTriggerTokenOnUpdate(oldSr, newSr *runsv1alpha1.StoryRun) error {
	if newSr == nil {
		return nil
	}
	newToken := strings.TrimSpace(newSr.GetAnnotations()[runsidentity.StoryRunTriggerTokenAnnotation])
	oldToken := ""
	if oldSr != nil {
		oldToken = strings.TrimSpace(oldSr.GetAnnotations()[runsidentity.StoryRunTriggerTokenAnnotation])
	}
	if oldToken == "" && newToken == "" {
		return nil
	}
	if newToken != oldToken {
		return fmt.Errorf("annotation %s is immutable once set", runsidentity.StoryRunTriggerTokenAnnotation)
	}
	return nil
}

func validateTriggerInputHashOnCreate(ctx context.Context, sr *runsv1alpha1.StoryRun) error {
	if sr == nil {
		return nil
	}
	ann := sr.GetAnnotations()
	if len(ann) == 0 {
		return nil
	}
	token := strings.TrimSpace(ann[runsidentity.StoryRunTriggerTokenAnnotation])
	if token == "" {
		return nil
	}
	hash := strings.TrimSpace(ann[runsidentity.StoryRunTriggerInputHashAnnotation])
	if hash == "" {
		return fmt.Errorf("annotation %s is required when %s is set", runsidentity.StoryRunTriggerInputHashAnnotation, runsidentity.StoryRunTriggerTokenAnnotation)
	}
	expected, err := computeStoryRunInputHash(ctx, sr)
	if err != nil {
		return err
	}
	if hash != expected {
		return fmt.Errorf("annotation %s must match the StoryRun inputs hash", runsidentity.StoryRunTriggerInputHashAnnotation)
	}
	return nil
}

func validateTriggerInputHashOnUpdate(ctx context.Context, _, newSr *runsv1alpha1.StoryRun) error {
	if newSr == nil {
		return nil
	}
	ann := newSr.GetAnnotations()
	if len(ann) == 0 {
		return nil
	}
	token := strings.TrimSpace(ann[runsidentity.StoryRunTriggerTokenAnnotation])
	if token == "" {
		return nil
	}
	newHash := strings.TrimSpace(ann[runsidentity.StoryRunTriggerInputHashAnnotation])
	if newHash == "" {
		return fmt.Errorf("annotation %s is required when %s is set", runsidentity.StoryRunTriggerInputHashAnnotation, runsidentity.StoryRunTriggerTokenAnnotation)
	}
	expected, err := computeStoryRunInputHash(ctx, newSr)
	if err != nil {
		return err
	}
	if newHash != expected {
		return fmt.Errorf("annotation %s must match the StoryRun inputs hash", runsidentity.StoryRunTriggerInputHashAnnotation)
	}
	return nil
}

func computeStoryRunInputHash(ctx context.Context, sr *runsv1alpha1.StoryRun) (string, error) {
	if sr == nil || sr.Spec.Inputs == nil || len(sr.Spec.Inputs.Raw) == 0 {
		return runsidentity.ComputeTriggerInputHash(nil)
	}

	var value any
	if err := json.Unmarshal(sr.Spec.Inputs.Raw, &value); err != nil {
		return "", fmt.Errorf("decode StoryRun inputs for trigger hash validation: %w", err)
	}
	if !storyRunInputsContainStorageRef(value) {
		return runsidentity.ComputeTriggerInputHashFromValue(value)
	}

	manager, err := storage.SharedManager(ctx)
	if err != nil {
		return "", fmt.Errorf("storyrun inputs reference offloaded data but storage is unavailable: %w", err)
	}
	if manager == nil {
		return "", fmt.Errorf("storyrun inputs reference offloaded data but storage is unavailable")
	}

	hydrated, err := manager.HydrateStorageRefsOnly(ctx, value)
	if err != nil {
		return "", fmt.Errorf("failed to hydrate offloaded StoryRun inputs for trigger hash validation: %w", err)
	}
	return runsidentity.ComputeTriggerInputHashFromValue(hydrated)
}

func storyRunInputsContainStorageRef(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		if ref, ok := typed[storage.StorageRefKey].(string); ok && ref != "" {
			return true
		}
		for _, nested := range typed {
			if storyRunInputsContainStorageRef(nested) {
				return true
			}
		}
	case []any:
		if slices.ContainsFunc(typed, storyRunInputsContainStorageRef) {
			return true
		}
	}
	return false
}

// fetchStory retrieves the Story referenced by the StoryRun.
//
// Behavior:
//   - Fetches the Story from the API server.
//   - Uses storyRef to determine namespace (defaults to StoryRun's namespace).
//   - Returns user-friendly error if Story does not exist.
//
// Arguments:
//   - ctx context.Context: for API call.
//   - c client.Client: Kubernetes client.
//   - sr *runsv1alpha1.StoryRun: contains the storyRef.
//
// Returns:
//   - *bubuv1alpha1.Story if found.
//   - types.NamespacedName used for the lookup.
//   - Error if not found or API call fails.
func fetchStory(ctx context.Context, c client.Reader, cfg *config.ControllerConfig, sr *runsv1alpha1.StoryRun) (*bubuv1alpha1.Story, types.NamespacedName, error) {
	story := &bubuv1alpha1.Story{}
	storyKey := sr.Spec.StoryRef.ToNamespacedName(sr)
	if err := webhookshared.ValidateCrossNamespaceReference(
		ctx,
		c,
		cfg,
		sr,
		"runs.bubustack.io",
		"StoryRun",
		"bubustack.io",
		"Story",
		storyKey.Namespace,
		storyKey.Name,
		"StoryRef",
	); err != nil {
		return nil, types.NamespacedName{}, err
	}
	if err := c.Get(ctx, storyKey, story); err != nil {
		if errors.IsNotFound(err) {
			return nil, storyKey, fmt.Errorf("referenced story '%s' not found", storyKey.String())
		}
		return nil, storyKey, fmt.Errorf("failed to get referenced story '%s': %w", storyKey.String(), err)
	}
	if strings.TrimSpace(sr.Spec.StoryRef.Version) != "" && strings.TrimSpace(story.Spec.Version) != strings.TrimSpace(sr.Spec.StoryRef.Version) {
		return nil, storyKey, fmt.Errorf("referenced story '%s' has version '%s', expected '%s'", storyKey.String(), story.Spec.Version, sr.Spec.StoryRef.Version)
	}
	return story, storyKey, nil
}

// validateInputsShapeAndSize ensures spec.inputs is a JSON object within size limits.
//
// Behavior:
//   - Skips validation if inputs is empty.
//   - Ensures inputs is a JSON object (not array or primitive).
//   - Ensures inputs doesn't exceed cfg.StoryRun.MaxInlineInputsSize.
//
// Arguments:
//   - cfg *config.ControllerConfig: for size limits.
//   - sr *runsv1alpha1.StoryRun: the StoryRun to validate.
//
// Returns:
//   - nil if inputs is valid or absent.
//   - Error describing the validation failure.
func validateInputsShapeAndSize(cfg *config.ControllerConfig, sr *runsv1alpha1.StoryRun) error {
	if sr.Spec.Inputs == nil || len(sr.Spec.Inputs.Raw) == 0 {
		return nil
	}
	b := webhookshared.TrimLeadingSpace(sr.Spec.Inputs.Raw)
	if err := webhookshared.EnsureJSONObject("spec.inputs", b); err != nil {
		return err
	}
	// Use StoryRun-specific knob; fall back to defaults if unset
	maxBytes := cfg.StoryRun.MaxInlineInputsSize
	if maxBytes <= 0 {
		maxBytes = config.DefaultControllerConfig().StoryRun.MaxInlineInputsSize
	}
	if err := webhookshared.EnforceMaxBytes(
		"spec.inputs",
		sr.Spec.Inputs.Raw,
		maxBytes,
		"Provide large inputs via an offloading mechanism instead of inlining",
	); err != nil {
		return err
	}
	return webhookshared.ValidateReferenceMaps(sr.Spec.Inputs.Raw, "spec.inputs")
}

// validateInputsSchema validates spec.inputs against the Story's inputsSchema.
//
// Behavior:
//   - Skips validation if Story has no inputsSchema.
//   - Uses gojsonschema to validate inputs against the schema.
//   - Returns all validation errors in a single message.
//
// Arguments:
//   - story *bubuv1alpha1.Story: provides the inputsSchema.
//   - sr *runsv1alpha1.StoryRun: provides the inputs to validate.
//
// Returns:
//   - nil if inputs is valid or schema is absent.
//   - Error describing the validation failures.
func validateInputsSchema(story *bubuv1alpha1.Story, sr *runsv1alpha1.StoryRun) error {
	if story.Spec.InputsSchema == nil || len(story.Spec.InputsSchema.Raw) == 0 {
		return nil
	}
	doc := []byte("{}")
	if sr.Spec.Inputs != nil && len(sr.Spec.Inputs.Raw) > 0 {
		doc = sr.Spec.Inputs.Raw
	}
	if webhookshared.ContainsStorageRef(doc) {
		scrubbed, err := webhookshared.ScrubStorageRefMetadata(doc)
		if err != nil {
			return fmt.Errorf("failed to sanitize storage ref metadata in spec.inputs: %w", err)
		}
		doc = scrubbed
	}
	if err := webhookshared.ValidateJSONAgainstSchema(doc, story.Spec.InputsSchema.Raw, "Story"); err != nil {
		return fmt.Errorf("spec.inputs is invalid against Story schema: %w", err)
	}
	return nil
}

// ensureStoryRunObservedGenerationMonotonic rejects stale status updates.
//
// Behavior:
//   - Compares old and new observedGeneration values.
//   - Rejects updates where observedGeneration decreases.
//   - Allows updates when old value is zero (unset).
//
// Arguments:
//   - oldSR *runsv1alpha1.StoryRun: previous StoryRun state.
//   - newSR *runsv1alpha1.StoryRun: proposed StoryRun state.
//
// Returns:
//   - nil if generation is monotonically increasing.
//   - Error if observedGeneration decreased.
func ensureStoryRunObservedGenerationMonotonic(oldSR, newSR *runsv1alpha1.StoryRun) error {
	oldGen := oldSR.Status.ObservedGeneration
	newGen := newSR.Status.ObservedGeneration
	if oldGen > 0 && newGen < oldGen {
		return fmt.Errorf("status.observedGeneration must be monotonically increasing (old=%d new=%d)", oldGen, newGen)
	}
	return nil
}

func warnOffloadedInputs(story *bubuv1alpha1.Story, sr *runsv1alpha1.StoryRun) admission.Warnings {
	if story == nil || sr == nil {
		return nil
	}
	if story.Spec.InputsSchema == nil || len(story.Spec.InputsSchema.Raw) == 0 {
		return nil
	}
	if sr.Spec.Inputs == nil || len(sr.Spec.Inputs.Raw) == 0 {
		return nil
	}
	if !webhookshared.ContainsStorageRef(sr.Spec.Inputs.Raw) {
		return nil
	}
	return admission.Warnings{
		"spec.inputs contains storage references; schema validation only checks reference shape, not offloaded content",
	}
}
