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
	"github.com/bubustack/bobrapet/pkg/enums"
)

// nolint:unused
// log is for logging in this package.
var storylog = logf.Log.WithName("story-resource")

// StoryWebhook sets up the webhook for Story in the manager.
type StoryWebhook struct {
	client.Client
	Config        *config.ControllerConfig
	ConfigManager *config.OperatorConfigManager
}

// SetupWebhookWithManager registers the webhook for Story in the manager.
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

func (d *StoryCustomDefaulter) controllerConfig() *config.ControllerConfig {
	if d.ConfigManager != nil {
		if cfg := d.ConfigManager.GetControllerConfig(); cfg != nil {
			return cfg
		}
	}
	if d.Config != nil {
		return d.Config
	}
	return config.DefaultControllerConfig()
}

// Default implements webhook.CustomDefaulter so a webhook will be registered for the Kind Story.
func (d *StoryCustomDefaulter) Default(_ context.Context, obj runtime.Object) error {
	story, ok := obj.(*bubushv1alpha1.Story)

	if !ok {
		return fmt.Errorf("expected an Story object but got %T", obj)
	}
	storylog.Info("Defaulting for Story", "name", story.GetName())

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

func (v *StoryCustomValidator) controllerConfig() *config.ControllerConfig {
	if v.ConfigManager != nil {
		if cfg := v.ConfigManager.GetControllerConfig(); cfg != nil {
			return cfg
		}
	}
	if v.Config != nil {
		return v.Config
	}
	return config.DefaultControllerConfig()
}

// ValidateCreate implements webhook.CustomValidator so a webhook will be registered for the type Story.
func (v *StoryCustomValidator) ValidateCreate(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	story, ok := obj.(*bubushv1alpha1.Story)
	if !ok {
		return nil, fmt.Errorf("expected a Story object but got %T", obj)
	}
	storylog.Info("Validation for Story upon creation", "name", story.GetName())

	webhookCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := v.validateStory(webhookCtx, story); err != nil {
		return nil, err
	}
	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator so a webhook will be registered for the type Story.
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

// ValidateDelete implements webhook.CustomValidator so a webhook will be registered for the type Story.
func (v *StoryCustomValidator) ValidateDelete(ctx context.Context, obj runtime.Object) (admission.Warnings, error) {
	story, ok := obj.(*bubushv1alpha1.Story)
	if !ok {
		return nil, fmt.Errorf("expected a Story object but got %T", obj)
	}
	storylog.Info("Validation for Story upon deletion", "name", story.GetName())

	return nil, nil
}

// validateStory performs basic invariants validation for Story specs.
// - Each step name must be unique and DNS-1123 compatible (enforced by CRD, but we double-check uniqueness)
// - If step.Ref is set, step.Type must be empty; if step.Type is set, Ref must be nil
// - All items in step.Needs must reference existing step names (no self-dependency)
// - For streaming pattern PerStory, referenced Engrams must be long-running modes is enforced at reconcile; here we only validate shape
func (v *StoryCustomValidator) validateStory(ctx context.Context, story *bubushv1alpha1.Story) error {
	if err := validateStorySize(story); err != nil {
		return err
	}
	cfg := v.controllerConfig()
	maxSize := pickStoryMaxWithSize(cfg)
	if err := validateOutputSize(story, maxSize); err != nil {
		return err
	}
	if err := validateStepsShape(ctx, v, story, maxSize); err != nil {
		return err
	}
	if err := validateNeedsExistence(story); err != nil {
		return err
	}
	if err := validateStepGraphAcyclic(story); err != nil {
		return err
	}
	if err := v.validateExecuteStoryReferences(ctx, story); err != nil {
		return err
	}
	return nil
}

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

func pickStoryMaxWithSize(cfg *config.ControllerConfig) int {
	if cfg == nil {
		cfg = config.DefaultControllerConfig()
	}
	maxSize := cfg.MaxStoryWithBlockSizeBytes
	if maxSize <= 0 {
		maxSize = config.DefaultControllerConfig().MaxStoryWithBlockSizeBytes
	}
	return maxSize
}

func validateOutputSize(story *bubushv1alpha1.Story, maxSize int) error {
	if story.Spec.Output != nil && len(story.Spec.Output.Raw) > maxSize {
		return fmt.Errorf("story 'output' block size of %d bytes exceeds maximum allowed size of %d bytes", len(story.Spec.Output.Raw), maxSize)
	}
	return nil
}

func validateStepsShape(ctx context.Context, v *StoryCustomValidator, story *bubushv1alpha1.Story, maxSize int) error {
	seen := make(map[string]struct{})
	for i := range story.Spec.Steps {
		s := &story.Spec.Steps[i]
		if _, exists := seen[s.Name]; exists {
			return fmt.Errorf("duplicate step name '%s'", s.Name)
		}
		seen[s.Name] = struct{}{}
		if s.Ref != nil && s.Type != "" {
			return fmt.Errorf("step '%s' must not set both ref and type", s.Name)
		}
		if s.Ref == nil && s.Type == "" {
			return fmt.Errorf("step '%s' must set either ref or type", s.Name)
		}
		for _, dep := range s.Needs {
			if dep == s.Name {
				return fmt.Errorf("step '%s' cannot depend on itself in needs", s.Name)
			}
		}
		if s.Ref != nil {
			if err := v.validateEngramStep(ctx, story.Namespace, s); err != nil {
				return err
			}
		}
		if s.With != nil && len(s.With.Raw) > maxSize {
			return fmt.Errorf("step '%s': 'with' block size of %d bytes exceeds maximum allowed size of %d bytes", s.Name, len(s.With.Raw), maxSize)
		}
		if err := validatePrimitiveShapes(s); err != nil {
			return err
		}
	}
	return nil
}

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
		if err := v.Client.Get(ctx, key, &target); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("step '%s' of type 'executeStory' references Story '%s/%s' which does not exist", step.Name, targetNamespace, withConfig.StoryRef.Name)
			}
			return fmt.Errorf("failed to validate executeStory reference for step '%s': %w", step.Name, err)
		}
	}
	return nil
}

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

func (v *StoryCustomValidator) validateEngramStep(ctx context.Context, namespace string, step *bubushv1alpha1.Step) error {
	// Fetch the Engram
	var engram bubushv1alpha1.Engram
	engramKey := types.NamespacedName{Name: step.Ref.Name, Namespace: namespace}
	if step.Ref.Namespace != nil && *step.Ref.Namespace != "" {
		engramKey.Namespace = *step.Ref.Namespace
	}
	if err := v.Client.Get(ctx, engramKey, &engram); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("step '%s' references engram '%s' which does not exist in namespace '%s'", step.Name, engramKey.Name, engramKey.Namespace)
		}
		return fmt.Errorf("failed to get engram for step '%s': %w", step.Name, err)
	}

	// Fetch the EngramTemplate
	var template v1alpha1.EngramTemplate
	if err := v.Client.Get(ctx, types.NamespacedName{Name: engram.Spec.TemplateRef.Name, Namespace: ""}, &template); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("step '%s' references engram '%s' which in turn references EngramTemplate '%s' that was not found", step.Name, engram.Name, engram.Spec.TemplateRef.Name)
		}
		return fmt.Errorf("failed to get EngramTemplate for step '%s': %w", step.Name, err)
	}

	// Validate the step's 'with' block against the template's inputSchema using shared validator.
	if step.With != nil && len(step.With.Raw) > 0 && template.Spec.InputSchema != nil && len(template.Spec.InputSchema.Raw) > 0 {
		if err := validateJSONAgainstSchema(step.With.Raw, template.Spec.InputSchema.Raw, "EngramTemplate"); err != nil {
			return fmt.Errorf("step '%s': %w", step.Name, err)
		}
	}

	return nil
}
