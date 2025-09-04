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
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
)

// Standard finalizer names
const (
	// Story finalizers
	StoryFinalizer          = "bubu.sh/story-cleanup"
	StoryReferenceFinalizer = "bubu.sh/story-reference-cleanup"

	// Engram finalizers
	EngramFinalizer          = "bubu.sh/engram-cleanup"
	EngramReferenceFinalizer = "bubu.sh/engram-reference-cleanup"

	// Impulse finalizers
	ImpulseFinalizer          = "bubu.sh/impulse-cleanup"
	ImpulseFinalizerName      = "bubu.sh/impulse-cleanup" // Alias for backward compatibility
	ImpulseReferenceFinalizer = "bubu.sh/impulse-reference-cleanup"

	// Execution finalizers
	StoryRunFinalizer = "runs.bubu.sh/storyrun-cleanup"
	StepRunFinalizer  = "runs.bubu.sh/steprun-cleanup"

	// Template finalizers (cluster-scoped)
	EngramTemplateFinalizer  = "catalog.bubu.sh/engramtemplate-cleanup"
	ImpulseTemplateFinalizer = "catalog.bubu.sh/impulsetemplate-cleanup"
)

// FinalizerManager provides standardized finalizer management
type FinalizerManager struct {
	client client.Client
}

// NewFinalizerManager creates a new finalizer manager
func NewFinalizerManager(client client.Client) *FinalizerManager {
	return &FinalizerManager{client: client}
}

// AddFinalizer adds a finalizer if not already present
func (fm *FinalizerManager) AddFinalizer(obj metav1.Object, finalizer string) bool {
	finalizers := obj.GetFinalizers()
	for _, f := range finalizers {
		if f == finalizer {
			return false // Already present
		}
	}
	finalizers = append(finalizers, finalizer)
	obj.SetFinalizers(finalizers)
	return true
}

// RemoveFinalizer removes a finalizer
func (fm *FinalizerManager) RemoveFinalizer(obj metav1.Object, finalizer string) bool {
	finalizers := obj.GetFinalizers()
	var result []string
	found := false
	for _, f := range finalizers {
		if f != finalizer {
			result = append(result, f)
		} else {
			found = true
		}
	}
	if found {
		obj.SetFinalizers(result)
	}
	return found
}

// HasFinalizer checks if a finalizer is present
func (fm *FinalizerManager) HasFinalizer(obj metav1.Object, finalizer string) bool {
	for _, f := range obj.GetFinalizers() {
		if f == finalizer {
			return true
		}
	}
	return false
}

// ReferenceValidator validates cross-resource references
type ReferenceValidator struct {
	client client.Client
}

// NewReferenceValidator creates a new reference validator
func NewReferenceValidator(client client.Client) *ReferenceValidator {
	return &ReferenceValidator{client: client}
}

// ValidateStoryReference validates a Story reference
func (rv *ReferenceValidator) ValidateStoryReference(ctx context.Context, storyRef, namespace string) error {
	if storyRef == "" {
		return fmt.Errorf("storyRef cannot be empty")
	}

	// Parse potential cross-namespace reference (ns/name format)
	targetNS, name := rv.parseReference(storyRef, namespace)

	var story bubushv1alpha1.Story
	err := rv.client.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: targetNS,
	}, &story)

	if err != nil {
		return fmt.Errorf("story reference validation failed: %w", err)
	}

	// Validate story is ready
	if !IsReady(story.Status.Conditions) {
		return fmt.Errorf("referenced story '%s' is not ready", storyRef)
	}

	return nil
}

// ValidateEngramReference validates an Engram reference
func (rv *ReferenceValidator) ValidateEngramReference(ctx context.Context, engramRef, namespace string) error {
	if engramRef == "" {
		return fmt.Errorf("engramRef cannot be empty")
	}

	// Parse potential cross-namespace reference
	targetNS, name := rv.parseReference(engramRef, namespace)

	var engram bubushv1alpha1.Engram
	err := rv.client.Get(ctx, types.NamespacedName{
		Name:      name,
		Namespace: targetNS,
	}, &engram)

	if err != nil {
		return fmt.Errorf("engram reference validation failed: %w", err)
	}

	// Validate engram is ready
	if !IsReady(engram.Status.Conditions) {
		return fmt.Errorf("referenced engram '%s' is not ready", engramRef)
	}

	return nil
}

// ValidateTemplateReference validates a template reference (cluster-scoped)
func (rv *ReferenceValidator) ValidateTemplateReference(ctx context.Context, templateRef string, templateType string) error {
	if templateRef == "" {
		return fmt.Errorf("templateRef cannot be empty")
	}

	switch templateType {
	case "engram":
		var template catalogv1alpha1.EngramTemplate
		err := rv.client.Get(ctx, types.NamespacedName{Name: templateRef}, &template)
		if err != nil {
			return fmt.Errorf("engramtemplate reference validation failed: %w", err)
		}

		// Validate template is ready
		if !IsReady(template.Status.Conditions) {
			return fmt.Errorf("referenced engramtemplate '%s' is not ready", templateRef)
		}

	case "impulse":
		var template catalogv1alpha1.ImpulseTemplate
		err := rv.client.Get(ctx, types.NamespacedName{Name: templateRef}, &template)
		if err != nil {
			return fmt.Errorf("impulsetemplate reference validation failed: %w", err)
		}

		// Validate template is ready
		if !IsReady(template.Status.Conditions) {
			return fmt.Errorf("referenced impulsetemplate '%s' is not ready", templateRef)
		}

	default:
		return fmt.Errorf("unknown template type: %s", templateType)
	}

	return nil
}

// ValidateStoryRunOwnership validates StoryRun → StepRun ownership
func (rv *ReferenceValidator) ValidateStoryRunOwnership(ctx context.Context, stepRun *runsv1alpha1.StepRun) error {
	if stepRun.Spec.StoryRunRef == "" {
		return fmt.Errorf("storyRunRef cannot be empty")
	}

	var storyRun runsv1alpha1.StoryRun
	err := rv.client.Get(ctx, types.NamespacedName{
		Name:      stepRun.Spec.StoryRunRef,
		Namespace: stepRun.Namespace, // StepRun must be in same namespace as StoryRun
	}, &storyRun)

	if err != nil {
		return fmt.Errorf("storyrun reference validation failed: %w", err)
	}

	// Validate ownership
	for _, ownerRef := range stepRun.GetOwnerReferences() {
		if ownerRef.Kind == "StoryRun" && ownerRef.Name == stepRun.Spec.StoryRunRef {
			return nil // Valid ownership found
		}
	}

	return fmt.Errorf("steprun '%s' is not owned by storyrun '%s'", stepRun.Name, stepRun.Spec.StoryRunRef)
}

// FindReferencingResources finds resources that reference the given resource
func (rv *ReferenceValidator) FindReferencingResources(ctx context.Context, resourceType, name, namespace string) ([]string, error) {
	var references []string

	switch resourceType {
	case "Story":
		// Find Impulses referencing this Story
		var impulses bubushv1alpha1.ImpulseList
		err := rv.client.List(ctx, &impulses)
		if err != nil {
			return nil, err
		}

		for _, impulse := range impulses.Items {
			targetNS, targetName := rv.parseReference(impulse.Spec.StoryRef, impulse.Namespace)
			if targetName == name && targetNS == namespace {
				references = append(references, fmt.Sprintf("Impulse:%s/%s", impulse.Namespace, impulse.Name))
			}
		}

		// Find StoryRuns referencing this Story
		var storyRuns runsv1alpha1.StoryRunList
		err = rv.client.List(ctx, &storyRuns)
		if err != nil {
			return nil, err
		}

		for _, storyRun := range storyRuns.Items {
			targetNS, targetName := rv.parseReference(storyRun.Spec.StoryRef, storyRun.Namespace)
			if targetName == name && targetNS == namespace {
				references = append(references, fmt.Sprintf("StoryRun:%s/%s", storyRun.Namespace, storyRun.Name))
			}
		}

	case "Engram":
		// Find StepRuns referencing this Engram
		var stepRuns runsv1alpha1.StepRunList
		err := rv.client.List(ctx, &stepRuns)
		if err != nil {
			return nil, err
		}

		for _, stepRun := range stepRuns.Items {
			if stepRun.Spec.EngramRef != "" {
				targetNS, targetName := rv.parseReference(stepRun.Spec.EngramRef, stepRun.Namespace)
				if targetName == name && targetNS == namespace {
					references = append(references, fmt.Sprintf("StepRun:%s/%s", stepRun.Namespace, stepRun.Name))
				}
			}
		}

		// TODO: Find Stories referencing this Engram in their steps

	case "EngramTemplate":
		// Find Engrams referencing this template
		var engrams bubushv1alpha1.EngramList
		err := rv.client.List(ctx, &engrams)
		if err != nil {
			return nil, err
		}

		for _, engram := range engrams.Items {
			if engram.Spec.Engine != nil && engram.Spec.Engine.TemplateRef == name {
				references = append(references, fmt.Sprintf("Engram:%s/%s", engram.Namespace, engram.Name))
			}
		}

	case "ImpulseTemplate":
		// Find Impulses referencing this template
		var impulses bubushv1alpha1.ImpulseList
		err := rv.client.List(ctx, &impulses)
		if err != nil {
			return nil, err
		}

		for _, impulse := range impulses.Items {
			if impulse.Spec.Engine != nil && impulse.Spec.Engine.TemplateRef == name {
				references = append(references, fmt.Sprintf("Impulse:%s/%s", impulse.Namespace, impulse.Name))
			}
		}
	}

	return references, nil
}

// parseReference parses a reference in "ns/name" or "name" format
func (rv *ReferenceValidator) parseReference(ref, defaultNamespace string) (namespace, name string) {
	return ParseReference(ref, defaultNamespace)
}

// OrphanDetector detects orphaned resources with broken references
type OrphanDetector struct {
	client    client.Client
	validator *ReferenceValidator
}

// NewOrphanDetector creates a new orphan detector
func NewOrphanDetector(client client.Client) *OrphanDetector {
	return &OrphanDetector{
		client:    client,
		validator: NewReferenceValidator(client),
	}
}

// DetectOrphanedStoryRuns finds StoryRuns with broken Story references
func (od *OrphanDetector) DetectOrphanedStoryRuns(ctx context.Context, namespace string) ([]runsv1alpha1.StoryRun, error) {
	var orphaned []runsv1alpha1.StoryRun

	var storyRuns runsv1alpha1.StoryRunList
	listOpts := []client.ListOption{}
	if namespace != "" {
		listOpts = append(listOpts, client.InNamespace(namespace))
	}

	err := od.client.List(ctx, &storyRuns, listOpts...)
	if err != nil {
		return nil, err
	}

	for _, storyRun := range storyRuns.Items {
		err := od.validator.ValidateStoryReference(ctx, storyRun.Spec.StoryRef, storyRun.Namespace)
		if err != nil {
			orphaned = append(orphaned, storyRun)
		}
	}

	return orphaned, nil
}

// DetectOrphanedStepRuns finds StepRuns with broken StoryRun references
func (od *OrphanDetector) DetectOrphanedStepRuns(ctx context.Context, namespace string) ([]runsv1alpha1.StepRun, error) {
	var orphaned []runsv1alpha1.StepRun

	var stepRuns runsv1alpha1.StepRunList
	listOpts := []client.ListOption{}
	if namespace != "" {
		listOpts = append(listOpts, client.InNamespace(namespace))
	}

	err := od.client.List(ctx, &stepRuns, listOpts...)
	if err != nil {
		return nil, err
	}

	for _, stepRun := range stepRuns.Items {
		err := od.validator.ValidateStoryRunOwnership(ctx, &stepRun)
		if err != nil {
			orphaned = append(orphaned, stepRun)
		}
	}

	return orphaned, nil
}
