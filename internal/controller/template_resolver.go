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

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
)

// TemplateResolver handles template resolution with clear precedence rules
type TemplateResolver struct {
	client client.Client
}

// NewTemplateResolver creates a new template resolver
func NewTemplateResolver(client client.Client) *TemplateResolver {
	return &TemplateResolver{client: client}
}

// ResolvedEngram contains the resolved Engram configuration
type ResolvedEngram struct {
	// Final resolved configuration
	Image     string                            `json:"image"`
	Config    interface{}                       `json:"config,omitempty"`
	Resources *bubushv1alpha1.WorkloadResources `json:"resources,omitempty"`
	Security  *bubushv1alpha1.WorkloadSecurity  `json:"security,omitempty"`
	Retry     *bubushv1alpha1.RetryPolicy       `json:"retry,omitempty"`
	Timeout   string                            `json:"timeout,omitempty"`

	// Resolution metadata
	TemplateUsed string `json:"templateUsed,omitempty"`
	Source       string `json:"source"` // "direct", "template", "fallback"
}

// ResolvedImpulse contains the resolved Impulse configuration
type ResolvedImpulse struct {
	// Final resolved configuration
	Image     string                            `json:"image"`
	Config    interface{}                       `json:"config,omitempty"`
	Resources *bubushv1alpha1.WorkloadResources `json:"resources,omitempty"`
	Security  *bubushv1alpha1.WorkloadSecurity  `json:"security,omitempty"`
	Retry     *bubushv1alpha1.RetryPolicy       `json:"retry,omitempty"`
	Timeout   string                            `json:"timeout,omitempty"`

	// Resolution metadata
	TemplateUsed string `json:"templateUsed,omitempty"`
	Source       string `json:"source"` // "direct", "template", "fallback"
}

// ResolveEngram resolves an Engram configuration with clear precedence:
// 1. Direct specification in Engram spec
// 2. Template defaults from EngramTemplate
// 3. System fallbacks
func (tr *TemplateResolver) ResolveEngram(ctx context.Context, engram *bubushv1alpha1.Engram) (*ResolvedEngram, error) {
	resolved := &ResolvedEngram{}

	// Step 1: Try to resolve from template if templateRef is provided
	var template *catalogv1alpha1.EngramTemplate
	if engram.Spec.Engine != nil && engram.Spec.Engine.TemplateRef != "" {
		var tmpl catalogv1alpha1.EngramTemplate
		err := tr.client.Get(ctx, types.NamespacedName{Name: engram.Spec.Engine.TemplateRef}, &tmpl)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve EngramTemplate '%s': %w", engram.Spec.Engine.TemplateRef, err)
		}
		template = &tmpl
		resolved.TemplateUsed = engram.Spec.Engine.TemplateRef
		resolved.Source = "template"
	}

	// Step 2: Apply template defaults first (if available)
	if template != nil {
		if template.Spec.Image != "" {
			resolved.Image = template.Spec.Image
		}

		if template.Spec.Defaults != nil {
			if template.Spec.Defaults.Config != nil {
				resolved.Config = template.Spec.Defaults.Config
			}
			if template.Spec.Defaults.Resources != nil {
				resolved.Resources = tr.convertTemplateResources(template.Spec.Defaults.Resources)
			}
			if template.Spec.Defaults.Security != nil {
				resolved.Security = tr.convertTemplateSecurity(template.Spec.Defaults.Security)
			}
			if template.Spec.Defaults.Retry != nil {
				resolved.Retry = tr.convertTemplateRetry(template.Spec.Defaults.Retry)
			}
			if template.Spec.Defaults.Timeout != nil {
				resolved.Timeout = *template.Spec.Defaults.Timeout
			}
		}
	}

	// Step 3: Override with Engram-specific configuration (highest precedence)
	if engram.Spec.With != nil {
		resolved.Config = engram.Spec.With
		if template == nil {
			resolved.Source = "direct"
		}
	}

	if engram.Spec.Resources != nil {
		resolved.Resources = engram.Spec.Resources
	}

	if engram.Spec.Security != nil {
		resolved.Security = engram.Spec.Security
	}

	if engram.Spec.Retry != nil {
		resolved.Retry = engram.Spec.Retry
	}

	if engram.Spec.Timeout != "" {
		resolved.Timeout = engram.Spec.Timeout
	}

	// Step 4: Apply system fallbacks for missing required fields
	if resolved.Image == "" {
		resolved.Image = "ghcr.io/bubustack/engram-default:latest"
		if resolved.Source == "" {
			resolved.Source = "fallback"
		}
	}

	if resolved.Timeout == "" {
		resolved.Timeout = "5m" // Default execution timeout
	}

	return resolved, nil
}

// ResolveImpulse resolves an Impulse configuration with clear precedence:
// 1. Direct specification in Impulse spec
// 2. Template defaults from ImpulseTemplate
// 3. System fallbacks
func (tr *TemplateResolver) ResolveImpulse(ctx context.Context, impulse *bubushv1alpha1.Impulse) (*ResolvedImpulse, error) {
	resolved := &ResolvedImpulse{}

	// Step 1: Try to resolve from template if templateRef is provided
	var template *catalogv1alpha1.ImpulseTemplate
	if impulse.Spec.Engine != nil && impulse.Spec.Engine.TemplateRef != "" {
		var tmpl catalogv1alpha1.ImpulseTemplate
		err := tr.client.Get(ctx, types.NamespacedName{Name: impulse.Spec.Engine.TemplateRef}, &tmpl)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve ImpulseTemplate '%s': %w", impulse.Spec.Engine.TemplateRef, err)
		}
		template = &tmpl
		resolved.TemplateUsed = impulse.Spec.Engine.TemplateRef
		resolved.Source = "template"
	}

	// Step 2: Apply template defaults first (if available)
	if template != nil {
		if template.Spec.Image != "" {
			resolved.Image = template.Spec.Image
		}

		if template.Spec.Defaults != nil {
			if template.Spec.Defaults.Config != nil {
				resolved.Config = template.Spec.Defaults.Config
			}
			if template.Spec.Defaults.Resources != nil {
				resolved.Resources = tr.convertTemplateResources(template.Spec.Defaults.Resources)
			}
			if template.Spec.Defaults.Security != nil {
				resolved.Security = tr.convertTemplateSecurity(template.Spec.Defaults.Security)
			}
			if template.Spec.Defaults.Retry != nil {
				resolved.Retry = tr.convertTemplateRetry(template.Spec.Defaults.Retry)
			}
			if template.Spec.Defaults.Timeout != nil {
				resolved.Timeout = *template.Spec.Defaults.Timeout
			}
		}
	}

	// Step 3: Override with Impulse-specific configuration (highest precedence)
	if impulse.Spec.With != nil {
		resolved.Config = impulse.Spec.With
		if template == nil {
			resolved.Source = "direct"
		}
	}

	if impulse.Spec.Resources != nil {
		resolved.Resources = impulse.Spec.Resources
	}

	if impulse.Spec.Security != nil {
		resolved.Security = impulse.Spec.Security
	}

	if impulse.Spec.Retry != nil {
		resolved.Retry = impulse.Spec.Retry
	}

	if impulse.Spec.Timeout != "" {
		resolved.Timeout = impulse.Spec.Timeout
	}

	// Step 4: Apply system fallbacks for missing required fields
	if resolved.Image == "" {
		resolved.Image = "ghcr.io/bubustack/impulse-default:latest"
		if resolved.Source == "" {
			resolved.Source = "fallback"
		}
	}

	if resolved.Timeout == "" {
		resolved.Timeout = "30s" // Default trigger timeout
	}

	return resolved, nil
}

// Helper functions to convert template defaults to runtime types

func (tr *TemplateResolver) convertTemplateResources(defaults *catalogv1alpha1.DefaultWorkloadResources) *bubushv1alpha1.WorkloadResources {
	if defaults == nil {
		return nil
	}

	resources := &bubushv1alpha1.WorkloadResources{
		NodeSelector:           defaults.NodeSelector,
		MaxConcurrentInstances: defaults.MaxConcurrentInstances,
		MaxExecutionTime:       defaults.MaxExecutionTime,
		MaxRetries:             defaults.MaxRetries,
	}

	if defaults.Requests != nil {
		resources.Requests = &bubushv1alpha1.ResourceRequests{
			CPU:              tr.stringValue(defaults.Requests.CPU),
			Memory:           tr.stringValue(defaults.Requests.Memory),
			EphemeralStorage: tr.stringValue(defaults.Requests.EphemeralStorage),
			GPU:              tr.stringValue(defaults.Requests.GPU),
		}
	}

	if defaults.Limits != nil {
		resources.Limits = &bubushv1alpha1.ResourceLimits{
			CPU:              tr.stringValue(defaults.Limits.CPU),
			Memory:           tr.stringValue(defaults.Limits.Memory),
			EphemeralStorage: tr.stringValue(defaults.Limits.EphemeralStorage),
			GPU:              tr.stringValue(defaults.Limits.GPU),
			MaxFileSize:      tr.stringValue(defaults.Limits.MaxFileSize),
			MaxArtifactSize:  tr.stringValue(defaults.Limits.MaxArtifactSize),
			MaxNetworkConns:  defaults.Limits.MaxNetworkConns,
		}
	}

	return resources
}

func (tr *TemplateResolver) convertTemplateSecurity(defaults *catalogv1alpha1.DefaultWorkloadSecurity) *bubushv1alpha1.WorkloadSecurity {
	if defaults == nil {
		return nil
	}

	return &bubushv1alpha1.WorkloadSecurity{
		RunAsNonRoot:             tr.boolValue(defaults.RunAsNonRoot),
		AllowPrivilegeEscalation: tr.boolValue(defaults.AllowPrivilegeEscalation),
		RequiredSecrets:          defaults.RequiredSecrets,
		NetworkPolicy:            tr.stringValue(defaults.NetworkPolicy),
	}
}

func (tr *TemplateResolver) convertTemplateRetry(defaults *catalogv1alpha1.DefaultRetryPolicy) *bubushv1alpha1.RetryPolicy {
	if defaults == nil {
		return nil
	}

	return &bubushv1alpha1.RetryPolicy{
		MaxRetries: tr.intValue(defaults.MaxRetries),
		Backoff:    tr.stringValue(defaults.Backoff),
		BaseDelay:  tr.stringValue(defaults.BaseDelay),
		MaxDelay:   tr.stringValue(defaults.MaxDelay),
	}
}

// Helper functions for safe pointer dereferencing

func (tr *TemplateResolver) stringValue(ptr *string) string {
	if ptr == nil {
		return ""
	}
	return *ptr
}

func (tr *TemplateResolver) boolValue(ptr *bool) bool {
	if ptr == nil {
		return false
	}
	return *ptr
}

func (tr *TemplateResolver) intValue(ptr *int) int {
	if ptr == nil {
		return 0
	}
	return *ptr
}

// ValidateTemplateCompatibility validates that an Engram/Impulse is compatible with its template
func (tr *TemplateResolver) ValidateTemplateCompatibility(ctx context.Context, templateRef, mode, templateType string) error {
	if templateRef == "" {
		return nil // No template to validate
	}

	switch templateType {
	case "engram":
		var template catalogv1alpha1.EngramTemplate
		err := tr.client.Get(ctx, types.NamespacedName{Name: templateRef}, &template)
		if err != nil {
			return fmt.Errorf("EngramTemplate '%s' not found: %w", templateRef, err)
		}

		// Validate mode compatibility
		if !tr.containsMode(template.Spec.SupportedModes, mode) {
			return fmt.Errorf("EngramTemplate '%s' does not support mode '%s'. Supported modes: %v",
				templateRef, mode, template.Spec.SupportedModes)
		}

	case "impulse":
		var template catalogv1alpha1.ImpulseTemplate
		err := tr.client.Get(ctx, types.NamespacedName{Name: templateRef}, &template)
		if err != nil {
			return fmt.Errorf("ImpulseTemplate '%s' not found: %w", templateRef, err)
		}

		// Validate mode compatibility (impulses only support deployment/statefulset)
		if !tr.containsMode(template.Spec.SupportedModes, mode) {
			return fmt.Errorf("ImpulseTemplate '%s' does not support mode '%s'. Supported modes: %v",
				templateRef, mode, template.Spec.SupportedModes)
		}

		if mode == "job" {
			return fmt.Errorf("Impulses cannot use job mode, only deployment or statefulset")
		}

	default:
		return fmt.Errorf("unknown template type: %s", templateType)
	}

	return nil
}

func (tr *TemplateResolver) containsMode(supportedModes []string, mode string) bool {
	for _, supported := range supportedModes {
		if supported == mode {
			return true
		}
	}
	return false
}
