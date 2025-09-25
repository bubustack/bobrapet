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

// Package refs provides typed cross-resource references for BubuStack APIs.
//
// This package implements a secure, multi-tenant-safe reference system that
// enables resources to reference each other while maintaining namespace isolation
// and providing drift detection capabilities.
//
// Key features:
// - Type-safe references with validation
// - Multi-tenancy safety (same-namespace by default)
// - Drift detection via UID tracking
// - Explicit cross-namespace references when needed
//
// The reference system follows Kubernetes conventions and provides a foundation
// for building complex workflows while maintaining security and operational clarity.
package refs

import (
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ObjectReference represents a typed reference to a Kubernetes object.
// This is the base type for all BubuStack cross-resource references.
//
// Multi-tenancy is enforced by defaulting to same-namespace references.
// Cross-namespace references must be explicitly specified and should be
// carefully controlled through RBAC policies.
//
// Example same-namespace reference:
//
//	storyRef:
//	  name: "my-story"
//
// Example cross-namespace reference:
//
//	storyRef:
//	  name: "shared-story"
//	  namespace: "shared-workflows"
type ObjectReference struct {
	// Name of the referenced object.
	// Must be a valid DNS-1123 label as defined by Kubernetes naming conventions.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	Name string `json:"name"`

	// Namespace of the referenced object.
	// If empty, defaults to same namespace as the referencing object.
	//
	// Cross-namespace references should be used carefully and only when
	// multi-tenancy is properly configured. Consider the security implications:
	// - Ensure RBAC policies allow cross-namespace access
	// - Validate that the target namespace is trusted
	// - Monitor cross-namespace references for security compliance
	//
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	Namespace *string `json:"namespace,omitempty"`
}

// StoryReference represents a reference to a Story resource.
// Stories are the main workflow definitions in BubuStack and are frequently
// referenced by Impulses (triggers) and other Stories (sub-workflows).
//
// The UID field provides drift detection - controllers can detect when
// the referenced Story has been deleted and recreated, enabling appropriate
// error handling and user notification.
type StoryReference struct {
	ObjectReference `json:",inline"`

	// UID of the referenced Story for drift detection.
	// This field is populated by controllers and should not be set by users.
	//
	// When a controller resolves this reference, it stores the target's UID.
	// On subsequent reconciliations, if the UID has changed, it indicates
	// the target was deleted and recreated, which may require special handling.
	//
	// +kubebuilder:validation:Optional
	UID *types.UID `json:"uid,omitempty"`
}

// EngramReference provides a structured reference to an Engram.
type EngramReference struct {
	ObjectReference `json:",inline"`
	UID             *types.UID `json:"uid,omitempty"`
}

// EngramTemplateReference provides a structured reference to a cluster-scoped EngramTemplate.
// It omits the namespace field as the referenced object is cluster-scoped.
type EngramTemplateReference struct {
	// Name of the referenced EngramTemplate.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// UID of the referenced EngramTemplate for drift detection.
	// +kubebuilder:validation:Optional
	UID *types.UID `json:"uid,omitempty"`
}

// ImpulseTemplateReference provides a structured reference to a cluster-scoped ImpulseTemplate.
// It omits the namespace field as the referenced object is cluster-scoped.
type ImpulseTemplateReference struct {
	// Name of the referenced ImpulseTemplate.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`

	// UID of the referenced ImpulseTemplate for drift detection.
	// +kubebuilder:validation:Optional
	UID *types.UID `json:"uid,omitempty"`
}

// ImpulseReference provides a structured reference to an Impulse.
type ImpulseReference struct {
	ObjectReference `json:",inline"`
	UID             *types.UID `json:"uid,omitempty"`
}

// TemplateReference represents a reference to a Template resource.
// Templates are cluster-scoped resources that define capabilities and schemas.
//
// Since templates are cluster-scoped, they don't have a namespace field.
// Templates provide the foundation for Engrams and Impulses by defining
// what can be configured and how it should behave.
type TemplateReference struct {
	// Name of the referenced template.
	// Templates are cluster-scoped so no namespace is needed.
	//
	// Template names should be globally unique within the cluster and
	// follow semantic versioning or other naming conventions to enable
	// upgrades and compatibility management.
	//
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	Name string `json:"name"`

	// UID of the referenced template for drift detection.
	// This field is populated by controllers and should not be set by users.
	//
	// Template drift detection is particularly important because templates
	// define the ABI and behavior expectations for dependent resources.
	//
	// +kubebuilder:validation:Optional
	UID *types.UID `json:"uid,omitempty"`
}

// StoryRunReference provides a structured reference to a StoryRun.
type StoryRunReference struct {
	ObjectReference `json:",inline"`

	// UID of the referenced StoryRun for drift detection.
	// This field is populated by controllers and should not be set by users.
	//
	// StoryRun drift detection helps maintain execution integrity and
	// enables proper cleanup of orphaned StepRuns.
	//
	// +kubebuilder:validation:Optional
	UID *types.UID `json:"uid,omitempty"`
}

// ResolveNamespace determines the target namespace for a reference, defaulting to the
// namespace of the referencing object if the reference itself does not specify one.
// This is the standard pattern for enabling optional cross-namespace references.
func ResolveNamespace(referencingObject client.Object, ref *ObjectReference) string {
	if ref.Namespace != nil && *ref.Namespace != "" {
		return *ref.Namespace
	}
	return referencingObject.GetNamespace()
}

// ToNamespacedName converts an ObjectReference into a types.NamespacedName,
// using the provided object to determine the default namespace.
func (ref *ObjectReference) ToNamespacedName(referencingObject client.Object) types.NamespacedName {
	return types.NamespacedName{
		Name:      ref.Name,
		Namespace: ResolveNamespace(referencingObject, ref),
	}
}
