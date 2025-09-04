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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Shared template types used by both EngramTemplate and ImpulseTemplate
// This ensures consistency between catalog entries

// TemplateSpec defines common fields for both EngramTemplate and ImpulseTemplate
type TemplateSpec struct {
	// Version of this template
	Version string `json:"version"`

	// Human-readable description
	Description string `json:"description,omitempty"`

	// Supported execution modes (job, deployment, statefulset)
	SupportedModes []string `json:"supportedModes"`

	// Configuration schema for the component itself (optional, for validation and tooling)
	ConfigSchema *runtime.RawExtension `json:"configSchema,omitempty"`

	// UI hints for builders/editors
	UIHints *UIHints `json:"uiHints,omitempty"`

	// Default configuration that can be overridden by users
	Defaults *TemplateDefaults `json:"defaults,omitempty"`

	// Example configurations
	Examples []Example `json:"examples,omitempty"`

	// OCI image reference
	Image string `json:"image,omitempty"`

	// Resource requirements and hints
	Resources *ResourceHints `json:"resources,omitempty"`

	// Security requirements
	Security *SecurityHints `json:"security,omitempty"`
}

// TemplateDefaults provides default configuration that users can override
// Maps to the same structure as Engram/Impulse specs
type TemplateDefaults struct {
	// Default workload configuration
	Workload *DefaultWorkloadConfig `json:"workload,omitempty"`

	// Default retry policy
	Retry *DefaultRetryPolicy `json:"retry,omitempty"`

	// Default timeout
	Timeout *string `json:"timeout,omitempty"`

	// Default resource configuration
	Resources *DefaultWorkloadResources `json:"resources,omitempty"`

	// Default security configuration
	Security *DefaultWorkloadSecurity `json:"security,omitempty"`

	// Default container configuration (passed to 'with' field)
	Config *runtime.RawExtension `json:"config,omitempty"`
}

// DefaultWorkloadConfig provides workload defaults (maps to WorkloadConfig)
type DefaultWorkloadConfig struct {
	Replicas                *int32                    `json:"replicas,omitempty"`
	Parallelism             *int32                    `json:"parallelism,omitempty"`
	Completions             *int32                    `json:"completions,omitempty"`
	UpdateStrategy          *DefaultUpdateStrategy    `json:"updateStrategy,omitempty"`
	Job                     *DefaultJobConfig         `json:"job,omitempty"`
	StatefulSet             *DefaultStatefulSetConfig `json:"statefulSet,omitempty"`
	ActiveDeadlineSeconds   *int64                    `json:"activeDeadlineSeconds,omitempty"`
	TTLSecondsAfterFinished *int32                    `json:"ttlSecondsAfterFinished,omitempty"`
}

// DefaultUpdateStrategy provides update strategy defaults
type DefaultUpdateStrategy struct {
	Type          *string                     `json:"type,omitempty"`
	RollingUpdate *DefaultRollingUpdateConfig `json:"rollingUpdate,omitempty"`
}

// DefaultRollingUpdateConfig provides rolling update defaults
type DefaultRollingUpdateConfig struct {
	MaxUnavailable *string `json:"maxUnavailable,omitempty"`
	MaxSurge       *string `json:"maxSurge,omitempty"`
}

// DefaultJobConfig provides job defaults
type DefaultJobConfig struct {
	BackoffLimit   *int32  `json:"backoffLimit,omitempty"`
	CompletionMode *string `json:"completionMode,omitempty"`
	Suspend        *bool   `json:"suspend,omitempty"`
}

// DefaultStatefulSetConfig provides StatefulSet defaults
type DefaultStatefulSetConfig struct {
	ServiceName         *string `json:"serviceName,omitempty"`
	PodManagementPolicy *string `json:"podManagementPolicy,omitempty"`
	// Note: VolumeClaimTemplates typically not defaulted, users specify them
}

// DefaultRetryPolicy provides retry defaults (maps to RetryPolicy)
type DefaultRetryPolicy struct {
	MaxRetries *int    `json:"maxRetries,omitempty"`
	Backoff    *string `json:"backoff,omitempty"`
	BaseDelay  *string `json:"baseDelay,omitempty"`
	MaxDelay   *string `json:"maxDelay,omitempty"`
}

// DefaultWorkloadResources provides resource defaults (maps to WorkloadResources)
type DefaultWorkloadResources struct {
	Requests               *DefaultResourceRequests `json:"requests,omitempty"`
	Limits                 *DefaultResourceLimits   `json:"limits,omitempty"`
	NodeSelector           map[string]string        `json:"nodeSelector,omitempty"`
	Tolerations            []string                 `json:"tolerations,omitempty"`
	MaxConcurrentInstances *int32                   `json:"maxConcurrentInstances,omitempty"`
	MaxExecutionTime       *string                  `json:"maxExecutionTime,omitempty"`
	MaxRetries             *int32                   `json:"maxRetries,omitempty"`
}

// DefaultResourceRequests provides resource request defaults
type DefaultResourceRequests struct {
	CPU              *string `json:"cpu,omitempty"`
	Memory           *string `json:"memory,omitempty"`
	EphemeralStorage *string `json:"ephemeralStorage,omitempty"`
	GPU              *string `json:"gpu,omitempty"`
}

// DefaultResourceLimits provides resource limit defaults
type DefaultResourceLimits struct {
	CPU              *string `json:"cpu,omitempty"`
	Memory           *string `json:"memory,omitempty"`
	EphemeralStorage *string `json:"ephemeralStorage,omitempty"`
	GPU              *string `json:"gpu,omitempty"`
	MaxFileSize      *string `json:"maxFileSize,omitempty"`
	MaxArtifactSize  *string `json:"maxArtifactSize,omitempty"`
	MaxNetworkConns  *int32  `json:"maxNetworkConns,omitempty"`
}

// DefaultWorkloadSecurity provides security defaults (maps to WorkloadSecurity)
type DefaultWorkloadSecurity struct {
	RunAsNonRoot             *bool    `json:"runAsNonRoot,omitempty"`
	AllowPrivilegeEscalation *bool    `json:"allowPrivilegeEscalation,omitempty"`
	RequiredSecrets          []string `json:"requiredSecrets,omitempty"`
	NetworkPolicy            *string  `json:"networkPolicy,omitempty"`
}

// UIHints provides UI/UX guidance for builders and editors
type UIHints struct {
	Category    string            `json:"category,omitempty"`    // messaging, ai, data, etc.
	Icon        string            `json:"icon,omitempty"`        // icon identifier
	Color       string            `json:"color,omitempty"`       // hex color
	Tags        []string          `json:"tags,omitempty"`        // search tags
	DisplayName string            `json:"displayName,omitempty"` // human-readable name
	Layout      map[string]string `json:"layout,omitempty"`      // UI layout hints
}

// Example provides template usage examples
type Example struct {
	Name        string                `json:"name"`
	Description string                `json:"description,omitempty"`
	Input       *runtime.RawExtension `json:"input,omitempty"`
	Config      *runtime.RawExtension `json:"config,omitempty"`
	Output      *runtime.RawExtension `json:"output,omitempty"`
}

// ResourceHints provides resource requirement guidance
type ResourceHints struct {
	CPU    string `json:"cpu,omitempty"`    // e.g., "100m"
	Memory string `json:"memory,omitempty"` // e.g., "256Mi"
	GPU    string `json:"gpu,omitempty"`    // e.g., "nvidia.com/gpu=1"

	// Node selection hints
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	Tolerations  []string          `json:"tolerations,omitempty"`
}

// SecurityHints provides security requirement guidance
type SecurityHints struct {
	RequiredSecrets  []string `json:"requiredSecrets,omitempty"`  // secret names needed
	NetworkAccess    []string `json:"networkAccess,omitempty"`    // external|internal|none
	PrivilegedAccess bool     `json:"privilegedAccess,omitempty"` // requires elevated privileges
}

// TemplateStatus defines common status fields for both templates
type TemplateStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Usage statistics
	UsageCount int32 `json:"usageCount,omitempty"`

	// Validation status
	ValidationStatus string   `json:"validationStatus,omitempty"` // valid|invalid|unknown
	ValidationErrors []string `json:"validationErrors,omitempty"`
}
