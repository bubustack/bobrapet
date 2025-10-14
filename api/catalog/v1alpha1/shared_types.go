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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/bubustack/bobrapet/pkg/enums"
)

// Shared template types used by both EngramTemplate and ImpulseTemplate
// Templates are like "packages" or "apps" in an app store - they define capabilities and contracts
// Think of them as reusable components that teams can share and build upon

// TemplateSpec defines the common "package definition" fields for templates
// This is like a package.json, Chart.yaml, or Dockerfile - it describes what the component can do
type TemplateSpec struct {
	// Version of this template package (use semantic versioning like "1.2.3")
	// This allows users to pin to specific versions or upgrade safely
	Version string `json:"version"`

	// Human-readable description of what this template does
	// Examples: "HTTP client with retry and timeout support", "OpenAI GPT integration"
	Description string `json:"description,omitempty"`

	// Which Kubernetes workload types this template supports
	// - job: One-shot tasks (like batch processing, CI builds)
	// - deployment: Always-on stateless services (like APIs, webhooks)
	// - statefulset: Always-on stateful services (like databases, file processors)
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=3
	SupportedModes []enums.WorkloadMode `json:"supportedModes"`

	// JSON Schema defining how users can configure this template
	// This validates the "with" field when users create Engrams/Impulses
	// Example: {"timeout": {"type": "string"}, "retries": {"type": "integer"}}
	// +kubebuilder:pruning:PreserveUnknownFields
	ConfigSchema *runtime.RawExtension `json:"configSchema,omitempty"`

	// Secret definitions that this template can use
	// Defines what secrets this template expects and how they should be structured
	// Users must provide corresponding secrets when creating Engrams/Impulses
	SecretSchema map[string]SecretDefinition `json:"secretSchema,omitempty"`

	// Container image that implements this template's functionality
	// This is the actual code that runs when someone uses this template
	// +kubebuilder:validation:MaxLength=512
	Image string `json:"image,omitempty"`

	// Default execution settings that template authors recommend
	// These provide sensible defaults but can be overridden by users
	Execution *TemplateExecutionPolicy `json:"execution,omitempty"`
}

// TemplateExecutionPolicy defines what template authors recommend for running this component
// These are the "manufacturer's recommended settings" that provide good defaults
// Users can override these when creating Engrams/Impulses if they have special requirements
type TemplateExecutionPolicy struct {
	// Image handling recommendations (pull policies, etc.)
	Images *TemplateImagePolicy `json:"images,omitempty"`

	// Resource recommendations (how much CPU/memory this typically needs)
	Resources *TemplateResourcePolicy `json:"resources,omitempty"`

	// Security requirements and recommendations
	Security *TemplateSecurityPolicy `json:"security,omitempty"`

	// Job-specific recommendations (for templates that run as jobs)
	Job *TemplateJobPolicy `json:"job,omitempty"`

	// Retry behavior recommendations
	Retry *TemplateRetryPolicy `json:"retry,omitempty"`

	// Default timeout recommendation
	Timeout *string `json:"timeout,omitempty"`

	// Service exposure defaults for realtime components (and impulses)
	Service *TemplateServicePolicy `json:"service,omitempty"`

	// Health check probes (liveness, readiness, startup)
	// These are defined using standard Kubernetes probe format
	Probes *TemplateProbePolicy `json:"probes,omitempty"`
}

// TemplateImagePolicy defines image-related template defaults
// Note: We don't include registry preferences as Kubernetes handles this natively
type TemplateImagePolicy struct {
	// Default image pull policy for this template (Always, Never, IfNotPresent)
	PullPolicy *string `json:"pullPolicy,omitempty"`
}

// TemplateResourcePolicy defines resource defaults for this template
type TemplateResourcePolicy struct {
	// Recommended CPU request for this template
	RecommendedCPURequest *string `json:"recommendedCpuRequest,omitempty"`
	// Recommended CPU limit for this template
	RecommendedCPULimit *string `json:"recommendedCpuLimit,omitempty"`
	// Recommended memory request for this template
	RecommendedMemoryRequest *string `json:"recommendedMemoryRequest,omitempty"`
	// Recommended memory limit for this template
	RecommendedMemoryLimit *string `json:"recommendedMemoryLimit,omitempty"`
	// Minimum resource requirements for this template
	MinCPURequest    *string `json:"minCpuRequest,omitempty"`
	MinMemoryRequest *string `json:"minMemoryRequest,omitempty"`
}

// TemplateSecurityPolicy defines security defaults for this template
type TemplateSecurityPolicy struct {
	// Whether this template requires running as non-root
	RequiresNonRoot *bool `json:"requiresNonRoot,omitempty"`
	// Whether this template requires read-only root filesystem
	RequiresReadOnlyRoot *bool `json:"requiresReadOnlyRoot,omitempty"`
	// Whether this template requires no privilege escalation
	RequiresNoPrivilegeEscalation *bool `json:"requiresNoPrivilegeEscalation,omitempty"`
	// Recommended user ID for this template
	RecommendedRunAsUser *int64 `json:"recommendedRunAsUser,omitempty"`
}

// TemplateJobPolicy defines job configuration defaults for this template
type TemplateJobPolicy struct {
	// Recommended backoff limit for this template
	RecommendedBackoffLimit *int32 `json:"recommendedBackoffLimit,omitempty"`
	// Recommended TTL after finished for this template
	RecommendedTTLSecondsAfterFinished *int32 `json:"recommendedTtlSecondsAfterFinished,omitempty"`
	// Recommended restart policy for this template
	RecommendedRestartPolicy *string `json:"recommendedRestartPolicy,omitempty"`
}

// TemplateRetryPolicy defines retry defaults for this template
type TemplateRetryPolicy struct {
	// Recommended max retries for this template
	RecommendedMaxRetries *int `json:"recommendedMaxRetries,omitempty"`
	// Recommended backoff strategy for this template
	RecommendedBackoff *string `json:"recommendedBackoff,omitempty"`
	// Recommended base delay for this template
	RecommendedBaseDelay *string `json:"recommendedBaseDelay,omitempty"`
	// Recommended max delay for this template
	RecommendedMaxDelay *string `json:"recommendedMaxDelay,omitempty"`
}

// TemplateServicePolicy defines default service exposure settings recommended by the template
type TemplateServicePolicy struct {
	// Ports defines external service ports and their target container ports
	Ports []TemplateServicePort `json:"ports,omitempty"`
}

// TemplateServicePort maps a Service port to a target container port
type TemplateServicePort struct {
	Name       string `json:"name,omitempty"`
	Protocol   string `json:"protocol,omitempty"`
	Port       int32  `json:"port"`
	TargetPort int32  `json:"targetPort"`
}

// TemplateProbePolicy defines health check probes recommended by the template
// Uses standard Kubernetes probe format for maximum compatibility
type TemplateProbePolicy struct {
	// Liveness probe checks if the container is alive
	// If this probe fails, the container will be restarted
	// +optional
	Liveness *corev1.Probe `json:"liveness,omitempty"`

	// Readiness probe checks if the container is ready to serve traffic
	// If this probe fails, the pod will be removed from service endpoints
	// +optional
	Readiness *corev1.Probe `json:"readiness,omitempty"`

	// Startup probe checks if the application has started
	// All other probes are disabled until this succeeds
	// Useful for slow-starting containers
	// +optional
	Startup *corev1.Probe `json:"startup,omitempty"`
}

// SecretDefinition defines the structure and requirements of a secret
// This handles both simple key-value secrets and complex structured secrets
type SecretDefinition struct {
	// Whether this secret is required for the template to function
	Required bool `json:"required,omitempty"`

	// Human-readable description of what this secret is used for
	// Example: "API key for OpenAI authentication", "Database connection credentials"
	Description string `json:"description,omitempty"`

	// How this secret should be mounted and accessed
	// - env: As environment variables (default for simple secrets)
	// - file: As mounted files (better for certificates, config files)
	// - both: Available as both env vars and files
	MountType enums.SecretMountType `json:"mountType,omitempty"`

	// For env mounting: what environment variable prefix to use
	// If not specified, uses secret name in uppercase
	// Example: "DATABASE" -> DATABASE_HOST, DATABASE_USER, DATABASE_PASSWORD
	EnvPrefix string `json:"envPrefix,omitempty"`

	// For file mounting: where to mount the secret files
	// Example: "/etc/secrets/database", "/opt/certs"
	MountPath string `json:"mountPath,omitempty"`

	// Expected keys in this secret (for validation and documentation)
	// Example: ["host", "username", "password"] or ["api-key", "org-id"]
	ExpectedKeys []string `json:"expectedKeys,omitempty"`
}

// TemplateStatus shows the current state and usage of this template
type TemplateStatus struct {
	// observedGeneration is the most recent generation observed for this Template. It corresponds to the
	// Template's generation, which is updated on mutation by the API Server.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Standard Kubernetes conditions (Ready, Available, etc.)
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// How many Engrams/Impulses are currently using this template
	// Useful for understanding template popularity and impact of changes
	UsageCount int32 `json:"usageCount,omitempty"`

	// Whether this template passed validation checks
	ValidationStatus enums.ValidationStatus `json:"validationStatus,omitempty"`

	// If validation failed, what were the specific errors?
	// Helps template authors fix issues with their schemas or configuration
	ValidationErrors []string `json:"validationErrors,omitempty"`
}
