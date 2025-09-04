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

// Unified workload configuration types used by both Engrams and Impulses
// This ensures consistency and reduces duplication

// EngineConfig defines how a component (Engram or Impulse) should be executed
type EngineConfig struct {
	// Execution mode: job (one-shot), deployment (stateless), statefulset (stateful)
	// Note: Engrams support all modes, Impulses support only deployment/statefulset (validated at template level)
	// +kubebuilder:default="job"
	// +kubebuilder:validation:Enum=job;deployment;statefulset
	Mode string `json:"mode,omitempty"`

	// Reference to Template that defines the image and defaults
	TemplateRef string `json:"templateRef,omitempty"`

	// Image pull policy for the container image
	// +kubebuilder:validation:Enum=Always;Never;IfNotPresent
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`

	// Workload configuration for deployment/statefulset/job modes
	Workload *WorkloadConfig `json:"workload,omitempty"`
}

// WorkloadConfig contains Kubernetes workload configuration
// Maps to essential fields from Deployment, StatefulSet, and Job APIs
type WorkloadConfig struct {
	// Scaling configuration
	Replicas    *int32 `json:"replicas,omitempty"`    // For Deployment/StatefulSet
	Parallelism *int32 `json:"parallelism,omitempty"` // For Job: max pods running simultaneously
	Completions *int32 `json:"completions,omitempty"` // For Job: desired successful completions

	// Update strategy for Deployment/StatefulSet
	UpdateStrategy *UpdateStrategy `json:"updateStrategy,omitempty"`

	// Job-specific configuration
	Job *JobConfig `json:"job,omitempty"`

	// StatefulSet-specific configuration
	StatefulSet *StatefulSetConfig `json:"statefulSet,omitempty"`

	// Lifecycle management
	ActiveDeadlineSeconds   *int64 `json:"activeDeadlineSeconds,omitempty"`   // Max duration (Job/Deployment)
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"` // Cleanup TTL (Job)
}

// UpdateStrategy defines how updates should be performed
type UpdateStrategy struct {
	// Type of update: RollingUpdate or Recreate (Deployment), RollingUpdate or OnDelete (StatefulSet)
	Type string `json:"type,omitempty"`

	// Rolling update configuration
	RollingUpdate *RollingUpdateConfig `json:"rollingUpdate,omitempty"`
}

// RollingUpdateConfig contains rolling update parameters
type RollingUpdateConfig struct {
	// Max number of pods that can be unavailable during update
	// Can be absolute number (ex: 5) or percentage (ex: 25%)
	MaxUnavailable *string `json:"maxUnavailable,omitempty"`

	// Max number of pods that can be created above desired replicas during update
	// Only for Deployment (not StatefulSet)
	MaxSurge *string `json:"maxSurge,omitempty"`
}

// JobConfig contains Job-specific configuration
type JobConfig struct {
	// Number of retries before considering job failed
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// How job completion is tracked: NonIndexed or Indexed
	CompletionMode *string `json:"completionMode,omitempty"`

	// Whether job should be suspended (not create pods)
	Suspend *bool `json:"suspend,omitempty"`
}

// StatefulSetConfig contains StatefulSet-specific configuration
type StatefulSetConfig struct {
	// Name of service governing this StatefulSet (required for StatefulSet)
	ServiceName string `json:"serviceName"`

	// Volume claim templates for persistent storage
	VolumeClaimTemplates []VolumeClaimTemplate `json:"volumeClaimTemplates,omitempty"`

	// Pod management policy: OrderedReady (default) or Parallel
	PodManagementPolicy *string `json:"podManagementPolicy,omitempty"`
}

// VolumeClaimTemplate defines a template for persistent volume claims
type VolumeClaimTemplate struct {
	Name         string   `json:"name"`
	Size         string   `json:"size"`                   // e.g., "10Gi"
	StorageClass *string  `json:"storageClass,omitempty"` // e.g., "fast-ssd"
	AccessModes  []string `json:"accessModes,omitempty"`  // e.g., ["ReadWriteOnce"]
}

// RetryPolicy defines retry behavior (used by both Engrams and Impulses)
type RetryPolicy struct {
	MaxRetries int    `json:"maxRetries,omitempty"`
	Backoff    string `json:"backoff,omitempty"`   // exponential|linear|fixed
	BaseDelay  string `json:"baseDelay,omitempty"` // duration string
	MaxDelay   string `json:"maxDelay,omitempty"`  // duration string
}

// WorkloadResources defines resource configuration (used by both Engrams and Impulses)
type WorkloadResources struct {
	// Resource requests and limits
	Requests *ResourceRequests `json:"requests,omitempty"`
	Limits   *ResourceLimits   `json:"limits,omitempty"`

	// Scheduling constraints
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	Tolerations  []string          `json:"tolerations,omitempty"`

	// Execution constraints
	MaxConcurrentInstances *int32  `json:"maxConcurrentInstances,omitempty"` // Max parallel instances
	MaxExecutionTime       *string `json:"maxExecutionTime,omitempty"`       // Max time per execution
	MaxRetries             *int32  `json:"maxRetries,omitempty"`             // Max retries per execution
}

// ResourceRequests defines minimum required resources
type ResourceRequests struct {
	CPU              string `json:"cpu,omitempty"`              // e.g., "100m", "0.5"
	Memory           string `json:"memory,omitempty"`           // e.g., "128Mi", "1Gi"
	EphemeralStorage string `json:"ephemeralStorage,omitempty"` // e.g., "1Gi"
	GPU              string `json:"gpu,omitempty"`              // e.g., "1", "0.5"
}

// ResourceLimits defines maximum allowed resources
type ResourceLimits struct {
	CPU              string `json:"cpu,omitempty"`              // e.g., "1000m", "2"
	Memory           string `json:"memory,omitempty"`           // e.g., "1Gi", "4Gi"
	EphemeralStorage string `json:"ephemeralStorage,omitempty"` // e.g., "5Gi"
	GPU              string `json:"gpu,omitempty"`              // e.g., "1", "2"

	// Additional limits
	MaxFileSize     string `json:"maxFileSize,omitempty"`     // Max size for file uploads/downloads
	MaxArtifactSize string `json:"maxArtifactSize,omitempty"` // Max size for output artifacts
	MaxNetworkConns *int32 `json:"maxNetworkConns,omitempty"` // Max concurrent network connections
}

// WorkloadSecurity defines security configuration (used by both Engrams and Impulses)
type WorkloadSecurity struct {
	RunAsNonRoot             bool     `json:"runAsNonRoot,omitempty"`
	AllowPrivilegeEscalation bool     `json:"allowPrivilegeEscalation,omitempty"`
	RequiredSecrets          []string `json:"requiredSecrets,omitempty"`
	NetworkPolicy            string   `json:"networkPolicy,omitempty"`
}
