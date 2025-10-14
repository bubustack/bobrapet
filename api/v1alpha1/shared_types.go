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
	"github.com/bubustack/bobrapet/pkg/enums"
	corev1 "k8s.io/api/core/v1"
)

// Shared types used across Engrams, Impulses, and Stories
// These types implement the hierarchical configuration system where settings cascade down:
// Controller defaults → Template → Engram/Impulse → Namespace → Story → Step → StepRun

// WorkloadSpec defines HOW to run something in Kubernetes
// This is where you specify whether it's a one-shot job, always-on service, or stateful application
//
// Note: Replica counts are NOT specified here - they're managed by external autoscaling tools:
// - Use KEDA for event-driven autoscaling (queue depth, HTTP requests, etc.)
// - Use HPA for metric-based autoscaling (CPU, memory, custom metrics)
// - Use VPA for vertical scaling (adjusting resource requests/limits)
// - Current replica counts are reported in the status section
type WorkloadSpec struct {
	// How to run this workload - the fundamental execution pattern
	// - job: Run once and complete (like a batch script or CI job)
	// - deployment: Always-on stateless service (like a web API or webhook listener)
	// - statefulset: Always-on stateful service (like a database or file processor with persistent storage)
	// +kubebuilder:default="job"
	Mode enums.WorkloadMode `json:"mode,omitempty"`

	// Job-specific settings (only applies when mode=job)
	Job *JobWorkloadConfig `json:"job,omitempty"`

	// StatefulSet-specific settings (only applies when mode=statefulset)
	StatefulSet *StatefulSetWorkloadConfig `json:"statefulSet,omitempty"`

	// Resource requirements (CPU, memory, etc.)
	Resources *WorkloadResources `json:"resources,omitempty"`

	// How to handle updates/upgrades
	UpdateStrategy *UpdateStrategy `json:"updateStrategy,omitempty"`
}

// ServiceExposure defines instance-level Service settings for exposing a workload
// Intended to be used by Impulse/Engram specs to choose exposure mode and metadata.
// Ports and health checks come from the template; this controls only exposure knobs.
type ServiceExposure struct {
	// Type of Service to create (ClusterIP default, NodePort, LoadBalancer)
	Type string `json:"type,omitempty"`
	// Additional labels to attach to the Service
	Labels map[string]string `json:"labels,omitempty"`
	// Additional annotations to attach to the Service
	Annotations map[string]string `json:"annotations,omitempty"`
}

// JobWorkloadConfig contains job-specific settings
type JobWorkloadConfig struct {
	// Number of pods to run in parallel
	Parallelism *int32 `json:"parallelism,omitempty"`

	// Number of successful completions needed
	Completions *int32 `json:"completions,omitempty"`

	// Number of retries before marking as failed
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// Time after which job is considered failed
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

	// TTL for cleanup after completion
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// StatefulSetWorkloadConfig contains statefulset-specific settings
// Note: For storage, we use controller-managed PVs instead of VolumeClaimTemplates
// This ensures RWX compatibility and single PV per StoryRun
type StatefulSetWorkloadConfig struct {
	// Service name for StatefulSet
	ServiceName string `json:"serviceName"`

	// Pod management policy
	PodManagementPolicy *string `json:"podManagementPolicy,omitempty"`
}

// ExecutionOverrides allows overriding template/policy defaults at instance level
type ExecutionOverrides struct {
	// Timeout for execution
	Timeout *string `json:"timeout,omitempty"`

	// Retry policy override
	Retry *RetryPolicy `json:"retry,omitempty"`

	// Security context override
	Security *WorkloadSecurity `json:"security,omitempty"`

	// Image pull policy override
	ImagePullPolicy *string `json:"imagePullPolicy,omitempty"`

	// ServiceAccount to use for the workload.
	// +optional
	ServiceAccountName *string `json:"serviceAccountName,omitempty"`

	// AutomountServiceAccountToken controls whether a service account token should be automatically mounted.
	// Defaults to false.
	// +optional
	AutomountServiceAccountToken *bool `json:"automountServiceAccountToken,omitempty"`

	// Probe overrides allow disabling template-defined probes at instance level
	// Note: You can only disable probes, not redefine them
	// +optional
	Probes *ProbeOverrides `json:"probes,omitempty"`
}

// ProbeOverrides allows disabling specific probes at instance level
// All fields default to false (probes enabled). Set to true to disable.
type ProbeOverrides struct {
	// DisableLiveness disables the liveness probe
	// +optional
	DisableLiveness bool `json:"disableLiveness,omitempty"`

	// DisableReadiness disables the readiness probe
	// +optional
	DisableReadiness bool `json:"disableReadiness,omitempty"`

	// DisableStartup disables the startup probe
	// +optional
	DisableStartup bool `json:"disableStartup,omitempty"`
}

// ExecutionPolicy defines execution configuration with hierarchical resolution:
// Controller defaults -> Template -> Engram/Impulse -> Namespace -> Story -> Step -> StepRun
type ExecutionPolicy struct {
	// Resource configuration (actual values, not defaults)
	Resources *ResourcePolicy `json:"resources,omitempty"`

	// Security configuration
	Security *SecurityPolicy `json:"security,omitempty"`

	// Job configuration
	Job *JobPolicy `json:"job,omitempty"`

	// Retry configuration
	Retry *RetryPolicy `json:"retry,omitempty"`

	// Timeout configuration
	Timeout *string `json:"timeout,omitempty"`

	// ServiceAccount to use for workloads created from this policy.
	// +optional
	ServiceAccountName *string `json:"serviceAccountName,omitempty"`
}

type ResourcePolicy struct {
	// CPU request
	CPURequest *string `json:"cpuRequest,omitempty"`

	// Memory request
	MemoryRequest *string `json:"memoryRequest,omitempty"`

	// CPU limit
	CPULimit *string `json:"cpuLimit,omitempty"`

	// Memory limit
	MemoryLimit *string `json:"memoryLimit,omitempty"`
}

type SecurityPolicy struct {
	// Whether to run as non-root user
	RunAsNonRoot *bool `json:"runAsNonRoot,omitempty"`

	// Whether to allow privilege escalation
	AllowPrivilegeEscalation *bool `json:"allowPrivilegeEscalation,omitempty"`

	// Read-only root filesystem
	ReadOnlyRootFilesystem *bool `json:"readOnlyRootFilesystem,omitempty"`

	// User ID to run as
	RunAsUser *int64 `json:"runAsUser,omitempty"`
}

type JobPolicy struct {
	// Backoff limit for jobs
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// TTL for job cleanup
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// Restart policy
	RestartPolicy *string `json:"restartPolicy,omitempty"`
}

// RetryPolicy defines retry behavior for steps and execution
type RetryPolicy struct {
	// Maximum number of retry attempts
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	MaxRetries *int32 `json:"maxRetries,omitempty"`

	// Base delay between retries
	// Examples: "1s", "5s", "30s"
	// +kubebuilder:default="1s"
	Delay *string `json:"delay,omitempty"`

	// Backoff strategy for retry delays
	// - exponential: 1s, 2s, 4s, 8s... (doubles each time)
	// - linear: 1s, 2s, 3s, 4s... (increases by delay each time)
	// - constant: 1s, 1s, 1s, 1s... (same delay each time)
	Backoff *enums.BackoffStrategy `json:"backoff,omitempty"`
}

// UpdateStrategy defines how workload updates should be performed
type UpdateStrategy struct {
	// Type of update strategy
	// - RollingUpdate: Gradually replace old pods with new ones
	// - Recreate: Delete all old pods before creating new ones
	Type *enums.UpdateStrategyType `json:"type,omitempty"`

	// Rolling update configuration (only applies when type=RollingUpdate)
	RollingUpdate *RollingUpdateConfig `json:"rollingUpdate,omitempty"`
}

// RollingUpdateConfig contains rolling update parameters
type RollingUpdateConfig struct {
	// Maximum number of pods that can be unavailable during update
	// Can be absolute number (ex: 5) or percentage (ex: 25%)
	MaxUnavailable *string `json:"maxUnavailable,omitempty"`

	// Maximum number of pods that can be created above desired replicas during update
	// Only for Deployment (not StatefulSet)
	MaxSurge *string `json:"maxSurge,omitempty"`
}

// WorkloadResources defines resource requirements and constraints
type WorkloadResources struct {
	// Resource requests and limits
	Requests *ResourceRequests `json:"requests,omitempty"`
	Limits   *ResourceLimits   `json:"limits,omitempty"`

	// Scheduling constraints
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	Tolerations  []string          `json:"tolerations,omitempty"`
}

// ResourceRequests defines minimum required resources
type ResourceRequests struct {
	CPU              *string `json:"cpu,omitempty"`              // e.g., "100m", "0.5"
	Memory           *string `json:"memory,omitempty"`           // e.g., "128Mi", "1Gi"
	EphemeralStorage *string `json:"ephemeralStorage,omitempty"` // e.g., "1Gi"
}

// ResourceLimits defines maximum allowed resources
type ResourceLimits struct {
	CPU              *string `json:"cpu,omitempty"`              // e.g., "1000m", "2"
	Memory           *string `json:"memory,omitempty"`           // e.g., "1Gi", "4Gi"
	EphemeralStorage *string `json:"ephemeralStorage,omitempty"` // e.g., "5Gi"
}

// WorkloadSecurity defines security configuration
type WorkloadSecurity struct {
	// Whether to run as non-root user
	RunAsNonRoot *bool `json:"runAsNonRoot,omitempty"`

	// Whether to allow privilege escalation
	AllowPrivilegeEscalation *bool `json:"allowPrivilegeEscalation,omitempty"`

	// Read-only root filesystem
	ReadOnlyRootFilesystem *bool `json:"readOnlyRootFilesystem,omitempty"`

	// User ID to run as
	RunAsUser *int64 `json:"runAsUser,omitempty"`

	// Required secrets for this workload
	RequiredSecrets []string `json:"requiredSecrets,omitempty"`
}

// StoragePolicy defines the configuration for object storage access.
type StoragePolicy struct {
	// S3 specifies the configuration for an S3-compatible object store.
	// +optional
	S3 *S3StorageProvider `json:"s3,omitempty"`
	// NOTE: GCS, AzureBlob, etc., can be added here in the future.
}

// S3StorageProvider configures access to an S3 or S3-compatible object store.
type S3StorageProvider struct {
	// Bucket is the name of the S3 bucket.
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`

	// Region is the AWS region of the bucket.
	// +optional
	Region string `json:"region,omitempty"`

	// Endpoint is the S3 endpoint URL. Useful for S3-compatible stores like MinIO.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Authentication configures how the workload authenticates with S3.
	// +kubebuilder:validation:Required
	Authentication S3Authentication `json:"authentication"`
}

// S3Authentication defines the authentication method for S3.
// Only one of the fields should be set.
type S3Authentication struct {
	// ServiceAccountAnnotations allows specifying annotations to be added to the
	// auto-generated ServiceAccount. This is the recommended approach for
	// IAM-based authentication on cloud providers (e.g., IRSA on EKS).
	// The key-value pairs will be added to the ServiceAccount's metadata.annotations.
	// +optional
	ServiceAccountAnnotations map[string]string `json:"serviceAccountAnnotations,omitempty"`

	// SecretRef references a Kubernetes Secret that contains the credentials.
	// The secret must contain keys 'accessKeyID' and 'secretAccessKey'.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`
}
