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

// WorkloadSpec captures the execution primitive that should back an Engram, Impulse,
// or Story step. Higher-level defaults flow from controller configuration down to
// the individual resource and finally to the generated Kubernetes workload.
//
// Replica counts are intentionally absent; autoscaling is delegated to purpose-built
// controllers (HPA, KEDA, VPA). The API only declares the shape of the workload.
type WorkloadSpec struct {
	// Mode selects the workload controller backing the component.
	//   * job        – short-lived, run-to-completion executions
	//   * deployment – continuously running, stateless processes
	//   * statefulset – continuously running processes with sticky identity
	// +kubebuilder:default="job"
	Mode enums.WorkloadMode `json:"mode,omitempty"`

	// Job config applies when Mode is job.
	Job *JobWorkloadConfig `json:"job,omitempty"`

	// StatefulSet config applies when Mode is statefulset.
	StatefulSet *StatefulSetWorkloadConfig `json:"statefulSet,omitempty"`

	// Resources specifies pod-level resource guarantees and limits.
	Resources *WorkloadResources `json:"resources,omitempty"`

	// UpdateStrategy determines how the workload should roll out changes.
	UpdateStrategy *UpdateStrategy `json:"updateStrategy,omitempty"`
}

// ServiceExposure declares how a generated Service should be materialised for a given
// workload. Template authors own the port definitions; this struct lets instance authors
// override exposure type or metadata.
type ServiceExposure struct {
	// Type is the Service type (ClusterIP, NodePort, LoadBalancer).
	Type string `json:"type,omitempty"`
	// Labels are merged into the Service metadata.
	Labels map[string]string `json:"labels,omitempty"`
	// Annotations are merged into the Service metadata.
	Annotations map[string]string `json:"annotations,omitempty"`
}

// JobWorkloadConfig contains knobs that map directly onto batch/v1 Job fields.
type JobWorkloadConfig struct {
	// Parallelism mirrors spec.parallelism.
	Parallelism *int32 `json:"parallelism,omitempty"`

	// Completions mirrors spec.completions.
	Completions *int32 `json:"completions,omitempty"`

	// BackoffLimit mirrors spec.backoffLimit.
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// ActiveDeadlineSeconds mirrors spec.activeDeadlineSeconds.
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty"`

	// TTLSecondsAfterFinished mirrors spec.ttlSecondsAfterFinished.
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// StatefulSetWorkloadConfig includes the subset of StatefulSet fields we support.
// Storage is provisioned by the controller; VolumeClaimTemplates are purposefully hidden.
type StatefulSetWorkloadConfig struct {
	// ServiceName is the headless Service used for stable network identities.
	ServiceName string `json:"serviceName"`

	// PodManagementPolicy mirrors spec.podManagementPolicy.
	PodManagementPolicy *string `json:"podManagementPolicy,omitempty"`
}

// ExecutionOverrides lets an instance tune execution characteristics while inheriting
// from template and controller defaults.
type ExecutionOverrides struct {
	// Timeout overrides the resolved execution timeout (Go duration string).
	Timeout *string `json:"timeout,omitempty"`

	// Retry overrides the resolved retry policy.
	Retry *RetryPolicy `json:"retry,omitempty"`

	// Debug enables verbose, component-level logging when true.
	// +optional
	Debug *bool `json:"debug,omitempty"`

	// Security overrides pod-level security settings.
	Security *WorkloadSecurity `json:"security,omitempty"`

	// ImagePullPolicy overrides the container pull policy.
	ImagePullPolicy *string `json:"imagePullPolicy,omitempty"`

	// MaxInlineSize overrides the inline payload threshold in bytes. Set to 0 to always offload.
	// +optional
	MaxInlineSize *int `json:"maxInlineSize,omitempty"`

	// ServiceAccountName overrides the ServiceAccount in use.
	// +optional
	ServiceAccountName *string `json:"serviceAccountName,omitempty"`

	// AutomountServiceAccountToken toggles automountServiceAccountToken on the pod spec.
	// +kubebuilder:default=true
	// +optional
	AutomountServiceAccountToken *bool `json:"automountServiceAccountToken,omitempty"`

	// Probes allows disabling template-defined probes; probe definitions themselves
	// remain owned by the template.
	// +optional
	Probes *ProbeOverrides `json:"probes,omitempty"`

	// Storage overrides the resolved storage policy (e.g., enable S3 or file providers).
	// +optional
	Storage *StoragePolicy `json:"storage,omitempty"`

	// Loop tunes fan-out semantics for loop primitives.
	// +optional
	Loop *LoopPolicy `json:"loop,omitempty"`
}

// ProbeOverrides disables controller-provided probes on a per-instance basis.
// Fields default to false which keeps the template-defined probe behaviour.
type ProbeOverrides struct {
	// DisableLiveness disables the liveness probe when true.
	// +optional
	DisableLiveness bool `json:"disableLiveness,omitempty"`

	// DisableReadiness disables the readiness probe when true.
	// +optional
	DisableReadiness bool `json:"disableReadiness,omitempty"`

	// DisableStartup disables the startup probe when true.
	// +optional
	DisableStartup bool `json:"disableStartup,omitempty"`
}

// ExecutionPolicy represents the fully materialised policy after hierarchical
// resolution (controller → template → namespace → story → step).
type ExecutionPolicy struct {
	// Resources enumerates the concrete pod resource assignments.
	Resources *ResourcePolicy `json:"resources,omitempty"`

	// Security enumerates the concrete pod security settings.
	Security *SecurityPolicy `json:"security,omitempty"`

	// Job enumerates the job controller settings.
	Job *JobPolicy `json:"job,omitempty"`

	// Retry enumerates the retry configuration.
	Retry *RetryPolicy `json:"retry,omitempty"`

	// Timeout is the execution deadline expressed as a Go duration string.
	Timeout *string `json:"timeout,omitempty"`

	// ServiceAccountName identifies the ServiceAccount used by derived workloads.
	// +optional
	ServiceAccountName *string `json:"serviceAccountName,omitempty"`

	// Storage enumerates the resolved storage policy applied to the workload.
	// +optional
	Storage *StoragePolicy `json:"storage,omitempty"`

	// Loop specifies default loop execution characteristics applied at the story policy level.
	// +optional
	Loop *LoopPolicy `json:"loop,omitempty"`

	// Realtime enumerates streaming-specific execution settings (e.g., TTL for lingering pods).
	// +optional
	Realtime *RealtimePolicy `json:"realtime,omitempty"`
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

// RealtimePolicy captures streaming-specific execution settings.
type RealtimePolicy struct {
	// TTLSecondsAfterFinished controls how long realtime resources (Deployments, Services) remain after finish.
	// Set to 0 to delete immediately, negative to keep indefinitely, or positive seconds to keep temporarily.
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

type JobPolicy struct {
	// Backoff limit for jobs
	BackoffLimit *int32 `json:"backoffLimit,omitempty"`

	// TTL for cleaning up child resources (StepRuns, Pods) after StoryRun finishes.
	// After this period expires, child resources are deleted to free up cluster resources.
	// The parent StoryRun record is preserved for observability.
	// Default: 3600 (1 hour)
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`

	// Retention period for the StoryRun record itself after it finishes.
	// After this period expires, the StoryRun is deleted to prevent etcd overload.
	// Set to 0 to keep StoryRuns forever (requires manual cleanup).
	// Set to -1 to delete StoryRun immediately after child cleanup (not recommended).
	// Recommended: 86400 (24 hours) to 604800 (7 days) for production systems.
	// Default: 86400 (24 hours)
	// +optional
	StoryRunRetentionSeconds *int32 `json:"storyRunRetentionSeconds,omitempty"`

	// Restart policy
	RestartPolicy *string `json:"restartPolicy,omitempty"`
}

// LoopPolicy customizes fan-out behaviour for loop primitives.
type LoopPolicy struct {
	// DefaultBatchSize controls how many iterations are launched per reconcile.
	// +optional
	DefaultBatchSize *int32 `json:"defaultBatchSize,omitempty"`

	// MaxBatchSize bounds the controller-driven batch size.
	// +optional
	MaxBatchSize *int32 `json:"maxBatchSize,omitempty"`

	// MaxConcurrency limits in-flight loop iterations.
	// +optional
	MaxConcurrency *int32 `json:"maxConcurrency,omitempty"`

	// MaxConcurrencyLimit caps MaxConcurrency even when higher values are requested.
	// +optional
	MaxConcurrencyLimit *int32 `json:"maxConcurrencyLimit,omitempty"`
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

	// File specifies configuration for a filesystem-backed object store.
	// +optional
	File *FileStorageProvider `json:"file,omitempty"`
	// NOTE: GCS, AzureBlob, etc., can be added here in the future.
	TimeoutSeconds int `json:"timeoutSeconds,omitempty"`
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

	// UsePathStyle forces path-style addressing for S3-compatible providers that require it.
	// +optional
	UsePathStyle bool `json:"usePathStyle,omitempty"`
}

// FileStorageProvider configures access to a filesystem-backed storage location.
type FileStorageProvider struct {
	// Path is the directory inside the container where the storage backend should write files.
	// Defaults to "/var/run/bubu/storage" when omitted.
	Path string `json:"path,omitempty"`

	// VolumeClaimName references an existing PersistentVolumeClaim mounted to the workload.
	// Mutually exclusive with EmptyDir.
	// +optional
	VolumeClaimName string `json:"volumeClaimName,omitempty"`

	// EmptyDir defines an ephemeral volume to provision for file storage. When both
	// EmptyDir and VolumeClaimName are omitted, an EmptyDir{} volume is used by default.
	// +optional
	EmptyDir *corev1.EmptyDirVolumeSource `json:"emptyDir,omitempty"`
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
