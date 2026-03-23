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

package config

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/observability"
	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/runtime/featuretoggles"
	"github.com/bubustack/core/templating"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ControllerDependencies holds all the shared dependencies required by the controllers.
// This struct is created once in main.go and passed to each controller, ensuring
// consistent access to shared services like configuration resolvers and templating evaluators.
type ControllerDependencies struct {
	client.Client
	APIReader         client.Reader
	Scheme            *runtime.Scheme
	ConfigResolver    *Resolver
	TemplateEvaluator *templating.Evaluator
}

// ControllerConfig holds configurable parameters for all controllers
type ControllerConfig struct {
	// StoryRun controller configuration
	StoryRun StoryRunConfig `json:"storyRun,omitempty"`

	// StepRun controller configuration
	StepRun StepRunConfig `json:"stepRun,omitempty"`

	// Story controller configuration
	Story StoryConfig `json:"story,omitempty"`

	// Engram controller configuration
	Engram EngramConfig `json:"engram,omitempty"`

	// Impulse controller configuration
	Impulse ImpulseConfig `json:"impulse,omitempty"`

	// Template controllers configuration
	Template TemplateConfig `json:"template,omitempty"`

	// Transport controller configuration
	TransportController TransportControllerConfig `json:"transportController,omitempty"`

	// MaxStoryWithBlockSizeBytes is the maximum allowed size for a Story's spec.steps.with block.
	// This prevents oversized resources from being stored in etcd.
	// +optional
	MaxStoryWithBlockSizeBytes int `json:"maxStoryWithBlockSizeBytes,omitempty"`

	DefaultEngramImage   string `json:"defaultEngramImage,omitempty"`
	DefaultImpulseImage  string `json:"defaultImpulseImage,omitempty"`
	DefaultCPURequest    string `json:"defaultCPURequest,omitempty"`
	DefaultCPULimit      string `json:"defaultCPULimit,omitempty"`
	DefaultMemoryRequest string `json:"defaultMemoryRequest,omitempty"`
	DefaultMemoryLimit   string `json:"defaultMemoryLimit,omitempty"`

	// Global Controller Configuration
	// Note: Per-controller MaxConcurrentReconciles are in StoryRun, StepRun, Story, etc.
	RequeueBaseDelay time.Duration   `json:"requeueBaseDelay,omitempty"`
	RequeueMaxDelay  time.Duration   `json:"requeueMaxDelay,omitempty"`
	CleanupInterval  metav1.Duration `json:"cleanupInterval,omitempty"`
	// MaxConcurrentReconciles provides a global fallback when per-controller values are zero.
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`

	// ReconcileTimeout bounds the duration of a single reconcile loop.
	// Set to 0 to disable deadline (not recommended for production).
	ReconcileTimeout time.Duration `json:"reconcileTimeout,omitempty"`

	// Transport configuration for hybrid execution
	Transport TransportConfig `json:"transport,omitempty"`

	// Image Configuration
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Resource Limits
	EngramCPURequest    string `json:"engramCpuRequest,omitempty"`
	EngramCPULimit      string `json:"engramCpuLimit,omitempty"`
	EngramMemoryRequest string `json:"engramMemoryRequest,omitempty"`
	EngramMemoryLimit   string `json:"engramMemoryLimit,omitempty"`

	// Retry and Timeout Configuration
	MaxRetries             int           `json:"maxRetries,omitempty"`
	DefaultStepTimeout     time.Duration `json:"defaultStepTimeout,omitempty"`
	ApprovalDefaultTimeout time.Duration `json:"approvalDefaultTimeout,omitempty"`
	ExternalDataTimeout    time.Duration `json:"externalDataTimeout,omitempty"`
	ConditionalTimeout     time.Duration `json:"conditionalTimeout,omitempty"`

	// Security Configuration
	RunAsNonRoot             bool     `json:"runAsNonRoot,omitempty"`
	ReadOnlyRootFilesystem   bool     `json:"readOnlyRootFilesystem,omitempty"`
	AllowPrivilegeEscalation bool     `json:"allowPrivilegeEscalation,omitempty"`
	DropCapabilities         []string `json:"dropCapabilities,omitempty"`
	RunAsUser                int64    `json:"runAsUser,omitempty"`

	// Job Configuration
	JobBackoffLimit                  int32                `json:"jobBackoffLimit,omitempty"`
	JobRestartPolicy                 corev1.RestartPolicy `json:"jobRestartPolicy,omitempty"`
	TTLSecondsAfterFinished          int32                `json:"ttlSecondsAfterFinished,omitempty"`
	StreamingTTLSecondsAfterFinished int32                `json:"streamingTTLSecondsAfterFinished,omitempty"`
	StoryRunRetentionSeconds         int32                `json:"storyRunRetentionSeconds,omitempty"`
	ServiceAccountName               string               `json:"serviceAccountName,omitempty"`
	AutomountServiceAccountToken     bool                 `json:"automountServiceAccountToken,omitempty"`

	// Templating Configuration
	TemplateEvaluationTimeout time.Duration `json:"templateEvaluationTimeout,omitempty"`
	TemplateMaxOutputBytes    int           `json:"templateMaxOutputBytes,omitempty"`
	// TemplateDeterministic disables non-deterministic helpers (for example, now()).
	TemplateDeterministic bool `json:"templateDeterministic,omitempty"`
	// TemplateOffloadedPolicy controls how templates behave when offloaded data is accessed.
	TemplateOffloadedPolicy string `json:"templateOffloadedPolicy,omitempty"`
	// TemplateMaterializeEngram is the Engram name used to materialize templates that require offloaded data.
	TemplateMaterializeEngram string `json:"templateMaterializeEngram,omitempty"`

	// ReferenceCrossNamespacePolicy controls cross-namespace reference behavior.
	// Supported values: "deny", "grant", "allow".
	ReferenceCrossNamespacePolicy string `json:"referenceCrossNamespacePolicy,omitempty"`

	// Telemetry Configuration
	TelemetryEnabled        bool `json:"telemetryEnabled,omitempty"`
	TracePropagationEnabled bool `json:"tracePropagationEnabled,omitempty"`

	// Development/Debug Configuration
	EnableVerboseLogging    bool `json:"enableVerboseLogging,omitempty"`
	EnableStepOutputLogging bool `json:"enableStepOutputLogging,omitempty"`
	EnableMetrics           bool `json:"enableMetrics,omitempty"`

	// Operator-level default storage configuration (applied when Story policy is absent)
	DefaultStorageProvider     string `json:"defaultStorageProvider,omitempty"`
	DefaultS3Bucket            string `json:"defaultS3Bucket,omitempty"`
	DefaultS3Region            string `json:"defaultS3Region,omitempty"`
	DefaultS3Endpoint          string `json:"defaultS3Endpoint,omitempty"`
	DefaultS3UsePathStyle      bool   `json:"defaultS3UsePathStyle,omitempty"`
	DefaultS3AuthSecretName    string `json:"defaultS3AuthSecretName,omitempty"`
	DefaultFileStoragePath     string `json:"defaultFileStoragePath,omitempty"`
	DefaultFileVolumeClaimName string `json:"defaultFileVolumeClaimName,omitempty"`
}

// Telemetry feature gate logger
var (
	toggleLogger = logf.Log.WithName("config-toggles")
)

// ApplyRuntimeToggles wires global observability/logging knobs from config.
func ApplyRuntimeToggles(cfg *ControllerConfig) {
	if cfg == nil {
		return
	}

	featuretoggles.Apply(
		featuretoggles.Features{
			TelemetryEnabled:         cfg.TelemetryEnabled,
			TracePropagationEnabled:  cfg.TracePropagationEnabled,
			VerboseLoggingEnabled:    cfg.EnableVerboseLogging,
			StepOutputLoggingEnabled: cfg.EnableStepOutputLogging,
			MetricsEnabled:           cfg.EnableMetrics,
		},
		featuretoggles.Sink{
			EnableTelemetry:         observability.EnableTracing,
			EnableTracePropagation:  observability.EnableTracePropagation,
			EnableVerboseLogging:    logging.EnableVerboseLogging,
			EnableStepOutputLogging: logging.EnableStepOutputLogging,
			EnableMetrics:           metrics.Enable,
		},
	)

	toggleLogger.Info("runtime toggles applied",
		"telemetry", cfg.TelemetryEnabled,
		"tracePropagation", cfg.TracePropagationEnabled,
		"verboseLogging", cfg.EnableVerboseLogging,
		"stepOutputLogging", cfg.EnableStepOutputLogging,
		"metrics", cfg.EnableMetrics,
	)
}

// Validation constants for controller configuration bounds.
const (
	// MinReconcileTimeout is the minimum allowed reconcile timeout to prevent aggressive cancellation.
	MinReconcileTimeout = 5 * time.Second
	// MaxReconcileTimeout is the maximum allowed reconcile timeout to prevent runaway reconciles.
	MaxReconcileTimeout = 30 * time.Minute
	// MinCleanupInterval is the minimum cleanup interval to prevent GC spinning.
	MinCleanupInterval = 10 * time.Second
	// MinRequeueDelay is the minimum requeue delay to prevent hot-loop requeues.
	MinRequeueDelay = 10 * time.Millisecond
	// MinHeartbeatInterval is the minimum heartbeat interval.
	MinHeartbeatInterval = 1 * time.Second
	// MinStoryWithBlockSizeBytes is the minimum allowed size for a Story with-block.
	MinStoryWithBlockSizeBytes = 1024
	// MaxStoryWithBlockSizeBytes is the maximum allowed size for a Story with-block.
	MaxStoryWithBlockSizeBytes = 1024 * 1024
)

const (
	TemplatingOffloadedPolicyError      = "error"
	TemplatingOffloadedPolicyInject     = "inject"
	TemplatingOffloadedPolicyController = "controller"
)

const (
	ReferenceCrossNamespacePolicyDeny  = "deny"
	ReferenceCrossNamespacePolicyGrant = "grant"
	ReferenceCrossNamespacePolicyAllow = "allow"
)

// ValidateControllerConfig ensures operator-provided knobs are sane.
//
// Behavior:
//   - Validates all controller configuration fields for safety bounds.
//   - Normalizes DropCapabilities (uppercase, trim whitespace).
//   - Accumulates all validation errors and returns them joined.
//
// Arguments:
//   - cfg *ControllerConfig: the configuration to validate.
//
// Returns:
//   - error: joined validation errors, or nil if valid.
//
// Side Effects:
//   - Mutates cfg.DropCapabilities in-place (normalization).
//
// Notes:
//   - Called after parsing ConfigMap to ensure safe runtime behavior.
//   - Negative durations are treated as unset by the duration helpers, but we validate them here.
func ValidateControllerConfig(cfg *ControllerConfig) error {
	if cfg == nil {
		return fmt.Errorf("controller config is nil")
	}
	var errs []error

	errs = append(errs, validateTemplatingSettings(cfg)...)
	errs = append(errs, validateReferenceSettings(cfg)...)
	errs = append(errs, validateTransportSettings(cfg)...)
	errs = append(errs, validateReconcileTimeoutBounds(cfg.ReconcileTimeout)...)
	errs = append(errs, validateCleanupIntervalBounds(cfg.CleanupInterval.Duration)...)
	errs = append(errs, validateStoryWithBlockSizeBounds(cfg.MaxStoryWithBlockSizeBytes)...)
	errs = append(errs, validateStorageDefaults(cfg)...)

	// RequeueDelay validation
	errs = append(errs, validateRequeueDelays(cfg.RequeueBaseDelay, cfg.RequeueMaxDelay, "controller")...)

	// Per-controller rate limiter validation
	errs = append(errs, validateRateLimiter(cfg.StoryRun.RateLimiter, "storyrun")...)
	errs = append(errs, validateRateLimiter(cfg.StepRun.RateLimiter, "steprun")...)
	errs = append(errs, validateRateLimiter(cfg.Story.RateLimiter, "story")...)
	errs = append(errs, validateRateLimiter(cfg.Engram.RateLimiter, "engram")...)
	errs = append(errs, validateRateLimiter(cfg.Impulse.RateLimiter, "impulse")...)
	errs = append(errs, validateRateLimiter(cfg.Template.RateLimiter, "template")...)
	errs = append(errs, validateRateLimiter(cfg.TransportController.RateLimiter, "transport")...)

	errs = append(errs, validateSchedulingConfig(cfg.StoryRun.Scheduling)...)

	errs = append(errs, normalizeAndValidateCapabilities(cfg)...)
	if cfg.RunAsUser < 0 {
		errs = append(errs, fmt.Errorf("security.run-as-user cannot be negative"))
	}

	return errors.Join(errs...)
}

func validateStorageDefaults(cfg *ControllerConfig) []error {
	provider := strings.TrimSpace(cfg.DefaultStorageProvider)
	switch provider {
	case "":
		return nil
	case contracts.StorageProviderFile:
		var errs []error
		if strings.TrimSpace(cfg.DefaultFileStoragePath) == "" {
			errs = append(errs, fmt.Errorf("controller.storage.file.path must be set when provider is file"))
		}
		if strings.TrimSpace(cfg.DefaultFileVolumeClaimName) == "" {
			errs = append(errs, fmt.Errorf("controller.storage.file.volume-claim-name must be set when provider is file"))
		}
		return errs
	case contracts.StorageProviderS3:
		return nil
	default:
		return nil
	}
}

func validateTemplatingSettings(cfg *ControllerConfig) []error {
	var errs []error
	if cfg.TemplateEvaluationTimeout < 0 {
		errs = append(errs, fmt.Errorf("templating.evaluation-timeout cannot be negative"))
	}
	if cfg.TemplateMaxOutputBytes < 0 {
		errs = append(errs, fmt.Errorf("templating.max-output-bytes cannot be negative"))
	}
	if policy := strings.TrimSpace(cfg.TemplateOffloadedPolicy); policy != "" {
		switch policy {
		case TemplatingOffloadedPolicyError, TemplatingOffloadedPolicyInject, TemplatingOffloadedPolicyController:
		default:
			errs = append(errs, fmt.Errorf("templating.offloaded-data-policy must be one of [%s, %s, %s]", TemplatingOffloadedPolicyError, TemplatingOffloadedPolicyInject, TemplatingOffloadedPolicyController))
		}
	}
	// Only "inject" policy requires a materialize engram (pod-based materialization).
	// "controller" policy resolves offloaded data in-process and does not need one.
	if strings.TrimSpace(cfg.TemplateOffloadedPolicy) == TemplatingOffloadedPolicyInject {
		if strings.TrimSpace(cfg.TemplateMaterializeEngram) == "" {
			errs = append(errs, fmt.Errorf("templating.materialize-engram must be set when offloaded-data-policy is inject"))
		}
	}
	return errs
}

func validateReferenceSettings(cfg *ControllerConfig) []error {
	policy := strings.TrimSpace(cfg.ReferenceCrossNamespacePolicy)
	if policy == "" {
		return nil
	}
	switch policy {
	case ReferenceCrossNamespacePolicyDeny,
		ReferenceCrossNamespacePolicyGrant,
		ReferenceCrossNamespacePolicyAllow:
		return nil
	default:
		return []error{fmt.Errorf("references.cross-namespace-policy must be one of [%s, %s, %s]", ReferenceCrossNamespacePolicyDeny, ReferenceCrossNamespacePolicyGrant, ReferenceCrossNamespacePolicyAllow)}
	}
}

func validateSchedulingConfig(cfg SchedulingConfig) []error {
	var errs []error
	if cfg.GlobalConcurrency < 0 {
		errs = append(errs, fmt.Errorf("storyrun.global-concurrency cannot be negative"))
	}
	for name, queue := range cfg.Queues {
		queueName := strings.TrimSpace(name)
		if queueName == "" {
			errs = append(errs, fmt.Errorf("storyrun.queue name cannot be empty"))
			continue
		}
		if issues := validation.IsDNS1123Label(queueName); len(issues) > 0 {
			errs = append(errs, fmt.Errorf("storyrun.queue.%s is invalid: %s", name, strings.Join(issues, ", ")))
		}
		if queue.Concurrency < 0 {
			errs = append(errs, fmt.Errorf("storyrun.queue.%s.concurrency cannot be negative", name))
		}
		if queue.DefaultPriority < 0 {
			errs = append(errs, fmt.Errorf("storyrun.queue.%s.default-priority cannot be negative", name))
		}
		if queue.PriorityAgingSeconds < 0 {
			errs = append(errs, fmt.Errorf("storyrun.queue.%s.priority-aging-seconds cannot be negative", name))
		}
	}
	return errs
}

func validateTransportSettings(cfg *ControllerConfig) []error {
	var errs []error
	if cfg.Transport.HeartbeatInterval <= 0 {
		errs = append(errs, fmt.Errorf("controller.transport.heartbeat-interval must be greater than 0"))
	} else if cfg.Transport.HeartbeatInterval < MinHeartbeatInterval {
		errs = append(errs, fmt.Errorf("controller.transport.heartbeat-interval must be at least %v", MinHeartbeatInterval))
	}
	if cfg.Engram.EngramControllerConfig.DefaultGRPCHeartbeatIntervalSeconds <= 0 {
		errs = append(errs, fmt.Errorf("engram.default-grpc-heartbeat-interval-seconds must be greater than 0"))
	}
	return errs
}

func validateReconcileTimeoutBounds(timeout time.Duration) []error {
	var errs []error
	if timeout < 0 {
		return []error{fmt.Errorf("controller.reconcile-timeout cannot be negative")}
	}
	if timeout == 0 {
		return nil
	}
	if timeout < MinReconcileTimeout {
		errs = append(errs, fmt.Errorf("controller.reconcile-timeout must be at least %v to prevent aggressive cancellation", MinReconcileTimeout))
	}
	if timeout > MaxReconcileTimeout {
		errs = append(errs, fmt.Errorf("controller.reconcile-timeout must not exceed %v to prevent runaway reconciles", MaxReconcileTimeout))
	}
	return errs
}

func validateCleanupIntervalBounds(interval time.Duration) []error {
	if interval <= 0 {
		return nil
	}
	if interval < MinCleanupInterval {
		return []error{fmt.Errorf("controller.cleanup-interval must be at least %v to prevent GC spinning", MinCleanupInterval)}
	}
	return nil
}

func validateStoryWithBlockSizeBounds(size int) []error {
	if size < 0 {
		return []error{fmt.Errorf("controller.max-story-with-block-size-bytes cannot be negative")}
	}
	if size == 0 {
		return nil
	}
	if size < MinStoryWithBlockSizeBytes {
		return []error{fmt.Errorf("controller.max-story-with-block-size-bytes must be at least %d bytes", MinStoryWithBlockSizeBytes)}
	}
	if size > MaxStoryWithBlockSizeBytes {
		return []error{fmt.Errorf("controller.max-story-with-block-size-bytes must not exceed %d bytes", MaxStoryWithBlockSizeBytes)}
	}
	return nil
}

func normalizeAndValidateCapabilities(cfg *ControllerConfig) []error {
	for _, cap := range cfg.DropCapabilities {
		if strings.TrimSpace(cap) == "" {
			cfg.DropCapabilities = NormalizeCapabilities(cfg.DropCapabilities)
			return []error{fmt.Errorf("security.drop-capabilities contains an empty entry")}
		}
	}
	cfg.DropCapabilities = NormalizeCapabilities(cfg.DropCapabilities)
	return nil
}

// validateRequeueDelays validates base and max delay consistency.
func validateRequeueDelays(base, max time.Duration, prefix string) []error {
	var errs []error
	if base < 0 {
		errs = append(errs, fmt.Errorf("%s.requeue-base-delay cannot be negative", prefix))
	}
	if max < 0 {
		errs = append(errs, fmt.Errorf("%s.requeue-max-delay cannot be negative", prefix))
	}
	if base > 0 && base < MinRequeueDelay {
		errs = append(errs, fmt.Errorf("%s.requeue-base-delay must be at least %v", prefix, MinRequeueDelay))
	}
	if base > 0 && max > 0 && base > max {
		errs = append(errs, fmt.Errorf("%s.requeue-base-delay cannot exceed %s.requeue-max-delay", prefix, prefix))
	}
	return errs
}

// validateRateLimiter validates a rate limiter configuration.
func validateRateLimiter(rl RateLimiterConfig, prefix string) []error {
	var errs []error
	if rl.BaseDelay < 0 {
		errs = append(errs, fmt.Errorf("%s.rate-limiter.base-delay cannot be negative", prefix))
	}
	if rl.MaxDelay < 0 {
		errs = append(errs, fmt.Errorf("%s.rate-limiter.max-delay cannot be negative", prefix))
	}
	if rl.BaseDelay > 0 && rl.BaseDelay < MinRequeueDelay {
		errs = append(errs, fmt.Errorf("%s.rate-limiter.base-delay must be at least %v", prefix, MinRequeueDelay))
	}
	if rl.BaseDelay > 0 && rl.MaxDelay > 0 && rl.BaseDelay > rl.MaxDelay {
		errs = append(errs, fmt.Errorf("%s.rate-limiter.base-delay cannot exceed %s.rate-limiter.max-delay", prefix, prefix))
	}
	return errs
}

// NormalizeCapabilities trims whitespace, uppercases, and filters empty entries
// from a capability slice. Returns ["ALL"] if the result is empty.
//
// Behavior:
//   - Trims whitespace and uppercases each entry.
//   - Filters out empty entries after trimming.
//   - Returns ["ALL"] if no valid capabilities remain.
//
// Arguments:
//   - caps []string: slice of capability names.
//
// Returns:
//   - []string: normalized slice of uppercase capability names, or ["ALL"] if empty.
//
// Notes:
//   - Used by both parseDropCapabilities and ValidateControllerConfig to ensure consistent normalization.
func NormalizeCapabilities(caps []string) []string {
	var result []string
	for _, cap := range caps {
		trimmed := strings.ToUpper(strings.TrimSpace(cap))
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return []string{"ALL"}
	}
	return result
}

// BindingControllerTuning captures throttling knobs for TransportBinding mutations.
type BindingControllerTuning struct {
	// MaxMutationsPerReconcile bounds how many bindings a controller may mutate
	// during a single reconcile loop. Zero or negative disables throttling.
	MaxMutationsPerReconcile int `json:"maxMutationsPerReconcile,omitempty"`
	// ThrottleRequeueDelay defines how long to wait before retrying once the
	// controller hits the mutation budget.
	ThrottleRequeueDelay time.Duration `json:"throttleRequeueDelay,omitempty"`
}

// SchedulingConfig captures global and per-queue scheduling controls for StoryRuns.
type SchedulingConfig struct {
	// GlobalConcurrency limits the total number of running StepRuns across the cluster.
	// Zero disables the global limit.
	GlobalConcurrency int32 `json:"globalConcurrency,omitempty"`

	// Queues defines per-queue concurrency and priority defaults.
	Queues map[string]QueueConfig `json:"queues,omitempty"`
}

// QueueConfig defines concurrency and default priority for a scheduling queue.
type QueueConfig struct {
	// Concurrency limits the number of running StepRuns in this queue.
	// Zero disables queue-level limiting.
	Concurrency int32 `json:"concurrency,omitempty"`

	// DefaultPriority applies when a Story does not set a priority explicitly.
	DefaultPriority int32 `json:"defaultPriority,omitempty"`

	// PriorityAgingSeconds increases effective priority based on time spent queued.
	// Zero or negative disables aging.
	PriorityAgingSeconds int32 `json:"priorityAgingSeconds,omitempty"`
}

// StoryRunConfig contains StoryRun controller settings
type StoryRunConfig struct {
	// MaxConcurrentReconciles is the maximum number of concurrent reconciles
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`

	// RateLimiter configuration
	RateLimiter RateLimiterConfig `json:"rateLimiter,omitempty"`

	// MaxInlineInputsSize is the maximum size in bytes for StoryRun spec.inputs.
	// Payloads larger than this will be rejected by the controller if webhooks are disabled.
	MaxInlineInputsSize int `json:"maxInlineInputsSize,omitempty"`

	// Binding config controls TransportBinding fan-out during reconciles.
	Binding BindingControllerTuning `json:"binding,omitempty"`

	// Scheduling controls global and per-queue execution limits.
	Scheduling SchedulingConfig `json:"scheduling,omitempty"`
}

// StepRunConfig contains StepRun controller settings
type StepRunConfig struct {
	// MaxConcurrentReconciles is the maximum number of concurrent reconciles
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`

	// RateLimiter configuration
	RateLimiter RateLimiterConfig `json:"rateLimiter,omitempty"`
}

// StoryConfig contains Story controller settings
type StoryConfig struct {
	// MaxConcurrentReconciles is the maximum number of concurrent reconciles
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`

	// RateLimiter configuration
	RateLimiter RateLimiterConfig `json:"rateLimiter,omitempty"`

	// Binding config controls TransportBinding fan-out during reconciles.
	Binding BindingControllerTuning `json:"binding,omitempty"`
}

// EngramConfig contains Engram controller settings
type EngramConfig struct {
	// MaxConcurrentReconciles is the maximum number of concurrent reconciles
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`

	// RateLimiter configuration
	RateLimiter RateLimiterConfig `json:"rateLimiter,omitempty"`
	// EngramControllerConfig holds configuration specific to Engram controllers.
	EngramControllerConfig EngramControllerConfig `json:"engramControllerConfig,omitempty"`
}

// ImpulseConfig contains Impulse controller settings
type ImpulseConfig struct {
	// MaxConcurrentReconciles is the maximum number of concurrent reconciles
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`

	// RateLimiter configuration
	RateLimiter RateLimiterConfig `json:"rateLimiter,omitempty"`
}

// TemplateConfig contains Template controller settings (EngramTemplate, ImpulseTemplate)
type TemplateConfig struct {
	// MaxConcurrentReconciles is the maximum number of concurrent reconciles
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`

	// RateLimiter configuration
	RateLimiter RateLimiterConfig `json:"rateLimiter,omitempty"`
}

// TransportControllerConfig contains Transport controller settings.
type TransportControllerConfig struct {
	// MaxConcurrentReconciles is the maximum number of concurrent reconciles
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`

	// RateLimiter configuration
	RateLimiter RateLimiterConfig `json:"rateLimiter,omitempty"`

	// HeartbeatTimeout is how long the operator waits before marking a transport binding stale.
	HeartbeatTimeout time.Duration `json:"heartbeatTimeout,omitempty"`
}

// RateLimiterConfig contains rate limiter settings
type RateLimiterConfig struct {
	// BaseDelay is the base delay for exponential backoff
	BaseDelay time.Duration `json:"baseDelay,omitempty"`

	// MaxDelay is the maximum delay for exponential backoff
	MaxDelay time.Duration `json:"maxDelay,omitempty"`
}

// EngramControllerConfig holds configuration specific to Engram controllers.
type EngramControllerConfig struct {
	// DefaultGRPCPort is the default port used for gRPC communication with realtime engrams.
	DefaultGRPCPort int `json:"defaultGRPCPort,omitempty"`
	// DefaultGRPCHeartbeatIntervalSeconds defines how frequently SDKs send heartbeat pings to the hub.
	DefaultGRPCHeartbeatIntervalSeconds int `json:"defaultGRPCHeartbeatIntervalSeconds,omitempty"`
	// DefaultMaxInlineSize is the default maximum size in bytes for inputs/outputs
	// to be passed directly as environment variables. Larger values will be offloaded
	// to the configured storage backend.
	DefaultMaxInlineSize int `json:"defaultMaxInlineSize,omitempty"`
	// DefaultStorageTimeout is the timeout for storage operations (upload/download) in seconds.
	// This should be tuned based on expected output sizes and S3 latency:
	//   timeout >= (max_output_mb / upload_bandwidth_mbps) * 1.5 + baseline_latency_sec
	DefaultStorageTimeoutSeconds int `json:"defaultStorageTimeoutSeconds,omitempty"`
	// DefaultGracefulShutdownTimeoutSeconds is the timeout for streaming engrams to drain
	// in-flight messages during graceful shutdown. Should be set to terminationGracePeriodSeconds - 10s
	// to leave adequate margin before Kubernetes sends SIGKILL.
	DefaultGracefulShutdownTimeoutSeconds int `json:"defaultGracefulShutdownTimeoutSeconds,omitempty"`
	// DefaultTerminationGracePeriodSeconds is the grace period for pod termination.
	// Applies to both batch Jobs and streaming Deployments.
	// Note: Cast to *int64 when assigning to PodSpec.TerminationGracePeriodSeconds.
	DefaultTerminationGracePeriodSeconds int `json:"defaultTerminationGracePeriodSeconds,omitempty"`

	// DefaultMaxRecvMsgBytes is the default max gRPC message size for receiving.
	DefaultMaxRecvMsgBytes int `json:"defaultMaxRecvMsgBytes,omitempty"`
	// DefaultMaxSendMsgBytes is the default max gRPC message size for sending.
	DefaultMaxSendMsgBytes int `json:"defaultMaxSendMsgBytes,omitempty"`
	// DefaultDialTimeoutSeconds is the client-side timeout for establishing a gRPC connection.
	DefaultDialTimeoutSeconds int `json:"defaultDialTimeoutSeconds,omitempty"`
	// DefaultChannelBufferSize is the in-memory channel buffer size for SDK streams.
	DefaultChannelBufferSize int `json:"defaultChannelBufferSize,omitempty"`
	// DefaultReconnectMaxRetries is the number of retries on transient gRPC connection errors.
	DefaultReconnectMaxRetries int `json:"defaultReconnectMaxRetries,omitempty"`
	// DefaultReconnectBaseBackoffMillis is the base backoff delay for reconnect attempts.
	DefaultReconnectBaseBackoffMillis int `json:"defaultReconnectBaseBackoffMillis,omitempty"`
	// DefaultReconnectMaxBackoffSeconds is the max backoff delay for reconnect attempts.
	DefaultReconnectMaxBackoffSeconds int `json:"defaultReconnectMaxBackoffSeconds,omitempty"`
	// DefaultHangTimeoutSeconds is the timeout for detecting a hung gRPC stream (no heartbeats).
	// Set to 0 to disable the hang watchdog.
	DefaultHangTimeoutSeconds int `json:"defaultHangTimeoutSeconds,omitempty"`
	// DefaultMessageTimeoutSeconds is the timeout for individual message operations.
	DefaultMessageTimeoutSeconds int `json:"defaultMessageTimeoutSeconds,omitempty"`
}

// TransportConfig holds configuration for the gRPC transport layer.
type TransportConfig struct {
	GRPC              GRPCConfig    `json:"grpc,omitempty"`
	HeartbeatInterval time.Duration `json:"heartbeatInterval,omitempty"`
}

// GRPCConfig holds gRPC-related settings for the operator and SDKs.
type GRPCConfig struct {
	// EnableDownstreamTargets controls whether the operator computes and injects
	// `BUBU_DOWNSTREAM_TARGETS` into batch-mode StepRuns that are upstream of
	// any streaming-mode Engrams in a Story.
	EnableDownstreamTargets bool `json:"enableDownstreamTargets,omitempty"`
	// DefaultTLSSecret provides a fallback TLS Secret when Engrams do not specify one.
	DefaultTLSSecret string `json:"defaultTLSSecret,omitempty"`
}

// DefaultControllerConfig returns the default configuration
func controllerConfigDefaults() ControllerConfig {
	return ControllerConfig{
		ImagePullPolicy:                  corev1.PullIfNotPresent,
		MaxConcurrentReconciles:          contracts.DefaultControllerMaxConcurrentReconciles,
		ServiceAccountName:               "",
		AutomountServiceAccountToken:     false,
		DefaultCPURequest:                "100m",
		DefaultCPULimit:                  "500m",
		DefaultMemoryRequest:             "128Mi",
		DefaultMemoryLimit:               "512Mi",
		JobBackoffLimit:                  0,
		JobRestartPolicy:                 corev1.RestartPolicyNever,
		TTLSecondsAfterFinished:          3600,
		StreamingTTLSecondsAfterFinished: 0,
		MaxRetries:                       3,
		CleanupInterval:                  metav1.Duration{Duration: time.Hour},
		ReconcileTimeout:                 30 * time.Second,
		StoryRun: StoryRunConfig{
			MaxConcurrentReconciles: 8,
			RateLimiter: RateLimiterConfig{
				BaseDelay: 50 * time.Millisecond,
				MaxDelay:  5 * time.Minute,
			},
			MaxInlineInputsSize: 1 * 1024, // 1 KiB
			Binding: BindingControllerTuning{
				MaxMutationsPerReconcile: 8,
				ThrottleRequeueDelay:     2 * time.Second,
			},
			Scheduling: SchedulingConfig{
				GlobalConcurrency: 0,
				Queues: map[string]QueueConfig{
					"default": {
						Concurrency:          0,
						DefaultPriority:      0,
						PriorityAgingSeconds: 60,
					},
				},
			},
		},
		StepRun: StepRunConfig{
			MaxConcurrentReconciles: 15,
			RateLimiter: RateLimiterConfig{
				BaseDelay: 100 * time.Millisecond,
				MaxDelay:  2 * time.Minute,
			},
		},
		Story: StoryConfig{
			MaxConcurrentReconciles: 5,
			RateLimiter: RateLimiterConfig{
				BaseDelay: 200 * time.Millisecond,
				MaxDelay:  1 * time.Minute,
			},
			Binding: BindingControllerTuning{
				MaxMutationsPerReconcile: 4,
				ThrottleRequeueDelay:     3 * time.Second,
			},
		},
		Engram: EngramConfig{
			MaxConcurrentReconciles: 5,
			RateLimiter: RateLimiterConfig{
				BaseDelay: 200 * time.Millisecond,
				MaxDelay:  1 * time.Minute,
			},
			EngramControllerConfig: EngramControllerConfig{
				DefaultGRPCPort:                       50051,
				DefaultGRPCHeartbeatIntervalSeconds:   10,
				DefaultMaxInlineSize:                  4 * 1024,         // 4 KiB
				DefaultStorageTimeoutSeconds:          300,              // 5 minutes (aligns with SDK default)
				DefaultGracefulShutdownTimeoutSeconds: 20,               // 20s (aligns with SDK default)
				DefaultTerminationGracePeriodSeconds:  30,               // 30s (Kubernetes default)
				DefaultMaxRecvMsgBytes:                10 * 1024 * 1024, // 10 MiB
				DefaultMaxSendMsgBytes:                10 * 1024 * 1024, // 10 MiB
				DefaultDialTimeoutSeconds:             10,
				DefaultChannelBufferSize:              16,
				DefaultReconnectMaxRetries:            10,
				DefaultReconnectBaseBackoffMillis:     500,
				DefaultReconnectMaxBackoffSeconds:     30,
				DefaultHangTimeoutSeconds:             0,
				DefaultMessageTimeoutSeconds:          30,
			},
		},
		Impulse: ImpulseConfig{
			MaxConcurrentReconciles: 5,
			RateLimiter: RateLimiterConfig{
				BaseDelay: 200 * time.Millisecond,
				MaxDelay:  1 * time.Minute,
			},
		},
		Template: TemplateConfig{
			MaxConcurrentReconciles: 2, // Templates change less frequently
			RateLimiter: RateLimiterConfig{
				BaseDelay: 500 * time.Millisecond, // Slower rate for templates
				MaxDelay:  10 * time.Minute,       // Longer backoff
			},
		},
		TransportController: TransportControllerConfig{
			MaxConcurrentReconciles: 2,
			RateLimiter: RateLimiterConfig{
				BaseDelay: 200 * time.Millisecond,
				MaxDelay:  1 * time.Minute,
			},
			HeartbeatTimeout: 2 * time.Minute,
		},
		MaxStoryWithBlockSizeBytes:    64 * 1024, // 64 KiB
		TemplateEvaluationTimeout:     30 * time.Second,
		TemplateMaxOutputBytes:        64 * 1024,
		TemplateDeterministic:         false,
		TemplateOffloadedPolicy:       TemplatingOffloadedPolicyInject, // inject so materialize StepRuns are created when templates reference offloaded data
		TemplateMaterializeEngram:     "materialize",                   // must match EngramTemplate name from materialize-engram (e.g. metadata.name: materialize)
		ReferenceCrossNamespacePolicy: ReferenceCrossNamespacePolicyDeny,
		Transport: TransportConfig{
			HeartbeatInterval: 30 * time.Second,
			GRPC: GRPCConfig{
				EnableDownstreamTargets: true,
			},
		},
		DropCapabilities: []string{"ALL"},
	}
}

// DefaultControllerConfig returns the default configuration
func DefaultControllerConfig() *ControllerConfig {
	cfg := controllerConfigDefaults()
	return &cfg
}

// buildControllerOptions builds controller.Options with an exponential failure rate limiter.
//
// The limiter delays prefer the per-controller RateLimiterConfig over the global
// ControllerConfig Requeue*Delay settings, falling back to the provided defaults.
func (c *ControllerConfig) buildControllerOptions(maxConcurrentReconciles int, limiter RateLimiterConfig, defaultBaseDelay, defaultMaxDelay time.Duration) controller.Options {
	concurrency := maxConcurrentReconciles
	if concurrency <= 0 {
		concurrency = c.MaxConcurrentReconciles
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	return controller.Options{
		MaxConcurrentReconciles: concurrency,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			kubeutil.FirstPositiveDuration(limiter.BaseDelay, c.RequeueBaseDelay, defaultBaseDelay),
			kubeutil.FirstPositiveDuration(limiter.MaxDelay, c.RequeueMaxDelay, defaultMaxDelay),
		),
	}
}

// BuildStoryRunControllerOptions builds controller.Options for the StoryRun controller
// by applying configured concurrency and exponential failure backoff.
//
// Behavior:
//   - Sets MaxConcurrentReconciles from c.StoryRun.MaxConcurrentReconciles.
//   - Uses an exponential failure rate limiter with base/max delays resolved via kubeutil.FirstPositiveDuration,
//     preferring c.StoryRun.RateLimiter over the global c.Requeue*Delay settings.
//
// Returns:
//   - controller.Options: configured options used during controller registration.
func (c *ControllerConfig) BuildStoryRunControllerOptions() controller.Options {
	return c.buildControllerOptions(
		c.StoryRun.MaxConcurrentReconciles,
		c.StoryRun.RateLimiter,
		100*time.Millisecond,
		2*time.Minute,
	)
}

// BuildStepRunControllerOptions builds controller.Options for the StepRun controller
// by applying configured concurrency and exponential failure backoff.
//
// Behavior:
//   - Sets MaxConcurrentReconciles from c.StepRun.MaxConcurrentReconciles.
//   - Uses an exponential failure rate limiter with base/max delays resolved via kubeutil.FirstPositiveDuration,
//     preferring c.StepRun.RateLimiter over the global c.Requeue*Delay settings.
//
// Returns:
//   - controller.Options: configured options used during controller registration.
func (c *ControllerConfig) BuildStepRunControllerOptions() controller.Options {
	return c.buildControllerOptions(
		c.StepRun.MaxConcurrentReconciles,
		c.StepRun.RateLimiter,
		100*time.Millisecond,
		2*time.Minute,
	)
}

// BuildStoryControllerOptions builds controller.Options for the Story controller by
// applying configured concurrency and exponential failure backoff.
//
// Behavior:
//   - Sets MaxConcurrentReconciles from c.Story.MaxConcurrentReconciles.
//   - Uses an exponential failure rate limiter with base/max delays resolved via kubeutil.FirstPositiveDuration,
//     preferring c.Story.RateLimiter over the global c.Requeue*Delay settings.
//
// Returns:
//   - controller.Options: configured options used during controller registration.
func (c *ControllerConfig) BuildStoryControllerOptions() controller.Options {
	return c.buildControllerOptions(
		c.Story.MaxConcurrentReconciles,
		c.Story.RateLimiter,
		200*time.Millisecond,
		1*time.Minute,
	)
}

// BuildEngramControllerOptions builds controller.Options for the Engram controller by
// applying configured concurrency and exponential failure backoff.
//
// Behavior:
//   - Sets MaxConcurrentReconciles from c.Engram.MaxConcurrentReconciles.
//   - Uses an exponential failure rate limiter with base/max delays resolved via kubeutil.FirstPositiveDuration,
//     preferring c.Engram.RateLimiter over the global c.Requeue*Delay settings.
//
// Returns:
//   - controller.Options: configured options used during controller registration.
func (c *ControllerConfig) BuildEngramControllerOptions() controller.Options {
	return c.buildControllerOptions(
		c.Engram.MaxConcurrentReconciles,
		c.Engram.RateLimiter,
		200*time.Millisecond,
		1*time.Minute,
	)
}

// BuildImpulseControllerOptions builds controller.Options for the Impulse controller by
// applying configured concurrency and exponential failure backoff.
//
// Behavior:
//   - Sets MaxConcurrentReconciles from c.Impulse.MaxConcurrentReconciles.
//   - Uses an exponential failure rate limiter with base/max delays resolved via kubeutil.FirstPositiveDuration,
//     preferring c.Impulse.RateLimiter over the global c.Requeue*Delay settings.
//
// Returns:
//   - controller.Options: configured options used during controller registration.
func (c *ControllerConfig) BuildImpulseControllerOptions() controller.Options {
	return c.buildControllerOptions(
		c.Impulse.MaxConcurrentReconciles,
		c.Impulse.RateLimiter,
		200*time.Millisecond,
		1*time.Minute,
	)
}

// BuildTemplateControllerOptions builds controller.Options for template reconcilers by
// applying configured concurrency and exponential failure backoff.
//
// Behavior:
//   - Sets MaxConcurrentReconciles from c.Template.MaxConcurrentReconciles.
//   - Uses an exponential failure rate limiter with base/max delays resolved via kubeutil.FirstPositiveDuration,
//     preferring c.Template.RateLimiter over the global c.Requeue*Delay settings.
//
// Returns:
//   - controller.Options: configured options used during controller registration.
func (c *ControllerConfig) BuildTemplateControllerOptions() controller.Options {
	return c.buildControllerOptions(
		c.Template.MaxConcurrentReconciles,
		c.Template.RateLimiter,
		500*time.Millisecond,
		10*time.Minute,
	)
}

// BuildTransportControllerOptions builds controller.Options for the Transport controller by
// applying configured concurrency and exponential failure backoff.
//
// Behavior:
//   - Uses c.TransportController.MaxConcurrentReconciles when positive; otherwise falls back
//     to the global MaxConcurrentReconciles via buildControllerOptions.
//   - Uses an exponential failure rate limiter with base/max delays resolved via kubeutil.FirstPositiveDuration,
//     preferring c.TransportController.RateLimiter over the global c.Requeue*Delay settings.
//
// Returns:
//   - controller.Options: configured options used during controller registration.
func (c *ControllerConfig) BuildTransportControllerOptions() controller.Options {
	return c.buildControllerOptions(
		c.TransportController.MaxConcurrentReconciles,
		c.TransportController.RateLimiter,
		200*time.Millisecond,
		1*time.Minute,
	)
}

// BuildCleanupControllerOptions builds controller.Options for cleanup controllers with
// conservative concurrency and a fixed exponential failure backoff.
//
// Returns:
//   - controller.Options: configured options used during controller registration.
func (c *ControllerConfig) BuildCleanupControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: 1, // Only one cleanup at a time
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			time.Minute, // Base delay
			time.Hour,   // Max delay
		),
	}
}
