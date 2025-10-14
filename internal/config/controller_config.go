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
	"time"

	"github.com/bubustack/bobrapet/pkg/cel"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// ControllerDependencies holds all the shared dependencies required by the controllers.
// This struct is created once in main.go and passed to each controller, ensuring
// consistent access to shared services like configuration resolvers and CEL evaluators.
type ControllerDependencies struct {
	client.Client
	Scheme         *runtime.Scheme
	ConfigResolver *Resolver
	CELEvaluator   cel.Evaluator
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

	// MaxStoryWithBlockSizeBytes is the maximum allowed size for a Story's spec.steps.with block.
	// This prevents oversized resources from being stored in etcd.
	// +optional
	MaxStoryWithBlockSizeBytes int `json:"maxStoryWithBlockSizeBytes,omitempty"`

	DefaultEngramImage    string `json:"defaultEngramImage,omitempty"`
	DefaultImpulseImage   string `json:"defaultImpulseImage,omitempty"`
	DefaultEngramGRPCPort int    `json:"defaultEngramGRPCPort,omitempty"`
	DefaultCPURequest     string `json:"defaultCPURequest,omitempty"`
	DefaultCPULimit       string `json:"defaultCPULimit,omitempty"`
	DefaultMemoryRequest  string `json:"defaultMemoryRequest,omitempty"`
	DefaultMemoryLimit    string `json:"defaultMemoryLimit,omitempty"`

	// Global Controller Configuration
	MaxConcurrentReconciles int             `json:"maxConcurrentReconciles,omitempty"`
	RequeueBaseDelay        time.Duration   `json:"requeueBaseDelay,omitempty"`
	RequeueMaxDelay         time.Duration   `json:"requeueMaxDelay,omitempty"`
	HealthCheckInterval     time.Duration   `json:"healthCheckInterval,omitempty"`
	CleanupInterval         metav1.Duration `json:"cleanupInterval,omitempty"`

	// ReconcileTimeout bounds the duration of a single reconcile loop.
	// Set to 0 to disable deadline (not recommended for production).
	ReconcileTimeout time.Duration `json:"reconcileTimeout,omitempty"`

	// Image Configuration
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Resource Limits
	EngramCPURequest    string `json:"engramCpuRequest,omitempty"`
	EngramCPULimit      string `json:"engramCpuLimit,omitempty"`
	EngramMemoryRequest string `json:"engramMemoryRequest,omitempty"`
	EngramMemoryLimit   string `json:"engramMemoryLimit,omitempty"`

	// Retry and Timeout Configuration
	MaxRetries             int           `json:"maxRetries,omitempty"`
	ExponentialBackoffBase time.Duration `json:"exponentialBackoffBase,omitempty"`
	ExponentialBackoffMax  time.Duration `json:"exponentialBackoffMax,omitempty"`
	DefaultStepTimeout     time.Duration `json:"defaultStepTimeout,omitempty"`
	ApprovalDefaultTimeout time.Duration `json:"approvalDefaultTimeout,omitempty"`
	ExternalDataTimeout    time.Duration `json:"externalDataTimeout,omitempty"`
	ConditionalTimeout     time.Duration `json:"conditionalTimeout,omitempty"`

	// Loop Processing Configuration
	MaxLoopIterations    int `json:"maxLoopIterations,omitempty"`
	DefaultLoopBatchSize int `json:"defaultLoopBatchSize,omitempty"`
	MaxLoopBatchSize     int `json:"maxLoopBatchSize,omitempty"`
	MaxLoopConcurrency   int `json:"maxLoopConcurrency,omitempty"`
	MaxConcurrencyLimit  int `json:"maxConcurrencyLimit,omitempty"`

	// Security Configuration
	RunAsNonRoot             bool     `json:"runAsNonRoot,omitempty"`
	ReadOnlyRootFilesystem   bool     `json:"readOnlyRootFilesystem,omitempty"`
	AllowPrivilegeEscalation bool     `json:"allowPrivilegeEscalation,omitempty"`
	DropCapabilities         []string `json:"dropCapabilities,omitempty"`
	RunAsUser                int64    `json:"runAsUser,omitempty"`

	// Job Configuration
	JobBackoffLimit              int32                `json:"jobBackoffLimit,omitempty"`
	JobRestartPolicy             corev1.RestartPolicy `json:"jobRestartPolicy,omitempty"`
	TTLSecondsAfterFinished      int32                `json:"ttlSecondsAfterFinished,omitempty"`
	ServiceAccountName           string               `json:"serviceAccountName,omitempty"`
	AutomountServiceAccountToken bool                 `json:"automountServiceAccountToken,omitempty"`

	// CEL Configuration
	CELEvaluationTimeout   time.Duration `json:"celEvaluationTimeout,omitempty"`
	CELMaxExpressionLength int           `json:"celMaxExpressionLength,omitempty"`
	CELEnableMacros        bool          `json:"celEnableMacros,omitempty"`

	// Telemetry Configuration
	TelemetryEnabled        bool `json:"telemetryEnabled,omitempty"`
	TracePropagationEnabled bool `json:"tracePropagationEnabled,omitempty"`

	// Development/Debug Configuration
	EnableVerboseLogging    bool `json:"enableVerboseLogging,omitempty"`
	EnableStepOutputLogging bool `json:"enableStepOutputLogging,omitempty"`
	EnableMetrics           bool `json:"enableMetrics,omitempty"`
}

// Telemetry feature gate
var telemetryEnabled bool

// EnableTelemetry enables or disables OpenTelemetry spans in controllers
func EnableTelemetry(enabled bool) { telemetryEnabled = enabled }

// IsTelemetryEnabled reports whether OpenTelemetry spans should be emitted
func IsTelemetryEnabled() bool { return telemetryEnabled }

// StoryRunConfig contains StoryRun controller settings
type StoryRunConfig struct {
	// MaxConcurrentReconciles is the maximum number of concurrent reconciles
	MaxConcurrentReconciles int `json:"maxConcurrentReconciles,omitempty"`

	// RateLimiter configuration
	RateLimiter RateLimiterConfig `json:"rateLimiter,omitempty"`

	// MaxInlineInputsSize is the maximum size in bytes for StoryRun spec.inputs.
	// Payloads larger than this will be rejected by the controller if webhooks are disabled.
	MaxInlineInputsSize int `json:"maxInlineInputsSize,omitempty"`
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
	DefaultTerminationGracePeriodSeconds int64 `json:"defaultTerminationGracePeriodSeconds,omitempty"`

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
	DefaultHangTimeoutSeconds int `json:"defaultHangTimeoutSeconds,omitempty"`
	// DefaultMessageTimeoutSeconds is the timeout for individual message operations.
	DefaultMessageTimeoutSeconds int `json:"defaultMessageTimeoutSeconds,omitempty"`
}

// DefaultControllerConfig returns the default configuration
func DefaultControllerConfig() *ControllerConfig {
	return &ControllerConfig{
		StoryRun: StoryRunConfig{
			MaxConcurrentReconciles: 8,
			RateLimiter: RateLimiterConfig{
				BaseDelay: 50 * time.Millisecond,
				MaxDelay:  5 * time.Minute,
			},
			MaxInlineInputsSize: 1 * 1024, // 1 KiB
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
		},
		Engram: EngramConfig{
			MaxConcurrentReconciles: 5,
			RateLimiter: RateLimiterConfig{
				BaseDelay: 200 * time.Millisecond,
				MaxDelay:  1 * time.Minute,
			},
			EngramControllerConfig: EngramControllerConfig{
				DefaultGRPCPort:                       50051,
				DefaultMaxInlineSize:                  1 * 1024,         // 1 KiB
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
				DefaultHangTimeoutSeconds:             30,
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
		MaxStoryWithBlockSizeBytes: 64 * 1024, // 64 KiB
		ReconcileTimeout:           1 * time.Minute,
	}
}

// BuildControllerOptions builds controller.Options from config
func (c *ControllerConfig) BuildStoryRunControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.StoryRun.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			pickDuration(c.StoryRun.RateLimiter.BaseDelay, c.RequeueBaseDelay, 100*time.Millisecond),
			pickDuration(c.StoryRun.RateLimiter.MaxDelay, c.RequeueMaxDelay, 2*time.Minute),
		),
	}
}

// BuildStepRunControllerOptions builds controller.Options for StepRun
func (c *ControllerConfig) BuildStepRunControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.StepRun.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			pickDuration(c.StepRun.RateLimiter.BaseDelay, c.RequeueBaseDelay, 100*time.Millisecond),
			pickDuration(c.StepRun.RateLimiter.MaxDelay, c.RequeueMaxDelay, 2*time.Minute),
		),
	}
}

// BuildStoryControllerOptions builds controller.Options for Story
func (c *ControllerConfig) BuildStoryControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.Story.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			pickDuration(c.Story.RateLimiter.BaseDelay, c.RequeueBaseDelay, 200*time.Millisecond),
			pickDuration(c.Story.RateLimiter.MaxDelay, c.RequeueMaxDelay, 1*time.Minute),
		),
	}
}

// BuildEngramControllerOptions builds controller.Options for Engram
func (c *ControllerConfig) BuildEngramControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.Engram.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			pickDuration(c.Engram.RateLimiter.BaseDelay, c.RequeueBaseDelay, 200*time.Millisecond),
			pickDuration(c.Engram.RateLimiter.MaxDelay, c.RequeueMaxDelay, 1*time.Minute),
		),
	}
}

// BuildImpulseControllerOptions builds controller.Options for Impulse
func (c *ControllerConfig) BuildImpulseControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.Impulse.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			pickDuration(c.Impulse.RateLimiter.BaseDelay, c.RequeueBaseDelay, 200*time.Millisecond),
			pickDuration(c.Impulse.RateLimiter.MaxDelay, c.RequeueMaxDelay, 1*time.Minute),
		),
	}
}

// BuildTemplateControllerOptions builds controller.Options for Template controllers
func (c *ControllerConfig) BuildTemplateControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.Template.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			pickDuration(c.Template.RateLimiter.BaseDelay, c.RequeueBaseDelay, 500*time.Millisecond),
			pickDuration(c.Template.RateLimiter.MaxDelay, c.RequeueMaxDelay, 10*time.Minute),
		),
	}
}

// BuildCleanupControllerOptions builds controller.Options for Cleanup
func (c *ControllerConfig) BuildCleanupControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: 1, // Only one cleanup at a time
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			time.Minute, // Base delay
			time.Hour,   // Max delay
		),
	}
}

// pickDuration returns the first non-zero duration in priority order,
// falling back to a sane default to prevent zero-delay hot requeues.
func pickDuration(primary, secondary, def time.Duration) time.Duration {
	if primary > 0 {
		return primary
	}
	if secondary > 0 {
		return secondary
	}
	return def
}
