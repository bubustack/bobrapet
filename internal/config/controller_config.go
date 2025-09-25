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
	JobBackoffLimit         int32                `json:"jobBackoffLimit,omitempty"`
	JobRestartPolicy        corev1.RestartPolicy `json:"jobRestartPolicy,omitempty"`
	TTLSecondsAfterFinished int32                `json:"ttlSecondsAfterFinished,omitempty"`
	ServiceAccountName      string               `json:"serviceAccountName,omitempty"`

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
				DefaultGRPCPort:      50051,
				DefaultMaxInlineSize: 1024, // 1 KiB
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
	}
}

// BuildControllerOptions builds controller.Options from config
func (c *ControllerConfig) BuildStoryRunControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.StoryRun.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			c.StoryRun.RateLimiter.BaseDelay,
			c.StoryRun.RateLimiter.MaxDelay,
		),
	}
}

// BuildStepRunControllerOptions builds controller.Options for StepRun
func (c *ControllerConfig) BuildStepRunControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.StepRun.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			c.StepRun.RateLimiter.BaseDelay,
			c.StepRun.RateLimiter.MaxDelay,
		),
	}
}

// BuildStoryControllerOptions builds controller.Options for Story
func (c *ControllerConfig) BuildStoryControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.Story.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			c.Story.RateLimiter.BaseDelay,
			c.Story.RateLimiter.MaxDelay,
		),
	}
}

// BuildEngramControllerOptions builds controller.Options for Engram
func (c *ControllerConfig) BuildEngramControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.Engram.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			c.Engram.RateLimiter.BaseDelay,
			c.Engram.RateLimiter.MaxDelay,
		),
	}
}

// BuildImpulseControllerOptions builds controller.Options for Impulse
func (c *ControllerConfig) BuildImpulseControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.Impulse.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			c.Impulse.RateLimiter.BaseDelay,
			c.Impulse.RateLimiter.MaxDelay,
		),
	}
}

// BuildTemplateControllerOptions builds controller.Options for Template controllers
func (c *ControllerConfig) BuildTemplateControllerOptions() controller.Options {
	return controller.Options{
		MaxConcurrentReconciles: c.Template.MaxConcurrentReconciles,
		RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](
			c.Template.RateLimiter.BaseDelay,
			c.Template.RateLimiter.MaxDelay,
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
