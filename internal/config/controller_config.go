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

	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

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
