/*
Copyright 2024.

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
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
)

// Resolver resolves configuration with hierarchical priority
// Priority (highest to lowest): StepRun > Story.Policy > Namespace > Controller Config
type Resolver struct {
	client        client.Client
	configManager *OperatorConfigManager
}

// NewResolver creates a new configuration resolver
func NewResolver(client client.Client, configManager *OperatorConfigManager) *Resolver {
	return &Resolver{
		client:        client,
		configManager: configManager,
	}
}

// GetOperatorConfig returns the current, raw operator configuration.
// This is the primary way for controllers to access global, non-hierarchical
// configuration values like default ports or image names.
func (cr *Resolver) GetOperatorConfig() *OperatorConfig {
	return cr.configManager.GetConfig()
}

// ResolvedExecutionConfig represents the final resolved configuration for a StepRun
type ResolvedExecutionConfig struct {
	Image                        string
	ImagePullPolicy              corev1.PullPolicy
	Resources                    corev1.ResourceRequirements
	Storage                      *v1alpha1.StoragePolicy
	ServiceAccountName           string
	AutomountServiceAccountToken bool
	RunAsNonRoot                 bool
	ReadOnlyRootFilesystem       bool
	AllowPrivilegeEscalation     bool
	RunAsUser                    int64
	BackoffLimit                 int32
	TTLSecondsAfterFinished      int32
	RestartPolicy                corev1.RestartPolicy
	DefaultStepTimeout           time.Duration
	MaxRetries                   int
	BackoffBase                  time.Duration
	BackoffMax                   time.Duration
	Secrets                      map[string]string

	// Health check probes (from template, can be disabled at instance level)
	LivenessProbe  *corev1.Probe
	ReadinessProbe *corev1.Probe
	StartupProbe   *corev1.Probe

	// Service configuration (from template)
	ServicePorts       []corev1.ServicePort
	ServiceLabels      map[string]string
	ServiceAnnotations map[string]string
}

// ResolveImagePullPolicy resolves the image pull policy from the hierarchy.
func (cr *Resolver) ResolveImagePullPolicy(ctx context.Context, step *runsv1alpha1.StepRun, story *v1alpha1.Story, engram *v1alpha1.Engram, template *catalogv1alpha1.EngramTemplate) corev1.PullPolicy {
	config := cr.configManager.GetConfig()
	return config.Controller.ImagePullPolicy
}

// ResolveExecutionConfig resolves the final execution configuration for a step
// by merging settings from all levels of the hierarchy.
// The precedence order is: StepRun -> Story Step -> Engram -> EngramTemplate -> Operator Config -> Hardcoded Defaults.
func (cr *Resolver) ResolveExecutionConfig(ctx context.Context, step *runsv1alpha1.StepRun, story *v1alpha1.Story, engram *v1alpha1.Engram, template *catalogv1alpha1.EngramTemplate) (*ResolvedExecutionConfig, error) {
	operatorConfig := cr.configManager.GetConfig()

	// 1. Start with hardcoded defaults and operator-level configuration.
	resolved := cr.getOperatorDefaults(operatorConfig)

	// 2. Apply EngramTemplate settings.
	cr.applyEngramTemplateConfig(template, resolved)

	// 3. Apply Engram-specific settings, which override the template.
	cr.applyEngramConfig(engram, resolved)

	// 4. Apply Story-level policies and step-specific overrides.
	cr.applyStoryConfig(story, step, resolved)

	// 5. Apply StepRun-specific overrides, which have the highest priority.
	cr.applyStepRunOverrides(step, resolved)

	// 6. Finalize the ServiceAccountName. If nothing else has specified one,
	// default to the one managed by the storyrun controller for the engram pods.
	if step != nil && (resolved.ServiceAccountName == "" || resolved.ServiceAccountName == "default") {
		resolved.ServiceAccountName = fmt.Sprintf("%s-engram-runner", step.Spec.StoryRunRef.Name)
	}

	return resolved, nil
}

// getOperatorDefaults initializes the configuration with values from the operator's config map.
func (cr *Resolver) getOperatorDefaults(config *OperatorConfig) *ResolvedExecutionConfig {
	return &ResolvedExecutionConfig{
		ImagePullPolicy: corev1.PullPolicy(config.Controller.ImagePullPolicy),
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(config.Controller.DefaultCPURequest),
				corev1.ResourceMemory: resource.MustParse(config.Controller.DefaultMemoryRequest),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(config.Controller.DefaultCPULimit),
				corev1.ResourceMemory: resource.MustParse(config.Controller.DefaultMemoryLimit),
			},
		},
		AutomountServiceAccountToken: config.Controller.AutomountServiceAccountToken,
		RunAsNonRoot:                 config.Controller.RunAsNonRoot,
		ReadOnlyRootFilesystem:       config.Controller.ReadOnlyRootFilesystem,
		AllowPrivilegeEscalation:     config.Controller.AllowPrivilegeEscalation,
		RunAsUser:                    config.Controller.RunAsUser,
		BackoffLimit:                 config.Controller.JobBackoffLimit,
		TTLSecondsAfterFinished:      config.Controller.TTLSecondsAfterFinished,
		RestartPolicy:                corev1.RestartPolicy(config.Controller.JobRestartPolicy),
		ServiceAccountName:           config.Controller.ServiceAccountName,
		DefaultStepTimeout:           config.Controller.DefaultStepTimeout,
		MaxRetries:                   config.Controller.MaxRetries,
		BackoffBase:                  config.Controller.ExponentialBackoffBase,
		BackoffMax:                   config.Controller.ExponentialBackoffMax,
	}
}

// applyEngramTemplateConfig applies settings from the EngramTemplate.
func (cr *Resolver) applyEngramTemplateConfig(template *catalogv1alpha1.EngramTemplate, config *ResolvedExecutionConfig) {
	if template == nil {
		return
	}

	// Image from template spec directly
	if template.Spec.Image != "" {
		config.Image = template.Spec.Image
	}

	if template.Spec.Execution == nil {
		return
	}
	exec := template.Spec.Execution

	// Image
	if exec.Images != nil && exec.Images.PullPolicy != nil {
		switch *exec.Images.PullPolicy {
		case "Always":
			config.ImagePullPolicy = corev1.PullAlways
		case "Never":
			config.ImagePullPolicy = corev1.PullNever
		case "IfNotPresent":
			config.ImagePullPolicy = corev1.PullIfNotPresent
		}
	}

	// Resources
	if exec.Resources != nil {
		if exec.Resources.RecommendedCPURequest != nil {
			config.Resources.Requests[corev1.ResourceCPU] = resource.MustParse(*exec.Resources.RecommendedCPURequest)
		}
		if exec.Resources.RecommendedCPULimit != nil {
			config.Resources.Limits[corev1.ResourceCPU] = resource.MustParse(*exec.Resources.RecommendedCPULimit)
		}
		if exec.Resources.RecommendedMemoryRequest != nil {
			config.Resources.Requests[corev1.ResourceMemory] = resource.MustParse(*exec.Resources.RecommendedMemoryRequest)
		}
		if exec.Resources.RecommendedMemoryLimit != nil {
			config.Resources.Limits[corev1.ResourceMemory] = resource.MustParse(*exec.Resources.RecommendedMemoryLimit)
		}
	}

	// Security
	if exec.Security != nil {
		if exec.Security.RequiresNonRoot != nil {
			config.RunAsNonRoot = *exec.Security.RequiresNonRoot
		}
		if exec.Security.RequiresReadOnlyRoot != nil {
			config.ReadOnlyRootFilesystem = *exec.Security.RequiresReadOnlyRoot
		}
		if exec.Security.RequiresNoPrivilegeEscalation != nil {
			config.AllowPrivilegeEscalation = !*exec.Security.RequiresNoPrivilegeEscalation
		}
		if exec.Security.RecommendedRunAsUser != nil {
			config.RunAsUser = *exec.Security.RecommendedRunAsUser
		}
	}

	// Job
	if exec.Job != nil {
		if exec.Job.RecommendedBackoffLimit != nil {
			config.BackoffLimit = *exec.Job.RecommendedBackoffLimit
		}
		if exec.Job.RecommendedTTLSecondsAfterFinished != nil {
			config.TTLSecondsAfterFinished = *exec.Job.RecommendedTTLSecondsAfterFinished
		}
		if exec.Job.RecommendedRestartPolicy != nil {
			switch *exec.Job.RecommendedRestartPolicy {
			case "Never":
				config.RestartPolicy = corev1.RestartPolicyNever
			case "OnFailure":
				config.RestartPolicy = corev1.RestartPolicyOnFailure
			}
		}
	}

	// Timeout & Retry
	if exec.Timeout != nil {
		if d, err := time.ParseDuration(*exec.Timeout); err == nil {
			config.DefaultStepTimeout = d
		}
	}
	if exec.Retry != nil && exec.Retry.RecommendedMaxRetries != nil {
		config.MaxRetries = *exec.Retry.RecommendedMaxRetries
	}

	// Health Check Probes
	if exec.Probes != nil {
		if exec.Probes.Liveness != nil {
			config.LivenessProbe = exec.Probes.Liveness.DeepCopy()
		}
		if exec.Probes.Readiness != nil {
			config.ReadinessProbe = exec.Probes.Readiness.DeepCopy()
		}
		if exec.Probes.Startup != nil {
			config.StartupProbe = exec.Probes.Startup.DeepCopy()
		}
	}

	// Service Configuration
	if exec.Service != nil && len(exec.Service.Ports) > 0 {
		config.ServicePorts = make([]corev1.ServicePort, 0, len(exec.Service.Ports))
		for _, p := range exec.Service.Ports {
			config.ServicePorts = append(config.ServicePorts, corev1.ServicePort{
				Name:       p.Name,
				Protocol:   corev1.Protocol(p.Protocol),
				Port:       p.Port,
				TargetPort: intstr.FromInt(int(p.TargetPort)),
			})
		}
	}
}

// applyEngramConfig applies settings from the Engram.
func (cr *Resolver) applyEngramConfig(engram *v1alpha1.Engram, config *ResolvedExecutionConfig) {
	if engram == nil {
		return
	}

	// Apply secrets from the Engram definition. These serve as a base.
	if engram.Spec.Secrets != nil {
		if config.Secrets == nil {
			config.Secrets = make(map[string]string)
		}
		for k, v := range engram.Spec.Secrets {
			config.Secrets[k] = v
		}
	}

	// Workload Resources
	if engram.Spec.ExecutionPolicy != nil && engram.Spec.ExecutionPolicy.Resources != nil {
		res := engram.Spec.ExecutionPolicy.Resources
		if res.CPURequest != nil {
			if res.CPURequest != nil {
				config.Resources.Requests[corev1.ResourceCPU] = resource.MustParse(*res.CPURequest)
			}
			if res.MemoryRequest != nil {
				config.Resources.Requests[corev1.ResourceMemory] = resource.MustParse(*res.MemoryRequest)
			}
		}
		if res.CPULimit != nil {
			if res.CPULimit != nil {
				config.Resources.Limits[corev1.ResourceCPU] = resource.MustParse(*res.CPULimit)
			}
			if res.MemoryLimit != nil {
				config.Resources.Limits[corev1.ResourceMemory] = resource.MustParse(*res.MemoryLimit)
			}
		}
	}

	// Execution Overrides
	cr.ApplyExecutionOverrides(engram.Spec.Overrides, config)
}

// ApplyExecutionOverrides applies ExecutionOverrides from either Engram or Impulse
// This is a public method so controllers can apply instance-level overrides
func (cr *Resolver) ApplyExecutionOverrides(overrides *v1alpha1.ExecutionOverrides, config *ResolvedExecutionConfig) {
	if overrides == nil {
		return
	}

	if overrides.ServiceAccountName != nil {
		config.ServiceAccountName = *overrides.ServiceAccountName
	}
	if overrides.AutomountServiceAccountToken != nil {
		config.AutomountServiceAccountToken = *overrides.AutomountServiceAccountToken
	}
	if overrides.Security != nil {
		sec := overrides.Security
		if sec.RunAsNonRoot != nil {
			config.RunAsNonRoot = *sec.RunAsNonRoot
		}
		if sec.ReadOnlyRootFilesystem != nil {
			config.ReadOnlyRootFilesystem = *sec.ReadOnlyRootFilesystem
		}
		if sec.AllowPrivilegeEscalation != nil {
			config.AllowPrivilegeEscalation = *sec.AllowPrivilegeEscalation
		}
		if sec.RunAsUser != nil {
			config.RunAsUser = *sec.RunAsUser
		}
	}
	if overrides.ImagePullPolicy != nil {
		switch *overrides.ImagePullPolicy {
		case "Always":
			config.ImagePullPolicy = corev1.PullAlways
		case "Never":
			config.ImagePullPolicy = corev1.PullNever
		case "IfNotPresent":
			config.ImagePullPolicy = corev1.PullIfNotPresent
		}
	}
	if overrides.Timeout != nil {
		if d, err := time.ParseDuration(*overrides.Timeout); err == nil {
			config.DefaultStepTimeout = d
		}
	}
	if overrides.Retry != nil && overrides.Retry.MaxRetries != nil {
		config.MaxRetries = int(*overrides.Retry.MaxRetries)
	}
	// Handle probe disabling at instance level
	if overrides.Probes != nil {
		if overrides.Probes.DisableLiveness {
			config.LivenessProbe = nil
		}
		if overrides.Probes.DisableReadiness {
			config.ReadinessProbe = nil
		}
		if overrides.Probes.DisableStartup {
			config.StartupProbe = nil
		}
	}
}

// applyStoryConfig applies settings from the Story
func (cr *Resolver) applyStoryConfig(story *v1alpha1.Story, stepRun *runsv1alpha1.StepRun, config *ResolvedExecutionConfig) {
	if story == nil || stepRun == nil {
		return
	}

	// Find the specific step in the story spec that this StepRun is executing
	var currentStep *v1alpha1.Step
	for i := range story.Spec.Steps {
		if story.Spec.Steps[i].Name == stepRun.Spec.StepID {
			currentStep = &story.Spec.Steps[i]
			break
		}
	}

	// Apply secrets from the step definition, overriding any from the Engram
	if currentStep != nil && currentStep.Secrets != nil {
		if config.Secrets == nil {
			config.Secrets = make(map[string]string)
		}
		for k, v := range currentStep.Secrets {
			config.Secrets[k] = v // Step secrets have higher priority
		}
	}

	// Wire storage policy from Story into resolved config so controllers can provision PVCs
	if story.Spec.Policy != nil && story.Spec.Policy.Storage != nil {
		config.Storage = story.Spec.Policy.Storage
	}
}

// applyStepRunOverrides applies settings from the StepRun
func (cr *Resolver) applyStepRunOverrides(stepRun *runsv1alpha1.StepRun, config *ResolvedExecutionConfig) {
	if stepRun == nil {
		return
	}
	// The StepRun ExecutionOverrides doesn't exist in the current API.
	// This function body is a placeholder for what it would look like if it did.
	// For now, we will just apply the timeout and retry from the StepRun spec itself.

	if stepRun.Spec.ExecutionOverrides != nil {
		overrides := stepRun.Spec.ExecutionOverrides
		if overrides.CPURequest != nil {
			config.Resources.Requests[corev1.ResourceCPU] = resource.MustParse(*overrides.CPURequest)
		}
		if overrides.CPULimit != nil {
			config.Resources.Limits[corev1.ResourceCPU] = resource.MustParse(*overrides.CPULimit)
		}
		if overrides.MemoryRequest != nil {
			config.Resources.Requests[corev1.ResourceMemory] = resource.MustParse(*overrides.MemoryRequest)
		}
		if overrides.MemoryLimit != nil {
			config.Resources.Limits[corev1.ResourceMemory] = resource.MustParse(*overrides.MemoryLimit)
		}
	}

	if stepRun.Spec.Timeout != "" {
		if d, err := time.ParseDuration(stepRun.Spec.Timeout); err == nil {
			config.DefaultStepTimeout = d
		}
	}

	if stepRun.Spec.Retry != nil && stepRun.Spec.Retry.MaxRetries != nil {
		config.MaxRetries = int(*stepRun.Spec.Retry.MaxRetries)
	}
}

// ToResourceRequirements converts to Kubernetes ResourceRequirements
func (config *ResolvedExecutionConfig) ToResourceRequirements() corev1.ResourceRequirements {
	return config.Resources
}

// ToPodSecurityContext converts to Kubernetes PodSecurityContext
func (config *ResolvedExecutionConfig) ToPodSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot: &config.RunAsNonRoot,
		RunAsUser:    &config.RunAsUser,
	}
}

// ToContainerSecurityContext converts to Kubernetes SecurityContext for a container
func (config *ResolvedExecutionConfig) ToContainerSecurityContext() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		ReadOnlyRootFilesystem:   &config.ReadOnlyRootFilesystem,
		AllowPrivilegeEscalation: &config.AllowPrivilegeEscalation,
		Capabilities: &corev1.Capabilities{
			Drop: []corev1.Capability{"ALL"},
		},
	}
}
