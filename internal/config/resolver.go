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
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/resolver/chain"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
)

// copyStringMap returns a defensive copy of the provided map.
// Returns nil when the source map is nil or empty.
func copyStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

const DefaultServiceAccountName = "default"

var resolverLogger = logf.Log.WithName("config-resolver")

const (
	resolverLayerTemplate = "template"
	resolverLayerEngram   = "engram"
	resolverLayerStory    = "story"
	resolverLayerStepRun  = "steprun"

	overrideLayerDefault     = "execution_overrides"
	overrideLayerEngram      = "engram_overrides"
	overrideLayerStoryPolicy = "story_policy_overrides"
	overrideLayerStoryStep   = "story_step_overrides"
	overrideLayerImpulse     = "impulse_overrides"
)

type executionOverrideLayerKey struct{}

// WithExecutionOverrideLayer annotates ctx so ApplyExecutionOverrides can emit telemetry labeled by caller.
func WithExecutionOverrideLayer(ctx context.Context, layer string) context.Context {
	if layer == "" {
		layer = overrideLayerDefault
	}
	return context.WithValue(ctx, executionOverrideLayerKey{}, layer)
}

func executionOverrideLayer(ctx context.Context) string {
	if v, ok := ctx.Value(executionOverrideLayerKey{}).(string); ok && v != "" {
		return v
	}
	return overrideLayerDefault
}

// safeParseQuantity attempts to parse a resource quantity string and returns
// the parsed value plus a boolean indicating success. On failure, logs a warning
// and returns a zero quantity.
//
// Behavior:
//   - Attempts resource.ParseQuantity on the input string.
//   - On success, returns the parsed quantity and true.
//   - On failure, logs a warning with the field name and invalid value, returns zero and false.
//
// Arguments:
//   - val string: the resource quantity string (e.g., "100m", "256Mi").
//   - field string: human-readable field name for logging (e.g., "CPURequest").
//
// Returns:
//   - resource.Quantity: the parsed quantity, or zero on error.
//   - bool: true if parsing succeeded, false otherwise.
func safeParseQuantity(val, field string) (resource.Quantity, bool) {
	q, err := resource.ParseQuantity(val)
	if err != nil {
		resolverLogger.Info("Ignoring invalid resource quantity", "field", field, "value", val, "error", err.Error())
		return resource.Quantity{}, false
	}
	return q, true
}

// Resolver resolves configuration with hierarchical priority
// Priority (highest to lowest): StepRun > Story.Policy > Namespace > Controller Config
type Resolver struct {
	k8sClient     client.Client
	configManager *OperatorConfigManager
}

// NewResolver creates a new configuration resolver for hierarchical config merging.
//
// Behavior:
//   - Creates a new Resolver with the provided Kubernetes client and config manager.
//   - The resolver delegates to configManager for operator-level defaults.
//
// Arguments:
//   - k8sClient client.Client: the Kubernetes client (currently unused, reserved for future use).
//   - configManager *OperatorConfigManager: the manager providing operator configuration.
//
// Returns:
//   - *Resolver: a new resolver ready for ResolveExecutionConfig calls.
func NewResolver(k8sClient client.Client, configManager *OperatorConfigManager) *Resolver {
	return &Resolver{
		k8sClient:     k8sClient,
		configManager: configManager,
	}
}

// ConfigNamespace returns the namespace where the operator ConfigMap lives.
func (cr *Resolver) ConfigNamespace() string {
	if cr == nil || cr.configManager == nil {
		return ""
	}
	return cr.configManager.ConfigNamespace()
}

// ConfigName returns the operator ConfigMap name.
func (cr *Resolver) ConfigName() string {
	if cr == nil || cr.configManager == nil {
		return ""
	}
	return cr.configManager.ConfigName()
}

// GetOperatorConfig returns the current, raw operator configuration.
//
// Behavior:
//   - Delegates to cr.configManager.GetConfig() to retrieve the current snapshot.
//   - Returns the pointer to the shared OperatorConfig (callers should treat as read-only).
//
// Returns:
//   - *OperatorConfig: the current operator configuration snapshot.
//
// Notes:
//   - This is the primary way for controllers to access global, non-hierarchical
//     configuration values like default ports or image names.
//   - The returned pointer should be treated as read-only; mutations are not thread-safe.
func (cr *Resolver) GetOperatorConfig() *OperatorConfig {
	return cr.configManager.GetConfig()
}

// ResolvedExecutionConfig represents the final resolved configuration for a StepRun
type ResolvedExecutionConfig struct {
	// Image is the container image selected after merging template and instance overrides.
	Image string
	// ImagePullPolicy is the policy controllers should use when creating pods.
	ImagePullPolicy corev1.PullPolicy
	// Resources represents the pod-level resource requirements.
	Resources corev1.ResourceRequirements
	// MaxInlineSize controls when payloads are offloaded to shared storage.
	MaxInlineSize int
	// Storage captures any resolved storage offload configuration (e.g. S3).
	Storage *v1alpha1.StoragePolicy
	// ServiceAccountName is the ServiceAccount pods should run as.
	ServiceAccountName string
	// AutomountServiceAccountToken indicates whether automountServiceAccountToken should be set.
	AutomountServiceAccountToken bool
	// RunAsNonRoot enforces non-root execution when true.
	RunAsNonRoot bool
	// ReadOnlyRootFilesystem enforces a read-only root filesystem when true.
	ReadOnlyRootFilesystem bool
	// AllowPrivilegeEscalation toggles privilege escalation in the container security context.
	AllowPrivilegeEscalation bool
	// DropCapabilities are dropped from the container security context.
	DropCapabilities []string
	// RunAsUser specifies the UID the pod should run as.
	RunAsUser int64
	// BackoffLimit is the Job backoff limit.
	BackoffLimit int32
	// TTLSecondsAfterFinished controls Job TTL cleanup.
	TTLSecondsAfterFinished int32
	// RestartPolicy is the pod restart policy associated with the Job.
	RestartPolicy corev1.RestartPolicy
	// DefaultStepTimeout is the reconciler-imposed execution timeout.
	DefaultStepTimeout time.Duration
	// MaxRetries controls how many retry attempts are permitted.
	MaxRetries int
	// Secrets contains resolved secret key/value mappings for the step.
	Secrets map[string]string
	// DebugLogs toggles verbose logging inside engrams/impulses.
	DebugLogs bool

	// Health check probes (from template, can be disabled at instance level)
	LivenessProbe  *corev1.Probe
	ReadinessProbe *corev1.Probe
	StartupProbe   *corev1.Probe

	// Service configuration (from template)
	ServicePorts []corev1.ServicePort
	// ServiceLabels augments the generated Service metadata.
	ServiceLabels map[string]string
	// ServiceAnnotations augments the generated Service metadata.
	ServiceAnnotations map[string]string
}

// ResolveExecutionConfig resolves the final execution configuration for a step
// by merging settings from all levels of the hierarchy.
//
// Behavior:
//   - Retrieves operator configuration and initializes with getOperatorDefaults.
//   - Applies EngramTemplate settings via applyEngramTemplateConfig.
//   - Applies Engram settings via applyEngramConfig.
//   - Applies Story/step settings via applyStoryConfig.
//   - Applies StepRun overrides via applyStepRunOverrides.
//   - Finalizes ServiceAccountName if not set (uses StoryRun-derived name).
//
// Arguments:
//   - ctx context.Context: propagated to sub-resolvers (currently unused).
//   - step *runsv1alpha1.StepRun: the StepRun for highest-priority overrides.
//   - story *v1alpha1.Story: the Story for policy and step-level settings.
//   - engram *v1alpha1.Engram: the Engram for secrets and execution policy.
//   - template *catalogv1alpha1.EngramTemplate: the template for recommended settings.
//   - storyStep *v1alpha1.Step: optional pre-resolved step (avoids lookup if provided).
//
// Returns:
//   - *ResolvedExecutionConfig: the fully merged configuration.
//   - error: non-nil when a fail-fast stage in the apply chain rejects template data.
//
// Notes:
//   - Precedence: StepRun > Story Step > Engram > EngramTemplate > Operator > Defaults.
func (cr *Resolver) ResolveExecutionConfig(ctx context.Context, step *runsv1alpha1.StepRun, story *v1alpha1.Story, engram *v1alpha1.Engram, template *catalogv1alpha1.EngramTemplate, storyStep *v1alpha1.Step) (*ResolvedExecutionConfig, error) {
	operatorConfig := cr.configManager.GetConfig()

	// 1. Start with hardcoded defaults and operator-level configuration.
	resolved := cr.getOperatorDefaults(operatorConfig)

	// 2. Apply EngramTemplate settings.
	if err := cr.applyEngramTemplateConfig(ctx, template, resolved); err != nil {
		return nil, err
	}

	// 3. Apply Engram-specific settings, which override the template.
	if err := cr.applyEngramConfig(ctx, engram, resolved); err != nil {
		return nil, err
	}

	// 4. Apply Story-level policies and step-specific overrides.
	if err := cr.applyStoryConfig(ctx, story, step, storyStep, resolved); err != nil {
		return nil, err
	}

	// 5. Apply StepRun-specific overrides, which have the highest priority.
	if err := cr.applyStepRunOverrides(ctx, step, resolved); err != nil {
		return nil, err
	}

	// 6. Finalize the ServiceAccountName. If nothing else has specified one,
	// default to the one managed by the storyrun controller for the engram pods.
	if step != nil {
		runner := runsidentity.NewEngramRunner(step.Spec.StoryRunRef.Name)
		if resolved.ServiceAccountName == "" || resolved.ServiceAccountName == DefaultServiceAccountName {
			resolved.ServiceAccountName = runner.ServiceAccountName()
			metrics.RecordResolverServiceAccountFallback(step.Spec.StoryRunRef.Name)
		}
		if resolved.ServiceAccountName == runner.ServiceAccountName() {
			// Ensure in-cluster client works for the managed runner service account.
			resolved.AutomountServiceAccountToken = true
		}
	}

	return resolved, nil
}

// getOperatorDefaults initializes a ResolvedExecutionConfig with values from the
// operator's config map, providing the base layer of the configuration hierarchy.
//
// Behavior:
//   - Creates a new ResolvedExecutionConfig struct.
//   - Copies ImagePullPolicy, Resources, Security, Job, and Retry settings from config.Controller.
//   - If DefaultStorageProvider is "s3" and bucket is set, initializes Storage.S3 with defaults.
//
// Arguments:
//   - config *OperatorConfig: the operator configuration to read defaults from.
//
// Returns:
//   - *ResolvedExecutionConfig: a new struct initialized with operator-level defaults.
//
// Notes:
//   - This is the lowest priority layer; subsequent apply* calls override these values.
//   - DropCapabilities is deep-copied to avoid shared slice references.
func (cr *Resolver) getOperatorDefaults(config *OperatorConfig) *ResolvedExecutionConfig {
	// Parse resource quantities with safe fallback
	cpuReq, _ := safeParseQuantity(config.Controller.DefaultCPURequest, "DefaultCPURequest")
	memReq, _ := safeParseQuantity(config.Controller.DefaultMemoryRequest, "DefaultMemoryRequest")
	cpuLim, _ := safeParseQuantity(config.Controller.DefaultCPULimit, "DefaultCPULimit")
	memLim, _ := safeParseQuantity(config.Controller.DefaultMemoryLimit, "DefaultMemoryLimit")

	resolved := &ResolvedExecutionConfig{
		ImagePullPolicy: config.Controller.ImagePullPolicy,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    cpuReq,
				corev1.ResourceMemory: memReq,
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    cpuLim,
				corev1.ResourceMemory: memLim,
			},
		},
		AutomountServiceAccountToken: config.Controller.AutomountServiceAccountToken,
		RunAsNonRoot:                 config.Controller.RunAsNonRoot,
		ReadOnlyRootFilesystem:       config.Controller.ReadOnlyRootFilesystem,
		AllowPrivilegeEscalation:     config.Controller.AllowPrivilegeEscalation,
		DropCapabilities:             append([]string(nil), config.Controller.DropCapabilities...),
		RunAsUser:                    config.Controller.RunAsUser,
		BackoffLimit:                 config.Controller.JobBackoffLimit,
		TTLSecondsAfterFinished:      config.Controller.TTLSecondsAfterFinished,
		RestartPolicy:                config.Controller.JobRestartPolicy,
		ServiceAccountName:           config.Controller.ServiceAccountName,
		DefaultStepTimeout:           config.Controller.DefaultStepTimeout,
		MaxRetries:                   config.Controller.MaxRetries,
		MaxInlineSize:                config.Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize,
	}

	// Operator-level default storage
	switch config.Controller.DefaultStorageProvider {
	case "s3":
		if config.Controller.DefaultS3Bucket != "" {
			resolved.Storage = &v1alpha1.StoragePolicy{
				S3: &v1alpha1.S3StorageProvider{
					Bucket:         config.Controller.DefaultS3Bucket,
					Region:         config.Controller.DefaultS3Region,
					Endpoint:       config.Controller.DefaultS3Endpoint,
					UsePathStyle:   config.Controller.DefaultS3UsePathStyle,
					Authentication: v1alpha1.S3Authentication{},
				},
			}
			if name := config.Controller.DefaultS3AuthSecretName; name != "" {
				resolved.Storage.S3.Authentication.SecretRef = &corev1.LocalObjectReference{Name: name}
			}
		}
	}

	// Engram-specific resource defaults override the global defaults when provided.
	if val := strings.TrimSpace(config.Controller.EngramCPURequest); val != "" {
		if q, ok := safeParseQuantity(val, "Controller.EngramCPURequest"); ok {
			resolved.Resources.Requests[corev1.ResourceCPU] = q
		}
	}
	if val := strings.TrimSpace(config.Controller.EngramCPULimit); val != "" {
		if q, ok := safeParseQuantity(val, "Controller.EngramCPULimit"); ok {
			resolved.Resources.Limits[corev1.ResourceCPU] = q
		}
	}
	if val := strings.TrimSpace(config.Controller.EngramMemoryRequest); val != "" {
		if q, ok := safeParseQuantity(val, "Controller.EngramMemoryRequest"); ok {
			resolved.Resources.Requests[corev1.ResourceMemory] = q
		}
	}
	if val := strings.TrimSpace(config.Controller.EngramMemoryLimit); val != "" {
		if q, ok := safeParseQuantity(val, "Controller.EngramMemoryLimit"); ok {
			resolved.Resources.Limits[corev1.ResourceMemory] = q
		}
	}

	return resolved
}

// applyEngramTemplateConfig applies settings from the EngramTemplate to the
// resolved config using the shared resolver chain.
//
// Behavior:
//   - Returns early when template or template.Spec.Execution is nil.
//   - Always applies template image before the staged execution policies.
//   - Executes each execution-policy helper via chain.Run so failures can either
//     bubble up (fail-fast) or be logged and skipped (log-and-skip).
//
// Returns:
//   - error: stage failure when ModeFailFast rejects template data.
func (cr *Resolver) applyEngramTemplateConfig(ctx context.Context, template *catalogv1alpha1.EngramTemplate, config *ResolvedExecutionConfig) error {
	if template == nil {
		return nil
	}
	cr.applyTemplateImage(template, config)
	exec := template.Spec.Execution
	if exec == nil {
		return nil
	}

	executor := chain.NewExecutor(resolverLogger.WithName("template-chain").WithValues("template", template.Name)).
		WithObserver(chain.NewMetricsObserver(resolverLayerTemplate))
	executor.
		Add(chain.Stage{
			Name: "imagePullPolicy",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return cr.applyTemplateImagePullPolicy(exec, config)
			},
		}).
		Add(chain.Stage{
			Name: "resources",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return cr.applyTemplateResources(exec, config)
			},
		}).
		Add(chain.Stage{
			Name: "security",
			Mode: chain.ModeFailFast,
			Fn: func(context.Context) error {
				return cr.applyTemplateSecurity(exec, config)
			},
		}).
		Add(chain.Stage{
			Name: "job",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return cr.applyTemplateJob(exec, config)
			},
		}).
		Add(chain.Stage{
			Name: "timeoutRetry",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return cr.applyTemplateTimeoutRetry(exec, config)
			},
		}).
		Add(chain.Stage{
			Name: "probes",
			Mode: chain.ModeFailFast,
			Fn: func(context.Context) error {
				return cr.applyTemplateProbes(exec, config)
			},
		}).
		Add(chain.Stage{
			Name: "service",
			Mode: chain.ModeFailFast,
			Fn: func(context.Context) error {
				return cr.applyTemplateService(exec, config)
			},
		}).
		Add(chain.Stage{
			Name: "storage",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return cr.applyTemplateStorage(exec, config)
			},
		})

	return executor.Run(ctx)
}

// applyTemplateImage copies the EngramTemplate's image into the resolved config
// when present.
//
// Behavior:
//   - Checks if template.Spec.Image is non-empty.
//   - If set, copies the image string to config.Image.
//
// Arguments:
//   - template *catalogv1alpha1.EngramTemplate: the template to read image from.
//   - config *ResolvedExecutionConfig: mutated in place with the image.
//
// Side Effects:
//   - Mutates config.Image if template specifies an image.
func (cr *Resolver) applyTemplateImage(template *catalogv1alpha1.EngramTemplate, config *ResolvedExecutionConfig) {
	if template.Spec.Image != "" {
		config.Image = template.Spec.Image
	}
}

// applyTemplateImagePullPolicy overrides the resolved ImagePullPolicy when the
// EngramTemplate specifies an explicit pull policy.
//
// Behavior:
//   - Checks if exec.Images.PullPolicy is set.
//   - Maps the string value to corev1.PullPolicy (Always, Never, IfNotPresent).
//   - Logs a warning for unrecognized values and keeps the previous policy.
//
// Arguments:
//   - exec *catalogv1alpha1.TemplateExecutionPolicy: the template execution policy.
//   - config *ResolvedExecutionConfig: mutated in place with the pull policy.
//
// Side Effects:
//   - Mutates config.ImagePullPolicy if template specifies a valid policy.
//   - Logs warning for invalid pull policy values.
func (cr *Resolver) applyTemplateImagePullPolicy(exec *catalogv1alpha1.TemplateExecutionPolicy, config *ResolvedExecutionConfig) error {
	if exec.Images != nil && exec.Images.PullPolicy != nil {
		policy := *exec.Images.PullPolicy
		switch policy {
		case string(corev1.PullAlways):
			config.ImagePullPolicy = corev1.PullAlways
			resolverLogger.V(1).Info("applied template image pull policy", "policy", policy)
		case string(corev1.PullNever):
			config.ImagePullPolicy = corev1.PullNever
			resolverLogger.V(1).Info("applied template image pull policy", "policy", policy)
		case string(corev1.PullIfNotPresent):
			config.ImagePullPolicy = corev1.PullIfNotPresent
			resolverLogger.V(1).Info("applied template image pull policy", "policy", policy)
		default:
			return fmt.Errorf("invalid image pull policy %q (valid: Always, Never, IfNotPresent)", policy)
		}
	}
	return nil
}

// applyTemplateResources overrides the resolved CPU/memory requests and limits
// when the EngramTemplate specifies recommended resource values.
//
// Behavior:
//   - Returns early if exec.Resources is nil.
//   - Applies RecommendedCPURequest to config.Resources.Requests[CPU].
//   - Applies RecommendedCPULimit to config.Resources.Limits[CPU].
//   - Applies RecommendedMemoryRequest to config.Resources.Requests[Memory].
//   - Applies RecommendedMemoryLimit to config.Resources.Limits[Memory].
//   - Logs warnings and skips invalid quantity strings instead of panicking.
//
// Arguments:
//   - exec *catalogv1alpha1.TemplateExecutionPolicy: the template execution policy.
//   - config *ResolvedExecutionConfig: mutated in place with resource values.
//
// Side Effects:
//   - Mutates config.Resources.Requests and config.Resources.Limits.
//   - Logs warnings for invalid resource quantity strings.
func (cr *Resolver) applyTemplateResources(exec *catalogv1alpha1.TemplateExecutionPolicy, config *ResolvedExecutionConfig) error {
	if exec.Resources == nil {
		return nil
	}
	var errs []error

	if exec.Resources.RecommendedCPURequest != nil {
		if q, err := resource.ParseQuantity(*exec.Resources.RecommendedCPURequest); err != nil {
			errs = append(errs, fmt.Errorf("cpu request: %w", err))
		} else {
			config.Resources.Requests[corev1.ResourceCPU] = q
			resolverLogger.V(1).Info("applied template CPU request", "value", q.String())
		}
	}
	if exec.Resources.RecommendedCPULimit != nil {
		if q, err := resource.ParseQuantity(*exec.Resources.RecommendedCPULimit); err != nil {
			errs = append(errs, fmt.Errorf("cpu limit: %w", err))
		} else {
			config.Resources.Limits[corev1.ResourceCPU] = q
			resolverLogger.V(1).Info("applied template CPU limit", "value", q.String())
		}
	}
	if exec.Resources.RecommendedMemoryRequest != nil {
		if q, err := resource.ParseQuantity(*exec.Resources.RecommendedMemoryRequest); err != nil {
			errs = append(errs, fmt.Errorf("memory request: %w", err))
		} else {
			config.Resources.Requests[corev1.ResourceMemory] = q
			resolverLogger.V(1).Info("applied template memory request", "value", q.String())
		}
	}
	if exec.Resources.RecommendedMemoryLimit != nil {
		if q, err := resource.ParseQuantity(*exec.Resources.RecommendedMemoryLimit); err != nil {
			errs = append(errs, fmt.Errorf("memory limit: %w", err))
		} else {
			config.Resources.Limits[corev1.ResourceMemory] = q
			resolverLogger.V(1).Info("applied template memory limit", "value", q.String())
		}
	}
	return errors.Join(errs...)
}

// applyTemplateSecurity overrides the resolved security context settings when
// the EngramTemplate specifies security requirements.
//
// Behavior:
//   - Returns early if exec.Security is nil.
//   - Applies RequiresNonRoot to config.RunAsNonRoot.
//   - Applies RequiresReadOnlyRoot to config.ReadOnlyRootFilesystem.
//   - Applies RequiresNoPrivilegeEscalation to config.AllowPrivilegeEscalation (inverted).
//   - Applies RecommendedRunAsUser to config.RunAsUser.
//
// Arguments:
//   - exec *catalogv1alpha1.TemplateExecutionPolicy: the template execution policy.
//   - config *ResolvedExecutionConfig: mutated in place with security values.
//
// Side Effects:
//   - Mutates config security-related fields.
func (cr *Resolver) applyTemplateSecurity(exec *catalogv1alpha1.TemplateExecutionPolicy, config *ResolvedExecutionConfig) error {
	if exec.Security == nil {
		return nil
	}
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
	resolverLogger.V(1).Info("Applied template security overrides",
		"runAsNonRoot", config.RunAsNonRoot,
		"readOnlyRootFilesystem", config.ReadOnlyRootFilesystem,
		"allowPrivilegeEscalation", config.AllowPrivilegeEscalation,
		"runAsUser", config.RunAsUser)
	return nil
}

// applyTemplateJob overrides the resolved Job settings when the EngramTemplate
// specifies recommended values.
//
// Behavior:
//   - Returns early if exec.Job is nil.
//   - Applies RecommendedBackoffLimit to config.BackoffLimit.
//   - Applies RecommendedTTLSecondsAfterFinished to config.TTLSecondsAfterFinished.
//   - Applies RecommendedRestartPolicy to config.RestartPolicy (Never or OnFailure).
//
// Arguments:
//   - exec *catalogv1alpha1.TemplateExecutionPolicy: the template execution policy.
//   - config *ResolvedExecutionConfig: mutated in place with Job settings.
//
// Side Effects:
//   - Mutates config.BackoffLimit, TTLSecondsAfterFinished, RestartPolicy.
func (cr *Resolver) applyTemplateJob(exec *catalogv1alpha1.TemplateExecutionPolicy, config *ResolvedExecutionConfig) error {
	if exec.Job == nil {
		return nil
	}
	var errs []error
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
		default:
			errs = append(errs, fmt.Errorf("invalid restart policy %q (valid: Never, OnFailure)", *exec.Job.RecommendedRestartPolicy))
		}
	}
	resolverLogger.V(1).Info("Applied template job overrides",
		"backoffLimit", config.BackoffLimit,
		"ttlSecondsAfterFinished", config.TTLSecondsAfterFinished,
		"restartPolicy", config.RestartPolicy)
	return errors.Join(errs...)
}

// applyTemplateTimeoutRetry overrides the resolved timeout and retry settings
// when the EngramTemplate specifies recommended values.
//
// Behavior:
//   - Parses exec.Timeout as a duration and stores in config.DefaultStepTimeout.
//   - Applies exec.Retry.RecommendedMaxRetries to config.MaxRetries.
//   - Logs warnings for invalid duration strings.
//
// Arguments:
//   - exec *catalogv1alpha1.TemplateExecutionPolicy: the template execution policy.
//   - config *ResolvedExecutionConfig: mutated in place with timeout/retry values.
//
// Side Effects:
//   - Mutates config.DefaultStepTimeout and config.MaxRetries.
//   - Logs warnings for invalid timeout durations.
func (cr *Resolver) applyTemplateTimeoutRetry(exec *catalogv1alpha1.TemplateExecutionPolicy, config *ResolvedExecutionConfig) error {
	var errs []error
	if exec.Timeout != nil {
		if d, err := time.ParseDuration(*exec.Timeout); err != nil {
			errs = append(errs, fmt.Errorf("timeout: %w", err))
		} else {
			config.DefaultStepTimeout = d
			resolverLogger.V(1).Info("applied template timeout", "value", d.String())
		}
	}
	if exec.Retry != nil && exec.Retry.RecommendedMaxRetries != nil {
		config.MaxRetries = *exec.Retry.RecommendedMaxRetries
		resolverLogger.V(1).Info("applied template max retries", "value", *exec.Retry.RecommendedMaxRetries)
	}
	return errors.Join(errs...)
}

// applyTemplateProbes copies the EngramTemplate's health check probes into the
// resolved config using deep copies.
//
// Behavior:
//   - Returns early if exec.Probes is nil.
//   - Deep copies exec.Probes.Liveness to config.LivenessProbe.
//   - Deep copies exec.Probes.Readiness to config.ReadinessProbe.
//   - Deep copies exec.Probes.Startup to config.StartupProbe.
//
// Arguments:
//   - exec *catalogv1alpha1.TemplateExecutionPolicy: the template execution policy.
//   - config *ResolvedExecutionConfig: mutated in place with probe definitions.
//
// Side Effects:
//   - Mutates config.LivenessProbe, ReadinessProbe, StartupProbe.
//
// Notes:
//   - Uses DeepCopy to avoid shared references between template and resolved config.
func (cr *Resolver) applyTemplateProbes(exec *catalogv1alpha1.TemplateExecutionPolicy, config *ResolvedExecutionConfig) error {
	if exec.Probes == nil {
		return nil
	}
	if exec.Probes.Liveness != nil {
		config.LivenessProbe = exec.Probes.Liveness.DeepCopy()
	}
	if exec.Probes.Readiness != nil {
		config.ReadinessProbe = exec.Probes.Readiness.DeepCopy()
	}
	if exec.Probes.Startup != nil {
		config.StartupProbe = exec.Probes.Startup.DeepCopy()
	}
	resolverLogger.V(1).Info("Applied template probes",
		"hasLiveness", config.LivenessProbe != nil,
		"hasReadiness", config.ReadinessProbe != nil,
		"hasStartup", config.StartupProbe != nil)
	return nil
}

// applyTemplateService converts the EngramTemplate's service port definitions
// into Kubernetes ServicePort objects.
//
// Behavior:
//   - Returns early if exec.Service is nil or has no ports.
//   - Allocates a new ServicePorts slice with capacity for all ports.
//   - Converts each template port to corev1.ServicePort with Name, Protocol, Port, TargetPort.
//
// Arguments:
//   - exec *catalogv1alpha1.TemplateExecutionPolicy: the template execution policy.
//   - config *ResolvedExecutionConfig: mutated in place with ServicePorts.
//
// Side Effects:
//   - Replaces config.ServicePorts with a new slice.
//
// Notes:
//   - TargetPort is converted from int32 to intstr.IntOrString using FromInt.
func (cr *Resolver) applyTemplateService(exec *catalogv1alpha1.TemplateExecutionPolicy, config *ResolvedExecutionConfig) error {
	if exec.Service == nil || len(exec.Service.Ports) == 0 {
		return nil
	}
	config.ServicePorts = make([]corev1.ServicePort, 0, len(exec.Service.Ports))
	for _, p := range exec.Service.Ports {
		config.ServicePorts = append(config.ServicePorts, corev1.ServicePort{
			Name:       p.Name,
			Protocol:   corev1.Protocol(p.Protocol),
			Port:       p.Port,
			TargetPort: intstr.FromInt(int(p.TargetPort)),
		})
	}
	resolverLogger.V(1).Info("Applied template service ports", "portCount", len(config.ServicePorts))
	return nil
}

// applyTemplateStorage converts the EngramTemplate's storage policy into a
// v1alpha1.StoragePolicy and stores it in the resolved config.
//
// Behavior:
//   - Returns early if exec.Storage is nil.
//   - Creates a new StoragePolicy with TimeoutSeconds from template.
//   - If exec.Storage.S3 is set, copies bucket, region, endpoint, and auth settings.
//   - If exec.Storage.File is set, copies path, VolumeClaimName, and EmptyDir.
//
// Arguments:
//   - exec *catalogv1alpha1.TemplateExecutionPolicy: the template execution policy.
//   - config *ResolvedExecutionConfig: mutated in place with Storage policy.
//
// Side Effects:
//   - Replaces config.Storage with a new StoragePolicy.
//
// Notes:
//   - Uses DeepCopy for EmptyDir to avoid shared references.
func (cr *Resolver) applyTemplateStorage(exec *catalogv1alpha1.TemplateExecutionPolicy, config *ResolvedExecutionConfig) error {
	if exec.Storage == nil {
		return nil
	}
	if exec.Storage.S3 == nil && exec.Storage.File != nil && config != nil && config.Storage != nil && config.Storage.S3 != nil {
		if exec.Storage.TimeoutSeconds > 0 {
			config.Storage.TimeoutSeconds = exec.Storage.TimeoutSeconds
		}
		resolverLogger.V(1).Info("Skipping template file storage because default S3 storage is configured",
			"timeoutSeconds", exec.Storage.TimeoutSeconds)
		return nil
	}
	policy := &v1alpha1.StoragePolicy{TimeoutSeconds: exec.Storage.TimeoutSeconds}
	var errs []error

	if exec.Storage.S3 != nil {
		tmpl := exec.Storage.S3
		bucket := strings.TrimSpace(tmpl.Bucket)
		switch {
		case bucket == "":
			errs = append(errs, errors.New("s3 bucket is empty"))
		case len(validation.IsDNS1123Subdomain(bucket)) > 0:
			errs = append(errs, fmt.Errorf("s3 bucket %q failed validation: %v", bucket, validation.IsDNS1123Subdomain(bucket)))
		default:
			authAnnotations := copyStringMap(tmpl.Authentication.ServiceAccountAnnotations)
			if authAnnotations == nil {
				authAnnotations = map[string]string{}
			}
			policy.S3 = &v1alpha1.S3StorageProvider{
				Bucket:       bucket,
				Region:       strings.TrimSpace(tmpl.Region),
				Endpoint:     strings.TrimSpace(tmpl.Endpoint),
				UsePathStyle: tmpl.UsePathStyle,
				Authentication: v1alpha1.S3Authentication{
					ServiceAccountAnnotations: authAnnotations,
				},
			}
			if tmpl.Authentication.SecretRef != nil {
				secretName := strings.TrimSpace(tmpl.Authentication.SecretRef.Name)
				switch secretName {
				case "":
					errs = append(errs, errors.New("s3 secret name is empty"))
				default:
					if issues := validation.IsDNS1123Subdomain(secretName); len(issues) > 0 {
						errs = append(errs, fmt.Errorf("s3 secret name %q failed validation: %v", secretName, issues))
					} else {
						policy.S3.Authentication.SecretRef = &corev1.LocalObjectReference{Name: secretName}
					}
				}
			}
		}
	}
	if exec.Storage.File != nil {
		tmpl := exec.Storage.File
		policy.File = &v1alpha1.FileStorageProvider{
			Path:            tmpl.Path,
			VolumeClaimName: tmpl.VolumeClaimName,
		}
		if tmpl.EmptyDir != nil {
			policy.File.EmptyDir = tmpl.EmptyDir.DeepCopy()
		}
	}
	config.Storage = policy
	resolverLogger.V(1).Info("Applied template storage",
		"hasS3", policy.S3 != nil,
		"hasFile", policy.File != nil,
		"timeoutSeconds", policy.TimeoutSeconds)
	return errors.Join(errs...)
}

// cloneStoragePolicy returns a deep copy of the given StoragePolicy.
//
// Behavior:
//   - Returns nil if input is nil.
//   - Uses DeepCopy to create an independent copy of the policy.
//
// Arguments:
//   - in *v1alpha1.StoragePolicy: the storage policy to clone.
//
// Returns:
//   - *v1alpha1.StoragePolicy: a deep copy of the input, or nil.
//
// Notes:
//   - Used to avoid shared references between config layers when merging policies.
func cloneStoragePolicy(in *v1alpha1.StoragePolicy) *v1alpha1.StoragePolicy {
	if in == nil {
		return nil
	}
	return in.DeepCopy()
}

// applyEngramConfig applies settings from the Engram to the resolved config,
// providing the third layer of the configuration hierarchy.
//
// Behavior:
//   - Returns early if engram is nil.
//   - Copies Engram.Spec.Secrets into config.Secrets (base layer for secrets).
//   - Applies ExecutionPolicy.Resources (CPURequest, MemoryRequest, CPULimit, MemoryLimit).
//   - Applies Engram.Spec.Overrides via ApplyExecutionOverrides.
//
// Arguments:
//   - engram *v1alpha1.Engram: the Engram to read settings from.
//   - config *ResolvedExecutionConfig: mutated in place with Engram values.
//
// Side Effects:
//   - Mutates config.Secrets, config.Resources, and fields via ApplyExecutionOverrides.
//
// Notes:
//   - Engram settings override template defaults but are overridden by Story/StepRun.
func (cr *Resolver) applyEngramConfig(ctx context.Context, engram *v1alpha1.Engram, config *ResolvedExecutionConfig) error {
	if engram == nil {
		return nil
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

	executor := chain.NewExecutor(resolverLogger.WithName("engram-chain").WithValues("engram", engram.Name)).
		WithObserver(chain.NewMetricsObserver(resolverLayerEngram))
	executor.
		Add(chain.Stage{
			Name: "resources",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return applyEngramResourceOverrides(engram, config)
			},
		}).
		Add(chain.Stage{
			Name: "executionOverrides",
			Mode: chain.ModeLogAndSkip,
			Fn: func(ctx context.Context) error {
				overrideCtx := WithExecutionOverrideLayer(ctx, overrideLayerEngram)
				return cr.ApplyExecutionOverrides(overrideCtx, engram.Spec.Overrides, config)
			},
		})

	return executor.Run(ctx)
}

func applyEngramResourceOverrides(engram *v1alpha1.Engram, config *ResolvedExecutionConfig) error {
	if engram.Spec.ExecutionPolicy == nil || engram.Spec.ExecutionPolicy.Resources == nil {
		return nil
	}
	res := engram.Spec.ExecutionPolicy.Resources
	var errs []error

	if res.CPURequest != nil {
		if q, err := resource.ParseQuantity(*res.CPURequest); err != nil {
			errs = append(errs, fmt.Errorf("cpu request %q invalid: %w", *res.CPURequest, err))
		} else {
			config.Resources.Requests[corev1.ResourceCPU] = q
		}
	}
	if res.MemoryRequest != nil {
		if q, err := resource.ParseQuantity(*res.MemoryRequest); err != nil {
			errs = append(errs, fmt.Errorf("memory request %q invalid: %w", *res.MemoryRequest, err))
		} else {
			config.Resources.Requests[corev1.ResourceMemory] = q
		}
	}
	if res.CPULimit != nil {
		if q, err := resource.ParseQuantity(*res.CPULimit); err != nil {
			errs = append(errs, fmt.Errorf("cpu limit %q invalid: %w", *res.CPULimit, err))
		} else {
			config.Resources.Limits[corev1.ResourceCPU] = q
		}
	}
	if res.MemoryLimit != nil {
		if q, err := resource.ParseQuantity(*res.MemoryLimit); err != nil {
			errs = append(errs, fmt.Errorf("memory limit %q invalid: %w", *res.MemoryLimit, err))
		} else {
			config.Resources.Limits[corev1.ResourceMemory] = q
		}
	}
	resolverLogger.V(1).Info("Applied Engram resource overrides",
		"cpuRequest", res.CPURequest, "memoryRequest", res.MemoryRequest,
		"cpuLimit", res.CPULimit, "memoryLimit", res.MemoryLimit)
	return errors.Join(errs...)
}

// ApplyExecutionOverrides applies ExecutionOverrides from either Engram or Impulse
// This is a public method so controllers can apply instance-level overrides.
func (cr *Resolver) ApplyExecutionOverrides(ctx context.Context, overrides *v1alpha1.ExecutionOverrides, config *ResolvedExecutionConfig) error {
	if overrides == nil {
		return nil
	}

	layer := executionOverrideLayer(ctx)
	executor := chain.NewExecutor(resolverLogger.WithName("overrides-chain").WithValues("layer", layer)).
		WithObserver(chain.NewMetricsObserver(layer))
	executor.
		Add(chain.Stage{
			Name: "basic",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return applyBasicOverrides(overrides, config)
			},
		}).
		Add(chain.Stage{
			Name: "security",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return applySecurityOverrides(overrides, config)
			},
		}).
		Add(chain.Stage{
			Name: "imagePolicy",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return applyImagePolicyOverride(overrides, config)
			},
		}).
		Add(chain.Stage{
			Name: "timeoutRetry",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return applyTimeoutRetryOverrides(overrides, config)
			},
		}).
		Add(chain.Stage{
			Name: "probes",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return applyProbeOverrides(overrides, config)
			},
		}).
		Add(chain.Stage{
			Name: "storage",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return applyStorageOverride(overrides, config)
			},
		})

	return executor.Run(ctx)
}

// applyBasicOverrides applies basic execution overrides to the resolved config.
//
// Behavior:
//   - Applies overrides.ServiceAccountName to config.ServiceAccountName.
//   - Applies overrides.AutomountServiceAccountToken to config.AutomountServiceAccountToken.
//   - Applies overrides.Debug to config.DebugLogs.
//
// Arguments:
//   - overrides *v1alpha1.ExecutionOverrides: the execution overrides to apply.
//   - config *ResolvedExecutionConfig: mutated in place with override values.
//
// Side Effects:
//   - Mutates config.ServiceAccountName, AutomountServiceAccountToken, DebugLogs.
func applyBasicOverrides(overrides *v1alpha1.ExecutionOverrides, config *ResolvedExecutionConfig) error {
	var applied []string
	if overrides.ServiceAccountName != nil {
		config.ServiceAccountName = *overrides.ServiceAccountName
		applied = append(applied, "serviceAccountName")
	}
	if overrides.AutomountServiceAccountToken != nil {
		config.AutomountServiceAccountToken = *overrides.AutomountServiceAccountToken
		applied = append(applied, "automountServiceAccountToken")
	}
	if overrides.Debug != nil {
		config.DebugLogs = *overrides.Debug
		applied = append(applied, "debug")
	}
	if overrides.MaxInlineSize != nil {
		config.MaxInlineSize = *overrides.MaxInlineSize
		applied = append(applied, "maxInlineSize")
	}
	if len(applied) > 0 {
		resolverLogger.V(1).Info("Applied basic overrides", "fields", applied)
	}
	return nil
}

// applySecurityOverrides applies security context overrides to the resolved config.
//
// Behavior:
//   - Returns early if overrides.Security is nil.
//   - Applies Security.RunAsNonRoot to config.RunAsNonRoot.
//   - Applies Security.ReadOnlyRootFilesystem to config.ReadOnlyRootFilesystem.
//   - Applies Security.AllowPrivilegeEscalation to config.AllowPrivilegeEscalation.
//   - Applies Security.RunAsUser to config.RunAsUser.
//
// Arguments:
//   - overrides *v1alpha1.ExecutionOverrides: the execution overrides to apply.
//   - config *ResolvedExecutionConfig: mutated in place with security values.
//
// Side Effects:
//   - Mutates config security-related fields.
func applySecurityOverrides(overrides *v1alpha1.ExecutionOverrides, config *ResolvedExecutionConfig) error {
	if overrides.Security == nil {
		return nil
	}
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
	resolverLogger.V(1).Info("Applied security overrides",
		"runAsNonRoot", config.RunAsNonRoot,
		"readOnlyRootFilesystem", config.ReadOnlyRootFilesystem,
		"allowPrivilegeEscalation", config.AllowPrivilegeEscalation,
		"runAsUser", config.RunAsUser)
	return nil
}

// applyImagePolicyOverride overrides the resolved ImagePullPolicy when the
// ExecutionOverrides specifies an explicit policy.
//
// Behavior:
//   - Returns early if overrides.ImagePullPolicy is nil.
//   - Maps the string value to corev1.PullPolicy (Always, Never, IfNotPresent).
//   - Invalid values are silently ignored.
//
// Arguments:
//   - overrides *v1alpha1.ExecutionOverrides: the execution overrides to apply.
//   - config *ResolvedExecutionConfig: mutated in place with the pull policy.
//
// Side Effects:
//   - Mutates config.ImagePullPolicy if a valid policy is specified.
func applyImagePolicyOverride(overrides *v1alpha1.ExecutionOverrides, config *ResolvedExecutionConfig) error {
	if overrides.ImagePullPolicy == nil {
		return nil
	}
	switch *overrides.ImagePullPolicy {
	case string(corev1.PullAlways):
		config.ImagePullPolicy = corev1.PullAlways
		resolverLogger.V(1).Info("Applied image pull policy override", "policy", corev1.PullAlways)
	case string(corev1.PullNever):
		config.ImagePullPolicy = corev1.PullNever
		resolverLogger.V(1).Info("Applied image pull policy override", "policy", corev1.PullNever)
	case string(corev1.PullIfNotPresent):
		config.ImagePullPolicy = corev1.PullIfNotPresent
		resolverLogger.V(1).Info("Applied image pull policy override", "policy", corev1.PullIfNotPresent)
	default:
		return fmt.Errorf("invalid image pull policy %q (valid: Always, Never, IfNotPresent)", *overrides.ImagePullPolicy)
	}
	return nil
}

// applyTimeoutRetryOverrides overrides the resolved timeout and max retries
// when the ExecutionOverrides specifies explicit values.
//
// Behavior:
//   - Parses overrides.Timeout as a duration and stores in config.DefaultStepTimeout.
//   - Applies overrides.Retry.MaxRetries to config.MaxRetries as an int.
//
// Arguments:
//   - overrides *v1alpha1.ExecutionOverrides: the execution overrides to apply.
//   - config *ResolvedExecutionConfig: mutated in place with timeout/retry values.
//
// Side Effects:
//   - Mutates config.DefaultStepTimeout and config.MaxRetries.
//
// Notes:
//   - Invalid timeout durations are silently ignored.
func applyTimeoutRetryOverrides(overrides *v1alpha1.ExecutionOverrides, config *ResolvedExecutionConfig) error {
	var errs []error
	if overrides.Timeout != nil {
		if d, err := time.ParseDuration(*overrides.Timeout); err == nil {
			config.DefaultStepTimeout = d
			resolverLogger.V(1).Info("Applied timeout override", "value", d.String())
		} else {
			errs = append(errs, fmt.Errorf("timeout override %q invalid: %w", *overrides.Timeout, err))
		}
	}
	if overrides.Retry != nil && overrides.Retry.MaxRetries != nil {
		config.MaxRetries = int(*overrides.Retry.MaxRetries)
		resolverLogger.V(1).Info("Applied max retries override", "value", config.MaxRetries)
	}
	return errors.Join(errs...)
}

// applyProbeOverrides disables health check probes in the resolved config when
// the ExecutionOverrides specifies them as disabled.
//
// Behavior:
//   - Returns early if overrides.Probes is nil.
//   - Sets config.LivenessProbe to nil if DisableLiveness is true.
//   - Sets config.ReadinessProbe to nil if DisableReadiness is true.
//   - Sets config.StartupProbe to nil if DisableStartup is true.
//
// Arguments:
//   - overrides *v1alpha1.ExecutionOverrides: the execution overrides to apply.
//   - config *ResolvedExecutionConfig: mutated in place with probe values.
//
// Side Effects:
//   - Mutates config.LivenessProbe, ReadinessProbe, StartupProbe (sets to nil).
//
// Notes:
//   - This only disables probes; it cannot add or modify probe definitions.
func applyProbeOverrides(overrides *v1alpha1.ExecutionOverrides, config *ResolvedExecutionConfig) error {
	if overrides.Probes == nil {
		return nil
	}
	var disabled []string
	if overrides.Probes.DisableLiveness {
		config.LivenessProbe = nil
		disabled = append(disabled, "liveness")
	}
	if overrides.Probes.DisableReadiness {
		config.ReadinessProbe = nil
		disabled = append(disabled, "readiness")
	}
	if overrides.Probes.DisableStartup {
		config.StartupProbe = nil
		disabled = append(disabled, "startup")
	}
	if len(disabled) > 0 {
		resolverLogger.V(1).Info("Disabled probes via override", "probes", disabled)
	}
	return nil
}

// applyStorageOverride replaces the resolved storage policy with a deep copy
// of the ExecutionOverrides storage policy.
//
// Behavior:
//   - Returns early if overrides.Storage is nil.
//   - Deep copies overrides.Storage to config.Storage via cloneStoragePolicy.
//
// Arguments:
//   - overrides *v1alpha1.ExecutionOverrides: the execution overrides to apply.
//   - config *ResolvedExecutionConfig: mutated in place with storage policy.
//
// Side Effects:
//   - Replaces config.Storage with a cloned policy.
//
// Notes:
//   - Uses cloneStoragePolicy to avoid shared references.
func applyStorageOverride(overrides *v1alpha1.ExecutionOverrides, config *ResolvedExecutionConfig) error {
	if overrides.Storage == nil {
		return nil
	}
	config.Storage = cloneStoragePolicy(overrides.Storage)
	resolverLogger.V(1).Info("Applied storage override",
		"hasS3", config.Storage != nil && config.Storage.S3 != nil,
		"hasFile", config.Storage != nil && config.Storage.File != nil)
	return nil
}

// applyStoryConfig applies settings from the Story to the resolved config,
// providing the fourth layer of the configuration hierarchy.
//
// Behavior:
//   - Returns early if story is nil.
//   - Applies Story.Spec.Policy.Execution via applyExecutionPolicyDefaults.
//   - Finds the specific step in story.Spec.Steps matching stepRun.Spec.StepID.
//   - Copies step-level secrets into config.Secrets (higher priority than Engram).
//   - Applies step-level execution overrides via ApplyExecutionOverrides.
//   - Applies Story.Spec.Policy.Storage via cloneStoragePolicy.
//
// Arguments:
//   - story *v1alpha1.Story: the Story to read settings from.
//   - stepRun *runsv1alpha1.StepRun: used to find the matching step by StepID.
//   - storyStep *v1alpha1.Step: optional pre-resolved step (avoids lookup if provided).
//   - config *ResolvedExecutionConfig: mutated in place with Story values.
//
// Side Effects:
//   - Mutates config.Secrets, config.Storage, and fields via ApplyExecutionOverrides.
//
// Notes:
//   - Story settings override Engram defaults but are overridden by StepRun.
func (cr *Resolver) applyStoryConfig(ctx context.Context, story *v1alpha1.Story, stepRun *runsv1alpha1.StepRun, storyStep *v1alpha1.Step, config *ResolvedExecutionConfig) error {
	if story == nil {
		return nil
	}

	// Find the specific step in the story spec that this StepRun is executing
	currentStep := storyStep
	if currentStep == nil && stepRun != nil {
		for i := range story.Spec.Steps {
			if story.Spec.Steps[i].Name == stepRun.Spec.StepID {
				currentStep = &story.Spec.Steps[i]
				break
			}
		}
	}

	// Apply secrets from the step definition, overriding any from the Engram.
	if currentStep != nil && currentStep.Secrets != nil {
		if config.Secrets == nil {
			config.Secrets = make(map[string]string)
		}
		for k, v := range currentStep.Secrets {
			config.Secrets[k] = v // Step secrets have higher priority
		}
	}

	executor := chain.NewExecutor(resolverLogger.WithName("story-chain").WithValues("story", story.Name)).
		WithObserver(chain.NewMetricsObserver(resolverLayerStory))
	if story.Spec.Policy != nil && story.Spec.Policy.Execution != nil {
		executor.Add(chain.Stage{
			Name: "policyExecution",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return applyExecutionPolicyDefaults(story.Spec.Policy.Execution, config)
			},
		})
	}
	if currentStep != nil {
		executor.Add(chain.Stage{
			Name: "stepExecutionOverrides",
			Mode: chain.ModeLogAndSkip,
			Fn: func(ctx context.Context) error {
				overrideCtx := WithExecutionOverrideLayer(ctx, overrideLayerStoryStep)
				return cr.ApplyExecutionOverrides(overrideCtx, currentStep.Execution, config)
			},
		})
	}
	if story.Spec.Policy != nil && story.Spec.Policy.Storage != nil {
		executor.Add(chain.Stage{
			Name: "policyStorage",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				config.Storage = cloneStoragePolicy(story.Spec.Policy.Storage)
				return nil
			},
		})
	}

	return executor.Run(ctx)
}

// applyExecutionPolicyDefaults applies Story-level ExecutionPolicy defaults to
// the resolved config.
//
// Behavior:
//   - Returns early if policy or config is nil.
//   - Applies Resources (CPURequest, MemoryRequest, CPULimit, MemoryLimit).
//   - Applies Security (RunAsNonRoot, AllowPrivilegeEscalation, ReadOnlyRootFilesystem, RunAsUser).
//   - Applies Job (BackoffLimit, TTLSecondsAfterFinished, RestartPolicy).
//   - Applies Retry.MaxRetries to config.MaxRetries.
//   - Applies Timeout to config.DefaultStepTimeout.
//   - Applies ServiceAccountName to config.ServiceAccountName.
//   - Applies Storage via cloneStoragePolicy.
//
// Arguments:
//   - policy *v1alpha1.ExecutionPolicy: the Story-level execution policy.
//   - config *ResolvedExecutionConfig: mutated in place with policy values.
//
// Side Effects:
//   - Mutates config fields for resources, security, job, retry, timeout, etc.
//
// Notes:
//   - These defaults are overridden by step-level ExecutionOverrides.
func applyExecutionPolicyDefaults(policy *v1alpha1.ExecutionPolicy, config *ResolvedExecutionConfig) error {
	if policy == nil || config == nil {
		return nil
	}
	var errs []error
	errs = append(errs, applyPolicyResourceDefaults(policy.Resources, config)...)
	applyPolicySecurityDefaults(policy.Security, config)
	applyPolicyJobDefaults(policy.Job, config)
	applyPolicyRetryDefaults(policy.Retry, config)
	errs = append(errs, applyPolicyTimeoutDefaults(policy.Timeout, config)...)
	applyPolicyServiceAccount(policy.ServiceAccountName, config)
	applyPolicyStorageDefaults(policy.Storage, config)
	return errors.Join(errs...)
}

func applyPolicyResourceDefaults(res *v1alpha1.ResourcePolicy, config *ResolvedExecutionConfig) []error {
	if res == nil {
		return nil
	}
	var errs []error
	if res.CPURequest != nil {
		if q, ok := safeParseQuantity(*res.CPURequest, "Story.CPURequest"); ok {
			config.Resources.Requests[corev1.ResourceCPU] = q
		} else {
			errs = append(errs, fmt.Errorf("story CPU request %q invalid", *res.CPURequest))
		}
	}
	if res.MemoryRequest != nil {
		if q, ok := safeParseQuantity(*res.MemoryRequest, "Story.MemoryRequest"); ok {
			config.Resources.Requests[corev1.ResourceMemory] = q
		} else {
			errs = append(errs, fmt.Errorf("story memory request %q invalid", *res.MemoryRequest))
		}
	}
	if res.CPULimit != nil {
		if q, ok := safeParseQuantity(*res.CPULimit, "Story.CPULimit"); ok {
			config.Resources.Limits[corev1.ResourceCPU] = q
		} else {
			errs = append(errs, fmt.Errorf("story CPU limit %q invalid", *res.CPULimit))
		}
	}
	if res.MemoryLimit != nil {
		if q, ok := safeParseQuantity(*res.MemoryLimit, "Story.MemoryLimit"); ok {
			config.Resources.Limits[corev1.ResourceMemory] = q
		} else {
			errs = append(errs, fmt.Errorf("story memory limit %q invalid", *res.MemoryLimit))
		}
	}
	resolverLogger.V(1).Info("Applied Story resource overrides",
		"cpuRequest", res.CPURequest, "memoryRequest", res.MemoryRequest,
		"cpuLimit", res.CPULimit, "memoryLimit", res.MemoryLimit)
	return errs
}

func applyPolicySecurityDefaults(sec *v1alpha1.SecurityPolicy, config *ResolvedExecutionConfig) {
	if sec == nil {
		return
	}
	if sec.RunAsNonRoot != nil {
		config.RunAsNonRoot = *sec.RunAsNonRoot
	}
	if sec.AllowPrivilegeEscalation != nil {
		config.AllowPrivilegeEscalation = *sec.AllowPrivilegeEscalation
	}
	if sec.ReadOnlyRootFilesystem != nil {
		config.ReadOnlyRootFilesystem = *sec.ReadOnlyRootFilesystem
	}
	if sec.RunAsUser != nil {
		config.RunAsUser = *sec.RunAsUser
	}
}

func applyPolicyJobDefaults(job *v1alpha1.JobPolicy, config *ResolvedExecutionConfig) {
	if job == nil {
		return
	}
	if job.BackoffLimit != nil {
		config.BackoffLimit = *job.BackoffLimit
	}
	if job.TTLSecondsAfterFinished != nil {
		config.TTLSecondsAfterFinished = *job.TTLSecondsAfterFinished
	}
	if job.RestartPolicy != nil {
		switch *job.RestartPolicy {
		case string(corev1.RestartPolicyNever), string(corev1.RestartPolicyOnFailure), string(corev1.RestartPolicyAlways):
			config.RestartPolicy = corev1.RestartPolicy(*job.RestartPolicy)
		}
	}
}

func applyPolicyRetryDefaults(retry *v1alpha1.RetryPolicy, config *ResolvedExecutionConfig) {
	if retry == nil || retry.MaxRetries == nil {
		return
	}
	config.MaxRetries = int(*retry.MaxRetries)
}

func applyPolicyTimeoutDefaults(timeout *string, config *ResolvedExecutionConfig) []error {
	if timeout == nil {
		return nil
	}
	d, err := time.ParseDuration(*timeout)
	if err != nil {
		return []error{fmt.Errorf("story timeout %q invalid: %w", *timeout, err)}
	}
	config.DefaultStepTimeout = d
	return nil
}

func applyPolicyServiceAccount(name *string, config *ResolvedExecutionConfig) {
	if name == nil || *name == "" {
		return
	}
	config.ServiceAccountName = *name
}

func applyPolicyStorageDefaults(storage *v1alpha1.StoragePolicy, config *ResolvedExecutionConfig) {
	if storage == nil {
		return
	}
	config.Storage = cloneStoragePolicy(storage)
}

// applyStepRunOverrides applies settings from the StepRun to the resolved config,
// providing the highest priority layer of the configuration hierarchy.
//
// Behavior:
//   - Returns early if stepRun is nil.
//   - Applies ExecutionOverrides (CPURequest, CPULimit, MemoryRequest, MemoryLimit, Debug).
//   - Parses stepRun.Spec.Timeout as a duration and stores in DefaultStepTimeout.
//   - Applies stepRun.Spec.Retry.MaxRetries to config.MaxRetries.
//
// Arguments:
//   - stepRun *runsv1alpha1.StepRun: the StepRun to read settings from.
//   - config *ResolvedExecutionConfig: mutated in place with StepRun values.
//
// Side Effects:
//   - Mutates config.Resources, DebugLogs, DefaultStepTimeout, MaxRetries.
//
// Notes:
//   - StepRun settings have the highest priority and override all other layers.
//   - Timeout and Retry are on spec level for backwards compatibility.
func (cr *Resolver) applyStepRunOverrides(ctx context.Context, stepRun *runsv1alpha1.StepRun, config *ResolvedExecutionConfig) error {
	if stepRun == nil {
		return nil
	}

	executor := chain.NewExecutor(resolverLogger.WithName("steprun-chain").WithValues("stepRun", stepRun.Name)).
		WithObserver(chain.NewMetricsObserver(resolverLayerStepRun))
	executor.
		Add(chain.Stage{
			Name: "executionOverrides",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return applyStepRunExecutionOverrides(stepRun, config)
			},
		}).
		Add(chain.Stage{
			Name: "timeout",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return applyStepRunTimeout(stepRun, config)
			},
		}).
		Add(chain.Stage{
			Name: "retry",
			Mode: chain.ModeLogAndSkip,
			Fn: func(context.Context) error {
				return applyStepRunRetry(stepRun, config)
			},
		})

	return executor.Run(ctx)
}

func applyStepRunExecutionOverrides(stepRun *runsv1alpha1.StepRun, config *ResolvedExecutionConfig) error {
	if stepRun.Spec.ExecutionOverrides == nil {
		return nil
	}
	overrides := stepRun.Spec.ExecutionOverrides
	var errs []error
	if overrides.CPURequest != nil {
		if q, err := resource.ParseQuantity(*overrides.CPURequest); err != nil {
			errs = append(errs, fmt.Errorf("cpu request %q invalid: %w", *overrides.CPURequest, err))
		} else {
			config.Resources.Requests[corev1.ResourceCPU] = q
		}
	}
	if overrides.CPULimit != nil {
		if q, err := resource.ParseQuantity(*overrides.CPULimit); err != nil {
			errs = append(errs, fmt.Errorf("cpu limit %q invalid: %w", *overrides.CPULimit, err))
		} else {
			config.Resources.Limits[corev1.ResourceCPU] = q
		}
	}
	if overrides.MemoryRequest != nil {
		if q, err := resource.ParseQuantity(*overrides.MemoryRequest); err != nil {
			errs = append(errs, fmt.Errorf("memory request %q invalid: %w", *overrides.MemoryRequest, err))
		} else {
			config.Resources.Requests[corev1.ResourceMemory] = q
		}
	}
	if overrides.MemoryLimit != nil {
		if q, err := resource.ParseQuantity(*overrides.MemoryLimit); err != nil {
			errs = append(errs, fmt.Errorf("memory limit %q invalid: %w", *overrides.MemoryLimit, err))
		} else {
			config.Resources.Limits[corev1.ResourceMemory] = q
		}
	}
	if overrides.Debug != nil {
		config.DebugLogs = *overrides.Debug
	}
	resolverLogger.V(1).Info("Applied StepRun resource overrides",
		"cpuRequest", overrides.CPURequest, "memoryRequest", overrides.MemoryRequest,
		"cpuLimit", overrides.CPULimit, "memoryLimit", overrides.MemoryLimit)
	return errors.Join(errs...)
}

func applyStepRunTimeout(stepRun *runsv1alpha1.StepRun, config *ResolvedExecutionConfig) error {
	if stepRun.Spec.Timeout == "" {
		return nil
	}
	d, err := time.ParseDuration(stepRun.Spec.Timeout)
	if err != nil {
		return fmt.Errorf("timeout %q invalid: %w", stepRun.Spec.Timeout, err)
	}
	config.DefaultStepTimeout = d
	return nil
}

func applyStepRunRetry(stepRun *runsv1alpha1.StepRun, config *ResolvedExecutionConfig) error {
	if stepRun.Spec.Retry != nil && stepRun.Spec.Retry.MaxRetries != nil {
		config.MaxRetries = int(*stepRun.Spec.Retry.MaxRetries)
	}
	return nil
}

// ToResourceRequirements converts the resolved config to Kubernetes ResourceRequirements.
//
// Behavior:
//   - Returns the Resources field directly (no transformation).
//
// Returns:
//   - corev1.ResourceRequirements: the CPU/memory requests and limits for pod spec.
//
// Notes:
//   - The returned struct is a copy of the internal Resources field.
func (config *ResolvedExecutionConfig) ToResourceRequirements() corev1.ResourceRequirements {
	return config.Resources
}

// ToPodSecurityContext converts the resolved config to Kubernetes PodSecurityContext.
//
// Behavior:
//   - Creates a new PodSecurityContext with RunAsNonRoot and RunAsUser.
//   - Returns a pointer to the newly created struct.
//
// Returns:
//   - *corev1.PodSecurityContext: the pod-level security context for pod spec.
//
// Notes:
//   - Callers should treat the returned pointer as owned; safe to mutate.
//   - Only includes RunAsNonRoot and RunAsUser; other fields use Kubernetes defaults.
func (config *ResolvedExecutionConfig) ToPodSecurityContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{
		RunAsNonRoot: &config.RunAsNonRoot,
		RunAsUser:    &config.RunAsUser,
	}
}

// ToContainerSecurityContext converts the resolved config to Kubernetes SecurityContext.
//
// Behavior:
//   - Reads DropCapabilities; defaults to ["ALL"] if empty.
//   - Converts string capabilities to corev1.Capability slice.
//   - Creates a new SecurityContext with ReadOnlyRootFilesystem, AllowPrivilegeEscalation, and Capabilities.
//
// Returns:
//   - *corev1.SecurityContext: the container-level security context for container spec.
//
// Notes:
//   - Callers should treat the returned pointer as owned; safe to mutate.
//   - Only includes security fields relevant to capability dropping and filesystem access.
func (config *ResolvedExecutionConfig) ToContainerSecurityContext() *corev1.SecurityContext {
	drop := config.DropCapabilities
	if len(drop) == 0 {
		drop = []string{"ALL"}
	}
	capabilities := make([]corev1.Capability, 0, len(drop))
	for _, capability := range drop {
		capabilities = append(capabilities, corev1.Capability(capability))
	}
	return &corev1.SecurityContext{
		ReadOnlyRootFilesystem:   &config.ReadOnlyRootFilesystem,
		AllowPrivilegeEscalation: &config.AllowPrivilegeEscalation,
		Capabilities:             &corev1.Capabilities{Drop: capabilities},
	}
}
