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

package podspec

import (
	"maps"

	"github.com/bubustack/bobrapet/internal/config"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Config provides common fields required to build a pod template.
//
// This struct captures all fields shared between StepRun and Impulse pod templates,
// allowing controllers to pass container name, labels, annotations, and resolved
// execution configuration through a single struct.
//
// Example usage:
//
//	cfg := podspec.Config{
//	    ContainerName:             "engram",
//	    Labels:                    labels,
//	    Annotations:               annotations,
//	    EnvVars:                   envVars,
//	    ResolvedConfig:            resolvedConfig,
//	    TerminationGracePeriodSec: 30,
//	}
//	template := podspec.Build(cfg)
type Config struct {
	// ContainerName is the name for the primary container (e.g., "engram", "impulse").
	ContainerName string

	// Labels are applied to the pod metadata.
	Labels map[string]string

	// Annotations are applied to the pod metadata.
	Annotations map[string]string

	// EnvVars are environment variables for the primary container.
	EnvVars []corev1.EnvVar

	// EnvFrom are environment variable sources for the primary container.
	EnvFrom []corev1.EnvFromSource

	// Volumes are pod-level volumes.
	Volumes []corev1.Volume

	// VolumeMounts are mount points for the primary container.
	VolumeMounts []corev1.VolumeMount

	// Ports are container ports to expose.
	Ports []corev1.ContainerPort

	// RestartPolicy overrides the pod restart policy (optional).
	RestartPolicy corev1.RestartPolicy

	// TerminationGracePeriodSec is the pod termination grace period.
	TerminationGracePeriodSec int64

	// ResolvedConfig provides image, resources, probes, and security settings.
	ResolvedConfig *config.ResolvedExecutionConfig
}

// Build constructs a PodTemplateSpec from the provided configuration.
//
// Behavior:
//   - Creates a primary container with image, probes, resources, and security context
//     from ResolvedConfig.
//   - Applies labels, annotations, env vars, volumes, and ports from Config.
//   - Sets TerminationGracePeriodSeconds if configured.
//   - Only sets RestartPolicy if non-empty.
//
// Arguments:
//   - cfg Config: the configuration for the pod template.
//
// Returns:
//   - corev1.PodTemplateSpec: the constructed pod template.
//
// Notes:
//   - Callers are responsible for applying additional domain-specific modifications
//     (e.g., storage env, secret artifacts) after this function returns.
func Build(cfg Config) corev1.PodTemplateSpec {
	templateLabels := CloneStringMap(cfg.Labels)
	templateAnnotations := CloneStringMap(cfg.Annotations)

	container := corev1.Container{
		Name:            cfg.ContainerName,
		Image:           cfg.ResolvedConfig.Image,
		ImagePullPolicy: cfg.ResolvedConfig.ImagePullPolicy,
		Resources:       cfg.ResolvedConfig.Resources,
		SecurityContext: cfg.ResolvedConfig.ToContainerSecurityContext(),
		LivenessProbe:   cfg.ResolvedConfig.LivenessProbe,
		ReadinessProbe:  cfg.ResolvedConfig.ReadinessProbe,
		StartupProbe:    cfg.ResolvedConfig.StartupProbe,
	}

	if len(cfg.EnvVars) > 0 {
		container.Env = append([]corev1.EnvVar(nil), cfg.EnvVars...)
	}
	if len(cfg.EnvFrom) > 0 {
		container.EnvFrom = append([]corev1.EnvFromSource(nil), cfg.EnvFrom...)
	}
	if len(cfg.VolumeMounts) > 0 {
		container.VolumeMounts = append([]corev1.VolumeMount(nil), cfg.VolumeMounts...)
	}
	if len(cfg.Ports) > 0 {
		container.Ports = append([]corev1.ContainerPort(nil), cfg.Ports...)
	}

	podSpec := corev1.PodSpec{
		ServiceAccountName:           cfg.ResolvedConfig.ServiceAccountName,
		AutomountServiceAccountToken: new(cfg.ResolvedConfig.AutomountServiceAccountToken),
		SecurityContext:              cfg.ResolvedConfig.ToPodSecurityContext(),
		Containers:                   []corev1.Container{container},
	}

	if len(cfg.ResolvedConfig.NodeSelector) > 0 {
		podSpec.NodeSelector = CloneStringMap(cfg.ResolvedConfig.NodeSelector)
	}
	if len(cfg.ResolvedConfig.Tolerations) > 0 {
		podSpec.Tolerations = append([]corev1.Toleration(nil), cfg.ResolvedConfig.Tolerations...)
	}
	if cfg.ResolvedConfig.Affinity != nil {
		podSpec.Affinity = cfg.ResolvedConfig.Affinity.DeepCopy()
	}
	if len(cfg.Volumes) > 0 {
		podSpec.Volumes = append([]corev1.Volume(nil), cfg.Volumes...)
	}
	if cfg.TerminationGracePeriodSec > 0 {
		podSpec.TerminationGracePeriodSeconds = new(cfg.TerminationGracePeriodSec)
	}
	if cfg.RestartPolicy != "" {
		podSpec.RestartPolicy = cfg.RestartPolicy
	}

	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      templateLabels,
			Annotations: templateAnnotations,
		},
		Spec: podSpec,
	}
}

// CloneStringMap creates a shallow copy of a string map.
//
// Behavior:
//   - Returns nil for nil or empty source.
//   - Copies all key-value pairs to new map.
//
// Arguments:
//   - src map[string]string: source map to clone.
//
// Returns:
//   - map[string]string: the cloned map.
func CloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	maps.Copy(dst, src)
	return dst
}
