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
	"fmt"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/core/contracts"
	corev1 "k8s.io/api/core/v1"
)

// SecretArtifacts captures the Kubernetes objects required to surface secrets inside pods.
//
// This struct collects environment variables, envFrom sources, volumes, and volume mounts
// that should be added to a pod spec to make secrets available to the container.
type SecretArtifacts struct {
	// EnvVars are individual environment variables referencing secret keys.
	EnvVars []corev1.EnvVar

	// EnvFrom are secret references that inject all keys with a prefix.
	EnvFrom []corev1.EnvFromSource

	// Volumes are secret-backed volumes.
	Volumes []corev1.Volume

	// VolumeMounts are mount points for secret volumes.
	VolumeMounts []corev1.VolumeMount
}

// Apply mutates a PodTemplateSpec by appending secret-related volumes, env vars, and mounts.
//
// Behavior:
//   - Appends Volumes to podSpec.Spec.Volumes.
//   - Appends EnvVars, EnvFrom, and VolumeMounts to the first container.
//   - No-op if artifacts are empty or podSpec has no containers.
//
// Arguments:
//   - podSpec *corev1.PodTemplateSpec: the pod template to mutate.
//
// Side Effects:
//   - Modifies podSpec.Spec.Volumes.
//   - Modifies podSpec.Spec.Containers[0].Env, EnvFrom, and VolumeMounts.
func (a *SecretArtifacts) Apply(podSpec *corev1.PodTemplateSpec) {
	if podSpec == nil || len(podSpec.Spec.Containers) == 0 {
		return
	}
	if a.IsEmpty() {
		return
	}

	podSpec.Spec.Volumes = append(podSpec.Spec.Volumes, a.Volumes...)

	container := &podSpec.Spec.Containers[0]
	container.Env = append(container.Env, a.EnvVars...)
	container.EnvFrom = append(container.EnvFrom, a.EnvFrom...)
	container.VolumeMounts = append(container.VolumeMounts, a.VolumeMounts...)
}

// IsEmpty returns true if the artifacts contain no volumes, env vars, or mounts.
func (a *SecretArtifacts) IsEmpty() bool {
	return len(a.Volumes) == 0 && len(a.EnvVars) == 0 && len(a.EnvFrom) == 0 && len(a.VolumeMounts) == 0
}

// BuildSecretArtifacts converts template secret definitions combined with resolved user mappings
// into Kubernetes primitives that can be attached to a pod.
//
// Behavior:
//   - Returns empty SecretArtifacts when mappings or secretSchema is empty.
//   - Iterates through user-provided secret mappings.
//   - Applies each secret according to its MountType (file, env, or both).
//
// Arguments:
//   - secretSchema map[string]catalogv1alpha1.SecretDefinition: template-defined secret definitions.
//   - mappings map[string]string: user-provided logical name to actual secret name mappings.
//
// Returns:
//   - SecretArtifacts: Kubernetes primitives (EnvVars, EnvFrom, Volumes, VolumeMounts) for the pod.
//
// Example:
//
//	artifacts := podspec.BuildSecretArtifacts(template.Spec.SecretSchema, engram.Spec.Secrets)
//	artifacts.Apply(&podTemplate)
func BuildSecretArtifacts(
	secretSchema map[string]catalogv1alpha1.SecretDefinition,
	mappings map[string]string,
) SecretArtifacts {
	if len(mappings) == 0 || len(secretSchema) == 0 {
		return SecretArtifacts{}
	}

	result := SecretArtifacts{}
	for logicalName, actualSecretName := range mappings {
		definition, ok := secretSchema[logicalName]
		if !ok {
			continue
		}
		sdkSecretKey := fmt.Sprintf("%s%s", contracts.SecretPrefixEnv, logicalName)
		applySecret(&result, logicalName, actualSecretName, sdkSecretKey, definition)
	}
	return result
}

// applySecret applies a single secret definition to the artifacts based on MountType.
func applySecret(
	artifacts *SecretArtifacts,
	logicalName,
	actualSecretName,
	sdkSecretKey string,
	definition catalogv1alpha1.SecretDefinition,
) {
	switch definition.MountType {
	case enums.SecretMountTypeFile:
		applyFileSecret(artifacts, logicalName, actualSecretName, sdkSecretKey, definition)
	case enums.SecretMountTypeEnv:
		applyEnvSecret(artifacts, logicalName, actualSecretName, sdkSecretKey, definition, true, true)
	case enums.SecretMountTypeBoth:
		applyFileSecret(artifacts, logicalName, actualSecretName, sdkSecretKey, definition)
		applyEnvSecret(artifacts, logicalName, actualSecretName, sdkSecretKey, definition, true, false)
	default:
		applyEnvSecret(artifacts, logicalName, actualSecretName, sdkSecretKey, definition, true, true)
	}
}

// applyFileSecret mounts a secret as a file volume and sets the SDK env var.
func applyFileSecret(
	artifacts *SecretArtifacts,
	logicalName,
	actualSecretName,
	sdkSecretKey string,
	definition catalogv1alpha1.SecretDefinition,
) {
	volumeName := fmt.Sprintf("secret-%s", logicalName)
	mountPath := definition.MountPath
	if mountPath == "" {
		mountPath = fmt.Sprintf("/etc/bubu/secrets/%s", logicalName)
	}

	artifacts.Volumes = append(artifacts.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: actualSecretName},
		},
	})
	artifacts.VolumeMounts = append(artifacts.VolumeMounts, corev1.VolumeMount{
		Name:      volumeName,
		MountPath: mountPath,
		ReadOnly:  true,
	})
	artifacts.EnvVars = append(artifacts.EnvVars,
		corev1.EnvVar{Name: sdkSecretKey, Value: fmt.Sprintf("file:%s", mountPath)},
	)
}

// applyEnvSecret exposes a secret as environment variables.
func applyEnvSecret(
	artifacts *SecretArtifacts,
	logicalName,
	actualSecretName,
	sdkSecretKey string,
	definition catalogv1alpha1.SecretDefinition,
	exposeNameForPrefix bool,
	exposeNameForKeys bool,
) {
	if len(definition.ExpectedKeys) > 0 {
		addExplicitKeyEnvVars(artifacts, logicalName, actualSecretName, sdkSecretKey, definition, exposeNameForKeys)
		return
	}
	prefix := resolveSecretPrefix(logicalName, definition)
	artifacts.EnvFrom = append(artifacts.EnvFrom, corev1.EnvFromSource{
		Prefix:    prefix,
		SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: actualSecretName}},
	})
	artifacts.EnvVars = append(artifacts.EnvVars,
		corev1.EnvVar{Name: sdkSecretKey, Value: fmt.Sprintf("env:%s", prefix)},
	)
	if exposeNameForPrefix {
		artifacts.EnvVars = append(artifacts.EnvVars,
			corev1.EnvVar{Name: fmt.Sprintf("%s_NAME", sdkSecretKey), Value: actualSecretName},
		)
	}
}

// addExplicitKeyEnvVars adds environment variables for explicitly defined secret keys.
func addExplicitKeyEnvVars(
	artifacts *SecretArtifacts,
	logicalName,
	actualSecretName,
	sdkSecretKey string,
	definition catalogv1alpha1.SecretDefinition,
	exposeSecretName bool,
) {
	prefix := definition.EnvPrefix
	if prefix == "" {
		prefix = fmt.Sprintf("%s_", logicalName)
	}
	for _, key := range definition.ExpectedKeys {
		artifacts.EnvVars = append(artifacts.EnvVars, corev1.EnvVar{
			Name: prefix + key,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: actualSecretName},
					Key:                  key,
				},
			},
		})
	}

	sdkValue := fmt.Sprintf("env:%s", definition.EnvPrefix)
	if definition.EnvPrefix == "" {
		sdkValue = fmt.Sprintf("env:%s_", logicalName)
	}
	artifacts.EnvVars = append(artifacts.EnvVars, corev1.EnvVar{Name: sdkSecretKey, Value: sdkValue})

	if exposeSecretName {
		artifacts.EnvVars = append(artifacts.EnvVars,
			corev1.EnvVar{Name: fmt.Sprintf("%s_NAME", sdkSecretKey), Value: actualSecretName},
		)
	}
}

// resolveSecretPrefix returns the environment variable prefix for a secret.
func resolveSecretPrefix(logicalName string, definition catalogv1alpha1.SecretDefinition) string {
	if definition.EnvPrefix != "" {
		return definition.EnvPrefix
	}
	return fmt.Sprintf("%s_", logicalName)
}
