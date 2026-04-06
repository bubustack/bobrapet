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
	"time"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	corestorage "github.com/bubustack/core/runtime/storage"
	corev1 "k8s.io/api/core/v1"
)

// StorageServiceAccountAnnotations returns the annotations that should be applied to a
// controller-managed ServiceAccount so workloads can access their configured storage backend.
//
// These annotations typically contain authentication credentials or configuration needed
// for storage backends like S3 (e.g., IAM role annotations for IRSA).
//
// Arguments:
//   - resolved: the resolved execution configuration containing storage settings.
//
// Returns:
//   - map[string]string: annotations to apply to the ServiceAccount, or nil if no storage
//     configuration requires ServiceAccount annotations.
func StorageServiceAccountAnnotations(resolved *config.ResolvedExecutionConfig) map[string]string {
	if resolved == nil || resolved.Storage == nil || resolved.Storage.S3 == nil {
		return nil
	}
	return CloneStringMap(resolved.Storage.S3.Authentication.ServiceAccountAnnotations)
}

// FormatStorageTimeout converts the resolved storage timeout into a user-friendly duration string.
//
// Behavior:
//   - Uses resolved.Storage.TimeoutSeconds when available and positive.
//   - Falls back to fallbackSeconds when resolved timeout is unavailable.
//   - Returns empty string when no valid timeout is available.
//
// Arguments:
//   - resolved *config.ResolvedExecutionConfig: the resolved execution config.
//   - fallbackSeconds int: fallback timeout when resolved timeout is unavailable.
//
// Returns:
//   - string: formatted duration (e.g., "30s") or empty string.
func FormatStorageTimeout(resolved *config.ResolvedExecutionConfig, fallbackSeconds int) string {
	if resolved != nil && resolved.Storage != nil && resolved.Storage.TimeoutSeconds > 0 {
		duration := time.Duration(resolved.Storage.TimeoutSeconds) * time.Second
		if duration%time.Second == 0 {
			return fmt.Sprintf("%ds", int64(duration/time.Second))
		}
		return duration.String()
	}
	if fallbackSeconds > 0 {
		return fmt.Sprintf("%ds", fallbackSeconds)
	}
	return ""
}

// ApplyStorageEnv converts the resolved storage policy into the shared runtime
// storage config and delegates to core/runtime/storage.ApplyEnv so controllers,
// connector, and SDK surfaces share the same env/volume wiring.
//
// Arguments:
//   - resolved *config.ResolvedExecutionConfig: supplies the storage policy.
//   - podSpec *corev1.PodSpec: mutated to include storage volumes when needed.
//   - container *corev1.Container: receives storage env vars and mounts.
//   - timeout string: formatted timeout string applied to relevant env vars.
func ApplyStorageEnv(
	resolved *config.ResolvedExecutionConfig,
	podSpec *corev1.PodSpec,
	container *corev1.Container,
	timeout string,
) {
	corestorage.ApplyEnv(buildRuntimeStorageConfig(resolved), podSpec, container, timeout)
}

// ApplySecretArtifacts wires the secret schema artifacts defined by an EngramTemplate into the pod spec.
//
// This function materializes template-defined secrets as Kubernetes volumes, environment variables,
// and volume mounts, enabling workloads to access sensitive configuration.
//
// Arguments:
//   - template: the EngramTemplate containing the secret schema definition.
//   - cfg: the resolved execution configuration containing actual secret values.
//   - podSpec: the PodTemplateSpec to modify with secret artifacts.
//
// Side Effects:
//   - Modifies podSpec.Volumes to include secret-backed volumes.
//   - Modifies podSpec.Spec.Containers[0].Env to include secret environment variables.
//   - Modifies podSpec.Spec.Containers[0].EnvFrom to include secret references.
//   - Modifies podSpec.Spec.Containers[0].VolumeMounts to include volume mounts.
func ApplySecretArtifacts(
	template *catalogv1alpha1.EngramTemplate,
	cfg *config.ResolvedExecutionConfig,
	podSpec *corev1.PodTemplateSpec,
) {
	if podSpec == nil ||
		len(podSpec.Spec.Containers) == 0 ||
		template == nil ||
		cfg == nil ||
		template.Spec.SecretSchema == nil {
		return
	}
	artifacts := BuildSecretArtifacts(template.Spec.SecretSchema, cfg.Secrets)
	artifacts.Apply(podSpec)
}

// buildRuntimeStorageConfig converts the resolved execution config to a runtime storage config.
func buildRuntimeStorageConfig(resolved *config.ResolvedExecutionConfig) *corestorage.Config {
	if resolved == nil {
		return nil
	}
	return toStorageConfig(resolved.Storage)
}

// toStorageConfig converts a StoragePolicy to the shared runtime storage config.
func toStorageConfig(policy *bubuv1alpha1.StoragePolicy) *corestorage.Config {
	if policy == nil {
		return nil
	}
	cfg := &corestorage.Config{}
	if policy.S3 != nil {
		cfg.S3 = &corestorage.S3Config{
			Bucket:       policy.S3.Bucket,
			Region:       policy.S3.Region,
			Endpoint:     policy.S3.Endpoint,
			UsePathStyle: policy.S3.UsePathStyle,
		}
		if policy.S3.Authentication.SecretRef != nil {
			cfg.S3.SecretName = policy.S3.Authentication.SecretRef.Name
		}
	}
	if policy.File != nil {
		file := &corestorage.FileConfig{
			Path:            policy.File.Path,
			VolumeClaimName: policy.File.VolumeClaimName,
		}
		if policy.File.EmptyDir != nil {
			file.EmptyDir = policy.File.EmptyDir.DeepCopy()
		}
		cfg.File = file
	}
	if cfg.S3 == nil && cfg.File == nil {
		return nil
	}
	return cfg
}
