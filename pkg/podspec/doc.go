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

// Package podspec provides utilities for building Kubernetes PodTemplateSpec objects.
//
// This package consolidates pod template building logic used by controllers when
// constructing Jobs, Deployments, and StatefulSets. It provides a consistent
// approach to applying:
//
//   - Container configuration (image, resources, probes, security context)
//   - Secret artifacts (environment variables, volumes, mounts)
//   - Storage configuration (S3, file-based storage env vars)
//   - Labels, annotations, and metadata
//
// # Pod Template Builder
//
// The [Build] function constructs a [corev1.PodTemplateSpec] from a [Config] struct,
// applying resolved execution settings consistently across all workload types:
//
//	cfg := podspec.Config{
//	    ContainerName:  "engram",
//	    Labels:         labels,
//	    ResolvedConfig: resolved,
//	}
//	template := podspec.Build(cfg)
//
// # Secret Artifacts
//
// The [BuildSecretArtifacts] function converts EngramTemplate secret schemas into
// Kubernetes primitives that can be attached to pods:
//
//	artifacts := podspec.BuildSecretArtifacts(template.Spec.SecretSchema, secrets)
//	artifacts.Apply(&podTemplate)
//
// # Storage Configuration
//
// The [ApplyStorageEnv] function wires storage policy settings into pod specs,
// delegating to the shared runtime storage helpers:
//
//	podspec.ApplyStorageEnv(resolved, &podSpec, &container, timeout)
//
// This package follows Kubernetes controller-runtime conventions and is designed
// to be used alongside [github.com/bubustack/bobrapet/internal/config] for
// configuration resolution.
package podspec
