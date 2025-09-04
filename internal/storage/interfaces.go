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

package storage

import (
	"context"

	corev1 "k8s.io/api/core/v1"
)

// StorageManager defines the interface for managing story-based storage
type StorageManager interface {
	// EnsureStoryRunStorage ensures persistent shared storage exists for a StoryRun
	// Uses persistent PVC strategy to avoid PV Released state and binding conflicts
	EnsureStoryRunStorage(ctx context.Context, namespace, storyName, storyRunID string) (string, error)

	// GetVolumeSpec returns volume configuration for engram pods
	GetVolumeSpec(pvcName string) ([]corev1.VolumeMount, []corev1.Volume, []corev1.EnvVar)

	// GetVolumeSpecForStep returns volume configuration with step-specific isolation
	GetVolumeSpecForStep(pvcName, stepRunID string) ([]corev1.VolumeMount, []corev1.Volume, []corev1.EnvVar)

	// LoadConfig loads storage configuration from ConfigMap
	LoadConfig(ctx context.Context) error
}
