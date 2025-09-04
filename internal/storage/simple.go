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
	"crypto/md5"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/bubustack/bobrapet/internal/logging"
	"github.com/bubustack/bobrapet/internal/metrics"
)

// SimpleStorageManager provides minimal storage management
// Delegates to existing Kubernetes storage solutions (CSI drivers, StorageClasses)
type SimpleStorageManager struct {
	client       client.Client
	config       SimpleStorageConfig
	configLoaded bool
}

// SimpleStorageConfig defines basic storage configuration
type SimpleStorageConfig struct {
	// Storage classes to use (delegate to existing CSI drivers)
	ArtifactsStorageClass string `json:"artifactsStorageClass"`

	// Access mode for cross-pod sharing (critical for engrams)
	SharedAccessMode string `json:"sharedAccessMode"`

	// Default storage size per namespace
	DefaultStorageSize string `json:"defaultStorageSize"`

	// Retention settings (days)
	RetentionDays int `json:"retentionDays"`
}

// NewSimpleStorageManager creates a new simple storage manager
func NewSimpleStorageManager(client client.Client) *SimpleStorageManager {
	return &SimpleStorageManager{
		client: client,
		config: getDefaultConfig(),
	}
}

// EnsureStoryRunStorage ensures shared PVC exists for the specific StoryRun
// Uses persistent PVC strategy - keeps PVC alive to avoid PV Released state
func (sm *SimpleStorageManager) EnsureStoryRunStorage(ctx context.Context, namespace, storyName, storyRunID string) (string, error) {
	// Ensure config is loaded
	if err := sm.ensureConfigLoaded(ctx); err != nil {
		return "", fmt.Errorf("failed to load storage config: %w", err)
	}

	startTime := time.Now()
	defer func() {
		metrics.RecordStorageOperation("simple", "ensure", time.Since(startTime), nil)
	}()

	logger := logging.NewControllerLogger(ctx, "simple-storage")

	// Generate unique PVC name per StoryRun: storyrun-id + short hash
	runHash := generateShortHash(storyRunID)
	pvcName := fmt.Sprintf("%s-%s", storyRunID, runHash)

	// Check if PVC already exists
	var existingPVC corev1.PersistentVolumeClaim
	err := sm.client.Get(ctx, client.ObjectKey{Name: pvcName, Namespace: namespace}, &existingPVC)
	if err == nil {
		logger.V(1).Info("StoryRun PVC already exists", "pvc", pvcName, "storyrun", storyRunID)
		return pvcName, nil
	}

	if !errors.IsNotFound(err) {
		return "", fmt.Errorf("failed to check PVC: %w", err)
	}

	// Create persistent storyrun-specific PVC
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "bobrapet",
				"app.kubernetes.io/component": "storyrun-storage",
				"bobrapet.bubu.sh/story":      storyName,
				"bobrapet.bubu.sh/storyrun":   storyRunID,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.PersistentVolumeAccessMode(sm.config.SharedAccessMode),
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(sm.config.DefaultStorageSize),
				},
			},
		},
	}

	// Use configured storage class
	if sm.config.ArtifactsStorageClass != "" {
		pvc.Spec.StorageClassName = &sm.config.ArtifactsStorageClass
	}

	if err := sm.client.Create(ctx, pvc); err != nil {
		logger.Error(err, "Failed to create story PVC", "pvc", pvcName, "story", storyName)
		return "", fmt.Errorf("failed to create story PVC: %w", err)
	}

	logger.Info("Created story PVC",
		"pvc", pvcName,
		"story", storyName,
		"storageClass", sm.config.ArtifactsStorageClass,
		"size", sm.config.DefaultStorageSize)

	return pvcName, nil
}

// GetVolumeSpec returns volume spec for mounting in engram pods
func (sm *SimpleStorageManager) GetVolumeSpec(pvcName string) ([]corev1.VolumeMount, []corev1.Volume, []corev1.EnvVar) {
	return sm.GetVolumeSpecForStep(pvcName, "")
}

// GetVolumeSpecForStep returns volume spec with step-specific isolation
func (sm *SimpleStorageManager) GetVolumeSpecForStep(pvcName, stepRunID string) ([]corev1.VolumeMount, []corev1.Volume, []corev1.EnvVar) {
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "shared-storage",
			MountPath: "/shared",
		},
	}

	volumes := []corev1.Volume{
		{
			Name: "shared-storage",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
				},
			},
		},
	}

	// Environment variables with step-specific isolation
	envVars := []corev1.EnvVar{
		{
			Name:  "BOBRAPET_SHARED_PATH",
			Value: "/shared",
		},
		{
			Name:  "BOBRAPET_ARTIFACTS_PATH",
			Value: "/shared/artifacts",
		},
		{
			Name:  "BOBRAPET_LOGS_PATH",
			Value: "/shared/logs",
		},
		{
			Name:  "BOBRAPET_HISTORY_PATH",
			Value: "/shared/history",
		},
	}

	// Add step-specific working directory if provided
	if stepRunID != "" {
		envVars = append(envVars, []corev1.EnvVar{
			{
				Name:  "BOBRAPET_STEP_WORKSPACE",
				Value: fmt.Sprintf("/shared/workspace/%s", stepRunID),
			},
			{
				Name:  "BOBRAPET_STEP_TEMP",
				Value: fmt.Sprintf("/shared/temp/%s", stepRunID),
			},
		}...)
	}

	return volumeMounts, volumes, envVars
}

// ensureConfigLoaded loads config lazily if not already loaded
func (sm *SimpleStorageManager) ensureConfigLoaded(ctx context.Context) error {
	if sm.configLoaded {
		return nil
	}
	err := sm.LoadConfig(ctx)
	if err == nil {
		sm.configLoaded = true
	}
	return err
}

// LoadConfig loads storage configuration from ConfigMap or uses defaults
func (sm *SimpleStorageManager) LoadConfig(ctx context.Context) error {
	var cm corev1.ConfigMap
	err := sm.client.Get(ctx,
		client.ObjectKey{Name: "bobrapet-storage-config", Namespace: "bobrapet-system"},
		&cm)

	if errors.IsNotFound(err) {
		// Use defaults if no config exists
		sm.config = getDefaultConfig()
		return nil
	}

	if err != nil {
		return fmt.Errorf("failed to load storage config: %w", err)
	}

	// Parse config from ConfigMap
	config := getDefaultConfig()

	if val, exists := cm.Data["artifacts-storage-class"]; exists && val != "" {
		config.ArtifactsStorageClass = val
	}
	if val, exists := cm.Data["shared-access-mode"]; exists && val != "" {
		config.SharedAccessMode = val
	}
	if val, exists := cm.Data["default-storage-size"]; exists && val != "" {
		config.DefaultStorageSize = val
	}

	sm.config = config
	return nil
}

// GetConfig returns current storage configuration
func (sm *SimpleStorageManager) GetConfig() SimpleStorageConfig {
	return sm.config
}

// Helper functions

func getDefaultConfig() SimpleStorageConfig {
	return SimpleStorageConfig{
		ArtifactsStorageClass: "hostpath-rwx",  // Use static PV with persistent PVC strategy
		SharedAccessMode:      "ReadWriteMany", // Back to RWM for parallel access within StoryRun
		DefaultStorageSize:    "1Gi",           // Standard storage size
		RetentionDays:         30,
	}
}

// generateShortHash creates a short hash from input string for unique PVC naming
func generateShortHash(input string) string {
	// Generate MD5 hash and take first 8 characters
	hash := fmt.Sprintf("%x", md5.Sum([]byte(input)))
	return hash[:8]
}
