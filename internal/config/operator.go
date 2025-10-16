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
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const strTrue = "true"

// OperatorConfig holds the configuration for the operator
type OperatorConfig struct {
	Controller ControllerConfig `json:"controller,omitempty"`
}

// DefaultOperatorConfig returns the default configuration
func DefaultOperatorConfig() *OperatorConfig {
	return &OperatorConfig{
		Controller: ControllerConfig{
			MaxConcurrentReconciles:      10,
			ImagePullPolicy:              corev1.PullIfNotPresent,
			JobBackoffLimit:              3,
			JobRestartPolicy:             corev1.RestartPolicyNever,
			TTLSecondsAfterFinished:      3600,
			ServiceAccountName:           "default",
			AutomountServiceAccountToken: false,
			DefaultEngramGRPCPort:        50051,
			DefaultCPURequest:            "100m",
			DefaultCPULimit:              "500m",
			DefaultMemoryRequest:         "128Mi",
			DefaultMemoryLimit:           "512Mi",
			MaxRetries:                   3,
			ExponentialBackoffBase:       1 * time.Second,
			ExponentialBackoffMax:        60 * time.Second,
			CleanupInterval:              metav1.Duration{Duration: 1 * time.Hour},
			ReconcileTimeout:             30 * time.Second,
		},
	}
}

// GetControllerConfig returns the controller-specific configuration
func (m *OperatorConfigManager) GetControllerConfig() *ControllerConfig {
	return &m.currentConfig.Controller
}

// OperatorConfigManager manages the operator's dynamic configuration
type OperatorConfigManager struct {
	client        client.Client
	namespace     string
	configMapName string
	currentConfig *OperatorConfig
	defaultConfig *OperatorConfig
	mu            sync.RWMutex
	lastSyncTime  time.Time
	SyncInterval  time.Duration
}

// NewOperatorConfigManager creates a new configuration manager
func NewOperatorConfigManager(k8sClient client.Client, namespace, configMapName string) *OperatorConfigManager {
	manager := &OperatorConfigManager{
		client:        k8sClient,
		namespace:     namespace,
		configMapName: configMapName,
		defaultConfig: DefaultOperatorConfig(),
		SyncInterval:  1 * time.Minute,
	}
	manager.currentConfig = manager.defaultConfig
	return manager
}

// GetConfig returns the current operator configuration
func (m *OperatorConfigManager) GetConfig() *OperatorConfig {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentConfig
}

// RefreshConfig clears the cache and forces a reload
func (ocm *OperatorConfigManager) RefreshConfig() {
	ocm.currentConfig = ocm.defaultConfig
}

// loadAndParseConfigMap loads the ConfigMap and parses it into OperatorConfig
func (m *OperatorConfigManager) loadAndParseConfigMap(ctx context.Context) (*OperatorConfig, error) {
	configMap := &corev1.ConfigMap{}
	err := m.client.Get(ctx, types.NamespacedName{
		Name:      m.configMapName,
		Namespace: m.namespace,
	}, configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to get operator config map: %w", err)
	}

	config := &OperatorConfig{}
	m.parseConfigMap(configMap, config)

	return config, nil
}

// Start runs the configuration synchronization loop.
// It implements the controller-runtime manager.Runnable interface.
func (m *OperatorConfigManager) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithName("config-manager")
	logger.Info("Starting operator config synchronizer")

	// Immediately try to sync on startup
	if err := m.sync(ctx); err != nil {
		logger.Error(err, "Initial configuration sync failed")
		// Depending on strictness, we might want to return the error
		// and prevent the manager from starting if the config is critical.
	}

	ticker := time.NewTicker(m.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping operator config synchronizer")
			return nil
		case <-ticker.C:
			if err := m.sync(ctx); err != nil {
				logger.Error(err, "Failed to sync operator configuration")
			}
		}
	}
}

// sync performs a single configuration synchronization.
func (m *OperatorConfigManager) sync(ctx context.Context) error {
	logger := log.FromContext(ctx)
	newConfig, err := m.loadAndParseConfigMap(ctx)
	if err != nil {
		return fmt.Errorf("failed to load operator configuration: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentConfig = newConfig
	m.lastSyncTime = time.Now()
	logger.Info("Successfully synced operator configuration")
	return nil
}

// parseConfigMap parses a ConfigMap into OperatorConfig
func (ocm *OperatorConfigManager) parseConfigMap(cm *corev1.ConfigMap, config *OperatorConfig) {
	parseControllerTimings(cm, config)
	parseImageConfig(cm, config)
	parseResourceLimits(cm, config)
	parseRetryAndTimeouts(cm, config)
	parseLoopConfig(cm, config)
	parseSecurityConfig(cm, config)
	parseJobConfig(cm, config)
	parseCELConfig(cm, config)
	parseTelemetryConfig(cm, config)
	parseDebugConfig(cm, config)
	parseEngramDefaults(cm, config)
	parseStoryRunConfig(cm, config)
}

func parseControllerTimings(cm *corev1.ConfigMap, config *OperatorConfig) {
	setMaxConcurrentReconciles(cm, config)
	setRequeueBaseDelay(cm, config)
	setRequeueMaxDelay(cm, config)
	setHealthCheckInterval(cm, config)
	setCleanupInterval(cm, config)
	setReconcileTimeout(cm, config)
	setDefaultEngramGRPCPort(cm, config)
}

func setMaxConcurrentReconciles(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["controller.max-concurrent-reconciles"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.MaxConcurrentReconciles = parsed
		}
	}
}

func setRequeueBaseDelay(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["controller.requeue-base-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.RequeueBaseDelay = parsed
		}
	}
}

func setRequeueMaxDelay(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["controller.requeue-max-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.RequeueMaxDelay = parsed
		}
	}
}

func setHealthCheckInterval(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["controller.health-check-interval"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.HealthCheckInterval = parsed
		}
	}
}

func setCleanupInterval(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["controller.cleanup-interval"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.CleanupInterval = metav1.Duration{Duration: parsed}
		}
	}
}

func setReconcileTimeout(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["controller.reconcile-timeout"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.ReconcileTimeout = parsed
		}
	}
}

func setDefaultEngramGRPCPort(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["controller.default-engram-grpc-port"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.DefaultEngramGRPCPort = parsed
		}
	}
}

func parseImageConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["images.default-engram"]; exists {
		config.Controller.DefaultEngramImage = val
	}
	if val, exists := cm.Data["images.default-impulse"]; exists {
		config.Controller.DefaultImpulseImage = val
	}
	if val, exists := cm.Data["images.pull-policy"]; exists {
		switch val {
		case string(corev1.PullAlways):
			config.Controller.ImagePullPolicy = corev1.PullAlways
		case string(corev1.PullNever):
			config.Controller.ImagePullPolicy = corev1.PullNever
		case string(corev1.PullIfNotPresent):
			config.Controller.ImagePullPolicy = corev1.PullIfNotPresent
		}
	}
}

func parseResourceLimits(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["resources.engram.cpu-request"]; exists {
		config.Controller.EngramCPURequest = val
	}
	if val, exists := cm.Data["resources.engram.cpu-limit"]; exists {
		config.Controller.EngramCPULimit = val
	}
	if val, exists := cm.Data["resources.engram.memory-request"]; exists {
		config.Controller.EngramMemoryRequest = val
	}
	if val, exists := cm.Data["resources.engram.memory-limit"]; exists {
		config.Controller.EngramMemoryLimit = val
	}
}

func parseRetryAndTimeouts(cm *corev1.ConfigMap, config *OperatorConfig) {
	setMaxRetries(cm, config)
	setExponentialBackoffBase(cm, config)
	setExponentialBackoffMax(cm, config)
	setDefaultStepTimeout(cm, config)
	setApprovalDefaultTimeout(cm, config)
	setExternalDataTimeout(cm, config)
	setConditionalTimeout(cm, config)
}

func setMaxRetries(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["retry.max-retries"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed >= 0 {
			config.Controller.MaxRetries = parsed
		}
	}
}

func setExponentialBackoffBase(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["retry.exponential-backoff-base"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.ExponentialBackoffBase = parsed
		}
	}
}

func setExponentialBackoffMax(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["retry.exponential-backoff-max"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.ExponentialBackoffMax = parsed
		}
	}
}

func setDefaultStepTimeout(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["timeout.default-step"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.DefaultStepTimeout = parsed
		}
	}
}

func setApprovalDefaultTimeout(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["timeout.approval-default"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.ApprovalDefaultTimeout = parsed
		}
	}
}

func setExternalDataTimeout(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["timeout.external-data-default"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.ExternalDataTimeout = parsed
		}
	}
}

func setConditionalTimeout(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["timeout.conditional-default"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.ConditionalTimeout = parsed
		}
	}
}

func parseLoopConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	setMaxLoopIterations(cm, config)
	setDefaultLoopBatchSize(cm, config)
	setMaxLoopBatchSize(cm, config)
	setMaxLoopConcurrency(cm, config)
	setMaxConcurrencyLimit(cm, config)
}

func setMaxLoopIterations(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["loop.max-iterations"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.MaxLoopIterations = parsed
		}
	}
}

func setDefaultLoopBatchSize(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["loop.default-batch-size"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.DefaultLoopBatchSize = parsed
		}
	}
}

func setMaxLoopBatchSize(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["loop.max-batch-size"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.MaxLoopBatchSize = parsed
		}
	}
}

func setMaxLoopConcurrency(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["loop.max-concurrency"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.MaxLoopConcurrency = parsed
		}
	}
}

func setMaxConcurrencyLimit(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["loop.max-concurrency-limit"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.MaxConcurrencyLimit = parsed
		}
	}
}

func parseSecurityConfig(cm *corev1.ConfigMap, config *OperatorConfig) {

	if val, exists := cm.Data["security.run-as-non-root"]; exists {
		config.Controller.RunAsNonRoot = val == strTrue
	}
	if val, exists := cm.Data["security.read-only-root-filesystem"]; exists {
		config.Controller.ReadOnlyRootFilesystem = val == strTrue
	}
	if val, exists := cm.Data["security.allow-privilege-escalation"]; exists {
		config.Controller.AllowPrivilegeEscalation = val == strTrue
	}
	if val, exists := cm.Data["security.run-as-user"]; exists {
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			config.Controller.RunAsUser = parsed
		}
	}
	if val, exists := cm.Data["security.automount-service-account-token"]; exists {
		config.Controller.AutomountServiceAccountToken = val == strTrue
	}
}

func parseJobConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["job.backoff-limit"]; exists {
		if parsed, err := strconv.ParseInt(val, 10, 32); err == nil {
			config.Controller.JobBackoffLimit = int32(parsed)
		}
	}
	if val, exists := cm.Data["job.ttl-seconds-after-finished"]; exists {
		if parsed, err := strconv.ParseInt(val, 10, 32); err == nil {
			config.Controller.TTLSecondsAfterFinished = int32(parsed)
		}
	}
}

func parseCELConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["cel.evaluation-timeout"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.CELEvaluationTimeout = parsed
		}
	}
	if val, exists := cm.Data["cel.max-expression-length"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.CELMaxExpressionLength = parsed
		}
	}
	if val, exists := cm.Data["cel.enable-macros"]; exists {
		config.Controller.CELEnableMacros = val == strTrue
	}
}

func parseTelemetryConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["telemetry.enabled"]; exists {
		config.Controller.TelemetryEnabled = val == strTrue
	}
	if val, exists := cm.Data["telemetry.trace-propagation"]; exists {
		config.Controller.TracePropagationEnabled = val == strTrue
	}
}

func parseDebugConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["debug.enable-verbose-logging"]; exists {
		config.Controller.EnableVerboseLogging = val == strTrue
	}
	if val, exists := cm.Data["debug.enable-step-output-logging"]; exists {
		config.Controller.EnableStepOutputLogging = val == strTrue
	}
	if val, exists := cm.Data["debug.enable-metrics"]; exists {
		config.Controller.EnableMetrics = val == strTrue
	}
}

func parseEngramDefaults(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["engram.default-max-inline-size"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize = parsed
		}
	}
}

func parseStoryRunConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["storyrun.max-inline-inputs-size"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.StoryRun.MaxInlineInputsSize = parsed
		}
	}
}
