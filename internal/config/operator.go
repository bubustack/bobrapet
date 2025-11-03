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
	"fmt"
	"strconv"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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

// SetAPIReader configures a non-cached reader for situations where the cache is not yet running.
func (m *OperatorConfigManager) SetAPIReader(reader client.Reader) {
	m.apiReader = reader
}

// OperatorConfigManager manages the operator's dynamic configuration
// It implements the controller-runtime reconcile.Reconciler interface
type OperatorConfigManager struct {
	client        client.Client
	apiReader     client.Reader
	namespace     string
	configMapName string
	currentConfig *OperatorConfig
	defaultConfig *OperatorConfig
	mu            sync.RWMutex
	lastSyncTime  time.Time
}

// NewOperatorConfigManager creates a new configuration manager
func NewOperatorConfigManager(k8sClient client.Client, namespace, configMapName string) *OperatorConfigManager {
	manager := &OperatorConfigManager{
		client:        k8sClient,
		apiReader:     nil,
		namespace:     namespace,
		configMapName: configMapName,
		defaultConfig: DefaultOperatorConfig(),
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

// LoadInitial performs a one-time synchronous load of the operator configuration.
func (m *OperatorConfigManager) LoadInitial(ctx context.Context) error {
	config, err := m.loadAndParseConfigMap(ctx)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.currentConfig = config
	m.lastSyncTime = time.Now()
	m.mu.Unlock()
	return nil
}

// loadAndParseConfigMap loads the ConfigMap and parses it into OperatorConfig
func (m *OperatorConfigManager) loadAndParseConfigMap(ctx context.Context) (*OperatorConfig, error) {
	reader := m.apiReader
	if reader == nil {
		reader = m.client
	}

	configMap := &corev1.ConfigMap{}
	err := reader.Get(ctx, types.NamespacedName{
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

// Reconcile handles ConfigMap changes and updates the operator configuration.
// This is the event-driven approach that reacts to ConfigMap updates immediately.
func (m *OperatorConfigManager) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	logger := log.FromContext(ctx).WithName("config-manager")

	// Only process our specific ConfigMap
	if req.Name != m.configMapName || req.Namespace != m.namespace {
		return reconcile.Result{}, nil
	}

	logger.Info("ConfigMap changed, reloading configuration", "configMap", req.NamespacedName)

	newConfig, err := m.loadAndParseConfigMap(ctx)
	if err != nil {
		logger.Error(err, "Failed to load operator configuration")
		// Requeue with exponential backoff on error
		return reconcile.Result{RequeueAfter: 30 * time.Second}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.currentConfig = newConfig
	m.lastSyncTime = time.Now()

	logger.Info("Successfully reloaded operator configuration",
		"configMap", req.NamespacedName,
		"lastSync", m.lastSyncTime.Format(time.RFC3339))

	return reconcile.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (m *OperatorConfigManager) SetupWithManager(mgr ctrl.Manager) error {
	// Create a predicate that only watches our specific ConfigMap
	configMapPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return e.Object.GetName() == m.configMapName &&
				e.Object.GetNamespace() == m.namespace
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectNew.GetName() == m.configMapName &&
				e.ObjectNew.GetNamespace() == m.namespace
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			// If ConfigMap is deleted, we should know about it
			return e.Object.GetName() == m.configMapName &&
				e.Object.GetNamespace() == m.namespace
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}

	// Note: We don't load initial config here because the cache isn't started yet.
	// The first reconcile event (triggered by the informer) will load the config.
	// This happens almost immediately after the manager starts.

	return ctrl.NewControllerManagedBy(mgr).
		Named("operator-config-manager").
		For(&corev1.ConfigMap{}).
		WithEventFilter(configMapPredicate).
		Complete(m)
}

// parseConfigMap parses a ConfigMap into OperatorConfig
func (ocm *OperatorConfigManager) parseConfigMap(cm *corev1.ConfigMap, config *OperatorConfig) {
	// Initialize nested structs with defaults first
	config.Controller = *DefaultControllerConfig()

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
	parseStepRunConfig(cm, config)
	parseStoryConfig(cm, config)
	parseEngramConfig(cm, config)
	parseImpulseConfig(cm, config)
	parseTemplateConfig(cm, config)
	parseStorageDefaults(cm, config)
}

func parseControllerTimings(cm *corev1.ConfigMap, config *OperatorConfig) {
	setMaxConcurrentReconciles(cm, config)
	setRequeueBaseDelay(cm, config)
	setRequeueMaxDelay(cm, config)
	setHealthCheckInterval(cm, config)
	setCleanupInterval(cm, config)
	setReconcileTimeout(cm, config)
	setDefaultEngramGRPCPort(cm, config)
	setMaxStoryWithBlockSizeBytes(cm, config)
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

func setMaxStoryWithBlockSizeBytes(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["controller.max-story-with-block-size-bytes"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.MaxStoryWithBlockSizeBytes = parsed
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
	// Default resource limits
	if val, exists := cm.Data["resources.default.cpu-request"]; exists {
		config.Controller.DefaultCPURequest = val
	}
	if val, exists := cm.Data["resources.default.cpu-limit"]; exists {
		config.Controller.DefaultCPULimit = val
	}
	if val, exists := cm.Data["resources.default.memory-request"]; exists {
		config.Controller.DefaultMemoryRequest = val
	}
	if val, exists := cm.Data["resources.default.memory-limit"]; exists {
		config.Controller.DefaultMemoryLimit = val
	}

	// Engram-specific resource limits
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
	if val, exists := cm.Data["security.service-account-name"]; exists {
		config.Controller.ServiceAccountName = val
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
	if val, exists := cm.Data["job.restart-policy"]; exists {
		switch val {
		case string(corev1.RestartPolicyAlways):
			config.Controller.JobRestartPolicy = corev1.RestartPolicyAlways
		case string(corev1.RestartPolicyOnFailure):
			config.Controller.JobRestartPolicy = corev1.RestartPolicyOnFailure
		case string(corev1.RestartPolicyNever):
			config.Controller.JobRestartPolicy = corev1.RestartPolicyNever
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
	if val, exists := cm.Data["storyrun.max-concurrent-reconciles"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.StoryRun.MaxConcurrentReconciles = parsed
		}
	}
	if val, exists := cm.Data["storyrun.rate-limiter.base-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.StoryRun.RateLimiter.BaseDelay = parsed
		}
	}
	if val, exists := cm.Data["storyrun.rate-limiter.max-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.StoryRun.RateLimiter.MaxDelay = parsed
		}
	}
	if val, exists := cm.Data["storyrun.max-inline-inputs-size"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.StoryRun.MaxInlineInputsSize = parsed
		}
	}
}

func parseStepRunConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["steprun.max-concurrent-reconciles"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.StepRun.MaxConcurrentReconciles = parsed
		}
	}
	if val, exists := cm.Data["steprun.rate-limiter.base-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.StepRun.RateLimiter.BaseDelay = parsed
		}
	}
	if val, exists := cm.Data["steprun.rate-limiter.max-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.StepRun.RateLimiter.MaxDelay = parsed
		}
	}
}

func parseStoryConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["story.max-concurrent-reconciles"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.Story.MaxConcurrentReconciles = parsed
		}
	}
	if val, exists := cm.Data["story.rate-limiter.base-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.Story.RateLimiter.BaseDelay = parsed
		}
	}
	if val, exists := cm.Data["story.rate-limiter.max-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.Story.RateLimiter.MaxDelay = parsed
		}
	}
}

func parseEngramConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	// Controller-level Engram config
	parseIntField(cm, "engram.max-concurrent-reconciles", &config.Controller.Engram.MaxConcurrentReconciles, true)
	parseDurationField(cm, "engram.rate-limiter.base-delay", &config.Controller.Engram.RateLimiter.BaseDelay)
	parseDurationField(cm, "engram.rate-limiter.max-delay", &config.Controller.Engram.RateLimiter.MaxDelay)

	// Engram-specific controller config
	parseEngramControllerConfig(cm, &config.Controller.Engram.EngramControllerConfig)
}

func parseEngramControllerConfig(cm *corev1.ConfigMap, cfg *EngramControllerConfig) {
	parseIntField(cm, "engram.default-grpc-port", &cfg.DefaultGRPCPort, true)
	parseIntField(cm, "engram.default-storage-timeout-seconds", &cfg.DefaultStorageTimeoutSeconds, true)
	parseIntField(cm, "engram.default-graceful-shutdown-timeout-seconds", &cfg.DefaultGracefulShutdownTimeoutSeconds, true)
	parseInt64Field(cm, "engram.default-termination-grace-period-seconds", &cfg.DefaultTerminationGracePeriodSeconds, true)
	parseIntField(cm, "engram.default-max-recv-msg-bytes", &cfg.DefaultMaxRecvMsgBytes, true)
	parseIntField(cm, "engram.default-max-send-msg-bytes", &cfg.DefaultMaxSendMsgBytes, true)
	parseIntField(cm, "engram.default-dial-timeout-seconds", &cfg.DefaultDialTimeoutSeconds, true)
	parseIntField(cm, "engram.default-channel-buffer-size", &cfg.DefaultChannelBufferSize, true)
	parseIntField(cm, "engram.default-reconnect-max-retries", &cfg.DefaultReconnectMaxRetries, false) // Can be 0
	parseIntField(cm, "engram.default-reconnect-base-backoff-millis", &cfg.DefaultReconnectBaseBackoffMillis, true)
	parseIntField(cm, "engram.default-reconnect-max-backoff-seconds", &cfg.DefaultReconnectMaxBackoffSeconds, true)
	parseIntField(cm, "engram.default-hang-timeout-seconds", &cfg.DefaultHangTimeoutSeconds, true)
	parseIntField(cm, "engram.default-message-timeout-seconds", &cfg.DefaultMessageTimeoutSeconds, true)
}

// Helper functions to reduce cyclomatic complexity
func parseIntField(cm *corev1.ConfigMap, key string, target *int, requirePositive bool) {
	if val, exists := cm.Data[key]; exists {
		if parsed, err := strconv.Atoi(val); err == nil {
			if !requirePositive || parsed > 0 {
				*target = parsed
			}
		}
	}
}

func parseInt64Field(cm *corev1.ConfigMap, key string, target *int64, requirePositive bool) {
	if val, exists := cm.Data[key]; exists {
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			if !requirePositive || parsed > 0 {
				*target = parsed
			}
		}
	}
}

func parseDurationField(cm *corev1.ConfigMap, key string, target *time.Duration) {
	if val, exists := cm.Data[key]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			*target = parsed
		}
	}
}

func parseImpulseConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["impulse.max-concurrent-reconciles"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.Impulse.MaxConcurrentReconciles = parsed
		}
	}
	if val, exists := cm.Data["impulse.rate-limiter.base-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.Impulse.RateLimiter.BaseDelay = parsed
		}
	}
	if val, exists := cm.Data["impulse.rate-limiter.max-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.Impulse.RateLimiter.MaxDelay = parsed
		}
	}
}

func parseTemplateConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["template.max-concurrent-reconciles"]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.Template.MaxConcurrentReconciles = parsed
		}
	}
	if val, exists := cm.Data["template.rate-limiter.base-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.Template.RateLimiter.BaseDelay = parsed
		}
	}
	if val, exists := cm.Data["template.rate-limiter.max-delay"]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.Template.RateLimiter.MaxDelay = parsed
		}
	}
}

func parseStorageDefaults(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data["controller.storage.provider"]; exists {
		config.Controller.DefaultStorageProvider = val
	}
	if val, exists := cm.Data["controller.storage.s3.bucket"]; exists {
		config.Controller.DefaultS3Bucket = val
	}
	if val, exists := cm.Data["controller.storage.s3.region"]; exists {
		config.Controller.DefaultS3Region = val
	}
	if val, exists := cm.Data["controller.storage.s3.endpoint"]; exists {
		config.Controller.DefaultS3Endpoint = val
	}
	if val, exists := cm.Data["controller.storage.s3.use-path-style"]; exists {
		config.Controller.DefaultS3UsePathStyle = val == strTrue
	}
	if val, exists := cm.Data["controller.storage.s3.auth-secret-name"]; exists {
		config.Controller.DefaultS3AuthSecretName = val
	}
}
