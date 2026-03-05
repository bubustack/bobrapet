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
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/runtime/operatorconfig"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	strTrue              = "true"
	maxRetriesUpperBound = 50
)

var (
	configParserLog  = ctrl.Log.WithName("operator-config").WithName("parser")
	configManagerLog = ctrl.Log.WithName("operator-config").WithName("manager")
)

// ConfigReloadReason describes why the configuration was reloaded.
type ConfigReloadReason string

const (
	// ConfigReloadInitial indicates the initial configuration load at startup.
	ConfigReloadInitial ConfigReloadReason = "initial"
	// ConfigReloadReconcile indicates a reload triggered by ConfigMap change.
	ConfigReloadReconcile ConfigReloadReason = "reconcile"
	// ConfigReloadRefresh indicates a manual refresh.
	ConfigReloadRefresh ConfigReloadReason = "refresh"
)

// OperatorConfig holds the configuration for the operator
type OperatorConfig struct {
	Controller ControllerConfig `json:"controller,omitempty"`
}

// Clone returns a deep copy of the OperatorConfig.
func (cfg *OperatorConfig) Clone() *OperatorConfig {
	if cfg == nil {
		return nil
	}
	clone := &OperatorConfig{
		Controller: cfg.Controller.Clone(),
	}
	return clone
}

// Clone returns a deep copy of the ControllerConfig.
func (c *ControllerConfig) Clone() ControllerConfig {
	if c == nil {
		return ControllerConfig{}
	}
	copyCfg := *c
	if len(c.DropCapabilities) > 0 {
		copyCfg.DropCapabilities = append([]string(nil), c.DropCapabilities...)
	}
	if len(c.StoryRun.Scheduling.Queues) > 0 {
		copyCfg.StoryRun.Scheduling.Queues = make(map[string]QueueConfig, len(c.StoryRun.Scheduling.Queues))
		for key, value := range c.StoryRun.Scheduling.Queues {
			copyCfg.StoryRun.Scheduling.Queues[key] = value
		}
	}
	return copyCfg
}

// DefaultOperatorConfig returns the default configuration with hardcoded values.
//
// Behavior:
//   - Creates a new ControllerConfig via DefaultControllerConfig().
//   - Overrides key fields with operator-level defaults (concurrency, images, resources, etc.).
//   - Returns a new OperatorConfig containing the configured ControllerConfig.
//
// Returns:
//   - *OperatorConfig: a new configuration struct with hardcoded defaults.
//
// Notes:
//   - These defaults are used when no ConfigMap is present or as fallback values.
//   - See DefaultControllerConfig() for controller-specific nested defaults.
func DefaultOperatorConfig() *OperatorConfig {
	controllerDefaults := controllerConfigDefaults()
	return &OperatorConfig{
		Controller: controllerDefaults,
	}
}

// GetControllerConfig returns the controller-specific configuration.
//
// Behavior:
//   - Acquires a read lock on m.mu.
//   - Returns a pointer to the Controller field of the current config snapshot.
//   - Releases the read lock.
//
// Returns:
//   - *ControllerConfig: pointer to the controller-specific configuration.
//
// Notes:
//   - Thread-safe; uses RWMutex for concurrent access.
//   - The returned pointer should be treated as read-only.
//   - Prefer GetConfig() for access to the full OperatorConfig when needed.
func (m *OperatorConfigManager) GetControllerConfig() *ControllerConfig {
	cfg := m.manager.CurrentConfig()
	if cfg == nil {
		return nil
	}
	clone := cfg.Controller.Clone()
	return &clone
}

// SetAPIReader configures a non-cached reader for situations where the cache
// is not yet running.
//
// Behavior:
//   - Stores the provided reader in m.apiReader.
//   - loadAndParseConfigMap will use this reader instead of the cached client.
//
// Arguments:
//   - reader client.Reader: a non-cached Kubernetes API reader.
//
// Side Effects:
//   - Mutates m.apiReader.
//
// Notes:
//   - Should be called before manager.Start() when loading initial config.
//   - The reader bypasses the controller-runtime cache for early startup scenarios.
func (m *OperatorConfigManager) SetAPIReader(reader client.Reader) {
	m.manager.SetAPIReader(reader)
}

// OperatorConfigManager manages the operator's dynamic configuration using the shared
// operatorconfig.Manager from the core module.
type OperatorConfigManager struct {
	manager         *operatorconfig.Manager[OperatorConfig]
	configNamespace string
	configName      string
}

// NewOperatorConfigManager creates a new configuration manager for the operator.
//
// Behavior:
//   - Creates a new OperatorConfigManager with the provided client and ConfigMap coordinates.
//   - Initializes defaultConfig and currentConfig with DefaultOperatorConfig().
//   - If a non-nil reader is supplied, configures it immediately so LoadInitial can
//     bypass the cache (which is not yet populated at startup). The variadic signature
//     keeps call-sites that do not need a reader (e.g. unit tests with fake clients)
//     backward-compatible.
//
// Arguments:
//   - k8sClient client.Client: the Kubernetes client for API interactions.
//   - namespace string: the namespace containing the operator ConfigMap.
//   - configMapName string: the name of the operator ConfigMap.
//   - reader client.Reader (optional): a non-cached API reader for startup loads.
//
// Returns:
//   - *OperatorConfigManager: a new manager ready for LoadInitial or SetupWithManager.
//
// Notes:
//   - The manager starts with default configuration; call LoadInitial to load from ConfigMap.
//   - Pass mgr.GetAPIReader() as the reader argument in production to avoid relying
//     on the unstarted cache during the initial config load.
func NewOperatorConfigManager(k8sClient client.Client, namespace, configMapName string, reader ...client.Reader) *OperatorConfigManager {
	shared := operatorconfig.NewManager(operatorconfig.Options[OperatorConfig]{
		Client:         k8sClient,
		Logger:         configManagerLog,
		ConfigMapKey:   types.NamespacedName{Name: configMapName, Namespace: namespace},
		ControllerName: "operator-config-manager",
		DefaultConfig:  DefaultOperatorConfig,
		ParseConfigMap: func(cm *corev1.ConfigMap) (*OperatorConfig, error) {
			return parseOperatorConfigMap(cm)
		},
		CloneConfig: func(cfg *OperatorConfig) *OperatorConfig {
			if cfg == nil {
				return nil
			}
			return cfg.Clone()
		},
		ApplyConfig: func(cfg *OperatorConfig) {
			ApplyRuntimeToggles(&cfg.Controller)
		},
		OnConfigApplied: func(reason operatorconfig.ReloadReason, cfg *OperatorConfig) {
			logConfigSummary(configManagerLog, cfg, ConfigReloadReason(reason))
		},
	})

	m := &OperatorConfigManager{
		manager:         shared,
		configNamespace: namespace,
		configName:      configMapName,
	}
	if len(reader) > 0 && reader[0] != nil {
		m.SetAPIReader(reader[0])
	}
	return m
}

// ConfigNamespace returns the namespace containing the operator ConfigMap.
func (m *OperatorConfigManager) ConfigNamespace() string {
	if m == nil {
		return ""
	}
	return m.configNamespace
}

// ConfigName returns the operator ConfigMap name.
func (m *OperatorConfigManager) ConfigName() string {
	if m == nil {
		return ""
	}
	return m.configName
}

// GetConfig returns the current operator configuration snapshot.
//
// Behavior:
//   - Acquires a read lock on m.mu.
//   - Returns the current configuration snapshot.
//   - Releases the read lock.
//
// Returns:
//   - *OperatorConfig: the current operator configuration.
//
// Notes:
//   - Thread-safe; uses RWMutex for concurrent access.
//   - The returned pointer should be treated as read-only.
func (m *OperatorConfigManager) GetConfig() *OperatorConfig {
	return m.manager.CurrentConfig()
}

// RefreshConfig resets the current configuration to defaults.
//
// Behavior:
//   - Acquires write lock on m.mu.
//   - Sets currentConfig back to defaultConfig.
//   - Does not trigger a reload from the ConfigMap.
//
// Side Effects:
//   - Mutates ocm.currentConfig under lock.
//
// Notes:
//   - This is a cache clear, not a reload. Use LoadInitial or wait for reconcile to reload.
//   - Thread-safe; uses RWMutex for concurrent access.
func (ocm *OperatorConfigManager) RefreshConfig() {
	ocm.manager.ResetToDefault()
}

// LoadInitial performs a one-time synchronous load of the operator configuration
// from the ConfigMap.
//
// Behavior:
//   - Logs the load attempt with ConfigMap coordinates.
//   - Calls loadAndParseConfigMap to fetch and parse the ConfigMap.
//   - On error, logs and returns the error.
//   - On success, acquires mutex, updates currentConfig and lastSyncTime.
//   - Applies runtime toggles via ApplyRuntimeToggles.
//   - Logs the loaded configuration summary.
//
// Arguments:
//   - ctx context.Context: propagated to the Kubernetes API GET.
//
// Returns:
//   - error: non-nil when loading or parsing fails.
//
// Side Effects:
//   - Reads from the Kubernetes API server.
//   - Mutates m.currentConfig and m.lastSyncTime under lock.
//   - Applies global runtime toggles (telemetry, logging, metrics).
//
// Notes:
//   - Should be called during startup before manager.Start().
//   - Use SetAPIReader first if the cache is not yet running.
func (m *OperatorConfigManager) LoadInitial(ctx context.Context) error {
	return m.manager.LoadInitial(ctx)
}

// logConfigSummary logs a structured summary of the loaded configuration.
// This provides observability into what configuration values are active.
func logConfigSummary(logger interface{ Info(string, ...any) }, config *OperatorConfig, reason ConfigReloadReason) {
	c := &config.Controller
	logger.Info("Operator configuration applied",
		"reason", reason,
		// Controller timing
		"reconcileTimeout", c.ReconcileTimeout,
		"cleanupInterval", c.CleanupInterval.Duration,
		"requeueBaseDelay", c.RequeueBaseDelay,
		"requeueMaxDelay", c.RequeueMaxDelay,
		// Job settings
		"ttlSecondsAfterFinished", c.TTLSecondsAfterFinished,
		"jobBackoffLimit", c.JobBackoffLimit,
		"serviceAccount", c.ServiceAccountName,
		// Per-controller concurrency
		"storyRunConcurrency", c.StoryRun.MaxConcurrentReconciles,
		"stepRunConcurrency", c.StepRun.MaxConcurrentReconciles,
		"storyConcurrency", c.Story.MaxConcurrentReconciles,
		// Toggles
		"telemetryEnabled", c.TelemetryEnabled,
		"verboseLogging", c.EnableVerboseLogging,
		"metricsEnabled", c.EnableMetrics,
	)
}

// Reconcile handles ConfigMap changes and updates the operator configuration.
// This is the event-driven approach that reacts to ConfigMap updates immediately.
//
// Behavior:
//   - Filters events to only process the configured ConfigMap.
//   - On ConfigMap update: loads and parses the new config, applies it.
//   - On ConfigMap delete: falls back to default configuration with a warning.
//   - On parse error: logs error, requeues with backoff.
//
// Notes:
//   - ConfigMap deletion is handled gracefully by falling back to defaults.
//   - This prevents the controller from requeuing forever on NotFound errors.
func (m *OperatorConfigManager) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return m.manager.Reconcile(ctx, req)
}

// SetupWithManager registers the operator config manager as a controller that
// watches the configured ConfigMap and reconciles on changes.
//
// Behavior:
//   - Builds a predicate that filters events to the configured ConfigMap name/namespace.
//   - Registers a controller named "operator-config-manager" watching corev1.ConfigMap.
//   - Completes with the OperatorConfigManager as the reconciler.
//
// Arguments:
//   - mgr ctrl.Manager: controller-runtime manager for controller registration.
//
// Returns:
//   - error: non-nil when controller registration fails.
//
// Side Effects:
//   - Registers the controller and ConfigMap watch with the manager.
//
// Notes:
//   - Does not load initial config; startup calls LoadInitial separately.
//   - GenericFunc returns false; periodic resync events are not processed.
func (m *OperatorConfigManager) SetupWithManager(mgr ctrl.Manager) error {
	return m.manager.SetupWithManager(mgr)
}

// parseOperatorConfigMap converts the ConfigMap data into an OperatorConfig snapshot.
func parseOperatorConfigMap(cm *corev1.ConfigMap) (*OperatorConfig, error) {
	config := &OperatorConfig{}

	// Initialize nested structs with defaults first (including operator-level overrides).
	defaults := DefaultOperatorConfig()
	config.Controller = defaults.Controller

	parseControllerTimings(cm, config)
	parseImageConfig(cm, config)
	parseResourceLimits(cm, config)
	parseRetryAndTimeouts(cm, config)
	if err := parseSecurityConfig(cm, config); err != nil {
		return nil, err
	}
	parseJobConfig(cm, config)
	parseTemplatingConfig(cm, config)
	parseReferenceConfig(cm, config)
	parseTelemetryConfig(cm, config)
	parseDebugConfig(cm, config)
	parseEngramDefaults(cm, config)
	parseStoryRunConfig(cm, config)
	parseStepRunConfig(cm, config)
	parseStoryConfig(cm, config)
	parseEngramConfig(cm, config)
	parseTransportConfig(cm, config)
	parseImpulseConfig(cm, config)
	parseTemplateConfig(cm, config)
	parseStorageDefaults(cm, config)

	if err := ValidateControllerConfig(&config.Controller); err != nil {
		return nil, fmt.Errorf("invalid operator configuration: %w", err)
	}
	return config, nil
}

// parseControllerTimings delegates to the timing-related setters for concurrency,
// requeue delays, cleanup intervals, and reconcile timeouts.
//
// Behavior:
//   - Calls setMaxConcurrentReconciles to override worker pool size.
//   - Calls setRequeueBaseDelay/setRequeueMaxDelay to tune exponential backoff.
//   - Calls setCleanupInterval to tune cleanup controller sweep frequency.
//   - Calls setReconcileTimeout to bound single reconcile loop duration.
//   - Calls setMaxStoryWithBlockSizeBytes to cap Story with-block size.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing timing keys.
//   - config *OperatorConfig: mutated in place with parsed timing values.
//
// Side Effects:
//   - Mutates config.Controller timing fields via sub-setters.
func parseControllerTimings(cm *corev1.ConfigMap, config *OperatorConfig) {
	setMaxConcurrentReconciles(cm, config)
	setRequeueBaseDelay(cm, config)
	setRequeueMaxDelay(cm, config)
	setCleanupInterval(cm, config)
	setReconcileTimeout(cm, config)
	setMaxStoryWithBlockSizeBytes(cm, config)
}

// setMaxConcurrentReconciles overrides the global Controller.MaxConcurrentReconciles when
// controller.max-concurrent-reconciles parses as a positive integer.
func setMaxConcurrentReconciles(cm *corev1.ConfigMap, config *OperatorConfig) {
	parseIntField(cm, contracts.KeyControllerMaxConcurrentReconciles, &config.Controller.MaxConcurrentReconciles, true)
}

// setRequeueBaseDelay overrides Controller.RequeueBaseDelay when
// controller.requeue-base-delay parses as a positive duration.
//
// Behavior:
//   - Reads "controller.requeue-base-delay" from cm.Data.
//   - Parses the value as a duration; ignores non-positive values.
//   - Stores the parsed value in config.Controller.RequeueBaseDelay.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - config *OperatorConfig: mutated in place with the parsed value.
//
// Side Effects:
//   - Mutates config.Controller.RequeueBaseDelay if key exists and is valid.
func setRequeueBaseDelay(cm *corev1.ConfigMap, config *OperatorConfig) {
	var parsed time.Duration
	if parseDurationField(cm, contracts.KeyRequeueBaseDelay, &parsed, true) {
		config.Controller.RequeueBaseDelay = parsed
	}
}

// setRequeueMaxDelay overrides Controller.RequeueMaxDelay when
// controller.requeue-max-delay parses as a positive duration.
//
// Behavior:
//   - Reads "controller.requeue-max-delay" from cm.Data.
//   - Parses the value as a duration; ignores non-positive values.
//   - Stores the parsed value in config.Controller.RequeueMaxDelay.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - config *OperatorConfig: mutated in place with the parsed value.
//
// Side Effects:
//   - Mutates config.Controller.RequeueMaxDelay if key exists and is valid.
func setRequeueMaxDelay(cm *corev1.ConfigMap, config *OperatorConfig) {
	var parsed time.Duration
	if parseDurationField(cm, contracts.KeyRequeueMaxDelay, &parsed, true) {
		config.Controller.RequeueMaxDelay = parsed
	}
}

// setCleanupInterval overrides Controller.CleanupInterval when
// controller.cleanup-interval parses as a positive duration.
//
// Behavior:
//   - Reads "controller.cleanup-interval" from cm.Data.
//   - Parses the value as a duration; ignores non-positive values.
//   - Wraps the parsed value in metav1.Duration and stores it.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - config *OperatorConfig: mutated in place with the parsed value.
//
// Side Effects:
//   - Mutates config.Controller.CleanupInterval if key exists and is valid.
func setCleanupInterval(cm *corev1.ConfigMap, config *OperatorConfig) {
	var parsed time.Duration
	if parseDurationField(cm, contracts.KeyCleanupInterval, &parsed, true) {
		config.Controller.CleanupInterval = metav1.Duration{Duration: parsed}
	}
}

// setReconcileTimeout overrides Controller.ReconcileTimeout when
// controller.reconcile-timeout parses as a non-negative duration (0 disables deadline).
//
// Behavior:
//   - Reads "controller.reconcile-timeout" from cm.Data.
//   - Parses the value as a duration; ignores negative values.
//   - Stores the parsed value in config.Controller.ReconcileTimeout.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - config *OperatorConfig: mutated in place with the parsed value.
//
// Side Effects:
//   - Mutates config.Controller.ReconcileTimeout if key exists and is valid.
func setReconcileTimeout(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data[contracts.KeyReconcileTimeout]; exists {
		val = strings.TrimSpace(val)
		parsed, err := time.ParseDuration(val)
		if err != nil {
			configParserLog.Info("Ignoring invalid value", "key", contracts.KeyReconcileTimeout, "value", val, "error", err.Error())
			return
		}
		if parsed < 0 {
			configParserLog.Info("Ignoring negative duration override", "key", contracts.KeyReconcileTimeout, "value", parsed)
			return
		}
		config.Controller.ReconcileTimeout = parsed
	}
}

// setMaxStoryWithBlockSizeBytes overrides Controller.MaxStoryWithBlockSizeBytes
// when controller.max-story-with-block-size-bytes parses as a positive integer.
//
// Behavior:
//   - Reads "controller.max-story-with-block-size-bytes" from cm.Data.
//   - Parses the value as an integer; ignores non-positive values.
//   - Stores the parsed value in config.Controller.MaxStoryWithBlockSizeBytes.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - config *OperatorConfig: mutated in place with the parsed value.
//
// Side Effects:
//   - Mutates config.Controller.MaxStoryWithBlockSizeBytes if key exists and is valid.
//
// Notes:
//   - Large values can increase memory usage during Story validation.
func setMaxStoryWithBlockSizeBytes(cm *corev1.ConfigMap, config *OperatorConfig) {
	parseIntField(cm, contracts.KeyMaxStoryWithBlockSizeBytes, &config.Controller.MaxStoryWithBlockSizeBytes, true)
}

// parseImageConfig reads image-related keys from the ConfigMap and applies them
// to the controller config for default container images and pull policies.
//
// Behavior:
//   - Reads "images.default-engram" and stores it in DefaultEngramImage.
//   - Reads "images.default-impulse" and stores it in DefaultImpulseImage.
//   - Reads "images.pull-policy" and maps to corev1.PullPolicy (Always/Never/IfNotPresent).
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing image keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller.DefaultEngramImage, DefaultImpulseImage, ImagePullPolicy.
//
// Notes:
//   - Invalid pull policy values are silently ignored.
func parseImageConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data[contracts.KeyDefaultEngramImage]; exists {
		config.Controller.DefaultEngramImage = val
	}
	if val, exists := cm.Data[contracts.KeyDefaultImpulseImage]; exists {
		config.Controller.DefaultImpulseImage = val
	}
	if val, exists := cm.Data[contracts.KeyImagePullPolicy]; exists {
		switch val {
		case contracts.PullPolicyAlways:
			config.Controller.ImagePullPolicy = corev1.PullAlways
		case contracts.PullPolicyNever:
			config.Controller.ImagePullPolicy = corev1.PullNever
		case contracts.PullPolicyIfNotPresent:
			config.Controller.ImagePullPolicy = corev1.PullIfNotPresent
		default:
			configParserLog.Info("ignoring invalid image pull policy", "value", val, "valid", []string{contracts.PullPolicyAlways, contracts.PullPolicyNever, contracts.PullPolicyIfNotPresent})
		}
	}
}

// parseResourceLimits copies resources.default/* and resources.engram/* entries
// from the operator ConfigMap into ControllerConfig.
//
// Behavior:
//   - Applies resources.default.cpu-request, cpu-limit, memory-request, memory-limit.
//   - Applies resources.engram.cpu-request, cpu-limit, memory-request, memory-limit.
//   - Each key is validated via applyQuantityOverride before storing.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing resource keys.
//   - config *OperatorConfig: mutated in place with resource quantity strings.
//
// Side Effects:
//   - Mutates config.Controller default and Engram resource fields.
//   - Logs applied overrides via configParserLog.
//
// Notes:
//   - Invalid quantity strings are logged and ignored.
//   - These values feed resolver defaults; resource.MustParse is used downstream.
func parseResourceLimits(cm *corev1.ConfigMap, config *OperatorConfig) {
	applyQuantityOverride(cm, contracts.KeyDefaultCPURequest, &config.Controller.DefaultCPURequest, "Controller.DefaultCPURequest")
	applyQuantityOverride(cm, contracts.KeyDefaultCPULimit, &config.Controller.DefaultCPULimit, "Controller.DefaultCPULimit")
	applyQuantityOverride(cm, contracts.KeyDefaultMemoryRequest, &config.Controller.DefaultMemoryRequest, "Controller.DefaultMemoryRequest")
	applyQuantityOverride(cm, contracts.KeyDefaultMemoryLimit, &config.Controller.DefaultMemoryLimit, "Controller.DefaultMemoryLimit")

	applyQuantityOverride(cm, contracts.KeyEngramCPURequest, &config.Controller.EngramCPURequest, "Controller.EngramCPURequest")
	applyQuantityOverride(cm, contracts.KeyEngramCPULimit, &config.Controller.EngramCPULimit, "Controller.EngramCPULimit")
	applyQuantityOverride(cm, contracts.KeyEngramMemoryRequest, &config.Controller.EngramMemoryRequest, "Controller.EngramMemoryRequest")
	applyQuantityOverride(cm, contracts.KeyEngramMemoryLimit, &config.Controller.EngramMemoryLimit, "Controller.EngramMemoryLimit")
}

// applyQuantityOverride validates and applies a resource.Quantity override from
// the ConfigMap to the target string pointer.
//
// Behavior:
//   - Reads the specified key from cm.Data.
//   - Validates the value can be parsed as a resource.Quantity.
//   - On success, stores the raw string in *target and logs the override.
//   - On failure, logs the error and returns without modifying target.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing resource keys.
//   - key string: the ConfigMap key to read (e.g., "resources.default.cpu-request").
//   - target *string: pointer to the field to update with the raw quantity string.
//   - field string: human-readable field name for logging.
//
// Side Effects:
//   - Mutates *target if key exists and is a valid quantity.
//   - Logs error or info message via configParserLog.
func applyQuantityOverride(cm *corev1.ConfigMap, key string, target *string, field string) {
	if val, exists := cm.Data[key]; exists {
		if _, err := resource.ParseQuantity(val); err != nil {
			configParserLog.Error(err, "Invalid resource quantity override", "key", key, "value", val)
			return
		}
		*target = val
		configParserLog.Info("Applied resource quantity override", "field", field, "key", key, "value", val)
	}
}

// parseRetryAndTimeouts fans out to the retry/timeouts setters for MaxRetries,
// exponential backoff bounds, and step/approval/external/conditional timeouts.
//
// Behavior:
//   - Calls setMaxRetries to override retry count.
//   - Calls setDefaultStepTimeout, setApprovalDefaultTimeout, setExternalDataTimeout,
//     setConditionalTimeout to override various timeout values.
//   - Logs a summary of applied retry/timeout overrides.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing retry/timeout keys.
//   - config *OperatorConfig: mutated in place with parsed values.
//
// Side Effects:
//   - Mutates config.Controller retry and timeout fields.
//   - Logs applied overrides via configParserLog.
func parseRetryAndTimeouts(cm *corev1.ConfigMap, config *OperatorConfig) {
	setMaxRetries(cm, config)
	setDefaultStepTimeout(cm, config)
	setApprovalDefaultTimeout(cm, config)
	setExternalDataTimeout(cm, config)
	setConditionalTimeout(cm, config)
	configParserLog.Info("Applied retry/timeouts overrides",
		contracts.KeyMaxRetries, config.Controller.MaxRetries,
		contracts.KeyDefaultStepTimeout, config.Controller.DefaultStepTimeout,
		contracts.KeyApprovalDefaultTimeout, config.Controller.ApprovalDefaultTimeout,
		contracts.KeyExternalDataTimeout, config.Controller.ExternalDataTimeout,
		contracts.KeyConditionalTimeout, config.Controller.ConditionalTimeout)
}

// setMaxRetries overrides ControllerConfig.MaxRetries when retry.max-retries
// parses as a non-negative integer.
//
// Behavior:
//   - Reads "retry.max-retries" from cm.Data.
//   - Parses the value as an integer; allows zero (disables retries).
//   - Clamps to maxRetriesUpperBound if the value exceeds the limit.
//   - Stores the parsed value in config.Controller.MaxRetries.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - config *OperatorConfig: mutated in place with the parsed value.
//
// Side Effects:
//   - Mutates config.Controller.MaxRetries if key exists and is valid.
//   - Logs applied override via configParserLog.
//
// Notes:
//   - Large values can drastically increase controller load.
func setMaxRetries(cm *corev1.ConfigMap, config *OperatorConfig) {
	var parsed int
	if parseIntField(cm, contracts.KeyMaxRetries, &parsed, false) {
		if parsed > maxRetriesUpperBound {
			configParserLog.Info("Clamping to upper bound", "key", contracts.KeyMaxRetries, "value", parsed, "max", maxRetriesUpperBound)
			parsed = maxRetriesUpperBound
		}
		config.Controller.MaxRetries = parsed
		configParserLog.Info("Applied override", "key", contracts.KeyMaxRetries, "value", parsed)
	}
}

// setDefaultStepTimeout overrides Controller.DefaultStepTimeout when
// timeout.default-step parses as a positive duration.
//
// Behavior:
//   - Reads "timeout.default-step" from cm.Data.
//   - Parses the value as a duration; ignores non-positive values.
//   - Stores the parsed value in config.Controller.DefaultStepTimeout.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - config *OperatorConfig: mutated in place with the parsed value.
//
// Side Effects:
//   - Mutates config.Controller.DefaultStepTimeout if key exists and is valid.
//   - Logs applied override via configParserLog.
//
// Notes:
//   - StepRun reconciles inherit this operator-level timeout.
func setDefaultStepTimeout(cm *corev1.ConfigMap, config *OperatorConfig) {
	var parsed time.Duration
	if parseDurationField(cm, contracts.KeyDefaultStepTimeout, &parsed, true) {
		config.Controller.DefaultStepTimeout = parsed
		configParserLog.Info("Applied override", "key", contracts.KeyDefaultStepTimeout, "value", parsed)
	}
}

// setApprovalDefaultTimeout stores timeout.approval-default when it parses as
// a positive duration.
//
// Behavior:
//   - Reads "timeout.approval-default" from cm.Data.
//   - Parses the value as a duration; ignores non-positive values.
//   - Stores the parsed value in config.Controller.ApprovalDefaultTimeout.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - config *OperatorConfig: mutated in place with the parsed value.
//
// Side Effects:
//   - Mutates config.Controller.ApprovalDefaultTimeout if key exists and is valid.
//   - Logs applied override via configParserLog.
//
// Notes:
//   - Approval workflows inherit this operator-wide timeout.
func setApprovalDefaultTimeout(cm *corev1.ConfigMap, config *OperatorConfig) {
	var parsed time.Duration
	if parseDurationField(cm, contracts.KeyApprovalDefaultTimeout, &parsed, true) {
		config.Controller.ApprovalDefaultTimeout = parsed
		configParserLog.Info("Applied override", "key", contracts.KeyApprovalDefaultTimeout, "value", parsed)
	}
}

// setExternalDataTimeout records timeout.external-data-default when it parses
// as a positive duration.
//
// Behavior:
//   - Reads "timeout.external-data-default" from cm.Data.
//   - Parses the value as a duration; ignores non-positive values.
//   - Stores the parsed value in config.Controller.ExternalDataTimeout.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - config *OperatorConfig: mutated in place with the parsed value.
//
// Side Effects:
//   - Mutates config.Controller.ExternalDataTimeout if key exists and is valid.
//   - Logs applied override via configParserLog.
//
// Notes:
//   - External data fetches share this operator-level deadline.
func setExternalDataTimeout(cm *corev1.ConfigMap, config *OperatorConfig) {
	var parsed time.Duration
	if parseDurationField(cm, contracts.KeyExternalDataTimeout, &parsed, true) {
		config.Controller.ExternalDataTimeout = parsed
		configParserLog.Info("Applied override", "key", contracts.KeyExternalDataTimeout, "value", parsed)
	}
}

// setConditionalTimeout loads timeout.conditional-default into
// Controller.ConditionalTimeout when the value parses as a positive duration.
//
// Behavior:
//   - Reads "timeout.conditional-default" from cm.Data.
//   - Parses the value as a duration; ignores non-positive values.
//   - Stores the parsed value in config.Controller.ConditionalTimeout.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - config *OperatorConfig: mutated in place with the parsed value.
//
// Side Effects:
//   - Mutates config.Controller.ConditionalTimeout if key exists and is valid.
//   - Logs applied override via configParserLog.
//
// Notes:
//   - Conditional execution paths share this operator-level limit.
func setConditionalTimeout(cm *corev1.ConfigMap, config *OperatorConfig) {
	var parsed time.Duration
	if parseDurationField(cm, contracts.KeyConditionalTimeout, &parsed, true) {
		config.Controller.ConditionalTimeout = parsed
		configParserLog.Info("Applied override", "key", contracts.KeyConditionalTimeout, "value", parsed)
	}
}

// parseSecurityConfig reads security.* keys from the ConfigMap and applies them
// to the controller config for pod/container security defaults.
//
// Behavior:
//   - Reads "security.run-as-non-root" and stores boolean in RunAsNonRoot.
//   - Reads "security.read-only-root-filesystem" and stores in ReadOnlyRootFilesystem.
//   - Reads "security.allow-privilege-escalation" and stores in AllowPrivilegeEscalation.
//   - Reads "security.run-as-user" and parses as int64 for RunAsUser.
//   - Reads "security.automount-service-account-token" for AutomountServiceAccountToken.
//   - Reads "security.service-account-name" for ServiceAccountName.
//   - Reads "security.drop-capabilities" and parses via parseDropCapabilities.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing security keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller security-related fields.
//
// Notes:
//   - Boolean keys expect "true" string for true, anything else is false.
func parseSecurityConfig(cm *corev1.ConfigMap, config *OperatorConfig) error {
	if val, exists := cm.Data[contracts.KeyRunAsNonRoot]; exists {
		config.Controller.RunAsNonRoot = val == strTrue
	}
	if val, exists := cm.Data[contracts.KeyReadOnlyRootFilesystem]; exists {
		config.Controller.ReadOnlyRootFilesystem = val == strTrue
	}
	if val, exists := cm.Data[contracts.KeyAllowPrivilegeEscalation]; exists {
		config.Controller.AllowPrivilegeEscalation = val == strTrue
	}
	if val, exists := cm.Data[contracts.KeyRunAsUser]; exists {
		if parsed, err := strconv.ParseInt(val, 10, 64); err == nil {
			if parsed <= 0 {
				return fmt.Errorf("security.run-as-user must be greater than 0")
			}
			config.Controller.RunAsUser = parsed
		} else {
			return fmt.Errorf("security.run-as-user must be a valid integer: %w", err)
		}
	}
	if val, exists := cm.Data[contracts.KeyAutomountServiceAccountToken]; exists {
		config.Controller.AutomountServiceAccountToken = val == strTrue
	}
	if val, exists := cm.Data[contracts.KeyServiceAccountName]; exists {
		config.Controller.ServiceAccountName = val
	}
	if val, exists := cm.Data[contracts.KeyDropCapabilities]; exists {
		if caps := parseDropCapabilities(val); len(caps) > 0 {
			config.Controller.DropCapabilities = caps
		}
	}
	configParserLog.Info("Applied security overrides",
		"runAsNonRoot", config.Controller.RunAsNonRoot,
		"readOnlyRootFilesystem", config.Controller.ReadOnlyRootFilesystem,
		"allowPrivilegeEscalation", config.Controller.AllowPrivilegeEscalation,
		"runAsUser", config.Controller.RunAsUser,
		"serviceAccountName", config.Controller.ServiceAccountName,
		"dropCapabilities", config.Controller.DropCapabilities,
	)
	return nil
}

// parseDropCapabilities splits a comma-separated capability string into a slice.
//
// Behavior:
//   - Splits the input on commas.
//   - Delegates to NormalizeCapabilities for trimming, uppercasing, and filtering.
//   - Returns ["ALL"] if no valid capabilities remain.
//
// Arguments:
//   - val string: comma-separated capability names (e.g., "NET_RAW,SYS_ADMIN").
//
// Returns:
//   - []string: slice of uppercase capability names, or ["ALL"] if empty.
//
// Notes:
//   - Uses shared NormalizeCapabilities helper for consistent normalization.
//   - Does not validate that capabilities are valid Linux capability names.
func parseDropCapabilities(val string) []string {
	parts := strings.Split(val, ",")
	result := NormalizeCapabilities(parts)
	if len(result) == 1 && result[0] == "ALL" && val != "" && val != "ALL" {
		configParserLog.Info("No valid capabilities found in input, defaulting to ALL", "input", val)
	}
	return result
}

// parseTransportConfig reads controller.transport.* keys from the ConfigMap and
// applies them to the controller config for gRPC transport settings.
//
// Behavior:
//   - Reads "controller.transport.grpc.enable-downstream-targets" for EnableDownstreamTargets.
//   - Reads "controller.transport.grpc.default-tls-secret" for DefaultTLSSecret.
//   - Reads "controller.transport.heartbeat-interval" for Transport.HeartbeatInterval.
//   - Reads "controller.transport.heartbeat-timeout" for TransportController.HeartbeatTimeout.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing transport keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller.Transport and TransportController fields.
//
// Notes:
//   - Duration values must be positive; non-positive values are ignored.
func parseTransportConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data[contracts.KeyTransportGRPCEnableDownstream]; exists {
		config.Controller.Transport.GRPC.EnableDownstreamTargets = val == strTrue
	}
	if val, exists := cm.Data[contracts.KeyTransportGRPCDefaultTLSSecret]; exists {
		config.Controller.Transport.GRPC.DefaultTLSSecret = strings.TrimSpace(val)
	}
	if val, exists := cm.Data[contracts.KeyTransportHeartbeatInterval]; exists {
		if parsed, err := time.ParseDuration(val); err == nil && parsed > 0 {
			config.Controller.Transport.HeartbeatInterval = parsed
		} else if err != nil {
			configParserLog.Info("Ignoring invalid value", "key", contracts.KeyTransportHeartbeatInterval, "value", val, "error", err.Error())
		}
	}
	if val, exists := cm.Data[contracts.KeyTransportHeartbeatTimeout]; exists {
		if parsed, err := time.ParseDuration(val); err == nil && parsed > 0 {
			config.Controller.TransportController.HeartbeatTimeout = parsed
		} else if err != nil {
			configParserLog.Info("Ignoring invalid value", "key", contracts.KeyTransportHeartbeatTimeout, "value", val, "error", err.Error())
		}
	}
	configParserLog.Info("Applied transport overrides",
		"enableDownstreamTargets", config.Controller.Transport.GRPC.EnableDownstreamTargets,
		"defaultTLSSecret", config.Controller.Transport.GRPC.DefaultTLSSecret,
		"heartbeatInterval", config.Controller.Transport.HeartbeatInterval,
		"heartbeatTimeout", config.Controller.TransportController.HeartbeatTimeout,
	)
}

// parseJobConfig reads job.* and storyrun.* keys from the ConfigMap and applies
// them to the controller config for Job and StoryRun settings.
//
// Behavior:
//   - Reads "job.backoff-limit" and stores as int32 in JobBackoffLimit.
//   - Reads "job.ttl-seconds-after-finished" for TTLSecondsAfterFinished.
//   - Reads "streaming.ttl-seconds-after-finished" for StreamingTTLSecondsAfterFinished.
//   - Reads "storyrun.retention-seconds" for StoryRunRetentionSeconds.
//   - Reads "job.restart-policy" and maps to corev1.RestartPolicy.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing job keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller Job-related fields.
//
// Notes:
//   - Invalid restart policy values are silently ignored.
func parseJobConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	// Use shared parseInt32Field helper for consistent parsing and logging
	parseInt32Field(cm, contracts.KeyJobBackoffLimit, &config.Controller.JobBackoffLimit, true)
	parseInt32Field(cm, contracts.KeyJobTTLSecondsAfterFinished, &config.Controller.TTLSecondsAfterFinished, false)
	parseInt32Field(cm, contracts.KeyStreamingTTLSecondsAfterFinished, &config.Controller.StreamingTTLSecondsAfterFinished, false)
	parseInt32Field(cm, contracts.KeyStoryRunRetentionSeconds, &config.Controller.StoryRunRetentionSeconds, false)

	if val, exists := cm.Data[contracts.KeyJobRestartPolicy]; exists {
		switch val {
		case contracts.RestartPolicyAlways:
			config.Controller.JobRestartPolicy = corev1.RestartPolicyAlways
		case contracts.RestartPolicyOnFailure:
			config.Controller.JobRestartPolicy = corev1.RestartPolicyOnFailure
		case contracts.RestartPolicyNever:
			config.Controller.JobRestartPolicy = corev1.RestartPolicyNever
		default:
			configParserLog.Info("Ignoring invalid value", "key", contracts.KeyJobRestartPolicy, "value", val, "valid", []string{contracts.RestartPolicyAlways, contracts.RestartPolicyOnFailure, contracts.RestartPolicyNever})
		}
	}
	configParserLog.Info("Applied job overrides",
		"backoffLimit", config.Controller.JobBackoffLimit,
		"ttlSecondsAfterFinished", config.Controller.TTLSecondsAfterFinished,
		"streamingTTLSecondsAfterFinished", config.Controller.StreamingTTLSecondsAfterFinished,
		"storyRunRetentionSeconds", config.Controller.StoryRunRetentionSeconds,
		"restartPolicy", config.Controller.JobRestartPolicy,
	)
}

// parseTemplatingConfig reads templating.* keys from the ConfigMap and applies them to the
// controller config for template evaluation settings.
//
// Behavior:
//   - Reads "templating.evaluation-timeout" and stores as duration in TemplateEvaluationTimeout.
//   - Reads "templating.max-expression-length" and stores as int in TemplateMaxExpressionLength.
//   - Reads "templating.max-output-bytes" and stores as int in TemplateMaxOutputBytes.
//   - Reads "templating.deterministic" and stores boolean in TemplateDeterministic.
//   - Reads "templating.offloaded-data-policy" and stores as string in TemplateOffloadedPolicy.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing templating keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller templating-related fields.
//
// Notes:
//   - TemplateMaxExpressionLength must be positive; non-positive values are ignored.
func parseTemplatingConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data[contracts.KeyTemplatingEvaluationTimeout]; exists {
		if parsed, err := time.ParseDuration(val); err == nil {
			config.Controller.TemplateEvaluationTimeout = parsed
		} else {
			configParserLog.Info("Ignoring invalid value", "key", contracts.KeyTemplatingEvaluationTimeout, "value", val, "error", err.Error())
		}
	}
	if val, exists := cm.Data[contracts.KeyTemplatingMaxExpressionLength]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed > 0 {
			config.Controller.TemplateMaxExpressionLength = parsed
		} else if err != nil {
			configParserLog.Info("Ignoring invalid value", "key", contracts.KeyTemplatingMaxExpressionLength, "value", val, "error", err.Error())
		}
	}
	if val, exists := cm.Data[contracts.KeyTemplatingMaxOutputBytes]; exists {
		if parsed, err := strconv.Atoi(val); err == nil && parsed >= 0 {
			config.Controller.TemplateMaxOutputBytes = parsed
		} else if err != nil {
			configParserLog.Info("Ignoring invalid value", "key", contracts.KeyTemplatingMaxOutputBytes, "value", val, "error", err.Error())
		}
	}
	if val, exists := cm.Data[contracts.KeyTemplatingDeterministic]; exists {
		config.Controller.TemplateDeterministic = val == strTrue
	}
	if val, exists := cm.Data[contracts.KeyTemplatingOffloadedPolicy]; exists {
		config.Controller.TemplateOffloadedPolicy = strings.TrimSpace(val)
	}
	if val, exists := cm.Data[contracts.KeyTemplatingMaterializeEngram]; exists {
		config.Controller.TemplateMaterializeEngram = strings.TrimSpace(val)
	}
	configParserLog.Info("Applied templating overrides",
		"evaluationTimeout", config.Controller.TemplateEvaluationTimeout,
		"maxExpressionLength", config.Controller.TemplateMaxExpressionLength,
		"maxOutputBytes", config.Controller.TemplateMaxOutputBytes,
		"deterministic", config.Controller.TemplateDeterministic,
		"offloadedPolicy", config.Controller.TemplateOffloadedPolicy,
		"materializeEngram", config.Controller.TemplateMaterializeEngram,
	)
}

// parseReferenceConfig reads references.* keys from the ConfigMap and applies them to the
// controller config for cross-namespace reference behavior.
func parseReferenceConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data[contracts.KeyReferencesCrossNamespacePolicy]; exists {
		config.Controller.ReferenceCrossNamespacePolicy = strings.TrimSpace(val)
	}
	configParserLog.Info("Applied reference overrides",
		"crossNamespacePolicy", config.Controller.ReferenceCrossNamespacePolicy,
	)
}

// parseTelemetryConfig reads telemetry.* keys from the ConfigMap and applies
// them to the controller config for OpenTelemetry settings.
//
// Behavior:
//   - Reads "telemetry.enabled" and stores boolean in TelemetryEnabled.
//   - Reads "telemetry.trace-propagation" and stores in TracePropagationEnabled.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing telemetry keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller.TelemetryEnabled, TracePropagationEnabled.
//   - Changes are applied at runtime via ApplyRuntimeToggles.
func parseTelemetryConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data[contracts.KeyTelemetryEnabled]; exists {
		config.Controller.TelemetryEnabled = val == strTrue
	}
	if val, exists := cm.Data[contracts.KeyTracePropagation]; exists {
		config.Controller.TracePropagationEnabled = val == strTrue
	}
}

// parseDebugConfig reads debug.* keys from the ConfigMap and applies them to
// the controller config for debugging and observability settings.
//
// Behavior:
//   - Reads "debug.enable-verbose-logging" and stores in EnableVerboseLogging.
//   - Reads "debug.enable-step-output-logging" and stores in EnableStepOutputLogging.
//   - Reads "debug.enable-metrics" and stores in EnableMetrics.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing debug keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller debug-related fields.
//   - Changes are applied at runtime via ApplyRuntimeToggles.
func parseDebugConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data[contracts.KeyVerboseLogging]; exists {
		config.Controller.EnableVerboseLogging = val == strTrue
	}
	if val, exists := cm.Data[contracts.KeyStepOutputLogging]; exists {
		config.Controller.EnableStepOutputLogging = val == strTrue
	}
	if val, exists := cm.Data[contracts.KeyEnableMetrics]; exists {
		config.Controller.EnableMetrics = val == strTrue
	}
}

// parseEngramDefaults reads engram.* keys from the ConfigMap and applies them
// to the controller config for Engram-specific defaults.
//
// Behavior:
//   - Reads "engram.default-max-inline-size" and stores in DefaultMaxInlineSize.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing engram keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize.
//
// Notes:
//   - Value must be positive; non-positive values are ignored.
func parseEngramDefaults(cm *corev1.ConfigMap, config *OperatorConfig) {
	// Use shared parseIntField helper for consistent parsing and logging
	if parseIntField(cm, contracts.KeyEngramDefaultInlineSize, &config.Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize, true) {
		configParserLog.Info("Applied override", "key", contracts.KeyEngramDefaultInlineSize, "value", config.Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize)
	}
}

// parseStoryRunConfig reads storyrun.* keys from the ConfigMap and applies them
// to the controller config for StoryRun controller settings.
//
// Behavior:
//   - Reads "storyrun.max-concurrent-reconciles" for MaxConcurrentReconciles.
//   - Reads "storyrun.rate-limiter.base-delay" for RateLimiter.BaseDelay.
//   - Reads "storyrun.rate-limiter.max-delay" for RateLimiter.MaxDelay.
//   - Reads "storyrun.max-inline-inputs-size" for MaxInlineInputsSize.
//   - Reads "storyrun.binding.max-mutations-per-reconcile" for Binding settings.
//   - Reads "storyrun.binding.throttle-requeue-delay" for Binding throttling.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing storyrun keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller.StoryRun fields.
func parseStoryRunConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	// Use shared helpers for consistent parsing and logging
	parseIntField(cm, contracts.KeyStoryRunMaxConcurrentReconciles, &config.Controller.StoryRun.MaxConcurrentReconciles, false)
	parseRateLimiterConfig(cm, contracts.KeyStoryRunRateLimiterBaseDelay, contracts.KeyStoryRunRateLimiterMaxDelay, &config.Controller.StoryRun.RateLimiter)
	parseIntField(cm, contracts.KeyStoryRunMaxInlineInputsSize, &config.Controller.StoryRun.MaxInlineInputsSize, true)
	parseIntField(cm, contracts.KeyStoryRunBindingMaxMutations, &config.Controller.StoryRun.Binding.MaxMutationsPerReconcile, false)
	parseDurationField(cm, contracts.KeyStoryRunBindingThrottleRequeueDelay, &config.Controller.StoryRun.Binding.ThrottleRequeueDelay, true)
	parseStoryRunSchedulingConfig(cm, config)
	configParserLog.Info("Applied StoryRun overrides",
		"maxConcurrentReconciles", config.Controller.StoryRun.MaxConcurrentReconciles,
		"rateLimiterBaseDelay", config.Controller.StoryRun.RateLimiter.BaseDelay,
		"rateLimiterMaxDelay", config.Controller.StoryRun.RateLimiter.MaxDelay,
		"maxInlineInputsSize", config.Controller.StoryRun.MaxInlineInputsSize,
		"bindingMaxMutations", config.Controller.StoryRun.Binding.MaxMutationsPerReconcile,
		"bindingThrottleDelay", config.Controller.StoryRun.Binding.ThrottleRequeueDelay,
		"globalConcurrency", config.Controller.StoryRun.Scheduling.GlobalConcurrency,
		"queueCount", len(config.Controller.StoryRun.Scheduling.Queues),
	)
}

func parseStoryRunSchedulingConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	if cm == nil || config == nil {
		return
	}
	if config.Controller.StoryRun.Scheduling.Queues == nil {
		config.Controller.StoryRun.Scheduling.Queues = map[string]QueueConfig{}
	}

	parseInt32Field(cm, contracts.KeyStoryRunGlobalConcurrency, &config.Controller.StoryRun.Scheduling.GlobalConcurrency, false)

	for key, val := range cm.Data {
		if !strings.HasPrefix(key, contracts.KeyStoryRunQueuePrefix) {
			continue
		}
		rest := strings.TrimPrefix(key, contracts.KeyStoryRunQueuePrefix)
		parts := strings.SplitN(rest, ".", 2)
		if len(parts) != 2 {
			configParserLog.Info("Ignoring queue override with invalid key", "key", key)
			continue
		}
		queueName := strings.ToLower(strings.TrimSpace(parts[0]))
		field := strings.TrimSpace(parts[1])
		if queueName == "" || field == "" {
			configParserLog.Info("Ignoring queue override with empty name or field", "key", key)
			continue
		}

		entry := config.Controller.StoryRun.Scheduling.Queues[queueName]
		switch field {
		case contracts.KeyStoryRunQueueConcurrencySuffix:
			if parseInt32Value(key, val, &entry.Concurrency, false) {
				config.Controller.StoryRun.Scheduling.Queues[queueName] = entry
			}
		case contracts.KeyStoryRunQueueDefaultPrioritySuffix:
			if parseInt32Value(key, val, &entry.DefaultPriority, false) {
				config.Controller.StoryRun.Scheduling.Queues[queueName] = entry
			}
		case contracts.KeyStoryRunQueuePriorityAgingSuffix:
			if parseInt32Value(key, val, &entry.PriorityAgingSeconds, false) {
				config.Controller.StoryRun.Scheduling.Queues[queueName] = entry
			}
		default:
			configParserLog.Info("Ignoring unknown queue override field", "key", key)
		}
	}
}

// parseStepRunConfig reads steprun.* keys from the ConfigMap and applies them
// to the controller config for StepRun controller settings.
//
// Behavior:
//   - Reads "steprun.max-concurrent-reconciles" for MaxConcurrentReconciles.
//   - Reads "steprun.rate-limiter.base-delay" for RateLimiter.BaseDelay.
//   - Reads "steprun.rate-limiter.max-delay" for RateLimiter.MaxDelay.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing steprun keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller.StepRun fields.
func parseStepRunConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	// Use shared helpers for consistent parsing and logging
	parseIntField(cm, contracts.KeyStepRunMaxConcurrentReconciles, &config.Controller.StepRun.MaxConcurrentReconciles, false)
	parseRateLimiterConfig(cm, contracts.KeyStepRunRateLimiterBaseDelay, contracts.KeyStepRunRateLimiterMaxDelay, &config.Controller.StepRun.RateLimiter)
	configParserLog.Info("Applied StepRun overrides",
		"maxConcurrentReconciles", config.Controller.StepRun.MaxConcurrentReconciles,
		"rateLimiterBaseDelay", config.Controller.StepRun.RateLimiter.BaseDelay,
		"rateLimiterMaxDelay", config.Controller.StepRun.RateLimiter.MaxDelay,
	)
}

// parseStoryConfig reads story.* keys from the ConfigMap and applies them
// to the controller config for Story controller settings.
//
// Behavior:
//   - Reads "story.max-concurrent-reconciles" for MaxConcurrentReconciles.
//   - Reads "story.rate-limiter.base-delay" for RateLimiter.BaseDelay.
//   - Reads "story.rate-limiter.max-delay" for RateLimiter.MaxDelay.
//   - Reads "story.binding.max-mutations-per-reconcile" for Binding settings.
//   - Reads "story.binding.throttle-requeue-delay" for Binding throttling.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing story keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller.Story fields.
func parseStoryConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	// Use shared helpers for consistent parsing and logging
	parseIntField(cm, contracts.KeyStoryMaxConcurrentReconciles, &config.Controller.Story.MaxConcurrentReconciles, false)
	parseRateLimiterConfig(cm, contracts.KeyStoryRateLimiterBaseDelay, contracts.KeyStoryRateLimiterMaxDelay, &config.Controller.Story.RateLimiter)
	parseIntField(cm, contracts.KeyStoryBindingMaxMutations, &config.Controller.Story.Binding.MaxMutationsPerReconcile, false)
	parseDurationField(cm, contracts.KeyStoryBindingThrottleRequeueDelay, &config.Controller.Story.Binding.ThrottleRequeueDelay, true)
	configParserLog.Info("Applied Story overrides",
		"maxConcurrentReconciles", config.Controller.Story.MaxConcurrentReconciles,
		"rateLimiterBaseDelay", config.Controller.Story.RateLimiter.BaseDelay,
		"rateLimiterMaxDelay", config.Controller.Story.RateLimiter.MaxDelay,
		"bindingMaxMutations", config.Controller.Story.Binding.MaxMutationsPerReconcile,
		"bindingThrottleDelay", config.Controller.Story.Binding.ThrottleRequeueDelay,
	)
}

// parseEngramConfig reads engram.* keys from the ConfigMap and applies them
// to the controller config for Engram controller settings.
//
// Behavior:
//   - Reads "engram.max-concurrent-reconciles" for MaxConcurrentReconciles.
//   - Reads "engram.rate-limiter.base-delay" for RateLimiter.BaseDelay.
//   - Reads "engram.rate-limiter.max-delay" for RateLimiter.MaxDelay.
//   - Delegates to parseEngramControllerConfig for Engram-specific settings.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing engram keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller.Engram fields.
func parseEngramConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	// Use shared helpers for consistent parsing and logging
	parseIntField(cm, contracts.KeyEngramMaxConcurrentReconciles, &config.Controller.Engram.MaxConcurrentReconciles, false)
	parseRateLimiterConfig(cm, contracts.KeyEngramRateLimiterBaseDelay, contracts.KeyEngramRateLimiterMaxDelay, &config.Controller.Engram.RateLimiter)

	// Engram-specific controller config
	parseEngramControllerConfig(cm, &config.Controller.Engram.EngramControllerConfig)

	configParserLog.Info("Applied Engram overrides",
		"maxConcurrentReconciles", config.Controller.Engram.MaxConcurrentReconciles,
		"rateLimiterBaseDelay", config.Controller.Engram.RateLimiter.BaseDelay,
		"rateLimiterMaxDelay", config.Controller.Engram.RateLimiter.MaxDelay,
		"defaultMaxInlineSize", config.Controller.Engram.EngramControllerConfig.DefaultMaxInlineSize,
	)
}

// parseEngramControllerConfig reads Engram-specific config keys from the ConfigMap
// and applies them to the EngramControllerConfig struct.
//
// Behavior:
//   - Reads "engram.default-grpc-port" for DefaultGRPCPort.
//   - Reads "engram.default-grpc-heartbeat-interval-seconds" for heartbeat timing.
//   - Reads "engram.default-storage-timeout-seconds" for storage operations.
//   - Reads "engram.default-graceful-shutdown-timeout-seconds" for shutdown.
//   - Reads "engram.default-termination-grace-period-seconds" for pod termination.
//   - Reads "engram.default-max-recv-msg-bytes" / "engram.default-max-send-msg-bytes".
//   - Reads "engram.default-dial-timeout-seconds" for gRPC dial timeout.
//   - Reads "engram.default-channel-buffer-size" for channel buffers.
//   - Reads "engram.default-reconnect-*" for reconnection policies.
//   - Reads "engram.default-hang-timeout-seconds" / "engram.default-message-timeout-seconds".
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing engram keys.
//   - cfg *EngramControllerConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates cfg fields for gRPC, storage, and reconnection settings.
func parseEngramControllerConfig(cm *corev1.ConfigMap, cfg *EngramControllerConfig) {
	parseIntField(cm, contracts.KeyEngramDefaultGRPCPort, &cfg.DefaultGRPCPort, true)
	parseIntField(cm, contracts.KeyEngramDefaultHeartbeatIntervalSeconds, &cfg.DefaultGRPCHeartbeatIntervalSeconds, true)
	parseIntField(cm, contracts.KeyEngramDefaultStorageTimeoutSeconds, &cfg.DefaultStorageTimeoutSeconds, true)
	parseIntField(cm, contracts.KeyEngramDefaultGracefulShutdownSeconds, &cfg.DefaultGracefulShutdownTimeoutSeconds, true)
	parseIntField(cm, contracts.KeyEngramDefaultTerminationGraceSeconds, &cfg.DefaultTerminationGracePeriodSeconds, true)
	parseIntField(cm, contracts.KeyEngramDefaultMaxRecvMsgBytes, &cfg.DefaultMaxRecvMsgBytes, true)
	parseIntField(cm, contracts.KeyEngramDefaultMaxSendMsgBytes, &cfg.DefaultMaxSendMsgBytes, true)
	parseIntField(cm, contracts.KeyEngramDefaultDialTimeoutSeconds, &cfg.DefaultDialTimeoutSeconds, true)
	parseIntField(cm, contracts.KeyEngramDefaultChannelBufferSize, &cfg.DefaultChannelBufferSize, true)
	parseIntField(cm, contracts.KeyEngramDefaultReconnectMaxRetries, &cfg.DefaultReconnectMaxRetries, false) // Can be 0
	parseIntField(cm, contracts.KeyEngramDefaultReconnectBaseBackoffMillis, &cfg.DefaultReconnectBaseBackoffMillis, true)
	parseIntField(cm, contracts.KeyEngramDefaultReconnectMaxBackoffSeconds, &cfg.DefaultReconnectMaxBackoffSeconds, true)
	parseIntField(cm, contracts.KeyEngramDefaultHangTimeoutSeconds, &cfg.DefaultHangTimeoutSeconds, true)
	parseIntField(cm, contracts.KeyEngramDefaultMessageTimeoutSeconds, &cfg.DefaultMessageTimeoutSeconds, true)

	configParserLog.Info("Applied Engram controller defaults",
		"grpcPort", cfg.DefaultGRPCPort,
		"heartbeatInterval", cfg.DefaultGRPCHeartbeatIntervalSeconds,
		"storageTimeout", cfg.DefaultStorageTimeoutSeconds,
		"gracefulShutdownTimeout", cfg.DefaultGracefulShutdownTimeoutSeconds,
		"terminationGracePeriod", cfg.DefaultTerminationGracePeriodSeconds,
		"dialTimeout", cfg.DefaultDialTimeoutSeconds,
		"reconnectMaxRetries", cfg.DefaultReconnectMaxRetries,
	)
}

// parseIntField reads an integer key from the ConfigMap and stores it in the
// target pointer.
//
// Behavior:
//   - Reads the specified key from cm.Data.
//   - Parses the value as an integer using strconv.Atoi.
//   - If requirePositive is true, ignores values <= 0.
//   - If requirePositive is false, ignores values < 0.
//   - On success, stores the parsed value in *target.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - key string: the ConfigMap key to read.
//   - target *int: pointer to the field to update with the parsed value.
//   - requirePositive bool: if true, only accept values > 0.
//
// Returns:
//   - bool: true if key exists and was successfully parsed and stored.
//
// Side Effects:
//   - Mutates *target if key exists and is valid.
//   - Logs error or info message via configParserLog on failure.
func parseIntField(cm *corev1.ConfigMap, key string, target *int, requirePositive bool) bool {
	if val, exists := cm.Data[key]; exists {
		val = strings.TrimSpace(val)
		parsed, err := strconv.Atoi(val)
		if err != nil {
			configParserLog.Error(err, "Invalid integer override", "key", key, "value", val)
			return false
		}
		if requirePositive && parsed <= 0 {
			configParserLog.Info("Ignoring non-positive integer override", "key", key, "value", parsed)
			return false
		}
		if !requirePositive && parsed < 0 {
			configParserLog.Info("Ignoring negative integer override", "key", key, "value", parsed)
			return false
		}
		*target = parsed
		return true
	}
	return false
}

// parseInt32Field reads an int32 key from the ConfigMap and stores it in target.
//
// Behavior:
//   - Reads the specified key from cm.Data.
//   - Parses the value as int32 using strconv.ParseInt.
//   - If requirePositive is true, ignores values <= 0.
//   - On success, stores the parsed value in *target and returns true.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - key string: the ConfigMap key to read.
//   - target *int32: pointer to store the parsed value.
//   - requirePositive bool: if true, only accepts values > 0.
//
// Side Effects:
//   - Mutates *target if key exists and is valid.
//   - Logs error or info message via configParserLog on failure.
func parseInt32Field(cm *corev1.ConfigMap, key string, target *int32, requirePositive bool) {
	if val, exists := cm.Data[key]; exists {
		val = strings.TrimSpace(val)
		parsed, err := strconv.ParseInt(val, 10, 32)
		if err != nil {
			configParserLog.Error(err, "Invalid int32 override", "key", key, "value", val)
			return
		}
		if requirePositive && parsed <= 0 {
			configParserLog.Info("Ignoring non-positive int32 override", "key", key, "value", parsed)
			return
		}
		if !requirePositive && parsed < 0 {
			configParserLog.Info("Ignoring negative int32 override", "key", key, "value", parsed)
			return
		}
		*target = int32(parsed)
	}
}

// parseInt32Value parses a raw string into int32 with optional positivity requirements.
func parseInt32Value(key, val string, target *int32, requirePositive bool) bool {
	trimmed := strings.TrimSpace(val)
	parsed, err := strconv.ParseInt(trimmed, 10, 32)
	if err != nil {
		configParserLog.Error(err, "Invalid int32 override", "key", key, "value", trimmed)
		return false
	}
	if requirePositive && parsed <= 0 {
		configParserLog.Info("Ignoring non-positive int32 override", "key", key, "value", parsed)
		return false
	}
	if !requirePositive && parsed < 0 {
		configParserLog.Info("Ignoring negative int32 override", "key", key, "value", parsed)
		return false
	}
	*target = int32(parsed)
	return true
}

// parseRateLimiterConfig parses rate limiter settings from the ConfigMap.
//
// Behavior:
//   - Reads "<prefix>.rate-limiter.base-delay" for BaseDelay.
//   - Reads "<prefix>.rate-limiter.max-delay" for MaxDelay.
//   - Uses parseDurationField for consistent error handling.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the keys.
//   - keyBaseDelay string: the key for base delay.
//   - keyMaxDelay string: the key for max delay.
//   - rl *RateLimiterConfig: pointer to store the parsed values.
//
// Side Effects:
//   - Mutates rl.BaseDelay and rl.MaxDelay if keys exist and are valid.
//   - Logs via parseDurationField helper.
func parseRateLimiterConfig(cm *corev1.ConfigMap, keyBaseDelay, keyMaxDelay string, rl *RateLimiterConfig) {
	parseDurationField(cm, keyBaseDelay, &rl.BaseDelay, false)
	parseDurationField(cm, keyMaxDelay, &rl.MaxDelay, false)
}

// applyControllerRateLimiterOverrides parses a controller's concurrency and
// rate-limiter overrides, logging the applied values for observability.
func applyControllerRateLimiterOverrides(
	cm *corev1.ConfigMap,
	concurrencyKey, baseDelayKey, maxDelayKey, controllerName string,
	concurrencyTarget *int,
	rl *RateLimiterConfig,
) {
	parseIntField(cm, concurrencyKey, concurrencyTarget, false)
	parseRateLimiterConfig(cm, baseDelayKey, maxDelayKey, rl)
	configParserLog.Info("Applied controller overrides",
		"controller", controllerName,
		"maxConcurrentReconciles", *concurrencyTarget,
		"rateLimiterBaseDelay", rl.BaseDelay,
		"rateLimiterMaxDelay", rl.MaxDelay,
	)
}

// parseDurationField reads a duration key from the ConfigMap and stores it in
// the target pointer.
//
// Behavior:
//   - Reads the specified key from cm.Data.
//   - Parses the value as a duration using time.ParseDuration.
//   - If requirePositive is true, ignores values <= 0.
//   - On success, stores the parsed value in *target.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing the key.
//   - key string: the ConfigMap key to read (e.g., "controller.requeue-base-delay").
//   - target *time.Duration: pointer to the field to update with the parsed value.
//   - requirePositive bool: if true, only accept durations > 0.
//
// Returns:
//   - bool: true if key exists and was successfully parsed and stored.
//
// Side Effects:
//   - Mutates *target if key exists and is valid.
//   - Logs error or info message via configParserLog on failure.
func parseDurationField(cm *corev1.ConfigMap, key string, target *time.Duration, requirePositive bool) bool {
	if val, exists := cm.Data[key]; exists {
		val = strings.TrimSpace(val)
		parsed, err := time.ParseDuration(val)
		if err != nil {
			configParserLog.Error(err, "Invalid duration override", "key", key, "value", val)
			return false
		}
		if requirePositive && parsed <= 0 {
			configParserLog.Info("Ignoring non-positive duration override", "key", key, "value", parsed)
			return false
		}
		*target = parsed
		return true
	}
	return false
}

// parseImpulseConfig reads impulse.* keys from the ConfigMap and applies them
// to the controller config for Impulse controller settings.
//
// Behavior:
//   - Reads "impulse.max-concurrent-reconciles" for MaxConcurrentReconciles.
//   - Reads "impulse.rate-limiter.base-delay" for RateLimiter.BaseDelay.
//   - Reads "impulse.rate-limiter.max-delay" for RateLimiter.MaxDelay.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing impulse keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller.Impulse fields.
func parseImpulseConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	applyControllerRateLimiterOverrides(
		cm,
		contracts.KeyImpulseMaxConcurrentReconciles,
		contracts.KeyImpulseRateLimiterBaseDelay,
		contracts.KeyImpulseRateLimiterMaxDelay,
		"Impulse",
		&config.Controller.Impulse.MaxConcurrentReconciles,
		&config.Controller.Impulse.RateLimiter,
	)
}

// parseTemplateConfig reads template.* keys from the ConfigMap and applies them
// to the controller config for Template controller settings.
//
// Behavior:
//   - Reads "template.max-concurrent-reconciles" for MaxConcurrentReconciles.
//   - Reads "template.rate-limiter.base-delay" for RateLimiter.BaseDelay.
//   - Reads "template.rate-limiter.max-delay" for RateLimiter.MaxDelay.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing template keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller.Template fields.
func parseTemplateConfig(cm *corev1.ConfigMap, config *OperatorConfig) {
	applyControllerRateLimiterOverrides(
		cm,
		contracts.KeyTemplateMaxConcurrentReconciles,
		contracts.KeyTemplateRateLimiterBaseDelay,
		contracts.KeyTemplateRateLimiterMaxDelay,
		"Template",
		&config.Controller.Template.MaxConcurrentReconciles,
		&config.Controller.Template.RateLimiter,
	)
}

// parseStorageDefaults reads controller.storage.* keys from the ConfigMap and
// applies them to the controller config for default storage settings.
//
// Behavior:
//   - Reads "controller.storage.provider" for DefaultStorageProvider (e.g., "s3").
//   - Reads "controller.storage.s3.bucket" for DefaultS3Bucket.
//   - Reads "controller.storage.s3.region" for DefaultS3Region.
//   - Reads "controller.storage.s3.endpoint" for DefaultS3Endpoint.
//   - Reads "controller.storage.s3.use-path-style" for DefaultS3UsePathStyle.
//   - Reads "controller.storage.s3.auth-secret-name" for DefaultS3AuthSecretName.
//
// Arguments:
//   - cm *corev1.ConfigMap: the operator ConfigMap containing storage keys.
//   - config *OperatorConfig: mutated in place with the parsed values.
//
// Side Effects:
//   - Mutates config.Controller storage-related fields.
//
// Notes:
//   - These defaults are used when Story/Engram do not specify storage policies.
func parseStorageDefaults(cm *corev1.ConfigMap, config *OperatorConfig) {
	if val, exists := cm.Data[contracts.KeyStorageProvider]; exists {
		provider := strings.TrimSpace(val)
		switch provider {
		case "":
			config.Controller.DefaultStorageProvider = ""
			configParserLog.Info("Disabled default storage provider override", "key", contracts.KeyStorageProvider)
		case contracts.StorageProviderS3, contracts.StorageProviderFile:
			config.Controller.DefaultStorageProvider = provider
		default:
			configParserLog.Info("Ignoring unsupported storage provider override",
				"key", contracts.KeyStorageProvider,
				"value", provider,
				"supported", []string{contracts.StorageProviderS3, contracts.StorageProviderFile})
		}
	}
	if val, exists := cm.Data[contracts.KeyStorageS3Bucket]; exists {
		config.Controller.DefaultS3Bucket = val
	}
	if val, exists := cm.Data[contracts.KeyStorageS3Region]; exists {
		config.Controller.DefaultS3Region = val
	}
	if val, exists := cm.Data[contracts.KeyStorageS3Endpoint]; exists {
		config.Controller.DefaultS3Endpoint = val
	}
	if val, exists := cm.Data[contracts.KeyStorageS3PathStyle]; exists {
		config.Controller.DefaultS3UsePathStyle = val == strTrue
	}
	if val, exists := cm.Data[contracts.KeyStorageS3AuthSecret]; exists {
		config.Controller.DefaultS3AuthSecretName = val
	}
	if val, exists := cm.Data[contracts.KeyStorageFilePath]; exists {
		config.Controller.DefaultFileStoragePath = strings.TrimSpace(val)
	}
	if val, exists := cm.Data[contracts.KeyStorageFileVolumeClaim]; exists {
		config.Controller.DefaultFileVolumeClaimName = strings.TrimSpace(val)
	}
	configParserLog.Info("Applied storage overrides",
		"provider", config.Controller.DefaultStorageProvider,
		"s3Bucket", config.Controller.DefaultS3Bucket,
		"s3Region", config.Controller.DefaultS3Region,
		"s3UsePathStyle", config.Controller.DefaultS3UsePathStyle,
		"filePath", config.Controller.DefaultFileStoragePath,
		"filePVC", config.Controller.DefaultFileVolumeClaimName,
	)
	applyStorageEnvDefaults(&config.Controller)
}

// applyStorageEnvDefaults propagates storage configuration from the operator
// ConfigMap into process-level environment variables consumed by Engram Job
// containers and the storage client library.
//
// Design note — global environment mutation:
// This function calls os.Setenv, which mutates the process-wide environment.
// setEnvIfEmpty only writes a variable when it is currently unset, making the
// call idempotent for the common case where the operator pod already has env
// vars injected by the Deployment manifest. However, the following limitations
// apply and should be understood by maintainers:
//
//  1. Values are write-once per process: once set, a variable is never cleared
//     or updated, even if the ConfigMap is deleted or the storage provider is
//     changed at runtime. The reconcile-triggered re-parse will be a no-op for
//     any variable that was already written.
//
//  2. Child processes inherit the environment: any Job pods spawned by the
//     operator after this call will inherit the variables set here, which is
//     the intended behaviour for storage credential propagation.
//
//  3. Race condition on concurrent reconciles: os.Setenv is not goroutine-safe
//     in general. In practice controller-runtime serialises ConfigMap reconciles
//     (single worker), so concurrent writes from this path are unlikely, but
//     tests that call parseOperatorConfigMap concurrently should be aware of this.
//
// TODO: replace os.Setenv with an explicit, tested configuration-injection layer
// (e.g. a structured StorageConfig passed directly to clients) to remove the
// dependency on global mutable state and make the propagation path testable.
func applyStorageEnvDefaults(cfg *ControllerConfig) {
	if cfg == nil {
		return
	}
	provider := strings.ToLower(strings.TrimSpace(cfg.DefaultStorageProvider))
	switch provider {
	case contracts.StorageProviderS3:
		if strings.TrimSpace(cfg.DefaultS3Bucket) == "" {
			return
		}
		setEnvIfEmpty(contracts.StorageProviderEnv, provider)
		setEnvIfEmpty(contracts.StorageS3BucketEnv, cfg.DefaultS3Bucket)
		if cfg.DefaultS3Region != "" {
			setEnvIfEmpty(contracts.StorageS3RegionEnv, cfg.DefaultS3Region)
		}
		if cfg.DefaultS3Endpoint != "" {
			setEnvIfEmpty(contracts.StorageS3EndpointEnv, cfg.DefaultS3Endpoint)
		}
		setEnvIfEmpty(contracts.StorageS3ForcePathStyleEnv, strconv.FormatBool(cfg.DefaultS3UsePathStyle))
	case contracts.StorageProviderFile:
		if strings.TrimSpace(cfg.DefaultFileStoragePath) == "" {
			return
		}
		setEnvIfEmpty(contracts.StorageProviderEnv, provider)
		setEnvIfEmpty(contracts.StoragePathEnv, cfg.DefaultFileStoragePath)
	}
}

// setEnvIfEmpty sets key=value in the process environment only when key is not
// already set. This prevents ConfigMap-derived defaults from overriding values
// injected by the Deployment manifest (e.g. from a Kubernetes Secret reference).
// See applyStorageEnvDefaults for the broader design note on env mutation.
func setEnvIfEmpty(key, value string) {
	if strings.TrimSpace(value) == "" || os.Getenv(key) != "" {
		return
	}
	_ = os.Setenv(key, value)
}
