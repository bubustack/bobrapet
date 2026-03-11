package config_test

import (
	"testing"
	"time"

	"github.com/bubustack/bobrapet/internal/config"
	"github.com/stretchr/testify/require"
)

func TestValidateControllerConfig_Defaults(t *testing.T) {
	cfg := config.DefaultControllerConfig()
	require.NoError(t, config.ValidateControllerConfig(cfg))
}

func TestValidateControllerConfig_TemplatingBounds(t *testing.T) {
	cfg := config.DefaultControllerConfig()
	cfg.TemplateEvaluationTimeout = -1 * time.Second
	err := config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "templating.evaluation-timeout cannot be negative")

	cfg = config.DefaultControllerConfig()
	cfg.TemplateOffloadedPolicy = config.TemplatingOffloadedPolicyInject
	cfg.TemplateMaterializeEngram = ""
	err = config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "templating.materialize-engram must be set")
}

func TestValidateControllerConfig_TransportHeartbeat(t *testing.T) {
	cfg := config.DefaultControllerConfig()
	cfg.Transport.HeartbeatInterval = 0
	err := config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "controller.transport.heartbeat-interval")

	cfg = config.DefaultControllerConfig()
	cfg.Engram.EngramControllerConfig.DefaultGRPCHeartbeatIntervalSeconds = 0
	err = config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "engram.default-grpc-heartbeat-interval-seconds")
}

func TestValidateControllerConfig_ReferencePolicy(t *testing.T) {
	cfg := config.DefaultControllerConfig()
	cfg.ReferenceCrossNamespacePolicy = "invalid"
	err := config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "references.cross-namespace-policy")
}

func TestValidateControllerConfig_ReconcileTimeoutBounds(t *testing.T) {
	cfg := config.DefaultControllerConfig()
	cfg.ReconcileTimeout = -1 * time.Second
	err := config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "controller.reconcile-timeout cannot be negative")

	cfg = config.DefaultControllerConfig()
	cfg.ReconcileTimeout = 0
	require.NoError(t, config.ValidateControllerConfig(cfg))

	cfg = config.DefaultControllerConfig()
	cfg.ReconcileTimeout = 1 * time.Second
	err = config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "controller.reconcile-timeout must be at least")

	cfg = config.DefaultControllerConfig()
	cfg.ReconcileTimeout = 31 * time.Minute
	err = config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "controller.reconcile-timeout must not exceed")
}

func TestValidateControllerConfig_DropCapabilities(t *testing.T) {
	cfg := config.DefaultControllerConfig()
	cfg.DropCapabilities = []string{"NET_RAW", "  chown "}
	require.NoError(t, config.ValidateControllerConfig(cfg))
	require.Equal(t, []string{"NET_RAW", "CHOWN"}, cfg.DropCapabilities)

	cfg = config.DefaultControllerConfig()
	cfg.DropCapabilities = []string{"", "CHOWN"}
	err := config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "security.drop-capabilities contains an empty entry")
}

func TestValidateControllerConfig_PriorityAging(t *testing.T) {
	cfg := config.DefaultControllerConfig()
	queue := cfg.StoryRun.Scheduling.Queues["default"]
	queue.PriorityAgingSeconds = -1
	cfg.StoryRun.Scheduling.Queues["default"] = queue
	err := config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "priority-aging-seconds cannot be negative")
}
