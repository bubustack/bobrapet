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

func TestValidateControllerConfig_LoopBounds(t *testing.T) {
	cfg := config.DefaultControllerConfig()
	cfg.MaxLoopIterations = 0
	err := config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "loop.max-iterations")

	cfg = config.DefaultControllerConfig()
	cfg.DefaultLoopBatchSize = -1
	err = config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "loop.default-batch-size cannot be negative")

	cfg = config.DefaultControllerConfig()
	cfg.DefaultLoopBatchSize = 10
	cfg.MaxLoopBatchSize = 5
	err = config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "loop.default-batch-size cannot exceed loop.max-batch-size")

	cfg = config.DefaultControllerConfig()
	cfg.MaxLoopBatchSize = -5
	err = config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "loop.max-batch-size cannot be negative")

	cfg = config.DefaultControllerConfig()
	cfg.MaxLoopConcurrency = -5
	err = config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "loop.max-concurrency cannot be negative")

	cfg = config.DefaultControllerConfig()
	cfg.MaxLoopConcurrency = 10
	cfg.MaxConcurrencyLimit = 5
	err = config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "loop.max-concurrency cannot exceed loop.max-concurrency-limit")

	cfg = config.DefaultControllerConfig()
	cfg.MaxConcurrencyLimit = -1
	err = config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "loop.max-concurrency-limit cannot be negative")
}

func TestValidateControllerConfig_CELBounds(t *testing.T) {
	cfg := config.DefaultControllerConfig()
	cfg.CELEvaluationTimeout = -1 * time.Second
	err := config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "cel.evaluation-timeout cannot be negative")

	cfg = config.DefaultControllerConfig()
	cfg.CELMaxExpressionLength = -100
	err = config.ValidateControllerConfig(cfg)
	require.ErrorContains(t, err, "cel.max-expression-length cannot be negative")
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
