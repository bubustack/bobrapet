package transport

import (
	"fmt"

	"github.com/bubustack/bobrapet/internal/config"
)

// EffectiveServicePort returns the first declared service port or falls back to defaultPort.
func EffectiveServicePort(resolved *config.ResolvedExecutionConfig, defaultPort int32) int32 {
	if resolved == nil {
		return defaultPort
	}
	for _, svcPort := range resolved.ServicePorts {
		if svcPort.Port > 0 {
			return svcPort.Port
		}
	}
	return defaultPort
}

// LocalConnectorEndpoint returns the loopback endpoint bindings should advertise when sidecars are injected.
func LocalConnectorEndpoint(port int32) string {
	if port <= 0 {
		port = 50051
	}
	return fmt.Sprintf("127.0.0.1:%d", port)
}
