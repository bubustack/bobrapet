package validation

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

// ValidateDownstreamTargets ensures each target selects exactly one transport mode
// and validates GRPC endpoint format.
//
// Behavior:
//   - Returns an error if a target does not set exactly one of grpc or terminate.
//   - For GRPC targets, validates that the endpoint is a valid host:port format.
//   - Supports both DNS names (service.namespace.svc:9000) and IP addresses.
//
// Arguments:
//   - targets []runsv1alpha1.DownstreamTarget: the list of downstream targets to validate.
//
// Returns:
//   - error: describes the first validation failure, or nil if all targets are valid.
func ValidateDownstreamTargets(targets []runsv1alpha1.DownstreamTarget) error {
	for idx := range targets {
		target := &targets[idx]
		count := 0
		if target.GRPCTarget != nil {
			count++
		}
		if target.Terminate != nil {
			count++
		}
		if count != 1 {
			return fmt.Errorf("spec.downstreamTargets[%d] must set exactly one of grpc or terminate", idx)
		}

		// Validate GRPC endpoint format if present
		if target.GRPCTarget != nil {
			if err := ValidateGRPCEndpoint(target.GRPCTarget.Endpoint); err != nil {
				return fmt.Errorf("spec.downstreamTargets[%d].grpc.endpoint: %w", idx, err)
			}
		}
	}
	return nil
}

// ValidateGRPCEndpoint validates that an endpoint string is a valid gRPC target address.
//
// Behavior:
//   - Accepts formats: "host:port", "hostname.namespace.svc:port", "ip:port".
//   - Rejects empty endpoints, missing ports, and invalid port numbers.
//   - Does not require the endpoint to be reachable (syntax check only).
//
// Arguments:
//   - endpoint string: the gRPC endpoint to validate.
//
// Returns:
//   - error: describes the validation failure, or nil if the endpoint is valid.
func ValidateGRPCEndpoint(endpoint string) error {
	if strings.TrimSpace(endpoint) == "" {
		return fmt.Errorf("endpoint must not be empty")
	}

	// Handle scheme prefix (grpc://, http://, etc.) by parsing as URL
	if strings.Contains(endpoint, "://") {
		u, err := url.Parse(endpoint)
		if err != nil {
			return fmt.Errorf("invalid endpoint URL: %w", err)
		}
		if u.Host == "" {
			return fmt.Errorf("endpoint URL must include host")
		}
		// Extract host:port from URL
		endpoint = u.Host
	}

	// Validate host:port format
	host, portStr, err := net.SplitHostPort(endpoint)
	if err != nil {
		return fmt.Errorf("endpoint must be in host:port format: %w", err)
	}

	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("endpoint host must not be empty")
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("invalid port number: %w", err)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535, got %d", port)
	}

	return nil
}
