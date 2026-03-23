package storage

import (
	"os"
	"strings"
)

// getEnvWithFallback returns the value for the provided key if set, otherwise
// returns the supplied fallback literal. The primary value is returned without
// trimming to preserve intentional whitespace (matching os.LookupEnv semantics).
func getEnvWithFallback(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(val)
	}
	return fallback
}

// getTrimmedEnv returns the trimmed value of the requested environment variable.
func getTrimmedEnv(key string) string {
	if key == "" {
		return ""
	}
	return strings.TrimSpace(os.Getenv(key))
}
