package storage

import (
	"os"
	"strings"
)

// getTrimmedEnv returns the trimmed value of the requested environment variable.
func getTrimmedEnv(key string) string {
	if key == "" {
		return ""
	}
	return strings.TrimSpace(os.Getenv(key))
}
