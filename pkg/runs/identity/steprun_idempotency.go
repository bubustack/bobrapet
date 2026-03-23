package identity

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

const maxIdempotencyKeyLength = 256

// StepRunIdempotencyKey builds a stable idempotency key for a step execution.
// The key is derived from namespace, StoryRun name, and Step ID, and is trimmed
// to the API's max length by falling back to a hash when needed.
func StepRunIdempotencyKey(namespace, storyRunName, stepID string) string {
	namespace = strings.TrimSpace(namespace)
	storyRunName = strings.TrimSpace(storyRunName)
	stepID = strings.TrimSpace(stepID)

	base := fmt.Sprintf("storyrun/%s/step/%s", storyRunName, stepID)
	if namespace != "" {
		base = fmt.Sprintf("ns/%s/%s", namespace, base)
	}
	if len(base) <= maxIdempotencyKeyLength {
		return base
	}

	sum := sha256.Sum256([]byte(base))
	return fmt.Sprintf("sha256:%x", sum)
}
