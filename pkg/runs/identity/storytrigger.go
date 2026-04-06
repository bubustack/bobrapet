package identity

import "fmt"

const (
	// StoryRunTriggerRequestNameAnnotation records the originating StoryTrigger name on StoryRuns created
	// through the durable trigger request path.
	StoryRunTriggerRequestNameAnnotation = "storyrun.bubustack.io/trigger-request-name"
	// StoryRunTriggerRequestUIDAnnotation records the originating StoryTrigger UID on StoryRuns created
	// through the durable trigger request path.
	StoryRunTriggerRequestUIDAnnotation = "storyrun.bubustack.io/trigger-request-uid"
)

// StoryTriggerIdentity returns the durable business identity for a trigger request.
// The key wins when present; otherwise the per-submission identifier is used.
func StoryTriggerIdentity(key, submissionID string) string {
	if key != "" {
		return key
	}
	return submissionID
}

// DeriveStoryTriggerName returns the deterministic StoryTrigger name for the
// provided story namespace/name and trigger identity.
func DeriveStoryTriggerName(storyNamespace, storyName, key, submissionID string) string {
	identity := StoryTriggerIdentity(key, submissionID)
	if identity == "" {
		return ""
	}

	hashSeed := storyNamespace + "/" + storyName + "/" + identity
	hashed := shortTokenHash(hashSeed)
	base := fmt.Sprintf("%s-trigger-%s", storyName, hashed)
	if len(base) <= storyRunNameMaxLength {
		return base
	}

	maxPrefix := max(storyRunNameMaxLength-len("-trigger-")-len(hashed), 1)
	prefix := storyName
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
		prefix = trimTrailingHyphen(prefix)
		if prefix == "" {
			prefix = "s"
		}
	}
	return fmt.Sprintf("%s-trigger-%s", prefix, hashed)
}

func trimTrailingHyphen(value string) string {
	for len(value) > 0 && value[len(value)-1] == '-' {
		value = value[:len(value)-1]
	}
	return value
}
