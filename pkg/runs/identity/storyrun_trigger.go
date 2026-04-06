package identity

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bubustack/core/contracts"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	// StoryRunTriggerTokenAnnotation carries the caller-provided trigger token for idempotent StoryRun creation.
	StoryRunTriggerTokenAnnotation = contracts.StoryRunTriggerTokenAnnotation
	// StoryRunTriggerInputHashAnnotation optionally carries a hash of the inputs for token-based validation.
	StoryRunTriggerInputHashAnnotation = "storyrun.bubustack.io/trigger-input-hash"
	// StoryRunRedriveTokenAnnotation requests a full redrive of the StoryRun when changed.
	StoryRunRedriveTokenAnnotation = "storyrun.bubustack.io/redrive-token"
	// StoryRunRedriveObservedAnnotation records the last redrive token processed by the controller.
	StoryRunRedriveObservedAnnotation = "storyrun.bubustack.io/redrive-observed"
	// StoryRunRedriveFromStepAnnotation requests a partial rerun starting from a step.
	// Format: "<step-name>:<token>" (token must change to retrigger).
	StoryRunRedriveFromStepAnnotation = "storyrun.bubustack.io/redrive-from-step"
	// StoryRunRedriveFromStepObservedAnnotation records the last redrive-from-step token processed.
	StoryRunRedriveFromStepObservedAnnotation = "storyrun.bubustack.io/redrive-from-step-observed"
)

const storyRunNameMaxLength = 253

// DeriveStoryRunName returns the deterministic StoryRun name for the trigger token.
// This matches the SDK naming algorithm so server-side validation stays consistent.
func DeriveStoryRunName(storyNamespace, storyName, token string) string {
	sanitized := sanitizeTokenSegment(token)
	hashSeed := storyNamespace + "/" + storyName + "/" + token
	hashed := shortTokenHash(hashSeed)
	needsHash := sanitized != token || storyNamespace != ""

	if sanitized == "" {
		sanitized = hashed
		needsHash = false
	}

	if needsHash {
		sanitized = fmt.Sprintf("%s-%s", sanitized, hashed)
	}

	base := fmt.Sprintf("%s-%s", storyName, sanitized)
	if len(base) <= storyRunNameMaxLength {
		return base
	}

	maxPrefix := max(storyRunNameMaxLength-1-len(hashed), 1)
	prefix := storyName
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
		prefix = strings.Trim(prefix, "-")
		if prefix == "" {
			prefix = "s"
		}
	}
	return fmt.Sprintf("%s-%s", prefix, hashed)
}

// ComputeTriggerInputHash returns a stable sha256 hash of trigger inputs.
// Inputs are decoded into JSON and re-encoded to ensure a canonical ordering.
func ComputeTriggerInputHash(raw []byte) (string, error) {
	payload := raw
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return "", fmt.Errorf("decode trigger inputs: %w", err)
	}
	return ComputeTriggerInputHashFromValue(value)
}

// ComputeTriggerInputHashFromValue returns a stable sha256 hash of the provided
// trigger input value. Nil values are treated as an empty JSON object to match
// ComputeTriggerInputHash(nil).
func ComputeTriggerInputHashFromValue(value any) (string, error) {
	if value == nil {
		value = map[string]any{}
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode trigger inputs: %w", err)
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

// ComputeTriggerInputHashFromRawExtension returns the trigger input hash from a RawExtension.
func ComputeTriggerInputHashFromRawExtension(inputs *runtime.RawExtension) (string, error) {
	if inputs == nil {
		return ComputeTriggerInputHash(nil)
	}
	return ComputeTriggerInputHash(inputs.Raw)
}

func sanitizeTokenSegment(token string) string {
	token = strings.ToLower(token)
	var b strings.Builder
	b.Grow(len(token))
	prevHyphen := false

	for _, r := range token {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevHyphen = false
		case r == '-':
			if b.Len() == 0 || prevHyphen {
				continue
			}
			b.WriteRune('-')
			prevHyphen = true
		default:
			if b.Len() == 0 || prevHyphen {
				continue
			}
			b.WriteRune('-')
			prevHyphen = true
		}
	}

	return strings.Trim(b.String(), "-")
}

func shortTokenHash(token string) string {
	sum := sha1.Sum([]byte(token))
	return hex.EncodeToString(sum[:8])
}
