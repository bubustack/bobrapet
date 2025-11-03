package naming

import (
	"fmt"
	"hash/fnv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

const maxDNS1123Length = validation.DNS1123LabelMaxLength

// Compose builds a DNS-1123 compliant resource name from the provided parts.
// When the concatenated name exceeds 63 characters, the prefix is truncated and
// a deterministic hash suffix is appended so the result remains stable across reconciliations.
func Compose(parts ...string) string {
	base := strings.Join(parts, "-")
	if len(base) <= maxDNS1123Length {
		return base
	}

	hash := fnv.New32a()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0}) // separator to avoid collisions on different segment splits
	}
	suffix := fmt.Sprintf("%08x", hash.Sum32())

	// Reserve space for "-" plus the hash suffix.
	prefixLen := maxDNS1123Length - len(suffix) - 1
	if prefixLen < 1 {
		prefixLen = maxDNS1123Length - len(suffix)
	}
	if prefixLen < 1 {
		// Fallback: suffix alone (truncated if needed)
		if len(suffix) > maxDNS1123Length {
			return suffix[:maxDNS1123Length]
		}
		return suffix
	}

	prefix := base[:prefixLen]
	prefix = strings.TrimSuffix(prefix, "-")
	if len(prefix) == 0 {
		prefix = base[:prefixLen]
		prefix = strings.Trim(prefix, "-")
		if len(prefix) == 0 {
			prefix = "resource"
		}
	}

	result := fmt.Sprintf("%s-%s", prefix, suffix)
	if len(result) > maxDNS1123Length {
		result = result[:maxDNS1123Length]
		result = strings.TrimSuffix(result, "-")
		if len(result) == 0 {
			if len(suffix) > maxDNS1123Length {
				return suffix[:maxDNS1123Length]
			}
			return suffix
		}
	}
	return result
}
