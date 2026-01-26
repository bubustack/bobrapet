package capabilities

import (
	"strings"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
)

// CloneAudioCodecs returns a deep copy of the provided audio codec slice so callers
// can mutate the result without affecting the original slice.
func CloneAudioCodecs(src []transportv1alpha1.AudioCodec) []transportv1alpha1.AudioCodec {
	return CloneSlice(src)
}

// CloneVideoCodecs returns a deep copy of the provided video codec slice.
func CloneVideoCodecs(src []transportv1alpha1.VideoCodec) []transportv1alpha1.VideoCodec {
	return CloneSlice(src)
}

// CloneBinaryCapabilities returns a deep copy of the provided binary capability slice.
func CloneBinaryCapabilities(src []string) []string {
	return CloneSlice(src)
}

// AppendUniqueAudioCodec normalizes the provided codec and appends it to dst only
// when no equivalent codec already exists in the slice.
func AppendUniqueAudioCodec(
	dst []transportv1alpha1.AudioCodec,
	codec transportv1alpha1.AudioCodec,
) []transportv1alpha1.AudioCodec {
	return AppendUniqueCapability(
		dst,
		codec,
		func(c transportv1alpha1.AudioCodec) bool { return c.Name != "" },
		CanonicalizeAudioCodec,
		func(a, b transportv1alpha1.AudioCodec) bool {
			return a.Name == b.Name &&
				a.SampleRateHz == b.SampleRateHz &&
				a.Channels == b.Channels
		},
	)
}

// AppendUniqueVideoCodec normalizes the provided codec and appends it to dst only
// when no equivalent codec already exists in the slice.
func AppendUniqueVideoCodec(
	dst []transportv1alpha1.VideoCodec,
	codec transportv1alpha1.VideoCodec,
) []transportv1alpha1.VideoCodec {
	return AppendUniqueCapability(
		dst,
		codec,
		func(c transportv1alpha1.VideoCodec) bool { return c.Name != "" },
		CanonicalizeVideoCodec,
		func(a, b transportv1alpha1.VideoCodec) bool {
			return a.Name == b.Name && a.Profile == b.Profile
		},
	)
}

// AppendUniqueBinary normalizes the provided MIME string and appends it to dst only
// when an identical entry does not already exist.
func AppendUniqueBinary(dst []string, mime string) []string {
	return AppendUniqueCapability(
		dst,
		mime,
		func(s string) bool { return s != "" },
		CanonicalCapabilityName,
		func(a, b string) bool { return a == b },
	)
}

// CanonicalizeAudioCodec trims and lowercases the codec name.
func CanonicalizeAudioCodec(codec transportv1alpha1.AudioCodec) transportv1alpha1.AudioCodec {
	codec.Name = CanonicalCapabilityName(codec.Name)
	return codec
}

// CanonicalizeVideoCodec trims and lowercases the codec name and profile.
func CanonicalizeVideoCodec(codec transportv1alpha1.VideoCodec) transportv1alpha1.VideoCodec {
	codec.Name = CanonicalCapabilityName(codec.Name)
	codec.Profile = CanonicalCapabilityName(codec.Profile)
	return codec
}

// CanonicalCapabilityName trims leading/trailing whitespace and lowercases the capability string.
func CanonicalCapabilityName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// AppendUniqueCapability normalizes the provided value, validates it, and appends
// it to dst only when no equivalent normalized entry already exists.
func AppendUniqueCapability[T any](
	dst []T,
	value T,
	valid func(T) bool,
	canonical func(T) T,
	equals func(a, b T) bool,
) []T {
	normalized := canonical(value)
	if !valid(normalized) {
		return dst
	}
	for _, existing := range dst {
		if equals(canonical(existing), normalized) {
			return dst
		}
	}
	return append(dst, normalized)
}

// CloneSlice returns a shallow copy of the provided slice.
func CloneSlice[T any](src []T) []T {
	if len(src) == 0 {
		return nil
	}
	out := make([]T, len(src))
	copy(out, src)
	return out
}
