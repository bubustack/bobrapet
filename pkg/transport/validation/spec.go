package validation

import (
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
)

// ValidationResult holds both the field errors and the primary reason code for status conditions.
type ValidationResult struct {
	// Errors contains all validation errors as field.ErrorList.
	Errors field.ErrorList
	// PrimaryReason is the condition reason code for the first (most significant) error.
	// Empty when validation passes.
	PrimaryReason string
}

// IsValid returns true if there are no validation errors.
func (r *ValidationResult) IsValid() bool {
	return len(r.Errors) == 0
}

// ValidateTransport runs spec-level validation and returns field errors that can be surfaced
// by reconcilers or admission webhooks.
func ValidateTransport(transport *transportv1alpha1.Transport) field.ErrorList {
	return ValidateTransportWithReason(transport).Errors
}

// ValidateTransportWithReason runs spec-level validation and returns both field errors
// and a structured reason code suitable for Kubernetes conditions.
//
// Behavior:
//   - Returns an empty result with no reason when validation passes.
//   - Returns the first applicable reason code for condition status updates.
//
// Arguments:
//   - transport *transportv1alpha1.Transport: the Transport to validate.
//
// Returns:
//   - *ValidationResult: contains errors and primary reason code.
func ValidateTransportWithReason(transport *transportv1alpha1.Transport) *ValidationResult {
	if transport == nil {
		return &ValidationResult{}
	}
	return validateTransportSpecWithReason(field.NewPath("spec"), &transport.Spec)
}

// validateTransportSpecWithReason performs validation and returns both errors and reason codes.
func validateTransportSpecWithReason(path *field.Path, spec *transportv1alpha1.TransportSpec) *ValidationResult {
	result := &ValidationResult{
		Errors: make(field.ErrorList, 0),
	}
	if spec == nil {
		result.Errors = append(result.Errors, field.Required(path, "spec must be provided"))
		result.PrimaryReason = conditions.ReasonValidationFailed
		return result
	}

	if strings.TrimSpace(spec.Driver) == "" {
		result.Errors = append(result.Errors, field.Required(path.Child("driver"), "driver must be specified"))
		if result.PrimaryReason == "" {
			result.PrimaryReason = conditions.ReasonTransportDriverMissing
		}
	}

	if !hasDeclaredCapabilities(spec) {
		result.Errors = append(
			result.Errors,
			field.Invalid(path, spec, "at least one capability (audio, video, or binary) must be declared"),
		)
		if result.PrimaryReason == "" {
			result.PrimaryReason = conditions.ReasonTransportCapabilitiesMissing
		}
	}

	audioResult := validateAudioCapabilitiesWithReason(path.Child("supportedAudio"), spec.SupportedAudio)
	result.Errors = append(result.Errors, audioResult.Errors...)
	if result.PrimaryReason == "" && audioResult.PrimaryReason != "" {
		result.PrimaryReason = audioResult.PrimaryReason
	}

	videoResult := validateVideoCapabilitiesWithReason(path.Child("supportedVideo"), spec.SupportedVideo)
	result.Errors = append(result.Errors, videoResult.Errors...)
	if result.PrimaryReason == "" && videoResult.PrimaryReason != "" {
		result.PrimaryReason = videoResult.PrimaryReason
	}

	binaryResult := validateBinaryCapabilitiesWithReason(path.Child("supportedBinary"), spec.SupportedBinary)
	result.Errors = append(result.Errors, binaryResult.Errors...)
	if result.PrimaryReason == "" && binaryResult.PrimaryReason != "" {
		result.PrimaryReason = binaryResult.PrimaryReason
	}

	return result
}

func validateAudioCapabilitiesWithReason(path *field.Path, codecs []transportv1alpha1.AudioCodec) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	seen := map[string]int{}
	for i, codec := range codecs {
		idx := path.Index(i)
		name := strings.TrimSpace(strings.ToLower(codec.Name))
		if name == "" {
			result.Errors = append(result.Errors, field.Required(idx.Child("name"), "codec name must be set"))
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonTransportCodecInvalid
			}
			continue
		}
		if prev, ok := seen[name]; ok {
			result.Errors = append(result.Errors, field.Duplicate(path.Index(prev).Child("name"), codec.Name))
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonTransportCodecDuplicate
			}
		}
		seen[name] = i
		if codec.SampleRateHz < 0 {
			result.Errors = append(
				result.Errors,
				field.Invalid(idx.Child("sampleRateHz"), codec.SampleRateHz, "must be non-negative"),
			)
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonTransportCodecInvalid
			}
		}
		if codec.Channels < 0 {
			result.Errors = append(result.Errors, field.Invalid(idx.Child("channels"), codec.Channels, "must be non-negative"))
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonTransportCodecInvalid
			}
		}
	}
	return result
}

func validateVideoCapabilitiesWithReason(path *field.Path, codecs []transportv1alpha1.VideoCodec) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	seen := map[string]int{}
	for i, codec := range codecs {
		idx := path.Index(i)
		name := strings.TrimSpace(strings.ToLower(codec.Name))
		if name == "" {
			result.Errors = append(result.Errors, field.Required(idx.Child("name"), "codec name must be set"))
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonTransportCodecInvalid
			}
			continue
		}
		if prev, ok := seen[name]; ok {
			result.Errors = append(result.Errors, field.Duplicate(path.Index(prev).Child("name"), codec.Name))
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonTransportCodecDuplicate
			}
		}
		seen[name] = i
	}
	return result
}

func validateBinaryCapabilitiesWithReason(path *field.Path, mimeTypes []string) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	seen := map[string]int{}
	for i, mt := range mimeTypes {
		idx := path.Index(i)
		value := strings.TrimSpace(strings.ToLower(mt))
		if value == "" {
			result.Errors = append(result.Errors, field.Required(idx, "mime type must be set"))
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonTransportMimeTypeInvalid
			}
			continue
		}
		if !strings.Contains(value, "/") {
			result.Errors = append(result.Errors, field.Invalid(idx, mt, "must be a valid MIME type (type/subtype)"))
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonTransportMimeTypeInvalid
			}
		}
		if prev, ok := seen[value]; ok {
			result.Errors = append(result.Errors, field.Duplicate(path.Index(prev), mt))
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonTransportCodecDuplicate
			}
		}
		seen[value] = i
	}
	return result
}

func hasDeclaredCapabilities(spec *transportv1alpha1.TransportSpec) bool {
	if spec == nil {
		return false
	}
	return len(spec.SupportedAudio) > 0 ||
		len(spec.SupportedVideo) > 0 ||
		len(spec.SupportedBinary) > 0
}
