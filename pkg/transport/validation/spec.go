//nolint:lll // validation error messages are clearer as single lines
package validation

import (
	"encoding/json"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/core/contracts"
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

	mergeValidationResult(result, validateAudioCapabilitiesWithReason(path.Child("supportedAudio"), spec.SupportedAudio))
	mergeValidationResult(result, validateVideoCapabilitiesWithReason(path.Child("supportedVideo"), spec.SupportedVideo))
	mergeValidationResult(result, validateBinaryCapabilitiesWithReason(path.Child("supportedBinary"), spec.SupportedBinary))
	mergeValidationResult(result, validateStreamingSettings(path.Child("streaming"), spec.Streaming))

	settingsErrs := ValidateTransportSecuritySettings(path.Child("defaultSettings"), spec.DefaultSettings)
	result.Errors = append(result.Errors, settingsErrs...)
	if result.PrimaryReason == "" && len(settingsErrs) > 0 {
		result.PrimaryReason = conditions.ReasonValidationFailed
	}

	return result
}

// ValidateTransportSecuritySettings rejects raw transport settings that configure an
// unsupported transport security mode.
func ValidateTransportSecuritySettings(path *field.Path, raw *runtime.RawExtension) field.ErrorList {
	if raw == nil {
		return nil
	}

	trimmed := strings.TrimSpace(string(raw.Raw))
	if trimmed == "" || trimmed == "null" {
		return nil
	}

	var decoded map[string]any
	if err := json.Unmarshal(raw.Raw, &decoded); err != nil {
		return field.ErrorList{
			field.Invalid(path, trimmed, "must be a valid JSON object"),
		}
	}

	envValue, exists := decoded["env"]
	if !exists {
		return nil
	}

	env, ok := envValue.(map[string]any)
	if !ok {
		return field.ErrorList{
			field.Invalid(path.Child("env"), envValue, "must be an object"),
		}
	}

	modeValue, exists := env[contracts.TransportSecurityModeEnv]
	if !exists {
		return nil
	}

	mode, ok := modeValue.(string)
	if !ok {
		return field.ErrorList{
			field.Invalid(path.Child("env").Child(contracts.TransportSecurityModeEnv), modeValue, "must be a string"),
		}
	}

	if strings.TrimSpace(strings.ToLower(mode)) != contracts.TransportSecurityModeTLS {
		return field.ErrorList{
			field.Invalid(
				path.Child("env").Child(contracts.TransportSecurityModeEnv),
				mode,
				`only "tls" is supported`,
			),
		}
	}

	return nil
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

//nolint:gocyclo // complex by design
func validateStreamingSettings(path *field.Path, settings *transportv1alpha1.TransportStreamingSettings) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	if settings == nil {
		return result
	}
	mergeValidationResult(result, validateBackpressureSettings(path.Child("backpressure"), settings.Backpressure))
	mergeValidationResult(result, validateFlowControlSettings(path.Child("flowControl"), settings.FlowControl))
	mergeValidationResult(result, validateDeliverySettings(path.Child("delivery"), settings.Delivery))
	mergeValidationResult(result, validateRoutingSettings(path.Child("routing"), settings.Routing))
	mergeValidationResult(result, validatePartitioningSettings(path.Child("partitioning"), settings.Partitioning))
	mergeValidationResult(result, validateLifecycleSettings(path.Child("lifecycle"), settings.Lifecycle))
	mergeValidationResult(result, validateObservabilitySettings(path.Child("observability"), settings.Observability))
	mergeValidationResult(result, validateRecordingSettings(path.Child("recording"), settings.Recording))
	if len(settings.Lanes) > 0 {
		seenNames := map[string]int{}
		seenKinds := map[string]int{}
		for i, lane := range settings.Lanes {
			idx := path.Child("lanes").Index(i)
			name := strings.TrimSpace(lane.Name)
			if name == "" {
				result.Errors = append(result.Errors, field.Required(idx.Child("name"), "lane name must be set"))
				if result.PrimaryReason == "" {
					result.PrimaryReason = conditions.ReasonValidationFailed
				}
			} else if prev, ok := seenNames[name]; ok {
				result.Errors = append(result.Errors, field.Duplicate(path.Child("lanes").Index(prev).Child("name"), lane.Name))
				if result.PrimaryReason == "" {
					result.PrimaryReason = conditions.ReasonValidationFailed
				}
			} else {
				seenNames[name] = i
			}
			kind := strings.TrimSpace(string(lane.Kind))
			if kind == "" {
				result.Errors = append(result.Errors, field.Required(idx.Child("kind"), "lane kind must be set"))
				if result.PrimaryReason == "" {
					result.PrimaryReason = conditions.ReasonValidationFailed
				}
			} else if prev, ok := seenKinds[kind]; ok {
				result.Errors = append(result.Errors, field.Duplicate(path.Child("lanes").Index(prev).Child("kind"), lane.Kind))
				if result.PrimaryReason == "" {
					result.PrimaryReason = conditions.ReasonValidationFailed
				}
			} else {
				seenKinds[kind] = i
			}
			if lane.MaxMessages != nil && *lane.MaxMessages < 0 {
				result.Errors = append(result.Errors, field.Invalid(idx.Child("maxMessages"), *lane.MaxMessages, "must be non-negative"))
				if result.PrimaryReason == "" {
					result.PrimaryReason = conditions.ReasonValidationFailed
				}
			}
			if lane.MaxBytes != nil && *lane.MaxBytes < 0 {
				result.Errors = append(result.Errors, field.Invalid(idx.Child("maxBytes"), *lane.MaxBytes, "must be non-negative"))
				if result.PrimaryReason == "" {
					result.PrimaryReason = conditions.ReasonValidationFailed
				}
			}
		}
	}
	if settings.FanIn != nil {
		mode := strings.TrimSpace(string(settings.FanIn.Mode))
		if mode == string(transportv1alpha1.TransportFanInQuorum) {
			if settings.FanIn.Quorum == nil || *settings.FanIn.Quorum <= 0 {
				result.Errors = append(result.Errors, field.Required(path.Child("fanIn").Child("quorum"), "quorum must be set when mode=quorum"))
				if result.PrimaryReason == "" {
					result.PrimaryReason = conditions.ReasonValidationFailed
				}
			}
		} else if settings.FanIn.Quorum != nil {
			result.Errors = append(result.Errors, field.Invalid(path.Child("fanIn").Child("quorum"), *settings.FanIn.Quorum, "quorum is only valid when mode=quorum"))
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonValidationFailed
			}
		}
		if settings.FanIn.TimeoutSeconds != nil && *settings.FanIn.TimeoutSeconds < 0 {
			result.Errors = append(result.Errors, field.Invalid(path.Child("fanIn").Child("timeoutSeconds"), *settings.FanIn.TimeoutSeconds, "must be non-negative"))
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonValidationFailed
			}
		}
		if settings.FanIn.MaxEntries != nil && *settings.FanIn.MaxEntries <= 0 {
			result.Errors = append(result.Errors, field.Invalid(path.Child("fanIn").Child("maxEntries"), *settings.FanIn.MaxEntries, "must be >= 1"))
			if result.PrimaryReason == "" {
				result.PrimaryReason = conditions.ReasonValidationFailed
			}
		}
	}
	return result
}

func mergeValidationResult(dst *ValidationResult, src *ValidationResult) {
	if dst == nil || src == nil || len(src.Errors) == 0 {
		return
	}
	dst.Errors = append(dst.Errors, src.Errors...)
	if dst.PrimaryReason == "" && src.PrimaryReason != "" {
		dst.PrimaryReason = src.PrimaryReason
	}
}

func validateBackpressureSettings(path *field.Path, settings *transportv1alpha1.TransportBackpressureSettings) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	if settings == nil || settings.Buffer == nil {
		return result
	}
	buf := settings.Buffer
	if buf.MaxMessages != nil && *buf.MaxMessages < 0 {
		result.Errors = append(result.Errors, field.Invalid(path.Child("buffer").Child("maxMessages"), *buf.MaxMessages, "must be non-negative"))
	}
	if buf.MaxBytes != nil && *buf.MaxBytes < 0 {
		result.Errors = append(result.Errors, field.Invalid(path.Child("buffer").Child("maxBytes"), *buf.MaxBytes, "must be non-negative"))
	}
	if buf.MaxAgeSeconds != nil && *buf.MaxAgeSeconds < 0 {
		result.Errors = append(result.Errors, field.Invalid(path.Child("buffer").Child("maxAgeSeconds"), *buf.MaxAgeSeconds, "must be non-negative"))
	}
	if raw := strings.TrimSpace(string(buf.DropPolicy)); raw != "" {
		switch buf.DropPolicy {
		case transportv1alpha1.BufferDropNewest, transportv1alpha1.BufferDropOldest:
		default:
			result.Errors = append(result.Errors, field.Invalid(path.Child("buffer").Child("dropPolicy"), raw, "must be drop_newest or drop_oldest"))
		}
	}
	if len(result.Errors) > 0 {
		result.PrimaryReason = conditions.ReasonValidationFailed
	}
	return result
}

//nolint:gocyclo // complex by design
func validateFlowControlSettings(path *field.Path, settings *transportv1alpha1.TransportFlowControlSettings) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	if settings == nil {
		return result
	}
	if raw := strings.TrimSpace(string(settings.Mode)); raw != "" {
		switch settings.Mode {
		case transportv1alpha1.FlowControlNone, transportv1alpha1.FlowControlCredits, transportv1alpha1.FlowControlWindow:
		default:
			result.Errors = append(result.Errors, field.Invalid(path.Child("mode"), raw, "must be none, credits, or window"))
		}
	}
	if settings.InitialCredits != nil {
		if settings.InitialCredits.Messages != nil && *settings.InitialCredits.Messages < 0 {
			result.Errors = append(result.Errors, field.Invalid(path.Child("initialCredits").Child("messages"), *settings.InitialCredits.Messages, "must be non-negative"))
		}
		if settings.InitialCredits.Bytes != nil && *settings.InitialCredits.Bytes < 0 {
			result.Errors = append(result.Errors, field.Invalid(path.Child("initialCredits").Child("bytes"), *settings.InitialCredits.Bytes, "must be non-negative"))
		}
	}
	if settings.AckEvery != nil {
		if settings.AckEvery.Messages != nil && *settings.AckEvery.Messages < 0 {
			result.Errors = append(result.Errors, field.Invalid(path.Child("ackEvery").Child("messages"), *settings.AckEvery.Messages, "must be non-negative"))
		}
		if settings.AckEvery.Bytes != nil && *settings.AckEvery.Bytes < 0 {
			result.Errors = append(result.Errors, field.Invalid(path.Child("ackEvery").Child("bytes"), *settings.AckEvery.Bytes, "must be non-negative"))
		}
		if settings.AckEvery.MaxDelay != nil && strings.TrimSpace(*settings.AckEvery.MaxDelay) != "" {
			if d, err := time.ParseDuration(strings.TrimSpace(*settings.AckEvery.MaxDelay)); err != nil || d <= 0 {
				result.Errors = append(result.Errors, field.Invalid(path.Child("ackEvery").Child("maxDelay"), *settings.AckEvery.MaxDelay, "must be a valid positive duration"))
			}
		}
	}
	if settings.PauseThreshold != nil && settings.PauseThreshold.BufferPct != nil {
		if *settings.PauseThreshold.BufferPct < 0 || *settings.PauseThreshold.BufferPct > 100 {
			result.Errors = append(result.Errors, field.Invalid(path.Child("pauseThreshold").Child("bufferPct"), *settings.PauseThreshold.BufferPct, "must be between 0 and 100"))
		}
	}
	if settings.ResumeThreshold != nil && settings.ResumeThreshold.BufferPct != nil {
		if *settings.ResumeThreshold.BufferPct < 0 || *settings.ResumeThreshold.BufferPct > 100 {
			result.Errors = append(result.Errors, field.Invalid(path.Child("resumeThreshold").Child("bufferPct"), *settings.ResumeThreshold.BufferPct, "must be between 0 and 100"))
		}
	}
	if len(result.Errors) > 0 {
		result.PrimaryReason = conditions.ReasonValidationFailed
	}
	return result
}

//nolint:gocyclo // complex by design
func validateDeliverySettings(path *field.Path, settings *transportv1alpha1.TransportDeliverySettings) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	if settings == nil {
		return result
	}
	if raw := strings.TrimSpace(string(settings.Ordering)); raw != "" {
		switch settings.Ordering {
		case transportv1alpha1.OrderingNone, transportv1alpha1.OrderingPerStream, transportv1alpha1.OrderingPerPartition:
		default:
			result.Errors = append(result.Errors, field.Invalid(path.Child("ordering"), raw, "must be none, per_stream, or per_partition"))
		}
	}
	if raw := strings.TrimSpace(string(settings.Semantics)); raw != "" {
		switch settings.Semantics {
		case transportv1alpha1.DeliveryBestEffort, transportv1alpha1.DeliveryAtLeastOnce:
		default:
			result.Errors = append(result.Errors, field.Invalid(path.Child("semantics"), raw, "must be best_effort or at_least_once"))
		}
	}
	if settings.Replay != nil {
		raw := strings.TrimSpace(string(settings.Replay.Mode))
		if raw != "" {
			switch settings.Replay.Mode {
			case transportv1alpha1.ReplayNone, transportv1alpha1.ReplayMemory, transportv1alpha1.ReplayDurable:
			default:
				result.Errors = append(result.Errors, field.Invalid(path.Child("replay").Child("mode"), raw, "must be none, memory, or durable"))
			}
		}
		if settings.Replay.RetentionSeconds != nil && *settings.Replay.RetentionSeconds < 0 {
			result.Errors = append(result.Errors, field.Invalid(path.Child("replay").Child("retentionSeconds"), *settings.Replay.RetentionSeconds, "must be non-negative"))
		}
		if settings.Replay.CheckpointInterval != nil && strings.TrimSpace(*settings.Replay.CheckpointInterval) != "" {
			if d, err := time.ParseDuration(strings.TrimSpace(*settings.Replay.CheckpointInterval)); err != nil || d <= 0 {
				result.Errors = append(result.Errors, field.Invalid(path.Child("replay").Child("checkpointInterval"), *settings.Replay.CheckpointInterval, "must be a valid positive duration"))
			}
		}
	}
	if len(result.Errors) > 0 {
		result.PrimaryReason = conditions.ReasonValidationFailed
	}
	return result
}

//nolint:gocyclo // complex by design
func validateRoutingSettings(path *field.Path, settings *transportv1alpha1.TransportRoutingSettings) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	if settings == nil {
		return result
	}
	if raw := strings.TrimSpace(string(settings.Mode)); raw != "" {
		switch settings.Mode {
		case transportv1alpha1.TransportRoutingAuto, transportv1alpha1.TransportRoutingHub, transportv1alpha1.TransportRoutingP2P:
		default:
			result.Errors = append(result.Errors, field.Invalid(path.Child("mode"), raw, "must be auto, hub, or p2p"))
		}
	}
	if raw := strings.TrimSpace(string(settings.FanOut)); raw != "" {
		switch settings.FanOut {
		case transportv1alpha1.TransportFanOutSequential, transportv1alpha1.TransportFanOutParallel:
		default:
			result.Errors = append(result.Errors, field.Invalid(path.Child("fanOut"), raw, "must be sequential or parallel"))
		}
	}
	if settings.MaxDownstreams != nil && *settings.MaxDownstreams <= 0 {
		result.Errors = append(result.Errors, field.Invalid(path.Child("maxDownstreams"), *settings.MaxDownstreams, "must be >= 1"))
	}
	for i, rule := range settings.Rules {
		rulePath := path.Child("rules").Index(i)
		if raw := strings.TrimSpace(string(rule.Action)); raw != "" {
			switch rule.Action {
			case transportv1alpha1.TransportRoutingRuleAllow, transportv1alpha1.TransportRoutingRuleDeny:
			default:
				result.Errors = append(result.Errors, field.Invalid(rulePath.Child("action"), raw, "must be allow or deny"))
			}
		}
		if rule.Target == nil || len(rule.Target.Steps) == 0 {
			continue
		}
		seen := make(map[string]struct{}, len(rule.Target.Steps))
		for j, stepName := range rule.Target.Steps {
			stepPath := rulePath.Child("target").Child("steps").Index(j)
			trimmed := strings.TrimSpace(stepName)
			if trimmed == "" {
				result.Errors = append(result.Errors, field.Required(stepPath, "target step name cannot be empty"))
				continue
			}
			if _, ok := seen[trimmed]; ok {
				result.Errors = append(result.Errors, field.Duplicate(stepPath, trimmed))
				continue
			}
			seen[trimmed] = struct{}{}
		}
	}
	if len(result.Errors) > 0 {
		result.PrimaryReason = conditions.ReasonValidationFailed
	}
	return result
}

func validatePartitioningSettings(path *field.Path, settings *transportv1alpha1.TransportPartitioningSettings) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	if settings == nil {
		return result
	}
	if raw := strings.TrimSpace(string(settings.Mode)); raw != "" {
		switch settings.Mode {
		case transportv1alpha1.TransportPartitionNone, transportv1alpha1.TransportPartitionPreserve, transportv1alpha1.TransportPartitionHash:
		default:
			result.Errors = append(result.Errors, field.Invalid(path.Child("mode"), raw, "must be none, preserve, or hash"))
		}
	}
	if settings.Key != nil && strings.TrimSpace(*settings.Key) == "" {
		result.Errors = append(result.Errors, field.Invalid(path.Child("key"), *settings.Key, "must be non-empty when set"))
	}
	if settings.Partitions != nil && *settings.Partitions <= 0 {
		result.Errors = append(result.Errors, field.Invalid(path.Child("partitions"), *settings.Partitions, "must be >= 1"))
	}
	if len(result.Errors) > 0 {
		result.PrimaryReason = conditions.ReasonValidationFailed
	}
	return result
}

func validateLifecycleSettings(path *field.Path, settings *transportv1alpha1.TransportLifecycleSettings) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	if settings == nil {
		return result
	}
	if raw := strings.TrimSpace(string(settings.Strategy)); raw != "" {
		switch settings.Strategy {
		case transportv1alpha1.TransportUpgradeRolling, transportv1alpha1.TransportUpgradeDrainCutover, transportv1alpha1.TransportUpgradeBlueGreen:
		default:
			result.Errors = append(result.Errors, field.Invalid(path.Child("strategy"), raw, "must be rolling, drain_cutover, or blue_green"))
		}
	}
	if settings.DrainTimeoutSeconds != nil && *settings.DrainTimeoutSeconds < 0 {
		result.Errors = append(result.Errors, field.Invalid(path.Child("drainTimeoutSeconds"), *settings.DrainTimeoutSeconds, "must be non-negative"))
	}
	if settings.MaxInFlight != nil && *settings.MaxInFlight < 0 {
		result.Errors = append(result.Errors, field.Invalid(path.Child("maxInFlight"), *settings.MaxInFlight, "must be non-negative"))
	}
	if len(result.Errors) > 0 {
		result.PrimaryReason = conditions.ReasonValidationFailed
	}
	return result
}

func validateObservabilitySettings(path *field.Path, settings *transportv1alpha1.TransportObservabilitySettings) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	if settings == nil || settings.Tracing == nil {
		return result
	}
	if settings.Tracing.SampleRate != nil && (*settings.Tracing.SampleRate < 0 || *settings.Tracing.SampleRate > 100) {
		result.Errors = append(result.Errors, field.Invalid(path.Child("tracing").Child("sampleRate"), *settings.Tracing.SampleRate, "must be between 0 and 100"))
	}
	if settings.Tracing.SamplePolicy != nil && strings.TrimSpace(*settings.Tracing.SamplePolicy) != "" {
		policy := strings.ToLower(strings.TrimSpace(*settings.Tracing.SamplePolicy))
		switch policy {
		case "rate", "random", "always", "never":
		default:
			result.Errors = append(result.Errors, field.Invalid(path.Child("tracing").Child("samplePolicy"), *settings.Tracing.SamplePolicy, "must be rate, random, always, or never"))
		}
	}
	if len(result.Errors) > 0 {
		result.PrimaryReason = conditions.ReasonValidationFailed
	}
	return result
}

func validateRecordingSettings(path *field.Path, settings *transportv1alpha1.TransportRecordingSettings) *ValidationResult {
	result := &ValidationResult{Errors: make(field.ErrorList, 0)}
	if settings == nil {
		return result
	}
	if raw := strings.TrimSpace(string(settings.Mode)); raw != "" {
		switch settings.Mode {
		case transportv1alpha1.TransportRecordingOff, transportv1alpha1.TransportRecordingMetadata, transportv1alpha1.TransportRecordingPayload:
		default:
			result.Errors = append(result.Errors, field.Invalid(path.Child("mode"), raw, "must be off, metadata, or payload"))
		}
	}
	if settings.SampleRate != nil && (*settings.SampleRate < 0 || *settings.SampleRate > 100) {
		result.Errors = append(result.Errors, field.Invalid(path.Child("sampleRate"), *settings.SampleRate, "must be between 0 and 100"))
	}
	if settings.RetentionSeconds != nil && *settings.RetentionSeconds < 0 {
		result.Errors = append(result.Errors, field.Invalid(path.Child("retentionSeconds"), *settings.RetentionSeconds, "must be non-negative"))
	}
	for i, fieldPath := range settings.RedactFields {
		trimmed := strings.TrimSpace(fieldPath)
		if trimmed == "" {
			result.Errors = append(result.Errors, field.Invalid(path.Child("redactFields").Index(i), fieldPath, "redact field cannot be empty"))
		}
	}
	if len(result.Errors) > 0 {
		result.PrimaryReason = conditions.ReasonValidationFailed
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
