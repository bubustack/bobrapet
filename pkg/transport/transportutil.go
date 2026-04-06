package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bubustack/tractatus/envelope"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
)

// MarshalStoryTransports serializes the Story transports slice to JSON.
func MarshalStoryTransports(transports []bubuv1alpha1.StoryTransport) (string, error) {
	if len(transports) == 0 {
		return "", nil
	}
	bytes, err := json.Marshal(transports)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// MarshalStoryTransportStatuses serializes effective transport statuses (including controller-selected mode) to JSON.
func MarshalStoryTransportStatuses(transports []bubuv1alpha1.StoryTransportStatus) (string, error) {
	if len(transports) == 0 {
		return "", nil
	}
	bytes, err := json.Marshal(transports)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// MarshalRuntimeTransportDescriptors serializes Story transports into the
// runtime-facing descriptor contract expected by engrams and transport-aware SDKs.
//
// Behavior:
//   - Returns an empty string when the Story declares no transports.
//   - Resolves descriptor.Kind from the referenced Transport when possible,
//     preferring provider, then driver, then the raw transportRef.
//   - Carries effective mode information from Story status when available.
//   - Includes transportRef and modeReason in descriptor.Config for downstream consumers.
func MarshalRuntimeTransportDescriptors(
	ctx context.Context,
	reader client.Reader,
	story *bubuv1alpha1.Story,
) (string, error) {
	if story == nil || len(story.Spec.Transports) == 0 {
		return "", nil
	}

	statusByName := make(map[string]bubuv1alpha1.StoryTransportStatus, len(story.Status.Transports))
	for _, status := range story.Status.Transports {
		statusByName[status.Name] = status
	}

	descriptors := make([]envelope.TransportDescriptor, 0, len(story.Spec.Transports))
	for _, declared := range story.Spec.Transports {
		descriptor := envelope.TransportDescriptor{
			Name: declared.Name,
			Kind: resolveRuntimeTransportKind(ctx, reader, declared.TransportRef),
		}

		if status, ok := statusByName[declared.Name]; ok {
			descriptor.Mode = string(status.Mode)
			if config := buildRuntimeTransportConfig(declared.TransportRef, status.ModeReason); len(config) > 0 {
				descriptor.Config = config
			}
		} else if config := buildRuntimeTransportConfig(declared.TransportRef, ""); len(config) > 0 {
			descriptor.Config = config
		}

		descriptors = append(descriptors, descriptor)
	}

	bytes, err := json.Marshal(descriptors)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func resolveRuntimeTransportKind(ctx context.Context, reader client.Reader, transportRef string) string {
	ref := strings.TrimSpace(transportRef)
	if ref == "" {
		return ""
	}
	if reader == nil {
		return ref
	}

	var transport transportv1alpha1.Transport
	if err := reader.Get(ctx, types.NamespacedName{Name: ref}, &transport); err != nil {
		return ref
	}
	if provider := strings.TrimSpace(transport.Spec.Provider); provider != "" {
		return provider
	}
	if driver := strings.TrimSpace(transport.Spec.Driver); driver != "" {
		return driver
	}
	return ref
}

func buildRuntimeTransportConfig(transportRef, modeReason string) map[string]any {
	config := map[string]any{}
	if ref := strings.TrimSpace(transportRef); ref != "" {
		config["transportRef"] = ref
	}
	if reason := strings.TrimSpace(modeReason); reason != "" {
		config["modeReason"] = reason
	}
	if len(config) == 0 {
		return nil
	}
	return config
}

// ResolveStoryTransport returns the named transport (or the first available one when name is empty)
// along with its declaration.
func ResolveStoryTransport(
	ctx context.Context,
	reader client.Reader,
	story *bubuv1alpha1.Story,
	transportName string,
) (*transportv1alpha1.Transport, *bubuv1alpha1.StoryTransport, error) {
	if story == nil {
		return nil, nil, fmt.Errorf("story must not be nil")
	}
	if reader == nil {
		return nil, nil, fmt.Errorf("reader must not be nil")
	}
	name := strings.TrimSpace(transportName)
	if len(story.Spec.Transports) == 0 {
		if name == "" {
			return nil, nil, fmt.Errorf("story %s does not declare any transports", story.Name)
		}
		return nil, nil, fmt.Errorf("story %s does not declare transport %q", story.Name, name)
	}
	for i := range story.Spec.Transports {
		decl := &story.Spec.Transports[i]
		if name != "" && decl.Name != name {
			continue
		}
		ref := strings.TrimSpace(decl.TransportRef)
		if ref == "" {
			continue
		}

		var transport transportv1alpha1.Transport
		if err := reader.Get(ctx, types.NamespacedName{Name: ref}, &transport); err != nil {
			return nil, decl, err
		}
		return &transport, decl, nil
	}

	if name != "" {
		return nil, nil, fmt.Errorf("transport %q is not declared on story %s", name, story.Name)
	}

	return nil, nil, fmt.Errorf("story %s has no default transport", story.Name)
}

// ResolveBindingEnvValue fetches a TransportBinding by name and returns its encoded env value.
// Returns the binding name as fallback if the binding cannot be fetched or encoded.
func ResolveBindingEnvValue(ctx context.Context, reader client.Reader, namespace, bindingName string) string {
	var binding transportv1alpha1.TransportBinding
	if err := reader.Get(ctx, types.NamespacedName{Name: bindingName, Namespace: namespace}, &binding); err != nil {
		return bindingName
	}
	value, err := EncodeBindingEnv(&binding)
	if err != nil {
		return bindingName
	}
	return value
}

// EncodeBindingEnv serializes binding metadata into a JSON envelope that can be passed via env.
func EncodeBindingEnv(binding *transportv1alpha1.TransportBinding) (string, error) {
	info := BuildBindingInfo(binding)
	if info == nil {
		return "", fmt.Errorf("binding info is nil")
	}
	ref := coretransport.BindingReference{
		Name:      binding.Name,
		Namespace: binding.Namespace,
	}
	return coretransport.EncodeBindingEnvelope(ref, info)
}

// BuildBindingInfo converts a TransportBinding into the shared BindingInfo proto.
func BuildBindingInfo(binding *transportv1alpha1.TransportBinding) *transportpb.BindingInfo {
	if binding == nil {
		return nil
	}
	endpoint := strings.TrimSpace(binding.Spec.ConnectorEndpoint)
	if endpoint == "" {
		endpoint = strings.TrimSpace(binding.Status.Endpoint)
	}
	info := &transportpb.BindingInfo{
		TransportRef:    strings.TrimSpace(binding.Spec.TransportRef),
		Driver:          strings.TrimSpace(binding.Spec.Driver),
		Endpoint:        endpoint,
		ProtocolVersion: coretransport.ProtocolVersion,
	}
	if binding.Spec.Audio != nil {
		for _, codec := range binding.Spec.Audio.Codecs {
			if name := strings.TrimSpace(codec.Name); name != "" {
				info.AudioCodecs = append(info.AudioCodecs, name)
			}
		}
	}
	if binding.Spec.Video != nil {
		for _, codec := range binding.Spec.Video.Codecs {
			if name := strings.TrimSpace(codec.Name); name != "" {
				info.VideoCodecs = append(info.VideoCodecs, name)
			}
		}
	}
	if binding.Spec.Binary != nil {
		for _, mt := range binding.Spec.Binary.MimeTypes {
			if name := strings.TrimSpace(mt); name != "" {
				info.BinaryTypes = append(info.BinaryTypes, name)
			}
		}
	}
	if binding.Spec.RawSettings != nil && len(binding.Spec.RawSettings.Raw) > 0 {
		info.Payload = append([]byte(nil), binding.Spec.RawSettings.Raw...)
	}
	return info
}
