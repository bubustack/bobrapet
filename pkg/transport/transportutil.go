package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

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

// ResolveBindingEnvValue fetches the TransportBinding and returns an encoded env payload.
func ResolveBindingEnvValue(
	ctx context.Context,
	reader client.Reader,
	namespace,
	bindingName string,
) string {
	bindingName = strings.TrimSpace(bindingName)
	if bindingName == "" {
		return ""
	}
	if reader == nil || namespace == "" {
		return bindingName
	}
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
		TransportRef: strings.TrimSpace(binding.Spec.TransportRef),
		Driver:       strings.TrimSpace(binding.Spec.Driver),
		Endpoint:     endpoint,
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
