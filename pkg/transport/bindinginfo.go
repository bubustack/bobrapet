package transport

import (
	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
)

// DecodeBindingInfo extracts BindingInfo proto from the serialized binding env value.
func DecodeBindingInfo(value string) (*transportpb.BindingInfo, error) {
	return coretransport.DecodeBindingInfo(value)
}
