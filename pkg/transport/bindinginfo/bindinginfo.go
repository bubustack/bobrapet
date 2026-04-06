package bindinginfo

import (
	"encoding/json"
	"fmt"
	"strings"

	coretransport "github.com/bubustack/core/runtime/transport"
	transportpb "github.com/bubustack/tractatus/gen/go/proto/transport/v1"
)

// Decode extracts BindingInfo from the serialized binding env value.
func Decode(value string) (*transportpb.BindingInfo, error) {
	return coretransport.DecodeBindingInfo(value)
}

// EnvOverrides parses the serialized payload for env overrides (if any).
func EnvOverrides(info *transportpb.BindingInfo) map[string]string {
	if info == nil || len(info.Payload) == 0 {
		return nil
	}
	var payload struct {
		Env map[string]any `json:"env"`
	}
	if err := json.Unmarshal(info.Payload, &payload); err != nil || len(payload.Env) == 0 {
		return nil
	}
	out := make(map[string]string, len(payload.Env))
	for key, raw := range payload.Env {
		name := strings.TrimSpace(key)
		if name == "" {
			continue
		}
		switch v := raw.(type) {
		case string:
			out[name] = v
		case fmt.Stringer:
			out[name] = v.String()
		default:
			out[name] = fmt.Sprint(v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
