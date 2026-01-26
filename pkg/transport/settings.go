package transport

import (
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"k8s.io/apimachinery/pkg/runtime"
)

// MergeSettings merges the default transport settings with overrides, returning a raw JSON blob.
func MergeSettings(base, overrides *runtime.RawExtension) ([]byte, error) {
	merged, err := kubeutil.MergeWithBlocks(base, overrides)
	if err != nil {
		return nil, err
	}
	if merged == nil || len(merged.Raw) == 0 {
		return nil, nil
	}
	return append([]byte(nil), merged.Raw...), nil
}
