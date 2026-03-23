package refs

import (
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// RequestsFromNames converts a list of resource names into reconcile.Request
// objects while trimming whitespace and removing duplicates. Empty or blank
// names are ignored, and the helper returns nil when no valid entries remain.
func RequestsFromNames(names ...string) []reconcile.Request {
	if len(names) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(names))
	requests := make([]reconcile.Request, 0, len(names))
	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: trimmed},
		})
	}
	if len(requests) == 0 {
		return nil
	}
	return requests
}
