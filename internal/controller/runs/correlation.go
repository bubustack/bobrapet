package runs

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bubustack/core/contracts"
	coreidentity "github.com/bubustack/core/runtime/identity"
)

func correlationIDFromAnnotations(obj metav1.Object) string {
	if obj == nil {
		return ""
	}
	ann := obj.GetAnnotations()
	if ann == nil {
		return ""
	}
	return strings.TrimSpace(ann[contracts.CorrelationIDAnnotation])
}

func correlationLabelValue(correlation string) string {
	trimmed := strings.TrimSpace(correlation)
	if trimmed == "" {
		return ""
	}
	return coreidentity.SafeLabelValue(trimmed)
}

func ensureCorrelationLabel(labels map[string]string, correlation string) bool {
	if labels == nil {
		return false
	}
	value := correlationLabelValue(correlation)
	if value == "" {
		return false
	}
	if labels[contracts.CorrelationIDLabelKey] == value {
		return false
	}
	labels[contracts.CorrelationIDLabelKey] = value
	return true
}
