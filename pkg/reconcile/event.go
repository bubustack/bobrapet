package reconcile

import (
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RecordEvent emits a Kubernetes event through the provided recorder while
// falling back to structured logging when the recorder is unavailable.
//
// Arguments:
//   - recorder record.EventRecorder: controller-runtime recorder; may be nil in tests.
//   - logger logr.Logger: optional logger used when the recorder is nil; zero-value loggers are ignored.
//   - obj client.Object: the Kubernetes object associated with the event.
//   - eventType string: event type such as corev1.EventTypeWarning.
//   - reason string: short machine-readable reason describing the event.
//   - message string: human-readable message that will be trimmed before emission.
//
// Returns:
//   - None. The helper is best-effort and silently drops empty messages.
func RecordEvent(
	recorder record.EventRecorder,
	logger logr.Logger,
	obj client.Object,
	eventType,
	reason,
	message string,
) {
	if obj == nil {
		return
	}
	trimmed := strings.TrimSpace(message)
	if trimmed == "" {
		return
	}
	if recorder != nil {
		recorder.Event(obj, eventType, reason, trimmed)
		return
	}
	if logger.GetSink() != nil {
		logger.Info(
			"Event recorder unavailable; logging warning instead",
			"eventType", eventType,
			"reason", reason,
			"message", trimmed,
			"object", client.ObjectKeyFromObject(obj),
		)
	}
}

// Emit emits a Kubernetes event if recorder is non-nil.
// It is a small shared wrapper to avoid repeating nil checks.
func Emit(recorder record.EventRecorder, obj runtime.Object, eventType, reason, message string) {
	if recorder == nil || obj == nil {
		return
	}
	recorder.Event(obj, eventType, reason, message)
}
