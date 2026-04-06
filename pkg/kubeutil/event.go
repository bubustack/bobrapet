/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubeutil

import (
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RecordEvent emits a Kubernetes event for obj when a recorder is configured,
// or logs the event metadata when the recorder is nil. Messages are trimmed
// before emission so controllers can pass multi-line errors safely.
//
// Arguments:
//   - recorder events.EventRecorder: events API event recorder (may be nil in tests).
//   - logger logr.Logger: contextual logger used when recorder is nil.
//   - obj client.Object: Kubernetes object receiving the event; required.
//   - eventType string: event type (e.g., corev1.EventTypeWarning).
//   - reason string: short, CamelCase reason for the event.
//   - message string: human-readable description of the event.
//
// Behavior:
//   - No-op if obj is nil or message is empty after trimming.
//   - Emits via recorder.Eventf when recorder is non-nil.
//   - Falls back to logger.Info when recorder is nil.
func RecordEvent(
	recorder events.EventRecorder,
	logger logr.Logger,
	obj client.Object,
	eventType string,
	reason string,
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
		recorder.Eventf(obj, nil, eventType, reason, "Reconcile", trimmed)
		return
	}

	if logger.GetSink() == nil {
		return
	}

	key := client.ObjectKeyFromObject(obj)
	logger.Info(
		"Event recorder unavailable; logging event",
		"eventType", eventType,
		"reason", reason,
		"message", trimmed,
		"namespace", key.Namespace,
		"name", key.Name,
	)
}
