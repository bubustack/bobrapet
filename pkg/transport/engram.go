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

package transport

import (
	"context"
	"fmt"
	"strings"

	"github.com/bubustack/core/contracts"
	coretransport "github.com/bubustack/core/runtime/transport"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MaxReadyMessageLength is the maximum length for transport readiness messages.
const MaxReadyMessageLength = 512

// EngramAnnotationPatcher provides methods to patch Engram transport annotations.
// This abstraction allows both bobrapet and bobravoz-grpc to share annotation
// logic while using their own Engram types.
type EngramAnnotationPatcher struct {
	// Client is the Kubernetes client for fetching and patching Engrams.
	Client client.Client
	// Logger is used for debug and error logging.
	Logger logr.Logger
}

// NewEngramAnnotationPatcher creates a new patcher with the given client and logger.
func NewEngramAnnotationPatcher(c client.Client, log logr.Logger) *EngramAnnotationPatcher {
	return &EngramAnnotationPatcher{
		Client: c,
		Logger: log,
	}
}

// PatchBindingRef patches the TransportBindingAnnotation on an Engram.
//
// Behavior:
//   - Sanitizes the binding value using coretransport.SanitizeBindingAnnotationValue.
//   - Only patches if the annotation value has changed.
//   - Returns nil if the Engram is not found (deleted before patch).
//
// Arguments:
//   - ctx context.Context: context for API calls.
//   - namespace string: the Engram's namespace.
//   - name string: the Engram's name.
//   - bindingValue string: the raw binding env value to sanitize and store.
//   - fetchEngram func(ctx context.Context, key types.NamespacedName) (client.Object, error):
//     function to fetch the Engram object.
//
// Returns:
//   - bool: true if the annotation was patched, false if unchanged or not found.
//   - error: non-nil on fetch/patch failure.
func (p *EngramAnnotationPatcher) PatchBindingRef(
	ctx context.Context,
	namespace, name string,
	bindingValue string,
	fetchEngram func(ctx context.Context, key types.NamespacedName) (client.Object, error),
) (bool, error) {
	bindingValue = strings.TrimSpace(bindingValue)
	if bindingValue == "" {
		return false, nil
	}

	sanitized := coretransport.SanitizeBindingAnnotationValue(bindingValue)
	if sanitized == "" {
		p.Logger.Info("Skipping transport annotation; unable to sanitize binding value",
			"namespace", namespace, "engram", name)
		return false, nil
	}

	key := types.NamespacedName{Namespace: namespace, Name: name}
	engram, err := fetchEngram(ctx, key)
	if err != nil {
		if apierrors.IsNotFound(err) {
			p.Logger.V(1).Info("Engram not found; skipping binding annotation",
				"namespace", namespace, "engram", name)
			return false, nil
		}
		return false, fmt.Errorf("fetch Engram %s/%s: %w", namespace, name, err)
	}

	annotations := engram.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	if annotations[contracts.TransportBindingAnnotation] == sanitized {
		p.Logger.V(1).Info("Engram binding annotation already up to date",
			"namespace", namespace, "engram", name, "binding", sanitized)
		return false, nil
	}

	beforeObj := engram.DeepCopyObject()
	before, ok := beforeObj.(client.Object)
	if !ok {
		return false, fmt.Errorf("engram %s/%s: DeepCopyObject did not return client.Object", namespace, name)
	}
	annotations[contracts.TransportBindingAnnotation] = sanitized
	engram.SetAnnotations(annotations)

	if err := p.Client.Patch(ctx, engram, client.MergeFrom(before)); err != nil {
		return false, fmt.Errorf("patch Engram %s/%s binding annotation: %w", namespace, name, err)
	}

	p.Logger.Info("Patched Engram binding annotation",
		"namespace", namespace, "engram", name, "binding", sanitized)
	return true, nil
}

// PatchReadyStatus patches the TransportReadyAnnotation and TransportReadyMessageAnnotation.
//
// Behavior:
//   - Truncates message to MaxReadyMessageLength runes.
//   - Only patches if ready flag or message has changed.
//   - Returns nil if the Engram is not found (deleted before patch).
//
// Arguments:
//   - ctx context.Context: context for API calls.
//   - namespace string: the Engram's namespace.
//   - name string: the Engram's name.
//   - ready bool: the transport readiness state.
//   - message string: the status message (will be trimmed and truncated).
//   - fetchEngram func(ctx context.Context, key types.NamespacedName) (client.Object, error):
//     function to fetch the Engram object.
//
// Returns:
//   - bool: true if annotations were patched, false if unchanged or not found.
//   - error: non-nil on fetch/patch failure.
func (p *EngramAnnotationPatcher) PatchReadyStatus(
	ctx context.Context,
	namespace, name string,
	ready bool,
	message string,
	fetchEngram func(ctx context.Context, key types.NamespacedName) (client.Object, error),
) (bool, error) {
	if name == "" {
		return false, nil
	}

	key := types.NamespacedName{Namespace: namespace, Name: name}
	engram, err := fetchEngram(ctx, key)
	if err != nil {
		if apierrors.IsNotFound(err) {
			p.Logger.V(1).Info("Engram not found; skipping readiness annotation",
				"namespace", namespace, "engram", name)
			return false, nil
		}
		return false, fmt.Errorf("fetch Engram %s/%s: %w", namespace, name, err)
	}

	annotations := engram.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}

	readyValue := "false"
	if ready {
		readyValue = "true"
	}

	trimmedMessage := TruncateMessage(message, MaxReadyMessageLength)

	currentReady := annotations[contracts.TransportReadyAnnotation]
	currentMessage := strings.TrimSpace(annotations[contracts.TransportReadyMessageAnnotation])
	if currentReady == readyValue && currentMessage == trimmedMessage {
		p.Logger.V(1).Info("Engram transport readiness already up to date",
			"namespace", namespace, "engram", name, "ready", readyValue)
		return false, nil
	}

	beforeObj := engram.DeepCopyObject()
	before, ok := beforeObj.(client.Object)
	if !ok {
		return false, fmt.Errorf("engram %s/%s: DeepCopyObject did not return client.Object", namespace, name)
	}
	annotations[contracts.TransportReadyAnnotation] = readyValue
	if trimmedMessage == "" {
		delete(annotations, contracts.TransportReadyMessageAnnotation)
	} else {
		annotations[contracts.TransportReadyMessageAnnotation] = trimmedMessage
	}
	engram.SetAnnotations(annotations)

	if err := p.Client.Patch(ctx, engram, client.MergeFrom(before)); err != nil {
		return false, fmt.Errorf("patch Engram %s/%s readiness annotation: %w", namespace, name, err)
	}

	p.Logger.Info("Patched Engram transport readiness",
		"namespace", namespace, "engram", name, "ready", readyValue, "message", trimmedMessage)
	return true, nil
}

// TruncateMessage trims whitespace and truncates to maxRunes.
func TruncateMessage(message string, maxRunes int) string {
	trimmed := strings.TrimSpace(message)
	if trimmed == "" {
		return ""
	}
	runes := []rune(trimmed)
	if len(runes) > maxRunes {
		return string(runes[:maxRunes])
	}
	return trimmed
}
