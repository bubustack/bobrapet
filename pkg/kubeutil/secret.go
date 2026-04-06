/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package kubeutil

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	managedStorageSecretAnnotation       = "bubustack.io/managed-storage-secret"
	managedStorageSecretSourceAnnotation = "bubustack.io/managed-storage-secret-source"
)

// SecretCopyAuthorization constrains which Secrets may be copied across
// namespaces by EnsureSecretCopy.
type SecretCopyAuthorization struct {
	AllowedNames []string
}

// AllowSecretCopyNames authorizes cross-namespace copies for the provided
// Secret names only.
func AllowSecretCopyNames(names ...string) SecretCopyAuthorization {
	allowed := make([]string, 0, len(names))
	for _, name := range names {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			allowed = append(allowed, trimmed)
		}
	}
	return SecretCopyAuthorization{AllowedNames: allowed}
}

// EnsureSecretCopy ensures a Secret exists in targetNamespace, copying data from sourceNamespace.
//
// Behavior:
//   - Returns early when name is empty or namespaces are equal.
//   - Requires explicit authorization for cross-namespace copies.
//   - Creates the target Secret if missing, marking it as managed.
//   - Updates the target Secret only when it is marked as managed.
//
// Arguments:
//   - ctx context.Context: request-scoped context.
//   - reader client.Reader: reader for source Secret (use APIReader if available).
//   - writer client.Client: writer for creating/updating the target Secret.
//   - sourceNamespace string: namespace holding the source Secret.
//   - targetNamespace string: namespace to receive the Secret copy.
//   - name string: Secret name.
//   - auth SecretCopyAuthorization: explicit cross-namespace authorization policies.
//
// Returns:
//   - error: on read/create/update failures.
func EnsureSecretCopy(
	ctx context.Context,
	reader client.Reader,
	writer client.Client,
	sourceNamespace string,
	targetNamespace string,
	name string,
	auth ...SecretCopyAuthorization,
) error {
	name = strings.TrimSpace(name)
	sourceNamespace = strings.TrimSpace(sourceNamespace)
	targetNamespace = strings.TrimSpace(targetNamespace)
	if name == "" || sourceNamespace == "" || targetNamespace == "" || sourceNamespace == targetNamespace {
		return nil
	}
	if !isSecretCopyAuthorized(name, auth...) {
		return fmt.Errorf("cross-namespace secret copy denied for %q: secret is not explicitly authorized", name)
	}
	expectedSource := storageSecretSourceKey(sourceNamespace, name)

	var source corev1.Secret
	if err := reader.Get(ctx, types.NamespacedName{Namespace: sourceNamespace, Name: name}, &source); err != nil {
		return err
	}

	var target corev1.Secret
	err := writer.Get(ctx, types.NamespacedName{Namespace: targetNamespace, Name: name}, &target)
	if apierrors.IsNotFound(err) {
		copy := corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: targetNamespace,
				Annotations: map[string]string{
					managedStorageSecretAnnotation:       "true",
					managedStorageSecretSourceAnnotation: expectedSource,
				},
			},
			Type: source.Type,
			Data: source.Data,
		}
		return writer.Create(ctx, &copy)
	}
	if err != nil {
		return err
	}

	if err := validateManagedSecretCopyTarget(&target, expectedSource); err != nil {
		return err
	}

	if target.Type == source.Type && reflect.DeepEqual(target.Data, source.Data) {
		return nil
	}

	patch := client.MergeFrom(target.DeepCopy())
	target.Type = source.Type
	target.Data = source.Data
	if target.Annotations == nil {
		target.Annotations = map[string]string{}
	}
	target.Annotations[managedStorageSecretSourceAnnotation] = expectedSource
	return writer.Patch(ctx, &target, patch)
}

func validateManagedSecretCopyTarget(target *corev1.Secret, expectedSource string) error {
	if target == nil {
		return fmt.Errorf("cross-namespace secret copy denied: target secret is nil")
	}
	if target.Annotations == nil || target.Annotations[managedStorageSecretAnnotation] != "true" {
		return fmt.Errorf("cross-namespace secret copy denied for %q in namespace %q: existing target secret is not controller-managed", target.Name, target.Namespace) //nolint:lll
	}
	if actual := strings.TrimSpace(target.Annotations[managedStorageSecretSourceAnnotation]); actual != expectedSource {
		return fmt.Errorf("cross-namespace secret copy denied for %q in namespace %q: existing target secret is managed for source %q, expected %q", target.Name, target.Namespace, actual, expectedSource) //nolint:lll
	}
	return nil
}

func storageSecretSourceKey(sourceNamespace, name string) string {
	return sourceNamespace + "/" + name
}

func isSecretCopyAuthorized(name string, auth ...SecretCopyAuthorization) bool {
	for _, policy := range auth {
		if slices.Contains(policy.AllowedNames, name) {
			return true
		}
	}
	return false
}
