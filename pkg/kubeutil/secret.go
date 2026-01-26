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
	"reflect"

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

// EnsureSecretCopy ensures a Secret exists in targetNamespace, copying data from sourceNamespace.
//
// Behavior:
//   - Returns early when name is empty or namespaces are equal.
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
) error {
	if name == "" || sourceNamespace == "" || targetNamespace == "" || sourceNamespace == targetNamespace {
		return nil
	}

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
					managedStorageSecretSourceAnnotation: sourceNamespace + "/" + name,
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

	if target.Annotations == nil || target.Annotations[managedStorageSecretAnnotation] != "true" {
		return nil
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
	target.Annotations[managedStorageSecretSourceAnnotation] = sourceNamespace + "/" + name
	return writer.Patch(ctx, &target, patch)
}
