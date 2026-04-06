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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// CloneServicePorts returns a deep copy of the provided ServicePort slice.
//
// Arguments:
//   - ports []corev1.ServicePort: ports to clone.
//
// Returns:
//   - []corev1.ServicePort: cloned ports, or nil if empty.
func CloneServicePorts(ports []corev1.ServicePort) []corev1.ServicePort {
	if len(ports) == 0 {
		return nil
	}
	out := make([]corev1.ServicePort, len(ports))
	copy(out, ports)
	return out
}

// PreserveImmutableServiceFields copies ClusterIP/IPFamilies metadata from the
// existing Service into the desired Service so patches do not attempt to mutate
// immutable fields.
//
// Arguments:
//   - current *corev1.Service: the existing Service.
//   - desired *corev1.Service: the desired Service to update.
func PreserveImmutableServiceFields(current, desired *corev1.Service) {
	if current == nil || desired == nil {
		return
	}
	desired.Spec.ClusterIP = current.Spec.ClusterIP
	desired.Spec.ClusterIPs = append([]string(nil), current.Spec.ClusterIPs...)
	desired.Spec.IPFamilies = append([]corev1.IPFamily(nil), current.Spec.IPFamilies...)
	desired.Spec.IPFamilyPolicy = current.Spec.IPFamilyPolicy
	desired.Spec.HealthCheckNodePort = current.Spec.HealthCheckNodePort
}

// EnsureService converges the live Service to match the desired spec by
// creating it when absent or patching metadata/spec differences. The returned
// OperationResult matches controllerutil.CreateOrUpdate semantics.
//
// Arguments:
//   - ctx context.Context: context for the operation.
//   - c client.Client: Kubernetes client.
//   - scheme *runtime.Scheme: scheme for setting owner references.
//   - owner client.Object: the owner object for the Service.
//   - desired *corev1.Service: the desired Service specification.
//
// Returns:
//   - *corev1.Service: the resulting Service.
//   - controllerutil.OperationResult: result of the operation.
//   - error: any error encountered.
func EnsureService(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	desired *corev1.Service,
) (*corev1.Service, controllerutil.OperationResult, error) {
	if desired == nil {
		return nil, controllerutil.OperationResultNone, fmt.Errorf("desired Service must not be nil")
	}
	if owner == nil {
		return nil, controllerutil.OperationResultNone, fmt.Errorf(
			"owner must not be nil when ensuring Service %s/%s",
			desired.Namespace,
			desired.Name,
		)
	}

	key := types.NamespacedName{Name: desired.Name, Namespace: desired.Namespace}
	current := &corev1.Service{}
	if err := c.Get(ctx, key, current); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, controllerutil.OperationResultNone, fmt.Errorf("failed to get Service %s: %w", key, err)
		}
		if err := controllerutil.SetControllerReference(owner, desired, scheme); err != nil {
			return nil, controllerutil.OperationResultNone, fmt.Errorf("failed to set Service owner reference: %w", err)
		}
		if err := c.Create(ctx, desired); err != nil {
			return nil, controllerutil.OperationResultNone, fmt.Errorf("failed to create Service %s: %w", key, err)
		}
		return desired, controllerutil.OperationResultCreated, nil
	}

	desiredCopy := desired.DeepCopy()
	PreserveImmutableServiceFields(current, desiredCopy)
	if servicesSemanticallyEqual(current, desiredCopy) {
		return current, controllerutil.OperationResultNone, nil
	}

	original := current.DeepCopy()
	current.Labels = desiredCopy.Labels
	current.Annotations = desiredCopy.Annotations
	current.Spec = desiredCopy.Spec
	if err := c.Patch(ctx, current, client.MergeFrom(original)); err != nil {
		return nil, controllerutil.OperationResultNone, fmt.Errorf("failed to patch Service %s: %w", key, err)
	}
	return current, controllerutil.OperationResultUpdated, nil
}

func servicesSemanticallyEqual(existing, desired *corev1.Service) bool {
	if existing == nil || desired == nil {
		return existing == desired
	}

	existingCopy := existing.DeepCopy()
	desiredCopy := desired.DeepCopy()
	normalizeServiceForComparison(existingCopy)
	normalizeServiceForComparison(desiredCopy)

	if !apiequality.Semantic.DeepEqual(existingCopy.Spec, desiredCopy.Spec) {
		return false
	}
	if !apiequality.Semantic.DeepEqual(existingCopy.Labels, desiredCopy.Labels) {
		return false
	}
	if !apiequality.Semantic.DeepEqual(existingCopy.Annotations, desiredCopy.Annotations) {
		return false
	}
	return true
}

func normalizeServiceForComparison(service *corev1.Service) {
	if service == nil {
		return
	}
	if service.Spec.Type == "" {
		service.Spec.Type = corev1.ServiceTypeClusterIP
	}
	if service.Spec.SessionAffinity == "" {
		service.Spec.SessionAffinity = corev1.ServiceAffinityNone
	}
	if service.Spec.InternalTrafficPolicy == nil && service.Spec.Type != corev1.ServiceTypeExternalName {
		policy := corev1.ServiceInternalTrafficPolicyCluster
		service.Spec.InternalTrafficPolicy = &policy
	}
	for i := range service.Spec.Ports {
		if service.Spec.Ports[i].Protocol == "" {
			service.Spec.Ports[i].Protocol = corev1.ProtocolTCP
		}
	}
}
