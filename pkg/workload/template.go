package workload

import (
	"reflect"

	corev1 "k8s.io/api/core/v1"
)

// PodTemplateChanged reports whether two PodTemplateSpec objects differ in the fields
// controllers commonly watch for drift (containers, init containers, volumes, labels,
// annotations, service account name, and image pull secrets). Callers can reuse this
// helper when deciding whether Deployments or StatefulSets need patching.
func PodTemplateChanged(current, desired *corev1.PodTemplateSpec) bool {
	if current == nil || desired == nil {
		return current != desired
	}

	switch {
	case !reflect.DeepEqual(current.Labels, desired.Labels):
		return true
	case !reflect.DeepEqual(current.Annotations, desired.Annotations):
		return true
	case !reflect.DeepEqual(current.Spec.Containers, desired.Spec.Containers):
		return true
	case !reflect.DeepEqual(current.Spec.InitContainers, desired.Spec.InitContainers):
		return true
	case !reflect.DeepEqual(current.Spec.Volumes, desired.Spec.Volumes):
		return true
	case current.Spec.ServiceAccountName != desired.Spec.ServiceAccountName:
		return true
	case !reflect.DeepEqual(current.Spec.ImagePullSecrets, desired.Spec.ImagePullSecrets):
		return true
	default:
		return false
	}
}
