package workload

import (
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
)

// PodTemplateChanged reports whether two PodTemplateSpec objects differ in the fields
// controllers commonly watch for drift (containers, init containers, volumes, labels,
// annotations, service account name, and image pull secrets). Callers can reuse this
// helper when deciding whether Deployments or StatefulSets need patching.
func PodTemplateChanged(current, desired *corev1.PodTemplateSpec) bool {
	if current == nil || desired == nil {
		return current != desired
	}

	currentCopy := current.DeepCopy()
	desiredCopy := desired.DeepCopy()
	normalizePodTemplateForComparison(currentCopy)
	normalizePodTemplateForComparison(desiredCopy)

	switch {
	case !apiequality.Semantic.DeepEqual(currentCopy.Labels, desiredCopy.Labels):
		return true
	case !apiequality.Semantic.DeepEqual(currentCopy.Annotations, desiredCopy.Annotations):
		return true
	case !apiequality.Semantic.DeepEqual(currentCopy.Spec.Containers, desiredCopy.Spec.Containers):
		return true
	case !apiequality.Semantic.DeepEqual(currentCopy.Spec.InitContainers, desiredCopy.Spec.InitContainers):
		return true
	case !apiequality.Semantic.DeepEqual(currentCopy.Spec.Volumes, desiredCopy.Spec.Volumes):
		return true
	case currentCopy.Spec.ServiceAccountName != desiredCopy.Spec.ServiceAccountName:
		return true
	case !apiequality.Semantic.DeepEqual(currentCopy.Spec.ImagePullSecrets, desiredCopy.Spec.ImagePullSecrets):
		return true
	default:
		return false
	}
}

func normalizePodTemplateForComparison(template *corev1.PodTemplateSpec) {
	if template == nil {
		return
	}
	normalizePodTemplateContainers(template.Spec.Containers)
	normalizePodTemplateContainers(template.Spec.InitContainers)
	normalizePodTemplateVolumes(template.Spec.Volumes)
}

func normalizePodTemplateContainers(containers []corev1.Container) {
	for i := range containers {
		container := &containers[i]
		if container.TerminationMessagePath == "" {
			container.TerminationMessagePath = corev1.TerminationMessagePathDefault
		}
		if container.TerminationMessagePolicy == "" {
			container.TerminationMessagePolicy = corev1.TerminationMessageReadFile
		}
		for j := range container.Ports {
			if container.Ports[j].Protocol == "" {
				container.Ports[j].Protocol = corev1.ProtocolTCP
			}
		}
	}
}

func normalizePodTemplateVolumes(volumes []corev1.Volume) {
	for i := range volumes {
		if secret := volumes[i].Secret; secret != nil && secret.DefaultMode == nil {
			mode := corev1.SecretVolumeSourceDefaultMode
			secret.DefaultMode = &mode
		}
		if configMap := volumes[i].ConfigMap; configMap != nil && configMap.DefaultMode == nil {
			mode := corev1.ConfigMapVolumeSourceDefaultMode
			configMap.DefaultMode = &mode
		}
		if projected := volumes[i].Projected; projected != nil && projected.DefaultMode == nil {
			mode := corev1.ProjectedVolumeSourceDefaultMode
			projected.DefaultMode = &mode
		}
	}
}
