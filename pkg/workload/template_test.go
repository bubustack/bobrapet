package workload

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodTemplateChangedIgnoresSemanticResourceEquality(t *testing.T) {
	t.Parallel()

	current := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{"app": "demo"},
			Annotations: map[string]string{"bubustack.io/story": "story"},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: "runner",
			Containers: []corev1.Container{{
				Name:  "engram",
				Image: "ghcr.io/bubustack/demo:latest",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("100m"),
						corev1.ResourceMemory: resource.MustParse("128Mi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
				},
			}},
		},
	}

	desired := current.DeepCopy()
	desired.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("0.1")
	desired.Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse("1.0")

	require.False(t, PodTemplateChanged(current, desired))
}

func TestPodTemplateChangedIgnoresAPIDefaultedContainerAndVolumeFields(t *testing.T) {
	t.Parallel()

	desired := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{"app": "demo"},
			Annotations: map[string]string{"bubustack.io/story": "story"},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: "runner",
			Containers: []corev1.Container{{
				Name:  "engram",
				Image: "ghcr.io/bubustack/demo:latest",
				Ports: []corev1.ContainerPort{{
					Name:          "grpc",
					ContainerPort: 50051,
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "tls",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: "demo-tls"},
				},
			}},
		},
	}

	current := desired.DeepCopy()
	current.Spec.Containers[0].Ports[0].Protocol = corev1.ProtocolTCP
	current.Spec.Containers[0].TerminationMessagePath = corev1.TerminationMessagePathDefault
	current.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
	mode := corev1.SecretVolumeSourceDefaultMode
	current.Spec.Volumes[0].Secret.DefaultMode = &mode

	require.False(t, PodTemplateChanged(current, desired))
}
