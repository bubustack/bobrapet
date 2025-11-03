package secretutil

import (
	"fmt"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	corev1 "k8s.io/api/core/v1"
)

// Artifacts captures the Kubernetes objects required to surface secrets inside pods.
type Artifacts struct {
	EnvVars      []corev1.EnvVar
	EnvFrom      []corev1.EnvFromSource
	Volumes      []corev1.Volume
	VolumeMounts []corev1.VolumeMount
}

// BuildArtifacts converts template secret definitions combined with resolved user mappings
// into Kubernetes primitives that can be attached to a pod.
func BuildArtifacts(secretSchema map[string]catalogv1alpha1.SecretDefinition, mappings map[string]string) Artifacts {
	if len(mappings) == 0 || len(secretSchema) == 0 {
		return Artifacts{}
	}

	result := Artifacts{}
	for logicalName, actualSecretName := range mappings {
		definition, ok := secretSchema[logicalName]
		if !ok {
			continue
		}
		sdkSecretKey := fmt.Sprintf("BUBU_SECRET_%s", logicalName)
		applySecret(&result, logicalName, actualSecretName, sdkSecretKey, definition)
	}
	return result
}

func applySecret(artifacts *Artifacts, logicalName, actualSecretName, sdkSecretKey string, definition catalogv1alpha1.SecretDefinition) {
	switch definition.MountType {
	case enums.SecretMountTypeFile:
		applyFileSecret(artifacts, logicalName, actualSecretName, sdkSecretKey, definition)
	case enums.SecretMountTypeEnv:
		applyEnvSecret(artifacts, logicalName, actualSecretName, sdkSecretKey, definition, true, true)
	case enums.SecretMountTypeBoth:
		applyFileSecret(artifacts, logicalName, actualSecretName, sdkSecretKey, definition)
		applyEnvSecret(artifacts, logicalName, actualSecretName, sdkSecretKey, definition, true, false)
	default:
		applyEnvSecret(artifacts, logicalName, actualSecretName, sdkSecretKey, definition, true, true)
	}
}

func applyFileSecret(artifacts *Artifacts, logicalName, actualSecretName, sdkSecretKey string, definition catalogv1alpha1.SecretDefinition) {
	volumeName := fmt.Sprintf("secret-%s", logicalName)
	mountPath := definition.MountPath
	if mountPath == "" {
		mountPath = fmt.Sprintf("/etc/bubu/secrets/%s", logicalName)
	}

	artifacts.Volumes = append(artifacts.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: actualSecretName},
		},
	})
	artifacts.VolumeMounts = append(artifacts.VolumeMounts, corev1.VolumeMount{
		Name:      volumeName,
		MountPath: mountPath,
		ReadOnly:  true,
	})
	artifacts.EnvVars = append(artifacts.EnvVars,
		corev1.EnvVar{Name: sdkSecretKey, Value: fmt.Sprintf("file:%s", mountPath)},
	)
}

func applyEnvSecret(
	artifacts *Artifacts,
	logicalName,
	actualSecretName,
	sdkSecretKey string,
	definition catalogv1alpha1.SecretDefinition,
	exposeNameForPrefix bool,
	exposeNameForKeys bool,
) {
	if len(definition.ExpectedKeys) > 0 {
		addExplicitKeyEnvVars(artifacts, logicalName, actualSecretName, sdkSecretKey, definition, exposeNameForKeys)
		return
	}
	prefix := resolvePrefix(logicalName, definition)
	artifacts.EnvFrom = append(artifacts.EnvFrom, corev1.EnvFromSource{
		Prefix:    prefix,
		SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: actualSecretName}},
	})
	artifacts.EnvVars = append(artifacts.EnvVars,
		corev1.EnvVar{Name: sdkSecretKey, Value: fmt.Sprintf("env:%s", prefix)},
	)
	if exposeNameForPrefix {
		artifacts.EnvVars = append(artifacts.EnvVars,
			corev1.EnvVar{Name: fmt.Sprintf("%s_NAME", sdkSecretKey), Value: actualSecretName},
		)
	}
}

func addExplicitKeyEnvVars(artifacts *Artifacts, logicalName, actualSecretName, sdkSecretKey string, definition catalogv1alpha1.SecretDefinition, exposeSecretName bool) {
	prefix := definition.EnvPrefix
	if prefix == "" {
		prefix = fmt.Sprintf("%s_", logicalName)
	}
	for _, key := range definition.ExpectedKeys {
		artifacts.EnvVars = append(artifacts.EnvVars, corev1.EnvVar{
			Name: prefix + key,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: actualSecretName},
					Key:                  key,
				},
			},
		})
	}

	sdkValue := fmt.Sprintf("env:%s", definition.EnvPrefix)
	if definition.EnvPrefix == "" {
		sdkValue = fmt.Sprintf("env:%s_", logicalName)
	}
	artifacts.EnvVars = append(artifacts.EnvVars, corev1.EnvVar{Name: sdkSecretKey, Value: sdkValue})

	if exposeSecretName {
		artifacts.EnvVars = append(artifacts.EnvVars,
			corev1.EnvVar{Name: fmt.Sprintf("%s_NAME", sdkSecretKey), Value: actualSecretName},
		)
	}
}

func resolvePrefix(logicalName string, definition catalogv1alpha1.SecretDefinition) string {
	if definition.EnvPrefix != "" {
		return definition.EnvPrefix
	}
	return fmt.Sprintf("%s_", logicalName)
}
