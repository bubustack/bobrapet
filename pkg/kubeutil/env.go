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
	corev1 "k8s.io/api/core/v1"

	"github.com/bubustack/core/contracts"
)

// ConfigEnvVars returns a slice containing the BUBU_STEP_CONFIG env var when
// raw is non-empty, otherwise it returns nil.
//
// Arguments:
//   - raw []byte: JSON configuration bytes.
//
// Returns:
//   - []corev1.EnvVar: slice containing the config env var, or nil if raw is empty.
func ConfigEnvVars(raw []byte) []corev1.EnvVar {
	if len(raw) == 0 {
		return nil
	}
	return []corev1.EnvVar{
		{Name: contracts.StepConfigEnv, Value: string(raw)},
	}
}

// AppendConfigEnvVar appends the BUBU_STEP_CONFIG env var to envVars when raw
// is non-empty.
//
// Arguments:
//   - envVars *[]corev1.EnvVar: pointer to the env var slice to append to.
//   - raw []byte: JSON configuration bytes.
//
// Side Effects:
//   - Appends to *envVars if raw is non-empty and envVars is non-nil.
func AppendConfigEnvVar(envVars *[]corev1.EnvVar, raw []byte) {
	if envVars == nil || len(raw) == 0 {
		return
	}
	*envVars = append(*envVars, corev1.EnvVar{Name: contracts.StepConfigEnv, Value: string(raw)})
}
