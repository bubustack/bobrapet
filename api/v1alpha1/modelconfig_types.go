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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// SecretRef defines a reference to a secret with a specific key
type SecretRef struct {
	// Name is the name of the secret
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Key is the key in the secret to reference
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Key string `json:"key"`
}

// FileMount defines a file to be mounted
type FileMount struct {
	// SecretRef references the secret containing the file content
	// +kubebuilder:validation:Required
	SecretRef SecretRef `json:"secretRef"`

	// MountPath is the path where the file should be mounted
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^(/[^/\0]+)+$|^/[^/\0]*$`
	MountPath string `json:"mountPath"`
}

// CredentialSpec defines credentials for a model binding
type CredentialSpec struct {
	// Secrets is a list of secret references to be used for authentication
	// +optional
	Secrets []SecretRef `json:"secrets,omitempty"`

	// Files is a list of file mounts to be used for configuration
	// +optional
	Files []FileMount `json:"files,omitempty"`

	// EnvVars is a map of environment variables to be set in the model container
	// +optional
	EnvVars map[string]string `json:"envVars,omitempty"`
}

// ModelRefSpec defines a structured reference to a model in the catalog
type ModelRefSpec struct {
	// Name is the name of the Model custom resource
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Version is the specific version of the model to use
	// This can be used to pin to a specific model version when multiple
	// versions are available in the model catalog
	// +optional
	Version string `json:"version,omitempty"`
}

// ModelConfigSpec defines the desired state of ModelConfig.
type ModelConfigSpec struct {
	// ModelRef references a predefined Model resource in the cluster
	// This structure contains the name of an existing Model custom resource
	// and optionally a specific version to use
	// +kubebuilder:validation:Required
	ModelRef ModelRefSpec `json:"modelRef"`

	// Endpoint is the API endpoint for the model
	// This allows overriding the default endpoint specified in the Model resource
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=`^(http|https)://.*`
	// +optional
	Endpoint string `json:"endpoint,omitempty"`

	// Credentials contains authentication and configuration details
	// +optional
	Credentials CredentialSpec `json:"credentials,omitempty"`

	// AdditionalConfig holds any supplementary configuration for the model binding
	// +optional
	AdditionalConfig runtime.RawExtension `json:"additionalConfig,omitempty"`
}

// ModelConfigStatus defines the observed state of ModelConfig.
type ModelConfigStatus struct {
	// Conditions represent the latest available observations of the model configuration's state
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// LastUpdated represents the last time the configuration was updated
	// +optional
	LastUpdated *metav1.Time `json:"lastUpdated,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=mc
// +kubebuilder:printcolumn:name="ModelRef",type="string",JSONPath=".spec.modelRef.name"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.modelRef.version"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// ModelConfig is the Schema for the modelconfigs API.
type ModelConfig struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModelConfigSpec   `json:"spec,omitempty"`
	Status ModelConfigStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ModelConfigList contains a list of ModelConfig.
type ModelConfigList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ModelConfig `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ModelConfig{}, &ModelConfigList{})
}
