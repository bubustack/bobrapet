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

// Package refs contains typed cross-resource reference types for BubuStack.
//
// This package provides a secure, multi-tenant-safe way to reference
// Kubernetes resources across the BubuStack ecosystem. All reference types
// include validation, drift detection, and multi-tenancy controls.
//
// +kubebuilder:object:generate=true
package refs
