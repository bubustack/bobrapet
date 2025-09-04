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

package controller

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// SecondaryResource defines a secondary resource watch
// This type is used by controllers for secondary resource watch configuration
type SecondaryResource struct {
	Object    client.Object
	Handler   handler.EventHandler
	Predicate predicate.Predicate
}

// FieldIndex defines a field index for efficient querying
// This type is used by controllers for field index configuration
type FieldIndex struct {
	Object       client.Object
	Field        string
	ExtractValue func(client.Object) []string
}

// Note: WatchBuilder and related configuration functions removed
// Controllers implement their watch setup directly for better control and clarity
// This eliminates unused abstraction layers and reduces complexity
