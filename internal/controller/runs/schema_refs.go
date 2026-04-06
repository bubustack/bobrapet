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

package runs

import (
	"fmt"
	"strings"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

func schemaRefEqual(a, b *runsv1alpha1.SchemaReference) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Ref == b.Ref && a.Version == b.Version
}

func resolveSchemaVersion(preferred, fallback string) string {
	if trimmed := strings.TrimSpace(preferred); trimmed != "" {
		return trimmed
	}
	return strings.TrimSpace(fallback)
}

func storySchemaRef(namespace, name, version, suffix string) *runsv1alpha1.SchemaReference {
	return buildSchemaRef("story", namespace, name, version, suffix)
}

func engramSchemaRef(namespace, name, version, suffix string) *runsv1alpha1.SchemaReference {
	return buildSchemaRef("engram", namespace, name, version, suffix)
}

func buildSchemaRef(kind, namespace, name, version, suffix string) *runsv1alpha1.SchemaReference {
	trimmedName := strings.TrimSpace(name)
	if trimmedName == "" {
		return nil
	}
	ref := buildSchemaID(kind, namespace, trimmedName, suffix)
	if ref == "" {
		return nil
	}
	return &runsv1alpha1.SchemaReference{
		Ref:     ref,
		Version: strings.TrimSpace(version),
	}
}

func buildSchemaID(kind, namespace, name, suffix string) string {
	trimmedKind := strings.TrimSpace(kind)
	trimmedSuffix := strings.TrimSpace(suffix)
	if trimmedKind == "" || trimmedSuffix == "" || strings.TrimSpace(name) == "" {
		return ""
	}
	trimmedNamespace := strings.TrimSpace(namespace)
	if trimmedNamespace == "" {
		return fmt.Sprintf("bubu://%s/%s/%s", trimmedKind, name, trimmedSuffix)
	}
	return fmt.Sprintf("bubu://%s/%s/%s/%s", trimmedKind, trimmedNamespace, name, trimmedSuffix)
}
