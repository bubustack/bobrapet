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
	"bytes"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

func TestCRDSpecRootsAreRequired(t *testing.T) {
	t.Helper()

	crds := loadCRDs(t)
	for _, crd := range crds {
		for _, version := range crd.Spec.Versions {
			schema := version.Schema.OpenAPIV3Schema
			if schema == nil {
				t.Fatalf("%s/%s has no OpenAPIv3 schema", crd.Name, version.Name)
			}
			if !contains(schema.Required, "spec") {
				t.Fatalf("%s/%s schema no longer requires spec", crd.Name, version.Name)
			}
			spec, ok := schema.Properties["spec"]
			if !ok {
				t.Fatalf("%s/%s schema lost spec property", crd.Name, version.Name)
			}
			if spec.Nullable {
				t.Fatalf("%s/%s spec property became nullable", crd.Name, version.Name)
			}
		}
	}
}

func TestCRDStatusConditionsAreMapKeys(t *testing.T) {
	t.Helper()

	crds := loadCRDs(t)
	for _, crd := range crds {
		for _, version := range crd.Spec.Versions {
			schema := version.Schema.OpenAPIV3Schema
			if schema == nil {
				t.Fatalf("%s/%s has no OpenAPIv3 schema", crd.Name, version.Name)
			}
			status, ok := schema.Properties["status"]
			if !ok {
				continue
			}
			conditions, ok := status.Properties["conditions"]
			if !ok {
				continue
			}
			if conditions.XListType == nil || *conditions.XListType != "map" {
				t.Fatalf("%s/%s conditions list lost map semantics (x-kubernetes-list-type)", crd.Name, version.Name)
			}
			if !contains(conditions.XListMapKeys, "type") {
				t.Fatalf("%s/%s conditions list lost map key metadata (x-kubernetes-list-map-keys)", crd.Name, version.Name)
			}
		}
	}
}

func loadCRDs(t *testing.T) []*apiextensionsv1.CustomResourceDefinition {
	t.Helper()

	dir := filepath.Join("..", "..", "config", "crd", "bases")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("failed to list %s: %v", dir, err)
	}

	var crds []*apiextensionsv1.CustomResourceDefinition
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("failed to read %s: %v", entry.Name(), err)
		}
		decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
		for {
			var crd apiextensionsv1.CustomResourceDefinition
			if err := decoder.Decode(&crd); err != nil {
				if err == io.EOF {
					break
				}
				t.Fatalf("failed to decode %s: %v", entry.Name(), err)
			}
			if crd.Name == "" {
				continue
			}
			crds = append(crds, &crd)
		}
	}
	if len(crds) == 0 {
		t.Fatalf("no CRDs decoded from %s", dir)
	}
	return crds
}

func contains(values []string, target string) bool {
	return slices.Contains(values, target)
}
