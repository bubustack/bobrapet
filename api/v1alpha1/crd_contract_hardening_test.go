package v1alpha1

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
)

//nolint:gocyclo // complex by design
func TestRootSpecFieldsAreRequired(t *testing.T) {
	t.Parallel()

	specEntries := []struct {
		name     string
		path     string
		typeName string
	}{
		{"Story", "story_types.go", "Story"},
		{"Engram", "engram_types.go", "Engram"},
		{"Impulse", "impulse_types.go", "Impulse"},
		{"StoryRun", filepath.Join("..", "runs", "v1alpha1", "storyrun_types.go"), "StoryRun"},
		{"StepRun", filepath.Join("..", "runs", "v1alpha1", "steprun_types.go"), "StepRun"},
		{"EngramTemplate", filepath.Join("..", "catalog", "v1alpha1", "engramtemplate_types.go"), "EngramTemplate"},
		{"ImpulseTemplate", filepath.Join("..", "catalog", "v1alpha1", "impulsetemplate_types.go"), "ImpulseTemplate"},
		{"Transport", filepath.Join("..", "transport", "v1alpha1", "transport_types.go"), "Transport"},
	}

	for _, entry := range specEntries {
		t.Run(entry.name, func(t *testing.T) {
			t.Helper()
			t.Parallel()

			fset := token.NewFileSet()
			node, err := parser.ParseFile(fset, entry.path, nil, parser.ParseComments)
			if err != nil {
				t.Fatalf("parse %s: %v", entry.path, err)
			}
			found := false
			for _, decl := range node.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok {
					continue
				}
				for _, spec := range gen.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok || typeSpec.Name.Name != entry.typeName {
						continue
					}
					structType, ok := typeSpec.Type.(*ast.StructType)
					if !ok {
						continue
					}
					for _, field := range structType.Fields.List {
						if len(field.Names) == 0 {
							continue
						}
						if field.Names[0].Name != "Spec" {
							continue
						}
						found = true
						if field.Tag == nil {
							t.Fatalf("spec field in %s lacks tags", entry.name)
						}
						tag := reflect.StructTag(strings.Trim(field.Tag.Value, "`"))
						jsonTag := tag.Get("json")
						if strings.Contains(jsonTag, "omitempty") {
							t.Fatalf("spec field in %s still uses json:\"spec,omitempty\": %q", entry.name, jsonTag)
						}
						if !strings.HasPrefix(jsonTag, "spec") {
							t.Fatalf("spec field in %s unexpected json tag %q", entry.name, jsonTag)
						}
					}
				}
			}
			if !found {
				t.Fatalf("spec field not found for %s", entry.name)
			}
		})
	}
}

func TestStatusConditionsUseListMap(t *testing.T) {
	t.Parallel()

	crdEntries := []struct {
		name    string
		path    string
		version string
	}{
		{"Story", filepath.Join("..", "..", "config", "crd", "bases", "bubustack.io_stories.yaml"), "v1alpha1"},
		{"Engram", filepath.Join("..", "..", "config", "crd", "bases", "bubustack.io_engrams.yaml"), "v1alpha1"},
		{"Impulse", filepath.Join("..", "..", "config", "crd", "bases", "bubustack.io_impulses.yaml"), "v1alpha1"},
		{"StoryRun", filepath.Join("..", "..", "config", "crd", "bases", "runs.bubustack.io_storyruns.yaml"), "v1alpha1"},
		{"StepRun", filepath.Join("..", "..", "config", "crd", "bases", "runs.bubustack.io_stepruns.yaml"), "v1alpha1"},
		{"EngramTemplate", filepath.Join("..", "..", "config", "crd", "bases", "catalog.bubustack.io_engramtemplates.yaml"), "v1alpha1"},
		{"ImpulseTemplate", filepath.Join("..", "..", "config", "crd", "bases", "catalog.bubustack.io_impulsetemplates.yaml"), "v1alpha1"},
		{"Transport", filepath.Join("..", "..", "config", "crd", "bases", "transport.bubustack.io_transports.yaml"), "v1alpha1"},
	}

	for _, entry := range crdEntries {
		t.Run(entry.name, func(t *testing.T) {
			t.Helper()
			t.Parallel()
			crd := loadCRD(t, entry.path)
			schema := findVersionSchema(t, crd, entry.version)

			statusSchema, ok := schema.Properties["status"]
			if !ok {
				t.Fatalf("status schema missing in %s", entry.name)
			}
			conditionsSchema, ok := statusSchema.Properties["conditions"]
			if !ok {
				t.Fatalf("status.conditions missing in %s", entry.name)
			}
			if conditionsSchema.XListType == nil || *conditionsSchema.XListType != "map" {
				t.Fatalf("status.conditions in %s missing x-kubernetes-list-type=map", entry.name)
			}
			if !containsString(conditionsSchema.XListMapKeys, "type") {
				t.Fatalf("status.conditions in %s missing x-kubernetes-list-map-keys=type", entry.name)
			}
		})
	}
}

func loadCRD(t *testing.T, path string) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(content), 4096)
	if err := decoder.Decode(&crd); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return &crd
}

func findVersionSchema(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition, version string) *apiextensionsv1.JSONSchemaProps {
	t.Helper()
	for _, v := range crd.Spec.Versions {
		if v.Name != version || v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
			continue
		}
		return v.Schema.OpenAPIV3Schema
	}
	t.Fatalf("version %s not found in CRD %s", version, crd.Name)
	return nil
}

func containsString(slice []string, value string) bool {
	return slices.Contains(slice, value)
}
