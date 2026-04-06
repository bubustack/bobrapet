package controller

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
)

type rbacRule struct {
	APIGroups []string `yaml:"apiGroups"`
	Resources []string `yaml:"resources"`
	Verbs     []string `yaml:"verbs"`
}

type rbacDocument struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Rules []rbacRule `yaml:"rules"`
}

// TestManagerRoleTransportRBAC ensures the generated manager ClusterRole does not
// inherit extra scaffold permissions for transports while retaining the StepRun
// controller's transportbinding verbs.
func TestManagerRoleTransportRBAC(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
	require.NoError(t, err)

	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var manager *rbacDocument
	for {
		var doc rbacDocument
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		if doc.Kind == "ClusterRole" && doc.Metadata.Name == "manager-role" {
			manager = &doc
			break
		}
	}
	require.NotNil(t, manager, "manager-role not found in config/rbac/role.yaml")

	assertNoTransportCreateDelete(t, manager.Rules)
	assertNoTransportFinalizers(t, manager.Rules)
	assertTransportBindingVerbs(t, manager.Rules)
}

func assertNoTransportCreateDelete(t *testing.T, rules []rbacRule) {
	t.Helper()
	for _, rule := range rules {
		if !contains(rule.Resources, "transports") {
			continue
		}
		if contains(rule.Verbs, "create") || contains(rule.Verbs, "delete") {
			t.Fatalf("manager-role grants create/delete on transport.bubustack.io/transports: verbs=%v", rule.Verbs)
		}
	}
}

func assertNoTransportFinalizers(t *testing.T, rules []rbacRule) {
	t.Helper()
	for _, rule := range rules {
		for _, resource := range rule.Resources {
			if resource == "transports/finalizers" || resource == "transportbindings/finalizers" {
				t.Fatalf("manager-role retains scaffold finalizer access for %s", resource)
			}
		}
	}
}

func assertTransportBindingVerbs(t *testing.T, rules []rbacRule) {
	t.Helper()
	expected := []string{"get", "list", "watch", "create", "update", "patch", "delete"}
	for _, rule := range rules {
		if !contains(rule.Resources, "transportbindings") {
			continue
		}
		for _, verb := range expected {
			if !contains(rule.Verbs, verb) {
				t.Fatalf("transportbindings rule is missing verb %q, verbs=%v", verb, rule.Verbs)
			}
		}
		return
	}
	t.Fatalf("manager-role lacks a transportbindings rule for StepRun operations")
}

func contains(list []string, value string) bool {
	return slices.Contains(list, value)
}
