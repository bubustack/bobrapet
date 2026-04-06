package storage

import (
	"strings"
	"testing"

	"github.com/bubustack/core/contracts"
)

func TestParseKubeRefString_EnforcesWorkloadNamespace(t *testing.T) {
	t.Setenv(contracts.StepRunNamespaceEnv, "tenant-a")

	t.Run("allows explicit same namespace", func(t *testing.T) {
		ref, err := parseKubeRefString("tenant-a/creds:token")
		if err != nil {
			t.Fatalf("parseKubeRefString returned error: %v", err)
		}
		if ref.Namespace != "tenant-a" {
			t.Fatalf("expected namespace tenant-a, got %q", ref.Namespace)
		}
		if ref.Name != "creds" || ref.Key != "token" {
			t.Fatalf("unexpected ref parsed: %+v", ref)
		}
	})

	t.Run("denies explicit cross namespace", func(t *testing.T) {
		_, err := parseKubeRefString("tenant-b/creds:token")
		if err == nil {
			t.Fatal("expected cross-namespace ref to be rejected")
		}
		if !strings.Contains(err.Error(), "cross-namespace references are not allowed") {
			t.Fatalf("expected cross-namespace error, got %v", err)
		}
	})
}

func TestParseKubeRefString_DeniesExplicitNamespaceWithoutWorkloadContext(t *testing.T) {
	_, err := parseKubeRefString("tenant-a/creds:token")
	if err == nil {
		t.Fatal("expected explicit namespace ref without workload context to be rejected")
	}
	if !strings.Contains(err.Error(), "without workload namespace context") {
		t.Fatalf("expected workload namespace context error, got %v", err)
	}
}

func TestParseKubeRefMap_EnforcesWorkloadNamespace(t *testing.T) {
	t.Setenv(contracts.StepRunNamespaceEnv, "tenant-a")

	t.Run("allows explicit same namespace", func(t *testing.T) {
		ref, err := parseKubeRefMap(map[string]any{
			"namespace": "tenant-a",
			"name":      "creds",
			"key":       "token",
		})
		if err != nil {
			t.Fatalf("parseKubeRefMap returned error: %v", err)
		}
		if ref.Namespace != "tenant-a" {
			t.Fatalf("expected namespace tenant-a, got %q", ref.Namespace)
		}
		if ref.Name != "creds" || ref.Key != "token" {
			t.Fatalf("unexpected ref parsed: %+v", ref)
		}
	})

	t.Run("denies explicit cross namespace", func(t *testing.T) {
		_, err := parseKubeRefMap(map[string]any{
			"namespace": "tenant-b",
			"name":      "creds",
			"key":       "token",
		})
		if err == nil {
			t.Fatal("expected cross-namespace ref to be rejected")
		}
		if !strings.Contains(err.Error(), "cross-namespace references are not allowed") {
			t.Fatalf("expected cross-namespace error, got %v", err)
		}
	})
}
