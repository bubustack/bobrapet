package packaging

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestMakefileHelmChartDoesNotOverwriteTemplates(t *testing.T) {
	t.Helper()

	content, err := os.ReadFile(filepath.Join(repoRoot(t), "Makefile"))
	if err != nil {
		t.Fatalf("read Makefile: %v", err)
	}
	if strings.Contains(string(content), "cp -R $(CHART_OVERRIDE_DIR)/$(CHART)/.") {
		t.Fatalf("Makefile still copies override templates into dist/charts; remove the cp -R to avoid missing resources in published charts") //nolint:lll
	}
}

func TestHelmChartVersionMatchesReleasePlease(t *testing.T) {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", ".release-please-manifest.json"))
	if err != nil {
		t.Fatalf("read release-please manifest: %v", err)
	}
	var manifest map[string]string
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("parse release-please manifest: %v", err)
	}
	releaseVersion, ok := manifest["."]
	if !ok || releaseVersion == "" {
		t.Fatalf("release-please manifest missing top-level version entry")
	}

	chartFile := filepath.Join(repoRoot(t), "hack", "charts", "bobrapet", "Chart.yaml")
	chartData, err := os.ReadFile(chartFile)
	if err != nil {
		t.Fatalf("read chart file: %v", err)
	}
	var chart struct {
		Version    string `yaml:"version"`
		AppVersion string `yaml:"appVersion"`
	}
	if err := yaml.Unmarshal(chartData, &chart); err != nil {
		t.Fatalf("parse Chart.yaml: %v", err)
	}
	if chart.Version != releaseVersion {
		t.Fatalf("chart version %q does not match release-please version %q", chart.Version, releaseVersion)
	}
	if chart.AppVersion != releaseVersion {
		t.Fatalf("chart appVersion %q does not match release-please version %q", chart.AppVersion, releaseVersion)
	}
}

func TestHelmChartValuesSchemaExists(t *testing.T) {
	t.Helper()

	schemaPath := filepath.Join(repoRoot(t), "hack", "charts", "bobrapet", "values.schema.json")
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		t.Fatalf("%s is missing (required for chart validation)", schemaPath)
	} else if err != nil {
		t.Fatalf("stat values schema: %v", err)
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate repo root")
		}
		dir = parent
	}
}
