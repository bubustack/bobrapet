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

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestOperatorConfigDefaultsToSeaweedFSS3(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile(filepath.Join("..", "..", "config", "manager", "operator-config.yaml"))
	if err != nil {
		t.Fatalf("failed to read operator config manifest: %v", err)
	}

	type configMap struct {
		Data map[string]string `yaml:"data"`
	}

	var manifest configMap
	if err := yaml.Unmarshal(content, &manifest); err != nil {
		t.Fatalf("failed to parse operator config manifest: %v", err)
	}

	provider := strings.TrimSpace(manifest.Data["controller.storage.provider"])
	bucket := strings.TrimSpace(manifest.Data["controller.storage.s3.bucket"])
	region := strings.TrimSpace(manifest.Data["controller.storage.s3.region"])
	endpoint := strings.TrimSpace(manifest.Data["controller.storage.s3.endpoint"])
	pathStyle := strings.TrimSpace(manifest.Data["controller.storage.s3.use-path-style"])

	if provider != "s3" {
		t.Fatalf("operator-config.yaml storage provider = %q, want %q", provider, "s3")
	}
	if bucket != "bubu-default" {
		t.Fatalf("operator-config.yaml storage bucket = %q, want %q", bucket, "bubu-default")
	}
	if region != "us-east-1" {
		t.Fatalf("operator-config.yaml storage region = %q, want %q", region, "us-east-1")
	}
	if endpoint != "http://seaweedfs-s3.seaweedfs:8333" {
		t.Fatalf("operator-config.yaml storage endpoint = %q, want %q", endpoint, "http://seaweedfs-s3.seaweedfs:8333")
	}
	if pathStyle != "true" {
		t.Fatalf("operator-config.yaml storage path-style = %q, want %q", pathStyle, "true")
	}
}

func TestOperatorConfigDefaultsMaxRecursionDepth(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile(filepath.Join("..", "..", "config", "manager", "operator-config.yaml"))
	if err != nil {
		t.Fatalf("failed to read operator config manifest: %v", err)
	}

	type configMap struct {
		Data map[string]string `yaml:"data"`
	}

	var manifest configMap
	if err := yaml.Unmarshal(content, &manifest); err != nil {
		t.Fatalf("failed to parse operator config manifest: %v", err)
	}

	depth := strings.TrimSpace(manifest.Data["engram.default-max-recursion-depth"])
	if depth != "32" {
		t.Fatalf("operator-config.yaml max recursion depth = %q, want %q", depth, "32")
	}
}
