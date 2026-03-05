//go:build integration
// +build integration

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

package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"sync"

	"github.com/bubustack/core/contracts"
	"go.uber.org/goleak"
)

var testMinioOnce sync.Once

// run one container for all tests
var testMinio *minioContainer

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// Integration test for S3 multipart uploads using manager.Uploader.
// This test is gated by environment variables and will be skipped by default.
// Required envs to run:
//   - BUBU_S3_INTEGRATION_ENABLED=1
//   - BUBU_S3_BUCKET (legacy BUBU_STORAGE_S3_BUCKET supported)
//   - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (and optionally AWS_SESSION_TOKEN)
//
// Optional envs:
//   - BUBU_S3_REGION (default us-east-1, legacy BUBU_STORAGE_S3_REGION supported)
//   - BUBU_S3_ENDPOINT (for MinIO or S3-compatible endpoints; legacy supported)
func TestS3Integration_UploaderRoundTrip(t *testing.T) {
	if os.Getenv(contracts.S3IntegrationEnv) != "1" {
		t.Skip("skipping S3 integration test; set %s=1 to enable", contracts.S3IntegrationEnv)
	}

	bucket := os.Getenv(contracts.StorageS3BucketEnv)
	if bucket == "" {
		t.Skip("skipping: %s not set", contracts.StorageS3BucketEnv)
	}
	if os.Getenv(contracts.StorageS3AccessKeyIDEnv) == "" || os.Getenv(contracts.StorageS3SecretAccessKeyEnv) == "" {
		t.Skip("skipping: AWS credentials not provided")
	}

	// Force S3 provider for the manager
	_ = os.Setenv(contracts.StorageProviderEnv, "s3")
	defer func() { _ = os.Unsetenv(contracts.StorageProviderEnv) }()

	// Make inline threshold small to guarantee offload
	_ = os.Setenv(contracts.MaxInlineSizeEnv, "16")
	defer func() { _ = os.Unsetenv(contracts.MaxInlineSizeEnv) }()

	// Use a unique stepRunID for namespacing
	stepRunID := "it-" + time.Now().UTC().Format("20060102T150405Z")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sm, err := NewManager(ctx)
	if err != nil {
		t.Skipf("manager init failed (likely missing/invalid S3 config): %v", err)
	}
	if sm == nil || sm.GetStore() == nil {
		t.Skip("store not initialized; skipping integration test")
	}

	// Large payload to trigger multipart path in uploader (size > 5MB preferred)
	// Keep it modest to avoid long CI times; uploader will still choose multipart if configured.
	// We rely on manager.Dehydrate to offload any value exceeding inline size.
	big := make([]byte, 256*1024) // 256 KiB
	for i := range big {
		big[i] = byte('a' + (i % 26))
	}

	original := map[string]any{"data": string(big)}

	// Dehydrate writes to S3 via manager.Uploader
	dehydrated, err := sm.Dehydrate(ctx, original, stepRunID)
	if err != nil {
		t.Fatalf("Dehydrate error: %v", err)
	}

	// Hydrate reads back from S3 and reconstructs the original value
	hydrated, err := sm.Hydrate(ctx, dehydrated)
	if err != nil {
		t.Fatalf("Hydrate error: %v", err)
	}

	// Spot-check the presence and length of the large field
	hydratedMap, ok := hydrated.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any after hydrate, got %T", hydrated)
	}
	val, ok := hydratedMap["data"].(string)
	if !ok {
		t.Fatalf("expected string in hydrated 'data' field, got %T", hydratedMap["data"])
	}
	if len(val) != len(big) {
		t.Fatalf("hydrated data length mismatch: got %d want %d", len(val), len(big))
	}
}

func TestS3Store_Integration(t *testing.T) {
	endpoint := os.Getenv("BUBU_TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("skipping S3 integration tests: BUBU_TEST_MINIO_ENDPOINT not set")
	}
	t.Setenv(contracts.StorageS3EndpointEnv, endpoint)
	t.Setenv(contracts.StorageS3RegionEnv, os.Getenv("BUBU_TEST_MINIO_REGION"))
	t.Setenv(contracts.StorageS3BucketEnv, os.Getenv("BUBU_TEST_MINIO_BUCKET"))
	t.Setenv(contracts.StorageS3AccessKeyIDEnv, os.Getenv("BUBU_TEST_MINIO_ACCESS_KEY_ID"))
	t.Setenv(contracts.StorageS3SecretAccessKeyEnv, os.Getenv("BUBU_TEST_MINIO_SECRET_ACCESS_KEY"))

	// Use a unique stepRunID for namespacing
	stepRunID := "it-" + time.Now().UTC().Format("20060102T150405Z")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sm, err := NewManager(ctx)
	if err != nil {
		t.Skipf("manager init failed (likely missing/invalid S3 config): %v", err)
	}
	if sm == nil || sm.GetStore() == nil {
		t.Skip("store not initialized; skipping integration test")
	}

	// Large payload to trigger multipart path in uploader (size > 5MB preferred)
	// Keep it modest to avoid long CI times; uploader will still choose multipart if configured.
	// We rely on manager.Dehydrate to offload any value exceeding inline size.
	big := make([]byte, 256*1024) // 256 KiB
	for i := range big {
		big[i] = byte('a' + (i % 26))
	}

	original := map[string]any{"data": string(big)}

	// Dehydrate writes to S3 via manager.Uploader
	dehydrated, err := sm.Dehydrate(ctx, original, stepRunID)
	if err != nil {
		t.Fatalf("Dehydrate error: %v", err)
	}

	// Hydrate reads back from S3 and reconstructs the original value
	hydrated, err := sm.Hydrate(ctx, dehydrated)
	if err != nil {
		t.Fatalf("Hydrate error: %v", err)
	}

	// Spot-check the presence and length of the large field
	hydratedMap, ok := hydrated.(map[string]any)
	if !ok {
		t.Fatalf("expected map[string]any after hydrate, got %T", hydrated)
	}
	val, ok := hydratedMap["data"].(string)
	if !ok {
		t.Fatalf("expected string in hydrated 'data' field, got %T", hydratedMap["data"])
	}
	if len(val) != len(big) {
		t.Fatalf("hydrated data length mismatch: got %d want %d", len(val), len(big))
	}
}
