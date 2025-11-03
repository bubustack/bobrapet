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

// Package storage provides object storage backends for BubuStack workloads.
//
// This package implements a unified storage interface for S3 and file-based
// storage, used by engram workloads to store and retrieve large data artifacts
// that exceed the 1MB Kubernetes annotation limit.
//
// # Storage Manager
//
// The [Manager] provides a unified interface for storage operations:
//
//	manager, err := storage.NewManager(config)
//	defer manager.Close()
//
//	err := manager.Put(ctx, key, data)
//	data, err := manager.Get(ctx, key)
//	err := manager.Delete(ctx, key)
//
// # Storage Backends
//
// S3 Storage:
//
//	store, err := storage.NewS3Store(storage.S3Config{
//	    Bucket:   "my-bucket",
//	    Region:   "us-west-2",
//	    Endpoint: "s3.amazonaws.com",
//	})
//
// File Storage:
//
//	store, err := storage.NewFileStore(storage.FileConfig{
//	    Path: "/data/storage",
//	})
//
// # Caching
//
// The storage manager includes an optional in-memory cache:
//
//	manager, err := storage.NewManager(storage.ManagerConfig{
//	    Store:       store,
//	    CacheSize:   100 * 1024 * 1024, // 100MB cache
//	    CacheTTL:    5 * time.Minute,
//	})
//
// # Metrics
//
// Storage operations emit Prometheus metrics:
//   - storage_operations_total: Total operations by type and status
//   - storage_operation_duration_seconds: Operation latency histogram
//   - storage_bytes_transferred: Bytes read/written
//
// # Environment Variables
//
// The [ApplyEnv] function configures storage via environment variables in pods.
// This is used by controllers when building workload pod specs.
package storage
