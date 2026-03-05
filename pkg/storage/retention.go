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
	"os"
	"strconv"
	"time"

	"github.com/bubustack/core/contracts"
)

const (
	// DefaultRetentionGCInterval throttles storage retention scans.
	DefaultRetentionGCInterval = 2 * time.Minute
	// DefaultRetentionMaxScan caps objects scanned per GC pass.
	DefaultRetentionMaxScan = 2000
	// DefaultRetentionMaxDelete caps deletions per GC pass.
	DefaultRetentionMaxDelete = 200

	storageRetentionGCIntervalEnv = contracts.PrefixEnv + "STORAGE_RETENTION_GC_INTERVAL"
	storageRetentionMaxScanEnv    = contracts.PrefixEnv + "STORAGE_RETENTION_MAX_SCAN"
	storageRetentionMaxDeleteEnv  = contracts.PrefixEnv + "STORAGE_RETENTION_MAX_DELETE"
)

// RetentionPolicy configures storage retention enforcement behavior.
type RetentionPolicy struct {
	GCInterval time.Duration
	MaxScan    int
	MaxDelete  int
}

// RetentionPolicyFromEnv resolves retention policy settings from environment variables.
func RetentionPolicyFromEnv() RetentionPolicy {
	policy := RetentionPolicy{
		GCInterval: DefaultRetentionGCInterval,
		MaxScan:    DefaultRetentionMaxScan,
		MaxDelete:  DefaultRetentionMaxDelete,
	}
	if raw := os.Getenv(storageRetentionGCIntervalEnv); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			policy.GCInterval = d
		}
	}
	if raw := os.Getenv(storageRetentionMaxScanEnv); raw != "" {
		if val, err := strconv.Atoi(raw); err == nil && val > 0 {
			policy.MaxScan = val
		}
	}
	if raw := os.Getenv(storageRetentionMaxDeleteEnv); raw != "" {
		if val, err := strconv.Atoi(raw); err == nil && val > 0 {
			policy.MaxDelete = val
		}
	}
	return policy
}
