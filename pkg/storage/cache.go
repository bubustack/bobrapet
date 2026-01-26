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
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/bubustack/core/contracts"
)

var (
	managerCacheMu sync.RWMutex
	managerCache   = make(map[string]*StorageManager)
)

// SharedManager returns a process-wide cached StorageManager keyed by the current
// storage configuration. Subsequent callers that resolve to the same configuration
// receive the cached instance, avoiding repeated backend initialization.
func SharedManager(ctx context.Context) (*StorageManager, error) {
	key, err := getCacheKey()
	if err != nil {
		return nil, err
	}

	managerCacheMu.RLock()
	if cached, ok := managerCache[key]; ok {
		managerCacheMu.RUnlock()
		return cached, nil
	}
	managerCacheMu.RUnlock()

	managerCacheMu.Lock()
	defer managerCacheMu.Unlock()

	// Double-checked locking to prevent race conditions on initialization.
	if cached, ok := managerCache[key]; ok {
		return cached, nil
	}

	manager, err := NewManager(ctx)
	if err != nil {
		return nil, err
	}

	managerCache[key] = manager
	return manager, nil
}

func getCacheKey() (string, error) {
	provider := strings.ToLower(os.Getenv(contracts.StorageProviderEnv))
	outputPrefix := os.Getenv(contracts.StorageS3OutputPrefixEnv)
	inputPrefix := os.Getenv(contracts.StorageS3InputPrefixEnv)
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%s|out=%s|in=%s", provider, outputPrefix, inputPrefix))

	switch provider {
	case "s3":
		builder.WriteString(fmt.Sprintf("|bucket=%s", getTrimmedEnv(contracts.StorageS3BucketEnv)))
		builder.WriteString(fmt.Sprintf("|region=%s", getTrimmedEnv(contracts.StorageS3RegionEnv)))
		builder.WriteString(fmt.Sprintf("|endpoint=%s", getTrimmedEnv(contracts.StorageS3EndpointEnv)))
		builder.WriteString(fmt.Sprintf("|pathstyle=%s", getTrimmedEnv(contracts.StorageS3ForcePathStyleEnv)))
		_, tlsStr, err := getS3TLSSetting()
		if err != nil {
			return "", err
		}
		builder.WriteString(fmt.Sprintf("|tls=%s", tlsStr))
	case "file":
		builder.WriteString(fmt.Sprintf("|path=%s", os.Getenv(contracts.StoragePathEnv)))
	}

	return builder.String(), nil
}

// resetSharedManagerCache clears the shared manager cache. It is intended for tests.
func resetSharedManagerCache() {
	managerCacheMu.Lock()
	defer managerCacheMu.Unlock()
	for key := range managerCache {
		delete(managerCache, key)
	}
}

// ResetSharedManagerCacheForTests clears the shared manager cache. Exposed for tests in
// dependent modules that need to ensure isolated configuration between cases.
func ResetSharedManagerCacheForTests() {
	resetSharedManagerCache()
}
