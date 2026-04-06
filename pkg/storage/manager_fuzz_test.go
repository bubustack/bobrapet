//go:build go1.18

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
	"encoding/json"
	"strings"
	"testing"
)

func FuzzHydrateValue(f *testing.F) {
	// Seed corpus with various structures
	f.Add(`{"key":"value"}`)
	f.Add(`{"$bubuStorageRef":"test/path.json"}`)
	f.Add(`{"nested":{"deep":{"very":{"much":"data"}}}}`)
	f.Add(`[1,2,3,4,5]`)
	f.Add(`{"array":[{"nested":"data"},{"more":"stuff"}]}`)
	f.Add(`{"a":{"b":{"c":{"d":{"e":{"f":{"g":{"h":"deep"}}}}}}}}`)

	f.Fuzz(func(t *testing.T, jsonStr string) {
		var data any
		if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
			t.Skip("invalid JSON")
		}

		// Create manager with mock store
		mock := newMockStore()
		// Add some test data to the mock
		testData := StoredObject{
			ContentType: "json",
			Data:        json.RawMessage(`{"test":"data"}`),
		}
		dataBytes, _ := json.Marshal(testData)
		mock.data["test/path.json"] = dataBytes

		sm := &StorageManager{
			store:         mock,
			maxInlineSize: 100,
		}

		ctx := context.Background()

		// Should not panic, even with deeply nested or malformed data
		_, err := sm.hydrateValue(ctx, data, 0, true)
		// Error is acceptable (e.g., max depth exceeded), but no panic
		_ = err
	})
}

func FuzzDehydrateValue(f *testing.F) {
	// Seed corpus
	f.Add(`{"key":"value"}`)
	f.Add(`{"large":"` + strings.Repeat("x", 200) + `"}`)
	f.Add(`{"nested":{"a":{"b":{"c":"deep"}}}}`)
	f.Add(`{"array":["item1","item2","item3"]}`)
	f.Add(`[1,2,3,4,5,6,7,8,9,10]`)

	f.Fuzz(func(t *testing.T, jsonStr string) {
		var data any
		if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
			t.Skip("invalid JSON")
		}

		mock := newMockStore()
		sm := &StorageManager{
			store:         mock,
			maxInlineSize: 50, // Small to trigger offloading
		}

		ctx := context.Background()

		// Should not panic
		_, err := sm.dehydrateValue(ctx, data, "test-run", "", 0, "")
		_ = err
	})
}
