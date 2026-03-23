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
	"io"

	"github.com/stretchr/testify/mock"
)

// mockStore implements the Store interface for testing
type mockStore struct {
	data   map[string][]byte
	writes int
	reads  int
	// lastMetadata captures metadata attached to the most recent Write call.
	lastMetadata *writeMetadata
	lastPath     string
}

// MockManager is a mock implementation of the StorageManager interface.
type MockManager struct {
	mock.Mock
}

// Hydrate mocks the Hydrate method for testing.
func (m *MockManager) Hydrate(ctx context.Context, data any) (any, error) {
	args := m.Called(ctx, data)
	return args.Get(0), args.Error(1)
}

// Dehydrate mocks the Dehydrate method for testing.
func (m *MockManager) Dehydrate(ctx context.Context, data any, stepRunID string) (any, error) {
	args := m.Called(ctx, data, stepRunID)
	return args.Get(0), args.Error(1)
}

func newMockStore() *mockStore {
	return &mockStore{
		data: make(map[string][]byte),
	}
}

// Write mocks the Write method for testing.
func (m *mockStore) Write(ctx context.Context, path string, reader io.Reader) error {
	m.writes++
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.data[path] = data
	m.lastPath = path
	if meta := writeMetadataFromContext(ctx); meta != nil {
		copyMeta := *meta
		m.lastMetadata = &copyMeta
	} else {
		m.lastMetadata = nil
	}
	return nil
}

// Read mocks the Read method for testing.
func (m *mockStore) Read(ctx context.Context, path string, writer io.Writer) error {
	m.reads++
	data, exists := m.data[path]
	if !exists {
		return fmt.Errorf("path not found: %s", path)
	}
	_, err := writer.Write(data)
	return err
}
