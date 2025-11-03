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
	"bytes"
	"context"
	"testing"
)

// TestStoreInterface verifies that our concrete implementations satisfy the Store interface
func TestStoreInterface(t *testing.T) {
	t.Run("mockStore satisfies Store", func(t *testing.T) {
		var _ Store = (*mockStore)(nil)
	})

	t.Run("FileStore satisfies Store", func(t *testing.T) {
		var _ Store = (*FileStore)(nil)
	})

	t.Run("S3Store satisfies Store", func(t *testing.T) {
		var _ Store = (*S3Store)(nil)
	})
}

// TestStoreContract verifies the expected behavior of any Store implementation
func TestStoreContract(t *testing.T) {
	// Test with FileStore (we can't easily test S3Store without AWS credentials)
	tmpDir := t.TempDir()
	store, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FileStore: %v", err)
	}

	ctx := context.Background()
	path := "contract-test/file.txt"
	data := []byte("contract test data")

	t.Run("Write then Read", func(t *testing.T) {
		// Write
		if err := store.Write(ctx, path, bytes.NewReader(data)); err != nil {
			t.Fatalf("Write() error = %v", err)
		}

		// Read
		var buf bytes.Buffer
		if err := store.Read(ctx, path, &buf); err != nil {
			t.Fatalf("Read() error = %v", err)
		}

		// Verify
		if !bytes.Equal(buf.Bytes(), data) {
			t.Errorf("Read() returned %v, want %v", buf.Bytes(), data)
		}
	})

	t.Run("Read non-existent returns error", func(t *testing.T) {
		var buf bytes.Buffer
		err := store.Read(ctx, "does-not-exist", &buf)
		if err == nil {
			t.Error("Read() should return error for non-existent path")
		}
	})

	t.Run("Overwrite existing file", func(t *testing.T) {
		newData := []byte("updated data")

		// Overwrite
		if err := store.Write(ctx, path, bytes.NewReader(newData)); err != nil {
			t.Fatalf("Write() error = %v", err)
		}

		// Read
		var buf bytes.Buffer
		if err := store.Read(ctx, path, &buf); err != nil {
			t.Fatalf("Read() error = %v", err)
		}

		// Should have new data
		if !bytes.Equal(buf.Bytes(), newData) {
			t.Errorf("After overwrite, Read() = %v, want %v", buf.Bytes(), newData)
		}
	})
}

// TestStoreEdgeCases tests edge cases that all stores should handle
func TestStoreEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FileStore: %v", err)
	}

	ctx := context.Background()

	t.Run("Empty data", func(t *testing.T) {
		path := "edge/empty.txt"
		emptyData := []byte{}

		if err := store.Write(ctx, path, bytes.NewReader(emptyData)); err != nil {
			t.Errorf("Write() with empty data error = %v", err)
		}

		var buf bytes.Buffer
		if err := store.Read(ctx, path, &buf); err != nil {
			t.Errorf("Read() empty file error = %v", err)
		}

		if len(buf.Bytes()) != 0 {
			t.Errorf("Expected empty file, got %d bytes", len(buf.Bytes()))
		}
	})

	t.Run("Large data", func(t *testing.T) {
		path := "edge/large.dat"
		largeData := bytes.Repeat([]byte("a"), 1024*1024) // 1MB

		if err := store.Write(ctx, path, bytes.NewReader(largeData)); err != nil {
			t.Errorf("Write() large data error = %v", err)
		}

		var buf bytes.Buffer
		if err := store.Read(ctx, path, &buf); err != nil {
			t.Errorf("Read() large file error = %v", err)
		}

		if !bytes.Equal(buf.Bytes(), largeData) {
			t.Errorf("Large file data mismatch")
		}
	})

	t.Run("Binary data", func(t *testing.T) {
		path := "edge/binary.dat"
		binaryData := make([]byte, 256)
		for i := range binaryData {
			binaryData[i] = byte(i)
		}

		if err := store.Write(ctx, path, bytes.NewReader(binaryData)); err != nil {
			t.Errorf("Write() binary data error = %v", err)
		}

		var buf bytes.Buffer
		if err := store.Read(ctx, path, &buf); err != nil {
			t.Errorf("Read() binary file error = %v", err)
		}

		if !bytes.Equal(buf.Bytes(), binaryData) {
			t.Errorf("Binary data mismatch")
		}
	})

	t.Run("Path with special characters", func(t *testing.T) {
		path := "edge/file-with-dashes_and_underscores.json"
		data := []byte("special path")

		if err := store.Write(ctx, path, bytes.NewReader(data)); err != nil {
			t.Errorf("Write() with special characters error = %v", err)
		}

		var buf bytes.Buffer
		if err := store.Read(ctx, path, &buf); err != nil {
			t.Errorf("Read() with special characters error = %v", err)
		}

		if !bytes.Equal(buf.Bytes(), data) {
			t.Errorf("Special path data mismatch")
		}
	})
}
