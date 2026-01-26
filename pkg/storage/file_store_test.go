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
	"os"
	"path/filepath"
	"testing"
)

func TestNewFileStore(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name:    "valid path",
			path:    t.TempDir(),
			wantErr: false,
		},
		{
			name:    "empty base path",
			path:    "",
			wantErr: true,
		},
		{
			name:    "non-existent path",
			path:    filepath.Join(t.TempDir(), "non-existent"),
			wantErr: false, // NewFileStore should create it
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewFileStore(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewFileStore() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestFileStore_Write(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	ctx := context.Background()
	testData := []byte("test content")
	testPath := "test/file.txt"

	err = fs.Write(ctx, testPath, bytes.NewReader(testData))
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}

	// Verify the file was created
	fullPath := filepath.Join(tmpDir, testPath)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		t.Error("Write() did not create file")
	}

	// Verify the content
	content, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("Failed to read written file: %v", err)
	}
	if !bytes.Equal(content, testData) {
		t.Errorf("Write() wrote %v, want %v", content, testData)
	}
}

func TestFileStore_Write_CreatesDirectories(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	ctx := context.Background()
	testData := []byte("nested content")
	testPath := "deep/nested/path/file.txt"

	err = fs.Write(ctx, testPath, bytes.NewReader(testData))
	if err != nil {
		t.Errorf("Write() error = %v", err)
	}

	// Verify the nested directories were created
	fullPath := filepath.Join(tmpDir, testPath)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		t.Error("Write() did not create nested directories")
	}
}

func TestFileStore_Read(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	ctx := context.Background()
	testData := []byte("test content for reading")
	testPath := "read/test.txt"

	// First write a file
	if err := fs.Write(ctx, testPath, bytes.NewReader(testData)); err != nil {
		t.Fatalf("Setup Write() error = %v", err)
	}

	// Now read it back
	var buf bytes.Buffer
	err = fs.Read(ctx, testPath, &buf)
	if err != nil {
		t.Errorf("Read() error = %v", err)
	}

	if !bytes.Equal(buf.Bytes(), testData) {
		t.Errorf("Read() got %v, want %v", buf.Bytes(), testData)
	}
}

func TestFileStore_Read_NonExistent(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	ctx := context.Background()
	var buf bytes.Buffer

	err = fs.Read(ctx, "does-not-exist.txt", &buf)
	if err == nil {
		t.Error("Read() should return error for non-existent file")
	}
}

func TestFileStore_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	fs, err := NewFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	ctx := context.Background()
	testCases := []struct {
		name string
		data []byte
		path string
	}{
		{
			name: "simple text",
			data: []byte("hello world"),
			path: "simple.txt",
		},
		{
			name: "binary data",
			data: []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD},
			path: "binary.dat",
		},
		{
			name: "large content",
			data: bytes.Repeat([]byte("a"), 10000),
			path: "large.txt",
		},
		{
			name: "empty file",
			data: []byte{},
			path: "empty.txt",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Write
			if err := fs.Write(ctx, tc.path, bytes.NewReader(tc.data)); err != nil {
				t.Fatalf("Write() error = %v", err)
			}

			// Read
			var buf bytes.Buffer
			if err := fs.Read(ctx, tc.path, &buf); err != nil {
				t.Fatalf("Read() error = %v", err)
			}

			// Verify
			if !bytes.Equal(buf.Bytes(), tc.data) {
				t.Errorf("Round trip failed: got %d bytes, want %d bytes", len(buf.Bytes()), len(tc.data))
			}
		})
	}
}
