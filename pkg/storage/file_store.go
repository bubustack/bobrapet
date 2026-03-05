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
	"os"
	"path/filepath"
	"strings"
)

// FileStore implements the Store interface for a local filesystem.
// It is intended for local development and testing. For production, use S3Store.
type FileStore struct {
	basePath string
}

// NewFileStore creates a new FileStore rooted at the given basePath.
func NewFileStore(basePath string) (*FileStore, error) {
	if strings.TrimSpace(basePath) == "" {
		return nil, fmt.Errorf("file store base path cannot be empty")
	}
	absPath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for file store: %w", err)
	}
	if err := os.MkdirAll(absPath, 0750); err != nil {
		return nil, fmt.Errorf("failed to create base directory for file store at %s: %w", absPath, err)
	}
	return &FileStore{basePath: absPath}, nil
}

// Write writes data from a reader to a file at the given path, relative to the basePath.
func (s *FileStore) Write(ctx context.Context, path string, reader io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Validate that joined path is within basePath to prevent path traversal
	fullPath := filepath.Join(s.basePath, path)
	relPath, err := filepath.Rel(s.basePath, fullPath)
	if err != nil || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) || filepath.IsAbs(relPath) {
		return fmt.Errorf("invalid storage path: path traversal detected (requested '%s', resolved to '%s')", path, fullPath)
	}
	// Additional safety: ensure fullPath is still within basePath after Clean
	if !strings.HasPrefix(filepath.Clean(fullPath), filepath.Clean(s.basePath)) {
		return fmt.Errorf("invalid storage path: resolved path '%s' escapes base path '%s'", fullPath, s.basePath)
	}
	if err := os.MkdirAll(filepath.Dir(fullPath), 0750); err != nil {
		return fmt.Errorf("failed to create directory for %s: %w", fullPath, err)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}
	defer func() { _ = file.Close() }()

	if _, err := io.Copy(file, reader); err != nil {
		return fmt.Errorf("failed to write to file %q: %w", fullPath, err)
	}
	return nil
}

// Read reads data from a file at the given path and writes it to a writer.
func (s *FileStore) Read(ctx context.Context, path string, writer io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Validate that joined path is within basePath to prevent path traversal
	fullPath := filepath.Join(s.basePath, path)
	relPath, err := filepath.Rel(s.basePath, fullPath)
	if err != nil || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) || filepath.IsAbs(relPath) {
		return fmt.Errorf("invalid storage path: path traversal detected (requested '%s', resolved to '%s')", path, fullPath)
	}
	// Additional safety: ensure fullPath is still within basePath after Clean
	if !strings.HasPrefix(filepath.Clean(fullPath), filepath.Clean(s.basePath)) {
		return fmt.Errorf("invalid storage path: resolved path '%s' escapes base path '%s'", fullPath, s.basePath)
	}

	file, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fullPath, err)
	}
	defer func() { _ = file.Close() }()

	_, err = io.Copy(writer, file)
	if err != nil {
		return fmt.Errorf("failed to read from file %s: %w", fullPath, err)
	}
	return nil
}

// List enumerates files under the provided prefix relative to the basePath.
func (s *FileStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := filepath.Join(s.basePath, prefix)
	relPath, err := filepath.Rel(s.basePath, root)
	if err != nil || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) || filepath.IsAbs(relPath) {
		return nil, fmt.Errorf("invalid storage prefix: path traversal detected (requested '%s', resolved to '%s')", prefix, root)
	}
	if !strings.HasPrefix(filepath.Clean(root), filepath.Clean(s.basePath)) {
		return nil, fmt.Errorf("invalid storage prefix: resolved path '%s' escapes base path '%s'", root, s.basePath)
	}

	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to stat prefix %s: %w", root, err)
	}

	results := make([]string, 0)
	if err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.basePath, path)
		if err != nil {
			return err
		}
		results = append(results, filepath.ToSlash(rel))
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to list prefix %s: %w", root, err)
	}
	return results, nil
}

// Delete removes the file at the provided path relative to the basePath.
func (s *FileStore) Delete(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fullPath := filepath.Join(s.basePath, path)
	relPath, err := filepath.Rel(s.basePath, fullPath)
	if err != nil || strings.HasPrefix(relPath, ".."+string(filepath.Separator)) || filepath.IsAbs(relPath) {
		return fmt.Errorf("invalid storage path: path traversal detected (requested '%s', resolved to '%s')", path, fullPath)
	}
	if !strings.HasPrefix(filepath.Clean(fullPath), filepath.Clean(s.basePath)) {
		return fmt.Errorf("invalid storage path: resolved path '%s' escapes base path '%s'", fullPath, s.basePath)
	}
	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to delete file %s: %w", fullPath, err)
	}
	return nil
}
