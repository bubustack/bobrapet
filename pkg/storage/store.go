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
	"io"
)

// Store is the interface for a generic storage backend.
// It provides a streaming Read/Write interface.
type Store interface {
	// Write saves the data from the reader to the storage backend at the specified path.
	Write(ctx context.Context, path string, reader io.Reader) error

	// Read retrieves the data from the storage backend at the specified path and writes it to the writer.
	Read(ctx context.Context, path string, writer io.Writer) error
}
