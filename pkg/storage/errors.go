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
	"errors"
	"os"
	"strings"
)

// ErrUnsupportedOperation indicates the storage backend does not support the requested operation.
var ErrUnsupportedOperation = errors.New("storage: operation not supported")

// IsNotFound reports whether a storage error indicates a missing object.
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "nosuchkey"):
		return true
	case strings.Contains(msg, "notfound"):
		return true
	case strings.Contains(msg, "status code: 404"):
		return true
	case strings.Contains(msg, "not found"):
		return true
	default:
		return false
	}
}
