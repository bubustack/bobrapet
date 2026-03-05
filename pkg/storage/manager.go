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
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/bubustack/core/contracts"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	// DefaultMaxInlineSize is the threshold in bytes above which data is offloaded to storage.
	//
	// Aligns with operator's DefaultMaxInlineSize (bobrapet/internal/config/controller_config.go:246).
	// Override via BUBU_MAX_INLINE_SIZE. Keeping this low (1 KiB) prevents apiserver/etcd overload
	// while allowing small outputs to remain inline for fast access.
	DefaultMaxInlineSize = 1 * 1024 // 1 KiB

	// DefaultMaxRecursionDepth is the maximum nesting depth for hydration/dehydration.
	//
	// Prevents stack overflow on deeply nested data structures. Override via BUBU_MAX_RECURSION_DEPTH.
	DefaultMaxRecursionDepth = 10

	// DefaultStorageTimeout provides ample time for large file uploads (e.g., 100MB at 1MB/s = 100s + overhead).
	//
	// Operators should tune this based on expected output sizes and S3 latency:
	//   timeout >= (max_output_mb / upload_bandwidth_mbps) * 1.5 + baseline_latency_sec
	// Override via BUBU_STORAGE_TIMEOUT.
	DefaultStorageTimeout = 300 * time.Second // 5min for storage ops

	// maxBulkOffloadSize is the threshold above which a partially-dehydrated collection
	// (map or slice) is replaced by a single bulk offload of the original data, even if
	// individual elements were already offloaded. This prevents many inline-sized values
	// from aggregating into a payload that exceeds the Kubernetes API/etcd gRPC limit.
	maxBulkOffloadSize = 256 * 1024 // 256 KiB

	storageRefKey    = "$bubuStorageRef"
	storagePathKey   = "$bubuStoragePath"
	storageTypeKey   = "$bubuStorageContentType"
	storageSchemaKey = "$bubuStorageSchema"
	storageSchemaVer = "$bubuStorageSchemaVersion"
	configMapRefKey  = "$bubuConfigMapRef"
	secretRefKey     = "$bubuSecretRef"
	defaultRefFormat = "auto"
)

const (
	StorageRefKey           = storageRefKey
	StoragePathKey          = storagePathKey
	StorageContentTypeKey   = storageTypeKey
	StorageSchemaKey        = storageSchemaKey
	StorageSchemaVersionKey = storageSchemaVer
)

var storageTracer = otel.Tracer("github.com/bubustack/bobrapet/pkg/storage")

type writeMetadata struct {
	ContentType    string
	ContentMD5     string
	ChecksumSHA256 string
	ContentLength  int64
}

type writeMetadataKey struct{}

func withWriteMetadata(ctx context.Context, meta *writeMetadata) context.Context {
	if meta == nil {
		return ctx
	}
	return context.WithValue(ctx, writeMetadataKey{}, meta)
}

func writeMetadataFromContext(ctx context.Context) *writeMetadata {
	if ctx == nil {
		return nil
	}
	if meta, ok := ctx.Value(writeMetadataKey{}).(*writeMetadata); ok {
		return meta
	}
	return nil
}

// getMaxInlineSize returns the max inline size from env or default
func getMaxInlineSize() (int, error) {
	if v := os.Getenv(contracts.MaxInlineSizeEnv); v != "" {
		val, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("invalid %s '%s': must be a non-negative integer: %w", contracts.MaxInlineSizeEnv, v, err)
		}
		if val < 0 {
			return 0, fmt.Errorf("invalid %s '%s': must be >= 0", contracts.MaxInlineSizeEnv, v)
		}
		return val, nil
	}
	return DefaultMaxInlineSize, nil
}

// getMaxRecursionDepth returns the max recursion depth from env or default
func getMaxRecursionDepth() int {
	if v := os.Getenv(contracts.MaxRecursionDepthEnv); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i > 0 {
			return i
		}
	}
	return DefaultMaxRecursionDepth
}

// getStorageTimeout returns the timeout for storage operations from env or default
func getStorageTimeout() time.Duration {
	if v := os.Getenv(contracts.StorageTimeoutEnv); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return DefaultStorageTimeout
}

// StoredObject is a wrapper for data offloaded to storage, providing metadata
// about the content type to ensure correct hydration.
type StoredObject struct {
	// ContentType indicates whether the stored data is "json" or "raw" text.
	ContentType string `json:"contentType"`
	// Data holds the actual content. For JSON, it's a RawMessage; for raw, it's a string.
	Data json.RawMessage `json:"data"`
}

// StorageManager handles the transparent offloading of large inputs and outputs
// to a configured storage backend.
type StorageManager struct {
	store         Store
	maxInlineSize int
	outputPrefix  string
	inputPrefix   string
}

// Context helpers for metrics attribution
type stepRunIDKey struct{}

type storageMetadataKey struct{}

type storageMetadata struct {
	Schema        string
	SchemaVersion string
}

// NamespacedKey composes a storage-safe key by prefixing the identifier with the namespace.
func NamespacedKey(namespace, id string) string {
	ns := strings.TrimSpace(namespace)
	key := strings.TrimSpace(id)
	if ns == "" || key == "" {
		return ""
	}
	return filepath.Join(ns, key)
}

// WithStepRunID attaches a StepRunID to the context for hydration metrics attribution.
func WithStepRunID(ctx context.Context, stepRunID string) context.Context {
	if stepRunID == "" {
		return ctx
	}
	return context.WithValue(ctx, stepRunIDKey{}, stepRunID)
}

func stepRunIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(stepRunIDKey{}).(string); ok {
		return v
	}
	return ""
}

// WithStorageSchema attaches optional schema metadata to the context for storage refs.
// Empty values are ignored by the offload path.
func WithStorageSchema(ctx context.Context, schema, schemaVersion string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	meta := storageMetadata{
		Schema:        strings.TrimSpace(schema),
		SchemaVersion: strings.TrimSpace(schemaVersion),
	}
	if meta.Schema == "" && meta.SchemaVersion == "" {
		return ctx
	}
	return context.WithValue(ctx, storageMetadataKey{}, meta)
}

func storageMetadataFromContext(ctx context.Context) storageMetadata {
	if ctx == nil {
		return storageMetadata{}
	}
	if v, ok := ctx.Value(storageMetadataKey{}).(storageMetadata); ok {
		return v
	}
	return storageMetadata{}
}

// GetStore returns the underlying Store implementation used by this manager.
// This is primarily useful for testing and debugging.
func (sm *StorageManager) GetStore() Store {
	return sm.store
}

// NewManager creates a new StorageManager, automatically configuring the
// storage backend based on environment variables.
func NewManager(ctx context.Context) (*StorageManager, error) {
	maxSize, err := getMaxInlineSize()
	if err != nil {
		return nil, err
	}

	outputPrefix := "outputs"
	if prefix := os.Getenv(contracts.StorageS3OutputPrefixEnv); prefix != "" {
		outputPrefix = prefix
	}
	inputPrefix := "inputs"
	if prefix := os.Getenv(contracts.StorageS3InputPrefixEnv); prefix != "" {
		inputPrefix = prefix
	}

	var store Store
	provider := os.Getenv(contracts.StorageProviderEnv)

	switch provider {
	case "s3":
		s3Store, err := NewS3Store(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize S3 storage: %w", err)
		}
		store = s3Store
	case "file":
		basePath := os.Getenv(contracts.StoragePathEnv)
		if basePath == "" {
			return nil, fmt.Errorf(
				"%s is set to 'file' but %s is not set",
				contracts.StorageProviderEnv,
				contracts.StoragePathEnv,
			)
		}
		fileStore, err := NewFileStore(basePath)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize file storage: %w", err)
		}
		store = fileStore
	case "", "none":
		// Disabled or not set
		store = nil
	default:
		return nil, fmt.Errorf("unknown %s '%s' (expected 's3', 'file', or 'none')", contracts.StorageProviderEnv, provider)
	}

	return &StorageManager{
		store:         store,
		maxInlineSize: maxSize,
		outputPrefix:  outputPrefix,
		inputPrefix:   inputPrefix,
	}, nil
}

// Hydrate recursively scans a data structure for storage references and replaces
// them with the actual content from the storage backend.
func (sm *StorageManager) Hydrate(ctx context.Context, data any) (result any, err error) {
	ctx, span := storageTracer.Start(ctx, "storage.Hydrate",
		trace.WithAttributes(attribute.Bool("storage.enabled", sm.store != nil)))
	if stepID := stepRunIDFromContext(ctx); stepID != "" {
		span.SetAttributes(attribute.String("storage.stepRunID", stepID))
	}
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "success")
		}
		span.End()
	}()

	if sm.store == nil {
		if containsStorageRef(data, 0) {
			err = fmt.Errorf(
				"found storage reference in inputs but storage is disabled (set %s=s3 or file)",
				contracts.StorageProviderEnv,
			)
			return nil, err
		}
	}

	timeout := getStorageTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, err = sm.hydrateValue(ctx, data, 0)
	if err != nil {
		trace.SpanFromContext(ctx).AddEvent(
			"hydrate.error",
			trace.WithAttributes(attribute.String("error", err.Error())),
		)
		return nil, err
	}
	if result != nil {
		size := estimateSize(result)
		recordHydrationSize(ctx, int64(size), stepRunIDFromContext(ctx))
		trace.SpanFromContext(ctx).AddEvent(
			"hydrate.success",
			trace.WithAttributes(attribute.Int("size_bytes", size)),
		)
	}
	return result, nil
}

// DehydrateInputs dehydrates StoryRun inputs.
func (sm *StorageManager) DehydrateInputs(ctx context.Context, data any, storyRunID string) (result any, err error) {
	ctx, span := storageTracer.Start(ctx, "storage.DehydrateInputs",
		trace.WithAttributes(attribute.Bool("storage.enabled", sm.store != nil),
			attribute.String("storage.storyRunID", storyRunID)))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "success")
		}
		span.End()
	}()

	if sm.store == nil {
		if data != nil {
			size := estimateSize(data)
			if size > sm.maxInlineSize {
				err = fmt.Errorf(
					"output size %d bytes exceeds inline limit %d and storage is disabled; "+
						fmt.Sprintf("either enable storage (%s=s3|file), ", contracts.StorageProviderEnv)+
						fmt.Sprintf("increase limit (%s=%d or higher), ", contracts.MaxInlineSizeEnv, sm.maxInlineSize)+
						"or reduce output size",
					size, sm.maxInlineSize, size,
				)
				return nil, err
			}
		}
		return data, nil
	}

	timeout := getStorageTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, err = sm.dehydrateValue(ctx, data, storyRunID, "", 0, sm.inputPrefix)
	if err != nil {
		trace.SpanFromContext(ctx).AddEvent(
			"dehydrate.inputs.error",
			trace.WithAttributes(attribute.String("error", err.Error())),
		)
		return nil, err
	}
	if result != nil {
		if encoded, encodeErr := json.Marshal(result); encodeErr == nil && len(encoded) > sm.maxInlineSize {
			ref, offloadErr := sm.offloadValue(ctx, encoded, "json", storyRunID, "input", sm.inputPrefix)
			if offloadErr != nil {
				trace.SpanFromContext(ctx).AddEvent(
					"dehydrate.inputs.error",
					trace.WithAttributes(attribute.String("error", offloadErr.Error())),
				)
				return nil, offloadErr
			}
			result = ref
		}
	}
	if data != nil {
		size := estimateSize(data)
		recordDehydrationSize(ctx, int64(size), storyRunID)
		trace.SpanFromContext(ctx).AddEvent(
			"dehydrate.inputs.success",
			trace.WithAttributes(attribute.Int("size_bytes", size)),
		)
	}
	return result, nil
}

// Dehydrate recursively checks the size of a data structure. If it exceeds the inline
// size limit, it saves the data to the storage backend and replaces it with a
// storage reference.
func (sm *StorageManager) Dehydrate(ctx context.Context, data any, stepRunID string) (result any, err error) {
	ctx, span := storageTracer.Start(ctx, "storage.Dehydrate",
		trace.WithAttributes(attribute.Bool("storage.enabled", sm.store != nil),
			attribute.String("storage.stepRunID", stepRunID)))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "success")
		}
		span.End()
	}()

	if sm.store == nil {
		if data != nil {
			size := estimateSize(data)
			if size > sm.maxInlineSize {
				err = fmt.Errorf("output size %d bytes exceeds inline limit %d and storage is disabled; "+
					fmt.Sprintf("either enable storage (%s=s3|file), ", contracts.StorageProviderEnv)+
					fmt.Sprintf("increase limit (%s=%d or higher), ", contracts.MaxInlineSizeEnv, sm.maxInlineSize)+
					"or reduce output size", size, sm.maxInlineSize, size,
					size, sm.maxInlineSize, size,
				)
				return nil, err
			}
		}
		return data, nil
	}

	timeout := getStorageTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, err = sm.dehydrateValue(ctx, data, stepRunID, "", 0, sm.outputPrefix)
	if err != nil {
		trace.SpanFromContext(ctx).AddEvent("dehydrate.error", trace.WithAttributes(attribute.String("error", err.Error())))
		return nil, err
	}
	if data != nil {
		size := estimateSize(data)
		recordDehydrationSize(ctx, int64(size), stepRunID)
		trace.SpanFromContext(ctx).AddEvent("dehydrate.success", trace.WithAttributes(attribute.Int("size_bytes", size)))
	}
	return result, nil
}

// validateStorageRef checks if a storage reference path is safe to use.
// It rejects:
// - Absolute paths (e.g., /etc/passwd, C:\secrets\)
// - Path traversal attempts (e.g., ../, ..\, or embedded .. components)
// This provides defense-in-depth against path traversal attacks from malicious
// or compromised upstream engrams, even if storage backend should enforce isolation.
func validateStorageRef(refPath string) error {
	if refPath == "" {
		return fmt.Errorf("storage reference path is empty")
	}

	// Reject absolute paths
	if filepath.IsAbs(refPath) {
		return fmt.Errorf("invalid storage reference: absolute paths not allowed (got '%s')", refPath)
	}

	// Check for path traversal using platform-independent approach
	// filepath.Clean normalizes the path, then we verify it's still relative and doesn't escape
	cleanPath := filepath.Clean(refPath)

	// After cleaning, check if path tries to escape (starts with ..)
	if len(cleanPath) >= 2 && cleanPath[0] == '.' && cleanPath[1] == '.' {
		return fmt.Errorf(
			"invalid storage reference: path traversal detected (got '%s', normalized to '%s')",
			refPath, cleanPath,
		)
	}

	// Also check for ../ or ..\ anywhere in the original path to catch obfuscation attempts
	// (e.g., "foo/../bar" would normalize to "bar" but indicates traversal intent)
	if strings.Contains(refPath, ".."+string(filepath.Separator)) ||
		strings.Contains(refPath, ".."+string('/')) || // Unix separator
		(runtime.GOOS == "windows" && strings.Contains(refPath, ".."+string('\\'))) { // Windows separator
		return fmt.Errorf("invalid storage reference: path traversal detected (got '%s')", refPath)
	}

	return nil
}

func (sm *StorageManager) hydrateValue(ctx context.Context, value any, depth int) (any, error) {
	maxDepth := getMaxRecursionDepth()
	if depth > maxDepth {
		return nil, fmt.Errorf("maximum recursion depth (%d) exceeded during hydration", maxDepth)
	}
	switch v := value.(type) {
	case map[string]any:
		return sm.hydrateMap(ctx, v, depth)
	case []any:
		return sm.hydrateSlice(ctx, v, depth)
	default:
		return value, nil
	}
}

func (sm *StorageManager) hydrateMap(ctx context.Context, m map[string]any, depth int) (any, error) {
	if refPath, refSubPath, ok := storageRefSelector(m); ok {
		if refSubPath == "" {
			return sm.hydrateFromStorageRef(ctx, refPath, depth)
		}
		return sm.hydrateFromStorageRefPath(ctx, refPath, refSubPath, depth)
	}
	if ref, ok := singleConfigMapRef(m); ok {
		return sm.hydrateFromConfigMapRef(ctx, ref)
	}
	if ref, ok := singleSecretRef(m); ok {
		return sm.hydrateFromSecretRef(ctx, ref)
	}
	hydratedMap := make(map[string]any)
	for k, val := range m {
		hv, err := sm.hydrateValue(ctx, val, depth+1)
		if err != nil {
			return nil, err
		}
		hydratedMap[k] = hv
	}
	return hydratedMap, nil
}

func (sm *StorageManager) hydrateSlice(ctx context.Context, s []any, depth int) (any, error) {
	hydrated := make([]any, len(s))
	for i, item := range s {
		hv, err := sm.hydrateValue(ctx, item, depth+1)
		if err != nil {
			return nil, err
		}
		hydrated[i] = hv
	}
	return hydrated, nil
}

func storageRefSelector(m map[string]any) (string, string, bool) {
	refRaw, exists := m[storageRefKey]
	if !exists {
		return "", "", false
	}
	refPath, ok := refRaw.(string)
	if !ok {
		return "", "", false
	}
	subPath := ""
	if rawPath, ok := m[storagePathKey]; ok {
		if typed, ok := rawPath.(string); ok {
			subPath = typed
		} else {
			return "", "", false
		}
	}
	for key := range m {
		if !isAllowedStorageRefKey(key) {
			return "", "", false
		}
	}
	return refPath, subPath, true
}

func singleConfigMapRef(m map[string]any) (*kubeRef, bool) {
	if ref, exists := m[configMapRefKey]; exists && len(m) == 1 {
		parsed, err := parseKubeRef(ref)
		if err == nil {
			return parsed, true
		}
	}
	return nil, false
}

func singleSecretRef(m map[string]any) (*kubeRef, bool) {
	if ref, exists := m[secretRefKey]; exists && len(m) == 1 {
		parsed, err := parseKubeRef(ref)
		if err == nil {
			return parsed, true
		}
	}
	return nil, false
}

func (sm *StorageManager) hydrateFromStorageRef(ctx context.Context, refPath string, depth int) (any, error) {
	if err := validateStorageRef(refPath); err != nil {
		return nil, fmt.Errorf("failed to read offloaded data from '%s': %w", refPath, err)
	}
	var buf bytes.Buffer
	if err := sm.store.Read(ctx, refPath, &buf); err != nil {
		return nil, fmt.Errorf("failed to read offloaded data from '%s': %w", refPath, err)
	}
	var storedObj StoredObject
	if err := json.Unmarshal(buf.Bytes(), &storedObj); err != nil {
		return nil, fmt.Errorf(
			"failed to unmarshal stored object from '%s', content may be malformed: %w",
			refPath, err,
		)
	}
	loaded, err := hydrateStoredObject(storedObj, refPath)
	if err != nil {
		return nil, err
	}
	return sm.hydrateValue(ctx, loaded, depth+1)
}

func (sm *StorageManager) hydrateFromStorageRefPath(ctx context.Context, refPath, subPath string, depth int) (any, error) {
	data, err := sm.hydrateFromStorageRef(ctx, refPath, depth)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(subPath) == "" {
		return data, nil
	}
	value, err := resolveStoragePath(data, subPath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve storage path '%s' in '%s': %w", subPath, refPath, err)
	}
	return sm.hydrateValue(ctx, value, depth+1)
}

func hydrateStoredObject(obj StoredObject, refPath string) (any, error) {
	switch obj.ContentType {
	case "json":
		var loaded any
		if err := json.Unmarshal(obj.Data, &loaded); err != nil {
			return nil, fmt.Errorf("failed to unmarshal hydrated JSON data from '%s': %w", refPath, err)
		}
		return loaded, nil
	case "raw":
		var raw string
		if err := json.Unmarshal(obj.Data, &raw); err != nil {
			return nil, fmt.Errorf("failed to unmarshal hydrated raw content from '%s': %w", refPath, err)
		}
		return raw, nil
	default:
		return nil, fmt.Errorf("unknown content type '%s' in stored object '%s'", obj.ContentType, refPath)
	}
}

func (sm *StorageManager) dehydrateValue(
	ctx context.Context,
	value any,
	id, keyPrefix string,
	depth int,
	pathPrefix string,
) (any, error) {
	maxDepth := getMaxRecursionDepth()
	if depth > maxDepth {
		return nil, fmt.Errorf("maximum recursion depth (%d) exceeded during dehydration", maxDepth)
	}
	switch v := value.(type) {
	case map[string]any:
		return sm.dehydrateMap(ctx, v, id, keyPrefix, depth, pathPrefix)
	case []any:
		return sm.dehydrateSlice(ctx, v, id, keyPrefix, depth, pathPrefix)
	case string:
		return sm.dehydrateString(ctx, v, id, keyPrefix, pathPrefix)
	default:
		return sm.dehydrateOther(ctx, v, id, keyPrefix, pathPrefix)
	}
}

func (sm *StorageManager) dehydrateMap(
	ctx context.Context,
	m map[string]any,
	id, keyPrefix string,
	depth int,
	pathPrefix string,
) (any, error) {
	dehydratedMap := make(map[string]any)
	hadOffload := false
	for k, val := range m {
		newPrefix := k
		if keyPrefix != "" {
			newPrefix = fmt.Sprintf("%s.%s", keyPrefix, k)
		}
		dv, err := sm.dehydrateValue(ctx, val, id, newPrefix, depth+1, pathPrefix)
		if err != nil {
			return nil, err
		}
		if containsStorageRef(dv, 0) {
			hadOffload = true
		}
		dehydratedMap[k] = dv
	}
	if !hadOffload {
		encodedMap, err := json.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal map for size check: %w", err)
		}
		if len(encodedMap) > sm.maxInlineSize {
			return sm.offloadValue(ctx, encodedMap, "json", id, keyPrefix, pathPrefix)
		}
	} else {
		// Some fields were individually offloaded, but the partially-dehydrated map may
		// still be too large to include inline. Check the encoded size of the result.
		encodedDehydrated, err := json.Marshal(dehydratedMap)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate dehydrated map size: %w", err)
		}
		if len(encodedDehydrated) > maxBulkOffloadSize {
			// Fall back to offloading the original map as a single unit.
			encodedMap, err := json.Marshal(m)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal map for bulk offload: %w", err)
			}
			return sm.offloadValue(ctx, encodedMap, "json", id, keyPrefix, pathPrefix)
		}
	}
	return dehydratedMap, nil
}

func (sm *StorageManager) dehydrateSlice(
	ctx context.Context,
	s []any,
	id, keyPrefix string,
	depth int,
	pathPrefix string,
) (any, error) {
	dehydrated := make([]any, len(s))
	hadOffload := false
	for i, item := range s {
		idx := strconv.Itoa(i)
		if keyPrefix != "" {
			idx = fmt.Sprintf("%s.%s", keyPrefix, idx)
		}
		dv, err := sm.dehydrateValue(ctx, item, id, idx, depth+1, pathPrefix)
		if err != nil {
			return nil, err
		}
		if containsStorageRef(dv, 0) {
			hadOffload = true
		}
		dehydrated[i] = dv
	}
	if !hadOffload {
		encodedSlice, err := json.Marshal(s)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal slice for size check: %w", err)
		}
		if len(encodedSlice) > sm.maxInlineSize {
			return sm.offloadValue(ctx, encodedSlice, "json", id, keyPrefix, pathPrefix)
		}
	} else {
		// Some items were individually offloaded, but the partially-dehydrated slice may
		// still be too large to include inline. Check the encoded size of the result.
		encodedDehydrated, err := json.Marshal(dehydrated)
		if err != nil {
			return nil, fmt.Errorf("failed to estimate dehydrated slice size: %w", err)
		}
		if len(encodedDehydrated) > maxBulkOffloadSize {
			// Fall back to offloading the original slice as a single unit.
			encodedSlice, err := json.Marshal(s)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal slice for bulk offload: %w", err)
			}
			return sm.offloadValue(ctx, encodedSlice, "json", id, keyPrefix, pathPrefix)
		}
	}
	return dehydrated, nil
}

func (sm *StorageManager) dehydrateString(
	ctx context.Context,
	v string,
	id, keyPrefix, pathPrefix string,
) (any, error) {
	if len(v) <= sm.maxInlineSize {
		return v, nil
	}
	marshaledString, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal raw string for storage: %w", err)
	}
	return sm.offloadValue(ctx, marshaledString, "raw", id, keyPrefix, pathPrefix)
}

func (sm *StorageManager) dehydrateOther(
	ctx context.Context,
	v any,
	id, keyPrefix, pathPrefix string,
) (any, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal value for size check: %w", err)
	}
	if len(encoded) <= sm.maxInlineSize {
		return v, nil
	}
	return sm.offloadValue(ctx, encoded, "json", id, keyPrefix, pathPrefix)
}

func storageRefContentType(contentType string) string {
	switch strings.ToLower(strings.TrimSpace(contentType)) {
	case "json":
		return "application/json"
	case "raw":
		return "text/plain"
	case "":
		return "application/octet-stream"
	default:
		return contentType
	}
}

// WriteBlob stores arbitrary binary data at the provided path with integrity metadata.
func (sm *StorageManager) WriteBlob(ctx context.Context, path string, contentType string, data []byte) error {
	if sm == nil || sm.store == nil {
		return fmt.Errorf("storage is disabled")
	}
	if err := validateStorageRef(path); err != nil {
		return fmt.Errorf("invalid storage path: %w", err)
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	meta := &writeMetadata{
		ContentType:   contentType,
		ContentLength: int64(len(data)),
	}
	md5Sum := md5.Sum(data)
	meta.ContentMD5 = base64.StdEncoding.EncodeToString(md5Sum[:])
	shaSum := sha256.Sum256(data)
	meta.ChecksumSHA256 = base64.StdEncoding.EncodeToString(shaSum[:])

	timeout := getStorageTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if err := sm.store.Write(withWriteMetadata(ctx, meta), path, bytes.NewReader(data)); err != nil {
		return fmt.Errorf("failed to write blob: %w", err)
	}
	return nil
}

// ReadBlob retrieves raw bytes from storage for the provided path.
func (sm *StorageManager) ReadBlob(ctx context.Context, path string) ([]byte, error) {
	if sm == nil || sm.store == nil {
		return nil, fmt.Errorf("storage is disabled")
	}
	if err := validateStorageRef(path); err != nil {
		return nil, fmt.Errorf("invalid storage path: %w", err)
	}
	timeout := getStorageTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var buf bytes.Buffer
	if err := sm.store.Read(ctx, path, &buf); err != nil {
		return nil, fmt.Errorf("failed to read blob: %w", err)
	}
	return buf.Bytes(), nil
}

// List enumerates storage object paths under the provided prefix when supported.
func (sm *StorageManager) List(ctx context.Context, prefix string) ([]string, error) {
	if sm == nil || sm.store == nil {
		return nil, fmt.Errorf("storage is disabled")
	}
	if err := validateStorageRef(prefix); err != nil {
		return nil, fmt.Errorf("invalid storage prefix: %w", err)
	}
	lister, ok := sm.store.(StoreLister)
	if !ok {
		return nil, ErrUnsupportedOperation
	}
	timeout := getStorageTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return lister.List(ctx, prefix)
}

// Delete removes the object at the provided path when supported.
func (sm *StorageManager) Delete(ctx context.Context, path string) error {
	if sm == nil || sm.store == nil {
		return fmt.Errorf("storage is disabled")
	}
	if err := validateStorageRef(path); err != nil {
		return fmt.Errorf("invalid storage path: %w", err)
	}
	deleter, ok := sm.store.(StoreDeleter)
	if !ok {
		return ErrUnsupportedOperation
	}
	timeout := getStorageTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return deleter.Delete(ctx, path)
}

// offloadValue handles the process of wrapping a value in a StoredObject,
// writing it to the storage backend, and returning a storage reference.
// Integrity metadata (content-type + checksums) is attached to the context so
// concrete Store implementations can apply it when persisting bytes.
func (sm *StorageManager) offloadValue(
	ctx context.Context,
	rawData []byte,
	contentType,
	id,
	keyPrefix,
	pathPrefix string,
) (any, error) {
	storedObj := StoredObject{
		ContentType: contentType,
		Data:        json.RawMessage(rawData),
	}

	fileName := fmt.Sprintf("%s.json", keyPrefix)
	if keyPrefix == "" {
		fileName = "output.json"
	}
	filePath := filepath.Join(pathPrefix, id, fileName)

	// Validate constructed path before writing (defense-in-depth; matches hydration path)
	if err := validateStorageRef(filePath); err != nil {
		return nil, fmt.Errorf("invalid storage path for offload: %w", err)
	}

	payloadBytes, err := json.Marshal(storedObj)
	if err != nil {
		return nil, fmt.Errorf("failed to encode stored object: %w", err)
	}

	meta := &writeMetadata{
		ContentType:   "application/json",
		ContentLength: int64(len(payloadBytes)),
	}
	md5Sum := md5.Sum(payloadBytes)
	meta.ContentMD5 = base64.StdEncoding.EncodeToString(md5Sum[:])
	shaSum := sha256.Sum256(payloadBytes)
	meta.ChecksumSHA256 = base64.StdEncoding.EncodeToString(shaSum[:])

	if err := sm.store.Write(withWriteMetadata(ctx, meta), filePath, bytes.NewReader(payloadBytes)); err != nil {
		return nil, fmt.Errorf("failed to write offloaded value for key '%s': %w", keyPrefix, err)
	}

	// Return the storage reference in place of the large value.
	ref := map[string]any{
		storageRefKey:  filePath,
		storageTypeKey: storageRefContentType(contentType),
	}
	metaCtx := storageMetadataFromContext(ctx)
	if metaCtx.Schema != "" {
		ref[storageSchemaKey] = metaCtx.Schema
	}
	if metaCtx.SchemaVersion != "" {
		ref[storageSchemaVer] = metaCtx.SchemaVersion
	}
	return ref, nil
}

// estimateSize estimates the JSON size of a value for metrics.
// This is a rough estimate and may not be exact.
func estimateSize(v any) int {
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(b)
}

// containsStorageRef walks the provided value looking for a map that is exactly a
// single key equal to the storageRefKey. It protects against deep recursion.
func containsStorageRef(value any, depth int) bool {
	maxDepth := getMaxRecursionDepth()
	if depth > maxDepth {
		return false
	}
	switch t := value.(type) {
	case map[string]any:
		if ref, ok := t[storageRefKey]; ok && isStorageRefMap(t) {
			_, isString := ref.(string)
			return isString
		}
		for _, v := range t {
			if containsStorageRef(v, depth+1) {
				return true
			}
		}
		return false
	case []any:
		for _, item := range t {
			if containsStorageRef(item, depth+1) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func isStorageRefMap(m map[string]any) bool {
	if m == nil {
		return false
	}
	for key := range m {
		if !isAllowedStorageRefKey(key) {
			return false
		}
	}
	return true
}

func isAllowedStorageRefKey(key string) bool {
	switch key {
	case storageRefKey, storagePathKey, storageTypeKey, storageSchemaKey, storageSchemaVer:
		return true
	default:
		return false
	}
}
