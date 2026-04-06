package v1alpha1

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bubustack/bobrapet/pkg/storage"
)

// ValidateReferenceMaps ensures configmap/secret/storage ref maps are well-formed.
//
// Behavior:
//   - Parses doc JSON into an interface tree.
//   - Walks maps/slices, validating any reference maps encountered.
//   - Returns the first error with a JSON path context.
//
// Arguments:
//   - doc []byte: JSON document bytes.
//   - field string: field label for error messages.
//
// Returns:
//   - nil if no invalid reference maps are found.
//   - Error with field/path details otherwise.
func ValidateReferenceMaps(doc []byte, field string) error {
	trimmed := TrimLeadingSpace(doc)
	if len(trimmed) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return fmt.Errorf("%s must be valid JSON: %w", field, err)
	}
	if err := validateReferenceMaps(value, "$"); err != nil {
		return fmt.Errorf("%s %w", field, err)
	}
	return nil
}

func validateReferenceMaps(value any, path string) error {
	switch v := value.(type) {
	case map[string]any:
		if _, ok := v[storage.StorageRefKey]; ok {
			if err := validateStorageRefMap(v); err != nil {
				return fmt.Errorf("has invalid storage ref at %s: %w", path, err)
			}
			return nil
		}
		if _, ok := v["$bubuConfigMapRef"]; ok {
			if err := validateConfigMapRefMap(v); err != nil {
				return fmt.Errorf("has invalid configmap ref at %s: %w", path, err)
			}
			return nil
		}
		if _, ok := v["$bubuSecretRef"]; ok {
			if err := validateSecretRefMap(v); err != nil {
				return fmt.Errorf("has invalid secret ref at %s: %w", path, err)
			}
			return nil
		}
		for key, entry := range v {
			next := path + "." + key
			if err := validateReferenceMaps(entry, next); err != nil {
				return err
			}
		}
	case []any:
		for i, entry := range v {
			next := fmt.Sprintf("%s[%d]", path, i)
			if err := validateReferenceMaps(entry, next); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateStorageRefMap(m map[string]any) error {
	if m == nil {
		return fmt.Errorf("storage ref map is nil")
	}
	for key := range m {
		if !isAllowedStorageRefKey(key) {
			return fmt.Errorf("storage ref has unexpected key %q", key)
		}
	}
	ref, ok := m[storage.StorageRefKey]
	if !ok {
		return fmt.Errorf("storage ref is missing %s", storage.StorageRefKey)
	}
	refStr, ok := ref.(string)
	if !ok || strings.TrimSpace(refStr) == "" {
		return fmt.Errorf("storage ref must be a non-empty string")
	}
	if path, ok := m[storage.StoragePathKey]; ok {
		if _, ok := path.(string); !ok {
			return fmt.Errorf("storage path must be a string")
		}
	}
	for _, key := range []string{storage.StorageContentTypeKey, storage.StorageSchemaKey, storage.StorageSchemaVersionKey} {
		if val, ok := m[key]; ok {
			if _, ok := val.(string); !ok {
				return fmt.Errorf("%s must be a string", key)
			}
		}
	}
	return nil
}

func validateConfigMapRefMap(m map[string]any) error {
	if m == nil {
		return fmt.Errorf("configmap ref map is nil")
	}
	if len(m) != 1 {
		return fmt.Errorf("configmap ref must only contain $bubuConfigMapRef")
	}
	return validateKubeRefValue(m["$bubuConfigMapRef"], "configmap")
}

func validateSecretRefMap(m map[string]any) error {
	if m == nil {
		return fmt.Errorf("secret ref map is nil")
	}
	if len(m) != 1 {
		return fmt.Errorf("secret ref must only contain $bubuSecretRef")
	}
	return validateKubeRefValue(m["$bubuSecretRef"], "secret")
}

func validateKubeRefValue(value any, refType string) error {
	switch v := value.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("%s reference string must be non-empty", refType)
		}
		return nil
	case map[string]any:
		return validateKubeRefMap(v, refType)
	default:
		return fmt.Errorf("%s reference must be string or object", refType)
	}
}

func validateKubeRefMap(m map[string]any, refType string) error {
	allowed := map[string]struct{}{
		"name":      {},
		"key":       {},
		"namespace": {},
		"format":    {},
	}
	for key := range m {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("%s ref has unexpected key %q", refType, key)
		}
	}
	name, _ := m["name"].(string)
	key, _ := m["key"].(string)
	if strings.TrimSpace(name) == "" || strings.TrimSpace(key) == "" {
		return fmt.Errorf("%s ref requires name and key", refType)
	}
	if namespace, ok := m["namespace"].(string); ok {
		if strings.TrimSpace(namespace) == "" {
			return fmt.Errorf("%s ref namespace cannot be empty", refType)
		}
	}
	if format, ok := m["format"].(string); ok && strings.TrimSpace(format) != "" {
		switch strings.ToLower(strings.TrimSpace(format)) {
		case "auto", "json", "raw":
			// ok
		default:
			return fmt.Errorf("%s ref format must be auto, json, or raw", refType)
		}
	}
	return nil
}

func isAllowedStorageRefKey(key string) bool {
	switch key {
	case storage.StorageRefKey,
		storage.StoragePathKey,
		storage.StorageContentTypeKey,
		storage.StorageSchemaKey,
		storage.StorageSchemaVersionKey:
		return true
	default:
		return false
	}
}
