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

package v1alpha1

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"github.com/xeipuuv/gojsonschema"

	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/bobrapet/pkg/storage"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const templateStringPattern = `^\s*\$?\{\{[\s\S]+\}\}\s*$`

// ResolveControllerConfig returns the effective controller configuration.
//
// Behavior:
//   - Checks ConfigManager first for live configuration.
//   - Falls back to embedded cfg if ConfigManager returns nil.
//   - Uses DefaultControllerConfig() as final fallback.
//   - Logs at V(1) level when falling back for observability.
//
// Arguments:
//   - log logr.Logger: for logging fallback events.
//   - mgr *config.OperatorConfigManager: optional ConfigManager for live config.
//   - cfg *config.ControllerConfig: optional embedded config fallback.
//
// Returns:
//   - Non-nil *config.ControllerConfig from the highest priority source.
func ResolveControllerConfig(log logr.Logger, mgr *config.OperatorConfigManager, cfg *config.ControllerConfig) *config.ControllerConfig {
	if mgr != nil {
		if resolved := mgr.GetControllerConfig(); resolved != nil {
			return resolved
		}
		log.V(1).Info("ConfigManager returned nil, falling back to embedded config")
	}
	if cfg != nil {
		return cfg
	}
	log.V(1).Info("No config available, using defaults")
	return config.DefaultControllerConfig()
}

// ResolveReferencePolicy returns the effective cross-namespace reference policy.
func ResolveReferencePolicy(cfg *config.ControllerConfig) string {
	if cfg == nil {
		cfg = config.DefaultControllerConfig()
	}
	policy := strings.TrimSpace(cfg.ReferenceCrossNamespacePolicy)
	if policy == "" {
		return config.ReferenceCrossNamespacePolicyDeny
	}
	return policy
}

// ValidateCrossNamespaceReference enforces cross-namespace policy for a reference.
//
// Returns nil when the reference is same-namespace or allowed by policy.
func ValidateCrossNamespaceReference(
	ctx context.Context,
	reader client.Reader,
	cfg *config.ControllerConfig,
	from client.Object,
	fromGroup string,
	fromKind string,
	toGroup string,
	toKind string,
	toNamespace string,
	toName string,
	refLabel string,
) error {
	if from == nil {
		return fmt.Errorf("reference check requires a referencing object")
	}
	fromNamespace := strings.TrimSpace(from.GetNamespace())
	targetNamespace := strings.TrimSpace(toNamespace)
	if targetNamespace == "" || fromNamespace == "" || targetNamespace == fromNamespace {
		return nil
	}

	switch ResolveReferencePolicy(cfg) {
	case config.ReferenceCrossNamespacePolicyAllow:
		return nil
	case config.ReferenceCrossNamespacePolicyGrant:
		allowed, err := refs.ReferenceGranted(ctx, reader, refs.GrantCheck{
			FromGroup:     strings.TrimSpace(fromGroup),
			FromKind:      strings.TrimSpace(fromKind),
			FromNamespace: fromNamespace,
			ToGroup:       strings.TrimSpace(toGroup),
			ToKind:        strings.TrimSpace(toKind),
			ToNamespace:   targetNamespace,
			ToName:        strings.TrimSpace(toName),
		})
		if err != nil {
			return fmt.Errorf("failed to evaluate ReferenceGrant for %s: %w", refLabel, err)
		}
		if !allowed {
			return fmt.Errorf("cross-namespace %s reference to %s/%s is not permitted (no ReferenceGrant)", refLabel, targetNamespace, toName)
		}
		return nil
	default:
		return fmt.Errorf("cross-namespace %s references are not allowed", refLabel)
	}
}

// TrimLeadingSpace removes leading ASCII whitespace characters from b.
//
// Behavior:
//   - Strips space, newline, tab, and carriage return from the start of b.
//   - Returns the remaining slice without allocating new memory.
//
// Arguments:
//   - b []byte: the byte slice to trim.
//
// Returns:
//   - The input slice with leading whitespace removed.
func TrimLeadingSpace(b []byte) []byte {
	for len(b) > 0 {
		if b[0] == ' ' || b[0] == '\n' || b[0] == '\t' || b[0] == '\r' {
			b = b[1:]
			continue
		}
		break
	}
	return b
}

// EnsureJSONObject returns an error when b is non-empty and not a valid JSON object.
//
// Behavior:
//   - Validates JSON syntax when b is non-empty.
//   - Checks if b starts with '{' (JSON object).
//   - Returns error if b is non-empty and starts with array '[' or primitive.
//   - Callers should trim leading whitespace before invoking.
//
// Arguments:
//   - field string: field name for error message (e.g., "spec.with").
//   - b []byte: the byte slice to check.
//
// Returns:
//   - nil if b is empty or starts with '{'.
//   - Error if b is a non-object JSON value.
func EnsureJSONObject(field string, b []byte) error {
	if len(b) == 0 {
		return nil
	}
	if !json.Valid(b) {
		return fmt.Errorf("%s must be valid JSON", field)
	}
	trimmed := TrimLeadingSpace(b)
	if len(trimmed) > 0 && trimmed[0] != '{' {
		return fmt.Errorf("%s must be a JSON object", field)
	}
	return nil
}

// PickMaxInlineBytes returns the maximum inline payload size (in bytes) enforced by webhook validation.
//
// Behavior:
//   - Uses cfg.Engram.EngramControllerConfig.DefaultMaxInlineSize if set.
//   - Falls back to config.DefaultControllerConfig() if cfg is nil.
//   - Falls back to 1024 bytes if the config value is zero.
//
// Arguments:
//   - cfg *config.ControllerConfig: may be nil.
//
// Returns:
//   - Maximum allowed inline bytes (at least 1024).
func PickMaxInlineBytes(cfg *config.ControllerConfig) int {
	if cfg == nil {
		cfg = config.DefaultControllerConfig()
	}
	maxBytes := cfg.Engram.EngramControllerConfig.DefaultMaxInlineSize
	if maxBytes == 0 {
		maxBytes = 1024
	}
	return maxBytes
}

// EnforceMaxBytes returns an error when raw exceeds max.
//
// Behavior:
//   - Compares len(raw) against max bytes.
//   - Appends hint to error message for context-specific guidance.
//
// Arguments:
//   - field string: field name for error message (e.g., "spec.with").
//   - raw []byte: the payload to check.
//   - max int: maximum allowed bytes.
//   - hint string: additional guidance appended to error (can be empty).
//
// Returns:
//   - nil if len(raw) <= max.
//   - Error describing the size violation.
func EnforceMaxBytes(field string, raw []byte, max int, hint string) error {
	if len(raw) > max {
		message := "%s too large (%d bytes > %d)"
		if hint != "" {
			message += ". " + hint
		}
		return fmt.Errorf(message, field, len(raw), max)
	}
	return nil
}

// validateJSONAgainstSchema validates a JSON document against a JSON Schema.
//
// Behavior:
//   - Normalizes the schema to support inline `required: true` flags.
//   - Uses gojsonschema to perform JSON Schema validation.
//   - Collects all validation errors into a single error message.
//
// Arguments:
//   - doc []byte: the JSON document to validate.
//   - schema []byte: the JSON Schema bytes.
//   - schemaName string: human-readable name for error messages (e.g., "EngramTemplate").
//
// Returns:
//   - nil if validation passes.
//   - Error describing schema normalization failure or validation errors.
func validateJSONAgainstSchema(doc []byte, schema []byte, schemaName string) error {
	// Normalize schema to support field-level required booleans (required: true)
	// by translating them into standard JSON Schema required arrays at the
	// appropriate object levels. This allows template authors to mark
	// properties as required inline without manually maintaining a separate
	// parent-level required list.
	normalizedSchema, err := normalizeSchemaBytes(schema)
	if err != nil {
		return fmt.Errorf("error validating against %s schema: failed to normalize schema: %w", schemaName, err)
	}

	schemaLoader := gojsonschema.NewStringLoader(string(normalizedSchema))
	documentLoader := gojsonschema.NewStringLoader(string(doc))
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return fmt.Errorf("error validating against %s schema: %w", schemaName, err)
	}
	if !result.Valid() {
		var errs []string
		for _, desc := range result.Errors() {
			errs = append(errs, desc.String())
		}
		return fmt.Errorf("object is invalid against %s schema: %v", schemaName, errs)
	}
	return nil
}

// ValidateJSONAgainstSchema is a shared wrapper for schema validation across webhooks.
//
// Behavior:
//   - Delegates to the internal validator, which normalizes schemas and allows storage refs.
//
// Arguments:
//   - doc []byte: the JSON document to validate.
//   - schema []byte: the JSON Schema bytes.
//   - schemaName string: human-readable name for error messages.
//
// Returns:
//   - nil if validation passes.
//   - Error describing normalization/validation failures.
func ValidateJSONAgainstSchema(doc []byte, schema []byte, schemaName string) error {
	return validateJSONAgainstSchema(doc, schema, schemaName)
}

// ValidateJSONSchemaDefinition validates that a JSON Schema is syntactically valid.
//
// Behavior:
//   - Normalizes the schema to support inline required flags.
//   - Uses gojsonschema to parse the schema without validating a document.
//
// Arguments:
//   - schema []byte: JSON-encoded JSON Schema.
//   - schemaName string: human-readable name for error messages.
//
// Returns:
//   - nil if schema is valid.
//   - Error describing schema parsing failures.
func ValidateJSONSchemaDefinition(schema []byte, schemaName string) error {
	if len(schema) == 0 {
		return nil
	}
	normalizedSchema, err := normalizeSchemaBytes(schema)
	if err != nil {
		return fmt.Errorf("error validating %s schema: failed to normalize schema: %w", schemaName, err)
	}
	schemaLoader := gojsonschema.NewBytesLoader(normalizedSchema)
	if _, err := gojsonschema.NewSchema(schemaLoader); err != nil {
		return fmt.Errorf("error validating %s schema: %w", schemaName, err)
	}
	return nil
}

// normalizeSchemaBytes rewrites property-level `required: true` flags into
// the parent object's `required` array.
//
// Behavior:
//   - Parses the schema JSON into a generic map structure.
//   - Recursively walks all properties and nested schema locations.
//   - Lifts inline `required: true` flags to parent `required` arrays.
//   - Preserves all other JSON Schema keywords and structure.
//
// Arguments:
//   - schema []byte: JSON-encoded JSON Schema.
//
// Returns:
//   - Normalized schema bytes with inline required flags converted.
//   - Error if JSON parsing or marshaling fails.
func normalizeSchemaBytes(schema []byte) ([]byte, error) {
	if len(schema) == 0 {
		return schema, nil
	}
	var root any
	if err := json.Unmarshal(schema, &root); err != nil {
		return nil, err
	}
	normalized := normalizeSchemaNode(root)
	out, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// normalizeSchemaNode walks an arbitrary JSON value.
//
// Behavior:
//   - For objects with `properties`, lifts boolean `required: true` flags.
//   - Recursively walks nested schema locations (items, allOf, etc.).
//   - Passes through primitives and arrays unchanged.
//
// Arguments:
//   - node any: arbitrary JSON value (object, array, or primitive).
//
// Returns:
//   - Normalized node with inline required flags converted.
func normalizeSchemaNode(node any) any {
	switch typed := node.(type) {
	case map[string]any:
		normalized := normalizeObjectSchema(typed)
		return allowStorageRefAlternative(normalized)
	case []any:
		for i := range typed {
			typed[i] = normalizeSchemaNode(typed[i])
		}
		return typed
	default:
		return node
	}
}

// normalizeObjectSchema processes a single schema object.
//
// Behavior:
//   - Lifts inline `required: true` flags from properties.
//   - Merges lifted flags into the parent required array.
//   - Recurses into nested schema locations (items, allOf, etc.).
//
// Arguments:
//   - obj map[string]any: a JSON object representing a schema.
//
// Returns:
//   - The same map, mutated with normalized required arrays.
func normalizeObjectSchema(obj map[string]any) map[string]any {
	requiredSet := liftInlineRequiredFlags(obj)
	mergeRequiredSet(obj, requiredSet)
	normalizeNestedSchemaLocations(obj)
	return obj
}

func allowStorageRefAlternative(schema map[string]any) any {
	if schema == nil || len(schema) == 0 {
		return schema
	}
	if isImplicitRefSchema(schema) {
		return schema
	}
	if anyOf, ok := schema["anyOf"].([]any); ok {
		if !schemaSliceHasImplicitRef(anyOf) {
			schema["anyOf"] = append(anyOf, storageRefSchema(), configMapRefSchema(), secretRefSchema())
		}
		if !schemaSliceHasTemplateString(anyOf) {
			schema["anyOf"] = append(schema["anyOf"].([]any), templateStringSchema())
		}
		return schema
	}
	if _, ok := schema["oneOf"].([]any); ok {
		return wrapWithAlternatives(schema)
	}

	return wrapWithAlternatives(schema)
}

func wrapWithAlternatives(schema map[string]any) map[string]any {
	wrapped := map[string]any{
		"anyOf": []any{schema, storageRefSchema(), configMapRefSchema(), secretRefSchema(), templateStringSchema()},
	}
	if title, ok := schema["title"]; ok {
		wrapped["title"] = title
	}
	if desc, ok := schema["description"]; ok {
		wrapped["description"] = desc
	}
	return wrapped
}

func schemaSliceHasImplicitRef(schemas []any) bool {
	for _, entry := range schemas {
		if schemaMap, ok := entry.(map[string]any); ok && isImplicitRefSchema(schemaMap) {
			return true
		}
	}
	return false
}

func schemaSliceHasTemplateString(schemas []any) bool {
	for _, entry := range schemas {
		if schemaMap, ok := entry.(map[string]any); ok && isTemplateStringSchema(schemaMap) {
			return true
		}
	}
	return false
}

func isImplicitRefSchema(schema map[string]any) bool {
	if isStorageRefSchema(schema) || isConfigMapRefSchema(schema) || isSecretRefSchema(schema) {
		return true
	}
	return false
}

func isStorageRefSchema(schema map[string]any) bool {
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	if _, hasRef := props[storage.StorageRefKey]; !hasRef {
		return false
	}
	// If additionalProperties is allowed (or defaulted), extra storage ref keys are valid.
	if schemaAllowsAdditionalProperties(schema) {
		return true
	}
	// Otherwise require the full storage ref shape to be explicitly declared.
	for _, key := range []string{
		storage.StoragePathKey,
		storage.StorageContentTypeKey,
		storage.StorageSchemaKey,
		storage.StorageSchemaVersionKey,
	} {
		if _, ok := props[key]; !ok {
			return false
		}
	}
	return true
}

func isConfigMapRefSchema(schema map[string]any) bool {
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, hasRef := props["$bubuConfigMapRef"]
	return hasRef
}

func isSecretRefSchema(schema map[string]any) bool {
	props, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, hasRef := props["$bubuSecretRef"]
	return hasRef
}

// ContainsStorageRef reports whether the JSON document contains a storage reference map.
//
// Behavior:
//   - Returns false for empty or invalid JSON.
//   - Walks nested objects/arrays to detect any map with $bubuStorageRef.
func ContainsStorageRef(doc []byte) bool {
	trimmed := TrimLeadingSpace(doc)
	if len(trimmed) == 0 {
		return false
	}
	var value any
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return false
	}
	return containsStorageRefValue(value)
}

// ScrubStorageRefMetadata removes storage ref metadata keys from any storage ref maps.
//
// Behavior:
//   - Removes $bubuStorageContentType, $bubuStorageSchema, and $bubuStorageSchemaVersion
//     when a map contains $bubuStorageRef.
//   - Returns the original document for empty input.
//   - Returns an error for invalid JSON.
func ScrubStorageRefMetadata(doc []byte) ([]byte, error) {
	trimmed := TrimLeadingSpace(doc)
	if len(trimmed) == 0 {
		return doc, nil
	}
	var value any
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return nil, err
	}
	scrubStorageRefMetadataValue(value)
	return json.Marshal(value)
}

func containsStorageRefValue(value any) bool {
	switch v := value.(type) {
	case map[string]any:
		if _, ok := v[storage.StorageRefKey]; ok {
			return true
		}
		for _, entry := range v {
			if containsStorageRefValue(entry) {
				return true
			}
		}
	case []any:
		for _, entry := range v {
			if containsStorageRefValue(entry) {
				return true
			}
		}
	}
	return false
}

func scrubStorageRefMetadataValue(value any) {
	switch v := value.(type) {
	case map[string]any:
		if _, ok := v[storage.StorageRefKey]; ok {
			delete(v, storage.StorageContentTypeKey)
			delete(v, storage.StorageSchemaKey)
			delete(v, storage.StorageSchemaVersionKey)
		}
		for _, entry := range v {
			scrubStorageRefMetadataValue(entry)
		}
	case []any:
		for _, entry := range v {
			scrubStorageRefMetadataValue(entry)
		}
	}
}

func isTemplateStringSchema(schema map[string]any) bool {
	if schema["type"] != "string" {
		return false
	}
	pattern, hasPattern := schema["pattern"].(string)
	return hasPattern && pattern == templateStringPattern
}

func schemaAllowsAdditionalProperties(schema map[string]any) bool {
	raw, has := schema["additionalProperties"]
	if !has {
		return true
	}
	if b, ok := raw.(bool); ok {
		return b
	}
	// When additionalProperties is a schema object, treat as "not guaranteed" to allow
	// storage metadata keys so callers still get storage ref alternatives.
	return false
}

func storageRefSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			storage.StorageRefKey:           map[string]any{"type": "string"},
			storage.StoragePathKey:          map[string]any{"type": "string"},
			storage.StorageContentTypeKey:   map[string]any{"type": "string"},
			storage.StorageSchemaKey:        map[string]any{"type": "string"},
			storage.StorageSchemaVersionKey: map[string]any{"type": "string"},
		},
		"required": []any{storage.StorageRefKey},
	}
}

func configMapRefSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"$bubuConfigMapRef": map[string]any{
				"anyOf": []any{
					map[string]any{"type": "string"},
					map[string]any{
						"type":                 "object",
						"additionalProperties": false,
						"properties": map[string]any{
							"name":      map[string]any{"type": "string"},
							"key":       map[string]any{"type": "string"},
							"namespace": map[string]any{"type": "string"},
							"format": map[string]any{
								"type": "string",
								"enum": []any{"auto", "json", "raw"},
							},
						},
						"required": []any{"name", "key"},
					},
				},
			},
		},
		"required": []any{"$bubuConfigMapRef"},
	}
}

func secretRefSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"properties": map[string]any{
			"$bubuSecretRef": map[string]any{
				"anyOf": []any{
					map[string]any{"type": "string"},
					map[string]any{
						"type":                 "object",
						"additionalProperties": false,
						"properties": map[string]any{
							"name":      map[string]any{"type": "string"},
							"key":       map[string]any{"type": "string"},
							"namespace": map[string]any{"type": "string"},
							"format": map[string]any{
								"type": "string",
								"enum": []any{"auto", "json", "raw"},
							},
						},
						"required": []any{"name", "key"},
					},
				},
			},
		},
		"required": []any{"$bubuSecretRef"},
	}
}

func templateStringSchema() map[string]any {
	return map[string]any{
		"type":    "string",
		"pattern": templateStringPattern,
	}
}

// liftInlineRequiredFlags scans properties for inline required flags.
//
// Behavior:
//   - Iterates through the "properties" map.
//   - Strips boolean `required: true` from each property schema.
//   - Collects property names with required=true into a set.
//   - Recursively normalizes nested property schemas.
//
// Arguments:
//   - obj map[string]any: schema object containing a "properties" map.
//
// Returns:
//   - Set of property names that had `required: true` (now removed).
//   - nil if no properties or no inline required flags.
func liftInlineRequiredFlags(obj map[string]any) map[string]struct{} {
	props, hasProps := obj["properties"].(map[string]any)
	if !hasProps {
		return nil
	}

	requiredSet := map[string]struct{}{}
	for propName, rawChild := range props {
		cleaned := stripBooleanRequired(rawChild, propName, requiredSet)
		props[propName] = normalizeSchemaNode(cleaned)
	}
	if len(requiredSet) == 0 {
		return nil
	}
	return requiredSet
}

// stripBooleanRequired checks for and removes boolean required flags.
//
// Behavior:
//   - Returns node unchanged if not a map.
//   - If node has `required: true`, removes it and adds propName to requiredSet.
//   - If node has `required: false`, removes the flag without adding to set.
//
// Arguments:
//   - node any: a property schema (may be any type).
//   - propName string: the property name (for adding to requiredSet).
//   - requiredSet map[string]struct{}: collects required property names.
//
// Returns:
//   - The node, potentially mutated (required flag removed).
func stripBooleanRequired(node any, propName string, requiredSet map[string]struct{}) any {
	childMap, ok := node.(map[string]any)
	if !ok {
		return node
	}
	if raw, has := childMap["required"]; has {
		if b, ok := raw.(bool); ok {
			if b {
				requiredSet[propName] = struct{}{}
			}
			delete(childMap, "required")
		}
	}
	return childMap
}

// mergeRequiredSet merges lifted required flags into the required array.
//
// Behavior:
//   - Returns early if requiredSet is empty.
//   - Extracts existing required array from obj.
//   - Appends new names, avoiding duplicates.
//   - Updates obj["required"] with the merged array.
//
// Arguments:
//   - obj map[string]any: schema object to update.
//   - requiredSet map[string]struct{}: property names to add.
//
// Side Effects:
//   - Mutates obj["required"] with merged values.
func mergeRequiredSet(obj map[string]any, requiredSet map[string]struct{}) {
	if len(requiredSet) == 0 {
		return
	}

	existingList := extractExistingRequired(obj)
	seen := make(map[string]struct{}, len(existingList))
	for _, name := range existingList {
		seen[name] = struct{}{}
	}

	for name := range requiredSet {
		if _, already := seen[name]; !already {
			existingList = append(existingList, name)
		}
	}

	out := make([]any, 0, len(existingList))
	for _, name := range existingList {
		out = append(out, name)
	}
	obj["required"] = out
}

// extractExistingRequired extracts the required array from a schema object.
//
// Behavior:
//   - Returns nil if "required" key is absent.
//   - Handles []any (JSON unmarshal result) and []string types.
//   - Extracts only string values; ignores non-string entries.
//
// Arguments:
//   - obj map[string]any: schema object.
//
// Returns:
//   - Slice of required property names.
//   - nil if absent or malformed.
func extractExistingRequired(obj map[string]any) []string {
	raw, has := obj["required"]
	if !has {
		return nil
	}

	switch typed := raw.(type) {
	case []any:
		var result []string
		for _, v := range typed {
			if s, ok := v.(string); ok {
				result = append(result, s)
			}
		}
		return result
	case []string:
		return append([]string{}, typed...)
	default:
		return nil
	}
}

// normalizeNestedSchemaLocations recurses into common JSON Schema keywords.
//
// Behavior:
//   - Normalizes items (array schema).
//   - Normalizes additionalProperties and not (single schema).
//   - Normalizes patternProperties, definitions, $defs (map of schemas).
//   - Normalizes allOf, anyOf, oneOf (array of schemas).
//
// Arguments:
//   - obj map[string]any: schema object to process.
//
// Side Effects:
//   - Mutates nested schema locations in obj.
func normalizeNestedSchemaLocations(obj map[string]any) {
	normalizeItemsNode(obj)
	normalizeSingleSchemaField(obj, "additionalProperties")
	normalizeSingleSchemaField(obj, "additionalItems")
	normalizeSingleSchemaField(obj, "contains")
	normalizeSingleSchemaField(obj, "contentSchema")
	normalizeSingleSchemaField(obj, "if")
	normalizeSingleSchemaField(obj, "then")
	normalizeSingleSchemaField(obj, "else")
	normalizeSingleSchemaField(obj, "propertyNames")
	normalizeSingleSchemaField(obj, "unevaluatedItems")
	normalizeSingleSchemaField(obj, "unevaluatedProperties")
	normalizeMapOfSchemas(obj, "patternProperties")
	normalizeMapOfSchemas(obj, "dependencies")
	normalizeMapOfSchemas(obj, "dependentSchemas")
	normalizeSchemaSlice(obj, "allOf")
	normalizeSchemaSlice(obj, "anyOf")
	normalizeSchemaSlice(obj, "oneOf")
	normalizeSchemaSlice(obj, "prefixItems")
	normalizeMapOfSchemas(obj, "definitions")
	normalizeMapOfSchemas(obj, "$defs")
	normalizeSingleSchemaField(obj, "not")
}

// normalizeItemsNode normalizes the "items" keyword.
//
// Behavior:
//   - Handles items as single schema (map) or tuple schemas (array).
//   - Recursively normalizes the schema(s).
//
// Arguments:
//   - obj map[string]any: schema object containing "items".
//
// Side Effects:
//   - Mutates obj["items"] with normalized schema(s).
func normalizeItemsNode(obj map[string]any) {
	items, has := obj["items"]
	if !has {
		return
	}
	switch typed := items.(type) {
	case map[string]any, []any:
		obj["items"] = normalizeSchemaNode(typed)
	}
}

// normalizeSingleSchemaField normalizes a single nested schema at a key.
//
// Arguments:
//   - obj map[string]any: schema object.
//   - key string: key name (e.g., "additionalProperties", "not").
//
// Side Effects:
//   - Mutates obj[key] with normalized schema if present.
func normalizeSingleSchemaField(obj map[string]any, key string) {
	if raw, has := obj[key]; has {
		if schemaMap, ok := raw.(map[string]any); ok {
			obj[key] = normalizeSchemaNode(schemaMap)
		}
	}
}

// normalizeMapOfSchemas normalizes a map of schemas at a key.
//
// Arguments:
//   - obj map[string]any: schema object.
//   - key string: key name (e.g., "definitions", "$defs", "patternProperties").
//
// Side Effects:
//   - Mutates each schema in obj[key] if present.
func normalizeMapOfSchemas(obj map[string]any, key string) {
	raw, has := obj[key].(map[string]any)
	if !has {
		return
	}
	for k, v := range raw {
		raw[k] = normalizeSchemaNode(v)
	}
}

// normalizeSchemaSlice normalizes an array of schemas at a key.
//
// Arguments:
//   - obj map[string]any: schema object.
//   - key string: key name (e.g., "allOf", "anyOf", "oneOf").
//
// Side Effects:
//   - Mutates each schema in obj[key] if present.
func normalizeSchemaSlice(obj map[string]any, key string) {
	raw, has := obj[key].([]any)
	if !has {
		return
	}
	for i := range raw {
		raw[i] = normalizeSchemaNode(raw[i])
	}
	obj[key] = raw
}
