package v1alpha1

import (
	"encoding/json"
	"fmt"

	"github.com/xeipuuv/gojsonschema"

	"github.com/bubustack/bobrapet/internal/config"
)

func trimLeadingSpace(b []byte) []byte {
	for len(b) > 0 {
		if b[0] == ' ' || b[0] == '\n' || b[0] == '\t' || b[0] == '\r' {
			b = b[1:]
			continue
		}
		break
	}
	return b
}

func ensureJSONObject(field string, b []byte) error {
	if len(b) > 0 && b[0] != '{' {
		return fmt.Errorf("%s must be a JSON object", field)
	}
	return nil
}

func pickMaxInline(cfg *config.ControllerConfig) int {
	maxBytes := cfg.Engram.EngramControllerConfig.DefaultMaxInlineSize
	if maxBytes == 0 {
		maxBytes = 1024
	}
	return maxBytes
}

func enforceMaxBytes(field string, raw []byte, max int) error {
	if len(raw) > max {
		return fmt.Errorf("%s too large (%d bytes). Provide large payloads via object storage and references instead of inlining", field, len(raw))
	}
	return nil
}

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

// normalizeSchemaBytes takes a JSON-encoded schema and rewrites any
// property-level `required: true` flags into the parent object's `required`
// array, recursively. It preserves all other keywords and structure.
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

// normalizeSchemaNode walks an arbitrary JSON value. For any object with a
// `properties` map, it collects boolean `required: true` flags from its child
// property schemas and moves them into the object's `required` array.
// It also recurses into common nested schema locations like items,
// additionalProperties, allOf/anyOf/oneOf, $defs/definitions and
// patternProperties.
func normalizeSchemaNode(node any) any {
	switch typed := node.(type) {
	case map[string]any:
		return normalizeObjectSchema(typed)
	case []any:
		for i := range typed {
			typed[i] = normalizeSchemaNode(typed[i])
		}
		return typed
	default:
		return node
	}
}

func normalizeObjectSchema(obj map[string]any) map[string]any {
	requiredSet := liftInlineRequiredFlags(obj)
	mergeRequiredSet(obj, requiredSet)
	normalizeNestedSchemaLocations(obj)
	return obj
}

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

func normalizeNestedSchemaLocations(obj map[string]any) {
	normalizeItemsNode(obj)
	normalizeSingleSchemaField(obj, "additionalProperties")
	normalizeMapOfSchemas(obj, "patternProperties")
	normalizeSchemaSlice(obj, "allOf")
	normalizeSchemaSlice(obj, "anyOf")
	normalizeSchemaSlice(obj, "oneOf")
	normalizeMapOfSchemas(obj, "definitions")
	normalizeMapOfSchemas(obj, "$defs")
	normalizeSingleSchemaField(obj, "not")
}

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

func normalizeSingleSchemaField(obj map[string]any, key string) {
	if raw, has := obj[key]; has {
		if schemaMap, ok := raw.(map[string]any); ok {
			obj[key] = normalizeSchemaNode(schemaMap)
		}
	}
}

func normalizeMapOfSchemas(obj map[string]any, key string) {
	raw, has := obj[key].(map[string]any)
	if !has {
		return
	}
	for k, v := range raw {
		raw[k] = normalizeSchemaNode(v)
	}
}

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
