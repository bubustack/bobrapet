package v1alpha1

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestEnsureJSONObject(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr string
	}{
		{name: "valid object", input: `{"a":1}`},
		{name: "array", input: `[1,2]`, wantErr: "must be a JSON object"},
		{name: "invalid json", input: `{"a":`, wantErr: "must be valid JSON"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := EnsureJSONObject("field", []byte(tt.input))
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestNormalizeSchemaBytesWrapsOneOf(t *testing.T) {
	schema := []byte(`{
		"oneOf": [
			{"type": "object", "properties": {"a": {"type": "string"}}},
			{"type": "object", "properties": {"b": {"type": "string"}}}
		]
	}`)

	normalized, err := normalizeSchemaBytes(schema)
	if err != nil {
		t.Fatalf("normalizeSchemaBytes error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(normalized, &out); err != nil {
		t.Fatalf("failed to unmarshal normalized schema: %v", err)
	}

	anyOf, ok := out["anyOf"].([]any)
	if !ok || len(anyOf) == 0 {
		t.Fatalf("expected top-level anyOf wrapper, got %v", out)
	}
	first, ok := anyOf[0].(map[string]any)
	if !ok {
		t.Fatalf("expected first anyOf entry to be an object, got %T", anyOf[0])
	}
	if _, ok := first["oneOf"]; !ok {
		t.Fatalf("expected wrapped schema to retain oneOf, got %v", first)
	}
}

func TestNormalizeSchemaBytesNormalizesIf(t *testing.T) {
	schema := []byte(`{
		"type": "object",
		"if": {"type": "string"}
	}`)

	normalized, err := normalizeSchemaBytes(schema)
	if err != nil {
		t.Fatalf("normalizeSchemaBytes error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(normalized, &out); err != nil {
		t.Fatalf("failed to unmarshal normalized schema: %v", err)
	}

	ifSchema, ok := out["if"].(map[string]any)
	if !ok {
		t.Fatalf("expected 'if' to be an object schema, got %T", out["if"])
	}
	if _, ok := ifSchema["anyOf"]; !ok {
		t.Fatalf("expected 'if' schema to be normalized with anyOf alternatives, got %v", ifSchema)
	}
}
