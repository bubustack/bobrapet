// Package validation provides shared validation utilities used across the codebase.
package validation

import (
	"encoding/json"
	"fmt"
)

// ValidateJSONSchema performs basic JSON schema validation by attempting to unmarshal
// the provided bytes as JSON, ensuring the schema is syntactically valid.
//
// Arguments:
//   - schemaBytes: raw JSON bytes representing a JSON schema.
//
// Returns:
//   - error: nil if the JSON is valid, or an error describing the validation failure.
func ValidateJSONSchema(schemaBytes []byte) error {
	if len(schemaBytes) == 0 {
		return nil
	}

	// Basic JSON validation
	var schema any
	if err := json.Unmarshal(schemaBytes, &schema); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}

	return nil
}
