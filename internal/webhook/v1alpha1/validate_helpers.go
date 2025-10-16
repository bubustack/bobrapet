package v1alpha1

import (
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
	schemaLoader := gojsonschema.NewStringLoader(string(schema))
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
