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

package runs

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"

	webhookshared "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
)

func validateJSONOutputBytes(output []byte, schema *runtime.RawExtension, schemaName string) error {
	if schema == nil || len(schema.Raw) == 0 {
		return nil
	}
	trimmed := webhookshared.TrimLeadingSpace(output)
	if len(trimmed) == 0 {
		return fmt.Errorf("%s output is empty but a schema is defined", schemaName)
	}
	return webhookshared.ValidateJSONAgainstSchema(trimmed, schema.Raw, schemaName)
}

func validateJSONOutputRaw(output *runtime.RawExtension, schema *runtime.RawExtension, schemaName string) error {
	if output == nil {
		return fmt.Errorf("%s output is empty but a schema is defined", schemaName)
	}
	return validateJSONOutputBytes(output.Raw, schema, schemaName)
}

func validateJSONInputsBytes(input []byte, schema *runtime.RawExtension, schemaName string) error {
	if schema == nil || len(schema.Raw) == 0 {
		return nil
	}
	trimmed := webhookshared.TrimLeadingSpace(input)
	if len(trimmed) == 0 {
		trimmed = []byte("{}")
	}
	if webhookshared.ContainsStorageRef(trimmed) {
		scrubbed, err := webhookshared.ScrubStorageRefMetadata(trimmed)
		if err != nil {
			return fmt.Errorf("%s inputs failed to sanitize storage ref metadata: %w", schemaName, err)
		}
		trimmed = scrubbed
	}
	return webhookshared.ValidateJSONAgainstSchema(trimmed, schema.Raw, schemaName)
}
