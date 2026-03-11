# Inputs, Schemas, and Defaults

This document covers StoryRun inputs: schema validation, defaults, and precedence.

## Who this is for

- Workflow authors defining inputs and schemas.
- Operators setting size limits and defaults.

## What you'll get

- How inputs are validated and defaulted.
- The precedence rules for input evaluation.
- Size limits and when to use storage refs.

## Schema validation

Stories can declare `spec.inputsSchema` as a JSON Schema. The admission webhook:

- Validates that the schema itself is well-formed JSON Schema.
- Validates StoryRun `spec.inputs` against the schema on create/update.

The StoryRun controller revalidates resolved inputs at runtime as a safety net
(for cases where webhooks are bypassed).

## StepRun inputs

StepRun inputs are resolved from Story `steps[].with` templates and upstream
outputs at runtime by the StepRun controller. When the referenced
EngramTemplate defines `spec.inputSchema`, the controller:

- Applies JSON Schema defaults to the resolved inputs.
- Validates the resolved inputs after templating.

If validation fails, the StepRun is marked Failed with reason
`InputSchemaFailed`.

Schema refs are also propagated to status when available:

- `StoryRun.status.inputSchemaRef` points to the Story inputs schema.
- `StepRun.status.inputSchemaRef` points to the EngramTemplate input schema.

## Defaults

Defaults are applied from JSON Schema `default` fields:

- Defaults are applied only when a field is missing.
- Defaults never override explicitly provided inputs.
- Defaults are applied recursively to nested objects when nested defaults exist.
- No type coercion is performed (e.g., `"1"` does not become `1`).

Precedence:

1. StoryRun `spec.inputs`
2. JSON Schema defaults from `spec.inputsSchema`

## Size limits

StoryRun inputs are subject to the inline size limit:

- `storyrun.max-inline-inputs-size` (operator config)

For larger payloads, use storage references (see `/docs/runtime/payloads.md`). The
platform does not auto-offload oversized StoryRun inputs; clients/SDKs must
offload and pass storage refs.

## JSON Schema support

Schema validation uses the `gojsonschema` library. Refer to that project for the
exact draft support and constraints.
