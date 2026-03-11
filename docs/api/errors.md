# Structured Errors (StepRun.status.error)

StepRun failures emit a structured error payload in `StepRun.status.error`.
The payload is versioned and must conform to the JSON Schema below:

- `/docs/contracts/steprun-error.schema.json`

## Who this is for

- Component/SDK authors emitting structured errors.
- Workflow authors debugging StepRun failures.

## What you'll get

- The required and optional fields in the error contract.
- How retryable/exit classification is represented.
- A concrete example payload.

## At a glance

- Errors are stored in `StepRun.status.error` and must conform to the v1 schema.
- `type` is a required categorical string; use `details` for machine-readable data.
- Unknown fields are rejected (`additionalProperties: false`).

## Schema summary

Required fields:
- `version` (string, current: `v1`)
- `type` (string enum)
- `message` (string)

Optional fields:
- `retryable` (boolean)
- `exitCode` (integer)
- `exitClass` (string: `success|retry|terminal|rateLimited`)
- `code` (string)
- `details` (object)

Allowed `type` values:

- `timeout`
- `storage_error`
- `serialization_error`
- `validation_error`
- `initialization_error`
- `execution_error`
- `unknown`

## Example

```json
{
  "version": "v1",
  "type": "timeout",
  "message": "batch execution timed out after 30s",
  "retryable": true,
  "exitCode": 124,
  "exitClass": "retry"
}
```

## Example (StepRun status)

```yaml
status:
  error:
    version: v1
    type: validation_error
    message: "input schema failed"
    retryable: false
    exitClass: terminal
```
