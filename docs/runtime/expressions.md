<!--
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
-->

# Template Expressions

This document defines where template expressions are evaluated, which context
variables are allowed, and how determinism is enforced across phases.

## Who this is for

- Workflow authors writing `{{ ... }}` expressions.
- Component authors consuming evaluated inputs.

## What you'll get

- The supported expression syntax and contexts.
- Determinism rules for batch vs streaming.
- Common patterns and pitfalls.

## Quick start

- Always wrap expressions in `{{ ... }}`. Plain strings are treated as literals.
- Use `inputs.*`, `steps.*`, and `packet.*` (streaming only) as your root contexts.
- For step names with dashes, use `steps['my-step']` or `index steps "my-step"`.

## Expression Forms

There are two supported forms:

1. Template strings (Go templates + Sprig):

```json
{
  "path": "{{ inputs.filePath }}",
  "flag": "{{ eq steps.validate.output.ok true }}"
}
```

2. Raw template expressions (for explicit execution by an Engram / SDK):

```json
{
  "$bubuTemplate": "steps.fetch.output.status | int | gt 200",
  "$bubuTemplateVars": {
    "threshold": 200
  }
}
```

Notes:
- Pipes are supported (`|`) via Go templates + Sprig.
- Both `{{ inputs.foo }}` and `{{ .inputs.foo }}` are accepted.
- `$bubuTemplateVars` (optional) merges into the evaluation context for that one expression.

## Template Engine

All templating uses Go `text/template` with the Sprig function library. This means:

- Pipes work as in Sprig/Go templates.
- Missing keys are errors (`missingkey=error`).
- Root names are normalized, so `inputs.foo` and `.inputs.foo` are accepted.
- Plain strings are not evaluated; use `{{ ... }}` to execute expressions.
- For step names with dashes, use `steps['name']` in simple path templates or
  `index steps "name"` in complex expressions.

Sprig functions are available with a few restrictions:

- Disabled for safety: `env`, `expandenv`, `getHostByName`.
- Random helpers and `now` are allowed only in runtime evaluation, not in
  deterministic inputs-only evaluation (see Determinism).

## Custom Helper Functions

Custom helpers are always available in addition to Sprig:

- `len` → Length of strings, arrays, slices, or maps. If the value is an offloaded storage ref,
  evaluation is blocked for materialization.
- `hash_of` → SHA256 of a string or byte array. Offloaded refs are blocked for materialization.
- `type_of` → Returns `string|number|bool|object|array|null`, or `offloaded(<ref>)` for storage refs.
- `exists` → `true` if a value is non-nil.
- `sample` → Returns the value (useful for probing); offloaded refs are blocked for materialization.

## Context Variables

- `inputs.*`: Story input payload (from StoryRun spec.inputs).
- `steps.*`: Step outputs (batch) or upstream packet outputs (streaming hub). In
  streaming, the hub flattens outputs so `steps.my-step` is the output object
  (no `.outputs` wrapper).
- `packet.*`: Per-packet metadata (streaming only).
- `now`: Current timestamp (runtime only; disallowed in deterministic inputs-only evaluation).
- `rand*`, `uuidv4`: Random helpers (runtime only; disallowed in deterministic inputs-only evaluation).

Unsupported aliases (rejected by validation):
- `trigger.*` -> use `inputs.*`
- `upstream.*` -> use `steps.*`

## Evaluation Matrix

### Batch stories (`spec.pattern: batch`)

| Field | When evaluated | Allowed contexts |
|---|---|---|
| `steps[*].if` | runtime (DAG controller) | `inputs`, `steps`, `now`, random helpers |
| `steps[*].with` | runtime (StepRun creation) | `inputs`, `steps`, `now`, random helpers |
| `spec.output` | runtime (StoryRun finalize) | `inputs`, `steps`, `now`, random helpers |
| `steps[*].runtime` | not supported | - |

### Streaming stories (`spec.pattern: streaming`)

| Field | When evaluated | Allowed contexts |
|---|---|---|
| `steps[*].with` | runtime (hub per-packet; deterministic inputs only) | `inputs` |
| `steps[*].runtime` | runtime (hub per-packet) | `inputs`, `steps`, `packet`, `now`, random helpers |
| `steps[*].if` | runtime (hub per-packet) | `inputs`, `steps`, `packet`, `now`, random helpers |
| `spec.output` | runtime (StoryRun finalize, if applicable) | `inputs`, `steps`, `now`, random helpers |

## Common patterns

### Step `if`

```yaml
if: "{{ gt (len steps['fetch'].output.items) 0 }}"
```

### Wait `until`

```yaml
with:
  until: "{{ inputs.ready }}"
```

### Streaming runtime config

```yaml
runtime:
  threshold: "{{ steps.transcribe.confidence }}"
```

## Determinism Rules

Deterministic evaluation forbids `now` and random helpers to keep results replayable.
This applies to inputs-only contexts (for example streaming `steps[*].with`). Runtime
evaluation can use `now`/random helpers as needed.

Template values are resolved at evaluation time and stored in the resulting
StepRun/packet; they remain stable across reconciliation once computed.

## Offloaded data and materialization

Templates may reference step outputs that are offloaded to storage (objects that contain
`$bubuStorageRef`). Direct evaluation of those values is blocked by default.

Behavior is controlled by `templating.offloaded-data-policy`:

| Policy | Behavior | Requires materialize engram |
|--------|----------|-----------------------------|
| `error` (default) | Reject evaluation with a clear error. | No |
| `inject` | Create a "materialize" StepRun pod to hydrate data and re‑evaluate the template. Only expressions with pipe functions trigger materialization; simple pass-throughs are left for the SDK. | Yes (`templating.materialize-engram`) |
| `controller` | Hydrate offloaded data from S3 in-process and evaluate templates directly in the reconciliation loop. No extra pod is spawned. All offloaded refs are resolved. | No |

When `inject` is enabled, the operator uses `templating.materialize-engram` (commonly set to
`materialize`) to resolve the template in the same StoryRun namespace.

### Per-Story override

The annotation `bubustack.io/controller-resolve: "true"` on a Story resource enables
controller-mode in-process resolution for that Story, regardless of the operator-level policy.

## Templating Configuration

Operator-level limits and behavior are controlled by:

- `templating.evaluation-timeout` (example: `30s`)
- `templating.max-output-bytes` (example: `65536`)
- `templating.deterministic` (`true|false`)
- `templating.offloaded-data-policy` (`error|inject|controller`)
- `templating.materialize-engram` (engram name, required only for `inject` policy)

## Validation

The Story webhook enforces the matrix above at admission time. Invalid templates are rejected with
clear field-level errors.
