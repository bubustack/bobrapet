---
name: crd-review
description: Review CRD type changes after editing *_types.go files. Checks markers, generation, backwards compatibility, JSON tags, webhook coverage. Invoke after any API type change.
---

<command-name>crd-review</command-name>

# CRD Review

Post-edit review after touching `*_types.go` files. Validates that type changes are complete, correct, and backwards-compatible.

## Trigger

Invoke after any change to:
- `api/**/*_types.go`
- Any file defining CRD structs with kubebuilder markers

## Review Checklist

### 1. Kubebuilder Markers
- All validation markers present (`+kubebuilder:validation:Required`, `+kubebuilder:validation:Enum`, `+kubebuilder:validation:Minimum`, etc.)?
- `+kubebuilder:object:root=true` on root types?
- `+kubebuilder:subresource:status` on types with status?
- `+kubebuilder:printcolumn` annotations up to date for new fields?
- `+kubebuilder:resource` scope correct (cluster vs namespaced)?

### 2. Generation Check
- Run `make manifests generate`
- Check for generation errors
- Verify no unexpected diff in `config/crd/bases/*.yaml`
- Verify `zz_generated.deepcopy.go` regenerated cleanly

### 3. Backwards Compatibility
- New fields have `+optional` marker or sensible defaults?
- No fields removed (only deprecated)?
- No type changes on existing fields (string → int, etc.)?
- Enum values only added, never removed?
- If breaking change is intentional: is there a migration path documented?

### 4. JSON Tags
- Every field has a `json:"fieldName"` tag?
- `omitempty` or `omitzero` used correctly?
  - `omitzero` for embedded structs (ObjectMeta, Status) — Go 1.25+ only
  - `omitempty` for optional scalar fields
- Field names are camelCase in JSON tags?
- No `json:"-"` on fields that should be persisted?

### 5. Webhook Coverage
- New required fields validated in admission webhooks?
- New immutable fields have immutability checks in `ValidateUpdate`?
- New enum fields validated against allowed values?
- Default values set in mutating webhooks (if applicable)?

### 6. Status Subresource
- Status fields only in the Status struct (not Spec)?
- Status conditions follow the metav1.Condition pattern?
- No spec fields leaked into status or vice versa?

### 7. Documentation
- Field comments (godoc) present on all public fields?
- Complex fields have usage examples in comments?

## Output Format

```markdown
# CRD Review: [type name] — [date]

## Files Reviewed
- `api/v1alpha1/xxx_types.go`

## Generation
- `make manifests generate`: [PASS/FAIL]
- CRD diff: [clean / N files changed]

## Results

| Check | Status | Detail |
|-------|--------|--------|
| [check] | PASS/WARN/FAIL | [detail with file:line] |

## Summary
- PASS: N, WARN: N, FAIL: N

## Required Actions
[List of FAIL items that must be fixed]
```

## Rules

- **Always run `make manifests generate`** as part of the review — do not skip.
- **Every WARN and FAIL must cite exact file and line.**
- **FAIL on any backwards-incompatible change** unless the user explicitly confirms it's intentional.
- **Do not modify source code** — report findings, let the user decide.
- **Check the diff**, not just the final state — understand what changed.
