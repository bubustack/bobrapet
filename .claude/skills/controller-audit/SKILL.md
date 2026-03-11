---
name: controller-audit
description: Audit controller reconciliation loops and admission webhooks against kubebuilder best practices. Report only — never modifies source code. Invoke after controller or webhook changes, or when reviewing code quality.
---

<command-name>controller-audit</command-name>

# Controller & Webhook Audit

Audit reconciliation loops and admission webhooks for correctness, idempotency, and kubebuilder best practices. **Report only — never modifies source code.**

## Trigger

Invoke after any change to:
- `internal/controller/*.go`
- `internal/webhook/*.go`
- Any reconciler or webhook handler

## Scope

Audit only the changed files plus their direct dependencies. If no specific files are mentioned, audit all controllers and webhooks.

## Audit Categories

### 1. Reconciliation Idempotency
- Does the reconciler converge on repeated runs with the same input?
- Are creates guarded by get-or-create (not blind create)?
- Are updates guarded by comparison (not blind update)?
- Is the reconciler safe to call at any time (no ordering assumptions)?

### 2. Status Conditions
- Are status conditions set before returning (not after)?
- Are terminal states handled (no infinite requeue on terminal resources)?
- Is `DeletionTimestamp` checked early and short-circuited?
- Are status updates using the status subresource (not spec updates)?

### 3. Error Handling & Requeue
- Are transient errors requeued with backoff (`ctrl.Result{RequeueAfter: ...}`)?
- Are permanent errors logged and NOT requeued?
- Is `apierrors.IsNotFound` handled (return nil, not error)?
- Are wrapped errors providing context (`fmt.Errorf("...: %w", err)`)?

### 4. Finalizers
- Added during reconciliation (not just on create)?
- Cleaned up before allowing deletion?
- Pre-delete logic runs before removing the finalizer?
- Finalizer string is a constant, not a magic string?

### 5. RBAC Markers
- Do `// +kubebuilder:rbac` markers match actual API calls in the reconciler?
- Are all accessed resources covered (including status subresources)?
- Are permissions minimal (no wildcard verbs unless justified)?

### 6. Webhook Patterns
- Validators are separate types from setup types?
- `ValidateCreate`, `ValidateUpdate`, `ValidateDelete` all implemented?
- Field paths in errors are anchored correctly (e.g. `spec.driver`, not just `driver`)?
- Immutability checks use normalized comparison (`strings.TrimSpace`)?
- Transient API errors: field errors returned first if available (user gets actionable feedback)?

### 7. Resource Ownership
- OwnerReferences set on created child resources?
- Controller reference set (not just owner)?
- Cross-namespace ownership avoided (K8s does not support it)?

### 8. Observability
- Key reconciliation decisions logged at appropriate levels?
- Error paths include enough context for debugging?
- Logging fires after DeletionTimestamp short-circuit (not before)?

## Output Format

```markdown
# Controller Audit: [date]

## Files Audited
- `path/to/file.go`

## Results

### [Category Name]
| Check | Status | File:Line | Notes |
|-------|--------|-----------|-------|
| [check] | PASS/WARN/FAIL | `file.go:123` | [detail] |

## Summary
- PASS: N
- WARN: N
- FAIL: N

## Recommendations
[Prioritized list of fixes, FAIL items first]
```

## Rules

- **Never modify source code.** This is a report-only skill.
- **Every WARN and FAIL must cite exact file and line/function.**
- **FAIL blocks task completion** — surface explicitly before finishing.
- **Do not audit generated files** (`zz_generated.*.go`, `config/crd/bases/`).
- **Do not audit test files** — focus on production code only.
