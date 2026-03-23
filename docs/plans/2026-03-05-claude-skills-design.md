# Claude Skills Design for BubuStack Projects

**Date:** 2026-03-05
**Scope:** bobrapet, bobravoz-grpc, bubu-sdk-go

## Skills

### 1. `/deep-implement` — all three projects
Structured Go/kubebuilder implementation workflow (Research → Plan → Annotate → Todo → Tests → Implement → Feedback → Lessons). Phase-gated with hard stops. Uses envtest, table-driven tests, `make test`, `make lint-fix`. Reminds to run `make manifests generate` after types changes.

### 2. `/controller-audit` — bobrapet + bobravoz-grpc
Report-only audit of controller/webhook code. Checks: idempotency, status conditions, error handling, requeue logic, finalizers, RBAC markers, webhook patterns, resource ownership. Outputs structured PASS/WARN/FAIL report with file:line citations.

### 3. `/crd-review` — bobrapet + bobravoz-grpc
Post-edit review after `*_types.go` changes. Checks: kubebuilder markers, generation drift, backwards compatibility, JSON tags, webhook coverage, status subresource, DeepCopy.

### 4. `/cross-debug` — all three projects
Guided cross-project debugging workflow. Identify → Locate owner → Trace logs/resources → Diagnose via docs → Fix with impact analysis.

## Distribution

| Skill | bobrapet | bobravoz-grpc | bubu-sdk-go |
|-------|----------|---------------|-------------|
| `/deep-implement` | yes | yes | yes |
| `/controller-audit` | yes | yes | no |
| `/crd-review` | yes | yes | no |
| `/cross-debug` | yes | yes | yes |
