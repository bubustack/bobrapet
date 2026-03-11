---
name: cross-debug
description: Guided debugging workflow for issues spanning multiple BubuStack ecosystem components (bobrapet, bobravoz-grpc, bubu-sdk-go, core). Invoke when a bug crosses project boundaries or when the root cause is unclear.
---

<command-name>cross-debug</command-name>

# Cross-Ecosystem Debug

Guided debugging workflow for issues that span multiple BubuStack components. Follows a structured trace from symptoms to root cause across project boundaries.

## When to use

- A bug manifests in one project but the root cause may be in another
- Resource status shows errors but the owning controller is unclear
- Streaming/transport issues that cross SDK ↔ bobravoz-grpc boundaries
- Storage ref resolution failures crossing operator ↔ SDK boundaries
- "It works in isolation but fails in the full stack" scenarios

## Ecosystem map

```
tractatus (protobuf contracts)
    ↓
  core (contracts, templating, transport runtime)
    ↓
  bobrapet (CRDs, controllers, webhooks, storage)
   / \
  /   \
bubu-sdk-go    bobravoz-grpc
  ↓
engrams/*
```

## Ownership boundaries

| Area | Owner | Key files |
|------|-------|-----------|
| CRDs, controllers, webhooks | bobrapet | `api/`, `internal/controller/`, `internal/webhook/` |
| Component runtime, storage ref resolution | bubu-sdk-go | `sdk.go`, `batch.go`, `stream.go`, `config_hydration.go`, `k8s/` |
| Streaming data plane, transport | bobravoz-grpc | `internal/hub/`, `internal/connector/`, `internal/controller/` |
| Contracts, templating | core | (shared module) |
| Protobuf definitions | tractatus | `.proto` files |

## Debug workflow

### Step 1: Identify the symptom
- What resource is affected? (StoryRun, StepRun, Transport, TransportBinding, etc.)
- What is the observed behavior vs expected?
- Get resource status: `kubectl describe <resource> -n <namespace>`

### Step 2: Locate the owner
Using the ownership table above, determine which project owns the failing component:
- StepRun stuck? → bobrapet controller first, then SDK
- Transport failing? → bobravoz-grpc controller, then check CRD in bobrapet
- Storage errors? → SDK first, then bobrapet storage client
- Template errors? → bobrapet expressions (controller-side) or SDK (with blocks)

### Step 3: Trace across boundaries
1. Read controller/component logs in the owning project
2. Check resource status conditions for clues
3. If the error crosses a boundary, identify the handoff point:
   - **Operator → SDK**: StepRun spec (inputs, config) → Pod env vars → SDK hydration
   - **Operator → bobravoz-grpc**: Transport/TransportBinding CRDs → controller reconciliation
   - **SDK → bobravoz-grpc**: `transport_connector.go` → gRPC hub/connector

### Step 4: Consult docs
Key documentation paths (all relative to `bobrapet/docs/`):
- `overview/architecture.md` — module map, runtime topology
- `overview/core.md` — workflow model, execution flow
- `runtime/lifecycle.md` — StoryRun/StepRun phases, terminal semantics
- `runtime/payloads.md` — inline vs storage refs, size limits
- `runtime/expressions.md` — template syntax, contexts, determinism
- `streaming/streaming-contract.md` — streaming message rules
- `api/errors.md` — structured error contract

### Step 5: Diagnose and propose fix
- Identify the root cause and which project needs the fix
- Assess cross-project impact: will the fix affect other modules?
- If the fix is in a different project, clearly state which project and files to modify

## Common scenarios

### StepRun stuck in Pending
1. `kubectl describe steprun <name>` — check conditions
2. Check bobrapet controller logs for reconciliation errors
3. Check if Engram image exists and is pullable
4. Check SDK startup in Engram pod logs

### Storage ref resolution failures
1. Check bobrapet `runtime/payloads.md` for size limits
2. Check SDK `config_hydration.go` and `storage.SharedManager`
3. Verify storage backend is accessible
4. Check if `BUBU_TEMPLATE_CONTEXT` env var is set correctly

### Transport/streaming issues
1. `kubectl get transports,transportbindings -A`
2. Check bobravoz-grpc controller logs
3. Check connector pod logs for data plane issues
4. Check SDK `transport_connector.go` for client-side errors

### Webhook rejections
1. Read the rejection message — it contains the field path
2. Find the validator in `internal/webhook/` matching the resource
3. Check the specific validation rule
4. Verify the input against the rule (TrimSpace normalization, immutability, etc.)

## Output format

```markdown
# Debug Trace: [issue summary]

## Symptom
## Resource Status
## Owner
## Trace
1. [Step] → [result]

## Root Cause
## Fix
**Project:** [which project]
**File:** [exact path]
**Change:** [description]

## Cross-Project Impact
```
