# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Commands

```bash
# Build
make build                        # Build manager binary (runs manifests -> generate -> fmt -> vet)

# Everyday local dev (most common)
make kind-load-controller         # docker-build -> kind load image -> rollout restart deployment
make deploy                       # Deploy controller to cluster (kustomize)
make install                      # Install CRDs into cluster

# Kind cluster management (hack/makefiles/kind.mk)
make kind-create                  # Create kind cluster (KIND_CLUSTER_NAME=bobrapet, hack/kind-config.yaml)
make kind-delete                  # Delete kind cluster
make kind-status                  # Check cluster status (nodes + pods)
make kind-get-kubeconfig          # Export kubeconfig to ~/.kube/config
make kind-load-image IMAGE=x:tag  # Load arbitrary image into kind cluster

# Testing
make test                         # Unit tests with envtest
make test-e2e                     # E2e tests with dedicated Kind cluster (bobrapet-test-e2e)
make setup-test-e2e               # Set up e2e Kind cluster
make cleanup-test-e2e             # Tear down e2e Kind cluster

# Code quality
make lint                         # Run golangci-lint (v2.9.0)
make lint-fix                     # Run golangci-lint with auto-fix
make fmt                          # go fmt ./...
make vet                          # go vet ./...

# Code generation
make manifests                    # Generate CRDs, WebhookConfigs, RBAC (controller-gen)
make generate                     # Generate DeepCopy methods (controller-gen)
make sync-templates               # Regenerate catalog template samples

# Docker
make docker-build                 # Build image (IMG=bobrapet:latest, uses Dockerfile.local)
make docker-push                  # Push image
make docker-buildx                # Multi-platform build

# Deployment
make install                      # Install CRDs into cluster
make uninstall                    # Remove CRDs from cluster
make deploy                       # Deploy controller to cluster
make undeploy                     # Remove controller from cluster

# Helm
make helm-chart                   # Generate Helm chart via helmify
```

**Fresh setup:** `make kind-create` -> `make install` -> `make deploy` -> `make kind-load-controller`
**Iteration:** edit code -> `make kind-load-controller` (rebuilds, loads, restarts)

## Architecture

**bobrapet** is the Kubernetes operator -- the spine of BubuStack. It defines CRDs, runs controllers, serves admission webhooks, manages storage, and orchestrates workflow execution.

### Project layout

```
api/              CRD type definitions (*_types.go) -- the API surface
cmd/              Entry point (main.go)
internal/
  controller/     Reconciliation loops for all resources
  webhook/        Admission webhooks (validation + mutation)
  storage/        Storage client for payload offloading
  config/         Operator runtime configuration
pkg/              Shared packages (enums, refs, helpers)
config/           Kustomize manifests: CRDs, RBAC, webhooks, manager
test/             E2e test suites
docs/             Comprehensive ecosystem documentation
hack/             Build and deployment scripts
```

### Core resources (CRDs)

- **Story** -- Declarative workflow definition (DAG of steps, policies, schemas)
- **StoryRun** -- Concrete execution of a Story with resolved inputs/outputs
- **StepRun** -- Execution record for a single step within a StoryRun
- **Engram / EngramTemplate** -- Executable component and its cluster-scoped definition
- **Impulse / ImpulseTemplate** -- Trigger that launches StoryRuns from external events
- **Transport / TransportBinding** -- Streaming transport configuration (Transport is cluster-scoped; TransportBinding is namespaced)

## BubuStack Ecosystem Context

BubuStack is a Kubernetes-native workflow orchestration platform. This workspace is part of a multi-module Go ecosystem at `/Users/kashotyan/personal/bubustack/`.

### Module map

| Module | Role |
|--------|------|
| `core` | Shared contracts, templating engine, transport runtime, identity helpers |
| `tractatus` | Protobuf service and message definitions for gRPC transport |
| **`bobrapet`** | **This project** -- Kubernetes operator: CRDs, controllers, webhooks |
| `bobravoz-grpc` | gRPC transport hub: streaming data plane, connector lifecycle |
| `bubu-sdk-go` | Go SDK for building Engrams and Impulses |
| `engrams/*` | Individual Engram/Impulse implementations |
| `bubuilder` | Web console and API server |
| `bubu-registry` | Git-backed component registry and CLI |

### Dependency graph (strict DAG -- no cycles)

```
tractatus (protobuf contracts)
    |
  core (contracts, templating, transport runtime)
    |
  bobrapet <- YOU ARE HERE (CRDs, controllers, webhooks, storage, enums)
   / \
  /   \
bubu-sdk-go    bobravoz-grpc
  |
engrams/*
```

Lower layers never import higher layers. Engrams depend on bubu-sdk-go, not on bobrapet internals.

### Documentation

Comprehensive docs live in `docs/` -- see `docs/README.md` for the full index.

Key docs for cross-project debugging:
- `docs/overview/architecture.md` -- Module map, runtime topology
- `docs/overview/core.md` -- Workflow model, execution flow
- `docs/runtime/lifecycle.md` -- StoryRun/StepRun phases and terminal semantics
- `docs/runtime/payloads.md` -- Inline vs storage refs, size limits
- `docs/runtime/expressions.md` -- Template syntax, contexts, determinism
- `docs/streaming/streaming-contract.md` -- Streaming message rules
- `docs/api/errors.md` -- Structured error contract
- `docs/api/crd-design.md` -- CRD resource model and relationships

## Key Conventions

- **`omitzero` JSON tag** -- Go 1.25 std lib supports it; used on embedded ObjectMeta/Status structs to correctly omit zero-value structs (unlike `omitempty`).
- **Webhooks use `GetAPIReader()`** (not cache) for Transport lookups -- intentional for freshness; each binding admission adds one live API round-trip.
- **Transport is cluster-scoped; TransportBinding is namespaced** -- lookup uses `types.NamespacedName{Name: transportName}` (no namespace), which is correct.
- **Webhook validators** are separate from setup types (`TransportValidator` / `TransportBindingValidator` vs `TransportWebhook` / `TransportBindingWebhook`).
- **Validation** uses `strings.TrimSpace` for both presence and immutability checks -- whitespace-normalised updates are not treated as changes.
- **Transient API errors**: if field errors exist before a lookup fails, field errors are returned (not the infra error) -- user gets actionable feedback first.
- **Offloaded data policy** (`templating.offloaded-data-policy`):
  - `error` (default) -- fail on offloaded data access.
  - `inject` -- pod-based materialization for pipe expressions; requires `templating.materialize-engram`.
  - `controller` -- in-process resolution: controller hydrates from S3 via `storage.SharedManager` and evaluates templates in the reconciliation loop. No extra pod, no materialize engram needed.
  - Per-Story override: annotation `bubustack.io/controller-resolve: "true"` enables controller-mode resolution regardless of operator policy.
- **In-process resolution files**: `resolve_inprocess.go` (`resolveOffloadedInProcess`, `resolveConditionInProcess`), `templating_policy.go` (policy helpers), `offloaded_refs.go` (detection).

### Test patterns

- `fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(...).Build()` for fake client
- `causeFields(err)` helper extracts field paths from `StatusError.Details.Causes`
- `containsField(fields, target)` for exact field-path matching
- `failingReader` -- always returns configured error (transient API failure sim)
- `slowReader` -- blocks until context done (timeout/cancellation sim)
- Table tests include `wantFields []string` for field-path anchoring assertions

## Debugging Across the Ecosystem

### Ownership boundaries

| Area | Owner |
|------|-------|
| CRDs, controllers, webhooks, reconciliation | **bobrapet** (this project) |
| Component runtime, storage ref resolution, status patching | bubu-sdk-go |
| Streaming data plane, transport bindings, connectors | bobravoz-grpc |
| Shared contracts, templating, transport runtime | core |
| Protobuf definitions | tractatus |

### Common cross-project debugging scenarios

- **StepRun stuck in Pending**: Check controller reconciliation (bobrapet) -> check if Engram image exists -> check SDK startup logs
- **Storage ref resolution failures**: Check bobrapet payloads.md for size limits -> check SDK config_hydration.go and storage.SharedManager
- **Streaming transport issues**: Check Transport/TransportBinding CRDs (bobrapet) -> check bobravoz-grpc hub/connector logs -> check SDK transport_connector.go
- **Template evaluation errors**: Check bobrapet expressions.md -> distinguish controller-evaluated (if conditions, step input) vs SDK-evaluated (with blocks)
- **Webhook rejections**: Check bobrapet internal/webhook/ validators -> check the specific validation rule and field path in error

### How to trace cross-boundary issues

1. Start with the resource status (kubectl get/describe) to identify which controller/component owns the failure
2. Check the owning project's logs for the specific error
3. If the error crosses a boundary (e.g. SDK reports storage error), trace into the other project's code
4. Use the docs (especially lifecycle.md, payloads.md, errors.md) to understand expected behavior

## Skills

Skills live in `.claude/skills/` and are invoked via `/skill-name` in Claude Code.

### `/deep-implement`
Structured implementation workflow (Research → Plan → Annotate → Todo → Tests → Implement) with phase gates. Adapted for Go/kubebuilder: envtest, table-driven tests, `make manifests generate` after types changes.

### `/controller-audit`
Report-only audit of controller reconciliation and webhook code. Checks idempotency, status conditions, error handling, requeue logic, finalizers, RBAC markers, webhook patterns. Outputs PASS/WARN/FAIL with file:line citations.

### `/crd-review`
Post-edit review after `*_types.go` changes. Runs `make manifests generate`, checks markers, backwards compatibility, JSON tags, webhook coverage, status subresource.

### `/cross-debug`
Guided debugging across bobrapet/bobravoz-grpc/bubu-sdk-go boundaries. Identifies ownership, traces logs/resources, consults docs, proposes fixes with cross-project impact analysis.

## Workflow Rules

- **Always use Makefile targets** for build, test, lint, docker, kind, and deployment operations. Never run raw `docker`, `kind`, `kubectl apply/patch/delete`, or other infrastructure commands directly — use the corresponding `make` target instead (e.g. `make docker-build`, `make test-e2e`, `make install`, `make deploy`).
- **Read-only kubectl is allowed** for debugging: `kubectl get`, `kubectl describe`, `kubectl logs`, `kubectl api-resources`, `kubectl config current-context`.
- **Ask before running** any infrastructure command that doesn't have a Makefile target (e.g. raw `docker`, `kind`, `kubectl apply`). The user will either point you to the right `make` target or approve the one-off command.

## Safety Rules

- **Never edit auto-generated files**: `config/crd/bases/*.yaml`, `config/rbac/role.yaml`, `**/zz_generated.*.go`
- **Never remove `// +kubebuilder:scaffold:*` markers**
- **Never bypass webhook size limits** -- large data flows through storage refs
- **After editing `*_types.go`**: run `make manifests generate` to regenerate CRDs and DeepCopy methods
- **After Go edits**: `make fmt`, then targeted `go test`
- **Prefer small, reviewable diffs**
