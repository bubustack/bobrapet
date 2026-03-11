# BubuStack Documentation

Welcome to the BubuStack docs. This index gives you a fast path into the core
workflow model, component ecosystem, and reliability semantics, with deep dives
by topic. The docs are grouped by themes to keep navigation simple.

---

## Design philosophy

BubuStack applies the Unix philosophy to workflow orchestration on Kubernetes:

1. **Do one thing and do it well.** Each Engram is a single-purpose component --
   transcribe audio, call an LLM, send a notification. It does its job and
   produces output. It knows nothing about the pipeline it runs in.

2. **Write programs to work together.** A Story composes Engrams into a DAG,
   wiring step outputs to step inputs. Swap one Engram for another and the
   pipeline keeps working -- just as replacing `jq` with `yq` in a shell
   pipeline requires no changes to the surrounding commands.

3. **Text is the universal interface.** Step outputs are JSON. Templates
   evaluate against a shared context of `inputs` and `steps`. There is no
   proprietary IPC, no binary coupling -- just structured data flowing between
   independent components.

The operator (`bobrapet`) is the kernel: it schedules, reconciles, and enforces
policy. It never owns business logic. The components (`engrams/*`) are
userspace: they own side effects, external calls, and data transformation.
The SDK (`bubu-sdk-go`) is libc: it provides the contract between the two.

---

## Quick routes

- **Getting started**: `/docs/getting-started/prerequisites.md`, `/docs/getting-started/quickstart.md`.
- **Building components**: `/docs/sdk/building-engrams.md`, `/docs/overview/component-ecosystem.md`, `/docs/runtime/inputs.md`, `/docs/api/errors.md`.
- **Operating the platform**: `/docs/operator/configuration.md`, `/docs/runtime/lifecycle.md`, `/docs/observability/overview.md`.
- **Streaming-first systems**: `/docs/streaming/streaming-contract.md`, `/docs/streaming/transport-settings.md`, `/docs/streaming/lifecycle-hooks.md`.

## System pillars

These pillars describe the full BubuStack platform. Each entry links to the best
starting docs for that area.

- **System Architecture** (Module structure, dependencies, runtime topology): `/docs/overview/architecture.md`
- **Workflow Foundations** (Core workflow model): `/docs/overview/core.md`, `/docs/api/crd-design.md`
- **Execution Engine** (Runs, scheduling, primitives): `/docs/runtime/primitives.md`, `/docs/runtime/lifecycle.md`
- **Durable Semantics** (Delivery guarantees, idempotency): `/docs/overview/durable-semantics.md`
- **Eventing and Triggers** (Impulses, triggers, dispatch): `/docs/overview/component-ecosystem.md`, `/docs/api/errors.md`
- **Engram Ecosystem** (SDKs, contracts, registry): `/docs/sdk/building-engrams.md`, `/docs/overview/component-ecosystem.md`, `/docs/runtime/inputs.md`
- **Streaming and Real-Time Workflows** (Transport contract + settings): `/docs/streaming/streaming-contract.md`, `/docs/streaming/transport-settings.md`
- **Observability and Debugging** (Tracing, metrics, logs): `/docs/observability/overview.md`
- **Security, Governance, Multi-Tenancy** (Scoping, versioning, policies): `/docs/api/scoping.md`, `/docs/api/versioning.md`
- **Operations and Lifecycle** (Operator config, upgrades, run lifecycle): `/docs/operator/configuration.md`, `/docs/runtime/lifecycle.md`, `/docs/api/migration.md`

## Recommended path (start to finish)

0. `/docs/getting-started/prerequisites.md` - System dependencies (cert-manager, S3 storage).
0. `/docs/getting-started/quickstart.md` - Install BubuStack and run your first example.
1. `/docs/overview/architecture.md` - System architecture, module map, and dependency rules.
2. `/docs/overview/core.md` - The workflow model, key resources, and execution flow.
3. `/docs/overview/component-ecosystem.md` - SDKs, contracts, registry patterns, and reliability semantics.
3a. `/docs/sdk/building-engrams.md` - Go SDK guide: interfaces, testing, and template definitions.
4. `/docs/overview/durable-semantics.md` - Delivery guarantees, recovery rules, and idempotency expectations.
5. `/docs/operator/configuration.md` - Operator configuration keys and defaults.
6. `/docs/api/crd-design.md` - Resource model, scopes, and relationships.
7. `/docs/runtime/primitives.md` - Step types, cleanup blocks, and batch-only primitives.
8. `/docs/runtime/expressions.md` - Templates, contexts, determinism, and materialization.
9. `/docs/runtime/inputs.md` - Schema defaults and validation rules.
10. `/docs/runtime/payloads.md` - Payload storage, size limits, and storage references.
11. `/docs/runtime/caching.md` - Output caching keys, modes, and TTLs.
12. `/docs/runtime/lifecycle.md` - StoryRun/StepRun lifecycle, phases, and reason codes.
13. `/docs/observability/overview.md` - Tracing, metrics, logs, and debugging flows.
14. `/docs/api/scoping.md` - Namespace boundaries and reference rules.
15. `/docs/api/versioning.md` - Version pinning behavior and compatibility.
16. `/docs/api/migration.md` - CRD version lifecycle, upgrade procedures, and conversion webhook plan.
17. `/docs/api/errors.md` - Structured error contract for StepRuns.
18. `/docs/streaming/streaming-contract.md` - Streaming data contract and message rules.
19. `/docs/streaming/transport-settings.md` - Streaming backpressure, flow control, and delivery settings.
20. `/docs/streaming/lifecycle-hooks.md` - Streaming lifecycle hooks (storyrun.ready, steprun.ready).

---

## Streaming-focused path

If you're building real-time pipelines, use this shorter path:

1. `/docs/streaming/streaming-contract.md` - Message contract, ordering, and control semantics.
2. `/docs/streaming/lifecycle-hooks.md` - Lifecycle hooks for streaming startup coordination.
3. `/docs/streaming/transport-settings.md` - Backpressure, routing, replay, partitioning, and recording.
4. `/docs/observability/overview.md` - Streaming metrics, traces, and debugging tips.
5. `/docs/overview/durable-semantics.md` - Delivery guarantees and replay expectations.

---

## Docs layout (by folder)

- `/docs/getting-started/` - Prerequisites, installation, and quickstart.
- `/docs/overview/` - The conceptual model and durable semantics.
- `/docs/operator/` - Operator configuration and runtime knobs.
- `/docs/api/` - CRD design, scoping, versioning, and error contracts.
- `/docs/runtime/` - Step semantics, expressions, inputs, payloads, caching, and lifecycle.
- `/docs/streaming/` - Streaming contract, transport configuration, and lifecycle hooks.
- `/docs/sdk/` - Go SDK guide for building Engrams and Impulses.
- `/docs/observability/` - Tracing, metrics, logs, and debugging workflows.

---

## By topic

| Topic | Start here | What you'll learn |
| --- | --- | --- |
| Getting started | `/docs/getting-started/quickstart.md` | Install BubuStack and run your first workflow. |
| Prerequisites | `/docs/getting-started/prerequisites.md` | System dependencies, cert-manager, S3 storage. |
| System architecture | `/docs/overview/architecture.md` | Module map, dependency graph, runtime topology, import rules. |
| Workflow model | `/docs/overview/core.md` | Core resources, DAG execution, batch vs streaming. |
| Component ecosystem | `/docs/overview/component-ecosystem.md` | SDKs, contracts, registry patterns, reliability semantics. |
| Durable semantics | `/docs/overview/durable-semantics.md` | Delivery guarantees, recovery rules, and idempotency expectations. |
| Operator configuration | `/docs/operator/configuration.md` | Config keys, defaults, and why they exist. |
| CRD design | `/docs/api/crd-design.md` | Resource scopes, relationships, and contracts. |
| Error contract | `/docs/api/errors.md` | Structured error payloads and retry classifications. |
| Step semantics | `/docs/runtime/primitives.md` | Built-in steps, cleanup flow, batch-only primitives. |
| Expressions | `/docs/runtime/expressions.md` | Template engine, allowed contexts, determinism rules. |
| Inputs & schemas | `/docs/runtime/inputs.md` | Defaults, validation, schema application. |
| Payloads | `/docs/runtime/payloads.md` | Inline vs storage refs, size limits, offload rules. |
| Caching | `/docs/runtime/caching.md` | Output cache keys, modes, and TTLs. |
| Lifecycle | `/docs/runtime/lifecycle.md` | Phases, reasons, and terminal semantics. |
| Observability | `/docs/observability/overview.md` | Tracing, metrics, logs, and debugging workflows. |
| Scoping | `/docs/api/scoping.md` | Namespace rules and cross-namespace policy. |
| Versioning | `/docs/api/versioning.md` | Version pinning and migration strategy. |
| CRD migration | `/docs/api/migration.md` | API version lifecycle, upgrade procedures, conversion webhooks. |
| Streaming contract | `/docs/streaming/streaming-contract.md` | Streaming message contract and termination rules. |
| Lifecycle hooks | `/docs/streaming/lifecycle-hooks.md` | Streaming startup hooks (storyrun.ready, steprun.ready). |
| Transport settings | `/docs/streaming/transport-settings.md` | Streaming backpressure, flow control, and delivery settings. |
| SDK guide | `/docs/sdk/building-engrams.md` | Building Engrams and Impulses with the Go SDK. |

---

## Docs style guide

- `/docs/style-guide.md` - Writing conventions for BubuStack docs.

---

If you're new, start with `/docs/overview/core.md` and move down the recommended path.
