# Core System Overview

This document explains the core workflow model and execution flow in BubuStack,
and links to the detailed reference docs that define the contract.

## Who this is for

- Workflow authors designing Stories and Steps.
- Component authors building Engrams or Impulses.
- Operators who need the resource map and execution flow.

## What you'll get

- A mental model of the core resources and how they relate.
- The lifecycle of a StoryRun from trigger to completion.
- Pointers to the deeper contract docs.

---

## Unix philosophy

BubuStack's resource model follows the Unix principle: **do one thing and do it
well**, and **write programs to work together**.

- An **Engram** is a program. It does one job -- fetch HTTP, summarize text,
  send a notification -- and produces structured output. It has no knowledge of
  the pipeline it participates in.
- A **Story** is a pipeline. It composes Engrams into a directed acyclic graph,
  wiring step outputs to step inputs through templates. Replace one Engram and
  the rest of the pipeline is unaffected.
- **Step outputs** are the universal interface. JSON flows between steps. There
  is no proprietary IPC -- just structured data evaluated through the template
  engine (`{{ steps["fetch"].output.body }}`).
- The **operator** is the kernel. It schedules, reconciles, and enforces policy.
  It never owns business logic or external side effects.
- **Engrams** are userspace. They own API calls, storage writes, and data
  transformation. The SDK provides the contract between operator and component.

This separation means every component is independently testable, replaceable,
and reusable. Want Slack instead of Discord? Change the Engram ref, not the
Story. Want Anthropic instead of OpenAI? Point the step at a different Engram.
The pipeline doesn't care.

---

## At a glance

- Story defines the workflow spec: DAG, policies, schemas, and templates.
- StoryRun is a concrete execution of a Story with resolved inputs and outputs.
- StepRun is the execution record for a single step within a StoryRun.
- Engram and EngramTemplate define executable components and their schemas.
- Impulse and ImpulseTemplate define triggers that launch StoryRuns.

---

## Core resources and relationships

| Resource | Purpose | Key fields |
| --- | --- | --- |
| Story | Declarative workflow definition. | `spec.pattern`, `spec.steps`, `spec.inputsSchema`, `spec.outputsSchema`, `spec.output`, `spec.compensations`, `spec.finally`, `spec.policy` |
| StoryRun | An execution of a Story with concrete inputs and outputs. | `spec.storyRef`, `spec.inputs`, `status.phase`, `status.stepStates`, `status.output` |
| StepRun | An execution of a single step within a StoryRun. | `spec.stepId`, `spec.input`, `spec.timeout`, `spec.retry`, `status.phase`, `status.output` |
| Engram | A configured component instance used by Story steps. | `spec.templateRef`, `spec.mode`, `spec.with`, `spec.executionPolicy` |
| EngramTemplate | Cluster-scoped component definition with schema and defaults. | `spec.inputSchema`, `spec.outputSchema`, `spec.execution` |
| Impulse | A trigger that launches StoryRuns from external events. | `spec.templateRef`, `spec.storyRef`, `spec.mapping`, `spec.deliveryPolicy`, `spec.throttle` |
| ImpulseTemplate | Cluster-scoped trigger definition. | `spec.contextSchema`, `spec.deliveryPolicy` |

---

## Execution flow (simplified)

1. An Impulse receives an event and maps it to StoryRun inputs.
2. The StoryRun controller resolves the Story and builds the DAG.
3. StepRuns are created as steps become ready.
4. If a template references offloaded data, the controller resolves it based on policy: in-process hydration from S3 (`controller`), pod-based materialization (`inject`), or rejection (`error`).
5. StepRuns execute Engrams or primitives and publish outputs.
6. StoryRun aggregates step states and final output.

---

## Durable semantics

Delivery guarantees, recovery rules, idempotency expectations, and signal semantics
are defined in `/docs/overview/durable-semantics.md`.

---

## Component ecosystem

SDK usage, contracts, registry patterns, and reliability semantics are summarized
in `/docs/overview/component-ecosystem.md`.

---

## Workflow graph and step model

The Story spec defines a DAG. Step order in YAML does not imply execution order.
Dependencies are expressed with `steps[].needs`. Independent steps run in parallel.

Supported step types:
- Engram steps (type omitted, `ref` set)
- `condition`, `parallel`, `sleep`, `stop`, `executeStory`
- `wait` and `gate` (batch only)

Failure handling and cleanup:
- `steps[].allowFailure` marks a step as non-fatal to the StoryRun.
- `spec.compensations` runs after failure, before `spec.finally`.
- `spec.finally` runs after completion regardless of outcome.
- Cleanup blocks work for both batch and streaming stories. In streaming mode,
  cleanup steps execute as batch jobs after the streaming topology terminates.

Details of primitive behavior are in `/docs/runtime/primitives.md`.

---

## Execution patterns: batch vs streaming

Stories declare `spec.pattern`:
- `batch`: short-lived StoryRuns, standard DAG scheduling.
- `streaming`: long-lived topologies where steps process packets.

Evaluation behavior differs by field:

| Field | Batch evaluation | Streaming evaluation |
| --- | --- | --- |
| `steps[].if` | Runtime (DAG controller). | Runtime per packet (hub). |
| `steps[].with` | Runtime at StepRun creation. | Runtime per packet, `inputs` only (deterministic). |
| `steps[].runtime` | Not used. | Runtime per packet (hub). |
| `spec.output` | Runtime at StoryRun finalize. | Runtime at StoryRun finalize (if applicable). |

Streaming steps can select a transport declared in `spec.transports`.
Expression rules and determinism are defined in `/docs/runtime/expressions.md`.

Streaming notes:
- The hub evaluates `if` per packet and routes to downstream Engram steps that depend on the upstream step.
- If a runtime `if` evaluates to false, that branch is skipped while other branches continue.
- `executeStory` honors `waitForCompletion`; when true the hub waits for the child StoryRun to finish.
- For steps with multiple `needs`, the hub performs a lightweight join when a correlation key is available (`bubu.join.key` or `message_id`).

---

## Inputs, outputs, and schemas

Stories define contracts:
- `spec.inputsSchema` validates StoryRun inputs.
- `spec.outputsSchema` validates StoryRun output.
- `spec.output` templates the final output from `inputs` and `steps`.

StoryRuns provide concrete inputs in `spec.inputs` and store final output in `status.output`.
Defaults and validation behavior are specified in `/docs/runtime/inputs.md`.

Final output is capped (1 MiB). When exceeded, the StoryRun succeeds but the
output is not stored in status (no automatic storage ref is written). See
`/docs/runtime/payloads.md`.

---

## Templating and expressions

Template strings like `{{ inputs.foo }}` and raw template forms (`$bubuTemplate`) are
used throughout Story fields. Use `{{ ... }}` for evaluation; plain strings are
treated as literals. The engine is Go templates plus Sprig with custom helpers
(see `/docs/runtime/expressions.md`). Deterministic inputs-only evaluation forbids `now`
to preserve replayability.

When a template references offloaded payloads, the platform either rejects the
evaluation (default) or injects a materialize StepRun to hydrate and resolve the
template, depending on `templating.offloaded-data-policy`. The materialize engram is
selected via `templating.materialize-engram`.

---

## Policies and overrides

Policies are applied hierarchically. For step execution:
`StepRun > Story step execution > Engram execution > Template execution`.

Story-wide defaults live under `spec.policy`, including:
- `timeouts.story` and `timeouts.step`
- `retries.stepRetryPolicy` and `retries.continueOnStepFailure`
- `storage` and `execution` defaults
- `execution.placement` for pod scheduling (`nodeSelector`, `tolerations`, `affinity`)
- `execution.cache` for output caching (`key`, `mode`, `ttlSeconds`)
- `queue` and `priority` for scheduling order (strict priority ordering, no preemption)
- `storyrun.queue.<name>.priority-aging-seconds` can raise effective priority for queued runs

`retries.stepRetryPolicy` supports `maxRetries`, `delay`, `backoff`, `maxDelay`,
and `jitter` (0–100 percent jitter applied to the computed delay).

### Policy layers

BubuStack has three distinct retry/backoff layers. They operate independently
and at different scopes:

| Layer | What it retries | Who drives it | Configuration |
| --- | --- | --- | --- |
| **Job backoff** | Pod failures within a single Kubernetes Job. Kubelet restarts the pod container up to `backoffLimit` times. | Kubernetes Job controller | `JobPolicy.backoffLimit`, `JobWorkloadConfig.backoffLimit`, or operator default `job.backoff-limit`. `RestartPolicy` is set to `Never` (new pod per retry) or `OnFailure` (in-place restart). |
| **Step retry** | Entire step execution (creates a new Job/pod). Triggered when a StepRun finishes with a retryable exit class and retries remain. | StepRun controller (`steprun_controller.go`) | `RetryPolicy.maxRetries`, `delay`, `maxDelay`, `backoff`, `jitter`. Template defaults via `TemplateRetryPolicy.recommendedMaxRetries`, `recommendedBaseDelay`, `recommendedMaxDelay`, `recommendedBackoff`. Operator default `retry.max-retries`. |
| **Trigger delivery retry** | Submission of a trigger event to create a StoryRun. Retried by the SDK inside the Impulse workload. | SDK (runs inside Impulse pod) | `TriggerDeliveryPolicy.retry` (maxAttempts, baseDelay, maxDelay, backoff). Defaults from `ImpulseTemplate.spec.deliveryPolicy`, overridable on `Impulse.spec.deliveryPolicy`. |

**Key distinction:** Job backoff and step retry both affect the same step but at
different levels. A step with `backoffLimit: 3` and `maxRetries: 2` can produce
up to `(3+1) * (2+1) = 12` pod attempts in the worst case (3 pod restarts per
Job × 3 Jobs). Trigger delivery retry is entirely separate and concerns
StoryRun creation, not step execution.

Template-level fields (`TemplateJobPolicy`, `TemplateRetryPolicy`) provide
recommended defaults prefixed with `recommended*`. These are merged into the
runtime policy when the corresponding field is unset, not when it is explicitly
set to zero. See `/docs/api/crd-design.md` for the full policy resolution chain.

Impulse trigger delivery behavior is configured separately via
`ImpulseTemplate.spec.deliveryPolicy` and `Impulse.spec.deliveryPolicy`. This
policy controls trigger deduplication and delivery retries; it does not replace
StepRun retry policies. Per-trigger throttling is configured on
`Impulse.spec.throttle` and enforced by the SDK running inside the Impulse
workload (templates do not provide throttle defaults).

`Impulse.spec.throttle` fields:
- `ratePerSecond`: steady-state token bucket rate (0 disables rate limiting)
- `burst`: token bucket burst size (defaults to `ratePerSecond` when omitted)
- `maxInFlight`: cap on concurrent trigger submissions (0 disables)

When throttling delays a trigger, the SDK patches `Impulse.status.throttledTriggers`
and `Impulse.status.lastThrottled` so operators can observe backpressure.

## Impulse Throttling Example

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Impulse
metadata:
  name: github-webhook
spec:
  templateRef:
    name: github-webhook
  storyRef:
    name: pr-review
  throttle:
    ratePerSecond: 5
    burst: 10
    maxInFlight: 20
```

Operator-level defaults and limits are documented in `/docs/operator/configuration.md`.

---

## Runtime objects and lifecycle

StoryRun and StepRun phases, reasons, and terminal semantics are defined in
`/docs/runtime/lifecycle.md`. `status.conditions` is the canonical lifecycle source of
truth for both resources.

---

## Payloads and storage

Payloads can be stored inline or offloaded to storage. Storage references use
`$bubuStorageRef` and related metadata fields. Size limits and reference
behavior are defined in `/docs/runtime/payloads.md`. The controller does not auto-offload
oversized Step inputs or StoryRun outputs; workflows must use storage refs and
aggregation engrams when payloads grow.

Design principle (DOTADIW: Do One Thing And Do It Well):
The controller is responsible for workflow orchestration and reconciliation only.
Engrams and Impulses own external side effects, storage writes, and storage
lifecycle (retention/GC). The controller does not manage the lifecycle of
storage objects produced by workloads.

---

## Scoping and versioning

Namespace scoping rules are defined in `/docs/api/scoping.md`. By default,
namespaced references must be within the same namespace. The operator config
`references.cross-namespace-policy` can enable ReferenceGrant-based sharing or
allow all cross-namespace references. Templates are cluster-scoped.

Versioning and pinning are described in `/docs/api/versioning.md`. The CRD version
lifecycle (`v1alpha1` → `v1beta1` → `v1`), upgrade procedures, and conversion
webhook plan are in `/docs/api/migration.md`.

---

## Operator configuration

Controller configuration, scheduling controls, and defaults are documented in
`/docs/operator/configuration.md`. The sample ConfigMap lives at
`config/manager/operator-config.yaml`.

---

## Where to dive deeper

| Area | Document | Focus |
| --- | --- | --- |
| Configuration | `/docs/operator/configuration.md` | Operator defaults, scheduling keys, and knobs |
| Durable semantics | `/docs/overview/durable-semantics.md` | Delivery guarantees, recovery rules, idempotency expectations |
| Step semantics | `/docs/runtime/primitives.md` | Primitive behavior and cleanup blocks |
| Expressions | `/docs/runtime/expressions.md` | Contexts, determinism, and materialization |
| Schemas | `/docs/runtime/inputs.md` | Defaults and validation rules |
| Payloads | `/docs/runtime/payloads.md` | Inline vs storage refs and size limits |
| Caching | `/docs/runtime/caching.md` | Output cache keys, modes, and TTLs |
| Lifecycle | `/docs/runtime/lifecycle.md` | Phases, reasons, and terminal rules |
| Scoping | `/docs/api/scoping.md` | Namespace boundaries and reference policy |
| Versioning | `/docs/api/versioning.md` | Pinning behavior and compatibility |
| CRD migration | `/docs/api/migration.md` | API version lifecycle and upgrade procedures |
