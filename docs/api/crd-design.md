# CRD Design: Bobrapet Resource Model

This document describes the CRD design for bobrapet, including resource
relationships, scope, and key behavioral features. It complements the
workflow model in `/docs/overview/core.md` and the lifecycle details in `/docs/runtime/lifecycle.md`.

## Who this is for

- Platform engineers and operators who need the CRD model.
- Workflow authors who want to understand what each resource represents.
- SDK/component authors integrating with BubuStack APIs.

## What you'll get

- Resource scopes, relationships, and core fields.
- Minimal schema snapshots and examples per CRD.
- How policy, validation, and status ownership work.

---

## Goals and principles

- **Separation of definition vs. execution**: templates and stories are declarative
  definitions, while runs are ephemeral execution records.
- **Clear scoping**: templates are cluster-scoped; instances and runs are namespaced.
- **Deterministic orchestration**: controllers resolve inputs, validate schemas,
  and derive execution policies in a stable, repeatable way.
- **Streaming support as a first-class mode**: batch and streaming share core types
  and diverge only where execution semantics require it.

---

## API groups and resources

| API group | Kind | Scope | Purpose |
| --- | --- | --- | --- |
| `bubustack.io` | `Story` | Namespaced | Declarative workflow graph and policy. |
| `bubustack.io` | `Engram` | Namespaced | Configured instance of an EngramTemplate. |
| `bubustack.io` | `Impulse` | Namespaced | Configured instance of an ImpulseTemplate. |
| `catalog.bubustack.io` | `EngramTemplate` | Cluster | Component definition + schemas + defaults. |
| `catalog.bubustack.io` | `ImpulseTemplate` | Cluster | Trigger definition + schemas + defaults. |
| `runs.bubustack.io` | `StoryRun` | Namespaced | Execution record of a Story. |
| `runs.bubustack.io` | `StepRun` | Namespaced | Execution record of a single step. |
| `transport.bubustack.io` | `Transport` | Cluster | Transport provider definition + settings. |
| `transport.bubustack.io` | `TransportBinding` | Namespaced | Runtime binding between engram and connector. |
| `policy.bubustack.io` | `ReferenceGrant` | Namespaced | Cross-namespace reference allowlist. |

---

## Core relationships

```
EngramTemplate (cluster) -> Engram (namespace)
ImpulseTemplate (cluster) -> Impulse (namespace)
Story (namespace) -> StoryRun (namespace) -> StepRun (namespace)
Story step -> Engram reference -> EngramTemplate
Story transports -> Transport -> TransportBinding
```

- **Templates** define contracts and defaults.
- **Instances** (Engram/Impulse) bind a template to real configuration and secrets.
- **Runs** capture execution outcomes, outputs, retries, and errors.

---

## Templates and instances

### EngramTemplate

Defines the **contract** for a component:

- `spec.inputSchema` and `spec.outputSchema` (JSON Schema)
- `spec.configSchema` and `spec.secretSchema`
- `spec.execution` defaults (resources, retry, timeouts, probes, RBAC)

### Engram

A namespaced instance that references a template and supplies:

- `spec.templateRef` (template identity)
- `spec.with` (configuration bound to `configSchema`)
- `spec.secrets` (bindings for `secretSchema`)
- `spec.executionPolicy` overrides

### ImpulseTemplate / Impulse

Parallel to Engrams, but oriented around **trigger delivery**:

- `spec.contextSchema` defines the trigger payload structure.
- `spec.deliveryPolicy` defines dedupe and retry defaults.

### Engram transport and TLS

Engrams can declare transport-level overrides via `spec.transport`:

- `spec.transport.tls.secretRef` references a Secret containing `tls.key`/`tls.crt`
  for hybrid transports that negotiate TLS.
- `spec.transport.tls.useDefaultTLS` requests the operator's default TLS
  secret when true (defaults to true when `secretRef` is unset).

---

## Workload modes

Every component-backed resource (Engram, Impulse) specifies a workload mode
that determines the Kubernetes primitive the controller creates.

| Mode | Kubernetes object | Use case |
| --- | --- | --- |
| `job` (default) | `batch/v1 Job` | Short-lived, run-to-completion step executions. |
| `deployment` | `apps/v1 Deployment` | Continuously running, stateless processes (Impulses, long-running Engrams). |
| `statefulset` | `apps/v1 StatefulSet` | Continuously running processes with sticky identity and stable network names. |

Modes are set on `WorkloadSpec.mode` (Impulse: `spec.workload.mode`, Engram:
inherited from template `supportedModes` and instance `spec.mode`).

### Job-specific configuration (`WorkloadSpec.job`)

| Field | Maps to | Purpose |
| --- | --- | --- |
| `parallelism` | `Job.spec.parallelism` | Number of pods running concurrently. |
| `completions` | `Job.spec.completions` | Total successful pod completions required. |
| `backoffLimit` | `Job.spec.backoffLimit` | Pod restart limit before Job fails. |
| `activeDeadlineSeconds` | `Job.spec.activeDeadlineSeconds` | Hard deadline for the entire Job. |
| `ttlSecondsAfterFinished` | `Job.spec.ttlSecondsAfterFinished` | Auto-cleanup delay after Job completes. |

### StatefulSet-specific configuration (`WorkloadSpec.statefulSet`)

| Field | Maps to | Purpose |
| --- | --- | --- |
| `serviceName` | `StatefulSet.spec.serviceName` | Headless Service for stable network identities. |
| `podManagementPolicy` | `StatefulSet.spec.podManagementPolicy` | `OrderedReady` (default) or `Parallel`. |

### Service exposure (`Impulse.spec.service`)

Impulses that expose network endpoints declare `spec.service`:

| Field | Purpose |
| --- | --- |
| `type` | Service type (`ClusterIP`, `NodePort`, `LoadBalancer`). |
| `labels` | Extra labels merged into the generated Service. |
| `annotations` | Extra annotations merged into the generated Service. |

Port definitions come from the ImpulseTemplate's `spec.execution.service.ports`.

### Update strategy (`WorkloadSpec.updateStrategy`)

Controls rollout behavior for Deployments and StatefulSets:

- `type`: `RollingUpdate` (default) or `Recreate`.
- `rollingUpdate.maxUnavailable` and `rollingUpdate.maxSurge` (Deployment only).

Replica counts are intentionally absent; autoscaling is delegated to HPA, KEDA,
or VPA.

---

## Story and Step definitions

### Story

`Story` defines the workflow graph, dependencies, and defaults:

- `spec.steps[]` define DAG nodes (Engram references or primitives).
- `spec.pattern` selects batch or streaming execution.
- `spec.inputsSchema` and `spec.outputsSchema` define runtime contracts.
- `spec.policy` defines defaults (timeouts, retry, storage, resources).

### Step

Each step can either:

- Reference an Engram (`ref`), or
- Use a primitive (`type`), such as `condition`, `parallel`, `sleep`, or `executeStory`.

For streaming steps:

- `step.with` is evaluated per packet by the hub using deterministic inputs only.
- `step.runtime` is evaluated per packet by the hub and passed to the engram.

See `/docs/runtime/expressions.md` for the evaluation matrix and allowed contexts.

---

## Execution overrides reference

`ExecutionOverrides` can be set on `Engram.spec.overrides` and `Step.execution`
to tune execution without changing the template. All fields are optional; unset
fields inherit from the template or operator default.

| Field | Type | Purpose |
| --- | --- | --- |
| `timeout` | `string` (Go duration) | Overrides the step execution timeout. |
| `retry` | `RetryPolicy` | Overrides retry behavior (maxRetries, delay, backoff, maxDelay, jitter). |
| `debug` | `bool` | Enables verbose, component-level logging. |
| `security` | `WorkloadSecurity` | Overrides pod security context (runAsNonRoot, readOnlyRootFilesystem, runAsUser, etc.). |
| `placement` | `PlacementPolicy` | Overrides node selectors, tolerations, and affinity. |
| `imagePullPolicy` | `string` | Overrides the container image pull policy. |
| `maxInlineSize` | `int` | Overrides the inline payload threshold in bytes. Set to 0 to always offload. |
| `serviceAccountName` | `string` | Overrides the ServiceAccount used by the pod. |
| `automountServiceAccountToken` | `bool` | Toggles `automountServiceAccountToken` on the pod spec. |
| `probes` | `ProbeOverrides` | Disables template-defined liveness/readiness/startup probes per-instance (`disableLiveness`, `disableReadiness`, `disableStartup`). |
| `storage` | `StoragePolicy` | Overrides the storage backend (S3 or file provider settings). |
| `cache` | `CachePolicy` | Overrides output caching (enabled, key, mode, ttlSeconds, salt). |
| `rbac` | `RBACRuleOverrides` | Adds extra Role rules to the generated ServiceAccount. |

`StepExecutionOverrides` (set on `StepRun.spec.executionOverrides`) adds three
additional fields that map directly to the Kubernetes Job spec:

| Field | Type | Purpose |
| --- | --- | --- |
| `backoffLimit` | `int32` | Overrides `Job.spec.backoffLimit`. |
| `ttlSecondsAfterFinished` | `int32` | Overrides `Job.spec.ttlSecondsAfterFinished`. |
| `restartPolicy` | `string` | Overrides pod restart policy (`Never` or `OnFailure`). |

---

## Template execution sub-features

`TemplateExecutionPolicy` (set on `EngramTemplate.spec.execution` or
`ImpulseTemplate.spec.execution`) provides the following defaults, all of which
can be overridden by Engram/Impulse instances or Story policy:

| Sub-policy | Key fields | Purpose |
| --- | --- | --- |
| `resources` (`TemplateResourcePolicy`) | `defaultCPURequest`, `defaultMemoryRequest`, `defaultCPULimit`, `defaultMemoryLimit` | Pod resource requests/limits baseline. |
| `security` (`TemplateSecurityPolicy`) | `runAsNonRoot`, `readOnlyRootFilesystem`, `runAsUser`, `dropCapabilities` | Pod security context defaults. |
| `job` (`TemplateJobPolicy`) | `recommendedBackoffLimit`, `recommendedTTLSecondsAfterFinished` | Kubernetes Job spec defaults (pod restart limit, cleanup TTL). Not step-retry; see policy layers. |
| `retry` (`TemplateRetryPolicy`) | `recommendedMaxRetries`, `recommendedBaseDelay`, `recommendedMaxDelay`, `recommendedBackoff` | Step-level retry recommendations merged into `RetryPolicy` when unset. |
| `service` (`TemplateServicePolicy`) | `ports[]` (name, port, targetPort, protocol) | Service port definitions for Deployment/StatefulSet-backed templates. |
| `probes` (`TemplateProbePolicy`) | `liveness`, `readiness`, `startup` | Container health check definitions. Instances can disable them via `ProbeOverrides`. |
| `storage` (`TemplateStoragePolicy`) | S3 (`bucket`, `region`, `endpoint`, `auth`) or file (`basePath`) | Storage backend defaults for output offloading. |
| `rbac` | `rules[]` | Extra RBAC PolicyRules for workloads that need API access. |

Template fields use the `recommended*` prefix to distinguish them from runtime
overrides. When a runtime field is unset, the controller merges the template
recommendation; explicitly set values (including zero) are never overwritten.

---

## Runs and status

### StoryRun

Represents a single execution of a Story. It captures:

- Resolved inputs and output
- Execution phase and conditions
- Attempts, timeouts, and final status

### StepRun

Represents a single step execution. It captures:

- Resolved inputs and outputs
- Exit code and exit class
- Structured error payload (see `/docs/api/errors.md`)
- Retry scheduling metadata

`status.conditions` is the canonical lifecycle signal. Phase fields are summaries.

#### StepRun advanced fields

| Field | Location | Purpose |
| --- | --- | --- |
| `spec.idempotencyKey` | Spec | Stable key used for exactly-once effect tracking. SDK checks this on retry to skip already-committed effects. |
| `spec.executionOverrides` | Spec | Per-StepRun resource and job overrides (CPU, memory, backoffLimit, TTL, restartPolicy, debug). |
| `spec.downstreamTargets` | Spec | gRPC or terminate targets for streaming handoff. Each entry carries an address, optional TLS config, and metadata. |
| `status.handoff` | Status | Tracks streaming handoff lifecycle (`phase`: none/requested/draining/cutover/ready, plus `connectorID`, `requestedAt`, `completedAt`). |
| `status.effects` | Status | Append-only ledger of side-effect records (name, key, committedAt, metadata). Used with `idempotencyKey` for exactly-once effect tracking. |
| `status.signals` | Status | Key-value map of the latest signal state for each named signal (used for inter-step coordination). |
| `status.signalEvents` | Status | Ordered log of signal events with monotonic `seq` numbers. The SDK replays these on reconnect to restore signal state. |
| `status.needs` | Status | Resolved dependency statuses from upstream steps. |

---

## Schema snapshots (minimal manifests)

These examples show the smallest useful shapes for each resource. Fields not
shown are optional; `status` is controller-owned.

### EngramTemplate (`catalog.bubustack.io/v1alpha1`)

```yaml
apiVersion: catalog.bubustack.io/v1alpha1
kind: EngramTemplate
metadata:
  name: http-client
spec:
  version: "1.0.0"
  description: "HTTP client"
  supportedModes:
  - job
  image: "ghcr.io/bubustack/engram-http:1.0.0"
  inputSchema:
    type: object
    required: ["url"]
    properties:
      url: {type: string}
      method: {type: string}
  outputSchema:
    type: object
    properties:
      status: {type: integer}
      body: {type: string}
  configSchema:
    type: object
    properties:
      timeout: {type: string}
  secretSchema:
    api:
      required: true
      description: "API token"
      mountType: env
      envPrefix: HTTP
      expectedKeys: ["token"]
```

### Engram (`bubustack.io/v1alpha1`)

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Engram
metadata:
  name: http-client-default
  namespace: workflows
spec:
  templateRef:
    name: http-client
    version: "1.0.0"
  mode: job
  with:
    timeout: "30s"
  secrets:
    api: http-client-secret
```

### ImpulseTemplate (`catalog.bubustack.io/v1alpha1`)

```yaml
apiVersion: catalog.bubustack.io/v1alpha1
kind: ImpulseTemplate
metadata:
  name: github-webhook
spec:
  version: "1.0.0"
  description: "GitHub webhook trigger"
  supportedModes:
  - deployment
  image: "ghcr.io/bubustack/impulse-github:1.0.0"
  contextSchema:
    type: object
    required: ["repository"]
    properties:
      repository: {type: object}
  deliveryPolicy:
    dedupe:
      mode: key
      keyTemplate: "{{ .repository.full_name }}"
    retry:
      maxAttempts: 5
      baseDelay: "2s"
      maxDelay: "30s"
      backoff: exponential
```

### Impulse (`bubustack.io/v1alpha1`)

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Impulse
metadata:
  name: github-webhook
  namespace: workflows
spec:
  templateRef:
    name: github-webhook
    version: "1.0.0"
  storyRef:
    name: deploy-app
  with:
    webhookSecretName: github-secret
  mapping:
    repository: "{{ .context.repository.full_name }}"
  deliveryPolicy:
    retry:
      maxAttempts: 3
      baseDelay: "1s"
      backoff: exponential
  throttle:
    maxInFlight: 5
    ratePerSecond: 10
```

### Story (`bubustack.io/v1alpha1`)

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: realtime-transcribe
  namespace: workflows
spec:
  pattern: streaming
  inputsSchema:
    type: object
    properties:
      language: {type: string}
  transports:
  - name: audio
    transportRef: livekit-default
    description: "Realtime audio transport"
  steps:
  - name: transcribe
    ref:
      name: speech-to-text
    transport: audio
    with:
      model: whisper
    runtime:
      language: "{{ inputs.language }}"
```

### StoryRun (`runs.bubustack.io/v1alpha1`)

```yaml
apiVersion: runs.bubustack.io/v1alpha1
kind: StoryRun
metadata:
  name: realtime-transcribe-001
  namespace: workflows
spec:
  storyRef:
    name: realtime-transcribe
  inputs:
    language: "en"
```

### StepRun (`runs.bubustack.io/v1alpha1`)

```yaml
apiVersion: runs.bubustack.io/v1alpha1
kind: StepRun
metadata:
  name: realtime-transcribe-001-transcribe
  namespace: workflows
spec:
  storyRunRef:
    name: realtime-transcribe-001
  stepId: transcribe
  engramRef:
    name: speech-to-text
  input:
    language: "en"
  timeout: "10m"
  retry:
    maxRetries: 2
    delay: "5s"
    backoff: exponential
```

### Transport (`transport.bubustack.io/v1alpha1`)

```yaml
apiVersion: transport.bubustack.io/v1alpha1
kind: Transport
metadata:
  name: livekit-default
spec:
  provider: livekit
  driver: livekit-grpc
  connectorImage: "ghcr.io/bubustack/transport-livekit:1.0.0"
  supportedAudio:
  - name: opus
    sampleRateHz: 48000
    channels: 2
  supportedBinary:
  - application/json
  configSchema:
    type: object
    properties:
      url: {type: string}
  defaultSettings:
    url: "wss://livekit.example.com"
```

### TransportBinding (`transport.bubustack.io/v1alpha1`)

```yaml
apiVersion: transport.bubustack.io/v1alpha1
kind: TransportBinding
metadata:
  name: audio-transcribe-binding
  namespace: workflows
spec:
  transportRef: livekit-default
  stepName: transcribe
  engramName: realtime-transcribe-001-transcribe
  driver: livekit-grpc
  audio:
    direction: subscribe
    codecs:
    - name: opus
      sampleRateHz: 48000
      channels: 2
  connectorEndpoint: "127.0.0.1:50051"
  rawSettings:
    room: "demo"
```

### ReferenceGrant (`policy.bubustack.io/v1alpha1`)

```yaml
apiVersion: policy.bubustack.io/v1alpha1
kind: ReferenceGrant
metadata:
  name: allow-shared-engram
  namespace: shared
spec:
  from:
  - group: bubustack.io
    kind: Story
    namespace: workflows
  to:
  - group: bubustack.io
    kind: Engram
    name: shared-ocr
```

---

## Status and validation surface

| Kind | Key status signals | Validation signal |
| --- | --- | --- |
| EngramTemplate | `status.conditions`, `status.usageCount` | `status.validationStatus`, `status.validationErrors` |
| ImpulseTemplate | `status.conditions`, `status.usageCount` | `status.validationStatus`, `status.validationErrors` |
| Engram | `status.conditions`, `status.phase`, `status.replicas`, `status.triggers` | `status.validationStatus`, `status.validationErrors` |
| Impulse | `status.conditions`, `status.phase`, `status.triggersReceived`, `status.storiesLaunched` | none (validation is admission-only) |
| Story | `status.conditions`, `status.stepsTotal`, `status.transports`, `status.triggers` | `status.validationStatus`, `status.validationErrors` |
| StoryRun | `status.conditions`, `status.phase`, `status.active`, `status.completed`, `status.error` | none (schema refs recorded in status) |
| StepRun | `status.conditions`, `status.phase`, `status.exitClass`, `status.retries`, `status.error` | none (schema refs recorded in status) |
| Transport | `status.conditions`, `status.availableAudio`, `status.availableVideo`, `status.availableBinary` | `status.validationStatus`, `status.validationErrors` |
| TransportBinding | `status.conditions`, `status.endpoint`, `status.negotiatedAudio`, `status.negotiatedVideo`, `status.negotiatedBinary` | none |
| ReferenceGrant | `status.conditions` | none |

---

## Status glossary (common fields)

- `status.conditions`: canonical lifecycle signal; controllers set Ready/Validated/Progressing style conditions.
- `status.phase`: human summary of lifecycle (Pending/Running/Succeeded/Failed/Finished/Canceled/Paused).
- `status.validationStatus`: controller validation outcome for declarative specs.
- `status.validationErrors`: human-readable validation failures (when validationStatus is not OK).
- `status.usageCount`: approximate count of downstream references (templates/engram/story).
- `status.triggers`: aggregate counter of run references (story/engram).
- `status.triggersReceived`: total events seen by an Impulse.
- `status.storiesLaunched`: total StoryRuns successfully started by an Impulse.
- `status.stepsTotal` / `status.stepsComplete` / `status.stepsFailed` / `status.stepsSkipped`: StoryRun progress summary.
- `status.active` / `status.completed`: StoryRun active or completed step names.
- `status.error`: structured error payload for StoryRun/StepRun failures.
- `status.exitClass`: StepRun exit interpretation (success/retry/terminal/rateLimited).
- `status.retries` / `status.nextRetryAt`: retry counter and next scheduled retry time for StepRun.
- `status.inputSchemaRef` / `status.outputSchemaRef`: resolved schema identifiers used at runtime.
- `status.endpoint`: TransportBinding resolved connector endpoint.
- `status.negotiatedAudio` / `status.negotiatedVideo` / `status.negotiatedBinary`: selected transport codecs/MIME types.

---

## Field ownership (spec vs status)

- `spec` fields are user-authored and validated by admission webhooks (defaulting
  may populate omitted fields).
- `status` fields are controller-owned and updated via the status subresource.
- Counters (`usageCount`, `triggers`, `storiesLaunched`, etc.) are best-effort,
  controller-maintained metrics for observability.
- Schema references (`status.inputSchemaRef`, `status.outputSchemaRef`) are set
  by run controllers after resolving the referenced Story/Engram.

---

## Schema reference resolution

When schemas are present, run controllers record the exact schema identifiers
used at runtime.

### StoryRun schema refs

`StoryRun` references the Story schema using:

- Input: `bubu://story/<namespace>/<story>/inputs`
- Output: `bubu://story/<namespace>/<story>/output`

Example:

```yaml
status:
  inputSchemaRef:
    ref: "bubu://story/workflows/realtime-transcribe/inputs"
    version: "1.2.0"
  outputSchemaRef:
    ref: "bubu://story/workflows/realtime-transcribe/output"
    version: "1.2.0"
```

### StepRun schema refs

`StepRun` references the Engram schema using:

- Input: `bubu://engram/<namespace>/<engram>/input`
- Output: `bubu://engram/<namespace>/<engram>/output`

Example:

```yaml
status:
  inputSchemaRef:
    ref: "bubu://engram/workflows/http-client-default/input"
    version: "1.0.0"
  outputSchemaRef:
    ref: "bubu://engram/workflows/http-client-default/output"
    version: "1.0.0"
```

Schema refs are omitted when the referenced resource does not declare a schema.

---

## Controller ownership map (who reconciles what)

Use this as a quick debugging guide. It lists the controller that owns each
resource’s lifecycle and the status fields you should expect it to update.

| Kind | Controller (file) | What it does | Status fields you can expect |
| --- | --- | --- | --- |
| EngramTemplate | `internal/controller/catalog/engramtemplate_controller.go` | Validates required fields (image/version), validates input/output/config schemas, computes usage count. | `status.validationStatus`, `status.validationErrors`, `status.conditions`, `status.usageCount`, `status.observedGeneration` |
| ImpulseTemplate | `internal/controller/catalog/impulsetemplate_controller.go` | Validates required fields, supported modes (deployment/statefulset), context/config schemas, computes usage count. | `status.validationStatus`, `status.validationErrors`, `status.conditions`, `status.usageCount`, `status.observedGeneration` |
| Engram | `internal/controller/engram_controller.go` | Resolves template ref, computes Story usage + StepRun trigger counts, emits validation conditions. | `status.validationStatus`, `status.validationErrors`, `status.conditions`, `status.usageCount`, `status.triggers`, `status.observedGeneration` |
| Impulse | `internal/controller/impulse_controller.go` | Resolves template + Story, reconciles workload + Service/SA/RBAC, aggregates trigger stats from StoryRuns. | `status.phase`, `status.conditions`, `status.replicas`, `status.readyReplicas`, `status.triggersReceived`, `status.storiesLaunched`, `status.failedTriggers`, `status.lastTrigger`, `status.lastSuccess`, `status.observedGeneration` |
| Story | `internal/controller/story_controller.go` | Validates references, computes usage + trigger counts, writes transport summary. | `status.validationStatus`, `status.validationErrors`, `status.conditions`, `status.usageCount`, `status.triggers`, `status.transports`, `status.stepsTotal` |
| StoryRun | `internal/controller/runs/storyrun_controller.go` | Validates inputs, orchestrates DAG, creates StepRuns, handles redrive, records schema refs. | `status.phase`, `status.conditions`, `status.active`, `status.completed`, `status.output`, `status.error`, `status.inputSchemaRef`, `status.outputSchemaRef` |
| StepRun | `internal/controller/runs/steprun_controller.go` | Executes step workloads, validates inputs/outputs, manages retries, records exit info + schema refs. | `status.phase`, `status.exitClass`, `status.exitCode`, `status.retries`, `status.nextRetryAt`, `status.output`, `status.error`, `status.inputSchemaRef`, `status.outputSchemaRef` |
| Transport | `internal/controller/transport/transport_controller.go` | Scaffolded controller (no reconcile logic yet). | Status fields are not automatically populated yet. |
| TransportBinding | `internal/controller/transport/transportbinding_controller.go` | Scaffolded controller (no reconcile logic yet). | Status fields are not automatically populated yet. |
| ReferenceGrant | none (resolved at validation time) | Cross-namespace checks via admission and controller reference resolution. | `status.conditions` only when/if a controller writes it. |

---

## Debugging quick map

- Template validation failures: check `EngramTemplate` / `ImpulseTemplate` status and controller logs (`engramtemplate`, `impulsetemplate`).
- Engram/Story reference errors: check `Engram` / `Story` status validation + controller logs (`engram`, `story`).
- Trigger counters not moving: check `StoryRun` status trigger tokens and Impulse/Story aggregation (`impulse`, `story`).
- StoryRun stuck: check `StoryRun` status phase + conditions; inspect DAG/StepRun creation (`storyrun`).
- StepRun failures: check `StepRun.status.exitClass`, `status.error`, and retry metadata (`steprun`).
- Transport binding empty status: transport controllers are currently scaffolded; status is not reconciled yet.

---

## Status field ownership by controller (selected fields)

| Status field(s) | Owner | Source |
| --- | --- | --- |
| `EngramTemplate.status.validationStatus`, `status.validationErrors`, `status.conditions`, `status.usageCount` | EngramTemplate controller | `internal/controller/catalog/engramtemplate_controller.go`, `internal/controller/catalog/template_helpers.go` |
| `ImpulseTemplate.status.validationStatus`, `status.validationErrors`, `status.conditions`, `status.usageCount` | ImpulseTemplate controller | `internal/controller/catalog/impulsetemplate_controller.go`, `internal/controller/catalog/template_helpers.go` |
| `Engram.status.validationStatus`, `status.validationErrors`, `status.conditions`, `status.usageCount`, `status.triggers` | Engram controller | `internal/controller/engram_controller.go` |
| `Story.status.validationStatus`, `status.validationErrors`, `status.conditions`, `status.usageCount`, `status.triggers`, `status.transports` | Story controller | `internal/controller/story_controller.go` |
| `Impulse.status.phase`, `status.conditions`, `status.replicas`, `status.readyReplicas` | Impulse controller | `internal/controller/impulse_controller.go`, `pkg/runs/status/impulse.go` |
| `Impulse.status.triggersReceived`, `status.storiesLaunched`, `status.failedTriggers`, `status.lastTrigger`, `status.lastSuccess` | Impulse controller (aggregated from StoryRuns) | `pkg/runs/status/impulse_stats.go` |
| `StoryRun.status.phase`, `status.conditions`, `status.startedAt`, `status.finishedAt`, `status.duration`, `status.attempts`, `status.triggerTokens` | StoryRun controller | `pkg/runs/status/storyrun.go` |
| `StoryRun.status.inputSchemaRef`, `status.outputSchemaRef` | StoryRun controller | `internal/controller/runs/storyrun_controller.go` |
| `StepRun.status.phase`, `status.conditions`, `status.startedAt`, `status.finishedAt`, `status.duration`, `status.lastFailureMsg` | StepRun controller | `pkg/runs/status/steprun.go` |
| `StepRun.status.exitClass`, `status.exitCode`, `status.retries`, `status.nextRetryAt`, `status.output`, `status.error` | StepRun controller | `internal/controller/runs/steprun_controller.go` |
| `StepRun.status.inputSchemaRef`, `status.outputSchemaRef` | StepRun controller | `internal/controller/runs/steprun_controller.go` |
| `Transport.status.*`, `TransportBinding.status.*` | Not reconciled yet | `internal/controller/transport/transport_controller.go`, `internal/controller/transport/transportbinding_controller.go` |

---

## Common troubleshooting scenarios

### Template validation fails

1. Inspect status and validation errors.
```bash
kubectl get engramtemplate <name> -o yaml
kubectl get impulsetemplate <name> -o yaml
```
2. Check controller-manager logs for schema or required-field errors.
```bash
kubectl logs -n <operator-namespace> deploy/<controller-manager> -c manager
```

### Engram or Story shows validation errors

1. Inspect validation status and error messages.
```bash
kubectl get engram <name> -n <namespace> -o yaml
kubectl get story <name> -n <namespace> -o yaml
```
2. Confirm referenced template/engram exists and matches the namespace rules.
```bash
kubectl get engramtemplate <name>
kubectl get engram <name> -n <namespace>
```

### StoryRun fails input schema validation

1. Read the StoryRun error and message.
```bash
kubectl get storyrun <name> -n <namespace> -o yaml
```
2. Compare the input payload with `Story.spec.inputsSchema`.
```bash
kubectl get story <story-name> -n <namespace> -o yaml
```

### StepRun fails or retries repeatedly

1. Inspect phase, exit class, and error payload.
```bash
kubectl get steprun <name> -n <namespace> -o yaml
```
2. If a pod name is present, fetch logs.
```bash
POD=$(kubectl get steprun <name> -n <namespace> -o jsonpath='{.status.podName}')
kubectl logs -n <namespace> pod/$POD
```

### Trigger counters not moving

1. Confirm StoryRun trigger tokens are being stamped.
```bash
kubectl get storyrun <name> -n <namespace> -o jsonpath='{.status.triggerTokens}'
```
2. Compare Story and Impulse aggregated counters.
```bash
kubectl get story <name> -n <namespace> -o yaml
kubectl get impulse <name> -n <namespace> -o yaml
```

### Streaming StepRun stuck waiting on transport

1. Check StepRun phase and transport condition message.
```bash
kubectl get steprun <name> -n <namespace> -o yaml
```
2. Ensure a TransportBinding exists for the step.
```bash
kubectl get transportbinding -n <namespace>
```

---

## Execution patterns (batch vs streaming)

### Batch Story example

Batch stories evaluate `steps[].with` at runtime and can reference predecessor
outputs using `steps.*` expressions.

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: batch-etl
  namespace: workflows
spec:
  pattern: batch
  inputsSchema:
    type: object
    properties:
      url: {type: string}
  steps:
  - name: fetch
    ref:
      name: http-client
    with:
      url: "{{ inputs.url }}"
  - name: transform
    ref:
      name: json-transform
    needs: ["fetch"]
    with:
      payload: "{{ steps.fetch.outputs.body }}"
```

### Streaming Story example (per-packet runtime)

Streaming stories keep `steps[].with` static and evaluate `steps[].runtime`
per packet, with access to `packet.*` and predecessor outputs.

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: realtime-intent
  namespace: workflows
spec:
  pattern: streaming
  inputsSchema:
    type: object
    properties:
      language: {type: string}
  transports:
  - name: audio
    transportRef: livekit-default
  steps:
  - name: transcribe
    ref:
      name: speech-to-text
    transport: audio
    with:
      model: whisper
    runtime:
      language: "{{ inputs.language }}"
  - name: classify
    ref:
      name: intent-classifier
    needs: ["transcribe"]
    transport: audio
    with:
      model: intent-v1
    runtime:
      text: "{{ steps.transcribe.outputs.text }}"
      speaker: "{{ packet.identity }}"
```

---

## Policy resolution

Execution policies are resolved hierarchically:

```
Operator defaults
  -> Template defaults
    -> Story policy
      -> Step overrides
        -> StepRun overrides (runtime)
```

This resolution applies to timeouts, retry behavior, resources, storage, security,
and other execution properties.

---

## Schema validation and defaults

- Story inputs are validated against `Story.spec.inputsSchema`.
- Step inputs are validated against `EngramTemplate.spec.inputSchema`.
- Step outputs are validated against `EngramTemplate.spec.outputSchema`.
- JSON Schema defaults are applied at runtime when fields are missing.

See `/docs/runtime/inputs.md` and `/docs/runtime/payloads.md` for full details.

---

## Streaming and transport

Streaming pipelines use a transport layer defined by:

- `Transport` (provider definition + schema)
- `TransportBinding` (runtime binding per step or per run)
- gRPC transport protos in `tractatus`

The streaming contract is documented in `/docs/streaming/streaming-contract.md`.

---

## References and scoping

Cross-namespace references default to same-namespace resolution. The operator
config key `references.cross-namespace-policy` controls behavior:

- `deny`: cross-namespace references are rejected by admission validation.
- `grant`: cross-namespace references are allowed only when a ReferenceGrant in the target namespace permits them.
- `allow`: all cross-namespace references are allowed.

ReferenceGrant matches the referencing resource (`spec.from`) and the target
resource (`spec.to`) and is always created in the target namespace.
Admission webhooks and controllers both enforce the policy.

See `/docs/api/scoping.md` for the full policy and examples.

---

## Versioning

- Templates and Stories support explicit `spec.version` fields.
- Runs record schema references for inputs and outputs.
- CRD versioning and migration plans are covered in `/docs/api/migration.md`.

---

## Related docs

- `/docs/overview/core.md`
- `/docs/runtime/lifecycle.md`
- `/docs/runtime/inputs.md`
- `/docs/runtime/payloads.md`
- `/docs/streaming/streaming-contract.md`
- `/docs/api/scoping.md`
