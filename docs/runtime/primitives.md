# Workflow Primitives

This document describes the built-in workflow primitives, with special focus on the batch-only `gate` and `wait` steps.

## Who this is for

- Workflow authors choosing between primitives vs Engrams.
- Operators debugging step behavior.

## What you'll get

- The semantics of each built-in step type.
- When batch-only primitives are available.
- Cleanup and failure behavior for primitive steps.

For expression syntax and evaluation rules, see `/docs/runtime/expressions.md`.
For input schema/defaults and payload contracts, see `/docs/runtime/inputs.md` and `/docs/runtime/payloads.md`.
For lifecycle/state semantics, see `/docs/runtime/lifecycle.md`.
For versioning and pinning, see `/docs/api/versioning.md`.
For namespace scoping rules, see `/docs/api/scoping.md`.

## Supported primitives

- `condition`
- `parallel`
- `sleep`
- `stop`
- `executeStory`
- `wait` (batch-only)
- `gate` (batch-only)

Removed primitives (no longer supported): `switch`, `throttle`, `batch` (as step types). The `batch` term still exists as a Story execution pattern (`spec.pattern: batch`).

### Support matrix

| Step type | Batch | Streaming | Notes |
| --- | --- | --- | --- |
| engram (type omitted, `ref` set) | yes | yes | Runs the referenced Engram |
| condition | yes | yes | Uses `if` for gating |
| parallel | yes | yes | Fan-out/fan-in control |
| sleep | yes | yes | Delay in execution |
| stop | yes | yes | Terminates the workflow |
| executeStory | yes | yes | Sub-workflow call |
| wait | yes | no | Batch-only primitive |
| gate | yes | no | Batch-only primitive |

## Map/reduce and dynamic fan-out

Map/reduce and dynamic fan-out are implemented via dedicated Engrams, not controller primitives.
Use an Engram that manages fan-out, aggregation, and storage refs for large intermediate data.
The controller will not offload or hydrate payloads for dynamic fan-out; storage actions must
be performed by the Engram itself.

Example (map-only using the map-reduce-adapter Engram):

```yaml
- name: map-items
  ref: map-reduce-adapter
  with:
    items: "{{ inputs.items }}"
    map:
      storyRef:
        name: per-item-story
      concurrency: 20
      batchSize: 200
```

For a full map+reduce example, see `engrams/map-reduce-adapter-engram/README.md` in the BubuStack repo.

## Parallel fan-in behavior

The `parallel` primitive fans out into child StepRuns and then aggregates completion:

- The parent `parallel` step stays `Running` until all child StepRuns are terminal.
- If any child fails and is not marked `allowFailure`, the parent step becomes `Failed`.
- If failed children are marked `allowFailure`, the parent step becomes `Succeeded` with an informational message.
- Skipped children are treated as terminal and do not fail the parent.

To allow failures for specific branches, set `allowFailure: true` on that branch in the parallel `with.steps` list.

## Batch-only primitives

`gate` and `wait` are allowed only in batch Stories. The admission webhook rejects these step types when `spec.pattern: streaming`.

## Cleanup blocks

Stories can declare cleanup steps that run after the main DAG finishes:

- `spec.compensations`: steps that run only when the story fails.
- `spec.finally`: steps that run after the story completes (success or failure).

Compensations run before finally steps. Both blocks accept the same `Step` schema
as `spec.steps`, including `needs`, `if`, and `with`.

Cleanup blocks work for both batch and streaming stories:

- **Batch:** cleanup steps run after the main DAG steps reach terminal phases.
- **Streaming:** cleanup steps run after the streaming topology terminates
  (i.e., when the hub stops and main step states are finalized). Cleanup steps
  always execute as batch jobs regardless of the Story's `spec.pattern`.

### Example

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: cleanup-flow
spec:
  pattern: batch
  steps:
  - name: deploy
    ref:
      name: deploy-app
  compensations:
  - name: rollback
    ref:
      name: rollback-app
    needs: ["deploy"]
  finally:
  - name: notify
    ref:
      name: slack-notify
```

### Streaming cleanup example

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: streaming-with-cleanup
spec:
  pattern: streaming
  steps:
  - name: transcribe
    ref:
      name: whisper-transcriber
  - name: respond
    ref:
      name: llm-responder
    needs: ["transcribe"]
  compensations:
  - name: release-session
    ref:
      name: session-cleanup
  finally:
  - name: log-metrics
    ref:
      name: metrics-reporter
```

When the streaming topology terminates (success or failure), `release-session`
runs only on failure. `log-metrics` runs after every termination. Both execute as
batch jobs.

## Gate

`gate` is a manual approval step. It pauses the workflow until a decision is written to the StoryRun status.

### Gate status schema

Gate decisions live under `status.gates` in the StoryRun and are keyed by step name:

- `state`: `Pending` | `Approved` | `Rejected`
- `message`: optional human-readable context
- `updatedAt`: optional timestamp
- `updatedBy`: optional identifier

Default behavior:
- If no decision is set, the step stays `Paused`.
- `Approved` -> step `Succeeded`
- `Rejected` -> step `Failed`

### Example Story

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: gated-workflow
spec:
  pattern: batch
  steps:
  - name: approve
    type: gate
    with:
      timeout: "30m"
      onTimeout: "fail"
```

### Approving a gate

```bash
kubectl patch storyrun <name> --type merge --subresource status \
  -p '{"status":{"gates":{"approve":{"state":"Approved","message":"ok"}}}}'
```

## Wait

`wait` pauses until its `with.until` template expression evaluates to `true`.
Use Go template syntax (`{{ ... }}`); plain strings are treated as literals.

### Wait schema

```yaml
with:
  until: "{{ inputs.ready }}"     # required
  timeout: "10m"                  # optional, duration
  pollInterval: "5s"              # optional, duration
  onTimeout: "fail"               # optional: fail | skip
```

Behavior:
- If `until` is true, the step transitions to `Succeeded`.
- If `until` is false or blocked by missing data, the step stays `Paused`.
- If `timeout` elapses:
  - `onTimeout: fail` -> step `Timeout`
  - `onTimeout: skip` -> step `Skipped`
- `pollInterval` controls the requeue cadence (bounded by a minimum safety threshold).

### Example Story

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: wait-for-signal
spec:
  pattern: batch
  steps:
  - name: wait-ready
    type: wait
    with:
      until: "{{ inputs.ready }}"
      pollInterval: "5s"
      timeout: "30m"
      onTimeout: "skip"
```

## StepState timing

`StoryRun.status.stepStates[stepName]` tracks timestamps:
- `startedAt`: when the step first enters a non-empty phase
- `finishedAt`: when the step reaches a terminal phase

For `gate` and `wait`, these timestamps reflect pause/resume behavior and timeouts.
