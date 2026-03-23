# Durable Semantics

This document defines the current durability guarantees and limits in BubuStack.
It is a contract for operators, SDK users, and workflow authors.

## Who this is for

- Operators who need to understand durability and recovery guarantees.
- Workflow authors who care about retries and idempotency.
- SDK/component authors implementing correct behavior on failures.

## What you'll get

- The exact delivery guarantees and failure boundaries.
- How retries, redrive, and cleanup behave.
- The constraints you must design around for correctness.

---

## Delivery model (current)

- StoryRun creation is at-least-once by default. If the same external trigger is
  retried without an idempotency token, multiple StoryRuns may be created.
- When a trigger token is provided via the SDK, the SDK derives a deterministic
  StoryRun name and treats `AlreadyExists` as idempotent when inputs match.
- Trigger tokens must only be reused with identical inputs; mismatches are rejected.
- Impulses can configure delivery behavior (dedupe + retry schedule) via
  `spec.deliveryPolicy`. The SDK enforces this when BUBU trigger policy
  environment variables are present.
- Impulses can throttle trigger submission via `spec.throttle`. The SDK enforces
  per-pod rate and concurrency limits and records throttled events in
  `Impulse.status.throttledTriggers` and `Impulse.status.lastThrottled`.
- StepRun creation is idempotent for Engram-backed steps because controllers derive
  deterministic StepRun names from StoryRun name and step name.
- Step execution is at-least-once. A StepRun may execute multiple times because
  retries and job recreation can re-run the same step.

## Explicit delivery guarantees (current)

The guarantees below are the explicit contract boundaries for BubuStack today.
If you need stronger guarantees (exactly-once effects), you must use idempotency
keys and ledgering as described later in this document.

- **StoryRun creation**
  - **At-least-once** when no trigger token is used.
  - **Idempotent for identical inputs** when a trigger token is used and the
    deterministic StoryRun name matches the token-derived name.
- **StepRun creation**
  - **Idempotent per step** for Engram-backed steps: StepRun names are derived
    deterministically from StoryRun name + step name.
- **Step execution**
  - **At-least-once**. Retries and Job recreation can re-run the same step.
- **Signals**
  - **Best-effort** delivery. No ordering or replay guarantee unless signal
    sequences are used and persisted in the StepRun status.
- **Streaming transport**
  - **Best-effort by default**. When `delivery.semantics=at_least_once` and replay
    is enabled, the hub provides at-least-once delivery with replay on reconnect.
    In-memory buffers can still drop messages on overflow in best-effort modes.

---

## Delivery matrix (current)

| Operation | Guarantee | Notes |
| --- | --- | --- |
| StoryRun creation (no trigger token) | At-least-once | Retries can create multiple StoryRuns. |
| StoryRun creation (with trigger token) | Idempotent for identical inputs | Reusing a token with different inputs is rejected. |
| StepRun creation (Engram-backed step) | Idempotent per step | Deterministic names prevent duplicate StepRuns for the same step. |
| Step execution | At-least-once | Retries and job recreation can re-run steps. |
| Signals | Best-effort | No ordering or replay guarantees. |
| Streaming transport (default) | Best-effort | At-least-once when delivery semantics + replay are enabled. |

---

## Trigger delivery policy

Impulse delivery policy controls how triggers dedupe and retry StoryRun creation.
It is configured on `ImpulseTemplate.spec.deliveryPolicy` and can be overridden
per `Impulse.spec.deliveryPolicy`.

Dedupe modes:
- `none`: no deduplication; repeated triggers may create multiple StoryRuns.
- `token`: a trigger token must be provided; missing tokens are rejected.
- `key`: the SDK derives a token from `dedupe.keyTemplate` and uses it for idempotency.

Key templates are evaluated deterministically with the SDK template engine. The
template can reference:
- `inputs` (trigger payload map)
- `story.name` and `story.namespace`
- `impulse.name` and `impulse.namespace`

Retry schedule (trigger delivery, not step execution):
- `maxAttempts`: total attempts including the first.
- `baseDelay`: initial retry delay (Go duration string).
- `maxDelay`: cap for computed delays.
- `backoff`: `exponential`, `linear`, or `constant`.

Retries are only attempted for retryable Kubernetes API errors. If a trigger
token is used (explicitly or via `dedupe.keyTemplate`), repeated attempts map to
the same StoryRun; if inputs differ, the SDK rejects the retry.

When a trigger token is set, the StoryRun must include a trigger input hash
annotation (`storyrun.bubustack.io/trigger-input-hash`) that matches the inputs.
The SDK sets this automatically; non-SDK clients must compute and supply it.

Custom clients that do not use the SDK must implement the same behavior to
respect the policy.

---

## Retry and idempotency expectations

- Retries are controlled by StepRun retry policies and can re-execute steps.
- Trigger delivery retries are separate from StepRun retries and only govern
  StoryRun creation.
- Safe retry requires idempotent external side effects or external idempotency keys.
- Use stable identifiers derived from StoryRun and StepRun identity for idempotency keys.
- Preserve the original trigger token at the event source and reuse it for retries
  so replays resolve to the same StoryRun.

---

## Exactly-once via idempotency (explicit model)

BubuStack does **not** provide native exactly-once execution for step side effects.
Instead, it supports **effectively exactly-once** behavior **only** when you:

1. Use stable idempotency keys derived from StoryRun/StepRun identity.
2. Record side effects in a durable ledger (StepRun status or external system).
3. Make external calls idempotent or transactional using those keys.

What “exactly-once” means in BubuStack:
- **Exactly-once side effects** are achieved **by the caller** using idempotency
  keys and ledgering. BubuStack provides the identifiers and persistence hooks,
  but it does not prevent duplicates by itself.
- **Exactly-once does not apply** to step execution. A step can run multiple
  times under retry/recreate conditions; only the **effects** can be deduped.

Known failure modes / boundaries:
- Job retries, controller restarts, or kube-apiserver errors can re-run a step.
- If your external system ignores idempotency keys, duplicate effects can occur.
- If you emit effects before recording them durably, you can observe duplicates
  on retry.

Recommended pattern:
1. Generate a stable idempotency key.
2. Check your effect ledger (or external system) to see if the effect exists.
3. Write durable state **before** side effects when possible.

---

## SDK usage patterns

Example: idempotent StoryRun creation with a trigger token.

```go
ctx := sdk.WithTriggerToken(ctx, "source-event-id-123")
run, err := sdk.StartStory(ctx, "my-story", inputs)
```

Example: stable idempotency keys for external side effects.

```go
key := fmt.Sprintf("storyrun/%s/step/%s", run.Name, stepID)
```

Example: record side effects in the StepRun ledger.

```go
if err := sdk.RecordEffect(ctx, key, "succeeded", map[string]any{"providerId": id}); err != nil {
    // Treat as soft failure if you can tolerate missing ledger entries.
}
```

---

## Recovery rules (current)

- On controller restart, StoryRun reconciliation rehydrates StepState from existing
  StepRuns and merges terminal phases without clobbering completed steps.
- StepRun reconciliation reattaches to the Job by name when it exists.
- If a Job is missing while a StepRun is still non-terminal, a new Job is created
  and the step is re-executed.
- Resume vs restart rules:
  - **Resume**: if the Job exists, the controller resumes monitoring and sets the StepRun to `Running`
    when it was still `Pending`.
  - **Restart**: if the Job is missing after a prior execution, the controller recreates the Job and
    records restart metadata on the StepRun.
  - Restart metadata is tracked via annotations:
    - `runs.bubustack.io/job-uid` (last observed Job UID)
    - `runs.bubustack.io/restart-count` (monotonic restart counter)
    - `runs.bubustack.io/restarted-at` (RFC3339 timestamp)
- `gate` and `wait` steps remain paused until their conditions are satisfied or
  timeouts apply; gate decisions live in StoryRun status.
- StoryRun redrive is annotation-driven: set `storyrun.bubustack.io/redrive-token`
  to a new value. The controller deletes child StepRuns/StoryRuns, clears step
  timers, resets StoryRun status, and re-runs with the same spec/inputs. StoryRun
  spec remains immutable; redrive uses metadata only. The controller records the
  last processed token in `storyrun.bubustack.io/redrive-observed`.

---

## Timers and schedules

- Story timeouts are enforced by the DAG reconciler.
- `wait` and `gate` support poll intervals and timeouts.
- `sleep` uses a durable timer persisted on the StoryRun (annotation-backed) and
  pauses execution until the deadline is reached.
- Timer precision is bounded by reconcile cadence and controller requeue delays.
- Cron/schedules are implemented as an external impulse (see
  the `cron-impulse` component in the engrams repository for implementation details).

---

## State persistence and history

- Durable state is stored in StoryRun and StepRun status.
- Large payloads are stored via storage references instead of inline status data.
- There is no durable event history log today; retention is managed by StoryRun
  retention settings and controller cleanup.
- Status updates are eventually consistent at the object level and follow a
  last-writer-wins model.
- Operational visibility relies on Kubernetes Events (best-effort, not durable,
  not replayable). BubuStack does not add new CRDs or persist a workflow event
  history log.
- Resource size guardrails are intentional: signals/effects are bounded lists,
  signal payloads are capped, and large payloads must be offloaded to storage
  refs. Avoid writing large aggregates to status.

---

## Signals and events

- Step-level signals are written to StepRun status and merged into step context.
- Signal delivery is best-effort. Payloads are capped and may be truncated.
- Signal **events** are appended to `status.signalEvents` with a monotonic sequence
  number for replay. The list is bounded; older events may be trimmed.
- The SDK exposes a replay helper that reads `status.signalEvents` and returns
  events after a given sequence number.
- Ordering is by `signalEvents[].seq` when available. The `status.signals` map is
  last-writer-wins and is intended for “latest value” lookups.
- Streaming transport buffers are in-memory and can drop messages on overflow.
- Kubernetes Events are used for operational diagnostics (e.g., retries, restarts,
  blocked templates) and should not be treated as a durable signal channel.

---

## External side effects guidance

- Write durable state before invoking external side effects when possible.
- Use idempotency keys derived from StoryRun or StepRun identity for external calls.
- Record effects in the StepRun `status.effects` ledger (or your own outbox) so retries can
  detect already-applied side effects.
- Prefer transactional outbox patterns or external systems that provide exactly-once
  guarantees when needed.
- SDK helper for effect dedupe:

```go
result, already, err := sdk.ExecuteEffectOnce(ctx, key, func(ctx context.Context) (any, error) {
	// perform side effect, return safe details for the effect ledger
	return map[string]any{"providerId": "abc"}, nil
})
if errors.Is(err, sdk.ErrEffectAlreadyRecorded) || already {
	// effect already recorded; skip duplicate side effects
}
_ = result
```

---

## Related references

- `/docs/overview/core.md` for core resources and execution flow.
- `/docs/runtime/primitives.md` for step semantics and gate/wait behavior.
- `/docs/runtime/lifecycle.md` for phase and terminal rules.
- `/docs/runtime/inputs.md` and `/docs/runtime/payloads.md` for size limits and storage refs.
