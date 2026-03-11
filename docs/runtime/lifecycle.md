# Run Lifecycle Contract

This document defines the lifecycle contract for StoryRun and StepRun.

## Who this is for

- Workflow authors interpreting run state.
- Operators troubleshooting stuck or failed runs.

## What you'll get

- The canonical phases and conditions.
- The meaning of terminal vs non-terminal states.
- How retries, timeouts, and cleanup affect status.

## Canonical status

`status.conditions` is the canonical lifecycle source of truth. `status.phase`
and `status.message` are summaries. The `Ready` condition reason uses stable
reason codes for transitions.

## StoryRun phases

| Phase | Terminal | Typical reason |
| --- | --- | --- |
| Pending | no | `Pending` |
| Running | no | `Running` |
| Paused | no | `Paused` |
| Blocked | no | `Blocked` |
| Scheduling | no | `Scheduled` |
| Succeeded | yes | `Completed` |
| Failed | yes | `ExecutionFailed` |
| Timeout | yes | `TimedOut` |
| Canceled | yes | `Canceled` |
| Finished | yes | `Canceled` |
| Aborted | yes | `Canceled` |
| Skipped | yes | `Skipped` |
| Compensated | yes | `Compensated` |

Notes:
- Stories fail fast by default. Set `spec.policy.retries.continueOnStepFailure: true`
  to keep scheduling independent steps after a failure.
- Fail-fast or dependency failures use `DependencyFailed`.
- Input/output schema validation uses `InputSchemaFailed` / `OutputSchemaFailed`.
- Compensation failures use `CompensationFailed`.
- Finally/cleanup failures use `CleanupFailed`.
- `spec.policy.timeouts.story` enforces a total StoryRun timeout and transitions
  the run to `Timeout` when exceeded.
- Steps marked `allowFailure: true` may fail without failing the StoryRun. These
  steps remain `Failed` in `status.stepStates`, and their names appear in
  `status.allowedFailures`.
- `status.attempts` increments each time the StoryRun enters `Running`.

## StepRun phases

| Phase | Terminal | Typical reason |
| --- | --- | --- |
| Pending | no | `Pending` |
| Running | no | `Running` |
| Paused | no | `Paused` |
| Blocked | no | `Blocked` |
| Scheduling | no | `Scheduled` |
| Succeeded | yes | `Completed` |
| Failed | yes | `ExecutionFailed` |
| Timeout | yes | `TimedOut` |
| Canceled | yes | `Canceled` |
| Finished | yes | `Canceled` |
| Aborted | yes | `Canceled` |
| Skipped | yes | `Skipped` |
| Compensated | yes | `Compensated` |

Retry scheduling uses the `RetryScheduled` reason while the phase is `Pending`.
