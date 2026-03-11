# Streaming Lifecycle Hooks

Lifecycle hooks are special packets emitted by the streaming hub when key
readiness milestones are reached. They allow steps to defer execution until the
streaming pipeline is fully established.

## Who this is for

- Engram authors building streaming components that need startup coordination.
- Story authors designing real-time pipelines with ordered initialization.
- Operators debugging streaming step startup timing.

## What you'll get

- The two lifecycle hook types and when they fire.
- How to consume hooks in step `if` conditions.
- Hook packet structure and available fields.
- Deduplication guarantees.

## Hook types

| Hook | Fires when | Use case |
|------|-----------|----------|
| `steprun.ready` | A single streaming step connects to the hub. | React to individual step availability. |
| `storyrun.ready` | **All** streaming steps in the Story have connected. | Defer work until the full pipeline is online. |

### Emission order

1. Each time a streaming step registers with the hub, a `steprun.ready` hook is
   emitted immediately for that step.
2. After every registration, the hub checks whether all streaming steps are now
   connected. If so, it emits `storyrun.ready`.

Both hooks are emitted **at most once** per StoryRun per step combination. The
hub tracks emitted hooks internally and deduplicates by
`namespace/storyRun:event:stepID`.

## Consuming hooks

Steps declare interest in lifecycle hooks via `if` conditions that match on
`packet.type`. The hub evaluates each step's `if` expression against the hook
packet and delivers it only if the condition passes.

### Example: trigger a greeting after all steps connect

```yaml
steps:
  - name: ingress
    transport: voice
    ref:
      name: livekit-webhook-impulse

  - name: greet
    needs:
      - ingress
    transport: voice
    if: '{{ eq (default "" packet.type) "storyrun.ready" }}'
    ref:
      name: openai-assistant
    with:
      prompt: "Greet the user warmly."
```

The `greet` step will not process any packets until the `storyrun.ready` hook
arrives, ensuring the full pipeline is online before the greeting fires.

### Example: react to individual step readiness

```yaml
  - name: monitor
    needs:
      - processor
    transport: data
    if: '{{ eq (default "" packet.type) "steprun.ready" }}'
    ref:
      name: health-checker
```

## Hook packet structure

Hooks are delivered as standard streaming packets with an Envelope of kind
`hook`. The packet fields available in expressions:

| Field | Value | Description |
|-------|-------|-------------|
| `packet.type` | `"storyrun.ready"` or `"steprun.ready"` | The hook event name. |
| `packet.identity` | Varies | Participant identity from routing metadata. |
| `packet.text` | `""` | Empty for hooks (no signal data). |
| `packet.history` | `""` | Empty for hooks. |

### Wire format (Envelope)

On the wire, hooks are encoded as a `StreamEnvelope` inside a `DataPacket`:

```json
{
  "kind": "hook",
  "type": "storyrun.ready",
  "hook": {
    "version": "v1",
    "event": "storyrun.ready",
    "source": "hub",
    "data": {
      "timestamp": "2026-03-08T12:00:00Z",
      "storyRun": {
        "name": "my-story-run-abc",
        "namespace": "default"
      }
    }
  }
}
```

For `steprun.ready`, the `data` object also includes:

```json
{
  "data": {
    "timestamp": "2026-03-08T12:00:00Z",
    "storyRun": {
      "name": "my-story-run-abc",
      "namespace": "default"
    },
    "step": {
      "name": "ingress"
    }
  }
}
```

## How the hub finds consumers

The hub scans all steps in the Story definition for `if` conditions that
contain the hook event name (case-insensitive substring match). Only matching
steps receive the hook packet. Steps without an `if` condition, or with
conditions that don't reference the hook name, are skipped.

## Transport settings for hooks

Hook delivery respects the same transport settings as regular packets:

- Backpressure and flow control apply.
- Buffer limits apply if the consumer is not yet ready to receive.
- Retry policies apply on send failure.

The hub resolves transport settings for the target step before delivering the
hook packet.

## Deduplication

Each hook is identified by the composite key:

```
{namespace}/{storyRunName}:{eventName}:{stepID}
```

The hub maintains an in-memory set of emitted hooks. Once a hook fires for a
given key, it will not fire again for the lifetime of that StoryRun session.

## Debugging hooks

- **Hook not firing**: Verify that all streaming steps have connected (for
  `storyrun.ready`). Check the hub logs for `"emitting lifecycle hook"` entries.
- **Step not receiving hook**: Verify the step's `if` condition matches the hook
  event name. The match is case-insensitive substring, so
  `{{ eq (default "" packet.type) "storyrun.ready" }}` works.
- **Hook firing multiple times**: This should not happen. If it does, check the
  hub's `emittedHooks` deduplication logic.

## Related docs

- `/docs/streaming/streaming-contract.md` -- Streaming message contract and termination rules.
- `/docs/streaming/transport-settings.md` -- Backpressure, flow control, and delivery settings.
- `/docs/runtime/expressions.md` -- Template syntax and `packet.*` context variables.
- `/docs/runtime/lifecycle.md` -- StoryRun/StepRun phases and terminal semantics.
