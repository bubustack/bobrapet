# Transport Streaming Settings

This document describes the structured streaming settings that control buffering,
flow control, ordering, and replay for real-time transports.

## Who this is for

- Connector/transport authors configuring runtime behavior.
- Operators tuning backpressure and replay limits.

## What you'll get

- The full transport settings schema.
- The merge/override rules for settings.
- Guidance for safe defaults in production.

## At a glance

- Structured settings live under `streaming` on `Transport` and `Story.spec.transports[]`.
- Settings are merged with transport-level defaults first, then story-level overrides.
- The hub enforces buffering, flow control, ordering/replay, routing, fan-in, partitioning, and recording.

## Merge and override rules

Structured settings are **merged** in this order (later entries override earlier ones):

1. `Transport.spec.streaming` (cluster-wide defaults)
2. `Story.spec.transports[].streaming` (per-story overrides)

Provider-specific settings can still be supplied via:

- `Transport.spec.defaultSettings` (raw JSON)
- `Story.spec.transports[].settings` (raw JSON)

Structured `streaming` fields **overlay** the raw JSON settings at the same level.

All duration strings use Go duration format (for example `250ms`, `1s`, `5m`).

## Validation

- Structured `streaming` fields are validated by admission webhooks (enums, ranges, required fields).
- Raw JSON settings are treated as provider-owned and are not validated by the operator.

## Defaults and guardrails

- If `flowControl` is omitted, the system defaults to `credits` with `ackEvery.messages=10`,
  `ackEvery.maxDelay=250ms`, `pauseThreshold=90`, and `resumeThreshold=50`. Set `mode: none` to disable.
- If `routing.maxDownstreams` is omitted, it defaults to `32`.
- If `lanes` is omitted on the **Transport**, defaults are injected for `audio`, `video`, `binary`, and `payload`
  with `maxMessages=100` and `maxBytes=1MiB`. If lanes are provided, missing per-lane caps are defaulted,
  but lanes are not auto-added.
- If buffer limits are omitted or set to `0`, the hub falls back to `BUBU_HUB_BUFFER_MAX_MESSAGES` (default 100),
  `BUBU_HUB_BUFFER_MAX_BYTES` (default 10MiB), and `drop_newest`.
- Operator guardrails can cap active streams, buffer count, and downstream fan-out regardless of per-transport settings.

---

## Example: Transport defaults

```yaml
apiVersion: transport.bubustack.io/v1alpha1
kind: Transport
metadata:
  name: livekit
spec:
  provider: livekit
  driver: livekit
  supportedAudio:
  - name: opus
  streaming:
    backpressure:
      buffer:
        maxMessages: 500
        maxBytes: 10485760
        maxAgeSeconds: 10
        dropPolicy: drop_oldest
    flowControl:
      mode: credits
      initialCredits:
        messages: 100
        bytes: 1048576
      ackEvery:
        messages: 10
        maxDelay: 250ms
      pauseThreshold:
        bufferPct: 90
      resumeThreshold:
        bufferPct: 50
    delivery:
      ordering: per_stream
      semantics: at_least_once
      replay:
        mode: memory
        retentionSeconds: 30
        checkpointInterval: 1s
    routing:
      mode: auto
      fanOut: sequential
      maxDownstreams: 8
      rules:
      - name: route-english
        when: "{{ packet.metadata.lang == \"en\" }}"
        action: allow
        target:
          steps: ["translate"]
      - name: drop-noisy
        when: "{{ packet.metadata.noisy == true }}"
        action: deny
        target:
          steps: ["transcribe"]
    lanes:
    - name: audio
      kind: audio
      direction: bidirectional
      maxMessages: 250
      maxBytes: 1048576
    - name: payload
      kind: payload
      direction: bidirectional
      maxMessages: 500
      maxBytes: 4194304
    fanIn:
      mode: all
      timeoutSeconds: 30
      maxEntries: 1000
    partitioning:
      mode: hash
      key: metadata.user_id
      partitions: 16
      sticky: true
    lifecycle:
      strategy: drain_cutover
      drainTimeoutSeconds: 15
      maxInFlight: 200
    observability:
      metrics:
        enabled: true
      tracing:
        enabled: true
        sampleRate: 5
        samplePolicy: rate
      watermark:
        enabled: true
        timestampSource: metadata.event_time_ms
    recording:
      mode: metadata
      sampleRate: 1
      retentionSeconds: 300
      redactFields:
      - payload.api_key
```

## Example: Story override

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: realtime-demo
spec:
  transports:
  - name: audio
    transportRef: livekit
    streaming:
      backpressure:
        buffer:
          maxMessages: 1000
          dropPolicy: drop_newest
      delivery:
        semantics: best_effort
```

---

## Settings reference

### `streaming.backpressure.buffer`
- `maxMessages` (int, >= 0): cap the number of buffered messages.
- `maxBytes` (int, >= 0): cap total buffered bytes.
- `maxAgeSeconds` (int, >= 0): max time a message can stay buffered.
- `dropPolicy` (`drop_newest` | `drop_oldest`): which messages to drop on overflow.

### `streaming.flowControl`
- `mode` (`none` | `credits` | `window`): flow control strategy.
- `initialCredits.messages` / `initialCredits.bytes`: starting credits.
- `ackEvery.messages` / `ackEvery.bytes`: ack/credit interval thresholds.
- `ackEvery.maxDelay` (duration string): max time between ack/credit updates.
- `pauseThreshold.bufferPct` (0..100): pause upstream when buffer crosses this percent.
- `resumeThreshold.bufferPct` (0..100): resume upstream below this percent.

### `streaming.delivery`
- `ordering` (`none` | `per_stream` | `per_partition`)
- `semantics` (`best_effort` | `at_least_once`)
- `replay.mode` (`none` | `memory` | `durable`)
- `replay.retentionSeconds` (int, >= 0)
- `replay.checkpointInterval` (duration string)

### `streaming.routing`
- `mode` (`auto` | `hub` | `p2p`): routing topology selection.
- `fanOut` (`sequential` | `parallel`): dispatch policy when multiple downstream steps consume a stream.
- `maxDownstreams` (int, >= 1): guardrail on downstream fan-out per step.
- `rules[].name` (string): optional identifier for a routing rule.
- `rules[].when` (string): Go template expression evaluated in streaming runtime scope.
- `rules[].action` (`allow` | `deny`): how the rule affects matching targets.
- `rules[].target.steps` (list): downstream step names the rule applies to (empty means all).

### `streaming.lanes`
- `lanes[].name` (string): unique lane identifier (e.g., `audio`, `payload`).
- `lanes[].kind` (`audio` | `video` | `binary` | `payload` | `control`): lane media type.
- `lanes[].direction` (`publish` | `subscribe` | `bidirectional`).
- `lanes[].maxMessages` (int, >= 0): per-lane buffered message cap (per downstream).
- `lanes[].maxBytes` (int, >= 0): per-lane buffered byte cap (per downstream).

### `streaming.fanIn`
- `mode` (`all` | `any` | `quorum`): join behavior when multiple upstream steps feed a step.
- `quorum` (int, >= 1): minimum number of upstreams when `mode=quorum`.
- `timeoutSeconds` (int, >= 0): max time to wait before abandoning a fan-in join.
- `maxEntries` (int, >= 1): cap concurrent fan-in joins held in memory.

### `streaming.partitioning`
- `mode` (`none` | `preserve` | `hash`)
- `key` (string): value used for hashing (e.g., `metadata.user_id`, `payload.session`, `inputs.tenant_id`).
- `partitions` (int, >= 1): number of partitions for hash mode.
- `sticky` (bool): keep existing partition assignment if present.

### `streaming.lifecycle`
- `strategy` (`rolling` | `drain_cutover` | `blue_green`)
- `drainTimeoutSeconds` (int, >= 0)
- `maxInFlight` (int, >= 0): cap in-flight messages during upgrades/reconnects.

### `streaming.observability`
- `metrics.enabled` (bool): enable per-stream metrics.
- `tracing.enabled` (bool)
- `tracing.sampleRate` (0..100)
- `tracing.samplePolicy` (string): `rate`, `always`, or `never`.
- `watermark.enabled` (bool)
- `watermark.timestampSource` (string): e.g. `metadata.event_time_ms`, `metadata.bubu.envelope.timestamp_ms`, `audio.timestamp_ms`.

### `streaming.recording`
- `mode` (`off` | `metadata` | `payload`)
- `sampleRate` (0..100)
- `retentionSeconds` (int, >= 0)
- `redactFields` (list of JSON paths relative to the packet, e.g., `payload.api_key`, `metadata.authorization`)

---

## Defaults and behavior

- `at_least_once` semantics imply `per_stream` ordering if not explicitly set.
- Replay modes automatically enable at-least-once semantics.
- If `replay.mode` is set and `checkpointInterval` is omitted, the hub uses a default checkpoint interval.
- Buffer thresholds are interpreted as utilization percentages (0..100).
- `routing.fanOut=parallel` dispatches downstream engram deliveries concurrently.
- `routing.mode=p2p` is honored only when the Story topology supports direct connections; otherwise the hub is used.
- Routing rules are evaluated per downstream step; deny rules override allow rules.
- If any `allow` rules exist, routing becomes an allowlist (only allowed targets route).
- `lifecycle.drainTimeoutSeconds` is enforced when `strategy` is `drain_cutover` or `blue_green` (and also when a drain timeout is set without an explicit strategy).
- When drain mode is enabled, the hub issues `flowControl.pause` while buffering for reconnect/cutover and resumes once buffers drain.
- `partitioning.mode=hash` uses the configured key to select `partition` in the stream envelope.
- `recording` writes JSON payloads to the configured storage backend; retention enforcement is backend-dependent.
- Lane caps apply per downstream stream buffer and are enforced before global buffer caps.
- Fan-in joins default to `all` (wait for every upstream listed in `step.needs`) unless overridden.
