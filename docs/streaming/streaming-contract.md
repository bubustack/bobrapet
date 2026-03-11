# Streaming Contract (Engrams + Connectors)

This document defines the runtime contract for streaming Engrams, transport connectors,
and the hub. It complements the gRPC transport protos in `tractatus/proto/transport/v1/transport.proto`.

## Who this is for

- Engram authors implementing streaming components.
- Connector authors implementing transport drivers.
- Operators debugging real-time pipelines.

## What you'll get

- The message validity rules and required fields.
- Ordering, termination, and error envelope semantics.
- How hubs, connectors, and Engrams coordinate.

## At a glance

- `StreamMessage` is the SDK-facing type used by streaming Engrams.
- The hub/connector data plane uses `DataPacket` + `StreamEnvelope` on gRPC streams.
- Ordering, replay, and flow control are negotiated through transport settings.
- Control directives carry lifecycle signals (pause/resume, handoff states).

## Key types (where to look)

- SDK message contract: `bubu-sdk-go/engram/client.go` (`StreamMessage`)
- Transport gRPC types: `tractatus/proto/transport/v1/transport.proto`
- Hub data plane types: `tractatus/proto/transport/v1/hub.proto`

## 1) Message validity

A streaming message is valid only when at least one of the following is present:

- Audio (`StreamMessage.Audio`)
- Video (`StreamMessage.Video`)
- Binary payload (`StreamMessage.Binary`)
- JSON payload (`StreamMessage.Payload`)
- JSON inputs (`StreamMessage.Inputs`)
- Transport descriptors (`StreamMessage.Transports`)

Metadata-only messages are **invalid**. When publishing, the SDK returns an error if
no audio, video, binary, payload, inputs, or transports are present.

## 2) Error envelope

Streaming errors are conveyed as standard `StreamMessage` payloads:

- `StreamMessage.Kind` must be `error`.
- `StreamMessage.Payload` must contain a JSON-encoded `StructuredError` (v1).
- Error messages **still** must satisfy message validity (payload required).

Error envelopes propagate downstream like any other message. They do not trigger
automatic retries; operators and downstream Engrams should decide how to react.

## 3) Envelope vs raw binary

If a message has `Payload`, `Inputs`, or `Transports`, the SDK wraps it into a transport
**Envelope** and sends it as a Binary frame with MIME type:

- `application/vnd.bubu.packet+json`

If you need to send raw binary data, use `StreamMessage.Binary` and leave `Payload`,
`Inputs`, and `Transports` empty.

## 4) Chunked binary frames

Large binary payloads may be split across multiple frames. Chunking uses
`StreamEnvelope` fields:

- `chunk_id`: unique identifier for the logical message within a stream.
- `chunk_index`: zero-based index of this chunk.
- `chunk_count`: total number of chunks for the message.
- `chunk_bytes`: optional size of this chunk's payload.
- `total_bytes`: optional total size after reassembly.

Rules:

- Only `BinaryFrame` payloads may be chunked.
- `stream_id` and `chunk_id` are required when chunking.
- All chunks for a message must share the same `stream_id`, `partition`, and
  metadata/envelope fields (except the `chunk_*` fields).
- Chunks may arrive out of order; the SDK and hub buffer and reassemble before
  delivering the message to Engrams.
- Invalid chunk metadata causes a publish/subscribe error. If chunks are missing,
  the reassembly entry can expire and the message is dropped.

## 5) Ordering and delivery

- Messages are sent on a single gRPC stream.
- Ordering is preserved **as sent** by the SDK.
- The SDK does not reorder or batch frames unless explicitly configured by the component.
- When `delivery.semantics=at_least_once` and replay is enabled, the hub assigns
  sequence numbers, tracks acknowledgements, and may replay unacked messages after
  reconnects. Downstream Engrams must tolerate duplicates.
- Flow control and acknowledgements travel over the hub `Process` stream via `FlowControl`
  (in `ProcessRequest` / `ProcessResponse`) between connectors and the hub.

## 6) Backpressure and timeouts

- Outgoing messages are buffered in a bounded channel (`BUBU_GRPC_CHANNEL_BUFFER_SIZE`).
- Sends respect `BUBU_GRPC_CHANNEL_SEND_TIMEOUT`. If a send blocks longer than this timeout,
  the publish loop fails and the session is reconnected.
- Incoming reads use `BUBU_GRPC_MESSAGE_TIMEOUT`. The SDK treats prolonged inactivity as
  an error and will reconnect according to the reconnect policy.
- If `BUBU_GRPC_HANG_TIMEOUT` is set, the SDK cancels the session when no control or
  data traffic is observed within the timeout window.

## 7) Termination semantics

- The SDK closes the **input** channel passed to `StreamingEngram.Stream` when the connector
  subscription ends.
- The SDK closes the **output** channel when the Engram returns from `Stream`.
- Engrams must respect context cancellation and return promptly on `ctx.Done()`.

## 8) Control channel

- The control channel carries lifecycle directives and capability negotiation.
- Engrams may optionally implement `ControlDirectiveHandler` to react to directives.
- Heartbeats are emitted by the connector and the SDK monitors liveness based on control
  traffic and configured timeouts.
- The control stream includes a protocol version header via gRPC metadata key
  `bubu-transport-protocol` (current `1.0.0`). Connectors may reject incompatible versions.
- Transport bindings embed `BindingInfo.protocol_version` (current `1.0.0`); connectors validate it at startup.
Handoff directives emitted by connectors:

- `handoff.draining`: connector is draining or losing hub connectivity; engrams should quiesce sends.
- `handoff.cutover`: connector has reconnected and traffic may resume.
- `handoff.ready`: connector is in steady state after handoff (optional).

## 9) Retry and reconnect

- Connector sessions are retried with exponential backoff according to
  `BUBU_GRPC_RECONNECT_BASE_BACKOFF`, `BUBU_GRPC_RECONNECT_MAX_BACKOFF`, and
  `BUBU_GRPC_RECONNECT_MAX_RETRIES`.
- When retries are exhausted, the SDK returns an error and the workload terminates.

## 10) Replay observability

- The hub exports replay checkpoint metrics:
  `bobravoz_hub_replay_last_ack` (last acked sequence) and
  `bobravoz_hub_replay_unacked_current` (current unacked count).

## 11) Lifecycle hooks

The hub emits lifecycle hook packets when streaming steps reach readiness milestones:

- **`steprun.ready`** -- Fired when an individual streaming step connects to the hub.
- **`storyrun.ready`** -- Fired when all streaming steps in the Story have connected.

Steps consume hooks via `if` conditions that match on `packet.type`:

```yaml
if: '{{ eq (default "" packet.type) "storyrun.ready" }}'
```

Hooks are delivered as standard packets with Envelope kind `hook`. Each hook
fires at most once per StoryRun per step combination.

See `/docs/streaming/lifecycle-hooks.md` for the full hook packet structure,
consumption patterns, and debugging tips.

## 12) Transport settings

Streaming backpressure, flow control, and delivery policies are configured through
`Transport.spec.streaming` and `Story.spec.transports[].streaming`. See
`/docs/streaming/transport-settings.md` for the full schema and examples.
