# Observability and Debugging

This guide explains how BubuStack exposes traces, metrics, logs, and run
introspection to help you debug workflows and operate them in production.
It covers both platform-level capabilities and user-facing workflows.

---

## Why observability matters

BubuStack runs multi-step workflows across controllers, jobs, and streaming
runtimes. Observability answers three questions:

- **What happened?** (logs, events, run timelines)
- **Why did it happen?** (errors, inputs/outputs, retries)
- **Where did it happen?** (traces, correlation IDs, streaming lanes)

---

## Tracing (OpenTelemetry)

BubuStack emits OpenTelemetry spans from core controllers and streaming
components. Trace context can be propagated end-to-end, and trace IDs are
recorded on runs to make UI/CLI lookup easy.

**What is traced**

- Controller reconciliation for StoryRuns and StepRuns.
- Storage hydration/offload operations.
- Streaming hub and connector pipelines.

**Where trace IDs live**

- `StoryRun.status.trace` (trace ID + span ID)
- `StepRun.status.trace` (trace ID + span ID)

**Operator toggles**

- `telemetry.enabled`: enables span emission.
- `telemetry.trace-propagation`: injects/extracts trace context for downstream systems.

These toggles are applied at runtime. Enabling telemetry will initialize the
OTLP exporter; disabling telemetry will flush and shut it down. Changing OTEL
exporter environment variables still requires a restart.

**Exporter configuration (OTLP)**

BubuStack uses the OTLP gRPC exporter. Configure via standard OTEL env vars:

- `OTEL_EXPORTER_OTLP_ENDPOINT` (e.g., `otel-collector:4317`)
- `OTEL_TRACES_EXPORTER` (`otlp` or `none`)
- `OTEL_SERVICE_NAME` (override service name)
- `OTEL_RESOURCE_ATTRIBUTES` (extra resource tags)

Exporter initialization and shutdown timeouts can be tuned via manager flags:

- `--tracing-init-timeout` (default `10s`)
- `--tracing-shutdown-timeout` (default `5s`)

Example:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=otel-collector.observability:4317
export OTEL_TRACES_EXPORTER=otlp
export OTEL_SERVICE_NAME=bobrapet-operator
```

**Streaming traces**

For real-time flows, tracing and sampling are configurable per transport using
`streaming.observability.tracing` (see `docs/streaming/transport-settings.md`).

---

## Metrics and SLOs

BubuStack exports controller and runtime metrics when enabled. These metrics
can be used to build SLO dashboards for:

- Success rate
- Latency (p50/p95)
- Queue depth
- Step volume and retry pressure

**Operator toggle**

- `debug.enable-metrics`: enables metric collection.

**Note:** The metrics *endpoint* is controlled by the manager flags (`--metrics-bind-address`, `--metrics-secure`) in `bobrapet/cmd/main.go`. Disabling `debug.enable-metrics` stops metric emission, but does not disable the endpoint by itself.

**Bubuilder SLO snapshot**

The Bubuilder UI provides an SLO snapshot derived from recent StoryRuns.
This is a quick-look summary and should be complemented by Prometheus
or your preferred telemetry backend for long-term trends.

## Streaming metrics (hub + connector)

When metrics are enabled, the streaming hub and connector expose Prometheus
metrics that help you understand throughput, buffering, drops, and replay.
Common signals include:

- Hub buffering: `bobravoz_hub_buffer_current_size`, `bobravoz_hub_buffer_current_bytes`
- Hub drops/flushes: `bobravoz_hub_messages_dropped_total`, `bobravoz_hub_buffer_flushed_total`
- Event-time observability: `bobravoz_hub_event_time_watermark_seconds`, `bobravoz_hub_event_time_lag_seconds`
- Replay state: `bobravoz_hub_replay_last_ack`, `bobravoz_hub_replay_unacked_current`
- Recording outcomes: `bobravoz_hub_stream_recordings_total`
- Connector drops and control flow: `bobravoz_connector_downstream_drops_total`, `bobravoz_connector_control_directives_total`
- Connector capability reporting: `bobravoz_connector_capability_changes_total`, `bobravoz_connector_capability_field_events_total`

Most hub metrics are labeled by `storyrun` and `step` to support per-pipeline dashboards.

---

## Logs and outputs

**Step logs**

- Primary source: SDK/offloaded logs (storage-backed)
- Fallback: Kubernetes pod logs

**Outputs**

- StoryRun output: `StoryRun.status.output`
- StepRun output: `StepRun.status.output`
- Large payloads can be offloaded and hydrated via storage references.
  Hydration requires a configured storage backend.

**Redaction controls**

Bubuilder can apply redaction to logs and outputs to avoid leaking secrets.
Redaction is best-effort and should be combined with secure logging practices.

---

## Debugging workflows

**Bubuilder UI**

- Run timeline and DAG visualization
- Per-step logs, inputs, and outputs (hydrated when storage is configured)
- Trace viewer (via trace ID links)
- Rerun-from-step and retry flows
- SLO dashboard snapshot
- Observability page (`/observability`) for trace lookup and SLO summaries

**Programmatic controls**

- Step retry via Bubuilder API (or by patching `StepRun.status.nextRetryAt` if you
  need a low-level/manual workflow).
- Rerun-from-step by setting `storyrun.bubustack.io/redrive-from-step` on a StoryRun.

---

## Correlation IDs

BubuStack propagates correlation IDs via labels/annotations so you can search
for related runs across systems. Bubuilder supports filtering runs by
correlation ID and labels.

---

## Security notes

- Avoid logging secrets in step output unless strictly required.
- Prefer redaction and offloading for sensitive payloads.
- Use dedicated service accounts with least privilege.
