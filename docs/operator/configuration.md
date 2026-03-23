# Operator Configuration

BubuStack controllers read operator configuration from a ConfigMap. The default
installation wires the ConfigMap referenced by the `--config-namespace` and
`--config-name` flags, and the sample values live in
`config/manager/operator-config.yaml`.

## Who this is for

- Platform engineers operating the BubuStack controller.
- Anyone tuning scheduling, limits, or defaults cluster-wide.

## What you'll get

- Where configuration lives and how it is loaded.
- The precedence rules for overrides.
- A complete list of supported config keys and defaults.

Only the keys listed here are consumed by the controller. Unknown keys are
ignored.

---

## Precedence

Manager flags and environment variables are process-level settings. They are
not sourced from the ConfigMap and are not overridden by it.

1. StepRun overrides (when supported for a setting).
2. Story policy overrides (`spec.policy`).
3. Operator ConfigMap defaults.

---

## Defaults When The ConfigMap Is Missing

If the operator ConfigMap does not exist at startup, the controller continues
with built-in defaults from `DefaultOperatorConfig` (which uses
`DefaultControllerConfig`).

---

## Manager Flags And Environment

These settings are read at process start from flags and environment variables.
They are not part of the operator ConfigMap.

| Flag / Env | Default | Purpose |
| --- | --- | --- |
| `--config-namespace` | `bobrapet-system` | Namespace containing the operator ConfigMap. |
| `--config-name` | `bobrapet-operator-config` | Name of the operator ConfigMap. |
| `--metrics-bind-address` | `0` | Metrics endpoint bind address. `0` disables the metrics service. |
| `--metrics-secure` | `true` | Serve metrics over HTTPS with authn/authz when enabled. |
| `--metrics-cert-path` | empty | Directory containing metrics server TLS certs. |
| `--metrics-cert-name` | `tls.crt` | Metrics server TLS certificate filename. |
| `--metrics-cert-key` | `tls.key` | Metrics server TLS key filename. |
| `--webhook-cert-path` | empty | Directory containing webhook TLS certs. |
| `--webhook-cert-name` | `tls.crt` | Webhook TLS certificate filename. |
| `--webhook-cert-key` | `tls.key` | Webhook TLS key filename. |
| `--health-probe-bind-address` | `:8081` | Liveness/readiness probe bind address. |
| `--leader-elect` | `false` | Enable leader election for the controller manager. |
| `--leader-election-id` | `d3a8b358.bubustack.io` | Leader election ID to prevent clashes. |
| `--leader-election-namespace` | empty | Namespace for leader election resources. Defaults to the in-cluster namespace and fails out of cluster if unset. |
| `--enable-http2` | `false` | Enable HTTP/2 for metrics and webhook servers. |
| `--tracing-init-timeout` | `10s` | Timeout for OTLP tracer initialization. |
| `--tracing-shutdown-timeout` | `5s` | Timeout for OTLP tracer shutdown. |
| `ENABLE_WEBHOOKS` | `true` | When set to `false`, disables admission webhooks. |

---

## Reload behavior

The operator watches its ConfigMap and applies updates at runtime. Settings
that only affect controller wiring (for example, worker counts, rate limiter
options) and templating evaluator construction are captured at startup and
require a restart to take effect. Other values that are resolved during
reconcile may update live.

---

## Scheduling Controls

`spec.policy.queue` assigns StoryRuns to a scheduling queue. Queue names must be
DNS-1123 labels and are lowercased at runtime.

`spec.policy.priority` defines ordering within the queue. Higher values run
first. Ordering is strict within the queue and enforced without preemption.
Priority aging can be enabled per queue to prevent starvation.

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `storyrun.global-concurrency` | `0` | Caps total running StepRuns across all queues. | Provides a safety valve for cluster load. |
| `storyrun.queue.<name>.concurrency` | `0` | Caps running StepRuns within a queue. | Prevents one workload class from starving others. |
| `storyrun.queue.<name>.default-priority` | `0` | Priority used when `spec.policy.priority` is unset. | Keeps ordering deterministic without forcing per-story config. |
| `storyrun.queue.<name>.priority-aging-seconds` | `60` | Adds effective priority based on queued time. | Prevents starvation under strict priority ordering. |

---

## Global Controller Behavior

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `controller.max-concurrent-reconciles` | `10` | Global reconcile worker cap (fallback for per-controller zeros). | Prevents runaway reconcile fan-out. |
| `controller.requeue-base-delay` | `0` (uses per-controller defaults) | Base delay for exponential requeue backoff. | Avoids hot loops during transient failures. |
| `controller.requeue-max-delay` | `0` (uses per-controller defaults) | Maximum requeue backoff delay. | Keeps retries bounded in time. |
| `controller.cleanup-interval` | `1h` | Interval for background cleanup loops. | Prevents GC from running too often. |
| `controller.reconcile-timeout` | `30s` | Deadline for a single reconcile loop (`0` disables the deadline). | Guards against stuck reconciles. |
| `controller.max-story-with-block-size-bytes` | `65536` | Upper bound for Story `with` block size. | Protects etcd and API server memory. |

---

## Images And Resources

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `images.default-engram` | empty | Default Engram image when not specified elsewhere. | Provides a cluster-wide baseline image. |
| `images.default-impulse` | empty | Default Impulse image when not specified elsewhere. | Provides a cluster-wide baseline image. |
| `images.pull-policy` | `IfNotPresent` | Image pull policy for workloads. | Controls cache vs freshness. |
| `resources.default.cpu-request` | `100m` | Default CPU request for workloads. | Ensures fair scheduling. |
| `resources.default.cpu-limit` | `500m` | Default CPU limit for workloads. | Prevents noisy neighbors. |
| `resources.default.memory-request` | `128Mi` | Default memory request. | Ensures fair scheduling. |
| `resources.default.memory-limit` | `512Mi` | Default memory limit. | Prevents memory exhaustion. |
| `resources.engram.cpu-request` | empty (inherits `resources.default.cpu-request`) | Engram-specific CPU request override. | Tailors runtime costs for Engrams. |
| `resources.engram.cpu-limit` | empty (inherits `resources.default.cpu-limit`) | Engram-specific CPU limit override. | Prevents a single Engram from saturating nodes. |
| `resources.engram.memory-request` | empty (inherits `resources.default.memory-request`) | Engram-specific memory request override. | Reserves memory for Engrams. |
| `resources.engram.memory-limit` | empty (inherits `resources.default.memory-limit`) | Engram-specific memory limit override. | Prevents OOM cascades. |

---

## Retry And Timeouts

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `retry.max-retries` | `3` | Default retry limit for StepRuns. | Balances resilience vs load. |
| `timeout.default-step` | `30m` | Default step timeout when not specified. | Prevents infinite execution. |
| `timeout.approval-default` | inherits `timeout.default-step` | Default approval timeout for gate steps. | Avoids stale approval waits. |
| `timeout.external-data-default` | inherits `timeout.default-step` | Timeout for external data access. | Bounds waiting on external systems. |
| `timeout.conditional-default` | inherits `timeout.default-step` | Timeout for conditional evaluation. | Prevents stuck conditional loops. |

---

## Security Defaults

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `security.run-as-non-root` | `false` | Forces workloads to run as non-root. | Reduces privilege risk. |
| `security.read-only-root-filesystem` | `false` | Mounts root filesystem as read-only. | Limits write surface in containers. |
| `security.allow-privilege-escalation` | `false` | Disables privilege escalation. | Blocks common container escapes. |
| `security.drop-capabilities` | `ALL` | Linux capabilities to drop. | Minimizes kernel attack surface. |
| `security.run-as-user` | `0` | Default UID for workloads. | Allows a cluster-wide UID baseline (0 means root). |
| `security.automount-service-account-token` | `false` | Default SA token mount toggle. | Reduces token exposure. |
| `security.service-account-name` | `default` | Default ServiceAccount name. | Ensures predictable identity. |

---

## Job And Retention

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `job.backoff-limit` | `3` | Job retry limit for failed Pods. | Avoids infinite Job retries. |
| `job.ttl-seconds-after-finished` | `3600` | TTL for completed Jobs. | Cleans up finished Pods. |
| `streaming.ttl-seconds-after-finished` | `0` | TTL for streaming workloads. | Keeps long-lived runs unless configured. |
| `job.restart-policy` | `Never` | Restart policy for job Pods. | Matches batch semantics. |
| `storyrun.retention-seconds` | `86400` | Retention for StoryRun objects. | Controls history retention vs etcd load. |

---

## Loop

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `loop.max-iterations` | `10000` | Maximum iterations for a loop step. | Prevents runaway loops from consuming unbounded resources. |
| `loop.default-batch-size` | `100` | Default batch size for loop execution. | Balances throughput vs memory usage. |
| `loop.max-batch-size` | `1000` | Hard cap on batch size. | Prevents oversized batches from overwhelming the controller. |
| `loop.max-concurrency` | `10` | Default max concurrent loop iterations. | Limits parallel execution per loop step. |
| `loop.max-concurrency-limit` | `50` | Hard cap on loop concurrency. | Prevents a single loop from starving other workloads. |

---

## Templating

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `templating.evaluation-timeout` | `30s` | Timeout for template evaluation. | Prevents slow templates from blocking reconciliation. |
| `templating.max-output-bytes` | `65536` | Maximum evaluated output size. | Avoids large payload explosions. |
| `templating.deterministic` | `false` | Restricts non-deterministic helpers. | Improves replay safety. |
| `templating.offloaded-data-policy` | `inject` | How to handle templates that reference offloaded data: `error` (fail), `inject` (pod-based materialization for pipe expressions), `controller` (in-process hydration from S3, no extra pod). | Controls correctness vs convenience vs latency. |
| `templating.materialize-engram` | `materialize` | Engram used to materialize offloaded refs. Required only when policy is `inject`. | Centralizes hydration behavior. |

---

## Reference, Telemetry, Debug

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `references.cross-namespace-policy` | `deny` | Cross-namespace reference policy. | Enforces tenant boundaries. |
| `telemetry.enabled` | `false` | Tracing toggle. | Allows controlled overhead. |
| `telemetry.trace-propagation` | `false` | Controls trace propagation. | Keeps distributed traces consistent. |
| `debug.enable-verbose-logging` | `false` | Increases log verbosity. | Useful for diagnostics. |
| `debug.enable-step-output-logging` | `false` | Logs step outputs. | Debugging with caution for sensitive data. |
| `debug.enable-metrics` | `false` | Metrics collection toggle. | Allows runtime visibility. |

**Note:** `debug.enable-metrics` controls metric emission in the operator. The metrics *endpoint* is controlled by the manager flags (`--metrics-bind-address`, `--metrics-secure`) in `bobrapet/cmd/main.go`.

### OpenTelemetry export

The operator exports traces using the OTLP gRPC exporter. Configure the exporter
via standard OTEL environment variables (set on the controller manager pod):

- `OTEL_EXPORTER_OTLP_ENDPOINT` (e.g., `otel-collector:4317`)
- `OTEL_TRACES_EXPORTER` (`otlp` or `none`)
- `OTEL_SERVICE_NAME` (overrides default service name)
- `OTEL_RESOURCE_ATTRIBUTES` (additional resource tags)

Example:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=otel-collector.observability:4317
export OTEL_TRACES_EXPORTER=otlp
export OTEL_SERVICE_NAME=bobrapet-operator
```

---

## StoryRun Controller

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `storyrun.max-concurrent-reconciles` | `8` | Worker count for StoryRun reconciles (`0` uses controller default). | Prevents controller overload. |
| `storyrun.rate-limiter.base-delay` | `50ms` | Base delay for StoryRun backoff. | Avoids hot loops. |
| `storyrun.rate-limiter.max-delay` | `5m` | Max delay for StoryRun backoff. | Keeps retries bounded. |
| `storyrun.max-inline-inputs-size` | `1024` | Max inline `spec.inputs` size in bytes. | Prevents etcd bloat. |
| `storyrun.binding.max-mutations-per-reconcile` | `8` | TransportBinding mutation budget. | Limits per-reconcile work. |
| `storyrun.binding.throttle-requeue-delay` | `2s` | Delay when mutation budget is hit. | Applies backpressure. |

---

## StepRun Controller

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `steprun.max-concurrent-reconciles` | `15` | Worker count for StepRun reconciles (`0` uses controller default). | Prevents overload under high fan-out. |
| `steprun.rate-limiter.base-delay` | `100ms` | Base delay for StepRun backoff. | Avoids hot loops. |
| `steprun.rate-limiter.max-delay` | `2m` | Max delay for StepRun backoff. | Keeps retries bounded. |

---

## Story Controller

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `story.max-concurrent-reconciles` | `5` | Worker count for Story reconciles (`0` uses controller default). | Protects API server from bursty updates. |
| `story.rate-limiter.base-delay` | `200ms` | Base delay for Story backoff. | Avoids hot loops. |
| `story.rate-limiter.max-delay` | `1m` | Max delay for Story backoff. | Keeps retries bounded. |
| `story.binding.max-mutations-per-reconcile` | `4` | TransportBinding mutation budget. | Limits per-reconcile work. |
| `story.binding.throttle-requeue-delay` | `3s` | Delay when mutation budget is hit. | Applies backpressure. |

---

## Engram Controller

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `engram.max-concurrent-reconciles` | `5` | Worker count for Engram reconciles (`0` uses controller default). | Protects API server from bursty updates. |
| `engram.rate-limiter.base-delay` | `200ms` | Base delay for Engram backoff. | Avoids hot loops. |
| `engram.rate-limiter.max-delay` | `1m` | Max delay for Engram backoff. | Keeps retries bounded. |
| `engram.default-max-inline-size` | `4096` | Default inline size for Engram IO. | Triggers offloading when exceeded. |
| `engram.default-grpc-port` | `50051` | Default gRPC port. | Ensures consistent connectivity. |
| `engram.default-grpc-heartbeat-interval-seconds` | `10` | Default gRPC heartbeat interval. | Detects disconnected clients. |
| `engram.default-storage-timeout-seconds` | `300` | Default storage timeout. | Bounds remote storage calls. |
| `engram.default-graceful-shutdown-timeout-seconds` | `20` | Default graceful shutdown timeout. | Allows orderly shutdown. |
| `engram.default-termination-grace-period-seconds` | `30` | Pod termination grace period. | Allows cleanup on shutdown. |
| `engram.default-max-recv-msg-bytes` | `10485760` | Max gRPC receive size. | Prevents oversized messages. |
| `engram.default-max-send-msg-bytes` | `10485760` | Max gRPC send size. | Prevents oversized messages. |
| `engram.default-dial-timeout-seconds` | `10` | Dial timeout for gRPC. | Avoids hanging connects. |
| `engram.default-channel-buffer-size` | `16` | Channel buffer size. | Bounds memory usage. |
| `engram.default-reconnect-max-retries` | `10` | Max gRPC reconnect retries. | Prevents infinite reconnect loops. |
| `engram.default-reconnect-base-backoff-millis` | `500` | Base backoff for reconnect. | Spreads reconnection attempts. |
| `engram.default-reconnect-max-backoff-seconds` | `30` | Max backoff for reconnect. | Bounds wait time. |
| `engram.default-hang-timeout-seconds` | `0` | Hang detection timeout. | Surfaces stalled connections. |
| `engram.default-message-timeout-seconds` | `30` | Message timeout for gRPC calls. | Prevents stuck calls. |

---

## Storage Defaults

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `controller.storage.provider` | empty | Default storage backend. | Ensures a known storage target. |
| `controller.storage.s3.bucket` | empty | Default S3 bucket name. | Centralizes storage location. |
| `controller.storage.s3.region` | empty | Default S3 region. | Required for AWS-compatible SDKs. |
| `controller.storage.s3.endpoint` | empty | S3 endpoint override. | Supports non-AWS S3 backends. |
| `controller.storage.s3.use-path-style` | `false` | Path-style addressing toggle. | Required by some S3-compatible stores. |
| `controller.storage.s3.auth-secret-name` | empty | Secret with S3 credentials. | Centralizes credential lookup. |
| `controller.storage.file.path` | empty | Default file storage path inside workload. | Required for file-backed storage. |
| `controller.storage.file.volume-claim-name` | empty | Default RWX PVC name for file storage. | Enables shared file storage across workloads. |

---

## Transport Controller

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `controller.transport.grpc.enable-downstream-targets` | `true` | Inject downstream targets in batch mode. | Keeps streaming topology consistent. |
| `controller.transport.grpc.default-tls-secret` | empty | Default TLS Secret for gRPC. | Allows centralized TLS config. |
| `controller.transport.heartbeat-interval` | `30s` | Transport heartbeat interval. | Detects missing data plane agents. |
| `controller.transport.heartbeat-timeout` | `2m` | Transport heartbeat timeout. | Marks bindings stale when missed. |

---

## Impulse Controller

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `impulse.max-concurrent-reconciles` | `5` | Worker count for Impulse reconciles (`0` uses controller default). | Protects API server from bursty triggers. |
| `impulse.rate-limiter.base-delay` | `200ms` | Base delay for Impulse backoff. | Avoids hot loops. |
| `impulse.rate-limiter.max-delay` | `1m` | Max delay for Impulse backoff. | Keeps retries bounded. |

---

## Template Controller

| Key | Default | Purpose | Why it exists |
| --- | --- | --- | --- |
| `template.max-concurrent-reconciles` | `2` | Worker count for Template reconciles (`0` uses controller default). | Templates change rarely. |
| `template.rate-limiter.base-delay` | `500ms` | Base delay for Template backoff. | Avoids hot loops. |
| `template.rate-limiter.max-delay` | `10m` | Max delay for Template backoff. | Keeps retries bounded. |
