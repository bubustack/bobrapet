# 🤖 bobrapet Helm Chart

BubuStack's Kubernetes-native workflow controller. Reconciles Stories, StoryRuns, Engrams, Impulses, and transport bindings so AI + data pipelines stay declarative.

## Prerequisites

- Kubernetes **1.30+**
- Helm **3.x**
- [cert-manager](https://cert-manager.io/) installed (for webhook TLS)
- CRDs installed separately via `make install` or the [installer manifest](https://github.com/bubustack/bobrapet/releases)

## Installation

```bash
# Add the BubuStack Helm repo
helm repo add bubustack https://bubustack.github.io/helm-charts
helm repo update

# Install the operator
helm install bobrapet bubustack/bobrapet \
  --namespace bobrapet-system \
  --create-namespace
```

**From local chart (development):**

```bash
helm install bobrapet ./hack/charts/bobrapet \
  --namespace bobrapet-system \
  --create-namespace
```

## Values

### Controller Manager

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `controllerManager.replicas` | int | `1` | Number of operator replicas |
| `controllerManager.manager.image.repository` | string | `ghcr.io/bubustack/bobrapet` | Operator container image |
| `controllerManager.manager.image.tag` | string | `0.1.1` | Image tag |
| `controllerManager.manager.imagePullPolicy` | string | `IfNotPresent` | Image pull policy |
| `controllerManager.manager.resources.requests.cpu` | string | `10m` | CPU request |
| `controllerManager.manager.resources.requests.memory` | string | `64Mi` | Memory request |
| `controllerManager.manager.resources.limits.cpu` | string | `500m` | CPU limit |
| `controllerManager.manager.resources.limits.memory` | string | `128Mi` | Memory limit |
| `controllerManager.nodeSelector` | object | `{}` | Node selector for scheduling |
| `controllerManager.tolerations` | list | `[]` | Tolerations |
| `controllerManager.topologySpreadConstraints` | list | `[]` | Topology spread constraints |

### Service Account

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `serviceAccount.create` | bool | `true` | Create a ServiceAccount |
| `serviceAccount.name` | string | `""` | Override ServiceAccount name |
| `serviceAccount.annotations` | object | `{}` | ServiceAccount annotations (e.g. IRSA) |
| `serviceAccount.automount` | bool | `true` | Automount SA token |

### Services

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `metricsService.type` | string | `ClusterIP` | Metrics service type |
| `metricsService.ports[0].port` | int | `8443` | Metrics port |
| `webhookService.type` | string | `ClusterIP` | Webhook service type |
| `webhookService.ports[0].port` | int | `443` | Webhook port |

### Operator Configuration

The `operatorConfig` section maps directly to the operator's ConfigMap. See [Operator Configuration](https://bubustack.io/docs/operator/configuration) for full documentation.

<details>
<summary>All operator config keys (click to expand)</summary>

| Key | Default | Description |
|-----|---------|-------------|
| **Controller Tuning** | | |
| `controllerMaxConcurrentReconciles` | `10` | Default max concurrent reconciles |
| `controllerRequeueBaseDelay` | `2s` | Base requeue delay |
| `controllerRequeueMaxDelay` | `1m` | Max requeue delay |
| `controllerReconcileTimeout` | `1m` | Reconcile timeout |
| `controllerCleanupInterval` | `1h` | Cleanup interval |
| **Per-Controller Concurrency** | | |
| `steprunMaxConcurrentReconciles` | `15` | StepRun controller concurrency |
| `storyrunMaxConcurrentReconciles` | `8` | StoryRun controller concurrency |
| `storyMaxConcurrentReconciles` | `5` | Story controller concurrency |
| `engramMaxConcurrentReconciles` | `5` | Engram controller concurrency |
| `impulseMaxConcurrentReconciles` | `5` | Impulse controller concurrency |
| `templateMaxConcurrentReconciles` | `2` | Template controller concurrency |
| **StoryRun Execution** | | |
| `storyrunGlobalConcurrency` | `0` | Global StoryRun concurrency limit (0 = unlimited) |
| `storyrunMaxInlineInputsSize` | `1024` | Max inline input bytes before offloading |
| `storyrunRetentionSeconds` | `86400` | StoryRun retention (24h) |
| **Engram Defaults** | | |
| `engramDefaultGrpcPort` | `50051` | Default gRPC port |
| `engramDefaultMaxInlineSize` | `4096` | Max inline payload bytes |
| `engramDefaultDialTimeoutSeconds` | `10` | gRPC dial timeout |
| `engramDefaultGracefulShutdownTimeoutSeconds` | `20` | Graceful shutdown timeout |
| **Transport** | | |
| `controllerTransportGrpcEnableDownstreamTargets` | `true` | Enable downstream transport targets |
| `controllerTransportHeartbeatInterval` | `30s` | Transport heartbeat interval |
| `controllerTransportHeartbeatTimeout` | `2m` | Transport heartbeat timeout |
| **Security** | | |
| `securityRunAsNonRoot` | `true` | Run pods as non-root |
| `securityRunAsUser` | `1000` | Pod UID |
| `securityAutomountServiceAccountToken` | `false` | Automount SA token in workloads |
| `referencesCrossNamespacePolicy` | `deny` | Cross-namespace reference policy |
| **Retry / Timeouts** | | |
| `retryMaxRetries` | `3` | Default max retries |
| `retryExponentialBackoffBase` | `1s` | Exponential backoff base |
| `retryExponentialBackoffMax` | `60s` | Exponential backoff cap |
| `timeoutDefaultStep` | `5m` | Default step timeout |
| `timeoutApprovalDefault` | `24h` | Default gate approval timeout |
| `timeoutExternalDataDefault` | `30m` | External data timeout |
| **Templating** | | |
| `templatingOffloadedDataPolicy` | `inject` | Offloaded data policy (`error`, `inject`, `controller`) |
| `templatingMaterializeEngram` | `materialize` | Materialize engram name |
| `templatingEvaluationTimeout` | `30s` | Template evaluation timeout |

</details>

## Configuration Examples

**Custom image and resources:**

```yaml
controllerManager:
  manager:
    image:
      repository: my-registry/bobrapet
      tag: latest
    resources:
      limits:
        cpu: "2"
        memory: 1Gi
```

**Higher concurrency for large clusters:**

```yaml
operatorConfig:
  steprunMaxConcurrentReconciles: "30"
  storyrunMaxConcurrentReconciles: "15"
  storyrunGlobalConcurrency: "50"
```

**Enable controller-resolve for offloaded data:**

```yaml
operatorConfig:
  templatingOffloadedDataPolicy: controller
```

## Upgrading

```bash
helm repo update
helm upgrade bobrapet bubustack/bobrapet --namespace bobrapet-system
```

> **CRDs are not managed by Helm.** After upgrading, apply the latest CRDs:
> ```bash
> make install  # or kubectl apply -f config/crd/bases/
> ```

## Uninstalling

```bash
helm uninstall bobrapet --namespace bobrapet-system
```

> **CRDs are not removed automatically.** To remove them:
> ```bash
> make uninstall  # or kubectl delete -f config/crd/bases/
> ```

## Links

- [Documentation](https://bubustack.io/docs)
- [Operator Configuration Reference](https://bubustack.io/docs/operator/configuration)
- [Getting Started](https://bubustack.io/docs/getting-started/quickstart)
- [GitHub](https://github.com/bubustack/bobrapet)
