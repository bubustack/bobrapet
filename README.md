# 🤖 bobrapet — A Declarative, Kubernetes-Native AI Workflow Engine
[![Go Reference](https://pkg.go.dev/badge/github.com/bubustack/bobrapet.svg)](https://pkg.go.dev/github.com/bubustack/bobrapet)
[![Go Report Card](https://goreportcard.com/badge/github.com/bubustack/bobrapet)](https://goreportcard.com/report/github.com/bubustack/bobrapet)
[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/bubustack)](https://artifacthub.io/packages/search?repo=bubustack)

Bobrapet is a powerful, cloud-native workflow engine for orchestrating complex AI and data processing pipelines on Kubernetes. It leverages the declarative power of Custom Resource Definitions (CRDs) to let you define, manage, and execute multi-step, event-driven workflows with flexibility and control.

## 🔗 Quick Links

- Operator docs: https://bubustack.io/docs/overview/core
- Quickstart: https://bubustack.io/docs/getting-started/quickstart
- CRD reference: https://bubustack.io/docs/api/crd-design
- API errors contract: https://bubustack.io/docs/api/errors
- Artifact Hub chart: https://artifacthub.io/packages/search?repo=bubustack&ts_query_web=bobrapet

## 🌟 Key Features

- **Declarative & GitOps-Friendly**: Define your entire workflow as a `Story` resource. Treat your AI pipelines as code and manage them through GitOps.
- **Advanced DAG Orchestration**: A Directed Acyclic Graph (DAG) engine orchestrates steps based on `needs`. Step order in YAML is ignored.
- **Parallel-by-Default Execution**: Independent steps run in parallel automatically; only `needs` creates ordering.
- **Dynamic Control Flow**: Use template-based conditions and dedicated engrams for filtering/transforming large datasets.
- **Composable & Reusable Components**: Package reusable tasks as `Engrams`, backed by versioned, cluster-scoped `EngramTemplates` for consistency.
- **Flexible Streaming Strategies**: Optimize for long-running tasks with `PerStory` (always-on) or `PerStoryRun` (on-demand) resource allocation.
- **Cross-Namespace Orchestration**: Securely reference resources and orchestrate workflows across multiple Kubernetes namespaces.
- **Event-Driven Triggers**: Initiate workflows from various event sources using `Impulses` that submit durable `StoryTrigger` requests.
- **Storage-Aware Payloads**: Large payloads can be offloaded to shared storage. The SDK and controllers both resolve storage references, and the `StoryTrigger` → `StoryRun` path offloads oversized accepted inputs when shared storage is configured.

## 🏗️ Architecture

High-level architecture, patterns, and controller internals are documented on the website:
- Overview and architecture: https://bubustack.io/docs/overview/architecture

## 📚 Core Concepts

- **`Story`**: The top-level definition of a workflow, composed of steps.
- **`Engram`**: A configured, runnable instance of a component (a "worker").
- **`Impulse`**: A trigger that submits durable `StoryTrigger` requests based on external events.
- **`StoryTrigger`**: The durable admission request for external trigger delivery.
- **`StoryRun`**: An instance of an executing `Story`.
- **`StepRun`**: An instance of a single `engram` step executing within a `StoryRun`.
- **`EffectClaim`**: The durable reservation/completion authority for one step side effect.
- **`EngramTemplate` & `ImpulseTemplate`**: Reusable, cluster-scoped definitions for `Engrams` and `Impulses`.

## 🧰 Workflow Primitives

See the guides for primitives, batch vs. streaming, impulses, and storage configuration:
- Runtime docs: https://bubustack.io/docs/runtime/primitives

Currently supported primitives:
- `condition`, `parallel`, `sleep`, `stop`, `executeStory`, `wait`, `gate`

`wait` and `gate` are **batch-only** primitives and are rejected in streaming Stories.

Example (gate + wait):
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
  - name: wait-ready
    type: wait
    needs: [approve]
    with:
      until: "inputs.ready == true"
      pollInterval: "5s"
```

To approve a gate, update the StoryRun status:
```bash
kubectl patch storyrun <name> --type merge --subresource status \\
  -p '{"status":{"gates":{"approve":{"state":"Approved","message":"ok"}}}}'
```

## 🚀 Quick Start

For fresh cluster bootstrap, cert-manager, storage, and a first workflow, use
the website quickstart:

- https://bubustack.io/docs/getting-started/quickstart
- https://bubustack.io/docs/getting-started/prerequisites

Install `bobrapet` from the Helm repo:

```bash
helm repo add bubustack https://bubustack.github.io/helm-charts
helm repo update
helm install bobrapet bubustack/bobrapet \
  --namespace bobrapet-system \
  --create-namespace
```

### Realtime add-on

Install `bobravoz-grpc` only if you want realtime Stories or the LiveKit
examples:
```bash
helm install bobravoz-grpc bubustack/bobravoz-grpc \
  --namespace bobrapet-system
```

If `bobrapet` is installed with a non-default Helm release name, install
`bobravoz-grpc` with:

```bash
helm install bobravoz-grpc bubustack/bobravoz-grpc \
  --namespace bobrapet-system \
  --set sharedCAIssuerName=<bobrapet-release>-bobrapet-shared-ca
```

## ⚙️ Environment variables (operator-injected; consumed by SDK)

For complete environment variable listings and defaults, see the operator configuration and transport reference:
- Operator config: https://bubustack.io/docs/operator/configuration
- gRPC transport: https://bubustack.io/docs/streaming/transport-settings

### Debug logging for Engrams, Impulses, and connectors

Set the new `debug` override whenever you need verbose runtime logs from an Engram or Impulse. The controller injects the `BUBU_DEBUG` environment variable so every SDK-based component (batch jobs, streaming deployments, impulses, and transport connectors) can emit additional diagnostics about inputs, outputs, bindings, and connector traffic.

```yaml
spec:
  overrides:
    debug: true        # Engram override

# or

spec:
  execution:
    debug: true        # Impulse override
```

The override flows through Story/Step policies and StepRuns as well. When enabled you will see truncated previews of inputs/outputs in pod logs, plus connector session traces for transport debugging.

## Controller tuning & metrics

- **Binding fan-out controls** – The operator ConfigMap now exposes `story.binding.*` and `storyrun.binding.*` keys (e.g. `story.binding.max-mutations-per-reconcile`, `storyrun.binding.throttle-requeue-delay`). Use these to limit how many `TransportBinding` objects a reconcile loop may mutate and to configure the backoff before the controller retries.
- **Transport binding telemetry** – Two Prometheus series (`bobrapet_transport_binding_operations_total` and `bobrapet_transport_binding_operation_duration_seconds`) record Story/StoryRun binding churn, labeled by controller and whether the binding actually mutated. These metrics complement the existing reconcile counters so SREs can see when connector negotiations are hot-looping.
- **Status surfacing** – `Transport.Status.LastHeartbeatTime`, `AvailableAudio/Video/Binary`, and the pending/failed binding counters now reflect connector feedback, so dashboards can alert if a driver stops publishing capabilities even though specs look valid.
- **Fine-grained streaming reconciles** – Child Deployments/Services raise story-scoped events annotated with `bubustack.io/story` and `bubustack.io/step`, so the Story controller can requeue only the impacted `story::step` instead of replaying every streaming workload.

## 🛠️ Local Development

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/bubustack/bobrapet.git
    cd bobrapet
    ```

2.  **Run the controller locally:**
    This command runs the operator on your machine, using your local `kubeconfig` to communicate with the cluster. This is great for rapid development and debugging.
    ```bash
    make run
    ```

3.  **Run tests:**
    ```bash
    make test
    ```

4.  **End-to-end tests (Kind optional):**
    ```bash
    make test-e2e
    ```

5.  **Generate the Helm chart (outputs to `dist/charts/`):**
    ```bash
    make helm-chart
    # or specify another name: make helm-chart CHART=mychart
    ```
    Chart overlays such as `Chart.yaml` and `README.md` live under
    `hack/charts/<chart>/`. Generated templates, values, and CRDs are written
    to `dist/charts/<chart>/`.

## 📢 Support, Security, and Changelog

- See `SUPPORT.md` for how to get help and report issues.
- See `SECURITY.md` for vulnerability reporting and security posture.
- See `CHANGELOG.md` for version history.

## 🤝 Community

- Code of Conduct: see [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) (Contributor Covenant v3.0)
- Discord: https://discord.gg/dysrB7D8H6

## 📄 License

Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
