# 🤖 bobrapet - A Declarative, Kubernetes-Native AI Workflow Engine
[![Go Reference](https://pkg.go.dev/badge/github.com/bubustack/bobrapet.svg)](https://pkg.go.dev/github.com/bubustack/bobrapet)
[![Go Report Card](https://goreportcard.com/badge/github.com/bubustack/bobrapet)](https://goreportcard.com/report/github.com/bubustack/bobrapet)

Bobrapet is a powerful, cloud-native workflow engine for orchestrating complex AI and data processing pipelines on Kubernetes. It leverages the declarative power of Custom Resource Definitions (CRDs) to let you define, manage, and execute multi-step, event-driven workflows with unparalleled flexibility and control.

For full product docs, visit: https://bubustack.io/docs/

## 🌟 Key Features

- **Declarative & GitOps-Friendly**: Define your entire workflow as a `Story` resource. Treat your AI pipelines as code and manage them through GitOps.
- **Advanced DAG Orchestration**: A sophisticated Directed Acyclic Graph (DAG) engine orchestrates your steps, enabling complex dependencies and execution flows.
- **Parallel-by-Default Execution**: For maximum performance, independent steps run in parallel automatically. Use the `needs` keyword to define an explicit execution order.
- **Dynamic Control Flow**: Implement powerful logic directly in your workflows with built-in primitives and Common Expression Language (CEL) for `if` conditions.
- **Composable & Reusable Components**: Package reusable tasks as `Engrams`, backed by versioned, cluster-scoped `EngramTemplates` for consistency.
- **Flexible Streaming Strategies**: Optimize for long-running tasks with `PerStory` (always-on) or `PerStoryRun` (on-demand) resource allocation.
- **Cross-Namespace Orchestration**: Securely reference resources and orchestrate workflows across multiple Kubernetes namespaces.
- **Event-Driven Triggers**: Initiate workflows from various event sources using `Impulses`.

## 🏗️ Architecture

The `bobrapet` operator is engineered for robustness and maintainability, following best practices for Kubernetes controller design. The core `StoryRun` controller, for example, is built on a modular, sub-reconciler pattern:

- **Main Controller**: Acts as a lean, high-level orchestrator.
- **RBAC Manager**: Manages all RBAC-related resources (`ServiceAccount`, `Role`, `RoleBinding`).
- **DAG Reconciler**: Contains the entire workflow state machine, handling state synchronization, dependency analysis, and scheduling.
- **Step Executor**: Manages the specific logic for launching different types of steps (`engram`, `executeStory`, etc.).

This clean separation of concerns makes the operator highly scalable, testable, and easy to extend.

## 📚 Core Concepts

- **`Story`**: The top-level definition of a workflow, composed of steps.
- **`Engram`**: A configured, runnable instance of a component (a "worker").
- **`Impulse`**: A trigger that creates workflow instances based on external events.
- **`StoryRun`**: An instance of an executing `Story`.
- **`StepRun`**: An instance of a single `engram` step executing within a `StoryRun`.
- **`EngramTemplate` & `ImpulseTemplate`**: Reusable, cluster-scoped definitions for `Engrams` and `Impulses`.

## 🧰 Workflow Primitives

Beyond running custom `Engrams`, `Story` resources can use a rich set of built-in primitives for advanced control flow:

- **`loop`**: Iterate over a list and expand a template step per item.
  - `with.items`: CEL‑resolvable data (evaluated with `inputs`, `steps` contexts)
  - `with.template`: a single `Step` to instantiate per item
  - Limits: max 100 iterations; creates child `StepRun`s and records them under `status.primitiveChildren[step]`; marks the loop step Running ("Loop expanded").

- **`parallel`**: Run multiple steps concurrently.
  - `with.steps[]`: array of `Step` entries; each branch’s `with` is CEL‑resolved with `inputs` and `steps`
  - Creates sibling `StepRun`s; marks the parallel step Running ("Parallel block expanded").

- **`stop`**: Terminate the workflow early.
  - `with.phase`: one of `Succeeded|Failed|Canceled` (defaults to `Succeeded`)
  - `with.message`: optional human message
  - Sets `StoryRun.status.phase/message` and returns.

- **`executeStory`**: Run another `Story` as a sub‑workflow.
  - `with.storyRef`: `{ name, namespace? }`
  - Current status: placeholder; marks step Succeeded with a message.

- **`condition`, `switch`, `setData`, `transform`, `filter`, `mergeData`**:
  - Batch path: controller marks these primitives Succeeded with outputs available (no pod launch).
  - Evidence: batch primitive completion (internal/controller/runs/step_executor.go:49-51)
  - Streaming path: `transform` is evaluated in the Hub (CEL over payload/inputs) and forwarded downstream.

- API declares additional types (`wait`, `throttle`, `batch`, `gate`) for future use.

## 🚀 Quick Start

### Prerequisites
- A running Kubernetes cluster (e.g., KinD, Minikube).
- `kubectl` configured to access your cluster.

### 1. Install the Operator

First, install the Custom Resource Definitions (CRDs):
```bash
make install
```

Next, deploy the operator controller to your cluster:
```bash
make deploy IMG=<your-repo>/bobrapet:latest
```
*(Replace `<your-repo>` with your container registry)*

### 2. Deploy a sample workflow

The following example defines a two-step workflow that fetches content from a URL and uses an AI model to summarize it. Notice how the `summarize` step implicitly depends on the output of the `fetch-content` step.

Apply the sample manifests, which include the necessary `EngramTemplates`, `Engrams`, and the `Story` definition:
```bash
kubectl apply -k config/samples
```

This creates:
- An `Engram` named `http-request-engram` to fetch web content.
- An `Engram` named `openai-summarizer-engram` to summarize text.
- A `Story` named `summarize-website-story` that defines the workflow.

### 3. Run the workflow

Create a `StoryRun` resource to trigger the workflow. This `StoryRun` provides the initial input `url` required by the `Story`.

```yaml
apiVersion: runs.bubustack.io/v1alpha1
kind: StoryRun
metadata:
  name: summarize-k8s-docs
spec:
  storyRef:
    name: summarize-website-story
  inputs:
    url: https://kubernetes.io/docs/concepts/overview/
```

### 4. Observe the results

Monitor the execution of the workflow by checking the `StoryRun` and its child `StepRuns`.

```bash
# Check the overall status of the workflow
kubectl get storyrun summarize-k8s-docs -o yaml

# Check the status of individual steps
kubectl get stepruns -l bubustack.io/storyrun=summarize-k8s-docs
```

## Environment variables (operator-injected; consumed by SDK)

- Identity: `BUBU_STORY_NAME`, `BUBU_STORYRUN_ID`, `BUBU_STEP_NAME`, `BUBU_STEPRUN_NAME`, `BUBU_STEPRUN_NAMESPACE`, `BUBU_STARTED_AT`
- Inputs/Config: `BUBU_INPUTS`, `BUBU_CONFIG`, `BUBU_EXECUTION_MODE`
- Storage: `BUBU_MAX_INLINE_SIZE`, `BUBU_STORAGE_PROVIDER`, `BUBU_STORAGE_TIMEOUT`, `BUBU_STORAGE_S3_BUCKET`, `BUBU_STORAGE_S3_REGION`, `BUBU_STORAGE_S3_ENDPOINT`
- gRPC (server/client): `BUBU_GRPC_PORT`, `BUBU_GRPC_MAX_RECV_BYTES`, `BUBU_GRPC_MAX_SEND_BYTES`, `BUBU_GRPC_CLIENT_MAX_RECV_BYTES`, `BUBU_GRPC_CLIENT_MAX_SEND_BYTES`, `BUBU_GRPC_MESSAGE_TIMEOUT`, `BUBU_GRPC_CHANNEL_SEND_TIMEOUT`, `BUBU_GRPC_RECONNECT_BASE_BACKOFF`, `BUBU_GRPC_RECONNECT_MAX_BACKOFF`, `BUBU_GRPC_RECONNECT_MAX_RETRIES`
- TLS (optional): `BUBU_GRPC_TLS_CERT_FILE`, `BUBU_GRPC_TLS_KEY_FILE`, `BUBU_GRPC_CA_FILE`, `BUBU_GRPC_CLIENT_TLS`, `BUBU_GRPC_CLIENT_CERT_FILE`, `BUBU_GRPC_CLIENT_KEY_FILE`, `BUBU_GRPC_REQUIRE_TLS`

See detailed tables in `bubustack.io/docs/reference`.

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

## 📢 Support, Security, and Changelog

- See `SUPPORT.md` for how to get help and report issues.
- See `SECURITY.md` for vulnerability reporting and security posture.
- See `CHANGELOG.md` for version history.

## 🤝 Community

- Code of Conduct: see [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) (Contributor Covenant v3.0)

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

