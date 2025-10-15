# 🤖 bobrapet - A Declarative, Kubernetes-Native AI Workflow Engine
[![Go Reference](https://pkg.go.dev/badge/github.com/bubustack/bobrapet.svg)](https://pkg.go.dev/github.com/bubustack/bobrapet)
[![Go Report Card](https://goreportcard.com/badge/github.com/bubustack/bobrapet)](https://goreportcard.com/report/github.com/bubustack/bobrapet)

Bobrapet is a powerful, cloud-native workflow engine for orchestrating complex AI and data processing pipelines on Kubernetes. It leverages the declarative power of Custom Resource Definitions (CRDs) to let you define, manage, and execute multi-step, event-driven workflows with flexibility and control.

Quick links:
- Operator docs: https://bubustack.io/docs/bobrapet
- Quickstart: https://bubustack.io/docs/bobrapet/guides/quickstart
- CRD reference: https://bubustack.io/docs/bobrapet/reference/crds

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

High-level architecture, patterns, and controller internals are documented on the website:
- Overview and architecture: https://bubustack.io/docs/bobrapet/explanations/architecture

## 📚 Core Concepts

- **`Story`**: The top-level definition of a workflow, composed of steps.
- **`Engram`**: A configured, runnable instance of a component (a "worker").
- **`Impulse`**: A trigger that creates workflow instances based on external events.
- **`StoryRun`**: An instance of an executing `Story`.
- **`StepRun`**: An instance of a single `engram` step executing within a `StoryRun`.
- **`EngramTemplate` & `ImpulseTemplate`**: Reusable, cluster-scoped definitions for `Engrams` and `Impulses`.

## 🧰 Workflow Primitives

See the guides for primitives, batch vs. streaming, impulses, and storage configuration:
- Guides: https://bubustack.io/docs/bobrapet/guides

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

For complete environment variable listings and defaults, see the operator configuration and transport reference:
- Operator config: https://bubustack.io/docs/bobrapet/reference/config
- gRPC transport: https://bubustack.io/docs/bobrapet/reference/grpc

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

