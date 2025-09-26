# 🤖 bobrapet - A Declarative, Kubernetes-Native AI Workflow Engine

Bobrapet is a powerful, cloud-native workflow engine for orchestrating complex AI and data processing pipelines on Kubernetes. It leverages the declarative power of Custom Resource Definitions (CRDs) to let you define, manage, and execute multi-step, event-driven workflows with unparalleled flexibility and control.

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

- **`executeStory`**: Run another `Story` as a sub-workflow, either synchronously or asynchronously.
- **`if` condition**: Use CEL expressions to conditionally execute steps.
- **`switch`**: Implement multi-way branching (like a switch/case statement).
- **`loop`**: Iterate over arrays or repeat actions.
- **`parallel`**: Group steps to be executed concurrently.
- And many more for flow control, data transformation, and state management (`sleep`, `stop`, `filter`, `transform`, `setData`, etc.).

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

### 2. Deploy a Sample Workflow

The following example defines a two-step workflow that fetches content from a URL and uses an AI model to summarize it. Notice how the `summarize` step implicitly depends on the output of the `fetch-content` step.

Apply the sample manifests, which include the necessary `EngramTemplates`, `Engrams`, and the `Story` definition:
```bash
kubectl apply -k config/samples
```

This creates:
- An `Engram` named `http-request-engram` to fetch web content.
- An `Engram` named `openai-summarizer-engram` to summarize text.
- A `Story` named `summarize-website-story` that defines the workflow.

### 3. Run the Workflow

Create a `StoryRun` resource to trigger the workflow. This `StoryRun` provides the initial input `url` required by the `Story`.

```yaml
# ./run.yaml
apiVersion: runs.bubu.sh/v1alpha1
kind: StoryRun
metadata:
  name: summarize-k8s-docs
spec:
  storyRef:
    name: summarize-website-story
  inputs:
    url: https://kubernetes.io/docs/concepts/overview/
```

Apply it:
```bash
kubectl apply -f ./run.yaml
```

### 4. Observe the Results

Monitor the execution of the workflow by checking the `StoryRun` and its child `StepRuns`.

```bash
# Check the overall status of the workflow
kubectl get storyrun summarize-k8s-docs -o yaml

# Check the status of individual steps
kubectl get stepruns -l bubu.sh/storyrun=summarize-k8s-docs
```

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

