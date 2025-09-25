# 🤖 bobrapet - Kubernetes-Native AI Workflow Engine

> **Declarative, Composable, Cloud-Native AI Workflows**

Bobrapet is a Kubernetes-native workflow engine designed specifically for AI and data processing workloads. It uses Custom Resource Definitions (CRDs) to define, execute, and manage complex, multi-step workflows directly within your Kubernetes cluster.

## 🌟 Key Features

- **Declarative Workflows**: Define complex pipelines as Kubernetes resources (`Story`).
- **Composable Components**: Package reusable tasks as `Engrams`, backed by versioned, cluster-scoped `EngramTemplates`.
- **Flexible Triggers**: Initiate workflows from events using `Impulses`, backed by `ImpulseTemplates`.
- **Full Observability**: Track the state of every workflow (`StoryRun`) and each individual step (`StepRun`).
- **Cross-Namespace Support**: Orchestrate workflows that span multiple Kubernetes namespaces, enabling multi-tenant and shared service models.

## 📚 Core Concepts

- **`Story`**: The top-level definition of a workflow, composed of steps.
- **`Engram`**: A configured, runnable instance of a component (a "worker").
- **`Impulse`**: A trigger that creates workflow instances based on external events.
- **`StoryRun`**: An instance of an executing `Story`.
- **`StepRun`**: An instance of a single step executing within a `StoryRun`.
- **`EngramTemplate` & `ImpulseTemplate`**: Reusable, cluster-scoped definitions for `Engrams` and `Impulses`.

## 🚀 Quick Start

### Prerequisites
- A running Kubernetes cluster (e.g., KinD, Minikube, or a cloud provider).
- `kubectl` configured to access your cluster.

### 1. Install the Operator

First, install the Custom Resource Definitions (CRDs) that define the `bobrapet` resources:
```bash
make install
```

Next, deploy the operator controller to your cluster:
```bash
make deploy
```

### 2. Deploy a Sample Workflow

This example defines a two-step workflow that fetches content from a URL and uses an AI model to summarize it.

Apply the sample manifests, which include the necessary `EngramTemplates`, `Engrams`, and the `Story` definition:
```bash
kubectl apply -k config/samples
```

This creates:
- An `Engram` named `http-request-engram` to fetch web content.
- An `Engram` named `openai-summarizer-engram` to summarize text.
- A `Story` named `summarize-website-story` that chains them together.

### 3. Run the Workflow

Create a `StoryRun` resource to trigger the workflow with a specific URL:

```yaml
# ./run.yaml
apiVersion: runs.bubu.sh/v1alpha1
kind: StoryRun
metadata:
  name: summarize-k8s-docs
spec:
  inputs:
    url: https://kubernetes.io/docs/concepts/overview/
  storyRef:
    name: summarize-website-story
```

Apply it:
```bash
kubectl apply -f ./run.yaml
```

### 4. Observe the Results

You can monitor the execution of the workflow by checking the `StoryRun` and its child `StepRuns`.

```bash
# Check the overall status of the workflow
kubectl get storyrun summarize-k8s-docs -o yaml

# Check the status of individual steps
kubectl get stepruns -l bubu.sh/storyrun=summarize-k8s-docs
```

## 🛠️ Development

To develop the operator locally:

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/bubustack/bobrapet.git
    cd bobrapet
    ```

2.  **Run the controller:**
    This command will run the operator on your local machine, using your local `kubeconfig` to communicate with the cluster.
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

