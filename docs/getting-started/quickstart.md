# Quickstart

Get BubuStack running on a local cluster and deploy your first workflow in
under 10 minutes.

## Who this is for

- Developers trying BubuStack for the first time.
- Platform operators evaluating the system.

## What you'll get

- A working BubuStack installation on a local kind cluster.
- Storage and cert-manager configured and ready.
- Your first example workflow running.

## Overview

```
1. Install cert-manager          (webhook TLS)
2. Install S3 storage            (payload offloading)
3. Install BubuStack controllers (bobrapet + bobravoz-grpc)
4. Deploy an example             (start experimenting)
```

## Step 1: Create a cluster

If you don't have a cluster, create one with kind:

```bash
kind create cluster --name bubustack
```

## Step 2: Install system dependencies

BubuStack ships a convenience script that installs cert-manager and SeaweedFS
in one command:

```bash
# From the bubustack workspace root
./examples/storage/seaweedfs/install.sh
```

This script:
1. Installs **cert-manager** v1.19.2 and waits for it to become ready.
2. Creates the `seaweedfs` namespace and an anonymous-access config secret.
3. Deploys **SeaweedFS** via Helm with a pre-configured `bubu-default` bucket.

If you prefer to install dependencies manually or use a different storage
backend, see `/docs/getting-started/prerequisites.md`.

### Verify storage is running

```bash
kubectl get pods -n seaweedfs
# All pods should be Running/Ready
```

## Step 3: Install BubuStack

Install the two core controllers via Helm:

```bash
# Add the Helm repo
helm repo add bubustack https://bubustack.github.io/helm-charts
helm repo update

# Install the workflow operator
helm install bobrapet bubustack/bobrapet

# Install the streaming transport hub
helm install bobravoz-grpc bubustack/bobravoz-grpc
```

Optionally, install the web console:

```bash
helm install bubuilder bubustack/bubuilder
```

### Verify controllers are running

```bash
kubectl get pods -l app.kubernetes.io/part-of=bubustack
# bobrapet-controller-manager and bobravoz-grpc-controller-manager should be Running
```

### Verify CRDs are installed

```bash
kubectl api-resources | grep bubustack
# Should list: stories, storyruns, stepruns, engrams, impulses, transports, etc.
```

## Step 4: Deploy an example

Each example in the `examples/` directory follows the same pattern:

```
bootstrap.yaml   Namespace, secrets, RBAC, transport definitions
engrams.yaml     Engram instances (component deployments)
story.yaml       Workflow definition (DAG of steps)
impulse.yaml     Trigger (webhook, cron, or Kubernetes event)
```

### Batch example: GitHub PR Review

```bash
cd examples/batch/github-pr-review

# 1. Edit bootstrap.yaml to add your API keys
#    (OpenAI key, GitHub token)

# 2. Apply in order
kubectl apply -f bootstrap.yaml
kubectl apply -f engrams.yaml
kubectl apply -f story.yaml
kubectl apply -f impulse.yaml
```

### Real-time example: Pod Crash Notifier

```bash
cd examples/realtime/pod-crash-notifier

kubectl apply -f bootstrap.yaml
kubectl apply -f engrams.yaml
kubectl apply -f story.yaml
kubectl apply -f impulse.yaml
```

### Verify your workflow

```bash
# Check that Engrams are ready
kubectl get engrams -A

# Check that the Story is registered
kubectl get stories -A

# Watch for StoryRuns (triggered by the Impulse)
kubectl get storyruns -A --watch
```

## What's next

- Read the example's `README.md` for detailed configuration options.
- Explore other examples in `examples/batch/` and `examples/realtime/`.
- See `/docs/overview/core.md` for the full workflow model.
- See `/docs/overview/component-ecosystem.md` to build your own Engrams.
- See `/docs/streaming/lifecycle-hooks.md` for streaming lifecycle events.

## Related docs

- `/docs/getting-started/prerequisites.md` -- System dependencies and storage options.
- `/docs/overview/architecture.md` -- System architecture and module map.
- `/docs/operator/configuration.md` -- Operator configuration keys and defaults.
- `/docs/overview/component-ecosystem.md` -- Building custom Engrams and Impulses.
