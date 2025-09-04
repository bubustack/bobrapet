# 🤖 bobrapet - Kubernetes-Native AI Workflow Engine

> **Declarative, Composable, Cloud-Native AI Workflows**

Bobrapet is a Kubernetes-native workflow engine designed specifically for AI and automation workloads. Inspired by Terraform, Ansible, AWS Step Functions, and modern CI/CD systems, it brings declarative workflow orchestration to the Kubernetes ecosystem with a focus on composability, observability, and developer experience.

[![Go Version](https://img.shields.io/badge/go-1.21+-blue.svg)](https://golang.org)
[![Kubernetes](https://img.shields.io/badge/kubernetes-1.25+-blue.svg)](https://kubernetes.io)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## 🌟 Key Features

### **Declarative Workflow Definitions**
- Define complex AI workflows using Kubernetes YAML manifests
- Version control your workflows alongside your code
- GitOps-ready with declarative configuration

### **Advanced Workflow Primitives**
- **Control Flow**: `condition`, `switch`, `loop`, `parallel`
- **Data Operations**: `filter`, `transform`, `setData`, `mergeData`
- **Orchestration**: `executeStory`, `wait`, `throttle`, `batch`
- **Human-in-the-Loop**: `gate` with approval workflows

### **Composable Architecture**
- **Stories**: Workflow definitions with reusable components
- **Engrams**: AI/ML components with standardized ABI contract
- **Impulses**: Flexible trigger mechanisms (HTTP, Cron, Events)
- **Runs**: Execution tracking with full observability

### **Cloud-Native by Design**
- Built on Kubernetes Custom Resource Definitions (CRDs)
- Follows Kubernetes patterns and best practices
- Scales with your cluster, integrates with existing tools

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   📋 Stories    │    │   🧠 Engrams    │    │   ⚡ Impulses   │
│   (Workflows)   │    │  (Components)   │    │   (Triggers)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌─────────────────────────┐
                    │    🎯 Workflow Engine   │
                    │   (Controllers)         │
                    └─────────────────────────┘
                                 │
                    ┌─────────────────────────┐
                    │     🏃 Executions       │
                    │  (StoryRuns/StepRuns)   │
                    └─────────────────────────┘
```

### **Design Principles**
- **DOTADIW (Do One Thing And Do It Well)**: Each component has a single, well-defined responsibility
- **Kubernetes-Native**: Leverage existing K8s patterns, tools, and ecosystem
- **Composable**: Build complex workflows from simple, reusable components
- **Observable**: Built-in monitoring, logging, and tracing capabilities

## 🚀 Quick Start

### Prerequisites
- Kubernetes 1.25+
- kubectl configured for your cluster
- Go 1.21+ (for development)

### 1. Install Bobrapet

```bash
# Install the CRDs
kubectl apply -f https://github.com/bubustack/bobrapet/releases/latest/download/install.yaml

# Deploy the controller
kubectl apply -f https://github.com/bubustack/bobrapet/releases/latest/download/manager.yaml
```

### 2. Create Your First AI Workflow

```yaml
# story.yaml
apiVersion: bubu.sh/v1alpha1
kind: Story
metadata:
  name: sentiment-analysis-pipeline
spec:
  inputs:
    text: { type: string, required: true }
    
  steps:
    - name: analyze-sentiment
      ref: sentiment-classifier
      with:
        text: "{{ .inputs.text }}"
    
    - name: route-by-sentiment
      type: switch
      cases:
        - when: "analyze-sentiment.output.sentiment == 'negative'"
          steps:
            - name: escalate
              ref: create-support-ticket
              with:
                priority: high
                content: "{{ .inputs.text }}"
        
        - when: "analyze-sentiment.output.sentiment == 'positive'"
          steps:
            - name: celebrate
              ref: send-celebration
              with:
                message: "Great feedback received!"
      
      default:
        - name: log-neutral
          ref: log-message
          with:
            level: info
            message: "Neutral sentiment detected"

---
# engram.yaml
apiVersion: bubu.sh/v1alpha1
kind: Engram
metadata:
  name: sentiment-classifier
spec:
  engine:
    mode: container
    image: ghcr.io/bubustack/sentiment-classifier:v1.0.0
  with:
    model: "cardiffnlp/twitter-roberta-base-sentiment-latest"
    confidence_threshold: 0.8

---
# impulse.yaml
apiVersion: bubu.sh/v1alpha1
kind: Impulse
metadata:
  name: feedback-webhook
spec:
  storyRef: sentiment-analysis-pipeline
  engine:
    mode: container
    image: ghcr.io/bubustack/impulse-http:v1.0.0
  with:
    path: "/api/feedback"
    method: "POST"
  contextMapping:
    text: "request.body.feedback"
```

### 3. Deploy and Test

```bash
# Deploy the workflow
kubectl apply -f story.yaml

# Trigger the workflow
curl -X POST http://your-cluster/api/feedback \
  -H "Content-Type: application/json" \
  -d '{"feedback": "This product is amazing!"}'

# Monitor execution
kubectl get storyruns
kubectl get stepruns
```

## 📚 Core Concepts

### Stories (Workflows)
Stories define your workflow logic using a declarative YAML syntax. They specify the sequence of steps, control flow, and data transformations needed to accomplish a business objective.

**Key Features:**
- **Input/Output Schemas**: Define expected data structures
- **Advanced Primitives**: Rich set of control flow and data operations
- **CEL Expressions**: Use Google's Common Expression Language for conditions
- **Nested Workflows**: Compose complex workflows from simpler ones

### Engrams (Components)
Engrams are reusable components that encapsulate specific functionality. They can be AI models, API calls, data transformations, or any containerized workload.

**ABI Contract:**
- **Input**: JSON on stdin
- **Output**: JSON on stdout  
- **Errors**: Structured errors on stderr
- **Exit Codes**: Standard Unix exit codes for different scenarios

**Engine Modes:**
- **Container**: Run as Kubernetes Jobs (most common)
- **Pooled**: Pre-warmed pods for low-latency execution
- **GRPC**: Connect to external services
- **WASI**: WebAssembly runtime (future)

### Impulses (Triggers)
Impulses define how workflows are triggered. They provide a flexible abstraction over various event sources and scheduling mechanisms.

**Supported Types:**
- **HTTP**: Webhook endpoints with authentication
- **Cron**: Scheduled execution with timezone support
- **Kafka**: Message queue integration
- **PubSub**: Cloud pub/sub systems
- **File System**: Watch for file changes
- **Custom**: Extensible plugin architecture

## 🎯 Workflow Primitives

Bobrapet provides a rich set of primitives for building sophisticated workflows:

### Control Flow
```yaml
# Conditional execution
- name: check-condition
  type: condition
  if: ".input.score > 0.8"
  then:
    - name: high-confidence-path
      ref: process-confident-result
  else:
    - name: low-confidence-path
      ref: request-human-review

# Multi-way branching
- name: route-by-category
  type: switch
  cases:
    - when: ".category == 'urgent'"
      steps: [...]
    - when: ".category == 'normal'"
      steps: [...]
  default: [...]

# Parallel execution
- name: parallel-processing
  type: parallel
  branches:
    - name: image-analysis
      ref: analyze-image
    - name: text-extraction
      ref: extract-text
  branchPolicy:
    mode: allMustSucceed
```

### Data Operations
```yaml
# Filter data
- name: filter-valid-items
  type: filter
  filter: ".items | select(.valid == true)"

# Transform data
- name: normalize-data
  type: transform
  transform: "{normalized: .value / .max, category: .type}"

# Set variables
- name: set-constants
  type: setData
  data:
    api_endpoint: "https://api.example.com"
    timeout: 30
```

### Advanced Features
```yaml
# Human approval gates
- name: require-approval
  type: gate
  approvers: ["user@company.com", "admin@company.com"]

# Sub-workflow execution
- name: detailed-analysis
  type: executeStory
  storyRef: detailed-analysis-pipeline
  subInputs:
    document: "{{ .current_document }}"

# Rate limiting
- name: api-call
  type: throttle
  rate: "100/min"
  burst: 10
  steps:
    - name: call-external-api
      ref: external-api-client
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
kubebuilder edit --plugins=helm/v1-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.

## 🛠️ Development

### Building from Source
```bash
# Clone the repository
git clone https://github.com/bubustack/bobrapet.git
cd bobrapet

# Install dependencies
go mod download

# Generate code
make generate

# Build the manager
make build

# Run tests
make test

# Build and push Docker image
make docker-build docker-push IMG=your-registry/bobrapet:latest
```

### Local Development
```bash
# Install CRDs into your cluster
make install

# Run the controller locally
make run

# In another terminal, apply test manifests
kubectl apply -f config/samples/
```

## 🔐 Security

### RBAC Configuration
```yaml
# Platform Developer Role
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: workflow-developer
rules:
- apiGroups: ["bubu.sh"]
  resources: ["stories", "engrams", "impulses"]
  verbs: ["get", "list", "watch", "create", "update", "patch"]
- apiGroups: ["runs.bubu.sh"] 
  resources: ["storyruns"]
  verbs: ["create"]

# Workflow Operator Role  
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: workflow-operator
rules:
- apiGroups: ["runs.bubu.sh"]
  resources: ["storyruns", "stepruns"]
  verbs: ["get", "list", "watch", "delete"]
```

### Security Features
- **Pod Security Standards**: Runs with minimal privileges
- **Network Policies**: Default-deny with explicit allow rules
- **Secret Management**: Secure credential injection
- **Resource Limits**: Prevent resource exhaustion

## 📊 Monitoring and Observability

Bobrapet provides comprehensive observability out of the box:

### Metrics
- Workflow execution rates and durations
- Step success/failure rates
- Resource utilization
- Queue depths and processing times

### Logging
- Structured logging with correlation IDs
- Execution traces across workflow steps
- Error details with context

### Health Checks
- Controller health and readiness probes
- Dependency health monitoring
- Circuit breaker patterns for external services

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Getting Started
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 🎯 Roadmap

- [ ] **Web UI Dashboard**: Visual workflow designer and monitoring
- [ ] **CLI Tool**: Command-line interface for workflow management  
- [ ] **VS Code Extension**: IDE integration for workflow development
- [ ] **Advanced Scheduling**: Multi-cluster and hybrid cloud support
- [ ] **ML Optimization**: Automatic workflow optimization using ML
- [ ] **Template Marketplace**: Community-driven template sharing

## 📞 Supportof 

- **Documentation**: [docs.bobrapet.io](https://docs.bobrapet.io)
- **Issues**: [GitHub Issues](https://github.com/bubustack/bobrapet/issues)
- **Discussions**: [GitHub Discussions](https://github.com/bubustack/bobrapet/discussions)
- **Community**: [Discord Server](https://discord.gg/bobrapet)

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

---

Built with ❤️ by the BubuStack team. Empowering the next generation of AI workflows.

