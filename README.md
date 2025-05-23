# bobrapet
// TODO(user): Add simple overview of use/purpose

## Bobrapet - Kubernetes Project Wrapper

Bobrapet is a Kubernetes operator that provides a Project resource for logical grouping and namespace management. Each Project creates a dedicated namespace with the same name (with spaces converted to hyphens) for isolation and organization of resources.

### Project Structure

A Project is a simple resource that primarily creates a namespace. Transports and other resources reference the Project, not the other way around.

```yaml
apiVersion: bubu.sh/v1alpha1
kind: Project
metadata:
  name: "My Project"
spec: {}  # Project is intentionally minimal
```

When this Project is created, it will create a namespace named `my-project` (lowercase with hyphens instead of spaces).

### Architecture

The Project controller:
- Creates a dedicated namespace with the project name (spaces converted to hyphens)
- Manages the lifecycle of the namespace (creates when project is created, deletes when project is deleted)
- Adds labels to the namespace to identify it as part of the project: `bubu.sh/project: "My Project"`

### Transport Architecture

Transport resources (like HTTP, WebSocket, etc.) reference Projects, not the other way around. For example, an HTTP transport would specify which Project it belongs to in its spec or through labels. This creates a more natural reference pattern in Kubernetes.

### Using port-forwarding

To access your project endpoints during development:

```bash
# Assuming an external controller has created an istio-ingressgateway in istio-system namespace:
make -f hack/Makefile kind-port-forward SERVICE="istio-system/istio-ingressgateway" LOCAL_PORT=8080 REMOTE_PORT=80
```

## Description
// TODO(user): An in-depth paragraph about your project and overview of use

## Getting Started

### Prerequisites
- go version v1.23.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/bobrapet:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don't work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/bobrapet:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/bobrapet:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/bobrapet/<tag or branch>/dist/install.yaml
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

## Contributing
// TODO(user): Add detailed information on how you would like others to contribute to this project

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

