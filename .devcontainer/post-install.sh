#!/bin/bash
set -x

curl -Lo ./kind "https://kind.sigs.k8s.io/dl/latest/kind-$(go env GOOS)-$(go env GOARCH)"
chmod +x ./kind && mv ./kind /usr/local/bin/kind

curl -L -o kubebuilder "https://go.kubebuilder.io/dl/latest/$(go env GOOS)/$(go env GOARCH)"
chmod +x kubebuilder && mv kubebuilder /usr/local/bin/

KUBECTL_VERSION=$(curl -L -s https://dl.k8s.io/release/stable.txt)
curl -LO "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/$(go env GOOS)/$(go env GOARCH)/kubectl"
chmod +x kubectl && mv kubectl /usr/local/bin/kubectl

curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

docker --version
go version
kubectl version --client

kind version
kubebuilder version
helm version