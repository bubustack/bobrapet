#!/bin/bash
set -x

export ARCH=$(case $(uname -m) in x86_64) echo -n amd64 ;; aarch64) echo -n arm64 ;; *) echo -n $(uname -m) ;; esac)

curl -Lo ./kind https://kind.sigs.k8s.io/dl/latest/kind-linux-$ARCH
chmod +x ./kind
mv ./kind /usr/local/bin/kind

curl -L -o kubebuilder https://go.kubebuilder.io/dl/latest/linux/$ARCH
chmod +x kubebuilder
mv kubebuilder /usr/local/bin/

KUBECTL_VERSION=$(curl -L -s https://dl.k8s.io/release/stable.txt)
curl -LO "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/$ARCH/kubectl"
chmod +x kubectl
mv kubectl /usr/local/bin/kubectl

curl -L https://istio.io/downloadIstio | ISTIO_VERSION=1.25.0 sh -
mv istio-1.25.0/bin/istioctl /usr/local/bin/istioctl
rm -rf istio-1.25.0

curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash -

docker network create -d=bridge --subnet=172.19.0.0/24 kind

docker --version
go version
kubectl version --client

kind version
kubebuilder version
helm version