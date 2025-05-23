#!/bin/bash

# install metallb
helm repo add metallb https://metallb.github.io/metallb
helm install metallb metallb/metallb --namespace metallb-system --create-namespace

kubectl apply -f hack/metallb-config.yaml

# install Istio with default profile
istioctl install --set profile=default -y

# Install NATS
helm repo add nats https://nats-io.github.io/k8s/helm/charts/
helm repo update
helm install nats nats/nats --namespace bobrapet-system

