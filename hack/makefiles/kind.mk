##@ Kind Cluster Management
## Kind Cluster Configuration
KIND_CLUSTER_NAME ?= bobrapet
KIND_IMAGE ?= kindest/node:v1.32.3
KIND_WAIT_TIMEOUT ?= 300s
KIND_CONFIG ?= $(CURDIR)/hack/kind-config.yaml
BOBRAPET_IMAGE ?= bobrapet:latest

# COG images.
HTTP_TRANSPORT_IMAGE ?= bubustack/http-transport:latest

.PHONY: kind-create
kind-create: ## Create a kind cluster
	@echo "Creating kind cluster '$(KIND_CLUSTER_NAME)'..."
	@if [ -f "$(KIND_CONFIG)" ]; then \
		echo "Using kind config file: $(KIND_CONFIG)"; \
		kind create cluster --name $(KIND_CLUSTER_NAME) --config $(KIND_CONFIG) --wait $(KIND_WAIT_TIMEOUT); \
	else \
		echo "Kind config file not found at $(KIND_CONFIG), using default settings"; \
		kind create cluster --name $(KIND_CLUSTER_NAME) --image $(KIND_IMAGE) --wait $(KIND_WAIT_TIMEOUT); \
	fi
	@echo "Kind cluster '$(KIND_CLUSTER_NAME)' created successfully!"

.PHONY: kind-delete
kind-delete: ## Delete the kind cluster
	@echo "Deleting kind cluster '$(KIND_CLUSTER_NAME)'..."
	@kind delete cluster --name $(KIND_CLUSTER_NAME)
	@echo "Kind cluster '$(KIND_CLUSTER_NAME)' deleted."

.PHONY: kind-load-image
kind-load-image: ## Load Docker image into kind cluster. Usage: make kind-load-image IMAGE=your-image:tag
ifndef IMAGE
	$(error IMAGE is not set. Please specify the image to load, e.g., make kind-load-image IMAGE=your-image:tag)
endif
	@echo "Loading image $(IMAGE) into kind cluster '$(KIND_CLUSTER_NAME)'..."
	@kind load docker-image $(IMAGE) --name $(KIND_CLUSTER_NAME)
	@echo "Image loaded successfully!"

.PHONY: kind-load-controller
kind-load-controller: ## Build and load the controller image into kind
	@echo "Building controller image..."
	@$(MAKE) docker-build
	@echo "Loading controller image into kind..."
	@$(MAKE) kind-load-image IMAGE=$(IMG)


.PHONY: kind-load-http-transport
kind-load-http-transport: ## Load the http transport image into kind
	@echo "Loading http transport image into kind..."
	@$(MAKE) kind-load-image IMAGE=$(HTTP_TRANSPORT_IMAGE)

.PHONY: kind-status
kind-status: ## Check status of kind cluster
	@echo "Checking kind cluster '$(KIND_CLUSTER_NAME)' status..."
	@kind get clusters | grep -q $(KIND_CLUSTER_NAME) && echo "Cluster exists!" || echo "Cluster does not exist."
	@if kind get clusters | grep -q $(KIND_CLUSTER_NAME); then \
		echo "Nodes:"; \
		kubectl --context kind-$(KIND_CLUSTER_NAME) get nodes; \
		echo "\nPods:"; \
		kubectl --context kind-$(KIND_CLUSTER_NAME) get pods -A; \
	fi

.PHONY: kind-port-forward
kind-port-forward: ## Forward local port to service in kind. Usage: make kind-port-forward SERVICE=namespace/service-name LOCAL_PORT=8080 REMOTE_PORT=80
ifndef SERVICE
	$(error SERVICE is not set. Please specify the service, e.g., make kind-port-forward SERVICE=default/myservice LOCAL_PORT=8080 REMOTE_PORT=80)
endif
ifndef LOCAL_PORT
	$(error LOCAL_PORT is not set. Please specify the local port)
endif
ifndef REMOTE_PORT
	$(error REMOTE_PORT is not set. Please specify the remote port)
endif
	@echo "Forwarding local port $(LOCAL_PORT) to $(SERVICE):$(REMOTE_PORT)..."
	@kubectl --context kind-$(KIND_CLUSTER_NAME) port-forward service/$(SERVICE) $(LOCAL_PORT):$(REMOTE_PORT)
