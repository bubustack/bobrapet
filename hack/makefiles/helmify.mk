HELMIFY ?= $(LOCALBIN)/helmify
VALUES_FILE ?= $(CURDIR)/hack/helm-values.yaml

.PHONY: helmify
helmify: $(HELMIFY) ## Download helmify locally if necessary.
$(HELMIFY): $(LOCALBIN)
	test -s $(LOCALBIN)/helmify || GOBIN=$(LOCALBIN) go install github.com/arttor/helmify/cmd/helmify@latest
    
helm: manifests kustomize helmify
	$(KUSTOMIZE) build config/default | $(HELMIFY)
	cp $(VALUES_FILE) $(CURDIR)/chart/values.yaml
	cd $(CURDIR)/chart && helm dependency build