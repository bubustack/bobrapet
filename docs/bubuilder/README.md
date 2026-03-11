# Bubuilder (UI + API)

The BubuStack UI and API server lives in the dedicated **bubuilder** project, not in bobrapet.

- **Project path:** [bubuilder](/Users/kashotyan/personal/bubustack/bubuilder)
- **Purpose:** K8s-native console for StoryRuns, StepRuns, Jobs, logs, storage, observability, and Story Builder.
- **Run:** See bubuilder’s README for `go run ./cmd/bubuilder-server`, UI dev server, and auth/storage configuration.

Bobrapet is the operator only (controllers, webhooks, CRDs). Use bubuilder for all UI and console logic.
