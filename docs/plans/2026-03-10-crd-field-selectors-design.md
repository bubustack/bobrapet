# CRD Field Selectors Design

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable server-side field filtering on StepRun, StoryRun, and TransportBinding CRDs using Kubernetes selectableFields (GA in 1.30+, supported by controller-gen v0.20.0).

**Architecture:** Add `+kubebuilder:selectablefield` markers to CRD type structs, regenerate CRD manifests, update bubuilder API handlers to use `client.MatchingFields` instead of post-fetch filtering. Keep existing runtime FieldIndexer registrations (they serve controller fan-out, not external queries).

**Tech Stack:** Go 1.25, kubebuilder, controller-gen v0.20.0, controller-runtime v0.22.4, k8s.io v0.35.0

---

## Selectable Fields

### StepRun (`api/runs/v1alpha1/steprun_types.go`)

| JSONPath | Purpose |
|----------|---------|
| `.spec.storyRunRef.name` | Filter by parent StoryRun |
| `.spec.stepID` | Filter by step name in Story |
| `.status.phase` | Filter by execution phase |

### StoryRun (`api/runs/v1alpha1/storyrun_types.go`)

| JSONPath | Purpose |
|----------|---------|
| `.spec.storyRef.name` | Filter by parent Story |
| `.status.phase` | Filter by execution phase |

### TransportBinding (`api/transport/v1alpha1/transportbinding_types.go`)

| JSONPath | Purpose |
|----------|---------|
| `.spec.transportRef` | Filter by Transport name |
| `.spec.storyRunRef.name` | Filter by parent StoryRun |

## Changes

### Task 1: CRD Markers — StepRun

Add markers above `StepRun` struct in `api/runs/v1alpha1/steprun_types.go`:

```go
// +kubebuilder:selectablefield:JSONPath=".spec.storyRunRef.name"
// +kubebuilder:selectablefield:JSONPath=".spec.stepID"
// +kubebuilder:selectablefield:JSONPath=".status.phase"
```

Run `make manifests` to regenerate CRD YAML.

### Task 2: CRD Markers — StoryRun

Add markers above `StoryRun` struct in `api/runs/v1alpha1/storyrun_types.go`:

```go
// +kubebuilder:selectablefield:JSONPath=".spec.storyRef.name"
// +kubebuilder:selectablefield:JSONPath=".status.phase"
```

Run `make manifests` to regenerate CRD YAML.

### Task 3: CRD Markers — TransportBinding

Add markers above `TransportBinding` struct in `api/transport/v1alpha1/transportbinding_types.go`:

```go
// +kubebuilder:selectablefield:JSONPath=".spec.transportRef"
// +kubebuilder:selectablefield:JSONPath=".spec.storyRunRef.name"
```

Run `make manifests` to regenerate CRD YAML.

### Task 4: Update bubuilder API — StepRun Filtering

In `bubuilder/internal/api/handlers.go`, replace post-fetch filtering in `handleStepsList()` with `client.MatchingFields`:

**Before** (in-memory):
```go
if storyRun != "" && step.Spec.StoryRunRef.Name != storyRun { continue }
if stepID != "" && step.Spec.StepID != stepID { continue }
if phase != "" && string(step.Status.Phase) != phase { continue }
```

**After** (server-side):
```go
if storyRun != "" {
    opts = append(opts, client.MatchingFields{"spec.storyRunRef.name": storyRun})
}
if stepID != "" {
    opts = append(opts, client.MatchingFields{"spec.stepID": stepID})
}
if phase != "" {
    opts = append(opts, client.MatchingFields{"status.phase": phase})
}
```

### Task 5: Update bubuilder API — StoryRun and TransportBinding Filtering

Apply same pattern to StoryRun and TransportBinding list handlers if they have post-fetch filtering.

## What This Enables

```bash
kubectl get stepruns --field-selector spec.storyRunRef.name=my-run
kubectl get stepruns --field-selector status.phase=Failed
kubectl get stepruns --field-selector spec.stepID=process,status.phase=Running
kubectl get storyruns --field-selector spec.storyRef.name=my-story
kubectl get transportbindings --field-selector spec.transportRef=livekit
```

## What Does NOT Change

- Runtime FieldIndexer registrations in `internal/setup/indexing.go` — these power controller watch fan-out
- Label-based queries in controllers — these serve different use cases
- Webhook logic — no webhook changes needed

## Risk

None — purely additive. CRD selectableFields are a declarative schema feature. Existing queries continue to work.
