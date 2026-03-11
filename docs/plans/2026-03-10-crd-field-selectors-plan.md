# CRD Field Selectors Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable server-side field filtering on StepRun, StoryRun, and TransportBinding CRDs so kubectl and bubuilder can filter without fetching all objects.

**Architecture:** Add `+kubebuilder:selectablefield` kubebuilder markers to CRD type structs, run `make manifests` to regenerate CRD YAML with `selectableFields` entries, then update bubuilder API handlers to use `client.MatchingFields` instead of post-fetch in-memory filtering. Existing runtime FieldIndexer registrations in `internal/setup/indexing.go` are kept — they serve controller watch fan-out, not external queries.

**Tech Stack:** Go 1.25, kubebuilder markers, controller-gen v0.20.0, controller-runtime v0.22.4, k8s.io/api v0.35.0

**Design doc:** `docs/plans/2026-03-10-crd-field-selectors-design.md`

---

## Task 1: Add selectableField markers to StepRun CRD

**Files:**
- Modify: `api/runs/v1alpha1/steprun_types.go:47-56` (marker block above `type StepRun struct`)

**Step 1: Add the markers**

Insert three `+kubebuilder:selectablefield` markers into the existing marker block above `type StepRun struct`. The markers go after the existing `+kubebuilder:printcolumn` and `+kubebuilder:rbac` markers, just before the `type StepRun struct` line.

The marker block should read:

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=step,categories={bubu,ai,runs}
// +kubebuilder:printcolumn:name="StoryRun",type=string,JSONPath=.spec.storyRunRef.name
// +kubebuilder:printcolumn:name="Step",type=string,JSONPath=.spec.stepId
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=.status.phase
// +kubebuilder:printcolumn:name="Retries",type=integer,JSONPath=.status.retries
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns,verbs=get;watch
// +kubebuilder:rbac:groups=runs.bubustack.io,resources=stepruns/status,verbs=patch;update
// +kubebuilder:selectablefield:JSONPath=".spec.storyRunRef.name"
// +kubebuilder:selectablefield:JSONPath=".spec.stepId"
// +kubebuilder:selectablefield:JSONPath=".status.phase"
type StepRun struct {
```

**Important:** The JSON field name is `stepId` (camelCase, matching the JSON tag on `StepID string \`json:"stepId"\``), NOT `stepID`.

**Step 2: Regenerate CRD manifests**

Run: `cd /Users/kashotyan/personal/bubustack/bobrapet && make manifests`

Expected: `config/crd/bases/runs.bubustack.io_stepruns.yaml` now contains a `selectableFields` section under the `v1alpha1` version entry:

```yaml
selectableFields:
- jsonPath: .spec.storyRunRef.name
- jsonPath: .spec.stepId
- jsonPath: .status.phase
```

**Step 3: Verify build**

Run: `cd /Users/kashotyan/personal/bubustack/bobrapet && make build`

Expected: Clean build, no errors.

**Step 4: Commit**

```bash
cd /Users/kashotyan/personal/bubustack/bobrapet
git add api/runs/v1alpha1/steprun_types.go config/crd/bases/runs.bubustack.io_stepruns.yaml
git commit -m "feat(api): add selectableField markers to StepRun CRD

Enable server-side field filtering on spec.storyRunRef.name, spec.stepId,
and status.phase for StepRun resources."
```

---

## Task 2: Add selectableField markers to StoryRun CRD

**Files:**
- Modify: `api/runs/v1alpha1/storyrun_types.go:45-52` (marker block above `type StoryRun struct`)

**Step 1: Add the markers**

Insert two `+kubebuilder:selectablefield` markers into the existing marker block. The marker block should read:

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=srun,categories={bubu,ai,runs}
// +kubebuilder:printcolumn:name="Story",type=string,JSONPath=.spec.storyRef.name
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=.status.phase
// +kubebuilder:printcolumn:name="Duration",type=string,JSONPath=.status.duration
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
// +kubebuilder:selectablefield:JSONPath=".spec.storyRef.name"
// +kubebuilder:selectablefield:JSONPath=".status.phase"
type StoryRun struct {
```

**Step 2: Regenerate CRD manifests**

Run: `cd /Users/kashotyan/personal/bubustack/bobrapet && make manifests`

Expected: `config/crd/bases/runs.bubustack.io_storyruns.yaml` now contains:

```yaml
selectableFields:
- jsonPath: .spec.storyRef.name
- jsonPath: .status.phase
```

**Step 3: Verify build**

Run: `cd /Users/kashotyan/personal/bubustack/bobrapet && make build`

Expected: Clean build, no errors.

**Step 4: Commit**

```bash
cd /Users/kashotyan/personal/bubustack/bobrapet
git add api/runs/v1alpha1/storyrun_types.go config/crd/bases/runs.bubustack.io_storyruns.yaml
git commit -m "feat(api): add selectableField markers to StoryRun CRD

Enable server-side field filtering on spec.storyRef.name and status.phase
for StoryRun resources."
```

---

## Task 3: Add selectableField markers to TransportBinding CRD

**Files:**
- Modify: `api/transport/v1alpha1/transportbinding_types.go:191-201` (marker block above `type TransportBinding struct`)

**Step 1: Add the markers**

Insert two `+kubebuilder:selectablefield` markers. The marker block should read:

```go
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=tb;tbind,categories={bubu,transport}
// +kubebuilder:printcolumn:name="Transport",type=string,JSONPath=.spec.transportRef
// +kubebuilder:printcolumn:name="Step",type=string,JSONPath=.spec.stepName
// +kubebuilder:printcolumn:name="Driver",type=string,JSONPath=.spec.driver
// +kubebuilder:printcolumn:name="Endpoint",type=string,JSONPath=.status.endpoint
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=.metadata.creationTimestamp
// +kubebuilder:selectablefield:JSONPath=".spec.transportRef"
// +kubebuilder:selectablefield:JSONPath=".spec.storyRunRef.name"

// TransportBinding is the Schema for the transportbindings API
type TransportBinding struct {
```

**Note:** `storyRunRef` is a pointer field (`*refs.StoryRunReference`). The apiserver handles nil pointers gracefully — if the field is nil, the selector value is empty string, so `--field-selector spec.storyRunRef.name=my-run` won't match resources where the field is absent.

**Step 2: Regenerate CRD manifests**

Run: `cd /Users/kashotyan/personal/bubustack/bobrapet && make manifests`

Expected: `config/crd/bases/transport.bubustack.io_transportbindings.yaml` now contains:

```yaml
selectableFields:
- jsonPath: .spec.transportRef
- jsonPath: .spec.storyRunRef.name
```

**Step 3: Verify build**

Run: `cd /Users/kashotyan/personal/bubustack/bobrapet && make build`

Expected: Clean build, no errors.

**Step 4: Commit**

```bash
cd /Users/kashotyan/personal/bubustack/bobrapet
git add api/transport/v1alpha1/transportbinding_types.go config/crd/bases/transport.bubustack.io_transportbindings.yaml
git commit -m "feat(api): add selectableField markers to TransportBinding CRD

Enable server-side field filtering on spec.transportRef and
spec.storyRunRef.name for TransportBinding resources."
```

---

## Task 4: Update bubuilder — StepRun server-side filtering

**Files:**
- Modify: `../bubuilder/internal/api/handlers.go:231-301` (`handleStepsList` function)

**Step 1: Move storyRun, stepID, and phase filters from post-fetch to pre-fetch**

In `handleStepsList()`, add `client.MatchingFields` options before the `List` call, and remove the corresponding post-fetch `continue` statements.

**Current code** (lines 244-277):
```go
opts := []client.ListOption{}
if namespace != "" {
    opts = append(opts, client.InNamespace(namespace))
}
if selector, err := buildLabelSelector(r); err != nil {
    writeError(w, http.StatusBadRequest, err.Error())
    return
} else if selector != nil {
    opts = append(opts, client.MatchingLabelsSelector{Selector: selector})
}
if err := clients.Client.List(ctx, &steps, opts...); err != nil {
    writeError(w, http.StatusInternalServerError, fmt.Sprintf("list stepruns: %v", err))
    return
}

items := make([]StepSummary, 0, len(steps.Items))
for i := range steps.Items {
    step := &steps.Items[i]
    if storyRun != "" && step.Spec.StoryRunRef.Name != storyRun {
        continue
    }
    // ... storyRunNamespace filter (keep this — not a selectable field) ...
    if stepID != "" && step.Spec.StepID != stepID {
        continue
    }
    if phase != "" && string(step.Status.Phase) != phase {
        continue
    }
    items = append(items, toStepSummary(step))
}
```

**New code:**
```go
opts := []client.ListOption{}
if namespace != "" {
    opts = append(opts, client.InNamespace(namespace))
}
if selector, err := buildLabelSelector(r); err != nil {
    writeError(w, http.StatusBadRequest, err.Error())
    return
} else if selector != nil {
    opts = append(opts, client.MatchingLabelsSelector{Selector: selector})
}
if storyRun != "" {
    opts = append(opts, client.MatchingFields{"spec.storyRunRef.name": storyRun})
}
if stepID != "" {
    opts = append(opts, client.MatchingFields{"spec.stepId": stepID})
}
if phase != "" {
    opts = append(opts, client.MatchingFields{"status.phase": phase})
}
if err := clients.Client.List(ctx, &steps, opts...); err != nil {
    writeError(w, http.StatusInternalServerError, fmt.Sprintf("list stepruns: %v", err))
    return
}

items := make([]StepSummary, 0, len(steps.Items))
for i := range steps.Items {
    step := &steps.Items[i]
    if storyRunNamespace != "" {
        refNS := step.Namespace
        if step.Spec.StoryRunRef.Namespace != nil && *step.Spec.StoryRunRef.Namespace != "" {
            refNS = *step.Spec.StoryRunRef.Namespace
        }
        if refNS != storyRunNamespace {
            continue
        }
    }
    items = append(items, toStepSummary(step))
}
```

**Note:** The `storyRunNamespace` filter stays as post-fetch because namespace is not a selectable field on the StoryRunReference sub-object.

**Step 2: Verify build**

Run: `cd /Users/kashotyan/personal/bubustack/bubuilder && go build ./...`

Expected: Clean build.

**Step 3: Commit**

```bash
cd /Users/kashotyan/personal/bubustack/bubuilder
git add internal/api/handlers.go
git commit -m "feat(api): use server-side field selectors for StepRun filtering

Replace post-fetch in-memory filtering with client.MatchingFields for
storyRunRef.name, stepId, and phase. Requires StepRun CRD selectableFields."
```

---

## Task 5: Update bubuilder — StoryRun server-side filtering

**Files:**
- Modify: `../bubuilder/internal/api/handlers.go:137-227` (`handleRunsList` function)

**Step 1: Move story and phase filters from post-fetch to pre-fetch**

In `handleRunsList()`, add `client.MatchingFields` options before the `List` call, and remove the corresponding post-fetch `continue` statements.

**Current code** (lines 153-208):
```go
opts := []client.ListOption{}
// ... namespace, label selector ...
if err := clients.Client.List(ctx, &listRuns, opts...); err != nil {
    // ...
}

items := make([]RunSummary, 0, len(listRuns.Items))
for i := range listRuns.Items {
    run := &listRuns.Items[i]
    if story != "" && run.Spec.StoryRef.Name != story {
        continue
    }
    // ... storyNamespace filter ...
    if phase != "" && string(run.Status.Phase) != phase {
        continue
    }
    // ... impulse, triggerToken filters ...
    items = append(items, toRunSummary(run))
}
```

**New code** — add before the `List` call:
```go
if story != "" {
    opts = append(opts, client.MatchingFields{"spec.storyRef.name": story})
}
if phase != "" {
    opts = append(opts, client.MatchingFields{"status.phase": phase})
}
```

Then remove the corresponding post-fetch checks:
- Remove: `if story != "" && run.Spec.StoryRef.Name != story { continue }`
- Remove: `if phase != "" && string(run.Status.Phase) != phase { continue }`
- Keep: `storyNamespace`, `impulse`, `impulseNamespace`, `triggerToken` post-fetch filters (not selectable fields)

**Step 2: Verify build**

Run: `cd /Users/kashotyan/personal/bubustack/bubuilder && go build ./...`

Expected: Clean build.

**Step 3: Commit**

```bash
cd /Users/kashotyan/personal/bubustack/bubuilder
git add internal/api/handlers.go
git commit -m "feat(api): use server-side field selectors for StoryRun filtering

Replace post-fetch in-memory filtering with client.MatchingFields for
storyRef.name and phase. Requires StoryRun CRD selectableFields."
```

---

## Verification

After all tasks:

1. `cd /Users/kashotyan/personal/bubustack/bobrapet && make build` — operator builds
2. `cd /Users/kashotyan/personal/bubustack/bubuilder && go build ./...` — bubuilder builds
3. Check generated CRDs contain `selectableFields`:
   ```bash
   grep -A3 "selectableFields" config/crd/bases/runs.bubustack.io_stepruns.yaml
   grep -A3 "selectableFields" config/crd/bases/runs.bubustack.io_storyruns.yaml
   grep -A3 "selectableFields" config/crd/bases/transport.bubustack.io_transportbindings.yaml
   ```
4. After deploying to a cluster (`make install`), verify:
   ```bash
   kubectl get stepruns --field-selector spec.storyRunRef.name=my-run
   kubectl get storyruns --field-selector spec.storyRef.name=my-story,status.phase=Failed
   kubectl get transportbindings --field-selector spec.transportRef=livekit
   ```
