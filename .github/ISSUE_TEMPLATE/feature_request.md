---
name: "Feature request"
about: "Pitch a new capability for the operator, transport, SDK, Engram, or docs"
labels: ["kind/feature"]
---

## Problem statement
What workflow or operational gap are you trying to solve? Include scale, latency, tenancy, or compliance constraints if relevant.

## Proposed change
Describe the behaviour you’d like to see. If this affects CRDs, Engram templates, or SDK APIs, list the new fields and defaults.

```
apiVersion: catalog.bubustack.io/v1alpha1
kind: EngramTemplate
spec:
  with:
    newField: ...
```

## Affected component(s)
- [ ] bobrapet
- [ ] bobravoz-grpc
- [ ] bubu-sdk-go
- [ ] Engram (name it)
- [ ] Impulse (name it)
- [ ] Docs / website

## Alternatives considered
What did you try already? Examples: custom Engram, Go Templates, external controller, different transport, etc.

## Additional context
Links, design docs, screenshots, or related issues/discussions.
