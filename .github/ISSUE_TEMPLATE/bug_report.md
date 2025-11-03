---
name: "Bug report"
about: "Report a reproducible issue in an operator, transport, SDK, Engram, or Impulse"
labels: ["kind/bug"]
---

## Component(s)
- [ ] bobrapet (Story/StoryRun controllers)
- [ ] bobravoz-grpc (transport operator)
- [ ] bubu-sdk-go
- [ ] Engram (name it below)
- [ ] Impulse (name it below)
- [ ] Docs / website

If Engram/Impulse:
```
name:
version/tag:
execution mode (job / deployment / statefulset):
```

## What happened?
Tell us what broke. Include the Story/StoryRun status, the expected behaviour, and what you observed instead.

## Minimal reproduction
1. Inputs/Story snippet (YAML or JSON)
2. Commands you ran (`kubectl`, `make`, etc.)
3. Cluster details (Kubernetes version, Kind/Minikube/managed cluster)

```
apiVersion: stories.bubustack.io/v1alpha1
kind: Story
metadata:
  name: example
spec:
  ...
```

## Logs & traces
- `kubectl logs` for controllers or Engrams (set `BUBU_DEBUG=true` if possible)
- Relevant excerpts from `storyrun` / `steprun` status
- TransportBinding / bobravoz logs if streaming is impacted

## Additional context
Anything else we should know? For example, custom overrides, secrets/providers, or recent upgrades.
