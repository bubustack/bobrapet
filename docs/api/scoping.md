# Namespace and Scoping Rules

This document defines namespace-scoping rules for references.

## Who this is for

- Platform engineers defining cross-namespace access rules.
- Workflow authors using shared components or stories.

## What you'll get

- The allowed cross-namespace policies and their behavior.
- How to use ReferenceGrant to enable specific access.
- Common pitfalls and how validation is enforced.

## At a glance

- The operator controls cross-namespace access via `references.cross-namespace-policy`.
- Same-namespace references are always allowed; only cross-namespace refs are gated.
- `ReferenceGrant` in the **target namespace** allows specific incoming references.

## Cross-namespace policy

Cross-namespace references are controlled by the operator config key
`references.cross-namespace-policy` with these values:

- `deny` (default): cross-namespace references are rejected by admission validation.
- `grant`: cross-namespace references are allowed only when a `ReferenceGrant`
  in the **target namespace** permits the reference.
- `allow`: all cross-namespace references are allowed (use with care).

The policy applies to these references:

- `StoryRun.spec.storyRef`
- `Impulse.spec.storyRef`
- `Story` steps with `ref` (Engram references)
- `executeStory` steps (`with.storyRef.namespace`)
- `StepRun.spec.storyRunRef` and `StepRun.spec.engramRef` (when set)

Admission webhooks enforce the policy on create/update. Controllers also enforce
it at runtime when resolving references, so invalid cross-namespace refs are
rejected even if webhooks are disabled.

## ReferenceGrant (grant mode)

`ReferenceGrant` is a namespaced resource that allows references **into** its
namespace. It matches:

- `spec.from`: the referencing resource (group, kind, namespace)
- `spec.to`: the target resource (group, kind, optional name)

Example (allow StoryRuns in `workflows` to reference a shared Engram in
`shared`):

```yaml
apiVersion: policy.bubustack.io/v1alpha1
kind: ReferenceGrant
metadata:
  name: allow-shared-engram
  namespace: shared
spec:
  from:
    - group: runs.bubustack.io
      kind: StoryRun
      namespace: workflows
  to:
    - group: bubustack.io
      kind: Engram
      name: shared-engram
```

Cluster-scoped templates (`EngramTemplate`, `ImpulseTemplate`) are global and do
not use namespaces.

## Notes on payload references

Payload references (`$bubuStorageRef`, `$bubuConfigMapRef`, `$bubuSecretRef`)
are resolved at runtime. ConfigMap/Secret refs default to the workload namespace
when no namespace is provided.
