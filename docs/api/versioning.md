# Versioning and Pinning

This document defines how workflow and component versioning works in BubuStack.

## Who this is for

- Workflow authors pinning Stories, Engrams, or Impulses.
- Operators managing upgrades and compatibility.

## What you'll get

- How `spec.version` is used in references.
- Which resources support version pinning.
- The expectations for compatibility across releases.

## At a glance

- `spec.version` is a contract label, not an automatic migration mechanism.
- References can pin to a version and are rejected if the target version differs.
- Pin when you need stability across upgrades or shared components.

## Story versioning

Stories can declare an explicit version:

```yaml
spec:
  version: "v1"
```

`spec.version` is an operator-level contract identifier. It does not imply
automatic migration or backward compatibility.

## Pinning references

References can optionally pin to a version. When set, admission webhooks verify
the target resource’s `spec.version` (or template version) matches.

Supported pinning fields:

- `storyRef.version` (StoryRun / Impulse / executeStory steps)
- `step.ref.version` (Engram references in Story steps)
- `templateRef.version` (Engram / Impulse template references)

If the target version does not match, the reference is rejected.

## Pinning example

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: process-orders
spec:
  version: "v2"
  steps:
  - name: charge
    ref:
      name: payments
      version: "1.4.0"
  - name: notify
    type: executeStory
    with:
      storyRef:
        name: send-receipt
        version: "v1"
```

## Resource-level migration

There is **no automatic migration** of resource-level versions today. If you
need to change schemas or runtime behavior:

1. Create a new Story/Engram/Impulse with a new `spec.version`.
2. Update references to pin the new version.
3. Remove or archive the old version when it is no longer used.

This keeps version changes explicit and prevents implicit behavior drift.

## CRD migration strategy

For the CRD API version lifecycle (`v1alpha1` → `v1beta1` → `v1`), upgrade
procedures, conversion webhook plans, and deprecation policy, see
`/docs/api/migration.md`.
