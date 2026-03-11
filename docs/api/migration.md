<!--
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# CRD Migration Strategy

This document defines the API version lifecycle, the upgrade procedure between
versions, and the planned introduction of conversion webhooks.

For resource-level versioning (`spec.version` and pinning), see
`/docs/api/versioning.md`.

## Who this is for

- Operators planning upgrades across CRD versions.
- Platform engineers defining conversion/webhook rollout.

## What you'll get

- The API lifecycle stages and their guarantees.
- The expected upgrade sequence between versions.
- How conversion webhooks fit into the plan.

## At a glance

- Today all CRDs are served as `v1alpha1` with no conversion webhook.
- Breaking changes in alpha may require manual re-creation after upgrade.
- Conversion webhooks arrive with `v1beta1` to enable multi-version serving.

---

## API version lifecycle

All BubuStack CRDs follow the Kubernetes API maturity convention:

| Stage | API version | Stability guarantee | Breaking changes |
| --- | --- | --- | --- |
| Alpha | `v1alpha1` (current) | None. Fields, defaults, and status shapes may change between releases. | Allowed in any release. |
| Beta | `v1beta1` (planned) | Schema is feature-complete. Breaking changes only with a migration path. | Rare; announced at least one release in advance. |
| GA | `v1` (planned) | Full backward compatibility within the major version. | None within `v1`. |

The same lifecycle applies to every API group:

- `bubustack.io` — Story, Engram, Impulse
- `runs.bubustack.io` — StoryRun, StepRun
- `catalog.bubustack.io` — EngramTemplate, ImpulseTemplate
- `transport.bubustack.io` — Transport, TransportBinding
- `policy.bubustack.io` — ReferenceGrant

API groups may advance independently. For example, `catalog.bubustack.io` may
reach `v1beta1` before `transport.bubustack.io`.

---

## Current state (`v1alpha1`)

All CRDs are served as `v1alpha1` only. There is a single storage version and
no conversion webhook. This is the expected state for an alpha API.

What this means for operators:

- **No backward-compatibility guarantee.** A new release may change field names,
  validation rules, default values, or status shapes.
- **No automatic migration.** Existing resources are not rewritten when the
  operator is upgraded.
- **Manual re-creation may be required.** If a release changes the stored
  schema, you must delete and recreate affected resources (see the upgrade
  procedure below).

---

## Planned progression

### v1alpha1 → v1beta1

**Trigger:** core field set is stable and no open design questions remain for
Story, StoryRun, StepRun, Engram, and Impulse.

**What changes:**

1. A new `v1beta1` package is introduced alongside `v1alpha1`.
2. `v1beta1` becomes the **storage version** (the version stored in etcd).
3. A **conversion webhook** is deployed. It converts between `v1alpha1` and
   `v1beta1` in both directions so existing clients can continue using
   `v1alpha1` during the transition.
4. `v1alpha1` is marked as deprecated but remains served for at least two
   releases after `v1beta1` ships.
5. Breaking changes from alpha are documented in the release notes with a
   migration section.

**What operators must do:**

1. Upgrade the operator (Helm chart or kustomize) to the release that
   introduces `v1beta1`.
2. Verify the conversion webhook is healthy:
   ```bash
   kubectl get validatingwebhookconfigurations,mutatingwebhookconfigurations \
     -l app.kubernetes.io/name=bobrapet
   ```
3. Existing `v1alpha1` resources are served transparently via the conversion
   webhook. No manual migration is required.
4. Migrate client code and manifests to `v1beta1` before the deprecation
   window closes.
5. After all manifests use `v1beta1`, the operator can be upgraded to a release
   that drops `v1alpha1`.

### v1beta1 → v1

**Trigger:** the API has been stable through at least two beta releases with no
breaking changes.

**What changes:**

1. A new `v1` package is introduced. `v1` becomes the storage version.
2. The conversion webhook converts between `v1beta1` and `v1`.
3. `v1beta1` is deprecated but served for at least two releases.
4. `v1alpha1` is removed (no longer served).

**What operators must do:**

1. Upgrade the operator.
2. Existing `v1beta1` resources are converted on the fly.
3. Migrate manifests to `v1`.

---

## Conversion webhook design

Conversion webhooks will follow the Kubebuilder **hub-and-spoke** pattern:

- **Hub version:** the storage version (e.g., `v1beta1` or `v1`).
- **Spoke versions:** older served versions that convert to/from the hub.

Implementation plan:

1. Scaffold conversion webhooks:
   ```bash
   kubebuilder create webhook --group bubustack.io --version v1beta1 \
     --kind Story --conversion --spoke v1alpha1
   ```
2. Implement `ConvertTo()` and `ConvertFrom()` on spoke types.
3. Handle field additions with sensible defaults (new fields absent in the
   spoke are set to their zero value or schema default).
4. Handle field removals by storing removed values in an annotation
   (`bubustack.io/v1alpha1-fields`) so round-trip conversion is lossless.
5. Test conversion round-trips:
   `v1alpha1 → hub → v1alpha1` must be identical to the original.

Conversion webhooks are **not scaffolded today** because there is only one
served version. They will be introduced in the same release that adds
`v1beta1`.

---

## Upgrade procedure (current, v1alpha1-only)

Because there is no conversion webhook yet, upgrades between `v1alpha1`
releases that contain breaking changes require manual steps.

### Non-breaking releases (patch / minor with no schema changes)

1. Upgrade the operator image (Helm `helm upgrade` or `make deploy`).
2. The new controller reconciles existing resources without changes.
3. No resource re-creation needed.

### Breaking releases (field renames, removed fields, changed defaults)

1. **Read the release notes.** Every breaking change is documented with a
   "Migration" section.
2. **Drain in-flight runs.** Wait for running StoryRuns to finish or cancel
   them:
   ```bash
   kubectl get storyruns -A --field-selector 'status.phase!=Succeeded,status.phase!=Failed'
   ```
3. **Export resources** (optional safety net):
   ```bash
   kubectl get stories,engrams,impulses -A -o yaml > backup.yaml
   ```
4. **Upgrade the operator.**
5. **Re-apply updated manifests.** Adjust manifests according to the migration
   notes and apply:
   ```bash
   kubectl apply -f manifests/
   ```
6. **Verify.** Check that resources are `Ready`:
   ```bash
   kubectl get stories,engrams,impulses -A
   ```

### CRD-only upgrades

When only the CRD schema changes (new optional fields, stricter validation):

1. Apply the new CRDs:
   ```bash
   kubectl apply --server-side -f config/crd/bases/
   ```
2. Upgrade the operator.
3. Existing resources remain valid. New fields take their default values.

---

## Deprecation policy

| Stability level | Minimum served duration after deprecation |
| --- | --- |
| Alpha (`v1alpha*`) | 0 releases (may be removed immediately) |
| Beta (`v1beta*`) | 2 releases after deprecation announcement |
| GA (`v1`) | 12 months or 4 releases, whichever is longer |

Deprecation is announced in:
- Release notes
- CRD `deprecated` field and `deprecationWarning` message
- Operator startup logs

---

## Multi-component coordination

BubuStack spans multiple components:

| Component | Repo | CRDs owned |
| --- | --- | --- |
| bobrapet | `bubustack/bobrapet` | Story, Engram, Impulse, StoryRun, StepRun, EngramTemplate, ImpulseTemplate, ReferenceGrant |
| bobravoz-grpc | `bubustack/bobravoz-grpc` | Transport, TransportBinding |
| bubu-sdk-go | `bubustack/bubu-sdk-go` | (none — client library) |

When a CRD version changes:

1. The operator that owns the CRD ships the new version first.
2. The SDK releases a compatible version that supports both old and new API
   versions.
3. Other operators (e.g., bobravoz-grpc) update their client imports.

Compatibility matrix between component versions will be published in the Helm
chart's `README.md` and in the release notes.

---

## FAQ

**Q: Will my StoryRuns survive an operator upgrade?**
A: Yes, for non-breaking releases. For breaking releases during `v1alpha1`,
in-flight runs should be drained first. Once conversion webhooks are in place
(`v1beta1`+), runs are converted transparently.

**Q: Can I run two API versions simultaneously?**
A: Not today (single version). Once `v1beta1` ships, both `v1alpha1` and
`v1beta1` will be served concurrently via the conversion webhook.

**Q: How do I know if a release is breaking?**
A: The Helm chart changelog and GitHub release notes tag breaking changes with
a `BREAKING:` prefix. The `CHANGELOG.md` uses Conventional Commits.

**Q: Do I need to update my SDK when the CRD version changes?**
A: The SDK tracks the operator's API version. When `v1beta1` ships, a new SDK
release will support both versions. Pin your SDK version to the compatible
range listed in the release notes.
