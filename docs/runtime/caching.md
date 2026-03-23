# Output Caching

BubuStack can reuse Step outputs based on a deterministic cache key. Caching is opt-in
and controlled via `execution.cache` on Story, Engram, or Step overrides.
Caching applies to batch Job execution; streaming/long-lived Deployments are not cached.

## Who this is for

- Workflow authors optimizing repeated batch steps.
- Operators tuning cache behavior and storage.

## What you'll get

- How cache keys are computed and used.
- The cache modes and when they apply.
- The interaction between caching and retries.

## Cache policy fields

`execution.cache` supports:

- `enabled`: turns caching on or off (default `true` when cache is set).
- `key`: cache key template. When empty, the resolved step inputs are hashed.
- `salt`: appended to cache keys to support explicit cache invalidation. Change this value to logically purge previous cache entries without deleting storage objects.
- `mode`: `read`, `write`, or `readWrite` (default).
- `ttlSeconds`: cache entry expiry (0 or negative means no expiry).

Cache key templates are evaluated with:

- `inputs`: the resolved step inputs (after template evaluation).
- `step`: `{id, run, story}`.
- `engram`: `{name, version}`.

## Storage backend

Cache entries are stored in the configured storage backend using the controller's
storage configuration. Entries are written under:

```
cache/<namespace>/<engram>/<version>/<hash>.json
```

The `<hash>` is a SHA-256 digest of the resolved cache key material.

If storage is disabled for the controller, cache reads and writes are skipped.

## Example

```yaml
apiVersion: bubustack.io/v1alpha1
kind: Story
metadata:
  name: cached-story
spec:
  pattern: batch
  policy:
    execution:
      cache:
        key: "{{ inputs.userId }}:{{ inputs.locale }}"
        mode: readWrite
        ttlSeconds: 3600
  steps:
  - name: summarize
    ref:
      name: summarize-engram
```

To scope caching to a single step, include step data in the key:

```yaml
execution:
  cache:
    key: "{{ step.id }}:{{ inputs.userId }}"
```
