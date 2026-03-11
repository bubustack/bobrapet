# Component Ecosystem Overview

This document summarizes how BubuStack's component ecosystem fits together,
including SDK usage, contracts, packaging/registry patterns, and reliability
semantics. It links to the detailed docs and repo references you'll use day-to-day.

## Who this is for

- Component authors building Engrams and Impulses.
- SDK users wiring triggers, inputs, and outputs.
- Platform operators packaging and distributing components.

## What you'll get

- Where the SDKs live and what they cover.
- The core contracts that define inputs, outputs, and errors.
- How components are packaged and discovered.
- The reliability guarantees you must design around.

---

## The rule: do one thing and do it well

An Engram does one job. A "fetch-and-summarize" Engram is two Engrams. A
"summarize-and-notify" Engram is two Engrams. If you can describe what it does
with "and", split it.

Stories compose Engrams into pipelines. This is the same principle as Unix: `curl`
fetches, `jq` transforms, `notify-send` alerts. Each program is small, testable,
and reusable. The shell (Story) wires them together.

When you follow this, every component becomes independently testable, swappable,
and reusable across Stories. When you don't, you get monoliths with implicit
coupling that break when requirements change.

---

## Quick start: authoring flow

1. Scaffold a component repo with `bubu component init`.
2. Implement an Engram or Impulse using `bubu-sdk-go`.
3. Define schemas in `EngramTemplate` or `ImpulseTemplate`.
4. Validate with `bubu-sdk-go/testkit` and `bubu-sdk-go/conformance`.
5. Publish to a Git-backed registry with `bubu publish`.

---

## Repo map (where to look)

- `tractatus/` — Protobuf contracts for gRPC transport.
- `core/` — Shared runtime contracts, templating engine, transport connector runtime.
- `bobrapet/` — CRDs, controllers, webhooks, and the runtime contract docs (`/docs/*`).
- `bubu-sdk-go/` — Go SDK, testkit, and conformance helpers.
- `bobravoz-grpc/` — gRPC bridge for external system integration.
- `bubuilder/` — Web console and REST API server.
- `bubu-registry/` — Git-backed registry and the `bubu` CLI.
- `engrams/` — Engram and Impulse component implementations.

For the full module dependency graph and layering rules, see
`/docs/overview/architecture.md`.

---

## Component SDKs

The Go SDK is the primary authoring surface for components:

- `bubu-sdk-go/README.md` contains usage patterns, runtime context, and configuration.
- `bubu-sdk-go/testkit/` provides a local harness for testing component behavior.
- `bubu-sdk-go/conformance/` provides contract-oriented test suites for batch and streaming.

Use the SDK to:

- Parse runtime context (story, run, step, transport settings).
- Emit outputs, logs, and structured errors.
- Integrate with streaming transports when enabled.

For a full development guide with interfaces, code examples, testing patterns,
and template definitions, see `/docs/sdk/building-engrams.md`.

---

## Contracts and schemas

Contracts live in two places:

- **CRD schemas** define the API surface for Stories, Runs, Engrams, and Impulses.
  These are authored in `bobrapet/api/**` and documented in `/docs/api/*`.
- **Runtime contracts** define environment variables, labels, annotations, and
  structured error payloads for SDKs. See `core/contracts` and `/docs/api/errors.md`.

Streaming components also follow the streaming message contract:

- `/docs/streaming/streaming-contract.md`
- `/docs/streaming/transport-settings.md`

---

## Packaging and registry patterns

BubuStack's registry uses Git-indexed metadata with versioned package manifests.
You can see examples in:

- `bubu-registry/examples/registry/namespaces/bubustack/index.yaml`
- `bubu-registry/examples/registry/namespaces/bubustack/packages/*/*.yaml`

Registry records capture package identity, versions, and metadata used by
operators and tooling to discover and resolve components.

The `bubu` CLI provides the scaffolding and publish workflow:

- `bubu component init` — create a starter repo with SDK wiring and templates.
- `bubu publish` — add or update a versioned template in a registry repo.

---

## Reliability semantics

Reliability guarantees are defined in `/docs/overview/durable-semantics.md`.
Key themes:

- StoryRun and StepRun creation are idempotent when deterministic IDs are used.
- Step execution is at-least-once; components must handle retries.
- Redrive and rerun behavior is driven by annotations and controller logic.

When designing components, assume retries, partial failure, and replays.
Use idempotency keys and structured errors to preserve correctness.

---

## Related reading

- `/docs/overview/core.md`
- `/docs/overview/durable-semantics.md`
- `/docs/api/errors.md`
- `/docs/runtime/primitives.md`
- `/docs/observability/overview.md`
