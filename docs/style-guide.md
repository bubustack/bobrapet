# Docs Style Guide

This guide keeps BubuStack documentation consistent, clear, and easy to scan.
Use it when adding or editing docs in `/docs/`.

## Voice and tone

- Be direct and concrete. Prefer short sentences.
- Use active voice and present tense.
- Avoid marketing language or vague claims.

## Terminology

- Use **BubuStack** (camel case) consistently.
- Use canonical resource names: `Story`, `StoryRun`, `StepRun`, `Engram`, `Impulse`.
- Prefer **real-time** (hyphenated) in prose. Keep `realtime-*` in example names if needed.

## Structure (recommended)

Use this structure for most docs:

1. Short intro paragraph (what the doc covers).
2. **Who this is for** (bulleted list).
3. **What you'll get** (bulleted list).
4. **At a glance** (optional for long docs).
5. Main content.
6. Examples.
7. Related docs.

For short reference pages, you can omit "At a glance."

## References and paths

- Use absolute doc paths like `/docs/runtime/inputs.md`.
- Keep links in code formatting when they are paths.
- When referencing a config key, wrap it in backticks.

## Examples

- Use `yaml` or `json` code blocks and keep them minimal.
- Show only fields relevant to the concept.
- Prefer real values over placeholders when possible.

Example (YAML):

```yaml
spec:
  policy:
    retries:
      maxRetries: 3
```

Example (JSON):

```json
{"version":"v1","type":"timeout","message":"step timed out"}
```

## Lists and tables

- Use short bullets.
- Use tables only when comparing or enumerating multiple fields.

## Consistency checks

Before submitting doc changes:

- Confirm links in `/docs/` resolve to existing files.
- Keep terminology aligned with resource names and CRD fields.
- Ensure examples match the current schema and defaults.
