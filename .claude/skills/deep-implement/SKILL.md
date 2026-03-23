---
name: deep-implement
description: Structured implementation workflow — research, plan, annotate, todo, implement. Use when the user wants to build a feature, fix a complex bug, or make significant changes. Invoked with /deep-implement or when user says "deep implement", "full workflow", or "research and plan this".
---

<command-name>deep-implement</command-name>

# Deep Implement — Structured Development Workflow

A disciplined pipeline that separates thinking from typing. Never write code until a written plan has been reviewed and approved.

-----

## ⛔ PHASE GATE RULES (MANDATORY — READ FIRST)

**This workflow has hard gates between every phase. Claude MUST NOT advance to the next phase unless the user provides an explicit approval keyword.**

|From → To        |User must say (exactly or equivalent)                    |
|-----------------|---------------------------------------------------------|
|Research → Plan  |"approve research", "research looks good", "move to plan"|
|Plan → Annotate  |(User adds annotations and says "check the plan")        |
|Annotate → Todo  |"approve plan", "plan looks good", "move to todo"        |
|Todo → Tests     |"approve todo", "todo looks good", "write tests"         |
|Tests → Implement|"implement", "start implementing", "go"                  |

**What counts as approval:**

- The user explicitly stating one of the trigger phrases above (or a clear equivalent)
- The user saying "yes" or "approved" in direct response to Claude asking "shall I proceed to [next phase]?"

**What does NOT count as approval:**

- Silence
- The user asking a question about the current phase
- The user giving feedback or corrections (this means they are still reviewing)
- The user saying "looks good" about a specific section (not the whole phase)
- Claude's own judgment that the output is "good enough"

**If in doubt, do NOT advance. Ask explicitly:**

> "Do you want me to proceed to [next phase], or do you have more feedback on the [current phase]?"

-----

## Artifact Location & Naming

All research and plan files go in `./docs/deep-research/` (relative to the project root). This directory must be in `.gitignore` — if it's not, add it before writing any files.

**File naming convention:**

- Research: `{feature-name}—research—{DD-MM-YY}.md`
- Plan:     `{feature-name}—plan—{DD-MM-YY}.md`
- Feedback: `{feature-name}—feedback—{DD-MM-YY}.md`

**{feature-name}** = max 2 words, lowercase, hyphen-separated.

-----

## Ongoing: Feedback Capture (All Phases)

Throughout **every phase**, whenever the user provides feedback that is **generic and widely applicable** (not task-specific):

1. Append to `docs/deep-research/{feature-name}—feedback—{DD-MM-YY}.md`
2. Write in constructive, actionable form
3. Categorize (e.g. "Architecture", "Testing", "Code Style", "Kubebuilder Patterns")

-----

## Phase 1: Research

**Goal:** Deeply understand the relevant part of the codebase before doing anything else.

### What to do

1. Read **every** relevant file thoroughly — implementations, not just interfaces
2. Trace the full flow: types → controller → webhook → status updates
3. Identify existing patterns: reconciliation style, error handling, test helpers
4. Check `docs/` for relevant design docs (expressions, lifecycle, payloads, etc.)
5. Note integration points with other ecosystem modules (core, SDK, bobravoz-grpc)
6. Write findings to `docs/deep-research/{feature-name}—research—{DD-MM-YY}.md`

### research.md structure

```markdown
# Research: [Topic]

## Overview
## File Map
## Data Flow
## Patterns & Conventions
## Integration Points (cross-module)
## Gotchas & Edge Cases
## Open Questions
```

### ⛔ HARD STOP — End of Phase 1

> Research complete — written to `docs/deep-research/{name}—research—{DD-MM-YY}.md`. Please review.
> **Say "approve research" or "move to plan" when satisfied.**

-----

## Phase 2: Plan

**Goal:** Write a detailed implementation plan based on approved research.

### Go/Kubebuilder-specific plan requirements

- Reference exact file paths and function names from the codebase
- If touching `*_types.go`: include `make manifests generate` step
- Include Testing Strategy section (mandatory) with:
  - envtest setup if testing controllers
  - Table-driven test cases following existing patterns
  - Fake client setup (`fake.NewClientBuilder().WithScheme(...)`)
- Note any cross-module impact (does this change affect SDK or bobravoz-grpc?)

### plan.md structure

```markdown
# Plan: [Feature/Change Name]

## Summary
## Changes
### [Area 1]
**File:** `exact/path.go`
**What:** [description]
(code snippet)

## New Files
## Testing Strategy
### Test files
### Key test cases (table-driven)
### Testing patterns to follow
## Cross-Module Impact
## Trade-offs & Alternatives
## Risks
```

### ⛔ HARD STOP — End of Phase 2

> Plan written to `docs/deep-research/{name}—plan—{DD-MM-YY}.md`. Review and annotate.
> **Say "check the plan" after adding notes, or "approve plan" if good as-is.**

-----

## Phase 3: Annotate (Repeat 1–6x)

Read plan file, address **every** user annotation, update plan, remove annotation markers. Repeat until user approves.

-----

## Phase 4: Todo List

Append granular task breakdown to plan file. Tests come before implementation.

```markdown
## Todo List

### Phase 1: [Setup / Prerequisites]
- [ ] Task description

### Phase 2: Tests (TDD)
- [ ] Write test: `path/to/test.go`
- [ ] Run `make test` — confirm tests fail for the right reasons

### Phase 3: [Implementation]
- [ ] Implement change in `path/to/file.go`
- [ ] Run `make test` — confirm tests pass
- [ ] Run `make lint-fix`

### Phase 4: [Generation / Finalization]
- [ ] Run `make manifests generate` (if types changed)
- [ ] Run full `make test`
- [ ] Run `make lint`
```

### ⛔ HARD STOP — say "write tests" or "approve todo" to proceed.

-----

## Phase 5: Tests (TDD)

Write failing tests before implementation.

### Go/Kubebuilder test conventions

- Use envtest for controller tests: `testEnv = &envtest.Environment{CRDDirectoryPaths: ...}`
- Use `fake.NewClientBuilder().WithScheme(scheme).WithObjects(...).Build()` for unit tests
- Table-driven tests with `wantFields []string` for webhook validation assertions
- Use existing helpers: `causeFields(err)`, `containsField()`, `failingReader`, `slowReader`
- Run `make test` to verify tests fail for the right reasons (compilation errors = fix first)

### ⛔ HARD STOP — say "implement" to proceed.

-----

## Phase 6: Implement

Execute the plan mechanically. Mark progress: `- [ ]` → `- [x]`.

### Go/Kubebuilder implementation rules

- After editing `*_types.go`: run `make manifests generate` immediately
- After any Go edit: `make fmt` then targeted `go test ./path/...`
- Never edit generated files (`zz_generated.*.go`, `config/crd/bases/*.yaml`)
- Never remove `// +kubebuilder:scaffold:*` markers
- Use Makefile targets for all operations (never raw docker/kind/kubectl mutating commands)
- Run `make lint-fix` after each implementation phase
- All tests from Phase 5 must pass by the end

-----

## Phase 7: Feedback & Iterate

Apply corrections immediately and concisely.

-----

## Phase 8: Lessons Learned

Ask user if they want a lessons-learned pass. If yes:
1. Read feedback file
2. Filter for broadly applicable insights
3. Update CLAUDE.md in the appropriate section
