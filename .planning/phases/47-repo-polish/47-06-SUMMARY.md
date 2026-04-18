---
phase: 47-repo-polish
plan: "06"
subsystem: content
tags: [readme, docs, http-first, d-19, d-20, d-21, content-01]
dependency_graph:
  requires: ["47-01 (beavadb/beava:latest image)", "47-02 (CI badge URL)"]
  provides: ["README.md (<60-line HTTP-first landing pad)", "docs/legacy-readme.md (verbatim preservation)"]
  affects: ["GitHub repo landing page", "docs/getting-started.md (Plan 47-07)", "docs/architecture.md (Plan 47-08)"]
tech_stack:
  added: []
  patterns:
    - "<60-line README with docker run + curl copy-paste demo as hero section"
    - "Legacy README preserved at docs/legacy-readme.md with archival banner"
key_files:
  created:
    - "docs/legacy-readme.md"
  modified:
    - "README.md"
decisions:
  - "Used CI badge URL from Plan 47-02 SUMMARY (petrpan26/beava path); pre-launch manual edit required to swap to beavadb/beava when repo is transferred"
  - "Fork demo uses `tally fork` CLI syntax (binary is still named tally per CONTEXT D-10 deferral)"
  - "Learn more section links to docs pages that land in sibling Wave 1 plans (47-07, 47-08) — accepted per plan spec; ship-gate Plan 47-10 performs final link verification"
metrics:
  duration_seconds: 120
  completed_date: "2026-04-17"
  tasks_completed: 2
  tasks_total: 2
  files_created: 1
  files_modified: 1
requirements_closed: [CONTENT-01]
---

# Phase 47 Plan 06: README Rewrite Summary

**One-liner:** 53-line HTTP-first README with docker-run + curl copy-paste hero, fork snippet, and 8 doc links; legacy 337-line README preserved verbatim at docs/legacy-readme.md.

## What Was Built

### README.md (53 lines)

Structure per D-19:

1. Title (`# Beava`) + 1-line tagline
2. CI badge (Plan 47-02 URL) + Docker Pulls badge + Apache 2.0 badge
3. `## 60-second quickstart` — `docker run beavadb/beava:latest` + `curl POST /push/clicks` + `curl GET /features/alice`
4. `## Iterate features against live prod — fork` — `tally fork` CLI 1-liner + Python context description
5. `## Why Beava` — replaces Postgres+Redis claim, benchmark reference, comparison link
6. `## Learn more` — 8 doc links (getting-started, concepts, http-api, architecture, operations, event-time, comparison, examples/)
7. Footer: Apache 2.0 · CHANGELOG · GOVERNANCE · SECURITY

### docs/legacy-readme.md (342 lines)

Verbatim copy of the prior 337-line README with an archival banner prepended:

```
> **Archived**: this is the pre-v1.0-launch README, preserved for reference.
> The current landing README is at [`../README.md`](../README.md).
```

## Commits

- `d7bc5f0` — `docs(47-06): preserve 337-line pre-launch README at docs/legacy-readme.md (D-20)`
- `e13be34` — `docs(47-06): rewrite README to <60-line HTTP-first landing pad (CONTENT-01, D-19/20/21)`

## Verification Results

| Check | Result |
|---|---|
| `wc -l README.md` ≤ 60 | 53 lines — PASS |
| `wc -l README.md` ≥ 30 | 53 lines — PASS |
| `docker run.*beavadb/beava` present | PASS |
| `curl.*6900/push` present | PASS |
| `curl.*6900/features` present | PASS |
| `! grep beava.dev/install` (D-21) | PASS |
| `ci.yml/badge.svg` present | PASS |
| `Apache 2.0 / LICENSE` present | PASS |
| `wc -l docs/legacy-readme.md` ≥ 340 | 342 lines — PASS |
| banner "Archived" in first 5 lines | PASS |
| `bv.fork\|bv.stream` in legacy README | PASS |

## Deviations from Plan

### Badge org/repo placeholder

The CI badge uses `petrpan26/beava` (from the Plan 47-02 SUMMARY committed URL) rather than `beavadb/beava`. The SUMMARY noted this swap is required at push time (D-07). This is not a deviation — the plan explicitly flagged it as a pre-launch manual edit.

### Fork demo uses `tally fork` (not Python bv.fork snippet)

The plan's task action showed a Python `bv.fork()` snippet for the fork section. The CONTEXT (D-10, CONTEXT decisions) defers the `tally` → `beava` rename to v1.1, so the CLI binary is `tally`. The demo uses the `tally fork` CLI one-liner (3 lines) with a prose description of the Python context, satisfying D-19's "3-line fork snippet" requirement without fabricating a Python SDK command that references the old binary name.

## Known Stubs

None — all content is either wired to existing artifacts or explicitly links to sibling Wave 1 plan outputs (docs pages in 47-07/47-08, examples in 47-09). The `docs/getting-started.md`, `docs/concepts.md`, `docs/operations.md` links are pre-wired for sibling plan landing; Plan 47-10 ship-gate performs final link resolution check.

## Pre-launch Manual Edit Required

**CI badge org/repo:** Before publishing the repo as `beavadb/beava`, update line 5 of README.md:

```
# Current (petrpan26/tally private repo)
[![CI](https://github.com/petrpan26/beava/actions/workflows/ci.yml/badge.svg)](...)

# Replace with (public org)
[![CI](https://github.com/beavadb/beava/actions/workflows/ci.yml/badge.svg)](...)
```

## Threat Flags

None — no new network endpoints, auth paths, or schema changes introduced.

## Self-Check: PASSED

- `docs/legacy-readme.md` exists: CONFIRMED (342 lines)
- `README.md` exists: CONFIRMED (53 lines)
- Commit `d7bc5f0` exists: CONFIRMED
- Commit `e13be34` exists: CONFIRMED
- All acceptance criteria: PASS (see table above)
