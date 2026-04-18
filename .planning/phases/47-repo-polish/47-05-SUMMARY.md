---
phase: 47-repo-polish
plan: "05"
subsystem: docs
tags: [docs, readme, content, CONTENT-11]
dependency_graph:
  requires: []
  provides: [directory-readmes]
  affects: [github-browsing-experience]
tech_stack:
  added: []
  patterns: [orientation-readme, module-role-doc]
key_files:
  created:
    - src/server/README.md
    - src/engine/README.md
    - src/state/README.md
  modified:
    - benchmark/README.md
decisions:
  - "deploy/README.md already existed with comprehensive content (125 lines) — preserved as-is rather than replacing with shorter template"
  - "watermarks.rs does not exist as a standalone file; WatermarkTracker is defined in event_time.rs — README corrected accordingly"
  - "run_matrix.sh does not exist on disk — omitted from benchmark index; fork-replay/ and recovery/ harnesses documented instead"
  - "beava.service is the real filename (not tally.service as in plan template) — README references the correct name"
metrics:
  duration_minutes: 8
  completed_date: "2026-04-18"
  tasks_completed: 2
  tasks_total: 2
  files_created: 3
  files_modified: 1
requirements_closed: [CONTENT-11]
---

# Phase 47 Plan 05: Directory READMEs Summary

Five directory READMEs written or extended so GitHub visitors browsing
`src/server/`, `src/engine/`, `src/state/`, `benchmark/`, and `deploy/`
find a 1-2 paragraph orientation with module role, key files, and read order.

## What Was Built

**Task 1 — src/ READMEs (3 new files)**

- `src/server/README.md`: HTTP + TCP server layer orientation. Names all 11
  files (`http.rs`, `http_ingest.rs`, `tcp.rs`, `auth.rs`, `protocol.rs`,
  `replica.rs`, `replica_client.rs`, `latency.rs`, `throughput.rs`,
  `signals.rs`, `shard_probe.rs`), explains the `EngineHandle` boundary, and
  gives a read order.
- `src/engine/README.md`: Pipeline engine orientation. Names all 10 files
  (`pipeline.rs`, `event_time.rs`, `operators.rs`, `window.rs`, `expression.rs`,
  `register.rs`, `recommend.rs`, `hll.rs`, `cms.rs`, `uddsketch.rs`). Notes
  CORR-01 fix location. Corrects plan template: `WatermarkTracker` lives in
  `event_time.rs`, not a separate `watermarks.rs`.
- `src/state/README.md`: Persistent state orientation. Names all 5 files
  (`event_log.rs`, `snapshot.rs`, `eviction.rs`, `eviction_tracker.rs`,
  `store.rs`). Documents `append_with_fsync` scaffold and CORR-07 eviction
  clock fix.

**Task 2 — benchmark/ extension + deploy/ (1 extended, 1 pre-existing)**

- `benchmark/README.md` EXTENDED: Added "Module role (orientation)" section
  with full sub-harness index table covering fraud-pipeline, replay, recovery,
  fork-replay, and http_load.sh. Updated the intro table (was "Two benchmarks",
  now 5 entries). All prior content preserved.
- `deploy/README.md`: Already existed with 125 lines of comprehensive content
  (beava.service, Caddyfile, provision.sh, smoke.sh, admin access, file layout,
  trade-offs). Preserved as-is — it already exceeds all acceptance criteria.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| 1 | `4b40284` | docs(47-05): directory READMEs for src/server, src/engine, src/state (CONTENT-11, D-31) |
| 2a | `385bec2` | docs(47-05): extend benchmark/README.md with module role + sub-harness list (CONTENT-11) |
| 2b | (pre-existing) | deploy/README.md already committed — no change needed |

## Existence and Line-Count Verification

| File | Exists | Lines | Min |
|------|--------|-------|-----|
| src/server/README.md | yes | 40 | 20 |
| src/engine/README.md | yes | 37 | 20 |
| src/state/README.md | yes | 41 | 20 |
| benchmark/README.md | yes | 263 | 50 |
| deploy/README.md | yes | 125 | 20 |

## Deviations from Plan

**1. [Rule 1 - Bug] watermarks.rs referenced in plan template does not exist**
- Found during: Task 1, engine README
- Issue: Plan template referenced `watermarks.rs` as a standalone file. Actual
  code has `WatermarkTracker` defined in `event_time.rs:212`.
- Fix: README references `event_time.rs` for `WatermarkTracker` — matches disk.
- Files modified: src/engine/README.md

**2. [Rule 1 - Bug] tally.service referenced in plan template does not exist**
- Found during: Task 2, deploy README review
- Issue: Plan template references `tally.service`; actual file is `beava.service`.
- Fix: Not applicable — existing deploy/README.md already correctly references
  `beava.service`.

**3. [Rule 1 - Bug] run_matrix.sh does not exist on disk**
- Found during: Task 2, benchmark README
- Issue: Plan template referenced `run_matrix.sh`; file is absent from benchmark/.
- Fix: Omitted from sub-harness index. `fork-replay/` and `recovery/` harnesses
  documented instead (both exist and have run scripts).

**4. deploy/README.md already existed**
- Found during: Task 2 pre-read
- Issue: Plan said "probably absent" but a 125-line file existed covering all
  acceptance criteria.
- Fix: Preserved existing file. Plan's commit 2 ("deploy/README.md new") is a
  no-op; acceptance criteria already met.

## Requirements Closed

- CONTENT-11: Directory READMEs for src/server, src/engine, src/state,
  benchmark (extend), deploy — all five directories now have a README.

## Self-Check: PASSED

Files verified to exist:
- src/server/README.md: FOUND
- src/engine/README.md: FOUND
- src/state/README.md: FOUND
- benchmark/README.md: FOUND
- deploy/README.md: FOUND

Commits verified:
- 4b40284: FOUND
- 385bec2: FOUND
