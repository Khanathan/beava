---
phase: 47-repo-polish
plan: "07"
subsystem: docs
tags: [docs, getting-started, concepts, operations, onboarding, durability, event-time]
dependency_graph:
  requires: [47-01]
  provides: [docs/getting-started.md, docs/concepts.md, docs/operations.md]
  affects: [README.md cross-links, docs/event-time.md cross-links, docs/http-api.md cross-links]
tech_stack:
  added: []
  patterns: [plain markdown docs, cross-linked reference system]
key_files:
  created:
    - docs/getting-started.md
    - docs/concepts.md
    - docs/operations.md
  modified:
    - docs/quickstart.md
decisions:
  - "quickstart.md replaced with 3-line redirect to getting-started.md; content superseded by Docker-first walkthrough"
  - "Port in getting-started examples is 6900 (HTTP) per Dockerfile / D-01; http-api.md still shows legacy 6401 (noted but not fixed — separate plan)"
  - "operations.md durability table documents HTTP group-commit window as ~1ms; ?sync=1 semantics preserved from http-api.md"
metrics:
  duration: "~25 min"
  completed: "2026-04-17"
  tasks_completed: 3
  tasks_total: 3
  files_created: 3
  files_modified: 1
---

# Phase 47 Plan 07: Core Docs — Getting Started, Concepts, Operations Summary

**One-liner:** Three production-quality docs pages covering 60-second Docker onboarding, six-primitive concepts primer, and pre-deploy operations reference — all cross-linked to event-time.md and http-api.md.

## What was built

### docs/getting-started.md (135 lines)

60-second Docker-first walkthrough (CONTENT-02, D-22):

- Prerequisites: Docker + curl only
- Step 1: `docker run -p 6900:6900 beavadb/beava:latest`
- Step 2: push event via HTTP (auto-registers stream)
- Step 3: read features via HTTP
- Step 4: Python SDK optional path (`pip install beava`, `@bv.stream`, `@bv.table`, `bv.App`)
- "What just happened" explanation linking to operations.md and event-time.md
- Persistent-data path via `examples/docker-compose.yml`
- Troubleshooting table for 4 common failure modes
- Cross-links: concepts.md, operations.md, event-time.md, http-api.md, python-sdk.md, examples/

### docs/concepts.md (239 lines)

Six-primitive primer (CONTENT-03, D-23):

- Stream: `@bv.stream`, auto-registration via HTTP, keyless vs keyed, WAL as durability primitive
- Table: `@bv.table`, per-entity row, TTL driven by event-time watermark
- Operator: all 16 built-ins in a reference table; sliding windows, event-time bucket expiry
- Fork: `bv.fork` context manager, LOG_FETCH catchup + SUBSCRIBE live-tail, data isolation, shadow-mode comparison
- Event time: why event-time-first matters (backfill, late arrivals, batch correctness); CORR-01 reference
- Watermarks: lateness window, per-stream override, γ-propagation, join idle-input caveat (v1.0)
- "Putting it together" end-to-end data-flow diagram

### docs/operations.md (308 lines)

Pre-deploy reference (CONTENT-05, D-27):

- Sizing: memory back-of-envelope table (3 pipeline shapes), disk, ports, CPU contention
- Durability: at-least-once model, fsync policy per ingest path (TCP / HTTP single / batch / NDJSON), `?sync=1` semantics, scaffolded fsync hook
- Crash recovery: ungraceful crash behavior, recovery sequence (base + deltas + WAL replay), crash-replay determinism cross-link, operator runbook with readiness polling
- Snapshot cycle: base/delta cadence table, manual trigger commands
- Tuning knobs: 11-entry env var table including `BEAVA_ADMIN_TOKEN`, `BEAVA_MEMORY_LIMIT_MB`, `BEAVA_HTTP_PORT`
- Observability: Prometheus metrics table with alert guidance, ring_buffer_drops vs late_events distinction, admin probes table (health / ready / warnings)
- Scaling posture: single-node today, Cloud roadmap, fork as read-scaling primitive
- Security baseline, Deployment (Docker / Compose / systemd / K8s), Next reading

### docs/quickstart.md (3 lines)

Replaced with redirect to getting-started.md. Previous content was Python-SDK-first and source-build-first; superseded.

## Cross-links verified

| From | To | Status |
|------|----|--------|
| getting-started.md | docs/event-time.md | Exists (Phase 46 Plan 08) |
| getting-started.md | docs/concepts.md | Written this plan |
| getting-started.md | docs/operations.md | Written this plan |
| getting-started.md | docs/http-api.md | Exists (Phase 45 Plan 05) |
| getting-started.md | docs/python-sdk.md | Exists |
| getting-started.md | examples/docker-compose.yml | Exists (Plan 47-01) |
| concepts.md | docs/event-time.md | Exists (Phase 46 Plan 08) |
| concepts.md | docs/operators.md | Exists |
| concepts.md | docs/architecture.md | Exists (Plan 47-08) |
| concepts.md | docs/operations.md | Written this plan |
| operations.md | docs/event-time.md#crash-replay-determinism | Exists (D-26) |
| operations.md | docs/architecture.md | Exists (Plan 47-08) |
| operations.md | docs/http-api.md | Exists |
| operations.md | examples/docker-compose.yml | Exists (Plan 47-01) |
| operations.md | deploy/ | Exists |
| operations.md | benchmark/ | Exists |

## Commits

| Task | Commit | Files | Description |
|------|--------|-------|-------------|
| 1 | `97e55ee` | docs/getting-started.md, docs/quickstart.md | 60-second Docker→push→read walkthrough (CONTENT-02, D-22) |
| 2 | `a258b1f` | docs/concepts.md | Stream/Table/Operator/Fork/Event-time/Watermarks primer (CONTENT-03, D-23) |
| 3 | `dd868db` | docs/operations.md | Sizing, durability, recovery, tuning reference (CONTENT-05, D-27) |

## Requirements closed

- CONTENT-02: `docs/getting-started.md` — 60-second Docker → push → read (D-22)
- CONTENT-03: `docs/concepts.md` — six-primitive primer with event-time cross-links (D-23)
- CONTENT-05: `docs/operations.md` — sizing, durability, crash-recovery, tuning reference (D-27)

## Deviations from Plan

None — plan executed exactly as written.

The plan's template content was used as the structural skeleton; section wording was written fresh against the actual codebase (Dockerfile ports confirmed as 6900/6400, WAL fsync policy confirmed against src/state/event_log.rs and src/server/README.md, operator list confirmed against src/engine/operators.rs and src/engine/README.md).

## Known Stubs

None. All content is grounded in confirmed codebase facts (ports, operator names, env vars, snapshot cadence defaults, benchmark numbers).

## Self-Check: PASSED

- `docs/getting-started.md`: EXISTS, 135 lines (>= 80)
- `docs/concepts.md`: EXISTS, 239 lines (>= 120)
- `docs/operations.md`: EXISTS, 308 lines (>= 150)
- Commits `97e55ee`, `a258b1f`, `dd868db`: all present in git log
