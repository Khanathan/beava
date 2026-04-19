---
phase: 52-event-log-recovery-ship-gate
plan: "09"
subsystem: docs
tags: [tpc, docs, architecture, operations, shard-sizing, ship-gate]
dependency_graph:
  requires:
    - 52-08 (pareto-c8-x8 benchmark cell — ship-gate criterion 3 implementation)
    - 52-07 (sharding parity proptest — TPC-CORR-05)
  provides:
    - docs/architecture-tpc.md (full TPC explainer, 8 sections)
    - docs/operations.md shard sizing section (6 subsections)
    - TPC-DX-04 fully satisfied
  affects:
    - New contributor onboarding path (architecture-tpc.md is the TPC entry point)
    - Operator runbook (operations.md now has shard diagnosis flow)
tech_stack:
  added: []
  patterns:
    - Verbatim ASCII diagram from design doc included in architecture doc
    - Operations runbook with 4-step diagnosis flow + threshold tuning table
key_files:
  created:
    - docs/architecture-tpc.md
  modified:
    - docs/operations.md
decisions:
  - Included TPC-SHARD-DESIGN.md target-architecture ASCII diagram verbatim (plan requirement)
  - Reshard workflow pointer in architecture-tpc.md links to operations.md sizing section (bidirectional cross-reference)
  - pareto-c8-x8/README.md cited in operations.md ship-gate criteria table (plan key_link requirement)
  - D-02 operator caution in reshard subsection — operators must not write to data/logs/ directly
metrics:
  duration: "~25 minutes"
  completed: "2026-04-18"
  tasks_completed: 2
  tasks_total: 2
  files_created: 1
  files_modified: 1
---

# Phase 52 Plan 09: TPC Architecture Docs — Summary

**One-liner:** TPC explainer (8-section `architecture-tpc.md`) and shard sizing runbook added to `operations.md`, closing TPC-DX-04.

---

## Completed Tasks

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Write docs/architecture-tpc.md (8 sections) | 68d223b | docs/architecture-tpc.md (created, 357 lines) |
| 2 | Update docs/operations.md — Shard Sizing & Hot-Shard Diagnosis | c513f9f | docs/operations.md (+126 lines) |

---

## What Was Built

### docs/architecture-tpc.md (new)

A standalone TPC explainer for new contributors and operators. All 8 required sections:

1. **Motivation** — DashMap contention ceiling (~300K EPS observed), TPC fix rationale,
   Apache Iggy precedent (P99 −60% after TPC migration, Feb 2026).
2. **Shard Model** — Target-architecture ASCII diagram (verbatim from TPC-SHARD-DESIGN.md),
   Shard struct contents, BEAVA_SHARDS defaults (physical cores on release, 1 on debug),
   ahash determinism guarantee.
3. **Routing** — shard_hint flow by source type, fast-path skip-rehash optimization,
   ShardKeyMissingWarning fallback to shard 0, backpressure contract (bounded SPSC,
   503 on inbox full).
4. **Joins** — Co-location requirement, JoinShardKeyMismatch at registration time (fatal,
   names both streams + suggested fix), why co-location is the correct v1.2 trade-off.
5. **Recovery** — Parallel per-shard recovery (N tasks, one per shard), boot barrier
   "recovered" sub-state, /ready returns 503 until all shards complete, /health stays 200
   throughout, performance improvement (7.0s → ~1.5s at N=8), boot guard on shard_count
   mismatch.
6. **Reshard Workflow** — Full CLI reference for `tally reshard --from N --to K`, 4-step
   procedure (stop, run tool, optional --replace atomic swap, restart), downtime = tool
   runtime, pointer to operations.md for sizing.
7. **Fork/Replica** — Re-hash on ingest (always `ahash(key) mod downstream_N`), upstream
   hint is fast-path only, LSN format (`u64`: `upstream_shard_id:8|stream_ord:16|seq:40`),
   max_lsn_seen tracking in snapshot v8, dedup closes rolling-restart double-emit window.
8. **Ship-Gate Rationale** — All 3 criteria with explanation of what each validates and
   why the threshold was chosen.

### docs/operations.md — Shard Sizing & Hot-Shard Diagnosis (appended)

6 subsections as specified:

1. **Choosing BEAVA_SHARDS** — per core-count guidance (8-core: start at 4, 16+: full
   count), power-of-2 recommendation, shard count mismatch boot refusal.
2. **Metrics to watch** — 5-row table: `beava_shard_keys_owned`, `reactor_utilization`,
   `cross_shard_fraction`, `inbox_full_total`, `inbox_depth`.
3. **BEAVA_HOT_SHARD_THRESHOLD tuning** — default 1.5×, with guidance for 1.2× (latency-
   sensitive) and 2.0× (skewed distributions).
4. **Hot-shard diagnosis flow** — 4-step runbook: `/debug/shards`, `shard_probe`,
   shard_key inspection, shard count increase for inherently skewed workloads.
5. **Reshard workflow** — 3-step CLI procedure, pointer to architecture-tpc.md, D-02
   operator caution (do not write to `data/logs/` directly).
6. **Ship-gate criteria as production health indicators** — 3-row table with thresholds
   and production meaning; link to `benchmark/pareto-c8-x8/README.md`.

---

## Deviations from Plan

None — plan executed exactly as written.

---

## Known Stubs

None. Both documents reference implemented endpoints (`/debug/shards`, `shard_probe`,
`/ready`), implemented CLI (`tally reshard`), and implemented benchmark cells
(`pareto-c8-x8`) from prior plans in this phase.

---

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced.
Both files are read-only documentation. T-52-09-01 (public doc, no secrets) and
T-52-09-02 (diagnosis flow cross-references implemented endpoints) are satisfied.

---

## Self-Check: PASSED

- `docs/architecture-tpc.md` exists: FOUND
- H2 section count >= 8: PASS (8 sections)
- ASCII diagram present: PASS
- `docs/operations.md` contains "Shard Sizing": PASS
- `data/logs` D-02 caution present: PASS
- `cross_shard_fraction` ship-gate criteria cited: PASS
- Reshard pointer to architecture-tpc.md: PASS
- `pareto-c8-x8/README.md` link present: PASS
- Commit 68d223b exists: PASS
- Commit c513f9f exists: PASS
