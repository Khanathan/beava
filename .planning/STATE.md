---
gsd_state_version: 1.0
milestone: v1.2
milestone_name: milestone
status: planning
stopped_at: Completed 48-01, 48-02, 48-03 plans
last_updated: "2026-04-18T12:39:17.683Z"
last_activity: "2026-04-18 — v1.2 roadmap created. 5 phases (48–52), 24 requirements mapped, 0 plans executed. Dependency graph: 48 → 49 → 50 → 51 → 52 (Phases 51 and the Wave 4 work within 52 can parallelize after Phase 50 ships — see ROADMAP.md)."
progress:
  total_phases: 5
  completed_phases: 1
  total_plans: 9
  completed_plans: 3
  percent: 33
---

# Project State

## Project Reference

See: `.planning/PROJECT.md` (updated 2026-04-18)

**Core value:** A skeptical engineer evaluating Beava on github.com can go from landing on the repo to correct, live feature values in under 60 seconds — from any language.
**Current focus:** Milestone v1.2 — Thread-Per-Core + Full Key-Shard. Roadmap complete; ready to plan Phase 48.

## Current Position

**Phase:** 48 (not started)
**Plan:** —
**Status:** Roadmap written; awaiting phase planning
**Progress:** ░░░░░░░░░░ 0% (0/5 phases)

**Last activity:** 2026-04-18 — v1.2 roadmap created. 5 phases (48–52), 24 requirements mapped, 0 plans executed. Dependency graph: 48 → 49 → 50 → 51 → 52 (Phases 51 and the Wave 4 work within 52 can parallelize after Phase 50 ships — see ROADMAP.md).

## Milestone Status

| Milestone | Status | Completed |
|-----------|--------|-----------|
| v1.0 Foundation | Complete | 2026-04-09 |
| v1.1 Event Log & Composable Pipelines | Complete | 2026-04-10 |
| v1.2 Fire-and-Forget PUSH | Complete | 2026-04-11 |
| v1.3 Concurrency & Batching | Complete | 2026-04-12 |
| v2.0 New API & Engine | Complete | 2026-04-13 |
| v2.1 Launch | Engineering complete; live-run ops pending | 2026-04-14 (eng) |
| v0 Restructure (21-26) | Complete | 2026-04-14 |
| v0 Data-Scientist Fork (27, 35-38) | Engineering complete | 2026-04-15 |
| v1.0-launch — Public Launch Readiness | Engineering complete — launch-day human-run pending | 2026-04-17 (eng) |
| **v1.2 — Thread-Per-Core + Full Key-Shard** | **Roadmap complete; Phase 48 not started** | **2026-04-18 (started)** |

## v1.2 Roadmap Summary

**Goal:** Intra-node scaling via thread-per-core + full key-shard — eliminate DashMap contention and cross-core cache-line bouncing to reach 1.5M–2.5M EPS on a 16-core box (5-6× current baseline), preserving correctness and migration-compat with today's single-shard state format.

**Ship gate for merging to main:**

1. Every 9-cell matrix cell within −5% of baseline at N=1 (migration-compat gate)
2. ≥3× baseline on `complex-c8-x8` at N=CPU_COUNT (architecture gate)
3. `shard_probe` cross_shard_fraction <40% on the release benchmark workload (architectural-fit gate)

| Phase | Name | Goal | Requirements |
|-------|------|------|--------------|
| 48 | shard-hint-scaffolding | Wire `shard_hint()` no-op at N=1; micro-bench gates | TPC-INFRA-01 |
| 49 | per-shard-state-store | `Shard` struct, `BEAVA_SHARDS` config, full test suite green at N=1 | TPC-INFRA-02, TPC-PERF-01, TPC-DX-01 |
| 50 | multi-shard-routing | SO_REUSEPORT, SPSC, pinning, backpressure, metrics, ≥3× gate | TPC-INFRA-03, TPC-INFRA-04, TPC-INFRA-07, TPC-PERF-02, TPC-PERF-03, TPC-PERF-04, TPC-CORR-01, TPC-CORR-03, TPC-DX-02 |
| 51 | cross-shard-queries-joins | Scatter-gather, JoinShardKeyMismatch, global watermark, /debug/shards | TPC-INFRA-05, TPC-PERF-05, TPC-PERF-06, TPC-CORR-04 |
| 52 | event-log-recovery-ship-gate | Per-shard log, parallel recovery, reshard tool, snapshot v8, parity test, 1M+ EPS, docs | TPC-INFRA-06, TPC-CORR-02, TPC-CORR-05, TPC-CORR-06, TPC-PERF-07, TPC-DX-03, TPC-DX-04 |

**Total requirements:** 24/24 mapped (100% coverage)
**Source of truth:** `.planning/arch/TPC-SHARD-DESIGN.md` + `.planning/arch/TPC-RESEARCH.md` + `.planning/research/SUMMARY.md`

## Launch Day Checklist

Six human-run items required before public launch. Execute in order — items 3 and 4
depend on item 1 (Docker Hub image live). Full detail in
`.planning/v1.0-launch-MILESTONE-AUDIT.md § Launch-Day Checklist`.

1. **Docker Hub push** — `docs/docker-publish-runbook.md` — build and push
   `beavadb/beava:latest` + `beavadb/beava:0.1.0`. Prerequisite for items 3, 4.

2. **GitHub repo settings wire-up** — `docs/github-repo-surface-runbook.md` — set
   description, topics (8 items), upload `site/assets/social-preview.png`.

3. **Fresh-VM smoke test (SHIP-02)** — `.planning/phases/47-repo-polish/SHIP-VM-SMOKE.md`
   — depends on Docker Hub image (item 1). 6-step runbook, SC-1/SC-2/SC-3 checklist.

4. **Quickstart GIF recording (SHIP-05)** —
   `.planning/phases/47-repo-polish/QUICKSTART-RECORDING-RUNBOOK.md` — depends on
   Docker Hub image (item 1). asciinema + agg, <3 MB output.

5. **HTTP EPS measurement (HTTP-09, CORR-02, OUTREACH precondition)** —
   `LOAD_TEST_REFERENCE_BOX_REQUIRED=1 bash benchmark/http_load.sh` — commits measured
   number to `benchmark/README.md`. Required before citing "100K+ EPS over HTTP".

6. **Outreach sign-off (SHIP-04)** —
   `.planning/phases/47-repo-polish/OUTREACH-AUDIT-CHECKLIST.md` — 10-item VC checklist

   + final package at `.planning/outreach/LAUNCH-PACKAGE-V8.md`.

## Performance Metrics

| Metric | Baseline (v1.0-launch) | Target (v1.2 N=CPU_COUNT) | Notes |
|--------|------------------------|---------------------------|-------|
| 9-cell benchmark matrix | Committed v1.0-launch BASELINE | Within −5% of baseline at N=1 | Migration-compat gate — Phase 48 onward |
| Single-stream TCP push EPS | ~350 K EPS | 1.5M–2.5M EPS (16-core) | Architecture gate via Phase 52 load test |
| 9-cell `complex-c8-x8` cell | baseline | ≥3× baseline at N=CPU_COUNT | Phase 50 ship-gate |
| shard_probe cross_shard_fraction | N/A | <40% on release workload | Phase 50 + Phase 52 gate |
| Recovery time (4.7 GB state) | ~7 s | ~1.5 s (parallel, N-thread) | Phase 52 parallel recovery gate |
| N=1 ↔ N=8 proptest parity | N/A | All operators identical | Phase 52 hard pre-merge gate |
| Pareto-workload Pareto cell | N/A | cross_shard_fraction <40% | Phase 52 ship-gate |

## Accumulated Context

### Architecture decisions locked 2026-04-18

- **Runtime:** tokio `current_thread` via `Builder::new_current_thread().build()` + `block_on()` per pinned shard thread (not `build_local()`). compio is the v1.3/Beava Cloud endpoint.
- **Default N_SHARDS:** `num_cpus::get_physical()` in release, 1 in debug builds (`cfg!(debug_assertions)` at startup).
- **Env wins over CLI:** `BEAVA_SHARDS` always beats `--shards N` (consistent with all other `BEAVA_*` vars).
- **Backpressure contract:** SPSC bounded queue, non-blocking `try_send`, drop on full, increment `beava_shard_inbox_full_total{shard}`, return HTTP 503 / TCP SHARD_OVERLOAD. Never block the listener thread.
- **Snapshot mismatch:** Hard-fail at boot with actionable error. No silent boot-empty.
- **Tuple shard_key missing field:** Reject at ingest (HTTP 400 / TCP SHARD_KEY_MISSING), increment `beava_events_dropped_total{reason="shard_key_missing"}`. Never panic.
- **Fork/replica:** Always re-hashes by downstream N. Upstream shard_hint is a fast-path hint only. No `--reshard-from` flag.
- **DashMap / ArcSwap:** Retained as compat shims through Waves 1-3; deleted at Wave 4 (Phase 52).
- **Channel primitive:** `crossbeam-channel::bounded` (MPSC in practice; single consumer per shard = SPSC semantics). Not rtrb or kanal.
- **SO_REUSEPORT:** Linux only (kernel 4-tuple-hash distribution). macOS falls back to single-listener + dispatcher.
- **N=1↔N=8 parity test:** proptest-driven; pre-merge gate for Phase 52.
- **Snapshot format v8:** `shard_count: u16` appended to `SnapshotHeader` via `#[serde(default = "default_shard_count")]`; default = 1 for v7 snapshots.

### Pitfall guards built into roadmap

| Pitfall | Severity | Phase | Guard |
|---------|----------|-------|-------|
| Cascading overload / inbox full | Launch-gate | 50 | TPC-CORR-01 backpressure contract |
| Silent empty-state on shard_count mismatch | Launch-gate | 52 | TPC-CORR-02 hard-fail guard |
| Tuple shard_key missing field crash | Launch-gate | 50 | TPC-CORR-03 reject + counter |
| Inter-shard join ordering non-determinism | Launch-gate | 51 | TPC-CORR-04 co-location guard at register |
| Hot-shard blind spot | Ship-gate | 51 | TPC-INFRA-05 /debug/shards |
| N=1↔N=8 parity | Ship-gate | 52 | TPC-CORR-05 proptest harness (pre-merge gate) |
| Uniform hash conceals Pareto imbalance | Ship-gate | 52 | TPC-PERF-07 Pareto workload cell |
| Legacy unlabeled metrics go dark | Ship-gate | 50 | TPC-INFRA-03/04 double-emit global sum |

### New Cargo deps by wave

| Crate | Wave / Phase | Type |
|-------|-------------|------|
| `rstest = "0.26"` | Wave 0 / Phase 48 | dev-dependency |
| `num_cpus = "1.17"` | Wave 1 / Phase 49 | dependency |
| `core_affinity = "0.8"` | Wave 2 / Phase 50 | dependency |
| `crossbeam-channel = "0.5"` | Wave 2 / Phase 50 | dependency |
| `metrics = "0.24"` | Wave 2 / Phase 50 | dependency |
| `metrics-exporter-prometheus = "0.16"` | Wave 2 / Phase 50 | dependency |
| `futures = "0.3"` | Wave 3 / Phase 51 | dependency |
| `proptest` | Wave 5 / Phase 52 | already in dev-deps |

### Outstanding todos

- v1.0-launch 6-item human-run checklist (independent of v1.2 engineering)
- Phase 47-03 code hygiene (INFRA-06/07/08) deferred to v1.1; de-facto state clean

## Phase History

- v1.x phases: `.planning/milestones/v1.0-ROADMAP.md`
- v2.0: `.planning/milestones/v2.0-ROADMAP.md`
- v2.1 Launch (Phase 20): `.planning/milestones/v2.1-ROADMAP.md`
- v0 Restructure (Phases 21-26): `.planning/milestones/v0-ROADMAP.md`
- v0 Data-Scientist Fork (Phases 27, 35-38): in-flight archival pending
- **v1.0-launch (Phases 45-47): `.planning/milestones/v1.0-launch-ROADMAP.md`** — archived 2026-04-17
- **v1.2 TPC (Phases 48-52): `.planning/ROADMAP.md`** — active

## Session Continuity

**Stopped at:** Completed 48-01, 48-02, 48-03 plans

**Next action (engineering):** `/gsd-plan-phase 48` — Phase 48: shard-hint scaffolding (Wave 0).

**Orthogonal ops (launch day — still pending):** v1.0-launch 6-item human-run checklist above remains outstanding. Run independently of v1.2 engineering work.
