---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: completed
stopped_at: Completed 46-05-PLAN.md
last_updated: "2026-04-17T23:46:54.358Z"
last_activity: 2026-04-17 — Phase 46-03 complete. push_batch_with_cascade_no_features takes &[(&Value, SystemTime)]; hashmap bucket coalescing eliminates min_event_time collapse (CORR-01); proptest 256 cases × 3 runs green; spot bench +10.48% above baseline; full 9-cell matrix deferred pending run_matrix.sh tooling fix.
progress:
  total_phases: 15
  completed_phases: 8
  total_plans: 30
  completed_plans: 25
  percent: 83
---

# Project State

## Project Reference

See: `.planning/PROJECT.md` (updated 2026-04-17)

**Core value:** A skeptical engineer evaluating Beava on github.com can go from landing on the repo to correct, live feature values in under 60 seconds — from any language.
**Current focus:** Milestone v1.0-launch — Public Launch Readiness (Phases 45-47)

## Current Position

**Phase:** 46 (Correctness Audit, Fixes & Ship-Gate) — in progress
**Plan:** 03 complete (Wave 2a — signature + group-by-bucket + proptest; CORR-01 closed); Plan 04 next
**Status:** Active — Phase 46-01, 46-02, 46-03 complete; ready to execute 46-04
**Last activity:** 2026-04-17 — Phase 46-03 complete. push_batch_with_cascade_no_features takes &[(&Value, SystemTime)]; hashmap bucket coalescing eliminates min_event_time collapse (CORR-01); proptest 256 cases × 3 runs green; spot bench +10.48% above baseline; full 9-cell matrix deferred pending run_matrix.sh tooling fix.

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
| **v1.0-launch — Public Launch Readiness** | **Active — Phase 45 ready to plan** | **—** |

## Performance Metrics

| Metric | Baseline (v2.0) | Target (v1.0-launch) | Notes |
|--------|-----------------|-----------------------|-------|
| 9-cell benchmark matrix | BASELINE committed | Within −5% of BASELINE | CORR-02 hard merge gate for 2a fix |
| Single-stream TCP push (baseline) | ~350 K EPS | Unchanged | Regression gate |
| HTTP `/push-batch/{stream}` throughput | N/A | >100 K EPS sustained (oha, reference box) | HTTP-09 ship criterion |
| Ring-buffer drop counter hot-path overhead | N/A | <100 ns per drop (cached Counter handle) | OBS-01 pitfall-4 mitigation |
| 2d.vi/vii atomic DashSet swap regression | N/A | Within 2% on 9-cell | CORR-10 ceiling |
| Docker image size (distroless/cc-debian12:nonroot) | N/A | <200 MB target (~80 MB expected) | INFRA-05 |
| CI pipeline (fmt + clippy + test) | N/A | <5 min | INFRA-03 |
| Fresh-VM time-to-first-feature-read | N/A | <60 s | SHIP-02 / CONTENT-02 / core-value gate |
| Phase 45 P01 | 35 | 3 tasks | 17 files |
| Phase 45 P02 | 12 | 3 tasks | 3 files |
| Phase 45-http-ingest-read-api P04 | 35 | 3 tasks | 5 files |
| Phase 45 P05 | 45 | 5 tasks | 7 files |
| Phase 46-correctness-audit-fixes P02 | 8m | 1 tasks | 1 files |
| Phase 46-correctness-audit-fixes P01 | 15m | 3 tasks | 14 files |
| Phase 46 P04 | 15 | 2 tasks | 7 files |
| Phase 46 P05 | 45 | 3 tasks | 28 files |

## Accumulated Context

### Decisions locked this milestone (2026-04-17)

- **One milestone (not three)** for all three LAUNCH.md blocks — unified ship gate.
- **Continue phase numbering from 45** (no reset; phase_dir_count=32; archive-target unsafe).
- **HTTP ingest reuses `handle_push_core_ex` + `require_loopback_or_token`** — zero duplicated ingest logic. HTTP and TCP inherit the 2a fix together.
- **Fix 2a via `&[(&Value, SystemTime)]` signature + group-by-bucket** (NOT per-event loop). Per-event revert history: commits `3818880` → `1cefc45`. 9-cell bench within −5% is the hard merge gate.
- **2d.i closes as "not a bug" + verification test** — `run_backfill` uses `push_for_backfill`, not `handle_push_batch`.
- **2d.ii/2d.iii/2d.iv confirmed HIGH-confidence bugs** with named code locations (tcp.rs:2703, eviction.rs:63, tcp.rs:1012-1222). Each has fit-on-one-screen fixes.
- **2d.v closes docs-only** — joins require both sides producing in v1; per-stream idle markers defer to v1.1 (DX-06).
- **2d.vi + 2d.vii combined as one fix** — atomic swap of `DashSet<String>` via `take_dirty_and_advance_gen()`.
- **Option A docs (flat markdown)** — dedicated site deferred post-launch. 8 docs pages under `docs/`.
- **Docker base: `gcr.io/distroless/cc-debian12:nonroot`** — NOT Alpine (MUSL allocator regression; Pitfall 14). Multi-stage via `cargo-chef`.
- **Load tester: `oha`** on reference box (NOT GitHub Actions runners) for HTTP >100 K EPS verification.
- **Keep Python SDK TCP path unchanged** — HTTP is additive. Single canonical event schema locked BEFORE Block 1 ships.
- **Keep `tally` binary name for v1.0-launch**; rename to `beava` in v1.1 to avoid doc churn.
- **Single ship-gate integration test** covers CORR-01 (2a) + CORR-05 (2d.i) + CORR-06 (2d.ii) simultaneously: `HTTP push → crash → recover → read features`.
- **46-03 decision: hashmap identity-key bucket coalescing** — `bucket_of(t) = t` (raw SystemTime as key); operators re-align per feature internally. Spot bench (complex-c8-x8, 30s): +10.48% above baseline. Full 9-cell matrix deferred pending run_matrix.sh OUTPUT_DIR tooling fix.
- **46-03 decision: D-26 is a no-op at HTTP layer** — http_ingest.rs already correct; fix lived entirely in tcp.rs handle_push_batch min_event_time collapse removal.

### Key design decisions (inherited, locked)

- Stream vs Table as sole public types
- `@tl.stream` / `@tl.table` decorators with class=source / function=derivation convention
- Table aggregation disabled in v0 restructure (sidesteps Case 3 retraction complexity; deferred)
- UDDSketch for percentile, CMS+heap for top_k, HLL for count_distinct — all hybrid exact-first
- Fixed 5s watermark default, per-stream configurable in this milestone (Block 2c / CORR-03)
- γ-model watermark propagation
- `/debug/warnings` unified observability; `tally suggest-config` CLI for tuning
- Local replica is scope-driven, not whole-cluster
- Data scientists fork via `tally fork --remote ... --streams ... --pipeline-file ...` running a local Beava server in replica mode

### Outstanding todos

None at milestone entry. Roadmap complete; Phase 45 ready to plan.

### Blockers

None. All three phases have clear inputs, disjoint code paths for 42 vs 43, and item-level dependencies for 44 are documented in ROADMAP.md phase-detail section.

### Research flags (surface during phase planning)

- **Phase 46 (2a group-by-bucket):** Decide sort-in-place contiguous grouping vs hash-map bucket coalescing via micro-benchmark at phase kickoff.
- **Phase 46 (2d.vi/vii atomic-swap):** Benchmark `ArcSwap<DashSet<String>>` vs `AtomicPtr` vs mutex-guarded swap against the 2% ceiling.
- **Phase 45 (100 K EPS HTTP target):** Design estimate unverified on current tree; profiling pass required before claiming ship criterion (serde overhead mitigation via `Bytes` + per-line parse if needed).
- **Phase 47 item ordering:** Docker + CI + clippy/fmt + community files + directory READMEs start day one. README rewrite + `examples/curl-ingest/` + `docs/http-api.md` + HTTP-variant `examples/fraud-scoring/` + `examples/session-features/` block on Phase 45. `docs/event-time.md` cross-linking blocks on Phase 46.

### v2.1 Launch — Remaining ops (async)

Runbook in `.planning/phases/26-test-migration-bench-docs-demo/26-04-SUMMARY.md § Resuming v2.1 Launch`. Not engineering-gated; Hetzner VM provision + 5-day live observation only. Independent of v1.0-launch.

### Deferred (explicitly post-launch)

- Table-input aggregation + full retraction propagation through DAG
- Outer joins (right/full)
- Session windows
- CEP / `match_recognize` patterns
- Horizontal scale-out / key-partitioned multi-threading
- Thread-per-core runtime (v1.2)
- Multi-node via Kafka (v1.3+)
- UDF / stateful scripting (Rhai / WASM)
- CI/CD regression-gate integration
- Multi-platform testing (macOS / Linux / Windows)
- Predicate-level replica scoping
- OpenAPI / Swagger UI, deploy-button integrations, Web UI for `/debug`
- CLI subcommands (`beava push/get/tail`)
- Per-stream idle markers for join watermark stalls (DX-06, v1.1)
- `tally` → `beava` binary rename (v1.1)

## Phase History

- v1.x phases: `.planning/milestones/v1.0-ROADMAP.md`, `.planning/milestones/v2.0-ROADMAP.md`
- v2.0: `.planning/milestones/v2.0-ROADMAP.md`
- v2.1 Launch (Phase 20): `.planning/milestones/v2.1-ROADMAP.md`
- v0 Restructure (Phases 21-26): `.planning/milestones/v0-ROADMAP.md`
- v0 Data-Scientist Fork (Phases 27, 35-38): in-flight archival pending `/gsd-complete-milestone` run
- v1.0-launch (Phases 45-47): active in `.planning/ROADMAP.md` (v1.0-launch section)

## Session Continuity

**Stopped at:** Completed 46-05-PLAN.md

**Next action:** Execute `45-03-PLAN.md` (Wave 2 — write handlers: `http_push_single`, `http_push_batch`, `http_push_ndjson`).

**Note:** The linter auto-implemented write handlers during 45-02 execution. Verify 45-03 plan scope before running — write handlers may already be live.

**Parallel workstream:** Phase 46 can begin planning/execution alongside Phase 45 — disjoint code paths (HTTP router vs engine internals).

**Phase 47:** Day-one items (Docker, CI, clippy/fmt, community files, directory READMEs, docs pages that don't cross-link to 45/46) can begin planning at the same time — critical path is Phase 45 + Phase 46 landing before ship-gate SHIP-02/03/04/05 closes.
