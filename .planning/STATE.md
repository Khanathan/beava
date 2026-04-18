---
gsd_state_version: 1.0
milestone: v1.2
milestone_name: tpc-full-shard
status: defining-requirements
stopped_at: ""
last_updated: "2026-04-18T00:00:00.000Z"
last_activity: 2026-04-18 — Started milestone v1.2 Thread-Per-Core + Full Key-Shard. Design doc hardened against 2026 research (commit 2a8d8fd). Running 4-agent domain research before requirements/roadmap. v1.0-launch launch-day checklist still pending (§ Launch Day Checklist below).
progress:
  total_phases: 0
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: `.planning/PROJECT.md` (updated 2026-04-18)

**Core value:** A skeptical engineer evaluating Beava on github.com can go from landing on the repo to correct, live feature values in under 60 seconds — from any language.
**Current focus:** Milestone v1.2 — Thread-Per-Core + Full Key-Shard. Defining requirements; roadmap next.

## Current Position

**Phase:** Not started (defining requirements for v1.2 TPC)
**Plan:** —
**Status:** Defining requirements
**Last activity:** 2026-04-18 — Milestone v1.2 Thread-Per-Core + Full Key-Shard started. Design doc + research committed (2a8d8fd). PROJECT.md updated with 5-wave scope. Domain research in flight.

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
| **v1.2 — Thread-Per-Core + Full Key-Shard** | **Defining requirements** | **2026-04-18 (started)** |

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

### Deferred to v1.1 backlog (user decision)

Three code-hygiene items were explicitly deferred by user during Phase 47. They do not
block launch. The de-facto state of the codebase is clean on all three (audit confirms);
the plan was simply not executed.

| Req | Item | De-facto state | v1.1 action |
|-----|------|---------------|-------------|
| INFRA-06 | TODO/FIXME/XXX sweep | Clean: 0 naked TODOs outside vendor; 2 `TODO(gh-TBD)` annotated items; `TODO-AUDIT.md` documents dispositions | File 2 GitHub issues at repo-go-public time |
| INFRA-07 | `println!`/`dbg!`/`eprintln!` audit | Clean: all non-vendor prints annotated `// Intentional: ... (Phase 47 audit)`; zero bare `dbg!` | Confirm annotation survey in v1.1 cleanup pass |
| INFRA-08 | `#![warn(missing_docs)]` + pub re-export docs | `#![warn(missing_docs)]` IS present in `src/lib.rs`; per-export coverage not fully verified | Complete doc-comment pass on `src/lib.rs` exports in v1.1 |

## Performance Metrics

| Metric | Baseline (v2.0) | Target (v1.0-launch) | Notes |
|--------|-----------------|-----------------------|-------|
| 9-cell benchmark matrix | BASELINE committed | Within −5% of BASELINE | CORR-02 hard merge gate for 2a fix |
| Single-stream TCP push (baseline) | ~350 K EPS | Unchanged | Regression gate |
| HTTP `/push-batch/{stream}` throughput | N/A | >100 K EPS sustained (oha, reference box) | HTTP-09 ship criterion — measurement pending (launch Step 5) |
| Ring-buffer drop counter hot-path overhead | N/A | <100 ns per drop (cached Counter handle) | OBS-01 pitfall-4 mitigation |
| 2d.vi/vii atomic DashSet swap regression | N/A | Within 2% on 9-cell | CORR-10 ceiling |
| Docker image size (distroless/cc-debian12:nonroot) | N/A | <200 MB target (~80 MB expected) | INFRA-05 |
| CI pipeline (fmt + clippy + test) | N/A | <5 min | INFRA-03 |
| Fresh-VM time-to-first-feature-read | N/A | <60 s | SHIP-02 / CONTENT-02 / core-value gate — measurement pending (launch Step 3) |

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
- **46-03 decision: hashmap identity-key bucket coalescing** — `bucket_of(t) = t` (raw SystemTime as key); operators re-align per feature internally. Spot bench (complex-c8-x8, 30s): +10.48% above baseline.
- **47-03 DEFERRED by user decision** — code-hygiene (INFRA-06/07/08) deferred to v1.1; de-facto state confirmed clean by audit.

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

Launch-day human-run items only (see Launch Day Checklist above). No engineering work remaining.

### Blockers

None. Engineering complete. Launch-day items are orchestration-only.

### v2.1 Launch — Remaining ops (async, independent)

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
- INFRA-06 / INFRA-07 / INFRA-08 formal plan execution (v1.1 code hygiene)

## Phase History

- v1.x phases: `.planning/milestones/v1.0-ROADMAP.md`, `.planning/milestones/v2.0-ROADMAP.md`
- v2.0: `.planning/milestones/v2.0-ROADMAP.md`
- v2.1 Launch (Phase 20): `.planning/milestones/v2.1-ROADMAP.md`
- v0 Restructure (Phases 21-26): `.planning/milestones/v0-ROADMAP.md`
- v0 Data-Scientist Fork (Phases 27, 35-38): in-flight archival pending `/gsd-complete-milestone` run
- **v1.0-launch (Phases 45-47): `.planning/milestones/v1.0-launch-ROADMAP.md`** — archived 2026-04-17

## Session Continuity

**Stopped at:** Milestone v1.2 Thread-Per-Core + Full Key-Shard in flight 2026-04-18. Design doc + research committed (2a8d8fd). PROJECT.md updated. Running 4-agent domain research, then REQUIREMENTS, then ROADMAP.

**Next action (engineering):** Finish v1.2 new-milestone scaffolding — research → REQUIREMENTS.md → ROADMAP.md → `/gsd-discuss-phase` on the first phase (Wave 0 scaffolding).

**Orthogonal ops (launch day — still pending):** v1.0-launch 6-item human-run checklist above remains outstanding. Items 3 and 4 depend on item 1. Run independently of v1.2 engineering work.
