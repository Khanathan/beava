# Phase 13.4 — Planner Auto-Decisions (gray-area defaults)

**Owner:** Phase 13.4 plan-phase orchestrator (parallel sibling agent for 13.4 only).
**Date:** 2026-05-03
**Why this file exists:** orchestrator instructed not to prompt the user; auto-pick recommended defaults and surface them here for review.

The four user-locked decisions in `13.4-CONTEXT.md` (D-01..D-04) are NOT auto-decisions — they are honored verbatim. The notes below cover **only** the planner-discretion areas explicitly delegated by CONTEXT §"Claude's Discretion" plus implementation gray-areas surfaced during decomposition.

---

## A-01 — Plan numbering & wave shape (10 plans, 5 waves after wave-conflict resolution)

CONTEXT estimate: ~8-10 plans across 3 waves. Picked **10 plans**. Initial assignment was 3 waves; wave-conflict resolution (file-overlap on `wire_request.rs` between Plans 03+04 and on `apply_shard.rs` between Plans 08+09) bumped Plan 04 → Wave 2, Plan 09 → Wave 4, Plan 10 → Wave 5. Final shape:

| Plan | Title | Wave | depends_on | Scope item |
|------|-------|------|------------|-----------|
| 13.4-01 | Op renames (ADR-002) | 1 | — | CONTEXT scope #1 |
| 13.4-02 | GET response → row-shape | 1 | — | #2 |
| 13.4-03 | OP_BATCH_GET (0x0024) opcode + dispatch | 1 | — | #3 |
| 13.4-04 | Verb-style HTTP route migration | 2 | 03 | #4 |
| 13.4-05 | Architectural-test allowlist (D-04) | 1 | — | #9 |
| 13.4-06 | force=True + dry_run=True register flags (D-01) | 2 | 01, 02 | #5 + #6 (folded) |
| 13.4-07 | Persistence::Memory backend (D-02) | 1 | — | #7 |
| 13.4-08 | OP_RESET (0x0040) + /reset route + test_mode gate (D-03) | 3 | 04, 07 | #8 |
| 13.4-09 | Global-table sentinel routing (ADR-003) | 4 | 05, 06, 08 | #10 |
| 13.4-10 | Microbench + throughput-run + closure (perf-gate + STATE/ROADMAP) | 5 | 01..09 | closure |

**Why Plan 04 moved to Wave 2:** Plans 03 and 04 both modify `crates/beava-runtime-core/src/wire_request.rs` (Plan 03 adds TcpBatchGet/HttpBatchGet variants; Plan 04 adds HttpPing variant). Same-wave file overlap is forbidden by the wave-conflict rule.

**Why Plan 09 moved to Wave 4:** Plans 08 and 09 both modify `crates/beava-server/src/apply_shard.rs` (Plan 08 adds dispatch_reset_sync; Plan 09 adds an ADR-003 sentinel-routing comment block). Plan 09's edit is comment-only but counts as a file write.

**Why Plan 10 moved to Wave 5:** Closure plan must run after all engine plans land. With Plan 09 in Wave 4, closure naturally falls to Wave 5.

**Effect on parallelism:** wave-1 still ships 5 plans in parallel (largest wave). The wave-2/3/4 sequential tail adds maybe 1-2 hours of executor wall-time but eliminates merge conflicts on shared files. Acceptable tradeoff.

**Why fold #5 + #6 into one plan:** D-01's diff matrix lives in `register_validate.rs`; `dry_run=True` is "uses force diff logic, returns JSON without applying" (~30 LOC riding on the same code). Splitting into two plans creates artificial file overlap → forced sequential. Single plan is cleaner. Both decisions are still individually traceable in the task split (Task 1 = force diff matrix, Task 2 = dry_run shim, Task 3 = wire to register entry points).

**Why fold global-sentinel into its own plan (not closure):** ADR-003 has a dedicated REQUIREMENT (`V0-GLOBAL-AGG-01`) and a dedicated acceptance gate (`python/tests/v0/test_global.py`); deserves its own plan + SUMMARY for traceability even though LOC count is small (~30 LOC).

**Why split closure off Plan 09:** the closure plan adds the criterion microbench (`apply_path_bench.rs` against Phase 12.9 baseline), runs the throughput matrix (4 shapes × 2 transports per Phase 8+ contract), and authors `13.4-SUMMARY.md` + `13.4-VERIFICATION.md`. Combining with sentinel routing would make Plan 09 too heavy (>3 tasks, multiple concerns).

## A-02 — Microbench cell choice (CONTEXT explicit)

Picked **`crates/beava-core/benches/apply_path_bench.rs`** — already exists, has a Phase 12.9 baseline in `.planning/perf-baselines.md` (per CONTEXT memory note: "post-AggOp-boxing fraud-team predicted ~6 KB weighted-avg per-entity"). No new bench file; existing bench re-runs against renamed ops + global-sentinel paths.

**Regression-gate per CLAUDE.md Phase 6+ contract:** 10% slower than Phase 12.9 baseline → WARN; 25% → BLOCK.

**Why this cell:** apply-path is the hot path; op renames are pure string-table changes (should be free); global-sentinel routing is "absence of a special-case rejection" (should be free); GET row-shape touches response encode (cold path); OP_BATCH_GET adds a new opcode (orthogonal to apply hot path). Net expected: zero regression. Bench is the regression tripwire.

## A-03 — Throughput-run cells (CONTEXT explicit)

Per Phase 8+ contract: re-run `crates/beava-bench/blast_shape_bench.rs` against:

- `small.json` (HTTP + TCP) — primary regression gate (±10% Phase 12.9 baseline)
- `medium.json` (HTTP + TCP)
- `large.json` (HTTP + TCP)
- `fraud-team.json` (HTTP + TCP) — primary tuning bench per memory `project_fraud_team_primary_bench`

Total 8 cells. Append rows to `.planning/throughput-baselines.md`. Plan 10 owns this.

## A-04 — Error code naming

Per CONTEXT delegation:

- **D-01 force flag rejection:** `force_required` (matches existing forward-looking pattern from 12.7's `unsupported_node_kind` and 12.6's `feature_removed_no_*_v0`). Reason text: "destructive change requires `force=True`".
- **D-03 reset disabled:** `reset_disabled_in_production` (CONTEXT explicit). HTTP 403, wire opcode `0xFFFF` error frame.
- **ADR-003 global vs entity arity mismatch (server-side; SDK side is 13.5):** N/A in 13.4 — engine accepts `entity_id=""` natively; no special error. Arity-mismatch enforcement is SDK-layer (Phase 13.5 owns).

## A-05 — TDD red→green per task split

Every code-bearing task in Plans 01-09 follows red→green:

- **Task N.a (red):** write failing test; commit `test(13.4-NN): ...`
- **Task N.b (green):** implement until test passes; commit `feat(13.4-NN): ...` (or `refactor:` for op renames since they're rewrites of existing code)

Per CLAUDE.md §Conventions §TDD Discipline, this is mandatory from Phase 3 onward. Plans 10's closure docs are pure-doc (single `docs(13.4-10):` commit, exempt per Note 4).

## A-06 — Section ownership for closure plan (Plan 10)

Plan 10 owns:
- `13.4-SUMMARY.md` — phase summary
- `13.4-VERIFICATION.md` — phase verification artifacts
- `.planning/perf-baselines.md` — append `## Phase 13.4` section
- `.planning/throughput-baselines.md` — append rows to existing tables
- `.planning/STATE.md` — DOES NOT TOUCH (parent orchestrator after 13.4/5/6/7 finish)
- `.planning/ROADMAP.md` — DOES NOT TOUCH (parent orchestrator)
- `CLAUDE.md` — DOES NOT TOUCH (parent orchestrator)

This matches the orchestrator constraint: "DO NOT modify .planning/STATE.md, .planning/ROADMAP.md, or anything outside .planning/phases/13.4-engine-prep-wire-spec/. The parent orchestrator owns STATE/ROADMAP advancement after all 4 sibling agents finish."

## A-07 — Verb-style HTTP route migration: ADD without REMOVE

Discovered during planning: existing routes are path-segment style (`POST /push/:event_name`, `POST /push-sync/:event_name`, `POST /push-batch/:event_name`, `POST /get`, `GET /get/:feature/:key`, `POST /register`). CONTEXT scope item #4 says verb-style POST should be the v0 contract: `POST /register / /push / /get / /batch_get / /reset / /ping`.

**Auto-decision:** ADD the new verb-style routes alongside the legacy ones (keep the legacy `/push/:event_name`, `/push-sync/:event_name`, etc.) so existing tests (~20 files per `phase12_6` SUMMARY) keep passing during the migration. Verb-style routes carry `event_name` / `table_name` in the JSON body. Phase 13.5/13.6/13.7 SDKs use the new verb-style routes; legacy routes can be removed in a follow-up phase if/when no callers remain.

Justification: hard-replace would break 20+ in-tree tests during Phase 13.4 itself, slowing the parallel 13.5/13.6/13.7 work. CONTEXT §scope item #4 says "additions/redesigns" — additive interpretation is consistent. Tests for the new routes are added as RED→GREEN in Plan 04.

## A-08 — OP_RESET wire frame for non-test_mode rejection (D-03)

CONTEXT D-03 says non-test_mode reset returns "structured error `reset_disabled_in_production` (HTTP 403 / wire opcode 0xFFFF)". Picked existing error-frame infrastructure: error opcode is `OP_GET_RESPONSE` for HTTP (with HTTP status 403); wire-level opcode is the existing `0xFFFF` reserved error frame from `wire.rs`. Plan 08 wires both transports identically.

## A-09 — Persistence::Memory snapshot policy (D-02 derivative)

CONTEXT D-02: snapshot is no-op in memory mode. Auto-decision: `SnapshotWriter::commit_no_op()` returns `Ok(())` immediately when `Persistence::Memory` is active; no file I/O. Existing snapshot-task scheduler (`snapshot_task.rs`) checks the persistence variant before invoking the writer.

## A-10 — `Server::new(Config { test_mode })` interface shape (D-03 derivative)

CONTEXT D-03 mentions `Server::new(Config { test_mode: true, .. })` as the in-process programmatic gate. Existing constructor is `ServerV18::bind(http_addr, tcp_addr, admin_addr)`. Auto-decision: extend `ServerV18::bind` with a `Config` struct fourth arg defaulting to `Config { test_mode: false, persistence: Persistence::Disk }` via a new `bind_with_config` constructor. Existing `bind(...)` stays as a thin wrapper that calls `bind_with_config(...).await` with defaults — back-compat preserved for existing callers.

Plan 07 (Persistence::Memory) introduces the `Config` struct; Plan 08 (OP_RESET) extends it with `test_mode`.

---

## Items intentionally NOT auto-decided (waiting for the parent orchestrator)

- ADR cross-referencing in CLAUDE.md `§ Events-Only Invariant` block — owned by parent orchestrator post-merge.
- STATE.md `## Status` advancement to "Phase 13.4 ready to execute" — owned by parent.
- ROADMAP.md `**Plans:**` list update from "to be planned" to the 10-plan list — Plan 10's closure DOES update ROADMAP per `gsd-tools.cjs verify.plan-structure` contract, but only the in-phase plan-list table; STATE-bearing fields stay parent-owned.

Wait — re-reading the orchestrator constraint: "DO NOT modify .planning/ROADMAP.md...The parent orchestrator owns STATE/ROADMAP advancement after all 4 sibling agents finish." So Plan 10 also does NOT touch ROADMAP.md. The closure plan only touches files inside `.planning/phases/13.4-engine-prep-wire-spec/` plus `.planning/perf-baselines.md` and `.planning/throughput-baselines.md` (which are append-only ledger files, not phase-status files).

---

*Auto-decisions captured 2026-05-03 by Phase 13.4 plan-phase orchestrator.*
