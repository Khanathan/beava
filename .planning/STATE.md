---
gsd_state_version: 1.0
milestone: v0.0
milestone_name: milestone
status: Executing Phase 18
stopped_at: Completed 18-10-PLAN.md
last_updated: "2026-04-25T22:30:00.000Z"
progress:
  total_phases: 23
  completed_phases: 9
  total_plans: 91
  completed_plans: 61
  percent: 67
---

# State: Beava v2 — v0 OSS Launch

**Project reference:** `.planning/PROJECT.md`
**Roadmap:** `.planning/ROADMAP.md` (26 phases — see roadmap for the full inserted-phase note)
**Requirements:** `.planning/REQUIREMENTS.md`
**Milestone:** v0 (first public OSS cut on beava.dev)
**Created:** 2026-04-22
**Last revised:** 2026-04-24 (late evening — post Phase 18 planning landing; reconciled ROADMAP.md to add Phase 12.5 + Phase 18 + update Phase 13.3 status to IN PROGRESS)

## Core Value

Feature authoring as composable Python code that ships to production unchanged. Users write `@bv.event` / `@bv.table(key=...)` / `bv.col(...)` / `.filter().group_by().agg()` / `.join()` / `bv.union(...)` / `app.register(...)` / `app.push(...)` / `app.get(...)`, deploy unchanged.

## Current Focus

**Phase 18 — Redis-shaped hand-rolled hot path. Plan 18-10 COMPLETE.**

Plan 18-10 landed 2026-04-25 (later that evening) — parse-stage optimization via hand-rolled scanners: `parse_msgpack_envelope` walks `rmp::decode` markers directly (no `rmp_serde` for envelope, no `JsonValue` intermediate, body slice via `Bytes::slice()` zero-copy through refcount); `parse_json_envelope` is a hand-rolled brace-counting scanner with string-state (sonic-rs `LazyValue` derive path measured ~380 ns/op — over the 150 ns target — fell back to D-2 fallback); `Row::Deserialize` rewritten via `BeavaValueVisitor` (walks `MapAccess` directly with `next_value_seed`, no JsonValue per field); `dispatch_push_sync` deserializes raw bytes directly into `Row` (`sonic_rs::from_slice::<Row>` for CT_JSON; `rmp_serde::from_slice::<Row>` for CT_MSGPACK); WAL body bytes are truly zero-copy from wire (`body.extend_from_slice` of the original payload slice — no re-serialize). 6 tasks, 9 commits incl 3 RED + GREEN pairs; +`rmp` 0.8 as direct dep.

**Microbench results (Apple M4, criterion):**
- `parse_msgpack_envelope`: **33.4 ns** (target ≤80 ns; **57.7× faster** than 18-09's 1,928 ns)
- `parse_json_envelope`: **77.1 ns** (target ≤150 ns; **7.6× faster** than 18-09's 583 ns)
- `msgpack_body_to_row`: 407.8 ns (informational; was JsonValue-intermediate)
- `json_body_to_row`: 402.9 ns (informational)

**End-to-end EPS (M4, small/tcp/parallel=4, 5s, no trace):**
- json: **57,464 EPS** (+141% / 2.41× vs 18-09's 23,799)
- msgpack: **52,646 EPS** (+126% / 2.26× vs 18-09's 23,324)

**Inversion:** msgpack now 86% the per-event cost of JSON (6,961 vs 8,067 ns trace total) — was 2.3× SLOWER in 18-09. The parse path is now uniform; msgpack edges ahead because `BeavaValueVisitor` body deserialize is marginally tighter for typical 6-field bodies.

The bottleneck remains the single mio apply thread (consistent finding since 18-04.6). At parallel=4 we hit ~57k EPS (json) — well under the Phase 13 ship-gate of 3M EPS/core. **Plan 18-04.7 (IoPool wiring into the serve loop) is the next throughput unlock**; this plan was about per-event efficiency on the existing single-thread path.

Plan 18-04.6 prior measurement still stands: 44k EPS TCP/small @ parallel=16 (mio EventLoop end-to-end).

Phase 13.3 remains open on worktree `phase-13.3-lockless-apply` — lockless apply (RefCell + LocalSet). Both tracks are independent; Phase 18 executes on `v2/greenfield` directly.

**Stopped at:** Completed 18-10-PLAN.md

## Shipped & Merged to `v2/greenfield`

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Foundation (workspace, axum, /health, /ready, logging, test harness) | ✅ merged |
| 2 | Sources + registry + version bumps + additive-only enforcement | ✅ merged |
| 2.5 | TCP wire listener + framing + full opcode table | ✅ merged |
| 3 | Python SDK skeleton + decorators + expression DSL | ✅ merged |
| 4 | Stateless ops + expression evaluator (server-side) | ✅ merged |
| 5 | Aggregation framework + 8 core operators | ✅ merged |
| 5.5 | Perf harness + retroactive baselines + 10%/25% regression gate | ✅ merged |
| 6 | WAL + idempotency | ✅ merged |
| 6.1 | Async durability — `SyncMode::{Periodic,PerEvent}` + `/push-sync` (Kafka-style acks=1 default, ~15× EPS lift; acks=all via push-sync) | ✅ merged |
| 7 | Snapshot + recovery + schema evolution | ✅ merged |
| 7.5 | End-to-end throughput harness + first baseline + per-phase throughput-run convention | ✅ merged |
| 8 | Point / ordinal / recency operators (15 ops) + TCP `OP_PUSH` | ✅ merged |
| 9 | Decay + velocity operators (16 ops + Python helpers) | ✅ merged |
| 10 | Sketch operators — HLL / CMS / TopK / UDDSketch / Bloom (5 ops) | ✅ merged |
| 11 | Bounded-buffer + geo operators (13 ops + `Value::{List,Map}`) | ✅ merged |
| 11.5 | Temporal MVCC tables + retraction primitive | ✅ merged |
| 13.1 | Perf regression fix — `spawn_blocking` for fsync (17k EPS restored at parallel=64) | ✅ merged |

**v2/greenfield HEAD:** `1495054` (docs session abandon 13.2). Test count: **850 tests green**.

## Shipped Partial — Awaiting Merge + Follow-up

| Branch / worktree | HEAD | What landed | What's left |
|---|---|---|---|
| `phase-12-joins` | `d541971` | Plan 12-02 (WAL replay for `TableUpsert/Delete/Retract`) + path-rewrites for 01/03/04/05/06 | Plans 12-01, 12-03, 12-04, 12-05, 12-06 on `phase-12-followup` worktree |
| `phase-13-ship` | `2ef5afc` | Plan 13-01 (`/metrics` Prometheus + middleware), Plan 13-03 (`env_var_overrides` hermetic fix) | Plans 13-02, 13-04, metric-counter wiring on `phase-13-followup` worktree |

## Remaining Work (priority order)

| # | Task | Where | Status |
|---|------|-------|--------|
| 1 | **Phase 13.3** — lockless apply (RefCell + LocalSet, Option 0) | New branch off `v2/greenfield`; plan: `.planning/ideas/phase-13.3-lockless-apply.md` | ⏳ NEXT |
| 2 | Phase 12 follow-up — Plans 12-01/03/04/05/06 (joins + `push_sync`/`push_many`/`push_table`/`delete_table`/`set`/`mset`/`mget`/`get_multi`) | `.claude/worktrees/phase-12-followup` (off `phase-12-joins`) | ⏳ pending |
| 3 | Phase 13 follow-up — Plans 13-02 (cold-entity GC sweep), 13-04 (perf gate), metric-counter wiring | `.claude/worktrees/phase-13-followup` (off `phase-13-ship`) | ⏳ pending |
| 4 | Merge sequence into `v2/greenfield`: 12-joins + 12-followup → 13-ship + 13-followup | Mainline | ⏳ after 2 & 3 |
| 5 | Final bench + ledger update (`beava-bench` at parallel=64 × small/medium/large × HTTP/TCP × BATCH_MS=0/1/5/20) | `.planning/throughput-baselines.md` | ⏳ after merges |
| 6 | Milestone audit → complete → cleanup (`gsd-audit-milestone` → `gsd-complete-milestone v0.1` → `gsd-cleanup`) | Lifecycle | ⏳ final |

**Deferred to v0.0.x point releases** (per Phase 13 CONTEXT D-16):

- Plan 13-05 docs site (quickstart/operators/concepts/http-api/architecture)
- Plan 13-06 `bv.fork()` local scoped replica subcommand
- Plan 13-07 `pip install beava` + Docker Hub + GitHub Releases packaging
- Plan 13-08 `playground.beava.dev` hosted tutorial

## Performance Snapshot

- **Post-fsync-fix ceiling** (macOS Apple-M4, `v2/greenfield` HEAD): ~17k EPS parallel=64 — apply-lock-bound. Phase 13.3 targets removing the Mutex to unlock the apply loop.
- **Phase 13 ship-gate target:** ≥3M EPS single-thread on three pipeline shapes (simple fraud, complex fraud, recommendation); P99 batch-get < 10ms; P99 `push_sync` < 10ms including fsync.
- **Baselines:** `.planning/perf-baselines.md` (70+ criterion rows, phases 2.5..11.5); `.planning/throughput-baselines.md` (end-to-end EPS + latency ledger).

## Accumulated Context

### Architectural decisions (locked)

- Python SDK is the canonical authoring UX; curl is the language-agnostic escape hatch
- Dual wire: HTTP/JSON + custom-framed TCP `[u32 len][u16 op][u8 content_type][payload]`; Redis-style strict-FIFO correlation (no request_id); `content_type` 0x01 JSON, 0x02 MessagePack reserved; `op=0xFFFF` error_response
- **beava-core WASM-portability invariant:** `beava-core` (expression, registry, ops, aggregations, sketches) stays syscall-free; only `beava-server` + WAL/snapshot crates touch fs/net. Unlocks v0.1+ browser-WASM + edge deployment without refactor
- `@bv.event` (immutable append-only) and `@bv.table(key=..., ttl=...)` (upsertable, with tombstone delete); temporal tables use MVCC (Phase 11.5)
- Aggregations via `Event.group_by(keys).agg(name=bv.<op>(...), ...)` produce Tables
- Stateless ops chain: `filter / select / drop / rename / with_columns / map / cast / fillna`
- Expression DSL: `bv.col("x")` with arithmetic, comparison, `& | ~`, `.isnull()`, `.cast()`
- Joins: event↔event windowed, event↔table enrichment (uses `as_of=` for temporal), table↔table key-matched; `bv.union(*events)` with schema-identity enforcement
- Single Rust process, single apply-loop thread (auxiliary threads for WAL fsync via `spawn_blocking`, HTTP accept, snapshot writer)
- In-memory state only; no RocksDB / fjall / SSD tiering
- Uniform event-time bucketing, cap 64 buckets per windowed operator
- Schema evolution: additive-only registry changes with monotonic version bumps
- Commercial tier (HA, replicas, cross-region) explicitly out of v0 OSS

### Operator catalogue shipped (55 ops)

- Core (8): count, sum, avg, min, max, variance, stddev, ratio — Phase 5
- Sketch (5): count_distinct (HLL), percentile (UDDSketch), top_k (SpaceSaving), bloom_member, entropy — Phase 10
- Point/ordinal (11) + recency (4): first, last, first_n, last_n, lag, first_seen, last_seen, age, has_seen, time_since, time_since_last_n, streak, max_streak, negative_streak, first_seen_in_window — Phase 8
- Decay (7) + velocity (8) + z_score (1): ewma (alias ema), ewvar, ew_zscore, decayed_sum, decayed_count, twa, rate_of_change, inter_arrival_stats, burst_count, delta_from_prev, trend, trend_residual, outlier_count, value_change_count, z_score — Phase 9
- Bounded-buffer (7) + geo (6): histogram, hour_of_day_histogram, dow_hour_histogram, seasonal_deviation, event_type_mix, most_recent_n, reservoir_sample, geo_velocity, geo_distance, geo_spread, unique_cells, geo_entropy, distance_from_home — Phase 11

### Pre-created worktrees (resume points)

```
.claude/worktrees/phase-12-followup     (base: phase-12-joins   @ d541971)
.claude/worktrees/phase-13-followup     (base: phase-13-ship    @ 2ef5afc)
.claude/worktrees/phase-13.2-followup   (base: phase-13.2-coalesce — ABANDONED; do not merge)
```

## Blockers

None active. Quota-wall blockers from the 2026-04-24 06:12 session have reset.

## Historical session notes

- `.planning/SESSION-STATE-2026-04-23.md` — Phase 2.5 → operator-family dispatch
- `.planning/SESSION-STATE-2026-04-24-0612.md` — post-quota-wall handoff with full branch-level detail

---
*State last rewritten: 2026-04-24 — reconciled with actual shipped state after parallel merges (6.1..11.5), Phase 12/13 partial landings, and Phase 13.1 fsync fix merge.*
