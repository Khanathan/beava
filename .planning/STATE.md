---
gsd_state_version: 1.0
milestone: v0.0
milestone_name: milestone
status: Executing Phase 18
stopped_at: "Phase 18 in flight — 18-04.8 (body→Row on IoPool) running; 18-12 + continuous pipelining queued; Phase 13.3 REJECTED"
last_updated: "2026-04-26T00:00:00.000Z"
progress:
  total_phases: 23
  completed_phases: 9
  total_plans: 94
  completed_plans: 65
  percent: 69
---

# State: Beava v2 — v0 OSS Launch

**Project reference:** `.planning/PROJECT.md`
**Roadmap:** `.planning/ROADMAP.md` (26 phases — see roadmap for the full inserted-phase note)
**Requirements:** `.planning/REQUIREMENTS.md`
**Milestone:** v0 (first public OSS cut on beava.dev)
**Created:** 2026-04-22
**Last revised:** 2026-04-26 (Phase 18 cleanup — 18-04.7 IoPool wiring + 18-11 hot-path optimization both landed and merged; bench burst pipelining landed; Phase 13.3 REJECTED per architectural decision: Beava commits to single-threaded data plane forever, scale-out via multi-instance Redis-cluster pattern)

## Core Value

Feature authoring as composable Python code that ships to production unchanged. Users write `@bv.event` / `@bv.table(key=...)` / `bv.col(...)` / `.filter().group_by().agg()` / `.join()` / `bv.union(...)` / `app.register(...)` / `app.push(...)` / `app.get(...)`, deploy unchanged.

## Current Focus

**Phase 18 — Redis-shaped hand-rolled hot path. Two plans remaining; one in flight, one queued.**

### Landed and merged on `v2/greenfield` (HEAD `913eaa3`):

- **Plan 18-09** — msgpack-on-TCP (CT_MSGPACK), Row::Deserialize impl, WAL v=2 binary records
- **Plan 18-10** — hand-rolled envelope parsers: `parse_msgpack_envelope` (33 ns / 57× faster), `parse_json_envelope` (77 ns / 7.6× faster), `BeavaValueVisitor` direct Row deserialize
- **Plan 18-04.7** — IoPool wiring into `serve_with_dirs`: parse + encode moved off apply thread, per-tick lifecycle [poll → distribute_reads → join → apply → distribute_writes → join]
- **Plan 18-11** — hot-path optimization: Row.0 → SmallVec<[(CompactString, Value); 8]>; Value::Str(CompactString); AggStateTable → hashbrown::HashMap+FxBuildHasher with raw_entry_mut; EntityKey SmallVec; Arc<EventDescriptor>; per-source aggregation index. agg stage 5× faster (3,191 → 529 ns), parse 6× faster (911 → 150 ns)
- **env::var caching** for trace flags (OnceLock per process — saves ~200 ns/event when trace OFF)
- **`TRACE_AGG_TIMING` env var split** so outer trace doesn't include inner eprintln cost
- **bench-v18 `--pipeline-depth N` flag** — burst pipelining baseline; 6-8× EPS lift on M4 loopback at p=16/pd=256

### In flight (background Opus executor):

- **Plan 18-04.8** — body→Row deserialization moves from apply thread to IoPool worker; expected: apply-thread `parse` stage 193 ns → ≤50 ns; IoPool runtime timing trace under same `BEAVA_TRACE_APPLY_TIMING` env var

### Queued (next session, after 18-04.8 lands):

- **Plan 18-12** — `Arc<str>` event_name in EventIdEntry::Stream (refcount bump vs String alloc); expected: bookkeeping stage 169 ns → ≤60 ns
- **Continuous pipelining for bench-v18** — replace burst send-N/read-N with split sender/receiver + tokio Semaphore; constant load on apply thread (no sawtooth gaps)

### Architectural decision LOCKED 2026-04-26:

**Phase 13.3 (in-process apply sharding via lockless RefCell + LocalSet) is REJECTED.** Beava commits to single-threaded data plane forever. Per-instance throughput ceiling = single apply thread (~1M EPS for simple counters, ~400k for medium aggregations on Linux Xeon post-current optimizations). For higher aggregate throughput, users run **multiple Beava instances** sharded at the entity-key level (Redis-cluster pattern). Cross-shard queries within a process are explicitly avoided.

### Headline numbers (M4 loopback, post-merge of 18-11 + 18-04.7 + env-cache):

- `parse_msgpack_envelope` microbench: **33.4 ns**
- `parse_json_envelope` microbench: **77.1 ns**
- agg stage (clean trace): **518 ns** (was 3,191 ns)
- TOTAL push (clean trace, p=4/pd=64): **964 ns** (was 5,154 ns) — **5.3× faster**
- Apply-thread theoretical max at p50 cycle: ~1.04M EPS single-thread
- Best observed EPS: **361k - 378k** at p=16/pd=256 (json/msgpack), bench-side bursty load is the wall

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

### Phase 18 (only two items left):

| # | Task | Where | Status |
|---|------|-------|--------|
| 1 | **Plan 18-04.8** — body→Row migration from apply thread to IoPool worker + IoPool runtime timing trace | In flight via background Opus executor on `v2/greenfield` | ⏳ in flight |
| 2 | **Plan 18-12** — `Arc<str>` event_name to kill bookkeeping String alloc | Queued; awaits 18-04.8 (file conflict on apply_shard.rs) | ⏳ queued |
| 3 | **Continuous pipelining for bench-v18** — split sender/receiver + Semaphore; replaces burst pattern | Queued (bundled with 18-12 dispatch) | ⏳ queued |

### Phase 18 cleanup (after the above land):

- Run combined post-everything EPS sweep + agg sub-stage trace; append to `throughput-baselines.md`
- Phase 18 SUMMARY.md (overall phase wrap)
- Phase 18 verification

### Other phase follow-ups (not Phase 18):

| # | Task | Where | Status |
|---|------|-------|--------|
| A | Phase 12 follow-up — Plans 12-01/03/04/05/06 (joins + `push_sync`/`push_many`/`push_table`/`delete_table`/`set`/`mset`/`mget`/`get_multi`) | `.claude/worktrees/phase-12-followup` (off `phase-12-joins`) | ⏳ pending |
| B | Phase 13 follow-up — Plans 13-02 (cold-entity GC sweep), 13-04 (perf gate), metric-counter wiring | `.claude/worktrees/phase-13-followup` (off `phase-13-ship`) | ⏳ pending |
| C | Merge sequence: 12-joins + 12-followup → 13-ship + 13-followup → v2/greenfield | Mainline | ⏳ after A & B |
| D | Final bench + ledger update (`beava-bench` at parallel=64 × small/medium/large × HTTP/TCP × BATCH_MS=0/1/5/20) | `.planning/throughput-baselines.md` | ⏳ after merges |
| E | Milestone audit → complete → cleanup (`gsd-audit-milestone` → `gsd-complete-milestone v0.0` → `gsd-cleanup`) | Lifecycle | ⏳ final |

### REJECTED (do not propose as future plans):

- ~~**Phase 13.3** — lockless apply (RefCell + LocalSet)~~ — single-threaded data plane LOCKED 2026-04-26; users scale out via multi-instance Redis-cluster pattern instead. Worktree `.claude/worktrees/phase-13.3-lockless-apply` archived for historical reference.

**Deferred to v0.0.x point releases** (per Phase 13 CONTEXT D-16):

- Plan 13-05 docs site (quickstart/operators/concepts/http-api/architecture)
- Plan 13-06 `bv.fork()` local scoped replica subcommand
- Plan 13-07 `pip install beava` + Docker Hub + GitHub Releases packaging
- Plan 13-08 `playground.beava.dev` hosted tutorial

## Performance Snapshot

- **Post-merge ceiling** (macOS Apple-M4, `v2/greenfield` HEAD `913eaa3` — post-18-11 + post-18-04.7 + bench burst pipelining): **~378k EPS** at p=16/pd=256 (msgpack), bench-side bursty load is the wall.
- **Apply-thread per-event work (clean trace):** 964 ns mean / 750 ns p50 → theoretical ~1.04M EPS single-thread at p50 cycle.
- **Per-stage breakdown (mean ns post-merge):** parse 193 (body→Row, will move to IoPool in 18-04.8), lookup 31, validate 32, wal_build 33, wal_append 43, agg 473, bookkeeping 169 (will drop to ~50 in 18-12).
- **Phase 13 ship-gate target:** ≥3M EPS/core single-instance on simple-fraud (medium pipeline) shape — REFRAMED (post-13.3-rejection) as **per-instance peak achievable on Linux Xeon with all 18-04.7 + 18-04.8 + 18-12 + future 18-05 io_uring + OP_PUSH_MANY**. For aggregate >1 instance ceiling: scale out (multiple Beava instances).
- **Baselines:** `.planning/perf-baselines.md` (criterion rows, phases 2.5..18-11); `.planning/throughput-baselines.md` (end-to-end EPS + latency ledger across 18-09, 18-10, 18-11, 18-04.7).

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
