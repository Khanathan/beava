---
gsd_state_version: 1.0
milestone: v0.0
milestone_name: milestone
status: Executing Phase 18
last_updated: "2026-04-26T23:00:00.000Z"
progress:
  total_phases: 23
  completed_phases: 9
  total_plans: 95
  completed_plans: 65
  percent: 68
---

# State: Beava v2 — v0 OSS Launch

**Project reference:** `.planning/PROJECT.md`
**Roadmap:** `.planning/ROADMAP.md` (26 phases — see roadmap for the full inserted-phase note)
**Requirements:** `.planning/REQUIREMENTS.md`
**Milestone:** v0 (first public OSS cut on beava.dev)
**Created:** 2026-04-22
**Last revised:** 2026-04-26 (Phase 19 CONTEXT.md captured at `.planning/phases/19-1m-bench/19-CONTEXT.md` — 4 areas locked: blast shape (4 modes side-by-side), pipelining (continuous + burst), Python harness via public app.push() multi-process, WIP stash receiver-flips-stop pattern. Phase 18 wrap items remaining: SUMMARY + verification + worktree archival decision)

**Session resumed:** 2026-04-26 23:00 UTC — Phase 19 discuss complete; ready for `/gsd-plan-phase 19`. Phase 18 SUMMARY deferred until after Phase 19 produces the headline 1M EPS data point.

## Core Value

Feature authoring as composable Python code that ships to production unchanged. Users write `@bv.event` / `@bv.table(key=...)` / `bv.col(...)` / `.filter().group_by().agg()` / `.join()` / `bv.union(...)` / `app.register(...)` / `app.push(...)` / `app.get(...)`, deploy unchanged.

## Current Focus

**Phase 18 — Redis-shaped hand-rolled hot path. ALL plans landed + continuous pipelining landed; only Phase 18 wrap (SUMMARY + verification + worktree archival decision) remains.**

### Landed and merged on `v2/greenfield` (HEAD `a809d04`):

- **Plan 18-09** — msgpack-on-TCP (CT_MSGPACK), Row::Deserialize impl, WAL v=2 binary records
- **Plan 18-10** — hand-rolled envelope parsers: `parse_msgpack_envelope` (33 ns / 57× faster), `parse_json_envelope` (77 ns / 7.6× faster), `BeavaValueVisitor` direct Row deserialize
- **Plan 18-04.7** — IoPool wiring into `serve_with_dirs`: parse + encode moved off apply thread, per-tick lifecycle [poll → distribute_reads → join → apply → distribute_writes → join]
- **Plan 18-04.8** — body→Row deserialize moved off apply onto IoPool worker; apply parse stage 193 → 77 ns; IoPool runtime timing trace under same `BEAVA_TRACE_APPLY_TIMING` env var
- **Plan 18-11** — hot-path optimization: Row.0 → SmallVec<[(CompactString, Value); 8]>; Value::Str(CompactString); AggStateTable → hashbrown::HashMap+FxBuildHasher with raw_entry_mut; EntityKey SmallVec; Arc<EventDescriptor>; per-source aggregation index. agg stage 5× faster (3,191 → 529 ns), parse 6× faster (911 → 150 ns)
- **Plan 18-12** — `Arc<str>` event_name in EventIdEntry::Stream + EventDescriptor.name_arc pre-allocated at registration; bookkeeping site refcount-bumps registry-resident Arc<str> (no per-push String alloc). EPS at p=16/pd=256 json **346k → 462k (+33.5%)**, msgpack **357k → 487k (+36.4%)**. Trace per-stage mean held flat (mutex+insert dominates the bookkeeping stage); the EPS lift came from removed allocator pressure / cache pollution that the in-window trace doesn't capture
- **env::var caching** for trace flags (OnceLock per process — saves ~200 ns/event when trace OFF)
- **`TRACE_AGG_TIMING` env var split** so outer trace doesn't include inner eprintln cost
- **bench-v18 `--pipeline-depth N` flag** — burst pipelining baseline; 6-8× EPS lift on M4 loopback at p=16/pd=256

### Phase 18 wrap (still TBD):

- **Phase 18 SUMMARY.md** — overall phase wrap covering 18-09, 18-10, 18-11, 18-04.7, 18-04.8, 18-12, plus continuous pipelining
- **Phase 18 verification** — `/gsd-verify-work 18` against the phase goal
- **`phase-13.3-lockless-apply` worktree archival decision** — delete vs rename to `archived/phase-13.3-rejected` (Phase 13.3 REJECTED 2026-04-26 per architectural decision)

### Architectural decision LOCKED 2026-04-26:

**Phase 13.3 (in-process apply sharding via lockless RefCell + LocalSet) is REJECTED.** Beava commits to single-threaded data plane forever. Per-instance throughput ceiling = single apply thread (~1M EPS for simple counters, ~400k for medium aggregations on Linux Xeon post-current optimizations). For higher aggregate throughput, users run **multiple Beava instances** sharded at the entity-key level (Redis-cluster pattern). Cross-shard queries within a process are explicitly avoided.

### Headline numbers (M4 loopback, post-merge of 18-12 + continuous pipelining, commit `a809d04`):

- `parse_msgpack_envelope` microbench: **33.4 ns**
- `parse_json_envelope` microbench: **77.1 ns**
- agg stage (clean trace): **500 ns** (was 3,191 ns at start of phase)
- TOTAL push (clean trace, p=4/pd=64): **888 ns** (was 5,154 ns at start of phase) — **5.8× faster**
- Apply-thread theoretical max at p50 cycle: ~1.13M EPS single-thread
- Best-of-3 EPS at p=16/pd=256 (continuous pipelining mode): **mean 375k json / 400k msgpack** with 3-7× tighter variance than burst mode. Burst-mode upper-tail EPS (462k/487k) still observed but with much wider variance band; continuous is the new default

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

### Phase 18 (all data-plane items landed):

| # | Task | Where | Status |
|---|------|-------|--------|
| 1 | **Plan 18-04.8** — body→Row migration from apply thread to IoPool worker + IoPool runtime timing trace | DONE 2026-04-26 (commits 9a1daec/6ed8b97/677d3ea on v2/greenfield). Apply parse 193 → 77 ns (-60%); apply TOTAL 974 → 941 ns; IoPool parse_body=4,265 ns mean; EPS p=16/pd=256 json 346k / msgpack 357k; new TRACE_APPLY io trace lives under same BEAVA_TRACE_APPLY_TIMING var | ✅ done |
| 2 | **Plan 18-12** — `Arc<str>` event_name to kill bookkeeping String alloc | DONE 2026-04-26 (commits e96c59b → adaa66e on v2/greenfield). EPS at p=16/pd=256 json 346k → 462k (+33.5%), msgpack 357k → 487k (+36.4%); EPS at p=4/pd=64 json 165k → 239k (+44.5%); apply TOTAL 941 → 888 ns. Trace per-stage mean held flat (mutex+insert dominates bookkeeping stage; ~50-100 ns alloc savings absorbed by ±25 ns variance band); EPS lift came from removed allocator pressure / cache pollution that in-window trace doesn't capture | ✅ done |
| 3 | **Continuous pipelining for bench-v18** — split sender/receiver + Semaphore; replaces burst pattern | DONE 2026-04-26 (commit a809d04 on v2/greenfield). `--continuous-pipeline` flag (default true); tokio::io::split + Semaphore + mpsc<Instant> for FIFO ack-pairing + latency batching to mirror burst's lock amortization. Best-of-3: json 322k → 375k mean (+16%, **7× tighter variance**), msgpack 374k → 400k mean (+7%, **3× tighter variance**). Continuous reports REAL per-event wall-clock latency vs burst's amortized batch_total/N | ✅ done |

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

- **Post-merge ceiling** (macOS Apple-M4, `v2/greenfield` HEAD `adaa66e` — post-18-12): **462k EPS (json) / 487k EPS (msgpack)** at p=16/pd=256, bench-side bursty load is the next wall (continuous pipelining is the queued unlock).
- **Apply-thread per-event work (clean trace):** 888 ns mean (was 941 ns post-18-04.8); theoretical ~1.13M EPS single-thread at p50 cycle.
- **Per-stage breakdown (mean ns post-18-12, n=67k):** parse 67, lookup 28, validate 29, wal_build 30, wal_append 36, agg 500, bookkeeping 194 (mutex + HashMap::insert; the 50-100 ns String alloc removal is absorbed in stage variance — see 18-12-SUMMARY.md for analysis).
- **Phase 13 ship-gate target:** ≥3M EPS/core single-instance on simple-fraud (medium pipeline) shape — REFRAMED (post-13.3-rejection) as **per-instance peak achievable on Linux Xeon with all 18-04.7 + 18-04.8 + 18-12 + future 18-05 io_uring + OP_PUSH_MANY**. For aggregate >1 instance ceiling: scale out (multiple Beava instances).
- **Baselines:** `.planning/perf-baselines.md` (criterion rows, phases 2.5..18-11); `.planning/throughput-baselines.md` (end-to-end EPS + latency ledger across 18-09, 18-10, 18-11, 18-04.7, 18-04.8, 18-12).

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
