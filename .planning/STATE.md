---
gsd_state_version: 1.0
milestone: v0.0
milestone_name: milestone
status: Executing Phase 12.7
last_updated: "2026-05-01T12:01:54Z"
progress:
  total_phases: 37
  completed_phases: 17
  total_plans: 157
  completed_plans: 122
  percent: 77
---

<!-- Session continuity (resume) -->
<!-- Last session: 2026-05-01 — /gsd-execute-phase 12.7 Plan 04 landed (Wave 2 sequel: server-side temporal-module strip). Whole-module deleted crates/beava-server/src/temporal_http.rs (~756 LOC) + crates/beava-core/src/temporal.rs (~394 LOC) + crates/beava-core/benches/temporal_mvcc.rs (~69 LOC orphan bench). DevAggState slimmed by 2 fields (temporal_stores + event_id_index); EventIdEntry enum (incl TableWrite + Stream variants) deleted; GlueResponse::TemporalResponse variant + server.rs:2186 consumer arm deleted; apply_shard.rs step 10 (orphan event_id_index lock) deleted. LOC delta: +76 / -1,358 (net ~1,282 removed; planning estimate 1,150). Plan 02's RED inventory dropped 15 → 8 occurrences (7 cleared: TemporalStore, MvccVersion, temporal_http, plus runtime_core_glue/server.rs cleanup of TemporalResponse leftover). 1 new architectural sub-test GREEN (app_temporal_fields_deleted); legacy_table_files_deleted partial 2/3 (Rust files gone; python/_tables.py pending Plan 06). Test maintenance: phase18_12_arc_str_bookkeeping_test #![cfg(any())]-gated (slated for deletion Plan 06); phase12_6_legacy_axum_killed::temporal_http_axum_handlers_deleted repointed to file-absence assertion. Commit: 4d0fabd (single GREEN; RED gate was Plan 02's existing architectural test). Workspace green, clippy clean, fmt clean, 100/100 test files PASSED. -->
<!-- Stopped at: Plan 12.7-04 CLOSED; ready for /gsd-execute-phase 12.7 next plan (12.7-05 persistence schema reset — RecordType::TableUpsert/TableDelete/Retract delete + FORMAT_VERSION 2→1 reset OR 12.7-06 Python SDK strip — both Wave-3, parallel-runnable) per CONTEXT.md wave order -->
<!-- Resume files: .planning/phases/12.7-table-strip/12.7-04-SUMMARY.md (Plan 04 narrative) + .planning/phases/12.7-table-strip/12.7-CONTEXT.md (locked decisions D-01..D-04) -->

# State: Beava v2 — v0 OSS Launch

**Project reference:** `.planning/PROJECT.md`
**Roadmap:** `.planning/ROADMAP.md` (26 phases — see roadmap for the full inserted-phase note)
**Requirements:** `.planning/REQUIREMENTS.md`
**Milestone:** v0 (first public OSS cut on beava.dev)
**Created:** 2026-04-22
**Last revised:** 2026-04-26 (Phase 19 CONTEXT.md captured at `.planning/phases/19-1m-bench/19-CONTEXT.md` — 4 areas locked: blast shape (4 modes side-by-side), pipelining (continuous + burst), Python harness via public app.push() multi-process, WIP stash receiver-flips-stop pattern. Phase 18 wrap items remaining: SUMMARY + verification + worktree archival decision)

**Session resumed:** 2026-04-27 — Phase 19.1 family fully complete (verdict PASS, HEAD `3e28b77`). Phase 19.2 consolidated from prior 19.2 + 19.3 + two opus audit findings.

**Phase 19.2 CONTEXT captured 2026-04-27** at `.planning/phases/19.2-big-apply-path-optimization/19.2-CONTEXT.md` (commit `666099b`) — 8 decisions locked across 7 questions:

- D-01: Field pre-extraction = indexed array (`Vec<&Value>`, register-time field-idx)
- D-02a/b: Process-static AHasher init + FxHasher for HLL ops
- D-03: EntityKey hybrid (`SingleU64`/`SingleStr`/`Multi`)
- D-04: Cluster shape = split-by-agg_id (shared EntityKey + lookup; per-agg `Vec<AggOp>`)
- D-05: **Remove `bv.unique_cells` + `bv.geo_entropy`** (catalogue 55→53; recipes: `count_distinct(quadkey)`, `entropy(quadkey)`)
- D-05a: Apply `max_categories` cap + drop-new + cap-hit metric to `bv.entropy`
- D-06: Cost-class metadata = hand-maintained `docs/operators/cost-class.md`
- D-07: `/debug/op-cost` endpoint feature-gated behind `BEAVA_DEV_ENDPOINTS=1`
- D-08: `apply_path_bench.rs` criterion microbench + Phase 19.2 rebaseline matrix

**Next:** `/gsd-plan-phase 19.2` to break into 6-8 plans across 3-4 waves.

---

**Phase 19.3 CLOSED 2026-04-28 at PASS-WITH-DEFICIT.** D-04 architectural fix landed (Plan 19.3-02: `WindowedOp::update_at`). Wrapper-bypass anti-pattern resolved. Performance lift +4.4% EPS / -1,526 ns agg-stage (within run-to-run noise band). Predicted lift was 60% overestimated due to cost-model conjecture in `19.2-INVESTIGATION.md §4`; flamegraph + cost-model investigation (`19.3-COST-MODEL.md`, `19.3-FLAMEGRAPH.md`) identified 5 NEW levers and superseded Plans 19.3-03/04/05. Memory `feedback_cost_model_from_flamegraph` saved.

**Phase 19.4 OPENED 2026-04-28** at `.planning/phases/19.4-final-100k-push/` — final v0 ship-gate optimization, flamegraph-derived scope. Goal: lift fraud-team K=10k zipfian from 73,743 EPS (post-19.3-A) to ≥100,000 EPS (PASS gate).

**Phase 19.4 sub-goals (all PASSED — final verdict 102,800 EPS at Plan 04 closure):**

1. **19.4-A** CountDistinct identity-hasher fix (`std::HashSet<u64>` rehashes via SipHash) → ~85k EPS (-1,180 ns/event, ~3h work) — **PASS** (79,367 EPS / 11,667 ns agg-stage)
2. **19.4-B** ExtractedFields SmallVec inline-cap 8→16 (TxnByUser cluster spills) → ~91k EPS (-530 ns/event, 1-line) — **PASS attempt #3** at quieter load (96,298 EPS / 10,329 ns agg-stage)
3. **19.4-C** Geo lat/lon pre-extraction (D-01 missed geo path) → ~94k EPS (-360 ns/event, ~4h) — **PASS** first attempt (94,733 EPS / 8,244 ns agg-stage; samply confirms `agg_geo::read_lat_lon` slow path eliminated, 0.000% self-time was 2.86%)
4. **19.4-D** ExtractedFields hoist above descriptor loop (carried from 19.3-04) → ~105k EPS (-1,200 ns/event predicted, -100 ns measured trace; hoist correctness confirmed by criterion -10.9%) — **PASS-on-EPS-goal** (102,800 EPS clears 100k Phase-19.4 PASS gate; trace-floor missed because cost model overstated post-Plan-02 cap-widening)
5. **19.4-E** Sanity flamegraph + throughput rebaseline + dual-measurement verification + Phase 19 closure — **PASS** (3 of 4 predicted hot-function shifts confirmed; 5-pipeline rebaseline no WARN/BLOCK; anti-pattern sweep 7/7 PASS; Phase 19 amended PASS-WITH-DEFICIT → PASS)

**Phase 19.4 CLOSED 2026-04-28 at PASS.** fraud-team K=10k zipfian sustained_eps cumulative trajectory:

- post-19.3 12,533 ns / 73,743 EPS → post-19.4-01 11,667 ns / 79,367 EPS → post-19.4-02 10,329 ns / 96,298 EPS → post-19.4-03 8,244 ns / 94,733 EPS → **post-19.4-04 8,344 ns / 102,800 EPS (Plan 04 closure measurement)** = +39% over the phase, **clears 100k v0 ship gate**.

**Phase 19 verdict amended PASS-WITH-DEFICIT → PASS** (cumulative path: Phase 19.1 bench wall-clock fix amendment + Phase 19.2/19.3/19.4 chained apply-path optimizations).

**Phase 19.5+ pivots to scale-out** (sharding deployment + multi-instance benchmarks per `project_no_sharded_apply`); vertical optimization stops here. **Phase 19.5 is OUT OF v0 ship critical path.**

**Next: v0 ship critical path:** Phase 14 → 15 → 12 followup → 12.5 → 16 → 13 followup → ship.

---

**Plan 12-07 closed 2026-04-29** (commit `9bb18c7`). Production binary on ServerV18; /get works HTTP+TCP without env-var workarounds; read_bench.py end-to-end ok=1000/1000 with p99=1.81 ms. Wave-by-wave: WireRequest TcpGet variants → TCP parser routing → apply_shard dispatch → real dispatch_get_batch (replaces stub) → OP_GET_RESPONSE = 0x0023 + TCP encoder → /health shim on mio HTTP listener → main.rs migrated to ServerV18 + Config::admin_addr → integration tests + read_bench.py validation → criterion microbench + throughput rebaseline. **Throughput regression-gate: small/tcp 694,144 EPS post-12-07 vs 642,760 EPS post-19.4 = +8.0% (PASS).** Plan 12-08 (push-and-get over mio HTTP+TCP) is unblocked. SUMMARY: `.planning/phases/12-server-side-async-push-coalescing/12-07-SUMMARY.md`.

**Verification artifacts (commit `ff5579a`):**

- `.planning/phases/19.4-final-100k-push/19.4-VERIFICATION.md` — OVERALL: PASS, full evidence
- `.planning/phases/19.4-final-100k-push/19.4-FLAMEGRAPH-POST.md` — sanity flamegraph + artifact analysis
- `.planning/phases/19.4-final-100k-push/19.4-05-SUMMARY.md` — plan summary
- `.planning/throughput-baselines.md` — ## 1M-event blast (rebaseline 19.4) section
- `.planning/perf-baselines.md` — ### Phase 19.4 — 19.4-E Final cumulative baseline section
- `.planning/phases/19-1m-bench/19-VERIFICATION.md` — Amendment 2026-04-28 (Phase 19.4 closure)
- `.planning/phases/19-1m-bench/19-SUMMARY.md` — verdict updated 2026-04-28

**Phase 19.1 OPENED 2026-04-27** as the consolidated umbrella for the post-Phase-19 follow-up work (rolls together what was originally proposed as 19.0.1 / 19.0.2 / 19.0.3 mini-phases). See ROADMAP.md → "Phase 19.1: Realistic-shape benchmark + bench/WAL fixes + complex-pipeline optimization" for the full goal/sub-goal/success-criteria block.

**Phase 19.1 scope:**

1. **Path B — fraud-team.json validation** (primary tuning benchmark; locked decision per memory `project_fraud_team_primary_bench`)
2. **Bench wall_clock fix** (1-line elapsed-move + tokio::select! per memory `project_phase19_bench_wallclock_fix`; flips Phase 19 verdict PASS-WITH-DEFICIT → PASS)
3. **WAL config bump** (4×32MiB tick=20ms middle-ground default candidate per memory `project_phase19_wal_experiment`; experimental 8×64MiB tick=100ms eliminated bimodal tail with +33% EPS but 512MB RSS)
4. **Re-baselined Phase 19 numbers** (re-run small/medium/large/large_phase9 + new fraud-team.json zipfian cell; amend 19-VERIFICATION verdict)
5. **Complex-pipeline apply-thread optimization** (≥1 of: WindowedOp lazy buckets / same-key batch sketch updates / OP_PUSH_MANY adoption — measured against fraud-team.json zipfian)

**Next:** `/gsd-discuss-phase 19.1` to capture context decisions (numbering, WAL default, histogram windowed semantics, stretch scope), then `/gsd-plan-phase 19.1` to break into 4–5 plans across 3 waves.

## Core Value

Feature authoring as composable Python code that ships to production unchanged. Users write `@bv.event` / `@bv.table(key=...)` / `bv.col(...)` / `.filter().group_by().agg()` / `app.register(...)` / `app.push(...)` / `app.get(...)`, deploy unchanged. Semantics: Redis-shaped, processing-time only (no event-time, no joins, no watermarks — locked 2026-04-30 per `project_redis_shaped_no_event_time_ever`).

## Architectural pivot 2026-04-30 — no event-time / no joins / no watermarks (PERMANENT)

**Locked.** State is `f(arrival-order events, query time)`. mio data plane is the only hot-path entry. Phases 14, 14.1, 15 archived. Phase 12 retitled "push/get API completion (joins/unions REMOVED)". Phase 17 reworked. Phase 12.5 archived (superseded by Plan 12-10). NEW Phase 12.6 inserted (v0 surface reduction — legacy axum kill + event-time strip + dead-code/redundancy sweep + windowed-op time-source swap + join/union removal + REQUIREMENTS sweep + mio-only enforcement). NEW Phase 25 inserted (session window operator family — v0.1+).

**v0 critical path post-pivot:** ~~Plan 12-10 (push-and-get on mio)~~ DEFERRED per Phase 12.6 D-04 → ~~Phase 12.6 (surface reduction)~~ ✅ **CLOSED 2026-04-30 (PASS-WITH-WARN)** → **Phase 13 (docs + packaging + ship) — NEXT**. Phase 25 (session windows) is v0.1+. Phases 14/14.1/15 are dead architecture — do not unarchive without explicit user override + new ADR.

## Current Focus

**Phase 12.6 CLOSED 2026-04-30 (PASS-WITH-WARN) — v0 surface reduction landed.** 15 plans across 8 waves (Plans 01-15 inclusive of Wave-1.5 gap closure 14+15). HEAD `1e318b1` (`chore: merge plan 12.6-12 (Phase 12.6 throughput baseline — PASS)`). 76 commits in the Phase 12.6 commit range. Workspace **1067 passed / 0 failed / 3 ignored** with `cargo clippy + cargo fmt` clean. Legacy axum data plane DELETED (~7475 LOC across `push.rs` / `http.rs` / `push_and_get.rs` / `tcp.rs` / legacy `Server` struct). mio is the SOLE data-plane runtime per `project_phase18_no_dual_runtime` — enforced by `phase12_6_mio_only_dataplane.rs` architectural test. `event_time_ms` / `event_time_field` / `tolerate_delay_ms` HARD ripped from push wire + register wire + EventDescriptor + DevAggState + WAL/snapshot schema (v1→v2) + Python SDK decorator. `OpNode::Join` / `OpNode::Union` / `JoinType` deleted. Path X swapped windowed-op time source from event_time_ms to server `now_ms()`. Microbench (Plan 11) captured 3 cells as first measurement; throughput rebaseline (Plan 12) at -0.94% on small/tcp gate cell vs post-12-08 baseline (PASS). All 5 CONTEXT decisions D-01..D-05 honored verbatim. SUMMARY: `.planning/phases/12.6-v0-surface-reduction/12.6-SUMMARY.md`. VERIFICATION: `.planning/phases/12.6-v0-surface-reduction/12.6-VERIFICATION.md`.

**Plan 12-07/08/09 closed 2026-04-29 (Phase 12 sequence).** main.rs migrated to ServerV18 (mio data plane); `/get` on mio HTTP+TCP via apply_shard; apply-loop overhead reduction 1095→75 ns/event (14.6×); TCP /get msgpack default. Legacy push.rs / push_and_get.rs / tcp.rs subsequently deleted by Phase 12.6 Plan 07.

**Phase 18 wrap items folded into Phase 12.6 closure** (worktree archival recorded by Plan 09; SUMMARY/verification work absorbed into Phase 12.6 SUMMARY's plan-by-plan TOC and per-plan SUMMARY references).

**Next: v0 critical path → Phase 13.** `/gsd-discuss-phase 13` for ship-readiness scope (Hetzner Linux baseline + multi-instance shard-scaling validation per `project_no_sharded_apply`; PyPI / Docker / GitHub Releases packaging; quickstart docs; concept docs / operator docs / HTTP API docs sweep with no-event-time pivot — D-05 deferred work). Plan 12-10 (push-and-get) DEFERRED entirely from v0 per Phase 12.6 D-04.

---

### Legacy: Phase 18 — Redis-shaped hand-rolled hot path landed + continuous pipelining landed; only Phase 18 wrap (SUMMARY + verification + worktree archival decision) remains. main.rs migration closed by Plan 12-07.

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
- **main.rs migration to ServerV18 completed in Plan 12-07 (commit `2ede08f`)** — production binary now boots ServerV18 (mio data plane) per memory `project_phase18_no_dual_runtime`. Legacy `Server` retained for `phase6_crash_probe` + `TestServer`.

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
| `phase-12-joins` | `d541971` | Plan 12-02 (WAL replay for `TableUpsert/Delete/Retract`) + path-rewrites for 01/03/04/05/06 | **ABANDONED 2026-04-30 per Phase 12.6-09** — joins removed permanently per `project_redis_shaped_no_event_time_ever`. Plan 12-02 (TableUpsert/Delete/Retract WAL replay) is non-join work; if revived, cherry-pick onto `v2/greenfield`, do NOT merge from this branch. Non-join survivors (Plans 12-01/03/04/05/06) tracked separately on `phase-12-followup`. |
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

### Worktree map post-Phase-12.6 (2026-04-30, recorded by Plan 12.6-09)

Post-architectural-pivot worktree fates. Plan 12.6-09 audits and records each branch's status:

| Worktree / branch | Status | Rationale |
|---|---|---|
| `phase-12-joins` (HEAD `d541971`) | **ABANDONED 2026-04-30 per Phase 12.6-09** | Joins removed permanently per `project_redis_shaped_no_event_time_ever`. Phase 12.6-04 deletes joins as architecture. Plan 12-02 (WAL replay for `TableUpsert/Delete/Retract`) is non-join work; if revived, cherry-pick onto `v2/greenfield` directly, do NOT merge from this branch. |
| `phase-12-followup` (off `phase-12-joins`) | **REBASE PENDING** | Off-branch dependency on `phase-12-joins` is now stale (parent ABANDONED). Either rebase onto `v2/greenfield` (preferred — preserves Plans 12-01/03/04/05/06 survivors) or abandon and recreate the followup branch fresh off `v2/greenfield`. |
| `phase-13-followup` (off `phase-13-ship` @ `2ef5afc`) | **KEEP** | Plans 13-02 (cold-entity GC sweep), 13-04 (perf gate), and metric-counter wiring still active per Phase 13 critical path. |
| `phase-13.1-perf-fix` | **KEEP** (already merged) | Phase 13.1 fsync regression fix landed; worktree may be cleaned up by Phase 13 lifecycle pass — not Phase 12.6's scope. |
| `phase-13-ship` | **KEEP** | Base branch for `phase-13-followup`; Plans 13-01 / 13-03 already merged to `v2/greenfield`. |
| `phase-13.2-followup` (off `phase-13.2-coalesce`) | **ABANDONED** (already noted line 261; Phase 12.6-09 confirms) | Phase 13.2 superseded by Phase 13.3 (which itself was rejected); branch is dead. |
| `phase-13.3-lockless-apply` | **ARCHIVED-REJECTED 2026-04-26** (already noted line 213; Phase 12.6-09 confirms + adds banners to `.planning/phases/13.3-lockless-apply/*.md`) | Single-threaded data plane locked per `project_no_sharded_apply`. Worktree was deleted 2026-04-26; planning files retained for historical reference and now banner-stamped. |
| `phase-15-event-time-pit` | **ARCHIVED 2026-04-30** (per no-event-time pivot) | Event-time gone permanently per `project_redis_shaped_no_event_time_ever`. Worktree may stay on disk for historical reference; do not check out for new work. Phase dir already moved to `.planning/phases/_archived-15-event-time-pit-killed-no-event-time/` per ROADMAP line 57. |
| `phase-16-sdk-source-annotation` | **NEEDS REASSESSMENT** | Phase 16 reworked 2026-04-30 (`tolerate_delay_ms` + `modifiable=True` references removed by no-event-time pivot; remaining `@bv.source` + `app.upsert/delete` scope is intact). Worktree status pending Phase 13 sweep — defer revisit. |

**Section ownership note:** Plan 12.6-09 owns this worktree-status sub-block. Plan 12.6-13 (Wave 8) owns the phase-progress block + Current Focus line. The Phase 12.5 dir banners + this map were added together in `docs(12.6-09)` GREEN commit.

## Blockers

None active. Quota-wall blockers from the 2026-04-24 06:12 session have reset.

## Historical session notes

- `.planning/SESSION-STATE-2026-04-23.md` — Phase 2.5 → operator-family dispatch
- `.planning/SESSION-STATE-2026-04-24-0612.md` — post-quota-wall handoff with full branch-level detail

---
*State last rewritten: 2026-04-24 — reconciled with actual shipped state after parallel merges (6.1..11.5), Phase 12/13 partial landings, and Phase 13.1 fsync fix merge.*
