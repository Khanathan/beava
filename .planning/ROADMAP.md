# Tally Roadmap

## Milestones

- [x] **v1.0 -- Foundation** (Phases 1-5) -- Complete 2026-04-09 -- archived
- [x] **v1.1–v1.3 -- Event Log, Pipelines, Concurrency** (Phases 6-15) -- Complete 2026-04-12
- [x] **v2.0 -- API & Engine** (Phases 16-19) -- Complete 2026-04-13 -- `.planning/milestones/v2.0-ROADMAP.md`
- [⏸] **v2.1 -- Launch** (Phase 20) -- PAUSED pending v0 restructure -- code artifacts on disk, see `.planning/milestones/v2.1-PAUSED-ROADMAP.md`. Resume after v0 ships.
- [ ] **v0 -- Restructure** (Phases 21-26) -- Active. Type system redesign: Stream + Table model, DataFrame-parity operators, watermark-based out-of-order handling, UDDSketch/CMS+heap hybrid sketches. Blocks launch.

## Phases

### v0 Restructure (Active)

**Milestone Goal:** Rebuild Tally's public API around two types (Stream + Table) and DataFrame-style operators. Introduce event-time watermarks (5s fixed, tunable later). Ship UDDSketch-backed percentile, CMS+heap-backed top_k, and HLL-hybrid count_distinct. Disable Table aggregation in v0 (deferred to v0.1). Result: clean minimal API that ships as the public v0 launch.

Key decisions (locked via design conversation 2026-04-14, captured in `.planning/research/v0-restructure-spec.md`):
- Two types: `Stream` (append-only log) + `Table` (keyed current-state with upsert + tombstone)
- `@tl.stream` / `@tl.table` decorators; class body = source, function body = derivation
- DataFrame-parity ops (filter, map, select, drop, rename, with_columns, cast, fillna, group_by.agg, join, union)
- Stream-input aggregation only in v0; Table-input `group_by().agg()` deferred to v0.1 (eliminates Case 3 retraction complexity)
- Watermarks: 5s fixed, γ model (alignment at join/agg boundaries), per-stream
- Hybrid sketches: percentile (exact→UDDSketch@256), count_distinct (exact→HLL@1024), top_k (exact→CMS+heap@1024)
- Joins: Stream↔Stream windowed (inner+left, `within=...`), Stream↔Table enrichment, Table↔Table same-key
- Query surface: GET/MGET/GET_MULTI; null-collapse; no SCAN/SUBSCRIBE in v0
- Unified `/debug/warnings` endpoint; `/debug/config-recommendations` + `tally suggest-config` CLI
- Forward-compat: `BackfillSource` trait, reserved `_op` wire field, reserved `mode="append"|"changelog"`

- [x] **Phase 21: Type system & SDK skeleton** — `@tl.stream`/`@tl.table` decorators, DAG walking from function params, schema inference via class attributes + type hints, operator catalog stubs, DataFrame-parity surface (completed 2026-04-14)
- [x] **Phase 22: Stream aggregation engine** — `group_by().agg()` on Stream inputs, ring-buffer windowing, all aggregation operators (count/sum/avg/min/max/variance/stddev, hybrid UDDSketch percentile, hybrid HLL count_distinct, hybrid CMS+heap top_k, first/last/first_n/last_n/ema/lag) (completed 2026-04-14)
- [x] **Phase 23: Joins** — Stream↔Stream windowed (inner + left, `within=...`), Stream↔Table enrichment at event-time, Table↔Table full-key match (marker-based; per-Table row storage redesign folded into Phase 24) (completed 2026-04-14)
- [ ] **Phase 24: Table storage redesign + Watermarks & event-time** — Table row primitive (`EntityState.table_rows` with Live/Tombstoned, v6→v7 snapshot, OP_PUSH_TABLE/OP_DELETE_TABLE opcodes, Python SDK `push(table,key,fields)`/`delete`); migrate Phase 23 TT-cascade off marker shim and un-ignore 7 deferred tests; per-stream watermarks (max(event_time)−5s, fixed) with γ propagation at join/agg boundaries, `_event_time` JSON field + wall-clock fallback, `tally_late_events_dropped_total{stream}` counter, event-time bucket routing, `event_time()` expression builtin — **1 / 5 plans complete** (24-01 storage primitive shipped 2026-04-14)
- [ ] **Phase 25: Query surface, TTL, warnings** — GET_MULTI opcode, `/debug/warnings` unified health endpoint, `/debug/config-recommendations`, `tally suggest-config` CLI, TTL defaults (Table 30d, Stream 90d, tombstone 7d) + override pattern + suggestion engine
- [ ] **Phase 26: Test migration, benchmarks, docs, demo rebuild** — port existing 744+ tests to new API, benchmark regression gate (within −5% of v2.0 baseline 1.1M eps), rewrite `docs/blog/streaming-shouldnt-require-a-platform-team.md`, rebuild Phase 20 traction demo against new API, sign-off

## Phase Details

### Phase 21: Type system & SDK skeleton
**Goal**: Ship the `Stream` and `Table` types + `@tl.stream` / `@tl.table` decorators + DataFrame-parity operator catalog stubs, with DAG-from-function-signature discovery and inferred output schemas. Foundation for all subsequent phases.
**Depends on**: None (first phase)
**Requirements**: TBD (captured during `/gsd-plan-phase 21`)
**Success Criteria** (what must be TRUE):
  1. User can declare a Stream source via `@tl.stream class X` with schema from class attributes + type hints; validate() succeeds
  2. User can declare a Table source via `@tl.table(key=[...]) class X` with composite key support; validate() succeeds
  3. User can declare a Stream derivation via `@tl.stream def X(...) -> Stream:` with upstream dependencies auto-discovered from function parameter types
  4. User can declare a Table derivation via `@tl.table(key=[...]) def X(...) -> Table:`
  5. Operator catalog stubs exist for filter, map, select, drop, rename, with_columns, cast, fillna — called on a Stream or Table returns same type; tests verify schema inference
  6. Pipeline DAG builds from function signatures; circular dependencies rejected at registration with a named-cycle error
  7. `.describe()` on any Stream/Table returns inferred schema; mismatch errors name the offending field + closest lexical match
  8. No Rust engine changes required — SDK scaffolding only, backed by placeholder engine ops that raise `NotImplemented` where needed
**Plans:** 3/3 plans complete
  - [x] 21-01-PLAN.md — Core decorators (class form) + schema inference + tl.col expression DSL; delete old @tl.source/@tl.dataset/EventSet/FeatureSet surface
  - [x] 21-02-PLAN.md — Stateless operators (filter/map/select/drop/rename/with_columns/cast/fillna) + function-form decorators + DAG discovery from parameter type hints + local tl.validate() wiring
  - [x] 21-03-PLAN.md — Aggregation operator catalog (16 ops) + .group_by().agg() stub + .join() stubs (3 shapes) + tl.union + Table.group_by() rejection + REGISTER JSON serialization

### Phase 22: Stream aggregation engine
**Goal**: `.group_by(keys).agg(...)` on Stream inputs produces a Table, backed by ring-buffer windowing and the full operator catalog (linear ops + hybrid sketches for percentile/count_distinct/top_k + first/last/ema/lag).
**Depends on**: Phase 21
**Requirements**: TBD
**Success Criteria**:
  1. User can call `stream.group_by(keys).agg(feature=tl.op(...))` returning a Table with one row per unique (keys) tuple
  2. All linear operators (count, sum, avg, variance, stddev) emit correct values under windowed semantics
  3. `tl.percentile` uses hybrid exact → UDDSketch at threshold=256; transition tested; α drift exposed in `/debug/key/:key`
  4. `tl.count_distinct` uses hybrid exact HashSet → HLL (precision=14) at threshold=1024; transition tested
  5. `tl.top_k` uses hybrid exact HashMap → CMS+heap at threshold=1024; transition tested
  6. `tl.first`, `tl.last`, `tl.first_n`, `tl.last_n` work by event-time
  7. `tl.ema`, `tl.lag` work on Stream inputs; registration rejects them on any Table-tainted input
  8. Ring-buffer windowing supports default windows (1m/5m/1h/24h) with configurable bucket granularity
**Plans:** 4/4 plans complete
  - [x] 22-01-PLAN.md — REGISTER JSON consumer + aggregation dispatch + OperatorState enum extension (burns v2.0 REGISTER path)
  - [x] 22-02-PLAN.md — Linear + order-sensitive operators: count/sum/avg/variance/stddev, min/max (bucket-granular), first/last/first_n/last_n (event-time), ema, lag
  - [x] 22-03-PLAN.md — Hybrid sketch operators (UDDSketch percentile + CMS top_k + per-bucket HLL count_distinct) + /debug/key/:key telemetry + benchmark matrix perf gate
  - [x] 22-04-PLAN.md — TCP REGISTER v0 wiring (v0→v2 translator) + BASELINE.json + criterion install + TopKHeap optimization + 9-cell matrix (all cells ≤5% baseline)

### Phase 23: Joins
**Goal**: Three join shapes work end-to-end: Stream↔Stream windowed, Stream↔Table enrichment, Table↔Table same-key.
**Depends on**: Phase 21, Phase 22
**Requirements**: TBD
**Success Criteria**:
  1. `stream_a.join(stream_b, on=[...], within="30m", type="inner"|"left")` emits joined events
  2. Stream↔Table enrichment: `stream.join(table, on=[...])` point-in-time joins event with Table's current row for that key
  3. Table↔Table same-key: `table_a.join(table_b, on=[...])` returns Table with union of fields, polars-style `_right` suffix on collision
  4. Schema inference correctly unions fields across join inputs
  5. Outer joins rejected at registration with "deferred to v0.1" error
  6. Partial-key joins rejected with "full-key required in v0" error
  7. Tests cover: identical-key joins, composite-key joins, late-event retractions through Stream↔Stream joins
**Plans:** 2/3 plans executed
  - [x] 23-01-PLAN.md — Stream↔Table enrichment (inner+left, _right suffix) + composite group_by keys (deferred from 22-04) — shipped 2026-04-14
  - [x] 23-02-PLAN.md — Stream↔Stream symmetric interval join (inner+left) with per-key event-time buffers and within-bounded eviction — shipped 2026-04-14
  - [ ] 23-03-PLAN.md — Table↔Table same-key join + tombstone propagation + cross-shape integration tests + 11-cell benchmark matrix regression gate

### Phase 24: Table storage redesign + Watermarks & event-time
**Goal**: (a) Ship the proper per-Table row storage model (`EntityState.table_rows` with Live/Tombstoned, 7d grace, snapshot v6→v7) that Phase 23 deferred, migrate the TT-cascade off marker shims, un-ignore the 7 deferred TT tests; and (b) event-time flows through the engine with a fixed 5-second lateness tolerance, late events dropped with an exposed counter, watermark aligned at join/agg boundaries via γ propagation.
**Depends on**: Phase 22 (for aggregation boundary alignment), Phase 23 (for join boundary alignment + storage carry-forward)
**Requirements**: TBD
**Success Criteria**:
  1. `EntityState.table_rows: AHashMap<String, TableRow>` exists with `TableRowState::Live | Tombstoned { since }`; `upsert_table_row` / `tombstone_table_row` / `get_table_row` / `gc_tombstones(now)` on StateStore; 7d tombstone grace
  2. Snapshot codec v7 round-trips Table rows including Tombstoned variant; v6 snapshots load with empty `table_rows` (backward-compat)
  3. `OP_PUSH_TABLE` (0x0B) and `OP_DELETE_TABLE` (0x0C) opcodes wired in TCP + Python SDK (`app.push(table,key,fields)`, `app.delete(table,key)`); `app.get(key)` returns merged view of live table_rows + static_features + stream ops
  4. Phase 23's TT-cascade reworked to consume `table_rows[A]` / `table_rows[B]` (not static_features markers); all 7 previously-ignored tests in `tests/test_join_table_table.rs` pass (total 12/12)
  5. Events carry an `_event_time` JSON field; absent → falls back to wall-clock arrival time
  6. Each Stream source tracks its watermark = max(event_time seen) − 5s; exposed in `/debug/key/:stream` and `/debug/streams/:name`
  7. Events with event_time < watermark are dropped; `tally_late_events_dropped_total{stream}` counter increments
  8. Stateless ops (filter/map/select/drop/rename/with_columns/cast/fillna) pass watermark through unchanged
  9. Joins take `min(wm_left, wm_right)` as output watermark; aggregations attach watermark to output Table
  10. Window buckets routed by event_time — out-of-order within 5s lands in correct historical bucket; past 5s dropped
  11. `now()` returns wall-clock; `event_time()` builtin returns current event's event-time; callable in derive + filter expressions
  12. Multi-shape integration DAG (source Stream + source Table + Enrich + Agg + TT-join) behaves correctly under in-order, out-of-order, late-drop, and tombstone-cascade scenarios
  13. 9-cell benchmark matrix passes within ±5% of v0 BASELINE.json; 4 new characterization cells recorded
**Plans:** 3/5 plans executed
  - [x] 24-01-PLAN.md — Table storage primitive (TableRow + TableRowState, StateStore methods, snapshot v6→v7 migration)
  - [x] 24-02-PLAN.md — OP_PUSH_TABLE / OP_DELETE_TABLE opcodes + Python SDK push/delete + merged GET view
  - [x] 24-03-PLAN.md — Migrate Phase 23 TT-cascade to table_rows; un-ignore 7 deferred TT tests; drop marker shim
  - [ ] 24-04-PLAN.md — Per-stream watermarks + γ propagation + event-time bucket routing + event_time() builtin + /debug/streams
  - [ ] 24-05-PLAN.md — Multi-shape integration tests + 9-cell benchmark gate + 4 Phase-24 characterization cells + phase SUMMARY

### Phase 25: Query surface, TTL, warnings
**Goal**: Ship the public query verbs (GET / MGET / GET_MULTI), the unified `/debug/warnings` feed, config-recommendation engine, and TTL overrides with defaults.
**Depends on**: Phase 21, Phase 22 (for live metrics to feed warnings)
**Requirements**: TBD
**Success Criteria**:
  1. `GET(table_name, key)` returns row or null for composite/simple keys
  2. `MGET(table_name, [keys])` returns `{key → row or null}` in a single round-trip
  3. `GET_MULTI([table_names], key)` returns `{table → row or null}` for feature-vector assembly
  4. Null-collapse on not-found (never-seen / tombstoned / pending all return null)
  5. `@tl.table(ttl="30d")` and `@tl.stream(history_ttl="90d")` defaults applied; user-override on decorator works
  6. `/debug/warnings` endpoint returns severity-sorted JSON feed covering config/data_quality/operational/safety/performance categories
  7. `/debug/config-recommendations` endpoint suggests TTL/history_ttl adjustments based on observed eviction/compaction/backfill-miss signals
  8. `tally suggest-config` CLI prints copy-pasteable decorator overrides
  9. `SCAN` and `SUBSCRIBE` opcodes are reserved but return "not implemented in v0" if called
**Plans:** TBD

### Phase 26: Test migration, benchmarks, docs, demo rebuild
**Goal**: Port the existing test suite to the new API, verify no performance regression, rewrite the launch blog, and rebuild Phase 20 traction demo against the new SDK.
**Depends on**: Phase 21, 22, 23, 24, 25
**Requirements**: TBD
**Success Criteria**:
  1. All pre-v0 tests (≥ 744 baseline) ported to new API; `cargo test && pytest` pass
  2. No references to old API surface (`@tl.source`, `@tl.dataset`, `EventSet`, `FeatureSet`) remain in SDK or tests
  3. Benchmark matrix: small/medium/large × 1c/4c/8c passes within −5% of v2.0 baseline (1.1M eps)
  4. `docs/blog/streaming-shouldnt-require-a-platform-team.md` rewritten to describe the new Stream/Table/retraction story honestly (no mentions of deferred features as shipped)
  5. Phase 20 traction demo replay CLI, demo.html, and 6-invariant smoke script ported to new API; pass locally
  6. `docs/` site updated with new SDK reference, migration note (internal-only, since pre-launch)
  7. Phase 20 (v2.1) artifacts ready to deploy via already-written Hetzner scripts — no re-provision needed, just recompile binary + redeploy
**Plans:** TBD

## Progress

**Execution Order:**
Phase 21 blocks all others. 22 + 23 parallelize after 21. 24 can start mid-22. 25 needs 21 + 22. 26 is last.

Dependency graph:
```
21 ─┬─► 22 ─┬─► 23 ─► 26
   │      │        ▲
   │      └─► 24 ───┤
   └────────► 25 ───┘
```

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 21. Type system & SDK skeleton | v0 | 3/3 | Complete   | 2026-04-14 |
| 22. Stream aggregation engine | v0 | 4/4 | Complete   | 2026-04-14 |
| 23. Joins | v0 | 2/3 | In Progress|  |
| 24. Table storage + Watermarks & event-time | v0 | 2/5 | In Progress|  |
| 25. Query surface, TTL, warnings | v0 | 0/? | Not planned | - |
| 26. Test migration, bench, docs, demo | v0 | 0/? | Not planned | - |
