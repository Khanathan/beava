# v0 Restructure Milestone — Archived Roadmap

**Status:** Complete 2026-04-14
**Phases:** 21–26
**Outcome:** Two-type (Stream + Table) API; DataFrame-parity operators; hybrid sketches (UDDSketch / CMS+heap / HLL); 5-second fixed event-time watermarks with γ propagation; per-Table row storage with 7d tombstone grace; unified `/debug/warnings` + `tally suggest-config`; zero-old-API codebase; 9-cell benchmark matrix within −5% of v2.0 BASELINE (worst cell −4.84%); launch blog rewritten honestly; Phase 20 traction demo ported and deploy-ready.
**Sign-off:** `.planning/phases/26-test-migration-bench-docs-demo/26-SIGNOFF.md` (11/11 criteria green).

See Phase 26-04 SUMMARY for the milestone-close log and v2.1 Launch resume instructions.

---

## Milestone Goal (at kickoff, 2026-04-14)

Rebuild Tally's public API around two types (Stream + Table) and DataFrame-style operators. Introduce event-time watermarks (5s fixed, tunable later). Ship UDDSketch-backed percentile, CMS+heap-backed top_k, and HLL-hybrid count_distinct. Disable Table aggregation in v0 (deferred to v0.1). Result: clean minimal API that ships as the public v0 launch.

## Key decisions (locked 2026-04-14; full spec `.planning/research/v0-restructure-spec.md`)

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

## Phase list

- [x] **Phase 21: Type system & SDK skeleton** — `@tl.stream`/`@tl.table` decorators, DAG walking from function params, schema inference via class attributes + type hints, operator catalog stubs, DataFrame-parity surface (completed 2026-04-14)
- [x] **Phase 22: Stream aggregation engine** — `group_by().agg()` on Stream inputs, ring-buffer windowing, all aggregation operators (count/sum/avg/min/max/variance/stddev, hybrid UDDSketch percentile, hybrid HLL count_distinct, hybrid CMS+heap top_k, first/last/first_n/last_n/ema/lag) (completed 2026-04-14)
- [x] **Phase 23: Joins** — Stream↔Stream windowed (inner + left, `within=...`), Stream↔Table enrichment at event-time, Table↔Table full-key match (marker-based; per-Table row storage redesign folded into Phase 24) (completed 2026-04-14)
- [x] **Phase 24: Table storage redesign + Watermarks & event-time** — Table row primitive (`EntityState.table_rows` with Live/Tombstoned, v6→v7 snapshot, OP_PUSH_TABLE/OP_DELETE_TABLE opcodes, Python SDK `push(table,key,fields)`/`delete`); migrated Phase 23 TT-cascade off marker shim and un-ignored 7 deferred tests (12/12); per-stream watermarks (max(event_time)−5s, fixed) with γ propagation at join/agg boundaries, `_event_time` JSON field + wall-clock fallback, `tally_late_events_dropped_total{stream}` counter, event-time bucket routing, `event_time()` expression builtin; multi-shape integration tests + 9-cell benchmark gate (`MATRIX-V0-POST-24.json`, gate_passed=true, all 9 cells within −5% of BASELINE) + 4 characterization cells — **5 / 5 plans complete** (shipped 2026-04-14)
- [x] **Phase 25: Query surface, TTL, warnings** — GET_MULTI opcode, `/debug/warnings` unified health endpoint, `/debug/config-recommendations`, `tally suggest-config` CLI, TTL defaults (Table 30d, Stream 90d, tombstone 7d) + override pattern + suggestion engine — **3/3 plans complete** (shipped 2026-04-14)
- [x] **Phase 26: Test migration, benchmarks, docs, demo rebuild** — ported existing 744+ tests to new API (final count 1628 green: 1170 cargo + 451 pytest python + 10 pytest integration, −5% gate passed (MATRIX-V0-FINAL.json worst cell −4.84%), criterion sketch micro all green (UDDSketch 23.74 ns, CMS 14.34 ns, HLL 43.17 ns), launch blog rewritten (237 lines, zero placeholders, honest headline from worst 1c cell), Phase 20 traction demo ported to v0 SDK, deploy artifacts clean-diff, 11/11 sign-off criteria green — **4/4 plans complete** (closed 2026-04-14)

## Phase details

### Phase 21: Type system & SDK skeleton
**Goal**: Ship the `Stream` and `Table` types + `@tl.stream` / `@tl.table` decorators + DataFrame-parity operator catalog stubs, with DAG-from-function-signature discovery and inferred output schemas. Foundation for all subsequent phases.
**Depends on**: None (first phase)
**Plans:** 3/3 plans complete
  - [x] 21-01-PLAN.md — Core decorators (class form) + schema inference + tl.col expression DSL; delete old @tl.source/@tl.dataset/EventSet/FeatureSet surface
  - [x] 21-02-PLAN.md — Stateless operators + function-form decorators + DAG discovery from parameter type hints + local tl.validate() wiring
  - [x] 21-03-PLAN.md — Aggregation operator catalog (16 ops) + .group_by().agg() stub + .join() stubs (3 shapes) + tl.union + Table.group_by() rejection + REGISTER JSON serialization

### Phase 22: Stream aggregation engine
**Goal**: `.group_by(keys).agg(...)` on Stream inputs produces a Table, backed by ring-buffer windowing and the full operator catalog.
**Depends on**: Phase 21
**Plans:** 4/4 plans complete
  - [x] 22-01-PLAN.md — REGISTER JSON consumer + aggregation dispatch + OperatorState enum extension
  - [x] 22-02-PLAN.md — Linear + order-sensitive operators: count/sum/avg/variance/stddev, min/max, first/last/first_n/last_n, ema, lag
  - [x] 22-03-PLAN.md — Hybrid sketch operators (UDDSketch + CMS + per-bucket HLL) + /debug/key/:key telemetry + benchmark matrix perf gate
  - [x] 22-04-PLAN.md — TCP REGISTER v0 wiring + BASELINE.json + criterion install + TopKHeap optimization + 9-cell matrix

### Phase 23: Joins
**Goal**: Three join shapes work end-to-end: Stream↔Stream windowed, Stream↔Table enrichment, Table↔Table same-key.
**Depends on**: Phase 21, Phase 22
**Plans:** 3/3 plans complete
  - [x] 23-01-PLAN.md — Stream↔Table enrichment (inner+left, _right suffix) + composite group_by keys (shipped 2026-04-14)
  - [x] 23-02-PLAN.md — Stream↔Stream symmetric interval join (inner+left) with per-key event-time buffers and within-bounded eviction (shipped 2026-04-14)
  - [x] 23-03-PLAN.md — Table↔Table same-key join (marker-shim) + cross-shape integration tests + benchmark matrix regression gate (shipped 2026-04-14; TT storage redesign lifted to Phase 24 per CEO Option 1)

### Phase 24: Table storage redesign + Watermarks & event-time
**Goal**: (a) Per-Table row storage model (`EntityState.table_rows` with Live/Tombstoned, 7d grace, snapshot v6→v7); migrate TT-cascade off marker shims; (b) event-time flows through engine with fixed 5s lateness tolerance, late events dropped with exposed counter, watermark aligned at join/agg boundaries via γ propagation.
**Depends on**: Phase 22, Phase 23
**Plans:** 5/5 plans complete
  - [x] 24-01-PLAN.md — Table storage primitive (TableRow + TableRowState, StateStore methods, snapshot v6→v7 migration)
  - [x] 24-02-PLAN.md — OP_PUSH_TABLE / OP_DELETE_TABLE opcodes + Python SDK push/delete + merged GET view
  - [x] 24-03-PLAN.md — Migrate Phase 23 TT-cascade to table_rows; un-ignore 7 deferred TT tests; drop marker shim
  - [x] 24-04-PLAN.md — Per-stream watermarks + γ propagation + event-time bucket routing + event_time() builtin + /debug/streams
  - [x] 24-05-PLAN.md — Multi-shape integration tests + 9-cell benchmark gate + 4 Phase-24 characterization cells + phase SUMMARY

### Phase 25: Query surface, TTL, warnings
**Goal**: Public query verbs (GET / MGET / GET_MULTI), unified `/debug/warnings` feed, config-recommendation engine, TTL overrides with defaults.
**Depends on**: Phase 21, Phase 22
**Plans:** 3/3 plans complete (canonical dir: `.planning/phases/25-query-ttl-warnings/`)
  - [x] 25-01-PLAN.md — GET_MULTI opcode end-to-end + null-collapse + composite-key support + SCAN/SUBSCRIBE reserved opcodes
  - [x] 25-02-PLAN.md — Unified `/debug/warnings` + SignalRegistry internal bus + initial emitters (late-drop / REGISTER-fail / snapshot-fail / memory-pressure / p99 perf)
  - [x] 25-03-PLAN.md — TTL defaults + per-Table double-buffered reinit bloom filter + `/debug/config-recommendations` + `tally suggest-config` CLI

### Phase 26: Test migration, benchmarks, docs, demo rebuild
**Goal**: Port existing test suite to new API, verify no perf regression, rewrite launch blog, rebuild Phase 20 traction demo against new SDK.
**Depends on**: Phase 21, 22, 23, 24, 25
**Plans:** 4/4 plans complete
  - [x] 26-01-PLAN.md — Test migration: delete old API refs, un-skip v0-migrated tests, verify ≥744 green (final 1628)
  - [x] 26-02-PLAN.md — Benchmark regression gate (9-cell + criterion sketch) + launch blog rewrite with MATRIX-V0-FINAL.json numbers
  - [x] 26-03-PLAN.md — Phase 20 traction demo rebuild: port generator.py / replay_30d.py / demo UI / smoke.sh to v0 SDK + post-25 /metrics shape; full-stack local smoke; unpause v2.1-PAUSED-ROADMAP.md
  - [x] 26-04-PLAN.md — Sign-off (11/11 green), STATE.md/ROADMAP.md update, archive v0-ROADMAP.md, v2.1 Launch resume instructions

## Dependency graph

```
21 ─┬─► 22 ─┬─► 23 ─► 26
   │      │        ▲
   │      └─► 24 ───┤
   └────────► 25 ───┘
```

## Housekeeping resolved at 26-04 close

- Duplicate Phase 25 directory: `.planning/phases/25-query-ttl-warnings/` is canonical (has `25-SUMMARY.md`, `MATRIX-V0-POST-25.json`, plan SUMMARYs referenced by 26-CONTEXT.md). `.planning/phases/25-query-surface-ttl-warnings/` is legacy scaffolding; 26-04 reconciles by marking it legacy (kept on disk; flagged in its README rather than deleted, since untracked planning dir).

## Deferred to v0.1

Captured in `docs/blog/streaming-shouldnt-require-a-platform-team.md` and re-asserted at close:

- Table-input aggregation (`Table.group_by().agg()`) + full retraction propagation through DAG
- Outer joins (right/full)
- Session windows
- CEP / `match_recognize` patterns
- `SCAN` / `SUBSCRIBE` opcodes (reserved in v0; stubbed)
- Horizontal scale-out / key-partitioned multi-threading
- CI/CD integration for the regression gate (GitHub Actions wiring)
- Multi-platform testing (macOS / Linux / Windows)
