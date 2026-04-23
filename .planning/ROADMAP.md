# Beava v2 — v0 OSS Launch Roadmap

**Milestone:** v0 (first public OSS cut on `beava.dev`)
**Granularity:** fine (13 phases; 3–8 plans per phase)
**Mode:** yolo (auto-approved; written to hold up unrevised)
**Parallelization:** enabled where indicated
**Created:** 2026-04-22
**Revised:** 2026-04-22 (re-planned to adopt v1 Python SDK API shape)
**Source:** `.planning/PROJECT.md`, `.planning/REQUIREMENTS.md`

## North Star

Feature authoring as composable Python code that ships to production unchanged. v0 ships the v1 Python SDK shape (`@bv.event` / `@bv.table` / `bv.col` / `.filter / .select / ... / .group_by().agg()` / `.join` / `bv.union` / `app.register` / `app.push` / `app.get` / `bv.fork`) on a new single-thread in-memory HTTP runtime with a 40+ operator catalogue.

## Architecture (locked, do not revisit in phases)

- **Runtime:** Single Rust process, single OS thread for the apply loop (plus auxiliary threads for WAL fsync, HTTP accept, snapshot writer)
- **State:** In-memory only (no RocksDB, no fjall, no tiered storage)
- **Durability:** WAL file per instance with 1–5ms group-commit fsync; periodic snapshots (default 30s) of in-memory state
- **Recovery:** Load latest snapshot + replay WAL from snapshot LSN
- **Wire:** HTTP/1.1 + JSON only; endpoints `POST /register`, `POST /push/{event}`, `POST /push-sync/{event}`, `POST /push-batch/{event}`, `POST /push-table/{table}`, `POST /delete-table/{table}`, `POST /get`, `GET /get/{feature}/{key}`, `POST /set`, `POST /mset`, `/metrics`, `/health`, `/ready`
- **Authoring UX:** Python SDK with v1-shaped decorator DSL, expression DSL, stateless ops, aggregation framework, joins, unions
- **Registration:** Additive-only with monotonic `registry_version` bumps; removals/changes return 409 with structured diff
- **Operator catalogue:** 40+ built-in aggregation operators spanning core, sketch, point, decay, velocity, recency, bounded-buffer, and geo families

## Phase Overview

| # | Phase | Goal | Reqs | Success criteria |
|---|-------|------|------|------------------|
| 1 | Foundation | Rust workspace, axum HTTP scaffolding, config, logging, test harness | 0 (infrastructure) | 4 ✅ **COMPLETE** |
| 2 | Sources + registry + version bumps | `/register` accepts DAG of event/table/derivation nodes; additive-only; monotonic version; registry persists in-memory | 12 | 5 ✅ **COMPLETE** |
| 2.5 | TCP wire listener + framing + full opcode table | Custom-framed TCP listener alongside HTTP; full v0 opcode table designed; `register` + `ping` handlers wired; rest return `op_not_implemented` placeholder | ~8 | 8 ✅ **COMPLETE** |
| 3 | Python SDK skeleton + decorators + expression DSL | `@bv.event`, `@bv.table`, `bv.col`, `bv.App(url)` (HTTP + TCP), register + validate, REGISTER JSON compiler | 20 | 7 |
| 4 | Stateless ops + expression evaluator (server-side) | 7/7 | Complete   | 2026-04-23 |
| 5 | Aggregation framework + core operators (8) | `group_by().agg()` DAG lands server-side; windowed bucket infra; core aggregations: count, sum, avg, min, max, variance, stddev, ratio | 15 | 6 |
| 6 | WAL + idempotency | Every push write-through fsynced before ACK; stream-level idempotency keys cached with TTL | 5 | 4 |
| 7 | Snapshot + recovery | Periodic full-state snapshot; restart replays snapshot + WAL; schema evolution survives restart | 5 | 4 |
| 8 | Point / ordinal / recency operators | first, last, first_n, last_n, lag, first_seen, last_seen, age, has_seen, time_since, time_since_last_n, streak, max_streak, negative_streak, first_seen_in_window | 15 | 4 |
| 9 | Decay + velocity operators | ewma, ewvar, ew_zscore, decayed_sum, decayed_count, twa, rate_of_change, inter_arrival_stats, burst_count, delta_from_prev, trend, trend_residual, outlier_count, value_change_count, z_score | 16 | 4 |
| 10 | Sketch operators | count_distinct (HLL), percentile (DDSketch), top_k (SpaceSaving), bloom_member, entropy | 5 | 4 |
| 11 | Bounded-buffer + geo operators | histogram, hour_of_day/dow_hour histograms, seasonal_deviation, event_type_mix, most_recent_n, reservoir_sample, geo_velocity, geo_distance, geo_spread, unique_cells, geo_entropy, distance_from_home | 13 | 4 |
| 11.5 | Temporal tables + retraction primitive | MVCC storage for `@bv.table(temporal=True, retention=...)`; `app.retract(event_id)` scoped to table upserts/deletes; wires `as_of=...` kwarg that Phase 12 joins consume; stream retraction deferred to v1 but event-IDs land now | ~10 | 6 |
| 12 | Joins + unions + push/get API completion | Event↔event windowed join, event↔table enrichment (incl. event-time PIT against temporal tables), table↔table join, `bv.union`; `push_sync` + `push_many` + `push_table` + `delete_table` + `set` + `mset` + `mget` + `get_multi` wired end-to-end | 13 | 5 |
| 13 | Observability + performance + docs + packaging + `bv.fork` + playground | `/metrics`, structured logs, perf gates on THREE pipelines (simple fraud, complex fraud, recommendations) ≥3M EPS, <10ms P99 batch get, SDK polish, docs, hosted interactive tutorial at playground.beava.dev, PyPI, GitHub Releases, Docker, `beava fork` subcommand | ~18 | 7 |

**Total:** 15 phases (Phase 2.5 inserted 2026-04-23 for dual HTTP+TCP wire; Phase 11.5 inserted 2026-04-23 for temporal tables + retraction primitive required by PIT stream↔table joins), ~163 requirements mapped (actual count confirmed after plan-time verification), ~79 success criteria.

**Phase 1 status:** ✅ **COMPLETE** on commits `b100e51`..`c21b6b7`. Cargo workspace, axum HTTP server, `/health` + `/ready` stubs, graceful shutdown, integration TestServer harness — all gates green. See `.planning/phases/01-foundation/01-SUMMARY.md`, `.planning/phases/01-foundation/01-VERIFICATION.md`.

## Parallelization

- **Phases 1 → 2 → 3 → 4 → 5 → 6 → 7** are strictly sequential — each depends on the one before. Phase 5 is where the apply loop first runs real aggregations; Phases 6–7 harden durability around it.
- **Phases 8 / 9 / 10 / 11** can run in parallel after Phase 7 — each operator family attaches to the existing apply loop + registry + window infra, touching independent operator modules. Recommended: sequence 8 → 9 → 10 → 11 unless explicitly running parallel worktrees.
- **Phase 11.5** (temporal tables + retraction) depends on 7 (needs WAL + snapshot); can run parallel with 8–11 since it touches its own table-storage module. MUST ship before Phase 12 because joins consume the `as_of=...` kwarg.
- **Phase 12** (joins/unions + push/get completion) depends on 7 AND 11.5; can overlap with 8–11 since joins live in their own module.
- **Phase 13** waits on everything for perf benchmarks + docs sign-off.

## Dependency graph

```
  Phase 1 (Foundation) ✅
       │
       ▼
  Phase 2 (Sources + registry + version bumps)
       │
       ▼
  Phase 2.5 (TCP wire listener + framing + full opcode table)
       │
       ▼
  Phase 3 (Python SDK + decorators + expression DSL, HTTP + TCP)
       │
       ▼
  Phase 4 (Stateless ops + expression evaluator server-side)
       │
       ▼
  Phase 5 (Aggregation framework + 8 core operators)
       │
       ▼
  Phase 6 (WAL + idempotency)
       │
       ▼
  Phase 7 (Snapshot + recovery + schema evolution)
       │
       ├────────────┬────────────┬────────────┬────────────┐
       ▼            ▼            ▼            ▼            ▼
  Phase 8       Phase 9      Phase 10     Phase 11     Phase 12
  (recency/     (decay/      (sketches)   (buffer+geo) (joins +
  point ops)    velocity)                              unions + API
                                                        completion)
       └────────────┴────────────┴────────────┴────────────┘
                                   │
                                   ▼
                     Phase 13 (obs + perf + docs + pkg + fork — ship)
```

## Phase details

### Phase 1: Foundation ✅ COMPLETE

**Goal:** A `beava` binary that boots from config, exposes an HTTP server with `/health` and `/ready` stubs, writes structured JSON logs, and runs under an integration test harness.

**Status:** Shipped. See `.planning/phases/01-foundation/01-SUMMARY.md` + `01-VERIFICATION.md`.

**Depends on:** Nothing.

**Requirements:** none (infrastructure phase).

**Success criteria:** (all ✅)
1. `cargo build --release` produces stripped binary; `./beava --config ./beava.yaml` starts HTTP listener, logs JSON
2. `curl localhost:$PORT/health` → 200; `/ready` returns 503 until flag flips
3. axum wired; graceful shutdown on SIGTERM
4. Integration-test harness (`TestServer::spawn()`) exists and tested

### Phase 2: Sources + registry + version bumps

**Goal:** `POST /register` accepts a JSON DAG of events, tables, and derivations; validates; persists in-memory; assigns monotonic `registry_version`. Additive-only — removals/changes return 409 with structured diff. No aggregations execute yet.

**Depends on:** Phase 1.

**Requirements:** SRV-API-01, SRV-API-02, SRV-API-11, SRV-API-12, SRV-REG-01, SRV-REG-02, SRV-REG-03, SRV-REG-05, SRV-REG-06, SDK-DEC-06, SDK-DEC-08, SDK-DEC-09 — 12 REQ-IDs.

**Success criteria:**
1. `POST /register` with a valid JSON DAG (1+ events, 0+ tables) returns 200 with `registry_version: 1` and `registered_descriptors` listing
2. Re-posting an identical DAG is a no-op; version unchanged
3. Posting an additive DAG (new event or table) returns 200 and bumps version
4. Posting a DAG that removes or changes an existing descriptor returns 409 with `{error: {code: "registration_conflict", diff: {added, removed, changed}}}` naming each change
5. Malformed payload (missing required fields, unknown node type) returns 400 with `{error: {code, path, reason}}` pointing to the offending path

### Phase 2.5: TCP wire listener + framing + full opcode table

**Goal:** Ship the server-side TCP fast-path alongside the existing HTTP listener. Custom-framed binary wire `[u32 length][u16 op][u32 request_id][payload bytes]` with the full v0 opcode table designed up front; `register` + `ping` handlers wired; every other opcode (push/push_sync/push_many/get/mget/set/mset) reserved and returns a structured `op_not_implemented` error so later phases just fill in handlers without touching the codec.

**Depends on:** Phase 2.

**Requirements:** SRV-API-NEW (TCP listener), SRV-WIRE-01 through SRV-WIRE-06 (framing), SRV-WIRE-REG-01 (register over TCP). New REQ-IDs added to REQUIREMENTS.md at plan-phase time.

**Success criteria:**
1. Server binds two listeners when configured: HTTP on `http_port`, TCP on `tcp_port` (both configurable via YAML/env); binary starts with both bound by default
2. Frame codec round-trips via proptest: arbitrary `(op, request_id, payload)` → bytes → parsed frame byte-identical
3. `op=ping` returns a pong frame with server's `registry_version` + build-version string
4. `op=register` over TCP delivers the same JSON DAG semantics as `POST /register` (200/400/409 equivalents returned as response frames with matching error shapes) — shares validation + diff engine with HTTP path (no duplicated logic)
5. Unknown / unimplemented opcode returns a `op_not_implemented` response frame; server does NOT close the connection (clients can retry other ops)
6. Connection lifecycle: client opens TCP, issues N requests on one connection (request_id disambiguates responses), closes cleanly; server-side graceful shutdown drains in-flight requests
7. Max frame size bounded (default 4 MiB, configurable); oversized frames produce `frame_too_large` error and connection reset
8. Integration smoke: `phase2_5_smoke.rs` — spin server, TCP-client sends ping + register + unknown-op; assert expected responses

### Phase 3: Python SDK skeleton + decorators + expression DSL

**Goal:** Ship the user-facing Python SDK that compiles decorators + expression DSL into the REGISTER JSON the server accepts. SDK supports both transports via URL scheme (`http://` for HTTP/JSON, `tcp://` for framed TCP) — Phase 3 exercises both against the Phase 2.5 server. Dogfood the DSL from Phase 3 onwards; curl remains the language-agnostic escape hatch.

**Depends on:** Phase 2.5.

**Requirements:** SDK-DEC-01 through SDK-DEC-09, SDK-COL-01 through SDK-COL-06, SDK-COL-08, SDK-APP-01, SDK-APP-02, SDK-APP-03, SDK-APP-15, SDK-WIRE-01 (HTTP transport), SDK-WIRE-02 (TCP transport), SDK-WIRE-03 (URL-scheme dispatch) — 20 REQ-IDs. SDK-COL-07 (schema-reference resolution) moved to Phase 4 because it requires the server-side expression evaluator.

**Success criteria:**
1. `@bv.event` class form extracts schema and registers event descriptor; function form resolves upstreams
2. `@bv.table(key=..., ttl=...)` class + function forms work; key validation at decoration time
3. `bv.col("x") > 100` expression produces expected `to_expr_string()` canonical form
4. `app.register(*descriptors)` topologically sorts the DAG, detects cycles, validates schemas, dispatches to HTTP or TCP based on URL scheme, receives `registry_version`
5. `app.validate(*descriptors)` runs zero-network-IO validation returning `list[ValidationError]`
6. End-to-end smoke: spawn TestServer (with both ports), register 2 events + 1 table from Python twice — once via `bv.App('http://...')` and once via `bv.App('tcp://...')` — identical registry state verifiable via `curl /registry`
7. SDK TCP client round-trips `ping` successfully; connection reuse across multiple `register`/`validate` calls on one App instance

### Phase 4: Stateless ops + expression evaluator (server-side)

**Goal:** Server-side expression parser + evaluator for the `bv.col(...)` canonical form. Stateless per-event op chain (`filter`/`select`/`drop`/`rename`/`with_columns`/`map`/`cast`/`fillna`) executes before aggregations see events. SDK clients register chained ops in their DAG nodes.

**Depends on:** Phase 3.

**Requirements:** SDK-OPS-01 through SDK-OPS-10, SDK-COL-07 (schema-reference resolution, moved from Phase 3 because the expression evaluator lands here), SRV-APPLY-06, SRV-APPLY-07 — 13 REQ-IDs.

**Success criteria:**
1. `Event.filter(bv.col("amount") > 100)` registered via SDK; server rejects events failing the predicate
2. `Event.with_columns(is_big=bv.col("amount") > 500)` adds a derived column visible to downstream nodes
3. Chained ops (`filter → select → with_columns → cast`) compose correctly; schema propagates through every step
4. Proptest-covered: random predicate + random event → truth-table equivalence between client-side eval and server-side eval
5. Malformed predicate in registration returns 400 with path pointing to the offending expression

**Plans:** 7/7 plans complete
- [x] 04-01-PLAN.md — Row + Value + SQL three-valued null logic (beava-core foundation)
- [x] 04-02-PLAN.md — Recursive-descent expression parser with Span tracking + column-pointing errors
- [x] 04-03-PLAN.md — Expression evaluator + cast/isnull builtins + determinism proptest
- [x] 04-04-PLAN.md — Op-chain executor + register-time schema propagator (8 ops + SDK-OPS-01..10 mechanics)
- [x] 04-05-PLAN.md — Register integration: HTTP/TCP parity for invalid_expression errors; OpChain caching
- [x] 04-06-PLAN.md — Phase 4 Rust acceptance: /dev/apply_ops endpoint (gated) + Rust SC1/SC2/SC3/SC5 smokes over HTTP + TCP (completed 2026-04-23)
- [x] 04-07-PLAN.md — Phase 4 Python acceptance: 8 SDK op methods + Python reference evaluator + SC1/SC2/SC3/SC5 Python smokes + SC4 hypothesis proptest (256 cases, client/server eval equivalence)

### Phase 5: Aggregation framework + core operators

**Goal:** `group_by(keys).agg(name=bv.<op>(...), ...)` produces a Table in the DAG; server's apply loop updates per-entity aggregation state for every registered feature touching the event's source. Core 8 operators land (count, sum, avg, min, max, variance, stddev, ratio). `Windowed<Op>` bucket infra.

**Depends on:** Phase 4.

**Requirements:** SDK-AGG-01 through SDK-AGG-06, AGG-CORE-01 through AGG-CORE-09 — 15 REQ-IDs.

**Success criteria:**
1. `Event.group_by("user_id").agg(cnt=bv.count(window="5m"))` registered via SDK produces a Table with `cnt` feature
2. Push to the event updates the aggregation; `/get` returns current value
3. All 8 core operators pass table-driven correctness tests
4. Uniform event-time bucketing cap 64 proven replay-deterministic: replaying the same event stream produces byte-identical state
5. Lifetime/windowless mode works when `window` omitted on compatible operators (ratio, count)
6. Validation: unknown field in `op.field` rejected at registration

**Plans:** 8 plans
- [x] 05-01-PLAN.md — AggOp enum + per-op state structs (Count/Sum/Avg/Min/Max/Variance/StdDev/Ratio) + Windowed<Op> 64-bucket tumbling (AGG-CORE-01..09, SDK-AGG-03)
- [x] 05-02-PLAN.md — `where=` predicate threading through apply path (SDK-AGG-04)
- [x] 05-03-PLAN.md — AggregationDescriptor + propagate_aggregation_schema (SDK-AGG-01, SDK-AGG-03)
- [x] 05-04-PLAN.md — Register-time Rule 11 + compiled_aggregations cache + HTTP/TCP wire errors (SDK-AGG-05, SDK-AGG-06)
- [ ] 05-05-PLAN.md — Apply loop hook + per-entity AggStateTable + /dev/apply_events (SDK-AGG-02, AGG-CORE-09)
- [ ] 05-06-PLAN.md — Feature query endpoints GET /get/{feature}/{key} + POST /get + cross-agg collision rule (SDK-AGG-02)
- [ ] 05-07-PLAN.md — Python SDK group_by + 8 bv.<op> helpers + REGISTER JSON serialization (SDK-AGG-01..06)
- [ ] 05-08-PLAN.md — Phase 5 Rust + Python acceptance smokes (SC1..SC6 coverage)

### Phase 6: WAL + idempotency

**Goal:** `/push` ACK returns only after event's LSN has been fsynced. Stream-level `dedupe_key` + window enforced: duplicate requests return the cached response byte-identical.

**Depends on:** Phase 5.

**Requirements:** SRV-DUR-01, SRV-DUR-02, SRV-DUR-03, SRV-DUR-04, SRV-DUR-05 — 5 REQ-IDs.

**Success criteria:**
1. Push event, kill process before fsync, restart → event NOT present. Push event, wait for ACK, kill → event IS present.
2. Duplicate push with same dedupe key within window returns byte-identical response; state unchanged between first and duplicate
3. Group-commit fsync adds P50 < 2ms to push-ACK latency at default config
4. WAL rotation: segments ≤ snapshot-covered LSN truncated; disk usage bounded

### Phase 7: Snapshot + recovery + schema evolution

**Goal:** Periodic snapshot serializes in-memory state + registry; restart loads snapshot + replays WAL-past-snapshot-LSN and resumes. Schema evolution preserved across restart.

**Depends on:** Phase 6.

**Requirements:** SRV-REG-04, SRV-RECOV-01, SRV-RECOV-02, SRV-RECOV-03, SRV-RECOV-04, SRV-RECOV-05 — 6 REQ-IDs.

**Success criteria:**
1. Run 1M events through the server, snapshot fires, restart → all features replayable; values match pre-restart
2. Add a new feature (additive registration + version bump), snapshot, restart → new feature still present
3. RTO: 10GB state snapshot + 1GB WAL tail → server online within 30s on NVMe
4. Corrupt snapshot (flipped byte) detected + logged; operator can fall back to previous

### Phase 8: Point / ordinal / recency operators

**Goal:** The point-shaped operator family lands — values, sequences, streaks, recency markers.

**Depends on:** Phase 7. **Parallelizable with Phases 9, 10, 11, 12.**

**Requirements:** AGG-POINT-01 through AGG-POINT-11, AGG-RECENCY-01 through AGG-RECENCY-04 — 15 REQ-IDs.

**Success criteria:**
1. All 15 operators pass table-driven correctness tests with deterministic replay
2. Operators round-trip through WAL + snapshot + recovery
3. Docs entry per operator in `docs/operators.md`
4. SDK descriptor constructors match v1 API (same parameter names)

### Phase 9: Decay + velocity operators

**Goal:** Exponentially-decayed and velocity-shaped operators land.

**Depends on:** Phase 7. **Parallelizable with 8, 10, 11, 12.**

**Requirements:** AGG-DECAY-01 through AGG-DECAY-07, AGG-VEL-01 through AGG-VEL-08, AGG-Z-01 — 16 REQ-IDs.

**Success criteria:**
1. All 15 operators pass correctness + determinism tests
2. `bv.ema()` alias resolves to `bv.ewma()` in the SDK
3. Half-life parameter validation at decoration time (duration string format)
4. Operators replay byte-identically after restart

### Phase 10: Sketch operators

**Goal:** Approximate-algorithm operators land with documented error bounds.

**Depends on:** Phase 7. **Parallelizable with 8, 9, 11, 12.**

**Requirements:** AGG-SKETCH-01 through AGG-SKETCH-05 — 5 REQ-IDs.

**Success criteria:**
1. `count_distinct`, `percentile`, `top_k` pass error-bound checks (within documented tolerances on reference datasets)
2. Sketch serialization round-trips through snapshot + WAL replay; deterministic under sketched inputs
3. `bloom_member` and `entropy` pass table-driven tests
4. Memory bounded per-entity by operator configuration

### Phase 11: Bounded-buffer + geo operators

**Goal:** Histograms, per-user baselines, and geo-shaped operators land.

**Depends on:** Phase 7. **Parallelizable with 8, 9, 10, 12.**

**Requirements:** AGG-BUFFER-01 through AGG-BUFFER-07, AGG-GEO-01 through AGG-GEO-06 — 13 REQ-IDs.

**Success criteria:**
1. All 13 operators pass correctness tests
2. Geo math verified against a reference implementation (`haversine` crate)
3. Structured outputs (histograms, reservoir samples) round-trip through `GET /get/{feature}/{key}` with `{value, meta?}` shape
4. Replay determinism preserved

### Phase 11.5: Temporal tables + retraction primitive

**Goal:** Server-side MVCC storage for `@bv.table(temporal=True, retention=...)` tables, plus an `app.retract(event_id)` primitive scoped to tables in v0. Wires the `as_of=...` kwarg the SDK already ships so Phase 12 joins can resolve event-time PIT lookups. Stream retraction is intentionally deferred to v1 — but the WAL + aggregation format land with stable event-IDs so stream retraction is additive later, not a breaking change.

**Depends on:** Phase 7 (needs WAL + snapshot; temporal versions ride on LSN). **Must ship before Phase 12** (joins consume `as_of=...`).

**Requirements:** SRV-TBL-TEMPORAL-01 through SRV-TBL-TEMPORAL-06 (MVCC storage, retention enforcement, version-at-lsn lookup, tombstone semantics, snapshot of historical versions, memory budget cap), SRV-RETRACT-01 through SRV-RETRACT-03 (retract API wire + idempotency + error shape for non-temporal targets), SDK-TBL-TEMPORAL-01 (already landed — decorator flag), SDK-APP-RETRACT-01 (Python client `app.retract(event_id)`). New REQ-IDs to be defined at plan-time.

**Success criteria:**
1. `@bv.table(temporal=True, retention="7d")` registered via SDK — server stores every version keyed by `(entity_key, lsn)`; evicts versions older than retention window
2. `GET /registry` reports temporal vs non-temporal tables; `as_of=<lsn>` query param on GET returns the version-at-lsn for temporal tables; 400 for non-temporal
3. `POST /retract` with `{event_id}` undoes a table upsert/delete (restores prior version); returns 404 for unknown event_id; returns 409 for events outside retention window
4. Stream retraction is explicitly rejected in v0: `POST /retract` against a stream event_id returns 501 with message pointing at the forward-compat plan
5. Acceptance smoke: register a temporal table, upsert value at t=0, upsert at t=1, retract the t=1 event, assert GET returns t=0 value; assert `GET /table?as_of=t=0` returns t=0 value regardless of retraction state
6. Memory budget: temporal storage ≤ N× non-temporal equivalent for retention window R; measured in Phase 13 perf gate

### Phase 12: Joins + unions + push/get API completion

**Goal:** Joins (event↔event windowed, event↔table enrichment, table↔table) and `bv.union` implemented end-to-end. `push_sync`, `push_many`, `push_table`, `delete_table`, `set`, `mset`, `mget`, `get_multi` wired. Joins against temporal tables use the `as_of=...` kwarg from Phase 11.5 to resolve event-time PIT lookups.

**Depends on:** Phase 7 and Phase 11.5 (for temporal join resolution). **Parallelizable with 8, 9, 10, 11.**

**Requirements:** SDK-JOIN-01, SDK-JOIN-02, SDK-JOIN-03, SDK-JOIN-04, SDK-JOIN-05, SDK-APP-04 through SDK-APP-14, SRV-API-03 through SRV-API-10, SRV-APPLY-08 — 13 REQ-IDs (some may overlap with Phase 3).

**Success criteria:**
1. Event↔event windowed join: every (L, R) pair with same join key within window emitted exactly once; old events drop
2. Event↔table join: enrichment against current table row; value changes visible after upsert
3. Table↔table join: key-matched; schema collision handled with `_right` suffix
4. `bv.union` produces concatenated stream; field-mismatch detected at registration
5. All push/get API variants pass end-to-end Python SDK tests against a real server

### Phase 13: Observability + performance + docs + packaging + `bv.fork` — ship

**Goal:** Ship-ready v0. Metrics, perf gates cleared, docs live on `beava.dev`, binaries + PyPI + Docker published, `beava fork` subcommand works.

**Depends on:** Phases 8–12 all complete.

**Requirements:** OBS-01 through OBS-04, PERF-01 through PERF-04, DOC-01 through DOC-06, PKG-01 through PKG-05, SDK-FORK-01 through SDK-FORK-04, TEST-01 through TEST-07 — ~16 REQ-IDs plus the test suite gate.

**Success criteria:**
1. `/metrics` exposes per-operator, per-endpoint, WAL, snapshot, registry-version metrics
2. Perf benchmark harness: ≥3M EPS on THREE pipeline shapes — simple fraud (5 aggregations, 1 entity type), complex fraud (15+ aggregations, 3 entity types + stream-stream join), recommendation (windowed counts + geo-velocity + user baselines + top-k). P99 batch-get < 10ms on each. (Expanded from single-shape 2026-04-23 per user request.)
3. Docs live: quickstart → operators → concepts → http-api → architecture; `README.md` 3-command smoke works
4. `playground.beava.dev` hosts an interactive tutorial — JS in docs calls real HTTP against a shared beava instance (per-session namespace); users see real `registry_version` bumps + validation errors + feature values without installing anything. Single VM/container; ~$10-20/mo infra. Note: v0.1+ roadmap ships a browser-WASM `@beava/browser` npm library for fully-serverless interactivity — deferred because `beava-core` is already WASM-portable by project invariant (syscall-free)
5. `pip install beava` works; `docker run beava/beava:v0` works; GitHub Release binaries available for 3 platforms
6. `bv.fork(...)` spawns a local scoped replica; features queryable against fork; fork cleans up on context exit
7. All TEST-* requirements pass; CI green; ship-ready tag

---

## Traceability (preview)

Populated in `REQUIREMENTS.md` traceability section. Summary: every REQ-ID maps to exactly one phase; Phase 1 ships zero scope-shipping REQ-IDs (infrastructure).

## Notes

- ROADMAP.md may be revised as phases complete and new-requirement discoveries force rebalancing. Revisions are committed as explicit changes.
- The previous 10-phase roadmap (commit `ad5a3ef`) was re-planned on 2026-04-22 when we pivoted from a JSON-only aggregation DSL to the v1 Python SDK API shape. Phase 1 (Foundation) work carries over unchanged.
