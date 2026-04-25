# Beava v2 — v0 OSS Launch Roadmap

**Milestone:** v0 (first public OSS cut on `beava.dev`)
**Granularity:** fine (19 phases; 3–8 plans per phase)
**Mode:** yolo (auto-approved; written to hold up unrevised)
**Parallelization:** enabled where indicated
**Created:** 2026-04-22
**Revised:** 2026-04-24 (added sub-phases 6.1 async-durability, 13.1 perf-regression-fix, 13.3 lockless-apply; abandoned 13.2 coalesce; marked all shipped phases ✅ COMPLETE)
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
| 3 | Python SDK skeleton + decorators + expression DSL | `@bv.event`, `@bv.table`, `bv.col`, `bv.App(url)` (HTTP + TCP), register + validate, REGISTER JSON compiler | 20 | ✅ **COMPLETE** |
| 4 | Stateless ops + expression evaluator (server-side) | `filter`/`select`/`drop`/`rename`/`with_columns`/`map`/`cast`/`fillna` + `bv.col` evaluator | 13 | ✅ **COMPLETE** |
| 5 | Aggregation framework + core operators (8) | `group_by().agg()` + 8 core ops + `Windowed<Op>` 64-bucket infra | 15 | ✅ **COMPLETE** |
| 5.5 | Perf harness + retroactive baselines | `criterion` workspace + `.planning/perf-baselines.md` + 10%/25% regression gate | ~10 | ✅ **COMPLETE** |
| 6 | WAL + idempotency | Group-commit fsync ACK + dedupe-key replay | 5 | ✅ **COMPLETE** |
| 6.1 | Async durability (SyncMode + /push-sync) | Adds Kafka-style acks=1 default to `/push` (~15× EPS lift) while preserving acks=all path via `/push-sync` | ~6 | ✅ **COMPLETE** |
| 7 | Snapshot + recovery | Periodic full-state snapshot; restart replays snapshot + WAL; schema evolution survives restart | 6 | ✅ **COMPLETE** |
| 7.5 | End-to-end throughput harness + first baseline | Reusable harness measuring sustained EPS + push/get latency through the live HTTP+TCP server. Tiered pipelines (small=1 / medium=5 / large=15 features). 60s wall-time time-bounded runs. Baselines committed to `.planning/throughput-baselines.md` keyed by hw-class. Establishes the per-phase throughput-run convention every operator phase (8–12) must honor. | 6 | ✅ **COMPLETE** |
| 8 | Point / ordinal / recency operators | first, last, first_n, last_n, lag, first_seen, last_seen, age, has_seen, time_since, time_since_last_n, streak, max_streak, negative_streak, first_seen_in_window + TCP `OP_PUSH` | 15 | ✅ **COMPLETE** |
| 9 | Decay + velocity operators | ewma, ewvar, ew_zscore, decayed_sum, decayed_count, twa, rate_of_change, inter_arrival_stats, burst_count, delta_from_prev, trend, trend_residual, outlier_count, value_change_count, z_score | 16 | ✅ **COMPLETE** |
| 10 | Sketch operators | count_distinct (HLL), percentile (UDDSketch), top_k (SpaceSaving), bloom_member, entropy | 5 | ✅ **COMPLETE** |
| 11 | Bounded-buffer + geo operators | histogram, hour_of_day/dow_hour histograms, seasonal_deviation, event_type_mix, most_recent_n, reservoir_sample, geo_velocity, geo_distance, geo_spread, unique_cells, geo_entropy, distance_from_home | 13 | ✅ **COMPLETE** |
| 11.5 | Temporal tables + retraction primitive | MVCC storage for `@bv.table(temporal=True, retention=...)`; `app.retract(event_id)` scoped to table upserts/deletes; wires `as_of=...` kwarg that Phase 12 joins consume; stream retraction deferred to v1 but event-IDs land now | ~10 | ✅ **COMPLETE** |
| 12 | Joins + unions + push/get API completion | Event↔event windowed join, event↔table enrichment (incl. event-time PIT against temporal tables), table↔table join, `bv.union`; `push_sync` + `push_many` + `push_table` + `delete_table` + `set` + `mset` + `mget` + `get_multi` wired end-to-end | 13 | 🟡 **PARTIAL** (1/6 plans on `phase-12-joins`; 5 plans pending on `phase-12-followup` worktree) |
| 12.5 | `push_and_get` combined endpoint | Single-round-trip `POST /push-and-get/{event}` and `OP_PUSH_AND_GET` (TCP) that applies a push and queries features atomically under one writer borrow. Read-your-writes by construction; ~2× latency win on fraud-decisioning hot path; `push_sync_and_get` parity for acks=all | ~8 | 📋 **PLANNED** (3 plans landed; executor pending merge round) |
| 13 | Observability + performance + docs + packaging + `bv.fork` + playground | `/metrics`, structured logs, perf gates on THREE pipelines (simple fraud, complex fraud, recommendations) ≥3M EPS, <10ms P99 batch get, SDK polish, docs, hosted interactive tutorial at playground.beava.dev, PyPI, GitHub Releases, Docker, `beava fork` subcommand | ~18 | 🟡 **PARTIAL** (2/8 plans on `phase-13-ship`; cold-entity GC + perf gate + metric wiring pending on `phase-13-followup` worktree; docs/fork/packaging/playground deferred to v0.0.x point releases per Phase 13 CONTEXT D-16) |
| 13.1 | Perf regression fix — fsync off the runtime thread | `spawn_blocking` for WAL fsync; restored 17k EPS at parallel=64 on macOS | 1 | ✅ **COMPLETE** |
| ~~13.2~~ | ~~Batch coalescing~~ | ~~ApplyConfig 6-knob + ApplyBuffer skeleton~~ | — | ❌ **ABANDONED** — superseded by Phase 13.3 (RefCell + LocalSet, simpler/faster Redis-shaped approach). Branch `phase-13.2-coalesce` is not to be merged; ApplyBuffer primitive is not reused. |
| 13.3 | Lockless apply via RefCell + LocalSet (Option 0) | Replace apply-state Mutex with single-thread `RefCell` + `LocalSet`; Redis-shaped event loop; target ~60ns/event; ~500 LoC refactor | ~4 | 🟡 **IN PROGRESS** — work on worktree `phase-13.3-lockless-apply` @ `34f82e8`; bottleneck investigation underway; not yet merged. Plans 13.3-01..04 landed in `.planning/phases/13.3-lockless-apply/` (rewritten 2026-04-24 as pure Option B, no mpsc) |
| 14 | Streaming semantics — Chunk A (correctness) | Per-stream watermark state + front-door drop of events older than `max_event_time - tolerate_delay_ms`; `beava_events_dropped_late_total{stream}` counter + rate-limited log; bucket widening `bucket_ms = (window + tolerate_delay) / 64` to fix the `agg_windowed` bucket-reset silent data-loss bug; register-time validator `tolerate_delay ≤ window`; WAL replay reconstructs watermark. **Watermark**: per-stream, default `tolerate_delay_ms = 5000`. **Drop policy**: silent + metric + rate-limited log. **No modifiability / no retraction on apply path in this phase** — that is Phase 14.1. | ~400 LoC | 📋 **PLANNED** (split from original Phase 14 on 2026-04-24 so the bucket-reset bug fix ships independently of opt-in modifiability) |
| 14.1 | Streaming semantics — Chunk B (opt-in modifiability) | `@bv.event(modifiable=True, modification_log_depth=16)` schema addition on source events; per-(entity, feature) K-event log lazy-allocated only for Tier 3 (order-sensitive) operators; insert-replay path for out-of-order events within tolerance; Tier 3 operator state redesign to replay from snapshot (ewma, streak, velocity, geo_velocity, etc.); basic retraction-impact analyzer for STREAM sources with human-readable warning codes (`BV-W-AGG-APPROX-MODIFIABLE`, `BV-W-AGG-REJECT-MODIFIABLE`, `BV-W-AGG-SUBTRACTIVE-OK`); docs pages at `beava.dev/w/`. Terminology: "retraction" renamed to "modifiable" / "change or delete" in all user-facing prose. | ~800 LoC + ~200 LoC analyzer | 📋 **PLANNED** |
| 15 | Event-time PIT temporal store | Temporal chain keyed by `(event_time_ms, lsn)` composite (LSN is tiebreaker); out-of-order upserts self-heal by slotting into event_time position; naturally commutative under any arrival order; retention axis shifts wall-time → event-time; retention DERIVED from watermark (`derived_retention_ms = max(S.tolerate_delay for streams S joining this table)`) with optional user override for longer retract horizons; `GET /table?as_of=...` moved behind `BEAVA_DEV_ENDPOINTS=1` (no public historical-extraction surface in v0); register-time diagnostic when new join grows derived retention; snapshot format bump. **Must land before Phase 12 Plan 04** so the stream↔table join call site uses `lookup_at_event_time(key, event.event_time_ms)` from day one — zero rework. | ~350 LoC | 📋 **PLANNED — blocks Phase 12 Plan 04** |
| 16 | SDK surface v0 ergonomics — explicit `@bv.source` + `app.upsert/delete` | Explicit `@bv.source` annotation on class-form `@bv.event` / `@bv.table` (derivations keep inferred-by-form contract); `app.upsert(T, {...})` + `app.delete(T, key={...})` verbs replace/complement `app.push_table` + `app.delete_table`; register-time enforcement (class-form without `@bv.source` errors; function-form with `@bv.source` errors; `app.upsert` on derivation → 400 `cannot_push_to_derivation`); `tolerate_delay_ms` + `modifiable=True` attach only to source-decorated things; derivations inherit from root source. Warning code `BV-W-SOURCE-NOT-ANNOTATED`. | ~250 LoC | 📋 **PLANNED** (before v0 ship-gate tag so public surface is stable) |
| 17 | Table aggregation with tiered modifiability (v0.1) | Unlock `@bv.table(temporal=True).group_by(...).agg(...)` with tiered semantics: **Tier C** (exact propagation) count / sum / avg / variance / histograms / ewma-subtractive; **Tier A** (best-effort, bounded drift) HLL — exact in ExactArray/HashSet modes, approximate only once promoted above 1024 — UDDSketch / TopK / CMS / Bloom / entropy; **Tier B** (deterministic-reject) min / max / first / last / streak / lag / first_n / last_n / time_since. Extended retraction-impact analyzer covers TABLE sources; runtime metric `beava_feature_promoted_to_approx_total{feature}`. Depends on Phase 14.1 (modifiability machinery) + Phase 15 (event-time temporal store). | ~650 LoC | 📋 **PLANNED (v0.1)** — ships post-v0 ship-gate; SDK currently returns `SDK-AGG-05 TypeError` on `@bv.table(...).group_by(...)` |
| 18 | Redis-shaped hand-rolled hot path | 8/11 | In Progress|  |

**Total:** 26 phases (Phase 2.5 inserted 2026-04-23 for dual HTTP+TCP wire; Phase 5.5 inserted 2026-04-23 for perf harness + retroactive baselines + per-phase regression gates; Phase 11.5 inserted 2026-04-23 for temporal tables + retraction primitive required by PIT stream↔table joins; Phase 7.5 inserted 2026-04-23 for end-to-end throughput harness + per-phase throughput-run convention; Phase 6.1 inserted 2026-04-24 to split async-durability out of the Phase 6 acks=all path; Phase 12.5 inserted 2026-04-24 for the `push_and_get` combined endpoint (single-RT atomic push+query); Phase 13.1 inserted 2026-04-24 for the fsync-off-runtime regression fix; Phase 13.3 inserted 2026-04-24 as the canonical apply-lock removal, replacing the abandoned 13.2 coalesce spike; Phase 14 added 2026-04-24 as streaming-semantics correctness (Chunk A — watermark + drop + bucket widening); Phase 14.1 added 2026-04-24 as opt-in modifiability (Chunk B — `@bv.event(modifiable=True)` + per-(entity,feature) K-event log + Tier 3 op replay + human-readable warning analyzer); Phase 15 added 2026-04-24 as event-time PIT temporal store (chain keyed by `(event_time, lsn)` composite; retention derived from watermark; `GET /table?as_of=...` moved behind dev gate; blocks Phase 12 Plan 04 by design); Phase 16 added 2026-04-24 as SDK surface v0 ergonomics (`@bv.source` explicit annotation + `app.upsert/delete` verbs); Phase 17 added 2026-04-24 as v0.1 table aggregation with tiered modifiability (Subtractive-exact / Approximate-best-effort / Deterministic-reject) and extended warning analyzer; Phase 18 added 2026-04-24 as the Redis-shaped hand-rolled hot path (replaces tokio on apply + wire path; 6 stages with HARD Linux-Xeon ≥3M EPS/core ship-gate at Stage 18.5). All further perf-optimization phases (sharding / io_uring / binary schema format) are deliberately held pending Phase 14 — they would re-architect around semantics that aren't yet correct). Terminology: "retraction" renamed to "modifiable" / "change or delete" in user-facing prose; internal type names (`Retracted` variant etc.) preserved as implementation detail. ~179 requirements mapped, ~88 success criteria.

**Phase 1 status:** ✅ **COMPLETE** on commits `b100e51`..`c21b6b7`. Cargo workspace, axum HTTP server, `/health` + `/ready` stubs, graceful shutdown, integration TestServer harness — all gates green. See `.planning/phases/01-foundation/01-SUMMARY.md`, `.planning/phases/01-foundation/01-VERIFICATION.md`.

## Parallelization

- **Phases 1 → 2 → 3 → 4 → 5 → 6 → 7 → 7.5** are strictly sequential — each depends on the one before. Phase 5 is where the apply loop first runs real aggregations; Phases 6–7 harden durability around it; Phase 7.5 builds the throughput harness on top of stable durability so EPS numbers reflect production shape (WAL fsync + snapshot/recovery in the path).
- **Phases 8 / 9 / 10 / 11** can run in parallel after Phase 7.5 — each operator family attaches to the existing apply loop + registry + window infra, touching independent operator modules. Recommended: sequence 8 → 9 → 10 → 11 unless explicitly running parallel worktrees. Each must include a "throughput run" task that re-runs the Phase 7.5 harness with that family's operators added to the medium/large pipelines and appends the result to `.planning/throughput-baselines.md`.
- **Phase 11.5** (temporal tables + retraction) depends on 7 (needs WAL + snapshot); can run parallel with 8–11 since it touches its own table-storage module. MUST ship before Phase 12 because joins consume the `as_of=...` kwarg. Throughput run measures upsert/retract path against the temporal-table workload variant.
- **Phase 12** (joins/unions + push/get completion) depends on 7 AND 11.5; can overlap with 8–11 since joins live in their own module. Throughput run adds the join-shape pipeline (event↔table enrichment) to the harness.
- **Phase 13** waits on everything for the final three-shape perf gate (simple fraud / complex fraud / recommendations ≥ 3M EPS) + `/metrics` + docs sign-off. By Phase 13 the throughput-baselines ledger has ~6 rows showing how EPS evolved phase-by-phase.

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
       ▼
  Phase 7.5 (End-to-end throughput harness + first baseline)
       │
       ├────────────┬────────────┬────────────┬────────────┐
       ▼            ▼            ▼            ▼            ▼
  Phase 8       Phase 9      Phase 10     Phase 11     Phase 12
  (recency/     (decay/      (sketches)   (buffer+geo) (joins +
  point ops)    velocity)                              unions + API
                                                        completion)
  ↓ each phase 8-12 ships a "throughput run" task using the 7.5 harness
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

**Plans:** 7/8 plans executed
- [x] 05-01-PLAN.md — AggOp enum + per-op state structs (Count/Sum/Avg/Min/Max/Variance/StdDev/Ratio) + Windowed<Op> 64-bucket tumbling (AGG-CORE-01..09, SDK-AGG-03)
- [x] 05-02-PLAN.md — `where=` predicate threading through apply path (SDK-AGG-04)
- [x] 05-03-PLAN.md — AggregationDescriptor + propagate_aggregation_schema (SDK-AGG-01, SDK-AGG-03)
- [x] 05-04-PLAN.md — Register-time Rule 11 + compiled_aggregations cache + HTTP/TCP wire errors (SDK-AGG-05, SDK-AGG-06)
- [x] 05-05-PLAN.md — Apply loop hook + per-entity AggStateTable + /dev/apply_events (SDK-AGG-02, AGG-CORE-09)
- [x] 05-06-PLAN.md — Feature query endpoints GET /get/{feature}/{key} + POST /get + cross-agg collision rule (SDK-AGG-02)
- [x] 05-07-PLAN.md — Python SDK group_by + 8 bv.<op> helpers + REGISTER JSON serialization (SDK-AGG-01..06)
- [x] 05-08-PLAN.md — Phase 5 Rust + Python acceptance smokes (SC1..SC6 coverage)

### Phase 5.5: Perf harness + retroactive baselines

**Goal:** Set up `criterion` bench harness workspace-wide. Write retroactive microbenches for every prior phase's hot path. Establish baseline numbers committed to `.planning/perf-baselines.md`. Establish the regression-gate convention (10% slower = warn; 25% slower = block) that every subsequent phase must honor. This is NOT about optimizing — just measuring, so regressions in later phases (8–13) surface incrementally rather than landing as a surprise in the Phase 13 perf gate.

**Depends on:** Phase 5 (all prior phases must exist to bench).

**Requirements:** PERF-HARNESS-01 (criterion workspace setup), PERF-HARNESS-02 (baselines file + hw-class tagging), PERF-HARNESS-03 (regression thresholds 10%/25%), PERF-BENCH-WIRE-01 (Phase 2.5 frame codec encode/decode throughput), PERF-BENCH-SDK-01 (Phase 3 REGISTER JSON compile throughput), PERF-BENCH-EXPR-01 (Phase 4 parse + eval per op), PERF-BENCH-OPCHAIN-01 (Phase 4 OpChain apply for 4-op chain), PERF-BENCH-AGG-01 (Phase 5 AggOp::update per op), PERF-BENCH-WINDOWED-01 (Phase 5 WindowedOp fold 64-bucket), PERF-BENCH-APPLY-01 (Phase 5 apply_event_to_aggregations per event) — ~10 REQ-IDs to be defined at plan-time.

**Success criteria:**
1. `cargo bench --workspace` runs all benches and produces baseline numbers per hw class
2. `.planning/perf-baselines.md` committed with machine-class-tagged results (e.g., `hw: apple-m1-pro; os: darwin; cpu-count: 10`)
3. Regression gate documented in CLAUDE.md §Conventions and plan-checker contract: every phase from here forward MUST ship at least one microbench
4. Retroactive baselines prove ≥1 bench per phase 2.5/3/4/5
5. Phase 13 end-to-end perf gate (≥3M EPS/core, P99 <10ms batch-get) still the final ship gate — Phase 5.5 does NOT replace it, just surfaces regressions early

**Plans:** 5/6 plans executed

### Phase 6: WAL + idempotency

**Goal:** `/push` ACK returns only after event's LSN has been fsynced. Stream-level `dedupe_key` + window enforced: duplicate requests return the cached response byte-identical.

**Depends on:** Phase 5.

**Requirements:** SRV-DUR-01, SRV-DUR-02, SRV-DUR-03, SRV-DUR-04, SRV-DUR-05 — 5 REQ-IDs.

**Success criteria:**
1. Push event, kill process before fsync, restart → event NOT present. Push event, wait for ACK, kill → event IS present.
2. Duplicate push with same dedupe key within window returns byte-identical response; state unchanged between first and duplicate
3. Group-commit fsync adds P50 < 2ms to push-ACK latency at default config
4. WAL rotation: segments ≤ snapshot-covered LSN truncated; disk usage bounded

**Plans:** 2/4 plans executed
- [x] 06-01-PLAN.md — beava-persistence crate + WAL record frame + WalWriter/WalReader (no fsync)
- [x] 06-02-PLAN.md — Group-commit fsync worker + segment rotation + truncate_up_to
- [ ] 06-03-PLAN.md — IdemCache + /push HTTP endpoint wiring (durable ACK + byte-identical dedupe replay)
- [ ] 06-04-PLAN.md — Crash UAT subprocess tests + criterion perf baselines + phase smoke + PHASE-SUMMARY

### Phase 7: Snapshot + recovery + schema evolution

**Goal:** Periodic snapshot serializes in-memory state + registry; restart loads snapshot + replays WAL-past-snapshot-LSN and resumes. Schema evolution preserved across restart.

**Depends on:** Phase 6.

**Requirements:** SRV-REG-04, SRV-RECOV-01, SRV-RECOV-02, SRV-RECOV-03, SRV-RECOV-04, SRV-RECOV-05 — 6 REQ-IDs.

**Success criteria:**
1. Run 1M events through the server, snapshot fires, restart → all features replayable; values match pre-restart
2. Add a new feature (additive registration + version bump), snapshot, restart → new feature still present
3. RTO: 10GB state snapshot + 1GB WAL tail → server online within 30s on NVMe
4. Corrupt snapshot (flipped byte) detected + logged; operator can fall back to previous

### Phase 7.5: End-to-end throughput harness + first baseline

**Goal:** Build a reusable, hardware-tagged throughput harness that drives a live `beava` server (HTTP + TCP) end-to-end and produces sustained EPS + push/get latency numbers. Capture the first baseline using only Phase 5 operators (count/sum/avg/min/max/variance/stddev/ratio) over Phase 6 WAL durability + Phase 7 snapshot/recovery in the path. Establish the per-phase "throughput run" convention so every operator phase from 8 onward appends a row to `.planning/throughput-baselines.md`. This is NOT about hitting 3M EPS — it is about starting the line and having a stable, comparable measurement system before the operator catalog grows.

**Depends on:** Phase 7 (needs durable + recoverable server in the loop so numbers are production-shaped, not toy-mode).

**Requirements:** THROUGHPUT-HARNESS-01 (harness crate + result schema), THROUGHPUT-HARNESS-02 (`.planning/throughput-baselines.md` ledger format + hw-class tagging matching perf-baselines.md convention), THROUGHPUT-HARNESS-03 (per-phase regression thresholds 10% warn / 25% block on the simple-fraud shape), THROUGHPUT-PIPELINES-01 (small/medium/large pipeline configs: 1 / 5 / 15 features, 1 entity type, 1 window), THROUGHPUT-WORKLOAD-01 (60s wall-time time-bounded run; record EPS, P50/P95/P99 push latency, P99 batch-get, RSS at peak), THROUGHPUT-FIRST-BASELINE-01 (Phase 5-operators-only baseline committed for all 3 sizes on at least one hw-class) — 6 REQ-IDs.

**Success criteria:**
1. `cargo bench --bench throughput` (or equivalent CLI) drives a real server over HTTP + TCP and returns structured results for the small / medium / large pipelines
2. `.planning/throughput-baselines.md` exists with hw-class-tagged rows for the first baseline (small/medium/large × HTTP/TCP) on at least one machine class
3. Plan-checker contract: every phase from 8 onward MUST include a "throughput run" task that re-runs the harness, appends a row, and asserts no > 25% regression on the simple-fraud shape
4. Harness output schema documented and stable across phases (numeric comparisons across phases must work mechanically)

**Plans:** to be written at plan-time (estimated 4 plans: harness crate + result schema, pipeline configs, baseline capture + ledger, smoke + SUMMARY).

### Phase 8: Point / ordinal / recency operators

**Goal:** The point-shaped operator family lands — values, sequences, streaks, recency markers.

**Depends on:** Phase 7.5 (uses throughput harness). **Parallelizable with Phases 9, 10, 11, 12.**

**Requirements:** AGG-POINT-01 through AGG-POINT-11, AGG-RECENCY-01 through AGG-RECENCY-04 — 15 REQ-IDs.

**Success criteria:**
1. All 15 operators pass table-driven correctness tests with deterministic replay
2. Operators round-trip through WAL + snapshot + recovery
3. Docs entry per operator in `docs/operators.md`
4. SDK descriptor constructors match v1 API (same parameter names)
5. Throughput run: harness re-run with this phase's operators added to medium/large pipelines; row appended to `.planning/throughput-baselines.md`; no > 25% regression on simple-fraud shape vs Phase 7.5 baseline

### Phase 9: Decay + velocity operators

**Goal:** Exponentially-decayed and velocity-shaped operators land.

**Depends on:** Phase 7.5 (uses throughput harness). **Parallelizable with 8, 10, 11, 12.**

**Requirements:** AGG-DECAY-01 through AGG-DECAY-07, AGG-VEL-01 through AGG-VEL-08, AGG-Z-01 — 16 REQ-IDs.

**Success criteria:**
1. All 15 operators pass correctness + determinism tests
2. `bv.ema()` alias resolves to `bv.ewma()` in the SDK
3. Half-life parameter validation at decoration time (duration string format)
4. Operators replay byte-identically after restart
5. Throughput run: harness re-run with decay/velocity ops in the medium/large pipelines; row appended to `.planning/throughput-baselines.md`; no > 25% regression on simple-fraud shape

### Phase 10: Sketch operators

**Goal:** Approximate-algorithm operators land with documented error bounds.

**Depends on:** Phase 7.5 (uses throughput harness). **Parallelizable with 8, 9, 11, 12.**

**Requirements:** AGG-SKETCH-01 through AGG-SKETCH-05 — 5 REQ-IDs.

**Success criteria:**
1. `count_distinct`, `percentile`, `top_k` pass error-bound checks (within documented tolerances on reference datasets)
2. Sketch serialization round-trips through snapshot + WAL replay; deterministic under sketched inputs
3. `bloom_member` and `entropy` pass table-driven tests
4. Memory bounded per-entity by operator configuration
5. Throughput run: harness re-run with sketch ops in the medium/large pipelines; row appended to `.planning/throughput-baselines.md`; no > 25% regression on simple-fraud shape (note: sketches add memory + CPU per insert — large-pipeline regression most likely here)

### Phase 11: Bounded-buffer + geo operators

**Goal:** Histograms, per-user baselines, and geo-shaped operators land.

**Depends on:** Phase 7.5 (uses throughput harness). **Parallelizable with 8, 9, 10, 12.**

**Requirements:** AGG-BUFFER-01 through AGG-BUFFER-07, AGG-GEO-01 through AGG-GEO-06 — 13 REQ-IDs.

**Success criteria:**
1. All 13 operators pass correctness tests
2. Geo math verified against a reference implementation (`haversine` crate)
3. Structured outputs (histograms, reservoir samples) round-trip through `GET /get/{feature}/{key}` with `{value, meta?}` shape
4. Replay determinism preserved
5. Throughput run: harness re-run with buffer/geo ops in the medium/large pipelines + a recommendation-shape pipeline variant exercising geo-velocity; row appended to `.planning/throughput-baselines.md`; no > 25% regression on simple-fraud shape

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
7. Throughput run: harness re-run with a temporal-table workload variant (upsert-heavy + occasional retract) appended; row added to `.planning/throughput-baselines.md`; baseline table-write throughput captured for the first time

### Phase 12: Joins + unions + push/get API completion — 🟡 PARTIAL

**Status:** Plan 12-02 shipped on branch `phase-12-joins` @ `d541971` (WAL replay for `TableUpsert/Delete/Retract`). Plans 12-01, 12-03, 12-04, 12-05, 12-06 pending on worktree `.claude/worktrees/phase-12-followup`.

**Goal:** Joins (event↔event windowed, event↔table enrichment, table↔table) and `bv.union` implemented end-to-end. `push_sync`, `push_many`, `push_table`, `delete_table`, `set`, `mset`, `mget`, `get_multi` wired. Joins against temporal tables use the `as_of=...` kwarg from Phase 11.5 to resolve event-time PIT lookups.

**Depends on:** Phase 7 and Phase 11.5 (for temporal join resolution). **Parallelizable with 8, 9, 10, 11.**

**Requirements:** SDK-JOIN-01, SDK-JOIN-02, SDK-JOIN-03, SDK-JOIN-04, SDK-JOIN-05, SDK-APP-04 through SDK-APP-14, SRV-API-03 through SRV-API-10, SRV-APPLY-08 — 13 REQ-IDs (some may overlap with Phase 3).

**Success criteria:**
1. Event↔event windowed join: every (L, R) pair with same join key within window emitted exactly once; old events drop
2. Event↔table join: enrichment against current table row; value changes visible after upsert
3. Table↔table join: key-matched; schema collision handled with `_right` suffix
4. `bv.union` produces concatenated stream; field-mismatch detected at registration
5. All push/get API variants pass end-to-end Python SDK tests against a real server
6. Throughput run: harness re-run with a join-shape pipeline (event↔table enrichment) appended to medium/large; row added to `.planning/throughput-baselines.md`; this is the last incremental data point before Phase 13's three-shape ship gate

### Phase 12.5: `push_and_get` combined endpoint — 📋 PLANNED

**Status:** 3 plans landed in `.planning/phases/12.5-push-and-get/` (12.5-CONTEXT.md + 12.5-01/02/03-PLAN.md). Executor pending — sits behind the Phase 12/13 merge round and Phase 13.3 lockless-apply landing.

**Goal:** Add a single-round-trip combined push + feature-query endpoint that applies a push and queries features for an entity key **atomically** under the same single-writer borrow scope. Read-your-writes by construction, ~2× latency win on the flagship fraud-decisioning hot path, zero change to throughput.

**Depends on:** Phase 12 (push/get API completion — combined endpoint reuses the same `/push` and `/get` plumbing). Phase 13.3 helps but is not strictly required (atomic borrow is independent of Mutex removal).

**Success criteria:**
1. `POST /push-and-get/{event_name}` with `{row, query}` body returns 200 with `{ack_lsn, registry_version, features, warnings?}` — push commits AND features reflect the newly-pushed event atomically (read-your-writes)
2. `POST /push-sync-and-get/{event_name}` returns ONLY after WAL fsync completes (acks=all parity with `/push-sync`)
3. TCP `OP_PUSH_AND_GET (0x0015)` and `OP_PUSH_SYNC_AND_GET (0x0016)` expose the same behavior with CT_JSON payloads; strict-FIFO connection semantics preserved; response op echoes request op
4. Python SDK `app.push_and_get(EventType, row={...}, entity_key={...}, features=[...], sync=False)` returns a `(ack, features_dict)` tuple; `sync=True` uses the acks=all path
5. Unknown feature in `query.features` returns 200 with that feature mapped to `null` and the feature name added to `warnings[]`; the push itself still commits and ack_lsn is returned
6. Latency on simple-fraud shape (1 push + 3 feature reads) on Apple-M4 LAN loopback: P50 < 300μs end-to-end over HTTP; baseline row appended to `.planning/perf-baselines.md` and `.planning/throughput-baselines.md`

**Out of scope (deferred to v0.1):** `push_and_get_multi` (push 1, query N keys), `push_many_and_get` (batch push + 1 query). `/metrics` wiring inherits Phase 13 once 13-05 ships per-endpoint counter middleware. CT_JSON only in v0 (parity with `OP_PUSH`).

### Phase 13: Observability + performance + docs + packaging + `bv.fork` — ship — 🟡 PARTIAL

**Status:** Plans 13-01 (`/metrics` Prometheus + middleware) and 13-03 (`env_var_overrides` hermetic fix) shipped on branch `phase-13-ship` @ `2ef5afc`. Plan 13-02 (cold-entity GC sweep), Plan 13-04 (perf gate), and metric-counter wiring pending on worktree `.claude/worktrees/phase-13-followup`. Plans 13-05..13-08 (docs site, `bv.fork`, PyPI/Docker/Releases, playground) deferred to v0.0.x point releases per Phase 13 CONTEXT D-16.

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

### Phase 13.1: Perf regression fix — fsync off the runtime thread — ✅ COMPLETE

**Status:** Merged to `v2/greenfield` as `5b60bdc` (merge) / `2f3a092` (impl) / `a03730e` (regression test). Restored ~17k EPS at parallel=64 on macOS Apple-M4.

**Goal:** Move the WAL fsync syscall off the Tokio `current_thread` runtime via `spawn_blocking` so long fsyncs don't starve the apply loop.

**Depends on:** Phase 6.1 (WAL async dispatch path).

**Success criteria:**
1. WAL fsync never runs on the runtime thread (verified by regression test `test_fsync_does_not_stall_runtime`)
2. 10× throughput regression observed pre-fix is closed (measured in `.planning/throughput-baselines.md`)
3. No new flakes; 850 tests green on `v2/greenfield`

### ~~Phase 13.2: Batch coalescing~~ — ❌ ABANDONED

**Status:** Spike shipped on branch `phase-13.2-coalesce` @ `2122a16` (Plan 01 — ApplyConfig 6-knob + ApplyBuffer skeleton + 20 tests; RYW default preserved). **Do not merge.** Follow-up plans 02–05 are cancelled.

**Why abandoned:** Phase 13.3 (RefCell + LocalSet) is simpler, faster, and Redis-shaped. The ApplyBuffer primitive from 13.2 is not reused — 13.3 removes the Mutex outright rather than amortizing contention across it.

### Phase 13.3: Lockless apply via RefCell + LocalSet (Option 0) — 🟡 IN PROGRESS

**Status:** Plans 13.3-01..04 landed in `.planning/phases/13.3-lockless-apply/`. Implementation work on worktree `.claude/worktrees/phase-13.3-lockless-apply` @ `34f82e8` — currently in bottleneck investigation (samply flamegraph + root cause). Not yet merged to `v2/greenfield`.

**Goal:** Replace the per-state Mutex in the apply loop with a single-thread `RefCell`-owning actor driven by Tokio's `LocalSet`. Apply becomes a Redis-shaped non-blocking loop; lock contention disappears; fsync stays off-thread (Phase 13.1). Target: ~60ns/event inside the apply loop; ~500 LoC refactor concentrated in `beava-server/src/apply/` and the runtime wiring.

**Depends on:** Phase 13.1 (fsync spawn_blocking) — must stay in place because the apply loop is still single-threaded and must not block on I/O.

**Success criteria:**
1. Apply loop owns state via `RefCell` inside a `LocalSet` task; no `Mutex` / `RwLock` on the hot apply path
2. Per-event apply cost measured ≤ 80ns on Apple-M4 (target 60ns) in the criterion microbench
3. End-to-end throughput on `beava-bench` small/medium/large × HTTP/TCP improves measurably vs. Phase 13.1 baseline at BATCH_MS=0 (single-event, sync mode)
4. Read-your-writes semantics preserved — existing acceptance smokes and crash UAT unchanged
5. No new flakes; full workspace test suite green; clippy + fmt clean
6. Regression gate row appended to `.planning/perf-baselines.md` and `.planning/throughput-baselines.md`

**Downstream gating:** Unblocks the Phase 13 ship-gate perf numbers (≥3M EPS/core on the three pipeline shapes) because the apply loop is today the bottleneck — the Mutex caps us at ~17k EPS regardless of fsync cost.

### Phase 14.1: Streaming semantics — Chunk B (opt-in modifiability) — 📋 PLANNED

**Status:** Plans landed 2026-04-24. Depends on Phase 14 (watermark + drop + bucket widening).

**Goal:** Ship opt-in `@bv.event(modifiable=True, modification_log_depth=16)` + per-(entity,feature) K-event log lazy-allocated for Tier 3 operators + register-time retraction-impact analyzer (BV-W-AGG-APPROX-MODIFIABLE / REJECT-MODIFIABLE / SUBTRACTIVE-OK) + Tier 3 operator state redesign (replay_from_log helpers) + WAL-replay-via-replay recovery (Option B, no new WAL record variant) + snapshot format v3.

**Depends on:** Phase 14 (watermark + tolerance window), Phase 13.3 (lockless apply borrow_mut scope).

**Plans:** 6 plans
- [ ] 14.1-01-PLAN.md — Schema: `modifiable` + `modification_log_depth` on EventDescriptor + Python SDK kwargs + register-time bounds validator
- [ ] 14.1-02-PLAN.md — `ModificationLog` ring buffer + `AggKind::tier()` classifier + `EntityRow` max_event_time_ms + lazy mod_logs slots
- [ ] 14.1-03-PLAN.md — `retraction_analyzer.rs` + register response warnings/errors arrays + Python `BeavaRetractionWarning` + docs stubs at beava.dev/w/
- [ ] 14.1-04-PLAN.md — Tier 3 `replay_from_log` helpers across decay/velocity/state/geo + proptest sorted-vs-shuffled equivalence (SC4)
- [ ] 14.1-05-PLAN.md — `apply_with_modifiability` fast/slow-path + push.rs wire-up + snapshot format v3 + WAL replay preserves mod state + end-to-end smoke
- [ ] 14.1-06-PLAN.md — Criterion microbench (mod=false ≤ 5 ns; mod=true slow-path ≤ 5 µs) + promotion/eviction metrics + beava-bench throughput rows + SUMMARY + VERIFICATION

**Success criteria:** see `.planning/phases/14.1-streaming-modifiability/14.1-CONTEXT.md` (SC1–SC8).

### Phase 15: Event-time PIT temporal store — 📋 PLANNED

**Status:** Plans landed 2026-04-24. Blocks Phase 12 Plan 04.

**Goal:** Swap the Phase-11.5 LSN-keyed MVCC chain to a `(event_time_ms, lsn)` composite key so stream↔table joins resolve point-in-time against event-time, out-of-order upserts self-heal without replay, retention derives from the DAG watermark, and `GET /table?as_of=...` moves behind the dev gate.

**Depends on:** Phase 11.5 (MVCC + retraction primitive), Phase 14 (watermark state must exist before sweep can use it).

**Plans:** 3 plans
- [ ] 15-01-PLAN.md — Core chain refactor: `(event_time, lsn)` composite + `lookup_at_event_time` + retraction preserved
- [ ] 15-02-PLAN.md — Registry DAG walk for `derived_retention_ms` + event-time sweep + `BV-I-RETENTION-GROWTH` diagnostic
- [ ] 15-03-PLAN.md — HTTP dev-gating + `event_time_field` enforcement + snapshot v2 + criterion bench + SUMMARY + VERIFICATION

**Success criteria:** see `.planning/phases/15-event-time-pit/15-CONTEXT.md` (SC1–SC7).

### Phase 18: Redis-shaped hand-rolled hot path — 🔄 IN PROGRESS

**Status:** Plan 18-01 COMPLETE (2026-04-25). `beava-runtime-core` crate scaffold, HTTP/1.1 + framed TCP parsers, WireRequest dispatch to AppState via `runtime_core_glue.rs`, `ServerV18::bind_v18` with tokio/axum admin sidecar, samply profiling procedure. Next: Plan 18-02 (inline WAL + pthread fsync).

**Goal:** Replace tokio on the apply + wire hot path with a hand-rolled event loop matching Redis 7.x architecture. Spec target: ≥3M EPS/core simple-fraud TCP on Linux Xeon. The hand-rolled hot path handles BOTH HTTP/1.1 + framed TCP for data-plane endpoints (`/push`, `/push-sync`, `/push-batch`, `/get`, `/upsert`, `/delete`, `/retract`); admin endpoints (`/metrics`, `/health`, `/ready`, `/registry`) stay on tokio/axum on a separate port (`8081`).

**Depends on:** Phase 13 ship-gate baseline (need to know the floor `tokio` produces before measuring lift). Phase 13.3 lockless-apply landing (apply thread already owns `RefCell<AppState>` directly — Phase 18 is a wire-stack rewrite, not a state-ownership rewrite). All Phase 8–11 + 12 + 12.5 operators must be on `v2/greenfield` so the wire rewrite measures the real workload.

**Stages and perf gates:**
1. **18.0** — research + translation (complete)
2. **18.1** — hand-rolled event loop + HTTP/1.1 + framed TCP (~2200 LoC, 6 tasks). Apple-M4 INFORMATIONAL gate. ✅ COMPLETE
3. **18.2** — inline WAL + pthread fsync (~300 LoC, 3 tasks). Apple-M4 INFORMATIONAL gate.
4. **18.3** — I/O threads for reads (Redis 6.0 pattern, ~500 LoC, 5 tasks). Apple-M4 INFORMATIONAL gate: 1–1.5M EPS/core aggregate with 4 I/O threads.
5. **18.4** — I/O threads for writes (~250 LoC, 3 tasks). Apple-M4 INFORMATIONAL gate: 2–2.5M EPS/core aggregate; tail p99 <5ms.
6. **18.4.5** — Linux Xeon bench infra setup (markdown/setup only, no code, no TDD).
7. **18.5** — `io_uring` on Linux (~600 LoC, 5 tasks). **HARD GATE on Linux Xeon: ≥3M EPS/core simple-fraud TCP.** Stretch ≥4M.
8. **18.6** — wire polish + VERIFICATION + SUMMARY (~400 LoC, 6 tasks). PERF GATE 6.1: full Phase 13 spec target on Linux for simple-fraud + complex-fraud + recommendation pipelines. PERF GATE 6.2: each micro-opt shows 5–10% individual uplift.

**Success criteria:** see `.planning/phases/18-redis-hand-roll/18-CONTEXT.md` (D-01..D-16 locked decisions) and per-stage plan documents (18-01..18-06). The phase-wide risks register lives at `18-risks.md` (8 risks with mitigations).

**Why this matters:** Phase 18 closes the throughput gap between Phase 13.3's apply-loop ceiling (~16k EPS/core measured) and the 3M EPS/core ship-gate target. Each stage has a clear perf gate, so regressions surface incrementally instead of at the end.

---

## Traceability (preview)

Populated in `REQUIREMENTS.md` traceability section. Summary: every REQ-ID maps to exactly one phase; Phase 1 ships zero scope-shipping REQ-IDs (infrastructure).

## Notes

- ROADMAP.md may be revised as phases complete and new-requirement discoveries force rebalancing. Revisions are committed as explicit changes.
- The previous 10-phase roadmap (commit `ad5a3ef`) was re-planned on 2026-04-22 when we pivoted from a JSON-only aggregation DSL to the v1 Python SDK API shape. Phase 1 (Foundation) work carries over unchanged.
