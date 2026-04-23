# State: Beava v2 — v0 OSS Launch

**Project reference:** `.planning/PROJECT.md` (rewritten 2026-04-22 to adopt v1 Python SDK API)
**Roadmap:** `.planning/ROADMAP.md` (13 phases; Phase 1 complete)
**Requirements:** `.planning/REQUIREMENTS.md` (~145 REQ-IDs across 20 categories; traceability table pending roadmapper re-run)
**Milestone:** v0 (first public OSS cut on beava.dev)
**Created:** 2026-04-22
**Revised:** 2026-04-22 (re-plan to v1 Python SDK API shape)

## Core Value

Feature authoring as composable Python code that ships to production unchanged. Users write `@bv.event` / `@bv.table(key=...)` / `bv.col(...)` / `.filter().group_by().agg()` / `.join()` / `bv.union(...)` / `app.register(...)` / `app.push(...)` / `app.get(...)`, deploy unchanged. The product inherits the v1 SDK shape (`main` branch `python/beava/`) on a new runtime (single-thread, in-memory, HTTP wire, 40+ operator catalogue).

## Current Focus

**Phase 2: Sources + registry + version bumps** — `POST /register` accepts a JSON DAG of event/table/derivation nodes; validates; persists in-memory; assigns monotonic `registry_version`. Additive-only — removals/changes return 409 with structured diff. No aggregations execute yet (those land in Phase 5).

## Current Position

- **Milestone:** v0
- **Phase:** 2 of 13 (Sources + registry + version bumps)
- **Plan:** Phase 1 complete — 5/5 plans executed; Phase 2 unplanned
- **Status:** Phase 1 complete; re-plan to v1 API shape committed; ready for Phase 2 planning
- **Progress:** █▱▱▱▱▱▱▱▱▱▱▱▱ 1/13 phases

## Performance Metrics

Measured at v0 ship (Phase 13 is the final gate; Phase 5 establishes baseline):

- **Apply loop throughput target:** ≥ 3M EPS single-thread (32-byte events × 5 aggregations, server-truth)
- **Batch-get latency target:** P50 < 2ms, P99 < 10ms (100 features × 1 key, warm cache)
- **`push_sync` latency target:** P99 < 10ms including group-commit fsync
- **WAL group-commit overhead:** P50 < 2ms added to push ACK
- **Recovery RTO:** < 30s for 10GB state on NVMe
- **Binary size:** ≤ 200MB stripped
- **Operator catalogue size:** 40+ in v0

Not yet measured. Perf harness introduced in Phase 5; hit-gate in Phase 13.

## Accumulated Context

### Architectural decisions (locked — from PROJECT.md)

- Python SDK is the canonical authoring UX; HTTP/JSON is the wire (no TCP binary in OSS)
- `@bv.event` (immutable append-only, was v1's `@bv.stream`) and `@bv.table(key=..., ttl=...)` (upsertable, with tombstone delete)
- Aggregations via `Event.group_by(keys).agg(name=bv.<op>(...), ...)` produce Tables
- Stateless ops chain: `.filter .select .drop .rename .with_columns .map .cast .fillna`
- Expression DSL: `bv.col("x")` with arithmetic, comparison, `& | ~`, `.isnull()`, `.cast()`
- Joins: event↔event windowed, event↔table enrichment, table↔table key-matched
- `bv.union(*events)` with schema-identity enforcement
- `app.register(*descriptors)` — DAG topological sort, cycle detection, schema propagation, additive-only with `registry_version` bump
- Single Rust process, single apply-loop thread (auxiliary threads only for WAL fsync, HTTP accept, snapshot writer)
- In-memory state only; no RocksDB, no fjall, no SSD tiering
- WAL file with 1–5ms group-commit fsync; periodic snapshot (default 30s)
- Uniform event-time bucketing, cap 64 buckets per windowed operator
- Schema evolution: additive-only registry changes with monotonic version bumps
- `bv.fork(...)` scoped local replica supported (v1 Phase 39 inheritance)
- Commercial tier (HA, replicas, cross-region) explicitly out of v0 OSS

### Operator catalogue (v0 scope — 40+ ops)

- Core (8): count, sum, avg, min, max, variance, stddev, ratio
- Sketch (5): count_distinct (HLL), percentile (DDSketch), top_k (SpaceSaving), bloom_member, entropy
- Point/ordinal (11): first, last, first_n, last_n, lag, first_seen, last_seen, age, has_seen, time_since, time_since_last_n
- Recency (4): streak, max_streak, negative_streak, first_seen_in_window
- Decay (7): ewma (alias ema), ewvar, ew_zscore, decayed_sum, decayed_count, twa
- Velocity/trend (8): rate_of_change, inter_arrival_stats, burst_count, delta_from_prev, trend, trend_residual, outlier_count, value_change_count
- Bounded-buffer (7): histogram, hour_of_day_histogram, dow_hour_histogram, seasonal_deviation, event_type_mix, most_recent_n, reservoir_sample
- Geo (6): geo_velocity, geo_distance, geo_spread, unique_cells, geo_entropy, distance_from_home
- Entity z-score (1): z_score

### Deferred / out of scope (v1+)

- Cross-entity / cross-shard operators
- Event emission / timers / CEP / state machines as operators
- Backfill + replay + branching (beyond what `bv.fork` covers)
- Table aggregation with retraction propagation (v0.1)
- Custom user-defined operators at runtime (plugin ABI future)
- SQL query language
- Multi-tenant isolation
- Multi-instance coordination / HA (commercial tier)
- TCP binary wire protocol in OSS
- Partial-key joins; right/full/outer joins
- `bv.union` with implicit schema reconciliation

### Decisions from Phase 1 (Foundation, complete)

- `axum` as HTTP framework; `tokio::current_thread` runtime
- Mutex-based `EnvGuard` to serialize env-var-touching tests (process-global env isn't thread-safe)
- Manual `Debug` impl for `Server` (`TcpListener` lacks `Debug`)
- `cli_smoke` tests use spawn+SIGTERM because binary starts a real HTTP server
- `foundation_smoke` uses `required-features = ["testing"]` in Cargo.toml test stanza (cfg(test) does NOT propagate to integration tests)
- `libc` dev-dep for `kill(pid, SIGTERM)` in subprocess smoke tests

### Active todos

- [x] Roadmap drafted (yolo auto-approved) — superseded by re-plan
- [x] Plan Phase 1 (5 plans)
- [x] Execute Phase 1 (36 tests green, foundation_smoke 2/2, acceptance gate passed)
- [x] Re-plan v2 to adopt v1 Python SDK API shape (2026-04-22)
- [ ] Plan Phase 2 (Sources + registry + version bumps) — NEXT
- [ ] Execute Phases 2 through 13

### Blockers

None.

### Open questions / follow-ups

- DESIGN-V2.md contains older content that predates the v1-API-shape pivot (§11 JSON aggregation DSL, §17 open-questions, §19 phase roadmap all stale). PROJECT.md + this STATE.md + ROADMAP.md are authoritative. DESIGN-V2.md can be refreshed in a later pass or left as historical context; not a blocker.
- PERF gates rely on fraud-shape bench harness (Phase 5) and full-target verification (Phase 13). If developer hardware can't sustain 3M EPS/core, Phase 13 verification may need a dedicated benchmark machine.

## Session Continuity

Last session: 2026-04-22 — Re-planned v2 from JSON aggregation DSL to v1 Python SDK API shape after deep research on `main` branch's `python/beava/` surface. Committed new PROJECT.md, REQUIREMENTS.md, ROADMAP.md. Deleted stale Phase 2 CONTEXT.md (was written against the JSON DSL). Phase 1 work intact.

Next session should:

1. Read `.planning/PROJECT.md`, `.planning/ROADMAP.md`, this file, and `.planning/REQUIREMENTS.md`
2. Confirm Phase 2 (Sources + registry + version bumps) is the current focus
3. Run `/gsd-plan-phase 2` to decompose Phase 2 into plans — or resume `/gsd-autonomous --from 2` for the full pipeline
4. Note: Phase 3 (Python SDK skeleton) depends on Phase 2, so `register` JSON schema decisions made in Phase 2 are load-bearing for the SDK compiler

### Phase 2 attach points (Rust)

- HTTP: `crates/beava-server/src/http.rs` — add `.merge(register_router())` in `router()`
- Registry: new `crates/beava-core/src/registry.rs` with `Registry { Arc<RwLock<RegistryInner>> }`, `RegistryInner { version: u64, events: BTreeMap<..>, tables: BTreeMap<..>, derivations: BTreeMap<..> }`
- Schema types: new `crates/beava-core/src/schema.rs` with `FieldType`, `EventSchema`, `TableSchema`
- Diff engine: new `crates/beava-core/src/registry_diff.rs` with `compute_diff(current, submitted) -> RegistryDiff { added, removed, changed }`
- Integration tests: `TestServer::spawn()` from Phase 1; add helper `TestServer::post_json(path, body)`

---
*State initialized: 2026-04-22 after roadmap creation.*
*Phase 1 complete: 2026-04-22 — workspace, HTTP, config, logging, test harness.*
*Re-plan committed: 2026-04-22 — v2 adopts v1 Python SDK API shape.*
