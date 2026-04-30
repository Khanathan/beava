# Beava v2

## What This Is

Beava is a single-binary real-time feature server built around a dataframe-style Python DSL. Users declare `@bv.event` sources, `@bv.table` snapshots, and derived features (aggregations, session windows, stateless transforms) using decorators and an expression DSL. The server ingests events over HTTP/JSON (curl-testable, LB/WAF-compatible) and a custom-framed TCP fast-path (low-latency SDK-to-server), maintains per-entity state in memory, and serves features sub-millisecond. The product is the spiritual successor to Beava v1 — same authoring experience, dramatically simpler runtime (single process, single thread, in-memory state, dual HTTP+TCP wire, processing-time semantics only — no event-time / no watermarks / no joins), and a richer operator catalogue.

## Core Value

**Feature authoring as composable Python code that ships to production unchanged.** A data scientist writes `@bv.event`, `.filter(...)`, `group_by("user").agg(...)`, `@bv.table(key=..., ttl=...)`, registers with `app.register()`, and their feature definitions run at live fraud-serving latency. Every architectural decision serves this: the Python SDK is the blessed UX, the HTTP wire is the contract, JSON is transport only, all state lives in RAM for correctness-by-construction, the server binary is a single Rust artifact a single operator can run, and semantics are Redis-shaped (processing-time only — state is a function of arrival-order events plus query time; no event-time discipline burden on users).

## Requirements

### Validated

(None yet — ship to validate)

### Active

Grouped by theme. Every entry is a hypothesis until shipped + used in production. Full enumerated REQ-IDs live in `REQUIREMENTS.md`.

**A. Python SDK — the canonical declaration surface**
- Decorators: `@bv.event`, `@bv.table(key=..., ttl=...)` in both class and function forms
- Stateless ops on Event/Table: `.filter .select .drop .rename .with_columns .map .cast .fillna`
- Expression DSL: `bv.col("x")` with `+ - * /`, `< > <= >= == !=`, `& | ~`, `.isnull()`, `.cast()`
- Aggregations via `event.group_by(keys).agg(name=bv.<op>(...), ...)` producing a Table; windowed ops use server-side `now_ms()` (processing-time)
- Session windows for activity-based grouping (`bv.session(gap_ms=..., inner=...)`); replace event-time-bucketed windows for activity grouping; v0.1+
- (Joins + `bv.union` deferred to v0.1+ alongside event-time work; not in v0 scope)
- Registration: `app.register(*descriptors)` — DAG topological sort, cycle detection, schema propagation, additive-only with version bump
- Push: `app.push(Event, dict)` async fire-and-forget; `app.push_sync(Event, dict)` returns `FeatureResult`; `app.push_many(Event, [dicts])` batched
- Table upsert: `app.push(Table, key, dict)`; `app.delete(Table, key)` tombstones
- Read: `app.get(key)` → `FeatureResult`; `app.mget([keys])`; `app.get_multi([Table1, Table2], key)`
- Direct write: `app.set(key, features)`, `app.mset({key: features, ...})`
- `app.validate(*descriptors)` → list of `ValidationError` for unit-test use

**B. Server — HTTP ingest, in-memory state, single thread**
- HTTP API: `POST /register`, `POST /push/{stream}`, `POST /push-batch/{stream}`, `POST /get`, `GET /get/{feature}/{key}`, `POST /set`, `POST /delete/{table}`, `GET /health`, `GET /ready`, `GET /metrics`
- Registration payload: JSON DAG of event/table/derivation nodes in topological order; server validates and assigns a `registry_version` (monotonic)
- Additive-only: re-posting adds-only DAGs succeeds with version bump; any removal / type-change / mutation returns 409 with a structured diff
- Single Rust process, single thread for the apply loop (plus auxiliary threads for WAL fsync and HTTP accept), in-memory state only

**C. Operator catalogue**
- **Core aggregations:** `bv.count`, `bv.sum`, `bv.avg`, `bv.min`, `bv.max`, `bv.variance`, `bv.stddev`, `bv.ratio`
- **Sketch aggregations:** `bv.count_distinct` (HLL), `bv.percentile` (DDSketch), `bv.top_k` (SpaceSaving), `bv.bloom_member`, `bv.entropy`
- **Point / ordinal aggregations:** `bv.first`, `bv.last`, `bv.first_n`, `bv.last_n`, `bv.lag`
- **Decay family:** `bv.ewma` (alias `ema`), `bv.ewvar`, `bv.ew_zscore`, `bv.decayed_sum`, `bv.decayed_count`, `bv.twa`
- **Velocity / trend:** `bv.rate_of_change`, `bv.inter_arrival_stats`, `bv.burst_count`, `bv.delta_from_prev`, `bv.trend`, `bv.trend_residual`, `bv.outlier_count`, `bv.value_change_count`
- **Recency / identity:** `bv.streak`, `bv.max_streak`, `bv.negative_streak`, `bv.time_since`, `bv.first_seen`, `bv.last_seen`, `bv.age`, `bv.has_seen`, `bv.first_seen_in_window`, `bv.time_since_last_n`
- **Bounded buffers:** `bv.histogram`, `bv.hour_of_day_histogram`, `bv.dow_hour_histogram`, `bv.seasonal_deviation`, `bv.event_type_mix`, `bv.most_recent_n`, `bv.reservoir_sample`
- **Geo:** `bv.geo_velocity`, `bv.geo_distance`, `bv.geo_spread`, `bv.distance_from_home` (Plan 19.2-06: `bv.unique_cells` + `bv.geo_entropy` removed; use `count_distinct(quadkey(lat,lon,zoom))` + `entropy(quadkey(...))` recipe instead)
- **Z-score at current event:** `bv.z_score` (uses running baseline of that entity)

**D. Durability + recovery**
- WAL file per instance, append-only; group-commit fsync every 1–5ms
- `push_sync` + async-awaited ACK wait for fsync-past-LSN
- Periodic snapshots (default 30s) of in-memory state to disk
- Recovery: load latest snapshot + replay WAL from snapshot LSN to present (RTO ≤ 30s at 10GB state on NVMe)
- Registry serialized alongside state so registrations survive restart
- Version bumps on registry changes are WAL'd

**E. Observability + operations**
- Prometheus-compatible `/metrics` with per-operator counters, per-endpoint QPS/latency histograms, WAL fsync latency
- `/health` liveness + `/ready` readiness (only flips after recovery completes)
- Structured JSON logs with optional `X-Trace-Id` propagation
- `beava fork` CLI to spawn a scoped local replica against a remote primary for experimentation (Python: `bv.fork(...)`)

**F. Performance**
- ≥ 3M events/sec sustained on modern NVMe server-class hardware (single-thread apply, 32-byte event, 5 aggregations updated per event)
- Batch `POST /get` of 100 features × 1 entity: P50 < 2ms, P99 < 10ms on warm cache
- `push_sync` P99 < 10ms including group-commit fsync

**G. Quality + devex**
- Integration tests exercise every operator via table-driven fixtures (push known events, query expected values)
- Registration DAG tests: cycles, missing deps, schema propagation, additive-only conflicts, version-bump monotonicity
- Python SDK tests hit a real beava instance over HTTP
- Quickstart: from `pip install beava` to first feature in under 5 minutes with ≤ 20 lines of Python
- `curl`-only quickstart alternative exists for language-agnostic users (JSON register + push + get)

**H. Packaging + deploy**
- Prebuilt Rust binaries for linux/amd64, linux/arm64, darwin/arm64 via GitHub Releases
- `pip install beava` ships the Python SDK
- Single Docker image published; zero-config `docker run beava/beava:v0`
- Configuration via env vars (`BEAVA_*`) + optional YAML file

### Out of Scope

Listed with reasoning to prevent re-adding.

- **Cross-entity / cross-shard features** — `co_occurrence_count`, `graph_degree`, stream-stream joins on non-matching shard keys. Single-thread single-process architecture locks this out. Different architecture problem for v2.x.
- **State exceeding single-box RAM** — no SSD overflow, no tiered storage, no cold cache. Users size their box; exceed → refuse new entities.
- **Multi-instance coordination / replication / HA in OSS** — horizontal scale belongs to commercial tier. Multi-instance via user-sharded deploys allowed; server does not coordinate.
- **Table aggregation with retraction propagation** — v0 forbids `table.group_by(...)`. v1 of beava already made this call; v2 inherits it. Retraction-aware table→table aggregations are v0.1 work.
- **Table mode: `"changelog"`** — only `"upsert"` mode in v0 (matches v1).
- **Timers / autonomous emission** — no `on_timer` callbacks, no debouncer, no session-end-by-timeout. Deferred to post-v0.
- **CEP / sequence pattern detection / state machines as operators** — not in the aggregation operator framework. Deferred.
- **Backfill + replay + branching** — the `bv.fork()` replica covers some of this use case; full branch/promote/discard semantics deferred.
- **Real-time multi-touch attribution as a built-in operator** — users can compose it from `@bv.event` + decorators in v2 but there's no blessed `bv.attribution(...)` op.
- **Protobuf / schema-registry wire** — custom framed binary is OK (see Wire format) but no Protobuf/FlatBuffers/Avro dependency in OSS.
- **Operator implementation by user Rust code** — v0 ships only the built-in catalogue. Plugin ABI deferred.
- **Multi-tenancy / per-tenant quotas** — beava is single-tenant by design.
- **All joins (event↔event, table↔table, stream↔table)** — Removed permanently from v0 architecture per `project_redis_shaped_no_event_time_ever` (locked 2026-04-30). v0.1+ may revisit, but joins of any shape are not part of Beava's core semantics. Users compose via push/get patterns.
- **`bv.union(*events)`** — Deferred with joins to v0.1+. Users multiplex client-side for v0.
- **Event-time / watermarks / PIT temporal store** — Removed permanently. State is a function of `(arrival-order events, query time)`. No `event_time_ms` on the wire, no `tolerate_delay_ms`, no `event_time_field` decorator, no `as_of=` join syntax. Late events are an undefined concept; `agg_windowed` operators index by server-side `now_ms()`. Phases 14, 14.1, 15 archived 2026-04-30 (`.planning/phases/_archived-*`).

## Context

**Origin:** Re-plan of v2 after extended design session + v1 API research. v1 lives on branches `main` and `arch/tpc-full-shard`. v1 shipped:
- Rich Python decorator DSL (`@bv.stream` + `@bv.table(key=...)`, function and class forms)
- Expression DSL (`bv.col("x") > 100`) with operator overloading
- Stateless ops chain (filter/select/drop/rename/with_columns/map/cast/fillna)
- 15 aggregation operators (count/sum/avg/min/max/variance/stddev/percentile/count_distinct/top_k/first/last/first_n/last_n/ema/lag)
- Stream-stream / stream-table / table-table joins, `bv.union`
- Explicit registration with DAG validation
- TCP binary wire protocol, `App.push`/`push_sync`/`push_many`/`get`/`mget`/`get_multi`/`set`/`mset`/`delete`/`fork`
- Fjall/RocksDB state, thread-per-core sharding

v1's ceilings (why rebuild): measured ~10K EPS/core on complex workloads due to `postcard` serialization on a 24KB per-entity state blob, `serde_json::Value` on the hot path, and O(n²) feature lookups.

**v2 inherits v1's API shape. v2 changes the runtime:**
- Single process, single thread (not TPC) for correctness-by-construction on atomic operators
- In-memory state only (no RocksDB/fjall) for elimination of serialization overhead
- Dual wire: HTTP/JSON for curl/LB/WAF/multi-language reach + custom-framed TCP fast-path for low-latency SDK access
- Additive-only registration with version bump (v1 allowed neither mutation nor version tracking)
- Expanded operator catalogue: 40+ primitives vs. v1's 15 (new: ewma/ewvar/decayed_sum/twa, velocity/trend family, recency family, bounded-buffer family, geo family, bloom/seasonal ops)
- Rename `@bv.stream` → `@bv.event` for clarity (events are immutable append-only; "stream" was ambiguous)

**Input artifacts:**
- `DESIGN-V2.md` — architectural decisions from the v2 design session (locked: single-thread, in-memory, WAL+snapshot, HTTP, additive register). Note: some subsections (§17 open-questions, §19 phase roadmap) are stale against this new API shape — they describe the JSON-DSL framing. Treat §4/§5 architecture, §15 durability, §18 non-goals, §22 decisions table as authoritative; other sections as historical context.
- `REQUIREMENTS.md` / `ROADMAP.md` — v2 re-planned against the v1 API shape (this document's sibling files)
- `main` branch `python/beava/` — the v1 Python SDK reference

**Repo / branding:** Repo codename `tally`, public product `beava`, site `beava.dev`. v1 Rust impl remains on `arch/tpc-full-shard`. v2 work on `v2/greenfield` branch.

## Constraints

- **Tech stack:** Rust server (hand-rolled mio data plane via ServerV18 — single OS thread for apply loop; tokio sidecar for admin endpoints only on a separate port); Python SDK over HTTP using `requests` or equivalent + framed-TCP fast-path
- **Architecture:** Single process, single thread for apply. In-memory state. WAL + periodic snapshot. No cross-process coordination.
- **Wire format:** Dual transport. (1) HTTP/1.1 + JSON on the default port — curl-compatible, language-agnostic. (2) Custom framed TCP on a second port — `[u32 length][u16 op][u32 request_id][payload bytes]`, same JSON payload body for v0 (MessagePack/custom encoding is v0.x territory). Python SDK chooses via URL scheme (`http://` vs `tcp://`). Full opcode table designed up front (register/ping/push/push_sync/push_many/get/mget/set/mset); handlers wired as their feature phases land. No Protobuf, no FlatBuffers.
- **Performance:** ≥ 3M events/sec single-thread apply; P99 batch-get < 10ms
- **Memory:** Linear in `entities × aggregations × bytes/agg`. Users size the box. No SSD overflow.
- **API compatibility:** Python SDK conceptually mirrors v1 shapes (class and function decorators, `.agg()`, `bv.col`) — explicit breaking changes from v1: `@bv.stream` renamed to `@bv.event`; `.join()` / `bv.union` / `as_of=` / `tolerate_delay_ms` / `event_time_field` removed (all event-time + join machinery deleted permanently). Wire is dual (HTTP/JSON + custom-framed TCP); v1's TCP framing is NOT reused (v2 uses the simpler `[len][op][content_type][payload]` frame, Redis-style strict-FIFO correlation, no `request_id`)
- **Registration:** Additive-only with monotonic version bumps; no in-place mutation of registered descriptors
- **Licensing:** Apache 2.0 OSS for v0. HA / replication / cross-region for commercial tier later.
- **Timeline:** Target v0 engineering-complete in 8–12 weeks from Phase 1 kickoff

## Key Decisions

Locked. Each is load-bearing for phase planning.

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Python SDK is the canonical authoring UX | v1 validated this shape; feature engineers live in Python; Feast/Tecton/Chronon converged on it | — Pending ship |
| Wire format is dual: HTTP/JSON + custom-framed TCP | HTTP: curl-testable, LB/WAF/CDN-compatible, serverless-friendly. TCP: low-latency SDK fast-path without HTTP header overhead. Same JSON payload body; no Protobuf | — Pending ship |
| Devex-first naming (plain English over streaming jargon) | "lateness"/"watermark" → DEAD per no-event-time pivot 2026-04-30; "idempotency" → `dedupe_key`/`dedupe_window` retained; table mode "append" → "upsert" retained; zero-config defaults (`keep_events_for`, `dedupe_window`) baked in. | — Pending ship |
| Redis-shaped semantics — no event-time, no watermarks, no joins, ever | Locked 2026-04-30. State = f(arrival-order events, query time). Simpler operational model + matches Redis-cluster scaling story + eliminates whole class of correctness bugs. See `project_redis_shaped_no_event_time_ever`. Session windows replace event-time grouping for activity aggregation. | — Locked permanently |
| TCP frame: `[u32 length][u16 op][u8 content_type][payload]` | Simpler than v1's framing. No request_id — Redis-style strict-FIFO correlation on a connection. `content_type` byte: 0x01 JSON (v0), 0x02 MessagePack (reserved for Phase 6/12 hot-path), 0xFFFF = error_response opcode. Opcode table designed up front in Phase 2.5 so later phases only fill in handlers | — Pending ship |
| beava-core stays WASM-portable (syscall-free invariant) | Every phase's core compute code (expression parser/evaluator, registry, ops, aggregations, sketches) lives in `beava-core` and has zero fs/net/syscall deps. Only `beava-server` + `beava-persistence` (WAL/snapshot) touch the outside world. Free constraint today; unlocks v0.1+ browser-WASM npm library + edge-compute WASM without a big refactor. Codified 2026-04-23 | — Pending ship (enforced Phase 4+) |
| Interactive tutorial via hosted playground (Phase 13) | v0.1+ gets a true in-browser WASM runtime. For v0, interactive tutorial = `playground.beava.dev` hosted server + JS in the docs page calling real HTTP. Users see real `registry_version` bumps, real validation errors, real feature values. ~$10-20/mo infra; zero new beava code. Fits naturally in Phase 13 docs milestone | — Pending ship Phase 13 |
| Rename `@bv.stream` → `@bv.event` | "Event" is unambiguous for append-only immutable sources; "stream" was overloaded in v1 | — Pending ship |
| Additive-only registration with monotonic `registry_version` bumps | Prevents silent breaking changes; makes "just re-run your registration" safe | — Pending ship |
| Tables upsert by primary key; deletes tombstone | Matches v1 semantics; serves enrichment use case | — Pending ship |
| Aggregations produce Tables (`event.group_by(keys).agg(...)` → Table) | Matches v1 duality; keeps the mental model consistent | — Pending ship |
| Registration is explicit (`app.register(*descriptors)`), not auto-on-push | Matches v1; preserves schema-validated-before-use invariant | — Pending ship |
| Single process, single thread (not TPC) | Correctness-by-construction for atomic operators; simplest mental model; horizontal scale via independent processes | — Pending ship |
| In-memory state only (no RocksDB/fjall) | Eliminates the serialization overhead that ceilinged v1 at 10K EPS/core | — Pending ship |
| WAL + periodic snapshots for durability | Redis RDB+AOF pattern; well-understood; bounded RTO | — Pending ship |
| Uniform processing-time bucketing, cap 64 buckets | Replay-deterministic; bucket time-source switched from `event_time_ms` → server-side `now_ms()` per 2026-04-30 pivot. DGIM exponential rejected. | — Pending ship |
| No timers, no emit pipeline, no CEP operators in v0 | Scope discipline; land the aggregation + ops surface first | — Pending ship |
| 55-operator catalogue (vs v1's 15) — preserved through no-event-time pivot via Path X (server-clock time source) | Coverage of EWMA/velocity/geo/recency/sketches/buffers that v1 users asked for | — Pending ship |
| Python SDK lands early (Phase 3) | Dogfoods the decorator DSL against Phase 2.5's dual-wire server while building primitives in later phases. SDK uses clean-room impl referencing v1 python/beava/ only for shape (no source copy). `bv.App()` with no URL auto-spawns a local beava subprocess on ephemeral ports — closes the "pip install + also install the server" gap for notebook users | — Pending ship |
| `bv.fork()` supported for scoped local experimentation | Matches v1 Phase 39 feature; high devex value | — Pending ship |
| Commercial tier (HA, replicas, cross-region) explicitly out of OSS | Clean OSS/commercial product-tier split for monetization | — Pending ship |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition:**
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone:**
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-22 after re-plan to adopt v1 Python SDK API shape*
