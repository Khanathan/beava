# Beava v2

## What This Is

Beava is a single-binary real-time feature server for fraud, ad-tech, and behavioral analytics. Push events in over HTTP, beava tracks per-entity features (counters, velocities, distances, rates, distributions) updated atomically on every event, and your application queries them via HTTP to power live scoring rules. Think "Redis for stateful streaming features," with 40+ purpose-built aggregation primitives instead of do-it-yourself Lua scripts.

## Core Value

**Declare a feature, push events, query it — in under 10 minutes, with curl alone.** Every architectural and product choice serves this: HTTP-first API, JSON-declarative feature registration, zero SDK requirement, single binary, in-memory state, batch lookup for sub-millisecond fraud/feature-serving decisions.

## Requirements

### Validated

(None yet — ship to validate)

### Active

Grouped by theme. Every entry is a hypothesis until shipped + used in production.

**A. Core server + API**
- [ ] REQ-API-01: HTTP endpoints `POST /register`, `POST /push/{stream}`, `POST /get`, `GET /get/{feature}/{key}` exposed; zero wire-protocol-versioning beyond JSON/HTTP
- [ ] REQ-API-02: Register payload accepts stream declaration + list of feature declarations in one call, idempotent on re-register
- [ ] REQ-API-03: Push endpoint accepts typed event, updates all affected features atomically, returns `{ack_lsn, idempotent_replay}`; no emits, no outcomes in v0 beyond ACK
- [ ] REQ-API-04: Batch get accepts `{keys: [...], features: [...]}`, returns `{key: {feature: value}}` map; per-request cap `keys × features ≤ 10000`
- [ ] REQ-API-05: Single get returns `{value}` (or `{value, meta}` for structured returns)
- [ ] REQ-API-06: Server runs as one process, one thread for event processing; auxiliary I/O threads for WAL fsync and HTTP only
- [ ] REQ-API-07: Single static binary under 200MB; zero external runtime dependencies

**B. Feature primitive catalogue (40+ types, all per-entity)**
- [ ] REQ-PRIM-CORE: count, sum, avg, min, max, stddev, variance, z_score, ratio, streak (+ max_streak, negative_streak), time_since, first_seen, last_seen, age, has_seen
- [ ] REQ-PRIM-DECAY: ewma, ewvar, ew_zscore, decayed_sum, decayed_count, twa
- [ ] REQ-PRIM-VELOCITY: rate_of_change, inter_arrival_stats, burst_count, delta_from_prev, trend, trend_residual, outlier_count, value_change_count
- [ ] REQ-PRIM-BUFFERS: histogram, hour_of_day_histogram, dow_hour_histogram, seasonal_deviation, event_type_mix, most_recent_n, reservoir_sample, time_since_last_n
- [ ] REQ-PRIM-GEO: geo_velocity, geo_distance, geo_spread, unique_cells, geo_entropy, distance_from_home
- [ ] REQ-PRIM-SKETCH: distinct (HLL), bloom_member, first_seen_in_window (bloom + ts), quantile (DDSketch), top_k (SpaceSaving), entropy
- [ ] REQ-PRIM-FILTER: every primitive accepts optional `where` clause using JSON DSL (`{field: {op: value}}` with and/or composition)
- [ ] REQ-PRIM-WINDOW: uniform event-time bucketing with cap 64 buckets per window; windowless "lifetime" mode; per-feature opt-in override

**C. Stream-level features**
- [ ] REQ-STREAM-01: Stream declaration with `shard_key`, typed schema (str, f64, i64, bool), mandatory `event_time` field
- [ ] REQ-STREAM-02: Stream-level `idempotency_key` + `idempotency_ttl_ms` decoration; duplicate request_id in TTL returns cached response byte-identical to original, no state mutation
- [ ] REQ-STREAM-03: Schema evolution via `schema_version: u8` in row header; server stores last N (default 8) schemas; on-read migration for historical rows

**D. Durability + recovery**
- [ ] REQ-DUR-01: WAL file per instance, append-only; group-commit fsync every 1-5ms or 1MB
- [ ] REQ-DUR-02: Write-through semantics: client push returns ACK only after WAL fsync past event's LSN
- [ ] REQ-DUR-03: Periodic snapshots (every 30s default) of in-memory state to disk; configurable cadence
- [ ] REQ-DUR-04: Recovery on restart: load latest snapshot + replay WAL from snapshot LSN; target RTO under 30s for 10GB state on NVMe
- [ ] REQ-DUR-05: Snapshot + WAL rotation: old snapshots pruned after next successful snapshot; WAL truncated past snapshot LSN

**E. Observability + operations**
- [ ] REQ-OBS-01: Prometheus-compatible `/metrics` endpoint with per-primitive counters, WAL/fsync latency histograms, per-endpoint QPS/latency
- [ ] REQ-OBS-02: `/health` liveness + `/ready` readiness endpoints; ready reflects recovery-complete state
- [ ] REQ-OBS-03: Structured JSON logs with levels; trace_id propagated from HTTP header

**F. Performance targets**
- [ ] REQ-PERF-01: Single-thread apply loop sustains ≥ 3M events/sec on modern NVMe server-class hardware (32-byte events, 5 operators updated per event)
- [ ] REQ-PERF-02: Batch get of 100 features × 1 key returns P50 < 2ms, P99 < 10ms on warm cache
- [ ] REQ-PERF-03: WAL group-commit adds P50 < 2ms to push ACK latency at default config

**G. Quality + devex**
- [ ] REQ-QUALITY-01: Full integration test coverage over every primitive via table-driven fixtures: push known events, query expected values
- [ ] REQ-QUALITY-02: Idempotency + crash-recovery tests: kill-and-restart verifies state restoration from snapshot+WAL
- [ ] REQ-QUALITY-03: curl-only quickstart demonstrated in `docs/quickstart.md` (≤ 10 commands, under 5 minutes to working feature server)
- [ ] REQ-QUALITY-04: Python SDK (sync + fire-and-forget only, HTTP-backed, no persistent connections, no callbacks) starter package

**H. Packaging + deploy**
- [ ] REQ-PKG-01: Prebuilt binaries for linux/amd64, linux/arm64, darwin/arm64 via GitHub Releases
- [ ] REQ-PKG-02: Single docker image published; zero-config start via `docker run beava/beava:v0`
- [ ] REQ-PKG-03: Configuration via env vars + one optional YAML file; no external config store required

### Out of Scope

Explicitly deferred or excluded. Includes reasoning to prevent re-adding.

- **Cross-entity / cross-shard features** — co-occurrence, graph degree, cross-entity joins — locked out of v0 by the single-process single-thread model. Would require a different architecture. Deferred indefinitely.
- **State exceeding a single box's RAM** — no SSD overflow, no tiered storage, no cold cache. Users size their box to their workload. If state exceeds RAM, server refuses new entities. Trades graceful degradation for architectural simplicity.
- **Multi-process / multi-instance coordination** — no built-in router, no replication, no cross-instance WAL shipping. Users run multiple independent beavas + shard at their application layer if horizontal scaling is needed. HA belongs in commercial tier.
- **Event emission / downstream pipelines** — no `on_emit` subscriptions, no operator-to-operator event routing, no webhook delivery. Every outcome is read from the `/get` endpoint. Simplifies the API to push + query only.
- **Timers / autonomous firing** — no operator callbacks fired without an incoming event. Debouncers, auction-close emissions, time-based session termination all deferred. Session-like semantics (close on next event after gap) are supported; true quiet-period detection is not.
- **State machines with transitions / CEP sequences / attribution-emitted events** — considered as flagship use cases, dropped for v0 because they require emit/timer machinery. Defer to v1.
- **Backfill + replay + branching** — forking state, replaying historical events against new operator definitions, promoting/discarding branches. Valuable but non-trivial; defer to v1.
- **Sketch-family advanced features** — rolling correlation, HLL Jaccard over self-snapshots, VarOpt weighted sample. Niche; v1 if demand.
- **Custom user-defined operators** — v0 ships only the built-in 40 primitives. Custom Rust operators require recompile; no runtime plugin system. Defer plugin model to v1+.
- **Multi-tenant isolation** — one tenant per deployment. No per-tenant quotas, rate limits, or resource isolation. Tenancy is a higher-layer concern.
- **SQL / declarative query language** — register DSL is JSON only. No ad-hoc SQL over state. v2+ if demand.

## Context

**Origin:** Full architectural pivot from the v1 implementation on branch `arch/tpc-full-shard`. v1 used a thread-per-core sharded architecture with fjall/RocksDB state and a TCP binary wire protocol. Measured on fraud-like complex cascade workloads, v1 ceilinged at ~10K events/sec/core due to serialization overhead (`postcard` on a 24KB per-entity state blob), `serde_json::Value` on the hot path, and O(n²) feature lookups. See `arch/tpc-full-shard` branch for the v1 codebase.

**Input artifact:** `DESIGN-V2.md` at the repo root captures the complete v2 architecture decisions, 40-primitive catalogue, HTTP API shape, single-thread rationale, and in-memory + WAL + snapshot durability model. It was written and refined through an extended session that:
- Researched exponential bucketing (DGIM, EWMA, forward decay) and rejected DGIM for replay determinism reasons
- Researched Redis pain points to pick flagship use cases
- Walked through each feature primitive and confirmed single-thread implementability
- Locked the full 40-primitive catalogue as v0 scope

**Market:** Beava (product) is the public name; `tally` is the repo codename. Public site is `beava.dev`. Positioning: feature-serving workloads that push Redis beyond its sweet spot — velocity/geo/streak/distribution features that teams currently hand-build with Lua scripts, Flink jobs, or ad-hoc batch pipelines. Flagship buyer verticals: fraud detection, ad-tech velocity capping, real-time personalization, behavioral biometrics.

**Prior research assets preserved:**
- `realtime-pain-points.md` — first-person notes on what Redis + Flink get wrong for feature serving
- `docs/http-api.md`, `docs/http-api-examples.sh` — HTTP API shape reference from v1 (close to v2 shape; re-verify during implementation)
- `BLOG-PART*-DRAFT.md`, `BLOG-SERIES-PLAN.md` — launch messaging drafts
- `LAUNCH.md`, `LAUNCH-COPY-CONVERSATION.md` — launch narrative
- `SENDO-DEMO*.md`, `SENDO-FARM-DEMO-PLAN.md` — concrete demo scenarios

**Why v2 diverges from v1 architecturally:**
- Single thread per process (not thread-per-core sharding) — correctness-by-construction for atomic sequential operators (rate limiters, ticketing, idempotency with cached results); no locks, no coordination, no MESI cache-line ping-pong. Horizontal scale = add independent processes.
- In-memory state (not RocksDB-backed hot cache) — eliminates serialization overhead entirely. Memory is the hard constraint.
- HTTP-first (not TCP binary protocol) — curl-testable, works through any LB/proxy/WAF, no SDK required for integration.
- JSON-declarative feature registration (not Python/Rust SDK trait implementation) — users declare what to track in JSON; server implements via built-in primitives. Zero operator code in user space.

## Constraints

- **Tech stack**: Rust server (ownership + perf), HTTP API (axum or actix), Python SDK (sync + fire-and-forget only, HTTP-backed). No external storage dependencies (RocksDB, fjall removed).
- **Architecture**: Single process, single thread for event processing. In-memory state. WAL + periodic snapshot for durability. No cross-process coordination.
- **Performance**: ≥3M events/sec/core sustained on typical fraud-shape workloads; P99 batch-get < 10ms.
- **Memory**: No SSD overflow. Users must size their box. Budget: ~7KB per entity for a rich 30-feature pack → ~700GB for 100M entities.
- **Compatibility**: HTTP/1.1 minimum; JSON request/response only in v0. No Protobuf, no TCP binary in OSS.
- **Licensing**: Apache 2.0 OSS for v0. Commercial-tier (HA, replicas, cross-region) is explicitly out of v0 scope.
- **Timeline**: v0 target is weeks, not months — aiming for engineering-complete in ~6-10 weeks from Phase 1 kickoff.

## Key Decisions

Decisions locked during the v2 design session (reference: `DESIGN-V2.md` §17 and surrounding discussion). Each is load-bearing for subsequent phase planning.

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Single thread per process (not TPC) | Correctness-by-construction for atomic operators; simplest mental model; horizontal scale via independent processes | — Pending ship |
| In-memory state only (no RocksDB/fjall) | Eliminates serialization overhead that ceilinged v1 at 10K EPS/core; 10-100× headroom | — Pending ship |
| WAL + periodic snapshots for durability | Redis RDB+AOF pattern; well-understood; bounded RTO; no tiered storage complexity | — Pending ship |
| HTTP-first API (not TCP binary) | curl-testable; zero SDK requirement; works through LBs/proxies/WAFs; matches modern serverless patterns | — Pending ship |
| JSON-declarative feature registration | Users declare primitives from fixed catalogue; zero operator-trait user code | — Pending ship |
| No emits / no timers / no backfill in v0 | Drops 60% of architectural machinery; enables ship in weeks not months; defers to v1 based on real demand | — Pending ship |
| No cross-entity / cross-shard features | Locked by single-process model; graph_degree / co_occurrence would require different architecture | — Pending ship |
| Uniform event-time buckets, cap 64 | Replay-deterministic; DGIM exponential rejected for breaking Min/Max/Percentile/HLL + replay invariance | — Pending ship |
| 40 primitive feature types in v0 | Covers ~80% of fraud / ad-tech / analytics needs from the research; concrete shippable catalogue | — Pending ship |
| Python SDK is thin HTTP wrapper | Sync + fire-and-forget only; no callbacks, no persistent connections; no SDK required at all (curl works) | — Pending ship |
| Commercial tier (HA, replicas) explicitly out of v0 | Keeps OSS focused; creates natural product-tier split for monetization | — Pending ship |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-04-22 after initialization from DESIGN-V2.md*
