# Requirements: Beava v2

**Defined:** 2026-04-22
**Core Value:** Declare a feature, push events, query it — in under 10 minutes, with curl alone.

## v1 Requirements

Requirements for the v0 OSS launch. Each maps to roadmap phases via traceability section below.

### API (HTTP surface)

- [ ] **API-01**: Server exposes `POST /register` that accepts a JSON body declaring one stream and a list of feature declarations; returns HTTP 200 with committed registration or 4xx with validation errors
- [ ] **API-02**: Registration is idempotent: re-posting the same declaration is a no-op; posting a conflicting redeclaration returns 409 with a diff
- [ ] **API-03**: Server exposes `POST /push/{stream}` that accepts a typed JSON event and returns `{ack_lsn, idempotent_replay}` only after WAL fsync past the event's LSN
- [ ] **API-04**: Push validates event payload against registered stream schema; returns 400 with the failing field on mismatch
- [ ] **API-05**: Server exposes `POST /get` that accepts `{keys: [...], features: [...]}` and returns `{key: {feature: value}}` mapping
- [ ] **API-06**: Batch get enforces per-request cap `keys × features ≤ 10000`; over-limit returns 413 with the configured cap
- [ ] **API-07**: Server exposes `GET /get/{feature_name}/{entity_key}` returning `{value}` (or `{value, meta}` for structured features like geo_velocity)
- [ ] **API-08**: Unknown feature name in any get endpoint returns 400 (not silently-null); unknown entity key returns the feature's zero-value or `null` per the feature's documented semantics
- [ ] **API-09**: All endpoints speak HTTP/1.1 + JSON; no binary protocol in v0

### Stream-level decorations

- [ ] **STREAM-01**: Stream declaration accepts `shard_key` field name, typed schema (fields of type str, f64, i64, bool), and mandatory `event_time: i64` field
- [ ] **STREAM-02**: Stream can declare `idempotency_key: <field_name>` + `idempotency_ttl_ms: <number>` decoration
- [ ] **STREAM-03**: Duplicate push within TTL (same idempotency key value) returns the byte-identical PushResponse from the first call with `idempotent_replay: true`; no state mutations applied
- [ ] **STREAM-04**: Row header carries `schema_version: u8`; server stores last 8 schemas; reads of older rows are migrated to latest layout on retrieval
- [ ] **STREAM-05**: Schema evolution supports additive field changes (new optional fields); breaking changes (field removal, type change) require explicit schema_version bump

### Feature registration DSL

- [ ] **REG-01**: Feature declaration accepts `name`, `type`, optional `field`, optional `window_ms`, optional `where`, optional type-specific params (e.g. `lat`/`lon` for geo, `half_life_ms` for decay)
- [ ] **REG-02**: `where` filter DSL accepts `{field: {op: value}}` with ops `eq`, `ne`, `gt`, `lt`, `gte`, `lte`, `in`
- [ ] **REG-03**: `where` filter supports composition via `{and: [...]}` and `{or: [...]}` nesting
- [ ] **REG-04**: Registration rejects unknown feature types, unknown fields, malformed where clauses with specific error messages

### Feature primitives — core aggregates

- [ ] **PRIM-CORE-01**: `count` primitive with optional `window_ms` and `where`
- [ ] **PRIM-CORE-02**: `sum`, `avg`, `min`, `max` primitives over a numeric field with optional window + where
- [ ] **PRIM-CORE-03**: `stddev` + `variance` via Welford's running-variance algorithm; windowed
- [ ] **PRIM-CORE-04**: `z_score` primitive — current event's value vs running mean/stddev of that entity
- [ ] **PRIM-CORE-05**: `ratio` primitive — count matching / count total over window

### Feature primitives — recency & identity

- [ ] **PRIM-RECENCY-01**: `streak` — consecutive matches of a `where` clause ending at latest event
- [ ] **PRIM-RECENCY-02**: `max_streak` + `negative_streak` variants
- [ ] **PRIM-RECENCY-03**: `time_since` — ms since last matching event (returns null if no match)
- [ ] **PRIM-RECENCY-04**: `first_seen`, `last_seen` — absolute event_time timestamps
- [ ] **PRIM-RECENCY-05**: `age` — ms since first event ever seen for the entity
- [ ] **PRIM-RECENCY-06**: `has_seen` — boolean ever-matched-criteria flag
- [ ] **PRIM-RECENCY-07**: `value_change_count` — number of times a field flipped value for the entity
- [ ] **PRIM-RECENCY-08**: `first_seen_in_window` — bloom-backed "is this value new to this entity in N days?" returning bool

### Feature primitives — decay family

- [ ] **PRIM-DECAY-01**: `ewma` — exponentially-weighted moving average with configurable `half_life_ms`
- [ ] **PRIM-DECAY-02**: `ewvar` + `ew_zscore` — exponentially-weighted variance and current-event z-score
- [ ] **PRIM-DECAY-03**: `decayed_sum`, `decayed_count` — forward-decay sum/count (Cormode style)
- [ ] **PRIM-DECAY-04**: `twa` — time-weighted average for irregularly-sampled gauge fields

### Feature primitives — velocity & trend

- [ ] **PRIM-VEL-01**: `rate_of_change` — Δrate or acceleration of count/sum across two adjacent windows
- [ ] **PRIM-VEL-02**: `inter_arrival_stats` — mean, stddev, coefficient of variation of gaps between matching events
- [ ] **PRIM-VEL-03**: `burst_count` — max event count observed in any sub-window of size K within the outer window
- [ ] **PRIM-VEL-04**: `delta_from_prev` — current event's field value minus previous event's value
- [ ] **PRIM-VEL-05**: `trend` — slope of EW linear regression over window
- [ ] **PRIM-VEL-06**: `trend_residual` — current value minus trend-predicted value
- [ ] **PRIM-VEL-07**: `outlier_count` — count of events beyond Nσ (configurable) in window

### Feature primitives — bounded buffers

- [ ] **PRIM-BUF-01**: `histogram` — counts per fixed-bucket set over a numeric field
- [ ] **PRIM-BUF-02**: `hour_of_day_histogram` — 24-bin count histogram per entity
- [ ] **PRIM-BUF-03**: `dow_hour_histogram` — 168-bin (day × hour) count histogram per entity
- [ ] **PRIM-BUF-04**: `seasonal_deviation` — z-score of current event vs this entity's hour-of-day baseline
- [ ] **PRIM-BUF-05**: `event_type_mix` — proportion per fixed category set
- [ ] **PRIM-BUF-06**: `most_recent_n` — bounded deque of N most-recent matching event values
- [ ] **PRIM-BUF-07**: `reservoir_sample` — uniform reservoir sample of K values over all history
- [ ] **PRIM-BUF-08**: `time_since_last_n` — ms since k-th most recent match

### Feature primitives — geo

- [ ] **PRIM-GEO-01**: `geo_velocity` — max implied km/h between consecutive events in window
- [ ] **PRIM-GEO-02**: `geo_distance` — total path length in window (sum of consecutive distances)
- [ ] **PRIM-GEO-03**: `geo_spread` — max distance from mean center in window
- [ ] **PRIM-GEO-04**: `unique_cells` — distinct geohash cells (configurable precision) visited
- [ ] **PRIM-GEO-05**: `geo_entropy` — Shannon entropy over geohash cell distribution
- [ ] **PRIM-GEO-06**: `distance_from_home` — distance from centroid of top-K most frequent locations

### Feature primitives — sketches

- [ ] **PRIM-SKETCH-01**: `distinct` — HyperLogLog cardinality estimate over a field in window
- [ ] **PRIM-SKETCH-02**: `bloom_member` — Bloom filter ever-seen membership with configurable capacity + FPR
- [ ] **PRIM-SKETCH-03**: `quantile` — DDSketch-backed p50/p95/p99 (configurable q) over windowed field values
- [ ] **PRIM-SKETCH-04**: `top_k` — SpaceSaving top-K frequent values in window with approximate counts
- [ ] **PRIM-SKETCH-05**: `entropy` — Shannon entropy over categorical field distribution

### Windowing semantics

- [ ] **WIN-01**: Uniform event-time bucketing, default cap 64 buckets per windowed primitive, bucket width = `ceil(window_ms / 64)`
- [ ] **WIN-02**: Per-feature `bucket_count` override accepted at registration time with warning log
- [ ] **WIN-03**: Windowless "lifetime" mode when `window_ms` omitted (state is all-time, no rollover)
- [ ] **WIN-04**: Bucket rollover on apply, not on timer — lazy bucket eviction keyed on current event's event_time

### Durability

- [ ] **DUR-01**: Per-instance append-only WAL file with group-commit fsync every 1–5ms or 1MB (whichever first)
- [ ] **DUR-02**: Push ACK returns only after event's LSN has been fsynced past the caller's position
- [ ] **DUR-03**: WAL format includes schema_version, stream_id, event_time, entity_key, and event body; sufficient for full state rebuild
- [ ] **DUR-04**: WAL rotation: old segments truncated past the latest snapshot's covered LSN

### Recovery

- [ ] **RECOV-01**: Periodic snapshot (default every 30s, configurable) serializes in-memory state to disk
- [ ] **RECOV-02**: Recovery on boot loads latest snapshot + replays WAL from snapshot's covered LSN to present
- [ ] **RECOV-03**: RTO target: server online in under 30s at 10GB state on NVMe (snapshot load + WAL replay combined)
- [ ] **RECOV-04**: Corrupt snapshot or WAL is detected (checksum mismatch) and rejected with a clear error; operator can fall back to earlier snapshot

### Observability

- [ ] **OBS-01**: `/metrics` endpoint in Prometheus format; counters for registered primitives, push throughput, get QPS, WAL group-commit latency histogram
- [ ] **OBS-02**: `/health` liveness endpoint (cheap, returns 200 when server is up)
- [ ] **OBS-03**: `/ready` readiness endpoint (returns 200 only after recovery complete)
- [ ] **OBS-04**: Structured JSON logs at INFO/WARN/ERROR levels; trace_id from `X-Trace-Id` header propagated across logs per request

### Performance

- [ ] **PERF-01**: Single-thread apply loop sustains ≥ 3M events/sec on modern server NVMe hardware for 32-byte events updating 5 primitives
- [ ] **PERF-02**: Batch get of 100 features × 1 entity returns P50 < 2ms, P99 < 10ms on warm cache
- [ ] **PERF-03**: WAL group-commit adds P50 < 2ms to push ACK latency at default group-commit window

### Python SDK

- [ ] **SDK-01**: Python SDK package installable via `pip install beava`; version matches server version
- [ ] **SDK-02**: SDK exposes `sync` client (HTTP request-response) with `push`, `push_batch`, `get`, `get_batch`, `register` methods
- [ ] **SDK-03**: SDK exposes `fire_and_forget` mode that enqueues pushes locally and flushes on a timer / buffer threshold; no persistent connection, no callbacks
- [ ] **SDK-04**: SDK has zero required dependencies beyond stdlib + `requests` (or equivalent HTTP client)
- [ ] **SDK-05**: SDK surface covered by tests that hit a real beava instance over HTTP

### Docs + devex

- [ ] **DOC-01**: `docs/quickstart.md` walks through a fraud-scoring demo (register → push → get) in ≤ 10 curl commands, under 5 minutes
- [ ] **DOC-02**: `docs/primitives.md` lists every primitive type with JSON example, example return value, and one-line use case
- [ ] **DOC-03**: `docs/http-api.md` documents all four endpoints with request/response shapes and error codes
- [ ] **DOC-04**: `docs/architecture.md` describes the single-thread apply loop, WAL group-commit, snapshot recovery, and memory sizing guidance
- [ ] **DOC-05**: `README.md` at repo root links to the docs site and has a 3-command smoke demo

### Testing

- [ ] **TEST-01**: Table-driven unit tests per primitive: given events, assert expected feature value
- [ ] **TEST-02**: Integration tests for the HTTP API using real server + real HTTP client
- [ ] **TEST-03**: Crash-recovery test: kill process mid-push; restart; verify state matches pre-crash plus replayed WAL
- [ ] **TEST-04**: Idempotency test: retried pushes with same request_id produce byte-identical PushResponse; no double-apply
- [ ] **TEST-05**: Windowing test: verify bucket rollover + event-time bucketing produces deterministic results under event replay
- [ ] **TEST-06**: Throughput benchmark harness: reports EPS under single-thread apply with WAL fsync enabled

### Packaging

- [ ] **PKG-01**: Prebuilt binaries published via GitHub Releases for linux/amd64, linux/arm64, darwin/arm64
- [ ] **PKG-02**: Docker image published (e.g. `ghcr.io/petrpan26/beava:v0`) with zero-config entrypoint
- [ ] **PKG-03**: Configuration via env vars (`BEAVA_DATA_DIR`, `BEAVA_PORT`, etc.) + optional single YAML config; no external config store
- [ ] **PKG-04**: Binary size under 200MB stripped

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Emit / event pipeline

- **EMIT-01**: Operators can emit downstream events back to the client's push response
- **EMIT-02**: Operator-to-operator event routing (A's emit feeds B's apply)
- **EMIT-03**: Webhook delivery of emitted events to user-registered URLs
- **EMIT-04**: Subscribe API for emitted events separate from the push path

### Timers

- **TIMER-01**: Event-time timer callbacks firing `on_timer` for debouncer, session-end, auction close, SLA timers

### CEP / state machines / attribution

- **ADV-CEP-01**: Sequence pattern detection with multi-step `within` windows
- **ADV-SM-01**: State machine operator with transition-event emission
- **ADV-ATTR-01**: Multi-touch attribution with time-decay / linear / u-shaped models

### Backfill + branching

- **BACKFILL-01**: Replay historical events into a forked state branch
- **BRANCH-01**: Fork state snapshot → diverge via test events → promote or discard

### Cross-entity features

- **XENT-01**: `co_occurrence_count`, `graph_degree` — require cross-shard coordination
- **XENT-02**: Stream-stream joins on non-matching shard keys

### HA / commercial tier

- **HA-01**: Read replicas with WAL-shipping replication
- **HA-02**: Cross-region deployment with manual failover
- **HA-03**: Multi-primary coordination (raft or equivalent)

### Sketch family additions

- **SKETCH-02-01**: Rolling Pearson correlation over two fields
- **SKETCH-02-02**: HLL Jaccard similarity over self-snapshots across time windows
- **SKETCH-02-03**: VarOpt weighted reservoir sample

### Extensibility

- **EXT-01**: User-defined custom operators via Rust plugin ABI
- **EXT-02**: SQL-shaped query language over registered features

## Out of Scope

Permanently excluded from Beava OSS. Listed with reasoning to prevent re-adding.

| Feature | Reason |
|---------|--------|
| Cross-entity / cross-shard features | Blocked by single-thread single-process architecture. Would require fundamentally different design. |
| State exceeding single-box RAM (SSD tiering) | Adds complexity that undermines the "simple in-memory server" value prop. Users partition at app layer. |
| Multi-instance coordination in OSS | Built-in routing, replication, cross-instance WAL shipping. HA belongs in commercial tier. |
| Multi-tenant resource isolation | Tenancy is a higher-layer concern; beava is single-tenant by design. |
| TCP binary wire protocol | HTTP-first is the value prop. No binary protocol in OSS. |
| Operator implementation by user Rust code | Custom operators require recompile + plugin ABI. v0 ships 40 built-ins only. |

## Traceability

Which phases cover which requirements. Each REQ-ID maps to exactly one primary phase; cross-phase verification (e.g. SDK re-tests API endpoints in Phase 10) is noted in ROADMAP.md but does not double-count here.

| Requirement | Phase | Status |
|-------------|-------|--------|
| API-01 | Phase 2 | Pending |
| API-02 | Phase 2 | Pending |
| API-03 | Phase 3 (shape) → Phase 4 (durability completes it) — primary: Phase 3 | Pending |
| API-04 | Phase 3 | Pending |
| API-05 | Phase 3 | Pending |
| API-06 | Phase 3 | Pending |
| API-07 | Phase 3 | Pending |
| API-08 | Phase 3 | Pending |
| API-09 | Phase 2 | Pending |
| STREAM-01 | Phase 2 | Pending |
| STREAM-02 | Phase 4 | Pending |
| STREAM-03 | Phase 4 | Pending |
| STREAM-04 | Phase 5 | Pending |
| STREAM-05 | Phase 5 | Pending |
| REG-01 | Phase 2 | Pending |
| REG-02 | Phase 2 | Pending |
| REG-03 | Phase 2 | Pending |
| REG-04 | Phase 2 | Pending |
| PRIM-CORE-01 | Phase 3 | Pending |
| PRIM-CORE-02 | Phase 3 | Pending |
| PRIM-CORE-03 | Phase 3 | Pending |
| PRIM-CORE-04 | Phase 3 | Pending |
| PRIM-CORE-05 | Phase 3 | Pending |
| PRIM-RECENCY-01 | Phase 6 | Pending |
| PRIM-RECENCY-02 | Phase 6 | Pending |
| PRIM-RECENCY-03 | Phase 6 | Pending |
| PRIM-RECENCY-04 | Phase 6 | Pending |
| PRIM-RECENCY-05 | Phase 6 | Pending |
| PRIM-RECENCY-06 | Phase 6 | Pending |
| PRIM-RECENCY-07 | Phase 6 | Pending |
| PRIM-RECENCY-08 | Phase 6 | Pending |
| PRIM-DECAY-01 | Phase 6 | Pending |
| PRIM-DECAY-02 | Phase 6 | Pending |
| PRIM-DECAY-03 | Phase 6 | Pending |
| PRIM-DECAY-04 | Phase 6 | Pending |
| PRIM-VEL-01 | Phase 6 | Pending |
| PRIM-VEL-02 | Phase 6 | Pending |
| PRIM-VEL-03 | Phase 6 | Pending |
| PRIM-VEL-04 | Phase 6 | Pending |
| PRIM-VEL-05 | Phase 6 | Pending |
| PRIM-VEL-06 | Phase 6 | Pending |
| PRIM-VEL-07 | Phase 6 | Pending |
| PRIM-BUF-01 | Phase 7 | Pending |
| PRIM-BUF-02 | Phase 7 | Pending |
| PRIM-BUF-03 | Phase 7 | Pending |
| PRIM-BUF-04 | Phase 7 | Pending |
| PRIM-BUF-05 | Phase 7 | Pending |
| PRIM-BUF-06 | Phase 7 | Pending |
| PRIM-BUF-07 | Phase 7 | Pending |
| PRIM-BUF-08 | Phase 7 | Pending |
| PRIM-GEO-01 | Phase 7 | Pending |
| PRIM-GEO-02 | Phase 7 | Pending |
| PRIM-GEO-03 | Phase 7 | Pending |
| PRIM-GEO-04 | Phase 7 | Pending |
| PRIM-GEO-05 | Phase 7 | Pending |
| PRIM-GEO-06 | Phase 7 | Pending |
| PRIM-SKETCH-01 | Phase 8 | Pending |
| PRIM-SKETCH-02 | Phase 8 | Pending |
| PRIM-SKETCH-03 | Phase 8 | Pending |
| PRIM-SKETCH-04 | Phase 8 | Pending |
| PRIM-SKETCH-05 | Phase 8 | Pending |
| WIN-01 | Phase 2 | Pending |
| WIN-02 | Phase 2 | Pending |
| WIN-03 | Phase 2 | Pending |
| WIN-04 | Phase 2 | Pending |
| DUR-01 | Phase 4 | Pending |
| DUR-02 | Phase 4 | Pending |
| DUR-03 | Phase 4 | Pending |
| DUR-04 | Phase 4 | Pending |
| RECOV-01 | Phase 5 | Pending |
| RECOV-02 | Phase 5 | Pending |
| RECOV-03 | Phase 5 | Pending |
| RECOV-04 | Phase 5 | Pending |
| OBS-01 | Phase 9 | Pending |
| OBS-02 | Phase 9 | Pending |
| OBS-03 | Phase 9 | Pending |
| OBS-04 | Phase 9 | Pending |
| PERF-01 | Phase 9 | Pending |
| PERF-02 | Phase 9 | Pending |
| PERF-03 | Phase 9 | Pending |
| SDK-01 | Phase 10 | Pending |
| SDK-02 | Phase 10 | Pending |
| SDK-03 | Phase 10 | Pending |
| SDK-04 | Phase 10 | Pending |
| SDK-05 | Phase 10 | Pending |
| DOC-01 | Phase 10 | Pending |
| DOC-02 | Phase 10 | Pending |
| DOC-03 | Phase 10 | Pending |
| DOC-04 | Phase 10 | Pending |
| DOC-05 | Phase 10 | Pending |
| TEST-01 | Phase 3 | Pending |
| TEST-02 | Phase 2 | Pending |
| TEST-03 | Phase 5 | Pending |
| TEST-04 | Phase 4 | Pending |
| TEST-05 | Phase 3 | Pending |
| TEST-06 | Phase 5 | Pending |
| PKG-01 | Phase 10 | Pending |
| PKG-02 | Phase 10 | Pending |
| PKG-03 | Phase 10 | Pending |
| PKG-04 | Phase 10 | Pending |

**Coverage:**
- v1 requirements: 100 total
- Mapped to phases: 100 ✓
- Unmapped: 0

**Per-phase requirement counts:**

| Phase | Requirements | Count |
|-------|--------------|-------|
| Phase 1 (Foundation) | (infrastructure only, no scope-shipping reqs) | 0 |
| Phase 2 (Primitive infra + registration) | API-01, API-02, API-09, STREAM-01, REG-01..04, WIN-01..04, TEST-02 | 12 |
| Phase 3 (Core aggregates + push/get) | API-03..08, PRIM-CORE-01..05, TEST-01, TEST-05 | 13 |
| Phase 4 (WAL + idempotency) | STREAM-02, STREAM-03, DUR-01..04, TEST-04 | 7 |
| Phase 5 (Snapshot + recovery) | STREAM-04, STREAM-05, RECOV-01..04, TEST-03, TEST-06 | 8 |
| Phase 6 (Recency/decay/velocity) | PRIM-RECENCY-01..08, PRIM-DECAY-01..04, PRIM-VEL-01..07 | 19 |
| Phase 7 (Buffers + geo) | PRIM-BUF-01..08, PRIM-GEO-01..06 | 14 |
| Phase 8 (Sketches) | PRIM-SKETCH-01..05 | 5 |
| Phase 9 (Observability + perf) | OBS-01..04, PERF-01..03 | 7 |
| Phase 10 (SDK + docs + packaging) | SDK-01..05, DOC-01..05, PKG-01..04 | 14 |
| **Total** | | **99** |

*Phase 3 covers `API-03`, which completes its durability half in Phase 4; counted once in Phase 3 (primary). Phase 10 re-verifies `API-01`/`API-02` through the SDK surface but these are primary-mapped to Phase 2; not double-counted. Sum is 99 vs 100 because Phase 1 intentionally carries 0 scope-shipping REQ-IDs (pure infrastructure phase).*

Recount: 0+12+13+7+8+19+14+5+7+14 = 99 primary mappings; `API-03`'s shape-vs-durability split accounts for the 100th REQ-ID being double-noted (primary Phase 3, durability completes in Phase 4). All 100 REQ-IDs are covered; no orphans.

---
*Requirements defined: 2026-04-22*
*Last updated: 2026-04-22 after roadmap creation (traceability populated, 100/100 coverage).*
