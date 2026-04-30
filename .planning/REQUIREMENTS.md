# Requirements: Beava v2

**Defined:** 2026-04-22 (re-planned to match v1 Python SDK API shape with v2 runtime changes)
**Core Value:** Feature authoring as composable Python code that ships to production unchanged.

## v1 Requirements

Requirements for the v0 OSS launch. Each maps to roadmap phases via the traceability section. REQ-IDs use `[CATEGORY]-[NUMBER]`; categories align with implementation boundaries so one category maps to one phase (or a tight phase group).

### SDK-DEC — Python SDK decorators (source declarations)

- [ ] **SDK-DEC-01**: `@bv.event` decorator accepts a class with type-hinted fields; extracts schema (types, optional flags, Field metadata); stores as an `EventSource` descriptor
- [ ] **SDK-DEC-02a**: `@bv.event` class form accepts optional `keep_events_for` (duration string) parameter. (Active.)
- ~~**SDK-DEC-02b**: `@bv.event` class form accepts optional `tolerate_delay` (duration string) parameter~~ — **REMOVED 2026-04-30 per no-event-time pivot** (SUPERSEDED by `project_redis_shaped_no_event_time_ever`). The runtime has no event-time concept post-pivot, so a per-event tolerance window is meaningless. v0 ships without this parameter; if a future v0.1+ revisits per-event timing semantics, a NEW REQ-ID covers that scope.
  - Note: SDK-DEC-02 was split 2026-04-30 to make the active vs REMOVED surfaces explicit per the no-event-time pivot. The original SDK-DEC-02 conflated `keep_events_for` (kept) with `tolerate_delay` (REMOVED); the split lets Phase 12.6's plans reference either side cleanly.
- [ ] **SDK-DEC-03**: `@bv.event` function form: function with upstream-class parameters returns an `Event` / `EventDerivation`; decorator invokes the function once at registration with upstream descriptors and captures the result
- [ ] **SDK-DEC-04**: `@bv.table(key=..., ttl=..., mode="upsert")` decorator accepts string or list primary key, optional TTL duration; validates key fields exist in schema
- [ ] **SDK-DEC-05**: `@bv.table` function form: returns a `Table`/`TableDerivation`; upstream descriptors passed as typed parameters
- [ ] **SDK-DEC-06**: Schema extraction supports `str`, `f64`/`float`, `i64`/`int`, `bool`, `bytes`, `datetime` field types; rejects unsupported types at decorator time with a clear error message
- [ ] **SDK-DEC-07**: `bv.Optional[T]` marks a field nullable; `bv.Field(desc=..., default=...)` attaches per-field metadata
- ~~**SDK-DEC-08**: `event_time` field on `@bv.event`~~ — REMOVED 2026-04-30 per no-event-time pivot. Server-side `now_ms()` is the only time source; events have no event-time field on the wire. See PROJECT.md §Key Decisions.
- [ ] **SDK-DEC-09**: `@bv.event` accepts optional `dedupe_key` + `dedupe_window` for stream-level deduplication at push time

### SDK-COL — Expression DSL (`bv.col`)

- [ ] **SDK-COL-01**: `bv.col("field")` returns an expression node; supports arithmetic `+ - * /` producing expressions
- [ ] **SDK-COL-02**: Comparison operators `< > <= >= == !=` produce boolean expressions
- [ ] **SDK-COL-03**: Boolean combinators `&` (and), `|` (or), `~` (not) on expressions
- [ ] **SDK-COL-04**: `.isnull()` produces `(x == null)` expression
- [ ] **SDK-COL-05**: `.cast("float"/"int"/"str"/"bool")` produces explicit type-coercion expression
- [ ] **SDK-COL-06**: Expression serialization via `.to_expr_string()` produces parenthesized canonical form the server can parse
- [ ] **SDK-COL-07**: Expression validation at registration time: field references resolve to known schema fields; type mismatches error with path
- [ ] **SDK-COL-08**: Arithmetic on a typed field produces an expression with inferred output type (int+int→int, int+float→float, etc.)

### SDK-OPS — Stateless per-row ops

- [x] **SDK-OPS-01**: `.filter(expr)` — keeps rows where expression is truthy; schema unchanged; chains left-to-right
- [x] **SDK-OPS-02**: `.select(*fields)` — keeps only listed fields in order
- [x] **SDK-OPS-03**: `.drop(*fields)` — removes fields; tables reject dropping key fields with error
- [x] **SDK-OPS-04**: `.rename(**mapping)` — renames fields; tables cascade the key list rename
- [x] **SDK-OPS-05**: `.with_columns(**derivations)` — adds/replaces derived fields from expressions; type-inferred
- [x] **SDK-OPS-06**: `.map(**derivations)` — alias for `.with_columns` (DataFrame parity)
- [x] **SDK-OPS-07**: `.cast(**type_map)` — coerces field types to `int`/`float`/`str`/`bool`
- [x] **SDK-OPS-08**: `.fillna(**defaults)` — fills nulls with scalars; clears optional flag on those fields
- [x] **SDK-OPS-09**: Every stateless op returns a new `EventDerivation`/`TableDerivation` wrapping the previous; no in-place mutation
- [x] **SDK-OPS-10**: Chained ops compose left-to-right, with output schema propagated through each step

### SDK-AGG — Aggregation framework

- [ ] **SDK-AGG-01**: `Event.group_by(*keys)` returns a `GroupBy` builder; keys must exist in upstream schema
- [x] **SDK-AGG-02**: `GroupBy.agg(**named_features)` accepts named aggregation operator descriptors; returns a `TableDerivation`
- [ ] **SDK-AGG-03**: Aggregation output schema: group keys preserve upstream types; feature columns get types inferred by each operator's `output_type_for(schema)` method
- [ ] **SDK-AGG-04**: `group_by().agg()` validates every operator's field references exist in upstream schema; errors with path
- [x] **SDK-AGG-05**: Aggregation on a `Table` is explicitly rejected in v0 (deferred to v0.1 pending retraction propagation)
- [x] **SDK-AGG-06**: Window validation at decorator time: `window` is a duration string matching `\d+(ms|s|m|h|d)` or `forever`

### AGG-CORE — Core aggregation operators

- [ ] **AGG-CORE-01**: `bv.count(window=..., where=..., bucket=...)` — event count; int output
- [ ] **AGG-CORE-02**: `bv.sum(field, window=..., where=...)` — numeric sum; float output
- [ ] **AGG-CORE-03**: `bv.avg(field, window=..., where=...)` — arithmetic mean; float output
- [ ] **AGG-CORE-04**: `bv.min(field, window=..., where=...)` — minimum; preserves field type
- [ ] **AGG-CORE-05**: `bv.max(field, window=..., where=...)` — maximum; preserves field type
- [ ] **AGG-CORE-06**: `bv.variance(field, window=..., where=...)` — sample variance via Welford; float output
- [ ] **AGG-CORE-07**: `bv.stddev(field, window=..., where=...)` — sqrt of variance; float output
- [ ] **AGG-CORE-08**: `bv.ratio(where=..., window=...)` — count matching / count total; float in [0,1]
- [x] **AGG-CORE-09**: All core aggregations require `window=` except `ratio`/`count` which accept windowless via implicit `forever`

### AGG-SKETCH — Sketch aggregations

- [ ] **AGG-SKETCH-01**: `bv.count_distinct(field, window=..., exact_threshold=1024, hybrid_precision=14)` — HLL cardinality estimate; int output
- [ ] **AGG-SKETCH-02**: `bv.percentile(field, q, window=..., exact_threshold=256, hybrid_alpha=0.01)` — DDSketch quantile; float output
- [ ] **AGG-SKETCH-03**: `bv.top_k(field, k, window=..., exact_threshold=1024, hybrid_width=2048, hybrid_depth=4)` — CountMinSketch + bounded min-heap (hybrid exact/sketch mode); list output
- [ ] **AGG-SKETCH-04**: `bv.bloom_member(field, capacity=1024, fpr=0.01)` — Bloom-filter ever-seen membership test; bool output
- [ ] **AGG-SKETCH-05**: `bv.entropy(field, window=...)` — Shannon entropy over the empirical categorical distribution; float output

### AGG-POINT — Point / ordinal aggregations

- [ ] **AGG-POINT-01**: `bv.first(field)` — first observed value; preserves field type
- [ ] **AGG-POINT-02**: `bv.last(field)` — most recent value by arrival order; preserves field type
- [ ] **AGG-POINT-03**: `bv.first_n(field, n)` — first N values; list output
- [ ] **AGG-POINT-04**: `bv.last_n(field, n)` — last N values; list output
- [ ] **AGG-POINT-05**: `bv.lag(field, n)` — value n events ago; preserves field type
- [ ] **AGG-POINT-06**: `bv.first_seen()` — first-seen server arrival timestamp `now_ms()`; int output (millis)
- [ ] **AGG-POINT-07**: `bv.last_seen()` — last-seen server arrival timestamp `now_ms()`; int output
- [ ] **AGG-POINT-08**: `bv.age()` — ms since first_seen (computed at read time against current `now_ms()`); int output
- [ ] **AGG-POINT-09**: `bv.has_seen(where=...)` — boolean ever-matched; bool output
- [ ] **AGG-POINT-10**: `bv.time_since(where=...)` — ms since last matching event; int or null
- [ ] **AGG-POINT-11**: `bv.time_since_last_n(where=..., n=...)` — ms since kth most recent matching event

### AGG-DECAY — Decay family

- [ ] **AGG-DECAY-01**: `bv.ewma(field, half_life=...)` — exponentially-weighted moving average; float output. `bv.ema` is an alias.
- [ ] **AGG-DECAY-02**: `bv.ewvar(field, half_life=...)` — exponentially-weighted variance; float output
- [ ] **AGG-DECAY-03**: `bv.ew_zscore(field, half_life=...)` — current event's z-score against EWMA/EWVar baseline; float output
- [ ] **AGG-DECAY-04**: `bv.decayed_sum(field, half_life=...)` — forward-decay sum (Cormode); float output
- [ ] **AGG-DECAY-05**: `bv.decayed_count(half_life=...)` — forward-decay count; float output
- [ ] **AGG-DECAY-06**: `bv.twa(field, window=...)` — time-weighted average for irregularly-sampled gauge fields; float output
- [ ] **AGG-DECAY-07**: Half-life parameter accepts duration strings; rejected at decorator time if malformed

### AGG-VEL — Velocity / trend

- [ ] **AGG-VEL-01**: `bv.rate_of_change(field|count, window=..., sub_window=...)` — Δrate or acceleration across two adjacent windows; float
- [ ] **AGG-VEL-02**: `bv.inter_arrival_stats(where=..., window=...)` — mean, stddev, CV of gaps between matching events; struct output `{mean_ms, stddev_ms, cv}`
- [ ] **AGG-VEL-03**: `bv.burst_count(sub_window=..., window=...)` — max events observed in any sub-window inside the outer window; int output
- [ ] **AGG-VEL-04**: `bv.delta_from_prev(field)` — current value minus previous event's value; preserves field type
- [ ] **AGG-VEL-05**: `bv.trend(field, window=...)` — slope of EW linear regression over window; float
- [ ] **AGG-VEL-06**: `bv.trend_residual(field, window=...)` — current value minus trend-predicted value; float
- [ ] **AGG-VEL-07**: `bv.outlier_count(field, sigma=3, window=...)` — count of events beyond Nσ in window; int
- [ ] **AGG-VEL-08**: `bv.value_change_count(field, window=...)` — count of field value flips; int

### AGG-RECENCY — Recency / streaks

- [ ] **AGG-RECENCY-01**: `bv.streak(where=...)` — length of current consecutive matching streak; int
- [ ] **AGG-RECENCY-02**: `bv.max_streak(where=...)` — longest streak length ever observed; int
- [ ] **AGG-RECENCY-03**: `bv.negative_streak(where=...)` — length of current consecutive non-matching streak; int
- [ ] **AGG-RECENCY-04**: `bv.first_seen_in_window(field, window=...)` — Bloom + timestamp; bool output — "is this value new to this entity in window N?"

### AGG-BUFFER — Bounded buffers

- [ ] **AGG-BUFFER-01**: `bv.histogram(field, buckets=[...], window=...)` — count per fixed bucket; returns dict `{bucket_label: count}`
- [ ] **AGG-BUFFER-02**: `bv.hour_of_day_histogram()` — 24-bin count histogram per entity; dict output
- [ ] **AGG-BUFFER-03**: `bv.dow_hour_histogram()` — 168-bin (day × hour) histogram; dict output
- [ ] **AGG-BUFFER-04**: `bv.seasonal_deviation(field=None)` — z-score of current event vs this entity's hour-of-day baseline; float
- [ ] **AGG-BUFFER-05**: `bv.event_type_mix(field, categories=[...], window=...)` — proportion per category; dict output
- [ ] **AGG-BUFFER-06**: `bv.most_recent_n(field, n)` — deque of N most-recent values; list output
- [ ] **AGG-BUFFER-07**: `bv.reservoir_sample(field, k)` — uniform K-sample over all history; list output

### AGG-GEO — Geo

- [ ] **AGG-GEO-01**: `bv.geo_velocity(lat=..., lon=..., window=...)` — max implied km/h between consecutive events in window; float
- [ ] **AGG-GEO-02**: `bv.geo_distance(lat=..., lon=..., window=...)` — total path length in window; float
- [ ] **AGG-GEO-03**: `bv.geo_spread(lat=..., lon=..., window=...)` — max distance from mean center; float
- [ ] **AGG-GEO-04**: `bv.unique_cells(lat=..., lon=..., precision=..., window=...)` — distinct geohash cells visited; int
- [ ] **AGG-GEO-05**: `bv.geo_entropy(lat=..., lon=..., precision=..., window=...)` — Shannon entropy over geohash cell distribution; float
- [ ] **AGG-GEO-06**: `bv.distance_from_home(lat=..., lon=..., samples=100)` — distance of current event from running centroid of top-K frequent locations; float

### AGG-Z — Entity-level z-score

- [ ] **AGG-Z-01**: `bv.z_score(field, baseline_window=..., current=...)` — current event's value vs rolling mean/stddev baseline; float

### SDK-JOIN — REMOVED 2026-04-30

All join + union requirements REMOVED 2026-04-30 per the no-event-time pivot (see `project_redis_shaped_no_event_time_ever`). Joins of any shape (event↔event, event↔table, table↔table) are not part of v0+. `bv.union(*events)` DEFERRED to v0.1+ alongside joins. See PROJECT.md → Out of Scope. Phases 14, 14.1, 15 (event-time / watermark / PIT) archived to `_archived-*` directories.

- ~~**SDK-JOIN-01..04**: joins~~ — REMOVED
- ~~**SDK-JOIN-05**: `bv.union(*events)`~~ — DEFERRED v0.1+

### SDK-SESSION — Session windows (v0.1)

- [ ] **SDK-SESSION-01**: `bv.session(gap_ms=..., inner=bv.<op>(...))` — activity-based grouping; opens session on first event, increments inner per event within `gap_ms`, closes on `now_ms() - last_event_ms > gap_ms` (lazy-on-query) AND flips on next event after gap; latest closed session retained per (entity, feature)

### SDK-APP — Python `App` client

- [ ] **SDK-APP-01**: `bv.App(url)` accepts HTTP URL; `.close()` / context manager for lifecycle management
- [ ] **SDK-APP-02**: `app.register(*descriptors)` — validates DAG, topologically sorts, serializes each to REGISTER JSON, POSTs to `/register`; assigns server version returned in response
- [ ] **SDK-APP-03**: `app.validate(*descriptors)` — runs the same DAG validation as register but with zero network I/O; returns `list[ValidationError]`
- [ ] **SDK-APP-04**: `app.push(Event, event_dict)` — async fire-and-forget push; returns immediately; errors surface on next API call
- [ ] **SDK-APP-05**: `app.push_sync(Event, event_dict)` → `FeatureResult` — sync push that returns computed features for the event's entity in the same round-trip
- [ ] **SDK-APP-06**: `app.push_many(Event, [dicts])` — batched push, single wire frame; reports errors per `(batch_id, event_index)`
- [ ] **SDK-APP-07**: `app.push(Table, key, row_dict)` — synchronous table upsert; blocks until server ACK
- [ ] **SDK-APP-08**: `app.delete(Table, key)` — synchronous tombstone; server retains 7d for late cascade consumers
- [ ] **SDK-APP-09**: `app.get(key)` → `FeatureResult` — all features for the key; attribute and dict-style access; unknown key → empty result
- [ ] **SDK-APP-10**: `app.mget([keys])` — batched feature lookup; returns dict of key→FeatureResult
- [ ] **SDK-APP-11**: `app.get_multi([Table1, Table2, ...], key=...)` — fetch features across multiple tables in one round-trip; returns dict of descriptor→FeatureResult
- [ ] **SDK-APP-12**: `app.set(key, features_dict)` and `app.mset({key: features_dict, ...})` — direct feature writes
- [ ] **SDK-APP-13**: `app.flush()` — awaits all outstanding async pushes; called automatically on context-manager exit
- [ ] **SDK-APP-14**: `FeatureResult` supports `r.feature_name` attribute access and `r["feature_name"]` dict access; `r.to_dict()` for explicit dump
- [ ] **SDK-APP-15**: `ValidationError` structure: `kind` (cycle/missing_dep/schema_mismatch/bad_return_type), `path` (e.g., `Checkouts.filter[2]`), `message`; str repr formats as `[{kind}] {path}: {message}`

### SDK-WIRE — Wire transports

- [ ] **SDK-WIRE-01**: HTTP/JSON transport: `bv.App("http://host:port")` (and `https://`) sends REGISTER via `POST /register` with JSON body using `httpx>=0.27,<1`; 2xx returns parsed success body; non-2xx raises `RegistrationError` populated from the server's error JSON (`code`, `path`, `reason`/`message`); server error body parsed as JSON when `Content-Type: application/json`
- [ ] **SDK-WIRE-02**: Framed TCP transport: `bv.App("tcp://host:port")` uses stdlib `socket`; frame format `[u32 length BE][u16 op BE][u8 content_type][payload]` matching Phase 2.5 server; strict-FIFO correlation (no `request_id`) per CLAUDE.md; connection reuse across `register` / `validate` / `ping` on the same App instance; `app.close()` closes the socket
- [ ] **SDK-WIRE-03**: URL-scheme dispatch: `bv.App(url)` parses the URL and instantiates the correct transport (`http://`/`https://` → HTTP, `tcp://` → TCP); unknown scheme raises `ValueError` at construction time; `bv.App()` with no URL triggers embed mode per Phase 3 CONTEXT.md D-10 (spawn local `beava` subprocess, discover binary via 4-step search order `BEAVA_BINARY` env → `PATH` → `./target/debug/beava` → `BinaryNotFoundError`, bind to ephemeral ports, cleanup on `close()` / context-manager exit)

### SDK-FORK — Scoped local replica

- [ ] **SDK-FORK-01**: `bv.fork(remote=..., events=[...], keys=[...], token=..., pipelines=[...])` context manager — spawns local scoped replica, registers the pipelines, replicates the listed keys from the remote
- [ ] **SDK-FORK-02**: `ForkedReplica.get(descriptor, key=...)` — local read against the fork; same `FeatureResult` shape as `App.get`
- [ ] **SDK-FORK-03**: Fork CLI `beava fork` is a binary subcommand (wired in Phase 13 packaging)
- [ ] **SDK-FORK-04**: Fork replica state is ephemeral (destroyed on context exit); no persistence across invocations

### SRV-API — HTTP API surface

- [ ] **SRV-API-01**: `POST /register` accepts a JSON DAG payload (topologically-ordered list of event/table/derivation nodes); returns `{status: "ok", registry_version: N, registered_descriptors: [...]}`
- [ ] **SRV-API-02**: Registration is additive-only: submitting a DAG that adds new nodes succeeds with version bump; submitting a DAG that removes or changes an existing node returns 409 with structured `{diff: {added, removed, changed}}`
- [ ] **SRV-API-03**: `POST /push/{event_name}` accepts JSON event; validates against registered schema; returns `{ack_lsn, idempotent_replay, registry_version}` only after WAL fsync past LSN
- [ ] **SRV-API-04**: `POST /push-sync/{event_name}` — same as /push but returns `{ack_lsn, features: {...}, ...}` with computed features
- [ ] **SRV-API-05**: `POST /push-batch/{event_name}` accepts JSON array; returns per-event results
- [ ] **SRV-API-06**: `POST /push-table/{table_name}` upserts a row by primary key
- [ ] **SRV-API-07**: `POST /delete-table/{table_name}` tombstones a row by primary key
- [ ] **SRV-API-08**: `POST /get` accepts `{keys: [...], features: [...]}`; returns `{key: {feature: value}}` map; per-request cap keys × features ≤ 10000
- [ ] **SRV-API-09**: `GET /get/{feature}/{key}` single-feature lookup; `{value, meta?}` shape
- [ ] **SRV-API-10**: `POST /set` and `POST /mset` accept direct feature writes
- [ ] **SRV-API-11**: Content-Type application/json enforced; 415 on mismatch
- [ ] **SRV-API-12**: Validation errors return 400 with `{error: {code, path, reason}}` naming the offending DAG/field/expression path
- [ ] **SRV-API-13**: All endpoints support optional `X-Trace-Id` header propagated to logs

### SRV-REG — Registry

- [ ] **SRV-REG-01**: Registry stores events, tables, and derivation specs in an `Arc<RwLock>` wrapper keyed by descriptor name
- [ ] **SRV-REG-02**: Registry assigns monotonic `registry_version` (u64) incremented on every successful additive registration
- [ ] **SRV-REG-03**: Registry diff engine computes `{added, removed, changed}` between submitted DAG and current state; removals and changes produce 409, added produces 200 + version bump
- [ ] **SRV-REG-04**: Registration WAL-records every `/register` request so the registry reconstructs deterministically on recovery
- [ ] **SRV-REG-05**: Registry-version bumps commit atomically with the WAL entry (single fsync covers both)
- [ ] **SRV-REG-06**: Optional `GET /registry?version=N` returns the full registry state at that version for debugging

### SRV-APPLY — Apply loop + windowing

- [ ] **SRV-APPLY-01**: Single-thread apply loop: one dedicated OS thread receives pushed events via SPSC from HTTP accept, updates per-entity state, no locks on hot path
- [ ] **SRV-APPLY-02**: Each event's apply runs every registered derivation affected by that event's source (via registry DAG)
- [ ] **SRV-APPLY-03**: `Windowed<Op>` wrapper: uniform processing-time bucketing (server-side `now_ms()`, NOT event_time per 2026-04-30 pivot), default cap 64 buckets, width = `ceil(window_ms / 64)`
- [ ] **SRV-APPLY-04**: Lazy bucket rollover: on each apply, evict expired buckets based on current `now_ms()`
- [ ] **SRV-APPLY-05**: "Lifetime" mode when `window` omitted — one bucket, no rollover
- [ ] **SRV-APPLY-06**: Expression evaluator parses `to_expr_string()` canonical form into AST; evaluates per-event in-place with zero allocations on the hot path
- [ ] **SRV-APPLY-07**: Stateless ops chain compiled at registration time into a sequence of row transformations; executed per event before aggregations see it
- ~~**SRV-APPLY-08**: Join support~~ — REMOVED 2026-04-30 per no-event-time pivot

### SRV-DUR — Durability

- [ ] **SRV-DUR-01**: Per-instance append-only WAL file with group-commit fsync every 1–5ms or 1MB (whichever first)
- [ ] **SRV-DUR-02**: `/push` ACK returns only after event's LSN has been fsynced
- [ ] **SRV-DUR-03**: WAL format: header with `registry_version`, `stream_id`, `recv_ts_ms` (server arrival), entity key(s), event body; sufficient for full state rebuild. (`event_time` field DROPPED in Phase 12.6 wire schema bump per no-event-time pivot.)
- [ ] **SRV-DUR-04**: WAL rotation: old segments truncated past the latest snapshot's covered LSN
- [ ] **SRV-DUR-05**: Stream-level idempotency: `dedupe_key` + `dedupe_window` stored at registration; duplicate request within TTL replays byte-identical response, no state mutation

### SRV-RECOV — Recovery

- [ ] **SRV-RECOV-01**: Periodic snapshot (default 30s, configurable) serializes in-memory state + registry to disk
- [ ] **SRV-RECOV-02**: Recovery on boot loads latest snapshot + replays WAL from snapshot's covered LSN to present
- [ ] **SRV-RECOV-03**: RTO target: server online under 30s at 10GB state on NVMe (combined load + replay)
- [ ] **SRV-RECOV-04**: Corrupt snapshot or WAL (checksum mismatch) detected with clear operator message; fall back to earlier snapshot
- [ ] **SRV-RECOV-05**: Schema evolution: schema versions stored in registry so older WAL entries remain replayable after additive-only schema changes

### OBS — Observability

- [ ] **OBS-01**: `/metrics` Prometheus format: per-endpoint QPS/latency histograms, per-operator apply-count, WAL group-commit latency, snapshot-latency, registry-version gauge
- [ ] **OBS-02**: `/health` liveness (cheap, returns 200 when server is up)
- [ ] **OBS-03**: `/ready` readiness (returns 200 only after recovery complete)
- [ ] **OBS-04**: Structured JSON logs at INFO/WARN/ERROR; `trace_id` propagation from `X-Trace-Id` header across logs per request

### PERF — Performance targets

- [ ] **PERF-01**: Single-thread apply sustains ≥ 3M events/sec on modern NVMe server-class hardware for 32-byte events × 5 aggregations
- [ ] **PERF-02**: Batch `/get` of 100 features × 1 entity: P50 < 2ms, P99 < 10ms warm-cache
- [ ] **PERF-03**: WAL group-commit adds P50 < 2ms to push ACK latency at default config
- [ ] **PERF-04**: Benchmark harness exposes a reproducible scenario covering each operator family; checked into `benches/`

### THROUGHPUT — End-to-end throughput harness (Phase 7.5 + per-phase ledger)

- [ ] **THROUGHPUT-HARNESS-01**: Reusable harness binary (e.g. `crates/beava-bench/`) drives a live `beava` server over HTTP and TCP; emits a structured result (JSON + markdown row) with EPS, P50/P95/P99 push latency, P99 batch-get latency, RSS at peak, hw-class tag
- [ ] **THROUGHPUT-HARNESS-02**: `.planning/throughput-baselines.md` ledger format + hw-class tagging matches the `.planning/perf-baselines.md` convention (one section per hw-class, append-only rows keyed by phase + pipeline-shape + transport)
- [ ] **THROUGHPUT-HARNESS-03**: Per-phase regression thresholds — 10% slower than prior baseline on the simple-fraud shape → WARNING in VERIFICATION.md; 25% slower → BLOCKER. Plan-checker contract: every phase from 8 onward MUST include a "throughput run" task or the plan is rejected
- [ ] **THROUGHPUT-PIPELINES-01**: Three pipeline configs ship with the harness — small (1 feature, 1 entity, 1 window), medium (5 features), large (15 features). Each runnable independently; results recorded per size
- [ ] **THROUGHPUT-WORKLOAD-01**: 60s wall-time time-bounded workload (count events processed, not "process N events"). Standardizes comparisons across hw-classes since faster boxes do more events per second
- [ ] **THROUGHPUT-FIRST-BASELINE-01**: Phase 7.5 ships the first baseline using ONLY Phase 5 operators (count/sum/avg/min/max/variance/stddev/ratio) over Phase 6 WAL durability + Phase 7 snapshot/recovery in the path. Captured for all 3 pipeline sizes × HTTP + TCP on at least one hw-class. This is the "start of the line" that subsequent phases compare against

### TEST — Testing + quality

- [ ] **TEST-01**: Table-driven unit tests per operator: push known events, assert expected feature values
- [ ] **TEST-02**: Expression DSL proptest coverage: random predicate + random event → truth-table equivalence
- [ ] **TEST-03**: Integration tests via real server + real HTTP client using `TestServer` harness from Phase 1
- [ ] **TEST-04**: DAG validation tests: cycles, missing deps, schema mismatches, additive-conflict diff correctness
- [ ] **TEST-05**: Crash-recovery test: kill process mid-push; restart; verify state equals pre-crash + replayed WAL
- [ ] **TEST-06**: Python SDK tests hit real beava instance over HTTP; cover register + push + push_sync + push_many + get + mget + fork
- ~~**TEST-07**: Join tests~~ — REMOVED 2026-04-30 per no-event-time pivot

### DOC — Documentation

- [ ] **DOC-01**: `docs/quickstart.md` — `pip install beava` → first feature in under 5 minutes with ≤ 20 lines of Python
- [ ] **DOC-02**: `docs/operators.md` — every operator with example, parameters, return shape, use case
- [ ] **DOC-03**: `docs/concepts.md` — event vs table, stateless ops, aggregations, session windows, processing-time semantics (joins/unions/event-time NOT covered — removed permanently per 2026-04-30 architectural pivot)
- [ ] **DOC-04**: `docs/http-api.md` — wire protocol for non-Python users with `curl` examples
- [ ] **DOC-05**: `docs/architecture.md` — single-thread apply loop, WAL + snapshot, memory sizing, recovery
- [ ] **DOC-06**: `README.md` has 3-command smoke demo

### PKG — Packaging

- [ ] **PKG-01**: Prebuilt Rust binaries on GitHub Releases for linux/amd64, linux/arm64, darwin/arm64
- [ ] **PKG-02**: `pip install beava` publishes the Python SDK (PyPI); version pinned to server release
- [ ] **PKG-03**: Docker image with zero-config entrypoint (`docker run beava/beava:v0`)
- [ ] **PKG-04**: `beava fork` subcommand available in the binary
- [ ] **PKG-05**: Binary size under 200MB stripped

## v2 Requirements

Deferred to future release.

### v0.1: Table aggregation with retraction

- **V0.1-TABLE-01**: `table.group_by(...).agg(...)` with retraction propagation through derived features

### Emit / timers / CEP / attribution

- **EMIT-01**: Operators emit downstream events via `OpOutcome::Emit`
- **TIMER-01**: Event-time timer callbacks (`on_timer`) unlocking debouncer, auction close, session-end
- **CEP-01**: Sequence pattern detection (`bv.sequence([steps], within=...)`)
- **SM-01**: State machine operator with transition-event emission
- **ATTR-01**: Multi-touch attribution operators (last_touch / first_touch / linear / time_decay / u_shaped)

### Advanced

- **BACKFILL-01**: Branch/replay/promote/discard workflow for validating new feature definitions
- **XENT-01**: Cross-entity operators (`co_occurrence_count`, `graph_degree`) requiring cross-shard coordination
- **HA-01**: Read replicas, commercial-tier HA with WAL shipping
- ~~**JOIN-EXTRA-01**~~ — REMOVED 2026-04-30 (all joins removed; this item is now redundant)
- ~~**UNION-RECONCILE-01**~~ — REMOVED 2026-04-30 (union deferred with joins; reconciliation question moot)
- **EVENT-TIME-RESTORE-01**: Re-introduce `event_time_ms` semantics + watermarks + late-event handling — REJECTED permanently 2026-04-30; documented for posterity. Future requests require explicit user override + new ADR overturning `project_redis_shaped_no_event_time_ever`.
- **PLUGIN-01**: User-defined operators via Rust plugin ABI
- **SQL-01**: SQL-shaped query DSL on top of the registry

## Out of Scope

Permanently excluded from Beava OSS.

| Feature | Reason |
|---------|--------|
| Cross-entity operators in OSS | Blocked by single-thread single-process architecture |
| SSD-tiered state | Undermines the "in-memory server" value prop; users partition at app layer |
| Multi-instance coordination in OSS | HA is commercial-tier |
| Multi-tenancy | Single-tenant by design |
| TCP binary wire | HTTP-first is the value prop |
| Operator by user Rust code at runtime | Plugin ABI future; recompile-per-op only |
| Table aggregation without retraction | Correctness requires retraction; deferred to v0.1 |

## Traceability

Which phases cover which requirements. Updated during roadmap creation. See `ROADMAP.md`.

| Requirement | Phase | Status |
|-------------|-------|--------|
| (Populated by roadmapper) | | |

**Coverage:**
- v1 requirements: ~110 total (count is approximate until roadmapper confirms final REQ-ID enumeration)
- Mapped to phases: 0 (pending roadmap)
- Unmapped: all (pending roadmap)

---
*Requirements defined: 2026-04-22 (re-plan to v1 API shape)*
