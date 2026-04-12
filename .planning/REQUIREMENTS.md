# Requirements: Tally

**Defined:** 2026-04-09
**Core Value:** Events go in, features come out — synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.

## v1.1 Requirements

Requirements for milestone v1.1: Composable Pipeline & Event Log.

### Composable Pipeline

- [x] **PIPE-01**: User can define a keyless stream that ingests raw events without aggregation
- [x] **PIPE-02**: User can define a keyed stream with explicit `depends_on` declaring upstream stream dependencies
- [x] **PIPE-03**: Events pushed to any stream automatically cascade through all dependent streams in topological order
- [x] **PIPE-04**: Circular dependencies are detected and rejected at registration time
- [x] **PIPE-05**: Dependent streams receive null/missing for upstream values not yet available (LEFT JOIN semantics)

### Event Log

- [x] **ELOG-01**: Keyless streams persist events as an append-only log on local SSD
- [x] **ELOG-02**: Keyed streams persist events as an append-only log that gets compacted (snapshot replaces old events)
- [x] **ELOG-03**: Event log writes do not block the hot path (buffered async writes)
- [x] **ELOG-04**: User can configure history TTL per stream controlling how long events are retained
- [x] **ELOG-05**: Background compaction removes events older than history TTL

### Backfill & Schema Evolution

- [x] **SCHM-01**: User can add new features to an existing stream without resetting state
- [x] **SCHM-02**: User can remove features from a stream without resetting remaining features
- [x] **SCHM-03**: User can register a new feature with `backfill=True` to auto-replay from event log
- [x] **SCHM-04**: Backfill replay uses cooperative yielding to avoid starving live traffic
- [x] **SCHM-05**: Backfill replays events using event timestamps (not wall clock) for deterministic results

### Operational

- [x] **OPS-01**: User can fetch features for multiple keys in a single MGET call
- [x] **OPS-02**: User can configure entity state TTL per dataset/stream
- [x] **OPS-03**: Incremental snapshot serialization only writes changed entities since last snapshot
- [x] **OPS-04**: Snapshot restore handles incremental format (base + deltas)

### Debug UI

- [x] **DBUI-01**: User can view stream topology DAG in a web UI served from the existing HTTP port
- [x] **DBUI-02**: User can see real-time throughput (messages/sec) per stream
- [x] **DBUI-03**: User can inspect current feature values for any entity key
- [x] **DBUI-04**: User can see memory usage breakdown (per stream, total)
- [x] **DBUI-05**: Debug UI is embedded in the binary (no separate process or npm build)
- [x] **DBUI-06**: User can explore the running pipeline through an interactive topology DAG with clickable nodes that drill into per-stream memory, state, throughput, and entity lookup (Phase 10.1)
- [x] **DBUI-07**: User can observe per-command latency histograms (p50/p95/p99) per TCP command and stream, via /debug/latency endpoint and Debug UI surface (Phase 10.2)

## v1.2 Requirements

Requirements for milestone v1.2: Performance — fire-and-forget PUSH and binary wire protocol. **Shipped 2026-04-11.**

### Performance

- [x] **PERF-01**: User can push events in fire-and-forget mode via `app.push()` without paying round-trip latency for feature responses; errors surface on next call via `drain_errors_nonblock` (Phase 11)
- [x] **PERF-02**: Event payloads on the PUSH hot path use a typed binary format instead of JSON, eliminating JSON serialization cost per event (Phase 11)

## v1.3 Requirements

Requirements for milestone v1.3: Concurrency & Client Batching. **Partially shipped 2026-04-12.**

### Performance

- [ ] **PERF-03**: Server-side async push coalescing — deferred (low ROI vs batch API)
- [x] **PERF-04**: Client-side batch push API — `app.push_many(stream, events)` wraps N events into one `OP_PUSH_BATCH` (0x0A) wire frame. 359k eps single-client batch on medium pipeline (Phase 13)
- [x] **PERF-05**: DashMap per-stream entity concurrency + parking_lot RwLock PipelineEngine. 1.1M eps aggregate @ 8 proc (Phase 14)

### Operational

- [ ] **OPS-05**: Off-thread snapshot I/O — deferred to post-launch

## v2.0 Requirements

Requirements for milestone v2.0: New API & Engine. **Active.**

### SDK API

- [x] **API-01**: User can define an event source with `@tl.source` decorator that compiles to a keyless stream RegisterRequest
- [x] **API-02**: User can define a derived dataset with `@tl.dataset(depends_on=[...])` decorator that declares upstream dependencies and compiles to a keyed stream RegisterRequest
- [x] **API-03**: User can declare typed input schemas with `EventSet` and output schemas with `FeatureSet` using `Field` descriptors with IDE autocomplete via `dataclass_transform`
- [x] **API-04**: User can explicitly aggregate events with `.group_by("key").agg(count=tl.count(window="1h"), ...)` instead of implicit keying
- [x] **API-05**: User can merge multiple event sources into one dataset with `tl.union(source_a, source_b)`
- [x] **API-06**: User can call `pipeline.validate()` locally to check DAG validity (cycles, missing deps, type mismatches) before server submission
- [x] **API-07**: Pipeline definitions are portable — the same JSON format works for startup registration, runtime REGISTER, and future ephemeral pipelines

### Engine

- [x] **ENG-01**: Enriched event propagation — upstream derive results are visible to downstream datasets via a side-channel accumulator (not event clone), enabling multi-stage computed features
- [ ] **ENG-02**: Feature projection — `select()`/`drop()` on a dataset restricts which features appear in PUSH/GET responses (response-layer filtering)
- [ ] **ENG-03**: Ephemeral pipeline flag — `ephemeral: bool`, `ttl`, `max_keys` fields on RegisterRequest with `#[serde(default)]` (schema-only, lifecycle deferred post-launch)

### Migration

- [ ] **MIG-01**: All existing tests (>= 744) are ported to the new `@tl.source`/`@tl.dataset` API before old API removal
- [ ] **MIG-02**: Old `@st.stream`, `@st.view`, and `_dataframe.py` public API are removed from the SDK
- [ ] **MIG-03**: No performance regression — full benchmark matrix passes within -5% of 1.1M eps baseline after all changes

## v2.1+ Requirements

Deferred to future release. Tracked but not in current roadmap.

### On-Demand Compute

- **FUT-01**: On-demand compute lifecycle — TTL enforcement, memory limits, pipeline-level eviction for ephemeral pipelines
- **FUT-02**: One-shot replay queries via S3 replay log
- **FUT-03**: Typed schema validation at REGISTER time
- **FUT-04**: Computation-pruning projection (skip unused operator evaluation)

### Event Log

- **ELOG-F1**: Event log compaction with merge (beyond TTL-based deletion)

### Schema Evolution

- **SCHM-F1**: Live schema migration of running operators (change window size)

### Performance (future)

- **PERF-F1**: Awaitable cross-shard fan-out with deadline (opt-in)
- **PERF-F2**: Dynamic shard rebalancing (hot-key migration across shards at runtime)
- **PERF-F3**: Disk/S3 state spill for entities exceeding RAM budget
- **PERF-03**: Server-side async push coalescing (deferred from v1.3)
- **OPS-05**: Off-thread snapshot I/O per shard (deferred from v1.3)

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Full WAL replacing snapshots | Event log is opt-in per stream; mandatory fsync kills latency for all streams |
| Point-in-time historical replay | Turns Tally from serving system to storage/analytics system |
| Distributed event log replication | Single-node by design; back up snapshots externally |
| Kafka-compatible event log API | Tally consumes, doesn't produce; event log is internal |
| Temporal joins with watermarks | Flink-level complexity; LEFT JOIN + lookups sufficient for feature serving |
| Cluster mode / distributed operation | Single-node by design |
| Client-side sharding / hash-ring routing across instances | Document, don't build — users can run N Tally instances with their own routing |
| Dynamic shard rebalancing (live MIGRATE) | Redis Cluster's MIGRATE took years to ship correctly; not justified for v1.3's target audience |
| Multi-tenancy / namespace isolation | Out of domain for a real-time feature server |
| Session windows | Only sliding/tumbling windows; session windows require watermarks |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| PIPE-01 | Phase 7 | Complete |
| PIPE-02 | Phase 7 | Complete |
| PIPE-03 | Phase 7 | Complete |
| PIPE-04 | Phase 7 | Complete |
| PIPE-05 | Phase 7 | Complete |
| ELOG-01 | Phase 6 | Complete |
| ELOG-02 | Phase 6 | Complete |
| ELOG-03 | Phase 6 | Complete |
| ELOG-04 | Phase 6 | Complete |
| ELOG-05 | Phase 6 | Complete |
| SCHM-01 | Phase 8 | Complete |
| SCHM-02 | Phase 8 | Complete |
| SCHM-03 | Phase 8 | Complete |
| SCHM-04 | Phase 8 | Complete |
| SCHM-05 | Phase 8 | Complete |
| OPS-01 | Phase 6 | Complete |
| OPS-02 | Phase 6 | Complete |
| OPS-03 | Phase 9 | Complete |
| OPS-04 | Phase 9 | Complete |
| DBUI-01 | Phase 10 | Complete |
| DBUI-02 | Phase 10 | Complete |
| DBUI-03 | Phase 10 | Complete |
| DBUI-04 | Phase 10 | Complete |
| DBUI-05 | Phase 10 | Complete |
| DBUI-06 | Phase 10.1 | Complete |
| DBUI-07 | Phase 10.2 | Complete |
| PERF-01 | Phase 11 | Complete |
| PERF-02 | Phase 11 | Complete |
| PERF-03 | — | Deferred |
| PERF-04 | Phase 13 | Complete |
| PERF-05 | Phase 14 | Complete |
| OPS-05  | — | Deferred |
| API-01 | Phase 16 | Complete |
| API-02 | Phase 16 | Complete |
| API-03 | Phase 16 | Complete |
| API-04 | Phase 16 | Complete |
| API-05 | Phase 16 | Complete |
| API-06 | Phase 16 | Complete |
| API-07 | Phase 16 | Complete |
| ENG-01 | Phase 17 | Complete |
| ENG-02 | Phase 18 | Pending |
| ENG-03 | Phase 18 | Pending |
| MIG-01 | Phase 19 | Pending |
| MIG-02 | Phase 19 | Pending |
| MIG-03 | Phase 19 | Pending |

**Coverage:**
- v1.1 requirements: 28 total, all complete
- v1.2 requirements: 2 total, all complete
- v1.3 requirements: 2 complete (PERF-04, PERF-05), 2 deferred (PERF-03, OPS-05)
- v2.0 requirements: 13 total, all mapped (Phase 16: 7, Phase 17: 1, Phase 18: 2, Phase 19: 3)
- Total: 45 requirements, 32 complete, 13 pending, 0 unmapped

---
*Requirements defined: 2026-04-09*
*Last updated: 2026-04-12 — v2.0 requirements mapped to Phases 16-19*
