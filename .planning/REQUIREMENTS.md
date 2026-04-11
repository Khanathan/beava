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

Requirements for milestone v1.3: Concurrency & Client Batching. **Active.**

### Performance

- [ ] **PERF-03**: Server-side async push coalescing — the server accumulates up to N async push frames (default 64) or T microseconds (default 200µs) per connection before dispatching them to a single batched handler under one state-lock acquisition. Multi-client aggregate async throughput on medium pipeline ≥ 200k eps @ 4 clients, and single-client async throughput is within ±5% of v1.2 baseline (Phase 12)
- [ ] **PERF-04**: Client-side batch push API — `app.push_many(stream, events)` wraps N events into one `OP_PUSH_BATCH` (0x0A) wire frame, reducing Python per-event loop overhead. Single-client async throughput via `push_many` ≥ 300k eps on medium pipeline. Error attribution surfaces `(batch_id, event_index)` via existing drain semantic; `app.push()` single-event API continues to work unchanged (Phase 13)
- [ ] **PERF-05**: Key-partitioned multi-threaded engine — `StateStore` is sharded across `num_shards` worker threads (default `std::thread::available_parallelism()`), each owning an exclusive `ShardStore` with no cross-thread locks on the hot path. Entity key → shard via stable `xxh3_64` hash. Cross-shard fan-out is fire-and-forget via bounded MPMC channels. Aggregate throughput on medium pipeline with 16 clients × 16 shards ≥ 1,000,000 eps; single-client throughput within ±10% of v1.2 (Phase 14)

### Operational

- [ ] **OPS-05**: Snapshot serialization runs off the main event-loop thread per shard — during a snapshot write, async PUSH throughput on the main path regresses by ≤ 5% (was 15–25% on v1.2). Snapshot write completes within the OPS-01-class budget (< 1 second per 100k entities). New v7 snapshot format uses a per-cycle manifest file as the atomic commit boundary across per-shard files; recovery from a partially-written snapshot set rolls back to the previous manifest (Phase 15)

### Locked Decisions (v1.3)

These are product-level tradeoffs accepted by research and approved for Phase 14 execution:

- **LD-1**: Cross-shard fan-out errors are fire-and-forget — target-shard errors surface in per-shard metrics, NOT in the originating client's `drain_errors_nonblock` queue. This is a deliberate regression from v1.2 semantics, required to preserve the shared-nothing hot path.
- **LD-2**: `num_shards` is persisted in the snapshot manifest + a config file. Changing shard count across restarts requires explicit `TALLY_ALLOW_RESHARD=1` and triggers a one-time re-route migration on load.
- **LD-3**: Snapshots are **shard-local consistent**, not globally consistent. A fan-out event may land in target-shard snapshot but not origin-shard snapshot within one cycle. Manifest guarantees per-shard files exist and hash-match, not that they reflect the same logical moment. Sibling to the existing "lose ~30s on crash" contract.
- **LD-4**: Shard routing uses `xxh3_64` with a fixed seed (not ahash, which isn't spec-stable across crate versions). Hash version byte is included in the manifest header.

## v1.4+ Requirements

Deferred to future release. Tracked but not in current roadmap.

### Event Log

- **ELOG-F1**: Event log compaction with merge (beyond TTL-based deletion)

### Schema Evolution

- **SCHM-F1**: Live schema migration of running operators (change window size)

### Pipeline

- **PIPE-F1**: Complex DAG transformations (map, filter, flatMap on keyless streams)

### Performance (future)

- **PERF-F1**: Awaitable cross-shard fan-out with deadline (opt-in, preserves v1.2 error semantics for non-hot-path callers)
- **PERF-F2**: Dynamic shard rebalancing (hot-key migration across shards at runtime)
- **PERF-F3**: Disk/S3 state spill for entities exceeding RAM budget

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
| PERF-03 | Phase 12 | In Progress |
| PERF-04 | Phase 13 | Pending |
| PERF-05 | Phase 14 | Pending |
| OPS-05  | Phase 15 | Pending |

**Coverage:**
- v1.1 requirements: 28 total, all mapped, all complete (DBUI-07 added post-original-roadmap via Phase 10.2)
- v1.2 requirements: 2 total, all mapped, all complete
- v1.3 requirements: 4 total, all mapped to phases 12–15
- Total: 34 requirements, 34 mapped, 0 unmapped

---
*Requirements defined: 2026-04-09*
*Last updated: 2026-04-11 — v1.3 requirements (PERF-03, PERF-04, PERF-05, OPS-05) added; v1.2 backfilled*
