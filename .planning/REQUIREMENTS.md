# Requirements: Tally

**Defined:** 2026-04-09
**Core Value:** Events go in, features come out — synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.

## v1.1 Requirements

Requirements for milestone v1.1: Composable Pipeline & Event Log.

### Composable Pipeline

- [x] **PIPE-01**: User can define a keyless stream that ingests raw events without aggregation
- [x] **PIPE-02**: User can define a keyed stream with explicit `depends_on` declaring upstream stream dependencies
- [ ] **PIPE-03**: Events pushed to any stream automatically cascade through all dependent streams in topological order
- [ ] **PIPE-04**: Circular dependencies are detected and rejected at registration time
- [x] **PIPE-05**: Dependent streams receive null/missing for upstream values not yet available (LEFT JOIN semantics)

### Event Log

- [x] **ELOG-01**: Keyless streams persist events as an append-only log on local SSD
- [x] **ELOG-02**: Keyed streams persist events as an append-only log that gets compacted (snapshot replaces old events)
- [x] **ELOG-03**: Event log writes do not block the hot path (buffered async writes)
- [x] **ELOG-04**: User can configure history TTL per stream controlling how long events are retained
- [x] **ELOG-05**: Background compaction removes events older than history TTL

### Backfill & Schema Evolution

- [ ] **SCHM-01**: User can add new features to an existing stream without resetting state
- [ ] **SCHM-02**: User can remove features from a stream without resetting remaining features
- [ ] **SCHM-03**: User can register a new feature with `backfill=True` to auto-replay from event log
- [ ] **SCHM-04**: Backfill replay uses cooperative yielding to avoid starving live traffic
- [ ] **SCHM-05**: Backfill replays events using event timestamps (not wall clock) for deterministic results

### Operational

- [x] **OPS-01**: User can fetch features for multiple keys in a single MGET call
- [x] **OPS-02**: User can configure entity state TTL per dataset/stream
- [ ] **OPS-03**: Incremental snapshot serialization only writes changed entities since last snapshot
- [ ] **OPS-04**: Snapshot restore handles incremental format (base + deltas)

### Debug UI

- [ ] **DBUI-01**: User can view stream topology DAG in a web UI served from the existing HTTP port
- [ ] **DBUI-02**: User can see real-time throughput (messages/sec) per stream
- [ ] **DBUI-03**: User can inspect current feature values for any entity key
- [ ] **DBUI-04**: User can see memory usage breakdown (per stream, total)
- [ ] **DBUI-05**: Debug UI is embedded in the binary (no separate process or npm build)

## v1.2+ Requirements

Deferred to future release. Tracked but not in current roadmap.

### Event Log

- **ELOG-F1**: Event log compaction with merge (beyond TTL-based deletion)

### Schema Evolution

- **SCHM-F1**: Live schema migration of running operators (change window size)

### Pipeline

- **PIPE-F1**: Complex DAG transformations (map, filter, flatMap on keyless streams)

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Full WAL replacing snapshots | Event log is opt-in per stream; mandatory fsync kills latency for all streams |
| Point-in-time historical replay | Turns Tally from serving system to storage/analytics system |
| Distributed event log replication | Single-node by design; back up snapshots externally |
| Kafka-compatible event log API | Tally consumes, doesn't produce; event log is internal |
| Temporal joins with watermarks | Flink-level complexity; LEFT JOIN + lookups sufficient for feature serving |
| Key-partitioned multi-threading | v1 is single-threaded; sharding is a future vertical scaling upgrade |
| Cluster mode / distributed operation | Single-node by design |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| PIPE-01 | Phase 7 | Complete |
| PIPE-02 | Phase 7 | Complete |
| PIPE-03 | Phase 7 | Pending |
| PIPE-04 | Phase 7 | Pending |
| PIPE-05 | Phase 7 | Complete |
| ELOG-01 | Phase 6 | Complete |
| ELOG-02 | Phase 6 | Complete |
| ELOG-03 | Phase 6 | Complete |
| ELOG-04 | Phase 6 | Complete |
| ELOG-05 | Phase 6 | Complete |
| SCHM-01 | Phase 8 | Pending |
| SCHM-02 | Phase 8 | Pending |
| SCHM-03 | Phase 8 | Pending |
| SCHM-04 | Phase 8 | Pending |
| SCHM-05 | Phase 8 | Pending |
| OPS-01 | Phase 6 | Complete |
| OPS-02 | Phase 6 | Complete |
| OPS-03 | Phase 9 | Pending |
| OPS-04 | Phase 9 | Pending |
| DBUI-01 | Phase 10 | Pending |
| DBUI-02 | Phase 10 | Pending |
| DBUI-03 | Phase 10 | Pending |
| DBUI-04 | Phase 10 | Pending |
| DBUI-05 | Phase 10 | Pending |

**Coverage:**
- v1.1 requirements: 24 total
- Mapped to phases: 24
- Unmapped: 0

---
*Requirements defined: 2026-04-09*
*Last updated: 2026-04-09 after roadmap creation*
