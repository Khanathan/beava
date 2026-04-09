# Requirements: Tally

**Defined:** 2026-04-09
**Core Value:** Events go in, features come out — synchronously, in one request-response cycle, with sub-millisecond latency and zero external dependencies.

## v1 Requirements

Requirements for initial release. Each maps to roadmap phases.

### Core Engine

- [x] **ENG-01**: Server maintains in-memory state store (HashMap<EntityKey, EntityState>) with live and static features
- [x] **ENG-02**: Sliding windows use bucketed ring buffer with configurable bucket granularity
- [x] **ENG-03**: count operator tracks event count within a time window
- [x] **ENG-04**: sum operator accumulates a numeric field within a time window
- [x] **ENG-05**: avg operator computes running average of a numeric field within a time window
- [x] **ENG-06**: Expression evaluator parses derive/where expressions at registration time into AST
- [x] **ENG-07**: Expression evaluator supports arithmetic (+, -, *, /), comparison (>, <, >=, <=, ==, !=), boolean (and, or, not), field access (field, Stream.field, _event.field), and builtins (abs, min, max, now)
- [x] **ENG-08**: Expression evaluator returns Missing on division-by-zero or missing inputs (no panics)

### Server Protocol

- [ ] **SRV-01**: TCP server accepts persistent connections on configurable port (default 6400)
- [ ] **SRV-02**: Binary protocol uses length-prefixed frames (4-byte u32 BE length + 1-byte opcode + payload)
- [ ] **SRV-03**: PUSH command ingests event to a stream and returns updated features synchronously
- [ ] **SRV-04**: GET command returns all current features for an entity key
- [ ] **SRV-05**: SET command writes static feature values for a key
- [ ] **SRV-06**: MSET command bulk-writes with cooperative yielding (chunked, non-blocking)
- [ ] **SRV-07**: REGISTER command accepts pipeline definitions as JSON
- [ ] **SRV-08**: HTTP management API serves health, metrics, debug, and pipeline CRUD on separate port (default 6401)

### Advanced Operators

- [ ] **OPS-01**: min operator tracks minimum value of a field within a time window
- [ ] **OPS-02**: max operator tracks maximum value of a field within a time window
- [ ] **OPS-03**: last operator stores most recent value of a field with timestamp
- [ ] **OPS-04**: distinct_count operator uses HyperLogLog with epoch-rotation for windowed approximate unique counts
- [ ] **OPS-05**: where-clause filtering supports conditional aggregation (e.g. count events where status == 'failed')

### Cross-Stream

- [ ] **XSTR-01**: @st.view computes derived features across multiple streams for the same entity key
- [ ] **XSTR-02**: st.lookup resolves cross-key feature references (e.g. merchant chargebacks for a user's transaction)
- [ ] **XSTR-03**: Single event fans out to update multiple streams when it contains keys for each

### Python SDK

- [ ] **SDK-01**: @st.stream decorator defines a stream with key field and feature declarations
- [ ] **SDK-02**: @st.view decorator defines cross-stream views with derive expressions
- [ ] **SDK-03**: Operator classes (st.count, st.sum, st.avg, st.min, st.max, st.distinct_count, st.last, st.derive, st.lookup) serialize to JSON
- [ ] **SDK-04**: TCP client with connection pooling communicates via binary protocol
- [ ] **SDK-05**: app.push() sends event and returns typed feature results
- [ ] **SDK-06**: app.get(), app.set(), app.mset() for read/write operations
- [ ] **SDK-07**: app.register() sends pipeline definitions to server

### Persistence

- [ ] **PERS-01**: Periodic snapshot serialization of full state to local file (default every 30s)
- [ ] **PERS-02**: Snapshot uses postcard + serde with versioned format (version byte per snapshot)
- [ ] **PERS-03**: Server loads latest snapshot on startup for crash recovery
- [ ] **PERS-04**: Snapshot write uses cooperative yielding to avoid blocking the event loop
- [ ] **PERS-05**: TTL-based key eviction removes inactive keys (default: 2x largest window)

## v2 Requirements

Deferred to future release. Tracked but not in current roadmap.

### Optimization

- **OPT-01**: Incremental snapshot serialization (chunked or COW data structures)
- **OPT-02**: Batch GET endpoint (MGET) for bulk feature reads
- **OPT-03**: Schema evolution (add/remove features without full state reset)

### Operations

- **OPS2-01**: Multi-tenancy / namespace isolation
- **OPS2-02**: Connection and stream count limits
- **OPS2-03**: Bundled binary distribution (pip install tally)

## Out of Scope

Explicitly excluded. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Key-partitioned multi-threading | v1 is single-threaded; sharding is a future vertical scaling upgrade |
| Cluster mode / distributed operation | Single-node by design; destroys zero-ops promise |
| Session windows | Too complex, niche; sliding windows cover fraud detection patterns |
| WAL / full durability | Every PUSH write to disk violates <100us p99 latency target |
| Point-in-time historical replay | Changes system from serving to storage — fundamentally different product |
| OAuth / authentication on TCP port | Management-only concern; TCP hot path must stay minimal |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| ENG-01 | Phase 1 | Complete |
| ENG-02 | Phase 1 | Complete |
| ENG-03 | Phase 1 | Complete |
| ENG-04 | Phase 1 | Complete |
| ENG-05 | Phase 1 | Complete |
| ENG-06 | Phase 1 | Complete |
| ENG-07 | Phase 1 | Complete |
| ENG-08 | Phase 1 | Complete |
| SRV-01 | Phase 2 | Pending |
| SRV-02 | Phase 2 | Pending |
| SRV-03 | Phase 2 | Pending |
| SRV-04 | Phase 2 | Pending |
| SRV-05 | Phase 2 | Pending |
| SRV-06 | Phase 2 | Pending |
| SRV-07 | Phase 2 | Pending |
| SRV-08 | Phase 4 | Pending |
| OPS-01 | Phase 5 | Pending |
| OPS-02 | Phase 5 | Pending |
| OPS-03 | Phase 5 | Pending |
| OPS-04 | Phase 5 | Pending |
| OPS-05 | Phase 5 | Pending |
| XSTR-01 | Phase 5 | Pending |
| XSTR-02 | Phase 5 | Pending |
| XSTR-03 | Phase 5 | Pending |
| SDK-01 | Phase 3 | Pending |
| SDK-02 | Phase 3 | Pending |
| SDK-03 | Phase 3 | Pending |
| SDK-04 | Phase 3 | Pending |
| SDK-05 | Phase 3 | Pending |
| SDK-06 | Phase 3 | Pending |
| SDK-07 | Phase 3 | Pending |
| PERS-01 | Phase 4 | Pending |
| PERS-02 | Phase 4 | Pending |
| PERS-03 | Phase 4 | Pending |
| PERS-04 | Phase 4 | Pending |
| PERS-05 | Phase 4 | Pending |

**Coverage:**
- v1 requirements: 36 total
- Mapped to phases: 36
- Unmapped: 0

Note: REQUIREMENTS.md header previously stated 30 requirements; the actual count is 36 (8 ENG + 8 SRV + 5 OPS + 3 XSTR + 7 SDK + 5 PERS). All 36 are mapped.

---
*Requirements defined: 2026-04-09*
*Last updated: 2026-04-09 — traceability populated after roadmap creation*
