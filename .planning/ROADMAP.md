# Roadmap: Tally

## Overview

Build Tally from a greenfield Rust project to a complete, single-binary real-time feature server with Python SDK. The journey follows a strict dependency order: core engine first (state store, windowed operators, expression evaluator), then the TCP server that exposes it to the network, then the Python SDK that makes it usable by ML engineers, then persistence and operational readiness, and finally the advanced operators and cross-stream features that distinguish Tally from a plain counter service. Each phase delivers a coherent, independently testable capability that unlocks the next.

## Phases

**Phase Numbering:**
- Integer phases (1, 2, 3): Planned milestone work
- Decimal phases (2.1, 2.2): Urgent insertions (marked with INSERTED)

Decimal phases appear between their surrounding integers in numeric order.

- [ ] **Phase 1: Core Engine** - In-memory state store, windowed ring buffer, count/sum/avg operators, and expression evaluator with Missing semantics
- [ ] **Phase 2: TCP Server and Binary Protocol** - tokio current_thread TCP server with full command set (PUSH, GET, SET, MSET, REGISTER) and synchronous push-through
- [ ] **Phase 3: Python SDK** - @st.stream/@st.view decorators, operator classes, TCP client with connection pooling, and typed feature results
- [ ] **Phase 4: Persistence and Operational Readiness** - Postcard snapshots, crash recovery, TTL eviction, and HTTP management API with health/metrics/debug endpoints
- [ ] **Phase 5: Advanced Operators and Cross-Stream** - min/max/last/distinct_count operators, where-clause filtering, cross-stream views, cross-key lookups, and event fan-out

## Phase Details

### Phase 1: Core Engine
**Goal**: The foundational engine — in-memory state store, windowed aggregation ring buffer, core operators, and expression evaluator — is fully functional and unit-tested without any networking
**Depends on**: Nothing (first phase)
**Requirements**: ENG-01, ENG-02, ENG-03, ENG-04, ENG-05, ENG-06, ENG-07, ENG-08
**Success Criteria** (what must be TRUE):
  1. A Rust test can create an EntityState, push a sequence of timestamped events, and read count/sum/avg values that correctly reflect only events within the configured window
  2. The bucketed ring buffer correctly expires old buckets as time advances, and reads across multiple buckets produce the accurate aggregate
  3. A derive expression string (e.g. "failed_tx_30m / tx_count_30m") is parsed at registration time into an AST and evaluated at event time to produce a numeric result
  4. The expression evaluator returns FeatureValue::Missing (not a panic, not NaN) for division-by-zero and for access to fields not present in the current state
  5. All operator state uses AHashMap and SystemTime-based window buckets so that client-supplied Unix timestamps are handled correctly from day one
**Plans:** 4 plans
Plans:
- [x] 01-01-PLAN.md — Project skeleton, core types (FeatureValue, TallyError), and time-bucketed RingBuffer
- [x] 01-02-PLAN.md — Core operators (CountOp, SumOp, AvgOp) with Redis-strict type checking
- [x] 01-03-PLAN.md — Expression parser (winnow Pratt) and evaluator with Missing propagation
- [x] 01-04-PLAN.md — State store (EntityState, StateStore) and PipelineEngine push-through integration

### Phase 2: TCP Server and Binary Protocol
**Goal**: A running Tally server accepts persistent TCP connections, parses binary frames, dispatches all five commands (PUSH, GET, SET, MSET, REGISTER) to the engine, and returns updated features synchronously in the same response
**Depends on**: Phase 1
**Requirements**: SRV-01, SRV-02, SRV-03, SRV-04, SRV-05, SRV-06, SRV-07, SRV-08
**Success Criteria** (what must be TRUE):
  1. A raw TCP client can connect to port 6400, send a length-prefixed REGISTER frame with a JSON pipeline definition, and receive an OK response
  2. After REGISTER, sending a PUSH frame returns a JSON map of all feature values for the entity key, computed synchronously before the response is sent
  3. GET returns the current feature map for a key; SET writes a static feature; both work correctly across separate TCP connections
  4. An MSET with 10,000 entries completes without starving concurrent PUSH/GET requests (cooperative yielding is observable via interleaved response timing)
  5. The HTTP management API on port 6401 responds to GET /health with a 200 OK
**Plans:** 5 plans
Plans:
- [x] 02-01-PLAN.md — Protocol layer: frame parsing, string encoding, command opcodes, REGISTER DTO, FeatureValue JSON conversion
- [x] 02-02-PLAN.md — TCP server with connection handler and command dispatch (PUSH, GET, SET, MSET, REGISTER)
- [x] 02-03-PLAN.md — HTTP health endpoint, main.rs entry point, and integration tests for all SRV-* requirements
- [x] 02-04-PLAN.md — Gap closure: unit tests for protocol error branches and types public API (G-02, G-04, G-05, G-06, G-08, G-09, G-10)
- [x] 02-05-PLAN.md — Gap closure: integration tests for server edge cases and behavioral coverage (G-01, G-03, G-07, G-11, G-12, G-13)
**UI hint**: no

### Phase 3: Python SDK
**Goal**: An ML engineer can define streams in Python using decorators, register them with the server, push events, and receive typed feature results — all without writing Rust or touching the wire protocol directly
**Depends on**: Phase 2
**Requirements**: SDK-01, SDK-02, SDK-03, SDK-04, SDK-05, SDK-06, SDK-07
**Success Criteria** (what must be TRUE):
  1. A Python script using @st.stream with count/sum/avg/derive operators can be registered and push events that return a typed FeatureResult object with named attribute access (features.tx_count_30m)
  2. @st.view with cross-stream derive expressions serializes correctly to JSON and registers successfully with the server
  3. All operator classes (st.count, st.sum, st.avg, st.min, st.max, st.distinct_count, st.last, st.derive, st.lookup) serialize to valid JSON pipeline definitions
  4. app.get(), app.set(), and app.mset() all work correctly against a running server with persistent pooled connections
  5. A conformance test verifies that the Python client's binary encoding matches the Rust server's expected wire format byte-for-byte
**Plans:** 4 plans
Plans:
- [x] 03-01-PLAN.md — Project skeleton, types (FeatureResult, exceptions), and binary protocol encoding with byte-level conformance tests
- [x] 03-02-PLAN.md — Operator descriptor classes, @stream/@view decorators with metaclass and mixin support
- [x] 03-03-PLAN.md — TCP client with auto-reconnect and App class (register/push/get/set/mset)
- [x] 03-04-PLAN.md — End-to-end integration tests against live Tally server

### Phase 4: Persistence and Operational Readiness
**Goal**: Tally survives restarts (snapshot persistence + crash recovery), reclaims memory for idle keys (TTL eviction), and exposes enough observability for production use (HTTP management API with pipeline CRUD, metrics, and debug endpoints)
**Depends on**: Phase 3
**Requirements**: PERS-01, PERS-02, PERS-03, PERS-04, PERS-05, SRV-08
**Success Criteria** (what must be TRUE):
  1. After pushing events to a running server, killing and restarting the process, and waiting for startup, GET returns feature values that reflect the pre-crash state (within the snapshot interval)
  2. The snapshot write never blocks PUSH/GET for more than a single event cycle — concurrent pushes during a snapshot are observable and correctly queued
  3. Entity keys that receive no events for 2x their largest window are automatically removed from memory; confirmed via GET returning empty and a decreasing memory metric
  4. GET /pipelines on port 6401 returns the registered pipeline definitions; GET /debug/key/:key returns full operator state internals; GET /metrics returns Prometheus-format counters
  5. Starting Tally with a snapshot from a different format version (bumped SNAPSHOT_FORMAT_VERSION) results in a clean startup from empty state, not a panic
**Plans:** 3 plans
Plans:
- [x] 04-01-PLAN.md — OperatorState enum refactor, snapshot save/load with postcard + versioning, TTL eviction logic
- [x] 04-02-PLAN.md — main.rs snapshot recovery, periodic snapshot/eviction timers, integration tests
- [x] 04-03-PLAN.md — HTTP management API: pipeline CRUD, metrics, debug, snapshot endpoints
**UI hint**: no

### Phase 5: Advanced Operators and Cross-Stream
**Goal**: All operators are implemented (min, max, last, distinct_count with windowed HLL), where-clause filtering is available, and cross-stream views with cross-key lookups and event fan-out work correctly
**Depends on**: Phase 4
**Requirements**: OPS-01, OPS-02, OPS-03, OPS-04, OPS-05, XSTR-01, XSTR-02, XSTR-03
**Success Criteria** (what must be TRUE):
  1. min and max operators return the correct extrema over the configured window, expiring old buckets as time advances; last returns the most recent field value with its timestamp
  2. distinct_count with epoch-based HLL rotation returns an approximate unique count that reflects only events within the window (not a monotonically growing total)
  3. A where-clause filtered aggregation (e.g. count(window="30m", where="status == 'failed'")) counts only events matching the filter, verified against a mixed event stream
  4. A @st.view that derives a feature from two streams (e.g. Transactions.tx_count_1h / Logins.login_count_1h) returns the correct combined value after pushing events to both streams
  5. A single PUSH event containing both user_id and merchant_id updates state for both entity keys, and a st.lookup feature on the user's view correctly reads the merchant's current feature value
**Plans**: TBD

## Progress

**Execution Order:**
Phases execute in numeric order: 1 → 2 → 3 → 4 → 5

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Core Engine | 0/4 | Planning complete | - |
| 2. TCP Server and Binary Protocol | 3/5 | Gap closure planned | - |
| 3. Python SDK | 0/4 | Planning complete | - |
| 4. Persistence and Operational Readiness | 0/3 | Planning complete | - |
| 5. Advanced Operators and Cross-Stream | 0/TBD | Not started | - |
