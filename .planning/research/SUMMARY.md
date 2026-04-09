# Project Research Summary

**Project:** Tally (Streamlet) — Real-Time Feature Server
**Domain:** Single-binary streaming feature server (Rust + Python SDK)
**Researched:** 2026-04-09
**Confidence:** HIGH

## Executive Summary

Tally is a real-time feature server: a single Rust binary that ingests events over a custom TCP protocol, computes stateful windowed aggregations and derived expressions in-memory, and returns updated features synchronously in the same request-response cycle. The domain is well-understood — the design sits between Redis (in-memory key-value serving) and Flink/Tecton (streaming aggregations) — and the key architectural decisions are validated by production patterns from tokio/mini-redis, Arroyo's sliding window approach, and the HyperLogLog literature. The recommended approach is a single-threaded tokio current_thread event loop with Rc<RefCell<Engine>> state ownership, bucketed ring buffers for windowed operators, a Pratt-parsed expression AST evaluated at event time, and periodic postcard snapshots for crash recovery. This is the Redis RDB model applied to a streaming feature engine.

The most important differentiator — synchronous push-through (POST event, get computed features back in the same response) — is also the core architectural constraint that drives every other decision: single-threaded runtime, no external dependencies, binary TCP protocol, in-memory state only. Competitors (Feast, Tecton, Hopsworks) are all eventually consistent and require multi-service infrastructure. Tally's value proposition is valid and underserved, particularly for fraud detection and ML teams at companies too small for the Tecton-scale stack.

The highest-risk areas are not algorithmic — they are implementation-time traps: snapshot serialization blocking the async event loop, processing-time vs. event-time window semantics, HyperLogLog lacking sliding-window semantics, and bincode's lack of schema evolution (resolved by switching to postcard). All of these have clear preventions that must be baked in from Phase 1, not retrofitted. The pitfalls research is unusually complete and high-confidence; treat it as a mandatory checklist during implementation.

## Key Findings

### Recommended Stack

The Rust stack is well-settled. Use tokio 1.x with Builder::new_current_thread() for the single-threaded event loop — this is the direct equivalent of Redis's event loop model and eliminates all lock overhead on the hot path. Use postcard (not bincode) for snapshot serialization; bincode carries a critical security advisory (RUSTSEC-2025-0141) and is unmaintained. Use winnow for the expression parser (evolved from nom, inline combinators, no grammar files). Use axum 0.8 for the HTTP management API on port 6401 — it runs on the same current_thread runtime with zero context-switch overhead. Replace all std::collections::HashMap instances with ahash's AHashMap on the hot path to avoid SipHash's 20-25% CPU overhead at 100K+ events/sec. Implement HyperLogLog directly in src/engine/hll.rs — all external HLL crates either require nightly or are minimally maintained; the algorithm is ~100 lines of Rust.

The Python SDK uses only stdlib (socket, struct, threading) with zero dependencies. This preserves the "pip install tally, no Rust build step" story. The SDK never touches the hot path — it serializes stream definitions to JSON at registration time, then sends binary-framed TCP commands.

**Core technologies:**
- tokio 1.x (current_thread): Async runtime, TCP server — Redis-like single-threaded event loop, no lock overhead
- postcard 1.1.3: Snapshot serialization — stable wire format, serde-compatible, replaces unmaintained bincode
- axum 0.8: HTTP management API — same tokio runtime, no context switch; tower middleware for /metrics, /health
- serde + serde_json 1.x: Serialization framework + JSON for event payloads and pipeline definitions
- ahash 0.8: Fast HashMap hasher — DoS-resistant, 2-5x faster than SipHash on string entity keys
- winnow 0.6: Expression parser for derive/where clauses — inline combinators, no grammar files, fast
- bytes 1.x: BytesMut for TCP frame buffering — zero-copy slicing via reference counting
- thiserror 2.x + anyhow 1.x: Typed errors in engine/state modules; contextual propagation in main.rs
- tracing + tracing-subscriber: Async-aware structured logging — span context propagates through tokio tasks
- prometheus-client 0.22: Metrics exposition — official OpenMetrics implementation without global state
- criterion 0.7: Statistical benchmarking — stable Rust, generates confidence intervals for throughput/latency benches
- Python stdlib only: TCP client with zero-dependency install story

### Expected Features

The feature landscape is researched against Feast, Tecton, Hopsworks, and Redis. The P1/P2/P3 breakdown is clear.

**Must have (table stakes):**
- Windowed count, sum, avg operators — core velocity features; every fraud detection system needs these three
- Expression parser + derive evaluation — failure_rate = failed_tx_30m / tx_count_30m; without this, Tally is just a counter
- TCP server with binary protocol (PUSH, GET, SET, MSET, REGISTER) — the hot path
- Synchronous push-through — the entire value proposition; other systems are eventually consistent
- Pipeline REGISTER command — stream schema must be known before events arrive
- Python SDK with @st.stream decorator and TCP client — ML engineers won't adopt a server-only tool
- Snapshot persistence + crash recovery — "zero ops" promise fails without crash recovery
- TTL-based key eviction — unbounded memory is a production blocker
- HTTP health check endpoint — required for k8s readiness probes and load balancers
- Direct feature write (SET/MSET) — offline-computed features (lifetime value, segments) must be injectable

**Should have (competitive):**
- min/max operators — outlier detection ("largest transaction in 24h")
- distinct_count via HyperLogLog — unique_merchants per window; strong fraud signal; ~12KB per key
- last operator — context features (last_country, last_merchant); simple single-value state
- Where-clause filtered aggregations — failed_tx_30m = count(where="status == 'failed'"); same expression evaluator
- Cross-stream views — UserRisk.tx_to_login_ratio; multi-stream derived features without ETL
- Cross-key lookups — merchant state enrichment on user event; highest complexity differentiator
- Event fan-out to multiple streams — one event updates both user and merchant state atomically
- MSET chunked cooperative yielding — bulk batch ingestion without blocking live PUSH/GET
- Full HTTP management API (pipeline CRUD, metrics, debug endpoints)

**Defer (v2+):**
- Key-partitioned multi-threading — single thread handles 100K+ events/sec; vertical scaling first
- Schema evolution without restart — design once base schema is stable in production
- Session windows — niche; sliding windows cover fraud detection patterns well
- Multi-tenancy / namespace isolation — run separate instances per tenant for v1
- Client-side sharding documentation — needed only once users outgrow a single node

### Architecture Approach

The architecture is a single-threaded async server with clear ownership boundaries: Engine owns StateStore and PipelineRegistry via direct fields, exposed to connection tasks as Rc<RefCell<Engine>> with a strict rule that borrows are never held across .await points. The HTTP management API runs as a spawn_local task on the same runtime and communicates with the engine via tokio::sync::mpsc + oneshot channels (request-reply pattern) because it cannot share a RefCell borrow across task boundaries. The snapshot task clones engine state while holding a brief RefCell borrow, then hands the clone to tokio::task::spawn_blocking for disk I/O. The build order is fully specified: types -> window -> hll -> operators -> expression -> pipeline -> state/store -> state/snapshot -> state/eviction -> dispatch -> view -> protocol -> connection -> tcp -> http -> main -> python SDK.

**Major components:**
1. TCP Listener + Connection (server/tcp.rs, server/connection.rs) — frame parsing with BytesMut + BufWriter, dispatch to engine; Connection struct pattern from tokio tutorial
2. Pipeline Engine (engine/dispatch.rs, engine/pipeline.rs, engine/view.rs) — hot path: event fan-out, operator update, derive evaluation, cross-stream view resolution, cross-key lookup; owns StateStore
3. State Store (state/store.rs) — HashMap<EntityKey, EntityState> with AHashMap hasher; single owner, no locks; EntityState holds live (operator) and static (SET/MSET) features per key
4. Operator implementations (engine/operators.rs, engine/window.rs, engine/hll.rs) — bucketed ring buffer for count/sum/avg/min/max; custom HyperLogLog for distinct_count; Last as single value + timestamp
5. Expression evaluator (engine/expression.rs) — Pratt parser to AST at REGISTER time; tree-walk evaluation at event time with no allocation; handles derive and where clauses
6. Snapshot + Eviction (state/snapshot.rs, state/eviction.rs) — periodic postcard serialization with atomic file rename; TTL eviction background task scanning last_event_at
7. HTTP Management API (server/http.rs) — axum router on port 6401; pipeline CRUD, /health, /metrics, /debug/key/:key; shared Arc<Metrics> with TCP path via atomics
8. Python SDK (python/streamlet/) — pure stdlib TCP client with connection pool; @st.stream/@st.view decorators serialize definitions to JSON for REGISTER command

### Critical Pitfalls

Research identified 11 critical and moderate pitfalls. Top 5 by severity and phase impact:

1. **Snapshot serialization blocks event loop** — Use tokio::task::yield_now().await between chunks of ~1024 keys during serialization, OR clone state then spawn_blocking for disk I/O. Never serialize synchronously in an async context on current_thread. Design into Phase 4 from the start — retrofitting cooperative yielding into a monolithic serializer is harder than writing it correctly initially.

2. **Processing-time vs. event-time window semantics** — Accept an optional _timestamp (Unix milliseconds) field in PUSH events from day one. Use SystemTime (not Instant) for all window bucket calculations so server-side timestamps are comparable to client-supplied Unix timestamps. This is a wire-format decision that is extremely expensive to change retroactively. Must be in Phase 1 + Phase 2.

3. **Divide-by-zero panics on cold start** — Derive expressions like failed_tx_30m / tx_count_30m divide by zero on first event for a new key. The expression evaluator must return FeatureValue::Missing (not panic, not NaN) for any division by zero or access to a missing field. NaN propagates silently into ML models; Missing propagates explicitly. Must be in Phase 1.

4. **Snapshot schema evolution (bincode trap)** — Resolved by using postcard instead of bincode. Still: embed an explicit SNAPSHOT_FORMAT_VERSION: u8 = 1 as the first byte of every snapshot file. On startup, version mismatch = start fresh, not panic. Write a migration test. Must be in Phase 4.

5. **HyperLogLog lacks sliding window semantics** — A naive HLL accumulates elements forever; it cannot subtract expired events. Implement epoch-based rotation: N HLL sketches (one per bucket), union non-expired sketches on read. Memory cost is N x 12KB per key per distinct_count feature. Design HLL data structure and window semantics together in Phase 5.

## Implications for Roadmap

Based on the dependency graph in FEATURES.md and the build order in ARCHITECTURE.md, the natural phase structure is:

### Phase 1: Core Engine
**Rationale:** The state store, windowed operators, and expression evaluator are the foundation everything else builds on. They have no external dependencies and are fully unit-testable without a running server. The most consequential correctness decisions (time representation, Missing semantics, hasher choice) must be made here — they are extremely expensive to change later.
**Delivers:** In-memory state store with AHashMap; BucketedWindow ring buffer for count/sum/avg; Pratt-parsed expression AST with tree-walk evaluator; FeatureValue::Missing for null/divide-by-zero; SystemTime-based window buckets.
**Addresses:** In-memory state store, windowed count/sum/avg, derive expression evaluation (FEATURES.md P1)
**Avoids:** SipHash throughput bottleneck (AHashMap from day one), divide-by-zero panics (Missing semantics in evaluator), processing-time/event-time skew (SystemTime from day one)

### Phase 2: TCP Server and Binary Protocol
**Rationale:** The engine can now process events; it needs a network interface. TCP protocol correctness (frame parsing, partial reads) must be solved before the Python SDK is written, since the SDK depends on the wire format being stable.
**Delivers:** tokio current_thread server; TcpListener + Connection with BytesMut + BufWriter; PUSH, GET, SET, MSET, REGISTER commands; binary protocol with length-prefixed frames; per-connection write timeouts; maximum frame length enforcement; MSET chunked cooperative yielding with explicit yield_now.
**Addresses:** TCP server + binary protocol, synchronous push-through, pipeline REGISTER, SET/MSET (FEATURES.md P1)
**Avoids:** TCP partial frame reads (read_exact, not read), slow client OOM (write timeouts), MSET starving hot path (explicit yield_now)

### Phase 3: Python SDK
**Rationale:** The wire protocol is now stable. ML engineers are the primary users — they need to define streams and push events in Python before the product can be validated. The SDK has no server-side dependencies beyond the already-built protocol.
**Delivers:** @st.stream and @st.view decorators; operator classes (st.count, st.sum, st.avg, st.derive, st.lookup); TCP client with persistent connection pool; REGISTER serialization; typed FeatureResult objects; @st.stream mixins for composable feature groups.
**Addresses:** Python SDK (FEATURES.md P1); reusable mixins (FEATURES.md differentiator)
**Avoids:** Per-request TCP connection overhead (connection pool from the start), Python/Rust endianness mismatch (protocol conformance test)

### Phase 4: Persistence and Operational Readiness
**Rationale:** The server now works end-to-end but does not survive restarts. Snapshot persistence, TTL eviction, HTTP management API, and health/metrics endpoints are needed to call this production-ready. The snapshot design is complex and must be done correctly.
**Delivers:** Periodic postcard snapshots with cooperative yielding and atomic rename; snapshot recovery on startup; SNAPSHOT_FORMAT_VERSION byte; TTL-based key eviction background task; HTTP management API on port 6401 (pipeline CRUD, /health, /metrics, /debug/key/:key, /snapshot); Prometheus metrics.
**Addresses:** Snapshot persistence + crash recovery, HTTP health check, HTTP management API, Prometheus metrics (FEATURES.md P1 + P2)
**Avoids:** Snapshot blocking event loop (chunked yield or spawn_blocking), snapshot corruption on code change (version byte + migration test), snapshot file corruption (atomic rename)

### Phase 5: Remaining Operators and Advanced Features
**Rationale:** The core product is validated. Add the remaining operators (min, max, last, distinct_count) and complex cross-stream features (views, lookups, fan-out). HyperLogLog and cross-key lookups are last because they have the most complex semantics.
**Delivers:** min and max operators; last operator; HyperLogLog distinct_count with epoch-based window rotation; where-clause filtered aggregations (reuses existing expression evaluator); cross-stream views; cross-key lookups with Missing propagation; event fan-out to multiple streams.
**Addresses:** min/max, distinct_count, last, where-clause filtering, cross-stream views, cross-key lookups, event fan-out (FEATURES.md P2)
**Avoids:** HLL growing monotonically without windowing (epoch rotation designed in), cross-key lookup on evicted key panicking (Missing semantics)

### Phase Ordering Rationale

- Engine before server: Operators and expression evaluator are fully testable without networking. Correctness invariants (Missing semantics, time representation) are far cheaper to establish here than to retrofit.
- Protocol before SDK: The Python SDK encodes the wire format. Any protocol change after the SDK ships requires a version bump and client migration. Stabilize the format in Phase 2.
- Persistence after end-to-end works: Snapshot design requires all core operator types to be defined (stable serde schema). Adding persistence before operators are stable risks multiple snapshot format versions.
- Complex operators last: Cross-key lookups depend on cross-stream views, which depend on working derives. HyperLogLog windowing is algorithmically independent but operationally complex — validate simpler operators in production first.
- Pitfall avoidance drives ordering: The processing-time vs event-time pitfall and the Missing semantics pitfall both require Phase 1 decisions. The snapshot blocking pitfall requires Phase 4 design. The HLL windowing pitfall requires Phase 5 design.

### Research Flags

Phases likely needing deeper research during planning:
- **Phase 5 (HLL windowing):** Epoch-based HLL rotation for sliding window distinct_count is algorithmically non-trivial. The memory profile (N buckets x 12KB per key) needs to be validated against target scale before committing. Research the tumbling window fallback as a documented alternative.
- **Phase 5 (Cross-key lookups):** The dependency between entity key TTL eviction and cross-key lookup Missing propagation creates subtle state coupling. Specify the semantics precisely before implementation.
- **Phase 2 (Connection backpressure):** Simple write timeout may not be sufficient for all production scenarios. Research tokio backpressure patterns (LengthDelimitedCodec, per-connection buffer caps) before finalizing connection handler.

Phases with standard patterns (skip research-phase):
- **Phase 1 (Core Engine):** Bucketed ring buffers, Pratt parsing, and AHashMap are well-documented with canonical implementations. ARCHITECTURE.md provides concrete code patterns.
- **Phase 2 (TCP Server):** The tokio mini-redis pattern (Connection struct, BytesMut, spawn_local) is fully documented in official tokio tutorials.
- **Phase 3 (Python SDK):** Pure stdlib TCP client with struct.pack framing is straightforward. Main risk is protocol conformance, addressed by a conformance test.
- **Phase 4 (Persistence):** postcard + atomic rename + spawn_blocking is a documented pattern. Versioning scheme is simple (one byte). Criterion benchmarks are standard.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | Core crates verified against official sources. postcard over bincode has RUSTSEC advisory. winnow over nom has lineage documented. One MEDIUM gap: Python connection pooling specifics (threading.local vs threading.Lock — both work, implementation detail). |
| Features | HIGH | Corroborated against Tecton, Feast, Hopsworks, Redis, Fennel documentation. P1/P2/P3 split derived from the dependency graph, not arbitrary. One note: where-clause filtered aggregations is marked P2 but has near-zero incremental cost once the expression evaluator exists — roadmapper may want to pull it into Phase 1. |
| Architecture | HIGH | Patterns sourced from tokio official docs, mini-redis canonical implementation, Arroyo sliding window blog, Cloudflare interpreter post. Rc<RefCell<Engine>> with LocalSet is well-understood. One open question: HTTP-to-engine mpsc channel adds latency to pipeline registration — acceptable for management API, worth documenting. |
| Pitfalls | HIGH | 11 pitfalls with specific prevention strategies, phase assignments, and verification tests. All core pitfalls verified against official Tokio docs and production post-mortems. Processing-time vs event-time and bincode schema evolution have the highest recovery cost if missed — both have clear preventions in the phase structure. |

**Overall confidence:** HIGH

### Gaps to Address

- **HLL windowing approach:** Epoch-based rotation vs. tumbling window documentation needs a concrete decision before Phase 5. The memory math (N buckets x 12KB x key count) must be validated against the target scale. Add a spike task at the start of Phase 5.
- **REGISTER authentication:** Unrestricted REGISTER allows any client to redefine pipelines. For v1, restricting REGISTER to the HTTP management port (6401) is sufficient. Confirm this is the intended access control model before Phase 2.
- **Snapshot memory spike:** Cloning full state for spawn_blocking creates up to 2x peak memory. For 1M keys at 5KB average = 10GB during snapshot. If the memory budget is tight, chunked cooperative yielding (in-task, no clone) is safer. Make this decision explicit in Phase 4.
- **String comparisons in expression language:** The where clause examples require string equality (status == 'failed'), but the expression grammar section only lists numeric types. Confirm whether string comparison is in scope for Phase 1 or Phase 2.

## Sources

### Primary (HIGH confidence)
- tokio docs.rs 1.51.0 — current_thread runtime, LocalSet, spawn_blocking, yield_now, framing tutorial
- tokio-rs/mini-redis (GitHub) — canonical Connection struct, BytesMut, BufWriter pattern
- bincode RUSTSEC-2025-0141 advisory — unmaintained status confirmed
- postcard crates.io v1.1.3 — stable wire format, serde-compatible
- axum 0.8 announcement (tokio.rs blog) — v0.8 requires hyper 1.x confirmed
- Arroyo: 10x faster sliding windows blog — bucketed ring buffer approach validated
- Tokio cooperative task yielding blog post — yield_now, operation budget, spawn_blocking patterns

### Secondary (MEDIUM confidence)
- Rust web frameworks 2026 comparison — axum dominance confirmed
- Rust serialization benchmark (djkoloski) — postcard vs bincode vs rkyv comparison
- hyperloglog-rs lib.rs — nightly requirement confirmed (drives custom HLL implementation decision)
- Tecton, Feast, Hopsworks, Fennel documentation — feature landscape and competitor analysis
- Cloudflare: Building fast interpreters in Rust — AST evaluation performance patterns
- Tokio top runtime mistakes (techbuddies.io) — pitfall verification

### Tertiary (LOW confidence)
- Various streaming feature store blog posts — market positioning validation
- Training-serving skew articles — event-time vs processing-time context

---
*Research completed: 2026-04-09*
*Ready for roadmap: yes*
