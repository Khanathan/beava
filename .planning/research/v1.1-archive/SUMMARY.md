# Project Research Summary

**Project:** Tally v1.1 — Composable Pipeline & Event Log
**Domain:** Real-time feature server — composable pipeline, SSD event log, backfill, schema evolution, incremental snapshots, debug UI
**Researched:** 2026-04-09
**Confidence:** HIGH

## Executive Summary

Tally v1.1 adds a composable pipeline execution model, an append-only SSD event log, backfill capability, schema evolution, and a debug web UI onto an already-proven v1.0 single-binary real-time feature server. The research is grounded in the existing ~8,400-line Rust codebase and draws from well-documented analogues: Redis AOF for the event log pattern, Apache Flink and RisingWave for composable DAG execution and backfill, and Tecton for feature backfill semantics. The recommended approach is to build in dependency order — event log first (foundation for everything), then DAG execution and keyless streams (composable pipeline core), then backfill and schema evolution (high complexity, depends on prior phases), then incremental snapshots and debug UI (optimizations/polish). No new infrastructure, no new major runtime changes; the existing current_thread tokio architecture is preserved throughout.

The single largest risk is latency regression on the PUSH hot path. Every new v1.1 feature touches the critical path: the event log appends on every PUSH, DAG execution adds pipeline evaluation, backfill competes for event loop time. The mitigation pattern is consistent across all three: buffered writes (never fsync on hot path), explicit cooperative yielding (rate-limit replay to 64 events per yield cycle), and mutual exclusion between background I/O tasks (snapshot and compaction never run concurrently). These patterns are not optional hardening — they must be the initial design, not retrofits.

The v1.1 feature set is uniquely differentiated: no other single-binary system has composable pipelines, SSD-backed event replay, automatic backfill on feature registration, and an embedded debug UI. The nearest competitors (Flink, RisingWave, Materialize) require clusters, object storage, or external sources. Tally's local-disk-only constraint is both its constraint and its competitive positioning. Research confidence is HIGH across all areas except incremental snapshots (MEDIUM interaction complexity — the design is clear, but recovery from missing base snapshots combined with schema evolution changes needs careful test coverage).

## Key Findings

### Recommended Stack

The existing v1.0 stack (tokio, serde/postcard, axum, ahash, winnow) remains unchanged. Four new crates are added for v1.1: `petgraph 0.8` for DAG construction and topological sort (O(V+E) with cycle detection, used by the Rust compiler toolchain itself), `crc32fast 1.5` for SIMD-accelerated per-record event log checksums, `rust-embed 8.11` for compiling the debug UI HTML/JS/CSS into the binary at build time (preserving the single-binary promise), and `tower-http 0.6` for CORS during debug UI development. The event log itself is hand-rolled using `std::fs + BufWriter` — the `commitlog` crate (v0.1.1, minimally maintained, mmap-based) is explicitly ruled out. Incremental snapshots require no new dependencies — they use the existing postcard serialization with a dirty-key HashSet. Add `"sync"` to the tokio feature flags to enable `broadcast::channel` for SSE debug streaming.

**Core new technologies (v1.1 additions):**
- `petgraph 0.8`: DAG construction and topological sort — O(V+E), cycle detection with offending node identification, future DAG visualization comes free
- `crc32fast 1.5`: Event log record checksums — SIMD-accelerated at multi-GB/s on x86/ARM, 335M+ downloads, negligible overhead at 100K events/sec
- `rust-embed 8.11`: Debug UI asset embedding — dev-mode filesystem fallback, native axum integration, zero extra deployment files
- `tower-http 0.6`: CORS for debug UI development — must match axum 0.8 (use v0.6, not v0.5)
- `std::fs + BufWriter` (stdlib): Event log segments — full control over fsync policy, rotation, and cooperative yielding

### Expected Features

The v1.1 feature set splits cleanly into P1 (must ship for milestone) and P2 (follow-on after core is validated). The critical insight from the dependency analysis: **SSD event log is the dependency root — keyless streams, backfill, and history TTL all require it.** Nothing else in v1.1 can be built without it.

**Must have (P1 — v1.1 milestone):**
- SSD append-only event log — opt-in per stream via `history=True`; `history_ttl` required at registration to prevent unbounded disk growth
- Keyless streams — raw event ingestion with no in-memory state; events write to log only; feeds downstream keyed streams via DAG
- Keyed streams with `depends_on` — explicit upstream dependency declarations; LEFT JOIN semantics (null for missing upstream values)
- DAG execution — topological sort at registration time; events cascade through pipeline automatically; cycle detection rejects invalid graphs
- History TTL per stream — bounded event log retention; defaults to the stream's largest window size
- Schema evolution (add features) — new operators initialized lazily on next event; no full state reset required
- Backfill from event log — replay historical events through new features; `backfill=True` flag; cooperative yielding to protect live traffic
- MGET — batch GET command (new TCP opcode); saves round-trips on ML inference paths; low implementation complexity

**Should have (P2 — after core validated):**
- Schema evolution (remove features) — less urgent than add; users rarely remove features in production
- Entity state TTL per dataset — extend existing global TTL to per-stream configuration
- Incremental snapshot serialization — dirty-key tracking + delta files; meaningful benefit at >1M keys
- Debug web UI — embedded SPA: DAG topology, throughput, memory, entity inspector, backfill progress
- Event log compaction — background segment rewrite; history TTL handles most cases initially as stopgap

**Defer to v1.2+:**
- Complex DAG transformations (map/filter/flatMap between stages) — `where` clause and `derive` cover most use cases
- Event log as external Kafka-compatible API — Tally consumes events, it does not distribute them
- Live schema migration (change window parameters without state reset) — reset + backfill is the supported path
- Batch PUSH (MPUSH) — PUSH in a loop + pipelining works for now

### Architecture Approach

The v1.1 architecture extends the existing `Arc<Mutex<AppState>>` single-threaded tokio pattern with three new subsystems added to `AppState`: `EventLog` (new `src/state/event_log.rs`, ~400 LOC), `DependencyGraph` (new `src/engine/dag.rs`, ~200 LOC), and `SchemaRegistry` (new `src/engine/schema.rs`, ~250 LOC). Two new background timers are added: fsync timer (1s interval) and log compaction timer (configurable). The hot path gains two new synchronous steps on PUSH: `event_log.append()` (buffered write, ~200ns) and DAG cascade evaluation. Total estimated new code: ~1,370 lines across 6 new files plus modifications to 8 existing files.

The `EntityState` structure requires a structural refactor **before any other v1.1 changes**: operators must be grouped by stream (from a flat `Vec<(String, OperatorState)>` to `HashMap<StreamName, StreamState>` where each `StreamState` has its own `last_event_at`). This refactor is the prerequisite for per-dataset TTL without cross-stream eviction conflicts, and it touches snapshot serialization, GET response assembly, and the PUSH handler simultaneously.

**Major components (new):**
1. `EventLog` (`src/state/event_log.rs`) — segmented append-only log, BufWriter per stream, Everysec/No fsync policy, compaction
2. `DependencyGraph` (`src/engine/dag.rs`) — petgraph DiGraph, topological sort, cycle detection at registration, cascade dispatch order
3. `SchemaRegistry` (`src/engine/schema.rs`) — feature signature hashing, diff-and-reconcile on re-register, migration sweep with yield_now
4. `BackfillEngine` (`src/engine/backfill.rs`) — log replay, 64 events/yield-cycle rate limit, live-first scheduling, `warming_up` status
5. `DebugUI` (`src/server/debug_ui.rs`) — rust-embed static assets, SSE broadcast channel, metrics snapshot polling at 10Hz

### Critical Pitfalls

1. **SSD event log fsync on hot path** — synchronous `fsync()` takes 200us-2ms on any SSD, instantly blowing the <100us PUSH p99 budget. Use `BufWriter` + `spawn_blocking` for periodic `fdatasync()` (every 1s or N events). Never start with synchronous writes "to get it working" — the refactor from sync to async changes durability semantics and all error handling paths.

2. **Backfill replay starves live traffic** — replaying 1M events at full speed consumes the single-threaded event loop for seconds to minutes. Rate-limit replay to 64 events per yield cycle. Check for pending live PUSH/GET before each batch. Mark features as `warming_up` in GET responses until backfill completes. Do not run replay in a separate thread — operator state is not thread-safe.

3. **Schema evolution corrupts in-memory operator state** — matching features by name only allows a changed-window operator to silently reuse stale ring buffer state. Compare features by signature hash (operator type + window duration + field + where_expr). Changed signature = drop and reinitialize. Apply migration sweep to ALL entity keys atomically with cooperative yielding.

4. **DAG dependency cycles cause infinite evaluation loops** — multi-stage composition allows cycles that the v1.0 two-level model structurally prevented. Run Kahn's algorithm at REGISTER time; reject cyclic graphs with a clear error naming the offending node. Store the topological order and use it exclusively for evaluation. Add depth limit (16 levels) as runtime safety net.

5. **Event log and snapshot I/O collision** — running periodic snapshots and log compaction concurrently saturates SSD write bandwidth. Implement mutual exclusion between background I/O tasks (only one heavy write operation at a time). Schedule compaction at the midpoint between snapshots. This is exactly the Redis model.

6. **Per-stream TTL conflicting eviction for shared keys** — short-TTL stream expiry silently evicts long-TTL stream operators for the same entity key if they share a flat EntityState. The EntityState refactor (per-stream StreamState grouping) is the structural fix and must land before per-dataset TTL is implemented.

7. **Replay time semantics mismatch** — during live processing, operators use wall-clock `now()` for both bucket assignment and expiry. During replay, `now` must be the event's historical timestamp for both. After replay completes, advance operator time to current wall-clock to expire stale buckets. Test: push 1000 events live, record features; wipe state, replay same 1000 events, verify identical features.

## Implications for Roadmap

The dependency graph from FEATURES.md and the architectural refactor requirements from PITFALLS.md jointly determine phase structure. Two hard ordering constraints: (1) EntityState refactor must precede everything because it touches every module; (2) event log must precede composable pipeline because keyless streams have nowhere to put events without it.

### Phase 1: Foundation — EntityState Refactor + Event Log + MGET

**Rationale:** The EntityState structural refactor is a cross-cutting change that invalidates snapshots and touches every module. Doing it first means the diff is clean and subsequent phases build on stable ground. The event log is the dependency root for all subsequent v1.1 features. MGET is independent and low-complexity — include here to deliver early user value with minimal risk.

**Delivers:** Per-stream operator grouping in EntityState (enables per-dataset TTL without cross-stream eviction), SSD append-only event log with BufWriter + periodic fdatasync, `history=True` opt-in per stream, mandatory `history_ttl` at registration, disk-full graceful degradation (drop from log not from processing), event log metrics, MGET TCP command.

**Avoids:** Pitfall 1 (fsync on hot path — buffered async from day one), Pitfall 8 (unbounded log growth — require history_ttl at registration), Pitfall 9 (cross-stream eviction — EntityState refactor)

### Phase 2: Composable Pipeline — Keyless Streams + DAG Execution

**Rationale:** Keyless streams require the event log (Phase 1). DAG execution requires `depends_on` declarations on keyed streams. These two features together define the composable pipeline concept — one without the other delivers nothing meaningful to users.

**Delivers:** Keyless stream type (no key field, no in-memory state, log-only), keyed streams with `depends_on` declarations, petgraph-based dependency graph at registration time, topological sort with cycle detection, event cascade on PUSH (events flow through pipeline in topological order automatically).

**Avoids:** Pitfall 5 (DAG cycles — cycle detection ships with DAG, not as follow-on hardening), Pitfall 2 (I/O collision — background task mutual exclusion framework established here)

### Phase 3: Backfill + Schema Evolution

**Rationale:** Backfill requires both the event log (Phase 1) and schema evolution — you cannot replay events into new features unless you can add features to existing streams without resetting all state. These features are tightly coupled and must be designed and implemented together.

**Delivers:** Schema evolution (add features with lazy initialization on next event for all existing keys), feature signature hashing for diff-and-reconcile, backfill engine with 64 events/yield-cycle rate limiting, live-first scheduling during backfill, `warming_up` status for features under backfill, backfill progress tracking in metrics.

**Avoids:** Pitfall 3 (backfill starvation — rate limiting and live-first scheduling from day one), Pitfall 4 (schema evolution operator corruption — signature hashing), Pitfall 7 (replay time semantics — historical timestamp used for both bucket assignment and expiry during replay)

### Phase 4: Incremental Snapshots + Schema Evolution (Remove)

**Rationale:** Incremental snapshots are an independent optimization — no other v1.1 features depend on them — but the dirty-key tracking design must be validated against the now-refactored EntityState structure from Phase 1. Schema evolution for removing features is lower urgency than adding and fits naturally as a Phase 3 complement.

**Delivers:** Dirty-key HashSet tracking (O(1) insert on PUSH/SET, swap-and-clear on snapshot), delta snapshot files (only dirty entities serialized), full snapshot every 10th cycle to bound recovery time, recovery from base + delta chain, schema evolution for removing features (stop evaluating, drop from snapshots, handle missing features on load), background I/O mutual exclusion implementation.

**Avoids:** Pitfall 6 (dirty tracking overhead — benchmark to confirm <5% throughput impact), Pitfall 2 (I/O collision — mutual exclusion implementation here)

### Phase 5: Debug Web UI + Event Log Compaction

**Rationale:** These are independent features with no blocking dependencies on other v1.1 work. The debug UI enhances observability without affecting pipeline correctness. Event log compaction is the long-term disk management solution (history TTL at registration is the short-term stopgap from Phase 1).

**Delivers:** Embedded debug UI (rust-embed static assets, vanilla HTML/JS/CSS, no npm build step), stream topology DAG visualization, per-stream throughput charts (SSE-powered), memory breakdown, entity state inspector, backfill progress display, bounded SSE broadcast channel (drop-on-full, never blocks hot path), event log compaction (background segment rewrite, Redis AOF rewrite model).

**Avoids:** Pitfall 10 (WebSocket/SSE backpressure — bounded channel with drop semantics, never block the hot path for debug delivery)

### Phase Ordering Rationale

- EntityState refactor first — it is a cross-cutting structural change. One clean diff, then all subsequent phases build on stable ground.
- Event log before composable pipeline — keyless streams have nowhere to put events without the log. Backfill has nothing to replay.
- Backfill and schema evolution together — backfill requires schema evolution. Separating them forces two design passes over the same operator state reconciliation logic.
- Incremental snapshots after core pipeline stabilizes — dirty-key tracking interacts with the EntityState structure from Phase 1. Validating this after Phase 1-3 refactors reduces rework risk.
- Debug UI last — purely operational. Does not affect pipeline correctness. Implementation is straightforward given existing HTTP management API infrastructure.

### Research Flags

Phases likely needing deeper research during planning:

- **Phase 3 (Backfill time semantics):** The transition between replayed events and live events for an entity currently being backfilled is underspecified in all research sources. Specifically: if a live PUSH arrives for an entity mid-backfill, how are the two event streams merged without double-counting at the boundary? Needs explicit design decision before coding.
- **Phase 4 (Incremental snapshot recovery):** Recovery path edge cases need explicit test case design before implementation: (a) corrupt delta record, (b) schema evolution change between base and delta (feature removed after base snapshot taken), (c) base snapshot missing entirely. These are not covered by the primary sources.

Phases with standard patterns (skip research-phase):

- **Phase 1 (Event Log):** Redis AOF is extremely well-documented. BufWriter + spawn_blocking is established Rust I/O pattern. STACK.md research is HIGH confidence.
- **Phase 2 (DAG Execution):** petgraph topological sort is one API call. Cycle detection return type is documented. Pattern is validated by Rust compiler toolchain usage.
- **Phase 5 (Debug UI):** rust-embed + axum SSE is a documented integration pattern. The UI itself is vanilla HTML/JS — no framework decisions needed.

## Confidence Assessment

| Area | Confidence | Notes |
|------|------------|-------|
| Stack | HIGH | All 4 new crates verified on crates.io; version compatibility confirmed; explicit rejection rationale for each alternative (commitlog, daggy, ServeDir, WebSocket) |
| Features | HIGH | Corroborated across Flink, RisingWave, Materialize, Redis, Tecton; dependency graph explicitly mapped with ordering constraints identified |
| Architecture | HIGH | Based on direct analysis of the existing ~8,400-line v1.0 codebase; component boundaries, modification scope, and LOC estimates are concrete |
| Pitfalls | HIGH | Core pitfalls verified against Redis, Flink, Databricks documentation; Tally v1.0 codebase analysis confirms the specific code paths at risk; 10 pitfalls with phase assignment and verification tests |

**Overall confidence:** HIGH

### Gaps to Address

- **Backfill + live traffic boundary:** How are replayed events and live events merged for an entity currently being backfilled? Risk of double-counting at the handoff point. Must be resolved in Phase 3 design before any backfill code is written.
- **Incremental snapshot recovery edge cases:** The base + delta recovery path is clear for happy path, but the combination of (missing base) + (schema evolution between base and delta) needs explicit specification and test cases before Phase 4 implementation.
- **Event log compaction correctness:** Background compaction rewrites segments while the state continues to change. If state changes between "start compaction" and "finish compaction", the compacted log may diverge from current state. Snapshot + compaction mutual exclusion helps but does not fully address this. Needs design attention in Phase 5.

## Sources

### Primary (HIGH confidence)
- Redis AOF persistence + fsync latency documentation — event log write pattern, background I/O mutual exclusion model
- Apache Flink state schema evolution docs — feature add/remove semantics, key evolution constraints, incremental checkpoint design (FLIP-151)
- petgraph crates.io + docs.rs — DAG API, toposort() O(V+E), Cycle(NodeIndex) return type
- crc32fast crates.io — SIMD acceleration confirmed, 335M+ downloads, no unsafe in public API
- rust-embed crates.io — axum 0.8 feature flag, dev-mode filesystem fallback confirmed
- Tally v1.0 codebase (~8,400 lines) — existing Arc<Mutex<AppState>> pattern, EntityState structure, snapshot design, v1.0 operator implementations

### Secondary (MEDIUM confidence)
- RisingWave backfill order control (2025) — backfill dependency ordering insight; topological sort applies to backfill cascade
- Tecton stream feature view documentation — backfill=True + feature_start_time pattern as user-facing API model
- Flink incremental checkpointing (FLIP-151) — RocksDB SSTable delta approach adapted to Tally's flat HashMap model
- Segmented log in Rust blog — validates hand-rolled approach over commitlog crate
- ActivityWatch rust-embed pattern — single-binary UI embedding reference implementation

### Tertiary (LOW confidence)
- Backfill + live traffic boundary at replay completion — inferred from backfill correctness literature; not directly documented for single-threaded systems; requires explicit design validation in Phase 3

---
*Research completed: 2026-04-09*
*Ready for roadmap: yes*
