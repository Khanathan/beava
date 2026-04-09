# Pitfalls Research

**Domain:** Adding composable pipelines, SSD event log, replay/backfill, schema evolution, incremental snapshots, and debug UI to an existing low-latency in-memory feature server (Rust, single-threaded tokio)
**Researched:** 2026-04-09
**Confidence:** HIGH (core pitfalls verified against Redis AOF documentation, Flink schema evolution docs, tokio runtime internals, and Tally v1.0 codebase analysis)

**Context:** This research focuses exclusively on pitfalls when ADDING v1.1 features to the existing Tally v1.0 codebase. For v1.0 pitfalls (snapshot blocking, MSET yielding, SipHash, TCP framing, etc.), see git history of this file.

---

## Critical Pitfalls

### Pitfall 1: SSD Event Log Write Blocks the Hot Path — Synchronous fsync Destroys <100us p99

**What goes wrong:**
Adding an append-only SSD event log means every PUSH must write to disk before returning features. A naive implementation calls `write()` + `fsync()` on the event log file synchronously in the PUSH handler. On Linux, `fsync()` on an SSD takes 200us-2ms depending on device queue depth and filesystem journaling. This single syscall blows the entire <100us PUSH latency budget. Under sustained load (100K events/sec), the fsync queue depth grows, pushing tail latency to 5-20ms. Redis documented this exact problem: during AOF persistence, the main thread's `write(2)` was blocked for 7 to 14 seconds during heavy I/O on a 10GB AOF file rewrite.

**Why it happens:**
Developers treat "append to file" as trivially fast. Appending IS fast (~100-300ns for the `write()` syscall itself), but durability requires `fsync()` which forces the SSD's internal write buffer to flash. The v1.0 Tally architecture is single-threaded `current_thread` tokio — any blocking syscall on the event loop thread stalls ALL connections. The memory architecture document mentions "periodic fsync (~100-300ns amortized per event)" but this confuses the amortized cost of buffered writes with the actual fsync latency.

**How to avoid:**
Three-layer approach:
1. **Buffer writes in memory** — accumulate events in a `Vec<u8>` write buffer. Append to the buffer on each PUSH (true O(1), ~50ns).
2. **Async flush via spawn_blocking** — periodically (every 100ms or every N events) flush the buffer to disk using `tokio::task::spawn_blocking`. The flush does `write()` + `fdatasync()` on the blocking thread pool, never touching the event loop thread.
3. **Accept bounded data loss** — events in the write buffer but not yet flushed are lost on crash. This is the same tradeoff as the existing 30-second snapshot interval. Document that event log durability is "at most 100ms behind."

Do NOT use `io_uring` or `tokio-uring` — they require the multi-threaded runtime and are Linux-only. Tally targets macOS+Linux, and the current runtime is `current_thread`. The `spawn_blocking` approach works everywhere.

**Warning signs:**
- PUSH p99 jumps from <100us to >1ms after event log is enabled
- `perf` shows `fdatasync` or `fsync` in the main thread's call stack
- Latency is bimodal: fast when SSD queue is empty, slow when other I/O (snapshots, compaction) is concurrent

**Phase to address:**
Phase 1 (SSD Event Log). The write buffer + async flush architecture must be the first thing built. Do NOT start with synchronous writes "to get it working" — the refactor from sync to async flush changes the durability semantics and error handling of every caller.

---

### Pitfall 2: Event Log Compaction and Snapshot I/O Collide — Double Disk Load Causes Latency Spikes

**What goes wrong:**
Tally v1.0 already runs periodic snapshots (clone state + `spawn_blocking` serialize + write to disk). V1.1 adds event log compaction (rewrite the log with only events within TTL). If both run concurrently, the SSD handles two heavy write workloads simultaneously. SSD write amplification causes the device write queue to fill, which increases latency for ALL writes — including the event log append buffer flush. Redis explicitly prevents this: "AOF rewrite is prevented if RDB snapshotting is in progress. BGSAVE is blocked if AOF rewrite is running."

**Why it happens:**
Both snapshot and compaction are "background" tasks on independent timers. Without coordination, they occasionally overlap. The developer assumes `spawn_blocking` isolates them from the hot path, but SSD I/O bandwidth is a shared resource. When the device queue is saturated, even buffered `write()` calls can stall at the OS level.

**How to avoid:**
Implement a simple mutual exclusion lock for background I/O tasks:
```rust
enum BackgroundTask { Snapshot, Compaction, None }
// Only one at a time. If compaction is due but snapshot is running, defer compaction.
```
Schedule compaction to run at the midpoint between snapshots (e.g., snapshot at t=0s, compaction at t=15s if both are on 30s intervals). Never overlap. This is exactly the Redis model.

Additionally, when incremental snapshots replace full snapshots, consider merging the snapshot and compaction into a single background task — they both walk state and write to disk.

**Warning signs:**
- Periodic latency spikes that are twice as severe as snapshot-only spikes
- SSD IOPS utilization hits device ceiling during combined operations
- `snapshot_duration_ms` metric shows high variance (fast when alone, slow when competing with compaction)

**Phase to address:**
Phase 2 (Compaction/TTL) and Phase 4 (Incremental Snapshots). Mutual exclusion must be designed when the first background I/O task (event log flush) is added in Phase 1.

---

### Pitfall 3: Backfill Replay Starves Live Traffic — No Priority Scheduling

**What goes wrong:**
When a new feature is registered with `backfill=True`, the server replays all events from the SSD event log through the new operator. For a stream with 1M events in the log, replay takes seconds to minutes. During replay, the single-threaded event loop is either (a) processing replay events, blocking all live PUSH/GET, or (b) interleaving replay and live traffic, but replay consumes 90%+ of the event loop's capacity because the replay queue always has more pending work than live traffic.

**Why it happens:**
Replay is a bounded-but-large burst of CPU work. The developer adds `yield_now()` between replay batches (learning from the MSET pitfall), but each replay batch still takes 500us-5ms of CPU. With 100K events to replay, even at 1024 events/batch with yields, the event loop spends most of its time on replay. Live PUSH requests queue up, and p99 latency degrades to 10-100ms for the entire replay duration.

**How to avoid:**
Rate-limit replay explicitly:
1. **Replay budget per event loop tick** — process at most N replay events (e.g., 64) per yield cycle, then yield and process ALL pending live traffic before resuming replay.
2. **Live-first scheduling** — check if any live PUSH/GET requests are pending before processing the next replay batch. Live requests always take priority.
3. **Expose replay progress** — the debug UI should show "backfill: 45,231 / 1,000,000 events replayed" so operators know the feature is still warming up.
4. **Degrade gracefully** — features being backfilled should return `Missing` until replay is complete, not stale/partial values. Mark features as `warming_up` in GET responses.

Do NOT attempt to run replay in a separate thread. Tally is single-threaded by design; operator state is not thread-safe. Replay must run on the event loop with explicit budgeting.

**Warning signs:**
- PUSH p99 spikes to >10ms when any new feature is registered with backfill
- GET returns partially-warmed features (e.g., `tx_count_1h = 3` when the log has 1000 events in the last hour)
- Server appears unresponsive during backfill of large streams

**Phase to address:**
Phase 2 (Backfill). The replay scheduler must be designed before the first line of replay code. Write a test: register a new feature with backfill=True on a stream with 100K logged events, fire concurrent PUSH requests, verify PUSH p99 stays under 1ms.

---

### Pitfall 4: Schema Evolution Silently Corrupts In-Memory Operator State

**What goes wrong:**
A user re-registers a stream with modified features: adds `tx_max_1h`, removes `tx_min_1h`, changes `tx_count_1h` window from 1h to 30m. The server must reconcile the new schema with existing in-memory `EntityState` for all keys. If existing operators are reused when the window configuration has changed (e.g., the `CountOp` ring buffer was sized for 60 buckets at 1-minute granularity for 1h, but is now expected to cover 30m), the operator produces incorrect values. Existing data in the ring buffer is misinterpreted.

**Why it happens:**
The v1.0 `EntityState.live_operators` is a `Vec<(String, OperatorState)>` keyed by feature name. On re-register, the naive approach is "keep operators with matching names, add new ones, remove old ones." But an operator's internal state (ring buffer size, bucket duration, field name) is baked into the `OperatorState` enum at creation time. Changing the window parameter requires creating a new operator, discarding the old state. Apache Flink documents this: "The structure of a key cannot be migrated as this may lead to non-deterministic behavior."

**How to avoid:**
Schema evolution must implement a **diff-and-reconcile** algorithm:
1. **Compare feature signatures, not just names** — two features with the same name but different window/field/type are DIFFERENT features. Hash the feature definition (operator type + window + bucket + field + where_expr) into a signature.
2. **New signature = new operator** — if the signature changed, drop the old operator state for that feature and create a fresh one. The feature starts cold (returns Missing until enough events arrive).
3. **Same signature = keep state** — only truly unchanged features retain their accumulated state.
4. **Removed features = drop operators** — clean up. Don't leave orphaned operators consuming memory.
5. **Migration atomicity** — apply the schema change to ALL entities' live_operators in one sweep, or none. Partial migration (some entities on old schema, some on new) causes inconsistent features across keys.

For the migration sweep, use chunked iteration with yield_now() to avoid blocking the event loop (same pattern as MSET).

**Warning signs:**
- Feature values change unexpectedly after re-registering a stream with modified windows
- Ring buffer panics or produces garbage after window resize
- Some entity keys return features from the old schema while others return the new

**Phase to address:**
Phase 3 (Schema Evolution). Must be designed with awareness of how `EntityState.live_operators` works in v1.0. Write a test: register stream, push 1000 events, re-register with changed window, verify old state is discarded and new operator starts fresh.

---

### Pitfall 5: DAG Dependency Cycles in Composable Pipeline Cause Infinite Evaluation Loops

**What goes wrong:**
V1.1 introduces keyless-to-keyed stream dependencies with DAG execution. A user accidentally creates a cycle: Stream A depends on View B, View B depends on Stream C, Stream C has a derive that references Stream A. On PUSH, the evaluation engine follows dependencies: A triggers B triggers C triggers A... resulting in infinite recursion or stack overflow. In the single-threaded event loop, this hangs the entire server.

**Why it happens:**
V1.0 has a simple two-level model: streams have operators, views have derives that reference streams. No stream can reference another stream, so cycles are structurally impossible. V1.1 introduces multi-stage composition where streams can depend on other streams. Without explicit cycle detection at registration time, the runtime evaluation must handle cycles — and it won't, because DAG evaluation assumes acyclicity.

**How to avoid:**
1. **Cycle detection at REGISTER time** — when a new stream/view is registered, build the dependency graph and run Kahn's algorithm (BFS topological sort). If the graph has a cycle (some nodes never reach in-degree 0), reject the registration with a clear error message naming the cycle.
2. **Store the topological order** — after validation, store the sorted evaluation order. On PUSH, evaluate in topological order only. This also eliminates redundant evaluations (each node computed exactly once).
3. **Depth limit at evaluation time** — as a safety net, cap the evaluation depth (e.g., 16 levels). If a cycle somehow bypasses the registration check, the depth limit prevents infinite recursion.
4. **Re-validate on every REGISTER** — when a new stream is added, re-run cycle detection on the full graph, not just the new node. Adding a single edge can create a cycle in a previously acyclic graph.

**Warning signs:**
- Server hangs on PUSH after registering a complex pipeline
- Stack overflow panics in the expression evaluator or pipeline engine
- Registration of mutually-referencing views succeeds when it should fail

**Phase to address:**
Phase 1 (Composable Pipeline / DAG). Cycle detection must be part of the registration validation from day one. Do NOT add it as a "hardening" step later — by then, users may have created pipelines that depend on the ability to register cycles.

---

### Pitfall 6: Incremental Snapshots Require Dirty Tracking That Adds Overhead to Every PUSH

**What goes wrong:**
V1.0 snapshots clone the entire state and serialize it. V1.1 wants incremental snapshots that only serialize changed entities. This requires tracking which entities were modified since the last snapshot ("dirty set"). The naive approach adds a `dirty: bool` flag to every `EntityState` and checks it during snapshot. But the PUSH handler must now set this flag on every event — adding a write to every hot-path operation. Worse, the dirty set itself must be collected during snapshot, requiring iteration over all entities to find dirty ones (O(n_entities) even when only 1% changed).

**Why it happens:**
Incremental snapshots look like a simple optimization ("just track what changed"). But in a system processing 100K events/sec, even a few nanoseconds of overhead per event compounds. The dirty tracking mechanism interacts with the existing clone-then-serialize model: you can't clone just the dirty entities without knowing which they are, and knowing which are dirty requires a data structure that is maintained on the hot path.

**How to avoid:**
Use a separate **dirty key set** (not a flag per entity):
1. Maintain a `HashSet<EntityKey>` (or `Vec<EntityKey>`) that records keys modified since last snapshot.
2. On PUSH, `dirty_set.insert(key)` — this is O(1) amortized and adds ~20-50ns per event (acceptable within 100us budget).
3. On snapshot, iterate only the dirty set, clone only those entities, serialize them as a delta file.
4. Clear the dirty set after snapshot completes.
5. Periodically (e.g., every 10th snapshot) write a full snapshot to bound recovery time.

The dirty set must be swapped atomically with the snapshot: take the current dirty set, replace it with an empty one, then serialize the taken set's entities on the blocking thread. This prevents races where a PUSH between "collect dirty" and "clear dirty" loses its tracking.

**Warning signs:**
- PUSH latency increases by >10ns after adding dirty tracking (measure with benchmarks)
- Snapshot still iterates all entities even when the dirty set is small
- Recovery from incremental snapshots takes longer than full snapshots (missing base snapshot)

**Phase to address:**
Phase 4 (Incremental Snapshots). Design the dirty tracking data structure before implementation. Benchmark the overhead of dirty set insertion on the hot path.

---

### Pitfall 7: Event Log Replay Produces Different Results Than Live Processing (Time Semantics Mismatch)

**What goes wrong:**
During live processing, operators use `SystemTime::now()` for window bucket assignment (the v1.0 behavior). During backfill replay, events are read from the log with their original timestamps. If the replay code passes the event's historical timestamp to the operator, window boundaries are calculated relative to the event time. But the operator's internal ring buffer tracks "current time" for expiry — during replay, "current time" advances non-monotonically as historical events are processed in log order. Ring buffer buckets expire incorrectly, producing different aggregation results than if the events had been processed in real time.

**Why it happens:**
The v1.0 ring buffer operators use "now" for two purposes: (1) determining which bucket to write into, and (2) expiring old buckets on read. During live processing, these are always the same wall-clock time. During replay, "now" for bucket assignment should be the event's timestamp, but "now" for expiry is ambiguous — should expired buckets be expired relative to event time or wall-clock time? Getting this wrong means replay produces different feature values than live processing for the same event sequence.

**How to avoid:**
Define and enforce a clear time model for replay:
1. **Replay uses event timestamps for both assignment AND expiry** — the operator's "now" is the event's timestamp, not wall-clock time. This means replay produces identical results to live processing (deterministic replay).
2. **Events must be replayed in timestamp order** — if the event log contains out-of-order timestamps, sort by timestamp before replay. The SSD log stores events in arrival order, which may differ from timestamp order.
3. **After replay completes, advance operator time to wall-clock "now"** — this expires any buckets that would have expired during the gap between the last replayed event and the current time.
4. **Test determinism explicitly** — push 1000 events live, record the resulting features. Wipe state, replay the same 1000 events from the log, verify identical features.

**Warning signs:**
- Backfilled features differ from live-computed features for the same event sequence
- Features "jump" when replay completes and the operator sees current wall-clock time
- Off-by-one errors in window boundaries during replay (events landing in wrong buckets)

**Phase to address:**
Phase 2 (Backfill). The replay time model must be defined before any replay code is written. This is the most subtle correctness issue in the entire v1.1 scope.

---

### Pitfall 8: Keyless Stream Event Log Grows Unboundedly Without Automatic TTL

**What goes wrong:**
V1.1 introduces keyless streams (raw event ingestion) with `history=True`. At 100K events/sec with ~300 bytes per event, the log grows at ~30MB/sec = ~2.5TB/day. The v2 architecture doc acknowledges this with the `history=True` opt-in flag, but a stream with history enabled and no TTL will fill the disk. When the disk fills, the event log write fails, the write buffer backs up in memory, and the server OOMs or starts dropping events silently.

**Why it happens:**
Developers focus on the append path and defer the cleanup path. The TTL compaction ("Redis-style AOF rewrite") is planned but implemented in a later phase than the event log. In the gap between "event log exists" and "compaction exists," the log grows without bound.

**How to avoid:**
1. **Require history TTL at stream registration** — if `history=True`, the user MUST specify a `history_ttl` (e.g., `"24h"`). Reject registration without a TTL.
2. **Implement simple truncation before full compaction** — before the compaction system is built, implement a crude "delete events older than TTL" by tracking file offsets. When the oldest event in the log exceeds TTL, truncate from the beginning. This is a temporary stopgap until proper compaction.
3. **Disk space monitoring** — expose `event_log_bytes` in the metrics endpoint. Alert when disk usage exceeds a configurable threshold (e.g., 80%).
4. **Fail safely on disk full** — if the write fails, drop the event from the log (not from processing). The event still updates in-memory features; it just won't be available for replay. Log a warning.

**Warning signs:**
- Disk usage grows linearly without bound on streams with `history=True`
- Server crashes or hangs when disk is full
- No metrics visible for event log size

**Phase to address:**
Phase 1 (SSD Event Log). The history TTL requirement and disk-full handling must ship with the event log itself, not deferred to the compaction phase.

---

### Pitfall 9: Per-Dataset Entity TTL Creates Conflicting Eviction for Shared Keys

**What goes wrong:**
V1.1 adds per-dataset (per-stream) entity state TTL. Stream A has TTL=1h, Stream B has TTL=24h. Both use `user_id` as the key. User "u123" has operators for both streams in a single `EntityState`. When Stream A's TTL expires for u123, what happens? If the entire EntityState is evicted, Stream B's 24h operators are lost. If only Stream A's operators are evicted, the `EntityState` must support partial eviction, which the v1.0 data structure doesn't support.

**Why it happens:**
V1.0 has a single global TTL (2x the largest window) applied to the entire EntityState. The data structure assumes all operators for a key live and die together. Per-dataset TTL breaks this assumption by requiring different lifetimes for different operators within the same entity.

**How to avoid:**
Refactor `EntityState.live_operators` from `Vec<(String, OperatorState)>` to a structure grouped by stream:
```rust
struct EntityState {
    streams: HashMap<StreamName, StreamState>,  // Per-stream operator groups
    static_features: AHashMap<String, StaticFeature>,
    // No single last_event_at — each StreamState has its own
}

struct StreamState {
    operators: Vec<(String, OperatorState)>,
    last_event_at: Option<SystemTime>,
}
```
Eviction then operates per-stream: if Stream A hasn't seen an event for its TTL, evict only `streams["A"]`. Stream B's operators survive independently. This is a significant refactor to `EntityState` and affects snapshot serialization, `get_all_features`, and the PUSH handler.

**Warning signs:**
- Operators for long-TTL streams are evicted when short-TTL streams expire
- Entity count drops unexpectedly despite active traffic on some streams
- Cross-stream views return Missing because one stream's operators were evicted

**Phase to address:**
Phase 1 (Entity State Refactor). This structural change to EntityState must happen before per-dataset TTL is implemented, ideally as the first change in v1.1. The refactor touches every module.

---

### Pitfall 10: Debug UI WebSocket Backpressure Stalls the Event Loop

**What goes wrong:**
The debug UI streams real-time data (throughput, memory, feature values) to a browser via WebSocket. If the browser tab is backgrounded, paused by the OS, or on a slow connection, the WebSocket write buffer grows. The WebSocket writer, running on the same event loop as the TCP server, attempts to flush the buffer. If the flush stalls (slow client), it holds the event loop, degrading PUSH/GET latency for all clients.

**Why it happens:**
The debug UI is a "nice to have" feature that runs on the same HTTP server (port 6401). Developers treat it as low-risk because it's "just monitoring." But the HTTP server shares the tokio runtime with the TCP hot path. A stuck WebSocket connection on the HTTP side steals event loop time from the TCP side.

**How to avoid:**
1. **Non-blocking WebSocket sends** — use a bounded channel (e.g., capacity=32) between the metrics producer and the WebSocket sender. If the channel is full, drop the oldest update (not block the producer). The metrics producer should never await on WebSocket delivery.
2. **Separate the debug data path** — the debug UI reads from a periodic metrics snapshot (every 100ms), not from real-time event callbacks. The metrics snapshot is a simple struct clone, not a live tap on the event stream.
3. **Connection timeout** — disconnect WebSocket clients that haven't ACK'd in >5 seconds. Don't let stale connections accumulate.
4. **Consider a separate runtime** — if the debug UI becomes complex, spawn it on a separate multi-threaded tokio runtime on a dedicated thread. This completely isolates it from the hot path. The extra thread is a small cost for reliability.

**Warning signs:**
- PUSH latency degrades when a debug UI browser tab is open
- Multiple open debug UI tabs cause proportional latency increase
- WebSocket connection count grows over time (stale connections not cleaned up)

**Phase to address:**
Phase 5 (Debug UI). Use bounded channels and drop-on-backpressure from the start. Test: open debug UI, pause the browser, verify PUSH p99 is unaffected.

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Synchronous event log write on hot path | Simple implementation, "get it working" | Destroys <100us p99 forever; must rewrite to buffered async | Never |
| Global entity TTL instead of per-stream | Simpler eviction, no EntityState refactor | Cross-stream features break; short-TTL evicts long-TTL operators | Only if per-dataset TTL is truly deferred to v1.2 |
| Skip cycle detection in DAG registration | Faster to implement pipeline composition | Server hangs on cyclic dependencies; requires restart | Never |
| Full state clone for incremental snapshot | Reuse existing snapshot code path | 2x peak memory; negates the point of incremental snapshots | Acceptable as Phase 1 stopgap while dirty tracking is built |
| Replay at full speed without rate limiting | Backfill completes faster | Live traffic starved during replay; p99 spikes to seconds | Never in production; acceptable in offline/maintenance mode only |
| Skip event log compaction in Phase 1 | Ship event log faster | Disk fills up for high-volume streams; manual cleanup required | Acceptable for 1-2 weeks of testing, but MUST require history_ttl |
| Store event log as raw JSON lines | Simple format, human-readable, easy debugging | 3-5x larger than binary format; 3-5x slower to read during replay | Acceptable for v1.1 if performance is adequate; optimize to binary in v1.2 |
| Schema diff by name only (not signature) | Simpler re-register logic | Changed window/field silently reuses stale operator state | Never |

---

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| Event log file + Snapshot file | Both use the same disk; concurrent heavy writes cause contention | Schedule snapshot and compaction to never overlap (Redis model); monitor SSD IOPS |
| Event log replay + Live PUSH handler | Replay calls the same `engine.push()` as live traffic, but with historical timestamps | Create a separate `engine.replay()` entry point that uses event timestamps, or parameterize `push()` with a time source |
| Schema evolution + Snapshot recovery | New schema is registered, snapshot is taken, server restarts, snapshot loads the new schema but entity operators are from the old schema | Snapshot must include the schema version/signature alongside each entity's operators; on load, reconcile operators against the current registered schema |
| Per-stream TTL + Cross-stream views | Stream A's operators are evicted by TTL, but View V still references Stream A features | View evaluation must handle Missing values from evicted streams gracefully; never panic on lookup of evicted stream operators |
| Debug UI WebSocket + TCP hot path | Both on same tokio runtime; WebSocket backpressure stalls TCP | Use bounded channels with drop semantics for WebSocket; never block the producer |
| MGET batch + Large entity count | MGET for 10K keys iterates all keys under lock, blocking PUSH for the duration | Apply same chunked-yield pattern as MSET: 1024 keys per chunk, yield_now() between chunks |
| Keyless streams + Keyed streams (fan-out) | Event pushed to keyless stream should fan out to keyed streams, but the fan-out logic in v1.0 only works with keyed streams | Redesign fan-out to handle keyless-to-keyed transitions: keyless stream logs the event, then the DAG evaluation pushes to downstream keyed streams |

---

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Event log fsync on every event | PUSH p99 jumps to 1-5ms | Buffer + async flush (spawn_blocking) | Immediately at any load |
| Full state iteration for dirty set collection | Snapshot takes O(n_entities) even when 0.1% changed | Maintain a separate dirty key set, swap on snapshot | >500K entities |
| Replay processes all events before yielding | Live PUSH queues for seconds during backfill | Rate-limit replay: max 64 events per yield cycle | Any backfill >1000 events |
| JSON-encoded event log | Replay reads 30MB/sec of JSON; serde_json parsing dominates CPU | Use postcard or a length-prefixed binary format for the event log | >10K events/sec replay throughput needed |
| Lock held during entire MGET | Arc<Mutex> locked for O(n_keys) GET operations | Chunked yield like MSET: 1024 keys per chunk | >1000 keys in a single MGET |
| DAG evaluation recomputes intermediate nodes | View A and View B both depend on Stream C; C is evaluated twice per event | Cache intermediate results in the topological evaluation pass | >10 views with shared dependencies |
| Debug UI polls state every frame (60fps) | Metrics collection steals 5% of event loop time | Poll at most 10 times per second; use last-known value between polls | Always, if not rate-limited |
| Event log file descriptor leak on compaction | Old log files not closed after rotation; fd limit hit | Explicitly close old file handles; use RAII (Drop) | After ~1000 compaction cycles |

---

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| Event log stores raw event payloads including PII | Sensitive data (user IDs, amounts, locations) persists on disk indefinitely | Document that event log contains raw events; apply same file permissions (0600) as snapshots; honor history TTL to limit PII retention |
| Debug UI accessible without authentication | Anyone on the network can view real-time feature values, entity keys, and stream definitions | Bind debug UI to localhost only by default; require explicit `--debug-bind 0.0.0.0` flag to expose on network |
| Schema evolution allows redefining key_field | Attacker re-registers a stream with a different key_field, causing all entity state to become orphaned | Reject re-registration that changes key_field; require explicit DELETE + re-REGISTER |
| Event log replay with crafted timestamps | Replaying events with future timestamps skews window aggregations | Clamp event timestamps to [log_start_time, now] during replay; reject events with timestamps outside the log's time range |

---

## "Looks Done But Isn't" Checklist

- [ ] **Event log durability:** Appending to the log works under normal conditions -- but verify behavior when the disk is full (should fail gracefully, not corrupt the log or crash)
- [ ] **Backfill correctness:** Replay produces features -- but verify they match live-computed features for the same event sequence (deterministic replay test)
- [ ] **Schema evolution migration:** Re-registering a stream updates the schema -- but verify ALL entities' operators are reconciled, not just the next entity to receive an event
- [ ] **Per-stream TTL eviction:** Old stream operators are evicted -- but verify operators for other streams on the same entity key survive
- [ ] **DAG cycle detection:** Simple cycles (A->B->A) are detected -- but verify transitive cycles (A->B->C->A) and self-references (A->A) are also caught
- [ ] **Incremental snapshot recovery:** Loading an incremental snapshot restores recent changes -- but verify recovery works when the base snapshot is missing or corrupt (should fall back to full snapshot or clean start)
- [ ] **MGET cooperative yielding:** MGET returns correct results -- but verify PUSH p99 stays under 1ms during concurrent MGET of 10K keys
- [ ] **Debug UI disconnection:** WebSocket streams data correctly -- but verify the server is unaffected when the WebSocket client disconnects abruptly (no resource leak, no panic)
- [ ] **Event log compaction:** Compaction reduces log size -- but verify no events within the TTL window are lost during compaction
- [ ] **Replay + live traffic interleaving:** Backfill completes while live traffic flows -- but verify no events are double-counted or missed at the boundary between "replayed" and "live"

---

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Event log corrupted (partial write on crash) | LOW | Detect corruption by validating last record's length+checksum; truncate the log to the last valid record; lose at most one event |
| Schema migration leaves orphaned operators | MEDIUM | Re-register the stream (triggers reconciliation); if operators are still orphaned, delete snapshot and restart fresh |
| Backfill produced wrong features (time semantics bug) | MEDIUM | Delete the affected operators from all entities (targeted wipe); re-run backfill with fixed replay logic; no need to restart server |
| Disk full from unbounded event log | LOW | (1) Manually delete old event log segments, (2) add history_ttl to the stream definition, (3) trigger compaction via HTTP API |
| DAG cycle caused server hang | LOW | Kill server, fix the pipeline definition to remove cycle, restart; cycle detection prevents recurrence |
| Incremental snapshot missing base | MEDIUM | Fall back to last full snapshot; accept staleness for entities changed after the full snapshot; schedule immediate full snapshot |
| Debug UI leak causes memory growth | LOW | Restart server; add connection timeout to prevent recurrence |

---

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| SSD write blocks hot path | Phase 1 (Event Log) | Benchmark: PUSH p99 stays <100us with event log enabled |
| Snapshot + compaction I/O collision | Phase 1 (Event Log) + Phase 4 (Incremental Snapshots) | Test: trigger snapshot and compaction simultaneously, verify no latency spike >2x baseline |
| Backfill starves live traffic | Phase 2 (Backfill) | Test: backfill 100K events, concurrent PUSH p99 stays <1ms |
| Schema evolution corrupts operators | Phase 3 (Schema Evolution) | Test: re-register stream with changed window, verify old operators discarded, new operator starts fresh |
| DAG dependency cycles | Phase 1 (Composable Pipeline) | Test: register A->B->A cycle, verify REGISTER returns error |
| Incremental snapshot dirty tracking overhead | Phase 4 (Incremental Snapshots) | Benchmark: PUSH throughput within 5% of baseline after adding dirty tracking |
| Replay time semantics mismatch | Phase 2 (Backfill) | Test: replay same events as live, verify identical feature values |
| Event log unbounded growth | Phase 1 (Event Log) | Test: stream with history_ttl=1h, verify log size bounded after 2h of writes |
| Per-stream TTL conflicting eviction | Phase 1 (Entity State Refactor) | Test: two streams with different TTLs on same key, verify independent eviction |
| Debug UI WebSocket backpressure | Phase 5 (Debug UI) | Test: open UI, pause browser, verify PUSH p99 unaffected |

---

## Sources

- Redis AOF persistence and fsync latency: https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
- Redis latency diagnostics (fsync, fork, AOF rewrite): https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/latency/
- Redis AOF rewrite blocking main thread write(2) for 7-14 seconds: https://github.com/redis/redis/issues/1019
- Redis incremental fsync during RDB save: https://github.com/redis/redis/pull/4758
- 7 types of Redis latency (AOF-specific): https://www.netdata.cloud/blog/7-types-of-redis-latency/
- Apache Flink state schema evolution constraints: https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/dev/datastream/fault-tolerance/serialization/schema_evolution/
- Databricks schema evolution in stateful streaming: https://www.databricks.com/blog/events-insights-complex-state-processing-schema-evolution-transformwithstate
- Schema evolution in streaming data pipelines: https://medium.com/@krthiak/schema-evolution-in-streaming-data-pipelines-d870b46d40d0
- Flink incremental snapshot design (FLIP-151): https://cwiki.apache.org/confluence/display/FLINK/FLIP-151:+Incremental+snapshots+for+heap-based+state+backend
- Backfill correctness and idempotency: https://medium.com/@manjindersingh_10145/designing-robust-data-pipelines-idempotency-replays-backfills-explained-640c9920f7b9
- Event stream processing backfills: https://jstaffans.github.io/posts/2016-11-05-backfills.html
- Uber Kappa architecture for timely stream processing: https://www.uber.com/us/en/blog/kappa-architecture-data-stream-processing/
- Backfilling real-time analytics pipeline for correctness: https://startree.ai/resources/backfilling-a-real-time-analytics-data-pipeline/
- Tokio spawn_blocking for file I/O: https://docs.rs/tokio/latest/tokio/runtime/index.html
- Tokio cooperative preemption: https://tokio.rs/blog/2020-04-preemption
- Kahn's algorithm for DAG cycle detection: https://en.wikipedia.org/wiki/Topological_sorting

---
*Pitfalls research for: Tally v1.1 composable pipeline, SSD event log, backfill, schema evolution*
*Researched: 2026-04-09*
