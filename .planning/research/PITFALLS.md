# Pitfalls Research

**Domain:** Real-time in-memory feature server (Rust, custom TCP, sliding windows, HyperLogLog, snapshot persistence, Python SDK)
**Researched:** 2026-04-09
**Confidence:** HIGH (core pitfalls verified against official Tokio docs, Rust stdlib docs, and multiple production post-mortems)

---

## Critical Pitfalls

### Pitfall 1: Snapshot Serialization Blocks the Async Event Loop

**What goes wrong:**
The snapshot job is triggered periodically (every 30 seconds). If the full state HashMap is serialized synchronously inside an async task without yielding, the single-threaded Tokio runtime is blocked for the entire duration. All PUSH and GET requests queue up unserved. For 1M keys the serialization walk alone can take hundreds of milliseconds, spiking p99 latency by 100x during snapshot windows.

**Why it happens:**
Developers see `tokio::spawn` and assume the task yields automatically. It does not. CPU-bound work inside an async task consumes the thread without hitting any `.await` point. The Tokio cooperative budget (128 operations) is only decremented by I/O and channel operations — not by iterator loops or serde encoding. The single-threaded runtime (`current_thread`) has no other thread to fall back on.

**How to avoid:**
Implement cooperative chunking explicitly in the snapshot path: serialize N keys per chunk, then call `tokio::task::yield_now().await` to hand the thread back. The CLAUDE.md design already specifies this ("Cooperative yielding during snapshot"). Budget ~1024 keys per chunk. Alternatively, use `tokio::task::spawn_blocking` to move the serialization to the blocking thread pool, but then the full state must be cloneable (expensive) or wrapped in an `Arc<Mutex<>>` (adds lock contention on hot path). Chunked yielding in-task is the correct approach here.

**Warning signs:**
- PUSH latency histograms show periodic p99 spikes every 30 seconds
- `tokio::runtime` metrics show long scheduler poll times
- `GET` requests pile up while snapshot is running

**Phase to address:**
Phase 4 (Persistence). Must be designed in from the start of the snapshot module — retrofitting cooperative yielding into a monolithic serializer is harder than writing it that way initially.

---

### Pitfall 2: bincode Has No Schema Evolution — Snapshots Become Unreadable on Code Changes

**What goes wrong:**
bincode serializes structs as positional binary — field order and types must match exactly between writer and reader. Adding a field to `OperatorState` or `EntityState` (e.g., adding `min_value` to `Counter`), renaming a variant, or reordering enum arms will make the snapshot completely unreadable. The server crashes on startup trying to deserialize a mismatched snapshot. Users lose all their feature state.

**Why it happens:**
bincode is a low-overhead format deliberately lacking self-description. There are no field names, no type tags, no version markers embedded in the format by default. The Rust forum confirms "bincode does not provide explicit forward or backward compatibility guarantees." This is fine for ephemeral data; it's a trap for persisted snapshots that survive code deployments.

**How to avoid:**
Two complementary defenses: (1) Embed an explicit version byte at the top of every snapshot file (`[1 byte: snapshot_format_version]`). On startup, if version mismatches, refuse to load and start fresh rather than crashing. (2) Wrap the serialized `OperatorState` enum so that adding new variants doesn't corrupt existing data — use `#[serde(other)]` or a dedicated `Unknown` variant as a catch-all. The CLAUDE.md design notes "versioned format" and "version byte per key allows migration on read" — this must be implemented, not just mentioned. Do not assume it comes for free.

**Warning signs:**
- Server panics on startup after any code change to state structs
- Error message: "failed to deserialize" or "unexpected end of input"
- Tests pass but production recovery fails after deployment

**Phase to address:**
Phase 4 (Persistence). Must be part of the initial snapshot design. Add a `SNAPSHOT_FORMAT_VERSION: u8 = 1` constant and write it as the first byte. Write a migration test that verifies a v1 snapshot can be loaded after adding a new field.

---

### Pitfall 3: MSET Chunked Yielding Is Not Actually Cooperative Without Explicit yield_now

**What goes wrong:**
The MSET handler processes a large batch (e.g., 100K keys). The developer chunks it into 1024-key groups with a loop. But without an explicit `tokio::task::yield_now().await` call between chunks, no yielding occurs — the loop burns through all chunks without ever suspending, blocking PUSH and GET requests for the full batch duration. This is the same root cause as Pitfall 1 but applied to MSET.

**Why it happens:**
There are no implicit yield points in Rust async code. Iterating a Vec, updating a HashMap, and encoding JSON all look synchronous to the Tokio scheduler. The `await` keyword is the only suspension mechanism. Writing `for chunk in batch.chunks(1024) { process(chunk); }` inside an async fn does NOT yield between iterations.

**How to avoid:**
The loop body must look like:
```rust
for chunk in batch.chunks(CHUNK_SIZE) {
    process_chunk(chunk, &mut state);
    tokio::task::yield_now().await;  // explicit — no other way
}
```
The chunk size controls the tradeoff between MSET throughput and PUSH/GET responsiveness. 1024 keys per chunk at ~500ns per key = ~500µs per chunk, which fits within a 1ms budget. Test this under load.

**Warning signs:**
- PUSH latency spikes during MSET operations (same symptom as Pitfall 1)
- `tokio::runtime` shows 0 task switches during large MSET
- No `yield_now` calls in the MSET handler code

**Phase to address:**
Phase 2 (Server / MSET command). Write a test that fires 50K PUSH requests concurrently with a 100K MSET and verifies PUSH p99 stays under 5ms.

---

### Pitfall 4: Processing-Time Windows Instead of Event-Time Windows

**What goes wrong:**
The window operators (count, sum, avg) use `Instant::now()` at event arrival time to determine which bucket to write into. When events arrive late (network jitter, retries, batch replay), their timestamps are recorded as "now" rather than when they occurred. A transaction from 25 minutes ago gets counted in the current 30-minute window instead of the one that was open when it happened. Features are silently wrong.

**Why it happens:**
Using arrival time is simpler — no timestamp field required, no clock synchronization needed, no out-of-order handling. It works correctly in the happy path when events arrive in real time. The problem only surfaces when clients retry, replay, or batch-ingest historical events, which happens in fraud detection workflows regularly.

**How to avoid:**
Design the PUSH protocol from day one to accept an optional `_timestamp` field in the event payload. The window bucket assignment uses this timestamp if present, falls back to `Instant::now()` if absent. Events with timestamps older than the longest registered window are discarded (they can't affect any current window). Events with future timestamps are clamped to now. This is a protocol decision — it's very difficult to add retroactively because the wire format and state store both need to change.

**Warning signs:**
- Feature values drift when events are replayed or retried
- `failed_tx_30m` counts don't match expected values in tests using pre-recorded event streams
- ML model performance degrades when trained features differ from served features (training-serving skew)

**Phase to address:**
Phase 1 (Core Engine / Window implementation). Add `event_timestamp: Option<Timestamp>` to the PUSH frame during Phase 2 wire protocol design.

---

### Pitfall 5: HashMap Default Hasher (SipHash) Is a Throughput Bottleneck at 100K+ events/sec

**What goes wrong:**
Rust's `std::collections::HashMap` uses SipHash 1-3 by default. SipHash is a cryptographic-quality hash chosen to prevent HashDoS attacks. For a single-threaded server with string entity keys (user IDs, transaction IDs), profiling consistently shows 20-25% of CPU time spent in hashing at high throughput. This directly caps the throughput ceiling below the 100K events/sec target.

**Why it happens:**
SipHash is deliberately slow compared to non-cryptographic alternatives. For internal server state that is not accessible to untrusted external input, the HashDoS protection is unnecessary overhead.

**How to avoid:**
Use `FxHashMap` from the `rustc-hash` crate as a drop-in replacement for all internal state maps (`EntityState`, pipeline registry, etc.). FxHashMap is 2-3x faster for string keys. For entity keys that come from external clients (user IDs), evaluate whether HashDoS is a real threat — if the server is internal only, FxHashMap is safe everywhere. Keep SipHash only for structures keyed on untrusted external data if serving public-facing clients. Add a benchmark (`benches/throughput.rs`) early and track hash time as a percentage of per-event cost.

**Warning signs:**
- `perf` or `cargo flamegraph` shows `sip_hash` or `hashbrown::hash` consuming >15% of cycles
- Throughput plateaus below 100K events/sec on a modern CPU
- Latency is stable per event but total throughput doesn't scale with optimizations elsewhere

**Phase to address:**
Phase 1 (Core Engine / State Store). Choose the hasher when writing `store.rs` — changing it later requires touching every HashMap instantiation.

---

### Pitfall 6: HyperLogLog Lacks Windowing — distinct_count Counts All Time, Not the Window

**What goes wrong:**
HyperLogLog is a mergeable sketch for cardinality estimation. A single HLL register accumulates elements forever — you cannot "un-add" an element when its window expires. The design calls for `distinct_count` per window (e.g., unique merchants in 24h), but a naive HLL implementation gives you distinct count since server start. As time passes, `unique_merchants` grows monotonically and never reflects the rolling window.

**Why it happens:**
HLL's mathematical structure supports union (merge two sketches) but not subtraction (remove elements from a sketch). This is a fundamental property, not a library limitation. Developers familiar with count/sum operators (which use bucketed ring buffers) assume distinct_count works the same way.

**How to avoid:**
Two approaches: (1) **Epoch-based rotation** — maintain N HLL sketches (one per bucket), rotate on bucket expiry, and compute the union of non-expired buckets on read. This approximates a sliding window with the same accuracy degradation as the count/sum buckets. Memory cost is `N × 12KB` per key per distinct_count feature. With 30 one-minute buckets for a 30m window, that's 360KB per key — acceptable for low-cardinality key spaces, problematic at scale. (2) **Tumbling windows only for distinct_count** — document that `distinct_count` is a tumbling window approximation, not a true sliding window. This matches what systems like Flink do. Be explicit in the Python SDK docs.

**Warning signs:**
- `unique_merchants` in tests grows without bound
- distinct_count values are far higher than expected after server has been running for hours
- Unit tests with time-advance don't show values decreasing when old events expire

**Phase to address:**
Phase 5 (Remaining Operators). The HLL data structure and its window semantics must be designed together — do not implement HLL and add windowing later.

---

### Pitfall 7: TCP Frame Parsing Assumes Complete Frames — Partial Reads Cause Silent Corruption

**What goes wrong:**
The binary protocol uses length-prefixed frames: `[4 bytes: length][1 byte: opcode][payload]`. A single call to `TcpStream::read()` may return fewer bytes than the frame length specifies — a partial read. If the handler processes bytes immediately without buffering until a complete frame is received, it will parse garbage: the opcode byte might actually be part of a string payload from the previous partial read. Corruption is silent — no error, wrong feature values returned.

**Why it happens:**
Developers test locally where TCP delivers complete frames almost always (loopback). Under real network conditions (even LAN), TCP can fragment arbitrarily at segment boundaries. The read-then-parse pattern works in the happy path and breaks silently under load or network variability.

**How to avoid:**
Use Tokio's `AsyncReadExt::read_exact` to read exactly N bytes — it buffers internally until the requested count is satisfied. The pattern:
```rust
let mut len_buf = [0u8; 4];
reader.read_exact(&mut len_buf).await?;
let frame_len = u32::from_be_bytes(len_buf) as usize;
let mut payload = vec![0u8; frame_len];
reader.read_exact(&mut payload).await?;
```
Alternatively use `tokio_util::codec::LengthDelimitedCodec` which handles this correctly. Also set a maximum frame length (e.g., 64MB) to prevent memory exhaustion from malformed length fields.

**Warning signs:**
- Protocol tests pass but integration tests fail intermittently under load
- Corrupted feature values returned sporadically
- Panic or malformed JSON parse errors in the protocol layer

**Phase to address:**
Phase 2 (Server / TCP and protocol). Write a unit test with a mock TCP stream that delivers frames in split pieces (1 byte at a time).

---

### Pitfall 8: Derive Expressions Cause Division-by-Zero Panics on Cold Start

**What goes wrong:**
Derive expressions like `failed_tx_30m / tx_count_30m` compute correctly when both operands are non-zero. On cold start, when a new entity key receives its first event, `tx_count_30m` is 0. Integer division by zero in Rust panics. Even with float types, `0.0 / 0.0 = NaN`, which propagates silently and produces feature values of `NaN` that ML models consume without error.

**Why it happens:**
The expression evaluator computes derives on every event, including the first event for a key. The author of a stream definition assumes non-zero denominators because in production data they rarely see 0 counts. The divide-by-zero edge case exists on cold start, on key eviction and re-initialization, and any time a denominator feature has a longer window than the numerator.

**How to avoid:**
The expression evaluator must handle division by zero at the language level — return a typed `FeatureValue::Missing` or `0.0` rather than panicking. Define the semantics in the SDK docs: "derive expressions that divide by a zero-valued feature return null." ML models receiving null features should have fallback handling. Add a `safe_divide(a, b)` builtin as syntactic sugar. Test every derive expression with zero-initialized state.

**Warning signs:**
- Server panics when processing first event for a new entity key
- `NaN` values appearing in feature responses (harder to catch — no panic)
- ML model predictions behave erratically for new users with no event history

**Phase to address:**
Phase 1 (Core Engine / Expression evaluator). This is a correctness invariant — the evaluator must never panic on valid feature state. Add fuzz testing of expressions with boundary values.

---

### Pitfall 9: Slow / Disconnected Clients Cause Unbounded Write Buffer Growth

**What goes wrong:**
A connected client issues a PUSH but stops reading responses (network pause, client bug, garbage collection pause on the Python side). The server's TCP write buffer fills up. Without backpressure, the server keeps accumulating response bytes for that client in memory. With hundreds of concurrent connections in a fraud detection system, a few stuck clients can consume gigabytes of buffer memory, eventually OOMing the server — taking down feature serving for all clients.

**Why it happens:**
TCP backpressure applies to the OS socket buffer, but application-level async write buffers in Tokio (via `AsyncWrite`) can grow unboundedly if writes are issued faster than the OS drains them. The common pattern of `socket.write_all(response).await` applies backpressure when the buffer is full (it awaits), but this blocks the connection handler task — it does not evict the stuck connection.

**How to avoid:**
Set per-connection write timeouts: wrap all socket writes with `tokio::time::timeout(WRITE_DEADLINE, socket.write_all(...))`. If the write times out, close the connection and log it. Set `SO_KEEPALIVE` and `TCP_USER_TIMEOUT` on all accepted sockets to detect dead connections quickly. Optionally cap the per-connection outstanding response buffer size and reject new requests if the buffer is full. For v1, a simple write timeout (e.g., 5 seconds) prevents the pathological case.

**Warning signs:**
- Server memory grows steadily under load without key count growth
- Client connections accumulate without corresponding throughput increase
- `debug/memory` endpoint shows large per-connection buffer allocations

**Phase to address:**
Phase 2 (Server / TCP connection handling). Write timeout must be part of the connection handler from the start.

---

### Pitfall 10: Cross-Key Lookups Create Implicit State Coupling That Breaks TTL Eviction

**What goes wrong:**
`st.lookup(MerchantActivity.chargeback_count_24h, on="merchant_id")` resolves a merchant feature for a user event. The user's view depends on merchant state. When the merchant key is evicted by TTL (no events for 2× the longest window), the lookup returns `None` or 0. If the derive expression downstream divides by the lookup result, it hits the division-by-zero case from Pitfall 8. Additionally, if a merchant key is evicted and then re-initialized on a new event, the user's view state silently reflects a reset merchant counter — providing stale cross-key features without any error.

**Why it happens:**
TTL eviction treats each key independently. It has no awareness of which other entities depend on a given key. The eviction timer for a merchant key runs independently of how frequently user keys look it up.

**How to avoid:**
Define clear semantics: cross-key lookup on an evicted key returns `FeatureValue::Missing` (not 0, not panic). All derive expressions must handle `Missing` propagation (a derive with any `Missing` input returns `Missing`). Document this behavior explicitly in the SDK. Do NOT attempt reference counting or dependency tracking between keys in v1 — it introduces complexity that defeats the simplicity goal. Expose `Missing` values distinctly in the response JSON (e.g., `null`) so clients know to apply fallbacks.

**Warning signs:**
- Lookup-based features return 0 sporadically for active merchants
- Features degrade in production after server has been running for days (eviction accumulates)
- Tests using short TTLs show unexpected behavior in cross-key derives

**Phase to address:**
Phase 5 (Remaining Operators / cross-key lookup). Design `FeatureValue` enum to include `Missing` from the start of Phase 1.

---

### Pitfall 11: Python SDK Uses SystemTime for Window Timestamps — Mismatches with Rust Instant

**What goes wrong:**
The Python SDK serializes events with timestamps from `time.time()` (wall clock, UTC). The Rust server uses `std::time::Instant` for window bucket assignment (monotonic, not wall clock). These two time bases are different: `Instant` cannot be converted to a Unix timestamp. If the server needs to compare event timestamps against the current window, mixing `SystemTime` (convertible to Unix epoch) with `Instant` (not) creates subtle bugs — especially during NTP corrections on the server, which jump `SystemTime` backward.

**Why it happens:**
`Instant` is idiomatic for measuring elapsed durations in Rust (monotonic, cheap). `SystemTime` is needed when you need wall-clock time that matches what external clients send. Developers reach for `Instant::now()` because it's what the Tokio docs use, not realizing it cannot be correlated with Unix timestamps from Python.

**How to avoid:**
Use `SystemTime::now()` for all window bucket calculations and event timestamps in the state store. Store window bucket boundaries as Unix timestamps (milliseconds since epoch, u64). Accept `_timestamp` from the client as Unix milliseconds. This allows the Rust side to compare event time with window boundaries consistently. Use `Instant` only for measuring server-internal durations (latency measurements, timeout deadlines). Do not mix the two.

**Warning signs:**
- Window boundaries drift relative to wall clock time over long uptimes
- Events with explicit timestamps fall into wrong buckets
- Tests using fixed event timestamps show non-deterministic window membership

**Phase to address:**
Phase 1 (Core Engine / Window implementation). The time representation decision must be made before any window code is written.

---

## Technical Debt Patterns

| Shortcut | Immediate Benefit | Long-term Cost | When Acceptable |
|----------|-------------------|----------------|-----------------|
| Skip `_timestamp` in PUSH protocol | Simpler wire format, easier to implement | Processing-time windows; features wrong on replay/retry | Never — add it as optional from day one |
| Use `std::HashMap` with SipHash | Zero dependencies | 20-25% CPU overhead at >50K events/sec; misses throughput target | Never for the hot-path state store |
| Skip snapshot version byte | Less code to write | Any struct change renders all snapshots unloadable | Never — one byte prevents catastrophic data loss |
| Monolithic snapshot (no chunked yield) | Simpler code | 100-500ms latency spike every 30 seconds | Never — defeats the latency promise |
| `NaN` for divide-by-zero in derives | No special-casing needed | Silent wrong features; ML models consume NaN silently | Never — define `Missing` semantics explicitly |
| Skip write timeout on TCP connections | Simpler connection handler | Stuck clients OOM server over time | Acceptable in dev/test only |
| HyperLogLog without windowing | Simple HLL implementation | `distinct_count` grows monotonically; never reflects rolling window | Acceptable only if documented as "since startup" semantics |

---

## Integration Gotchas

| Integration | Common Mistake | Correct Approach |
|-------------|----------------|------------------|
| Python SDK → Rust TCP | Use Python `struct.pack(">H")` for string length but forget big-endian for the 4-byte frame length, causing silent misparse | Match the wire format exactly: `u32::from_be_bytes` on Rust, `struct.pack(">I", len)` on Python; write a protocol conformance test |
| Python SDK → Rust TCP | Open a new TCP connection per request (like HTTP) | Maintain a persistent connection pool in the Python SDK; connection setup is ~1ms, destroying the latency budget |
| Python SDK connection pool → Rust server | Pool returns a half-closed connection after Rust server restart | Python pool must detect EOF on read (empty bytes returned) and reconnect; test reconnect behavior explicitly |
| HTTP management API → Pipeline REGISTER | Register a pipeline after events have already arrived for that stream | Server must queue or discard events for unregistered streams gracefully, not panic; define behavior on first REGISTER |
| serde_json → Rust feature response | Return feature value as `f64::NAN` serialized as JSON | JSON spec does not allow NaN/Infinity; `serde_json` will panic or return an error; normalize to `null` in the response serializer |
| Prometheus metrics → HTTP management API | Emit metrics as text in the hot path (PUSH handler) | Metrics must be accumulated as atomic counters and only formatted on the `/metrics` scrape endpoint — never in the event path |

---

## Performance Traps

| Trap | Symptoms | Prevention | When It Breaks |
|------|----------|------------|----------------|
| Per-event `Vec` allocation for derive results | Throughput plateaus ~40K events/sec; allocator shows high churn | Pre-allocate a feature result map per connection and reuse across events | >10K events/sec |
| JSON parsing of full event payload on every PUSH | CPU time dominated by serde_json; simple events cost 10µs not 1µs | Parse only the key field eagerly; defer parsing of other fields to operators that need them | >20K events/sec with large event payloads |
| Locking `Arc<Mutex<HashMap>>` for pipeline registry | Lock contention shows up in flamegraph even though it's read-mostly | Use `arc_swap` or `RwLock` for the pipeline registry; writes (REGISTER) are rare, reads (PUSH) are constant | >5K events/sec with frequent REGISTER calls |
| Snapshot to same file without atomic rename | Server crash during write corrupts the snapshot file permanently | Write to `snapshot.bin.tmp`, then `rename()` atomically; `rename()` is atomic on POSIX | First server crash coinciding with snapshot write |
| Cloning entity keys as `String` per lookup | Memory allocations dominate flamegraph for cross-key lookups | Intern entity keys or use `Arc<str>` for keys appearing in multiple structures | >50K events/sec with cross-key lookups |
| Expiry scan runs on every event | O(n_keys) expiry check per event; 1M keys = 1M checks per event | Run expiry scan as a periodic background task (every 60 seconds), not per-event | >100K unique keys in the state store |

---

## Security Mistakes

| Mistake | Risk | Prevention |
|---------|------|------------|
| No maximum frame length in TCP protocol | Malicious client sends `length = u32::MAX` (4GB); server allocates 4GB and OOMs | Enforce a maximum frame length (e.g., 64MB); disconnect clients sending oversized frames |
| Unbounded expression complexity in REGISTER | Malicious pipeline definition with 10,000 nested derives; evaluation hangs the event loop | Limit expression depth (e.g., max 10 operators per derive chain) and number of features per stream at REGISTER time |
| No REGISTER authentication | Any client can redefine pipelines, wiping all live operator state for existing pipelines | In v1, restrict REGISTER to the HTTP management API (separate port); add IP allowlist or shared secret before production use |
| Derive expressions accessing arbitrary memory fields | `_event.some_deeply_nested.path` that doesn't exist panics in the evaluator | All field accesses must return `Missing` on non-existent paths, never panic; test with malformed events |
| Snapshot file readable by other processes | State contains entity-level behavioral features (user fraud signals) | Ensure snapshot file has permissions 0600; document this in the ops guide |

---

## "Looks Done But Isn't" Checklist

- [ ] **Snapshot recovery:** Loading a snapshot starts the server correctly — but verify that features are actually present and correct for existing keys, not just that the server boots without error
- [ ] **Window expiry:** The 30-minute window looks correct at t=35m — but verify that events from t=0 to t=5m are excluded and events from t=5m to t=35m are included (off-by-one in bucket indexing is common)
- [ ] **distinct_count windowing:** The HLL appears to reset correctly — but verify by checking that `unique_merchants` at t=25h does NOT include merchants from the first hour
- [ ] **Derive with null inputs:** `failure_rate = failed_tx_30m / tx_count_30m` returns a value on first event — verify it does NOT return NaN or panic; it should return `Missing` or `null`
- [ ] **MSET cooperative yielding:** The large MSET appears to complete — but verify PUSH p99 stays under 1ms during concurrent MSET of 50K keys by running both simultaneously in a test
- [ ] **TCP frame parsing:** Protocol tests pass on loopback — but verify with a test that splits every frame at a random byte boundary and confirms correct parsing
- [ ] **Python SDK reconnect:** The Python client works after server restart — but verify it reconnects correctly when the connection is closed mid-request (not just between requests)
- [ ] **Cross-key lookup on evicted key:** FraudSignals view works for active merchants — but verify behavior when the merchant key has been TTL-evicted; it must return `null`, not panic

---

## Recovery Strategies

| Pitfall | Recovery Cost | Recovery Steps |
|---------|---------------|----------------|
| Snapshot unreadable after code change | MEDIUM | (1) Rename corrupt snapshot to `.bak`, (2) restart server (starts fresh), (3) ingest last N hours of events via MSET to rebuild state from offline store |
| Server OOM from slow client buffers | LOW | (1) Identify stuck connection via `debug/memory`, (2) force disconnect, (3) add write timeout to prevent recurrence |
| divide-by-zero panic in expression evaluator | HIGH | Requires code fix + deploy; features are unavailable until patched; prevent by treating this as a correctness invariant in Phase 1 |
| HLL distinct_count wrong (no windowing) | MEDIUM | (1) Document behavior as "since-epoch" in SDK, (2) implement epoch-rotation in next release, (3) existing state is irrecoverable — wait for natural TTL eviction |
| Processing-time vs event-time skew | HIGH | Requires protocol change (adding `_timestamp` to wire format), Python SDK version bump, re-ingestion of affected data; extremely expensive to fix after SDK is shipped |
| Snapshot write corrupts file (no atomic rename) | HIGH | (1) Check if `.tmp` file exists and is valid, (2) manually rename to restore, (3) fix atomic rename in code; if no `.tmp`, start fresh |

---

## Pitfall-to-Phase Mapping

| Pitfall | Prevention Phase | Verification |
|---------|------------------|--------------|
| Snapshot blocks event loop | Phase 4 (Persistence) | Benchmark: PUSH p99 stays flat during periodic snapshot writes |
| bincode schema evolution | Phase 4 (Persistence) | Test: add field to OperatorState, serialize, load from old format, verify graceful error |
| MSET blocking without yield_now | Phase 2 (Server) | Test: concurrent PUSH during 100K MSET, verify PUSH p99 < 2ms |
| Processing-time vs event-time | Phase 1 (Core Engine) | Test: send event with `_timestamp` 10m ago, verify it lands in correct window bucket |
| SipHash throughput bottleneck | Phase 1 (Core Engine) | Benchmark: `benches/throughput.rs` shows >100K events/sec with FxHashMap |
| HLL lacks windowing | Phase 5 (Remaining Operators) | Test: distinct_count at t=25h excludes events from hour 0 |
| TCP partial frame reads | Phase 2 (Server / Protocol) | Test: mock TCP stream delivering frames 1 byte at a time |
| Divide-by-zero in derives | Phase 1 (Core Engine) | Test: first event for new key, all derives with zero denominators return Missing |
| Slow client write buffer growth | Phase 2 (Server) | Test: connect client, stop reading, verify server disconnects client after write timeout |
| Cross-key lookup on evicted key | Phase 5 (Remaining Operators) | Test: lookup on TTL-evicted merchant key returns null, no panic |
| Python/Rust time base mismatch | Phase 1 (Core Engine) | Test: event with explicit Unix timestamp lands in correct bucket on Rust side |
| TCP max frame length | Phase 2 (Server / Protocol) | Test: send frame with length field = MaxU32, verify server disconnects gracefully |

---

## Sources

- Tokio cooperative task yielding: https://tokio.rs/blog/2020-04-preemption
- Tokio top runtime mistakes: https://www.techbuddies.io/2026/03/21/top-5-tokio-runtime-mistakes-that-quietly-kill-your-async-rust/
- Tokio backpressure and Framed I/O: https://biriukov.dev/docs/async-rust-tokio-io/1-async-rust-with-tokio-io-streams-backpressure-concurrency-and-ergonomics/
- TCP protocol framing with Tokio: https://tokio.rs/tokio/tutorial/framing
- Rust HashMap performance (SipHash): https://nnethercote.github.io/perf-book/hashing.html
- bincode compatibility guarantees: https://users.rust-lang.org/t/bincode-compatibility-guarantees/25611
- Rust SystemTime vs Instant: https://doc.rust-lang.org/std/time/struct.Instant.html
- Suspend-aware time bugs in Rust: https://www.rippling.com/blog/rust-suspend-time
- Arroyo sliding window implementation: https://www.arroyo.dev/blog/how-arroyo-beats-flink-at-sliding-windows/
- HyperLogLog in streaming_algorithms crate: https://docs.rs/streaming_algorithms/latest/streaming_algorithms/struct.HyperLogLog.html
- fasteval expression evaluator (safe untrusted expressions): https://github.com/likebike/fasteval
- Real-time feature store late data handling: https://oneuptime.com/blog/post/2026-01-24-streaming-late-data/view
- Training-serving skew: https://www.qwak.com/post/real-time-feature-engineering
- Redis single-thread pitfalls: https://adamdrake.com/redis-performance-triage-handbook.html
- Async backpressure design: https://medium.com/@speedcraft21/async-backpressure-in-rust-designing-systems-that-refuse-work-safely-98f88661a717

---
*Pitfalls research for: real-time in-memory feature server (Tally)*
*Researched: 2026-04-09*
