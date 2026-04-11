# Pitfalls Research: v1.3 Concurrency & Client Batching

**Domain:** Adding key-partitioned multi-threading, async coalescing, client batch API, and off-main-thread snapshots to an existing single-threaded tokio real-time feature server.
**Researched:** 2026-04-11
**Confidence:** HIGH on integration-specific pitfalls (grounded in existing code at cited paths), MEDIUM on prior-art claims (Scylla/Dragonfly/Redis/Aerospike — well-documented public architecture writeups).

---

## Key context verified before writing

- Current `SharedState = Arc<Mutex<AppState>>` (`src/server/tcp.rs:7,91`) — a single `std::sync::Mutex` wraps the whole engine + store + event_log + metrics + latency + throughput. This is the lock v1.3 is trying to shard.
- Lock is held across the *entire* PUSH pipeline, including `event_log.append` (`tcp.rs:331`) — which is I/O. Already a latency risk; becomes a deadlock risk once snapshots run off-thread on another lock holder.
- `handle_push_core_ex` already does `state.lock().unwrap_or_else(|e| e.into_inner())` — the codebase *already ignores mutex poisoning*. This is load-bearing for the parking_lot discussion below.
- Snapshot `spawn_blocking` already exists (`src/main.rs:337`) and clones state under lock on the main thread first (Key Decision table flagged this as "Clone-then-spawn_blocking ⚠️ Revisit"). Phase 15 has to change this model, not invent it.
- Phase 11 post-verification found **three** subtle perf regressions that only showed on multi-pipeline re-verification: HLL read on async path (148× slowdown, large async), drain fast-path regression (166× slowdown, 1M extra syscalls), residual JSON serialize on log append. **Multi-threading will hide similar bugs behind shard-variance noise** unless we bench per-shard, not just aggregate.
- `throughput: ThroughputTracker` and `latency: LatencyTracker` live directly inside `AppState` (`tcp.rs:85-87`), bumped on every push. These are the obvious "shared counter bounces between cores" footgun if not restructured.
- Cross-stream fan-out (`tcp.rs:364-398`) and cross-key lookups (`st.lookup` in `engine/pipeline.rs`) are the cross-shard operations v1.3 has to handle. Fan-out loops over `fan_out_targets()` and writes to multiple streams *using keys from the same event payload* — which means one event can touch keys that hash to different shards.

---

## CRITICAL pitfalls (cause data corruption, deadlock, or silent perf collapse)

### C-1. Cross-shard fan-out deadlock from double lock acquisition
**Phase:** 14
**What goes wrong:** An event pushed to `Transactions` (keyed on `user_id`) fan-outs to `MerchantActivity` (keyed on `merchant_id`). If the routing is "lock shard A, then call `engine.push` which routes to shard B and locks it," any *other* thread doing the reverse (`merchant_id → user_id` via a different stream) deadlocks immediately. Classic AB-BA.
**Warning sign:** Any code in the shard worker that calls into another shard synchronously. Grep for cross-shard calls inside a locked region.
**Prevention:**
- Do NOT hold shard locks across cross-shard dispatch. Fan-out becomes an **asynchronous enqueue** to the target shard's inbox channel. Originating shard releases its lock first, then enqueues.
- Define a global shard ordering: if two shards ever MUST be locked together (there should be zero such cases), always acquire in ascending shard index order.
- Write a loom test that models two fan-out targets whose key hashes land on the opposite shards and runs concurrent pushes in both directions.
- Current fan-out code at `src/server/tcp.rs:364-398` is the site to refactor — today it does `engine.push(target_name, ...)` inline.

### C-2. Event ordering violated by coalescing + cross-shard dispatch
**Phase:** 12 (coalescing introduces it), 14 (sharding makes it worse)
**What goes wrong:** Phase 11 established that errors surface on the client's next call in the order they were pushed (drain-errors semantic, `11-VERIFICATION.md` line 228). Coalescing a connection's inbound frames and then dispatching them to N shards in parallel means events that were received in order A₁,B₁,A₂,B₂ (A routes to shard 1, B to shard 2) are processed out of order. If A₁ errors and B₁ errors, the drain can surface B₁ first.
**Warning sign:** The batch handler iterates events and dispatches to shards without tracking a per-connection sequence number. Tests that push a known-bad event mixed with good events and assert `drain_errors_nonblock` returns them in push order will flake.
**Prevention:**
- Attach a monotonic per-connection `seq: u64` to every coalesced event before dispatch. Errors carry the seq; the drain queue on the connection sorts (or streams in order) by seq before surfacing to the client.
- Alternative: preserve per-connection FIFO by routing *all* of a connection's events through a single ordering barrier before fan-out. Cheaper: keep the coalesced batch as an atomic unit and report batch-level errors with event-index-within-batch (Phase 13's success criterion #5 already implies this — re-use the mechanism in Phase 12).
- Explicit test: bench.py flag that injects a bad event at a known index and asserts drain order.

### C-3. Snapshot "consistent cross-shard view" is impossible without pausing the world
**Phase:** 15 (but planned in 14)
**What goes wrong:** v1.2 snapshots are trivially consistent — one lock, one state, one serialization. Once state is sharded, there is no single lock that gives a consistent snapshot. If the snapshot walks shards sequentially (shard 0 locked, cloned, unlocked; shard 1 locked, cloned, unlocked), a write that touches both shards can land in shard 1's snapshot but not shard 0's. On recovery, features that should have been linked cross-stream appear out of sync.
**Warning sign:** A test that pushes an event fan-outing to two streams on different shards, triggers a snapshot right after, crashes, and asserts both targets recovered. Today (single shard) this passes trivially; post-sharding it will fail intermittently.
**Prevention:**
- **Accept shard-local consistency.** Snapshots are globally eventually-consistent — document this as a v1.3 locked decision. Tally's existing snapshot contract ("lose ~30s on crash") already admits non-determinism, so shard-local is a sibling relaxation.
- Per-shard snapshot files with a **manifest** (`tally.snapshot.manifest.{cycle}`) containing: cycle number, per-shard seq, SHA-256 of each shard file, timestamp. Recovery reads the manifest, verifies all shards hash-match, falls back to previous cycle if not.
- Atomic manifest rename is the commit point, not individual shard renames. This mirrors Redis Cluster's approach to RDB-per-shard (Dragonfly ships one RDB per thread and uses a manifest).
- **Incremental dirty sets become per-shard.** Phase 9 dirty tracking (`store.rs:91-95`) is already a single `AHashSet<EntityKey>`; it must move into `ShardStore`. Do NOT keep a global dirty set — it re-introduces cross-shard contention on every push.

### C-4. Atomic memory ordering: `Relaxed` on the shard routing table
**Phase:** 14
**What goes wrong:** If the shard array is ever rebuilt (even "only at startup" code paths) and the publication uses `Relaxed`, a reader thread can observe a half-initialized `Vec<Mutex<ShardStore>>` header.
**Prevention:**
- **Make the shard vector immutable after startup.** Allocate once, wrap in `Arc<[Shard]>`, never swap. Then ordering is irrelevant. Explicitly document: dynamic rebalancing is a v2+ feature and requires a full re-think.
- If any counter IS shared cross-shard (throughput aggregation in debug UI, see M-2), it must use `AtomicU64::fetch_add` with `Relaxed` (fine for counters) but any *publish* of struct state must use `AcqRel`.

### C-5. `std::sync::Mutex` poisoning vs existing `.unwrap_or_else(|e| e.into_inner())` pattern
**Phase:** 14
**What goes wrong:** Current code at `src/server/tcp.rs:293` (and many other sites) reads `state.lock().unwrap_or_else(|e| e.into_inner())` — it deliberately **ignores** poisoning. This is defensible when there's one lock. With N shards, a panic in shard 3's operator leaves shard 3 poisoned; the next push to shard 3 silently continues against possibly-corrupt state. Worse: postcard serialization during snapshot of a half-mutated operator can produce an unreadable snapshot file that kills recovery.
**Prevention:**
- Use **`parking_lot::Mutex`** for shard locks. No poisoning, faster uncontended path (~2x in benchmarks), smaller footprint. Known caveat: parking_lot isn't reentrant, and its `Guard` isn't `Send` across await points — but we never await inside shard locks anyway (see C-7).
- Audit every `OperatorState::push` and `::read` for panic paths; convert to `TallyError` returns. Any `unwrap`/`expect`/integer overflow in operator hot code is a shard corruption risk.
- On panic in a shard worker, reset that shard's `ShardStore` to empty and re-load from last snapshot — treat it as a local crash.

### C-6. Cache-line false sharing on `Vec<Mutex<ShardStore>>`
**Phase:** 14
**What goes wrong:** Without padding, multiple shards' mutex state and `ShardStore` headers land on the same 64-byte cache line. Shard 0's write to its own mutex state invalidates shard 1's cache line → coherence-traffic storm → performance collapses well before contention-on-the-mutex does. This is *the* canonical shared-nothing footgun; Scylla's Seastar paper, Dragonfly's architecture blog, and Aerospike's per-thread allocator writeup all call it out.
**Warning sign:** Aggregate throughput scales sub-linearly with shard count (e.g., 2 shards give 1.4× not 1.9×).
**Prevention:**
- Wrap each shard in **`crossbeam_utils::CachePadded<Mutex<ShardStore>>`**. Non-negotiable. This is the difference between 10× speedup and 2× speedup on 16 cores.
- Bench specifically: 1, 2, 4, 8, 16 shards on an empty-operator pipeline. If scaling is sub-linear, false sharing is the prime suspect — verify with `perf c2c` or `perf stat -e cache-misses,l1d.replacement`.

### C-7. `std::sync::MutexGuard` held across `.await`
**Phase:** 12, 14
**What goes wrong:** In an async handler, holding a `std::sync::MutexGuard` across an `.await` point is a bug. Compiles silently on `current_thread` runtime; compile-errors on multi-thread. Current code at `tcp.rs:293` avoids this by keeping the handler sync.
**Prevention:**
- Keep shard-lock critical sections strictly synchronous. Coalescing accumulates events in a **connection-local** buffer (no lock), then dispatches in one batched lock acquisition.
- Switch the server tokio runtime to **multi-thread** at the Phase 14 boundary. It will immediately compile-error on any std-Mutex-across-await — free correctness gate.
- **Do NOT use `tokio::sync::Mutex` for shard state.** Tokio Mutex is ~10× slower than parking_lot for uncontended access.

---

## HIGH pitfalls (significant perf loss or hard-to-diagnose flakiness)

### H-1. `ThroughputTracker` and `LatencyTracker` as global shared atomics re-introduce contention
**Phase:** 14
**What goes wrong:** `AppState.throughput` and `AppState.latency` are bumped on every push (`tcp.rs:433,442`). If these stay as one struct and shards write to them directly, every push is a cross-core cache-line bounce → contention equal to a single global lock.
**Prevention:**
- Make trackers **per-shard**. `ShardStore.throughput` and `ShardStore.latency` owned and mutated only by that shard's worker — no atomic, just a plain struct.
- Debug UI does a **scatter-gather** on HTTP GET. Accept ~100µs cost on an HTTP request that runs once per UI poll.

### H-2. Coalescing latency budget breach on sync PUSH
**Phase:** 12
**What goes wrong:** Phase 12's 200µs timer makes async pushes wait. Sync PUSH (OP_PUSH, not OP_PUSH_ASYNC) must **bypass coalescing** — otherwise the sync p99 (currently 87µs) gets 200µs added and breaks the `<100µs` budget.
**Prevention:**
- Coalescing loop matches opcode: `OP_PUSH_ASYNC` → buffer. `OP_PUSH`, `OP_GET`, `OP_FLUSH`, `OP_MSET` → flush current buffer synchronously, then dispatch the sync command immediately.
- Add a sync PUSH mixed with async pushes in the Phase 12 test suite. Assert sync p99 unchanged vs v1.2.

### H-3. Flush-on-error reorders events vs drain semantics
**Phase:** 12
**What goes wrong:** `handle_push_batch` takes one lock and processes a batch. If event 3 of 64 errors, the current `handle_push_core_ex` pattern would be to abort. Client expects events 1,2 applied, event 3 errored, event 4 still processed.
**Prevention:**
- Batch handler applies events in order; on error, records `(seq, error)` and **continues**. Errors are drained in seq order.
- Do NOT wrap the batch in a transaction. Partial success is the correct semantic.

### H-4. Snapshot dirty-set backpressure: dirty set grows faster than off-thread snapshot completes
**Phase:** 15
**What goes wrong:** At sustained 500k eps, dirty keys accumulate faster than the blocking pool can serialize + write. 500k × 30s = 15M dirty keys queued.
**Prevention:**
- **Never start a new snapshot while the previous one is writing.** Snapshot cycle is serialized on itself.
- Add a metric for skipped-snapshot cycles; alert if > 0.
- Replace the clone-then-serialize pattern with a **per-shard streamed serialization**: each shard serializes its own dirty subset directly into the file (spawn_blocking per shard), no clone.

### H-5. Snapshot tmp-file races across shards
**Phase:** 15
**Prevention:**
- Filename: `tally.snapshot.base.{seq}.shard{N}.tmp` → `tally.snapshot.base.{seq}.shard{N}`.
- Manifest file is `tally.snapshot.manifest.{seq}.tmp` → atomic rename on commit.

### H-6. Batch decoding cost exceeds per-event dispatch gain
**Phase:** 13
**What goes wrong:** Same shape as Phase 11's HLL-on-async bug: perf fix introduces new hot path, new hot path has its own bottleneck.
**Prevention:**
- Decode into a pre-sized `Vec<DecodedEvent>` with capacity = N (read count prefix first).
- Re-use a thread-local decode arena if allocation shows up in profile.
- Benchmark decode in isolation **before** wiring it into the server.
- **Run the bench on all three pipeline sizes** (Phase 11 lesson).

### H-7. Batch max size unbounded → OOM attack
**Phase:** 13
**Prevention:**
- Hard cap: max batch events = 16,384 (matches Redis pipeline implicit cap).
- Reject frames that claim more; return `STATUS_ERROR "batch too large"`, close connection.
- Test: raw-TCP test sends `count=10B` and asserts clean reject, no OOM, no crash.

---

## MODERATE pitfalls

### M-1. Work-stealing tokio runtime causes cache-miss storms
**Phase:** 14
**Prevention:**
- **Pin one task per shard** via dedicated `std::thread`s with `core_affinity`, not tokio tasks.
- TCP read loop is tokio; shard workers are plain threads; bridge via channels. This is how Dragonfly and Scylla arrange their reactors.

### M-2. Head-of-line blocking on a hot shard
**Phase:** 14
**Prevention:**
- Document as a known limitation. Uniform hash routing assumes uniform keys.
- Add a `/debug/shards` endpoint that shows per-shard inbox depth and throughput.
- Do NOT add multi-inbox-per-shard priority queues in v1.3. Users can solve hotspots with better key design.

### M-3. Partial batch frame decode on connection drop
**Phase:** 13
**Prevention:** Keep the per-frame fresh allocation. If `read_exact` fails, the whole connection closes, no partial state possible.

### M-4. Per-shard epoch reconciliation on recovery
**Phase:** 14, 15
**Prevention:** Manifest is the source of truth. Cycle is committed only when ALL shards' files exist and match their manifest hashes. Cycle numbers are **global**, not per-shard.

### M-5. Python GIL if batch encoding tempts a C extension
**Phase:** 13
**Prevention:** Keep SDK pure Python. If encoding is slow, use `struct.pack` + `bytearray` + preallocated buffers.

### M-6. Flaky multi-thread bench results make pass/fail gates unreliable
**Phase:** 12, 14
**Prevention:**
- Bench methodology: 5-run median, not single run. Warm-up discarded. Core-pinned via `taskset`.
- Gate as "median >= target AND σ < 10% of median."
- Record per-shard throughput separately.

### M-7. Loom cost: tests too slow to run in CI
**Phase:** 14
**Prevention:**
- Use loom only for shard-dispatch invariants (C-1, C-2, C-3): 2-3 threads, 3-5 ops.
- Gate loom tests behind `cargo test --release --features loom` — not in default CI.

---

## LOW pitfalls

### L-1. Per-stream latency histogram in debug UI becomes per-shard-per-stream
**Phase:** 14
**Prevention:** Scatter-gather on HTTP GET is cheap (~1 QPS from a browser).

### L-2. Drain-errors timing drift under coalescing
**Phase:** 12
**Prevention:** Document. Python tests that probe drain immediately after a bad push must call `flush()` first to cross the coalescing barrier.

### L-3. `cleanup_old_snapshots` with per-shard files
**Phase:** 15
**Prevention:** Extend the cleanup helper to glob `tally.snapshot.*.shard*` and match on cycle seq. Unit test it.

---

## Prior art: what Scylla / Dragonfly / Redis / Aerospike got wrong first time

All MEDIUM confidence (public architecture blogs, cited from memory of well-known writeups):

- **Scylla / Seastar reactor (~2014).** Early versions suffered from tail-latency inversion under skewed workloads. Fix: explicit admission control per shard + cross-shard work donation. Our equivalent: M-2 + inbox depth metric.
- **DragonflyDB (2022).** Initial shared-nothing decision paid off for read throughput but hit a snag on cross-shard multi-key commands (MGET across shards). Eventually scattered the command, gathered results. Our equivalent: C-1.
- **Redis Cluster's MIGRATE command.** Slot rebalancing was notoriously complex and shipped with correctness bugs for years. Lesson: **do not build dynamic rebalancing**. C-4.
- **Aerospike per-thread allocator.** Initial version had false sharing on allocator metadata across NUMA nodes. Fix: per-thread slab allocators + cache-line padding. C-6.
- **Redis RDB-on-fork.** Redis uses `fork()` for snapshot isolation. Tally can't do this on any reasonable platform, so we pay via per-shard manifest + shard-local consistency (C-3). Dragonfly explicitly chose NOT to fork and instead does a custom snapshot iterator — we follow that path.

---

## Phase attribution summary

| # | Pitfall | Phase | Severity |
|---|---------|-------|----------|
| C-1 | cross-shard deadlock | 14 | CRITICAL |
| C-2 | coalescing reorders events | 12 (+ 14) | CRITICAL |
| C-3 | snapshot cross-shard consistency | 14 (plan) + 15 (impl) | CRITICAL |
| C-4 | atomic ordering on routing table | 14 | CRITICAL |
| C-5 | mutex poisoning → parking_lot | 14 | CRITICAL |
| C-6 | false sharing → CachePadded | 14 | CRITICAL |
| C-7 | no std MutexGuard across await | 12, 14 | CRITICAL |
| H-1 | per-shard trackers, no shared atomics | 14 | HIGH |
| H-2 | sync PUSH bypasses coalescer | 12 | HIGH |
| H-3 | batch error semantics preserve drain order | 12 | HIGH |
| H-4 | dirty-set backpressure | 15 | HIGH |
| H-5 | per-shard tmp filenames | 15 | HIGH |
| H-6 | batch decode cost profiling | 13 | HIGH |
| H-7 | batch max size cap | 13 | HIGH |
| M-1 | work-stealing vs pinned threads | 14 | MODERATE |
| M-2 | head-of-line on hot shard | 14 | MODERATE |
| M-3 | partial batch frame | 13 | MODERATE |
| M-4 | per-shard epoch reconciliation | 14, 15 | MODERATE |
| M-5 | Python SDK stays pure Python | 13 | MODERATE |
| M-6 | flaky bench gates → σ not just median | 12, 14 | MODERATE |
| M-7 | loom cost, limit scope | 14 | MODERATE |
| L-1 | scatter-gather for debug histograms | 14 | LOW |
| L-2 | drain timing drift | 12 | LOW |
| L-3 | cleanup_old_snapshots per-shard | 15 | LOW |

---

## The "Phase 11 class" of bug: subtle hot-path regression

Phase 11 shipped its gate cleanly on medium/async/100k-events, then re-verification on (large × async × 3-HLL) found a **148× slowdown** because fan-out re-entered a code path that still paid HLL cost. The v1.3 equivalent traps:

1. **Anything that crosses shard boundaries must be benched on a workload that actually crosses shards.** Single-client bench with one entity key will never touch the cross-shard path. Add a Zipfian multi-key bench to every phase from 14 onward.
2. **Run the full pipeline matrix (small/medium/large)** on every gate. Phase 11 gate was medium-only — it missed the HLL disaster.
3. **Measure per-shard, not just aggregate.** An aggregate 1M eps can hide one shard at 800k and fifteen at 13k.
4. **Bench during a snapshot cycle**, not between them. Phase 15's "<5% regression during write" is only meaningful if the bench overlaps a write.
5. **Benchmark the decode path in isolation before wiring it in** (H-6).

---

## Files cited as evidence

- `src/server/tcp.rs` (lines 7, 47-91, 124-234, 251-398, 433-448)
- `src/state/store.rs` (lines 83-106)
- `src/state/snapshot.rs` (lines 18-32, 88-110)
- `src/main.rs` (lines 320-394)
- `.planning/phases/11-fire-and-forget-push/11-VERIFICATION.md` (lines 119-176)
