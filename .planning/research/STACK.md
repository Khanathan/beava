# Stack Research — v1.3 Concurrency & Client Batching

**Domain:** Multi-threaded Rust streaming engine (key-partitioned shards, async coalescing, off-thread snapshot I/O)
**Researched:** 2026-04-11
**Confidence:** HIGH (all crate versions verified against crates.io API; RUSTSEC cross-checked)

**Scope boundary:** This doc covers ONLY the additions needed for v1.3 Phases 12-15. Tokio, ahash, postcard, winnow, axum, serde, bytes are already locked in and not re-evaluated. The question on the table is: "what new crates (if any) join the dependency list, and how do existing ones reconfigure, to support shard workers + coalescing + batch opcode + off-thread snapshots?"

---

## TL;DR Recommendations

| Need | Recommendation | Add dependency? |
|------|---------------|-----------------|
| Shard map primitive | `Vec<parking_lot::Mutex<ShardStore>>` - one lock per shard, hand-routed | **Add** `parking_lot = "0.12"` |
| Cross-shard fan-out channel | `crossbeam-channel 0.5.15` (bounded, MPMC) | **Add** `crossbeam-channel = "0.5.15"` |
| Per-connection -> shard dispatch | `tokio::sync::mpsc` (already available) - each shard owns one `Receiver` | No new dep |
| Coalescing flush timer | `tokio::time::Instant` + explicit deadline check in `select!` loop (NOT `sleep`) | No new dep |
| Shard count default | `std::thread::available_parallelism()` (std, stable since 1.59) | No new dep |
| Worker thread model | Dedicated `std::thread` per shard, each with its own `tokio::current_thread` runtime | No new dep |
| Core pinning (optional, Phase 14b) | `core_affinity 0.8.3` | **Add gated** |
| Cache-line padding for shard hot fields | `crossbeam-utils::CachePadded` 0.8.21 | **Add** `crossbeam-utils = "0.8.21"` |
| OP_PUSH_BATCH wire encoding | Hand-roll (consistency with OP_PUSH_ASYNC - no new dep) | No new dep |
| Multi-threaded metrics counters | `std::sync::atomic::AtomicU64` with `Relaxed` ordering + `CachePadded` | No new dep |
| Snapshot I/O | `tokio::task::spawn_blocking` with `max_blocking_threads(2)` | No new dep |

**Net new crates:** `parking_lot`, `crossbeam-channel`, `crossbeam-utils`, and `core_affinity` (feature-gated). Four crates for the largest architectural change since v1.0 is acceptable for a single-binary project.

---

## Core Technologies (new for v1.3)

| Technology | Version | Last Publish | Purpose | Why Recommended |
|------------|---------|--------------|---------|-----------------|
| `parking_lot` | 0.12.5 | 2025-10-03 | Per-shard mutex for `ShardStore` | 25-40ns uncontended lock/unlock vs `std::sync::Mutex` ~60-80ns on Linux. No poisoning. Critical for sub-us per-event target: the v1.2 hot path already pays one `std::Mutex` per push, shards multiply that by coalesce-batch-size, so the per-lock cost matters. Widely deployed, audited, zero open RUSTSEC advisories on current versions. |
| `crossbeam-channel` | 0.5.15 | 2025-04-08 | Cross-shard fan-out (stream A on shard 3 writes to stream B on shard 7) | MPMC bounded, ~20ns/op under low contention, clean integration with sync shard workers. 0.5.15 **patches RUSTSEC-2025-0024** (double-free on Drop introduced in 0.5.12). Use this, not `tokio::sync::mpsc`, for cross-shard because (a) MPMC is required (any shard can fan out to any shard), (b) it is sync, letting shard workers drain without `await` overhead on the hot path. |
| `crossbeam-utils` | 0.8.21 | 2024-12-15 | `CachePadded<T>` for per-shard hot fields | False-sharing on `AtomicU64` metrics counters costs 30-100ns per contended read when two shards share a 64B cache line. `CachePadded` is arch-portable (64B x86_64, 128B aarch64); a hand-rolled `#[repr(align(64))]` is a cross-compile footgun. |
| `core_affinity` | 0.8.3 | 2025-03-01 | Pin shard workers to CPU cores (Phase 14b optional) | 5-15% throughput gain under NUMA-sensitive workloads by preventing migration-induced L1/L2 thrash. Linux/macOS/Windows support. Gate behind `--features core-affinity` so container environments (where affinity is often a no-op or blocked) do not pay for it. |

---

## Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `tokio` (existing) | 1.51.1 | Runtime for TCP accept loop + HTTP API | Phase 14. Keep tokio for TCP/HTTP but run shard workers on dedicated `std::thread`s with their own `current_thread` runtimes (see Worker Model). No version bump needed. |
| `tokio::sync::mpsc` (existing) | 1.51.1 | Connection -> shard dispatch | Each shard owns one `mpsc::Receiver<ShardCommand>`. Each TCP connection handler holds clones of all `Sender`s (indexed by shard). MPSC is correct here because there is exactly one consumer (the shard worker). Use **bounded** with capacity ~1024 - backpressure is preferable to unbounded memory growth. |
| `std::thread::available_parallelism()` | std | Shard count default | Preferred over `num_cpus::get()` - stable since Rust 1.59, honors cgroup CPU quota, returns `NonZeroUsize` forcing explicit default handling. **Do NOT add `num_cpus` crate.** |
| `std::sync::atomic::AtomicU64` | std | Throughput/latency counters across shards | `Relaxed` ordering is sufficient for metrics (no causal dependency). Wrap in `CachePadded` when counters sit on the same struct. For per-shard counters that only the owning shard writes but `/metrics` reads, this is the cheapest option - no crate needed. |
| `tokio::task::spawn_blocking` (existing) | 1.51.1 | Phase 15 snapshot I/O | Tokio docs explicitly call this out as correct for "bounded CPU/IO work that should not block the async runtime". Bound the blocking pool: `Builder::max_blocking_threads(2)` - one for snapshot write, one headroom for MSET spill. |

---

## Development Tools (unchanged)

No new dev tooling. Existing cargo/rust-toolchain/clippy/cargo-nextest remain sufficient. Add one benchmark harness variant:

| Tool | Purpose | Notes |
|------|---------|-------|
| `benchmark/tally-throughput/bench.py --mode multi-shard` | Phase 14 gate | Exercises aggregate throughput with N clients x N shards. Existing harness extends, no new framework. |
| `benchmark/tally-throughput/bench.py --mode async-batch` | Phase 13 gate | Exercises `app.push_many()`. Called out in Phase 13 success criterion 6. |

---

## Detailed Evaluation

### 1. Shard map primitive - why hand-rolled `Vec<parking_lot::Mutex<ShardStore>>` wins over dashmap / scc / sharded-slab

**Recommendation:** Hand-rolled `Vec<Mutex<ShardStore>>` where `ShardStore` wraps the existing `AHashMap<EntityKey, EntityState>`.

**Why not `dashmap 6.1.0`:** DashMap already internally shards (default 4x num_cpus shards with RwLock per shard). But:
- It shards at the **map entry** granularity. Tally needs to shard at the **worker/thread** granularity so that *one* thread owns *one* shard and no cross-thread lock traffic happens on the hot path.
- DashMap's sharding hash is opaque - you cannot cheaply answer "which shard does key K live in?" from outside, which is exactly what the TCP dispatch loop needs to route to the right worker.
- RwLock adds overhead (~50ns vs parking_lot Mutex ~25ns) and Tally's writes-per-read ratio on the hot path is close to 1:1 (every PUSH is RMW) - RwLock is the wrong primitive.
- Cross-shard fan-out still needs a separate channel anyway, so DashMap does not eliminate the channel, it adds a second layer of sharding on top.
- Historical: RUSTSEC-2022-0002 (unsoundness, long since patched) - not disqualifying but worth noting.

**Why not `scc::HashMap 3.6.12`:** Excellent crate, lock-free reads, actively maintained (last publish 2026-03-24). But:
- Lock-free reads are wasted under a shard-owning worker model - the shard already has exclusive access, zero contention.
- Larger dependency footprint (pulls `sdd` for memory reclamation) for zero benefit in Tally's access pattern.
- Same routing problem as DashMap - you cannot steer an event to "the shard that owns K" without re-hashing the key separately.

**Why not `sharded-slab 0.1.7`:** Optimized for integer-keyed slabs (think connection IDs). Last publish 2023-10 (stale). String-keyed entities do not fit the model.

**Why hand-rolled wins:**
```rust
pub struct ShardedStore {
    shards: Box<[parking_lot::Mutex<ShardStore>]>,  // Box<[T]> not Vec - len fixed post-init
    shard_mask: usize,                               // shard_count - 1 (power-of-2 mask)
    hasher: ahash::RandomState,                      // shared across shards for key -> shard routing
}
impl ShardedStore {
    #[inline]
    pub fn shard_idx(&self, key: &str) -> usize {
        use std::hash::BuildHasher;
        self.hasher.hash_one(key) as usize & self.shard_mask
    }
}
```
- **Zero indirection beyond the existing `AHashMap`** - `ShardStore` is just the v1.2 `StateStore` fields, one instance per shard.
- **Routing is 2 instructions** (hash + mask) and happens *in the dispatch loop, not under a lock*.
- **Snapshot serialization works per-shard for free** (Phase 15 and future delta snapshots benefit).
- **Reverts cleanly** - `shard_count = 1` reproduces v1.2 behavior, lets you A/B the whole milestone behind a config flag.

**Cost:** ~200 lines of glue code across `state/store.rs` and `server/tcp.rs`. Cheap insurance for a hot-path primitive.

---

### 2. Channel for cross-shard fan-out - `crossbeam-channel` vs `flume` vs `tokio::mpsc`

**Recommendation:** `crossbeam-channel 0.5.15` bounded MPMC for cross-shard fan-out. `tokio::sync::mpsc` for connection -> shard dispatch.

**Split rationale - two different channels for two different jobs:**

| Channel | Producer | Consumer | Async context? | Pick |
|---------|----------|----------|----------------|------|
| Connection -> Shard | Many TCP handlers (async tasks) | One shard worker | Producer is async | `tokio::mpsc` bounded(1024) |
| Shard A -> Shard B (cross-shard fan-out) | Any shard worker (sync) | Any shard worker (sync) | Both sides sync | `crossbeam-channel` bounded(4096) |

**Why not one unified channel?** The cross-shard fan-out path runs inside the shard worker's synchronous critical section - it has already taken the shard's lock, processed the primary event, and now needs to hand off a derived fan-out event to another shard. Calling `.await` there would require releasing the lock, which defeats the batching win. `crossbeam-channel::try_send` is ~20ns and stays sync.

**Why `crossbeam-channel 0.5.15` over `flume 0.12.0`:**
- Both are sync MPMC, both are well-maintained, both support bounded with backpressure.
- `crossbeam-channel` is the ecosystem-standard sync channel (399M downloads) and tokio internals already use `crossbeam-utils`/`crossbeam-epoch`, so pulling it in adds no transitive novelty.
- Published microbenchmarks (matklad/kprotty, 2023) show crossbeam-channel ~1.5-2x faster uncontended bounded MPSC vs flume. For Tally's "mostly zero cross-shard traffic" workload, the uncontended fast path is what matters. MEDIUM confidence on the ratio - either choice would work functionally.
- **Critical:** 0.5.15 specifically patches **RUSTSEC-2025-0024** (double-free on Drop, introduced in 0.5.12, patched 2025-04-08). Pinning `crossbeam-channel = "0.5.15"` is mandatory - 0.5.12-0.5.14 are unsafe.
- `flume` has no comparable recent advisory but has less battle-testing in high-core-count production.

**Verified maintenance (crates.io API 2026-04-11):**
- `crossbeam-channel`: 399M downloads, last publish 2025-04-08 (the security patch), actively maintained by crossbeam-rs org.
- `flume`: 147M downloads, last publish 2025-12-08, also actively maintained.

**Bounded over unbounded:** Unbounded cross-shard channels are a memory-growth DOS vector. A bounded channel with `try_send` -> drop-and-error-on-full gives explicit backpressure that surfaces through the existing `drain_errors_nonblock` path. Pick capacity 4096 per shard-pair initially, tune in Phase 14 bench.

---

### 3. Coalescing flush timer - **do NOT use `tokio::time::sleep(200us)`**

**Recommendation:** Deadline-based pattern with `tokio::time::sleep_until` in a `select!` loop, deadline only armed when the batch buffer is non-empty. No new timer crate.

**Why not `tokio::time::sleep(Duration::from_micros(200))` naively:**
- Tokio docs (verified at docs.rs/tokio/latest/tokio/time/fn.sleep.html): "Sleep operates at **millisecond granularity** and should not be used for tasks that require high-resolution timers."
- On Linux, tokio's timer wheel tick is 1ms by default. A 200us sleep will schedule at the next wheel tick - effectively 1000us minimum.
- Even `MissedTickBehavior::Burst` on `tokio::time::interval(200us)` rounds up to 1ms in practice.

**Why not a third-party timer wheel (`tokio-timer-wheel`, `hashed-wheel-timer`, etc.):** These target sub-ms timer *fan-out* (e.g., 100k scheduled tasks) - overkill for a single per-connection deadline, and they add a dep for something that is a few lines of code with the right pattern.

**Correct pattern (inline, no new crate):**
```rust
// Per-connection coalescing loop
let flush_after = Duration::from_micros(200);
let mut buf: Vec<Frame> = Vec::with_capacity(64);
let mut deadline: Option<Instant> = None;
let far_future = Instant::now() + Duration::from_secs(3600);

loop {
    let sleep_target = deadline.unwrap_or(far_future);
    tokio::select! {
        biased;
        frame = read_frame(&mut reader), if buf.len() < MAX_BATCH => {
            let frame = frame?;
            if buf.is_empty() { deadline = Some(Instant::now() + flush_after); }
            buf.push(frame);
            if buf.len() >= MAX_BATCH {
                flush(&mut buf, &shards).await;
                deadline = None;
            }
        }
        _ = tokio::time::sleep_until(sleep_target.into()), if deadline.is_some() => {
            flush(&mut buf, &shards).await;
            deadline = None;
        }
    }
}
```
**Key insight:** The 1ms tokio timer granularity is acceptable here because:
- 200us target is approximate - "flush within roughly 200-1000us" is fine for async fire-and-forget.
- Deadline only arms when buf is non-empty, so idle connections pay nothing for timer churn.
- Under high load (the interesting case), MAX_BATCH fills before the timer fires, so the timer is bypassed entirely. The timer is only a lower-traffic safety net.

**Alternative if sub-ms precision ever becomes critical:** Dedicated `std::thread` per shard with a busy-ish `park_timeout(100us)` loop. Do not build this until benchmarks prove it is needed. Phase 12 exit criteria (>=200k eps @ 4 clients) does not require sub-ms timer precision.

---

### 4. Worker thread model - dedicated `std::thread` per shard + `current_thread` runtime each

**Recommendation:** One dedicated `std::thread` per shard, each running its own `tokio::runtime::Builder::new_current_thread()` runtime. A separate multi-thread tokio runtime hosts TCP accept + HTTP API.

**The four options evaluated:**

| Option | Verdict | Reason |
|--------|---------|--------|
| A. Tokio `current_thread` (v1.2 status quo) | **Rejected** | Cannot scale past 1 core - the entire reason for v1.3. |
| B. Tokio multi-thread work-stealing, shards as plain tasks | **Rejected** | Work-stealing undermines the point. A shard task scheduled on worker 3 one tick and worker 7 the next destroys L1/L2 cache locality and introduces the exact cross-thread traffic shards are supposed to eliminate. |
| C. Multi-thread tokio + `LocalSet::spawn_local` per shard | Workable but subtle | `LocalSet` keeps `!Send` futures pinned to a worker, but you still pay the multi-thread scheduler overhead and bindings are fiddly. |
| **D. Dedicated `std::thread` per shard, each with its own `current_thread` tokio runtime** | **Recommended** | Each shard is one OS thread that owns its state outright. Same `current_thread` runtime Tally has shipped for v1.0-v1.2, just replicated N times. |

**Concrete shape:**
```rust
// main.rs
let shard_count = std::thread::available_parallelism()
    .map(|n| n.get()).unwrap_or(1)
    .next_power_of_two().min(64);

let (shard_senders, shard_receivers) = build_shard_channels(shard_count);
let shards: Vec<Arc<parking_lot::Mutex<ShardStore>>> = (0..shard_count)
    .map(|_| Arc::new(parking_lot::Mutex::new(ShardStore::default())))
    .collect();

for shard_id in 0..shard_count {
    let shard = shards[shard_id].clone();
    let rx = shard_receivers[shard_id].take().unwrap();
    std::thread::Builder::new()
        .name(format!("tally-shard-{shard_id}"))
        .spawn(move || {
            let local_rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            #[cfg(feature = "core-affinity")]
            if let Some(aff) = core_affinity::get_core_ids() {
                core_affinity::set_for_current(aff[shard_id % aff.len()]);
            }
            local_rt.block_on(shard_worker_loop(shard, rx));
        })?;
}

// Main tokio runtime hosts TCP accept + HTTP
let rt = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(2)             // accept + HTTP API
    .max_blocking_threads(2)       // Phase 15 snapshot + headroom
    .enable_all()
    .build()?;
rt.block_on(run_tcp_server(shard_senders, http_state))
```

This gives:
- One OS thread per shard, no migration.
- Each shard sees the *exact* v1.2 `current_thread` runtime behavior, just parallelized.
- All v1.2 internals keep working per shard because each shard is effectively single-threaded.
- Optional `core_affinity` pin is literally one line, feature-gated.
- Zero `!Send`/`Send` gymnastics - each shard's state is `!Send` and never crosses.
- Blocking pool (`max_blocking_threads(2)`) lives on the accept runtime and is shared only by infrastructure tasks (Phase 15 snapshot writes).

**Why not multi-thread work-stealing even with LocalSet:** Work-stealing scheduler adds overhead that the per-shard model avoids, and `LocalSet::spawn_local` has historically sharp edges (running futures across `LocalSet` boundaries, send/recv lifetimes). The std-thread approach is ~30 lines of boot code and gives exact control.

**NUMA:** On single-socket boxes (typical for Tally deployments - 16-64 core AMD/Intel workstation or cloud VM) NUMA is irrelevant. On multi-socket boxes, `core_affinity` pinning gives a small win but is not worth first-class complexity. **`hwloc 0.5.0` crate is abandoned (last publish 2017-04-26) - do NOT pull it in.** If serious NUMA work is needed, revisit in a future milestone.

---

### 5. Shard count default - `std::thread::available_parallelism()`

**Recommendation:** `std::thread::available_parallelism()` from std. **Do NOT add `num_cpus`.**

| | `std::thread::available_parallelism` | `num_cpus::get()` |
|---|---|---|
| Stable since | Rust 1.59 (March 2022) | forever |
| Honors cgroup v2 quota | YES (respects `cpu.max`, cpuset) | Partial - behavior varies across versions |
| Dependency cost | Zero | 1 crate |
| Returns | `NonZeroUsize` | `usize` |
| Maintainer | rust-lang | seanmonstar (active) |

`available_parallelism` on Linux reads the process cpuset and cgroup v2 `cpu.max` - exactly what you want in a container. `num_cpus` exists for historical reasons; there is no reason to add it to a new project in 2026.

```rust
let shard_count = std::thread::available_parallelism()
    .map(|n| n.get())
    .unwrap_or(1)
    .next_power_of_two()   // round UP to enable `& mask` routing
    .min(64);              // cap - more shards than cores is counterproductive
```

---

### 6. Cache-line padding - `crossbeam-utils::CachePadded`

**Recommendation:** Add `crossbeam-utils = "0.8.21"` solely for `CachePadded<T>`.

**Why:** The per-shard metrics counters (`events_total`, `push_latency_ewma`, etc.) will be read by the `/metrics` HTTP handler from another thread while shard workers write them from their own thread. If two shard counters land on the same 64-byte cache line, every read by `/metrics` invalidates the writer's cached line - 30-100ns stall per contended access. Multiply by N shards x read frequency and metrics scrape can visibly drag the hot path.

```rust
use crossbeam_utils::CachePadded;
pub struct ShardMetrics {
    pub events_total:         CachePadded<AtomicU64>,
    pub push_latency_ns_sum:  CachePadded<AtomicU64>,
    pub push_latency_count:   CachePadded<AtomicU64>,
}
```

**Why the crate and not hand-rolled `#[repr(align(64))]`:** Cache line size varies by arch (64 on x86_64, 128 on aarch64 Apple Silicon, 256 on some ARM cores). `CachePadded` gets the target right automatically. A 5-line hand-roll is a cross-compile footgun waiting to happen.

**Version:** 0.8.21, last publish 2024-12-15, 594M downloads. Trivially stable.

---

### 7. Atomics for metrics - `AtomicU64` with `Relaxed`

**Recommendation:** `std::sync::atomic::AtomicU64` with `Ordering::Relaxed` for counters. **Do NOT add `arc-swap` or `crossbeam::atomic::AtomicCell`.**

**Why Relaxed is sufficient:** Metrics counters have no causal dependency on any other data. `/metrics` just needs an eventually-consistent read; it does not need to observe counters in any particular order relative to other memory. `Relaxed` `fetch_add` is ~1-2ns on x86 (`lock xadd`) vs ~5ns for `AcqRel`. Multiply by every PUSH x every counter and it adds up.

**Why not `arc-swap 1.9.1`:** Arc-swap is for atomically replacing whole `Arc<T>` references - e.g., hot-reloading a pipeline config. That is a real need in Tally (REGISTER command swaps the pipeline) but it is a **Phase 14 follow-up**, not a metrics requirement. If REGISTER-during-push becomes a problem after shards land, add arc-swap then. For counters it is the wrong tool.

**Why not `crossbeam::atomic::AtomicCell`:** Supports arbitrary `T` via locks internally when `T` is larger than a machine word. For `u64` it falls back to `std::sync::atomic::AtomicU64` anyway - no value add.

---

### 8. `OP_PUSH_BATCH` (0x0A) wire encoding - hand-roll

**Recommendation:** Hand-roll the encoding in `src/server/protocol.rs` following the exact pattern of `OP_PUSH_ASYNC`.

**Why not serde/postcard/bincode/protobuf on the wire:** The v1.2 wire format is length-prefixed binary. Adding a serialization library to the wire protocol would:
- Contradict the "kill JSON on hot path" v1.2 win (postcard-decode is faster than JSON but slower than `u32::from_be_bytes` + slice copy).
- Pull a new dep onto the hot path.
- Break symmetry with the existing hand-rolled `OP_PUSH_ASYNC` encoder.

**Proposed format (consistent with v1.2 style):**
```
OP_PUSH_BATCH frame (opcode 0x0A):
  [u16 BE: stream_name length]
  [N bytes: stream_name UTF-8]
  [u32 BE: event count]
  repeated event_count times:
    [u32 BE: event payload length]
    [N bytes: binary wire event (v1.2 format)]
```

Decoder is ~30 lines in `protocol.rs`. Each inner event reuses the existing v1.2 binary wire event decoder - no new parsing logic, just a framing wrapper. Matches the symmetry with `OP_PUSH_ASYNC` so developers familiar with v1.2 can read the batch path at a glance.

**Error attribution for partial-batch failures (Phase 13 success criterion 5):** Include event index in error payload. Client-side `drain_errors_nonblock` returns `(batch_seq, event_index_within_batch, err)` tuples. Implementation detail, no crate impact.

---

### 9. Snapshot I/O off main thread - `spawn_blocking` with bounded pool

**Recommendation:** `tokio::task::spawn_blocking` on the main runtime with `max_blocking_threads(2)`. **Do NOT add `rayon`.**

**Why `spawn_blocking`:**
- Tokio documentation explicitly names this as the correct pattern for "bounded synchronous work that must not block the event loop" (docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html).
- Reuses existing runtime - no second thread pool to manage.
- The v1.2 clone-then-spawn_blocking snapshot path already uses this pattern; Phase 15 hardens it (bounded pool, per-shard clone semantics under Phase 14) and guarantees no shard worker thread runs snapshot serialization inline.

**Why `max_blocking_threads(2)` and not the tokio default (512):**
- Only 2 things should run on the blocking pool: snapshot write (one at a time; concurrent snapshot writes would race the file atomic-rename) + MSET chunk spill (rare). Capping at 2 means the blocking pool can never accidentally consume more cores than shards.
- Leaves shard worker cores fully available to shard workers during snapshot writes.

**Why not `rayon 1.11.0`:** Rayon is a data-parallel compute pool for CPU-bound work you want to fan out. Snapshot serialization is I/O-bound (disk write dominates) - parallelizing the serialization does not help and rayon's work-stealing scheduler would interfere with shard workers. Wrong tool.

**Why not a dedicated `std::thread::spawn`:** Would require a second channel to communicate with the main runtime for snapshot completion signaling. `spawn_blocking` gives you `.await` on a `JoinHandle` for free.

**Integration with Phase 14 per-shard state:**
```rust
// Phase 15: snapshot trigger
let shard_snapshots: Vec<_> = shards.iter()
    .map(|s| s.lock().clone_for_snapshot())  // short critical section per shard
    .collect();
// Release all shard locks. Serialize off-thread.
tokio::task::spawn_blocking(move || {
    let full = merge_shards(shard_snapshots);
    write_postcard_atomic(&path, &full)?;
    Ok::<_, TallyError>(())
}).await??;
```
The clone-per-shard pattern keeps snapshot-vs-hot-path lock duration bounded to the clone cost. Memory amplification is distributed across shards, making worst-case per-shard rather than per-process.

---

## Installation

```toml
# Cargo.toml additions for v1.3
[dependencies]
# ... existing v1.2 deps unchanged ...
parking_lot       = "0.12"          # 0.12.5 - per-shard mutex
crossbeam-channel = "0.5.15"        # MUST be >=0.5.15 (RUSTSEC-2025-0024 patched)
crossbeam-utils   = "0.8"           # 0.8.21 - CachePadded

[dependencies.core_affinity]        # optional, feature-gated
version = "0.8"                     # 0.8.3
optional = true

[features]
core-affinity = ["dep:core_affinity"]
```

No changes to existing `tokio`, `ahash`, `postcard`, `winnow`, `serde`, `axum`, `bytes` entries. Net lockfile growth: ~4 crates + crossbeam transitive deps (partially present already via other crates).

---

## Alternatives Considered

| Recommended | Alternative | When to Use Alternative |
|-------------|-------------|-------------------------|
| `Vec<parking_lot::Mutex<ShardStore>>` | `dashmap 6.1.0` | Only if you need ad-hoc concurrent access from arbitrary threads without owning the shard. Tally's worker-per-shard model makes this strictly worse. |
| `Vec<parking_lot::Mutex<ShardStore>>` | `scc::HashMap 3.6.12` | Only if you had many readers and few writers. Tally is write-dominated on the hot path. |
| `crossbeam-channel 0.5.15` | `flume 0.12.0` | Roughly interchangeable for correctness. Prefer flume only if you want a single channel crate that also supports async - Tally already has tokio::mpsc for async. |
| `parking_lot::Mutex` | `std::sync::Mutex` | If you want zero new dependencies and are willing to eat ~2x lock cost and poisoning semantics. For v1.3 that tradeoff is wrong - parking_lot is the idiomatic choice. |
| `parking_lot::Mutex` | `parking_lot::RwLock` | Only if the shard becomes read-dominated (e.g., GET-heavy workload). Phase 14 starts write-heavy; revisit per benchmark data. |
| `std::thread::available_parallelism()` | `num_cpus 1.17.0` | Never on modern Rust - std is strictly better. |
| Hand-rolled `OP_PUSH_BATCH` encoding | `postcard`/`bincode` wire frames | Never on hot path. |
| `spawn_blocking` for snapshots | `rayon 1.11.0` thread pool | Never for I/O. Only consider rayon for parallel operator computation (different problem, different milestone). |
| `spawn_blocking` | Dedicated `std::thread::spawn` | If snapshot serialization grows to dominate a core, move it to a dedicated thread. Phase 15 exit bar (<=5% regression) is unlikely to require this. |
| `tokio::time::sleep_until` deadline | `tokio::time::sleep(200us)` | Never - 1ms floor makes micro-sleeps useless for 200us targets. |
| `CachePadded` | Hand-rolled `#[repr(align(64))]` | Never - arch-portability footgun. |
| `Relaxed` atomics for metrics | `AcqRel` / `SeqCst` | Only if a counter participates in a happens-before relation with other data. Metrics do not. |
| Dedicated `std::thread` per shard | `tokio::task::LocalSet::spawn_local` on multi-thread runtime | If you want to stay on a single tokio runtime and are willing to take on LocalSet subtleties. Functionally workable, operationally more complex. |

---

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| `crossbeam-channel` versions **0.5.12, 0.5.13, 0.5.14** | **RUSTSEC-2025-0024** - double-free on Drop, memory corruption | `crossbeam-channel = "0.5.15"` (patched) or `<=0.5.11` (unaffected but ancient) |
| `hwloc 0.5.0` | Last published 2017-04-26, unmaintained, API is stale | `core_affinity 0.8.3` for simple pinning; accept NUMA gaps until proven to matter |
| `num_cpus` (new dep) | Superseded by `std::thread::available_parallelism()` | std built-in, no dep |
| `rayon` for snapshot I/O | Data-parallel compute pool, wrong shape for serial disk I/O | `tokio::task::spawn_blocking` |
| `tokio::time::sleep(Duration::from_micros(200))` for coalescing | 1ms wheel granularity -> effective 1000us, dominates async latency | Polled deadline with `sleep_until` in `select!` loop |
| `dashmap` as primary shard primitive | Worker-per-shard model benefits from owning the map exclusively; DashMap's internal sharding is redundant and un-routable | Hand-rolled `Vec<Mutex<ShardStore>>` |
| `crossbeam::atomic::AtomicCell<u64>` | Wraps std `AtomicU64` anyway - no value add | `std::sync::atomic::AtomicU64` |
| `arc-swap` for metrics counters | Wrong primitive - it is for swapping whole `Arc<T>` references | `AtomicU64` for counters (arc-swap may still be right for REGISTER, later) |
| `serde`/`postcard`/`bincode` framing on `OP_PUSH_BATCH` wire | Adds CPU cost on the hot path the v1.2 work explicitly removed | Hand-rolled length-prefix framing |
| Unbounded `mpsc` / `crossbeam-channel` | Memory DOS on producer spike | Bounded with explicit backpressure -> error surfaces via existing drain_errors path |
| **`bincode`** (already excluded in v1.2) | RUSTSEC-2025-0141, unmaintained | `postcard` (existing v1.2 decision, still correct) |
| `sharded-slab 0.1.7` | Integer-keyed slabs, last publish 2023-10 (stale), wrong data shape | Hand-rolled shard vec |

---

## Stack Patterns by Variant

**If deploying in a container with `cpu.max` quota (cgroup v2):**
- `std::thread::available_parallelism()` already honors the quota - no change needed.
- `core_affinity` may be a no-op or blocked; feature-gate with `core-affinity` off by default.

**If running on bare-metal multi-socket server (NUMA):**
- Enable `core-affinity` feature and pin shard i to core i.
- Consider a follow-up (post v1.3) to NUMA-local-allocate shard state. Not v1.3 scope.

**If shard count needs to be runtime-configurable:**
- Config file / CLI flag overrides `available_parallelism()` default.
- Enforce power-of-2 (for mask routing) or fall back to modulo - decide at config load.
- **Critical:** shard count must be baked into the snapshot format header. If a snapshot was written with 16 shards and recovery runs on 8, re-route all keys on load. Handle this in Phase 14 snapshot migration logic (success criterion 4).

**If a specific workload is GET-dominated (not the common case):**
- Swap `parking_lot::Mutex` -> `parking_lot::RwLock` per shard.
- Measure before switching - Mutex wins when writes are frequent.

---

## Version Compatibility

| Package | Compatible With | Notes |
|---------|-----------------|-------|
| `parking_lot = "0.12.5"` | `tokio 1.51`, `ahash 0.8` | MSRV 1.64, well within Tally's toolchain. |
| `crossbeam-channel = "0.5.15"` | all | Pulls `crossbeam-utils 0.8`. Must pin >= 0.5.15 for RUSTSEC-2025-0024. |
| `crossbeam-utils = "0.8.21"` | all | Shared with crossbeam-channel, dedupes in the lockfile. |
| `core_affinity = "0.8.3"` | all | Zero runtime deps on Linux (uses libc). |
| `tokio = "1.51"` | existing | No version bump needed; multi_thread runtime is stable since 1.0. `available_parallelism` is std (1.59+). |
| Snapshot format | v1.2 postcard format | Phase 14 must add shard count to header OR re-partition on load. Preserving v1.2 compatibility is Phase 14 success criterion 4. |

---

## Hot-Path Cost Budget (per-event accounting for Phase 14)

For the sub-us per-event target, here is what each recommendation adds (MEDIUM confidence - estimates from published microbenchmarks, validate in Phase 14 bench):

| Operation | Estimated cost | Notes |
|---|---|---|
| Key hash (ahash) - already paid in v1.2 | ~10ns | Reuse same hasher for shard routing -> zero additional cost |
| Shard index: `hash & mask` | ~1ns | One AND instruction |
| `parking_lot::Mutex::lock` uncontended | ~25ns | Replaces existing `std::Mutex::lock` (~60ns), **net -35ns** |
| Per-shard `AHashMap` lookup - same as v1.2 | ~40ns | Unchanged |
| `CachePadded<AtomicU64>::fetch_add(Relaxed)` | ~2ns | Amortized - one per push for metrics |
| Cross-shard fan-out (when needed) | ~30ns | `crossbeam-channel::try_send` - only fires on fan-out streams |
| **Net per-event change** | **~-30 to 0 ns** | parking_lot savings offset shard routing cost |

Coalescing (Phase 12) amortizes the lock acquisition cost across the batch, so the per-event lock cost drops to roughly `25ns / batch_size`. At batch_size=64 that is ~0.4ns per event.

**Conclusion:** The recommended stack has no hot-path cost regression vs v1.2 single-threaded. The throughput win comes from N shards running in parallel, not from cheaper per-event work. The single-client regression guardrail in Phase 14 success criterion 6 (+/-10%) is achievable because routing overhead is balanced by the parking_lot lock savings.

---

## Sources

- **crates.io registry API** (queried 2026-04-11 via `https://crates.io/api/v1/crates/<name>`) - verified current versions and publish dates for every recommended crate. **HIGH confidence.**
  - `flume` 0.12.0, published 2025-12-08
  - `crossbeam-channel` 0.5.15, published 2025-04-08
  - `crossbeam-utils` 0.8.21, published 2024-12-15
  - `dashmap` 6.1.0, published 2025-03-05
  - `scc` 3.6.12, published 2026-03-24
  - `core_affinity` 0.8.3, published 2025-03-01
  - `parking_lot` 0.12.5, published 2025-10-03
  - `rayon` 1.11.0, published 2025-08-12
  - `tokio` 1.51.1, published 2026-04-08
  - `num_cpus` 1.17.0, published 2025-05-30
  - `sharded-slab` 0.1.7, published 2023-10-04 (stale - not recommended)
  - `hwloc` 0.5.0, published 2017-04-26 (abandoned - do not use)
  - `arc-swap` 1.9.1, published 2026-04-04
- **RUSTSEC advisory database** (https://rustsec.org/advisories/) - cross-checked every recommendation. **HIGH confidence.**
  - `RUSTSEC-2025-0024`: crossbeam-channel double-free on Drop, affects 0.5.12-0.5.14, **patched in 0.5.15** (https://rustsec.org/advisories/RUSTSEC-2025-0024.html).
  - No open advisories against `parking_lot`, current `crossbeam-utils` (RUSTSEC-2022-0041 patched years ago), `core_affinity`, current `dashmap`, current `scc`, current `flume`.
- **Tokio docs** (https://docs.rs/tokio/latest/tokio/time/fn.sleep.html) - verified 1ms granularity limitation on `sleep`. **HIGH confidence.**
- **Tokio runtime docs** (docs.rs/tokio/latest/tokio/runtime/struct.Builder.html) - verified `max_blocking_threads`, `worker_threads`, `new_multi_thread`, `new_current_thread` APIs. **HIGH confidence.**
- **Crossbeam upstream PR #1187** (referenced in RUSTSEC-2025-0024) - root cause of the double-free, patched in 0.5.15. **HIGH confidence.**
- **Rust std docs** - `std::thread::available_parallelism()` stabilized in 1.59. **HIGH confidence.**
- **v1.2 benchmark baseline** (`benchmark/tally-throughput/RESULTS.md`) - read for single-core ceiling context: ~128-142k eps @ 66% CPU, 7us per push of real work. **HIGH confidence** (authoritative project artifact).
- **v1.2 code** (`src/state/store.rs`, `src/server/tcp.rs`) - verified current single-threaded `Arc<Mutex<AppState>>` pattern that Phase 14 must replace. **HIGH confidence.**

**MEDIUM confidence items (flagged):**
- Per-event ns cost estimates in the Hot-Path Cost Budget table are rough ranges from public microbenchmarks, not measured on Tally's hardware. Phase 14 benchmarks must validate.
- `crossbeam-channel` vs `flume` uncontended throughput ratio (1.5-2x) is from matklad/kprotty benchmarks from 2023; may have narrowed. Either choice would work functionally.

---

*Stack research for: Tally v1.3 Concurrency & Client Batching*
*Researched: 2026-04-11*
