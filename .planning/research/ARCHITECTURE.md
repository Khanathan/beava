# Architecture Research: v1.3 Concurrency & Client Batching

**Domain:** Integration of key-partitioned multi-threading, async coalescing, client batch API, and off-main-thread snapshots with Tally's existing single-threaded tokio + binary TCP + postcard snapshot architecture.
**Researched:** 2026-04-11
**Confidence:** HIGH for existing-shape analysis (code read directly); MEDIUM-HIGH for integration design (principles well-established, specific Tally-flavored tradeoffs rely on judgment).

---

## Executive Summary

v1.2's architecture is a **tightly-coupled Big Mutex** (`Arc<Mutex<AppState>>`) with a single `current_thread` tokio runtime. Every hot-path handler (`handle_push_core_ex`, `handle_mget`, snapshot tick, eviction, fsync, compaction) acquires the same lock. That is the thing v1.3 must carefully unwind.

The four v1.3 capabilities decompose naturally along the push path, but they are **not equally risky**:

| Phase | Shape | Risk | Unlocks |
|---|---|---|---|
| 12 Coalescing | Additive to `handle_connection` read loop + new `handle_push_batch` sharing `handle_push_core_ex` | Low | Amortizes lock acquisition; prerequisite for 13 & 14 lock amortization |
| 13 `push_many` / OP_PUSH_BATCH | Wire-format + Python client change; server reuses Phase 12 batch handler | Very low | Single-client throughput |
| 14 Key-partitioned sharding | Full `AppState` refactor, runtime model change, cross-shard channels, snapshot format bump to v7 | **High** | Multi-core scaling — the actual milestone thesis |
| 15 Off-thread snapshot | Extract snapshot writer into its own task that drains per-shard dirty sets | Medium | Eliminates 15-25% duty-cycle loss |

**Recommended build order: 12 → 13 → 14 → 15** (the drafted order).

---

## 1. Existing Hot-Path Shape (v1.2 baseline)

### 1.1 The Big Mutex

`SharedState = Arc<Mutex<AppState>>`. `AppState` (src/server/tcp.rs:49-88) contains fields that do not belong together in v1.3:

- **Hot-path, per-key-sharded (→ v1.3 per-shard):** `engine` reads, `store`, per-stream `last_event_at`, dirty tracking
- **Hot-path, global (→ v1.3 shared):** `engine` pipeline DAG (read-only after register), `metrics.events_total`, `latency` histograms, `throughput` EWMA
- **Cold-path, periodic:** `snapshot_*`, `backfill_tracker`, `backfill_complete`

### 1.2 Push path under lock (measured cost from 11-VERIFICATION.md)

For a single `OP_PUSH_ASYNC` (the hot path), `handle_push_core_ex` does **all** of this under one lock acquisition:

1. `engine.push_with_cascade_no_features` → primary stream operator updates
2. BFS over `downstream_map` → cascade pushes (same lock)
3. `store.mark_dirty(primary_key)`
4. `event_log.append(primary)` — in-memory buffered write
5. `event_log.append(cascade_target)` × N
6. `store.mark_dirty(cascade_key)` × N
7. Fan-out loop: for each `engine.fan_out_targets()` whose `key_field` is in the payload, call `engine.push_no_features(target, ...)`, mark dirty, append log
8. Throughput tracker `bump_unique` across all touched streams
9. `metrics.push_latency_seconds` + `events_total`
10. `latency.record_push` + optional slow-query capture

At 142k eps on medium (post-11), that is ~7µs under lock per event. **Every fixed cost (lock acquire, `event_log.append` inner BufWriter write, dirty set insert) pays once per event.** That is the Phase 12 amortization target.

### 1.3 Why single-thread wins on single-client and loses on concurrent

`#[tokio::main(flavor = "current_thread")]` in main.rs:27 means every task (TCP accept, per-connection loop, snapshot timer, eviction, fsync, compaction, HTTP handler) cooperatively shares one OS thread. Contended `std::sync::Mutex` on current-thread tokio is cheap, but **every `.await` inside a locked section would be a deadlock waiting to happen**. The code is correct: it never `.await`s while holding the lock. That invariant is load-bearing and must survive v1.3.

---

## 2. Phase 12 — Server-side Async Coalescing

### 2.1 Where the per-connection accumulator lives

**Proposal:** a local `ConnAccumulator` struct **stack-allocated inside `handle_connection`** (not on `AppState`, not heap-allocated per frame).

```rust
struct ConnAccumulator {
    // Grouped by primary stream name.
    by_stream: Vec<(String, Vec<PendingEvent>)>,
    oldest_at: Option<Instant>,
    total_frames: usize,
}
struct PendingEvent {
    payload: serde_json::Value, // decoded once by parse_command
    raw_payload: Vec<u8>,       // for the binary event-log path
    ts: SystemTime,
}
```

**Why connection-local, not `AppState`-local:** avoids ever locking `AppState` for a coalescing primitive; keeps the accumulator thread-local by construction.

### 2.2 Flush trigger hierarchy

Four triggers, all additive; any one of them drains the accumulator via one `handle_push_batch` call:

| Priority | Trigger | Default | Rationale |
|---|---|---|---|
| 1 (forced) | Any non-PushAsync command arrives (GET, SET, MGET, PUSH sync, FLUSH) | — | Guarantees ordering — sync commands cannot observe state that hasn't committed the async tail |
| 2 (forced) | Connection close | — | No lost writes |
| 3 (size) | `total_frames >= N` | 64 | Bound tail latency on steady load |
| 4 (time) | `now - oldest_at >= T µs` | 200 | Bound tail latency on trickle load |

**Flush-on-read-exhaustion opportunistic trigger:** after each `reader.read_u32().await`, if the socket read *would have blocked*, the accumulator has already taken its natural batch. **This is the highest-leverage trigger.**

```rust
loop {
    tokio::select! {
        biased;
        result = reader.read_u32() => {
            // parse frame
            match cmd {
                Command::PushAsync { .. } => {
                    accum.add(..);
                    if accum.total_frames >= BATCH_N { flush_batch(&mut accum, &state).await?; }
                }
                other => {
                    if !accum.is_empty() { flush_batch(&mut accum, &state).await?; }
                    dispatch(other, ...).await?;
                }
            }
        }
        _ = tokio::time::sleep_until(accum.deadline(BATCH_T)), if !accum.is_empty() => {
            flush_batch(&mut accum, &state).await?;
        }
    }
}
```

### 2.3 `handle_push_batch` — new batched handler, reuses `handle_push_core_ex`

Inside the batch handler, acquire the lock **once**, then for each `(stream, events)` group:

1. Look up the stream's `key_field`, cascade targets, fan-out targets **once** (currently looked up per event)
2. For each event, run the operator updates
3. `event_log.append` can batch-write the whole group via a new `append_many(stream, &[bytes])`
4. `mark_dirty` can take a batch (`mark_dirty_many(&[&str])`)
5. Fan-out loop runs per event but reuses the same `fan_out_targets` Vec once

### 2.4 Error attribution — this is the subtle part

**Current contract (post-11):** errors from PushAsync are written as STATUS_ERROR frames immediately by `handle_connection`. There is **no server-side error queue** — "drain semantic" is purely client-side: the Python client reads STATUS_ERROR frames off the socket before its next public call.

**What Phase 12 must preserve:** if event #37 in a batch of 64 fails, the client must see (a) an error for event 37 specifically, and (b) events 38-63 must still be processed.

**Proposed contract:**

1. `handle_push_batch` returns `Vec<Result<(), TallyError>>` — one entry per input event in input order
2. The read loop walks the result vector; for each `Err`, it writes **one STATUS_ERROR frame** immediately, with a payload that includes a batch index ("batch event #37: unknown stream 'X'")
3. Successful events write nothing

---

## 3. Phase 13 — `push_many` + OP_PUSH_BATCH (0x0A)

### 3.1 Wire format

```
Frame layout (after the generic [u32 len][u8 opcode] envelope):

OP_PUSH_BATCH (0x0A)
[u16 stream_name_len][stream_name_bytes]
[u32 count]
repeated count times:
    [u32 event_len]
    [event_bytes]  // same format as OP_PUSH_ASYNC payload
```

**Why `[u32 event_len]` prefix per event:** lets the server decode event i without having to peek at field-count + walk forward. Matches the outer frame-length convention.

**All events in one OP_PUSH_BATCH frame must target the same stream.** This matches `push_many(Transactions, events)` Python API; lets server skip per-event stream lookup; clients that need multi-stream batches concatenate multiple OP_PUSH_BATCH frames.

### 3.2 Python SDK integration

`push_many` is a **separate code path** from `push`:

- Takes `stream_cls` + iterable of event dicts
- Calls `encode_push_binary_payload` once per event (the same function v1.2 `push` calls)
- Prepends `[u16 stream_len][stream][u32 count]`, then for each event `[u32 event_len][event_bytes]`
- Wraps in one `[u32 frame_len][u8 OP_PUSH_BATCH]` envelope
- Sends via `send_frame_no_recv` (same as `push`)
- Error drain via existing `drain_errors_nonblock` — unchanged

### 3.3 Backward compat

`OP_PUSH_ASYNC` (0x07) and `OP_PUSH_BATCH` (0x0A) coexist indefinitely. Keep `OP_PUSH_ASYNC` forever as the single-event fast path.

---

## 4. Phase 14 — Key-Partitioned Multi-Threaded Engine

This is the big one. Right model for Tally is **thread-per-shard, shared-nothing hot path, message-passing for cross-shard work** (Seastar/ScyllaDB pattern).

### 4.1 Runtime model change

**Current:** one `current_thread` tokio runtime, one OS thread, all tasks cooperatively interleaved.

**Proposed:**
- **N shard workers**, where N = `std::thread::available_parallelism()` or from `TALLY_SHARDS` env var. Each is a dedicated OS thread running its own `current_thread` tokio runtime (Seastar/Glommio pattern), **not** one multi-thread tokio runtime.
- **Why thread-per-shard not multi-thread tokio:** multi-thread tokio lets any task run on any worker, re-introducing cross-core cache invalidation. Thread-per-shard gives Redis-like locality.
- **One "front door" thread** runs the TCP listener, accepts connections, and hands each TCP socket (fd) to a shard worker that handles the connection's entire lifetime.

Cross-shard hops happen via message-passing when the push key doesn't belong to the connection's home shard. Clients that care about hotspot distribution open N connections.

### 4.2 State decomposition

| Field | v1.2 location | v1.3 location | Rationale |
|---|---|---|---|
| `StateStore.entities` | AppState mutex | **per-shard** `Mutex<ShardStore>` | Key-partitioned |
| `StateStore.dirty_keys` | AppState mutex | **per-shard** | Snapshot drains per-shard |
| `StateStore.deleted_keys` | AppState mutex | **per-shard** | Same |
| `PipelineEngine.streams/views/dag/topo_order/downstream_map` | AppState mutex | **global `ArcSwap<PipelineEngine>`** | Read-only after register |
| `event_log` (per-stream `LogWriter`) | AppState mutex | **per-shard** | Each shard writes its own per-stream log files |
| `metrics.events_total` / `push_latency_seconds` | AppState mutex | **per-shard `AtomicU64`**, aggregated on read | No cross-shard contention |
| `throughput: ThroughputTracker` | AppState mutex | **per-shard** | Merged on debug read |
| `latency: LatencyTracker` | AppState mutex | **per-shard** histograms | hdrhistogram merges cheaply |
| `backfill_tracker` | AppState mutex | **global `Mutex`** | Cold path |
| Snapshot coordinator state | AppState mutex | **global `Mutex`** | Cold path |

**Key insight — `PipelineEngine` is effectively read-only after register.** Putting the engine behind `arc_swap::ArcSwap<Arc<PipelineEngine>>` means every shard reads its own pointer with zero synchronization, and REGISTER publishes a new Arc atomically.

### 4.3 Shard routing — hash function choice

**Requirements:** (a) deterministic across process restarts, (b) stable across Rust/crate versions, (c) fast, (d) well-distributed.

**Recommendation: `xxh3_64` with a fixed seed (e.g., 0), `shard = (xxh3_64(key) % N)`.**

| Hash | Stable across versions? | Notes |
|---|---|---|
| `ahash` | **NO** — randomized seed, algorithm may change | **Do not use for shard routing.** |
| `rustc-hash` / `FxHash` | Not explicit | Poor distribution on short keys |
| `xxh3` via `xxhash-rust` | **YES** — spec-stable, version-stable | Very fast, well-tested |
| `seahash` | **YES** — spec-stable | Less ecosystem momentum than xxh3 |

**Do NOT use `N = 2^k` modulo-mask** — `num_cpus` often isn't power-of-two (12, 20, 24, 48 cores). `%` is irrelevant cost compared to per-event work.

### 4.4 Cross-shard fan-out and cascade — the hard part

**Three categories of cross-stream work:**

1. **Same-key cascade** (`Transactions` → `FraudScore`, both keyed by `user_id`): same shard. **No cross-shard work.**
2. **Fan-out to different key** (push to `Transactions` [user_id] also updates `MerchantActivity` [merchant_id]): **cross-shard.**
3. **Cascade to different-key downstream** (`Transactions` [user_id] cascades to `UserRiskAggregate` [user_id]): same key field, same shard. Local.

**So only category 2 needs a cross-shard channel.**

**Proposed cross-shard mechanism:**
- Each shard has a bounded MPSC inbox: `tokio::sync::mpsc::channel::<CrossShardMsg>(4096)`
- `CrossShardMsg::PushBatch { target_stream, events: Vec<...> }` — always batched, never one-at-a-time
- Home shard releases its lock first, then enqueues
- Target shard's worker loop is a `select!` over (a) its TCP connections' frames and (b) its cross-shard inbox

**Error handling:** cross-shard work is **fire-and-forget**. Origin shard does not `await` target shard's result. Errors logged to target shard's metrics but **not** propagated back to originating client. This is a **deliberate semantic change from v1.2** — surface in PITFALLS as a **locked decision** that needs user-facing documentation.

**Ordering guarantees across shards:** none. Events A and B that fan out to different shards may be reordered. Acceptable because operators are per-entity and fan-out targets are per-entity too.

### 4.5 Event log per-shard or global?

**Recommendation: per-shard event log directory** (`events/shard-0/`, `events/shard-1/`, ...).

- Each shard's log writer is a separate `BufWriter<File>` — no contention
- fsync per-shard on its own 1-second timer
- Per-stream log files become per-(shard, stream) log files: `events/shard-3/transactions.log`
- **Backfill complication:** replay reads all shards' copies of that stream's log and interleaves. Acceptable cold-path cost.

### 4.6 Snapshot format bump to v7, per-shard files, manifest

```
tally.snapshot.manifest.0000000005        # {seq: 5, num_shards: 12, shards: [0..11], format: 7}
tally.snapshot.base.0000000005.shard-00
tally.snapshot.base.0000000005.shard-01
...
tally.snapshot.delta.0000000006.shard-00
tally.snapshot.delta.0000000006.shard-03  # only shards with dirty keys write deltas
```

**Manifest commit protocol:**
1. Coordinator signals each shard "prepare snapshot at seq N"
2. Each shard drains its dirty set under its own lock, releases, serializes off-thread, writes `.shard-K.tmp` → fsync → rename
3. Coordinator waits for all shards to report done
4. Coordinator writes `manifest.N.tmp` → fsync → rename → fsync parent dir
5. Cleanup: delete files whose seq < previous manifest's seq

**Crash recovery with a partial snapshot set:** if a manifest file for seq N does not exist but shard files for N do, N is incomplete. **Roll back to the previous manifest**. Same atomicity model as PostgreSQL commit files.

**Backward compat with v1.2 single-file snapshots:**
- On startup, check for `manifest.*` first (v7)
- Fall back to `tally.snapshot.base.*` scan (v6) — current code, keep it
- Load v6 into shard 0, re-shard by key on the fly: iterate `store.entities`, compute `xxh3(key) % N`, route each entity into the correct shard

**Backward compat when `N` changes across restarts:** persist `num_shards` in a **config file** written on first run; user must explicitly change it (and accept the re-shard migration).

### 4.7 REGISTER replication to shards

`PipelineEngine` behind `arc_swap::ArcSwap<Arc<PipelineEngine>>`. REGISTER acquires a **separate** `Mutex<()>` (one at a time), builds a new `PipelineEngine`, publishes via `arcswap.store(Arc::new(new_engine))`. Every shard reads via `arcswap.load()` which is essentially free.

Strictly better than the alternatives:
- **Broadcast channel to all shards**: risk of partial application if a shard is stuck. Reject.
- **Shared RwLock**: non-zero cost on every hot-path read. Reject.

**Subtlety:** pipeline registration also touches `event_log.register_stream` — in v1.3 this must happen on every shard worker. Coordinator sends a `CoordMsg::RegisterStream { name, history_ttl }` to every shard's inbox after swapping the engine. Allowed to be slightly asynchronous: PUSH races with REGISTER can see an engine with the stream but log not yet open; log writer handles lazy-open.

### 4.8 MGET + GET across shards

MGET is already a batch op. In v1.3:
- Route each key by hash to its shard
- Scatter: for each involved shard, build a sub-MGET and send via oneshot-reply channel (`CrossShardMsg::MGet { keys, reply: oneshot::Sender }`)
- Gather: the connection handler awaits all replies, merges in input order

GET is just scatter-1 / gather-1.

**Latency impact:** GET gains a channel round-trip (~1-2µs). Still well within `<50µs p99`.

### 4.9 Debug UI / HTTP scatter-gather

| Endpoint | Strategy |
|---|---|
| `/debug/topology` | Reads `ArcSwap<PipelineEngine>`. Zero change. |
| `/debug/state/:key` | Hash key to shard, one-shot message. |
| `/debug/memory` | Scatter to all shards, sum results. |
| `/debug/throughput` | Scatter, merge per-stream EWMAs. |
| `/debug/latency` | Scatter, merge histograms (hdrhistogram.merge). |
| `/debug/backfill` | Coordinator-owned state, direct read. |
| `/metrics` (Prometheus) | Scatter, sum. |

---

## 5. Phase 15 — Snapshot I/O Off Main Thread

### 5.1 Today's state

`main.rs:222-413` already uses `spawn_blocking` for the serialize + write part. The **clone-under-lock** is what blocks the main thread. Phase 9 incremental snapshots reduced this: on delta cycles, only dirty entities are cloned. Base snapshots still clone the entire store under the Big Mutex.

### 5.2 Phase 15 after Phase 14 is the right order

With Phase 14's per-shard state, Phase 15 becomes trivial: **each shard does its own clone under its own lock**, in parallel with the other shards. Per-shard stall is ~1/N of today's stall; PUSH throughput on other shards unaffected.

```
Snapshot coordinator
   │
   └──► CoordMsg::PrepareSnapshot { seq, full: bool }  ──► shard 0..N workers
        │
        │ Each shard, on receiving the message:
        │   1. Acquire its own lock
        │   2. Clone dirty (or full)
        │   3. Release lock
        │   4. spawn_blocking per-shard writer task
        │   5. On success, reply via oneshot
        │
        ▼
   Coordinator waits for all replies, writes manifest.N, cleans up
```

### 5.3 If Phase 15 landed before Phase 14

Also useful as a standalone improvement to v1.2, but most of the machinery gets rewritten in Phase 14 anyway. Recommendation: ship after 14.

### 5.4 Partial-write recovery

Manifest file is the transaction boundary. Missing manifest → roll back to previous manifest. Orphan shard files are detected in cleanup pass.

Manifest must be fsync'd to disk, directory fsync'd after rename, before coordinator reports success.

---

## 6. Build Order Recommendation

**Recommended order: 12 → 13 → 14 → 15 (the drafted order).**

### 6.1 Rationale

**Why 12 before 13:** Phase 13 (OP_PUSH_BATCH) is a wire-format + SDK change whose server-side handler **is** the Phase 12 batch handler. Building 13 first means writing `handle_push_batch` twice. 12 first establishes the shared primitive.

**Why 12+13 before 14:** (a) Phase 14 is the largest architectural change; going in with a well-tested `handle_push_batch` primitive that shard workers can reuse means Phase 14's cross-shard channel can send `PushBatch` messages from day one. (b) 12+13 are **independently shippable wins** that validate the milestone thesis before taking on the 2-3 week Phase 14 refactor. De-risks.

**Why 14 before 15:** Phase 15 is dramatically simpler in a sharded world (§5.2). Doing 15 first means reworking it after 14.

**Why NOT 14 first:** max risk upfront, no early wins, no shared primitives to reuse.

**Why NOT 15 before 14:** 15% duty-cycle stall is real but not blocking. 1M eps goal depends on 14, not 15.

### 6.2 Alternatives — brief verdicts

| Alt | Verdict | Reasoning |
|---|---|---|
| 12 → 13 → 15 → 14 | **No.** | Phase 15 gets redone after 14 |
| 14 → 12 → 13 → 15 | **No.** | Max risk front-loaded, no de-risking |
| 12+13 parallel → 14 → 15 | **Maybe — if two engineers.** | Share `handle_push_batch`; solo dev should stay sequential |
| **12 → 13 → 14 → 15** | **YES (drafted order)** | Shared primitives flow forward |

---

## 7. New Components Introduced by v1.3

| Component | Phase | File (proposed) | Responsibility |
|---|---|---|---|
| `ConnAccumulator` | 12 | `src/server/accumulator.rs` | Per-connection buffer + flush triggers |
| `handle_push_batch` | 12 | `src/server/tcp.rs` | Batched push under a single lock |
| `engine.push_batch_no_features` | 12 | `src/engine/pipeline.rs` | Iterate events for a single stream, share lookups |
| `event_log.append_many` | 12 | `src/state/event_log.rs` | Batch-append to one stream's log |
| `store.mark_dirty_many` | 12 | `src/state/store.rs` | Batch dirty-set insert |
| `OP_PUSH_BATCH (0x0A)` decoder | 13 | `src/server/protocol.rs` | Wire format decoder |
| Python `App.push_many` | 13 | `python/tally/_app.py` | Client-side batching API |
| `ShardStore` / `ShardWorker` | 14 | `src/shard/mod.rs` (new) | Owns one shard; runs its own current_thread runtime |
| `Coordinator` | 14 | `src/shard/coordinator.rs` | Accepts TCP, assigns connections, REGISTER ArcSwap |
| `CrossShardMsg` | 14 | `src/shard/message.rs` | Enum: PushBatch, MGet, DebugQuery, RegisterStream, PrepareSnapshot |
| `ShardRouter` / `key_to_shard` | 14 | `src/shard/routing.rs` | `xxh3_64(key) % num_shards` |
| `ArcSwap<PipelineEngine>` wrap | 14 | `src/engine/pipeline.rs` | Globally shared immutable snapshot |
| `SnapshotManifest` + v7 format | 14 + 15 | `src/state/snapshot.rs` | New `.manifest.NNN` file |
| `shard.clone_dirty_for_snapshot` | 14 | `src/shard/store.rs` | Per-shard version of existing clone |
| `SnapshotCoordinator` | 15 | `src/state/snapshot_coord.rs` | Orchestrates per-shard snapshot cycles |

---

## 8. Architectural Risks (feed pitfalls researcher)

1. **Cross-shard fan-out error swallowing** (§4.4). v1.2 surfaces fan-out errors on the originating PUSH. v1.3 cannot without re-introducing sync coordination. **Locked decision risk.** Mitigation: per-shard per-stream metrics; alert on cross-shard push error rate.
2. **Deterministic hash compatibility** (§4.3). Pin the `xxhash-rust` crate version; include hash version in manifest header.
3. **`num_shards` drift across restarts** (§4.6). Persist `num_shards` in manifest + config file; require explicit env var bump to trigger re-shard.
4. **REGISTER vs. PUSH race under ArcSwap** (§4.7). Benign but subtle. Unit test.
5. **Coalescing error ordering under multi-client** (§2.4). Per-connection accumulators are independent. OK if not shared across connections.
6. **Bounded cross-shard channel backpressure** (§4.4). Hot shard falling behind creates backpressure on sender. Mitigation: channel size tuning, shed to metrics + drop, always batch.
7. **Manifest fsync ordering** (§5.4). Strict: all shard writes → all shard fsyncs → parent dir fsync → manifest.tmp → fsync → rename → dir fsync.
8. **Snapshot clone vs. active writes on same shard** (§5.2). Even per-shard, clone runs under own lock. Massive state in one shard (hot key) still stalls **itself**. Mitigation: rely on even distribution; Arc<EntityState> COW as v2 fallback.
9. **Event log backfill across shards** (§4.5). Scatter-read + merge helper. Verify no global-ordering assumption in Phase 7 cascade backfill.
10. **Thread-per-core cooperative scheduling** (§4.1). Long-running sync work on a shard starves its own tasks including cross-shard inbox. Mitigation: hard budget; `yield_now()` in MGET chunked loop.

---

## 9. Data Flow — Before and After

### 9.1 PUSH path (v1.2 today)

```
TCP read  →  parse_command  →  acquire AppState mutex  →
    engine.push_with_cascade_no_features  →
    mark_dirty + event_log.append + fan-out  →
    throughput.bump_unique + metrics + latency  →
release mutex  →  (async: no response; sync: write frame)
```

### 9.2 PUSH path (v1.3 after Phase 12 — coalescing only, still single-threaded)

```
TCP read  →  parse_command  →
  PushAsync? ↓ yes               ↓ no (sync command)
  accumulator.add(event)         flush accumulator → acquire mutex → dispatch
  accumulator full or timer? ↓ yes
  flush = acquire mutex ONCE
    for each group (stream, events):
      engine.push_batch_no_features(stream, events)
      event_log.append_many(stream, bytes_slice)
      store.mark_dirty_many(&keys)
      handle cross-stream fan-out
    throughput.bump_unique(touched)  [once per batch]
    metrics.events_total += N
  release mutex
  (write STATUS_ERROR frames for failures, if any)
```

### 9.3 PUSH path (v1.3 after Phase 14 — sharded)

```
Coordinator thread:  accept(TCP) → pick home shard S → handoff socket FD
Shard S worker (own runtime, own mutex):
  read frame → parse →
  PushAsync? ↓ yes
  accumulator.add → flush (size/time) →
    for each (stream, events):
      for event:
        primary_key = extract(event, stream.key_field)
        if hash(primary_key) % N == S:
          shard[S].push_local(event)
          for cascade_target in same_shard_targets:
            shard[S].push_local(cascade_target)
          for fan_out_target with key on other shard T:
            cross_shard_outbox[T].push(event)  [accumulates]
    for target_shard T with non-empty outbox:
      shard[T].inbox.send(CrossShardMsg::PushBatch { stream, events })

Shard T worker (its own thread):
  select! over (own connections' reads, cross_shard inbox)
    cross_shard_msg → push_batch_local_only(events)
```

### 9.4 Snapshot path

**v1.2:**
```
Periodic timer (main thread)
  → acquire mutex
  → clone_for_snapshot_with_gc (2-7s for 1M keys, under lock)  ← THE STALL
  → clear_dirty, release mutex
  → spawn_blocking(serialize + write + fsync + rename)
```

**v1.3 after Phase 15 + 14:**
```
Coordinator (own thread) → broadcast CoordMsg::PrepareSnapshot to every shard

Each shard (parallel):
  → acquire own mutex
  → clone_dirty for own subset (~1/N of v1.2 stall)  ← much shorter
  → clear_dirty, release own mutex
  → spawn_blocking(serialize own shard + write own file + fsync)
  → reply via oneshot

Coordinator:
  → wait for all replies (bounded timeout; missing shard = abort cycle)
  → write manifest.N.tmp → fsync → rename → fsync parent dir
  → cleanup old files
```

---

## 10. Confidence Assessment

| Area | Confidence | Notes |
|---|---|---|
| Existing architecture shape | HIGH | Read directly from source |
| Phase 12 coalescing design | HIGH | Standard pattern (Redis I/O threads, Netty batching) |
| Phase 13 wire format + SDK | HIGH | Trivial extension of Phase 11 binary encoding |
| Phase 14 sharding runtime model | MEDIUM-HIGH | Seastar/Glommio well-established; Rust tokio specifics need a 1-day spike |
| Phase 14 cross-shard semantics | MEDIUM | Error-swallowing decision affects user-facing semantics — needs explicit CEO review |
| Phase 14 snapshot format v7 | HIGH | Manifest pattern is standard |
| Phase 15 post-sharding | HIGH | Trivial split of existing `spawn_blocking` |
| Build order | HIGH | Dependency-driven, backed by shared-primitive reuse |

**Confidence is weakest on Phase 14 cross-shard error semantics** because that is a product decision, not just technical. Flag for CEO review before Phase 14 execution.

---

## 11. Files referenced

- `CLAUDE.md` (project charter)
- `.planning/PROJECT.md` (current state, constraints, key decisions)
- `.planning/ROADMAP.md` lines 210-275 (v1.3 drafted phases)
- `src/main.rs` lines 27-500 (runtime construction, snapshot timer)
- `src/server/tcp.rs` lines 49-88 (`AppState`), 124-235 (`handle_connection`), 238-483 (`handle_push_core_ex` and `handle_push_async`)
- `src/state/store.rs` lines 83-200 (`StateStore` → `ShardStore`)
- `src/state/snapshot.rs` lines 20-30 (v6 `SNAPSHOT_FORMAT_VERSION`; v7 manifest adds on top)
- `src/engine/pipeline.rs` lines 580-678 (cascade — same-key stays local, fan-out goes cross-shard)
- `src/engine/pipeline.rs` lines 973-980 (`fan_out_targets`)
- `src/state/event_log.rs` lines 51-92 (EventLog per-stream writers)
- `.planning/phases/11-fire-and-forget-push/11-VERIFICATION.md` lines 120-176 (post-verification bottleneck analysis)
- `benchmark/tally-throughput/PATH-TO-100K-1M.md` lines 96-150 (Lever D/E analysis)
