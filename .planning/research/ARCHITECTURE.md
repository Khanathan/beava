# Architecture Integration Map: TPC + Full Key-Shard (v1.2)

**Scope:** Integration points and wave boundaries for `.planning/arch/TPC-SHARD-DESIGN.md`
**Researched:** 2026-04-18
**Branch:** `arch/tpc-full-shard`
**NOT included:** runtime choice rationale (TPC-RESEARCH.md), Python/CLI features (FEATURES.md), crate versions (STACK.md)

---

## 1. Module-Level Impact Map

### Wave 0 — shard_hint Scaffolding

**New files/modules:**
- `src/routing/mod.rs` — top-level routing module; re-exports `ShardRouter`, `shard_hint_for_key`
- `src/routing/shard_hint.rs` — `shard_hint(key: &str, n_shards: usize) -> u32` via `ahash`; includes the "always returns 0 when n_shards == 1" fast-path
- `src/routing/config.rs` — reads `BEAVA_SHARDS` env var at startup; `debug_assertions` defaults to 1, release defaults to `num_cpus::get_physical()`
- `benches/wave0_shard_hint.rs` — micro-bench: hash overhead per-event (<100 ns gate) and SPSC channel roundtrip (<10 μs gate)

**Existing files to modify:**
- `src/engine/pipeline.rs` (line 404 `PipelineEngine` struct) — `EventSource` trait method `shard_hint(&self, event: &Event) -> u32` added; TCP and HTTP push paths call it immediately after parse
- `src/server/tcp.rs` (line 1280 `handle_push_core_ex`) — call `routing::shard_hint_for_key(primary_key, n_shards)` and attach result to event context; at N=1 the result is always 0, no routing change occurs
- `src/server/http_ingest.rs` (line 143 `http_push_single`, line 191 `http_push_batch`) — same `shard_hint` annotation on the ingest path
- `src/server/shard_probe.rs` — extend `record_event` to also record the computed `shard_hint` value; zero-cost when `BEAVA_SHARD_PROBE` is unset (existing hot-path gate at line 80 already guards it)

**Existing files to delete:** None at Wave 0.

**Test files affected:** None at Wave 0 — `shard_hint` always returns 0 for N=1. All existing tests pass unchanged.

---

### Wave 1 — Per-Shard State Store

**New files/modules:**
- `src/shard/mod.rs` — `Shard` struct: owns `AHashMap<EntityKey, EntityState>` (plain, not DashMap), `WatermarkState` (per-shard watermarks), `DirtySet` (plain `HashSet` — single-threaded owner, no arc-swap needed), and an `EventLog` handle pointing to `data/shard-{N}/streams/{name}/log.bin`
- `src/shard/runtime.rs` — `spawn_shard(id: usize, inbox: Receiver<ShardMsg>, core_hint: Option<usize>)` — creates a `tokio::runtime::Builder::new_current_thread().build_local()` runtime on a pinned OS thread; calls `core_affinity::set_for_current()` as best-effort
- `src/shard/message.rs` — `ShardMsg` enum: `Push { stream, key, payload, response_tx }`, `Query { key, response_tx }`, `Shutdown`

**Existing files to modify:**
- `src/state/store.rs` — `StateStore` is not deleted yet; at Wave 1 it becomes a thin compatibility wrapper that delegates to the single `Shard` (shard-0) when `N_SHARDS=1`. The `DashMap<EntityKey, EntityState>` field (`entities`, line 218) and the `ArcSwap<DashSet>` dirty-keys field (`dirty_keys`, line 226) are preserved inside `StateStore` for N=1 compatibility but are no longer the primary data path
- `src/state/event_log.rs` — `EventLog::new(log_dir: PathBuf)` (line 218) — `log_dir` now receives `data/shard-0/streams` for N=1; constructor unchanged; all callers in `main.rs` pass the shard-scoped path
- `src/main.rs` (line 612) — change `EventLog::new(event_log_dir)` path from `$BEAVA_DATA_DIR/events` to `$BEAVA_DATA_DIR/shard-0/streams`; snapshot_path similarly moves to `$BEAVA_DATA_DIR/shard-0/beava.snapshot`
- `src/server/tcp.rs` `ConcurrentAppState` (line 89) — add `shard_router: Arc<ShardRouter>` field; at N=1 ShardRouter delegates all ops to the in-process shard without queuing

**Existing files to delete:** None at Wave 1 — DashMap infrastructure stays live for compatibility verification.

**Test files affected:**
- `tests/test_concurrent.rs` — `make_concurrent_state` (line 38) must be updated to construct `ShardRouter` with N=1; test continues to pass because N=1 is byte-identical to current behavior
- `tests/test_snapshot.rs`, `tests/test_incremental_snapshot.rs` — snapshot path assertions will break if they hardcode `beava.snapshot` at the working directory root; must be updated to use the shard-scoped path
- `tests/test_snapshot_rollover_race.rs` — directly tests the `ArcSwap<DashSet>` swap (`take_dirty_and_advance_gen`); will need updating when dirty-set moves into `Shard` at Wave 4

---

### Wave 2 — Multi-Shard Routing

**New files/modules:**
- `src/routing/dispatcher.rs` — `ShardDispatcher`: on Linux binds N sockets via `SO_REUSEPORT`; on macOS falls back to single-listener + fan-out; routes `ShardMsg` to the shard-local SPSC `crossbeam_channel::bounded()` inbox
- `src/routing/spsc.rs` — one `(Sender<ShardMsg>, Receiver<ShardMsg>)` pair per shard, created at startup; `Sender` clones live in the dispatcher, `Receiver` lives exclusively on the shard thread

**Existing files to modify:**
- `src/server/tcp.rs` — `run_tcp_server` (around line 2000+) — split into "listener accepts connection" (tokio multi-thread) and "shard handles push" (shard's current_thread runtime); listener calls `dispatcher.route(shard_hint, msg)` rather than calling `handle_push_core_ex` directly
- `src/server/http_ingest.rs` — `http_push_single` (line 143) and `http_push_batch` (line 191) — compute `shard_hint`, send `ShardMsg::Push` through dispatcher, await response channel; `handle_push_core_ex` call (line 171) becomes a shard-local call that never crosses threads
- `src/shard/runtime.rs` — add `core_affinity::set_for_current()` call at shard-thread start; log warn-once on macOS if pinning returns false

**Existing files to delete:** None at Wave 2.

**Test files affected:**
- `tests/test_concurrent.rs` — concurrent push tests assume a shared `StateStore`; after Wave 2, state is per-shard. Tests with multi-key pushes that land on different shards will no longer share state between events in the same test; must be re-scaffolded to pin to one key (always shard-0 in a test with N=1) or use scatter-gather query to verify
- `tests/ship_gate.rs` — 9-cell matrix must re-run with `N=CPU_COUNT`; gate: ≥3× baseline on `complex-c8-x8`
- `tests/bench_concurrent_maps.rs` — DashMap contention bench becomes obsolete; can be archived or repurposed as a before/after comparison fixture

---

### Wave 3 — Cross-Shard Queries + Joins

**New files/modules:**
- `src/routing/scatter.rs` — `scatter_gather<Req, Resp>(shards, req, merge_fn)` — fans out a request to all N shards, awaits all responses, merges; used by `GET /streams` and global-watermark reads
- `src/engine/join_validator.rs` — validates that streams in a join declare the same `shard_key`; called from `PipelineEngine::register` at registration time

**Existing files to modify:**
- `src/server/http_ingest.rs` (line 77 `http_list_streams`, line 79 `http_get_stream`) — replace direct `engine.list_streams()` call with `scatter_gather` across all shards; each shard returns its local stream list; dispatcher merges
- `src/engine/event_time.rs` `WatermarkTracker` (line 210) — add `publish_to_global(atomic_slot: &AtomicU64)` method called per-shard every N events; global watermark is `min(per_shard_atomics)` read by any thread; the `DashMap<String, AtomicU64>` (line 215) on the shared `PipelineEngine` is retired when the last shared-state consumer is removed at Wave 3 end
- `src/engine/pipeline.rs` `PipelineEngine::register` — call `join_validator::validate_shard_keys` before inserting the stream; return `BeavaError::Protocol` if streams in a join declare incompatible shard keys

**Existing files to delete:** None at Wave 3 — old watermark DashMap is kept as a read-forwarding facade until all consumers migrate.

**Test files affected:**
- `tests/test_watermarks.rs`, `tests/test_watermarks_per_stream_lateness.rs` — both test `WatermarkTracker` directly; must be updated to verify per-shard atomic publishing, not just the shared DashMap fetch_max
- `tests/test_join_stream_stream.rs`, `tests/test_join_stream_table.rs` — join tests that omit `shard_key` will need to declare it explicitly or be updated to verify the new registration-time error
- `tests/test_fork_watermark_propagation.rs` — tests global watermark propagation through fork/replica; must be updated to verify lazy-publish semantics across shards

---

### Wave 4 — Per-Shard Event Log + Recovery

**New files/modules:**
- `src/shard/recovery.rs` — `recover_shard(id: usize, data_dir: &Path) -> Result<Shard>` — reads `data/shard-{id}/streams/{name}/log.bin`, replays events, reconstructs `AHashMap` state; one thread per shard, called in parallel from startup
- `src/tools/reshard.rs` (binary or subcommand) — reads all `data/shard-0/streams/{name}/log.bin` (N=1 layout), replays events through `shard_hint(key, new_N)`, writes to `data/shard-{i}/streams/{name}/log.bin`; used for N=1→N=K migrations

**Existing files to modify:**
- `src/state/event_log.rs` `EventLog` struct — `log_dir` (line 206) now always receives `data/shard-{id}/streams`; no structural change to `EventLog`; file name becomes `log.bin` instead of `{stream}.log`; callers provide the shard-scoped path
- `src/state/snapshot.rs` — snapshot format v8: prepend `shard_count: u16` after `SNAPSHOT_FORMAT_VERSION` (line 34); legacy v7 snapshots (missing `shard_count`) treated as N=1 on read; `SNAPSHOT_FORMAT_VERSION` bumped to 8
- `src/main.rs` — startup recovery: launch N recovery threads in parallel, join all, assemble `Vec<Shard>`; replaces the sequential `load_incremental_snapshots` call (around line 655)

**Existing files to delete:**
- `src/state/store.rs` `StateStore.entities` (`DashMap<EntityKey, EntityState>`, line 218) — after Wave 4, the per-shard `Shard.state: AHashMap` is the authoritative store; `StateStore` is either deleted or retained as a zero-size compatibility shim forwarding to shard-0
- `src/state/store.rs` `StateStore.dirty_keys` (`ArcSwap<DashSet>`, line 226) — replaced by per-shard `DirtySet: HashSet`; the `arc_swap` crate dep can be removed from `Cargo.toml` if no other site uses it after this wave

**Test files affected:**
- `tests/test_snapshot_rollover_race.rs` — tests the `ArcSwap<DashSet>` swap directly; becomes inapplicable once dirty-set moves per-shard; must be rewritten to test per-shard dirty-set behavior
- `tests/test_incremental_snapshot.rs` — snapshot path assertions and format-version checks; must be updated for v8 format and shard-scoped paths
- `tests/test_replica_log_fetch.rs`, `tests/test_replica_batch.rs` — fork/replica tests that assume a single central event log; must be updated to read from the shard-scoped log path and verify that the replica re-hashes on ingest
- `tests/bench_log_fetch_upstream.rs` — bench path likely hardcodes the old `events/` log directory; must be updated

---

## 2. Data-Flow Diagram: Current vs Target Per Wave

### Current (main, v1.0)

```
TCP client                     HTTP client
    |                              |
    | raw bytes                    | HTTP POST /push/{stream}
    v                              v
TcpListener (tokio multi-thread)  axum handler (tokio multi-thread)
    |                              |
    | parse frame (protocol.rs)    | serde_json::from_slice
    +----------+--------------------+
               |
               v
        handle_push_core_ex  (tcp.rs:1280)
               |
               | state.engine.read()        [RwLock — many readers]
               | &state.store               [DashMap — no outer lock]
               v
        PipelineEngine::push_with_cascade
               |
               | per-entity DashMap lock (one shard per key hash)
               v
        EntityState mutation (operators, watermark observe)
               |
               | WatermarkTracker::observe  [DashMap<str,AtomicU64> — lock-free]
               | store.mark_dirty(key)      [ArcSwap<DashSet> load + insert]
               v
        EventLog::append_raw (event_log.rs) [O_APPEND fd — kernel atomic]
               |
               | data/events/{stream}.log   [one file per stream, flat dir]
               v
        Response to caller
```

Contention points at scale: RwLock on engine (write on REGISTER), DashMap shard locks on hot keys, ArcSwap arc-refcount on every dirty-mark, per-stream log fd shared by all threads.

---

### Wave 0 (shard_hint annotated, N=1, no routing change)

```
TCP/HTTP parse
    |
    | shard_hint(primary_key, N=1) → always 0   [NEW annotation, <100 ns]
    v
handle_push_core_ex  (unchanged behavior)
    v
[identical to current]
```

No structural change. `shard_hint` is a pure annotation that flows through but causes no branching at N=1.

---

### Wave 1 (Shard struct, N=1 still single-writer, compatibility preserved)

```
TCP/HTTP parse
    |
    | shard_hint(key, N=1) → 0
    v
ShardRouter.route(hint=0)          [NEW: thin wrapper at N=1]
    |
    v
Shard-0  (same OS thread, same tokio runtime as today)
    |
    | Shard.state: AHashMap<EntityKey, EntityState>   [replaces DashMap]
    | Shard.event_log → data/shard-0/streams/{name}/log.bin  [path changed]
    | Shard.watermark: WatermarkState (AHashMap, no DashMap)  [per-shard]
    | Shard.dirty_set: HashSet<EntityKey>  [plain, no arc-swap]
    v
Response to caller
```

At N=1 the shard runs on the same thread as the listener; no SPSC queue, no latency hop.

---

### Wave 2 (multi-shard, SO_REUSEPORT on Linux, SPSC channels)

```
TCP listener-0   TCP listener-1 ...  HTTP listeners
 (shard-0's        (shard-1's          (tokio pool,
  accept loop)      accept loop)        parses + routes)
    |                  |                   |
    | SO_REUSEPORT (Linux) / single-listener fallback (macOS)
    |
    | shard_hint(key, N) → shard_id  [computed post-parse, pre-send]
    |
    v
Dispatcher
    |
    +---- crossbeam_channel::bounded SPSC inbox per shard ----+
    |                                                         |
    v                                                         v
Shard-0 thread                                           Shard-N thread
 (pinned, current_thread tokio)
    |
    | AHashMap state, per-shard log, per-shard watermark
    |
    | response_tx oneshot channel back to listener
    v
Listener writes response to socket
```

**Where `shard_hint` is computed:** inside the dispatcher, after frame parse, before the channel send. For TCP: computed from the primary key field extracted from the already-parsed frame. For HTTP: computed in `http_push_single`/`http_push_batch` immediately after `serde_json::from_slice`. Both paths know `stream_name` at that point and look up the stream's `key_field` from a read-only snapshot of `PipelineEngine` stream definitions (no shard lock needed for the routing decision; stream definitions change only on REGISTER).

**Where the SPSC queue lives:** one `(Sender<ShardMsg>, Receiver<ShardMsg>)` pair per shard, created at startup. Multiple listener threads / axum tasks hold a clone of the `Sender` side (crossbeam `Sender` is `Clone`). The `Receiver` is owned exclusively by the shard thread — never shared, never cloned. This is logically SPSC (one consumer) but MPSC (multiple producers) in crossbeam terms; crossbeam's bounded channel is correct for this pattern.

---

### Wave 3 (cross-shard reads, scatter-gather)

```
GET /features/{key}
    |
    | shard_hint(key, N) → shard_id  (point read — zero scatter)
    v
Shard-shard_id.query(key)
    v
Response

GET /streams  (listing — rare)
    |
    | scatter_gather(all N shards, ListStreams, merge_fn)
    |--- ShardMsg::ListStreams → Shard-0 → streams_0
    |--- ShardMsg::ListStreams → Shard-1 → streams_1
    |--- ...
    v
Merged stream list

Global watermark (lazy):
    Each shard publishes max_event_time per stream to global AtomicU64 array
    every N events. Global watermark = min(array[0..N]) for that stream.
```

---

### Wave 4 (per-shard log, parallel recovery)

```
Startup:
    thread-0: recover_shard(0, data/shard-0) → Shard-0
    thread-1: recover_shard(1, data/shard-1) → Shard-1
    ...
    join_all() → Vec<Shard>   (recovery time ÷ N_SHARDS)

On-disk layout (target):
    data/
      shard-0/
        streams/
          transactions/log.bin   (was: data/events/transactions.log)
          balances/log.bin
        beava.snapshot            (was: beava.snapshot at working dir root)
      shard-1/
        streams/
          transactions/log.bin
        beava.snapshot
```

**Operator shard-awareness:** Filter, map, agg, and fork operators are shard-transparent — they operate on the event payload and entity state within a single shard's `AHashMap`. No operator looks at a key that lives on a different shard. Join is the only shard-aware operator: it requires co-located streams (same `shard_key`) enforced at registration time (Wave 3). There is no cross-shard join in v1.2.

---

## 3. Migration-Compat Dance

### Current `data/` layout (today, N=1)

```
$BEAVA_DATA_DIR/                    (default: working directory ".")
  events/
    {stream_name}.log               [O_APPEND binary, postcard frames]
  beava.snapshot                    [postcard v7, base+delta pair]
```

`EventLog::new` (event_log.rs:218) receives `$BEAVA_DATA_DIR/events` as `log_dir`. Per-stream files are named `{sanitized_stream_name}.log` (event_log.rs:238).

### Wave 1 target layout (N=1, path-compatible)

```
$BEAVA_DATA_DIR/
  shard-0/
    streams/
      {stream_name}/
        log.bin                     [same content, new path and filename]
    beava.snapshot                  [format v7 — unchanged at Wave 1]
```

`EventLog::new` receives `$BEAVA_DATA_DIR/shard-0/streams/{stream_name}` and the log file is named `log.bin`. The binary frame format (`[u32 BE len][postcard bytes]`) is unchanged — this is a path and filename change only.

**Upgrade path for existing installations:** `main.rs` startup checks for `$BEAVA_DATA_DIR/events/` existence. If found, prints a migration warning. If `BEAVA_AUTO_MIGRATE=1`, invokes the reshard tool (Wave 4) in N=1 mode to rename files in-place before binding sockets.

### Snapshot format: `shard_count` header

Current: `SNAPSHOT_FORMAT_VERSION = 7` (snapshot.rs:34). Header: `[7u8][type_tag: u8][postcard payload]`.

Wave 4 target: version 8. Header: `[8u8][type_tag: u8][shard_count: u16 LE][postcard payload]`.

**Missing-header fallback:** when loading a file whose first byte is 7 (or lower legacy values), the reader takes the `shard_count = 1` path — treats the snapshot as N=1, loads all keys into shard-0. This is the byte-compat guarantee from design doc §7.

Read path pseudocode:
```rust
match version {
    8 => {
        let shard_count = u16::from_le_bytes(next_two_bytes);
        // validate env N == shard_count, or enter re-shard mode
    }
    7 | 6 => {
        // legacy: assume shard_count = 1; migrate transparently
    }
    _ => return None, // unsupported format
}
```

### Event log: rename, not dual-write

The design doc (§4) commits to `data/shard-N/streams/{name}/log.bin`. No dual-write period. The reshard tool (Wave 4, `src/tools/reshard.rs`) does a one-time migration by reading old logs and writing to the new layout. The server refuses to start if both old (`events/`) and new (`shard-0/`) layouts are present simultaneously, preventing accidental split-brain. Migration is explicit, logged, and one-shot.

---

## 4. Arc-Swap Dirty-Set Question

### Current mechanism (Phase 46 / CORR-10)

`StateStore.dirty_keys: ArcSwap<DashSet<EntityKey>>` (store.rs:226). `take_dirty_and_advance_gen()` atomically swaps in a fresh empty `DashSet` and bumps `snapshot_gen`, so the snapshotter sees a consistent set without a window where a concurrent writer's insert is erased by a concurrent `clear()`.

Per-entity `dirty_gen: AtomicU64` (EntityState, store.rs:152) short-circuits 99% of `mark_dirty` calls before they touch the shared DashSet's shard locks.

### TPC migration verdict

Under TPC, each `Shard` is single-threaded. There is **no concurrent writer** to its state. The arc-swap mechanism's entire purpose disappears.

Per-shard replacement:
```rust
struct Shard {
    dirty_set: HashSet<EntityKey>,   // plain — no arc-swap, no DashSet
    snapshot_gen: u64,               // plain — no AtomicU64
    // ...
}
```

The per-shard snapshotter runs on the shard's own thread (or sends `ShardMsg::Snapshot` and awaits a response). It can `std::mem::take(&mut shard.dirty_set)` — zero-cost, zero atomic, zero lock. The per-entity `dirty_gen: AtomicU64` on `EntityState` similarly becomes a plain `u64`.

**Is arc-swap deleted?** Yes, removed from the hot path at Wave 4. The `arc_swap` crate dep can be dropped from `Cargo.toml` if no other site uses it after this wave. The `dashmap` crate likewise becomes unnecessary for state storage (may still be needed for `SubscriberRegistry` and `shard_probe.rs` histograms unless those are also migrated).

### Cross-shard scatter-gather snapshot consistency

For a snapshot covering all shards, the approach is: send `ShardMsg::BeginSnapshot` to each shard sequentially. Each shard serializes its dirty set on its own thread and returns it. Since each shard processes its inbox serially, "begin snapshot" is a sequentially consistent fence within each shard's own timeline. Cross-shard snapshot consistency is **not** perfectly atomic (shard-0 may snapshot 10ms before shard-3), but this is acceptable under Beava's at-least-once + client-dedup model.

`tests/test_snapshot_rollover_race.rs` must be rewritten to test this scatter-gather consistency model rather than the arc-swap swap model.

---

## 5. Watermark Tracker Relocation

### Current structure

`WatermarkTracker` (event_time.rs:210) lives as a value field on `PipelineEngine` (`pub watermarks: SharedWatermarks`, pipeline.rs:427). `SharedWatermarks = WatermarkTracker` is a type alias (not `Arc<>`) — event_time.rs:584. It wraps three `DashMap`s: `observed_max`, `last_event_time`, `watermark_lateness`, using `AtomicU64` fetch_max for lock-free observe.

`PipelineEngine` is behind a `RwLock` on `ConcurrentAppState` (tcp.rs:93). Watermark observe happens inside the read-lock scope — already lock-free at the watermark level but still holds the engine RwLock.

### Target: per-shard WatermarkState + global lazy publish

Each `Shard` owns a `WatermarkState`:
```rust
struct WatermarkState {
    observed_max: AHashMap<String, u64>,    // no DashMap — single writer
    lateness: AHashMap<String, Duration>,   // per-stream lateness override
}
```

**Global watermark** (for `/metrics` and the `beava_shard_watermark_lag_seconds` gauge): each shard publishes its per-stream max to a global structure (e.g., `DashMap<(stream_name, shard_id), AtomicU64>`) every N events (batched, not per-event). Any reader computes `min(shard_i.max)` across all shards for a given stream. This is the lazy-publish model from design doc §5.

**Migration path:**
- **Wave 1**: `WatermarkTracker` moves into `Shard`, `DashMap`s replaced by `AHashMap`. `PipelineEngine.watermarks` becomes a read-only facade reading from shard-0 for N=1.
- **Wave 3**: Global publish array introduced. `PipelineEngine.watermarks` DashMap fields removed. Facade reads from the global array for N>1.
- **Wave 4**: `WatermarkTracker` struct in event_time.rs is deleted or reduced to a re-export of the per-shard type.

**Tests that break:**
- `tests/test_watermarks.rs` and `tests/test_watermarks_per_stream_lateness.rs` — both construct `WatermarkTracker::new()` directly and call `observe`/`watermark`. Must be updated to construct a `WatermarkState` inside a test shard, or the `WatermarkTracker` name is preserved as a new-type wrapper around the per-shard struct for test compatibility.

---

## 6. Build Order Dependency Graph

### Strict ordering (must come first)

**Wave 0 must land first**, universally. It adds `shard_hint` as a no-op annotation with N=1 always returning 0. Zero behavior change. All tests pass. This is the prerequisite for every subsequent wave — without it, TCP/HTTP parsers have no `shard_hint` value to route on.

**Wave 1 must follow Wave 0.** The `Shard` struct, per-shard state, and the single-shard dispatcher all depend on `shard_hint` being computed in the parse path. Wave 1 also establishes the data-directory layout (`data/shard-0/`) that Wave 4's parallel recovery reads.

### What can parallelize

**Wave 3 (cross-shard queries + joins) is largely independent of Wave 4 (log layout + recovery).** Both depend on Wave 2's shard threads being live, but they address orthogonal concerns:
- Wave 3 is about the read path (scatter-gather for listings, global watermark, join constraint enforcement).
- Wave 4 is about the write path (log files, snapshot format, recovery parallelism).

These can be developed in parallel by different engineers after Wave 2 ships. Ship order should be Wave 3 before Wave 4 to validate scatter-gather reads before restructuring recovery (reads are lower-risk).

### What blocks shipping (the critical path)

```
Wave 0 (shard_hint scaffold)
    ↓
Wave 1 (Shard struct, N=1 compat)
    ↓
Wave 2 (multi-shard routing + SO_REUSEPORT + pinning)
         ↓ [branch here — parallelizable]
Wave 3 (scatter-gather + joins)        Wave 4 (log layout + recovery)
         ↓                                      ↓
         +------------------+-------------------+
                            ↓
                     Wave 5 (production readiness)
                            ↓
                   Merge to main (ship gates)
```

**Wave 2 is the architectural gate.** Until Wave 2 ships (SO_REUSEPORT + SPSC channels + pinning), the TPC hypothesis is untested on Beava's actual workload. The 9-cell matrix re-run at Wave 2 end is the first real signal. If `shard_probe` reports `cross_shard_fraction > 40%` at that point, the merge to main is blocked and the design must be reconsidered before Wave 3+.

**Wave 4 (parallel recovery) blocks the sub-second recovery story** but does not block the throughput goal. Wave 5 requires Wave 4 to be complete to run the N=1 ↔ N=8 property-parity test, since per-shard log is required for deterministic replay across different shard counts.

### Wave ordering surprise

The design doc presents waves 0-4 as strictly sequential. The actual dependency graph shows **Wave 3 and Wave 4 can be developed in parallel after Wave 2**. The practical implication: Wave 4 (log layout migration, re-sharding tool, snapshot format bump, parallel recovery) is the longest individual task and should start as soon as Wave 2's shard threading is unblocked — it should not wait for Wave 3's scatter-gather to be polished first.

A second surprise: **`StateStore` and its `ArcSwap<DashSet>` survive until Wave 4**, not Wave 1. They remain as compatibility shims across Waves 1-3. Tests relying on `StateStore::dirty_keys` (the arc-swap swap behavior in `test_snapshot_rollover_race.rs`) remain valid and passing until Wave 4 deletes the field. This means the `arc_swap` and `dashmap` crate dependencies stay in `Cargo.toml` significantly longer than a casual reading of the wave plan implies — they cannot be removed until Wave 4 completes.

---

## Source Citations (existing code)

| Location | Relevant context |
|---|---|
| `src/server/tcp.rs:89–230` | `ConcurrentAppState` struct: all shared state fields |
| `src/server/tcp.rs:1280` | `handle_push_core_ex` signature and push-path logic |
| `src/server/tcp.rs:1611` | `handle_push_batch` + shard_probe hook |
| `src/server/http_ingest.rs:143` | `http_push_single` — HTTP single-event push path |
| `src/server/http_ingest.rs:191` | `http_push_batch` — HTTP batch push path |
| `src/server/shard_probe.rs:67–80` | `init_from_env` + `record_event` hot-path guard |
| `src/engine/pipeline.rs:404` | `PipelineEngine` struct with `watermarks: SharedWatermarks` |
| `src/engine/pipeline.rs:427` | `pub watermarks: SharedWatermarks` field |
| `src/engine/event_time.rs:210` | `WatermarkTracker` struct (DashMap-backed) |
| `src/engine/event_time.rs:584` | `type SharedWatermarks = WatermarkTracker` (value type, not Arc) |
| `src/state/store.rs:217` | `StateStore` struct with DashMap entities + ArcSwap dirty_keys |
| `src/state/store.rs:226` | `dirty_keys: arc_swap::ArcSwap<dashmap::DashSet<EntityKey>>` |
| `src/state/store.rs:152` | `EntityState.dirty_gen: AtomicU64` (per-entity gen watermark) |
| `src/state/event_log.rs:82` | `LockFreeStreamLog` (O_APPEND fd, kernel-atomic writes) |
| `src/state/event_log.rs:206` | `EventLog.log_dir: PathBuf` |
| `src/state/event_log.rs:218` | `EventLog::new(log_dir)` constructor |
| `src/state/event_log.rs:238` | Per-stream file naming: `{sanitized_name}.log` |
| `src/state/snapshot.rs:34` | `SNAPSHOT_FORMAT_VERSION = 7` |
| `src/main.rs:572` | `snapshot_path` construction from `BEAVA_SNAPSHOT_PATH` |
| `src/main.rs:612–614` | `event_log_dir = $BEAVA_DATA_DIR/events` (changes at Wave 1) |
| `tests/test_snapshot_rollover_race.rs` | Tests arc-swap swap behavior directly — breaks at Wave 4 |
| `tests/test_concurrent.rs:38` | `make_concurrent_state` — must be updated at Wave 1 |
| `tests/test_watermarks.rs` | Direct `WatermarkTracker` construction — breaks at Wave 3 |
| `tests/test_join_stream_stream.rs` | Join tests without shard_key — break at Wave 3 |
