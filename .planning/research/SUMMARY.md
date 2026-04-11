# Project Research Summary

**Project:** Tally v1.3 — Concurrency & Client Batching
**Domain:** Real-time feature server — break the single-core ceiling via key-partitioned multi-threading, async push coalescing, SDK batch API, and off-main-thread snapshot I/O
**Researched:** 2026-04-11
**Confidence:** MEDIUM-HIGH — risks concentrate in Phase 14 (product decisions + tokio runtime coordination)

---

## Executive Summary

v1.3 is mostly **table stakes for a >100k eps server**: every adjacent system (Redis pipelines, Aerospike `batch_write`, DragonflyDB `--num_shards`, Scylla `--smp`) already ships these four capabilities. The four phases decompose cleanly along the push path but are **not equally risky** — Phase 14 (sharded runtime) is the largest architectural change since v1.0, while 12, 13, and 15 are additive/surgical around it.

Research consensus on **build order is 12 → 13 → 14 → 15** (the drafted order). Phase 12 establishes `handle_push_batch` as a shared primitive that Phase 13's wire format reuses verbatim and Phase 14's cross-shard workers also reuse as their inbound dispatch handler. Phase 15 becomes trivial after 14 because each shard clones-and-writes its own snapshot segment under its own lock in parallel.

**Critical risks concentrate in Phase 14:** cross-shard fan-out error semantics are a **product decision, not just engineering** — v1.2 surfaces fan-out errors on the originating PUSH, and v1.3 cannot preserve that without reintroducing synchronous cross-shard coordination that defeats the point. Cache-line false sharing, `std::Mutex` poisoning under panic, and snapshot cross-shard consistency are the other load-bearing concerns. These need explicit locked decisions before Phase 14 plans are written.

---

## Build Order Decision: 12 → 13 → 14 → 15

1. **12 → 13:** Phase 13's `OP_PUSH_BATCH` server handler **is** Phase 12's `handle_push_batch`. 12 first establishes the primitive; 13 becomes wire-format + Python SDK only.
2. **13 → 14:** 12+13 are independently shippable wins that **de-risk** the 2–3 week Phase 14 refactor. Phase 14's cross-shard channel can then send `PushBatch` messages reusing the same handler from day one.
3. **14 → 15:** Phase 15 is "trivial split of existing `spawn_blocking`" after sharding — each shard clones under its own lock in parallel. Shipping 15 first means rewriting it after 14. The 15–25% snapshot stall is real but not blocking 1M eps; 14 is.

**Rejected:** 14-first (max risk, no early wins); 12→13→15→14 (redoes 15).

---

## Stack Additions

| Crate | Version | Phase | Purpose | Notes |
|---|---|---|---|---|
| `parking_lot` | `0.12` (0.12.5) | 14 | Per-shard `Mutex<ShardStore>` | ~25ns uncontended vs std ~60ns. **No poisoning** — load-bearing for panic-in-operator safety. Not reentrant; Guard is !Send — fine because shard locks never cross `.await`. |
| `crossbeam-channel` | **`>=0.5.15`** (pin) | 14 | Sync MPMC for cross-shard fan-out | **MUST be ≥0.5.15** — RUSTSEC-2025-0024 (double-free on Drop) affects 0.5.12–0.5.14. Sync channel keeps cross-shard dispatch inside shard critical section without `.await`. |
| `crossbeam-utils` | `0.8` (0.8.21) | 14 | `CachePadded<T>` for per-shard counters | Arch-portable (64B x86_64, 128B aarch64). Non-negotiable for false-sharing defense — without it, 16-shard scaling drops to ~2× due to cache-line bounces. |
| `core_affinity` | `0.8` (0.8.3) | 14 optional | CPU pinning, feature-gated | Behind `--features core-affinity` (default OFF). Bare-metal NUMA boxes get 5–15% win; containers often no-op. |
| `xxhash-rust` | `0.8` (pin exactly) | 14 | `xxh3_64` for shard routing | **Fifth crate, from ARCHITECTURE research, not STACK.** ahash is NOT spec-stable across crate versions — routing must survive `cargo update`. xxh3 spec-stable + fixed seed. Include hash-version byte in manifest header. |

**Rejected:** `dashmap`/`scc`/`sharded-slab` (worker-per-shard already owns the map); `flume` (interchangeable, crossbeam wins ecosystem); `num_cpus` (superseded by `std::thread::available_parallelism()`, stable since 1.59, honors cgroup v2); `hwloc` (abandoned 2017); `rayon` for snapshot I/O (wrong tool); `tokio::time::sleep(200us)` for coalescing (1ms wheel granularity — use deadline-armed `sleep_until`).

**Net hot-path cost change:** ~0 to -30ns per event (parking_lot savings offset shard routing cost), so Phase 14 single-client ±10% budget is achievable.

---

## Architectural Seams by Phase

### Phase 12 — Server-side async coalescing
- **`src/server/tcp.rs` `handle_connection`** — new `ConnAccumulator` stack-allocated **connection-local** (never on AppState). Four flush triggers: size (N=64), time (T=200µs), forced-on-sync-command, forced-on-connection-close. `select! { biased; read | sleep_until(deadline) if !empty }` pattern.
- **`src/server/tcp.rs` new `handle_push_batch`** — takes **one** lock, groups events by primary stream, iterates once per group. Looks up `key_field`, cascade targets, `fan_out_targets` **once per stream**.
- **`src/engine/pipeline.rs` new `push_batch_no_features(stream, &[events])`**; **`src/state/event_log.rs` `append_many`**; **`src/state/store.rs` `mark_dirty_many`**.
- **Error contract:** returns `Vec<Result<(), TallyError>>` in input order; read loop writes STATUS_ERROR frames with `(batch_id, event_index)` in drain order.

### Phase 13 — `push_many` + OP_PUSH_BATCH (0x0A)
- **`src/server/protocol.rs`** — new decoder ~30 lines. Wire: `[u16 stream_len][stream][u32 count][ for each: [u32 event_len][event_bytes] ]`. All events in one frame target **same stream**.
- **`python/tally/_app.py` `App.push_many`** — reuses existing `encode_push_binary_payload`, prepends envelope, sends via `send_frame_no_recv`. Drain errors via unchanged `drain_errors_nonblock`.
- **`src/server/tcp.rs` dispatch** — decodes into pre-sized `Vec<DecodedEvent>`, calls `handle_push_batch` from Phase 12. Zero new hot-path logic.
- **Hard cap:** 16,384 events per batch (matches Redis pipeline). Reject oversized → STATUS_ERROR + close conn.
- **Backward compat:** `OP_PUSH_ASYNC 0x07` stays forever as single-event fast path.

### Phase 14 — Key-partitioned multi-threaded engine
- **Runtime model — `src/main.rs`:** replace `current_thread` tokio with **one dedicated `std::thread` per shard, each running its own `current_thread` tokio runtime** (Seastar/Glommio pattern). Plus **one multi-thread tokio runtime** for TCP accept + HTTP (`worker_threads=2`, `max_blocking_threads=2`). Rejected: multi-thread work-stealing (destroys locality); `LocalSet::spawn_local` (subtle).
- **`src/shard/mod.rs` (new) `ShardStore` + `ShardWorker`:** owns `entities`, `dirty_keys`, `deleted_keys`, per-stream `event_log`, per-shard `throughput`, per-shard `latency`, per-shard `CachePadded<AtomicU64>` metrics.
- **`src/shard/routing.rs` `key_to_shard`:** **`xxh3_64(key, seed=0) % N`** — do NOT use ahash. Shard vec is `Arc<[Shard]>`, immutable after startup (no dynamic rebalancing in v1.3).
- **`src/shard/message.rs` `CrossShardMsg`:** `{ PushBatch, MGet { keys, reply: oneshot }, DebugQuery, RegisterStream, PrepareSnapshot }`. `crossbeam-channel` bounded(4096) per shard inbox. **Always batched** — never single-event cross-shard sends.
- **`src/engine/pipeline.rs` `ArcSwap<Arc<PipelineEngine>>`:** engine read-only after REGISTER. Every shard reads `arcswap.load()` — near-zero cost, no locking. Rejected: broadcast-to-shards (partial application risk); shared RwLock (hot-path read cost).
- **Coordinator — `src/server/tcp.rs`:** accepts TCP, picks home shard, hands off FD. Connection lifetime lives on one shard worker.
- **MGET/GET scatter-gather:** hash each key to shard, send `CrossShardMsg::MGet { keys, reply }`, merge in input order. GET gains ~1–2µs, stays inside <50µs p99.
- **Event log per-shard — `src/state/event_log.rs`:** directory becomes `events/shard-N/stream.log`. Each shard owns its own `BufWriter<File>` + fsync timer. Backfill cold path interleaves across shards.
- **Snapshot format v7 — `src/state/snapshot.rs`:** new `tally.snapshot.manifest.{seq}` containing `{seq, num_shards, per_shard_hashes, format_version: 7, hash_version}`. Per-shard files: `tally.snapshot.base.{seq}.shard-NN`. **Manifest atomic-rename is the commit point**; missing manifest → roll back to previous (Postgres commit-file model, Dragonfly per-thread RDB). **v6 compat:** check manifest first, fall back to v6 scan, load into shard 0 and re-shard by xxh3 on the fly.

### Phase 15 — Snapshot I/O off main thread
- **`src/state/snapshot_coord.rs` (new) `SnapshotCoordinator`:** broadcasts `CoordMsg::PrepareSnapshot { seq, full }` to every shard.
- **Per-shard flow:** each shard acquires own lock → clones dirty → releases lock → `spawn_blocking` per-shard serialize+write+fsync → `oneshot` reply. All N shards in parallel; per-shard stall ~1/N of v1.2.
- **Manifest commit:** coordinator waits for all replies → `manifest.N.tmp` → fsync → rename → fsync parent dir → cleanup old.
- **Backpressure:** never start a new snapshot cycle while previous is writing. Metric for skipped cycles; alert if > 0.
- **`POST /snapshot?wait=true&timeout_ms=N` (D3):** trivial add after Phase 15. Mirrors Redis `SAVE` vs `BGSAVE`.

---

## Top 8 Pitfalls (ranked from 24 cataloged in PITFALLS.md)

| # | Pitfall | Phase | Severity | Prevention |
|---|---|---|---|---|
| 1 | **Cross-shard fan-out deadlock** (AB-BA when two events fan out through opposite-direction stream pairs) | 14 | **CRITICAL** | Release shard lock BEFORE enqueueing cross-shard message. Fan-out is async enqueue, never nested lock. Loom test with 2 shards + 2 fan-out directions. (C-1) |
| 2 | **Cache-line false sharing on shard vec** (16 shards yields 2× not 10×) | 14 | **CRITICAL** | Wrap per-shard metrics/atomics in `crossbeam_utils::CachePadded`. Bench 1/2/4/8/16 shards on empty pipeline — sub-linear = false sharing; verify with `perf c2c`. (C-6) |
| 3 | **`std::Mutex` poisoning under panic-on-shard** (current code already ignores poisoning; silent corruption risk with N shards) | 14 | **CRITICAL** | Use `parking_lot::Mutex` (no poisoning, 2× faster). Audit operator `push`/`read` for panics → `TallyError`. Panic → reset shard + reload from snapshot. (C-5) |
| 4 | **`std::MutexGuard` across `.await`** (compiles silent on current_thread, UB on multi-thread) | 12, 14 | **CRITICAL** | Keep shard critical sections strictly sync. Never `tokio::sync::Mutex` for shard state (~10× slower). Multi-thread runtime at Phase 14 boundary = free compile-time gate. (C-7) |
| 5 | **Coalescing reorders events vs drain** (Phase 11 drain-in-push-order guarantee) | 12, 14 | **CRITICAL** | Per-connection monotonic `seq: u64`; drain sorts/streams by seq. Batch-level error surface reuses `(batch_id, event_index)` from Phase 13 success criterion 5. Bench test: bad event at known index. (C-2) |
| 6 | **Snapshot cross-shard consistency impossible without stop-the-world** | 14 plan, 15 impl | **CRITICAL — product decision** | Accept **shard-local consistency**. Document as v1.3 locked decision, sibling to "lose ~30s on crash". Manifest + SHA-256 per shard is commit boundary. Incremental dirty sets become per-shard. (C-3) |
| 7 | **Head-of-line blocking on a hot shard** (skewed keys) | 14 | HIGH → MODERATE | Document. `/debug/shards` exposes inbox depth + per-shard eps. **Do not add priority queues** — users fix with key design. (M-2) |
| 8 | **Phase-11-class hot-path regression** (Phase 11 missed 148× slowdown on large×async×HLL because gate was medium-only) | 12, 13, 14, 15 | HIGH | Bench matrix MUST cover small/medium/large × sync/async × with-HLL on every gate. Zipfian multi-key for cross-shard. **Measure per-shard, not just aggregate**. Bench DURING snapshot cycle. |

Full catalog: 7 CRITICAL, 7 HIGH, 7 MODERATE, 3 LOW in PITFALLS.md.

---

## Locked Decisions Needing Product Review

### LD-1. Cross-shard fan-out error swallowing
**Change:** v1.2 surfaces fan-out errors on the originating PUSH's drain queue. In v1.3 under sharding, fan-out becomes an **async enqueue to target shard's inbox**; origin shard does **not** await. Target-shard errors log to that shard's metrics but **do not propagate** to originating client's drain.
**Why forced:** Preserving v1.2 semantic reintroduces sync coordination that defeats Phase 14, and creates AB-BA deadlock surface (C-1).
**Mitigation options:**
- **(a) Fire-and-forget + per-shard per-stream error metrics + alert on rate** — recommended, matches Flink keyed-state shuffle.
- (b) Best-effort eventual drain merge — complex, partial guarantee.
- (c) Awaitable deadline — conflicts with Phase 12 coalescing. Defer.

**Recommendation:** Adopt (a). Document in release notes + `/debug/shards`. Lock in Phase 14 plan doc.

### LD-2. `num_shards` persistence and migration contract
Persist `num_shards` in manifest + config file. Changing across restarts requires **explicit `TALLY_ALLOW_RESHARD=1`** and triggers migration on load. Silent shard-count drift (e.g., 32 → default → 16) is a correctness trap.
**Recommendation:** Adopt. Phase 14 plan must include explicit migration test.

### LD-3. Snapshot is shard-local consistent, not globally consistent
Sibling relaxation to "losing ~30s on crash is acceptable". Fan-out events may land in target-shard snapshot but not origin-shard snapshot within one cycle. Manifest guarantees per-shard files exist and hash-match, NOT that they reflect the same logical moment.
**Recommendation:** Document in PROJECT.md "Key Decisions" table.

### LD-4. Hash function for shard routing — xxh3, not ahash
Pin `xxhash-rust` version; include hash-version byte in manifest header. ahash is NOT spec-stable across crate versions — a `cargo update` could re-shard everything on next restart. Spec-stability is load-bearing for snapshot-across-restart correctness.
**Recommendation:** Use `xxh3_64` with fixed seed. Add `xxhash-rust = "0.8"` to Phase 14 `Cargo.toml` (fifth new crate beyond STACK's four).

---

## Open Questions for Roadmapper

1. **Connection → home-shard assignment policy:** round-robin (simple, may starve) vs first-frame key hash (locality-optimal, requires peeking). Defer to Phase 14 plan; default to round-robin.
2. **`shard_count` power-of-two:** research slightly prefers non-power-of-two with `%` (trivial cost); `num_cpus` rarely is 2^k.
3. **Sync PUSH bypasses coalescer** — Phase 12 must wire opcode-discriminated flush-on-arrival. Test coverage: mix sync + async, assert sync p99 unchanged vs v1.2.
4. **REGISTER vs PUSH race under ArcSwap** — allowed to be slightly async with lazy log-open; needs explicit test.
5. **Cross-shard channel backpressure behavior** when target shard is full: `try_send → Err(Full)` surfaces via drain. Drop vs retry? Lock in Phase 14 plan.
6. **Debug UI per-shard surfaces (D2):** FEATURES lists as "add if time permits". Research recommends including — incremental cost low after Phase 14, high-visibility differentiation vs Scylla/Dragonfly.

---

## Confidence Assessment

| Area | Confidence |
|---|---|
| Stack choices, versions, security | **HIGH** (crates.io verified 2026-04-11, RUSTSEC cross-checked) |
| Feature table-stakes / differentiator | **HIGH** (adjacent systems well-documented) |
| Existing Tally architecture | **HIGH** (read directly from source) |
| Phase 12 coalescing design | **HIGH** (canonical tokio idiom) |
| Phase 13 wire format + SDK | **HIGH** (trivial extension of Phase 11 encoding) |
| Phase 14 runtime model | **MEDIUM-HIGH** (Seastar pattern established; tokio specifics need 1-day spike) |
| **Phase 14 cross-shard error semantics (LD-1)** | **MEDIUM — weakest point** (product decision, not engineering) |
| Phase 14 snapshot format v7 + manifest | **HIGH** (Postgres commit-file pattern) |
| Phase 15 post-Phase-14 | **HIGH** (trivial split of existing spawn_blocking) |
| Build order 12→13→14→15 | **HIGH** (dependency-driven) |

**Overall: MEDIUM-HIGH.** Risk concentrated in LD-1 (product call) and Phase 14 runtime coordination unknowns.

---

## Source Files Index

- **`.planning/research/STACK.md`** (HIGH) — Per-crate eval, versions, RUSTSEC, rejected alternatives, worker-thread boot code, hot-path cost budget, Cargo.toml additions. Read for Phase 14 Cargo changes or Phase 12 timer code.
- **`.planning/research/FEATURES.md`** (HIGH) — Table stakes vs differentiators, competitor matrix (Redis/Aerospike/Scylla/Dragonfly/Flink), concrete API shapes (`push_many`, `OP_PUSH_BATCH`, `--shards N`, coalescing knobs, `/snapshot?wait=true`, debug UI), MVP per phase. Read for Phase 13 SDK or any user-facing surface.
- **`.planning/research/ARCHITECTURE.md`** (HIGH / MEDIUM-HIGH) — v1.2 hot-path under Big Mutex, phase-by-phase integration, state decomposition table, cross-shard fan-out mechanism, snapshot v7 manifest commit protocol, MGET scatter-gather, new components table, data-flow before/after. Read for any Phase 14 plan.
- **`.planning/research/PITFALLS.md`** (HIGH / MEDIUM) — 24 pitfalls by severity with phase attribution, prior-art traps (Scylla/Dragonfly/Redis/Aerospike), "Phase 11 class" meta-lesson on bench matrix coverage. Read for any phase risk section or bench methodology.

---

*Research phase complete. Next: define v1.3 REQUIREMENTS.md, then spawn gsd-roadmapper with this summary as input.*
