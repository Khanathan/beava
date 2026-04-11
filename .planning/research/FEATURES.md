# Feature Research — v1.3 Concurrency & Client Batching

**Domain:** Real-time feature server / high-throughput in-memory K/V store
**Researched:** 2026-04-11
**Confidence:** HIGH (adjacent systems are well-documented and API shapes are stable)
**Scope:** ONLY the four capabilities v1.3 introduces — async coalescing, batch push client API, key-partitioned multi-threading, off-main-thread snapshot. Existing Tally features (v1.0/v1.1/v1.2) are NOT re-researched. For v1.1 feature research, see `v1.1-archive/FEATURES.md`.

---

## Executive Summary

Every one of v1.3's four phases lands Tally somewhere users already are. Redis users know `MSET` and pipelines. Aerospike users know batch-write with per-record error reporting. DragonflyDB and Scylla users know shard-per-thread with `--num_shards` / `--smp`. Kafka+Flink refugees expect keyed state to be partition-local and for the ingest API to accept arrays, not one-at-a-time.

**Bottom line:** v1.3 is mostly **table stakes** for any system advertising >100k eps. The differentiator is the combination — Redis has pipelines but not streaming aggregations; Flink has keyed state but not sub-ms synchronous reads; Scylla has thread-per-core but not streaming features. Tally's job in v1.3 is to match the API idioms users already know so the win ("pipelines + features + zero ops") feels obvious.

**Critical recommendations:** Copy Aerospike's per-record error semantic for `push_many`. Copy Redis's pipeline latency-budget model for async coalescing. Copy DragonflyDB's `--num_shards` knob with `num_cpus` default for Phase 14. Do NOT copy Scylla's CQL surface, Redis Cluster's 16384 hash slots (overkill for single-node), or Flink's async state backends.

---

## Feature Landscape

### Table Stakes (Users Expect These)

Missing any of these would be surprising given Tally's performance claims.

| # | Feature | Why Expected | Complexity | Dep on existing? |
|---|---------|--------------|------------|------------------|
| T1 | **Batch push API on the client** (`push_many`, analogous to Redis `MSET` / Aerospike `batch_write` / Kafka producer batching) | Every high-throughput system has one. Per-event round-trip is known to be the bottleneck. Users already use v1.1 `MSET`/`MGET` and will ask "why not `push_many`?". | LOW–MEDIUM | Depends on v1.2 `OP_PUSH_ASYNC` + drain-errors semantic; new opcode `OP_PUSH_BATCH 0x0A` is additive. |
| T2 | **Per-event error reporting inside a batch** ("valid events land, bad ones surface via drain") | Aerospike default: "all keys processed even if there are failures, failures returned separately for each record." Redis pipelines surface errors per-command. Users will reject all-or-nothing as "too strict for streaming." | MEDIUM | Reuses v1.2 `drain_errors_nonblock`; requires adding `(batch_id, event_index)` attribution to the error payload. |
| T3 | **Server-side pipelining / coalescing of async frames** (Redis `pipeline()`, read-side batching) | Redis clients pipeline on the client; Dragonfly + ScyllaDB coalesce on the server. Users running 4+ concurrent clients on v1.2 will hit the per-event lock contention and file bugs. This is already scoped as Phase 12. | MEDIUM | Depends on v1.2 connection read loop + `state.lock()` granularity; no new SDK surface. |
| T4 | **Key-partitioned multi-threading with `num_cpus` default** | Nobody in 2026 ships a >100k eps server that saturates 1 core out of 48. DragonflyDB default is `num_cpus`; Scylla default is `--smp = num_cpus`; Aerospike auto-pins per NUMA node. Users will ask "why is CPU utilization at 2%?" on day one. | HIGH | The largest structural change since v1.0 — rewrites `StateStore`, fan-out, and snapshot pathways. No dependencies on other v1.3 phases but synergistic with T3. |
| T5 | **Explicit shard count knob** (`--shards N` with `num_cpus` default) | DragonflyDB ships `--num_shards`, Scylla ships `--smp`, Aerospike ships `service.service-threads`. Users with a single 96-core box want to say "use 32, leave the rest for GC/snapshots/other processes." Transparent-only will be asked to be overridden within a week. | LOW (once T4 lands) | Depends on T4. |
| T6 | **Snapshot write does not stall the hot path** | Redis `BGSAVE` (background save via fork); Aerospike device writes are async; Scylla flushes memtables on separate reactor fibers. Users today see 15–25% throughput loss during Tally snapshots — this already generates support questions. | MEDIUM | Depends on v1.1 incremental-snapshot dirty-key set; `spawn_blocking` integration. Independent of T4 but easier after T4 (per-shard snapshots). |
| T7 | **Crash recovery from concurrently-written snapshot** (atomic rename, resume from newest consistent file) | Redis RDB atomic rename, Aerospike device superblock, Scylla sstable atomic append+link — every system has this. Tally already has `.tmp → rename`; T6 must preserve it. | LOW | Reuses existing Phase 7/9 mechanism. |
| T8 | **Cross-shard fan-out still works** (event with `user_id` + `merchant_id` updates both shards) | Flink users expect keyed state per key; when a single event touches two keys, they expect the framework to route both. Breaking v1.0's event-fan-out semantics under multi-threading would be a silent correctness regression. | HIGH | Depends on T4 AND on v1.0 fan-out; cross-shard channel with batching is the standard answer. |
| T9 | **Backward-compatible single-event `push()`** | Redis pipelines don't break single `SET`; Aerospike batch doesn't break single `put`. Existing Tally users must not rewrite to get v1.3 wins. | LOW | v1.2 `OP_PUSH_ASYNC` stays; `OP_PUSH_BATCH` is additive. |

### Differentiators (Competitive Advantage)

These are where v1.3 can pleasantly surprise users.

| # | Feature | Value Proposition | Complexity | Dep on existing? |
|---|---------|-------------------|------------|------------------|
| D1 | **Batch push with typed feature response for sync mode** (`push_many_sync` returns `list[FeatureMap]`) | Redis pipelines return responses in order. No streaming-feature system offers this — Flink is async-only, RisingWave returns via SQL subscriptions. Gives Tally users a way to debug/test batches with full feedback without N round-trips. | MEDIUM | Depends on T1 + existing `push_sync` response path. Low-priority: async is the hot path. |
| D2 | **Per-shard observability in the existing debug UI** (Phase 10.1 topology DAG extends to show per-shard queue depth, events/sec, lock-hold histogram) | DragonflyDB has `INFO` per-shard; Scylla has per-shard Prometheus metrics; neither has a visual topology. Tally's Phase 10 debug UI is already a differentiator — extending it per-shard is low-incremental-cost and very visible. | MEDIUM | Depends on T4 + v1.1 debug UI; mostly adds a "shard" dimension to existing metrics. |
| D3 | **Forced-snapshot-and-wait management API** (`POST /snapshot?wait=true`) | Redis has `BGSAVE` (fire-and-forget) AND `SAVE` (synchronous); Aerospike has `asinfo -v "truncate"`. The "wait until durable" option is essential for CI tests and pre-deploy snapshots. Tally already has `POST /snapshot`; adding `?wait=true` is trivial once T6 lands. | LOW | Depends on T6 + existing HTTP management API. |
| D4 | **Awaitable cross-shard fan-out with a deadline** (event fan-out completes within N ms or surfaces as a drain error) | Flink's cross-key-group ops are async-opaque. Giving users a deadline-based "did the fan-out land?" signal is rare and valuable for fraud use cases where cross-entity signals are correlated. | HIGH | Depends on T4 + T8; risk of over-engineering. Recommend deferring: start with fire-and-forget fan-out (matches Flink), revisit if users ask. |
| D5 | **Latency-budget server-side coalescing knobs** (flush at `min(N events, T µs)`, both tunable) | Redis Streams `XADD MAXLEN` has a tuning knob; Kafka producer has `linger.ms` + `batch.size`. Copying these knob names (`--coalesce-max-events`, `--coalesce-max-wait-us`) makes the feature instantly legible to Kafka refugees. | LOW–MEDIUM (once T3 core lands) | Depends on T3. |
| D6 | **Single-client throughput parity between batched and non-batched when batched** (push_many ≥ single-push path with no regressions) | Most systems show a 2–5× batch speedup because the non-batch path is already fast. Tally's non-batch path is already competitive; a clean batch win signals "the SDK is not in your way." Roadmap target (≥300k eps) meets this. | MEDIUM | Depends on T1. |

### Anti-Features (Commonly Requested, Often Problematic)

Things adjacent systems have that Tally should NOT add in v1.3 (and in some cases ever).

| # | Feature | Why Requested | Why Problematic | Alternative |
|---|---------|---------------|-----------------|-------------|
| A1 | **Cluster mode / multi-node sharding** (Redis Cluster 16384 hash slots, Scylla token rings) | "How do I scale past one box?" | Violates PROJECT.md's "single-node by design." Adds gossip, failover, split-brain, re-sharding — months of work for a problem most users don't have at 1M eps/node. | Document client-side sharding (hash to N instances), don't build. Already in Out of Scope. |
| A2 | **All-or-nothing batch semantics** (Cassandra LOGGED BATCH, Redis `MULTI/EXEC` transactions) | "I want atomicity." | Kills throughput (synchronization barrier per batch), surfaces as "why did 10,000 good events fail because one had a bad field?" Aerospike explicitly documents batches are NOT transactional for this reason. | Per-event error reporting (T2). Atomicity belongs at the stream-partition level, not the batch level. |
| A3 | **Lock-free shared state across shards** (lock-free hashmaps, RCU, MVCC) | "Lock-free is faster." | Shared-nothing with message passing is simpler and (per DragonflyDB) faster at this scale. Lock-free cross-shard access reintroduces the contention T4 is designed to eliminate. | Shared-nothing + explicit cross-shard channel (matches Dragonfly + Scylla). |
| A4 | **Transparent shard-count auto-tuning** (no knob, system decides) | "Just use all my cores." | Defensible default (`num_cpus`) is fine, but no knob = no escape hatch when users want to reserve cores for other processes or for snapshot I/O. Every serious shard-per-core DB ships the knob. | `--shards N` with `num_cpus` default (T5). |
| A5 | **Dynamic shard rebalancing** (Scylla token streaming, Redis Cluster slot migration) | "What if my keys are skewed?" | Huge complexity, rarely needed at single-node scale. If a user has 1 hot key out of 10M, rebalancing won't help (single key = single shard anyway). | Document hash function, accept that skew is an application problem. |
| A6 | **Batch push returning inline feature responses in async mode** | "I want to batch AND see the features." | Defeats the purpose of fire-and-forget — forces server to hold response buffer per event in the batch. Pick one: batch-async (fast, no response) or batch-sync (slow, with response). | D1 covers the sync case; async batch returns OK only (matches Kafka producer). |
| A7 | **Shared snapshot across shards in one file, written by main thread** | "Simpler file layout." | Reintroduces the stall T6 exists to eliminate. Main thread serializing N shards' state sequentially while holding their locks = worst of both worlds. | Per-shard snapshot segments written concurrently by worker pool; merge metadata on recovery. |
| A8 | **Pipeline DAG that crosses shards implicitly** (derive expression references a feature owned by another shard, resolved on read) | "It just works transparently." | Forces cross-shard read on every hot-path eval. Our v1.0 `lookup` + v1.1 cross-stream views need explicit cross-shard semantics — `lookup` becomes an explicit message send. | Keep `lookup` semantics but route across shards via an explicit channel with a cached read (matches how Flink broadcasts keyed state). |

---

## Feature Dependencies

```
v1.2 OP_PUSH_ASYNC + drain_errors_nonblock (existing)
    │
    ├──enables──> T3  Server-side async coalescing (Phase 12)
    │               │
    │               └──enables──> T1  Client push_many (Phase 13)
    │                                │
    │                                └──enables──> D1  Batch sync with feature responses
    │                                             D6  Batched single-client parity
    │
    ├──enables──> T4  Key-partitioned multi-threading (Phase 14)
    │               │
    │               ├──requires──> T5  --shards knob
    │               ├──requires──> T8  Cross-shard fan-out (from v1.0 fan-out)
    │               ├──enables──> D2  Per-shard debug UI
    │               └──enables──> T6  Snapshot off-thread (Phase 15)
    │                                │
    │                                ├──requires──> v1.1 dirty-key incremental snapshot
    │                                ├──requires──> T7  Atomic rename (existing)
    │                                └──enables──> D3  Forced snapshot-and-wait
    │
    └──enables──> T9  Backward-compat single push() (no new work, just preserve)

T2 (per-event error reporting) is a HORIZONTAL requirement on both T1 and T3.
```

### Dependency Notes

- **T1 requires T3:** Client batching without server coalescing just moves the work; server must already handle "N events under one lock" for batching to pay off.
- **T4 requires T8:** Cross-shard fan-out is not optional — existing v1.0 pipelines rely on one event updating multiple streams. Breaking this is a silent correctness regression.
- **T6 easier after T4:** Per-shard snapshots are naturally parallelizable; off-main-thread becomes "snapshot worker pool" instead of "one big blocking task on spawn_blocking."
- **D4 (awaitable fan-out) conflicts with T3's latency budget:** If fan-out must complete synchronously within a deadline, coalescing can't buffer the originating event for 200µs. Defer D4.
- **T2 is horizontal:** Both client batching (T1) AND server coalescing (T3) need a shared error attribution format — `(batch_id: u32, event_index: u32, error_kind)` — so tests written for Phase 12 still work in Phase 13.

---

## Concrete API Shape Recommendations

### `push_many` Python API (Phase 13)

```python
# Async (hot path, default)
app.push_many(Transactions, [
    {"user_id": "u1", "amount": 10.0, ...},
    {"user_id": "u2", "amount": 20.0, ...},
    ...
])  # returns None, errors surface via drain_errors_nonblock

# Sync with responses (differentiator D1, optional)
feature_maps: list[FeatureMap] = app.push_many_sync(Transactions, events)
# returns in order; one entry per event; exceptions raised inline if ALL events failed
```

**Rationale:**
- Signature mirrors `app.mset(dict)` from v1.0 — same mental model ("batch variant of single-call API").
- Async returns `None` (not a future) — matches Kafka producer `send()` with linger and Aerospike async writes.
- Name `push_many` (not `push_batch`) because SDK already uses "many/sync" as modifiers (`push_sync`); consistency with existing vocabulary beats exact match to `OP_PUSH_BATCH`.

### `OP_PUSH_BATCH 0x0A` Wire Format

```
[4 bytes: frame length (u32 BE)]
[1 byte:  opcode = 0x0A]
[2 bytes: stream name length (u16 BE)]
[N bytes: stream name (UTF-8)]
[4 bytes: batch_id (u32 BE)]          <-- for error attribution
[4 bytes: event count (u32 BE)]
[for each event:
    [4 bytes: event length (u32 BE)]
    [N bytes: binary event payload (same format as OP_PUSH_ASYNC payload)]
]
```

**Rationale:**
- `batch_id` is server-assigned on arrival, NOT client-supplied. Drain errors reference `(batch_id, event_index)`; batch_id is opaque to the client but included in error payloads so the client can surface "the 3rd event of the batch you sent at T" if needed.
- Per-event length prefix (not a single payload length) enables per-event error isolation: one malformed event doesn't poison the rest of the batch. Matches Kafka's per-record framing inside a producer batch.
- Reuses existing binary event payload format from Phase 11 — zero new serialization code.

### Drain Error Payload (T2 extension)

```
Existing error record: { kind, stream, message }
Extended:              { kind, stream, message, batch_id?, event_index? }
```

Single-event pushes leave `batch_id`/`event_index` unset (backward compatible). Batched pushes always set them.

### `--shards N` Configuration (Phase 14 / T5)

```
--shards N          (default: num_cpus::get(), min 1, max 256)
```

**Default is `num_cpus`, mirroring DragonflyDB.** Not `num_cpus - 1` (Aerospike's approach of reserving a "fabric" core) — Tally is single-node, no network fabric to protect. Cap at 256 because beyond that the cross-shard fan-out channel matrix explodes.

### Coalescing Knobs (Phase 12 / D5)

```
--coalesce-max-events N   (default 64,  range 1..=1024)
--coalesce-max-wait-us T  (default 200, range 0..=10000)
```

**Name choice:** `coalesce` not `batch` (batch is the client-side concept, coalesce is the server-side concept). `max-wait-us` microseconds unit makes latency cost explicit — users set "200µs" and immediately understand the p50 impact.

### Forced Snapshot Management Endpoint (D3)

```
POST /snapshot                          -> existing, fire-and-forget, returns 202
POST /snapshot?wait=true                -> NEW, blocks until durable, returns 200 with {bytes, duration_ms}
POST /snapshot?wait=true&timeout_ms=5000 -> 408 Request Timeout if budget exceeded
```

Matches Redis's `BGSAVE` vs `SAVE` split.

### Per-Shard Debug UI Surfaces (D2)

Extend existing topology DAG (v1.1 Phase 10.1) with a "Shards" tab:

- Per-shard: events/sec, queue depth (inbound channel), average lock hold time, dirty-key count (for snapshot pressure), memory bytes.
- Per-cross-shard-edge: fan-out events/sec between each shard pair (matrix heatmap — `N×N` where N = num_shards; highlights skew).
- Shard-hotness list: top 10 shards by events/sec, top 10 by memory — surfaces "one shard has 40% of keys" skew problems.

**Matches what DragonflyDB exposes via `INFO server` per-thread and what Scylla exposes via per-shard Prometheus labels.** Tally's differentiator is rendering it in a live topology view instead of raw metrics.

---

## Competitor Capability Matrix

| Capability | Redis / Redis Cluster | Aerospike | ScyllaDB | DragonflyDB | RisingWave / Materialize | Kafka+Flink | **Tally v1.3 plan** |
|------------|----------------------|-----------|----------|-------------|--------------------------|-------------|---------------------|
| **Client batch write API** | `MSET` (sync), `pipeline()` (persistent conn, multi-command) | `batch_write` / `operate` w/ record list | `BATCH` (CQL), not preferred — prefer async individual writes | Redis-compat `MSET` + `pipeline()` | SQL `INSERT ... VALUES (...), (...)` bulk | Producer `send()` w/ `linger.ms` + `batch.size` | `push_many(stream, events)` → `OP_PUSH_BATCH 0x0A` (T1) |
| **Per-record batch error semantics** | Pipeline: one error per command, valid commands succeed | **Default: all keys processed, per-record failure** | BATCH is all-or-nothing (LOGGED); UNLOGGED is per-record | Pipeline semantics inherited | Partial success via error rows | Per-record ack with `RecordMetadata` / `Exception` | **Per-event via drain (copy Aerospike default)** (T2) |
| **Server-side read pipelining / coalescing** | Automatic on pipelined conn; no buffering beyond socket | Batch subtransaction aggregation per node | Prepared statement + token-aware routing amortizes | Per-shard inbound queue coalescing | Ingest micro-batches (100ms default) | Broker-side linger | `--coalesce-max-events` + `--coalesce-max-wait-us` (T3, D5) |
| **Shard-per-thread model** | Single-threaded core (Redis OSS); Cluster = multi-node | Multi-threaded w/ NUMA pinning | **Yes — shard-per-core (Seastar reactor)** | **Yes — shared-nothing shard-per-thread** | Actors on tokio, not 1:1 core | Task slots, key groups | **Yes — shard-per-worker, `num_cpus` default** (T4) |
| **Shard count configuration knob** | N/A (single-thread) | `service.service-threads` | `--smp N` (default=auto) | `--num_shards N` | `--parallelism` | `parallelism.default` | `--shards N` (T5) |
| **Cross-shard fan-out** | N/A | Automatic via index | Coordinator scatter-gather (expensive) | Message-passing between shards (VLL lock mgr for multi-key) | SQL query plan handles it | Key-group shuffle via network | **Explicit cross-shard channel with batching** (T8) |
| **Snapshot off main thread** | `BGSAVE` via fork (COW) | Device write is async by design | Memtable flush on separate reactor fibers | Fork-based RDB snapshot on one shard at a time | Checkpoint barriers | Distributed snapshot (Chandy-Lamport) | `spawn_blocking` + per-shard snapshot segments (T6) |
| **Force snapshot + wait** | `SAVE` (sync) vs `BGSAVE` (async) | `asinfo` snapshot control | N/A (different durability model) | `DEBUG SAVE` | `FLUSH` | Savepoints via REST API | `POST /snapshot?wait=true` (D3) |
| **Per-shard observability** | `INFO` (single-thread) | `asinfo -v "statistics/threads"` | Per-shard Prometheus | `INFO` per-thread + CLUSTER SHARDS | Per-actor metrics | Per-task-slot metrics + UI | **Per-shard tab in existing debug UI topology** (D2) |
| **Dynamic rebalancing** | Cluster slot migration | Auto-rebalance on node change | Token streaming | Migration on shard count change | Not user-facing | Rescale via savepoint | **NOT DOING** (A5) |
| **Cluster mode / multi-node** | Redis Cluster | Multi-node native | Multi-DC native | Cluster mode (newer) | Multi-node native | Multi-node native | **NOT DOING** (A1, single-node by design) |

---

## MVP Definition for v1.3

### Must Ship (Phases 12–15)

- [x] **T1** Client `push_many` + `OP_PUSH_BATCH 0x0A` — Phase 13
- [x] **T2** Per-event error reporting inside a batch — horizontal across Phase 12 + 13
- [x] **T3** Server-side async coalescing — Phase 12
- [x] **T4** Key-partitioned multi-threading — Phase 14
- [x] **T5** `--shards N` knob with `num_cpus` default — Phase 14
- [x] **T6** Snapshot I/O off main thread — Phase 15
- [x] **T7** Crash recovery across partial/concurrent snapshot write — Phase 15 (preserve existing)
- [x] **T8** Cross-shard fan-out — Phase 14
- [x] **T9** Backward-compat single `push()` — free, just don't break

### Add if Time Permits (Same Milestone)

- [ ] **D2** Per-shard debug UI surfaces — low incremental cost after T4; high demo value
- [ ] **D3** `POST /snapshot?wait=true` — trivial after T6
- [ ] **D5** Coalescing knob names + documentation (`--coalesce-max-events` etc.) — trivial after T3, big DX payoff

### Defer to Later Milestone

- [ ] **D1** `push_many_sync` with inline feature responses — interesting for tests/debug but not hot-path; validate demand first
- [ ] **D4** Awaitable cross-shard fan-out with deadline — over-engineered without a concrete user request; conflicts with T3
- [ ] **D6** Single-client parity on non-batched path — likely free, but don't make it a gate

### Never (Already Out of Scope)

- [ ] A1 Cluster / multi-node — `PROJECT.md` explicit
- [ ] A5 Dynamic rebalancing — complexity without benefit at single-node
- [ ] A8 Implicit cross-shard pipeline DAG — contradicts shared-nothing

---

## Specific Answers to Roadmap Questions

1. **All-or-nothing vs per-event errors for batch push?**
   **Per-event errors via drain.** Aerospike's default is "all keys processed, failures returned separately per record." Redis pipelines do the same. All-or-nothing (Cassandra LOGGED BATCH) is explicitly disliked by streaming users because one bad field shouldn't nuke 10k good events. **Error payload: `{batch_id, event_index, kind, message}`.**

2. **Transparent scaling vs `--shards N`?**
   **Both: `num_cpus` default, `--shards N` knob.** Every shard-per-core DB ships the knob (DragonflyDB `--num_shards`, Scylla `--smp`, Aerospike `service-threads`). Users reserve cores for snapshots, GC, other processes, or NUMA pinning. Transparent-only gets a feature request within a week.

3. **Force snapshot and wait?**
   **Yes — `POST /snapshot?wait=true&timeout_ms=N`.** Mirrors Redis `SAVE` vs `BGSAVE`. Essential for CI tests, pre-deploy checkpoints, and integration tests. Trivial to add after Phase 15 (T6) lands. Default remains async (current behavior).

4. **Per-shard observability shape?**
   **Extend existing Phase 10 debug UI topology DAG with a "Shards" tab.** Per-shard metrics (eps, queue depth, lock hold, memory, dirty keys) + cross-shard fan-out heatmap matrix + hotness list. This beats DragonflyDB and Scylla on presentation (they have numbers, not visuals) while matching them on substance. Prometheus endpoint gets shard-labeled metrics for free.

5. **Cross-shard fan-out semantic?**
   **Fire-and-forget with per-shard channel batching (matches Flink's keyed-state shuffle).** Explicit channel, not lock-free shared state. Awaitable deadline (D4) is tempting but conflicts with T3 coalescing and adds complexity without a concrete user request — defer unless a user asks. Document the semantic: "fan-out events are guaranteed to land on the target shard within N ms under normal load; under backpressure, fan-out events can fail and surface via drain."

---

## Sources

- **DragonflyDB:** [Shared-nothing architecture](https://github.com/dragonflydb/dragonfly/blob/main/docs/df-share-nothing.md), [6.43M RPS on 64-core Graviton](https://www.dragonflydb.io/blog/dragonfly-achieves-6-million-rps-on-64-core-graviton3), [Redis threading analysis](https://www.dragonflydb.io/blog/redis-analysis-part-1-threading-model), [DeepWiki reference](https://deepwiki.com/dragonflydb/dragonfly) — confirmed `--num_shards` default = `num_cpus`, shared-nothing with message passing (not locks), VLL for multi-key ops, P50 0.3ms / P99 1.1ms at 6.43M ops/sec.
- **Aerospike batch semantics:** [Batch Operations blog](https://aerospike.com/blog/batch-operations-in-aerospike/), [Batched commands guide](https://aerospike.com/docs/server/guide/batch), [Python client docs](https://aerospike-python-client.readthedocs.io/en/latest/client.html), [aerospike_helpers.batch package](https://aerospike-python-client.readthedocs.io/en/latest/aerospike_helpers.batch.html) — confirmed default is "all keys processed even on failure, per-record errors returned in BatchRecords with per-operation status"; explicitly non-transactional batches.
- **Redis pipeline / MSET / BGSAVE:** Training-data knowledge; stable since Redis 2.x. Pipeline = persistent-connection batching with in-order responses; MSET atomic on single instance; BGSAVE forks for COW snapshot; SAVE is synchronous alternative.
- **ScyllaDB / Cassandra:** Training-data knowledge; Seastar shard-per-core reactor; `--smp` knob; LOGGED BATCH is all-or-nothing and discouraged for throughput; UNLOGGED BATCH is per-record.
- **Kafka producer / Flink keyed state:** Training-data knowledge; `linger.ms` + `batch.size` for producer-side batching; per-record callback ack; Flink keyed state partitioned via key groups with cross-key-group shuffles on rescale.
- **Tally internal:** `/data/home/tally/.planning/PROJECT.md`, `/data/home/tally/.planning/ROADMAP.md` (v1.3 section lines 210–275), `/data/home/tally/CLAUDE.md` — existing v1.0–v1.2 architecture, opcodes, and constraints.

**Confidence levels:**
- DragonflyDB `--num_shards`, shared-nothing: **HIGH** (official docs verified this session)
- Aerospike per-record batch errors: **HIGH** (official docs + blog verified this session)
- Redis pipeline / BGSAVE / MSET semantics: **HIGH** (stable long-documented behavior)
- Scylla `--smp`, Seastar reactor: **HIGH** (stable, widely documented)
- Kafka producer `linger.ms` + per-record callbacks: **HIGH** (stable API since Kafka 0.9)
- RisingWave / Materialize ingest batching specifics: **MEDIUM** (less verified; used as a directional data point, not a design anchor)
- Flink keyed-state shuffle semantics: **MEDIUM** (stable but varies by state backend)

---
*Feature research for: Tally v1.3 Concurrency & Client Batching*
*Researched: 2026-04-11*
