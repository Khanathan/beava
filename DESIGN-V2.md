# Beava v2 — Design Document

**Status:** Draft, greenfield rebuild
**Date:** 2026-04-22
**Supersedes:** v1 architecture (./.planning/ tree)
**Author:** design captured from session, reviewed vs eng-review discipline

---

## 1. Why Rebuild

v1 accumulated architectural debt that caps per-core throughput at ~4-13K EPS on complex cascade workloads (measured, samply-verified). Root causes:

1. **Fjall as hot-path state backend** — every event triggers `postcard::to_stdvec` + `postcard::from_bytes` on a 24KB `EntityState` blob. Serialize/deserialize dominates CPU even with NVMe. Upper bound ~10-15K EPS/core regardless of other optimizations.
2. **`serde_json::Value` in the hot path** — every field access is `match` + `as_*` + unwrap. ~50-100ns per access × 100s of accesses per event.
3. **O(n²) feature lookup** — `stream_state.operators.iter_mut().find(|(n, _)| *n == *fname)` scales badly with feature count.
4. **Bench counts client submissions, not server-processed events** — all historical "1.3M EPS" numbers were client-side fiction. Server truth on complex was ~130 EPS post-saturation.
5. **No write-back cache layer** — fjall treated as hot state, not as durability tier.
6. **No batching at the shard boundary** — state access cost amortized only at ingest (ConnAccumulator).

v2 addresses all six at the architectural level, not as incremental patches.

---

## 2. Goals / Non-Goals

### Goals

1. **≤ 1M EPS aggregate** on a 10-core box (~100K EPS/core) on complex cascade workloads, server-truth measured.
2. **≤ 64 TB total state** via SSD overflow; hot working set (~5-20% of total) fits in RAM cache.
3. **< 1ms reads** from hot cache on materialized features (Redis-class).
4. **Sub-second freshness** on writes. Per-event durability via group-commit WAL.
5. **First-class backfill** — replay historical events into current state, with deterministic semantics.
6. **First-class branching** — fork state at a point in time, drive it with test events, promote or discard.
7. **Single binary** — no external Kafka/Flink/Redis dependencies. Default deployment = 1 primary + N read replicas.
8. **Observable and debuggable** — every metric that matters is exposed; samply/pprof integration built in.

### Non-Goals

1. **Horizontal write scaling** — single-primary model. Writing to multiple primaries requires a distributed coordination story that's out of scope for v2. Address when working set exceeds 64TB or write throughput exceeds 1M EPS.
2. **Exactly-once across failures** — at-least-once with idempotent operators is the correctness model. EOS (end-of-stream exactly-once via transactional commits) is Flink territory; not worth the complexity here.
3. **SQL query engine** — operators are declared via Python/Rust SDK (typed). No ad-hoc SQL. Punt to v3.
4. **Multi-tenant isolation** — one tenant per deployment. No per-tenant resource quotas.
5. **Windowed stream-stream joins across shards beyond simple hash-partitioning** — supported but not optimized; complex join semantics are v3.

---

## 3. Core Primitives

### 3.1 Event

Fundamental unit. Immutable. Has:
- `stream_id: u32` (schema-registered stream)
- `schema_id: u32` (typed row schema; packed `#[repr(C)]` layout)
- `event_time: u64` (milliseconds since epoch)
- `primary_event_id: u64` (shard-scoped unique, for retractions)
- `row: Bytes` (fixed-layout, zero-copy)

No `serde_json::Value`. Ever. Events arrive typed, stay typed.

### 3.2 Stream

Append-only event sequence. Declared by SDK with:
- Schema (typed fields)
- Shard key (for partition routing)
- **TTL tag** (defines retention window — hot RAM, SSD, or cloud tier)
- Optional filter predicate

```python
@bv.stream(shard_key="user_id", ttl="7d", archive="s3://bucket/events/")
class Transactions:
    user_id: str
    amount: f64
    merchant_id: str
    timestamp: i64
```

TTL tag drives tiering policy. Events older than TTL on local SSD are moved to cloud archive and evicted locally; reads from cold tier are slow (seconds) but replayable.

### 3.3 Operator (KV-Native)

Each operator expresses its state update as **KV primitive operations**, not as in-memory struct mutations:

```rust
trait Operator {
    // Write path
    fn apply(&self, event: &Row, cache: &mut StateCache);

    // Read path
    fn read(&self, entity_key: &str, cache: &StateCache) -> FeatureValue;

    // Durability: serialize current-bucket values for WAL flush
    fn flush_current(&self, cache: &StateCache) -> Vec<KvOp>;
}
```

Example — `Count(window=1h, bucket=1min)`:
- Key layout: `count | {entity_id} | {bucket_min_since_epoch}` → `u64`
- Apply: increment current-bucket KV in memory
- Read: sum 60 current+sealed buckets (or read materialized view)
- Flush: write dirty (current_bucket) KVs to RocksDB

No `EntityState` blob. Operator = key layout + RMW pattern on small primitives.

### 3.4 Materialized View (Current-Window Value)

For every `(entity, feature)`, maintain a **current window aggregate** in memory:

```
materialized[(entity, feature)] → current window value (scalar or sketch)
```

Write path updates materialized on every event (~20ns). Read path does single lookup (~1μs).

**Lazy + sparse** — only hot entities are cached. Cold entities compute on read via bucket range scan. LFU admission policy with size-bounded cache.

### 3.5 Shard

One shard = one core = one OS thread = one tokio `current_thread` runtime.

Shard owns:
- `hot_cache: AHashMap<EntityKey, RowState>` (bounded by RAM budget)
- `dirty_set: HashSet<EntityKey>`
- `materialized: HashMap<(EntityKey, FeatureId), Value>`
- `current_buckets: HashMap<(EntityKey, FeatureId, BucketId), Partial>`
- `rocksdb_partition: rocksdb::DB` (column-family-scoped view)
- `wal: WalWriter` (per-shard sequence)
- `inbox_rx: spsc::Receiver<ShardBatch>`

Single-writer. No locks on the hot path. Reads served from hot cache or RocksDB via `rocksdb::DB::get` (block cache).

### 3.6 Backfill (first primitive)

Backfill is **not** a separate codepath. It's a tagged event batch:

```
BackfillBatch {
    events: Vec<Event>,
    mode: {Deterministic | Parallel},
    source: {LocalWAL | S3Archive | ExternalReplay},
    fence: BackfillFence,  // all realtime events after this fence wait
}
```

Backfill events flow through the same operator pipeline as realtime. Deterministic mode serializes backfill and realtime at the fence (predictable order). Parallel mode replays backfill concurrently with realtime (faster, weaker semantics — only usable if operators are commutative/associative, which most are).

### 3.7 Branch (first primitive)

A **branch** is a forked state namespace:

```
Branch {
    id: BranchId,
    parent_snapshot: SnapshotId,
    diverged_at: EventSeq,
    events_since_fork: Vec<Event>,  // branch-only events
    status: {Open, Promoted, Discarded},
}
```

Creates a RocksDB snapshot at fork point. Subsequent writes to the branch go to a copy-on-write overlay (separate column family, keyed by branch ID). Realtime continues to the main trunk.

Promote = merge branch CF into trunk CF atomically. Discard = drop branch CF.

Used for:
- **A/B testing feature definitions** — fork, deploy new operator, validate outputs vs trunk
- **Backfill validation** — fork, backfill historical events, compare final state to trunk
- **Replay for debugging** — fork from snapshot before a suspected bug, replay events, observe divergence

---

## 4. Architecture Overview

```
                            ┌───────────────────────────────────┐
                            │          Client SDKs              │
                            │  Python, Rust, TypeScript         │
                            │  push_sync, push_many, subscribe  │
                            └─────────┬─────────────────────────┘
                                      │ TCP binary (typed rows)
                                      ▼
┌──────────────────────────────────────────────────────────────────────┐
│                       PRIMARY PROCESS                                 │
│                                                                       │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │   Accept loop (per-core via SO_REUSEPORT)                     │ │
│  │     ├─ Decode typed row (no JSON)                              │ │
│  │     ├─ Compute shard_hint                                      │ │
│  │     └─ Per-target-shard batcher (1ms window, 1k events)       │ │
│  └──────────────────────────┬─────────────────────────────────────┘ │
│                             │ batched SPSC                           │
│                             ▼                                         │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │   Shard thread (×N, one per core)                             │  │
│  │                                                                │  │
│  │   ┌─────────────────────────────────────────────────────┐    │  │
│  │   │  Drain batch from inbox                              │    │  │
│  │   │  Group by entity_key (write-combine)                 │    │  │
│  │   │                                                       │    │  │
│  │   │  For each unique entity:                             │    │  │
│  │   │    - Check hot_cache; fetch from RocksDB on miss    │    │  │
│  │   │    - For each operator:                              │    │  │
│  │   │        - Update current_bucket (in-RAM)              │    │  │
│  │   │        - Update materialized (in-RAM, if cached)     │    │  │
│  │   │    - Mark dirty                                      │    │  │
│  │   │    - Append to WAL buffer                            │    │  │
│  │   │                                                       │    │  │
│  │   │  Reply to client (sync-ACK) OR fire-forget          │    │  │
│  │   └─────────────────────────────────────────────────────┘    │  │
│  │                                                                │  │
│  │   Bg tasks on shard (same thread, interleaved):              │  │
│  │     - Bucket rollover (every 1min): flush sealed → RocksDB   │  │
│  │     - Cache eviction (LRU when over budget)                  │  │
│  │     - WAL fsync trigger (signals group-commit coordinator)   │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                             │                                         │
│                             ▼                                         │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │   Group-commit WAL coordinator (own thread)                   │  │
│  │     - fsync every 1-5ms OR 1MB, whichever first               │  │
│  │     - Signal waiters past their LSN                           │  │
│  │     - Ship segments to read replicas + cloud archive          │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                             │                                         │
│                             ▼                                         │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │   RocksDB (per-shard column families)                         │  │
│  │     cf:state/{shard_id}          ← KV-native state           │  │
│  │     cf:buckets/{shard_id}        ← sealed time buckets       │  │
│  │     cf:branch/{branch_id}        ← COW overlays               │  │
│  │     cf:snapshots/{snapshot_id}   ← point-in-time refs         │  │
│  │     cf:metadata                  ← schemas, branches, etc.    │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                             │                                         │
│                             ▼                                         │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │   Cloud tier (optional: S3/GCS/R2)                            │  │
│  │     - Old event log segments (past TTL on local SSD)          │  │
│  │     - Periodic full snapshots for DR                          │  │
│  │     - Replayable on-demand                                    │  │
│  └──────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
                             │ WAL stream
                             ▼
┌──────────────────────────────────────────────────────────────────────┐
│                   READ REPLICAS (×M, optional)                        │
│                                                                       │
│  Tails primary's WAL. Rebuilds hot_cache + materialized by replaying │
│  events. Serves reads only (no writes). Lag = WAL replication delay. │
│                                                                       │
│  Can be promoted to primary on failure (raft-lite or manual).        │
└──────────────────────────────────────────────────────────────────────┘
```

---

## 5. Storage Layout (RocksDB Column Families)

```
cf:state/{shard_id}
    Key:   {entity_key} | {stream_id} | {feature_id}
    Value: operator state (small, fixed-size for numeric ops; variable for sketches)
    Access: per-event RMW via hot_cache; flush on eviction or rollover

cf:buckets/{shard_id}
    Key:   {entity_key} | {stream_id} | {feature_id} | {bucket_id}
    Value: sealed bucket aggregate (u64, f64, HLL bytes, etc.)
    Access: range scan for window reads; append on bucket rollover

cf:branches/{branch_id}
    Key:   same layout as cf:state/cf:buckets
    Value: copy-on-write overlay entries
    Access: checked before trunk CF on reads; promoted via merge

cf:snapshots/{snapshot_id}
    Stores RocksDB-native snapshots (checkpoints) + metadata

cf:metadata
    Schemas, stream defs, branch registry, shard topology
    Keyed by name; small; rarely written

cf:wal/{shard_id}
    Append-only event records for WAL
    Separate CF to get independent compaction schedule
```

### Why RocksDB, not fjall

1. **Maturity** — Rust bindings maintained by Facebook, used in TiKV, CockroachDB, Meta internal. Fjall is younger (1-2 years).
2. **Column families** — first-class support. Fjall's partitions are similar but API less ergonomic for 5+ CFs.
3. **Snapshots + checkpoints** — native, transactional, essential for branching. Fjall has snapshots but less battle-tested.
4. **Merge operator** — RocksDB supports `MergeOperator` which could replace client-side RMW for pure counters (out of scope for v2 but future win).
5. **Write batching + atomic commits** — `WriteBatch` across CFs with atomicity. Critical for branch promotion.
6. **Tunable compression + compaction** — Leveled, Universal, FIFO. FIFO is ideal for `cf:wal` (time-ordered, TTL drop).

Trade-off: ~15-25MB more binary size vs fjall. Acceptable.

---

## 6. Write Path (with ASCII)

```
Event arrives (typed row on TCP)
    │
    ▼
[Accept loop on core K]
    │ compute shard_hint(event.shard_key) % N
    │
    ▼
[Per-target-shard batcher]
    │ accumulate 1-1000 events or 1ms
    │
    ▼ SPSC send (one batch, not per-event)
    │
    ▼
[Shard thread on core target_shard]
    │
    ├─► WAL append (in-memory buffer)
    │       │
    │       └─► WAL coordinator fsyncs every 1-5ms → signals LSN done
    │                                                    │
    │                                                    ▼ (for sync-ACK path)
    │                                              client receives ACK
    │
    ├─► Group batch events by entity_key
    │
    └─► For each unique entity in batch:
          │
          ├─► hot_cache.get_or_fetch(entity)
          │     │
          │     └─► cache MISS: rocksdb.get(cf:state, key)
          │           │ deserialize (fixed-layout, ~500ns for 500B row)
          │           │ admit to cache (LFU)
          │           │
          │     └─► cache HIT: ~20ns
          │
          ├─► For each (feature, operator) this event affects:
          │     │
          │     ├─► current_bucket[(entity, feature, current_bucket_id)] += delta
          │     │     (~50ns, pure in-memory)
          │     │
          │     ├─► materialized[(entity, feature)] += delta
          │     │     (~20ns if in cache; skip if not cached)
          │     │
          │     └─► (for sketches: merge into current bucket's sketch)
          │
          └─► mark dirty
               │
               └─► (eventually: bucket rollover flushes dirty to rocksdb)

Per-event total on hot path: ~5-10μs (cache hit path)
                              ~50-100μs (cache miss with RocksDB read)
```

**Per-core throughput target: ~100K EPS with 95%+ cache hit rate on complex workloads.**

---

## 7. Read Path (with ASCII)

```
Feature query: "user_123.tx_count_1h"
    │
    ├─► Is materialized[(user_123, count_1h)] in cache?
    │     │
    │     YES → return cached value (~1μs)
    │     │
    │     NO  → compute from buckets:
    │             │
    │             ├─► hot_bucket = current_buckets[(user_123, count_1h, current_id)]
    │             │     (~50ns, in-memory)
    │             │
    │             ├─► cold_buckets = rocksdb.range_scan(
    │             │       cf:buckets,
    │             │       prefix="count_1h|user_123|",
    │             │       from=now-59min, to=now-1min
    │             │   )
    │             │     (~50-100μs, 59 entries from SST + block cache)
    │             │
    │             ├─► agg = hot_bucket + sum(cold_buckets)
    │             │
    │             ├─► admit to materialized cache (LFU)
    │             │
    │             └─► return agg

Total: 1μs on hit (~95% of queries), 50-100μs on miss (~5%)
```

**Per-core read throughput: ~1M QPS on hot cached entities.**

---

## 8. Backfill (first primitive)

```
BACKFILL FROM S3 ARCHIVE
═══════════════════════════

backfill_cli replay \
    --from s3://bucket/events/2026-04-01.log \
    --stream Transactions \
    --branch backfill-validation-2026-04-22 \
    --mode deterministic

    │
    ▼
┌─────────────────────────────────────────────┐
│ Backfill Coordinator                         │
│                                              │
│ 1. Download segment from S3                  │
│ 2. Create branch `backfill-validation-...`   │
│    (forks state at current point)            │
│ 3. Set fence: realtime events during         │
│    backfill go to pending queue              │
│ 4. Replay events → branched shard threads    │
│ 5. After replay: merge pending realtime      │
│ 6. Validate: diff branch vs trunk            │
│ 7. Promote OR discard                        │
└─────────────────────────────────────────────┘
    │
    ▼
Shard threads process backfill events through SAME pipeline:
    - Same KV-native operators
    - Same current_bucket + materialized logic
    - Different target CF: cf:branches/{branch_id}

Fence coordination:
    - Realtime events arrive during backfill
    - They are queued to branch's pending queue (not applied)
    - After backfill completes, pending queue drains to branch
    - Eventually: branch merged to trunk atomically
```

Modes:

- **Deterministic** — backfill + realtime serialized at fence. Guarantees same final state as if all events arrived in order. Slower but reproducible.
- **Parallel** — backfill and realtime interleave. Faster. Only correct if operators are commutative (count/sum/min/max/distinct yes; median/percentile no).

### Backfill throughput target

Backfill is **bulk-optimized**: larger batches (10K-100K events), higher cache pressure (touch many cold entities), bypass WAL (branched state flushed in bulk), parallel across shards.

Target: **~5M EPS aggregate for backfill** (50× write path throughput).

---

## 9. Branching (first primitive)

```
BRANCH LIFECYCLE
═══════════════════

Trunk state at t=T₀:
    user_123.count_1h = 47
    user_456.count_1h = 12
    ...

│
│ fork branch "test-new-velocity-features" at t=T₀
▼

    ┌─ Trunk continues receiving realtime events ─▶ t=T₁
    │                                              │
    │  user_123.count_1h = 54                      │
    │  user_456.count_1h = 18                      │
    │                                              │
    └─ Branch: rocksdb snapshot @ T₀              │
       + cf:branches/test-new-velocity overlay     │
                                                    │
       Backfill: replay historical events           │
       against branch's KV state                    │
                                                    │
       Branch state at T_branch:                    │
           user_123.new_feature = 22                │
           user_123.count_1h = 47 + backfill delta  │
                                                    │
                                                    ▼
                                               t=T_merge

At merge time:
    OPTION 1: PROMOTE
      - Validate branch state matches expectations
      - Atomic WriteBatch: merge cf:branches/test-* → cf:state
      - Delete branch CF
      - Trunk state now includes branch's changes

    OPTION 2: DISCARD
      - DropColumnFamily(cf:branches/test-*)
      - Trunk unchanged
      - Branch state gone

Realtime events during branch lifetime:
    - Applied to trunk (not branch)
    - Branch doesn't see them unless explicitly forwarded
    - On promote: merge conflicts possible (same entity touched both sides)
      → last-write-wins by event_time is default, configurable per operator
```

**Use cases:**

1. **Feature definition change validation**
   ```
   fork branch, deploy new operator version, replay 24h of events,
   compare branch vs trunk for top 1000 users, confirm diff matches expectation,
   promote
   ```

2. **Backfill correctness testing**
   ```
   fork branch, backfill from old system's archive, compute final state,
   compare to old system's last-known state, discard (validation only)
   ```

3. **Replay debugging**
   ```
   fork branch at snapshot before incident, replay events slowly with operator
   observability turned on, identify when state diverged, fix operator, re-validate
   ```

**RocksDB implementation:**
- `DB::create_cf(cf:branches/{id})`
- Branch writes go through `WriteBatch` targeting branch CF
- Reads check branch CF first, fall through to trunk CF
- Promote via `WriteBatch` merging branch entries into trunk + `drop_cf`
- Discard via `drop_cf`

---

## 10. Event Log + Cloud Tiering

Event log is separate from state — it's the **source of truth for replay**.

```
EVENT LOG LAYOUT
═══════════════════

cf:wal/{shard_id}
    Key: {event_seq_u64}
    Value: serialized event (typed row bytes + metadata)

Structure:
    - Append-only
    - Per-shard monotonic sequence
    - FIFO compaction in RocksDB (oldest segments first)
    - TTL driven by stream's ttl tag

Retention tiers:
    ┌─────────────────────────────────────┐
    │ HOT (RAM buffer)                     │ ~1-5s of recent events
    │  - Served to read replicas live      │
    │  - Buffered pre-fsync                │
    └────────────┬────────────────────────┘
                 │ fsync (group commit, 1-5ms)
                 ▼
    ┌─────────────────────────────────────┐
    │ WARM (local SSD, RocksDB WAL)        │ up to stream TTL
    │  - Queryable, replayable             │
    │  - Served to replicas                │
    └────────────┬────────────────────────┘
                 │ TTL expiry triggers offload
                 ▼
    ┌─────────────────────────────────────┐
    │ COLD (cloud object storage)          │ indefinite retention
    │  - Per-stream-per-day segments       │
    │  - S3/GCS/R2 with configurable       │
    │    lifecycle (IA, Glacier, etc.)     │
    │  - Replayable via backfill on demand │
    └─────────────────────────────────────┘

Stream TTL tag examples:
    ttl="1h"        → aggressive eviction, cost-minimizing
    ttl="7d"        → common default for fraud/ad-tech
    ttl="90d"       → compliance-heavy use cases
    ttl="forever"   → disabled local TTL; cloud retention controls it
```

### Offload process

Async background task per shard:
1. Identify segments past TTL
2. Compact into daily parquet/avro files with column-family index
3. Upload to S3 with content-addressed naming
4. Update metadata in `cf:metadata` (new cold tier entry)
5. Delete local segments
6. Read path checks metadata index first; cold fetch on demand

### Replay from cloud

```python
bv.backfill.replay(
    stream="Transactions",
    from_time="2025-11-01T00:00:00Z",
    to_time="2025-11-07T23:59:59Z",
    into_branch="validation-2026-04-22",
)
```

Coordinator:
1. Queries metadata for matching segments (indexed by stream + time)
2. Spawns parallel readers streaming from S3
3. Feeds events into backfill pipeline (see §8)

Replay rate: bounded by S3 throughput (~100-500 MB/s per connection, parallelizable). Typical: 2-10M events/sec aggregate.

---

## 11. Operators — KV-Native Patterns

Canonical operators and their KV layouts:

### Count / Sum / Avg (numeric, windowed)

```
Key:   {op} | {entity} | {feature} | {bucket_id}
Value: u64 (count) | f64 (sum) | (f64, u64) for avg

Write:  current_bucket[key] += delta
Read:   sum over [now-window, now] buckets + current
Roll:   on bucket advance, materialized -= expired_bucket_value
```

### Min / Max (windowed)

```
Key:   {op} | {entity} | {feature} | {bucket_id}
Value: (val, count_of_val)  # track count for eviction semantics

Write:  update in place if new extreme
Read:   min/max over window buckets + current
Roll:   if extreme was in expired bucket → rescan remaining buckets
```

Rescan cost: O(buckets_in_window) on eviction. Amortizes over bucket duration; negligible.

### Last / First

```
Key:   {op} | {entity} | {feature}
Value: (value, event_time)

Write:  update if event_time > stored (Last) or < stored (First)
Read:   single KV lookup
Roll:   no-op (not window-bound unless TTL)
```

### Distinct Count (HLL sketch)

```
Key:   {op} | {entity} | {feature} | {bucket_id}
Value: HLL sketch bytes (~256B-16KB depending on precision)

Write:  pfadd(current_bucket.hll, event.value)
Read:   pfmerge across window buckets + current
Roll:   no clean decrement; on eviction, recompute merge of remaining buckets
```

### Enrich From Table (cross-key lookup)

```
Looks up a value from a replicated reference table.

Key:   table:{table_name} | {lookup_key}
Value: table row bytes

Write:  table row updates go to all shards (broadcast, low-volume)
Read:   per-event lookup on shard-local replica (lock-free)
```

Reference tables are small (< 10M rows), replicated across all shards. Updates flow via separate admin API, not the event stream.

### Stream-Stream Join (windowed)

```
Buffer layout per side:
    Key:   join:{join_id} | side:{L|R} | {join_key} | {event_time}
    Value: event row bytes

Emit layout:
    When a new event arrives on L:
      range_scan({join_id} | R | {join_key} | now-window, now)
      emit matches × (L, R)
```

Hot-path cost: range scan for match side each event. For small window (< 1h) and bounded cardinality, ~100-200μs on cached. Higher on cold.

### Time-decay variants (for sketches)

For HLL/TopK/Percentile where clean bucket eviction requires rescan:

Alternative: **exponential decay sketch** — each write adds with decay factor. No bucket accounting. Approximate but O(1) read.

Configurable per operator via `@bv.count(decay="exponential", half_life="10min")`.

---

## 12. Batching — First-Class per Operator

Each operator has a batch-apply variant:

```rust
trait Operator {
    fn apply(&self, event: &Row, cache: &mut StateCache);

    // NEW: batched apply — used when multiple events target same entity
    fn apply_batch(&self, events: &[&Row], cache: &mut StateCache) {
        // Default: loop
        for e in events { self.apply(e, cache); }
    }
}
```

Numeric operators override `apply_batch` for amortization:

```rust
impl Operator for CountOp {
    fn apply_batch(&self, events: &[&Row], cache: &mut StateCache) {
        // Single cache read, single write, N increments
        let slot = cache.get_or_init_current(key);
        *slot += events.len() as u64;  // bulk increment
    }
}

impl Operator for SumOpF64 {
    fn apply_batch(&self, events: &[&Row], cache: &mut StateCache) {
        let slot = cache.get_or_init_current(key);
        // Could SIMD-sum: f64x8 over events
        *slot += events.iter().map(|e| e.get_f64(self.offset)).sum::<f64>();
    }
}
```

SIMD for numeric aggregates: f64x4/x8 via `std::simd` or explicit AVX2/NEON intrinsics on batches ≥ 64. Measured win: 1.3-2× on hot-hot entities.

**Per-shard batching pattern** (§4 diagram):
1. Drain inbox batch (say 1000 events)
2. Group by entity_key → ~300 unique entities (Zipf)
3. For each entity: fetch state once, call `apply_batch` per operator
4. Write state back once per entity

Amortization: 1000 events processed with ~300 state fetches + ~300 state writes instead of 1000 of each. Net: ~3-5× throughput on heavy-tail workloads.

---

## 13. Thread-per-Core Model

```
Process layout:
    ┌───────────────────────────────────────────────────┐
    │   Main thread                                      │
    │     - Parses config, CLI args                      │
    │     - Opens RocksDB                                │
    │     - Spawns N shard threads (N = core count)      │
    │     - Spawns WAL coordinator thread                │
    │     - Spawns replication shipper thread            │
    │     - Spawns cloud archive uploader thread         │
    │     - Spawns HTTP admin/metrics server (axum)      │
    │     - Block on SIGTERM for graceful shutdown       │
    └───────────────────────────────────────────────────┘

Shard thread (×N):
    ┌───────────────────────────────────────────────────┐
    │   Tokio current_thread runtime                     │
    │   Pinned to core (core_affinity crate)             │
    │                                                    │
    │   Owns:                                            │
    │     - Shard struct (hot cache, dirty set, ...)    │
    │     - RocksDB CF handles (Arc clones, thread-safe)│
    │     - Local SPSC inbox (crossbeam channel)         │
    │     - Local oneshot response channels              │
    │                                                    │
    │   No locks on hot path. Single writer invariant.   │
    │   Async operations: tokio::time::sleep for timers, │
    │   tokio::select! for inbox + timer + shutdown.     │
    └───────────────────────────────────────────────────┘

WAL coordinator (×1 global):
    ┌───────────────────────────────────────────────────┐
    │   Dedicated OS thread (non-tokio)                  │
    │   Collects WAL appends from all shards (MPSC)      │
    │   fsync every 1-5ms or 1MB                         │
    │   Signals waiters (oneshot) past their LSN         │
    └───────────────────────────────────────────────────┘

Replication shipper (×1):
    ┌───────────────────────────────────────────────────┐
    │   Tokio multi-thread runtime                       │
    │   Serves WAL stream to read replicas via TCP       │
    │   Replicas request from specific LSN               │
    └───────────────────────────────────────────────────┘
```

**Key principle**: compute-heavy work is on shard threads (pinned, no I/O). I/O-heavy work is on separate threads/runtimes (WAL fsync, replication, cloud upload).

### Cross-shard dispatch

Some events target a different shard than their source (e.g., downstream table keyed by merchant_id when source is keyed by user_id):

```rust
enum ShardOp {
    Push { event: Event },
    PushRouted { event: Event, op_list: &'static [OpId] },  // for cross-shard cascade
    GetFeature { key: String, feature_id: u32, reply_tx: oneshot::Sender<Value> },
    Snapshot { reply_tx: oneshot::Sender<SnapshotId> },
    ...
}
```

Cross-shard via SPSC from source shard's outbox → target shard's inbox. Low latency (crossbeam channel ~100ns).

---

## 14. Read Replicas

```
PRIMARY                                READ REPLICA (×M)
────────                               ────────────────
                                       
State: source of truth                 State: replicated snapshot + WAL tail
Writes: accepted here only             Writes: rejected (forwarded to primary)
Reads: local hot cache                 Reads: local hot cache (eventually consistent)

WAL stream ──────────────────▶ Replica WAL consumer
                                   │
                                   ▼
                               Apply to local shards
                                   │
                                   ▼
                               Update local hot cache
                                   │
                                   ▼
                               Serve reads at local latency
```

### Consistency model

- **Eventual consistency** — replica lags primary by replication RTT + batch window. Typical: 5-50ms.
- **Per-request consistency token** — client can pass `min_lsn` with read request; replica waits for that LSN before serving. Enables "read-your-own-writes" when needed.
- **Monotonic read** — within a session, reads never go backward in time.

### Failover

Manual for v2 (operator runs promote command). Future: raft-lite for auto-promote. Explicitly out of scope for v2 — keep it simple.

### Use cases for replicas

1. **Read scaling** — 1 primary writer + 10 replica readers handles 10× read QPS
2. **Geo-distribution** — replicas in other regions for low-latency reads
3. **Analytics isolation** — heavy ad-hoc queries hit replicas, don't impact write path
4. **Warm standby** — manual failover target on primary failure

---

## 15. Durability + Recovery

### Write durability (group-commit WAL)

```
Write path:
    1. Shard thread accepts event
    2. Appends to local WAL buffer
    3. Notifies WAL coordinator of new LSN
    4. Applies to hot cache (current_bucket, materialized)
    5. Waits for WAL coordinator to signal LSN fsync'd (for sync-ACK mode)
    6. Returns ACK to client

WAL coordinator:
    Every 1-5ms OR when buffer hits 1MB:
        fsync() to RocksDB's WAL
        Signal all waiting shards (notify_all via channel)
        Ship segments to replicas (async)

Durability guarantee:
    Once client receives ACK → event is fsynced to disk.
    Recovery from crash: RocksDB replays WAL automatically.
```

### State durability (write-back to RocksDB)

Hot state persistence:
- Bucket rollover (every 1min): dirty current_buckets → RocksDB `cf:buckets`
- Cache eviction (when over budget): dirty entries → RocksDB `cf:state`
- Periodic snapshot (every 30s): hot cache snapshot → RocksDB checkpoint

### Recovery

On restart:
1. Open RocksDB. WAL replay happens automatically (RocksDB built-in).
2. Load latest cache snapshot from `cf:snapshots` (~1-5s for 64GB cache).
3. Replay event log since snapshot to rebuild current_buckets + materialized (~10-60s depending on load).
4. Shards enter online state; replicas resync.

**Target RTO (recovery time objective): < 60s** for 64GB cache + 1h WAL tail.

---

## 16. Failure Modes

| Failure | Detection | Recovery | Data loss window |
|---------|-----------|----------|------------------|
| Process crash | Health check | RocksDB WAL replay on restart | 0 (events fsynced) |
| Disk full | Write error | Stop accepting writes; page operator | 0 |
| Disk corruption | RocksDB checksum mismatch | Restore from cloud snapshot + replay | ~last snapshot |
| Memory exhaustion | OOM kill | Same as crash; RocksDB replays | 0 |
| Network partition (client ↔ primary) | Client timeout | Client retries; idempotent operators handle duplicates | 0 |
| Replica lag | Monitoring | Alert; investigate network | N/A |
| Clock skew | Event time != wall time | Use event_time from event, not wall; skew tolerated in buckets | N/A |
| Shard thread panic | Caught by tokio JoinHandle | Restart shard; replay from WAL | 0 |
| WAL coordinator death | Shard waiters timeout | Respawn; resume from last LSN | 0 |
| Cloud upload failure | Metric + retry | Retry with backoff; if persistent, keep segment local past TTL | 0 (events still on local SSD) |
| Branch promote failure mid-commit | RocksDB WriteBatch atomicity | Either fully promoted or fully rolled back | 0 |

---

## 17. Locked Decisions

Decisions locked in session 2026-04-22. Each entry records: the decision, the rationale, and where the doc needs follow-up edits. Cross-cutting principle locked alongside: **devex-first** — sensible defaults, high-level domain operators as the public API, tuning knobs opt-in. See §3.8.

1. **Storage engine: RocksDB.** Battle-tested, native column families for snapshot/branch model, atomic WriteBatch across CFs, 10+ years of large-scale production use. Phase 53 spike with fjall hit the STOP perf gate 2,468× on a similar workload — known-bad on this shape. No pluggability per §18.10.

2. **Runtime: tokio `current_thread` + `core_affinity` pinning.** Cross-platform (macOS + Linux), proven, no glommio Linux-only lock-in. io_uring gains show up only at I/O-saturation; write-back cache architecture avoids that regime. Glommio revisit if we ever become syscall-bound.

3. **WAL placement: separate per-shard append-only segment files** (not RocksDB CF, not RocksDB internal WAL). Matches Kafka/Redpanda/Flink shape — sequential append, whole-segment ship to S3, natural TTL lifecycle per §10. RocksDB handles state durability via its own WAL; our event log is a separate concern. Recovery uses a stored LSN in state CF to replay event log deterministically. User note: "we need WAL because RocksDB WAL is slow; we use LRU hot cache for mutation + query and offload to RocksDB" — confirms separate-WAL-plus-LRU-write-back (already the design in §3.5 / §15).

4. **Branch semantics: disjoint-keyspace stream/table extensions, not trunk overlays.** Reframe: a branch is a NEW virtual stream or table that inherits initial state from a parent snapshot and layers its own data on top. Writes to a branch never touch the parent. Promote = graduate the branch to a first-class production stream/table (or cut consumers over); discard = drop the branch. **No merge-back-into-trunk, no conflict resolution** — because by construction writes never overlap. This simplifies §9 substantially; `@bv.branch_safe` annotations and per-operator merge resolvers are no longer needed. **Downstream edit: §9 needs rewrite; §22 "Branching" confidence rating updates.**

5. **Bucketing: uniform event-time buckets, default cap 64 per operator, per-operator opt-in override.** DGIM-style exponential time-buckets were researched and rejected — they break deterministic replay (bucket merges depend on arrival grouping, not event_time, so backfill produces different state than realtime), break Min/Max (coarse bucket loses sub-interval extreme), break Percentile (merging DDSketches across unequal bucket widths ≠ valid window quantile), and break HLL windowing without the Sliding-HLL variant (~4× memory). Uniform `bucket_id = floor(event_time / width)` is byte-identical under replay — the only shape compatible with §8 backfill guarantees. **Proposed Phase 1 additions (pending confirmation):** (a) EWMA / forward-decay operator family with watermark-driven decay, one f64 per `(entity, feature)`, for velocity/recency/heat features — ~60× memory win vs bucketed equivalent for the ops that tolerate weighted-sum semantics; (b) DDSketch inside each time bucket for the Percentile operator in Phase 7. **Downstream edit: §11 expands with EWMA family + DDSketch-for-percentile; §19 Phase 1 note about EWMA landing alongside uniform buckets.**

6. **Schema evolution: versioned packed rows + on-read migration.** `schema_version: u8` in row header. Server stores last N (default 8) schema defs. Write path unchanged — fixed layout per version, zero overhead. Read path migrates old rows to latest layout on load. Deprecation policy retires old versions after all in-flight rows have been compacted out. Upgrade path to Avro-style registry or FlatBuffers available if real schema churn justifies it; not needed for v2. **Downstream edit: §3.1 adds schema_version field; new subsection in §5 for schema defs storage in `cf:metadata`.**

7. **Cross-shard stream-stream join: auto-shuffle by default, `shard_local=True` opt-in for perf tuning.** Devex-first: `@bv.join(L, R, on="user_id")` just works regardless of L/R shard_keys. System cross-shard-SPSC-shuffles the non-matching side to the join shard. Power users declare `shard_local=True` to fail at register time if shard_keys don't match, pinning the join to zero-crossing performance. §2 non-goals already permits unoptimized cross-shard SSJ in v2. **Downstream edit: §11 SSJ subsection rewrite to reflect auto-shuffle-default.**

8. **HA scope: OSS = single server only. Replicas, HA failover, cross-region, replica-write-forwarding = commercial tier.** Major scope reduction: Phase 4 (Replicas + replication protocol) drops from OSS roadmap. OSS v2 has WAL for durability, RocksDB for state, cloud archive for DR — enough for single-server production use. Replication hooks stay in the WAL design for future commercial tier but no consumer ships in OSS. **Downstream edit: §14 moves behind a "Commercial Tier (not OSS)" marker; §19 phases renumber (9 phases, ~20 weeks); §24 success criteria drops replica deployment line; §16 failure modes drops replica-lag row for OSS.**

9. **Recovery: lazy warmup, snapshot every 10s, WAL retain 1h.** Server online in ~10-15s at 1M EPS (just RocksDB open + state-CF replay from last snapshot LSN). Hot cache warms lazily from RocksDB as queries land; P99 read latency degraded for ~5-10min during warmup window but server is fully available. Opt-in `--eager-warmup` flag for users who want steady P99 from first query. Redis-class perceived RTO without the full-preload wait. **Downstream edit: §15 rewrite of recovery sequence; §24 success criterion updated to "server accepting traffic in <15s at 1M EPS."**

10. **Branch lifecycle: 30d default TTL + warnings + extend API.** `branch.extend("30d")` trivially prolongs. Warnings at 25d / 28d / day-of via logs + metric + SDK notification on branch handle access. Explicit `branch.delete()` always available. 30d covers a typical experiment cycle. **Downstream edit: §9 rewrite (per decision 4) will include TTL section.**

### Follow-up confirmations needed

- **Decision 5, EWMA operator family and DDSketch-for-percentile in Phase 1/7 scope** — proposed as part of Option A; user's response locked the bucket-count knob explicitly but didn't address EWMA/DDSketch. Flag if either should be dropped or deferred.

### Downstream doc edits staged (not yet applied in this pass)

- §2 Non-Goals: add "HA, read replicas, cross-region replication — commercial tier"
- §3.1 Event: add `schema_version: u8` field
- New §3.8 "Devex-first operator design" principle section
- §9 Branching: full rewrite per decision 4 (disjoint keyspace, no merge conflicts, TTL)
- §11 Operators: expand with EWMA family, DDSketch-for-percentile, auto-shuffle SSJ
- §14 Read Replicas: move behind "Commercial Tier (not OSS)" marker
- §15 Durability + Recovery: lazy warmup rewrite
- §16 Failure Modes: drop replica rows from OSS section
- §19 Phase Roadmap: drop Phase 4 (Replicas), renumber, revise total duration
- §22 Key Design Decisions: update table per locked choices
- §24 Success Criteria: drop replica deployment criterion, update RTO line

---

## 18. NOT in Scope (v2)

Explicitly deferred:

1. **Distributed writes across multiple primaries** — v2 is single-primary. Multi-primary requires raft or similar; that's v3.
2. **Secondary indexes** — KV-native operators target specific entity keys. No ad-hoc query by non-key field without scan. Add later if needed.
3. **SQL / declarative query language** — operators declared via SDK. No SQL parser. v3.
4. **Full transactional semantics across operators** — operators are individually idempotent. No multi-op atomicity. If needed, use branching for validation.
5. **Time-travel queries** — can read historical buckets but no `AS OF TIMESTAMP` clause. Add if user demand.
6. **Built-in schema registry service** — schemas managed via SDK annotations. No separate registry server like Confluent. v3 if needed.
7. **Operator hot-reload** — operator definitions are compiled into server binary. New operator = new deployment. Branching provides validation path.
8. **Multi-tenant resource isolation** — one tenant per deployment. No per-tenant CPU/memory quotas.
9. **Cross-region replication** — read replicas can be geo-distributed but promotion requires operator action. No auto-failover across regions.
10. **Fjall or other storage backend pluggability** — RocksDB only. Less abstraction, less code, more focus.

---

## 19. Phase Roadmap

Suggested sequencing. Each phase produces a testable, shippable artifact.

| Phase | Scope | Duration | Shippable? |
|-------|-------|----------|------------|
| **0** | Scaffold: Rust workspace, RocksDB integration, tokio shard framework, typed row crate | 2 weeks | Skeleton process + "hello event" end-to-end |
| **1** | Core KV-native operators: Count/Sum/Avg/Min/Max/Last/First, windowed + unwindowed | 3 weeks | Single-node writes + reads work |
| **2** | Hot cache + eviction + write-back to RocksDB | 2 weeks | Bounded-memory operation with SSD overflow |
| **3** | WAL + group-commit + recovery | 2 weeks | Per-event durability + crash recovery |
| **4** | Read replicas + replication protocol | 2 weeks | 1 primary + N replicas deployment |
| **5** | Backfill primitive + S3 tiering | 2-3 weeks | Replay from cloud archive |
| **6** | Branch primitive + promote/discard | 2 weeks | Forked state + validation workflow |
| **7** | Sketch operators: HLL, TopK, Percentile | 2 weeks | Distinct counts + percentile queries |
| **8** | Stream-stream join | 2 weeks | Cross-stream joined features |
| **9** | Batched operators + SIMD | 1-2 weeks | Perf pass; hits 100K EPS/core target |
| **10** | Production hardening: metrics, ops tooling, docs | 2 weeks | Ready to deploy |

**Total: ~22-24 weeks (~6 months) to v2.0.** First 3 phases (~7 weeks) gives "usable prototype for internal testing." Phases 4-6 make it production-ready. 7-10 close the perf/feature gaps.

---

## 20. Test Plan (per-phase)

Every phase must ship with tests covering:

### Phase 1 (Core operators)
- Unit tests per operator: happy path + edge cases (empty window, zero delta, null input)
- Property tests: commutativity where expected (count/sum/min/max), idempotency
- Integration: single-event roundtrip + range-query read
- Perf regression: µs/event budget per operator
- **Estimated: ~500 tests for phase 1**

### Phase 2 (Cache + write-back)
- LRU eviction correctness (under memory pressure, verify oldest evicted first)
- Dirty flush on eviction (verify RocksDB has the right data)
- Cache coherence: write → evict → re-read matches pre-evict value
- Concurrent load: many writes across many entities, verify cache stays bounded
- **Estimated: ~100 integration tests**

### Phase 3 (WAL + durability)
- Crash-resistance: kill process mid-write, recover, verify state
- Group commit: verify multiple waiters share one fsync
- WAL replay correctness: post-restart state equals pre-crash
- WAL corruption: detect + reject bad segments
- **Estimated: ~50 tests (chaos + recovery scenarios)**

### Phase 4 (Replicas)
- Replica lag metrics accurate
- Monotonic read guarantees on replica
- min_lsn read token respected
- Network partition + reconnect without data loss
- **Estimated: ~30 distributed tests**

### Phase 5 (Backfill + tiering)
- Replay from S3 produces same state as live ingest of same events
- Deterministic mode fence correctness
- Parallel mode commutative op verification
- Tiering: event past TTL deleted locally + retrievable from S3
- **Estimated: ~50 tests**

### Phase 6 (Branching)
- Fork → branch writes don't affect trunk
- Promote → trunk sees branch changes atomically
- Discard → branch state gone, trunk unchanged
- Realtime events during branch lifetime go to correct target
- **Estimated: ~40 tests**

### Phase 7 (Sketches)
- HLL accuracy: within 2% of true distinct count on 1M-entry test
- TopK accuracy: top-10 matches ground truth on 100K events
- Percentile: p99 within 1% of ground truth
- Merge correctness: bucket rollover merges produce same result as fresh compute
- **Estimated: ~80 tests**

### Phase 8 (SSJ)
- Match correctness: all (L, R) pairs with same join_key within window emit
- Window expiry: old events drop, no stale matches
- Cross-shard dispatch correctness
- **Estimated: ~40 tests**

### Coverage diagram (sample for phase 1 CountOp)

```
[+] src/operators/count.rs
    │
    ├── CountOp::apply()
    │   ├── [★★★ TESTED] Increment in current bucket — count_test.rs:12
    │   ├── [★★  TESTED] First apply for entity creates state — count_test.rs:34
    │   ├── [GAP] Bucket boundary rollover — NEEDS TEST
    │   └── [GAP] Event with event_time in past (bucket before current) — NEEDS TEST
    │
    ├── CountOp::read()
    │   ├── [★★★ TESTED] Sum across 60 buckets — count_test.rs:56
    │   ├── [★★  TESTED] Empty state returns 0 — count_test.rs:78
    │   └── [GAP] Partial window (only 10 buckets filled) — NEEDS TEST
    │
    └── CountOp::apply_batch()
        ├── [GAP] Bulk increment amortization — NEEDS TEST
        └── [GAP] Mixed event_time across batch — NEEDS TEST

COVERAGE: 5/10 paths tested (50%) — need 5 more tests before Phase 1 ships.
```

---

## 21. Migration from v1

Two options:

### Option A: Hard cut (recommended)

- v2 is new binary, new wire protocol, new SDK versions
- v1 continues to run during transition
- Customer migrates by redeploying with v2 SDK + replaying events into v2
- No code shared

Pros: clean slate, no compatibility debt
Cons: customer has to replay (can be automated via backfill from v1's event log)

### Option B: Gradual replacement

- v2 implements v1 wire protocol temporarily
- v1 binary is stood down, v2 takes its place
- Internal operators migrated one at a time

Pros: customer doesn't notice
Cons: carries v1 tech debt into v2 (serde_json Value, O(n²) find, blob state)

**Recommendation: Option A.** The reason for the rebuild is to escape v1's architectural constraints. Importing v1's wire protocol imports those constraints.

---

## 22. Key Design Decisions (Summary Table)

| Decision | Choice | Confidence | Rationale |
|----------|--------|-----------|-----------|
| Storage | RocksDB | 8/10 | Battle-tested, column families, snapshots for branching. User explicitly requested. |
| Hot path state | In-memory AHashMap write-back | 10/10 | Measured 100× improvement over per-event RocksDB RMW |
| Operator state layout | KV-native, small values | 9/10 | Enables batched reads, amortized writes, natural tiering |
| Typed rows | End-to-end (no JSON Value) | 10/10 | Phase 59.6 proved 400× speedup potential |
| Materialized views | Lazy + sparse (LFU-cached) | 9/10 | Full materialization is O(entities × features) = too big |
| Thread model | tokio current_thread per core | 8/10 | Ergonomic, proven, cross-platform. Glommio alternative for pure Linux later |
| Durability | Group-commit WAL, 1-5ms window | 9/10 | Kafka/Postgres pattern; per-event durable at 1M EPS |
| Backfill | Typed event replay through same operator pipeline | 10/10 | Single code path, semantically identical to realtime |
| Branching | RocksDB column family overlay | 7/10 | Clean atomic promote via WriteBatch; conflict semantics still open |
| Replication | WAL streaming + local apply | 9/10 | Simpler than raft; manual failover for v2 |
| Cloud tier | S3/GCS/R2 parquet segments, lifecycle policies | 8/10 | Replayable, cheap, standard |
| Event TTL | Stream-declared, drives local eviction + cloud offload | 10/10 | Explicit per-stream control |

---

## 23. What Could Go Wrong (Honest Assessment)

1. **Branching complexity in production** — conflict resolution + merge semantics are subtle. May find edge cases where branch state diverges badly. Mitigation: strict opt-in, good observability on branch diffs, `@bv.branch_safe` annotations required per operator.

2. **Sketch rescan cost on cold entities** — if HLL/TopK bucket eviction triggers expensive rescans, cold-heavy workloads could stall. Mitigation: time-decay variants as fallback; measure carefully in phase 7.

3. **Read replica lag during high-write bursts** — replication can fall behind at 1M EPS writes. Mitigation: backpressure primary when replica lag exceeds threshold; alert operator.

4. **RocksDB compaction stalls** — at 64TB state with high write rate, compaction can consume significant I/O and stall writes. Mitigation: tune level/universal compaction; consider FIFO for event log; measure carefully.

5. **Cache thrashing on scan workloads** — one-hit-wonder queries can evict hot entries. Mitigation: W-TinyLFU admission filter after plain LRU proves insufficient (phase 2.5).

6. **Backfill during realtime can OOM** — large backfill + hot cache + materialized cache fights for RAM. Mitigation: backfill uses separate (smaller) cache budget; bucket writes bypass materialized.

7. **Schema evolution pain** — packed typed rows break on field add/remove. Mitigation: version in header, on-read migration, deprecation cycle.

8. **"Just one more operator" scope creep** — seven operators in scope, will be pressure to add dozens. Mitigation: hard NOT-in-scope list; push new operators to v2.1 after initial ship.

---

## 24. Success Criteria

v2 ships when:

1. Server-truth benchmark shows **≥ 100K EPS/core aggregate** on fraud-pipeline complex workload (measured via `server_processed_events` counter, not client-side).
2. Read latency p99 ≤ 5ms for materialized features (hot cache hit).
3. Read latency p99 ≤ 100ms for uncached features (RocksDB range scan).
4. Crash recovery in < 60s for 64GB cache + 1h WAL.
5. Backfill replays at ≥ 5M EPS aggregate from S3.
6. Branch promote completes in < 1s for branches with ≤ 1M dirty entries.
7. Single binary ≤ 200MB, zero external runtime dependencies beyond RocksDB.
8. All phase test suites passing (~900 tests estimated across 10 phases).

If any of these misses, v2 isn't shipping. No fuzzy "it's good enough" — the whole point of the rebuild is to hit these numbers.

---

## End of Design Doc
