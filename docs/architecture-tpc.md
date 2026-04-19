# Thread-Per-Core Architecture

This document explains the Thread-Per-Core (TPC) + full key-shard architecture introduced
in Beava v1.2. Audience: new contributors and operators. Read this document to understand
why the system is built the way it is, how events flow through shards, and what to do
when scaling or resharding.

Related reading:
- [Operations](operations.md) — env vars, sizing guidance, and hot-shard diagnosis runbook
- [Architecture](architecture.md) — storage layout, WAL format, and fork-replica design

---

## Motivation

Pre-v1.2, all entity state lived in a single `DashMap<Entity, Row>` shared across all
tokio worker threads. `DashMap` internally partitions its own buckets with RwLocks; under
sustained write pressure at N>4 cores, multiple threads contend on the same internal
lock shards, producing cache-line bouncing on x86 and aarch64 alike.

Measured throughput ceiling on a 10-core Apple M4: ~300–350K EPS (TCP). Target throughput:
1.5M EPS per 16-core production box. The gap is not addressable with more cores — each
additional core returns diminishing throughput because contention and cache-line bouncing
grow superlinearly past ~8 threads.

**The fix:** give each shard thread exclusive ownership of a disjoint key range. No
cross-thread state access on the hot path. No locks. No DashMap. Each core processes its
own slice of the key space from input to feature output.

This is the canonical pattern used by ScyllaDB (Seastar), Redpanda, and Apache Iggy
(which measured P99 −60%, P9999 −57%, and +18% throughput after migrating from tokio
work-stealing to thread-per-core in Feb 2026).

---

## Shard Model

### Target Architecture

```
TCP listener (tokio)                 HTTP listener (axum/tokio)
        |                                      |
        +-- parse --+ shard_hint(key)          +-- parse --+ shard_hint(key)
                    |                                       |
            +-------+----------------+              +-------+---------+
            v                        v              v                 v
      shard 0 queue            shard 1 queue    shard 2 queue    shard N-1 queue
      (SPSC channel or         ...
       ring buffer)
            |
+---------- shard 0 (pinned OS thread, current_thread tokio runtime) ----------+
|   owns keys where hash(key) mod N == 0                                      |
|   - per-shard state (no DashMap — plain HashMap, single-threaded access)    |
|   - per-shard event log (append-only, single writer)                        |
|   - per-shard watermark tracker                                             |
|   - per-shard dirty set                                                     |
|   - per-shard HTTP response channel back to listener                        |
+------------------------------------------------------------------------------+
```

### Shard Struct

Each shard owns a `Shard` struct containing:
- **AHashMap** — entity state. Single-threaded; no locks required.
- **WatermarkState** — per-stream watermark tracking for this shard's key range.
- **EventLog handle** — append-only log at `data/shard-N/streams/{stream_name}/log.bin`.
- **Per-shard metrics** — reactor utilization, inbox depth, keys owned, events dropped.

### How Many Shards

`BEAVA_SHARDS` defaults to `num_cpus::get_physical()` in release builds and `1` in debug
builds. Release default gives each physical core its own shard. Logical (hyperthreaded)
cores are excluded — two HT siblings share L1/L2 and running a shard on each provides no
benefit for Beava's memory-bound workload.

Override at startup: `BEAVA_SHARDS=4 tally serve`. The override always wins. Setting
`BEAVA_SHARDS=1` produces behavior identical to the pre-v1.2 single-writer architecture.

Events route by `hash(event.key) mod N`. The hash function is **ahash**, which is
deterministic across process restarts for identical inputs — same key always lands on
the same shard given the same `N`.

---

## Routing

### shard_hint Flow

Every event carries a `shard_hint` computed at the ingest boundary. The hint is a `u32`
index into the shard array. Sources set it as follows:

| Source | shard_hint value |
|--------|-----------------|
| HTTP push | `ahash(stream_key) mod N` |
| TCP push | `ahash(stream_key) mod N` |
| Kafka (future) | `kafka_partition` (preserves producer's partition) |
| Replica log (fork) | upstream `shard_hint` in `OP_LOG_FETCH` metadata (fast-path hint only) |

TCP and HTTP parsers attach the hint before any allocation or copy. The shard router reads
the hint and dispatches the event over the target shard's bounded SPSC channel. The
listener thread does not block; it writes to the channel and returns.

**Optimization: skip rehash.** When a fork replica's `upstream_N == downstream_N` and
key-space partitions match, the upstream's `shard_hint` can be forwarded without
recomputing `ahash`. This is a performance hint, not a correctness constraint — the
replica always verifies routing on every ingest.

### Fallback: Undeclared shard_key

If a stream's `shard_key=` field is not declared and `N > 1`, events fall back to shard 0.
The engine emits a `ShardKeyMissingWarning` log entry (once per stream per restart). This
keeps single-key-field streams working without explicit declaration but produces uneven
load distribution when N>1.

For balanced shard distribution, always declare `shard_key=` explicitly:

```python
@bv.stream(shard_key="user_id")
class Transactions:
    user_id: str
    amount: float
    _event_time: int
```

If a tuple shard key field is absent on an inbound event, the event is **rejected at
ingest** (HTTP 400 / TCP error) and `beava_events_dropped_total{reason="shard_key_missing"}`
is incremented. The shard thread never receives a malformed event.

### Backpressure

The SPSC channel between listener and shard is **bounded** (default 64K events,
configurable via `BEAVA_SHARD_INBOX_SIZE`). When a hot shard's inbox fills:

1. The listener drops the event at the boundary.
2. `beava_shard_inbox_full_total{shard="N"}` is incremented.
3. The client receives HTTP 503 (HTTP push) or a TCP error response.

The listener thread is never blocked. Retrying at the client side is the recovery path.

---

## Joins

### Co-Location Requirement

Join operators require both streams to declare the same `shard_key=`. When a join event
for key K arrives, it must route to the same shard on both streams — possible only if
`ahash(K) mod N` is identical, which is only guaranteed when both streams use the same
declared key.

Mismatched shard keys produce a `JoinShardKeyMismatch` error at stream registration time.
The error is **fatal and actionable**: it names both stream names and the conflicting keys,
and suggests the fix.

```
JoinShardKeyMismatch: stream "transactions" uses shard_key="user_id"
  but joined stream "accounts" uses shard_key="account_id"
  Fix: align shard_key declarations before registering the join.
```

Registration is rejected until the mismatch is resolved. No data is ingested into a
misconfigured join.

### Why Co-Location is Required

A join between streams A and B on key K needs the A-side state for K and the B-side state
for K to reside on the same shard thread. If A routes K to shard 3 and B routes K to
shard 7, there is no cross-thread mechanism to perform the join without a lock —
eliminating the core benefit of the TPC architecture. Co-location is the correct trade-off
for v1.2; broadcast joins and re-shard-per-join are deferred to v1.3+.

---

## Recovery

### Parallel Per-Shard Recovery

On boot, Beava spawns N recovery tasks — one per shard — each pinned to its shard's
thread. Each task independently replays its own per-shard event log at:

```
data/shard-N/streams/{stream_name}/log.bin
```

Recovery tasks run in parallel. No cross-shard coordination is needed during replay
because each shard owns a disjoint key range. The main thread blocks on a **boot barrier**
that tracks a "recovered" sub-state for every shard.

### /ready Gate

`GET /ready` returns **503** until every shard passes its "recovered" state. `GET /health`
returns **200** from process start (liveness = process is alive). This distinction matters
for Kubernetes probes:

```yaml
livenessProbe:
  httpGet:
    path: /health
readinessProbe:
  httpGet:
    path: /ready
```

Do not route traffic to a pod that returns 503 from `/ready`. The pod is replaying its
event log; forwarding writes before replay completes can create ordering anomalies.

### Recovery Performance

Parallel recovery scales with core count. Baseline comparison:

| Shards | Data size | Recovery time |
|--------|-----------|---------------|
| N=1 (pre-v1.2) | 4.7 GB | 7.0 s |
| N=8 | 4.7 GB | ~1.5 s |

The 4.7× improvement is approximately linear with shard count — each shard reads 1/N of
the total data independently.

### Boot Guard

If the on-disk snapshot records `shard_count=N` but the server is started with
`BEAVA_SHARDS=K` where K≠N, **the server refuses to boot**:

```
snapshot shard_count=4 but BEAVA_SHARDS=8
  Run 'tally reshard --from 4 --to 8 --data-dir /var/lib/beava' then restart.
```

This prevents silent data loss (booting with empty shards for the missing key ranges).
Auto-reshard on boot is not performed — migrations must be explicit and operator-auditable.

---

## Reshard Workflow

### When to Reshard

Reshard when:
- Changing `BEAVA_SHARDS` on an existing data directory.
- Moving from a single-shard (N=1) deployment to multi-shard.
- A persistent hot-shard condition cannot be resolved by tuning `BEAVA_HOT_SHARD_THRESHOLD`
  (see [Operations § Shard Sizing & Hot-Shard Diagnosis](operations.md#shard-sizing--hot-shard-diagnosis)).

### CLI Reference

```bash
tally reshard \
  --from N \          # current shard count (must match snapshot shard_count)
  --to K \            # target shard count
  --data-dir PATH \   # source data directory (must not be locked by a running server)
  --output PATH \     # destination directory for resharded data
  [--replace]         # atomically swap output into data-dir on completion
```

### Steps

1. **Stop the server.** The reshard tool refuses to run against a data directory locked by
   a running server. Downtime equals tool runtime.
2. **Run the tool.** It acquires the data-dir lock, reads the source snapshot and per-shard
   logs, replays every entry through `ahash(key) mod K` to route to the new K-shard layout,
   and writes a v8 snapshot with `shard_count=K` to the output directory.
3. **With `--replace`:** the tool atomically swaps the output directory into place via
   `rename(2)`. The original data directory is preserved with a `.bak` suffix unless
   already backed up.
4. **Restart** with `BEAVA_SHARDS=K`. Recovery runs normally against the new layout.

Downtime is bounded by the tool's runtime (primarily I/O throughput on the NVMe). There
is no in-process or online reshard in v1.2; online reshard is deferred to v1.3+.

For sizing guidance and deciding when to reshard, see
[Operations § Shard Sizing & Hot-Shard Diagnosis](operations.md#shard-sizing--hot-shard-diagnosis).

---

## Fork/Replica

### Re-Hash on Ingest

A fork replica is an independent Beava process that subscribes to an upstream's event log
via `OP_LOG_FETCH`. The replica's shard count (`downstream_N`) may differ from the
upstream's (`upstream_N`).

On every ingest, the replica computes:

```
downstream_shard = ahash(event.key) mod downstream_N
```

The upstream's `shard_hint` in the `OP_LOG_FETCH` metadata is a **fast-path hint only**.
When `upstream_N == downstream_N` and the key-space partition matches, the replica can
skip recomputing the hash. When they differ (or when the replica cannot verify the match),
it rehashes unconditionally. This is always correct — the upstream hint is never
authoritative at the replica.

This design means: a replica can change its shard count independently of the upstream,
including across upstream rolling restarts. No `--reshard-from` flag is required; rehashing
on ingest is the default and only behavior.

### LSN-Based Dedup

Every log entry carries a monotonic **LSN** (Log Sequence Number):

```
u64: [ upstream_shard_id: 8 bits | stream_ord: 16 bits | seq: 40 bits ]
```

The 40-bit `seq` per `(stream, upstream_shard)` pair supports approximately 1 trillion
events — sufficient for any realistic stream lifetime.

The replica tracks `max_lsn_seen(stream, upstream_shard)` persistently in snapshot v8
metadata. On reconnect (e.g., after an upstream rolling restart), the replica discards
any event whose LSN is ≤ `max_lsn_seen` for the corresponding `(stream, upstream_shard)`
pair. This closes the double-emit window that previously existed during rolling restarts
of upstream shards.

Snapshot v8 includes `replica_lsn_map: HashMap<(StreamName, UpstreamShardId), u64>` with
`#[serde(default)]`, so pre-v8 snapshots load as an empty map — a fresh replica starts
with no dedup state (the standard v1.0-launch upgrade path).

---

## Ship-Gate Rationale

Three criteria must pass before the TPC branch merges to main. These are production health
baselines after v1.2 ships.

### Criterion 1: N=1 Within −5% of v1.1 Baseline

All cells of the 9-cell benchmark matrix must be within −5% of their baseline values at
`BEAVA_SHARDS=1`.

**Why:** Operators upgrading from v1.1 to v1.2 without changing their shard count must see
no performance regression. The TPC plumbing (routing layer, per-shard structs, boot
barrier) adds a small constant overhead. −5% is the allowed budget for that overhead. If
any cell exceeds −5% regression, the overhead must be profiled and reduced before shipping.

### Criterion 2: complex-c8-x8 ≥ 3× at N=CPU_COUNT

The `complex-c8-x8` benchmark cell (8 concurrent clients, 8-stream complex pipeline)
must achieve at least 3× the baseline throughput when running at `BEAVA_SHARDS=CPU_COUNT`.

**Why:** This validates that the TPC architecture delivers its core promise — multi-core
scaling. 3× on a typical 8-core prod box is the minimum threshold for the architectural
bet to be worthwhile. If the measurement falls below 3×, the bottleneck must be identified
(routing overhead, SPSC contention, cross-shard queries) and resolved.

### Criterion 3: pareto-c8-x8 cross_shard_fraction < 40%

The Pareto (Zipf 80/20) workload benchmark must report `shard_probe` `cross_shard_fraction`
below 40% on the `pareto-c8-x8` cell.

**Why:** Skewed key distributions are the adversarial case for shard routing. If 20% of
keys receive 80% of events and those hot keys all hash to the same shard, that shard
becomes the bottleneck regardless of core count. Cross-shard fraction below 40% confirms
the routing is architecturally sound for real-world workloads. Above 40% indicates that
the shard key strategy needs adjustment — either through a different `shard_key=`
declaration or through operator-level key salting.
