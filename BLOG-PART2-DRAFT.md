# Two microbenches that lied

_Learnings from building Beava, part 2. The first post covered why there is an OSS lane open for a Python-decorator streaming engine. This post is about the engineering mistakes we made assuming the microbench was telling the truth._

---

I have two benchmarks from the Beava code base that disagree with each other by three orders of magnitude. They are measuring the same code. Both were written carefully, both ran cleanly, both produced clean Criterion output. One of them said the project was catastrophically broken. The other said it was fine.

The second one was right.

That gap is the whole post. It turns out that two pieces of conventional wisdom in the Rust systems community are both wrong in ways that matter. Both were found by running a microbench, getting a scary number, and only later running the integration bench and getting a boring number.

## Finding one: DashMap's uncontended lock-acquisition is the bottleneck, not contention

The received wisdom: "DashMap is fine until you have contention. If you have contention, shard harder."

Here is what we actually measured.

Phase 52 baseline on a 10-core M4, running a complex 9-cell matrix workload at N=1 (one tokio worker). Keys were distributed across all 16 internal DashMap lock shards. Zero contention by construction: one thread, 16 shards, 1/16th hit rate on any given shard lock. There was literally nobody for the lock to contend with.

`pprof` capture:

```
DashMap::_entry                60.8% of CPU samples
PipelineEngine::push_internal  66.2% inclusive
```

60% of CPU, on a workload with no contention. What is it doing?

Read the DashMap source. Every call to `_entry(key)` does this:

1. Hash the key.
2. Modulo the hash to pick an internal shard (0-15).
3. Acquire the `RwLock` on that shard. Even if nobody is contending, this is an atomic operation that fetches a cache line, performs a CAS, establishes happens-before ordering.
4. Fetch the internal `HashMap` for that shard.
5. Hash the key again (the inner HashMap hashes it).
6. Walk the bucket chain.
7. Return an entry handle whose Drop releases the lock.

Steps 3 and 4 are the interesting ones. The uncontended lock path is not free. The CAS, the cache-line fetch, the memory ordering, the deref through the `Arc<RwLock<HashMap<K, V>>>` indirection. None of it is expensive in isolation. At 300K events per second, the per-event budget is around 3 microseconds. Losing 1 microsecond of it to "take a lock nobody wants" is a fifth of the budget, gone.

This is not a DashMap bug. DashMap is doing exactly what its docs say. The bug is in the assumption that people make when they reach for it: _the cost is only paid when someone else is trying to hold the lock._ Not true. The cost is paid every time you take the lock, contended or not. On a single hot ingest thread that does 300K ops per second, you are paying it 300K times per second.

The fix is not "make the lock faster" or "shard more aggressively." The fix is to make the hot path not take a lock. Thread-per-core, one pinned thread per shard, state owned by exactly one thread, plain `HashMap`, no concurrent primitives. ScyllaDB did this. Redpanda did this. Apache Iggy did this ([their migration post](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/) from February 2026 is the canonical writeup).

When we made that change, the `DashMap::_entry` line in the profile disappeared. It is gone, because there is no DashMap. The new `Shard` struct holds `AHashMap<Key, EntityState>` directly. Events arrive on a shard's SPSC queue, the shard thread dequeues and processes, the state lives on that thread, no other thread touches it. At N=8 on Linux reference hardware the engine does 918K events per second. At N=16 we project 1.5M-2.5M.

The lesson is not "use thread-per-core." Thread-per-core is old news; ScyllaDB has been doing it for a decade. The lesson is that **you do not reach for thread-per-core because you have lock contention. You reach for it because uncontended lock-acquisition is already a tax you are paying whether you know it or not.** The profile shows you the tax. You just have to believe the profile more than you believe the README.

## Finding two: LSM-tree durability does not cost 3-5x. It costs ~15%.

The received wisdom: "LSM-tree backends like fjall or RocksDB are 3-5x slower than in-memory HashMaps because disk."

Here is what we actually measured.

Phase 53 of Beava put fjall under the per-shard state. Previously state was an `AHashMap` in RAM, protected by being owned by one pinned thread. Fjall is a durable LSM-tree: write goes to an in-memory journal, writes get flushed to SSTables on disk, reads hit a block cache, crash recovery replays the journal. Well-maintained Rust crate, modern design ([fjall-rs.github.io](https://fjall-rs.github.io/) has their own deep dives). The promise: durable-by-default state, crash-safe without snapshot replay, unbounded size.

Before committing to the swap, I wrote a Phase 53-01 spike microbenchmark. `benches/fjall_spike.rs`. Criterion, sample-size 30, measurement time 5 seconds. 100 read-modify-write ops on a pre-populated 1000-key partition, identical payload shape to production. Fjall ran with `fsync_ms(None)` (background fsync disabled) to be generous, i.e., the bench was already _optimistic_ vs the production `fsync_ms(5)` target.

The spike verdict:

| Metric               | AHashMap   | fjall      | Regression  |
|----------------------|-----------|-----------|-------------|
| time per 100 ops     | 94 µs     | 233 ms    | +246,648%   |
| throughput (Melem/s) | 1.06      | 0.000429  | -99.96%     |

2,468x slower. The project had a pre-agreed `-25%` microbench budget. The spike missed the budget by a factor of 9,000. The STOP gate fired. The plan was to halt Phase 53 and renegotiate scope.

I overrode the gate.

This was uncomfortable. Writing "the microbench failed 2,468x, ship it anyway" in a commit message is not a thing I love doing. But here is the reasoning.

Read the fjall source. `PartitionHandle::insert` at `fjall-2.11.2/src/partition/mod.rs:980`:

1. Take the journal writer mutex.
2. Write the KV pair to the journal file. This is a `write()` syscall even in `PersistMode::Buffer` (no fsync, but still crosses the kernel boundary, still lands in the filesystem cache).
3. Insert to memtable.
4. Check memtable overflow; if overflowed, schedule a flush to SSTable.

Every `insert`. Per call. The kernel boundary is the expensive part on macOS APFS; ~23 µs per op, vs AHashMap's 9 ns. The 2,500x gap is consistent with "100 ops, each one pays ~23 µs to cross into the kernel for a buffered write."

Here is the question the spike did not answer: **in production, does the hot path actually call `PartitionHandle::insert` per event?**

The answer is no. In the production design, every shard has a write-through AHashMap cache sitting in front of its fjall partition. The event arrives, hits the cache, the cache updates the entity state in memory, returns. Fjall only sees two classes of writes:

1. **Eviction writes:** when a cold key gets pushed out of the cache because a hotter key came in.
2. **Background flush:** a timer flushes dirty cache entries to fjall in the background, batched.

On a realistic streaming workload with any kind of key locality (which all of them have: the hot 1% of users generate 30% of the events), the cache absorbs the hot set. Fjall sees something like 1 write per thousand events, not 1 per event.

The spike was measuring the path you take if you have no cache. We have a cache. The spike was measuring the wrong thing.

The integration bench is the one that answered the real question. Phase 53 Plan 06 ship-gate: 9-cell Criterion matrix plus Pareto workload, fjall-backed build vs `state-inmem` feature build, per-shard, N=CPU_COUNT. Regression budget on the slow cell: `-15%` vs the committed v1.0-launch baseline.

Result: inside budget. Durable LSM-tree state costs roughly 15% throughput versus pure-RAM AHashMap on the realistic production workload. Not 3-5x. Not 2,468x. Fifteen percent. That is the price of durability, and it is much less than the internet will tell you.

(Footnote: the _first_ attempt at this integration bench also lied, in a different way. pprof showed both the fjall and in-memory builds spending ~65% of samples in `DashMap::_entry`. We had routed ingest through a legacy `PipelineEngine::push_internal` path at N=1 that still wrote to DashMap regardless of the configured state backend. Both builds were measuring DashMap-vs-DashMap noise, not fjall-vs-AHashMap. That bug has its own phase; retiring the legacy path is Phase 54. The lesson there is the same lesson as everywhere else in this post: the thing you are measuring is not necessarily the thing you think you are measuring.)

## Why "buffer mode" is the load-bearing choice

Here is the config stack that makes durability cheap:

1. **`PersistMode::Buffer`** on every fjall write. Not `PersistMode::Sync`. The buffered write lands in the filesystem cache and returns; it does not block on a platter. Durability still holds because the journal is a sequential append, and an OS crash at worst loses the last `fsync_ms` window (default 5 ms).
2. **Background fsync on a timer.** `BEAVA_FJALL_FSYNC_MS=5`. Every 5 ms the journal flushes. The hot path never waits.
3. **Write-through AHashMap cache per shard.** The hot set lives in the cache. Cache evicts to fjall on capacity pressure. Fjall is the cold store, not the hot store.
4. **Per-shard partition.** No cross-shard state means no cross-shard fjall coordination. Each shard is a single-writer fjall partition with no locks.

Take any one of those away and the numbers get worse fast. Synchronous fsync on every write costs milliseconds. No cache costs the 2,468x from the spike. Cross-shard partitions cost lock acquisition. The 15% number depends on all four layers being in place.

This is also why the microbench lied. The spike tested _none_ of these layers. It called `PartitionHandle::insert` directly on a raw partition, per op, with no cache, no batching, no eviction logic. It measured the primitive, not the system.

## What this means if you are building something similar

Both findings share a shape. Call it the microbench lie.

A microbench measures the cost of the primitive you ask it to measure. It does not know whether production calls that primitive per event or per thousand events. It does not know whether a lock will be contended or uncontended. It does not know whether a write will go through a cache or straight to disk.

An integration bench measures the path production actually takes. It is harder to set up, harder to interpret, slower to run. It is also the only one that gives you the number you actually want to know.

If I had trusted the fjall spike, Phase 53 would have been cancelled. Beava would not have durable state. The internet would be saying "LSM is 3-5x slower" and I would be nodding along. We would have a worse engine.

If I had trusted the DashMap README, Beava would still have DashMap. We would have assumed the 65% profile hit was a real workload issue, not a default-config tax, and we would have tried to fix it by sharding harder. We would have a worse engine.

Three practical takeaways:

1. **Write the integration bench first.** Microbenches are a magnifying glass; they amplify the part of the path you can instrument. They do not tell you whether that part matters. Measure end-to-end first, then decide which parts to zoom in on.
2. **Suspect uncontended locks.** Any "concurrent" primitive on your hot path is paying for safety you may not need. Check the profile. If the lock shows up at 60% of samples on an uncontended workload, the lock itself is the cost.
3. **Defaults lie.** "In-memory is always faster than disk." "DashMap is free until contention." Both are true in some regime. Neither is true in yours until you measure.

The engine that came out of this: per-shard pinned thread, plain `AHashMap<Key, EntityState>` as the hot path, per-shard fjall partition as the durable substrate, write-through cache between them, background fsync. 315K events per second at N=1 on an M4 laptop. 918K at N=8 on Linux reference hardware. State is durable on write, SIGKILL-safe, and the project does not need a snapshot-replay story on startup because the fjall journal does it for us.

The whole thing is one binary. No JVM. No cluster. No Kafka. No sidecar.

That is the engine. The next post is about what it unlocks: a world where the data scientist and the agent both submit compute plans against live state, without an ML platform team in between.

---

**Sources and context**

- Phase 52 DashMap baseline pprof: `.planning/phases/53-fjall-state-backend/53-VERIFICATION.md`
- Phase 53 spike: `benches/fjall_spike.rs`, `.planning/phases/53-fjall-state-backend/53-01-SPIKE-RESULTS.md`
- Fjall source: `~/.cargo/registry/src/.../fjall-2.11.2/src/partition/mod.rs:980`
- TPC architecture design: `.planning/arch/TPC-SHARD-DESIGN.md`
- Apache Iggy TPC migration post: [iggy.apache.org/blogs/2026/02/27](https://iggy.apache.org/blogs/2026/02/27/thread-per-core-io_uring/)
- Fjall project: [fjall-rs.github.io](https://fjall-rs.github.io/)

Beava is [beava.dev](https://beava.dev). The code is at the repo formerly known as tally.

---

_DRAFT: ~2650 words. Need to add: (1) an ASCII diagram of the cache+fjall layering, (2) actual links to the repo paths, (3) consider naming Iggy/ScyllaDB as honest prior art earlier. Voice check: this is terser than Part 1 will be. Intentional: the audience for this post is Rust systems engineers, they want the numbers and the receipts._
