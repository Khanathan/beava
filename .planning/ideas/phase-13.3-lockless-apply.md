# Phase 13.3 (research + plan): lockless apply via commutativity + keyed sharding

> **Status**: design plan, not yet a phase. Captured 2026-04-24 at 06:30 EDT after Phase 13.1/13.2 performance analysis. Supersedes Phase 13.2's "batch-before-global-lock" approach. Research-first plan for next session to pick up.

---

## Problem statement

Current `/push` throughput on merged v2/greenfield (post Phase 13.1 fsync fix) plateaus at **~17,000 EPS at parallel=64** on Apple-M4 / Darwin. Phase 13.1 profiling identified the new bottleneck: `state_tables.lock()` in `crates/beava-server/src/push.rs:303` serializes all in-flight pushes through a `parking_lot::Mutex`.

Per-event decomposition (see `.planning/throughput-baselines.md` § "POST-MERGE quiescent rerun" + investigation notes):

| Stage | Cost at parallel=64 |
|-------|---------------------|
| HTTP/JSON parse | ~10 µs |
| WAL append (Periodic mode) | ~2-5 µs |
| **mutex acquisition under contention** | **~10-15 µs** |
| **mutex handoff (park/unpark syscalls)** | **~3-5 µs** |
| Entity_key parse + hashmap lookup | ~200 ns |
| **Apply compute** (1 cheap op) | **~50 ns** |
| Response build + JSON serialize | ~1-2 µs |
| Various tokio `.await` poll overhead | ~5-10 µs |
| **Total** | **~30-55 µs → 17k EPS** |

**Actual apply compute is <1% of the per-event budget.** The other 99% is waiting on the mutex, syscalls around it, and serialization layers. The mutex is the bottleneck and the mutex contents (apply) is basically free.

## The insight: we don't need a lock at all

Two independent observations:

### Observation 1: single-thread tokio doesn't need a mutex

The production server runs `tokio::runtime::Builder::new_current_thread()` (see `crates/beava-server/src/main.rs:37`). On a current-thread runtime, only one future polls at a time. Two futures cannot actually execute concurrently on CPU. A `parking_lot::Mutex` ends up doing futex syscalls to coordinate tasks that are already coordinated by tokio's scheduler — pure overhead.

### Observation 2: most aggregations are commutative

For any commutative operator, order of arrival doesn't matter. We can apply events in any order (or concurrently, if we had concurrency) and produce the same result. The lock is not protecting against a correctness issue — it's protecting against a nonexistent race.

The only operators that actually need ordering are those that read prior state (lag, streak, first/last, recency, joins). Even those only need **per-entity** ordering, not global — events for `user-A` and `user-B` have no causal relationship.

### Combined: the lock is serving no purpose v0 can't get rid of

- Single-thread: lock adds park/unpark syscalls for no real contention protection
- Commutative ops: lock prevents a race that wouldn't matter anyway
- Order-sensitive ops: need per-key order, which is already guaranteed by WAL LSN ordering within a key

So: replace the global lock with one of three architectures that exploits these properties.

---

## Three architectural options

### Option A — Atomic / lockless per-slot (for commutative ops only)

Each per-entity state slot uses atomic types. Apply is a single atomic RMW. No lock anywhere.

| Op | State atomic primitive | Per-event cost |
|----|----------------------|----------------|
| count | `AtomicU64::fetch_add(1)` | ~5 ns |
| sum (f64) | CAS loop on `AtomicU64` bitcast | ~10 ns |
| min | `fetch_min` / CAS loop | ~10 ns |
| max | `fetch_max` / CAS loop | ~10 ns |
| avg | `(AtomicU64 count, AtomicU64 sum_bits)` — two separate atomics | ~20 ns |
| variance/stddev | Welford: three atomics, Relaxed ordering → slight accuracy drift | ~30 ns |
| ratio | Two count atomics (num, denom) | ~10 ns |
| count_distinct (HLL) | Per-register `AtomicU8::fetch_max(new_rho)` | ~30 ns |
| bloom_member | `AtomicU64::fetch_or(bit)` on each of K registers | ~50 ns |

**Pro**: ns-cost apply. True concurrency if we ever go multi-thread. No lock.
**Con**: doesn't work for order-sensitive ops (streak, lag, sketches w/ collapse, joins).
**Accuracy**: variance/stddev gain tiny numerical drift under out-of-order updates. Fraud/ad-tech: acceptable. Billing: not acceptable (but Beava isn't a billing system).

### Option B — Per-key (sharded) RefCell ownership

Hash each entity key into N shards. Each shard owns a `RefCell<StateSlot>`. Apply for an entity always goes to the same shard → serial within shard, lockless because only one task accesses each shard.

```rust
struct ShardedState {
    shards: [RefCell<HashMap<EntityKey, StateSlot>>; 64],
}

impl ShardedState {
    fn apply(&self, entity_key: &EntityKey, event: &Event) {
        let shard_idx = hash(entity_key) % 64;
        let mut shard = self.shards[shard_idx].borrow_mut();  // <- single-thread, no lock
        let slot = shard.entry(entity_key.clone()).or_default();
        op.update(slot, event);
    }
}
```

Wait — `RefCell<T>` is `!Sync`, so it can't live in a shared `Arc<ShardedState>` across awaits without unsafe. Need either:
1. `thread_local!` state (each OS thread has its own), plus thread-affinity router (Level 2 sharding from `parallelism-levels.md`)
2. `Mutex<HashMap>` per shard (lock contention reduced 64×, still has futex cost)
3. Actor-per-shard (N actor tasks own N shards, route events by hash)

Actor-per-shard is cleanest on current_thread tokio: N tokio tasks each own 1 shard's state via move semantics. No lock anywhere. Router hashes event → routes to shard actor task → actor applies serially.

**Pro**: works for all operators including order-sensitive (per-key order preserved within actor).
**Con**: adds ~200 ns channel-send cost per event (vs atomic fetch_add).
**Accuracy**: bit-exact; no drift.

### Option C — Single actor with coalesce + drain-many receive

One actor task owns all state. Push tasks send events via `mpsc`. Actor drains N events per poll and applies in a tight loop. No lock (actor is the unique writer).

```rust
enum StateRequest {
    Apply { event: Event, ack: oneshot::Sender<Lsn> },
    Get { query: Query, reply: oneshot::Sender<Value> },
}

async fn state_actor(mut rx: mpsc::Receiver<StateRequest>, mut state: StateTables) {
    let mut batch = Vec::with_capacity(4096);
    loop {
        rx.recv_many(&mut batch, 4096).await;
        for req in batch.drain(..) {
            match req {
                Apply { event, ack } => {
                    let lsn = apply_event(&mut state, event);
                    let _ = ack.send(lsn);
                }
                Get { query, reply } => {
                    let v = query_state(&state, query);
                    let _ = reply.send(v);
                }
            }
        }
    }
}
```

Per-event cost on current_thread tokio:
- `mpsc::send` from push task: ~100 ns
- `recv_many` in actor (amortized over batch of 4096): ~0.02 ns / event dequeue
- actor apply: ~50-300 ns depending on op
- oneshot ACK roundtrip: ~100 ns
- **Total: ~300-500 ns per event**

**Pro**: works for all operators. Simpler than per-shard. Batching happens naturally. No lock.
**Con**: single actor is the new bottleneck (but it's doing ONLY apply compute + channel ops, which is ~300ns/event, = ~3M EPS per actor core).
**Accuracy**: bit-exact.

---

## Recommended path: Option C, then selectively Option A for hot ops

Phase 13.3-01 (week 1): **single-actor refactor** (Option C). Replaces `state_tables.lock()` with message-passing to an actor. ~500 LoC refactor.

Phase 13.3-02 (optional, if C isn't enough): **atomic-slot specialization for the 8 commutative ops** (Option A). Per-op micro-optimization. Drops the ops' per-event cost from ~50 ns (inside actor) to ~5-10 ns (atomic on shared slot, actor not involved). Lifts throughput another 3-5× for count/sum/avg-dominated pipelines.

Phase 13.3-03 (long-term, if needed): **shard the actor** (Option B). N actors own N shards, route by `hash(entity_key) % N`. Goes from single-actor ~3M EPS to N-actor ~N×3M EPS. Only needed if Option C's single-actor throughput is inadequate — i.e., if v0 targets > 3M EPS per PROCESS, not just per CORE.

---

## Expected numbers, stacked

**Baseline** (today, post-Phase 13.1 fsync fix, locked apply path, single-thread tokio, parallel=64 saturating):

```
17,000 EPS    Apple-M4 / Darwin / HTTP / small shape
~50 µs        per-event cost (99% mutex + scheduler, 1% apply)
```

**After 13.3-01 (Option C, single actor)**:

```
500k-1M EPS     per core, macOS (HTTP parse still dominant at ~10µs per request)
~1-2 µs         per-event cost on HTTP path
~300-500 ns     per-event cost on TCP path (no JSON parse)
```

Projection: HTTP parse is now the bottleneck (~10 µs / parse). TCP avoids this — we'd see the first real TCP throughput lift here (5-10× over HTTP).

**After 13.3-01 + 13.3-02 (Option C + atomic commutative ops)**:

```
3-5M EPS        per core, macOS + TCP (atomic apply, no actor indirection for commutative ops)
~200-400 ns     per-event cost
```

Meets the v0 Phase 13 ship target of ≥3M EPS/core.

**After 13.3-01 + 13.3-02 + 13.3-03 (N-actor sharding)**:

```
N × 3-5M EPS     per-process (linear scaling across cores)
24-40M EPS       on 8-core box
```

Beyond v0 scope (the ship target is per-core, not per-process), but documents the ceiling.

**Linux projection**: multiply each row by 2-3× due to `fdatasync` being 150× faster than macOS `F_FULLSYNC` (fsync no longer blocks, but other syscalls are faster too). So Linux + Option C + TCP = ~10M+ EPS / core. Linux + Options C+A+B on 8-core = ~100M+ EPS / process.

---

## Implementation plan structure (next session, ~4-5 plans)

### Plan 13.3-01: Actor-based state ownership (single actor, Option C)
- New `StateActor` in `crates/beava-server/src/state_actor.rs` owning `StateTables`
- `mpsc::Receiver<StateRequest>` + `recv_many` with batch size 4096
- `Apply` and `Get` variants of `StateRequest`
- Replace `app.state_tables.lock()` call-sites with `state_tx.send(Apply { ... }).await`
- Register `/push`, `/push-sync`, `/push-many`, `/get`, `/mget` all through the actor
- Remove `parking_lot::Mutex<StateTables>` entirely
- Tests: RYW preserved, ordering within key preserved, no-lock-no-syscalls verified via strace/samply
- Criterion bench: per-event cost before/after — target 10× lift at parallel=64

### Plan 13.3-02: Atomic specialization for 8 commutative ops
- New `AtomicSlot` variant alongside existing `Slot` in state representation
- Per-op atomic implementations:
  - `CountOp::update_atomic(slot: &AtomicCountSlot)` — `fetch_add`
  - `SumOp::update_atomic(slot: &AtomicSumSlot)` — CAS loop on f64-bitcast
  - `MinOp`, `MaxOp` — CAS loops
  - `AvgOp` — two atomics (count, sum)
  - `VarianceOp`, `StdDevOp` — three atomics with Relaxed ordering; doc the drift
  - `RatioOp` — two count atomics
- Registration-time decision: pure commutative → atomic slot; else → actor-owned slot
- Bench: specifically the count-only shape → target 3M+ EPS / core macOS TCP
- Doc: accuracy trade-off for variance under concurrent updates

### Plan 13.3-03 (optional): Sharded actors
- N actors (default = num_cpus) each own 1 shard
- Router task hashes `entity_key % N` and forwards to shard actor
- Each shard actor: unchanged from 13.3-01
- Cross-shard gets (mget, mset) fan out to N oneshots, gather responses
- Bench: parallel=64 on 8-core → target N × 13.3-02 throughput

### Plan 13.3-04: Hermetic benchmarks + regression row
- New `.planning/phases/13.3-lockless-apply/13.3-throughput-row.md` per-phase file
- All 6 cells (small/medium/large × http/tcp) at parallel=64, BEFORE (baseline) and AFTER (13.3-01, 13.3-02, 13.3-03)
- Document the progression as a table in the phase SUMMARY
- Update `CLAUDE.md §Performance Discipline` — note that future phases apply lockless-apply conventions

### Plan 13.3-05: SUMMARY + VERIFICATION + plan-checker contract
- `13.3-SUMMARY.md` per template
- `13.3-VERIFICATION.md` per template, status `passed` if 5 SCs verified
- Success criteria:
  1. `state_tables.lock()` references eliminated from hot path (grep check)
  2. Apply throughput ≥ 10× at parallel=64 vs merged greenfield baseline
  3. RYW preserved when actor mode is synchronous (ACK after apply)
  4. Per-key ordering preserved for all order-sensitive operators (streak, lag, joins, sketch retraction)
  5. Atomic-ops path proven bit-identical to actor path for commutative ops on identical event streams

---

## Open research questions for next session

1. **Welford's variance under atomic / out-of-order updates**: how much numerical drift vs exact? Run 1M synthetic events, compare online-atomic vs exact-sequential. If drift < 0.1% on typical fraud distributions, accept.
2. **tokio `mpsc::recv_many` in practice**: measure actual batch size under load. If it stays ~1 event per wake (no batching), amortized savings don't materialize. If it stays ~100-1000, the design holds.
3. **HLL merge commutativity**: HLL is commutative (register-wise max) — atomic per-register suffices. Verify for count_distinct.
4. **CMS commutativity**: CMS counter updates are integer adds, commutative under atomic fetch_add. Heap maintenance for top_k is order-sensitive. Option: keep CMS updates atomic, move TopKHeap maintenance into actor batch-drain cycle.
5. **UDDSketch commutativity**: insert is bucket-counter increment (commutative). Collapse is order-sensitive (need actor). Insert-hot workloads benefit; collapse-hot don't.
6. **Join state ordering**: event↔event joins need both sides' rings in LSN order. Actor-per-key or actor-per-join-node.
7. **/get consistency model**: actor model naturally serializes reads with writes. Do we need to offer a `/get-relaxed` that reads from atomic slots directly for lower latency (at the cost of RYW violation)?

---

## Why not just fix Phase 13.2's coalescing?

Phase 13.2 as currently drafted (Plan 01 landed, 02-05 deferred) treats the lock as necessary and tries to amortize it across batches. This works: BATCH_MS=5 gives ~5× lift. But it's a workaround, not a fix.

The actor refactor is ~500 LoC — comparable to 13.2's full scope when you count MergeClass + bucket grouping + parallel apply + bench. And it gives ~10-50× lift instead of ~5× because it removes the mutex entirely rather than amortizing it.

Recommendation: **pause Phase 13.2 at Plan 01 (already landed as foundation). Pursue 13.3 directly.** Phase 13.2's ApplyBuffer primitive is still useful inside the actor for cross-request coalescing (Option C's batch-drain IS coalescing). Nothing wasted.

---

## Not in scope for Phase 13.3

- Multi-threaded apply runtime (keep single-thread mental model per PROJECT.md)
- Distributed sharding / replication (v1 territory)
- Persistent atomic state (all atomics are in-memory; WAL still owns durability)
- Symbolic Python frontend interaction (stays on the expression evaluator path, not the apply path)

---

## Reading list for the resuming agent

- `.planning/throughput-baselines.md` § "POST-MERGE quiescent rerun" — current regressed-and-fixed numbers
- `.planning/ideas/parallelism-levels.md` — Levels 1/2/3/4 context
- `crates/beava-server/src/push.rs` — the current locked path
- `crates/beava-server/src/apply_buffer.rs` — Phase 13.2's Plan 01 foundation (keep)
- `crates/beava-core/src/agg_apply.rs` — apply_event_to_aggregations entry point
- `crates/beava-core/src/config.rs::ApplyConfig` — Phase 13.2's 6 knobs (repurpose for 13.3)
- Tokio documentation: `mpsc::recv_many`, actor patterns

---

## One-paragraph summary

The current `state_tables.lock()` is 99% of per-event overhead. It protects against a multi-thread race that doesn't exist on a single-thread runtime, AND it serializes operators that are commutative anyway. Replace it with (1) single actor owning state via message-passing channels — ~10× lift, gets to ~500k EPS; then (2) atomic slot types for the 8 commutative operators — another 3-5× lift, gets to 3M+ EPS / core; optionally (3) sharded actors — scales linearly across cores. The 3M EPS/core Phase 13 ship target is achievable with steps 1 + 2 alone, on current hardware, without breaking the single-thread mental model. Plan 13.3 has 5 sub-plans, ~1000 LoC total, and supersedes Phase 13.2's Plans 02-05 (keeping 13.2 Plan 01's foundation).
