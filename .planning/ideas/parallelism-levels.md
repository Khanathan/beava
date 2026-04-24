# Parallelism levels — what we're leaving on the table

> **Status:** post-v0 / Phase 13 candidate. Not on v0 ROADMAP. Captured 2026-04-23.

## The "single-thread mental model" of v0

PROJECT.md and STATE.md establish: single-thread tokio current_thread runtime, one apply loop, one event at a time. This is deliberate for v0:
- Ownership simplicity — no locking
- Predictable replay determinism
- Single mental model that scales to 3M EPS / **core**

But "single-thread" leaves three legitimate parallelism wins on the table.

---

## Level 1 — per-node fan-out (intra-event)

A pushed `Transaction` matches N aggregation nodes. Each node owns a disjoint slice of per-entity state. Updating `tx_count_5m[alice]` is independent of `tx_sum_5m[alice]`.

### Today
```rust
for node in matching_nodes {
    apply_event_to_node(state, node, event);
}
```
Sequential. ACK latency = sum(per-node cost).

### Could be
```rust
matching_nodes.par_iter().for_each(|node| {
    apply_event_to_node(&state.shard(node), node, event);
});
```
Parallel. ACK latency = max(per-node cost).

### Properties
- ✅ Determinism preserved on replay (no shared state between nodes)
- ✅ Threshold-gated: serial under N=4, parallel above
- ✅ One crate dep (`rayon`), ~20 LoC
- ❌ Worse cache locality (worker threads touch N different state slots)
- ❌ Thread-pool overhead dominates for small fan-outs

### Sweet spot
Phase 13's complex-fraud pipeline (15+ ops/event, 3 entity types). Bad fit for simple-fraud (5 ops).

### Roadmap fit
**Phase 13 tactical lever.** If the simple-fraud benchmark lands below 3M EPS at the ship gate, drop in `par_iter` as a one-commit fix. Zero architectural risk.

### Trade-off depth (this is the level worth thinking about hardest)

**Why Level 1 is uniquely safe — per-event transaction semantics are preserved.**

Single-thread today: ACK after all N nodes update sequentially.
Level 1 parallel: ACK after all N nodes update in parallel (max latency, not sum).
Either way: from the client's perspective, the event is **atomic** — push → wait → ACK once everything is applied. Replay determinism, /get consistency, and snapshot-at-LSN-boundary all work identically. The transaction model doesn't change.

This is the property that makes Level 1 cheap to land. Levels 2 + 3 also preserve atomicity in different ways (Level 2 by serializing within a shard; Level 3 by treating the batch as the transaction unit), but they're architectural shifts. Level 1 is just "parallel inside the existing transaction."

**The non-obvious win is latency, not throughput.**

A common confusion: "parallel apply will give us more EPS." It won't, mostly. Single-thread apply on one core is already saturating one core. EPS is bounded by total core × per-event cost. Parallel apply within an event reduces P99 latency for fat-fan-out events (the heavy-sketch-insert that takes 5µs while others take 50ns no longer blocks them) but doesn't add throughput on a single core. **Throughput multiplier comes from Level 2 (per-entity sharding across cores).**

So Level 1's real pitch: "P99 push latency stays predictable as your pipeline grows." It's a feature for users who add ops over time and don't want their tail latency degrading linearly with op count.

**Threshold matters — parallel loses for small fan-outs.**

Modern thread-pool dispatch (rayon work-stealing, tokio spawn): ~200ns of overhead per job on Apple-M4. Trade-off math:

| Per-node cost | N nodes | Sequential total | Parallel total | Verdict |
|---------------|---------|------------------|----------------|---------|
| 50ns (`count++`) | 4 | 200ns | ~850ns (200ns/job × 4 + 50ns work) | **Parallel loses 4×** |
| 50ns | 16 | 800ns | ~1.0µs | Parallel barely loses |
| 1µs (sketch insert) | 4 | 4µs | ~1.8µs | **Parallel wins 2.2×** |
| 1µs | 16 | 16µs | ~3.2µs | **Parallel wins 5×** |

Implementation must threshold on `(per_node_cost × N) > dispatch_overhead × N`. Practically: serial under 8-ish nodes OR under 500ns/op average; parallel above. Heuristic at register-time based on the operator catalog's known costs.

**Cache locality is the silent regression.**

Single-thread apply touches N node-state slots on the same CPU's L1/L2 cache. For one entity (say `user_id=alice`) with 15 ops, all 15 state structs typically fit in one L2 page. Sequential apply hits cache on every access.

Parallel apply: N worker threads each pull their node's state slot. Cross-core L1 misses, possibly L2 misses too. On hot/repeated entities (the actual fraud workload — same accounts touched repeatedly), the cache regression can outweigh the parallelism win.

**Mitigation**: keep per-entity state slots co-located by entity (not by node). The natural shape is `state[entity_key] = {node_a_state, node_b_state, ...}` — already cache-friendly within an entity. Parallel apply over nodes for a SINGLE entity then becomes a worse layout (each thread pulls a different slot from the same entity's struct). The "right" parallel layout would be `state[node][entity]`, which is what we already have — but then per-event apply touches different cores' caches.

There's a real trade here. Sequential keeps entity-local; parallel keeps node-local. Neither is strictly better; depends on whether the working set is "same entity, many ops" (sequential wins) or "many entities, few ops each" (parallel wins). Most fraud workloads lean sequential-friendly because users get hit repeatedly.

**Determinism caveat for non-pure ops.**

Most aggregations are pure: `count`, `sum`, `min`, `max`, `variance`, `ewma`, `streak`. Order-independent. Replay-deterministic regardless of thread scheduling.

Some are not: `reservoir_sample` (RNG-driven), `top_k` (CMS hash-collision resolution under contention). If their internal RNG is `thread_rng()`, parallel scheduling produces different results across runs — which breaks replay determinism.

**Mitigation**: every non-pure op must use a per-entity-key seeded RNG (`hash(entity_key, lsn)`), not a thread-local. This is already the right call for any production fraud system, but it's a discipline we'd need to enforce at the operator-trait level.

**Failure semantics get more complex.**

Today: one node panics → single-thread apply unwinds → `/push` returns 500 → WAL has the event but state is unmodified (apply-after-append) or modified-up-to-the-failed-node. Either way, deterministic.

Parallel: one node panics on a worker thread. Sibling threads keep running. Need to either:
- Wait for siblings to finish, then propagate the panic (latency hit on errors)
- Cancel siblings (rust async cancellation is messy; rayon doesn't have it)
- Mark the failed node "errored", continue ACK with partial-update flag (weakens transaction guarantee — now ACK ≠ "all nodes updated")

Cleanest path: "wait for siblings, then propagate panic." Means errors are slower but transaction semantics stay intact. This is what we should do.

**Scheduler interaction breaks "single-thread" optics.**

PROJECT.md's "single-thread mental model" is a feature — users see one CPU core in `top` and reason about Beava's behavior simply. Adding rayon means N OS threads, and `top` shows multi-core utilization even on a single in-flight event. Not a correctness issue, but it changes the marketing story slightly. Worth being explicit when documenting: "single apply *loop*, parallel work *within* the loop."

**Where it doesn't apply: derivation chains.**

Level 1 only parallelizes nodes that are **siblings** in the DAG — direct attachments to the source event with no inter-node data dependencies. Derived features that read another feature's mid-event state (e.g., `feature_C = f(feature_A)`) must wait for the parent to apply first. The parallel scope is the **leaf set of the DAG layer**, not the whole DAG.

In practice this is fine because the typical pipeline shape is "one event source → many flat aggregations" — the leaf set is wide and the chain depth is shallow.

### Bottom line for Level 1

Per-event transaction semantics: preserved. Latency: better for heavy fan-out. Throughput: roughly unchanged on a single core. Cache: trade-off depending on workload shape. Determinism: requires non-pure ops to use seeded RNGs. Failure handling: needs a sibling-wait policy. Mental model: still "one apply loop" but with parallel work inside.

Net: low-risk, well-scoped, naturally fits Phase 13 perf-tune. Worth a single feature-flag-gated commit when tail-latency becomes the constraint.

---

## Level 2 — per-entity sharding (inter-event)

Hash(entity_key) → shard. Each core owns a shard, processes events serially within it. Like Kafka partitions, like Flink keyed state.

```
/push → router (hash entity_key) → {shard_0, shard_1, ..., shard_N-1} → parallel apply
```

### Properties
- ✅ Strong per-key determinism (events for `user-42` always hit the same shard)
- ✅ Scales linearly with cores — this is how you go past "3M EPS / core" to "24M EPS on 8 cores"
- ❌ Major architectural shift — moves Beava from "single apply loop" to "N apply loops + router"
- ❌ Joins across keys need cross-shard coordination
- ❌ Snapshot becomes per-shard; recovery replays per-shard WAL
- ❌ Breaks the PROJECT.md mental model

### Roadmap fit
**v0.1 or v1 headline feature.** "Beava 1.0 scales linearly across cores." Whole milestone, not a phase. Touches WAL layout, snapshot format, recovery, registry, /push routing. Worth a separate roadmap pass.

---

## Level 3 — batch-parallel apply (synergy with Phase 6.1)

Once Phase 6.1 lands (ACK-after-append, fsync every N ms), the WAL buffer accumulates events between fsync ticks. The batch becomes the natural parallel unit.

```
buffer = [evt1, evt2, ..., evtN]   // accumulated between fsync ticks
fsync(buffer)
par_apply_grouped_by_entity(buffer)  // group by entity_key, parallel across groups
```

### Properties
- ✅ Per-entity-key serial order preserved (group-by inside the batch)
- ✅ Cross-entity parallelism free (different keys = different groups)
- ✅ Pairs naturally with Phase 6.1's periodic-flush architecture
- ✅ Best fit for high throughput
- ❌ Adds "apply delay" up to fsync_interval_ms (acceptable trade per Phase 6.1)

### Sweet spot
High-throughput / async-durability deployments. Pairs perfectly with `BEAVA_WAL_SYNC_MODE=periodic`.

### Roadmap fit
**Phase 13 perf-tune candidate**, OR an extension of Phase 6.1 if perf data demands it. Implementation is a few dozen LoC: rayon's `par_chunks_by(entity_key)` over the WAL buffer at flush time.

---

## Recommended ordering

| Order | Level | Trigger | Risk |
|-------|-------|---------|------|
| 1 | **1 (per-node fan-out)** | Phase 13 ship-gate misses 3M EPS on a fan-out-heavy pipeline | Low — opt-in via threshold |
| 2 | **3 (batch-parallel)** | Same as level 1 + Phase 6.1 already shipped | Low — natural extension of 6.1 |
| 3 | **2 (per-entity sharding)** | v1 milestone — "scale across cores" is the headline | High — architectural |

Levels 1 + 3 can coexist within v0/v0.1 without breaking the single-thread mental model (still single apply *loop*, just parallel work *within* the loop). Level 2 is the model break.

## What this is NOT

- Not multi-instance (that's replication, v1.x territory)
- Not multi-process (forks share nothing — we'd be talking to a sidecar at that point)
- Not GPU parallelism (no aggregation operator vectorizes well enough to amortize PCIe latency)
- Not Velox-style columnar SIMD (already discussed in `v0.1-symbolic-python-frontend.md` — wrong workload shape for streaming state)

## Open question for v0.1+

Could the symbolic-Python frontend (the chalk-style tracer in `v0.1-symbolic-python-frontend.md`) detect "this UDF is pure + per-row" and automatically hint level-1 parallelism? Probably yes — the IR tells you fan-out structure. Worth thinking about when v0.1 starts.
