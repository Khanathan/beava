# Thread-Per-Core + Full Key-Shard Architecture

**Branch:** `arch/tpc-full-shard`
**Created:** 2026-04-18
**Status:** Design exploration — NOT a v1.0-launch item. Targets v1.2 or "Beava Cloud" internal runtime.
**Author / owner:** primary maintainer
**Supersedes:** v1.2 roadmap item "Thread-per-core mode" (deferred per PROJECT.md Out-of-Scope at v1.0-launch).

---

## Motivation

Today's Beava is **tokio multi-threaded + `DashMap`-sharded state**. 8 worker threads contend on lock shards inside DashMap; push batches acquire per-stream locks. Measured ceiling: ~350K EPS TCP / 314K 9-cell-matrix baseline on a 10-core M4 laptop.

Measured bottleneck (Phase 14 shard-probe investigations + Phase 41 hot-path mutex removal): **cross-core synchronization dominates above ~8 cores**. Each additional core returns diminishing throughput because lock-shard contention and cache-line bouncing grow superlinearly. "Just add cores" does not scale.

**Thread-per-core (TPC) + full key-shard** is the canonical fix used by ScyllaDB, Redpanda, glommio-based systems, and FoundationDB's per-process architecture. Every event is routed at ingest time to exactly one core based on a shard hint derived from its key; that core owns its K/N slice of state; **no lock contention, no cache-line bouncing across cores**.

Expected throughput ceiling: **~2M EPS per 16-core box** (5-6× the current baseline) for steady-state single-stream ingest, assuming the shard-hint distribution is balanced. This is the "Beava Cloud can accept real workloads" primitive.

## Goals

1. Every event is **routed to one owning shard** at the earliest possible point (ideally inside the network parser, before any allocation or copy).
2. Each shard is served by **one pinned OS thread**. No cross-shard locks on the hot path.
3. **State is partitioned, not replicated.** Shard N holds entities whose `shard_hint(key) mod N_SHARDS == N`. Cross-shard queries are either explicit scatter-gather or disallowed.
4. **EventSet sources emit a `shard_hint`** alongside every event. Pre-existing knowledge (Kafka partition, replica log offset, sticky HTTP connection) is preserved into the routing layer — sources that already partition by key don't re-hash.
5. **Event log is per-shard**, not central. Enables independent recovery + replay per shard.
6. **Watermark propagation across shards** is explicit and bounded — no silent cross-shard invariants.
7. **Correctness is preserved.** All existing test suites pass. In particular: crash-replay determinism, event-time bucketing, fork-replay parity.
8. **Migration-path-first design.** Single-shard mode (N_SHARDS=1) compiles to current behavior. No flag day.

## Non-goals

- **Horizontal scale across nodes.** TPC is intra-node scaling. Multi-node still waits on v1.3+ or Beava Cloud.
- **Exactly-once semantics.** At-least-once + client dedup remains.
- **Replacing tokio wholesale.** Tokio current_thread-per-core or `glommio`-style. We keep the async ergonomics; we just eliminate cross-thread task migration.
- **Breaking the current HTTP/TCP API.** Clients see the same endpoints, same shapes. Routing happens inside the binary.

## Prior art we lean on

- **ScyllaDB** (C++) — shard-per-core, message-passing over internal queues, `seastar` framework.
- **Redpanda** (C++) — same pattern, Kafka-compatible wire.
- **glommio** (Rust) — Linux-only (`io_uring`), thread-per-core with per-shard reactors. DataDog's Rust-native version of Seastar.
- **FoundationDB** (Flow language over C++) — partition-per-process, message-passing. Proof that correctness is preserved with strong sharding.
- **Meilisearch** / **Tantivy** — Rust single-binary search; not sharded but informs the pinned-thread performance posture.
- **Tokio current_thread + channels** — baseline Rust pattern. Tokio docs now include TPC guidance.

Key lesson from all of the above: **the shard hint must travel with the event through every hop**. If Kafka partition 3 of `transactions` always goes to shard 3, you preserve the partition → shard mapping end-to-end. If you re-hash inside the engine, you lose that guarantee and either stall correctness or bounce events across cores.

## Current architecture (summary, for diff)

```
TCP listener (tokio)         HTTP listener (axum/tokio)
        |                                |
        +----------- parse ---------------+
                         |
                  handle_push_core_ex
                  handle_push_batch
                         |
        tokio worker threads (8×)
                         |
         PipelineEngine (shared via Arc)
              state store (DashMap — internally 16-shard locked)
              event log (single append-only per stream)
              watermark tracker (DashMap per-stream)
                         |
          HTTP/TCP response path
```

**Contention points:**
- DashMap internal shard locks (tested at Phase 14, 24, 40, 41)
- Per-stream write locks (Phase 40) — better but still cross-thread
- `arc-swap` dirty set (Phase 46, CORR-10) — read side is lock-free but writers compete on Arc refcount
- Event log append lock (current single log per stream)

## Target architecture

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

**Key properties:**
- Each shard is single-writer for its own state. No locks. No `DashMap`. No `arc-swap`.
- Ingest path hashes `shard_hint` once, routes to shard queue, returns to listener. Shard thread dequeues and processes.
- Query path (`GET /features/{key}`) hashes key, routes to shard, shard returns feature vector. Cross-shard queries (`GET /streams` — global listing) scatter-gather from all shards.
- Fork / replica: upstream's `shard_hint` is preserved on the wire. Local replica uses identical shard count → identical routing → identical state. Different shard counts → re-route.

## Design dimensions

### 1. Runtime choice

**Option A — tokio current_thread, one runtime per pinned thread.**
- Pros: No new dep. Tokio 1.x has mature `tokio::runtime::Builder::new_current_thread()`. Works on macOS + Linux. axum integrates.
- Cons: Per-shard runtime adds overhead vs native Seastar/glommio. IO reactor per shard is still tokio's default-scheduled epoll, not io_uring.
- Fit: best for v1.2 — incremental migration, no platform lock-in.

**Option B — `glommio` runtime.**
- Pros: true Seastar-style async I/O via io_uring, per-core reactor. ~2× throughput over tokio on pure I/O.
- Cons: Linux-only (no macOS → dev machines cut out). Fewer crate-ecosystem integrations. Would need to port axum → `hyper` + glommio TCP streams.
- Fit: post-v1.2, possibly "Beava Cloud exclusive" to justify the platform split.

**Option C — custom with `rayon` or raw `std::thread::spawn` + pinning.**
- Pros: no async at all in the shards; straight-line synchronous event processing.
- Cons: loses async for I/O (DB calls, HTTP upstreams). Probably wrong for our use case.

**Proposed choice: Option A (tokio current_thread per shard).** Lowest-risk migration, preserves macOS dev experience, enables glommio swap later if we need another 2× after TPC lands.

### 2. Shard-hint API

Add to `EventSet` and event-source types:

```rust
pub trait EventSource {
    // Existing: produce events
    fn poll_next(&mut self) -> Option<Event>;

    // NEW: deterministically compute shard hint from the event
    // Sources that already partition (Kafka, replica log by stream key)
    // should return the source's own partition/shard id to preserve end-to-end
    // routing. Sources without prior knowledge return hash(key).
    fn shard_hint(&self, event: &Event) -> u32;
}
```

For HTTP push: `shard_hint = hash(stream_key)`.
For TCP push: same.
For Kafka (future source): `shard_hint = kafka_partition` — preserves producer's partition choice.
For replica log (fork): `shard_hint = upstream_shard_id` — preserves upstream routing.
For `OP_LOG_FETCH`: the upstream embeds shard_hint in the log entry metadata.

**Fallback path:** if `shard_hint` is missing or invalid, route to shard `hash(any_key_field) mod N`. Never panic on malformed routing.

### 3. Cross-shard queries

Three categories:

**a) Point-keyed queries (99% of reads):** `GET /features/{key}` — route to owning shard via `hash(key) mod N`. Zero scatter.

**b) Listing queries (rare):** `GET /streams`, `GET /streams/{name}` — scatter-gather. Fan out to all shards, merge responses at the listener. Small result sets; acceptable overhead.

**c) Join operators (dangerous):** a join between stream A and stream B where keys are mixed means events from A on shard-3 may need to reach B-side state on shard-7. Options:
  - **Broadcast small side, partition large side** (Flink idiom). Works if one side is a dimension table.
  - **Re-shard both sides on the join key** (expensive, requires per-join data flow).
  - **Co-locate joined streams** (constrain: both streams must declare the same shard key). Best fit for v1.2; explicit constraint in `@bv.stream(shard_key=...)`.

Start with (c)'s co-location constraint. Joins require explicit `shard_key` agreement at registration. Error out at register time if streams in a join disagree.

### 4. Per-shard event log

Current: one append-only log per *stream*. Target: one append-only log per *shard* per stream.

On disk: `data/shard-N/streams/{stream_name}/log.bin`.

Recovery: each shard reads its own log independently, replays into its own state. Parallel recovery → recovery time scales down with core count. This is a *win* on top of the correctness preservation.

Open question: what if `N_SHARDS` changes between runs? Answer: re-shard during recovery. Read all old logs, route each entry by new `shard_hint`, write to new-layout logs. Expensive but one-time.

### 5. Watermark propagation across shards

Each shard tracks its own per-stream watermark. Global watermark for a stream = `min(shard watermarks)`. Implemented via:
- Each shard publishes its watermark to a global atomic once per N events (batched publish, not per-event).
- Readers interested in global watermark read `min` across published atomics.
- Per-entity TTL eviction uses the shard-local watermark (no cross-shard needed).
- Join operator (co-located) uses shard-local watermarks.

This is lazy synchronization — watermarks lag slightly behind true min. Acceptable because watermark lateness is already an expected tolerance.

### 6. HTTP / TCP listener → shard routing

Listener threads (tokio multi-threaded, 2-4 of them for I/O) parse the request, compute `shard_hint`, send over SPSC channel to the target shard's inbox. Shard processes, returns result via response channel. Listener awaits response, writes to socket.

Channel choice: `kanal` or `crossbeam-channel` SPSC. Zero-copy event handoff via `bytes::Bytes`.

One risk: latency from the extra hop. Measure carefully. If listener → shard adds >50μs, consider co-locating the listener on the shard thread (shard owns the HTTP accept for its own slice of connections via SO_REUSEPORT).

### 7. Migration compatibility

**Single-shard mode (`BEAVA_SHARDS=1`) must be byte-compatible with current state format.**
- State directory layout: `data/shard-0/` contains what `data/` contains today.
- Event log format: unchanged.
- Snapshot format: extended with a `shard_count: u16` field at the top. N=1 snapshots load cleanly.
- Wire format: unchanged. TCP opcodes unchanged. HTTP endpoints unchanged.

Transition:
1. v1.1 — TPC behind `BEAVA_SHARDS=1` flag (default 1). Existing prod untouched.
2. v1.1.x — users opt in to `BEAVA_SHARDS=N` on fresh data dirs. Test at scale.
3. v1.2 — default N equals CPU count. Re-sharding tool included for existing snapshots.

## Implementation plan (phase-level, for future milestone)

Not decomposed to task-level yet. This is a **v1.2 scope** or **Beava Cloud internal** body of work; v1.0-launch has shipped. Keeping waves intentional:

**Wave 0 — scaffolding & benchmarks**
- [ ] Add `shard_hint` trait method to `EventSource`. Default impl `hash(key)`. Wire through TCP + HTTP parsers. (Backward-compatible; always returns 0 for N_SHARDS=1.)
- [ ] Micro-bench: `hash(key)` overhead per-event. Budget: <100 ns per ingest.
- [ ] Micro-bench: SPSC channel roundtrip (listener → shard → response). Budget: <10 μs.

**Wave 1 — per-shard state store**
- [ ] Introduce `Shard` struct encapsulating per-shard state (HashMap, not DashMap), event log, watermark, dirty set.
- [ ] Compile-time `N_SHARDS` = 1 first. Verify full test suite passes with the new per-shard plumbing using N=1.
- [ ] Extend to runtime-configurable `N_SHARDS`; default 1.

**Wave 2 — multi-shard routing**
- [ ] Listener-to-shard SPSC channels.
- [ ] Shard threads pinned via `core_affinity` crate (best-effort on macOS; strict on Linux).
- [ ] Multi-shard integration test: N=4, push events, query features, verify per-shard ownership correct.
- [ ] 9-cell matrix re-run against N=CPU_COUNT. Target: ≥3× baseline for `complex-c8-x8` cell.

**Wave 3 — cross-shard queries + joins**
- [ ] `GET /streams` scatter-gather.
- [ ] Join operator shard-key co-location constraint (register-time validation).
- [ ] Global watermark lazy-publish across shards.

**Wave 4 — event log per-shard + recovery**
- [ ] Re-layout event log on disk: `data/shard-N/streams/{name}/log.bin`.
- [ ] Parallel recovery (one thread per shard).
- [ ] Re-sharding tool: migrate existing N=1 data to N=K by replaying old logs into new layout.
- [ ] Fork/replica preserves upstream shard_hint on the wire (extend `OP_LOG_FETCH` header).

**Wave 5 — production readiness**
- [ ] Load test: sustained 1M+ EPS on 16-core box. Committed benchmark result.
- [ ] Property test: N=1 and N=8 on same event stream produce identical feature values.
- [ ] Failover: kill one shard thread (simulated) — verify graceful degradation / restart.
- [ ] Documentation: `docs/architecture-tpc.md` + updated `docs/operations.md` with shard sizing.

**Estimated effort:** 8-12 weeks for one engineer. Feasible as a single v1.2 milestone or as an internal Cloud initiative that forks from OSS at Wave 2.

## Risks

1. **Co-located join constraint is restrictive.** Users expressing joins across mis-partitioned streams today will need to re-declare. Mitigation: error at register time with actionable message; provide `SHARD_KEY` inference heuristic.
2. **Latency regression on the listener → shard hop.** SPSC handoff costs ~1μs but N is small. Monitor carefully. If unacceptable for the sub-ms read path, move the HTTP accept onto shard threads via SO_REUSEPORT.
3. **Pinning behavior on macOS.** `core_affinity` is best-effort on Darwin. Dev-machine throughput may be lower than Linux prod. Accept; prod is Linux.
4. **Snapshot format migration.** Adding `shard_count` to snapshot header is forward-compatible, but N=1→N=K rewrites require the re-sharding tool and downtime. Document the migration clearly.
5. **Watermark lag across shards.** Lazy publish means global watermark trails per-shard watermark by the publish interval. Tune interval; alert on excessive lag via `beava_watermark_lag_seconds` gauge (the same gauge SRE-STREAM requested in the persona review).
6. **Re-sharding during fork is a footgun.** Fork-replica must either match upstream shard count exactly or run the re-sharding path at ingest. Document; provide a `--reshard-from N` flag on `beava fork`.

## Prior Beava experiments to reuse

- `src/server/shard_probe.rs` — from Phase 14, measures per-shard contention. Directly informs placement strategy.
- `src/server/throughput.rs` — existing throughput harness. Extend with per-shard counters.
- Phase 40 (per-stream write locks) + Phase 41 (hot-path mutex removal) — the diagnostic work that motivated this. Direct pre-requisites.
- Commits `3818880` → `1cefc45` (per-event batch-coalesce revert) — cautionary tale; the lesson is that correctness-preserving refactors need benchmark gates. Apply the same pattern here (9-cell matrix gate on every wave).

## Benchmark expectations

| Metric | Today (main) | Target (TPC N=8) | Target (TPC N=16) |
|---|---:|---:|---:|
| Single-stream TCP push EPS | 314K | 1.2M | 2.0M |
| 9-cell matrix `complex-c8-x8` | baseline | +3×-4× | +5×-6× |
| Recovery time (4.7 GB state) | 7.0 s | 1.5 s (parallel) | 0.8 s |
| p99 feature-read latency | sub-ms | sub-ms (co-located) | sub-ms |
| Cross-shard query latency (`GET /streams`) | N/A | ~10 μs (scatter) | ~15 μs |
| Memory per entity | 616 B | unchanged | unchanged |

Ship gate for merging TPC to main: **every cell of the 9-cell matrix within −5% of baseline at N=1, and ≥3× at N=8**.

## Open questions

1. **Default N_SHARDS?** CPU count? CPU count minus 2 (reserve for listener)? Environmental (`BEAVA_SHARDS`)?
2. **macOS dev experience.** Should `cargo run` default to N=1 for dev, N=cpu_count for release? Probably yes via `cfg(debug_assertions)`.
3. **SO_REUSEPORT strategy.** Shard-thread-owned HTTP accept vs listener-dispatched routing. Measure before picking.
4. **Fork re-sharding.** Is `beava fork --reshard-from upstream-N` sufficient, or do we need silent re-sharding on every fork?
5. **Python SDK impact.** Does `@bv.stream` need a `shard_key=` annotation, or do we infer from the primary key?
6. **Metrics.** Add `beava_shard_lag_seconds{shard}` gauge? `beava_cross_shard_fanout_total` counter?

## What's on this branch vs main

**This branch (`arch/tpc-full-shard`):**
- `.planning/arch/TPC-SHARD-DESIGN.md` (this file) — design exploration.
- No code changes yet. First code PR will be Wave 0 scaffolding (`shard_hint` trait method, wired through TCP+HTTP, zero behavior change).

**Main:**
- v1.0-launch shipped. Current tokio multi-threaded + DashMap architecture unchanged.
- v1.1 adoption-first work (`.planning/milestones/v1.1-prep/ADOPTION-FIRST-PLAN.md`) proceeds in parallel on main.

**Merge strategy:** squash-merge into main only when Wave 2 + 9-cell matrix gate pass. Everything prior stays on the branch as experimentation.

---

*Design doc. No code changes on this branch yet. Add Wave 0 scaffolding as separate PRs against this branch before anything touches main.*
