# Thread-Per-Core + Full Key-Shard Architecture

**Branch:** `arch/tpc-full-shard`
**Created:** 2026-04-18
**Status:** Design hardened against 2026-04-18 research (`.planning/arch/TPC-RESEARCH.md`). All 6 original open questions resolved. Ready for planning. NOT a v1.0-launch item — targets v1.2 or "Beava Cloud" internal runtime.
**Author / owner:** primary maintainer
**Supersedes:** v1.2 roadmap item "Thread-per-core mode" (deferred per PROJECT.md Out-of-Scope at v1.0-launch).

---

## Motivation

Today's Beava is **tokio multi-threaded + `DashMap`-sharded state**. 8 worker threads contend on lock shards inside DashMap; push batches acquire per-stream locks. Measured ceiling: ~350K EPS TCP / 314K 9-cell-matrix baseline on a 10-core M4 laptop.

Measured bottleneck (Phase 14 shard-probe investigations + Phase 41 hot-path mutex removal): **cross-core synchronization dominates above ~8 cores**. Each additional core returns diminishing throughput because lock-shard contention and cache-line bouncing grow superlinearly. "Just add cores" does not scale.

**Thread-per-core (TPC) + full key-shard** is the canonical fix used by ScyllaDB, Redpanda, Apache Iggy (Rust, Feb 2026 — migrated from tokio work-stealing to TPC/compio, P99 −60%), and FoundationDB's per-process architecture. Every event is routed at ingest time to exactly one core based on a shard hint derived from its key; that core owns its K/N slice of state; **no lock contention, no cache-line bouncing across cores**.

Expected throughput ceiling: **1.5M–2.5M EPS per 16-core box** (5-6× the current baseline) for steady-state single-stream ingest, contingent on the shard-hint distribution being balanced AND shard_probe reporting `cross_shard_fraction < 40%` on the target workload. Iggy's independent 5M msgs/sec result at 1KB sets the plausible ceiling for a broker workload; Beava's stateful per-key aggregation lands lower. This is the "Beava Cloud can accept real workloads" primitive.

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
- **Replacing tokio wholesale.** Tokio `current_thread` per-core (v1.2), with a clear migration path to `compio` (v1.3 / Beava Cloud). We keep the async ergonomics; we just eliminate cross-thread task migration.
- **Breaking the current HTTP/TCP API.** Clients see the same endpoints, same shapes. Routing happens inside the binary.

## Prior art we lean on

- **Apache Iggy** (Rust, Feb 2026) — **the load-bearing 2026 case study.** Migrated from tokio work-stealing to thread-per-core on `compio` + `io_uring`, one shard per physical core pinned via `sched_setaffinity`. Measured P99 −60%, P9999 −57%, +18% throughput (fsync mode, 32 partitions). Peak: 5M msgs/sec at 1KB. They evaluated and rejected `monoio` and `glommio`. Our migration is the same shape.
- **ScyllaDB** (C++) — shard-per-core, message-passing over internal queues, `seastar` framework. [Shard-Aware Port](https://www.scylladb.com/2021/04/27/connect-faster-to-scylla-with-a-shard-aware-port/) — sticky-connection routing lesson applies to our HTTP keep-alive story.
- **Redpanda** (C++) — same pattern, Kafka-compatible wire. Operational cap ~1000 partitions/shard; a shard can own many streams without blowing up.
- **FoundationDB** (Flow over C++) — partition-per-process, message-passing. Proof that correctness is preserved with strong sharding.
- **glommio** (Rust) — **historical reference only.** Linux-only (`io_uring`, kernel ≥5.8). **Effectively unmaintained** as of 2026 (DataDog walked away; Iggy's team explicitly rejected it for this reason).
- **compio** (Rust) — Iggy's choice. Cross-platform: `io_uring` on Linux, IOCP on Windows, `kqueue` via `polling` on macOS. The only TPC runtime that doesn't compromise our macOS dev story. Decoupled driver/executor architecture (vs monoio's tightly coupled design).
- **monoio** (Rust, ByteDance) — active, production-used at ByteDance. Linux io_uring primary; macOS `legacy` fallback mode is compromised. Rejected by Iggy for "limited io_uring feature coverage and insufficient maintenance pace."
- **Meilisearch** / **Tantivy** — Rust single-binary search; not sharded but informs the pinned-thread performance posture.
- **Tokio current_thread + channels** — baseline Rust pattern. `Builder::new_current_thread().build_local()` is the modern (2025+) API; `LocalSet` has documented overhead. Tokio is building a `LocalRuntime` type ([tokio-rs/tokio#6739](https://github.com/tokio-rs/tokio/issues/6739)) as the eventual canonical API.

Key lesson from all of the above: **the shard hint must travel with the event through every hop**. If Kafka partition 3 of `transactions` always goes to shard 3, you preserve the partition → shard mapping end-to-end. If you re-hash inside the engine, you lose that guarantee and either stall correctness or bounce events across cores.

Additional lessons from Iggy's 2026 migration (all directly applicable to Beava):
- `RefCell` borrows across `.await` panic at runtime. Shard-local state access must never cross await boundaries.
- Background event broadcasts create non-deterministic state — incompatible with crash-replay determinism.
- `io_uring` completions are not in submission order. Any io_uring-backed event-log writer must track completion→submission order correctness.
- io_uring only wins with heavy syscall batching. Per-event uring submit loses to epoll-batched writev.

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
- Pros: No new dep. Tokio 1.x has mature `tokio::runtime::Builder::new_current_thread()`. Works on macOS + Linux + Windows. axum integrates.
- Cons: Per-shard runtime adds overhead vs native Seastar/compio. IO reactor per shard is still tokio's epoll, not io_uring.
- Fit: best for v1.2 — incremental migration, no platform lock-in.

**Sub-option A' (recommended within Option A):** Use `Builder::new_current_thread().build_local()` — the modern (2025+) Tokio API. Avoids manual `LocalSet`, which Deno has documented adds measurable overhead vs inherently-local tasks. Track [tokio-rs/tokio#6739 `LocalRuntime`](https://github.com/tokio-rs/tokio/issues/6739) as the eventual canonical API.

**~~Option B — `glommio` runtime.~~ (obsolete)**
- Glommio is **effectively unmaintained as of 2026.** DataDog walked away; Apache Iggy explicitly rejected it when doing this exact migration. Linux-only (io_uring, kernel ≥5.8). Kept here only as historical reference.
- Replacement: **Option D (compio)** below.

**Option C — custom with `rayon` or raw `std::thread::spawn` + pinning.**
- Pros: no async at all in the shards; straight-line synchronous event processing.
- Cons: loses async for I/O (DB calls, HTTP upstreams). Probably wrong for our use case.

**Option D — `compio` runtime.** *(the 2026 replacement for glommio)*
- Pros: true TPC async I/O via `io_uring` (Linux), IOCP (Windows), `kqueue`/`polling` (macOS). **The only modern TPC runtime with a viable macOS dev story.** Decoupled driver/executor architecture (unlike monoio's tightly-coupled design). Apache Iggy picked it Feb 2026 after evaluating all alternatives; shipped P99 −60%, P9999 −57%, +18% throughput (fsync, 32 partitions), peak 5M msgs/sec at 1KB.
- Cons: Newer ecosystem than tokio. Axum currently tokio-native; would need an HTTP server on top of `compio-net` (Iggy wrote their own). API surface may still evolve — docs flag valid-until ~2026-07-18 for compio 0.18.x.
- Fit: the v1.3 / Beava Cloud endpoint. Do **not** adopt for v1.2; too many unknowns. But frame the v1.2 progression as "tokio current_thread → compio" so the migration stays open.

**Option E — `monoio` runtime.**
- Pros: active, production-used at ByteDance. `!Send`-by-design, which fits a shard-owned state model natively.
- Cons: Linux io_uring primary; macOS `legacy` mode is compromised. Iggy rejected it for "limited io_uring feature coverage and insufficient maintenance pace." Tightly-coupled driver/executor limits customization.
- Fit: fallback if compio regresses or loses momentum.

**Proposed choice for v1.2: Option A + sub-option A'** (tokio `current_thread` via `build_local()` per pinned shard). Lowest-risk migration, preserves macOS dev, avoids betting the farm on a still-evolving runtime. **Proposed v1.3 endpoint: Option D (compio)** once Wave 2–5 ship-gates prove the TPC architecture on tokio and we measure a ceiling worth punching through.

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

Channel choice: start with **`crossbeam-channel::bounded()` in SPSC configuration** (one producer = listener, one consumer = owning shard). Zero-copy event handoff via `bytes::Bytes`. Crossbeam is the safe, well-tested default. Upgrade to `rtrb` (wait-free ring buffer) only if Wave 0 micro-bench measures the handoff itself as the bottleneck — the `Bytes` payload doesn't justify `rtrb`'s ergonomic hit (fixed-size ring, no error-on-close). Apache Iggy uses `flume` but without benchmark justification; we follow crossbeam on safety grounds. `kanal` is fast but benchmarks are maintainer-run; avoid until independently validated.

One risk: latency from the extra hop. Measure carefully. If listener → shard adds >50μs, co-locate the listener on the shard thread: shard owns the HTTP accept for its slice of connections via SO_REUSEPORT (see §Q3 resolution).

**Backpressure contract (locked 2026-04-18):** SPSC channel is **bounded**. On overflow (hot shard falling behind):
- Drop the event at the listener boundary.
- Increment `beava_shard_inbox_full_total{shard="N"}` counter.
- Return HTTP **503 Service Unavailable** (HTTP push) or TCP error response code (TCP push) to the client.
- Client-side retries handle recovery. Matches Beava's existing ring-buffer-drop precedent from Phase 46 / CORR-10.

Bounded-queue size is tunable via `BEAVA_SHARD_INBOX_SIZE` (default: 64k events, resized-at-startup based on measured steady-state throughput). Do **not** block the listener thread — blocking any listener on a hot shard stalls all other shards when the dispatcher is shared.

### 7. Migration compatibility

**Single-shard mode (`BEAVA_SHARDS=1`) must be byte-compatible with current state format.**
- State directory layout: `data/shard-0/` contains what `data/` contains today.
- Event log format: unchanged.
- Snapshot format: extended with a `shard_count: u16` field at the top. N=1 snapshots load cleanly.

**Mismatch at boot (locked 2026-04-18):** if snapshot `shard_count != BEAVA_SHARDS`, the server **refuses to boot**. Actionable error message: `"snapshot shard_count=N but BEAVA_SHARDS=K — run 'tally reshard --from N --to K' then restart"`. No silent boot-empty (would cause data loss); no auto-reshard on first boot (would hide migrations from operators).
- Wire format: unchanged. TCP opcodes unchanged. HTTP endpoints unchanged.

Transition:
1. v1.2 initial — TPC plumbing lands with `BEAVA_SHARDS=1` default (Wave 0–1 scaffolding). Existing prod untouched.
2. v1.2 mid — users opt in to `BEAVA_SHARDS=N>1` on fresh data dirs. Wave 2–3 multi-shard routing + scatter-gather. Ship-gates per wave.
3. v1.2 final — default N = `num_cpus::get_physical()` in release builds, N=1 in debug. Re-sharding tool included for existing snapshots. Wave 4–5 complete.
4. v1.3 / Beava Cloud — migrate Option A (tokio current_thread) → Option D (compio) for the io_uring ceiling.

## Implementation plan (phase-level, for future milestone)

Not decomposed to task-level yet. This is a **v1.2 scope** or **Beava Cloud internal** body of work; v1.0-launch has shipped. Keeping waves intentional:

**Wave 0 — scaffolding & benchmarks**
- [ ] Add `shard_hint` trait method to `EventSource`. Default impl `hash(key)`. Wire through TCP + HTTP parsers. (Backward-compatible; always returns 0 for N_SHARDS=1.)
- [ ] Micro-bench: `hash(key)` overhead per-event. Budget: <100 ns per ingest.
- [ ] Micro-bench: SPSC channel roundtrip (listener → shard → response). Budget: <10 μs.

**Wave 1 — per-shard state store**
- [ ] Introduce `Shard` struct encapsulating per-shard state (HashMap, not DashMap), event log, watermark, dirty set.
- [ ] **Runtime-configurable `N_SHARDS` from day 1**, default 1. Skip the compile-time-1-first intermediate — it complicates the migration-compat story in §7 and saves no meaningful perf.
- [ ] Verify full test suite passes at N=1 before exercising N>1.

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
- [ ] Fork/replica **always re-hashes on ingest** by the downstream's N. Upstream `shard_hint` on the wire becomes an *optimization hint* (skip re-hash if upstream_N == downstream_N AND key-space partition matches), never a constraint. Extend `OP_LOG_FETCH` header to carry it as a hint only. **No `--reshard-from upstream-N` CLI flag** — silent re-sharding on every fork is the only path.

**Wave 5 — production readiness**
- [ ] Load test: sustained 1M+ EPS on 16-core box. Committed benchmark result.
- [ ] Property test: N=1 and N=8 on same event stream produce identical feature values.
- [ ] Failover: kill one shard thread (simulated) — verify graceful degradation / restart.
- [ ] Documentation: `docs/architecture-tpc.md` + updated `docs/operations.md` with shard sizing.

**Estimated effort:** 8-12 weeks for one engineer. Feasible as a single v1.2 milestone or as an internal Cloud initiative that forks from OSS at Wave 2.

## Risks

1. **Co-located join constraint is restrictive.** Users expressing joins across mis-partitioned streams today will need to re-declare. Mitigation: error at register time with actionable message; provide `SHARD_KEY` inference heuristic.
2. **Latency regression on the listener → shard hop.** SPSC handoff costs ~1μs but N is small. Monitor carefully. If unacceptable for the sub-ms read path, move the HTTP accept onto shard threads via SO_REUSEPORT.
3. **Pinning behavior on macOS.** `core_affinity` is best-effort on Darwin. On Apple Silicon (aarch64) specifically, the XNU scheduler **silently ignores thread-to-core pinning requests** and routes threads to P-cores vs E-cores via QoS class. This is a kernel limitation, not a crate bug. **Not a correctness problem** — shard ownership of state is enforced by the routing layer, not by CPU affinity. It only affects L1/L2 cache locality. Dev-machine throughput on Apple Silicon will be lower than a Linux prod box and will vary run-to-run. Accept; prod is Linux.
4. **Snapshot format migration.** Adding `shard_count` to snapshot header is forward-compatible, but N=1→N=K rewrites require the re-sharding tool and downtime. Document the migration clearly.
5. **Watermark lag across shards.** Lazy publish means global watermark trails per-shard watermark by the publish interval. Tune interval; alert on excessive lag via `beava_watermark_lag_seconds` gauge (the same gauge SRE-STREAM requested in the persona review).
6. **Re-sharding during fork is NOT a footgun (resolved).** Fork-replica always re-hashes on ingest by its own downstream N (see Q4 resolution). Upstream's `shard_hint` is an optimization hint, never a constraint. No CLI flag needed; behavior is implicit and correct by default.

## Prior Beava experiments to reuse

- `src/server/shard_probe.rs` — from Phase 14, measures per-shard contention. Directly informs placement strategy. **Architectural go/no-go gate:** shard_probe must report `cross_shard_fraction < 40%` on the 9-cell matrix for the TPC bet to be right for Beava's actual workload. If higher, re-examine before committing Wave 2+.
- `src/server/throughput.rs` — existing throughput harness. Extend with per-shard counters.
- Phase 40 (per-stream write locks) + Phase 41 (hot-path mutex removal) — the diagnostic work that motivated this. Direct pre-requisites.
- Commits `3818880` → `1cefc45` (per-event batch-coalesce revert) — cautionary tale; the lesson is that correctness-preserving refactors need benchmark gates. Apply the same pattern here (9-cell matrix gate on every wave).

## Benchmark expectations

| Metric | Today (main) | Target (TPC N=8) | Target (TPC N=16) |
|---|---:|---:|---:|
| Single-stream TCP push EPS | 314K | 0.9M–1.5M | 1.5M–2.5M |
| 9-cell matrix `complex-c8-x8` | baseline | +3×-4× | +5×-6× |
| Recovery time (4.7 GB state) | 7.0 s | 1.5 s (parallel) | 0.8 s |
| p99 feature-read latency | sub-ms | sub-ms (co-located) | sub-ms |
| Cross-shard query latency (`GET /streams`) | N/A | ~10 μs (scatter) | ~15 μs |
| Memory per entity | 616 B | unchanged | unchanged |

Target ranges — not point estimates. Achievable number is contingent on `shard_probe` cross_shard_fraction measured on the user's workload (see Prior Beava experiments). Iggy independently validates a 5M msgs/sec ceiling on a broker workload; Beava's stateful aggregation lands lower in the range. Monoio's 16-core ~3× Tokio result corroborates the bottom of the range.

**Ship gate for merging TPC to main:**
1. Every cell of the 9-cell matrix within −5% of baseline at N=1 (migration-compat gate).
2. ≥3× baseline on `complex-c8-x8` at N=CPU_COUNT (architecture gate).
3. `shard_probe` cross_shard_fraction <40% on the release benchmark workload (architectural-fit gate).

## Resolved design questions

*(All six original open questions closed via 2026-04-18 research — see `.planning/arch/TPC-RESEARCH.md` for evidence.)*

**Q1 — Default `N_SHARDS`: physical core count. No separate listener thread.**
- `BEAVA_SHARDS = num_cpus::get_physical()` (not logical). On a 16-core-with-HT box (32 logical, 16 physical), this yields 16 shards. Matches ScyllaDB / Redpanda / Iggy practice. HT siblings contend on L1/L2; running two shards per physical core has no upside for Beava's memory-bound work.
- No dedicated listener thread. Shards accept sockets themselves via `SO_REUSEPORT` on Linux (see Q3).
- Env override: `BEAVA_SHARDS=N` always wins. `BEAVA_SHARDS=1` compiles to current single-writer behavior (§7 migration compat).

**Q2 — macOS dev experience: soft-downshift in debug builds, accept best-effort pinning.**
- `cfg(debug_assertions)`: default `BEAVA_SHARDS=1`. Dev machines run many other processes; TPC benefits are invisible and the complexity slows iteration.
- Release builds: default `BEAVA_SHARDS=num_cpus::get_physical()`.
- On macOS (any build): `core_affinity::set_for_current()` is called but treated as best-effort. Warn-once at startup if pinning failed silently.
- On Apple Silicon specifically: see Risk #3 — kernel silently ignores core pinning; threads land via QoS class. Not a correctness problem.

**Q3 — SO_REUSEPORT strategy: shard-thread-owned accept on Linux, single-listener fallback on macOS.**
- **Linux (prod):** Each shard binds its own socket to the shared address:port via `SO_REUSEPORT`. The kernel distributes new TCP connections across sockets via a 4-tuple hash. Each shard's accept loop runs on its shard thread — no listener hop for the connect path. Note: a client's TCP connection landing on shard 2 doesn't imply its events key to shard 2; event-level shard_hint routing still happens inside the push handler (via internal SPSC queue when re-routing is needed). Accept the bounded re-route cost.
- **macOS (dev):** `SO_REUSEPORT` semantics are closer to BSD `SO_REUSEADDR` (address sharing, no `SO_REUSEPORT_LB`). With `BEAVA_SHARDS=1` default this is moot. If a user overrides to N>1 on macOS, fall back to single-listener + dispatcher and log a one-time warn.
- **Measurement gate:** Wave 0 micro-bench must show listener-dispatched overhead ≥10 μs vs SO_REUSEPORT in realistic steady-state traffic. If the gap is smaller, dispatcher is simpler and preferred.

**Q4 — Fork re-sharding: always re-hash on ingest; upstream N is irrelevant.**
- Replica's downstream shard count is independent of upstream's. At the replica ingest entrypoint, `shard_hint(event) = hash(event.key) mod downstream_N`. Upstream's `shard_hint` in `OP_LOG_FETCH` metadata is a *fast-path hint* (skip hashing if `upstream_N == downstream_N` AND key-space partition matches), not a constraint.
- Alternative — requiring downstream N to match upstream — is brittle (upstream may change across restarts; multiple upstreams impossible) and provides no correctness benefit.
- **No `--reshard-from upstream-N` CLI flag.** Silent re-sharding on every fork is the only path. Simpler; correct.

**Q5 — Python SDK impact: add `shard_key=` parameter to `@bv.stream`.**

```python
@bv.stream(shard_key="user_id")  # explicit; required when joined
class Transactions:
    user_id: str
    amount: float
    _event_time: int
```

- Omitted `shard_key=`: fall back to the stream's primary-key field (first dataclass field). Preserves current ergonomics for simple cases.
- Joins: require **explicit** `shard_key=` agreement across all joined streams; error at registration time with an actionable message.
- Multi-field shard keys supported as tuple (`shard_key=("region", "user_id")`); hashed via `ahash` server-side for determinism. **Missing-field behavior (locked 2026-04-18):** if any tuple field is absent on an event, the event is **rejected at ingest** (not routed to shard 0, not panicked) — increment `beava_events_dropped_total{reason="shard_key_missing"}` and return HTTP 400 / TCP error. Prevents shard-thread panic (Iggy RefCell lesson applied to Beava's field-extraction path).
- `shard_hint` is an internal wire-format field only (see Q4); Python users see `shard_key`.
- Pattern matches Faust/Bytewax prior art (both shard state by user-declared key at registration time).

**Q6 — Metrics: per-shard-labeled, reactor utilization first.**

| Metric | Type | Purpose |
|---|---|---|
| `beava_shard_reactor_utilization{shard="N"}` | gauge 0..1 | Fraction of last 1-sec window shard was not idle. **The single most important shard metric** (matches Scylla/Redpanda). |
| `beava_shard_inbox_depth{shard="N"}` | gauge | SPSC queue backlog. Non-zero steady state = shard falling behind. |
| `beava_shard_events_total{shard="N",outcome="accepted\|dropped"}` | counter | Per-shard ingest volume. |
| `beava_cross_shard_fanout_total{op="list_streams\|global_watermark\|scatter_read"}` | counter | Operations touching all shards. Should stay low. |
| `beava_shard_keys_owned{shard="N"}` | gauge | Distinct keys currently routed to shard. Exposes hot-shard imbalance. |
| `beava_shard_watermark_lag_seconds{shard="N"}` | gauge | Per-shard max-event-time − wall-clock. Global = min across shards. |
| `beava_shard_inbox_full_total{shard="N"}` | counter | Events dropped because shard SPSC inbox was full. Pairs with HTTP 503 / TCP error. (Backpressure contract — see §6.) |
| `beava_events_dropped_total{reason="..."}` | counter | Events rejected at ingest. Reasons: `shard_key_missing` (tuple field absent), `malformed_routing`, `payload_too_large`, `inbox_full`. |

The global `beava_watermark_lag_seconds` (unlabeled, requested by SRE-STREAM persona review) stays; it becomes a derived `min(beava_shard_watermark_lag_seconds)`. Generic name `beava_shard_lag_seconds` dropped as too vague.

## Still-open questions (unresolved by research)

1. **compio macOS-kqueue throughput vs Linux io_uring.** No published numbers; Iggy reports macOS is "good enough for dev" but doesn't quantify. **Wave 0 smoke-bench** on a macOS M-series laptop + matching Linux box will settle it. Expect 2–3× Linux advantage; not a show-stopper either way.
2. **NUMA on 32+ core boxes.** Target deploy is ≤16 cores for v1.2; Beava Cloud may go to 64 cores on NUMA boxes. Tokio and compio are both NUMA-naïve. Flag as a Beava Cloud-era design question; out of scope here.
3. **io_uring syscall batching strategy** (compio-era). Iggy flags heavy batching as the difference between winning and losing against epoll. Not relevant while we're on tokio/Option A; becomes a Wave 2+ sub-question if/when we swap to compio.

## What's on this branch vs main

**This branch (`arch/tpc-full-shard`):**
- `.planning/arch/TPC-SHARD-DESIGN.md` (this file) — design doc, hardened against 2026-04-18 research.
- `.planning/arch/TPC-RESEARCH.md` — companion research doc (runtime landscape, prior-art synthesis, open-question resolutions with source URLs).
- No code changes yet. First code PR will be Wave 0 scaffolding (`shard_hint` trait method, wired through TCP+HTTP, zero behavior change).

**Main:**
- v1.0-launch shipped. Current tokio multi-threaded + DashMap architecture unchanged.
- v1.1 adoption-first work (`.planning/milestones/v1.1-prep/ADOPTION-FIRST-PLAN.md`) proceeds in parallel on main.

**Merge strategy:** squash-merge into main only when Wave 2 + 9-cell matrix gate pass. Everything prior stays on the branch as experimentation.

---

*Design doc. No code changes on this branch yet. Add Wave 0 scaffolding as separate PRs against this branch before anything touches main.*
