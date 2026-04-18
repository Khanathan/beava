# Requirements: Beava — milestone v1.2 Thread-Per-Core + Full Key-Shard

**Defined:** 2026-04-18
**Core Value:** A skeptical engineer evaluating Beava on github.com can go from landing on the repo to correct, live feature values in under 60 seconds — from any language.
**Milestone Goal:** Intra-node scaling via thread-per-core + full key-shard — eliminate DashMap contention and cross-core cache-line bouncing to reach 1.5M–2.5M EPS on a 16-core box (5-6× current baseline), preserving correctness + migration-compat with today's single-shard state format.
**Sources of truth:** `.planning/arch/TPC-SHARD-DESIGN.md` + `.planning/arch/TPC-RESEARCH.md` + `.planning/research/SUMMARY.md`.
**Predecessor:** v1.0-launch requirements archived in `.planning/milestones/v1.0-launch-REQUIREMENTS.md` (all HTTP-01..10, CORR-01..10, INFRA-01..10, CONTENT-01..11, SHIP-02..05 shipped).

## v1.2 Requirements

Requirements scoped to the v1.2 milestone. Each maps to a roadmap phase (48–52, Waves 0–4+5). REQ-IDs use `TPC-[CATEGORY]-[NN]` to disambiguate from prior-milestone CORR/INFRA namespaces.

### TPC-INFRA — Plumbing, Config, Observability

- [x] **TPC-INFRA-01**: A developer running the test suite sees `EventSource::shard_hint(&self, event) -> u32` wired through TCP and HTTP parsers on every push path; default impl hashes the primary key. At `N_SHARDS=1` the value is always 0 and routing is a no-op. (Wave 0)
- [ ] **TPC-INFRA-02**: An operator can set `BEAVA_SHARDS=N` via env or `tally serve --shards N` via CLI; debug builds default to 1, release builds default to `num_cpus::get_physical()`; env wins over CLI when both are set. (Wave 1)
- [ ] **TPC-INFRA-03**: An operator can scrape `GET /metrics` and receive Prometheus-format metrics emitted via the `metrics` + `metrics-exporter-prometheus` crates; existing hand-rolled `/metrics` is migrated without regressing any current metric. (Wave 2)
- [ ] **TPC-INFRA-04**: An operator observes six per-shard labeled series — `beava_shard_reactor_utilization{shard}`, `beava_shard_inbox_depth{shard}`, `beava_shard_events_total{shard,outcome}`, `beava_shard_keys_owned{shard}`, `beava_shard_watermark_lag_seconds{shard}`, `beava_shard_inbox_full_total{shard}` — plus `beava_events_dropped_total{reason}` and `beava_cross_shard_fanout_total{op}`. (Wave 2)
- [ ] **TPC-INFRA-05**: A user calls `GET /debug/shards` and receives per-shard diagnostics (inbox depth, reactor utilization, keys owned, hot-shard warning when a shard's `keys_owned` exceeds 2× the fleet mean). (Wave 3)
- [ ] **TPC-INFRA-06**: A probe hitting `GET /ready` during shard recovery receives 503 until every shard finishes replaying its per-shard log; `GET /health` stays 200 throughout (process-is-alive, not ready-to-serve). (Wave 4)
- [ ] **TPC-INFRA-07**: The existing `BEAVA_ENTITIES_SHARDS` env var (DashMap tuning knob) is renamed or deprecated to avoid collision with the new `BEAVA_SHARDS` — deprecation emits a warn-once with a pointer to `BEAVA_SHARDS` docs. (Wave 2)

### TPC-PERF — Throughput, Routing, Pinning

- [ ] **TPC-PERF-01**: A per-shard `Shard` struct owns its state (`AHashMap<Entity, Row>`, plain `HashSet<String>` dirty-set, `WatermarkState`, per-shard `EventLog` handle) with zero shared-mutable access from other shards. DashMap and ArcSwap remain as StateStore compat shims until Wave 4 then are deleted. (Wave 1)
- [ ] **TPC-PERF-02**: On Linux, every shard thread is pinned via `core_affinity::set_for_current` to a specific physical core at startup; on macOS, pinning is attempted and a warn-once fires if the kernel silently ignores it (Apple Silicon P/E-core QoS). (Wave 2)
- [ ] **TPC-PERF-03**: Listener threads hand events to shard threads via `crossbeam-channel::bounded` SPSC queues (one queue per listener→shard pair); default capacity is `BEAVA_SHARD_INBOX_SIZE=65536`, overridable via env. Zero-copy handoff via `bytes::Bytes`. (Wave 2)
- [ ] **TPC-PERF-04**: On Linux, each shard binds its own TCP + HTTP accept socket to the shared port via `SO_REUSEPORT`; the kernel 4-tuple-hashes new connections across shards. On macOS, a single listener thread + dispatcher is used (no `SO_REUSEPORT_LB` on Darwin). (Wave 2)
- [ ] **TPC-PERF-05**: A user calling `GET /streams` receives the fleet-wide stream list via scatter-gather — the handler fans out to every shard via `futures::join_all`, merges results, returns a single response. p99 latency <15 μs added vs a point query. (Wave 3)
- [ ] **TPC-PERF-06**: Each shard publishes its per-stream max event-time to a global atomic once per `N_EVENTS` (batched); the global watermark for any stream is computed as `min` across per-shard atomics. Per-entity TTL eviction uses the shard-local watermark only (no cross-shard read). (Wave 3)
- [ ] **TPC-PERF-07**: The 9-cell benchmark matrix gains a **Pareto-workload cell** (80/20 key distribution) that exercises hot-shard behavior; the standard 9 cells continue to validate uniform-hash throughput. Ship-gate: uniform cells ≥3× baseline at N=CPU_COUNT; Pareto cell cross_shard_fraction <40%. (Wave 5)

### TPC-CORR — Correctness Guards, Determinism

- [ ] **TPC-CORR-01**: When a shard's SPSC inbox is full, the listener drops the event, increments `beava_shard_inbox_full_total{shard="N"}`, and returns HTTP 503 (HTTP push) or the TCP error code `SHARD_OVERLOAD` (TCP push). The listener thread never blocks on a hot shard. (Wave 2)
- [ ] **TPC-CORR-02**: If the snapshot's `shard_count: u16` field disagrees with `BEAVA_SHARDS`, the server refuses to boot and emits the actionable error `"snapshot shard_count=N but BEAVA_SHARDS=K — run 'tally reshard --from N --to K' then restart"`. No silent boot-empty. (Wave 4)
- [ ] **TPC-CORR-03**: When an event arrives without a declared tuple `shard_key` field (e.g., `shard_key=("region","user_id")` but event has no `region`), the ingest path rejects the event, increments `beava_events_dropped_total{reason="shard_key_missing"}`, and returns HTTP 400 (HTTP) or the TCP error code `SHARD_KEY_MISSING` (TCP). Shard threads never panic on field-extraction failure. (Wave 2)
- [ ] **TPC-CORR-04**: At stream-registration time, joining two streams whose declared `shard_key=` values disagree produces a fatal `JoinShardKeyMismatch` error that names both streams, both shard keys, and suggests the exact decorator fix. Registration is rejected; the engine does not start with the inconsistent pipeline. (Wave 3)
- [ ] **TPC-CORR-05**: A proptest-driven parity harness feeds the same event stream to an N=1 engine and an N=8 engine; feature values for every key at every event-time bucket must be identical across all operators (filter, map, agg, join, fork). This is a pre-ship gate — merging to main requires this harness green. (Wave 5)
- [ ] **TPC-CORR-06**: On fork/replica ingest, the replica always re-hashes events via `hash(event.key) mod downstream_N`; the upstream `shard_hint` in `OP_LOG_FETCH` metadata is used only as a fast-path optimization (skip hashing when `upstream_N == downstream_N`), never as a constraint. No `--reshard-from` CLI flag exists. (Wave 4)

### TPC-DX — User-Facing Surfaces

- [ ] **TPC-DX-01**: A Python SDK user can declare `@bv.stream(shard_key="user_id")` or `@bv.stream(shard_key=("region", "user_id"))`; if `shard_key=` is omitted, the stream falls back to the dataclass's first field (primary-key heuristic). Tuple keys are hashed server-side via `ahash` for deterministic shard assignment. (Wave 1)
- [ ] **TPC-DX-02**: At `N_SHARDS > 1`, a stream with no declared `shard_key=` emits a `ShardKeyMissingWarning` on `/debug/warnings` naming the stream and recommending `@bv.stream(shard_key=...)`; the stream still runs (all events route to shard 0 deterministically). At `N=1`, no warning fires. (Wave 2)
- [ ] **TPC-DX-03**: An operator runs `tally reshard --from N --to K --data-dir /var/lib/beava --output /var/lib/beava-new` and receives an atomic, offline-safe migration of per-shard logs and snapshot; downtime = tool runtime; the original data dir is untouched until a `--replace` flag is passed. (Wave 4)
- [ ] **TPC-DX-04**: A user reading `docs/architecture-tpc.md` (new) can understand the shard model, routing, joins, and operational posture end-to-end; `docs/operations.md` gains a "Shard sizing & hot-shard diagnosis" section citing `beava_shard_keys_owned` and `shard_probe`. (Wave 5)

### TPC-PERSIST — Durable Per-Shard State (fjall backend, Phase 53)

- [ ] **TPC-PERSIST-01**: Every `Shard` instance owns a `fjall::Partition` at `data/shard-N/fjall/` for entity state; get/set/iterate go through fjall with identical semantics to the pre-Phase-53 AHashMap path; no cross-shard fjall contention (each shard has its own partition or keyspace). (Phase 53)
- [ ] **TPC-PERSIST-02**: A process killed with SIGKILL mid-workload restarts, replays fjall's WAL, and serves reads with feature values identical to the last-acknowledged write — **without running the event-log replay path**. Snapshot-replay becomes optional diagnostic, not the crash-recovery critical path. (Phase 53)
- [ ] **TPC-PERSIST-03**: Operator runs `tally migrate-to-fjall --data-dir ./data [--replace]`; tool converts v8 in-memory snapshots to per-shard fjall partitions in-place; original v8 snapshot preserved as `snapshot.v8.bak` until `--replace` passes; downtime = tool runtime. (Phase 53)
- [ ] **TPC-PERSIST-04**: A soak test with 100 GB of entity state on a 32 GB RAM box sustains sub-ms p99 feature-read latency; validates fjall bloom filters + block cache behavior for out-of-RAM workloads. (Phase 53)
- [ ] **TPC-PERSIST-05**: Performance regression vs Phase 52 in-memory baseline is bounded: 9-cell matrix + Pareto cell at N=CPU_COUNT regress by at most **−15%**; fork/replica parity tests (N=1↔N=8 proptest from Phase 52) remain green with fjall-backed state. (Phase 53)
- [ ] **TPC-PERSIST-06**: `docs/architecture-tpc.md` gains a "State durability (fjall)" section; `docs/operations.md` documents `BEAVA_FJALL_*` tuning knobs (block cache size, compaction threads, WAL fsync cadence) and restart/recovery semantics. (Phase 53)

## Future Requirements

Deferred to a future v1.2.x polish milestone or v1.3+:

- Co-located multi-join support (joins today require all joined streams to declare identical `shard_key=`; future work: broadcast-small-side / stream-stream re-shard).
- Live `BEAVA_SHARDS` reconfiguration (today: requires `tally reshard` + restart).
- `compio` runtime migration for the Linux `io_uring` ceiling (v1.3 / Beava Cloud).
- NUMA-aware shard placement on 32+ core boxes (Beava Cloud era).
- Fork/replica LSN-based dedup for upstream rolling-restart double-emit window (TBD — Wave 4 sub-design).
- Hot-key salting framework support (today: application-level concern; Beava surfaces the problem via metrics only).

## Out of Scope

Explicitly excluded from v1.2:

- **compio runtime migration** — v1.3 / Beava Cloud. TPC on `tokio::current_thread` is the v1.2 bet.
- **`monoio`, `glommio`** — rejected (see `TPC-RESEARCH.md`).
- **Python SDK breaking changes** — existing pipelines keep working; fallback behavior preserves v1.1-and-earlier compat at N=1.
- **Live reshard without downtime** — offline `tally reshard` only.
- **Web UI for shard observability** — `/debug/shards` JSON + Prometheus scrape is sufficient.
- **Multi-node scale-out** — v1.3+ roadmap.

## Traceability

Filled by roadmapper 2026-04-18. Maps each REQ-ID to the phase that delivers it.

| Phase | Name | Delivers |
|-------|------|----------|
| 48 | shard-hint-scaffolding | TPC-INFRA-01 |
| 49 | per-shard-state-store | TPC-INFRA-02, TPC-PERF-01, TPC-DX-01 |
| 50 | multi-shard-routing | TPC-INFRA-03, TPC-INFRA-04, TPC-INFRA-07, TPC-PERF-02, TPC-PERF-03, TPC-PERF-04, TPC-CORR-01, TPC-CORR-03, TPC-DX-02 |
| 51 | cross-shard-queries-joins | TPC-INFRA-05, TPC-PERF-05, TPC-PERF-06, TPC-CORR-04 |
| 52 | event-log-recovery-ship-gate | TPC-INFRA-06, TPC-CORR-02, TPC-CORR-05, TPC-CORR-06, TPC-PERF-07, TPC-DX-03, TPC-DX-04 |

**Coverage:** 24/24 requirements mapped (1 + 3 + 9 + 4 + 7 = 24)
