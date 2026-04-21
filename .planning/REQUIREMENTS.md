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
- [x] **TPC-INFRA-02**: An operator can set `BEAVA_SHARDS=N` via env or `tally serve --shards N` via CLI; debug builds default to 1, release builds default to `num_cpus::get_physical()`; env wins over CLI when both are set. (Wave 1)
- [ ] **TPC-INFRA-03**: An operator can scrape `GET /metrics` and receive Prometheus-format metrics emitted via the `metrics` + `metrics-exporter-prometheus` crates; existing hand-rolled `/metrics` is migrated without regressing any current metric. (Wave 2)
- [ ] **TPC-INFRA-04**: An operator observes six per-shard labeled series — `beava_shard_reactor_utilization{shard}`, `beava_shard_inbox_depth{shard}`, `beava_shard_events_total{shard,outcome}`, `beava_shard_keys_owned{shard}`, `beava_shard_watermark_lag_seconds{shard}`, `beava_shard_inbox_full_total{shard}` — plus `beava_events_dropped_total{reason}` and `beava_cross_shard_fanout_total{op}`. (Wave 2)
- [ ] **TPC-INFRA-05**: A user calls `GET /debug/shards` and receives per-shard diagnostics (inbox depth, reactor utilization, keys owned, hot-shard warning when a shard's `keys_owned` exceeds 2× the fleet mean). (Wave 3)
- [ ] **TPC-INFRA-06**: A probe hitting `GET /ready` during shard recovery receives 503 until every shard finishes replaying its per-shard log; `GET /health` stays 200 throughout (process-is-alive, not ready-to-serve). (Wave 4)
- [ ] **TPC-INFRA-07**: The existing `BEAVA_ENTITIES_SHARDS` env var (DashMap tuning knob) is renamed or deprecated to avoid collision with the new `BEAVA_SHARDS` — deprecation emits a warn-once with a pointer to `BEAVA_SHARDS` docs. (Wave 2)

### TPC-PERF — Throughput, Routing, Pinning

- [x] **TPC-PERF-01**: A per-shard `Shard` struct owns its state (`AHashMap<Entity, Row>`, plain `HashSet<String>` dirty-set, `WatermarkState`, per-shard `EventLog` handle) with zero shared-mutable access from other shards. DashMap and ArcSwap remain as StateStore compat shims until Wave 4 then are deleted. (Wave 1)
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
- [x] **TPC-CORR-04** (**RELAXED in Phase 56**): At stream-registration time, joining two streams whose declared `shard_key=` values disagree MUST NOT reject registration. Instead, `register()` emits a logged `CrossShardJoinWarning` that names both streams, both shard keys, the join field, and the perf impact ("+1 inbox hop per event; +partition for join buffer"). The warning is surfaced via `/debug/warnings` (`cross_shard_joins` array) and the counter `beava_crossshard_joins_registered_total{join_id}` increments. The pipeline starts successfully; runtime path (Phase 56 TPC-CORR-08/09) handles the mismatch via hash(join.on)%N routing. Pre-Phase-56: this was a fatal `JoinShardKeyMismatch` error. (Wave 3 original; relaxed Phase 56)
- [ ] **TPC-CORR-05**: A proptest-driven parity harness feeds the same event stream to an N=1 engine and an N=8 engine; feature values for every key at every event-time bucket must be identical across all operators (filter, map, agg, join, fork). This is a pre-ship gate — merging to main requires this harness green. (Wave 5)
- [ ] **TPC-CORR-06**: On fork/replica ingest, the replica always re-hashes events via `hash(event.key) mod downstream_N`; the upstream `shard_hint` in `OP_LOG_FETCH` metadata is used only as a fast-path optimization (skip hashing when `upstream_N == downstream_N`), never as a constraint. No `--reshard-from` CLI flag exists. (Wave 4)

### TPC-DX — User-Facing Surfaces

- [x] **TPC-DX-01**: A Python SDK user can declare `@bv.stream(shard_key="user_id")` or `@bv.stream(shard_key=("region", "user_id"))`; if `shard_key=` is omitted, the stream falls back to the dataclass's first field (primary-key heuristic). Tuple keys are hashed server-side via `ahash` for deterministic shard assignment. (Wave 1)
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
- [ ] **TPC-PERSIST-05A**: After legacy engine removal, the `MODE=complex DURATION=60 CPUS=8 CLIENTS=8` benchmark on the reference box produces EPS within **−15%** of the Phase 52 baseline committed at `benchmark/fraud-pipeline/baseline-N8-complex.json`. Unlocks the Phase 53-deferred ship gate. (Phase 54)

### TPC-ARCH — Single Hot Path (Phase 54)

- [ ] **TPC-ARCH-01**: Every push entrypoint — TCP `handle_push_batch`, HTTP `http_push_single`/`http_push_batch`, and replica ingest — routes through the shard-thread SPSC dispatch at N=1 as well as N>1, so `push_with_cascade_on_shard` + fjall `PartitionHandle` is the sole hot path. `StateStore`, `PipelineEngine::push_internal`, and `push_batch_with_cascade_no_features` are deleted. `dashmap` and `arc-swap` are removed from `[dependencies]`. Grep-ZERO gates enforce this in CI. (Phase 54)

### TPC-CORR (continued) / TPC-SOURCE — Phase 55

- [x] **TPC-CORR-07**: A primary Stream event whose cascade produces a downstream Table row MUST place that row on the shard owning `hash(output_key) % N`, not on the input event's shard. `shard_key=` on streams becomes a pure source-ingress hint only (which shard accepts the event first); all downstream cascades shuffle by the downstream's own `key_field`. Same-shard fast path + batched cross-shard dispatch (end-of-batch coalesce; one `try_send` per (source_shard, target_shard) pair). Per-source-shard delivery cursor (`last_cascaded_lsn` in per-shard event log header) makes cascade recoverable from event-log replay. Cross-shard target inbox full → source shard blocks new ingress (503 / `SHARD_OVERLOAD`); acked PUSHes remain recoverable. Boot-time rematerialization: snapshot v9 bump triggers event-log replay through new cascade path. Perf gate: `MODE=complex DURATION=60 CPUS=8 CLIENTS=8` ≥ 85% of Phase 54 Wave 5 baseline (1,339,446 EPS) = **≥ 1,138,529 EPS**. (Phase 55)
- [x] **TPC-SOURCE-01**: Python SDK gains `@bv.source_table(key=K, entity_ttl=...)` decorator declaring a CDC-style keyed input. Explicit wire commands — TCP opcodes `UPSERT_TABLE_ROW` / `DELETE_TABLE_ROW` / `UPSERT_TABLE_BATCH` / `DELETE_TABLE_BATCH`; HTTP routes `POST /table/{name}` (upsert), `DELETE /table/{name}/{key}` (single delete), `POST /table/{name}/batch` (upsert batch), `POST /table/{name}/batch/delete` (delete batch). Batch variants accept ≥ 10K rows/call. UPSERT is full-replace (CDC-native, idempotent); DELETE is hard-delete + pending-retraction marker written to event log (Phase 57 consumes). `source_lsn: u64` (opaque, no monotonicity check) is stored per row and echoed on every ack (array ack on batch, in input order) for resumable replication. Source-table writes do NOT fire cascades in v1.2 (Phase 57 territory); source tables are passive enrichment targets for Phase 56's EnrichFromTable. (Phase 55)

### TPC-CORR (continued) — Phase 56

- [x] **TPC-CORR-08**: `EnrichFromTable` MUST return the correct enrichment regardless of which shard the driving event lands on. When the right-side key hashes to a different shard than the current shard, the operator dispatches `ShardOp::ReadEntityAt { target_shard, table_name, key, reply }` (single-key) or `ShardOp::ReadEntityBatch { target_shard, table_name, keys, reply }` (per-target coalesced) and blocks the source shard on the oneshot reply. When `hash(key) % N == current_shard`, the operator reads directly from the local `PartitionHandle` (same-shard fast path — zero inbox hop). Missing rows preserve existing `Missing` semantics (null-safe enrichment fields; `beava_enrich_missing_total{table}` increments). Target-inbox-full propagates `BeavaError::ShardOverload` upward → client sees 503 / `SHARD_OVERLOAD` (whole batch retry). Metrics: `beava_enrich_cross_shard_total{table}`, `beava_enrich_intra_shard_total{table}`, `beava_enrich_missing_total{table}`. Verified by `tests/cross_shard_enrich_from_table.rs` with `Txns(shard_key=user_id)` on shard-J + `@bv.source_table(Countries, key=country_code)` on shard-K (J≠K) asserting the joined output has Country fields populated. (Phase 56)
- [x] **TPC-CORR-09**: `StreamStreamJoin` with mismatched left/right `shard_key=` declarations MUST produce correct joined events by routing both sides to the shard owning `hash(join.on) % N`. The buffer lives on the join-owning shard in a dedicated fjall partition `ssj-<join_id>/` (single-writer-per-shard invariant preserved). Source-shard dispatch: accumulate per-batch, coalesce, `try_send` `ShardOp::SsjInsert { side: Left|Right, join_key, event, reply }` to the target shard; the target evaluates the match inline and emits any resulting joined output to its own downstream via the existing (Phase 55) cascade path. When `shard_key=join.on` on both sides (co-located), no relaxation applies — no extra hop. Register-time `TPC-CORR-04` relaxation: `register()` no longer rejects; emits `CrossShardJoinWarning` + increments `beava_crossshard_joins_registered_total{join_id}`; `/debug/warnings` gains `cross_shard_joins: [{join_id, left_shard_key, right_shard_key, on_field, perf_note}]`. Metrics: `beava_ssj_cross_shard_total{join_id}`, `beava_crossshard_joins_registered_total{join_id}`. Verified by `tests/cross_shard_stream_stream_join.rs` + `tests/register_crossshard_join_warning.rs`. (Phase 56)

### TPC-CORR (continued) — Phase 57

- [x] **TPC-CORR-10**: Retractions MUST propagate end-to-end through cross-shard joins and cascades. Every emitted downstream row tracks its contributing input events (`contributing_inputs: {primary_event_id, source_table_keys?, left_event_id?, right_event_id?}`) co-located with the row in its fjall partition. Tombstones / deletes on any tracked input trigger `ShardOp::RetractDownstream { target_shard, stream_name, row_key, reason, depth }` to the owning shard of every affected downstream output; target-side idempotency (no-op on already-retracted rows). Source-table DELETE's PendingRetraction markers (from Phase 55-02) are consumed here — EnrichFromTable retracts every downstream row whose `source_table_keys` contains the deleted key. StreamStreamJoin tombstones on L or R retract every previously-emitted joined output referencing the tombstoned side. Cascade depth capped at 16 hops (D-B5) — overflow raises `BeavaError::RetractionDepthExceeded` + `beava_retraction_depth_exceeded_total`. Late retractions (event_time < watermark - history_ttl) skip with `tracing::warn!` + `beava_retraction_beyond_history_total{operator}` + `/debug/warnings.retraction_beyond_history` dedupe'd at 60s. Metrics: `beava_retractions_sent_total{operator,reason}`, `beava_retractions_applied_total{operator}`, `beava_retractions_nooped_total{operator}`, `beava_retraction_beyond_history_total{operator}`, `beava_retraction_depth_exceeded_total`. Perf: ≤ 10% write-path overhead with zero retractions firing (Phase 56 baseline 1,195,914 EPS → floor **1,076,322 EPS** on `MODE=complex DURATION=60 CPUS=8 CLIENTS=8` with `BEAVA_SHARD_INBOX_SIZE=1048576`). Schema: snapshot v10 bump; `contributing_inputs` field uses `#[serde(default)]` so pre-v10 rows load as `None` (no retraction possible — treated same as beyond-history). Verified by `tests/crossshard_source_table_delete_retraction.rs`, `tests/crossshard_ssj_retraction.rs`, `tests/late_retraction_warning.rs`, `tests/retraction_depth_guard.rs`, extended `tests/sharding_parity.rs`. (Phase 57)

### TPC-PERF (continued) — Phase 58

- [x] **TPC-PERF-08**: The TCP PUSH hot path MUST NOT spawn a per-connection tokio task. On Linux, each shard thread binds its own `TcpListener` via `SO_REUSEPORT` (reusing `bind_reuseport_tcp` from Phase 50) and runs a `current_thread` tokio runtime that accepts + handles PUSH frames inline via `FuturesUnordered` (default cap `BEAVA_MAX_CONNS_PER_SHARD=256`). On macOS, each shard owns a dedicated `std::thread` running a blocking `TcpListener::accept` loop (D-B1); `BEAVA_SHARDS_SINGLE_LISTENER=1` selects the legacy single-accept fallback (D-B2). HTTP PUSH path (axum) is UNCHANGED — Phase 59 handles wire-format, not runtime. Replica ingest (TCP opcode path) uses the same per-shard accept pattern. Observable gates: (a) `tokio::runtime::task::*` combined ≤ 15% of samply leaf samples under `MODE=complex N=8` (currently ~60% per Phase 54 pprof); (b) per-shard listener count = `BEAVA_SHARDS` (Linux: N LISTEN sockets on the same port; macOS: N accept threads); (c) `MODE=complex DURATION=60 CPUS=8 CLIENTS=8 BEAVA_SHARD_INBOX_SIZE=1048576` aggregate ≥ **1,621,616 EPS** (= Phase 57 baseline 1,297,293 × 1.25); (d) p99 per-event push latency must NOT regress vs Phase 57's 30,667.5 µs client-observed median-of-p99. Verified by `tests/tokio_spawn_absence_smoke.rs`, `tests/per_shard_listener_smoke.rs`, `tests/http_push_still_works.rs`, and `scripts/samply-probe-tokio-share.sh`. (Phase 58)

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
| 51 | cross-shard-queries-joins | TPC-INFRA-05, TPC-PERF-05, TPC-PERF-06, TPC-CORR-04 (relaxed in Phase 56) |
| 52 | event-log-recovery-ship-gate | TPC-INFRA-06, TPC-CORR-02, TPC-CORR-05, TPC-CORR-06, TPC-PERF-07, TPC-DX-03, TPC-DX-04 |
| 53 | fjall-state-backend | TPC-PERSIST-01, TPC-PERSIST-02, TPC-PERSIST-03, TPC-PERSIST-06 (TPC-PERSIST-04 + TPC-PERSIST-05 deferred to Phase 54) |
| 54 | legacy-engine-removal | TPC-ARCH-01, TPC-PERSIST-04, TPC-PERSIST-05A (TPC-PERSIST-04 + TPC-PERSIST-05A deferred from Phase 53 — closed here) |
| 55 | stream-table-cascade-crossshard-and-source-tables | TPC-CORR-07, TPC-SOURCE-01 |
| 56 | enrich-from-table-and-stream-stream-join-crossshard | TPC-CORR-04 (relaxation), TPC-CORR-08, TPC-CORR-09 |
| 57 | retraction-across-crossshard-joins | TPC-CORR-10 |
| 58 | tokio-connection-handling-rewrite | TPC-PERF-08 |

**Coverage:** 37/37 requirements mapped (1 + 3 + 9 + 4 + 7 + 4 + 3 + 2 + 2 + 1 + 1 = 37; adds 6 TPC-PERSIST-* + 1 TPC-ARCH-* + 2 Phase-55 + 2 Phase-56 + 1 Phase-57 + 1 Phase-58 requirements; TPC-CORR-04 is re-delivered by Phase 56 as a relaxation)
