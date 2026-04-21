# Beava Roadmap — v1.2 Thread-Per-Core + Full Key-Shard

## Milestones

- [x] **v1.0 -- Foundation** (Phases 1-5) -- Complete 2026-04-09 -- archived
- [x] **v1.1–v1.3 -- Event Log, Pipelines, Concurrency** (Phases 6-15) -- Complete 2026-04-12
- [x] **v2.0 -- API & Engine** (Phases 16-19) -- Complete 2026-04-13
- [x] **v2.1 -- Launch** (Phase 20) -- Engineering complete 2026-04-14 (live-run ops pending, calendar-gated) -- `.planning/milestones/v2.1-ROADMAP.md`
- [x] **v0 -- Restructure + Data-Scientist Fork** (Phases 21-38) -- Phases 21-27, 36-37 complete; Phases 35, 38 planned.
- [x] **v1.0-launch -- Public Launch Readiness** (Phases 45-47) -- Engineering complete 2026-04-17 -- `.planning/milestones/v1.0-launch-ROADMAP.md`
- [ ] **v1.2 -- Thread-Per-Core + Full Key-Shard** (Phases 48, 49, 50, 50.5, 51, 52, 53, 54) -- Active 2026-04-18
- [ ] **v1.3 -- Cross-Shard Correctness + Perf Runway** (Phases 55-64) -- Planned 2026-04-20 (correctness: 55 Stream→Table + source tables; 56 EnrichFromTable + SSJ; 57 retraction. perf: 58 Tokio rewrite; 59 binary wire; 60 hot-key salting; 61 metrics hoist; 62 allocator pooling; 63 fjall tuning; 64 Rust bench client. Target: 2.5–3M EPS complex N=8 on reference laptop.)

## Phases

- [x] **Phase 48: 48-shard-hint-scaffolding** — Wire `EventSource::shard_hint()` through every push path; establish micro-bench gates (no routing change at N=1) (completed 2026-04-18)
- [x] **Phase 49: 49-per-shard-state-store** — Introduce `Shard` struct with per-shard AHashMap state; `BEAVA_SHARDS` env + CLI flag; full test suite green at N=1 (completed 2026-04-18 — `49-VERIFICATION.md` status: passed)
- [x] **Phase 50: 50-multi-shard-routing** — SO_REUSEPORT shard accept on Linux, SPSC channels, core_affinity pinning, backpressure contract, per-shard labeled metrics (completed 2026-04-19 — gaps closed by Phase 50.5)
- [x] **Phase 50.5: 50.5-shard-thread-completion** — Wire the Phase 50 shard thread receiver to actually own per-shard state (currently a stub that discards SPSC events, per `50-DEBUG-SESSION.md`). Unlocks the real TPC parallelism that Phase 50 promised but never delivered. Split ship-gate: **macOS dev ≥1.5× baseline (~460K EPS)** / **Linux prod ≥3× baseline (~918K EPS)**. Linux CI reference-box run is the merge gate. (completed 2026-04-19)
- [x] **Phase 51: 51-cross-shard-queries-joins** — `GET /streams` scatter-gather, `JoinShardKeyMismatch` at register time, lazy global watermark, `GET /debug/shards` hot-shard visibility (completed 2026-04-19)
- [x] **Phase 52: 52-event-log-recovery-ship-gate** — Per-shard log layout, parallel recovery, `tally reshard` tool, snapshot v8 hard-fail guard, fork/replica re-hash, N=1↔N=8 proptest parity, 1M+ EPS load test, architecture docs (completed 2026-04-19)
- [x] **Phase 53: 53-fjall-state-backend** — Replace per-shard in-memory AHashMap state with `fjall` LSM-tree backend (per-shard partitions); state is durable-by-default, unbounded size, crash-safe without snapshot replay (completed 2026-04-19 — engineering-complete; 4/6 TPC-PERSIST-* closed; PERSIST-04 soak + PERSIST-05A bench deferred to Phase 54 because pprof showed legacy `PipelineEngine::push_internal` bypasses the fjall path at N=1 — see `53-VERIFICATION.md`)
- [x] **Phase 54: 54-legacy-engine-removal** — Retire the DashMap-backed `StateStore` and `PipelineEngine::push_internal` / `push_batch_with_cascade_no_features` legacy paths. Route every push entrypoint (TCP `handle_push_batch`, HTTP `http_push_*`, replica ingest) through shard-thread SPSC dispatch at N=1 as well as N>1, so `push_with_cascade_on_shard` + fjall is the sole hot path. Closes Phase 52-10 BLOCKER and unlocks the deferred Phase 53 perf gates (`-15%` 9-cell bench + 100 GB soak). (completed 2026-04-20 — engineering-complete; TPC-PERSIST-04 soak `human_needed`, evidence file gated.)
- [x] **Phase 55: 55-stream-table-cascade-crossshard-and-source-tables** — Fix the Stream→Table aggregation correctness bug (downstream rows on `hash(output_key) % N`), introduce source tables (`@bv.source_table`) with UPSERT/DELETE + `source_lsn` echo. (completed 2026-04-20 — engineering-complete; SC-6 N>1 boot rematerialization fan-out deferred to 55-NEXT #1 per user acceptance 2026-04-20; perf gate 1,246,190 EPS ≥ 1,138,529 floor.)
- [x] **Phase 56: 56-enrich-from-table-and-stream-stream-join-crossshard** — Make EnrichFromTable and StreamStreamJoin correct across shards. Three new `ShardOp` variants (`ReadEntityAt`, `ReadEntityBatch`, `SsjInsert`) + same-shard fast paths + per-batch coalesce. TPC-CORR-04 relaxed from hard-reject to `CrossShardJoinWarning`. (completed 2026-04-21 — engineering-complete; TPC-CORR-08 + TPC-CORR-09 closed; TPC-CORR-04 relaxation landed. Default-pipeline perf gate 1,195,914 EPS ≥ 1,059,261 floor (PASSED, +12.9%). Cross-shard-scenario SC-5 `human_needed` — blocked on Phase 55 SDK source-table wire-registration gap; remediation filed as 56-NEXT #6.)
- [x] **Phase 57: 57-retraction-across-crossshard-joins** — Retraction propagation through cross-shard joins and cascades. Every emitted downstream row tracks `contributing_inputs` (primary_event_id + source_table_keys + left/right_event_id); tombstones / source-table DELETEs trigger `ShardOp::RetractDownstream` fan-out to owning shards with 16-hop depth guard + history_ttl warn+skip + 60s dedupe'd `/debug/warnings.retraction_beyond_history`. (completed 2026-04-21 — engineering-complete; **TPC-CORR-10 closed**; all SC-1..SC-4 + D-B5 depth guard GREEN. Default-pipeline perf gate 1,297,293 EPS ≥ 1,076,322 floor (PASSED, +20.5% headroom; +8.5% vs Phase 56 baseline). Advisory D-D4 retraction-firing micro-bench deferred on same Phase 55 SDK gap as Phase 56 SC-5 / 56-NEXT #6; NOT a gate per plan.)

## Phase Details

### Phase 48: 48-shard-hint-scaffolding
**Goal**: Every push path carries a computed `shard_hint` value — always 0 at N=1 — with confirmed sub-100 ns hashing overhead and sub-10 μs SPSC roundtrip, establishing the no-regression baseline for the entire TPC migration.
**Depends on**: Nothing (first v1.2 phase; v1.0-launch complete)
**Requirements**: TPC-INFRA-01
**Success Criteria** (what must be TRUE):
  1. A developer running `cargo test` with N_SHARDS=1 sees all existing tests pass unchanged — `shard_hint()` returns 0 for every event and no routing branch is taken.
  2. A developer running the Wave 0 micro-bench suite sees `hash(key)` overhead reported as <100 ns per event and SPSC channel roundtrip as <10 μs.
  3. A developer inspecting TCP (`handle_push_core_ex`) and HTTP (`http_push_single`, `http_push_batch`) push paths can verify `shard_hint()` is computed immediately after parse on every event.
  4. The 9-cell benchmark matrix run at N=1 after scaffolding is within ±1% of the committed v1.0-launch baseline (no performance regression from the annotation).
**Plans**: 3 plans
Plans:
- [x] 48-01-PLAN.md — TDD: `src/routing/shard_hint.rs` trait + ahash default impl + TCP/HTTP call-site wiring (Wave 1)
- [x] 48-02-PLAN.md — Criterion bench `benches/shard_scaffold.rs` with 3 event shapes, <100 ns gate (Wave 2)
- [x] 48-03-PLAN.md — Nightly CI workflow `bench-nightly.yml` + committed baseline `benchmark/shard_scaffold/README.md` (Wave 3)
**UI hint**: no

### Phase 49: 49-per-shard-state-store
**Goal**: The `Shard` struct — owning per-shard AHashMap state, plain HashSet dirty-set, per-shard WatermarkState, and per-shard EventLog handle — exists and is the sole data path at N=1, with `BEAVA_SHARDS` configurable from day one and DashMap retained as a compatibility shim.
**Depends on**: Phase 48
**Requirements**: TPC-INFRA-02, TPC-PERF-01, TPC-DX-01
**Success Criteria** (what must be TRUE):
  1. An operator sets `BEAVA_SHARDS=4` via env or `tally serve --shards 4` and the server starts with 4 shard slots allocated; when both are provided, the env var takes precedence.
  2. A developer running `cargo test` at N=1 sees the full integration test suite green — state is owned by `Shard-0`, routing through `ShardRouter` is a no-op, and output is byte-identical to the v1.0-launch baseline.
  3. A Python SDK user can declare `@bv.stream(shard_key="user_id")` or `@bv.stream(shard_key=("region","user_id"))`; omitting `shard_key=` falls back to the first field with no error at N=1.
  4. A developer inspects `src/shard/mod.rs` and confirms `Shard.state` is `AHashMap` (not DashMap) and `Shard.dirty_set` is a plain `HashSet` — no lock on the per-shard hot path.
  5. The 9-cell benchmark matrix run at N=1 after this phase is within −5% of the v1.0-launch baseline (migration-compat gate).
**Plans**: 6 plans
Plans:
- [x] 49-01-PLAN.md — num_cpus dep + BEAVA_SHARDS/--shards config surface; warn-once + N=1 enforcement; startup INFO log (Wave 1) (shipped — verification passed)
- [x] 49-02-PLAN.md — TDD: ShardedStateStore trait + Shard struct skeleton (AHashMap, HashSet dirty, EventLog) + ShardedStateStoreV1 (Wave 2) (shipped — verification passed)
- [x] 49-03-PLAN.md — TDD: WatermarkTracker relocation to WatermarkState in Shard; DashMap type deleted; golden N=1 regression test (Wave 3) (shipped — verification passed; DashMap deletion tracked in 52-10 blocker)
- [x] 49-04-PLAN.md — TDD: StreamDefinition.shard_key + #[serde(default)]; Python @bv.stream(shard_key=...) surface (Wave 2, parallel with 49-02)
- [x] 49-05-PLAN.md — Integration: ShardedStateStoreV1 wired into push path at N=1; full test suite green (Wave 4)
- [x] 49-06-PLAN.md — Ship-gate: golden watermark integration test + 9-cell matrix within -5% baseline; human verify (Wave 5)
**UI hint**: no

### Phase 50: 50-multi-shard-routing
**Goal**: Multiple pinned shard threads accept and process events concurrently — SO_REUSEPORT on Linux, SPSC channels from listeners to shards, core_affinity pinning, drop-on-full backpressure returning HTTP 503, per-shard labeled Prometheus metrics, and ≥3× throughput gate on the 9-cell matrix at N=CPU_COUNT.
**Depends on**: Phase 49
**Requirements**: TPC-INFRA-03, TPC-INFRA-04, TPC-INFRA-07, TPC-PERF-02, TPC-PERF-03, TPC-PERF-04, TPC-CORR-01, TPC-CORR-03, TPC-DX-02
**Success Criteria** (what must be TRUE):
  1. An operator scrapes `GET /metrics` and receives Prometheus-format metrics including six per-shard labeled series (`beava_shard_reactor_utilization{shard}`, `beava_shard_inbox_depth{shard}`, `beava_shard_events_total{shard,outcome}`, `beava_shard_keys_owned{shard}`, `beava_shard_watermark_lag_seconds{shard}`, `beava_shard_inbox_full_total{shard}`) plus `beava_events_dropped_total{reason}` and `beava_cross_shard_fanout_total{op}`.
  2. A user pushing an HTTP event when a shard's SPSC inbox is full receives HTTP 503 and observes `beava_shard_inbox_full_total{shard="N"}` increment; the listener thread never stalls (non-blocking try_send contract).
  3. A user pushing an event with a declared `shard_key=("region","user_id")` where the event is missing `region` receives HTTP 400 and observes `beava_events_dropped_total{reason="shard_key_missing"}` increment; no shard thread panics.
  4. A user running at N>1 without a declared `shard_key=` on a stream sees `ShardKeyMissingWarning` on `GET /debug/warnings`; at N=1 no warning fires.
  5. A developer running the 9-cell benchmark matrix at N=CPU_COUNT sees `complex-c8-x8` at ≥3× the v1.0-launch baseline, and `shard_probe` reports `cross_shard_fraction <40%` on the release workload.
  6. An operator who previously used `BEAVA_ENTITIES_SHARDS` sees a warn-once log message at startup pointing to `BEAVA_SHARDS` docs; the legacy var is deprecated but does not crash the server.
**Plans**: 8 plans
Plans:
- [x] 50-01-PLAN.md — Cargo.toml Wave 2 deps (core_affinity, crossbeam-channel, metrics, metrics-exporter-prometheus) + metrics module + parallel /metrics emit (Wave 1) (shipped)
- [x] 50-02-PLAN.md — Per-shard labeled metric series: 7 labeled + 2 unlabeled; record_shard_event wired into TCP + HTTP (Wave 2) (shipped)
- [x] 50-03-PLAN.md — Shard thread spawn + core_affinity pinning + catch_unwind quarantine + ready-barrier (Wave 2) (shipped — completed via 50.5)
- [x] 50-04-PLAN.md — SPSC channels + ShardEvent + backpressure contract (drop + counter + HTTP 503 / TCP SHARD_OVERLOAD 0x10) (Wave 3) (shipped — completed via 50.5)
- [x] 50-05-PLAN.md — SO_REUSEPORT per-shard TCP + HTTP accept on Linux; single-listener macOS fallback; AxumServerSet (Wave 3) (shipped — completed via 50.5)
- [x] 50-06-PLAN.md — Tuple shard_key missing-field reject (HTTP 400 / TCP 0x12) + ShardKeyMissingWarning + BEAVA_ENTITIES_SHARDS deprecation (Wave 3) (shipped)
- [x] 50-07-PLAN.md — Shard loop gauge emission + shard_probe extension + N=2 integration test (Wave 4) (shipped)
- [x] 50-08-PLAN.md — Ship-gate: 9-cell matrix at N=CPU_COUNT (≥3× gate) + cross_shard_fraction gate + metrics parity test + human verify (Wave 5) (shipped via 50.5 ship-gate)
**UI hint**: no

### Phase 50.5: 50.5-shard-thread-completion
**Goal**: Complete the shard-thread side that Phase 50 left as a stub (`src/shard/thread.rs:160 TODO(50-04)`). Currently the SPSC dispatch works but the receiver discards events and real processing still runs on the legacy single-engine path — so BEAVA_SHARDS>1 produces no parallelism. This phase wires the shard thread to own per-shard state and dispatch through the TPC path, unlocking the actual parallelism TPC was designed for.
**Depends on**: Phase 50 Fix #1 committed (Wave-1 clamp removed, zero-cost N=1 bypass); `50-DEBUG-SESSION.md` + `50.5-FIX-PLAN.md` as source of truth.
**Requirements**: TPC-PERF-02 (pinning — measurable), TPC-PERF-03 (SPSC receiver — actually reads), TPC-CORR-01 (backpressure end-to-end) — these were claimed by Phase 50 but only partially delivered; Phase 50.5 completes them.
**Success Criteria** (what must be TRUE):
  1. Setting `BEAVA_SHARDS=8` produces 8 live shard threads in `/debug/shards`, each showing non-zero `events_total{shard=N}` after a workload — not just shard 0.
  2. At N=CPU_COUNT the shard threads process their own SPSC-delivered events (no fallback to legacy `engine.push_with_cascade` at N>1).
  3. `shard_probe` `cross_shard_fraction <40%` on the release workload at N=CPU_COUNT.
  4. **macOS dev gate**: `complex-c8-x8` at N=CPU_COUNT ≥ **1.5× baseline (~460K EPS)**. Single-accept-thread + BSD-compat SO_REUSEPORT caps dev throughput; this is the macOS dev-mode gate.
  5. **Linux prod gate (merge criterion)**: `complex-c8-x8` at N=CPU_COUNT ≥ **3× baseline (~918K EPS)** on a Hetzner CX41-class reference box. This is the ship-gate for merging v1.2 to main.
  6. N=1 regression gate holds: `complex-c8-x8` at N=1 stays within −5% of 314,931 baseline (Fix #1 already reestablishes this).
**Plans**: 3 plans
Plans:
- [x] 50.5-01-PLAN.md — TDD: Wave 0 cascade-shape grep + widen ShardResult::Ok(FeatureMap) + shard thread owns per-shard state via push_with_cascade_on_shard; handle_push_core_ex at N>1 awaits oneshot and skips legacy cascade (Wave 1)
- [x] 50.5-02-PLAN.md — TDD: Linux bind_reuseport_tcp wired into boot path; macOS per-connection Arc<str> interning; N=2 per-shard metrics parity (Wave 2)
- [x] 50.5-03-PLAN.md — Ship-gate measurement: N=1 regression (auto) + macOS dev-gate (auto) + Linux Hetzner CCX43 prod-gate (human-run merge criterion); benchmark README update (Wave 3)
**UI hint**: no

### Phase 51: 51-cross-shard-queries-joins
**Goal**: Read paths that touch multiple shards — stream listing, global watermark, and join validation — are correctly scatter-gathered or enforced at register time, with hot-shard visibility via `GET /debug/shards`.
**Depends on**: Phase 50
**Requirements**: TPC-INFRA-05, TPC-PERF-05, TPC-PERF-06, TPC-CORR-04
**Success Criteria** (what must be TRUE):
  1. A user calls `GET /streams` and receives the fleet-wide stream list merged from all shards; the response includes `scatter_latency_us` and the added p99 latency vs a point query is <15 μs.
  2. A user registering a join between two streams with differing `shard_key=` declarations receives a `JoinShardKeyMismatch` error that names both streams, both keys, and shows the exact decorator fix; the pipeline does not start.
  3. An operator calls `GET /debug/shards` and receives per-shard diagnostics (inbox depth, reactor utilization, keys owned, watermark lag); a shard whose `keys_owned` exceeds 1.5× the fleet mean is flagged in `hot_shards` in the response.
  4. Each shard publishes its per-stream max event-time to a global atomic; the global watermark for any stream is `min` across per-shard atomics; per-entity TTL eviction reads only the shard-local watermark (no cross-shard read on the eviction hot path).
**Plans**: 5 plans
Plans:
- [x] 51-01-PLAN.md — TDD: GlobalWatermarkStore (Arc<Box<[AtomicU64]>>), WatermarkState.publish_if_due, BEAVA_WATERMARK_PUBLISH_INTERVAL env (Wave 1)
- [x] 51-02-PLAN.md — TDD: scatter_gather in src/routing/scatter.rs; GET /streams + GET /streams/{name} handlers updated; beava_cross_shard_fanout_total increment (Wave 2)
- [x] 51-03-PLAN.md — TDD: GET /debug/shards endpoint; ShardDiagnosticsReport; hot-shard detection at 1.5× (BEAVA_HOT_SHARD_THRESHOLD); log-warn-once/60s (Wave 2)
- [x] 51-04-PLAN.md — TDD: join_validator::validate_shard_keys; JoinShardKeyMismatch D-12 locked message; pipeline.rs register() integration; /debug/warnings signal (Wave 2)
- [x] 51-05-PLAN.md — Integrated verification: full test suite + human verify GET /streams, GET /debug/shards, JoinShardKeyMismatch, watermark counter (Wave 3)
**UI hint**: no

### Phase 52: 52-event-log-recovery-ship-gate
**Goal**: Per-shard event logs on disk, parallel recovery at startup, the `tally reshard` migration tool, snapshot v8 with hard-fail guard on shard-count mismatch, correct fork/replica re-hashing, and all three pre-ship gates green (N=1↔N=8 proptest parity, ≥3× throughput, Pareto cross_shard_fraction <40%).
**Depends on**: Phase 51 (and Phase 50 for the throughput gate; Phase 51 for the parity harness requiring per-shard log)
**Requirements**: TPC-INFRA-06, TPC-CORR-02, TPC-CORR-05, TPC-CORR-06, TPC-PERF-07, TPC-DX-03, TPC-DX-04
**Success Criteria** (what must be TRUE):
  1. A server starting with a snapshot whose `shard_count` disagrees with `BEAVA_SHARDS` refuses to boot and emits the exact error `"snapshot shard_count=N but BEAVA_SHARDS=K — run 'tally reshard --from N --to K' then restart"` — no silent empty-state start.
  2. A probe hitting `GET /ready` during shard recovery receives 503 with `shards_recovering` listed; once all shards complete log replay, `GET /ready` returns 200 and `GET /health` has been 200 throughout.
  3. An operator runs `tally reshard --from 1 --to 8 --data-dir ./data --out-dir ./data-new` and receives an atomic offline migration; the source dir is untouched until `--replace` is passed; a N=1→N=8 round-trip produces byte-identical state to the original.
  4. A fork/replica ingesting from an upstream with a different shard count re-hashes every event via `hash(event.key) mod downstream_N`; no `--reshard-from` CLI flag exists; fork/replica parity tests (upstream and downstream feature values agree) are green.
  5. A developer runs the proptest parity harness feeding the same event stream to N=1 and N=8 engines and sees identical feature values for every key at every event-time bucket across all operators (filter, map, agg, join, fork) — this harness is a hard pre-merge gate.
  6. A developer runs the full 9-cell matrix plus the Pareto-workload cell at N=CPU_COUNT and sees: (a) every standard cell within −5% of baseline at N=1; (b) `complex-c8-x8` ≥3× baseline at N=CPU_COUNT; (c) Pareto cell `cross_shard_fraction <40%`; (d) sustained ≥1M EPS on a 16-core reference box.
  7. A user reads `docs/architecture-tpc.md` and understands the shard model, routing, joins, recovery, and reshard process end-to-end; `docs/operations.md` has a "Shard sizing & hot-shard diagnosis" section.
**Plans**: TBD
**UI hint**: no

### Phase 53: 53-fjall-state-backend
**Goal**: Per-shard state moves from in-memory `AHashMap` to the `fjall` LSM-tree (per-shard partitions under `data/shard-N/fjall/`). State is durable on write, unbounded in size, and crash-safe via fjall's WAL — snapshots become fjall checkpoints. A `tally migrate-to-fjall` tool converts v8 in-memory snapshots to fjall partitions in place. Process kill + restart produces byte-identical state within the last-acknowledged LSN.
**Depends on**: Phase 52 (ship-gate green — fjall lands atop the TPC architecture, not underneath it; final v1.2 phase)
**Requirements**: TPC-PERSIST-01, TPC-PERSIST-02, TPC-PERSIST-03, TPC-PERSIST-04, TPC-PERSIST-05, TPC-PERSIST-06
**Success Criteria** (what must be TRUE):
  1. A developer inspecting `Shard` sees `state: fjall::Partition` (not `AHashMap`); get/set/iterate operations go through fjall with identical semantics to the pre-Phase-53 HashMap path.
  2. An operator SIGKILLs the server mid-workload and restarts it; the process comes up reading fjall's WAL and restores feature values identical to the last acknowledged write — **no snapshot replay needed**.
  3. A developer runs a soak test pushing 100 GB of state on a 32 GB RAM box; feature-read p99 stays sub-ms (validates fjall's bloom filters + block cache hold up on out-of-RAM state).
  4. An operator runs `tally migrate-to-fjall --data-dir ./data` and receives an in-place conversion of v8 in-memory snapshots to per-shard fjall partitions; downtime = tool runtime; the original v8 snapshot is preserved as `snapshot.v8.bak` until `--replace` is passed.
  5. The 9-cell matrix and Pareto cell at N=CPU_COUNT with fjall-backed state regress by at most **−15%** vs the Phase 52 in-memory baseline (fjall has intrinsic overhead vs HashMap; bounded regression accepted for the durability + unbounded-state wins).
  6. The N=1↔N=8 proptest parity harness (from Phase 52) runs green against fjall-backed state for every operator.
  7. `docs/architecture-tpc.md` gains a "State durability (fjall)" section; `docs/operations.md` documents `BEAVA_FJALL_*` tuning knobs and recovery semantics.
**Plans**: 7 plans
Plans:
- [x] 53-01-PLAN.md — Wave 0 spike: fjall 2.11 dep + Criterion read-modify-write bench + postcard size histogram + SIGKILL verification + cache-stats API probe (W-4); CONTINUE/STOP gate
- [x] 53-02-PLAN.md — Wave 1: fjall backend plumbing (`src/shard/fjall_backend.rs`: keyspace + partition lifecycle, FjallConfig, 7 BEAVA_FJALL_* env clamps with real sysinfo-driven CACHE_MB default, smoke round-trip test)
- [x] 53-03-PLAN.md — Wave 2a: swap `Shard.state` AHashMap → `PartitionHandle`; StoreView fjall RMW arms; `read_entity_from_shard` helper; gate `ShardedStateStoreV1` behind `state-inmem` Cargo feature
- [x] 53-03B-PLAN.md — Wave 2b (parallel): `ShardedStateStoreFjall` backend + `src/shard/thread.rs` fjall port + ConcurrentAppState plumbing + proptest module gating
- [x] 53-04-PLAN.md — Wave 3: `tally migrate-to-fjall` CLI with per-stream shard_key routing (W-2) + `tally reshard` fjall awareness
- [x] 53-05-PLAN.md — Wave 4: SIGKILL crash-recovery integration test with ephemeral port (W-8) + N=1↔N=8 proptest parity port to fjall with file-level cfg gate (W-3) + 3–4 per-shard fjall Prometheus metrics (4th conditional on spike outcome, W-4)
- [ ] 53-06-PLAN.md — Wave 5: 9-cell + Pareto bench regression gate (automated −15% budget) + 100 GB Hetzner CCX43 soak (human-verify, p99 < 1 ms) + architecture + ops docs with W-4 conditional cache-ratio alert **[DEFERRED to Phase 54 — pprof showed legacy DashMap bypass at N=1; bench would measure noise]**
**UI hint**: no


### Phase 54: 54-legacy-engine-removal
**Goal**: Retire the DashMap-backed legacy engine path. Every push entrypoint — TCP `handle_push_batch`, HTTP `http_push_single`/`http_push_batch`, replica ingest — routes through the shard-thread SPSC dispatch at N=1 as well as N>1, so `push_with_cascade_on_shard` + fjall `PartitionHandle` is the sole hot path. `StateStore` and `PipelineEngine::push_internal` / `push_batch_with_cascade_no_features` are deleted. DashMap dependency is removed from `[dependencies]`.
**Depends on**: Phase 53 (fjall backend landed). Source of truth: Phase 52-10 BLOCKER SUMMARY + Phase 53 pprof finding in `53-VERIFICATION.md` + `53-06-SUMMARY.md`.
**Requirements**: TPC-PERSIST-04 (deferred from 53), TPC-PERSIST-05A (deferred from 53), TPC-ARCH-01 (NEW — single hot path; added to REQUIREMENTS.md in Wave 0).
**Success Criteria** (what must be TRUE):
  1. A developer running `grep -r "DashMap" src/` finds ZERO occurrences outside `Cargo.lock` and comments referencing prior architecture. The `dashmap` dependency is removed from `Cargo.toml`.
  2. A developer running `grep -r "StateStore\b" src/` finds ZERO occurrences of the `StateStore` struct (the DashMap-backed one). `src/state/store.rs` either no longer exists or contains only type aliases that re-export shard-backed types.
  3. A developer running `grep -rn "push_internal\|push_batch_with_cascade_no_features" src/` finds ZERO occurrences. Those methods are removed from `PipelineEngine`.
  4. Re-running `cargo test --release --test profile_ingest -- --nocapture --ignored profile_ingest_hot_path` shows ZERO samples in `DashMap::_entry` or `DashMap::_get` in the top-20 leaf functions. `PartitionHandle::insert` or equivalent fjall symbols appear in the inclusive top-20 instead.
  5. A developer runs `MODE=complex DURATION=60 bash benchmark/fraud-pipeline/run_bench.sh` at N=8 on the reference box and sees EPS within `-15%` of the Phase 52 committed baseline (unlocks deferred TPC-PERSIST-05A gate).
  6. A developer runs the 100 GB Hetzner CCX43 soak and sees p99 < 1 ms sustained 8 h (unlocks deferred TPC-PERSIST-04 gate; `human_needed` per user decision 2026-04-19 — evidence-file gated).
  7. Full test suite `cargo test --all -- --test-threads=1` reports zero regressions vs Phase 53 baseline (884 default / 888 state-inmem). The `state-inmem` Cargo feature is removed outright in Wave 4 unless the -15% gate misses (CONTEXT §Area 5 contingency).
**Locked decisions (2026-04-19)**:
  - Cross-shard TT-cascade: SCATTER-GATHER (NOT register-time shard_key constraint). `cascade_table_upsert_on_shard` dispatches SPSC sends to every affected shard and joins. Researcher recommendation REJECTED.
  - Hetzner CCX43 soak: `human_needed` — phase transitions to `passed` on evidence-file commit; Claude prepares the runbook + script.
**Plans**: 6 plans
Plans:
- [x] 54-00-baseline-and-scaffolding-PLAN.md — Wave 0: capture pprof + EPS baseline, grep-ZERO scripts (RED), 3 ingest-routing integration tests (RED), REQUIREMENTS.md TPC-ARCH-01 patch (completed 2026-04-19 — baseline EPS 197,122; −15% floor 167,553; all 6 RED gates in place)
- [x] 54-01-rewire-ingest-through-spsc-PLAN.md — Wave 1: send_to_shard helper + unify HTTP/TCP N=1 bypass + port replica.notify_subscribers hook (completed 2026-04-20 — 3 Wave-0 RED tests GREEN; 884 lib tests pass; Pass A aee409e / Pass B 1914fa0 / Pass C da5739f + 52e178a; 1 additional test #[ignore]'d for Wave 3 migration)
- [x] 54-02-storeview-widening-and-scatter-gather-cascade-PLAN.md — Wave 2: widen StoreView (5 methods) + cascade_table_upsert_on_shard SCATTER-GATHER + migrate operators/register (completed 2026-04-20 — Pass A bfa62fb: StoreView +5 methods + Shard take_dirty/iter_entities + 2 new ShardOp variants, 8/8 tests on both backends; Pass B 85651a2: scatter-gather cascade via crossbeam try_send + blocking oneshot + deadlock analysis, 2/2 cross_shard_tt_cascade; Pass C no-op — operators.rs/register.rs already StateStore-free; 884/888 lib tests unchanged)
- [x] 54-03-migrate-remaining-statestore-callers-PLAN.md — Wave 3: boot-replay direct fjall insert + 6 non-shim DashMap users → RwLock<AHashMap> + test migration
- [x] 54-04-delete-legacy-surface-PLAN.md — Wave 4: delete StateStore + legacy pipeline methods + DashMap/arc-swap deps; flip grep-ZERO scripts GREEN (completed 2026-04-19 — 9 commits A1 b435145 → A6b 602c3ab + close 945d4ab; 3 ship_gate tests flipped GREEN; dashmap + arc-swap dropped from Cargo.toml; StreamStore DashMap struct deleted; state-inmem retained as no-op marker per CONTEXT §Area 5 Option B with full 139-cfg-gate collapse deferred to 54-NEXT; 784/819 lib baseline preserved; all 3 grep-ZERO scripts exit 0)
- [x] 54-05-perf-gates-and-soak-runbook-PLAN.md — Wave 5: pprof re-run showed ZERO DashMap in top-20 (was 61.2%); fjall + crossbeam symbols dominate; candidate MODE=complex N=8 EPS = 1,339,446 (+580% vs 197,122 baseline, 6.8× gain, 7× headroom over the 167,553 floor); Hetzner CCX43 100GB 8h soak runbook + script + evidence-dir shipped with human_needed verification contract; 54-VERIFICATION.md records 6/7 SC auto-passed (TPC-ARCH-01 ✅ + TPC-PERSIST-05A ✅) and TPC-PERSIST-04 human_needed (evidence-file gated). Commits 2660478 + 56a5a9a + close commit. Phase 54 engineering-complete 2026-04-20.
**UI hint**: no

### Phase 55: 55-stream-table-cascade-crossshard-and-source-tables
**Goal**: Fix the Stream→Table aggregation correctness bug — downstream table rows must live on the shard owning `hash(output_key) % N`, not on the input event's shard. `shard_key=` on streams becomes a pure source-ingress hint (which shard accepts the event first); all downstream cascades shuffle by the downstream's own key_field. Also introduces source tables (`@bv.source_table(key=K)`) as a new input kind for CDC-style keyed state (Postgres/MySQL replication target) with UPSERT/DELETE commands, bulk variants, and `source_lsn` echo for resumable replication.
**Depends on**: Phase 54 (engineering-complete — legacy DashMap + StateStore deleted; scatter-gather cascade pattern exists via `cascade_table_upsert_on_shard`).
**Requirements**: TPC-CORR-07 (NEW — Stream→Table downstream row on hash(output_key) shard), TPC-SOURCE-01 (NEW — `@bv.source_table` SDK + UPSERT/DELETE wire commands).
**Success Criteria** (what must be TRUE):
  1. A primary `Transactions` PUSH whose downstream `MerchantActivity[merchant_X]` hashes to a different shard than the input's `user_id` produces a row on `hash(merchant_X) % N` — verified by a test that asserts `read_entity_from_shard(correct_shard, "merchant_X")` returns the row AND every other shard returns None.
  2. A Debezium-style CDC connector can UPSERT rows into a `@bv.source_table(key="country_code")` via `POST /table/Countries` (HTTP) or `UPSERT_TABLE_ROW` (TCP); batch variant handles ≥10K rows/call; `source_lsn` is echoed back in the ack.
  3. DELETE on a source table is hard-delete + pending-retraction marker written to the event log (Phase 57 consumes the marker). Re-applying the same UPSERT with the same fields is a no-op (idempotent full-replace).
  4. Cross-shard backpressure contract: cascade target inbox full → source shard blocks new ingress (listener returns 503 / SHARD_OVERLOAD); acked PUSHes are always recoverable within the existing 5 ms fsync window; cascade delivery is asynchronous with at-least-once semantics via event-log replay + per-source-shard delivery cursor.
  5. Near-overload metrics exposed: `beava_shard_inbox_high_watermark_total{shard}`, `beava_cascade_queue_depth{source,target}`, `beava_cascade_lag_seconds{source,target}` visible via /metrics.
  6. Boot-time full rematerialization: starting the server against a pre-Phase-55 snapshot triggers automatic event-log replay through the new cascade path, rebuilding downstream state correctly-sharded. Primary entity state reused from snapshot; downstream tables rebuilt.
  7. Perf gate: `MODE=complex DURATION=60 CPUS=8 CLIENTS=8 bash benchmark/fraud-pipeline/run_bench.sh` ≥ 85% of Phase 54 baseline (≥ 935,000 EPS). Implementation MUST include same-shard fast path AND batched cross-shard dispatch (coalesce multiple writes to the same target into one SPSC send) from day one — no fast paths deferred.
**Locked decisions (2026-04-20)**:
  - Split across three phases (55/56/57) per user decision: 55 = Stream→Table shuffle + source tables; 56 = EnrichFromTable + SSJ cross-shard; 57 = retraction across cross-shard joins.
  - Durability: primary PUSH ack = 5 ms fsync of event log (existing); cascade intent recovered via event-log replay with per-source-shard delivery cursor (works for both fjall and in-mem uniformly).
  - Backpressure: acked events always recoverable; cascade async; block new PUSHes on near-overload; expose near-overload metrics.
  - Migration: no dual-shard / lazy migration — boot-time full rematerialization on pre-55 snapshot.
  - Source table wire shape: explicit new commands (not reusing PUSH); full-replace on UPDATE (CDC-native); hard-delete + pending-retraction marker; optional entity_ttl; batch UPSERT/DELETE variants; source_lsn echoed back on ack.
  - No cascade fires on source-table writes in Phase 55 — source tables are passive enrichment targets. Retractions / downstream recomputation on source-table writes is Phase 57 territory.
**Plans**: 5 plans
- [x] 55-00-PLAN.md — Wave 0: RED-test set for all 7 SCs (TPC-CORR-07 + TPC-SOURCE-01 contracts)
- [x] 55-01-PLAN.md — Wave 1: CascadeTarget trait + CascadeBuffer + delivery cursor + 5 cascade metrics (TPC-CORR-07 core)
- [x] 55-02-PLAN.md — Wave 2: source-table wire (TCP 0x14-0x17 + HTTP /table/{name}) + @bv.source_table SDK (TPC-SOURCE-01)
- [x] 55-03-PLAN.md — Wave 3: snapshot v9 + boot rematerialization via SyncCascadeTargets (TPC-CORR-07 migration)
- [x] 55-04-PLAN.md — Wave 4: perf gate (>= 1,138,529 EPS floor) + 55-VERIFICATION.md + close
**UI hint**: no

### Phase 56: 56-enrich-from-table-and-stream-stream-join-crossshard
**Goal**: Make EnrichFromTable and StreamStreamJoin work correctly across shards. EnrichFromTable does a cross-shard read via `ShardOp::ReadEntityAt` when the right-side key's shard differs from the current shard (today: silently returns Missing). StreamStreamJoin buffer lives on `hash(join.on) % N`; both sides route there; the register-time TPC-CORR-04 rejection of mismatched shard_keys is relaxed at runtime (becomes a co-location warning with perf impact noted).
**Depends on**: Phase 55 (Stream→Table cascade path + cross-shard SPSC dispatch primitives landed; source tables available as enrichment targets).
**Requirements**: TPC-CORR-08 (NEW — EnrichFromTable cross-shard), TPC-CORR-09 (NEW — StreamStreamJoin cross-shard; TPC-CORR-04 rejection relaxed).
**Success Criteria** (what must be TRUE):
  1. A stream `Txns(shard_key=user_id)` with `enrich_from(Countries, on=country_code)` where Countries is `@bv.source_table(key=country_code)` returns the correct enrichment regardless of which shard the Txn landed on — verified by a test that stages Country rows on shard-K and Txn events on shard-J (J ≠ K) and asserts the joined output has the Country fields populated.
  2. StreamStreamJoin of `LeftStream(shard_key=user_id)` × `RightStream(shard_key=session_id)` on `user_id` produces correct joined events — both sides route to shard owning `hash(user_id) % N`; join buffer lives there.
  3. Existing TPC-CORR-04 register-time error (JoinShardKeyMismatch) is replaced by a warning: `register` succeeds with a logged `CrossShardJoinWarning` naming the expected perf impact; no correctness regression.
  4. EnrichFromTable read-side latency budget: p99 per-event latency on complex pipeline ≤ 2× Phase 55 baseline (cross-shard reads are synchronous but batched per-event and pipelined across multiple enrichments in the same downstream).
  5. Perf gate: complex pipeline with ≥1 cross-shard EnrichFromTable per event, ≥ 85% of Phase 55 baseline EPS.
**Locked decisions (carryover from Phase 55 scoping)**:
  - EnrichFromTable: synchronous cross-shard read (block shard thread on oneshot); batched + pipelined where a downstream has multiple enrichments (decision: Q6-b from phase-55 scoping — revisit if p99 budget is exceeded).
  - StreamStreamJoin buffer ownership: shard owning `hash(join.on) % N` (decision: Q5b-a from phase-55 scoping).
**Plans**: 5 plans
Plans:
- [x] 56-00-PLAN.md — Wave 0: RED tests for SC-1..SC-5 + REQUIREMENTS.md TPC-CORR-04 relaxation + TPC-CORR-08 + TPC-CORR-09
- [x] 56-01-PLAN.md — Wave 1: ShardOp::ReadEntityAt / ReadEntityBatch / SsjInsert + Shard::read_entity_at + Shard::apply_ssj_insert + pipeline.rs helpers + 5 metric counters
- [x] 56-02-PLAN.md — Wave 2: EnrichFromTable cross-shard read with same-shard fast path + per-batch coalesce by (target_shard, table)
- [x] 56-03-PLAN.md — Wave 3: StreamStreamJoin routes via hash(join.on)%N + TPC-CORR-04 relaxation (CrossShardJoinWarning) + /debug/warnings cross_shard_joins field
- [x] 56-04-PLAN.md — Wave 4: perf gate (default pipeline PASSED 1,195,914 EPS; crossshard scenario human_needed on SDK gap) + 56-VERIFICATION.md + close
**UI hint**: no

### Phase 57: 57-retraction-across-crossshard-joins
**Goal**: Implement retraction propagation through cross-shard joins and cascades. Every emitted downstream row tracks its contributing input events (left/right ids for SSJ, source-row key for EnrichFromTable, primary event id for Stream→Table). Tombstones / deletes on any input trigger retractions on the owning shards of affected downstream outputs. Source-table DELETE's pending-retraction markers (from Phase 55) are consumed here.
**Depends on**: Phase 56 (cross-shard joins work correctly; retraction is the correctness refinement on top).
**Requirements**: TPC-CORR-10 (NEW — retractions flow through joins and cascades end-to-end).
**Success Criteria** (what must be TRUE):
  1. DELETE on a source table row causes all downstream enrichments that consulted that row to emit retraction tombstones on their owning shards; verified by a test that UPSERTs a source row, pushes a stream event that enriches from it, DELETEs the source row, and asserts the derived downstream row is tombstoned.
  2. Tombstoning an entity on either side of a StreamStreamJoin retracts every previously-emitted joined-output row that referenced it; verified by a test that stages L×R matches, emits joined outputs, tombstones an L key, and asserts the joined outputs are retracted.
  3. Late-retraction window: retractions apply to events within `history_ttl` of the current watermark; events older than that are documented as "cannot be retracted" and produce a `RetractionBeyondHistory` warning counter.
  4. Perf impact: retraction tracking adds ≤ 10% overhead on the write path (not the retraction event itself). Measured by comparing Phase 56 baseline against Phase 57 on the standard complex bench WITH zero actual retractions firing.
**Locked decisions (carryover)**:
  - Contributing-input tracking per emitted row (Q5a-c from phase-55 scoping).
  - Late retractions outside history_ttl window: warn + skip (not in scope to rewrite history).
**Plans**: 5 plans
Plans:
- [x] 57-00-PLAN.md — Wave 0: RED tests for SC-1..SC-4 + depth guard + sharding_parity extension + REQUIREMENTS.md TPC-CORR-10
- [x] 57-01-PLAN.md — Wave 1: ShardOp::RetractDownstream + RetractReason/Outcome + Shard::apply_retraction + 5 metrics + ContribSet + snapshot v10 + pipeline.rs helper
- [x] 57-02-PLAN.md — Wave 2: Stream→Table contributing_inputs.primary_event_id emission + tombstone fan_out_retraction_for_primary + 16-hop depth guard
- [x] 57-03-PLAN.md — Wave 3: EnrichFromTable source_table_keys + SSJ left/right_event_id + source-table DELETE PendingRetraction consumer + late-retraction warning via /debug/warnings.retraction_beyond_history
- [x] 57-04-PLAN.md — Wave 4: perf gate PASSED 1,297,293 EPS ≥ 1,076,322 floor (+20.5% headroom, +8.5% vs Phase 56 baseline); advisory D-D4 micro-bench deferred on Phase 55 SDK gap (57-NEXT #1 / 56-NEXT #6); 57-VERIFICATION + scripts/verify-retraction-metrics.sh + phase close
**UI hint**: no

### Phase 58: 58-tokio-connection-handling-rewrite
**Goal**: Eliminate per-connection Tokio task spawn/drop churn — the biggest measured server-side CPU cost (60% of samples in samply profiles). Replace the "accept → spawn per-connection task" pattern with long-lived accept-loops per-CPU via SO_REUSEPORT (Linux) and dedicated accept-thread-per-shard (macOS). Inline `handle_push_batch` from the accept loop without spawning. Target: recover the 25–40% of CPU currently spent on runtime overhead.
**Depends on**: Phase 57 (correctness baseline established before optimization).
**Requirements**: TPC-PERF-08 (NEW — connection-handling overhead ≤ 15% of CPU under steady load).
**Success Criteria** (what must be TRUE):
  1. samply profile of `MODE=complex N=8` shows `tokio::runtime::task` / `drop_in_place<Task::Cell<async_main closure>>` combined ≤ 15% of leaf samples (currently ~60%).
  2. Each shard has a dedicated accept loop (Linux SO_REUSEPORT socket, macOS dispatched thread) that inlines `handle_push_batch` without `tokio::spawn` per connection.
  3. Perf gate: ≥ +25% EPS vs Phase 57 baseline on complex N=8 (≥ ~1.17M × 1.25 = 1.46M EPS if Phase 57 holds at Phase 55's 935K minimum, or ≥ 1.43M if Phase 55 hits the 1.15M range).
  4. No regression in p99 latency — tail latency should improve or match.
**Plans**: 5 plans
Plans:
- [x] 58-00-PLAN.md — Wave 0: RED tests (tokio_spawn_absence, per_shard_listener, http_push_still_works) + samply-probe-tokio-share.sh + REQUIREMENTS TPC-PERF-08 row
- [x] 58-01-PLAN.md — Wave 1: Linux SO_REUSEPORT per-shard TcpListener + FuturesUnordered inline handler + BEAVA_MAX_CONNS_PER_SHARD=256; delete spawn_linux_per_shard_accept_loops
- [x] 58-02-PLAN.md — Wave 2: macOS dedicated std::thread per shard (D-B1) + BEAVA_SHARDS_SINGLE_LISTENER=1 fallback (D-B2); handle_connection_blocking + MacosConnSlot RAII
- [x] 58-03-PLAN.md — Wave 3: Replica ingest rides unified per-shard accept path; opcode-dispatch parity audit + replica_ingest_routing extension at N=4
- [x] 58-04-PLAN.md — Wave 4: perf gate HUMAN_NEEDED (best candidate 1,376,450 EPS on macOS dev host, +6.1% vs Phase 57 baseline, −15.1% below 1,621,616 floor; contingency C1 invoked / C2 N/A / C3 human_needed); samply probe harness-unable (TOKIO_SHARE_PCT=0.0 — coverage sentinel still RED; probe exercises wrong surface); p99 latency parity (−0.11% vs P57); structural change delivered (tokio::spawn-per-conn = 0 on all production PUSH paths); 58-VERIFICATION + 58-PERF-GATE committed; SC-1 + SC-3 human_needed pending Linux prod-host run + probe harness extension (58-NEXT #1)
**UI hint**: no

### Phase 59: 59-binary-wire-format-for-push
**Goal**: Eliminate JSON re-serialization on the PUSH hot path — currently ~11% of CPU (serde_json::format_escaped_str + from_utf8 on the shard-dispatch path). Replace JSON with a binary codec (length-prefixed postcard or custom) for TCP PUSH; HTTP PUSH stays JSON for compatibility. Zero-copy payload via `bytes::Bytes` end-to-end from wire → shard inbox → fjall insert.
**Depends on**: Phase 58 (Tokio overhead reduced first so this work's impact is measurable).
**Requirements**: TPC-PERF-09 (NEW — PUSH path JSON cost ≤ 3% of CPU).
**Success Criteria** (what must be TRUE):
  1. samply profile shows `serde_json::*` + `from_utf8` combined ≤ 3% of leaf samples on TCP PUSH path.
  2. Python + Rust SDKs emit the new binary format on TCP; HTTP path unchanged.
  3. `bytes::Bytes` payload is forwarded from TCP read → ShardEvent → fjall insert with ZERO `serde_json::to_vec` / `to_string` re-serialization.
  4. Perf gate: ≥ +10% EPS vs Phase 58 C1 baseline (1,376,450 EPS × 1.10 = **≥ 1,514,095 EPS**).
  5. Backward compatibility: servers accept both old (JSON) and new (binary) wire formats for ≥ 1 release cycle; SDKs negotiate on handshake.
**Locked decisions (2026-04-20 auto-CONTEXT)**:
  - REUSE existing `decode_event_binary` (TYPE_NULL/BOOL/I64/F64/STR + u16 BE field_count — production since Phase 11); do NOT add postcard/bincode/rkyv to wire path (D-A1).
  - HTTP PUSH path UNCHANGED (stays JSON per D-A4; `axum` + `http_ingest.rs` untouched).
  - `OP_NEGOTIATE_WIRE_FORMAT = 0x18` + `WIRE_BINARY_PASSTHROUGH = 1 << 0` capability bit; D-B2 auto-detect (binary-first, JSON-fallback) makes handshake optional.
  - D-B3 backward-compat: server accepts JSON-over-TCP OP_PUSH for ≥ 1 release cycle (removal = 59-NEXT #1).
  - D-E1 payload-size DoS cap: `BEAVA_MAX_PAYLOAD_BYTES` env, default 1 MiB.
  - Contingency ladder for Wave 4 perf gate: C1 pre-allocate per-shard BytesMut → C2 inline decode (skip Value) → C3 human_needed.
**Plans**: 5 plans
Plans:
- [x] 59-00-PLAN.md — Wave 0: RED tests (wire-negotiation, binary-push-bytes-passthrough, json-over-tcp-still-accepted, binary-decode-fuzz) + samply-probe-json-share.sh + verify-no-tcp-json-reserialize.sh + REQUIREMENTS TPC-PERF-09 row + always-on counters
- [x] 59-01-PLAN.md — Wave 1: src/wire/ module + PayloadFmt + ShardEvent.payload_fmt + Bytes passthrough (tcp.rs:2159 + :2538 + thread.rs:724 WASTE eliminated); BEAVA_MAX_PAYLOAD_BYTES DoS cap
- [x] 59-02-PLAN.md — Wave 2: OP_NEGOTIATE_WIRE_FORMAT (0x18) opcode + Command::NegotiateWireFormat + handle_sync_command dispatch + 3 unit tests
- [x] 59-03-PLAN.md — Wave 3: Python SDK OP_NEGOTIATE constants + BeavaClient.negotiate_wire_format + BEAVA_WIRE_NEGOTIATE env-opt-in + pre-59-server fallback test (3 Rust integration tests + 8 Python pytest cases) + Python 0.1.0 → 0.2.0
- [x] 59-04-PLAN.md — Wave 4: perf gate (best-of-3 C0 = 1,494,631 EPS; −1.3% below floor within 6% variance; D-D3 samply PASSED 2.5; p99 −15% IMPROVED) + 59-PERF-GATE.md + 59-VERIFICATION.md (SC-1/2/3/5 passed, SC-4 human_needed Linux-host re-run) + close
**UI hint**: no

### Phase 59.6: 59.6-typed-pipeline-records
**Goal**: Replace `serde_json::Value` as the in-pipeline event/state representation with typed, fixed-layout row records compiled from SDK-declared `@bv.stream` / `@bv.source_table` / `@bv.table` schemas at register time. Wire codec, engine operators (EnrichFromTable + 16 agg ops + SSJ), and state store (inmem + fjall; snapshot v11) all work on typed rows. `Value` fallback preserved for dynamic / debug paths. Target: per-event shard-thread cost 8.5μs → ~1-2μs (5× lift), per-shard ceiling ~118K EPS → ~500K-1M EPS, 10-shard node 5-10M EPS.
**Depends on**: Phase 59.5 (shard_key routing + source_table replication landed); inserts between 59.5 and 60. Phase 60 (hot-key salting) resumes after — orthogonal axis that multiplies with this work.
**Requirements**: TPC-PERF-11 (NEW — per-event shard-thread cost ≤ 2.0μs on complex-c8-x8 workload); TPC-CORR-07 (NEW — typed-row ↔ Value fallback parity under proptest).
**Success Criteria** (what must be TRUE):
  1. 59.5-W3.5 per-event histogram shows `pipeline` phase ≤ 2.0μs avg (down from 8.5μs) at sustained 1M+ EPS on complex-c8-x8.
  2. `@bv.stream` / `@bv.source_table` / `@bv.table` classes compile to `RegisteredSchema` at register time; SDK → server carries schema in REGISTER; server stores typed rows.
  3. All 17 operators (EnrichFromTable + 16 agg) have typed-row implementations; generic `Value` dispatch retained for ad-hoc / dynamic-schema paths only.
  4. Fjall state store emits snapshot v11 with packed-row encoding; v10→v11 in-place migration tool + round-trip test.
  5. Perf gate: ≥ +3× EPS vs Phase 59 C1 baseline (1,514,095 × 3.0 = **≥ 4,542,285 EPS**) on complex-c8-x8 at N=8; fallback contingency ladder if short.
  6. Backward compatibility: Python SDK ≥ v0.3.0 negotiates typed-pipeline capability; pre-59.6 clients continue to work via `Value` fallback for ≥ 1 release cycle.
**Plans**: 8 plans
Plans:
- [x] 59.6-00-PLAN.md — Wave 0: RED scaffolding (11 tests + parity harness + verify-typed-path.sh + bench stub + 2 AtomicU64 counters + TPC-PERF-11 row)
- [x] 59.6-01-PLAN.md — Wave 1: schema runtime (RegisteredSchema, Row, SchemaRegistry) + engine accessors + REGISTER JSON consumer + Python _schema_compile + _serialize emit
- [x] 59.6-02-PLAN.md — Wave 2: wire codec (OP_PUSH_TYPED_BATCH 0x19 + WIRE_TYPED_PIPELINE 1<<1 + src/wire/typed.rs decoder + ShardEvent.schema_id + PayloadFmt::TypedRow + Python SDK v0.3.0 _pack_typed_batch + push_many dispatch)
- [x] 59.6-03-PLAN.md — Wave 3: ShardOp::PushTypedRow + engine.push_typed_on_shard + EnrichFromTableTyped + run_typed_enrich_cascade + SC-3 GREEN
- [x] 59.6-04-PLAN.md — Wave 4: 7 typed simple aggs (Count/Sum/Avg/Min/Max/Last/First) + TypedAggOp trait + Shard::entity_state_typed + V11_FORMAT declaration + SC-4 (2 of 3) GREEN
- [x] 59.6-05-PLAN.md — Wave 5: V11 snapshot writer/reader + fjall put_entity_typed/get_entity_typed + StreamStreamJoinTyped + typed SsjInsert + SC-7+SC-8+SC-9+SC-10 GREEN + verify-typed-path.sh exit 0
- [x] 59.6-06-PLAN.md — Wave 6: 9 advanced typed aggs (DistinctCount/Percentile/TopK/Stddev/Variance + Ema/Lag/FirstN/LastN) + SideBand + Python SDK REGISTER ack schema_id echo + SC-6 GREEN + sharding_parity extended
- [x] 59.6-07-PLAN.md — Wave 7: perf gate best-of-3 + pipeline-phase latency measurement + samply probe + 59.6-PERF-GATE.md + 59.6-VERIFICATION.md + ROADMAP/STATE/REQUIREMENTS updates + docs/architecture.md + close

**Status:** **Engineering-complete** (2026-04-21) — typed-row pipeline lands across 8 waves. Criterion typed-pipeline-phase cascade = 22.97 ns/event (370× below 8.5μs Value-path baseline; 87× under 2.0μs TPC-PERF-11 target). Aggregate-EPS SC-5 deferred to Phase 64 Rust bench client / Linux-host re-run per same Phase 58/59 HUMAN_NEEDED precedent (macOS Python-client ceiling = measurement vehicle saturated; server hits backpressure on every client). 9/10 SCs PASSED; 41 typed-path integration tests GREEN; all 6 grep invariants GREEN; zero regressions on prior-phase tests.
**UI hint**: no

### Phase 59.7: 59.7-typed-windowed-cascade
**Goal**: Close the two gaps Phase 59.6 left open: (1) typed windowed aggregations — `operators_typed_aggs.rs` today has no `RingBuffer` / no window support, so `window="1h"/"24h"/"7d"` features (everywhere in fraud-pipeline) would silently produce lifetime counts if routed typed. Add `TypedRingBuffer` + 10 windowed typed agg impls (Count/Sum×2/Avg/Min×2/Max×2/Last/First) + V11 snapshot extension. (2) typed cascade-direct — `run_typed_enrich_cascade` today bridges every typed event back to Value via `row_to_value` + `push_with_cascade_on_shard`, so even typed-eligible downstream state still walks the Value path. Rewrite as a real typed walker that writes `entity_state_typed` directly (same-shard inline, cross-shard via new `ShardOp::RunTypedAggCascadeStep`). Feature-gated by `BEAVA_TYPED_CASCADE_DIRECT=1` env until stable. Target: `push_internal_on_shard` drops <1% on state-inmem; ≥+10% aggregate EPS on fjall.
**Depends on**: Phase 59.6 (engineering-complete — typed ingest + unwindowed typed aggs + V11 snapshot scaffold landed).
**Requirements**: TPC-PERF-11 (closes aggregate-EPS gap identified in 59.6 profile); TPC-CORR-07 (extends typed↔Value parity to windowed aggs and cross-shard cascade).
**Success Criteria** (what must be TRUE):
  1. `src/engine/operators_typed_aggs_windowed.rs` exists with windowed typed impls for Count, Sum(i64,f64), Avg(f64), Min(i64,f64), Max(i64,f64), Last, First — honoring `window` + `bucket` semantics identically to Value-path ops.
  2. `TypedRingBuffer` struct + `Shard.entity_ringbuffers_typed: AHashMap<(String,String,u16), TypedRingBuffer>` field on both fjall + inmem Shard variants.
  3. V11 snapshot serializes `entity_ringbuffers_typed` — round-trip proptest GREEN.
  4. `ShardOp::RunTypedAggCascadeStep` variant wired; target-shard handler runs `run_typed_agg_step` only (no further cascade).
  5. `PipelineEngine::run_typed_direct_cascade` replaces `run_typed_enrich_cascade` as primary walker; Value fallback preserved for non-typed-compatible downstreams.
  6. `tests/typed_windowed_aggregation_parity.rs` — 100K events, `window=5s bucket=1s`, FeatureMap identical at 20 event-time checkpoints.
  7. `tests/typed_cascade_crossshard_parity.rs` — N=8 fraud-pipeline-shaped cascade, typed-direct and Value-cascade produce byte-identical state.
  8. All Phase 59.6 SC-1..SC-10 remain GREEN.
  9. Perf gate: fraud-pipeline complex-c8-x8 aggregate EPS ≥ +10% vs Phase 59.6 baseline (1,322,525 median → ≥ 1,454,778) OR samply shows `push_internal_on_shard` <1% on state-inmem.
  10. `is_typed_cascade_compatible` returns `true` only when a feature has an actual typed implementation available (structural + semantic check).
**Plans**: 6 plans
Plans:
- [x] 59.7-00-PLAN.md — Wave 0: RED scaffolding — 14 ignored parity tests (10 windowed + 4 crossshard) + BEAVA_TYPED_CASCADE_DIRECT env flag + Criterion regression bench (3 pinned cells) + is_wave4_typed_compatible → is_typed_cascade_compatible rename + 2 new metrics counters (TYPED_CASCADE_DIRECT_DISPATCHED, TYPED_CASCADE_VALUE_FALLBACK) + REQUIREMENTS Phase 59.7 extension rows
- [x] 59.7-01-PLAN.md — Wave 1: TypedRingBuffer{I64,F64,Avg} + Shard::entity_ringbuffers_typed AHashMap field (both state-inmem + fjall variants) + 4 windowed typed agg impls (CountOpTypedWindowed / SumOpTypedWindowedI64 / SumOpTypedWindowedF64 / AvgOpTypedWindowedF64) + update_windowed trait method + 4 parity tests flipped GREEN
- [x] 59.7-02-PLAN.md — Wave 2: 6 remaining windowed typed agg impls (MinOpTypedWindowed{I64,F64} / MaxOpTypedWindowed{I64,F64} / LastOpTypedWindowedInlineStr / FirstOpTypedWindowedInlineStr) + TypedRingBufferEnum extended to 8 variants + V11 snapshot typed_ringbuffers extension (save + load + round-trip proptest) + 10/10 windowed parity tests GREEN
- [x] 59.7-03-PLAN.md — Wave 3: ShardOp::RunTypedAggCascadeStep variant + dispatch arm in src/shard/thread.rs + PipelineEngine::build_typed_agg_ops_for factory (Arc<dyn TypedAggOp> cache at finalize_dag) + get_typed_state_schema + try_extract_event_time_from_typed_row accessors + run_typed_direct_cascade_same_shard walker + 1/4 crossshard parity tests GREEN (same-shard case)
- [ ] 59.7-04-PLAN.md — Wave 4: run_typed_direct_cascade promoted to full cross-shard walker (per-downstream target_shard compute + cross-shard dispatch via RunTypedAggCascadeStep + per-downstream Value fallback for non-typed-compatible features + whole-cascade fallback for retraction-capable inputs) + 4/4 crossshard parity tests GREEN
- [ ] 59.7-05-PLAN.md — Wave 5: perf gate (fraud-pipeline best-of-3 × 2 configs + Criterion regression bench + samply probe state-inmem) + 59.7-PERF-GATE.md (C0/C1/C2/C3 ladder + HUMAN_NEEDED escalation if needed) + 59.7-VERIFICATION.md (SC-1..SC-10 table) + ROADMAP/STATE/REQUIREMENTS/docs/architecture.md updates + phase-close commit
**UI hint**: no

### Phase 60: 60-hotkey-mitigation-via-application-salting
**Goal**: Fix the Zipf-1.2 hot-shard bottleneck — today shard-0 saturates at ~450K EPS while shards 1–7 are idle (`/debug/shards` shows `inbox_depth=65536` on shard-0 vs 0 everywhere else). Under Pareto-80/20 workloads (TPC-PERF-07), a single shard is the ceiling. Approach: application-layer salting. Users declare `shard_key="user_id:salt(N)"` and Beava appends a random 0..N suffix at ingest, splitting hot keys across N virtual sub-shards. Cross-sub-shard scatter-gather on read.
**Depends on**: Phase 59 (per-event hot-path cost reduced so salt fan-out is affordable).
**Requirements**: TPC-PERF-10 (NEW — Pareto-80/20 workload EPS ≥ +50% vs uniform-key baseline on same hardware).
**Success Criteria** (what must be TRUE):
  1. SDK supports `@bv.stream(shard_key="user_id:salt(16)")` — ingest appends `:0`..`:15` suffix based on a per-event random draw or hash of a secondary field.
  2. Reads of a salted key scatter-gather across all salt values; results aggregated via the operator's existing combine semantics (sum/count are commutative; last-value reads pick the freshest by event_time).
  3. Perf gate: under Pareto-80/20 workload (matching TPC-PERF-07 cell), aggregate EPS ≥ +50% vs Phase 59 baseline on the same hardware; hot-shard `inbox_depth` stays ≤ 50% of `BEAVA_SHARD_INBOX_SIZE` under steady load.
  4. No correctness regression on uniform workload — salt is opt-in per stream.
  5. Metric `beava_shard_hot_key_owner_ratio` exposes "what % of events on this shard came from the top-1% of keys" so operators can identify candidates for salting.
**Locked decisions (2026-04-20)**:
  - Salt cardinality: power-of-2 in [2, 256] (D-A2); source tables cannot declare salt (D-D3); at most one tuple element may carry `:salt(N)` (D-A5).
  - Suffix derivation: `ahash(primary_event_id) % N` — deterministic across retries/replicas (D-B1); storage key = `"<key>:<salt_idx>"` (D-B2).
  - Read fan-out: reuse Phase 56 `ShardOp::ReadEntityBatch` — per-target-shard coalesce; same-shard salt hits stay inline (D-C3). Combine via operator's existing commutative semantics (D-C2).
  - Register-time sample-event guard rejects salt declaration when key contains `:` (D-G1); mixed-salt joins emit `SaltedJoinWarning` (D-D2) — not reject.
  - Perf-gate harness: extended `benches/pareto_workload.rs` + `benchmark/fraud-pipeline/run_bench.sh` with a fraud-pipeline variant declaring `salt(16)` on Transactions; uniform workload regression budget ±2% of Phase 59 baseline (D-F4).
  - Contingency ladder: C1 salt(64) → C2 double-salting → C3 human_needed (D-F5).
**Plans**: 5 plans (re-planned 2026-04-21 — old-design plans 01-04 replaced after interactive redirect to `salt=N` kwarg API)
Plans:
- [x] 60-00-PLAN.md — Wave 0: TPC-PERF-10 row + 22 RED integration tests (tagged `#[ignore = "60-W[1-4]"]`) + `scripts/verify-salt-feature-complete.sh` grep-gate (exits 1 pre-Wave 1) + `pareto_salted_c8_x8` bench placeholder — Complete 2026-04-21 (commits 8eaaaa4, dd37e17)
- [ ] 60-01-PLAN.md — Wave 1 (re-planned 2026-04-21): `validate_salt(n: u16)` + `SaltError` + `SaltedJoinWarning` + `StreamDefinition.salt: Option<u16> (#[serde(default)])` + Python `@bv.stream(salt=N)` kwarg + REGISTER JSON `"salt"` field + source-table rejection (D-A5) + rename 6 W1 tests from old string-DSL API
- [ ] 60-02-PLAN.md — Wave 2: `shard_hint_for_event_salted` + `derive_storage_key` + 9 `shard_hint_for_event` call-site threads in pipeline.rs + store/store_fjall salt arg + TCP/HTTP ingest wiring + D-B4 colon-in-key guard
- [ ] 60-03-PLAN.md — Wave 3: `expand_salt_variants` + `combine_salt_variants` + `dispatch_salted_read_scatter` (per-target-shard coalesce via `ShardOp::ReadEntityBatch`) + `get_entity_salted` point-read contract (D-C5, N× cost) + EnrichFromTable salted-right-side + `tests/sharding_parity.rs` salted N=1↔N=8 subcase
- [ ] 60-04-PLAN.md — Wave 4: 3 new metrics (`beava_shard_hot_key_owner_ratio`, `beava_salt_fanout_reads_total`, `beava_salt_ingest_writes_total`) + `salted_streams` on `/debug/shards` + Pareto best-of-3 perf-gate (D-F2 throughput ≥+50% AND D-F3 NEW p99 read latency ≤2× unsalted) + D-F4 ±2% uniform regression + D-F5 inbox gate + `60-PERF-GATE.md` + `60-VERIFICATION.md` + `docs/architecture-tpc.md § hot-key salting` + human verify + close
**UI hint**: no

### Phase 61: 61-metrics-hot-path-hoist
**Goal**: Eliminate per-event `metrics_util::Registry::get_or_create_counter` overhead (~3.5% of CPU in current profile). Cache counter/histogram handles at stream-register time; the per-event path becomes a pointer dereference + atomic increment.
**Depends on**: Phase 60 (optimizations ordered biggest-lever-first).
**Requirements**: TPC-PERF-11 (NEW — metrics overhead ≤ 0.5% of CPU).
**Success Criteria** (what must be TRUE):
  1. samply shows `metrics_util::*` ≤ 0.5% of leaf samples.
  2. `register_stream` + `register_table` pre-allocate counter handles for every metric the stream touches on its hot path; stored in `StreamState::counters: [Arc<AtomicU64>; N_METRICS]`.
  3. Perf gate: ≥ +3% EPS vs Phase 60 baseline.
**Plans**: TBD
**UI hint**: no

### Phase 62: 62-allocator-and-feature-row-pooling
**Goal**: Reduce allocator churn on the hot path — `Vec::drop`, `RawVecInner::reserve`, `indexmap::insert_full` collectively cost ~10% of CPU. Pool `Vec<FeatureValue>` buffers per shard thread; pre-size IndexMap buckets at register time; use bumpalo for per-batch scratch allocations.
**Depends on**: Phase 61.
**Requirements**: TPC-PERF-12 (NEW — allocator churn ≤ 3% of CPU).
**Success Criteria** (what must be TRUE):
  1. samply shows `Vec::drop` + `RawVecInner::reserve` + allocator frames combined ≤ 3% of leaf samples.
  2. Feature rows are allocated from a per-shard pool; `Vec::drop` at the end of batch returns buffers to the pool instead of freeing.
  3. IndexMap capacity pre-sized at stream registration based on `max_keys` or a default.
  4. Perf gate: ≥ +5% EPS vs Phase 61 baseline.
**Plans**: TBD
**UI hint**: no

### Phase 63: 63-fjall-cache-and-compaction-tuning
**Goal**: Close the fjall-vs-inmem throughput gap. Today fjall is ~93% of inmem on complex N=8 (lz4_flex decompression is the 6% tax). Tune block cache size to working-set; increase memtable size to reduce flush frequency; tune compaction schedule for the measured ingest rate.
**Depends on**: Phase 62.
**Requirements**: TPC-PERF-13 (NEW — fjall EPS ≥ 95% of inmem EPS on the standard complex N=8 bench).
**Success Criteria** (what must be TRUE):
  1. `MODE=complex N=8 DURATION=60` shows fjall EPS ≥ 95% of inmem EPS on the reference box.
  2. `BEAVA_FJALL_CACHE_MB` default tuning formula revised based on Phase 62 measurements; `BEAVA_FJALL_MAX_MEMTABLE_MB` default revised.
  3. lz4 decompression cost ≤ 3% of CPU (currently ~6%).
**Plans**: TBD
**UI hint**: no

### Phase 64: 64-rust-bench-client
**Goal**: Unblock true server-ceiling measurement by eliminating the Python + GIL bottleneck in the bench harness. `bench.py` clients cap at ~200–400K EPS each due to GIL + TCP framing in Python; this has been bounding our server measurements to ~1.3M EPS aggregate at CPUs=8. Replace with a Rust harness that can push ≥ 2M EPS per client.
**Depends on**: Phase 63.
**Requirements**: TPC-PERF-14 (NEW — bench client ceiling ≥ 10× current Python per-client rate).
**Success Criteria** (what must be TRUE):
  1. `benchmark/fraud-pipeline/bench-rust` binary replicates the Python bench.py feature set (Zipfian key distribution, checkpoint JSONL, latency sampling, phase=final envelope with error semantics matching Deviation-4 fix).
  2. Single Rust client sustains ≥ 2M EPS against a localhost beava server.
  3. Re-runs of the Phase 54 baseline bench using the Rust client measure a new server ceiling and document it — this phase is an **instrumentation unlock**, not a perf optimization; the server EPS number may reveal new bottlenecks to queue as Phase 65+.
**Plans**: TBD
**UI hint**: no

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 48. Shard-hint scaffolding | 3/3 | Complete | 2026-04-18 |
| 49. Per-shard state store | 6/6 | Complete | 2026-04-18 |
| 50. Multi-shard routing | 8/8 plans | **Partial — gaps_found** (shard thread stub; see 50-DEBUG-SESSION.md + 50.5-FIX-PLAN.md; Fix #1 restores N=1 parity in progress) | — |
| 50.5. Shard-thread completion | 3/3 | Complete    | 2026-04-19 |
| 51. Cross-shard queries + joins | 5/5 | Complete    | 2026-04-19 |
| 52. Event log, recovery, ship-gate | 10/10 | Complete    | 2026-04-19 |
| 53. Fjall state backend | 6/7 plans (06 deferred) | **Engineering-complete** — 4/6 TPC-PERSIST closed; PERSIST-04 + PERSIST-05A gates deferred to Phase 54 (legacy DashMap bypass at N=1) | 2026-04-19 |
| 54. Legacy engine removal | 6/6 | **Engineering-complete** — TPC-ARCH-01 ✅ + TPC-PERSIST-05A ✅ closed; TPC-PERSIST-04 human_needed (Hetzner CCX43 8h soak; evidence-file gated). pprof DashMap → 0% in top-20; EPS +580% (197K → 1.34M). | 2026-04-20 (eng) |
| 55. Stream→Table cascade cross-shard + source tables | 5/5 | Complete    | 2026-04-20 |
| 56. EnrichFromTable + StreamStreamJoin cross-shard | 5/5 | **Engineering-complete** — TPC-CORR-08 ✅ + TPC-CORR-09 ✅ closed; TPC-CORR-04 relaxation landed. Default-pipeline perf gate 1,195,914 EPS PASSED (+12.9% over 1,059,261 floor; −4.0% vs P55 baseline). Cross-shard scenario SC-5 human_needed — Phase 55 SDK source-table wire-registration gap (56-NEXT #6). | 2026-04-21 |
| 57. Retraction across cross-shard joins | 4/5 | In Progress|  |
| 58. Tokio connection-handling rewrite | 5/5 | **Engineering-complete** — structural tokio-churn elimination landed on both Linux (SO_REUSEPORT per-shard + FuturesUnordered inline handler) and macOS (dedicated `std::thread` per shard + handle_connection_blocking); 0 `tokio::spawn(handle_connection)` in production PUSH path. Perf gate 1,376,450 EPS (+6.1% vs P57) on macOS dev host — 15.1% below 1,621,616 floor; p99 parity (−0.11%); SC-1 + SC-3 `human_needed` pending Linux prod-host run + probe-harness extension (58-NEXT #1). | 2026-04-21 (eng) |
| 59. Binary wire format for PUSH | 5/5 | **Engineering-complete** — TCP OP_PUSH `bytes::Bytes` end-to-end (no JSON re-serialize); `OP_NEGOTIATE_WIRE_FORMAT=0x18` + Python SDK handshake (v0.2.0); D-B3 JSON-over-TCP compat ≥1 release cycle; `BEAVA_MAX_PAYLOAD_BYTES` DoS cap. Samply D-D3 PASSED (JSON_SHARE_PCT=2.5 ≤ 3.0). p99 latency −15% IMPROVED. Perf gate best-of-3 1,494,631 EPS = −1.3% below 1,514,095 floor within 6% run variance on macOS; SC-4 human_needed pending Linux prod-host re-run (59-NEXT #1). | 2026-04-21 (eng) |
| 60. Hot-key mitigation via application salting | 0/5 | Planned 2026-04-20 — plans landed; awaiting Wave 0 execution. Architectural fix for Zipf hot-shard ceiling (≥+50% under Pareto-80/20); delivers TPC-PERF-10. | — |
| 61. Metrics hot-path hoist | 0/? | Not started — 3.5% CPU savings | — |
| 62. Allocator + feature-row pooling | 0/? | Not started — 5-10% CPU savings | — |
| 63. fjall cache + compaction tuning | 0/? | Not started — close fjall→inmem gap to ≥95% | — |
| 64. Rust bench client | 0/? | Not started — instrumentation unlock; removes Python GIL ceiling | — |
