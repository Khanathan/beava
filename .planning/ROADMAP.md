# Beava Roadmap — v1.2 Thread-Per-Core + Full Key-Shard

## Milestones

- [x] **v1.0 -- Foundation** (Phases 1-5) -- Complete 2026-04-09 -- archived
- [x] **v1.1–v1.3 -- Event Log, Pipelines, Concurrency** (Phases 6-15) -- Complete 2026-04-12
- [x] **v2.0 -- API & Engine** (Phases 16-19) -- Complete 2026-04-13
- [x] **v2.1 -- Launch** (Phase 20) -- Engineering complete 2026-04-14 (live-run ops pending, calendar-gated) -- `.planning/milestones/v2.1-ROADMAP.md`
- [x] **v0 -- Restructure + Data-Scientist Fork** (Phases 21-38) -- Phases 21-27, 36-37 complete; Phases 35, 38 planned.
- [x] **v1.0-launch -- Public Launch Readiness** (Phases 45-47) -- Engineering complete 2026-04-17 -- `.planning/milestones/v1.0-launch-ROADMAP.md`
- [ ] **v1.2 -- Thread-Per-Core + Full Key-Shard** (Phases 48-52) -- Active 2026-04-18

## Phases

- [ ] **Phase 48: 48-shard-hint-scaffolding** — Wire `EventSource::shard_hint()` through every push path; establish micro-bench gates (no routing change at N=1)
- [ ] **Phase 49: 49-per-shard-state-store** — Introduce `Shard` struct with per-shard AHashMap state; `BEAVA_SHARDS` env + CLI flag; full test suite green at N=1
- [ ] **Phase 50: 50-multi-shard-routing** — SO_REUSEPORT shard accept on Linux, SPSC channels, core_affinity pinning, backpressure contract, per-shard labeled metrics; ≥3× baseline on `complex-c8-x8` at N=CPU_COUNT
- [ ] **Phase 51: 51-cross-shard-queries-joins** — `GET /streams` scatter-gather, `JoinShardKeyMismatch` at register time, lazy global watermark, `GET /debug/shards` hot-shard visibility
- [ ] **Phase 52: 52-event-log-recovery-ship-gate** — Per-shard log layout, parallel recovery, `tally reshard` tool, snapshot v8 hard-fail guard, fork/replica re-hash, N=1↔N=8 proptest parity, 1M+ EPS load test, architecture docs

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
- [ ] 48-01-PLAN.md — TDD: `src/routing/shard_hint.rs` trait + ahash default impl + TCP/HTTP call-site wiring (Wave 1)
- [ ] 48-02-PLAN.md — Criterion bench `benches/shard_scaffold.rs` with 3 event shapes, <100 ns gate (Wave 2)
- [ ] 48-03-PLAN.md — Nightly CI workflow `bench-nightly.yml` + committed baseline `benchmark/shard_scaffold/README.md` (Wave 3)
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
**Plans**: TBD
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
**Plans**: TBD
**UI hint**: no

### Phase 51: 51-cross-shard-queries-joins
**Goal**: Read paths that touch multiple shards — stream listing, global watermark, and join validation — are correctly scatter-gathered or enforced at register time, with hot-shard visibility via `GET /debug/shards`.
**Depends on**: Phase 50
**Requirements**: TPC-INFRA-05, TPC-PERF-05, TPC-PERF-06, TPC-CORR-04
**Success Criteria** (what must be TRUE):
  1. A user calls `GET /streams` and receives the fleet-wide stream list merged from all shards; the response includes `scatter_latency_us` and the added p99 latency vs a point query is <15 μs.
  2. A user registering a join between two streams with differing `shard_key=` declarations receives a `JoinShardKeyMismatch` error that names both streams, both keys, and shows the exact decorator fix; the pipeline does not start.
  3. An operator calls `GET /debug/shards` and receives per-shard diagnostics (inbox depth, reactor utilization, keys owned, watermark lag); a shard whose `keys_owned` exceeds 2× the fleet mean is flagged with `status: "hot"` in the response.
  4. Each shard publishes its per-stream max event-time to a global atomic; the global watermark for any stream is `min` across per-shard atomics; per-entity TTL eviction reads only the shard-local watermark (no cross-shard read on the eviction hot path).
**Plans**: TBD
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

## Progress

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 48. Shard-hint scaffolding | 0/3 | Planned | — |
| 49. Per-shard state store | 0/? | Not started | — |
| 50. Multi-shard routing | 0/? | Not started | — |
| 51. Cross-shard queries + joins | 0/? | Not started | — |
| 52. Event log, recovery, ship-gate | 0/? | Not started | — |
