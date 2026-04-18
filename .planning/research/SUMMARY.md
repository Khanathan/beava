# v1.2 TPC — Research Synthesis

**Milestone:** v1.2 Thread-Per-Core + Full Key-Shard
**Date:** 2026-04-18
**Sources:** STACK.md · FEATURES.md · ARCHITECTURE.md · PITFALLS.md (+ `.planning/arch/TPC-SHARD-DESIGN.md` + `.planning/arch/TPC-RESEARCH.md`)
**Confidence:** HIGH — all claims backed by direct source-code inspection or two prior design docs.

---

## User decisions locked 2026-04-18

1. **Backpressure contract:** SPSC bounded queue, drop on overflow, increment `beava_shard_inbox_full_total{shard="N"}`, return HTTP 503 / TCP error. Client retries handle recovery.
2. **Snapshot `shard_count` mismatch at boot:** hard-fail with actionable error (`"run 'tally reshard --from N --to K' then restart"`). No silent boot-empty.
3. **Tuple `shard_key` missing field on event:** reject at ingest, increment `beava_events_dropped_total{reason="shard_key_missing"}`, return HTTP 400.
4. **N=1 ↔ N=K property parity test:** lands in Wave 5 / pre-ship gate (already scoped there).

## Scope headlines — what v1.2 ships

1. `BEAVA_SHARDS` env + `--shards` CLI flag; N=1 in debug, `num_cpus::get_physical()` in release.
2. `EventSource::shard_hint()` trait wired through TCP + HTTP ingest — Wave 0 no-op (N_SHARDS=1), Wave 2 live.
3. Per-shard `Shard` struct (`AHashMap` state, plain `HashSet` dirty-set, `WatermarkState`, `EventLog`) replacing DashMap + ArcSwap in hot paths.
4. Listener→shard SPSC bounded channels with drop-on-full + `beava_shard_inbox_full_total` + HTTP 503.
5. `SO_REUSEPORT`-per-shard accept on Linux; single-listener fallback on macOS.
6. Per-shard labeled metrics via `metrics` + `metrics-exporter-prometheus` — NEW to Cargo.toml; adds `/metrics` scrape endpoint.
7. `GET /debug/shards` (inbox depth, reactor utilization, keys owned) + shard-recovery-aware `/health` / `/ready`.
8. `GET /streams` scatter-gather; `JoinShardKeyMismatch` enforced at register time with actionable error.
9. Snapshot format v8 with `shard_count: u16` tail field; hard-fail on mismatch at boot.
10. Parallel per-shard recovery + `tally reshard --from N --to K` offline migration tool.
11. `@bv.stream(shard_key=...)` Python SDK; `ShardKeyMissingWarning` on `/debug/warnings` when `shard_key` omitted at N>1.
12. N=1 ↔ N=8 property parity gate (proptest) + Pareto-workload benchmark added to 9-cell matrix.

## What v1.2 explicitly defers

- **compio runtime migration** — v1.3 / Beava Cloud. v1.2 stays on tokio `current_thread` + `build_local()`.
- **io_uring syscall batching strategy** — relevant only after compio swap.
- **NUMA awareness on 32+ core boxes** — Beava Cloud era.
- **Hot-key salting as a framework concern** — application-level mitigation; Beava surfaces the problem via metrics.
- **Python SDK breaking changes** — existing pipelines keep working (fallback to primary-key field at N=1; warn at N>1).

## Stack additions (NEW deps — compressed from STACK.md)

| Crate | Version | Purpose | Wave |
|---|---|---|---|
| `num_cpus` | 1.17 | physical-core detection for default N_SHARDS | 1 |
| `core_affinity` | 0.8.3 | shard-thread pinning | 2 |
| `crossbeam-channel` | 0.5.15 | SPSC listener→shard bounded queue | 2 |
| `ahash` | existing re-export | tuple shard_key hashing | 1 |
| `metrics` | 0.24 | per-shard labeled metrics | 2 |
| `metrics-exporter-prometheus` | 0.16 | `/metrics` scrape endpoint | 2 |
| `rstest` | 0.26 | shard-count-parameterized tests | 1+ |
| `futures` | 0.3 | `join_all` for scatter-gather | 3 |
| `proptest` | existing | N=1↔N=K parity harness | 5 / ship-gate |

**Existing deps affected:** `dashmap` and `arc-swap` stay until Wave 4 (compat shims for StateStore); `tokio` `current_thread` flavor is already present. Snapshot serializer is `postcard` — tail-append `shard_count: u16` for backward compat (v6→v7 pattern reused).

## Wave-by-wave scope (for roadmapper)

### Wave 0 — Shard-hint scaffolding (NEW phase)
- **Ships:** `EventSource::shard_hint(&self, event) -> u32` trait method (default impl hashes primary key); TCP + HTTP parsers compute and thread `shard_hint` (always 0 when N=1); micro-benches for `hash(key)` <100 ns and SPSC roundtrip <10 μs.
- **Requires:** no new Cargo deps yet; `rstest` added for test parameterization.
- **Ship-gate:** 9-cell matrix within ±1% of baseline (scaffolding is no-op at N=1).

### Wave 1 — Per-shard state store
- **Ships:** `Shard` struct in `src/state/shard.rs` (AHashMap state, HashSet dirty, WatermarkState, event-log handle); runtime `BEAVA_SHARDS` env + `--shards` CLI parsed; `StateStore` gains a one-shard code path that round-trips through `Shard`; full test suite passes at N=1.
- **Requires:** `num_cpus`, `ahash` (existing), `rstest`.
- **Ship-gate:** 9-cell matrix within −5% of baseline at N=1; all integration tests green.

### Wave 2 — Multi-shard routing
- **Ships:** `core_affinity`-pinned shard threads; listener→shard SPSC channels (`crossbeam-channel::bounded`); drop-on-full + `beava_shard_inbox_full_total` + HTTP 503; `SO_REUSEPORT`-per-shard accept on Linux; single-listener fallback on macOS; `metrics` + `metrics-exporter-prometheus` + `/metrics` scrape endpoint; six per-shard labeled metrics (reactor_utilization, inbox_depth, events_total, keys_owned, watermark_lag, inbox_full_total).
- **Requires:** `core_affinity`, `crossbeam-channel`, `metrics`, `metrics-exporter-prometheus`.
- **Ship-gate:** ≥3× baseline on `complex-c8-x8` at N=CPU_COUNT; `shard_probe` cross_shard_fraction <40% on release workload.

### Wave 3 — Cross-shard queries + joins
- **Ships:** `GET /streams` scatter-gather via `futures::join_all`; `JoinShardKeyMismatch` enforced at register time with actionable error naming both streams + suggested decorator fix; lazy global-watermark publish via per-shard atomics; `GET /debug/shards` (inbox depth, utilization, keys owned, hot-shard detection).
- **Requires:** `futures`.
- **Ship-gate:** scatter-gather latency <15 μs for `/streams`; all existing join tests green at N>1 (with shard_key co-location declared).

### Wave 4 — Per-shard event log + recovery + fork/replica + reshard tool
- **Ships:** `data/shard-N/streams/{name}/log.bin` layout; parallel per-shard recovery; `tally reshard --from N --to K` offline tool (atomic swap of data dir); snapshot v8 with `shard_count: u16` + hard-fail on mismatch at boot; fork/replica always re-hashes on ingest by downstream N (no `--reshard-from` CLI flag); delete DashMap + ArcSwap from StateStore; `docs/architecture-tpc.md`; updated `docs/operations.md` with shard sizing / hot-shard diagnosis.
- **Requires:** nothing new; opens removal of `dashmap` + `arc-swap` deps from `Cargo.toml`.
- **Ship-gate:** parallel recovery time ≤ (single-thread) / N × 1.3 on 4.7 GB state; N=1 → N=8 reshard tool round-trip byte-identical state; fork/replica parity test (upstream & downstream feature values agree).

### Wave 5 — Production readiness (absorbed ship-gate)
- **Ships:** N=1↔N=8 proptest parity harness (same event stream → identical feature values across shard counts, all operators); sustained 1M+ EPS load test on 16-core reference box (committed benchmark); failover test (kill one shard thread → graceful degradation); Pareto-workload benchmark added to 9-cell matrix (validates cross_shard_fraction gate on skewed data).
- **Requires:** `proptest` (existing), `oha` (existing from v1.0-launch).
- **Ship-gate:** all three hard gates: (1) N=1 within −5% of baseline; (2) ≥3× on `complex-c8-x8` at N=CPU_COUNT; (3) shard_probe cross_shard_fraction <40%.

## Candidate requirements (seeds for REQUIREMENTS.md — 24 items)

**TPC-INFRA (plumbing, config, observability):**
- TPC-INFRA-01: `EventSource::shard_hint()` trait wired through TCP + HTTP parsers
- TPC-INFRA-02: `BEAVA_SHARDS` env + `--shards` CLI flag with debug=1 / release=physical default
- TPC-INFRA-03: `metrics` + `metrics-exporter-prometheus` integrated; `/metrics` scrape endpoint
- TPC-INFRA-04: Six per-shard labeled metrics (reactor_utilization, inbox_depth, events_total, keys_owned, watermark_lag, inbox_full_total)
- TPC-INFRA-05: `GET /debug/shards` endpoint exposing per-shard diagnostics + hot-shard detection
- TPC-INFRA-06: Shard-recovery-aware `/health` + `/ready` (ready waits for all shards to complete recovery)
- TPC-INFRA-07: Rename `BEAVA_ENTITIES_SHARDS` or deprecate to avoid naming collision with `BEAVA_SHARDS`

**TPC-PERF (throughput, routing, pinning):**
- TPC-PERF-01: Per-shard `Shard` struct (AHashMap + HashSet + WatermarkState + EventLog) — no shared DashMap on hot path
- TPC-PERF-02: `core_affinity`-pinned shard threads with best-effort macOS pinning
- TPC-PERF-03: `crossbeam-channel::bounded` SPSC listener→shard handoff
- TPC-PERF-04: `SO_REUSEPORT`-per-shard accept on Linux; single-listener fallback on macOS
- TPC-PERF-05: `GET /streams` scatter-gather via `futures::join_all`
- TPC-PERF-06: Lazy global-watermark publish across shards (per-shard atomics, global = min)
- TPC-PERF-07: 9-cell benchmark matrix + Pareto-workload cell added

**TPC-CORR (correctness guards, determinism):**
- TPC-CORR-01: Backpressure contract — drop on inbox full + `beava_shard_inbox_full_total` + HTTP 503
- TPC-CORR-02: Snapshot shard_count mismatch hard-fails boot with actionable error
- TPC-CORR-03: Tuple shard_key missing-field rejects event + `beava_events_dropped_total{reason="shard_key_missing"}` + HTTP 400
- TPC-CORR-04: `JoinShardKeyMismatch` enforced at register time (fatal, names both streams)
- TPC-CORR-05: N=1 ↔ N=8 property parity proptest (all operators) — pre-ship gate
- TPC-CORR-06: Fork/replica always re-hashes on ingest by downstream N (no `--reshard-from` flag)

**TPC-DX (user-facing surfaces):**
- TPC-DX-01: `@bv.stream(shard_key=...)` Python SDK decorator with tuple multi-field support
- TPC-DX-02: `ShardKeyMissingWarning` on `/debug/warnings` when `shard_key` omitted and N>1
- TPC-DX-03: `tally reshard --from N --to K` offline migration tool
- TPC-DX-04: `docs/architecture-tpc.md` + shard-sizing section in `docs/operations.md`

## Pitfall → phase mapping

| Pitfall (from PITFALLS.md) | Severity | Addressed in | Guard |
|---|---|---|---|
| Cascading overload / inbox full | launch-gate | Wave 2 | TPC-CORR-01 backpressure contract |
| Silent empty-state on rolling restart | launch-gate | Wave 4 | TPC-CORR-02 hard-fail guard |
| Tuple shard_key missing field | launch-gate | Wave 2 (enforcement) + Wave 1 (detection) | TPC-CORR-03 reject + counter |
| Inter-shard join ordering | launch-gate | Wave 3 (co-location) + Wave 5 (parity test) | TPC-CORR-04 + TPC-CORR-05 |
| Fork/replica double-count window | ship-gate | Wave 4 | **GAP — see below** |
| Test-suite fragility | ship-gate | Wave 1 (N=1 test pass) + Wave 5 (parity) | TPC-CORR-05 |
| Hot-shard blind spot | ship-gate | Wave 3 `/debug/shards` | TPC-INFRA-05 |
| Silent fallback to primary-key field | launch-gate | Wave 1 | TPC-DX-02 warning |

## Conflicts resolved

- **`shard_key` omitted vs missing field** — FEATURES.md's `ShardKeyMissingWarning` and PITFALLS.md's "missing field crash" are TWO distinct cases, not a conflict. Omitted `shard_key=` decorator parameter → warn + route to shard 0 (TPC-DX-02). Declared tuple `shard_key=(...)` but event is missing a field → HTTP 400 + counter (TPC-CORR-03).
- **ArcSwap dirty-set lifespan** — ARCHITECTURE.md says ArcSwap survives through Wave 3 as a compat shim; design doc reads like it's gone at Wave 1. Synthesis: ArcSwap stays until Wave 4 (when DashMap is deleted together). Wave 1 introduces `Shard` but `StateStore` still exists as the outer facade.

## Open questions still unresolved (research could not close)

1. **Fork/replica double-emit dedup.** Upstream rolling restart while replica is subscribed can re-emit a log range. Wave 4 needs an LSN-based dedup plan; design doc silent on this. **Action:** one-hour design sub-plan at Wave 4 kickoff, not blocking earlier waves.
2. **Hot-shard detection threshold.** `/debug/shards` should flag imbalance but the exact cross_shard_fraction / keys_owned ratio that triggers the warning is TBD. **Action:** pick per Wave 3 plan from measured 9-cell matrix cross_shard_fraction distribution.
3. **compio macOS-kqueue vs Linux io_uring throughput.** Not blocking for v1.2 (we're on tokio). Settle before v1.3 planning.
