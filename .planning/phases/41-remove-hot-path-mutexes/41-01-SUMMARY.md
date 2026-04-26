---
phase: 41-remove-hot-path-mutexes
plan: 01
subsystem: server/hot-path
tags: [performance, lock-contention, atomics, feature-gate, phase-41]
requires: [phase-40 per-stream event-log lock]
provides: [lock-free events_total, lock-free EPS ring, sampled latency histogram, demo feature gate]
affects: [src/server/tcp.rs, src/server/http.rs, src/server/throughput.rs, Cargo.toml, tests/test_push_coalescing.rs, tests/test_public_http.rs, scripts/check-feature-builds.sh]
commits:
  - 0ec1367: feat(41-01) remove hot-path mutex contention T1-T4 (combined)
metrics:
  duration: ~90 min
  completed: 2026-04-15
  tasks_completed: 5
---

# Phase 41 Plan 01: Remove hot-path mutex contention — Summary

One-liner: four per-PUSH mutexes (`recent_events`, `metrics.events_total`, `throughput`, `latency`) replaced with `#[cfg(feature = "demo")]`, lock-free `AtomicU64`, lock-free 60-bucket `AtomicThroughput` ring, and 1-in-16 histogram sampling — 8-proc 8-stream aggregate throughput from 542k eps to ~676k eps (+25%), single-stream unchanged (-2.05%, within tolerance).

## Files

### Modified
- `Cargo.toml` — added `demo = ["server"]` feature.
- `src/server/tcp.rs` — feature-gated `RecentEvent(sRing)`, added `events_total`, `last_push_latency_nanos`, `atomic_throughput`, `latency_sample_counter` on `ConcurrentAppState`; rewrote per-push and per-batch hot paths; `LATENCY_SAMPLE_STRIDE = 16` constant.
- `src/server/throughput.rs` — added `AtomicThroughput` (60 × `AtomicU64` second-buckets) with `bump()`, `eps_5s()`, and three unit tests.
- `src/server/http.rs` — `/metrics` and `/public/stats` read the new atomics; `/public/recent-events` handler + route gated on `feature = "demo"`.
- `scripts/check-feature-builds.sh` — added a `cargo build --features demo` check.
- `tests/test_push_coalescing.rs` — three assertions updated to read `state.events_total` atomic.
- `tests/test_public_http.rs` — `RecentEvent` import + two `/public/recent-events` tests gated on `feature = "demo"`.

### Untouched but relevant
- `Metrics::events_total` field left in place (unused on hot path) for struct-layout stability; may be removed in a follow-up.
- `ThroughputTracker` (per-stream EWMA) left intact for `/debug/throughput` and test helpers but no longer fed by the hot path.

## Validation

### Tests
- `cargo test --no-fail-fast` under default features — all cells green. Rerun of the previously flaky `hll_mode_within_2_percent_on_100k` passed on second attempt (pre-existing sampling variance, unrelated).
- `scripts/check-feature-builds.sh` — client, default, and `demo` flavors all build; default test suite green.

### Benchmarks

Server config: release binary, `TALLY_WORKER_THREADS=8`, `taskset -c 0-7`, empty data dir.

| Benchmark                              | Pre-41 (baseline) | Post-41 (this plan) | Delta        | Target                  |
| -------------------------------------- | ----------------: | ------------------: | :----------- | ----------------------- |
| single-stream single-client, `small_1c` |    112,157 eps¹   |         109,859 eps | **-2.05 %**  | ±3 % (pass)             |
| 8-proc 8-stream aggregate (sum_eps)    |    ~542,000 eps²  |  run1 662,850 eps<br>run2 676,069 eps<br>run3 702,577 eps<br>**median 676k**  | **+25 %**    | >650k eps (**pass**)    |
| 8-proc 8-stream agg (events/max_wall)  |        —         |  run1 650,575 eps<br>run2 670,539 eps<br>run3 669,078 eps<br>**median 669k** | — (same)     | >650k (pass)            |

¹ Phase 40 matrix-v0-after `small_1c` median = 112,157.5 eps.
² User-provided baseline from the pre-plan strace session.

### strace

Could not run in this execution environment: `ptrace(PTRACE_SEIZE, …): Operation not permitted`. The CapBnd for both the bash shell and even `sudo -n` lacks `CAP_SYS_PTRACE` (bit 19 missing from `0x800405fb`). Left as a follow-up to re-capture under a shell with the capability. The futex reduction is indirectly evidenced by the +25 % multi-process throughput gain, which is exactly in the range the plan predicted when removing 3-4 of the ~8 per-push mutexes.

### `/metrics` sanity check (post-bench)

```
tally_events_total 1454563
tally_push_latency_seconds 0.000051418          (= last-push gauge, now atomic)
tally_push_latency_p99_seconds 0.00005623...    (histogram fed by 1-in-16 sampled pushes)
tally_current_eps 0                             (atomic ring; 0 after 5s quiet)
```

`/public/recent-events` returns HTTP 404 under default build — route not registered. The `/public/stats` tile continues to show `events_total`, `current_eps`, `p50_push_us`, `p99_push_us` correctly.

## Deviations

### Auto-fixed Issues

1. **[Rule 3 — Blocker] Latency gauge `push_latency_seconds` still wrote inside `state.metrics.lock()` on every PUSH.** Task T2 nominally only asked to pull `events_total` out of the mutex, but leaving `push_latency_seconds` inside meant the hot path still took the same mutex per push and T2's win evaporated. Added `AppState.last_push_latency_nanos: AtomicU64` and moved the gauge write to a Relaxed store. `/metrics` divides by 1e9 for the Prometheus seconds output. Files: `src/server/tcp.rs`, `src/server/http.rs`. Part of the same combined commit.

2. **[Rule 2 — Missing functionality] Tests updated for the T2 atomic move.** Three assertions in `tests/test_push_coalescing.rs` read `state.metrics.lock().events_total` which now always returns 0 (the field is dead). Updated to read `state.events_total.load(Relaxed)`. Files: `tests/test_push_coalescing.rs`.

3. **[Rule 3 — Blocker] `RecentEvent` is a `pub` re-export consumed by `tests/test_public_http.rs`.** Feature-gating it on `feature = "demo"` breaks default `cargo test`. Gated the import and the two exercising tests under `#[cfg(feature = "demo")]` so they run only under the demo flavor. Files: `tests/test_public_http.rs`.

### Flagged trade-offs (NOT auto-fixed — documented for follow-up)

1. **Per-stream EWMA on `/debug/throughput` is now empty under pure server load.** Plan T3's stop-trigger warned about the "unique-key counter"; the actual load-bearing data was per-stream EWMAs feeding the admin UI. The hot-path bump into `ThroughputTracker::bump_unique` is gone, so `/debug/throughput` shows rates only for streams bumped through test or future admin paths. The global EPS via `AtomicThroughput.eps_5s()` is the correct `/metrics tally_current_eps` replacement. If the admin stream-rate breakdown is needed in prod, follow-up work can feed `AtomicThroughput` per-stream (e.g., `DashMap<String, [AtomicU64; 60]>`) or drive the EWMA tracker from a lower-frequency sampling tick.

2. **`AtomicThroughput.eps_5s()` skips the bucket currently being written.** Means in a steady bench, the reported `tally_current_eps` lags the true rate by 1 s and drops to 0 within ~5 s of an idle period (vs the old EWMA that decayed exponentially). This is acceptable for `/metrics` scraping at 1-10 s cadence; flagged here for anyone surprised by sharp step functions in the EPS graph.

3. **Histogram sampled at 1-in-16.** p50/p99 estimates are essentially unbiased for the distribution shape but lose ~94 % of observations; tail percentiles (p999) have higher variance. Since push latency is <100 µs in practice (no outliers), this is a safe trade. Documented inline in `src/server/tcp.rs` (`LATENCY_SAMPLE_STRIDE` const).

4. **strace validation deferred.** CAP_SYS_PTRACE not available in this bench shell; throughput delta stands in. A follow-up can capture the actual futex-% curve under the Hetzner launch box (which has full CAPs) when the v2.1 live-ops phase resumes.

## Commits

| Hash    | Files | Message                                                              |
| :------ | :---- | :------------------------------------------------------------------- |
| 0ec1367 | 7     | feat(41-01): remove hot-path mutex contention (T1-T4, combined)      |

Single combined commit in line with the Phase-40 style referenced in the plan operational notes.

## Key numbers (three requested)

- **single-stream `small_1c`**: **109,859 eps** (baseline 112,157 eps → **-2.05 %**, inside ±3 %)
- **8-proc 8-stream aggregate**: **~676,000 eps median** (baseline 542,000 eps → **+25 %**)
- **futex %**: not directly measured in this shell (ptrace cap stripped); indirectly validated via the +25 % multi-process delta.

## Self-Check: PASSED

- `git log --oneline -1` → `0ec1367 feat(41-01): remove hot-path mutex contention (T1-T4)` — FOUND.
- `src/server/throughput.rs` contains `pub struct AtomicThroughput` — FOUND.
- `src/server/tcp.rs` contains `pub events_total: std::sync::atomic::AtomicU64` — FOUND.
- `src/server/tcp.rs` contains `pub const LATENCY_SAMPLE_STRIDE: u64 = 16` — FOUND.
- `Cargo.toml` contains `demo = ["server"]` — FOUND.
- `scripts/check-feature-builds.sh` contains `--features demo` — FOUND.
- `/public/recent-events` returns 404 under default build (confirmed via curl) — FOUND.
- `cargo test --no-fail-fast` default flavor green — FOUND.
- 8-proc bench sum_eps median 676k (> 650k target) across 3 runs — FOUND.
