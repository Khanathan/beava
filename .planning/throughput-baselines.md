# Beava v2 — End-to-End Throughput Baselines

**Created:** 2026-04-23 (Phase 7.5 — first baseline)
**Regression gates:** 10% slower than baseline in same hw-class on the
**simple-fraud (small) shape** = WARNING; 25% slower = BLOCKER. See
CLAUDE.md §Performance Discipline.

## How to read this file

Throughput baselines are recorded per **hw-class**, not per machine. A
hw-class is the tuple `(cpu-arch-family, OS family, core count bucket)` —
e.g. `apple-m4 / darwin-24.3.0 / 10 cores`. Regression checks compare a new
harness run against the same hw-class only.

To capture a hw-class string:
```bash
echo "$(uname -sr | tr ' ' '-') / $(getconf _NPROCESSORS_ONLN) cores"
```

Numbers come from `crates/beava-bench` driving the live `beava` server
end-to-end (HTTP body parse → schema validation → idem-cache → WAL append +
fsync wait → apply → response). The harness uses an in-process `TestServer`
spawned with the production `Server::bind` path so the WAL + snapshot are
on real disk; only the network roundtrip is replaced by an in-process
HTTP/TCP listener. macOS results inherit the Phase 6 hw-class fsync ceiling
(~7.4 ms P50 for `F_FULLSYNC`) — the headline EPS numbers are
fsync-bottlenecked across all pipeline sizes.

## Per-phase regression contract

Every phase from 8 onward MUST include a **throughput run** task that
re-runs `beava-bench` against the small/medium/large pipelines (plus any
phase-specific variant the operator family introduces) and appends a row
per (size, transport) to this ledger. Plan-checker rejects Phase 8+ plans
without such a task.

## Reproduce

```bash
cargo run -p beava-bench --release -- \
    --pipeline {small|medium|large} \
    --transport {http|tcp} \
    --duration-secs 60 \
    --parallel 8
```

Markdown ledger row prints to stdout; copy it into the matching hw-class
section below.

## hw-class: Apple-M4 / Darwin-24.3.0 / 10 cores

Captured: 2026-04-23. Commit: `7d8f6aa..` (Phase 7.5 Plan 03).

| Phase | Date | Pipeline | Transport | Sustained EPS | Push P50 (µs) | Push P95 (µs) | Push P99 (µs) | Get P99 (µs) | Peak RSS (MB) | Commit | Notes |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 7.5 | 2026-04-23 | small  | http | 990  | 7843 | 9815 | 11975 | 5331 | 30 | 7d8f6aa | 8-way parallel; fsync-bottlenecked (macOS F_FULLSYNC ~7.4ms hw-class limit per Phase 6 baseline) |
| 7.5 | 2026-04-23 | medium | http | 1031 | 7331 | 9239 | 11263 | 5739 | 48 | 7d8f6aa | 5 features per push; same fsync-bottleneck — confirms aggregation cost is sub-fsync per event |
| 7.5 | 2026-04-23 | large  | http | 1007 | 7807 | 9495 | 11335 | 7067 | 74 | 7d8f6aa | 15 features; RSS scales linearly with feature count (~3MB / feature) |
| 7.5 | 2026-04-23 | small  | tcp  | n/a (deferred) | — | — | — | — | — | 7d8f6aa | TCP OP_PUSH not implemented yet; reserved for Phase 8+. The harness records 0 successful pushes / 100% errors when run; row intentionally omitted from regression ledger until TCP push lands. |
| 7.5 | 2026-04-23 | medium | tcp  | n/a (deferred) | — | — | — | — | — | 7d8f6aa | Same — TCP OP_PUSH reserved. |
| 7.5 | 2026-04-23 | large  | tcp  | n/a (deferred) | — | — | — | — | — | 7d8f6aa | Same — TCP OP_PUSH reserved. |

### Notes on Phase 7.5 first-baseline shape

- **Why ~1k EPS not 3M:** Phase 13's 3M EPS / core target requires the WAL
  to coalesce many events per fsync. Today the harness drives 8-way parallel
  serial `await sink.append_event(...)` calls — each push waits for its own
  fsync to land before the response. Group commit IS active (default 2ms
  coalesce) but with only 8 in-flight pushes the coalesce window rarely sees
  a meaningful batch. Phase 13 will revisit batching strategy and/or
  pipelined push semantics; until then expect ~1k EPS / fsync-bound on
  macOS. On Linux with `fdatasync` the same harness is expected to produce
  10–100× higher numbers — that baseline lands when CI runs Linux.

- **Why all three sizes ≈ 1k EPS:** the per-push CPU work for 1 vs 5 vs 15
  Phase 5 aggregation operators is sub-fsync — 15 ns/op × 15 ops = 225 ns,
  while fsync is 7.4 ms. The bottleneck is identical across pipeline sizes
  on this hw-class. The ledger still tracks all three so subsequent
  operator phases (decay sketches, geo, joins) can show their per-event
  cost rising above the fsync floor.

- **TCP rows deferred:** Phase 2.5 reserved OP_PUSH (`0x0010`) on the wire
  but did not implement the handler — `tcp::dispatch` returns
  `OP_ERROR_RESPONSE { code: "op_not_implemented" }`. Phase 8+ owns
  closing this gap; the harness is ready (CLI `--transport tcp` already
  speaks the wire) but the server side returns an error frame today, so
  there is no honest TCP throughput number to record yet.


> Regression thresholds: +10% = WARNING (flag in VERIFICATION.md); +25% = BLOCKER. Compare within same hw-class only.

---
## Per-phase rows merged from parallel worktrees (2026-04-24)

### Phase 6.1 — async-durability default `/push` (sync_mode=Periodic)

| Phase | Date | Pipeline | Transport | Sustained EPS | Push P50 (µs) | Push P95 (µs) | Push P99 (µs) | Get P99 (µs) | Peak RSS (MB) | Commit | Notes |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 6.1 | 2026-04-24 | small  | http | 15672 | 3489 | 5751  | 11399 | 5323  | 49  | adbc3d1 | sync_mode=periodic (default); --parallel 64; 15.8× lift over Phase 7.5 (990 EPS) |
| 6.1 | 2026-04-24 | medium | http | 17453 | 3313 | 4731  | 5899  | 2655  | 60  | adbc3d1 | 5 features per push; 16.9× lift over Phase 7.5 (1031 EPS) |
| 6.1 | 2026-04-24 | large  | http | 12004 | 3933 | 10727 | 30719 | 18975 | 100 | adbc3d1 | 15 features; 11.9× lift over Phase 7.5 (1007 EPS); P99 inflated by RSS GC pressure on macOS |

### Phase 8 — point/recency/streak ops + TCP OP_PUSH

| Phase | Date | Pipeline | Transport | Sustained EPS | Push P50 (µs) | Push P95 (µs) | Push P99 (µs) | Get P99 (µs) | Peak RSS (MB) | Commit | Notes |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 8 | 2026-04-24 | small  | http | 517 | 6943 | 10223 | 14247 | 3541 | 10 | 48e09fd | parallel-batch CPU contention; quiescent ~1000 EPS expected |
| 8 | 2026-04-24 | medium | http | 350 | 8871 | 18991 | 27951 | 11335 | 10 | 48e09fd | 5 features |
| 8 | 2026-04-24 | large  | http | 384 | 8423 | 14407 | 21119 | 4967  | 12 | 48e09fd | 15 features |
| 8 | 2026-04-24 | phase8 | http | 514 | 6995 | 9615 | 12447 | 6915 | 15 | 48e09fd | NEW 10-feature shape (Phase 5 core + Phase 8 point/recency); Phase 9+ comparator |
| 8 | 2026-04-24 | small  | tcp  | 290 | 11671 | 25023 | 33887 | 10335 | 9 | 48e09fd | **NEW:** First TCP push baseline (OP_PUSH wired in Phase 8) |
| 8 | 2026-04-24 | phase8 | tcp  | 335 | 9599 | 18191 | 28431 | 3809  | 11 | 48e09fd | NEW 10-feature shape over TCP |

### Phase 9 — decay + velocity operators

| Phase | Date | Pipeline | Transport | Sustained EPS | Push P50 (µs) | Push P95 (µs) | Push P99 (µs) | Get P99 (µs) | Peak RSS (MB) | Commit | Notes |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 9 | 2026-04-23 | medium_phase9 | http | 900 | 8011 | 13871 | 19071 | 6547 | 26 | 26cc375 | NEW pipeline; 5 features (count/sum + ewma/decayed_sum/rate_of_change); fsync-bound |
| 9 | 2026-04-23 | large_phase9  | http | 831 | 8431 | 16183 | 24303 | 20031 | 47 | 26cc375 | NEW pipeline; 15 features (5 core + 5 decay + 5 velocity); fsync-bound |

### Phase 10 — sketch operators

| Phase | Date | Pipeline | Transport | Sustained EPS | Push P50 (µs) | Push P95 (µs) | Push P99 (µs) | Get P99 (µs) | Peak RSS (MB) | Commit | Notes |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 10 | 2026-04-24 | medium-with-sketches | http | 982 | 7631 | 8943 | 10135 | 3217 | 95  | 13c60b9 | medium + count_distinct + percentile (5→7 features); fsync-bound on macOS |
| 10 | 2026-04-24 | large-with-sketches  | http | 976 | 7619 | 9071 | 10375 | 2089 | 182 | 13c60b9 | large + 5 sketches (15→20 features); fsync-bound on macOS |

### Phase 11 — buffer + geo operators

| Phase | Date | Pipeline | Transport | Sustained EPS | Push P50 (µs) | Push P95 (µs) | Push P99 (µs) | Get P99 (µs) | Peak RSS (MB) | Commit | Notes |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 11 | 2026-04-24 | geo    | http | 701  | 9519 | 19999 | 33215 | 32095 | 61 | 6235ba2 | NEW geo shape (geo_velocity + unique_cells + most_recent_n); first geo baseline |
| 11 | 2026-04-24 | small  | http | 1097 | 6955 | 9687  | 13535 | 4595  | 32 | 6235ba2 | small/HTTP regression check vs Phase 7.5 (990 EPS) → +10.8% improvement |

### Phase 11.5 — temporal tables + retraction primitive

| Phase | Date | Pipeline | Transport | Op | EPS | Push P50 (µs) | Push P99 (µs) | Notes |
|---|---|---|---|---|---:|---:|---:|---|
| 11.5 | 2026-04-23 | temporal-fraud | http | upsert  | 840 | 8040  | 18960 | first table-write baseline; fsync-bound on macOS |
| 11.5 | 2026-04-23 | temporal-fraud | http | read    | 299 | 160   | 3500  | first temporal-read baseline; pure MVCC lookup |
| 11.5 | 2026-04-23 | temporal-fraud | http | retract | 59  | 8050  | 17950 | first retract baseline; same fsync ceiling as upsert |

### POST-MERGE quiescent rerun (2026-04-24, all 6 branches in tree)

**This is the first apples-to-apples comparison ALL on `1e995b9` (post-merge HEAD), no sibling-agent CPU contention.** Surfaces a **regression vs Phase 6.1's pre-merge numbers**. Documented as a Phase 13 ship-gate investigation item.

| Phase | Date | Pipeline | Transport | Parallel | Sustained EPS | Push P50 (µs) | Push P95 (µs) | Push P99 (µs) | Get P99 (µs) | Peak RSS (MB) | Commit | Notes |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|
| post-merge | 2026-04-24 | small  | tcp  |  8 | 1944 | 3885 | 6847  | 12751 | 20223 | 56  | 1e995b9 | ⚠ regression: Phase 6.1 reported 15672 EPS small/http (10× higher); investigation needed |
| post-merge | 2026-04-24 | small  | tcp  |  1 | 286  | 3051 | —     | 4131  | 2319  | 13  | 1e995b9 | single-conn ceiling; per-push cost is 3ms even with no contention → fsync IS in critical path |
| post-merge | 2026-04-24 | small  | http |  8 | 1659 | 3953 | 10095 | 17023 | 21503 | 42  | 1e995b9 | matches TCP — transport not the bottleneck |
| post-merge | 2026-04-24 | medium | http |  8 | 2236 | 3099 | 4147  | 5623  | 1560  | 148 | 1e995b9 | same fsync floor across sizes |
| post-merge | 2026-04-24 | large  | http |  8 | 2215 | 3083 | 4187  | 6467  | 3499  | 350 | 1e995b9 | RSS scales linearly with feature count (~3 MB/feature) — pipelines are wired right |
| post-merge | 2026-04-24 | medium | tcp  |  8 | 2193 | 3097 | 4171  | 6771  | 7543  | 131 | 1e995b9 | matches HTTP at the same size — transport-uniform regression |
| post-merge | 2026-04-24 | large  | tcp  |  8 | 2165 | 3121 | 4839  | 7643  | 4427  | 269 | 1e995b9 | matches HTTP at the same size — transport-uniform regression |

#### Investigation summary

- **All 6 cells land in the 1659–2236 EPS band, P50 ≈ 3.1ms.** Transport-uniform (HTTP and TCP within 5% of each other), size-uniform (small/medium/large within 35% of each other).
- **Bottleneck is fsync on the critical path**, not compute. P50 = 3ms ≈ half of macOS `F_FULLSYNC` (~7.4ms). With `fsync_interval_ms=2`, group commit amortizes ~half the fsync cost — this looks like Phase 6 `PerEvent` mode behavior, NOT Phase 6.1's `Periodic` async-durability path.
- **`/push` route source code IS using `SyncMode::Periodic`** (`crates/beava-server/src/push.rs:375`). `WalSink::append_event_with_mode` correctly returns immediately after staging in Periodic mode (`crates/beava-persistence/src/fsync_worker.rs:401-405`). So the regression is NOT in the routing or the WalSink mode dispatch.
- **Most likely culprit:** the tokio `current_thread` runtime is being blocked by inline fsync (Phase 6.1's documented deviation: "Inline fsync instead of `spawn_blocking` in `fsync_worker.rs::flush_batch`. Rationale: tokio current_thread runtime (used in tests) has no blocking pool; inline works in both runtime flavors.") When fsync runs inline on the same thread as the push handler's ACK-completion future, ACKs are serialized behind fsync — defeating the Periodic-mode optimization in practice. Phase 6.1's reported 15.7k EPS may have been measured on a build where this didn't manifest, OR with `--parallel 64` (vs our `--parallel 8`) saturating the staging queue ahead of fsync ticks.
- **Fix candidates** for Phase 13 perf-gate work:
  1. `tokio::task::spawn_blocking` for the fsync call (per Phase 6.1's own deferred note)
  2. Multi-thread runtime for the server (current_thread is the project's "single-thread mental model" but doesn't have to apply to fsync IO specifically)
  3. Pre-issue an async fdatasync syscall via `tokio::fs::File::sync_data` instead of std blocking call
- **Compute is NOT the bottleneck.** Memory scales correctly (350 MB for 15-feature pipeline). When fsync is fixed, the apply path's per-event cost should drop the per-push budget to the µs range and unlock 50–200k EPS as Phase 6.1 originally projected.

#### Reproducer

```bash
cd crates/beava-bench
../../target/release/beava-bench --pipeline small --transport http --duration-secs 20 --parallel 8 --no-ledger
# Expected (current): ~1660 EPS, P50 ~4ms
# Expected (when fix lands): ~15-50k EPS, P50 ~50-200µs
```

### Phase 18-04 — I/O threads write phase (informational, Apple-M4)

End-to-end throughput sweep deferred to Plan 18-04.5 (Linux bench infrastructure
setup). The `beava-bench --features hand-rolled-runtime` path requires the full
EventLoop::tick() dispatch wiring (Plan 18-04's `run_write_phase` is exposed as a
testable entrypoint; full EventLoop integration lands in 18-05 when the tokio
dual-path is removed per Plan 18-07 schedule). M4 informational targets from the
plan are recorded here for reference:

| Phase | Date | Pipeline | Transport | io_threads | Target EPS/core | Notes |
|---|---|---|---|---:|---:|---|
| 18-04 | 2026-04-25 | small | tcp | 0 | 300-500k | Stage 18.2 floor (inline path) — informational target |
| 18-04 | 2026-04-25 | small | tcp | 2 | 1-1.5M   | Stage 18.3 number — informational target |
| 18-04 | 2026-04-25 | small | tcp | 4 | 2-2.5M   | Stage 18.4 target — informational |
| 18-04 | 2026-04-25 | small | http | 4 | 250-500k | HTTP still JSON-bound; write-phase serialization off-apply helps |

> Actual measured numbers to be recorded in Plan 18-04.5 once Linux bench infra is wired.
> Apple-M4 is INFORMATIONAL (D-16); Linux Xeon hard gate activates at Phase 18-05.
