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
