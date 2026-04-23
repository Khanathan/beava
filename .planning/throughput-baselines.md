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
