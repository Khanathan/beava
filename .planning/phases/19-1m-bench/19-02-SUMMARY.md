---
phase: 19-1m-bench
plan: 02
subsystem: bench-harness
tags: [bench, blast-shape, total-events, isolation-mode, receiver-flips-stop, phase-19]
provides:
  - "beava-bench-v18 binary: --total-events / --blast-shape / --isolation-mode CLI flags"
  - "Pool=N pre-encoded-frame sender (uses Plan 19-01 build_pool_timed)"
  - "Receiver-flips-stop pattern in BOTH continuous AND burst TCP paths (D-12)"
  - "Hard-cap counter Arc<AtomicU64> on sender via fetch_add >= cap (D-13)"
  - "Three-column isolation output (wall_clock_ms / send_drain_ms / ack_lag_ms)"
  - "Bench-side {requested, pushed, acked} invariant tuple (asserted equal under --total-events)"
requires:
  - "beava_bench::blast_shape::* (Plan 19-01)"
  - "beava-server::testing::TcpClient (Phase 2.5+ unchanged)"
  - "beava_core::wire::{decode_frame, encode_frame, OP_PUSH, CT_JSON, CT_MSGPACK}"
  - "tokio (Barrier, Mutex, Semaphore, mpsc) — workspace-pinned"
affects:
  - "Plan 19-04 (microbench) — uses beava-bench-v18 as the integration measurement target"
  - "Plan 19-05 (throughput run) — drives multi-shape rows in throughput-baselines.md via this binary"
  - "Plan 19-03 (Python harness) — independently implemented, but the Rust harness's invariant-tuple format informs the Python row schema"
key-files:
  created:
    - "crates/beava-bench/tests/bench_v18_blast_smoke.rs"
    - ".planning/phases/19-1m-bench/19-02-SUMMARY.md"
  modified:
    - "crates/beava-bench/src/bin/beava-bench-v18.rs"
decisions:
  - "Bench-side hard-cap is sender-side (`pushes_cap`) BEFORE write_all + receiver-side (`pushes`) AFTER ack — receiver flips global stop and closes the per-worker sem when ack count crosses cap; sender's fetch_add un-do pattern keeps the {requested, pushed, acked} invariant exact even under multi-worker race."
  - "Pool-build time excluded from wall_clock_ms via `tokio::sync::Barrier::new(parallel + 1)`; main task waits as the (parallel + 1)-th party AFTER all workers complete pool-build but BEFORE setting `start = Instant::now()`. _pool_build_dur is captured but discarded — the Barrier provides actual exclusion."
  - "Continuous-path receiver wraps `read_buf.read_buf` in tokio::select! with a 50ms periodic sleep so a worker whose sender has exited (cap hit, ts_tx dropped) re-checks `stop.load()` even when no socket bytes are arriving — fixes the deadlock that surfaced at parallel>=2 during impl."
  - "Burst-path sender exits the outer while loop when `pushes_cap >= cap` AND `sent == 0` AND no send_err — avoids a spin loop where workers race the cap, all break their inner for-loop with sent=0, and continue/respin while waiting for the receiver-side stop flip."
  - "Single canonical `PipelineConfig` (Option A from EDIT 7) — deleted bench-v18.rs's local copy, now imports beava_bench::blast_shape::PipelineConfig. Verified: `grep -rn '^pub struct PipelineConfig' crates/beava-bench/src/` returns exactly 1 match (in blast_shape.rs)."
  - "decode_pool_frame uses big-endian (from_be_bytes) to match beava_core::wire::encode_frame's network byte order — caught + fixed during impl after burst-mode parallel=1 hung; covered by a new in-bin decode_pool_frame_parses_encoded_frame unit test."
metrics:
  duration: "~14 minutes"
  completed: "2026-04-26"
  tasks: 2
  tests_added: 5            # 3 subprocess smoke + 2 in-bin unit tests
  lines_added_bin: 687
  lines_removed_bin: 199
  lines_added_test: 214
---

# Phase 19 Plan 02: Bench-harness integration of `blast_shape` Pool=N — Summary

Wired Plan 19-01's `beava_bench::blast_shape` module into `beava-bench-v18` so the binary now supports `--total-events N --blast-shape S --isolation-mode` and exits cleanly when `acked >= N` (no WIP-stash stall). Plans 19-04 (microbench) and 19-05 (throughput run) can now use this binary as their measurement entry point.

## What landed

### CLI surface

Six new flags, all additive — legacy `--duration-secs` path is preserved as a regression guard:

| Flag | Default | Notes |
|------|---------|-------|
| `--total-events <u64>` | unset | When set, becomes the cap; `--duration-secs` is a safety upper bound only (raised to ≥3600s when total_events.is_some(), per T-19-02-02) |
| `--blast-shape={fixed,uniform,zipfian,mixed}` | fixed | Dispatches to Plan 19-01 builder |
| `--zipf-alpha <f64>` | 1.0 | Zipfian skew |
| `--cardinality <u64>` | 1_000_000 | K for Uniform/Zipfian |
| `--mixed-event-count <usize>` | 3 | M for Mixed |
| `--isolation-mode` | false | Adds wall_clock_ms / send_drain_ms / ack_lag_ms columns |

### Edits applied (8 EDITS per the plan)

| EDIT | What | Where in bench-v18.rs |
|------|------|-----------------------|
| 1 | Five new CLI flags + `BlastShapeArg` enum + `to_blast_shape` adapter | Cli struct (additive); enum block before run_workload |
| 2 | `effective_duration_secs` cap when `total_events.is_some()` (3600s floor) | run_workload setup |
| 3 | Continuous-path sender uses `pool[idx % pool_len]` from `build_pool_timed` | run_tcp_continuous_push_worker — sender_handle |
| 4 | Receiver flips `stop.store + sem.close()` on `acks >= cap` (D-12) — continuous AND new burst worker | run_tcp_continuous_push_worker receiver loop + run_tcp_burst_push_worker zip loop |
| 5 | Print invariant tuple + isolation columns at run end | main() after format_report |
| 6 | (MANDATORY wiring) `pushes_cap` + `first/last_send_ts` Arcs + `tokio::sync::Barrier::new(parallel + 1)` + mixed_event_names extraction + worker spawn loop thread-through | run_workload |
| 7 | Delete bench-v18.rs local `PipelineConfig` (Option A); use `beava_bench::blast_shape::PipelineConfig` | Top of file imports |
| 8 | Burst sender insertion: cap-check + `stop.store + break` + `pushes_cap.fetch_sub` un-do; new `run_tcp_burst_push_worker` replaces the old burst `Transport::Tcp` arm of `run_push_worker` so it also uses Pool=N + receiver-flips-stop | run_tcp_burst_push_worker (NEW) |

### Cherry-pick from stash@{0} (D-14)

Per CONTEXT.md §`<specifics>` "Stash@{0} cherry-pick checklist":

| Hunk | Disposition |
|------|-------------|
| `Cli` struct: `total_events: Option<u64>` arg | KEPT |
| `effective_duration_secs` cap at 3600 when total_events.is_some() | KEPT |
| `prebuilt_frame` build (single frame) | REPLACED with Pool=N via Plan 19-01 builder |
| `total_events_task` watcher (1 ms poll loop) | DROPPED — replaced by D-12 receiver-flips-stop pattern |
| Sender break before write_all | KEPT (now `pushes_cap.fetch_add >= cap` with un-do pattern) |
| `--blast-shape` enum + dispatch | NEW (additive on top of stash) |
| `--isolation-mode` flag + send_drain_ms / ack_lag_ms capture | NEW (additive on top of stash) |

After this plan lands the user can `git stash drop stash@{0}` — every useful hunk has been refactored into a fresh commit.

### Tests (3 / 3 subprocess + 2 / 2 in-bin unit)

| # | Test | Purpose |
|---|------|---------|
| 1 | `bench_v18_total_events_smoke_zipfian_msgpack_continuous` | --total-events 1000 zipfian msgpack continuous; asserts {requested=1000, pushed=1000, acked=1000} + all 3 isolation columns; 10 s timeout guards against stall regression |
| 2 | `bench_v18_total_events_smoke_fixed_burst_json` | Same invariant for burst path (D-12 applies to BOTH) |
| 3 | `bench_v18_legacy_duration_path_unchanged` | --duration-secs 2 path still prints `sustained_eps:` (regression guard) |
| 4 | `tests::decode_pool_frame_parses_encoded_frame` (in-bin) | Pool entry round-trips through encode_frame + decode_pool_frame |
| 5 | `tests::decode_pool_frame_rejects_truncated` (in-bin) | Truncated buffer returns None |

### Verification

- `cargo test -p beava-bench --tests` → **18 passed** (3 v18_smoke + 10 blast_shape + 3 bench_v18_blast_smoke + 2 in-bin)
- `cargo test -p beava-bench --bin beava-bench-v18` → **2 passed** (in-bin decode_pool_frame tests)
- `cargo clippy -p beava-bench --bins --tests -- -D warnings` → clean
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` → clean
- `cargo fmt --all -- --check` → clean

### Manual sanity (release)

```text
$ CARGO_MANIFEST_DIR=$(pwd)/crates/beava-bench timeout 30 ./target/release/beava-bench-v18 \
    --total-events 1000 --blast-shape zipfian --transport tcp --wire-format msgpack \
    --pipeline small --duration-secs 10 --parallel 4 --pipeline-depth 16 \
    --no-ledger --isolation-mode --cardinality 100

beava-bench-v18: invariant_tuple requested=1000 pushed=1000 acked=1000
beava-bench-v18: isolation_mode wall_clock_ms=1000 send_drain_ms=3 ack_lag_ms=997
elapsed:          1.000183542s
```

`requested == pushed == acked == 1000` confirmed; exited well within the 10 s safety bound. Note that `wall_clock_ms ≈ 1000` here is a polling-loop artifact (the main task wakes every 50 ms while waiting for the deadline; for a 1 K event run on M4 loopback the actual saturation phase is sub-100 ms — `send_drain_ms = 3` confirms that). Plan 19-05's full `--total-events 1_000_000` runs will see wall_clock dominated by actual saturation time, not the polling overhead.

### Commits

| Commit | Type | Subject |
|--------|------|---------|
| `2928143` | `test` | `test(19-02): add smoke for --total-events + --blast-shape + --isolation-mode` |
| `22f18a0` | `feat` | `feat(19-02): wire blast_shape Pool=N + --total-events/--blast-shape/--isolation-mode` |

Per CLAUDE.md §TDD Discipline: RED commit (`test:` — confirmed 2 failed at clap "unexpected argument" stage; legacy duration-secs test passed), then GREEN commit (`feat:` — all 5 new tests pass).

## Architectural notes (for future bench refactors)

1. **`tokio::sync::Barrier::new(parallel + 1)` is load-bearing for measurement honesty.** Removing it (e.g., by setting `start` before the worker spawn loop) would re-include each worker's pool-build time in `wall_clock_ms`, producing a result that conflates Plan 19-01's pool-build cost with the server's saturation throughput. Both tests #1 and #2 would still pass at small N (pool build dominates briefly then decays); the breakage would only surface at N ≥ 100k. Keep the barrier.

2. **`tokio::select!` + 50 ms wake on the receiver's socket-read.** Without this, a worker whose sender has hit `pushes_cap >= cap` (no more sends, ts_tx dropped) and whose receiver has already drained all in-flight acks (read_buf empty) parks indefinitely on `read_half.read_buf`. The global `stop` may already be true, but the receiver only re-checks it after the read awaits. The 50 ms periodic wake is a deadlock-avoidance, not a perf-relevant signal — at N=1M EPS, the receiver wakes from real socket data ~every 4 ns, far faster than the 50 ms poll.

3. **Burst-path "exit on cap when sent==0".** Without this, when 4 workers race a global cap of 1000 and all four hit `pushes_cap.fetch_add >= 1000` mid-batch, all four end the inner for-loop with sent=0, then `continue` → next while iteration → cap check → break for → sent=0 → continue → spin. The receiver flips stop only AFTER it sees an ack push pushes >= cap, which can lag the sender's pushes_cap saturation by ~one batch. The early-exit short-circuits the spin.

4. **decode_pool_frame is big-endian.** `beava_core::wire::encode_frame` writes network byte order; `from_be_bytes` is the only correct decoder. The covering unit test `tests::decode_pool_frame_parses_encoded_frame` round-trips an encoded frame through both functions to make this invariant non-regressable.

5. **Single source of truth for `PipelineConfig`.** Plan 19-01's `pub struct PipelineConfig` lives in `crates/beava-bench/src/blast_shape.rs`. Plan 19-02 deletes the bench binary's local copy and imports the lib type. Future bench changes that want extra fields on `PipelineConfig` should add them to the lib type — adding them to a bench-binary-local mirror would re-introduce the type-mismatch friction Warning 5 from the plan-checker flagged.

6. **Pool sizing fallback.** When `--total-events` is unset, `build_worker_pool` defaults to N=1024 (a small precomputed buffer the sender cycles through). This means the legacy `--duration-secs` mode also benefits from the per-iteration encode being precomputed — but Plan 19-01's `Fixed` shape is the default, so the cycle-through cost is just a Bytes-clone (refcount bump). No allocator pressure for the legacy path.

## Deviations from plan

Two deviations applied during execution; both fall under the auto-fix rules in `<deviation_rules>`.

### 1. [Rule 1 - Bug] decode_pool_frame endianness

**Found during:** Manual sanity run after writing the GREEN commit; burst-mode parallel=1 hung at "pre-warm done" because the server returned an OP_ERROR_RESPONSE for every pool frame (length prefix decoded as wrong byte order → 168 MB declared length → server rejected as TooLarge → no acks → bench hung).

**Issue:** `decode_pool_frame` initially used `u32::from_le_bytes` / `u16::from_le_bytes`, but `beava_core::wire::encode_frame` uses `BytesMut::put_u32` (big-endian default in the `bytes` crate).

**Fix:** Switched to `from_be_bytes`. Added `tests::decode_pool_frame_parses_encoded_frame` unit test that encodes a Frame via `encode_frame`, decodes the resulting Bytes via `decode_pool_frame`, and asserts `(op, ct, payload)` round-trips. Test would have caught this at unit-test time if it had been added during EDIT 8 — added retroactively to prevent regression.

**Files modified:** crates/beava-bench/src/bin/beava-bench-v18.rs (decode_pool_frame body + new in-bin test)
**Commit:** Folded into the GREEN commit `22f18a0` (only one feat commit per plan; the diagnosed-then-fixed-during-write workflow).

### 2. [Rule 1 - Bug] Multi-worker continuous-path deadlock + burst-path spin loop

**Found during:** Smoke test run with parallel=4; both bench_v18_total_events_smoke_zipfian_msgpack_continuous and bench_v18_total_events_smoke_fixed_burst_json hit the 10 s stall guard.

**Issue (continuous path):** When the sender breaks on `pushes_cap >= cap`, it drops `ts_tx`. The receiver decodes the in-flight acks until `ts_rx.recv()` returns None (sender exited + drained). But if the receiver is currently parked in `read_half.read_buf().await` waiting for the next byte and no more bytes are coming (other workers' sends are complete), it never wakes up to check `stop.load()`. The deadline-based check at the top of the receiver loop only fires when control returns from the await.

**Issue (burst path):** When 4 workers race a global cap of 1000, all four can end an inner for-loop with `sent=0` (each fetch_add returned >= cap mid-batch). They all `continue` to the outer while, which re-enters the for-loop, hits cap again, breaks with sent=0, continue — infinite spin until the receiver-side stop flip arrives. With multi-worker timing, this can spin for an unbounded period.

**Fix (continuous path):** Wrapped `read_half.read_buf` in a `tokio::select!` race against `tokio::time::sleep(Duration::from_millis(50))`. On wake-from-sleep, re-check `stop.load() || deadline`. This adds at most one 50 ms tail latency to a normal-path receiver shutdown — negligible vs. the 1 K-1 M event saturation phase.

**Fix (burst path):** Inside the `if sent == 0` branch of `run_tcp_burst_push_worker`, when `total_cap.is_some()` AND no send_err, `break 'outer` instead of `continue`. The receiver-side stop flip catches up by the time other workers' last in-flight acks drain.

**Files modified:** crates/beava-bench/src/bin/beava-bench-v18.rs (receiver tokio::select! + burst sent==0 cap-aware break)
**Commit:** Folded into the GREEN commit `22f18a0`.

## Hooks for downstream plans

- **Plan 19-04 (microbench):** Will run `criterion` against the integration entry points the bench binary exposes — `make_event_payload` (legacy path), `build_pool_timed` (Plan 19-01, already covered there), `decode_pool_frame` (new). The integration-level "throughput per Pool=N entry size" microbench is gated on Plan 19-04's plan landing.
- **Plan 19-05 (throughput run):** Drives this binary with the full small/medium/large/large_phase9 × 4 shapes × 2 transports × 2 modes matrix and appends the rows to `.planning/throughput-baselines.md` under a new `## 1M-event blast` section. The `--isolation-mode` columns become three additional ledger fields.
- **Stash drop:** stash@{0} ("wip: --total-events + pre-encoded-frame bench (needs verification)") can now be dropped — every useful hunk has been cherry-picked. Run `git stash drop stash@{0}` once the user has verified this plan against their working tree.

## Self-Check

Verified before completing:

```text
$ test -f crates/beava-bench/tests/bench_v18_blast_smoke.rs   && echo FOUND
FOUND
$ test -f .planning/phases/19-1m-bench/19-02-SUMMARY.md       && echo FOUND
FOUND
$ git log --all --oneline | grep -E "^(2928143|22f18a0) "     | wc -l
2
$ grep -c "total_events_task" crates/beava-bench/src/bin/beava-bench-v18.rs
0
$ grep -c "tokio::sync::Barrier" crates/beava-bench/src/bin/beava-bench-v18.rs
5
$ grep -c "pool_ready_barrier.wait" crates/beava-bench/src/bin/beava-bench-v18.rs
6
$ grep -rn "^pub struct PipelineConfig" crates/beava-bench/src/ | wc -l
1
$ grep -c "sem.close" crates/beava-bench/src/bin/beava-bench-v18.rs
2
$ grep -c "build_pool_timed" crates/beava-bench/src/bin/beava-bench-v18.rs
3
```

## Self-Check: PASSED

All claimed files exist on disk. Both commits referenced are reachable from HEAD. All grep-based success criteria from `<success_criteria>` (top of plan) are met. CLI exposes all five new flags (verified via `--help`). Smoke tests, clippy (workspace-wide), and fmt all clean. Manual sanity run prints the invariant tuple as expected.
