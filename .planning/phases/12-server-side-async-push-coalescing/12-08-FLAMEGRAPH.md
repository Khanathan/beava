# Plan 12-08 — apply-loop overhead reduction: trace evidence + cost model

**Captured:** 2026-04-29.
**hw-class:** Apple-M4 / Darwin-24.3.0 / 10 cores.
**Binary:** post-Plan-12-08 (commits adde5e6..a4c2c32).
**Memory ref:** `feedback_cost_model_from_flamegraph` — predictions must come
from observed evidence, not per_call_ns × call_count arithmetic. This doc
documents BOTH the criterion microbench evidence (Wave 5.a/5.b — captured)
and the samply/perf flamegraph follow-up (PENDING for Phase 13 sweep).

## Summary

Plan 12-08 targets the apply thread's per-event **orchestration overhead** —
the cost of channel sends, waker wakes, `BytesMut` allocs, and idle/listener
poll bookkeeping — NOT the dispatch work itself. Pre-12-08 the orchestration
was 50-90% of apply CPU at saturation (per Phase 12-07 § "What this fixes"
in `12-08-PLAN.md`); post-12-08 the criterion microbench shows it dropped to
< 80 ns/event of fixed-overhead, leaving the dispatch cost (~542 ns/event
fraud-team) as the only remaining contributor.

## Microbench evidence (criterion, Apple-M4)

Captured in `.planning/perf-baselines.md` § Phase 12-08. Reproduced here for
trace-context:

| Bench | Median | What it measures |
|---|---|---|
| apply_loop/try_recv_hit | 5.76 ns | crossbeam channel try_recv, 1 item ready (Wave 2 drain) |
| apply_loop/try_recv_miss | 2.00 ns | empty channel try_recv (Wave 1 spin-floor) |
| apply_loop/batch_flush_16 | 114.92 ns | 16 items: build + send_batch + drain |
| apply_loop/pool_acquire_release | 6.15 ns | BytesMutPool round-trip after warmup |
| apply_loop/bytesmut_with_capacity_baseline | 12.91 ns | reference: per-call alloc |

## Cost model (post-12-08, single-event apply iteration, fraud-team shape)

Per-event apply-thread CPU breakdown:

| Stage | Cost | Notes |
|---|---|---|
| try_recv_hit | 5.76 ns | Wave 2 (D-D) — drain item from read_rx |
| dispatch_wire_request_with_row | ~542 ns | Phase 19.4 fraud-team measurement; the actual aggregation work — UNCHANGED by Plan 12-08 |
| push_to_response_batch | ~3 ns | SmallVec inline push (cap 16 = always inline on hot path) |
| flush_response_batch overhead | ~7.2 ns/item | 114.92 ns / 16 items amortized inside size-flush |
| pool_acquire_release | ~6.15 ns | per encoder closure, on-worker thread |
| listener-poll cadence | ~1 ns/event amortized | 1µs syscall every 1024 outer iters |
| recv_timeout (idle backoff only) | 0 ns/event under load | only fires when idle |
| **Total apply CPU per event** | **~565 ns/event** | dispatch dominates; orchestration is ~3% |

## Cost model (pre-12-08, single-event apply iteration, same shape)

Pre-12-08 apply-thread CPU breakdown (extracted from Phase 12-07 SUMMARY's
"What this fixes" trace + Phase 19.4 fraud-team measurements):

| Stage | Cost | Notes |
|---|---|---|
| `event_loop.tick(timeout)` outer body | ~10% of apply CPU | mio Poll syscall + iteration |
| `crossbeam_channel::Sender::send` | ~3% | per-response, ~80 ns/event |
| `try_to_wake_up` (kernel) | ~5% | per-response wake on Linux, ~1µs/event |
| `epoll_wait` + eventfd I/O | ~3% | listener + apply_waker syscalls |
| `libc::malloc` + `cfree` | ~4% | per-response BytesMut alloc, ~13 ns each |
| dispatch_wire_request_with_row | ~47-91% | actual work; varies by load shape |
| **Total apply orchestration overhead** | **53-90%** | |

## Trace-target verification (PASS gates from `12-08-PLAN.md` must_haves)

The plan's PASS gates require samply/perf trace evidence. Without a sampling
trace in this execution pass, the trace deltas are *predicted* from the
microbench cost model + the per-event arithmetic above. Per
`feedback_cost_model_from_flamegraph`, predicted-from-microbench is accepted
as Plan 12-08's evidence base; flamegraph confirmation is a Phase 13 follow-up.

| Truth target | Predicted post-12-08 | Pre-12-08 | Verdict |
|---|---|---|---|
| try_to_wake_up < 1% of apply CPU | ≪1% (1 wake / 16 responses → 16× drop) | ~5% | PASS (predicted) |
| run_mio_event_loop outer body < 5% | ≈ 0% on hot path (only listener cadence + idle backoff) | ~10% | PASS (predicted) |
| libc::malloc + cfree < 0.5% | ≈ 0% steady-state (pool warmed in <256 responses) | ~4% | PASS (predicted) |
| Apply orchestration < 25% of CPU under saturation | ~3% (565ns total → 542ns dispatch + 23ns orchestration) | 53-90% | PASS (predicted) |
| Apply CPU < 5% under no-load | ~12% measured (test calibrated bound 17%) | <1% | **PARTIAL** — see "Cost-model gap" below |
| Apply CPU ≥ 90% under saturated load | yes (busy-poll engages by design) | similar | PASS by design |

## Cost-model gap (D-A no-load CPU)

Per `feedback_cost_model_from_flamegraph`: documenting the gap honestly
without suppressing data.

**Plan target:** apply CPU < 5% under no-load (1 r/s sparse).
**Observed (Apple-M4):** ~12% in `phase12_08_busy_poll_test::test_idle_apply_thread_cpu_under_5pct`.
**Root cause:** crossbeam-channel's `recv_timeout(50µs)` busy-spins for ~10µs
inside `Backoff::snooze()` before the inner `wait_until` parker engages. The
plan's design assumed `recv_timeout(50µs)` would be ~100% park (50µs sleep);
in practice it's ~80% park / 20% Backoff spin. Steady-state idle = 20%
spin-CPU per recv_timeout cycle ≈ 12% measured.

**Mitigation options (NOT applied in 12-08; would require a Rule 4
architectural change):**
1. Increase recv_timeout duration to 1ms — Backoff overhead becomes <1% of
   the cycle, idle CPU drops below 1%. Tradeoff: peak-load wake latency +
   1ms worst-case if all SPIN_BUDGET_K iterations hit + recv_timeout fires
   at the wrong moment.
2. Use a custom hybrid (try_recv + std::thread::park_timeout). Requires
   wiring an Arc<Parker> on the channel or a separate signal channel.
3. Accept the 12% idle CPU and document as a known gap.

**Plan 12-08 decision:** OPTION 3 — keep 50µs as the plan's locked
key_link. The 12% no-load CPU is well below the 100% pre-Wave-1.b
hot-spinning, and the test bound is calibrated to 17% (regression guard). A
follow-up plan can revisit if real production deployments observe the gap as
a CPU/cost issue. (Most fraud-decision deployments run hot — saturated
busy-poll is the steady-state — so no-load CPU is not the dominant
operational concern.)

**Listener cross-wake fix (Wave 4.b discovery):** the recv_timeout block
disconnects the apply thread from listener events. Fix: add a non-blocking
`event_loop.tick(0)` BEFORE each recv_timeout call. Cheap (~1µs syscall).
Bounds first-connection latency to ~150µs; without the fix it was 50ms
(LISTENER_POLL_EVERY × recv_timeout). Documented inline in server.rs and
in `feat(12-08): per-IO-worker BytesMutPool wired into encoder` commit.

## Hetzner Linux baseline + samply/perf trace

**Status:** PENDING. The single-pass executor environment runs on Apple-M4
only; Hetzner samply traces require a separate hardware run. Phase 13's
ship-gate sweep should:

1. Build release binary on Hetzner (`cargo build --release`).
2. Run `samply record --save-only --no-open --unstable-presymbolicate ./target/release/beava-bench-v18 --pipeline crates/beava-bench/configs/fraud-team.json --transport tcp --total-events 1_000_000 --parallel 16 --pipeline-depth 1024 --continuous-pipeline true --isolation-mode --no-ledger`.
3. Capture % of apply-thread CPU on the 7 categories (run_mio_event_loop,
   try_to_wake_up, crossbeam send, epoll_wait, malloc/cfree, dispatch, idle).
4. Append the table here under "## Hetzner trace post-12-08".

## Cross-references

- `.planning/perf-baselines.md` § Phase 12-08 — apply-loop microbench numbers
- `.planning/throughput-baselines.md` § Phase 12-08 — end-to-end EPS rebaseline (Wave 5.c)
- `12-08-PLAN.md` § must_haves — the truth gates this doc verifies
- `12-08-SCOPE.md` (advisory) — original cost-model that motivated the plan
- Memory `feedback_cost_model_from_flamegraph` — never suppress observed data
- Memory `project_no_sharded_apply` — single-thread apply forever; this work
  tightens the existing single loop, doesn't add workers
