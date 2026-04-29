---
phase: 12-server-side-async-push-coalescing
plan: 12-08
type: scope-not-yet-planned
captured: 2026-04-29
status: ready-for-planning
depends_on: 12-07
---

# Plan 12-08 (proposed) — Apply-thread overhead reduction (busy-poll + drain + response batch + buffer pool)

## Diagnosis (perf-driven)

Per `samply` + `perf` traces of the Plan-12-07 production binary on Apple-M4 and Hetzner Linux EPYC KVM, the apply thread spends only **~10% of its budget on real per-request work** during read-heavy load (196k r/s) and **~47% on real aggregation work** during saturated push (51k EPS Linux, 92k EPS macOS). The remaining 50-90% is per-request orchestration overhead split across:

| Source | % of apply CPU (Linux read 196k r/s) | What it is |
|---|---:|---|
| `run_mio_event_loop` outer body | ~10% | Per-iteration mio dispatch + channel poll |
| `crossbeam_channel::Sender::send` | ~3% | Sending response back to IO worker |
| Kernel `try_to_wake_up` | ~5% | Waking IO worker thread per response |
| `epoll_wait` + eventfd I/O | ~3% | Apply blocking on its own mio Poll |
| `libc::malloc` + `cfree` | ~4% | Per-request response BytesMut alloc |
| Syscall entry/exit | ~4% | Context-switch overhead |
| Spin locks (kernel) | ~3% | NUMA / cache-line bouncing |
| Real work (`dispatch_get_batch` + serde) | ~9% | Actual /get logic |

**Apply is also idle ~42% of wall-time during push** — blocked in `epoll_wait` waiting for IO worker to enqueue events. Per-event the gap is ~6 µs of idle between events.

## Goal

Cut apply-thread overhead from 53% (push) / 91% (read) to **under 25%** by:

1. Eliminating per-event kernel wake-up coordination
2. Eliminating apply's idle gaps (apply becomes "always-on" at single-core 100% under load)
3. Amortizing IO-worker wake-up cost across batches of responses
4. Eliminating per-response `Bytes` allocator churn

## Architectural constraints (must NOT change)

- `project_no_sharded_apply` — single apply thread, NEVER share state across cores
- `project_phase18_no_dual_runtime` — mio data plane only
- `project_no_same_key_batching` — sketch reads MUST iterate per-cell; no coalescing
- Push and read are both serialized through the apply thread (Redis-shaped)
- Plan 12-07's main.rs migration to ServerV18 stays

## Locked decisions

**D-A: Adaptive busy-poll for apply (matches Aeron / LMAX / DPDK pattern; Redis is event-loop-blocking but has `io-threads` for parallel I/O which Beava already does via IoPool).**

Apply replaces its blocking `epoll_wait` on its own mio Poll with a `try_recv()` busy-poll on `work_ring` channel. Adaptive idle behavior:

```rust
// Pseudo-code
let mut idle_spins = 0;
loop {
    while let Ok(item) = work_ring.try_recv() {
        dispatch_one(item);
        idle_spins = 0;
    }
    idle_spins += 1;
    if idle_spins > 10_000 {
        // True idle — fall back to short blocking recv to free the core
        match work_ring.recv_timeout(Duration::from_micros(50)) {
            Ok(item) => { dispatch_one(item); idle_spins = 0; }
            Err(RecvTimeoutError::Timeout) => continue,  // keep waiting
            Err(_) => break,  // shutdown
        }
    }
}
```

K=10,000 spin iterations ≈ ~50-100 µs of CPU under typical clock, then fall back to a 50µs blocking recv. CPU profile: ~100% under load, drops to <5% when truly idle (>50 µs no events).

Rationale: under load, apply never blocks → no kernel wake-ups → no `try_to_wake_up` cost. Under no-load, apply doesn't burn 100% CPU forever.

**D-B: Response batching from apply → IO worker, hybrid threshold.**

Apply collects responses into a per-IO-worker batch buffer. Flush when EITHER:
- 16 responses queued, OR
- 100 µs elapsed since first response in batch

The 100µs timer is checked on each spin iteration via `Instant::now()` — no separate timer thread (apply is busy-polling, so it already cycles fast enough to see the deadline). Adds a `batch_started_at: Option<Instant>` per-write-ring channel.

At low load (1 r/s): timer dominates → flush per request → no batching artifact, latency unchanged.
At high load (196k r/s): batches of ~16 → 1 wake-up per 16 responses → drops `try_to_wake_up` cost ~16×.

Trade-off: tail-latency floor under sparse-burst load increases by ≤100 µs. Acceptable.

**D-C: Per-IO-worker response BytesMut pool (per-thread).**

Each IO worker maintains a thread-local `Vec<BytesMut>` pool of pre-sized buffers (e.g., 256 buffers × 4 KiB capacity = 1 MiB per worker). Response build path:

```rust
// Acquire from pool (or alloc if pool empty)
let mut buf = pool.pop().unwrap_or_else(|| BytesMut::with_capacity(4096));
buf.clear();
serde_json::to_writer(&mut buf, &response).unwrap();
let frozen = buf.freeze();
// Return Bytes; pool reclamation happens when Bytes Drop runs (refcount-based)
```

Reclamation via `Bytes::try_unsplit` or wrapping in a custom `RecyclableBytes` whose `Drop` returns the buffer to the pool. Implementation choice (planner decides between `bytes::BytesMut::with_capacity` recycling vs `crossbeam_queue::ArrayQueue`-backed pool with a custom Bytes wrapper).

Eviction: if pool grows past 512 buffers, drop excess on push back. Bounds memory.

**D-D: Drain-until-empty on apply → IO worker write_ring sends.**

Apply's per-iteration loop drains all available work_ring items before returning to the outer mio dispatch:

```rust
loop {
    // Drain ALL queued work (not just one)
    while let Ok(item) = work_ring.try_recv() {
        dispatch_one(item);
    }
    // Then check timers + idle fall-through
    flush_response_batch_if_due();
    ...
}
```

This is mostly a code-pattern change inside `run_mio_event_loop`. Already partially landed in Phase 18-13 but apply still re-enters the outer mio loop more often than necessary.

## Plan structure (waves)

- **Wave 1**: Adaptive busy-poll on apply (D-A). Replace `epoll_wait`-based blocking with `try_recv`+spin+timeout. Test idle CPU + saturated CPU.
- **Wave 2**: Drain-until-empty pattern (D-D). Tighten the inner loop. Test that no events are dropped.
- **Wave 3**: Response batching with timer (D-B). Add `batch_started_at` + flush logic. Test latency at sparse load (must stay < 100 µs additional) + throughput at saturated load (must drop wake-ups ≥10×).
- **Wave 4**: Response BytesMut pool (D-C). Implement pool + RecyclableBytes wrapper. Test no allocator churn under load.
- **Wave 5**: Apple-M4 + Hetzner regression-gate runs. perf record + samply confirm `try_to_wake_up` < 1% of apply, `run_mio_event_loop` < 5%. Append rows to `perf-baselines.md` + `throughput-baselines.md`.

Each wave is red-green-paired per CLAUDE.md TDD.

## Estimated impact

| Metric | Pre-12-08 (Hetzner) | Post-12-08 estimate |
|---|---:|---:|
| Push EPS (saturated, fraud-team) | 51,131 | ~95,000 (1.85×) |
| Read req/sec (single-cell, 32 workers) | 196,548 | ~360,000 (1.83×) |
| Apply idle % (under load) | 42% | <5% |
| Apply real-work % | 47% (push) / 9% (read) | ~75% / ~40% |

| Metric | Pre-12-08 (Apple-M4) | Post-12-08 estimate |
|---|---:|---:|
| Push EPS | 92,213 | ~170,000 |
| Read req/sec | 175,843 | ~320,000 |

## Files to read

- `/Users/petrpan26/work/tally/CLAUDE.md` (TDD discipline + perf gates)
- `/Users/petrpan26/work/tally/.planning/STATE.md`
- `/Users/petrpan26/work/tally/.planning/perf-baselines.md` § Phase 12-07 (compare against)
- `/Users/petrpan26/work/tally/.planning/throughput-baselines.md` § Phase 12-07
- `/Users/petrpan26/work/tally/crates/beava-server/src/server.rs:895+` (`run_mio_event_loop`)
- `/Users/petrpan26/work/tally/crates/beava-server/src/apply_shard.rs:88+` (`dispatch_one`)
- `/Users/petrpan26/work/tally/crates/beava-runtime-core/src/work_ring.rs` (RingItem channel)
- Phase 18 wrap notes in STATE.md (existing IoPool architecture)

## Out of scope

- Push-and-get atomic endpoint — Plan 12-10
- TCP read schema (msgpack body+response) — Plan 12-09
- Cross-CPU sharded apply — REJECTED per `project_no_sharded_apply`
- io_uring backend — Phase 18.5+ deferred

## Risks

1. **Adaptive busy-poll timer drift on virtualized environments** — KVM scheduler can stretch spin iterations unpredictably. Mitigation: use `Instant::now()` for the 100µs timer (wall-clock based), not iteration count. Test on Hetzner KVM specifically.
2. **Response batch holding state_tables lock too long?** — No: response batching is on the WRITE side (apply → IO worker), not the lock side. Lock is released between dispatches as today.
3. **Pool fragmentation with mixed response sizes** — small risk; bound by max pool size (512 × 4 KiB = 2 MiB max per worker). Sized by experimental tuning.
4. **Test stability under busy-poll** — tests that boot a ServerV18 and let it idle now have apply at 100% briefly until the K=10,000 spin runs out. Tests should sleep ≥200 µs after `boot_v18()` for apply to settle into adaptive-blocking. Audit phase18_04_* and phase12_07_* tests.

## Status

- **NOT YET PLANNED** — needs `/gsd-plan-phase 12` (or scoped planner) to break into red-green tasks
- **Blocking:** the Plan 12-09 push-and-get latency target (P50 < 300 µs HTTP) WITHOUT 12-08 lands at ~600 µs because of the 5 µs/event overhead Plan 12-08 removes.
