# Phase 18 Plan 01 — samply profiling procedure

## Goal

Verify that the hand-rolled mio event loop reduces reactor + scheduler CPU cost
from ~43% (tokio Phase 13.3 baseline) to < 15%, confirming the core Phase 18
thesis: removing tokio's per-event task-spawn overhead is the dominant
performance win.

## Phase 13.3 tokio baseline (M4 MacBook Pro, 2024-04)

| Metric | Value |
|--------|-------|
| Reactor + scheduler frames | ~43% of CPU samples |
| Per-event task switches | ~4 (poll → wake → schedule → run) |
| Small pipeline TCP EPS | ~2.1M / core |
| Small pipeline HTTP EPS | ~1.4M / core |

Source: `.planning/throughput-baselines.md` Phase 13.3 row.

## Phase 18 targets (after Plan 18-01)

| Metric | Target | Note |
|--------|--------|------|
| Reactor + scheduler frames | < 15% CPU | Gate 1.2 in 18-01-PLAN.md |
| Per-event task switches | ≤ 1 | No async wakeup per event |
| TCP EPS | 15–50k / core | Setup phase; real uplift in 18-02 |
| HTTP EPS | 8–25k / core | Setup phase; real uplift in 18-02 |

Plan 18-01 is a scaffold plan — the mio loop accepts connections but the full
inline WAL + zero-copy dispatch land in Plans 18-02 through 18-06. These EPS
numbers are sanity bounds, not the Phase 18 final target.

## Prerequisites

```bash
# Install samply (https://github.com/mstange/samply)
cargo install samply

# macOS: enable perf counters (one-time, requires reboot)
sudo sysctl -w kern.perf_event_paranoid=-1
```

## Step 1: Build with debug symbols + release optimizations

```bash
cargo build --release --features hand-rolled-runtime -p beava-bench
```

## Step 2: Run the bench under samply

```bash
# TCP transport — 30-second run
samply record \
  cargo bench -p beava-bench \
  --features hand-rolled-runtime \
  -- --pipeline small --transport tcp --duration-secs 30

# HTTP transport — 30-second run
samply record \
  cargo bench -p beava-bench \
  --features hand-rolled-runtime \
  -- --pipeline small --transport http --duration-secs 30
```

samply opens a Firefox Profiler tab automatically after the run.

## Step 3: Read the flame graph

1. In Firefox Profiler, select the **beava** thread (the event-loop thread,
   not the admin/tokio threads).
2. Switch to the **Call Tree** view, sort by **Self** (descending).
3. Look for these frame categories:

   | Frame | What it measures |
   |-------|-----------------|
   | `mio::Poll::poll` | Reactor wait — time in `epoll_wait` / `kqueue` |
   | `tokio::runtime::*` | Residual tokio scheduler frames (should be near zero) |
   | `crossbeam_channel::*` | Channel overhead (inter-thread handoff) |

4. Sum the percentages for the reactor + scheduler category.

## Step 4: Gate check

- **< 15%** — Gate 1.2 passes. Record the value in the table below.
- **15–25%** — WARNING. Investigate before Plan 18-02. Common causes:
  - mio `Poll::poll` called too frequently (check `tick` timeout)
  - crossbeam channel contention (check SPSC queue configuration)
- **> 25%** — BLOCKER. Plan 18-02 cannot start until resolved.

## Comparison against tokio baseline

| Phase | Transport | Reactor+Sched % | EPS/core | Notes |
|-------|-----------|-----------------|----------|-------|
| 13.3 (tokio) | TCP | ~43% | 2.1M | baseline |
| 13.3 (tokio) | HTTP | ~43% | 1.4M | baseline |
| 18-01 (mio) | TCP | _TBD_ | _TBD_ | update after run |
| 18-01 (mio) | HTTP | _TBD_ | _TBD_ | update after run |

Fill in the TBD cells and commit after the first manual profiling run.

## Recording baselines

After measuring, append a row to `.planning/throughput-baselines.md`:

```
| 18-01 | small | tcp  | M4 | <date> | <eps> | <p99_ms> | hand-rolled scaffold |
| 18-01 | small | http | M4 | <date> | <eps> | <p99_ms> | hand-rolled scaffold |
```

And update `.planning/perf-baselines.md` with the mio reactor % numbers.

## Notes on Plan 18-01 scaffold limitations

- The mio listeners are bound but the full event-loop dispatch (inline WAL,
  zero-copy response) is NOT wired yet — Plans 18-02 through 18-04 complete it.
- The EPS numbers from Plan 18-01 reflect setup overhead, not the final
  hand-rolled throughput.
- Gate 1.2 (reactor < 15%) is still meaningful: even without full dispatch,
  the mio poll loop's CPU share should be well below tokio's, confirming the
  architectural direction before more work lands.
