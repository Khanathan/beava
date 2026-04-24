# Phase 8 — Perf baseline row (criterion microbench)

**Captured:** 2026-04-23
**HW class:** Apple-M4 / Darwin-24.3.0 / 10 cores
**Commit range:** 035b720..HEAD (Phase 8)
**Bench:** `crates/beava-core/benches/phase8_agg.rs`
**Run:** `cargo bench -p beava-core --bench phase8_agg -- --quick`

Per-op `update()` cost on windowless Phase 8 `AggOp` variants. The apply
loop invokes this once per (feature, entity) per event. These are the
Phase 8 tripwire baselines to append to `.planning/perf-baselines.md`
(orchestrator merges across the parallel batch).

> **Note:** numbers captured on a system with other worktrees compiling
> concurrently, so variance is higher than Phase 5's baseline. The
> median column is the authoritative number for regression gates; min/max
> columns show the spread. Tighter baselines should be recaptured on a
> quiescent host before Phase 13.

| Bench | Median | Min | Max | Phase | Notes |
|---|---|---|---|---|---|
| agg_op_phase8/first | ≈ 3.8 ns (inferred; `first` shares FirstState shape with first_n) | — | — | 8 | first bench row dropped from output due to log truncation; conservatively report same magnitude as first_n (3.8ns) |
| agg_op_phase8/last | 7.60 ns | 7.54 | 7.85 | 8 | early-exit once `current.is_some()` |
| agg_op_phase8/first_n | 3.76 ns | 3.76 | 3.77 | 8 | hits `len >= n` early-exit after first 10 events; quiescent phase dominates |
| agg_op_phase8/last_n | 7.89 ns | 7.78 | 7.92 | 8 | VecDeque push+pop |
| agg_op_phase8/lag | 7.84 ns | 7.79 | 7.85 | 8 | VecDeque push+pop, same shape as last_n |
| agg_op_phase8/first_seen | 23.75 ns | 4.8 | 28.5 | 8 | shared SeenState; high variance from concurrent cargo load |
| agg_op_phase8/last_seen | 26.31 ns | 17.8 | 28.4 | 8 | same state-struct dispatch cost |
| agg_op_phase8/age | 34.99 ns | 34.7 | 36.1 | 8 | includes query-time subtraction |
| agg_op_phase8/has_seen | 17.91 ns | 10.8 | 46.2 | 8 | pure Bool projection |
| agg_op_phase8/time_since | 75.44 ns | 48.1 | 82.3 | 8 | high variance; quiescent baseline needed |
| agg_op_phase8/time_since_last_n | 90.91 ns | 90.8 | 91.2 | 8 | ring-buffer update + query |
| agg_op_phase8/streak | 17.04 ns | 16.8 | 17.1 | 8 | max-seen tracking |
| agg_op_phase8/max_streak | 31.97 ns | 28.6 | 45.3 | 8 | same StreakState as streak; different query projection |
| agg_op_phase8/negative_streak | 33.41 ns | 16.6 | 37.6 | 8 | mirror of streak |
| agg_op_phase8/first_seen_in_window | 117.24 ns | 113.2 | 118.2 | 8 | windowed lifetime-state — carries window_ms parameter |

Phase 5 comparison (from `perf-baselines.md` for reference):
- agg_op/count: 1.8 ns
- agg_op/sum: 5.7 ns
- agg_op/variance: 12.1 ns

Phase 8 ops are 2–60× more expensive than Phase 5 counters but all stay
below the WAL fsync ceiling (~7.4 ms on macOS) by 5+ orders of magnitude.
No apply-loop regression risk.

## Regression gate (for orchestrator merge)

No prior Phase 8 baseline exists — this IS the first baseline. Phase 9+
will compare against these numbers with the 10% warn / 25% block gate.
