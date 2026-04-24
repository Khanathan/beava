# Phase 9: Decay + Velocity Operators — Context

**Gathered:** 2026-04-23
**Mode:** Auto (orchestrator-dispatched, parallel sibling to Phase 8/10/11/11.5)
**Branch:** worktree-agent-abc51d42 (rebased from `v2/greenfield` HEAD `157630f`)

## Phase Boundary

Add 16 new aggregation operators to the Phase 5 framework: 7 decay (`ewma`, `ewvar`, `ew_zscore`, `decayed_sum`, `decayed_count`, `twa` + half-life parsing) + 8 velocity (`rate_of_change`, `inter_arrival_stats`, `burst_count`, `delta_from_prev`, `trend`, `trend_residual`, `outlier_count`, `value_change_count`) + 1 z-score (`z_score`).

**Reused infra (untouched):** `AggOp` enum dispatch + `WindowedOp` 64-bucket fold + `AggOpDescriptor` + `agg_compile.rs` Rule 11 + `agg_apply.rs` apply loop + `RegistryInner.compiled_aggregations` cache + Phase 4 `Expr` for optional `where=`.

**Out of scope:** sketches (Phase 10), bounded-buffer (Phase 11), geo (Phase 11), joins (Phase 12), retraction.

## Implementation Decisions

### D-01 — Extend AggKind enum, NOT a parallel "DecayKind"

All 16 operators land as new variants of the existing `AggKind` (alphabetic-ish suffixes after `Ratio`). Each variant gets a per-op state struct in `agg_state.rs` (or a new `agg_state_decay.rs` / `agg_state_velocity.rs` if file size grows past ~600 LOC). `AggOp` enum gets one new variant per operator. `AggOp::new`, `update`, `update_with_row`, `query` match arms extended.

**Rationale:** Matches Phase 5 D-01 (zero-cost dispatch, no Box<dyn>). One enum variant + match arm per op is the locked pattern. Adding 16 variants takes the enum from 9 → 25 — within the 40-by-Phase-11 budget.

**Implication:** `AggKind` Copy + Eq + Serialize stay valid. Schema serde may grow but stays additive (no compatibility break — Phase 7 snapshot bodies survive because new variants are forward-only).

### D-02 — All decay/velocity ops carry `event_time_ms` + persist (value, last_event_time)

Per math reference in the prompt:
- EWMA: state = `(value, last_event_time)`; α = `1 - exp(-Δt / half_life_ms)` where `Δt = event_time - last_event_time` (clamp to 0 if event_time < last_event_time, late events keep prior state — no time travel).
- EWVAR: state = `(mean, m2, last_event_time)`; same α-update.
- EW_ZSCORE: query-time only — needs an EWVAR underneath; we make EW_ZSCORE wrap an internal `EwVarState` and emit `(current_value - mean) / sqrt(m2)`. Simpler than two separate features.
- DECAYED_SUM / DECAYED_COUNT: forward-decay (Cormode); state = `(decayed_total, last_event_time)`. On update: `decayed_total = decayed_total * exp(-Δt / half_life_ms) + delta`.
- TWA: time-weighted-average over a window; state = `(sum_v_dt, sum_dt, last_v, last_t)`; uses windowed bucketing (window kwarg required) but the *fold* across buckets is custom (not the Phase 5 generic Welford combine). Treat TWA as a windowed-only op that overrides bucket-fold logic.
- RATE_OF_CHANGE / DELTA_FROM_PREV / VALUE_CHANGE_COUNT / TREND / TREND_RESIDUAL: state contains `(prev_value, prev_t, ...)` and possibly an EW linear-regression accumulator (`(sum_x, sum_y, sum_xx, sum_xy, n)`).
- INTER_ARRIVAL_STATS: state = `(last_event_t, gap_count, gap_mean, gap_m2)` (Welford on inter-arrival gaps). Output = struct `{mean_ms, stddev_ms, cv}` — emit as a single `Value::Map(BTreeMap)` (or new `Value::Struct`); since we have no `Map` variant, emit as `Value::F64(mean_ms)` only and document `stddev_ms` / `cv` as Phase 13 follow-up. **DECISION: emit `mean_ms` only; struct return deferred to Phase 13** (avoids row.rs surgery this phase).
- BURST_COUNT: max events seen in any sub-window inside outer window. Implemented as windowed-only; `sub_window_ms` < `window_ms`; state = `WindowedOp` with extra inner-bucket aggregator emitting max-of-sub-window-counts.
- OUTLIER_COUNT: count of events where `|x - mean| > sigma * stddev`; state = `VarianceState` (reused) + counter that increments on each event when condition fires (using *current* mean+stddev *before* updating). Default `sigma = 3.0`.
- Z_SCORE (entity-level): query-time `(current_event_value - mean) / stddev` over `baseline_window`. State = `VarianceState` + `last_seen_value`. Emits Null if n<2.

### D-03 — `half_life` parameter unifies `bv.ewma`, `bv.ewvar`, `bv.ew_zscore`, `bv.decayed_sum`, `bv.decayed_count`

Wire JSON: `params.half_life: "5m"` (duration string, parsed via existing `parse_duration_to_ms` to `Option<u64>` ms).

**Validation at Rule 11 (agg_compile.rs):**
- For decay ops: `half_life` is **required**. Missing → `AggregationInvalidWindow` (or new `AggregationInvalidHalfLife`) error code.
- Half-life 0 or "forever" → invalid (decay needs positive finite half-life).
- Adds `half_life_ms: Option<u64>` to `AggOpDescriptor`.

**Decision:** introduce `AggregationInvalidHalfLife` ErrorCode variant; reuse `parse_duration_to_ms` parser (rejects "forever" by returning `Ok(None)` which we then map to error for half-life context).

### D-04 — Velocity ops: `window` required for windowed ops; `delta_from_prev` is windowless

`rate_of_change`, `inter_arrival_stats`, `burst_count`, `trend`, `trend_residual`, `outlier_count`, `value_change_count` all take `window=` (required). `delta_from_prev` is windowless (lifetime). `z_score` takes `baseline_window=` (required, becomes `window_ms`).

**Bucketing reuse:** all windowed velocity ops route through existing `WindowedOp` infra with new `AggKind` variants. The 64-bucket fold logic in `WindowedOp::query` extends per-op — for trend/trend_residual we add a `combine` method on the regression accumulator (Welford-style for slopes); for outlier_count/value_change_count the bucket-fold is plain summation (already supported by WindowedOp's count-style fold).

### D-05 — `bv.ema` SDK alias to `bv.ewma`

Pure Python sugar: `python/beava/_agg.py` defines `def ema(*args, **kwargs): return ewma(*args, **kwargs)`. No server change. Wire JSON `op` is always `"ewma"`.

### D-06 — TWA: window REQUIRED; bucket-fold custom

`bv.twa(field, window=...)` — window is required (per AGG-DECAY-06 wording "for irregularly-sampled gauge fields"). State per bucket: `(sum_v_dt, sum_dt, last_v, last_t)`. Query: sum across active buckets `Σsum_v_dt / Σsum_dt`. Returns Null if Σsum_dt == 0.

### D-07 — Output types

- ewma, ewvar, ew_zscore, decayed_sum, decayed_count, twa, rate_of_change, trend, trend_residual, z_score → `F64`
- delta_from_prev → inherit upstream field type (preserves type per req)
- inter_arrival_stats → `F64` (mean_ms in v0 — see D-02; struct deferred)
- burst_count, outlier_count, value_change_count → `I64`

`output_type_for` extended in `agg_op.rs`.

### D-08 — Bench coverage (Phase 6+ rule)

`crates/beava-core/benches/phase9_decay_velocity.rs`:
- 16 microbenches: `agg_op/{op_name}` per-op `update()` with a representative row.
- 1 windowed-fold bench: `windowed/fold_ewma_5m_1Mevt`.
- 1 apply-loop bench: `apply/decay_3agg_100ent_1Kevt` (3 decay features, 100 entities, 1k events).
- Baselines written to `09-perf-row.md` (NOT `.planning/perf-baselines.md` per orchestrator hard constraint).

### D-09 — Throughput run

Add `crates/beava-bench/configs/medium.json` and `large.json` are NOT modified (sibling phases own them). Instead, create `crates/beava-bench/configs/medium_phase9.json` + `large_phase9.json` — copies of medium/large with 5 of the 16 new ops added. Run small (unchanged simple-fraud) + medium_phase9 + large_phase9 against HTTP transport. Write rows to `09-throughput-row.md`. TCP rows recorded as `n/a (TCP push deferred to Phase 8)`.

### D-10 — TDD red→green per task; smoke test additive

Per CLAUDE.md §Conventions (mandatory phase 3+):
- Each operator gets its own task (16 ops + bench + throughput + smoke + Python SDK = ~20 tasks).
- Each task: red commit (`test(09-NN): subject`) → green commit (`feat(09-NN): subject`).
- Phase smoke test (`phase9_smoke.rs`) is additive — register a DAG with all 16 ops via HTTP, push events, query each via `/get`, assert sane outputs.

### Claude's Discretion
- Exact LOC split between `agg_state_decay.rs` and `agg_state_velocity.rs` (single combined file allowed if <800 LOC).
- Whether to introduce `Value::Map` for inter_arrival_stats struct (current decision: NO — emit mean_ms only, defer struct).
- Default `sigma` value for `outlier_count` (chose 3.0 per req).
- Bench `apply/` event count tuning if 1Kevt is too noisy.

## Files in scope (additive)
- `crates/beava-core/src/agg_state.rs` — possibly split into `agg_state_decay.rs` + `agg_state_velocity.rs`
- `crates/beava-core/src/agg_op.rs` — extend `AggKind`, `AggOp`, `output_type_for`
- `crates/beava-core/src/agg_compile.rs` — extend `parse_agg_kind`, half_life validation
- `crates/beava-core/src/agg_windowed.rs` — extend `WindowedOp` for new kinds (custom bucket-fold for TWA)
- `crates/beava-core/src/register_validate.rs` — new ErrorCode variants
- `crates/beava-core/src/agg_op.rs` — extend `AggOpDescriptor` with `half_life_ms`, `sub_window_ms`, `sigma`
- `crates/beava-core/benches/phase9_decay_velocity.rs` — NEW
- `crates/beava-bench/configs/medium_phase9.json`, `large_phase9.json` — NEW
- `python/beava/_agg.py` — 16 new module-level functions + `ema` alias
- `crates/beava-server/tests/phase9_smoke.rs` — NEW
- `.planning/phases/09-decay-velocity-operators/09-throughput-row.md` — NEW
- `.planning/phases/09-decay-velocity-operators/09-perf-row.md` — NEW

## Files NOT to touch (orchestrator hard constraints)
`.planning/STATE.md`, `.planning/throughput-baselines.md`, `.planning/perf-baselines.md`, `.planning/ROADMAP.md`, `.planning/REQUIREMENTS.md`, `CLAUDE.md`, `.planning/phases/0[78]-*`, `.planning/phases/1[01]*`, `.planning/phases/11.5-*`.

## Acceptance signals
- Test count grows from 624 → ~700+ (one or more tests per op).
- All gates green: `cargo test --workspace --features beava-server/testing -- --test-threads=1`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo fmt --all --check`.
- Phase smoke registers + pushes + queries 16 ops end-to-end.
- ≥1 criterion bench file added; rows in `09-perf-row.md`.
- Throughput rows in `09-throughput-row.md`; no >25% regression on simple-fraud (small) HTTP shape.
