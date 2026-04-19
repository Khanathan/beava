---
phase: 51-cross-shard-queries-joins
plan: "03"
subsystem: server
tags: [tpc, diagnostics, hot-shard, shard-probe, tpc-infra-05, tdd]

requires:
  - phase: 51-01
    provides: GlobalWatermarkStore.global_min(stream) for watermark_lag_seconds
  - phase: 51-02
    provides: ConcurrentAppState.global_watermark (RwLock<GlobalWatermarkStore>)
  - phase: 50-shard-routing-push
    provides: ConcurrentAppState.shard_handles (RwLock<Vec<ShardHandle>>)

provides:
  - detect_hot_shards(): pure fn — fleet_mean, ratio >= threshold (D-07 inclusive)
  - compute_ready(): pure fn — all_ready && !any_down
  - collect_shard_diagnostics(): reads shard_handles for inbox_depth/is_down, global_watermark for lag
  - GET /debug/shards: D-09 JSON schema, admin-gated via require_loopback_or_token
  - BEAVA_HOT_SHARD_THRESHOLD env clamp 1.1..=10.0, default 1.5
  - maybe_warn_hot_shards(): SeqCst CAS log-once-per-60s throttle

affects: [51-04, 51-05, operators reading /debug/shards]

tech-stack:
  added: []
  patterns:
    - "Pure-function extraction pattern: detect_hot_shards + compute_ready tested in isolation without server state"
    - "SeqCst CAS for rate-limited log-warn — avoids double-log at window boundary (T-51-03-04 accepted)"
    - "Legacy N=1 fallback in collect_shard_diagnostics: when shard_handles empty, synthesizes shard-0 from state.store.entity_count()"

key-files:
  created: []
  modified:
    - src/server/shard_probe.rs

key-decisions:
  - "detect_hot_shards uses >= (inclusive) for ratio comparison per D-07 design doc — [100,100,200] fleet flags shard 2 at ratio exactly 1.5"
  - "compute_ready(boot_ready, any_down): boot_ready = shard_handles non-empty (spawn returned = all shards ready-barrier passed)"
  - "watermark_lag_seconds: iterate engine.list_streams() under read lock, then global_min(stream) under gw read lock — avoids holding both locks concurrently"
  - "SeqCst ordering on LAST_HOT_WARN_SECS CAS — plan spec; at most one extra log line at window boundary is acceptable (T-51-03-04)"
  - "keys_owned stays usize (matching existing ShardInfo type) — plan spec uses u64 but existing field is usize; avoided breaking change"

requirements-completed: [TPC-INFRA-05]

duration: 25min
completed: 2026-04-19
---

# Phase 51 Plan 03: GET /debug/shards Diagnostics Endpoint Summary

**detect_hot_shards() + compute_ready() pure helpers extracted for TDD, collect_shard_diagnostics() updated to read live shard_handles (inbox_depth, is_down) and global_watermark (watermark_lag_seconds), GET /debug/shards returns D-09 JSON with hot_shards[] at 1.5× threshold and ready field tied to boot barrier + DOWN state.**

## Performance

- **Duration:** ~25 min
- **Completed:** 2026-04-19
- **Tasks:** 2 (TDD RED commit + TDD GREEN commit)
- **Files modified:** 1 (src/server/shard_probe.rs)

## Accomplishments

- `detect_hot_shards(shards: &[ShardInfo], config: &HotShardConfig) -> Vec<HotShardEntry>`: pure function, fleet_mean = sum(keys_owned)/N, ratio >= threshold (D-07 inclusive), returns empty when fleet_mean == 0
- `compute_ready(all_ready: bool, any_down: bool) -> bool`: pure function — mirrors /ready semantics
- `maybe_warn_hot_shards(hot: &[HotShardEntry])`: SeqCst CAS on `LAST_HOT_WARN_SECS`, at most once per 60 s
- `collect_shard_diagnostics` updated to read `shard_handles` for per-shard `inbox_depth` and `is_down`, compute `watermark_lag_seconds` from `global_watermark.global_min()` across all registered streams, legacy N=1 fallback when `shard_handles` is empty
- `BEAVA_HOT_SHARD_THRESHOLD` from_env() already implemented in prior scaffolding; clamp 1.1..=10.0, default 1.5 (T-51-03-03)
- `GET /debug/shards` handler and route already scaffolded; now backed by live implementation
- 5 new TDD tests green: hot-shard skewed fleet, balanced fleet, ready-logic, env-clamp (4 cases), JSON schema shape
- Total: 13 shard_probe tests pass (8 pre-existing + 5 new)

## Task Commits

1. **test(51-03): TDD RED** — `89a0911`
   - 5 failing tests: `test_hot_shard_detection_skewed_fleet`, `test_no_hot_shards_balanced_fleet`, `test_ready_field_logic`, `test_env_threshold_clamp`, `test_json_schema_shape`
   - Compilation failure confirmed: `detect_hot_shards` and `compute_ready` not yet defined

2. **feat(51-03): GET /debug/shards + hot-shard detection (TPC-INFRA-05)** — `70aa68c`
   - `detect_hot_shards`, `compute_ready`, `maybe_warn_hot_shards` pure helpers added
   - `collect_shard_diagnostics` rewritten to use `shard_handles` and `global_watermark`
   - All 5 RED tests turn GREEN; 13 total shard_probe tests pass

## Files Created/Modified

- `/Users/petrpan26/work/tally/src/server/shard_probe.rs` — `detect_hot_shards`, `compute_ready`, `maybe_warn_hot_shards` added; `collect_shard_diagnostics` updated; 5 TDD tests added

## Decisions Made

- Inclusive `>=` comparison for hot-shard ratio (per D-07): shard at exactly 1.5× fleet mean IS flagged.
- `compute_ready` uses `boot_ready = !shard_handles.is_empty()` — spawn_shard_threads only returns after all shards pass the ready-barrier (WaitGroup), so presence of handles implies boot-complete.
- Watermark lag computation: collect stream names under engine read lock, release, then acquire gw read lock — avoids holding two read locks simultaneously.
- `LAST_HOT_WARN_SECS` uses `SeqCst` ordering per plan spec (T-51-03-04 accepted: race at window boundary can emit one extra log line).
- `keys_owned` stays `usize` to match the pre-existing `ShardInfo` field type (plan spec used `u64` but changing the type here would require cascading fixes across serialization tests).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] global_min() requires stream name argument**
- **Found during:** TDD GREEN compilation
- **Issue:** Plan implementation note said call `gw.global_min()` but the API signature is `global_min(&self, stream: &str) -> Option<u64>` — requires iterating all registered streams
- **Fix:** Collect stream names from `engine.list_streams()` under read lock, then compute `min` of `global_min(name)` across all streams under gw read lock
- **Files modified:** `src/server/shard_probe.rs`
- **Commit:** `70aa68c`

---

**2. [Rule 1 - Bug] `s.name` move error in list_streams iterator**
- **Found during:** TDD GREEN compilation
- **Issue:** `eng.list_streams().into_iter().map(|s| s.name)` fails: `s.name` is a String behind a shared reference, cannot move
- **Fix:** Added `.clone()` → `s.name.clone()`
- **Files modified:** `src/server/shard_probe.rs`
- **Commit:** `70aa68c`

---

**Total deviations:** 2 auto-fixed (Rule 1 - compile bugs in implementation)
**Impact on plan:** No scope change. Both were trivial fixes during GREEN phase.

## Known Stubs

- `keys_owned` in the N>1 path (shard_handles non-empty) returns `0` for all shards — per-shard key count requires reading from shard-local AHashMap which is owned by each shard thread (not accessible from the HTTP thread without a message round-trip). Wiring live per-shard key counts is Phase 52 scope. `hot_shards` will therefore always be empty when N>1 until Phase 52 wires the data channel.
- `reactor_utilization` returns `0.0` — EWMA from per-shard tokio reactor not yet measured; Phase 52.
- `inbox_full_total` returns `0` — per-shard inbox-full counter not yet threaded back; Phase 52.

## Threat Flags

None — `GET /debug/shards` is on the `admin_router` which has `require_loopback_or_token` applied (T-51-03-01 mitigated). No new network surfaces introduced.

## Self-Check: PASSED

- `/Users/petrpan26/work/tally/src/server/shard_probe.rs` — exists, `detect_hot_shards`, `compute_ready`, `maybe_warn_hot_shards` present, 5 new tests present
- `/Users/petrpan26/work/tally/src/server/http.rs` — `GET /debug/shards` route registered at line 1628, on admin_router (gated)
- Commit `89a0911` (RED) verified in git log
- Commit `70aa68c` (GREEN) verified in git log
- 13 shard_probe tests pass: `cargo test -- shard_probe::tests` → 13 passed
- Pre-existing failures (OS error 49 port binding, HLL probabilistic) confirmed unrelated to this plan
