---
phase: 25-query-ttl-warnings
plan: 03
subsystem: signals+server+benchmark
tags: [warnings, health, observability, benchmark-gate, phase-closeout]

dependency_graph:
  requires:
    - WARNINGS-DEDUPE-01      # SignalRegistry dedupe (delivered in 25-02)
    - WARNINGS-SEVERITY-01    # Severity enum + sort (delivered in 25-02)
    - SUGGEST-ENGINE-01       # recommend_config (Plan 25-02)
    - SUGGEST-HTTP-01         # /debug/config-recommendations (Plan 25-02)
  provides:
    - WARNINGS-FEED-01
    - WARNINGS-CATEGORIES-01
    - BENCH-25-GATE-01
    - PHASE-25-CLOSEOUT-01
  affects:
    - Phase 26 (demo rebuild consumes /debug/warnings; benchmark gate
      becomes the regression baseline for the v0 milestone close-out)

tech-stack:
  added: []
  patterns:
    - emitter-fan-out-from-recommender
    - poll-cycle-idempotent-dedupe
    - observational-probe-in-matrix-runner
    - two-file-test-split-per-plan-artifact-list

key-files:
  created:
    - tests/test_warnings_feed.rs
    - tests/test_warnings_dedupe.rs
    - tests/test_warnings_integration.rs
    - .planning/phases/25-query-ttl-warnings/MATRIX-V0-POST-25.json
  modified:
    - src/server/signals.rs
    - src/main.rs
    - benchmark/tally-throughput/bench_v0.py

key-decisions:
  - "Config-category emitter is a PURE FAN-OUT over `recommend_config` output.
    It does NOT re-derive thresholds or re-read the EvictionTracker; it takes
    the recommender's verdict verbatim. Rationale: the recommender is the
    single source of truth for the config-knob decision (tested by the
    Plan 25-02 `test_config_recommendations.rs` suite). Fanning its output
    through the SignalRegistry means the warnings feed and the
    `/debug/config-recommendations` endpoint are guaranteed to agree by
    construction — no second-source drift."
  - "Signal id scheme for config: `config.{knob}` (e.g. `config.UserProfile.ttl`).
    Stable across polling cycles so the SignalRegistry's id-keyed dedupe
    collapses repeat observations into one entry. Matches the scheme
    already in use for `register.failure.{pipeline}`, `late_drop.{stream}`,
    `memory.pressure`, `snapshot.failure`, `perf.push_p99_slo_breach`."
  - "Config signal severity: Info. Config recommendations are advisory —
    they never indicate an outage. Operational (snapshot failure) and
    Safety (registration failure) emit at Error; Performance (p99 SLO
    breach) and Data-quality (late-drop rate) at Warning. The severity
    ladder carries the severity-descending sort contract without extra
    work in the feed handler."
  - "Config emitter action payload includes `copy_paste` so the Debug UI
    can render an 'apply this recommendation' button directly from
    /debug/warnings without a second round-trip to
    /debug/config-recommendations. Both surfaces remain in sync because
    both read from the same `recommend_config` return value."
  - "Expanded the plan's 12-test target to 20 tests (10 feed + 6 dedupe +
    4 integration). The original plan spec allocates 8 feed + 4 dedupe in
    Task 1 and 4 integration in Task 2. The 2-test expansion in the
    dedupe file covers the config-emitter-specific dedupe path (across
    polling cycles, with action-payload refresh) which the plan's
    generic dedupe tests did not exercise. The 2-test expansion in the
    feed file adds the history_ttl knob recognition and the
    cross-category single-feed integration case — both mandated by the
    `must_haves.truths` block but not explicitly broken out into tests."
  - "Benchmark matrix: 9 gated cells all within ±5% of BASELINE.json.
    Worst regression: large_1c at -3.83% (noise; on the same host +
    kernel, the absolute difference is ~4.5K eps, well inside the 3-run
    coefficient of variation measured in Phase 24). The 1 Hz signal-check
    ticker hypothesised in the plan's Task 2 §4 risk notes does not
    appear in the runtime profile — the SignalRegistry is
    contention-free for single-writer poll cycles, and
    `emit_config_recommendations` is an O(tables) no-op when all knobs
    are within threshold (which is the case during the bench run since
    the tracker has zero evictions)."
  - "Added `--output` as an alias for `--out` on the bench CLI so the
    literal command from the plan text works verbatim."

metrics:
  duration: ~1h
  completed: 2026-04-14
  tasks: 2
  commits:
    - 5fc6826    # 25-03 Task 1: config emitter + 5-category tests + dedupe tests
    # Task 2 commit added on final metadata push below.

requirements-completed:
  - WARNINGS-FEED-01
  - WARNINGS-CATEGORIES-01
  - BENCH-25-GATE-01
  - PHASE-25-CLOSEOUT-01
---

# Phase 25 Plan 03: Unified warnings feed + benchmark gate + phase close-out Summary

**One-liner:** Wired Plan 25-02's `recommend_config` output into the
Plan 25-02 `SignalRegistry` as `Category::Config` / `Severity::Info`
warnings (closing the last unwired category), added 20 tests across three
files covering all five signal categories and the full dedupe /
integration lifecycle, and captured a 9-cell benchmark matrix
(MATRIX-V0-POST-25.json) that clears the ±5% regression gate against
`BASELINE.json` with a worst-cell delta of -3.83%.

## What shipped

### Task 1 — Config emitter + per-category tests (commit `5fc6826`)

- **`src/server/signals.rs`**: new `emit_config_recommendations(registry,
  recs)` function. Fans every `ConfigRecommendation` returned by
  `recommend_config` into the signal registry with:
  - `id = format!("config.{}", knob)` — stable per knob for cross-poll
    dedupe.
  - `severity = Severity::Info` — recommendations are advisory, not
    outage indicators.
  - `category = Category::Config`.
  - `title` switches on the knob suffix: `"TTL too short"` for
    `*.ttl`, `"history_ttl too short"` for `*.history_ttl`, generic
    otherwise.
  - `action` payload: `{type: "config_change", knob, current, suggested,
    copy_paste}` — the UI can render an "apply suggestion" button
    without fetching `/debug/config-recommendations` separately.
  - `evidence`: includes `evidence_url =
    "/debug/config-recommendations#{knob}"` so click-through works.

- **`src/main.rs::poll_signal_sources`**: fourth emitter added alongside
  the existing late-drop / memory-pressure / p99 emitters. Reads the
  engine once under a short read-lock, calls `recommend_config`, fans
  the output. Runs on the same 30s snapshot cycle as the other emitters.

- **`tests/test_warnings_feed.rs`** — 10 integration tests:
  1. `test_warning_feed_returns_empty_on_healthy_engine`
  2. `test_config_category_from_recommendations`
  3. `test_config_category_covers_history_ttl_knob`
  4. `test_data_quality_category_from_late_drops` (via
     `emit_late_drop_signals` with bootstrap + rate samples)
  5. `test_operational_category_from_snapshot_failure`
  6. `test_operational_category_from_memory_pressure_via_record`
     (tests the severity-escalation Warning→Critical boundary)
  7. `test_safety_category_from_registration_failure`
  8. `test_performance_category_from_p99_slo_breach`
  9. `test_all_five_categories_fire_in_one_feed` (severity-sort +
     category-coverage contract in one response)
  10. `test_schema_shape_matches_context_md` (all required fields
      present on every warning)

- **`tests/test_warnings_dedupe.rs`** — 6 integration tests:
  1. `test_same_id_preserves_first_seen`
  2. `test_distinct_ids_coexist`
  3. `test_config_emitter_dedupes_across_polls` (explicit cycle-to-cycle
     dedupe simulation)
  4. `test_config_emitter_updates_suggestion_on_redup` (action payload
     refresh on re-observation)
  5. `test_dedupe_registry_ephemeral_on_restart`
  6. `test_direct_record_and_emitter_agree_on_dedupe`

### Task 2 — Integration tests + benchmark gate + SUMMARYs

- **`tests/test_warnings_integration.rs`** — 4 end-to-end tests through
  the live Axum router:
  1. `test_late_drop_fires_and_clears` — bootstrap → rate cross →
     age-out past window.
  2. `test_config_recommendation_surfaces_as_warning` — same
     recommendation visible on both `/debug/warnings?category=config`
     and `/debug/config-recommendations`.
  3. `test_registration_failure_warning_persists_until_resolve` — 5
     back-to-back polls all see the signal; explicit age-out removes it.
  4. `test_warnings_endpoint_load` — 100 concurrent `/debug/warnings`
     requests all return consistent snapshots (exactly 3 signals
     pre-seeded), and the registry is not mutated by reads.

- **`benchmark/tally-throughput/bench_v0.py`** (modified):
  - Added `--output` as an alias for `--out` so the literal plan command
    works.
  - Added `_probe_warnings_endpoint(host)` that hits
    `http://{host}:{http_port}/debug/warnings` 3 times per matrix run
    and records `samples_us` + `median_us` under the output key
    `warnings_endpoint_probe`. Observational only — NOT part of the
    regression gate.

- **`.planning/phases/25-query-ttl-warnings/MATRIX-V0-POST-25.json`** —
  captured with the following shape (label `v0-post-25-03`,
  `gate_passed: true`, 7 runs per cell, 30K events per run):

| Cell        | eps_median | delta vs BASELINE | p99 µs  | pass |
|-------------|-----------:|------------------:|--------:|------|
| small_1c    |    113,921 |            -1.01% |    9.94 | ✓    |
| small_4c    |     28,065 |            +0.02% |  661.98 | ✓    |
| small_8c    |     30,558 |            +0.63% | 1418.98 | ✓    |
| medium_1c   |    113,671 |            -1.56% |   10.42 | ✓    |
| medium_4c   |     27,951 |            -0.86% |  669.36 | ✓    |
| medium_8c   |     30,626 |            +1.33% | 1497.53 | ✓    |
| large_1c    |    111,939 |            -3.83% |   11.25 | ✓    |
| large_4c    |     28,500 |            +1.43% |  654.64 | ✓    |
| large_8c    |     29,975 |            -2.28% | 1458.40 | ✓    |

**Characterisation cells (no gate):**
- `join_small_1c`: 110,793 eps (~97% of `small_1c`)
- `enrich_small_1c`: 113,964 eps (~100%)
- `late_events_small_1c`: 120,237 eps (~105%)
- `tombstone_cascade_small_1c`: 22,922 eps (20% — sync ack path)
- `tt_join_real_small_1c`: 22,533 eps (20% — sync ack path)
- `enrich_with_wm_small_1c`: 131,847 eps (~116% — warm-run amplification)

**Warnings-endpoint probe (observational):**
- URL: `http://localhost:6401/debug/warnings`
- Samples (µs): 1648.55 (cold), 439.41, 323.69
- Median (µs): 439.41
- The cold first call picks up route-table construction + allocator
  warm-up; subsequent calls settle under 500µs. Well below any SLO.

## Test results

| Suite                        | Before 25-03 | After 25-03 |
|------------------------------|-------------:|------------:|
| `cargo test --lib`           |          722 |     **722** (unchanged; emitter is a thin wrapper, all assertions covered in integration tests) |
| `test_signal_registry`       |       19 / 19|     19 / 19 |
| `test_debug_warnings_endpoint` |       10 / 10|     10 / 10 |
| `test_config_recommendations`|         8 / 8 |       8 / 8 |
| `test_warnings_feed`         |            — |   **10 / 10** (new) |
| `test_warnings_dedupe`       |            — |    **6 / 6** (new) |
| `test_warnings_integration`  |            — |    **4 / 4** (new) |
| `cargo test --tests` (all)   |        green |     **green** (no regressions from Plans 25-01 / 25-02) |

Total new Rust tests: **20 integration**.

## Deviations from plan

All within deviation-rule scope (Rule 2: auto-add missing critical functionality):

1. **Rule 2 — expanded test count from 12 to 20.** Plan specified 8
   feed + 4 dedupe + 4 integration = 16 tests. I shipped 10 feed + 6
   dedupe + 4 integration = 20. The +2 in the feed file cover the
   `history_ttl` knob title path (mandated by the config emitter's
   branch logic but not explicitly called out in Task 1 behaviour
   enumeration) and the cross-category single-feed contract (mandated by
   `must_haves.truths` but not pulled out as its own test). The +2 in
   the dedupe file cover the config emitter's cycle-to-cycle dedupe and
   its action-payload refresh semantics — both behaviours the production
   path relies on at every poll cycle.

2. **Rule 2 — added `--output` CLI alias on `bench_v0.py`.** The plan's
   command line text used `--output` but the existing argparse only
   accepted `--out`. Added an alias so the literal plan command works
   without surprises.

3. **Plan 25-02 already shipped most of the warnings infrastructure.**
   The `<interfaces>` block in Plan 25-03 described a `warnings.rs`
   module with a `WarningRegistry`; Plan 25-02 had already landed an
   equivalent `signals.rs` with `SignalRegistry`, all five emitters
   (register, snapshot, memory, late-drop, p99), the `/debug/warnings`
   handler under the admin gate, 19 signal-registry unit tests, and 10
   endpoint integration tests. This plan therefore did not re-create a
   parallel module — instead it added the one missing emitter (config)
   and the test coverage the plan named. No functional gap vs the
   `must_haves.truths` or `success_criteria` blocks.

4. **Warning struct field naming.** The plan's `<interfaces>` suggested
   `Warning` / `details` (evidence field). The shipped implementation
   uses Plan 25-02's `Signal` / `evidence` — semantically identical,
   named before 25-03 existed. The endpoint schema matches CONTEXT.md
   §Warnings; only the in-process type names differ.

5. **Data-quality threshold uses rate/sec, not ratio.** Plan 25-03
   behaviour text reads "late-drop rate > 1%" (ratio). Plan 25-02's
   emitter uses `threshold_per_sec` (rate). Kept 25-02's shape: a rate
   threshold is directly observable from the
   `tally_late_events_dropped_total` counter delta over the poll
   interval, whereas a ratio requires a denominator (total events
   pushed) that is not currently aggregated per-stream at the signal
   cadence. Documented here; no gap in the test matrix — the
   `data_quality` category still triggers on the production late-drop
   path.

## Threat register — disposition

| Threat ID  | Status    | Evidence                                                                                                                            |
|------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------|
| T-25-03-01 | Mitigated | `/debug/warnings` lives in `admin_router` (http.rs line 1409), gated by `require_loopback_or_token`. Covered by `test_debug_warnings_is_admin_gated`. |
| T-25-03-02 | Accepted  | No explicit 1024-entry cap shipped this plan — registry cap is a post-v0 item. Observable via `SignalRegistry::len()`; the `age_out` call on every `/debug/warnings` fetch bounds growth in practice for the 7d window. |
| T-25-03-03 | Accepted  | SignalRegistry uses `Arc<RwLock<_>>`; re-emission after resolve is idempotent — signal re-appears at next poll, which is the intended behaviour. |
| T-25-03-04 | Mitigated | The 1 Hz ticker was not implemented — signal emission runs on the existing 30s snapshot cycle. Benchmark matrix confirms no throughput regression (-3.83% worst cell, within ±5%). |
| T-25-03-05 | Mitigated | Register-failure emitter uses `pipeline_name` only in `id`/`title`; the `detail` carries the error string which the handler-boundary code (`src/server/tcp.rs:1743-1749`, `src/server/http.rs:226-233`) constructs from TallyError — no raw submitted JSON is exposed. |
| T-25-03-06 | Mitigated | Matrix run used 7 runs per cell, median taken per cell, with a warm-up pass across `small` / `medium` / `large` pipelines before the gated runs. Same host-load pattern carried from Phase 24-05. |

## Self-Check: PASSED

Files (absolute paths):

- `/data/home/tally/src/server/signals.rs` — FOUND (modified; `emit_config_recommendations` added)
- `/data/home/tally/src/main.rs` — FOUND (modified; config emitter in `poll_signal_sources`)
- `/data/home/tally/tests/test_warnings_feed.rs` — FOUND (created; 10 tests)
- `/data/home/tally/tests/test_warnings_dedupe.rs` — FOUND (created; 6 tests)
- `/data/home/tally/tests/test_warnings_integration.rs` — FOUND (created; 4 tests)
- `/data/home/tally/benchmark/tally-throughput/bench_v0.py` — FOUND (modified; `--output` alias + `_probe_warnings_endpoint`)
- `/data/home/tally/.planning/phases/25-query-ttl-warnings/MATRIX-V0-POST-25.json` — FOUND (created; gate_passed=true)

Commits verified on `main`:

- `5fc6826` feat(25-03): wire config recommendations into /debug/warnings + per-category tests

Test gates (2026-04-14):

- `cargo test --lib` — 722 / 722.
- `cargo test --test test_warnings_feed` — 10 / 10.
- `cargo test --test test_warnings_dedupe` — 6 / 6.
- `cargo test --test test_warnings_integration` — 4 / 4.
- `cargo test --test test_signal_registry` — 19 / 19 (no regression).
- `cargo test --test test_debug_warnings_endpoint` — 10 / 10 (no regression).
- `cargo test --test test_config_recommendations` — 8 / 8 (no regression).

Matrix gate (MATRIX-V0-POST-25.json):
- `gate_passed: true`
- 9 / 9 cells within ±5% of BASELINE.json (worst: large_1c at -3.83%)
- Warnings-endpoint probe median: 439µs (observational)

Plan 25-03 is complete. Phase 25 is closed — see `25-SUMMARY.md`.
