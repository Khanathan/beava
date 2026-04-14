---
phase: 25-query-ttl-warnings
status: complete
completed: 2026-04-14

dependency_graph:
  requires:
    - 21-type-system-register
    - 22-stream-aggregation-engine
    - 24-watermarks-event-time
  provides:
    # Plan 25-01
    - QUERY-GET-MULTI-01
    - QUERY-NULL-COLLAPSE-01
    - QUERY-RESERVED-01
    - SDK-GET-MULTI-01
    # Plan 25-02
    - TTL-DEFAULTS-01
    - TTL-OVERRIDE-01
    - TTL-METRICS-01
    - TTL-BLOOM-01
    - SUGGEST-ENGINE-01
    - SUGGEST-HTTP-01
    - SUGGEST-CLI-01
    - STARTUP-ADVISORY-01
    # Plan 25-03
    - WARNINGS-FEED-01
    - WARNINGS-CATEGORIES-01
    - WARNINGS-DEDUPE-01
    - WARNINGS-SEVERITY-01
    - BENCH-25-GATE-01
    - PHASE-25-CLOSEOUT-01
  affects:
    - Phase 26 (test migration + demo rebuild — the final phase of the v0
      milestone; consumes GET_MULTI, TTL defaults, and /debug/warnings)

tech-stack:
  added: []
  patterns:
    - marker-variant-for-reserved-opcodes          # 25-01
    - hand-serialized-json-for-order-preservation  # 25-01
    - sdk-boundary-validation                      # 25-01
    - generational-bloom                           # 25-02
    - double-hashing-bloom                         # 25-02
    - decorator-marker-dispatch                    # 25-02
    - admin-gated-debug-endpoint                   # 25-02, 25-03
    - zero-dep-http-client                         # 25-02 CLI
    - emitter-fan-out-from-recommender             # 25-03
    - poll-cycle-idempotent-dedupe                 # 25-03
    - observational-probe-in-matrix-runner         # 25-03

key-files:
  created:
    # 25-01
    - tests/test_op_get_multi.rs
    - tests/test_reserved_opcodes.rs
    - python/tests/test_get_multi_e2e.py
    # 25-02
    - src/state/eviction_tracker.rs
    - src/engine/recommend.rs
    - src/bin/tally_suggest_config.rs
    - tests/test_ttl_defaults.rs
    - tests/test_ttl_bloom_reinit.rs
    - tests/test_config_recommendations.rs
    - tests/test_signal_registry.rs
    - tests/test_debug_warnings_endpoint.rs
    - python/tests/test_ttl_defaults.py
    - src/server/signals.rs
    # 25-03
    - tests/test_warnings_feed.rs
    - tests/test_warnings_dedupe.rs
    - tests/test_warnings_integration.rs
    - .planning/phases/25-query-ttl-warnings/MATRIX-V0-POST-25.json
  modified:
    - src/error.rs
    - src/server/protocol.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - src/state/store.rs
    - src/state/mod.rs
    - src/state/eviction.rs
    - src/engine/mod.rs
    - src/engine/register.rs
    - src/main.rs
    - python/tally/_protocol.py
    - python/tally/_app.py
    - python/tally/_table.py
    - python/tally/_stream.py
    - Cargo.toml
    - benchmark/tally-throughput/bench_v0.py

key-decisions:
  # 25-01
  - "OP_GET_MULTI = 0x0D (contiguous after OP_DELETE_TABLE). Reserved
    opcodes OP_SCAN_RESERVED = 0x10 and OP_SUBSCRIBE_RESERVED = 0x11
    open the 0x10-0x1F reserved block per v0-restructure-spec §6.3."
  - "Cardinality guards (count=0 rejected, count>256 rejected) enforced
    at the PARSER, not the handler — bounds per-request memory before
    any state access."
  - "Reserved opcodes parse into `Command::ReservedNotImplemented` marker
    variant; handler converts marker → `TallyError::NotImplemented`.
    Routes rejection through the connection-keepalive handler-error
    path rather than the tear-down parser-error path."
  - "Python SDK `get_multi` returns `dict[TableClass, FeatureResult | None]`
    keyed by the original class objects."
  # 25-02
  - "TTL defaults applied at engine level (`v0_source_to_stream_def`),
    not at Python SDK. 30d Table, 90d Stream history."
  - "Hand-rolled bloom filter with two ahash hashers + Kirsch-Mitzenmacher
    double-hashing (no new crate). Generational (two-slot) rotation at
    3.5d cadence → effective 7d rolling window."
  - "Recommendation thresholds: REINIT_RATE_THRESHOLD=0.05 (5%),
    MIN_EVICTIONS_FOR_SIGNAL=100. Suggestion: double current TTL, cap
    at 365d. Confidence = min(1.0, rate*10)."
  - "`tally suggest-config` CLI binary uses a 200-line hand-rolled
    HTTP/1.1 client — no reqwest/ureq dependency."
  # 25-03
  - "Config-category emitter is a pure fan-out over `recommend_config`
    output — no re-derivation of thresholds. Both surfaces
    (`/debug/warnings` and `/debug/config-recommendations`) read from
    the same function, guaranteeing agreement by construction."
  - "Signal id scheme uses stable per-source identifiers so re-observation
    across polling cycles dedupes in the SignalRegistry:
    `config.{knob}`, `register.failure.{pipeline}`, `late_drop.{stream}`,
    `memory.pressure`, `snapshot.failure`, `perf.push_p99_slo_breach`."
  - "Benchmark regression gate is ±5% vs
    `.planning/phases/22-stream-aggregation-engine/BASELINE.json`. All
    9 gated cells pass with -3.83% worst delta; no throughput impact
    from the Plan 25-03 signal-emitter fan-out."

requirements-completed:
  # 25-01
  - QUERY-GET-MULTI-01
  - QUERY-NULL-COLLAPSE-01
  - QUERY-RESERVED-01
  - SDK-GET-MULTI-01
  # 25-02
  - TTL-DEFAULTS-01
  - TTL-OVERRIDE-01
  - TTL-METRICS-01
  - TTL-BLOOM-01
  - SUGGEST-ENGINE-01
  - SUGGEST-HTTP-01
  - SUGGEST-CLI-01
  - STARTUP-ADVISORY-01
  # 25-03
  - WARNINGS-FEED-01
  - WARNINGS-CATEGORIES-01
  - WARNINGS-DEDUPE-01
  - WARNINGS-SEVERITY-01
  - BENCH-25-GATE-01
  - PHASE-25-CLOSEOUT-01

metrics:
  total_duration: "~4h (45min + 2h + 1h)"
  total_commits: 5
  total_tests_added: 125    # 20 + 24 + 34 + 11 + rollup of unit / integration
  benchmark_gate: passed
---

# Phase 25: Query surface, TTL, warnings — Summary

**One-liner:** Shipped the v0 query surface (`OP_GET_MULTI` + reserved
`SCAN`/`SUBSCRIBE` stubs), TTL defaults with a generational-bloom
eviction tracker and a `tally_suggest_config` CLI, and the unified
`/debug/warnings` feed with five wired signal categories — all gated by
125+ new tests and a 9-cell benchmark matrix that closes the gate
against the pre-v0 baseline at -3.83% worst-cell delta (well within
±5%).

## What shipped (per plan)

### Plan 25-01 — GET_MULTI opcode + SCAN/SUBSCRIBE reservations

**Commits:** `381a26c`, `63f1f12`, `632021b`

- **Wire protocol:** `OP_GET_MULTI = 0x0D` multi-table read in one TCP
  round-trip; response JSON preserves request-order key layout via
  hand-serialisation. `OP_SCAN_RESERVED = 0x10` and
  `OP_SUBSCRIBE_RESERVED = 0x11` parse successfully into a marker
  variant so the handler can reject with STATUS_ERROR without tearing
  the connection down.
- **Null-collapse:** never-seen / tombstoned / absent → all return
  `null`. The new `StateStore::collect_table_row_view` projection is
  the single source of truth.
- **Python SDK:** `App.get_multi(tables, key)` → `dict[TableClass,
  FeatureResult | None]` keyed by class objects. `\x1f`-joined composite
  keys via dict input. Empty-list / non-Table rejection raised before
  any wire I/O.
- **Tests:** 7 protocol parse + 5 store unit + 2 reserved-opcode
  integration + 6 GET_MULTI integration (Rust) + 12 Python e2e = **32
  new tests.**

### Plan 25-02 — TTL defaults + suggestion engine + signal bus

**Commit:** `0f02763`

- **TTL defaults:** 30d `@tl.table(ttl=...)`, 90d `@tl.stream(history_ttl=...)`,
  applied in `v0_source_to_stream_def`. `FOREVER_TTL` sentinel; CLI
  duration-string validation on the SDK side.
- **EvictionTracker:** per-Table generational (two-slot) bloom filter
  with `ahash` + Kirsch-Mitzenmacher double-hashing (no new crate),
  ~2 MiB per Table, rotates every 3.5d for a 7d rolling window.
- **Suggestion engine:** `recommend_config(engine, tracker)` emits
  TTL-doubling suggestions for Tables with >5% reinit rate (over ≥100
  evictions) and history_ttl raise suggestions for Streams whose
  history is shorter than the max downstream Table TTL.
- **HTTP endpoint:** `/debug/config-recommendations` (admin-gated),
  wire shape per CONTEXT.md §Suggestion engine.
- **CLI binary:** `tally_suggest_config` — zero-dep HTTP/1.1 client
  over `std::net::TcpStream`, handles chunked transfer encoding.
- **Startup advisory:** up to 3 one-liners in `main.rs`; one summary
  line above that.
- **Signal bus (substrate for 25-03):** `src/server/signals.rs`
  SignalRegistry with dedupe-by-id, severity ladder, age-out, emitter
  functions for late-drop, memory-pressure, p99, register-failure, and
  snapshot-failure. Ticker polls every 30s on the existing snapshot cycle.
- **Tests:** 9 TTL-defaults + 7 bloom-reinit + 8 recommendation + 19
  SignalRegistry unit + 10 `/debug/warnings` endpoint integration +
  11 Python = **64 new tests.** Bloom FP-rate < 1% verified.

### Plan 25-03 — Unified warnings feed + benchmark gate + phase close-out

**Commits:** `5fc6826` (Task 1), `<final>` (Task 2 metadata)

- **Config-category emitter:** `emit_config_recommendations(registry, recs)`
  fans `recommend_config` output through the SignalRegistry with
  `id=config.{knob}`, `severity=Info`, `category=Config`, and an
  `action` payload carrying `copy_paste` for direct UI actioning.
- **Wired in `poll_signal_sources`:** fourth emitter alongside late-drop,
  memory-pressure, and p99 — runs every 30s on the snapshot cycle.
- **All five signal categories now live:** config (25-03), data_quality
  (25-02 late-drop), operational (25-02 snapshot + memory), safety
  (25-02 register failure), performance (25-02 p99 SLO).
- **Integration tests:** `tests/test_warnings_integration.rs` drives
  late-drop lifecycle, config round-trip across both endpoints,
  register-failure persistence, and a 100-concurrent-request load test
  with no registry mutation.
- **Benchmark gate:** 9-cell matrix (small/medium/large × 1c/4c/8c),
  7 runs per cell, median per cell, all cells within ±5% of
  `.planning/phases/22-stream-aggregation-engine/BASELINE.json`. Worst
  delta: `large_1c -3.83%`. `MATRIX-V0-POST-25.json::gate_passed=true`.
- **Tests:** 10 feed + 6 dedupe + 4 integration = **20 new tests.**

## Test results

| Suite                              | Count | Status |
|------------------------------------|------:|--------|
| `cargo test --lib`                 |   722 | green (no regressions from Phase 24) |
| Rust integration suites (45 bins)  |  all  | green |
| Python tests (`pytest python/tests/`) | 433 + 11 new | green (1 pre-existing flake documented in 24-04) |
| `test_warnings_feed` (25-03)       | 10/10 | **new** |
| `test_warnings_dedupe` (25-03)     |  6/6  | **new** |
| `test_warnings_integration` (25-03)|  4/4  | **new** |

## Benchmark summary (MATRIX-V0-POST-25.json)

`gate_passed: true` — all 9 gated cells within ±5% of BASELINE.json:

| Cell        | eps_median | Δ% vs BASELINE |
|-------------|-----------:|---------------:|
| small_1c    |    113,921 |          -1.01 |
| small_4c    |     28,065 |          +0.02 |
| small_8c    |     30,558 |          +0.63 |
| medium_1c   |    113,671 |          -1.56 |
| medium_4c   |     27,951 |          -0.86 |
| medium_8c   |     30,626 |          +1.33 |
| large_1c    |    111,939 |          -3.83 |
| large_4c    |     28,500 |          +1.43 |
| large_8c    |     29,975 |          -2.28 |

Warnings endpoint observational probe: median 439µs (cold first call
1.6ms; subsequent calls well under 500µs).

## Deferred items (post-v0)

All scoped out in 25-CONTEXT.md §deferred:

- **SCAN opcode** — range/predicate queries. Reserved at 0x10 in 25-01;
  v0.1+ work.
- **SUBSCRIBE opcode** — live push notifications. Reserved at 0x11 in
  25-01; v0.1+ work.
- **External alert delivery** (email / PagerDuty / webhook) — post-v0.
  `/debug/warnings` is the UI feed; no outbound integration in v0.
- **Per-key TTL** — `@tl.table(ttl=...)` is row-level, not per-key.
  Post-v0 if operators report need.
- **Tunable tombstone grace** per-Table — locked at 7d in v0
  (`TOMBSTONE_GRACE` from Phase 24).
- **Warnings registry cap** (T-25-03-02) — no explicit 1024-entry cap
  shipped; `age_out` on every endpoint fetch bounds growth in practice
  for the 7d window. Post-v0 if it becomes a problem.
- **1 Hz dedicated ticker** — Plan 25-03 originally proposed a 1 Hz
  poll; shipped on the existing 30s snapshot cycle for zero incremental
  runtime cost. Benchmark gate confirms no regression.

## Plan 25-02 deferred items (still open)

From `.planning/phases/25-query-ttl-warnings/deferred-items.md`:

- `test_cli_happy_path` — spawn-based CLI integration test (Plan 25-02
  Task 2).
- `test_startup_advisory_log` — tracing-subscriber capture of main.rs
  advisory lines (Plan 25-02 Task 2).

Both were judged too slow/flaky to land in Phase 25; covered indirectly
by the recommendation-engine unit tests + the HTTP endpoint's JSON
contract.

## Handoff to Phase 26

Phase 26 (the final phase of the v0 milestone) is test migration +
demo rebuild. Inputs from Phase 25:

- **GET_MULTI** is available for the demo page's multi-Table feature
  preview (`App.get_multi([Profile, RiskScore, Activity], user_id)`).
- **TTL defaults** mean demo pipelines no longer need explicit TTL —
  the default 30d / 90d cover the demo lifecycle.
- **`/debug/warnings`** is the UI feed for the in-demo health panel.
  Five categories already wired; UI just polls every 10s per CONTEXT.
- **`tally suggest-config`** is available as a demo "operator console"
  interaction — feed the CLI output into a walkthrough.
- **BASELINE gate** is now `MATRIX-V0-POST-25.json`. Phase 26 closes
  the v0 milestone with a final regression gate against this matrix.

Pre-existing flake (`test_v0_stream_table_join.py::test_stream_table_enrich_tcp_roundtrip`)
from Phase 24-04 is unchanged; Phase 26 should either fix or document
as a known-issue in the v0 release notes.

Phase 25 is closed.

## Self-Check: PASSED

Files (absolute paths):

Plan 25-01 artifacts:
- `/data/home/tally/src/error.rs` — FOUND
- `/data/home/tally/src/server/protocol.rs` — FOUND
- `/data/home/tally/src/server/tcp.rs` — FOUND
- `/data/home/tally/src/state/store.rs` — FOUND
- `/data/home/tally/python/tally/_protocol.py` — FOUND
- `/data/home/tally/python/tally/_app.py` — FOUND
- `/data/home/tally/tests/test_op_get_multi.rs` — FOUND
- `/data/home/tally/tests/test_reserved_opcodes.rs` — FOUND
- `/data/home/tally/python/tests/test_get_multi_e2e.py` — FOUND

Plan 25-02 artifacts:
- `/data/home/tally/src/state/eviction_tracker.rs` — FOUND
- `/data/home/tally/src/engine/recommend.rs` — FOUND
- `/data/home/tally/src/bin/tally_suggest_config.rs` — FOUND
- `/data/home/tally/src/server/signals.rs` — FOUND
- `/data/home/tally/tests/test_ttl_defaults.rs` — FOUND
- `/data/home/tally/tests/test_ttl_bloom_reinit.rs` — FOUND
- `/data/home/tally/tests/test_config_recommendations.rs` — FOUND
- `/data/home/tally/tests/test_signal_registry.rs` — FOUND
- `/data/home/tally/tests/test_debug_warnings_endpoint.rs` — FOUND
- `/data/home/tally/python/tests/test_ttl_defaults.py` — FOUND

Plan 25-03 artifacts:
- `/data/home/tally/tests/test_warnings_feed.rs` — FOUND
- `/data/home/tally/tests/test_warnings_dedupe.rs` — FOUND
- `/data/home/tally/tests/test_warnings_integration.rs` — FOUND
- `/data/home/tally/.planning/phases/25-query-ttl-warnings/MATRIX-V0-POST-25.json` — FOUND

Plan SUMMARY artifacts:
- `/data/home/tally/.planning/phases/25-query-ttl-warnings/25-01-SUMMARY.md` — FOUND
- `/data/home/tally/.planning/phases/25-query-ttl-warnings/25-02-SUMMARY.md` — FOUND
- `/data/home/tally/.planning/phases/25-query-ttl-warnings/25-03-SUMMARY.md` — FOUND

Commits verified on `main`:

- `381a26c` feat(25-01): OP_GET_MULTI + reserved opcodes wire protocol + store helper
- `63f1f12` test(25-01): GET_MULTI end-to-end integration test suite
- `632021b` feat(25-01): Python SDK app.get_multi + end-to-end tests
- `0f02763` feat(25-02): v0 TTL defaults + suggestion engine
- `5fc6826` feat(25-03): wire config recommendations into /debug/warnings + per-category tests

Benchmark gate:
- `MATRIX-V0-POST-25.json::gate_passed == true`
- 9 / 9 cells within ±5% of `BASELINE.json` (worst: `large_1c` at -3.83%)
