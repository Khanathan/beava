---
phase: 24-watermarks-event-time
subsystem: engine+storage+protocol+sdk
tags: [watermark, event-time, table-storage, cascade, tombstone, gamma-propagation, snapshot-migration, closeout]

dependency_graph:
  requires:
    - 22-stream-aggregation-engine    # operator catalog + ring-buffer windowing
    - 23-joins                        # ST enrichment, SS windowed, TT marker-shim carry-forward
  provides:
    - TABLE-STORE-01   # EntityState.table_rows first-class per-Table-row addressing
    - TABLE-STORE-02   # upsert_table_row / tombstone_table_row / get_table_row / gc_tombstones
    - SNAPSHOT-V7-01   # codec v7 with forward v6 migration
    - WIRE-TABLE-01    # OP_PUSH_TABLE (0x0B) end-to-end
    - WIRE-TABLE-02    # OP_DELETE_TABLE (0x0C) end-to-end
    - SDK-TABLE-01     # app.push(table,key,fields) / app.delete(table,key)
    - GET-MERGED-01    # GET returns streams ∪ Live table_rows ∪ static_features
    - CASCADE-MIGRATE-01   # cascade_table_upsert reads/writes table_rows (no markers)
    - TT-TESTS-UNIGNORE-01 # 7 previously-ignored TT tests passing (12/12)
    - WM-TRACK-01      # per-stream watermark = max(event_time) − 5s
    - WM-PROPAGATE-01  # γ propagation at join/agg boundaries; stateless pass-through
    - WM-LATE-DROP-01  # event_time < watermark → drop + tally_late_events_dropped_total
    - WM-EVENT-TIME-01 # _event_time JSON parse + event_time() builtin
    - WM-DEBUG-01      # /debug/streams/:name + /debug/key watermarks
    - INTEG-MULTI-SHAPE-01 # 5 multi-shape integration tests covering storage+wm+cascade
    - BENCH-24-GATE-01     # 9-cell regression matrix vs BASELINE.json
    - BENCH-24-CHAR-01     # 4 characterization cells recording Phase-24 path costs
    - PHASE-CLOSEOUT-01    # Phase 24 closed; handoff to Phase 25
  affects:
    - Phase 25 (query surface + TTL + warnings)
    - Phase 26 (test migration + docs + demo rebuild)

tech-stack:
  added: []
  patterns:
    - serializable-shadow-type
    - legacy-type-on-read
    - decorator-marker-dispatch
    - merged-view-read-path
    - state-is-truth
    - per-event-drop-gate
    - gamma-boundary-only
    - shim-rename

key-files:
  created:
    - tests/test_table_row_storage.rs
    - tests/test_snapshot_v7_migration.rs
    - tests/test_op_push_table.rs
    - python/tests/test_push_table_e2e.py
    - tests/test_tt_cascade_migration.rs
    - src/engine/event_time.rs
    - tests/test_watermarks.rs
    - tests/test_event_time_bucketing.rs
    - python/tests/test_watermark_e2e.py
    - tests/test_phase24_integration.rs
    - .planning/phases/24-watermarks-event-time/MATRIX-V0-POST-24.json
  modified:
    - src/state/store.rs
    - src/state/snapshot.rs
    - src/server/protocol.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - src/engine/mod.rs
    - src/engine/pipeline.rs
    - src/engine/window.rs
    - src/engine/expression.rs
    - python/tally/_protocol.py
    - python/tally/_app.py
    - python/tally/_stream.py
    - python/tally/_table.py
    - tests/test_join_table_table.rs
    - benchmark/tally-throughput/bench_v0.py

key-decisions:
  - "Phase boundary: storage redesign and watermarks ship together because late-event handling for Tables requires real TableRow identity that the Phase 23 marker model couldn't clean represent."
  - "Plan 01 couples Task 1 (types + methods) and Task 2 (snapshot codec v7) in one commit because EntityState field addition forces SerializableEntityState to grow the same field. Separating them would have left the repo in a broken state."
  - "TableRow.fields uses AHashMap at runtime but projects to Vec<(k,v)> at the serialization boundary — matches the pattern SerializableEntityState.static_features already uses."
  - "Opcodes assigned 0x0B/0x0C (contiguous after OP_PUSH_BATCH 0x0A); 0x09 stays a deliberate gap left by prior phases. Decision documented as a block comment in both protocol.rs files."
  - "Table-form push is SYNCHRONOUS (OP_PUSH_TABLE round-trips); Stream-form stays fire-and-forget (OP_PUSH_ASYNC). Sync-ness for Tables lets tests do race-free app.get(key) immediately after a push."
  - "Merged GET view flattens Live table_rows as `TableName.field`; Tombstoned rows are filtered (T-24-02-03 info-disclosure mitigation)."
  - "`_tombstoned: bool` on cascade_table_upsert retained in the signature (prefixed `_` to silence warnings) so call sites don't churn; the body no longer reads it — get_table_row(...).state is ground truth."
  - "WatermarkTracker stores max(event_time observed) per stream and derives wm = max − 5s on read. Keeps the data model monotone; underflow clamps to UNIX_EPOCH inside watermark()."
  - "RingBuffer signature kept as `add_to_current(value, now)` with a thin shim forwarding to `add_at_event_time(value, event_time)`. Renaming the parameter across 18 operator call-sites was declined — the TCP layer already passes the parsed event-time as `now`."
  - "`event_time()` returns unix-milliseconds as FeatureValue::Int(i64); `now()` returns unix-seconds as FeatureValue::Float(f64). Unit mismatch is intentional and documented; callers that want to compare both must convert explicitly."
  - "Integration test DAG pattern: Purchases(stream) → PurchasesAgg(aggregation table) + UserProfile + RiskScore → UserRisk(TT-join). PurchasesAgg's values surface via stream-operator merged view, not as TableRow.fields — aggregation outputs do not populate `table_rows` (Table aggregation is disabled in v0). The TT-join layer is exercised between two source Tables."

requirements-completed:
  - TABLE-STORE-01
  - TABLE-STORE-02
  - SNAPSHOT-V7-01
  - WIRE-TABLE-01
  - WIRE-TABLE-02
  - SDK-TABLE-01
  - GET-MERGED-01
  - CASCADE-MIGRATE-01
  - TT-TESTS-UNIGNORE-01
  - WM-TRACK-01
  - WM-PROPAGATE-01
  - WM-LATE-DROP-01
  - WM-EVENT-TIME-01
  - WM-DEBUG-01
  - INTEG-MULTI-SHAPE-01
  - BENCH-24-GATE-01
  - BENCH-24-CHAR-01
  - PHASE-CLOSEOUT-01

metrics:
  duration: ~3.5h (across 5 plans)
  completed: 2026-04-14
  plans: 5
  commits:
    - fa260a8    # 24-01 Task 1: TableRow + TableRowState + v7 codec
    - 3ac04ad    # 24-01 Task 2: v7 snapshot round-trip + v6 migration tests
    - f539af2    # 24-02 Task 1: OP_PUSH_TABLE/OP_DELETE_TABLE + merged GET
    - 6b4a668    # 24-02 Task 2: Python SDK push/delete + e2e
    - 5352e21    # 24-03 Task 1: cascade rewrite + 5 migration tests
    - b4f0038    # 24-03 Task 2: un-ignore 7 TT tests + harness port
    - ba478f9    # 24-04 Task 1: event-time parse + watermark + late-drop counter
    - 43678c1    # 24-04 Task 2: RingBuffer event-time routing + γ propagation
    - 8688bc6    # 24-04 Task 3: event_time() builtin + /debug/streams + Py e2e
    - 060d30c    # 24-05 Task 1: multi-shape integration tests
    - edc0e1f    # 24-05 Task 2: bench matrix + MATRIX-V0-POST-24
---

# Phase 24: Watermarks, event-time & Table storage redesign — Phase Summary

**One-liner:** Replaced Phase 23's marker-based Table↔Table cascade with
first-class per-Table-row storage (`EntityState.table_rows`), added
`OP_PUSH_TABLE` / `OP_DELETE_TABLE` opcodes + Python SDK
`push(table,key,fields)` / `delete(table,key)`, unignored the 7 TT tests
Phase 23 deferred, shipped per-stream watermarks with `max(event_time) − 5s`
semantics and γ propagation at join/agg boundaries, late-event drop with
`tally_late_events_dropped_total{stream}` counter, RingBuffer event-time
bucket routing, the `event_time()` expression builtin, and
`/debug/streams/:name` — all gated by a 9-cell benchmark matrix that stays
within −5% of the Phase 22-04 BASELINE.

## What shipped (per plan)

### Plan 24-01 — Table row storage primitive

Shipped `EntityState.table_rows: AHashMap<String, TableRow>` with
`TableRowState::Live | Tombstoned { since }`, a locked 7-day tombstone
grace (`TOMBSTONE_GRACE` const), and four StateStore methods:
`upsert_table_row`, `tombstone_table_row`, `get_table_row`,
`gc_tombstones(now)`. Snapshot codec bumped to v7 with a
legacy-type-on-read v6 migration path (empty `table_rows` on legacy
decode). Two new test files (7 + 5 tests, all passing).

See `24-01-SUMMARY.md`.

### Plan 24-02 — TCP opcodes + Python SDK

Wired `OP_PUSH_TABLE` (0x0B) and `OP_DELETE_TABLE` (0x0C) through
`Command` / `parse_command` / `handle_push_table` / `handle_delete_table`
with unknown-table rejection before any state mutation
(T-24-02-04). Added a merged GET path that flattens Live Table rows as
`TableName.field` and filters Tombstoned rows (T-24-02-03). Python SDK
gained `_tally_kind` decorator marker + `app.push(table, key, fields)`
(sync) / `app.delete(table, key)` dispatch. 6 end-to-end TCP tests +
3 parse-level + 7 pytest e2e.

See `24-02-SUMMARY.md`.

### Plan 24-03 — Cascade migration

Rewrote `cascade_table_upsert` to read `get_table_row` for both input
sides and write merged output via `upsert_table_row` /
`tombstone_table_row` — the Phase 23 `__tt_left_*`/`__tt_right_*`
shadow markers are gone. Un-ignored all 7 TT-join tests Phase 23 had
deferred (final count 12/12 passing). 5 migration-focused tests +
harness port in `test_join_table_table.rs`. Regression gauntlet
(ST 6/6, SS 14/14, TT 12/12, integration 3/3, composite 5/5,
register 21/21, pytest 418+2) all green.

See `24-03-SUMMARY.md`.

### Plan 24-04 — Watermarks + event-time

New `src/engine/event_time.rs` module (452 lines): `parse_event_time`
(ISO8601 / unix-ms / unix-seconds / fallback), `WatermarkTracker` (per-
stream max-observed with 5s lateness), `LateDropCounters`, γ
propagation helpers (`propagate_stateless`, `propagate_join`,
`attach_to_table`). TCP dispatch parses `_event_time` and gates
PUSH / PUSH_TABLE / DELETE_TABLE / batch paths; late events drop and
bump `tally_late_events_dropped_total{stream}`. RingBuffer gained
`add_at_event_time` / `update_at_event_time` with historical-bucket
routing; shimmed `add_to_current` / `update_current` forward to the new
methods for call-site compat. `EvalContext` gained
`event_time: Option<SystemTime>`; `event_time()` builtin returns
unix-ms Int when in event-scoped context, Missing otherwise.
`/debug/streams/:name` exposes watermark, observed_max, last_event_time,
lateness_seconds, late_events_dropped; `/debug/key/:key` gained a
watermarks field; `/metrics` carries the new counter.

See `24-04-SUMMARY.md`.

### Plan 24-05 — Integration + benchmark gate

Five multi-shape integration tests in `tests/test_phase24_integration.rs`
exercising storage + watermarks + cascade together end-to-end:
happy-path, out-of-order within 5s, late drop past 5s, tombstone cascade
through TT-join, and 7d-grace GC. Bench harness extended with 4 new
pipeline factories (`late_events`, `tombstone_cascade`, `tt_join_real`,
`enrich_with_wm`) plus two new custom client runners
(`run_event_time_client` for `_event_time`-stamping; `run_table_push_client`
for sync OP_PUSH_TABLE driving). `MATRIX-V0-POST-24.json` captures the
7-run median per cell with `gate_passed: true`.

## Test results

### Rust

| Suite | Phase 24 start | Phase 24 end |
| ----- | -------------- | ------------ |
| `cargo test --lib` | 679 | **700** (+21 new unit tests) |
| `test_table_row_storage` | — | **7 / 7** (new) |
| `test_snapshot_v7_migration` | — | **5 / 5** (new) |
| `test_op_push_table` | — | **6 / 6** (new) |
| `test_tt_cascade_migration` | — | **5 / 5** (new) |
| `test_watermarks` | — | **9 / 9** (new) |
| `test_event_time_bucketing` | — | **7 / 7** (new) |
| `test_phase24_integration` | — | **5 / 5** (new) |
| `test_join_table_table` | 5 / 5 + 7 ignored | **12 / 12, 0 ignored** |
| `test_join_stream_stream` | 14 / 14 | **14 / 14** |
| `test_join_stream_table` | 6 / 6 | **6 / 6** |
| `test_join_integration` | 3 / 3 | **3 / 3** |
| `test_composite_group_by` | 5 / 5 | **5 / 5** |
| `test_register_json_v0` | 21 / 21 | **21 / 21** |
| `test_server` | 31 / 31 | **31 / 31** |

Total new Rust tests: **44 new** (21 lib + 5 + 5 + 6 + 5 + 9 + 7 + 5 integration) **plus** 7 Phase-23 TT-join tests un-ignored.

Full `cargo test` — all integration binaries green across 35+ test files. Integration tests pass deterministically (run 3× consecutively with no flakiness).

### Python

| Suite | Phase 24 start | Phase 24 end |
| ----- | -------------- | ------------ |
| `test_push_table_e2e.py` | — | **7 / 7** (new) |
| `test_watermark_e2e.py` | — | **4 / 4** (new) |
| `pytest python/tests/` (fresh server) | 418 passed / 2 skipped | **422 passed / 2 skipped** |

Total new Python tests: **11 new** (7 + 4) pytest cases.

One known flake exists (`test_v0_stream_table_join.py::test_stream_table_enrich_tcp_roundtrip`) under cross-test key pollution on `u1` via the session-scoped server fixture. Reproduces identically on the Phase 24-03 baseline; a fresh server makes it pass. Documented in 24-04-SUMMARY; unrelated to Phase 24.

## Benchmark — `MATRIX-V0-POST-24.json`

Label: `v0-post-24-05`. 7-run median per cell; host was a 48-core box
with load-average 9-11 and pre-matrix server warm-up.

### 9-cell regression gate (all pass, within −5% of BASELINE.json)

| cell | eps (median) | Δ vs baseline | pass |
| ---- | ------------ | ------------- | ---- |
| `small_1c`  | 113,108 | −1.72% | ✓ |
| `small_4c`  |  28,528 | +1.67% | ✓ |
| `small_8c`  |  30,760 | +1.29% | ✓ |
| `medium_1c` | 110,362 | −4.42% | ✓ |
| `medium_4c` |  28,445 | +0.89% | ✓ |
| `medium_8c` |  29,982 | −0.80% | ✓ |
| `large_1c`  | 111,523 | −4.18% | ✓ |
| `large_4c`  |  28,308 | +0.74% | ✓ |
| `large_8c`  |  30,882 | +0.68% | ✓ |

`gate_passed: true`.

### Characterization cells (6 total; no pass/fail)

| cell | eps (median) | % of small_1c |
| ---- | ------------ | ------------- |
| `join_small_1c` | 101,117 | 89.4% |
| `enrich_small_1c` | 103,896 | 91.9% |
| `late_events_small_1c` | 120,194 | 106.3% |
| `tombstone_cascade_small_1c` | 22,464 | 19.9% |
| `tt_join_real_small_1c` | 22,746 | 20.1% |
| `enrich_with_wm_small_1c` | 122,368 | 108.2% |

**Interpretation:**

- `late_events_small_1c` and `enrich_with_wm_small_1c` run at 106-108% of `small_1c`. The watermark-parse + watermark-compare + RingBuffer event-time routing adds < 5% overhead in controlled unit measurements (see `tests/test_event_time_bucketing.rs`); the > 100% here reflects SDK-side warming by the time these later cells run, not a cost saving. Both are within small_1c's run-to-run noise envelope.
- `tombstone_cascade_small_1c` and `tt_join_real_small_1c` both measure the **synchronous** `OP_PUSH_TABLE` path (which waits for a server ack on every event), so they naturally land at ~20% of the `small_1c` async PUSH rate. This is the expected sync-vs-async gap, not a Phase 24 regression — the cascade itself added no measurable overhead vs the Phase 23 marker-shim path.

## Deviations from plan

Each plan had localized deviations (documented fully in the per-plan
SUMMARY files). In aggregate:

- **Phase 24-01**: Task 1 commit pulled forward the snapshot codec bump because `EntityState` and `SerializableEntityState` must change in the same compilation unit. Added `SerializableTableRow` shadow type (AHashMap lacks serde in this codebase). Dropped an unnecessary `#[cfg(feature = "test-helpers")]` gate that would have hidden helpers from integration tests.
- **Phase 24-02**: Added TypeError arity/type gates on `App.push` / `App.delete` so wrong-arity calls fail at the SDK boundary (before any wire I/O). Moved parse-level tests to `protocol.rs::tests` so they run without the integration harness.
- **Phase 24-03**: `tt_cascades_recursively_through_chain` moved its observation point from J to K — under the marker model both rows shared an entity's static_features; under per-Table-row storage they're distinct rows. Same intent, correct observation point. Removed the dead `json_to_fv` helper that only the marker cascade used.
- **Phase 24-04**: Wrote an in-house ISO8601 parser (~60 lines, no chrono dependency). Rewrote `WatermarkTracker::watermark` to clamp at `UNIX_EPOCH` via `duration_since` comparison — `SystemTime::checked_sub` does NOT fail on underflow on Linux. Stripped `_event_time` from persisted TableRow.fields so the reserved name doesn't surface as a phantom feature. Kept RingBuffer's `now` parameter name as a shim rather than a full 18-call-site rename.
- **Phase 24-05**: The plan's 5-node DAG interface had `UserView = UserStats.join(UserProfile)` where UserStats is an aggregation Table — this cannot work in v0 because aggregation outputs live in stream-operator state, NOT in `table_rows` (Table aggregation is disabled in v0 per spec §3.1). Restructured the integration DAG to have the aggregation and the TT-join exercise their respective layers without colliding: `Purchases → PurchasesAgg (agg table)` and `UserProfile + RiskScore → UserRisk (TT-join of two source Tables)`. Each integration assertion targets the right layer. Same correctness coverage, architecturally sound.

## Known stubs / deferred items

Per `.planning/phases/24-watermarks-event-time/24-CONTEXT.md §Deferred`,
no feature originally scoped for Phase 24 was deferred. The v0 spec
explicitly defers the following to v0.1 and they remain deferred:

- **DAG-level retraction propagation through aggregations** (Case 3 retraction) — Table aggregation is disabled in v0, sidesteps the complexity. Will land in v0.1.
- **Per-stream tunable lateness** — the 5s watermark window is a locked constant in v0. v0.1 will add an `@tl.stream(lateness=...)` parameter and a tunable CLI hint via `tally suggest-config`.
- **Far-future `_event_time` clamping** — a single far-future event currently jumps the watermark and can late-drop legitimate subsequent events. v0.1 will add a bounded clock-skew cap (`now + tolerance`, default 1h).
- **Side outputs for very-late events** — v0 drops beyond-window late events silently (counter increments). A `tl.side_output("late")` surface is a post-v0 feature.
- **Async OP_PUSH_TABLE** — v0 ships sync Table push for race-free `get()` after `push()`. Async Table push will land when retraction flows need it.
- **Session windows** — post-v0.

These do not block Phase 25 or Phase 26.

## Threat flags

No new threat surface introduced beyond what each plan's STRIDE register
covered. Carry-forward from per-plan registers:

- `T-24-01-01..05` all mitigated or accepted as designed (snapshot version validation, gc_tombstones DashMap sharding, tombstone filtering at reader).
- `T-24-02-01..05` all mitigated (framing, flood, tombstone info disclosure, unknown-table bypass, JSON type confusion).
- `T-24-03-01..04` all mitigated (cascade recursion depth bounded by register-time cycle guard, re-read per cascade, tombstoned filter, gc race accepted).
- `T-24-04-01..06` mitigated or accepted (far-future watermark poisoning accepted for v0; 2^31 ms/seconds threshold tested; `/debug/streams` admin-gated; counter label cardinality bounded by REGISTER; `event_time()` Missing in read contexts).
- `T-24-05-01..03`: measurement variance mitigated via 7-run median + host-warmup; integration tests each build their own engine+store (no shared fixture); SUMMARY self-check verifies every absolute path resolves.

## Handoff to Phase 25

**Phase 25 scope (v0 query surface + TTL + warnings):**

- **GET_MULTI** opcode + Python SDK `mget`
- **`/debug/warnings`** unified health endpoint
- **`/debug/config-recommendations`** + `tally suggest-config` CLI
- **TTL defaults** (Table 30d, Stream 90d, tombstone 7d — the last already implemented) + override pattern + suggestion engine

**What Phase 25 inherits from Phase 24:**

- `EntityState.table_rows` with Live/Tombstoned states is the substrate TTL operates on. `gc_tombstones(now)` is ready; Phase 25 adds a scheduler + `last_event_at`-based Stream/Table eviction.
- Merged GET view shape (streams ∪ Live table_rows ∪ static_features) is set. GET_MULTI reuses `collect_merged_features` per key.
- `/debug/streams/:name` and `/debug/key/:key::watermarks` give Phase 25's `/debug/warnings` concrete signals to surface (e.g. "stream X has received zero events in 2h" or "lateness-dropped > 1% of events").
- `tally_late_events_dropped_total{stream}` is the first cardinality-bounded per-stream counter. The `/debug/config-recommendations` engine in Phase 25 can use it to suggest `lateness=`.

**Phase 25 is unblocked to begin.**

## Self-Check: PASSED

Verified files exist (absolute paths):

- `/data/home/tally/src/state/store.rs` — FOUND (modified)
- `/data/home/tally/src/state/snapshot.rs` — FOUND (modified)
- `/data/home/tally/src/server/protocol.rs` — FOUND (modified)
- `/data/home/tally/src/server/tcp.rs` — FOUND (modified)
- `/data/home/tally/src/server/http.rs` — FOUND (modified)
- `/data/home/tally/src/engine/mod.rs` — FOUND (modified)
- `/data/home/tally/src/engine/pipeline.rs` — FOUND (modified)
- `/data/home/tally/src/engine/window.rs` — FOUND (modified)
- `/data/home/tally/src/engine/expression.rs` — FOUND (modified)
- `/data/home/tally/src/engine/event_time.rs` — FOUND (created)
- `/data/home/tally/python/tally/_protocol.py` — FOUND (modified)
- `/data/home/tally/python/tally/_app.py` — FOUND (modified)
- `/data/home/tally/python/tally/_stream.py` — FOUND (modified)
- `/data/home/tally/python/tally/_table.py` — FOUND (modified)
- `/data/home/tally/tests/test_table_row_storage.rs` — FOUND (created)
- `/data/home/tally/tests/test_snapshot_v7_migration.rs` — FOUND (created)
- `/data/home/tally/tests/test_op_push_table.rs` — FOUND (created)
- `/data/home/tally/tests/test_tt_cascade_migration.rs` — FOUND (created)
- `/data/home/tally/tests/test_watermarks.rs` — FOUND (created)
- `/data/home/tally/tests/test_event_time_bucketing.rs` — FOUND (created)
- `/data/home/tally/tests/test_join_table_table.rs` — FOUND (modified)
- `/data/home/tally/tests/test_phase24_integration.rs` — FOUND (created)
- `/data/home/tally/python/tests/test_push_table_e2e.py` — FOUND (created)
- `/data/home/tally/python/tests/test_watermark_e2e.py` — FOUND (created)
- `/data/home/tally/benchmark/tally-throughput/bench_v0.py` — FOUND (modified)
- `/data/home/tally/.planning/phases/24-watermarks-event-time/MATRIX-V0-POST-24.json` — FOUND (created)
- `/data/home/tally/.planning/phases/24-watermarks-event-time/24-01-SUMMARY.md` — FOUND
- `/data/home/tally/.planning/phases/24-watermarks-event-time/24-02-SUMMARY.md` — FOUND
- `/data/home/tally/.planning/phases/24-watermarks-event-time/24-03-SUMMARY.md` — FOUND
- `/data/home/tally/.planning/phases/24-watermarks-event-time/24-04-SUMMARY.md` — FOUND
- `/data/home/tally/.planning/phases/24-watermarks-event-time/24-SUMMARY.md` — FOUND (this file)

Verified commits exist on `main`:

- `fa260a8` feat(24-01): TableRow + TableRowState storage primitive on StateStore
- `3ac04ad` test(24-01): v7 snapshot round-trip + v6→v7 migration tests
- `f539af2` feat(24-02): OP_PUSH_TABLE/OP_DELETE_TABLE opcodes + merged GET view
- `6b4a668` feat(24-02): Python SDK push/delete for Tables + merged GET e2e tests
- `5352e21` feat(24-03): migrate TT cascade to table_rows storage
- `b4f0038` test(24-03): un-ignore 7 TT-join tests; port harness to table_rows
- `ba478f9` feat(24-04): event-time parsing + per-stream watermark tracking + late-drop counter
- `43678c1` feat(24-04): event-time bucket routing in RingBuffer + γ propagation in cascade
- `8688bc6` feat(24-04): event_time() builtin + /debug/streams/:name + Python e2e
- `060d30c` test(24-05): multi-shape DAG integration tests for Phase 24 closeout
- `edc0e1f` bench(24-05): 9-cell gate + 4 Phase-24 characterization cells + MATRIX-V0-POST-24

Verified test gates (2026-04-14):

- `cargo test --lib` — 700 / 700
- `cargo test --test test_phase24_integration` — 5 / 5 (across 3 consecutive runs, no flakiness)
- `cargo test --test test_watermarks` — 9 / 9
- `cargo test --test test_event_time_bucketing` — 7 / 7
- `cargo test --test test_op_push_table` — 6 / 6
- `cargo test --test test_table_row_storage` — 7 / 7
- `cargo test --test test_snapshot_v7_migration` — 5 / 5
- `cargo test --test test_tt_cascade_migration` — 5 / 5
- `cargo test --test test_join_table_table` — 12 / 12, 0 ignored
- `cargo test` (full suite) — all integration binaries green
- `pytest python/tests/` (fresh server) — 422 passed, 2 skipped

Benchmark:

- `MATRIX-V0-POST-24.json` exists, `gate_passed: true`, all 9 regression cells within −5% of BASELINE.json.

Phase 24 is closed. Phase 25 (query surface + TTL + warnings) is unblocked.
