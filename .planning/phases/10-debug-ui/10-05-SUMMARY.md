---
phase: 10-debug-ui
plan: 05
subsystem: infra
tags: [integration-tests, raw-tcp, http, debug-ui, sha256, vendor-drift, nyquist]

requires:
  - phase: 10
    provides: Plan 10-01 vendored htmx/d3/dagre-d3 + VENDOR.md manifest
  - phase: 10
    provides: Plan 10-02 ThroughputTracker public API on AppState
  - phase: 10
    provides: Plan 10-03 /debug/topology, /debug/throughput, /debug/memory (extended), /, /static/{*file}
  - phase: 10
    provides: Plan 10-04 index.html, app.css, app.js, favicon.svg, icons.svg under src/server/ui/
provides:
  - tests/test_debug_ui.rs -- 15 #[tokio::test] integration tests covering DBUI-01..05
  - SHA256 drift tests for htmx.min.js, d3.min.js, dagre-d3.min.js
  - Restored compile-clean integration-test surface (tests/test_server.rs, tests/test_pipeline.rs, tests/test_snapshot.rs)
affects: []

tech-stack:
  added:
    - sha2 = "0.10"   # dev-dependency only; re-hashes vendored JS at test time
  patterns:
    - "Raw HTTP/1.1 over tokio::net::TcpStream for integration tests -- no reqwest, no hyper client"
    - "bind 127.0.0.1:0 for random port per test so tests are parallel-safe and never collide with dev servers on 6400/6401/6501"
    - "SHA256 drift check: re-hash embedded bytes at test time, parse the expected hex from VENDOR.md"
    - "Test name parity with 10-VALIDATION.md -- the verifier greps this file for exact test names"

key-files:
  created:
    - tests/test_debug_ui.rs
  modified:
    - Cargo.toml            # sha2 = "0.10" under [dev-dependencies]
    - Cargo.lock            # sha2 transitive deps pinned
    - tests/test_server.rs  # AppState literal grown from 5 -> 12 fields
    - tests/test_pipeline.rs # make_state_with_event_log gains throughput field
    - tests/test_snapshot.rs # FeatureDef::{Count,Sum} literals gain backfill: false; SnapshotState literals gain backfill_complete: vec![]

key-decisions:
  - "Raw TCP over tokio::net::TcpStream, NOT reqwest. Matches the existing tests/test_server.rs idiom and avoids pulling reqwest (+ hyper + native-tls) into the dev-dependency closure for a debug-only UI test file."
  - "Test name parity with VALIDATION.md is load-bearing -- the Phase 10 verifier greps the file for exact test names. Every row in the 10-VALIDATION.md table maps one-to-one to a function in tests/test_debug_ui.rs."
  - "register_test_pipeline builds directly against PipelineEngine::register / register_view rather than round-tripping through the TCP REGISTER opcode. Keeps the test setup concise and avoids spinning up a TCP listener we do not exercise."
  - "SHA256 drift tests re-hash on-disk bytes at test time (not commit time) so a future PR that edits the vendored JS without updating VENDOR.md fails cargo test loudly."
  - "throughput_decays_when_idle uses a 500 ms idle sleep (wider than the original 250 ms guidance in the plan) -- at tau_5s = 5.0 s the expected decay over 500 ms is ~10%, which is safely above the strict less-than assertion threshold without making the test slow. Not flaky on first run."
  - "push_event calls engine.push(...) directly and then calls app.throughput.bump_unique([name]) with a single-element slice. This mirrors the shape of handle_sync_command's Push arm well enough for the throughput EWMA to become non-zero after a burst of 5 pushes."
  - "Test-side stale-literal fix covers test_snapshot.rs as well as test_server.rs and test_pipeline.rs. Plan 10-02's SUMMARY called out test_pipeline/test_server AppState literals and the test_snapshot backfill_complete literal; in the actual code base test_snapshot.rs also needed `backfill: false` added to four FeatureDef::{Count,Sum} literals because Phase 8 added that field without touching the tests."

patterns-established:
  - "Pattern: integration tests spawn a fresh Tally HTTP server per #[tokio::test] on 127.0.0.1:0 and return (port, SharedState). The caller mutates SharedState directly for setup and issues raw HTTP/1.1 requests via the port."
  - "Pattern: vendored-asset drift detection = re-hash on disk + parse expected hex from VENDOR.md. The VENDOR.md file IS the source of truth; the .min.js files on disk are compared against it."
  - "Pattern: test-file struct-literal maintenance lives in the same plan as the test additions that depend on them. Plan 10-05 is the test-surface plan; it fixed every stale literal that accumulated under Plan 02's lib-only gate."

requirements-completed:
  - DBUI-01
  - DBUI-02
  - DBUI-03
  - DBUI-04
  - DBUI-05

duration: ~8min
completed: 2026-04-10
---

# Phase 10 Plan 05: Debug UI Integration Tests + Stale Literal Cleanup

**Close the Nyquist validation loop for Phase 10: a new `tests/test_debug_ui.rs`
file with 15 raw-TCP HTTP/1.1 integration tests, plus a cleanup pass over
three stale struct literals in other integration tests so the full workspace
`cargo test` is green end-to-end.**

## Performance

- **Duration:** ~8 min of executor wall time (3 tasks, zero deviations, zero
  compile-retry cycles)
- **Started:** 2026-04-10T13:10:00Z
- **Completed:** 2026-04-10
- **Tasks:** 3 (2 `type="auto"` + 1 `type="auto"` pure verification)
- **Files created:** 1 (`tests/test_debug_ui.rs`)
- **Files modified:** 5 (`Cargo.toml`, `Cargo.lock`, `tests/test_server.rs`,
  `tests/test_pipeline.rs`, `tests/test_snapshot.rs`)

## Accomplishments

- **`tests/test_debug_ui.rs`** — 744-line new integration test file with a
  raw-TCP HTTP/1.1 client (`http_get(port, path)`), a fresh-server helper
  (`start_debug_ui_server`) that binds `127.0.0.1:0`, a four-node test
  pipeline registrar (`register_test_pipeline`), a push helper that bumps
  the throughput tracker, and 15 `#[tokio::test(flavor = "current_thread")]`
  functions whose names match `10-VALIDATION.md` row-for-row.
- **SHA256 drift tests** — `static_htmx_is_vendored_and_hashed`,
  `static_d3_is_vendored_and_hashed`, and `static_dagre_is_vendored_and_hashed`
  each re-hash the served bytes with `sha2::Sha256` and compare against the
  64-char hex hash parsed from `src/server/ui/vendor/VENDOR.md`. Any
  byte-level tampering of the vendored files will fail
  `cargo test --test test_debug_ui` loudly.
- **Stale literal cleanup** — three other integration tests were left
  non-compiling by prior plans' lib-only `cargo check` gates. Task 1 brought
  them up to date with the current struct shapes:
    - `tests/test_server.rs` — `AppState` literal grown from 5 to 12 fields
      (`throughput`, `backfill_tracker`, `backfill_complete`,
      `snapshot_cycle`, `snapshot_seq`, `last_base_seq`, `previous_base_seq`)
    - `tests/test_pipeline.rs::make_state_with_event_log` — gains
      `throughput: tally::server::throughput::ThroughputTracker::new()`
    - `tests/test_snapshot.rs` — four `FeatureDef::{Count,Sum}` literals gain
      `backfill: false`; three `SnapshotState` literals gain
      `backfill_complete: vec![]`
- **`sha2 = "0.10"` added to `[dev-dependencies]`** — dev-only, does not
  affect the release binary closure.
- **Full workspace `cargo test` is green:**
  - `cargo test --lib` — 461/461 passing (Plan 10-02's throughput unit tests,
    Phase 6/7/8/9 unit tests, expression parser, operators, etc.)
  - `cargo test --test test_debug_ui` — 15/15 passing
  - `cargo test --test test_server` — 28/28 passing
  - `cargo test --test test_pipeline` — 23/23 passing
  - `cargo test --test test_snapshot` — 7/7 passing
  - `cargo test --test test_incremental_snapshot` — 6/6 passing
  - Total: **540 tests across the workspace, zero failures**

## Task Commits

1. **Task 1: Fix stale `AppState`/`SnapshotState`/`FeatureDef` literals in
   tests/test_server.rs, tests/test_pipeline.rs, tests/test_snapshot.rs** —
   `7a402de` (fix)
2. **Task 2: Create tests/test_debug_ui.rs with all 15 DBUI integration tests
   + sha2 dev-dep** — `274a6d0` (feat)
3. **Task 3: Full workspace `cargo test` verification** — no code changes;
   verified 540/540 passing across the workspace. No commit needed for a
   pure-verification task.

## Test Case Map (VALIDATION.md ↔ test_debug_ui.rs)

| Requirement | 10-VALIDATION.md row (verbatim) | `tests/test_debug_ui.rs` function |
|-------------|--------------------------------|-----------------------------------|
| DBUI-01 | `topology_endpoint_emits_nodes_and_edges` | `topology_endpoint_emits_nodes_and_edges` |
| DBUI-01 | `topology_includes_cascade_edges` | `topology_includes_cascade_edges` |
| DBUI-01 | `topology_includes_view_nodes` | `topology_includes_view_nodes` |
| DBUI-02 | `throughput_endpoint_emits_per_stream_ewma` | `throughput_endpoint_emits_per_stream_ewma` |
| DBUI-02 | `throughput_reflects_recent_pushes` | `throughput_reflects_recent_pushes` |
| DBUI-02 | `throughput_decays_when_idle` | `throughput_decays_when_idle` |
| DBUI-02 | `throughput::does_not_double_count_cascade` | (Plan 10-02, `src/server/throughput.rs` unit test) |
| DBUI-03 | `entity_lookup_reuses_existing_endpoint` | `entity_lookup_reuses_existing_endpoint` |
| DBUI-04 | `memory_endpoint_emits_per_stream_breakdown` | `memory_endpoint_emits_per_stream_breakdown` |
| DBUI-04 | `memory_endpoint_backward_compatible` | `memory_endpoint_backward_compatible` |
| DBUI-05 | `static_index_is_embedded` | `static_index_is_embedded` |
| DBUI-05 | `static_css_is_embedded` | `static_css_is_embedded` |
| DBUI-05 | `static_htmx_is_vendored_and_hashed` | `static_htmx_is_vendored_and_hashed` |
| DBUI-05 | `static_dagre_is_vendored_and_hashed` | `static_dagre_is_vendored_and_hashed` |
| DBUI-05 | `static_d3_is_vendored_and_hashed` | `static_d3_is_vendored_and_hashed` |
| DBUI-05 | `static_unknown_returns_404` | `static_unknown_returns_404` |
| DBUI-05 | `release_build_embeds_assets` | deferred to manual browser smoke test |

Row 17 (`release_build_embeds_assets`) is explicitly manual-only per
10-VALIDATION.md §Manual-Only Verifications.

## Phase-Scoped Suite Command

```bash
cargo test --test test_debug_ui
```

Expected output: `test result: ok. 15 passed; 0 failed; 0 ignored;`

Full workspace suite:

```bash
cargo test
```

Expected output (per binary): `test result: ok. N passed; 0 failed` with zero
`FAILED` lines anywhere.

## Key Decisions

- **Raw TCP over `tokio::net::TcpStream`, NOT `reqwest`.** Matches the
  existing `tests/test_server.rs` idiom and avoids pulling `reqwest` +
  `hyper` + `native-tls` into the dev-dep closure for a debug-only UI test
  file. The `http_get(port, path)` helper hand-builds `GET {path} HTTP/1.1`
  requests, reads to EOF after `Connection: close`, splits on `\r\n\r\n`,
  and parses a status code + lowercased header map. Total helper is ~30
  lines.
- **`bind 127.0.0.1:0` per test.** Every `#[tokio::test]` calls
  `start_debug_ui_server()` which binds to a fresh random loopback port and
  spawns a new tokio task. No shared state across tests, no collision with
  pre-existing dev servers on 6400/6401/6501. `tokio::test` flavor
  `current_thread` is used so the runtime shuts down cleanly at the end of
  each test.
- **`register_test_pipeline` builds directly against the engine** rather
  than round-tripping through the TCP `REGISTER` opcode. Three
  `StreamDefinition`s (Transactions / Logins / Aggregates with `depends_on =
  ["Transactions"]` for the cascade edge) plus one `ViewDefinition`
  (UserRisk with a `Lookup` feature for the lookup edge) are registered by
  acquiring the state mutex and calling `engine.register(...)` +
  `engine.register_view(...)` directly.
- **`push_event` bumps the throughput tracker.** After calling
  `engine.push(stream, event, store, now_ts)`, the helper also calls
  `app.throughput.bump_unique([stream_name])` with a single-element slice.
  This mirrors the relevant portion of `handle_sync_command`'s Push arm so
  `/debug/throughput` observes the pushes during tests.
- **`throughput_decays_when_idle` uses a 500 ms idle sleep** rather than
  the 250 ms in the plan's initial draft. At `tau_5s = 5.0 s` the expected
  decay over 500 ms is `1 - exp(-0.5/5.0) ≈ 9.5%`, which is safely above
  the strict less-than threshold the test asserts. Not flaky on first run
  (passed clean with zero retries).
- **Task-1 scope grew by one file** — in addition to the plan-specified
  `tests/test_server.rs` and `tests/test_pipeline.rs`, `tests/test_snapshot.rs`
  also had stale `FeatureDef::{Count,Sum}` literals missing `backfill: false`
  (Phase 8 added that field) and stale `SnapshotState` literals missing
  `backfill_complete: vec![]`. All three were folded into the same Task 1
  commit because they are a single compile unit — skipping them would have
  left `cargo test --tests` broken for Task 3.

## Deviations from Plan

None — plan executed exactly as written, with a minor scope absorption in
Task 1 for `tests/test_snapshot.rs`. Plan 10-02's SUMMARY already flagged the
`test_snapshot` stale-literal problem as Plan 10-05's responsibility, so
folding it into Task 1 is in-scope per that plan's deferred-work note. The
fix landed in the same commit as the `test_server.rs` / `test_pipeline.rs`
fixes because all three files are required to compile together for
`cargo test --tests` to run.

The `throughput_decays_when_idle` test was initially drafted to use a 250 ms
sleep per the plan comment, but the final test uses 500 ms for a comfortable
decay margin. Both values would pass; 500 ms gives more signal if the EWMA
formula ever regresses. Documented in the test body comment.

## Issues Encountered

None. Every step worked on the first run:

- `cargo check --tests` was clean after Task 1 (the stale-literal fixes).
- `cargo test --test test_debug_ui` reported 15/15 passing on the very first
  invocation after Task 2, including the timing-sensitive
  `throughput_decays_when_idle` test.
- `cargo test` (full workspace) reported 540/540 passing across all test
  binaries with zero `FAILED` lines.

## User Setup Required

None. No new environment variables, no external services, no config changes.
`sha2 = "0.10"` is pulled in automatically by `cargo` when tests are built.

## Validation Sign-Off Checklist

Phase 10 — 10-VALIDATION.md Sign-Off state:

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
  (`tests/test_debug_ui.rs`, `VENDOR.md`, `src/server/throughput.rs` unit tests)
- [x] No watch-mode flags
- [x] Feedback latency < 30 s (phase-scoped `cargo test --test test_debug_ui`
      runs in ~0.55 s after compile cache is warm)
- [x] `nyquist_compliant: true` can be set in 10-VALIDATION.md frontmatter by
      the verifier pass

**Approval:** ready for `/gsd-verify-work` sign-off.

## Next Phase Readiness

- **`/gsd-verify-work 10`** — Plan 10-05 completes Phase 10's Wave 4. The
  verifier should grep `tests/test_debug_ui.rs` for each row name in
  `10-VALIDATION.md` and confirm `cargo test` is green.
- **Phase 10.1 Latency Debugger** — the scope addition from 2026-04-10
  (STATE.md Pending Todos) is queued for insertion AFTER Phase 10 verification
  passes and BEFORE the v1.1 milestone lifecycle. Plan 10-05 has no bearing
  on 10.1's scope; they are orthogonal.
- **Phase 10.2 Interactive DAG Drill-In** — the scope addition from Plan 10-04
  is also queued as a separate insertion; orthogonal to Plan 10-05.
- **v1.1 milestone** — after Phase 10 (and 10.1 / 10.2) pass verification,
  the milestone lifecycle (`audit → complete → cleanup`) runs on the
  Composable Pipeline & Event Log target.

## Self-Check: PASSED

Files:
- `tests/test_debug_ui.rs` — FOUND (`async fn start_debug_ui_server`,
  `async fn http_get`, `fn sha256_hex`, `fn expected_hash_for`,
  15 `#[tokio::test(flavor = "current_thread")]` functions with the exact
  names listed in the Test Case Map above).
- `tests/test_server.rs` — FOUND
  (`throughput: tally::server::throughput::ThroughputTracker::new()` and
  `backfill_tracker: Arc::new(BackfillTracker::default())` present;
  `use tally::server::tcp::{AppState, BackfillTracker, Metrics, SharedState};`
  import present).
- `tests/test_pipeline.rs::make_state_with_event_log` — FOUND
  (`throughput: tally::server::throughput::ThroughputTracker::new()`
  present).
- `tests/test_snapshot.rs` — FOUND (`backfill: false,` present in all four
  `FeatureDef::{Count,Sum}` literals; `backfill_complete: vec![],` present
  in all three `SnapshotState` literals).
- `Cargo.toml` — FOUND (`sha2 = "0.10"` under `[dev-dependencies]`).
- `src/server/ui/vendor/VENDOR.md` — FOUND (SHA256 manifest entries for
  htmx.min.js, d3.min.js, dagre-d3.min.js).

Commits:
- `7a402de` — FOUND in `git log --oneline` (Task 1, fix stale literals).
- `274a6d0` — FOUND in `git log --oneline` (Task 2, add test_debug_ui.rs).

Gates:
- `cargo check --tests` — exits 0 with zero errors and zero warnings.
- `cargo test --test test_debug_ui` — 15 of 15 tests pass.
- `cargo test --test test_server` — 28 of 28 tests pass.
- `cargo test --test test_pipeline` — 23 of 23 tests pass.
- `cargo test --test test_snapshot` — 7 of 7 tests pass.
- `cargo test --test test_incremental_snapshot` — 6 of 6 tests pass.
- `cargo test --lib` — 461 of 461 tests pass.
- Full workspace: 540 of 540 tests pass, zero `FAILED` lines.

---

*Phase: 10-debug-ui*
*Plan: 05*
*Completed: 2026-04-10*
