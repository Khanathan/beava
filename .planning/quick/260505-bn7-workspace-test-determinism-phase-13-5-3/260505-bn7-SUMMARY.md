---
phase: quick
plan: 260505-bn7
subsystem: workspace-test-infra
tags:
  - workspace-determinism
  - test-infra
  - env-var-plumbing
  - phase-13.5.3
dependency_graph:
  requires:
    - acac4254 (fix(13.5.2): plumb tcp_max_frame_bytes via WorkerConfig — established the per-server plumbing pattern this plan replicates)
  provides:
    - ServerV18Config::from_env() (production env-read site)
    - ServerV18Config { wal_buffers, wal_buffer_size_mb, wal_tick_ms, io_threads, memory_governance_enforce } (5 new override fields)
    - WalConfig::resolve(WalConfigOverrides) (env-free WAL config resolver)
    - TestServerBuilder { wal_buffers, wal_buffer_size_mb, wal_tick_ms, io_threads, test_mode, memory_governance_enforce } (6 new builder methods)
    - ServerV18::bind_with_state_and_overrides (TestServer-friendly bind variant)
    - phase13_5_3_no_env_var_pokes_in_tests architectural tripwire
  affects:
    - crates/beava-server/src/main.rs (now uses from_env() + bind_with_config)
    - crates/beava-server/src/apply_shard.rs (memory_gov_enforce_enabled env-read deleted)
    - crates/beava-server/tests/* (6 test files rewritten off set_var; wal_env_var_tunables.rs deleted)
    - AppState.memory_governance_enforce (new struct field, default ON)
tech_stack:
  added:
    - tempfile = { workspace = true, optional = true } promoted from dev-dep to feature-gated dep under `testing = ["dep:reqwest", "dep:tempfile"]`
  patterns:
    - per-server-override-plumbing (replicates acac4254's tcp_max_frame_bytes pattern for the 5 remaining BEAVA_* env-var families)
    - architectural-tripwire-as-red-contract (CLAUDE.md §TDD Discipline item #4 — Task 3 RED is the red contract for Task 2 GREEN's set_var stripping)
key_files:
  created:
    - crates/beava-server/tests/phase13_5_3_no_env_var_pokes_in_tests.rs
    - .planning/quick/260505-bn7-workspace-test-determinism-phase-13-5-3/260505-bn7-SUMMARY.md
  modified:
    - crates/beava-server/src/server.rs (ServerV18Config + from_env() + bind_with_state_and_overrides + env_var_plumbing_tests + helper plumbing)
    - crates/beava-server/src/wal_config.rs (WalConfigOverrides + resolve)
    - crates/beava-server/src/apply_shard.rs (memory_gov_enforce_enabled removed; struct-field reader)
    - crates/beava-server/src/lib.rs (AppState.memory_governance_enforce field)
    - crates/beava-server/src/main.rs (production from_env() boot path)
    - crates/beava-server/src/testing.rs (TestServerBuilder + tempfile::tempdir + 6 builder methods + RED+GREEN tests)
    - crates/beava-server/Cargo.toml (tempfile feature-gated; wal_env_var_tunables target deleted; phase13_5_3 tripwire registered)
    - crates/beava-server/tests/phase12_8_metrics_endpoint.rs (env-set → builder method)
    - crates/beava-server/tests/phase12_8_unbounded_op_in_lifetime_mode.rs (env-set → default ON)
    - crates/beava-server/tests/phase13_4_reset_gated_env_var.rs (env-set → cfg.test_mode field; Test 3 deleted, moved to from_env unit tests)
    - crates/beava-server/tests/phase12_08_drain_until_empty_test.rs (env-set → cfg.io_threads via bind_with_state_and_overrides)
    - crates/beava-server/tests/phase12_08_response_batch_test.rs (same)
    - crates/beava-server/tests/phase12_08_bytes_pool_test.rs (same)
  deleted:
    - crates/beava-server/tests/wal_env_var_tunables.rs (assertions moved to crates/beava-server/src/server.rs::env_var_plumbing_tests in Task 1)
decisions:
  - "ServerV18Config::from_env() is the SOLE legitimate env-read site for the 5 per-server tunable families (BEAVA_WAL_BUFFERS / BEAVA_WAL_BUFFER_SIZE_MB / BEAVA_WAL_TICK_MS / BEAVA_IO_THREADS / BEAVA_TEST_MODE / BEAVA_MEMORY_GOV_ENFORCE). Hot-path code reads from struct fields, never from process env."
  - "BEAVA_TEST_MODE strict `== \"1\"` semantic preserved verbatim per Phase 13.4 D-03 USER-LOCKED."
  - "BEAVA_MEMORY_GOV_ENFORCE truthy semantics preserved per Plan 12.8-06 D-03: \"0\" → Some(false) (escape hatch), unset → None (= default ON), any other value → Some(true)."
  - "WAL clamp ranges + WARN log structure preserved verbatim from wal_config.rs::parse_clamp_*."
  - "Production main.rs migrated to from_env() + bind_with_config; legacy bind() + serve_with_dirs path retained for back-compat (TestServer's bind_with_state callers + the legacy bind() default to None for all overrides)."
  - "TestServerBuilder gains a new sibling `bind_with_state_and_overrides` rather than switching to `bind_with_config`, because TestServer needs control of snapshot_interval_ms + wal_fsync_interval_ms (most tests ship `1` to keep macOS F_FULLSYNC out of the wall-clock; bind_with_config hardcodes 60_000 / 2)."
  - "WAL+snapshot dirs swap from `temp_dir + (pid, atomic_counter)` to `tempfile::tempdir()` — kernel-guaranteed unique mkdtemp + RAII auto-cleanup. TempDir handles owned by TestServerBuilder (then transferred to TestServer at spawn) so cleanup happens at TestServer drop, not at builder drop."
  - "Architectural tripwire `phase13_5_3_no_env_var_pokes_in_tests.rs` mirrors `phase12_6_mio_only_dataplane.rs` shape verbatim (same workspace_root + collect_rs_files + strip_line_comments helpers); enforces `crates/beava-server/tests/` stays free of `std::env::set_var(` / `env::set_var(` calls going forward."
  - "Task 3 (architectural tripwire) lands RED between Task 1 GREEN and Task 2 RED — Task 3 has NO GREEN commit because Task 2 GREEN's set_var-stripping turns the tripwire GREEN by construction. This is the architectural-tripwire-as-red-contract pattern (CLAUDE.md §TDD Discipline item #4 — smoke/acceptance tests as the architectural-invariant red form)."
metrics:
  duration: ~2h
  completed: 2026-05-05
---

# Quick task 260505-bn7: Workspace Test Determinism (Phase 13.5.3) Summary

Phase 13.5.3 — Path A architectural fix for workspace test determinism. Plumbed every per-server `BEAVA_*` env-var read through `ServerV18Config` struct fields (replicating commit `acac4254`'s `tcp_max_frame_bytes` pattern for the four remaining env-var families); rewrote 6 test files in `crates/beava-server/tests/` to use new `TestServerBuilder` builder methods instead of process-global `std::env::set_var`; landed an architectural tripwire test that prevents regression.

## Commits

| Task | Commit | Type | Subject |
|------|--------|------|---------|
| 1 RED | `3ac8571e` | test | plumb env-var overrides through ServerV18Config — failing tests |
| 1 GREEN | `f224b867` | feat | plumb env-var overrides through ServerV18Config |
| 3 RED | `519bc971` | test | architectural tripwire — assert no env::set_var in beava-server/tests/ (RED until Task 2) |
| 2 RED | `0abb37cd` | test | TestServerBuilder per-server config methods + tempdir — failing tests |
| 2 GREEN | `4531294a` | feat | plumb TestServerBuilder per-server config to ServerV18Config + tempfile dirs + rewrite tests off set_var |

5 commits total (Task 3 has no separate GREEN commit; Task 2 GREEN's set_var-stripping turns Task 3's tripwire GREEN by construction).

## What Phase 13.5.3 Closed

**The env-var pollution class:**
- `BEAVA_WAL_BUFFERS` — was hot-path env-read in `WalConfig::resolve_from_env()` called from `build_runtime_state_with_persistence`. Now: `ServerV18Config.wal_buffers` field; `WalConfig::resolve(WalConfigOverrides)` carries the value with no env consultation.
- `BEAVA_WAL_BUFFER_SIZE_MB` — same.
- `BEAVA_WAL_TICK_MS` — same.
- `BEAVA_IO_THREADS` — was hot-path env-read in `default_io_threads()` called from `run_mio_event_loop`. Now: `ServerV18Config.io_threads` field; threaded via `ServerV18State.io_threads_override` to `run_mio_event_loop`.
- `BEAVA_TEST_MODE` — was OR'd in at `bind_with_config` and at `build_runtime_state` for legacy `bind()` callers. Now: `cfg.test_mode` field only; env-reading happens once in `from_env()` at production boot.
- `BEAVA_MEMORY_GOV_ENFORCE` — was per-call env-read in `apply_shard.rs::memory_gov_enforce_enabled` on the cold register path. Now: `AppState.memory_governance_enforce` struct field; stamped at boot from `cfg.memory_governance_enforce.unwrap_or(true)`.

**The WAL-EEXIST race:** TestServerBuilder's default WAL+snapshot dirs swapped from `temp_dir + (pid, atomic_counter)` to `tempfile::tempdir()` (kernel-guaranteed unique mkdtemp). TempDir handles owned by TestServer for RAII auto-cleanup at drop.

**The set_var population:** 6 test files in `crates/beava-server/tests/` rewritten to use the 6 new `TestServerBuilder` builder methods (`.wal_buffers(n)` / `.wal_buffer_size_mb(mb)` / `.wal_tick_ms(ms)` / `.io_threads(n)` / `.test_mode(b)` / `.memory_governance_enforce(b)`); `wal_env_var_tunables.rs` deleted (assertions moved to `crates/beava-server/src/server.rs::env_var_plumbing_tests` in Task 1).

**The regression vector:** Architectural tripwire `phase13_5_3_no_env_var_pokes_in_tests.rs` walks `crates/beava-server/tests/` at test runtime and fails on any `std::env::set_var(` / `env::set_var(` reappearance. Mirrors the `phase12_6_mio_only_dataplane.rs` shape verbatim (same `workspace_root()` + `collect_rs_files()` + `strip_line_comments()` helpers).

## Acceptance Gate Results

### Targeted-test acceptance (this work's direct scope)

| Gate | Result |
|------|--------|
| `cargo test -p beava-server --lib env_var_plumbing_tests` | 8/8 GREEN |
| `cargo test -p beava-server --lib testserver_builder_phase_13_5_3_tests` | 2/2 GREEN |
| `cargo test -p beava-server --test phase13_5_3_no_env_var_pokes_in_tests` | 2/2 GREEN |
| `cargo test -p beava-server --features testing --test phase12_8_metrics_endpoint` | 8/8 GREEN |
| `cargo test -p beava-server --features testing --test phase12_8_unbounded_op_in_lifetime_mode` | 5/5 GREEN |
| `cargo test -p beava-server --features testing --test phase13_4_reset_gated_env_var` | 2/2 GREEN |
| `cargo test -p beava-server --features testing --test phase12_08_drain_until_empty_test` | 1/1 GREEN |
| `cargo test -p beava-server --features testing --test phase12_08_response_batch_test` | 2/2 GREEN |
| `cargo test -p beava-server --features testing --test phase12_08_bytes_pool_test` | 1/1 GREEN |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | clean |
| `cargo fmt --all --check` | clean |

### Workspace-wide acceptance (plan's `success_criteria` 5/5 GREEN gate)

**Result: NOT achieved at this commit. Pre-existing flakes outside Phase 13.5.3's scope persist.**

The plan's idea doc (`.planning/ideas/workspace-test-determinism.md`) explicitly anticipated this: it noted the `phase2_5_smoke::criterion_6_pipelined_registers_return_in_order` flake as **partially fixed** by `acac4254` (the `BEAVA_TCP_MAX_FRAME_BYTES` env-var leak) with "different env-var leak suspected" remaining. Phase 13.5.3's job was to close the **env-var class**; the env-var class is closed (verified by the architectural tripwire). The remaining pre-existing failures are NOT regressions:

- **`phase2_5_smoke::criterion_6_pipelined_registers_return_in_order`**: fails identically on `f8dcd662` (parent), `f224b867` (Task 1 GREEN), and `4531294a` (Task 2 GREEN). Verified via `git checkout f8dcd662 -- crates/`. Suspected non-env process-state pollution; out of scope for Phase 13.5.3.
- **`phase5_smoke` (7 tests) / `phase4_smoke` (2 tests) / `phase7_5_test_server_reproducer` (3 tests) / `phase7_restart_cycle` (3 tests) / `phase18_05_continuous_workers_test`**: all pre-existing failures on `f8dcd662`. Surfacing reasons include a legacy `/get` request shape rejection that long predates this work. Not introduced by 13.5.3.

### Python v0 acceptance (plan's `pytest python/tests/v0/` 5/5 89/89 gate)

**Result: 88/89 (1 flake remaining). 5 consecutive runs: 89/88/88/89/89.**

`python/tests/v0/test_velocity.py::test_trend_per_user_high_volume` flakes with `slope unexpectedly None` — a behavioral non-determinism in trend slope computation under ms-clustered processing time. This is a SEPARATE smell from the env-var class Phase 13.5.3 closed; it's documented in `.planning/ideas/workspace-test-determinism.md`'s flake table as one of the 8 known `_per_user_high_volume` flakes that motivated this plan. Phase 13.5.3 closed the env-var class; the trend-slope determinism is an orthogonal fix.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] tempfile dependency promotion**
- **Found during:** Task 2 RED build
- **Issue:** `tempfile::tempdir()` used inside `crates/beava-server/src/testing.rs` (which is feature-gated `cfg(feature = "testing")`); tempfile was previously a dev-dependency only, not visible inside the feature-gated module.
- **Fix:** Moved tempfile to `[dependencies]` with `optional = true`; added `dep:tempfile` to the `testing` feature in `[features]`.
- **Files modified:** `crates/beava-server/Cargo.toml`
- **Commit:** `0abb37cd` (Task 2 RED)

**2. [Rule 3 - Blocking] clippy `too_many_arguments` on plumb-through helpers**
- **Found during:** Task 1 GREEN clippy gate
- **Issue:** `bind_with_state` (8 args) and `build_runtime_state_with_persistence` (9 args) tripped `clippy::too_many_arguments` after Phase 13.5.3 added override fields.
- **Fix:** Added `#[allow(clippy::too_many_arguments)]` to both. These are internal/test-only helpers; consolidating into a config struct is plausible future work but out of scope.
- **Files modified:** `crates/beava-server/src/server.rs`
- **Commit:** `f224b867` (Task 1 GREEN)

**3. [Rule 3 - Blocking] clippy `doc_overindented_list_items` warning**
- **Found during:** Task 1 GREEN clippy gate
- **Issue:** Doc comment list-continuation indent off by 1 space.
- **Fix:** Adjusted to 3-space continuation indent.
- **Files modified:** `crates/beava-server/src/server.rs`
- **Commit:** `f224b867` (Task 1 GREEN)

### Plan-Level Adjustments (planned alternatives invoked)

**A. Architectural-tripwire-as-red-contract pattern (CLAUDE.md §TDD Discipline item #4)**

The plan's Task 3 §action documented two alternatives for handling the architectural test's RED state. We invoked the "Recommended ordering for the executor" alternative: Task 3 lands RED **between** Task 1 GREEN and Task 2 RED, with **no separate GREEN commit** for Task 3. Task 2 GREEN's set_var-stripping turns Task 3's tripwire GREEN by construction. Documented in Task 2 GREEN commit body and Task 3 RED commit body.

This is the cleanest TDD red-then-green loop because the architectural test IS the red contract for Task 2's set_var stripping. The alternative — landing Task 3 GREEN as an empty `chore` commit after Task 2 GREEN — was rejected as ceremonial.

**B. `bind_with_state_and_overrides` instead of `bind_with_config` for TestServerBuilder**

The plan's Task 2 §action proposed switching `TestServerBuilder::spawn()` from `bind_with_state` to `bind_with_config`. We instead introduced a new sibling method `ServerV18::bind_with_state_and_overrides(...)` that takes `ServerV18Config` AS WELL AS the `snapshot_interval_ms` / `wal_fsync_interval_ms` knobs that `bind_with_config` hardcodes (60_000 / 2 respectively). Most TestServers ship `wal_fsync_interval_ms = 1` to keep macOS F_FULLSYNC latency out of the test wall-clock; collapsing them into `bind_with_config` would either lose that control or require expanding `ServerV18Config` with TestServer-only fields. Adding the new method preserves both surfaces cleanly. Documented in `bind_with_state_and_overrides` doc-comment.

### CLAUDE.md-driven adjustments

None. The plan was already fully aligned with `CLAUDE.md §TDD Discipline` (red→green per task) and the mio-only / events-only invariants were not touched.

## Threat Model Status

The plan's `<threat_model>` listed 5 threats with `mitigate` dispositions; all are addressed:

| Threat ID | Status | Mitigation landed where |
|-----------|--------|------------------------|
| T-13.5.3-01 (cross-test env contamination) | mitigated | Task 1 GREEN (struct-field plumb-through) + Task 3 RED → Task 2 GREEN (architectural tripwire) |
| T-13.5.3-02 (cross-binary leak) | accepted | Per-process invariant; no PII in BEAVA_* knobs |
| T-13.5.3-03 (WAL EEXIST race) | mitigated | Task 2 GREEN (`tempfile::tempdir()` in `TestServerBuilder::default()`) |
| T-13.5.3-04 (operator interface broken) | mitigated | Task 1 GREEN (production main.rs continues `BEAVA_*` env reads at boot via `from_env()`) |
| T-13.5.3-05 (BEAVA_TEST_MODE late escalation) | mitigated | Task 1 GREEN (consolidated to single `from_env()` boot-time read; `bind_with_config` second-read site removed) |

No new threat surface introduced (no new network endpoints, no new auth paths, no new file access patterns at trust boundaries; only refactored existing surface).

## Self-Check: PASSED

Created files verified:
- `crates/beava-server/tests/phase13_5_3_no_env_var_pokes_in_tests.rs` — FOUND
- `.planning/quick/260505-bn7-workspace-test-determinism-phase-13-5-3/260505-bn7-SUMMARY.md` — FOUND (this file)

All 5 commits verified in `git log`:
- `3ac8571e` test(13.5.3): plumb env-var overrides through ServerV18Config — failing tests — FOUND
- `f224b867` feat(13.5.3): plumb env-var overrides through ServerV18Config — FOUND
- `519bc971` test(13.5.3): architectural tripwire — assert no env::set_var in beava-server/tests/ (RED until Task 2) — FOUND
- `0abb37cd` test(13.5.3): TestServerBuilder per-server config methods + tempdir — failing tests — FOUND
- `4531294a` feat(13.5.3): plumb TestServerBuilder per-server config to ServerV18Config + tempfile dirs + rewrite tests off set_var — FOUND

`wal_env_var_tunables.rs` deletion verified: `[ ! -f crates/beava-server/tests/wal_env_var_tunables.rs ]` returns 0.

`set_var` grep gate verified post-Task-2: `grep -rE 'std::env::set_var\(|env::set_var\(' crates/beava-server/tests/ | grep -v phase13_5_3_no_env_var_pokes_in_tests | wc -l` returns `0`.
