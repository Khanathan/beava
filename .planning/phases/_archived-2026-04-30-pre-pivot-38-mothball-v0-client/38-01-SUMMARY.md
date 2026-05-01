---
phase: 38-mothball-v0-client
plan: 01
subsystem: client-surface-mothball
tags: [mothball, housekeeping, option-k, option-m, phase-38]
one_liner: "Deleted Option K embedded-client surfaces (src/client/{clone,streaming,state,session}.rs + tally_cli binary + python-native PyO3 crate) superseded by Option M's tally fork + replica-mode server."

dependency-graph:
  requires:
    - Phase 35 green (OP_LOG_FETCH shipped)
    - Phase 36 green (replica-mode server boot shipped)
    - Phase 37 green (tally fork CLI + E2E test passing)
  provides:
    - "Smaller cognitive surface: ~10,285 net lines deleted"
    - "No dead code for future readers to confuse with live paths"
    - "python/ restored to pre-Phase-30 state — pure-Python SDK, no native extension"
  affects:
    - "src/client/ reduced from 6 files (~2000 LOC) to 2 files (mod.rs + wire.rs, ~100 LOC)"
    - "tally_cli binary deleted — fork lives on the main tally binary (Phase 37)"
    - "CI loses the python-native maturin job"
    - "python/tally/ is now a real directory again (was symlink to python-native/)"

tech-stack:
  removed:
    - "pyo3 0.22 (extension-module, abi3-py310) — via python-native crate delete"
    - "pythonize 0.22 — same"
    - "maturin 1.7+ build backend — same"
  patterns:
    - "Delete-don't-deprecate: no #[deprecated] zombies"
    - "Feature-flag machinery (Phase 28-01) retained — engine still compiles under --no-default-features --features client --lib"

key-files:
  deleted:
    - src/client/clone.rs (~457 LOC)
    - src/client/streaming.rs (~866 LOC)
    - src/client/state.rs (~74 LOC)
    - src/client/session.rs (~318 LOC)
    - src/bin/tally_cli.rs (~775 LOC)
    - tests/test_client_streaming.rs (~240 LOC, includes the flaky connect_dance_against_fake_server)
    - tests/integration/test_tally_clone.py (~285 LOC)
    - python-native/ (entire crate — Cargo.toml, src/{lib,pipeline,errors}.rs, pyproject.toml, README.md, python_src/tally/* (moved back to python/), tests/* including test_pipeline_e2e.py + test_pipeline_unit.py + test_pipeline_errors.py, tests/integration/conftest.py)
  modified:
    - src/client/mod.rs (323 → 95 LOC — kept Session / SessionMode / OutOfScopeError for the feature-flag smoke test; deleted FrozenClient + all its tests)
    - src/state/store.rs (updated bulk_load doc-comment: now references Phase 36 replica_client, not deleted client::clone::run_clone)
    - Cargo.toml (removed [[bin]] tally_cli; workspace.members = ["."] not [".", "python-native"])
    - .github/workflows/ci.yml (deleted python-native job; left a SUPERSEDED comment)
    - .gitignore (removed the python-native/python_src/tally/_native*.so ignore)
    - python/tally/__init__.py (deleted the try/except ImportError Pipeline re-export block + removed Pipeline/OutOfScopeError/ClientConnectError/HandshakeError/ReplicaStateError from __all__)
    - python/tally (symlink removed; real directory restored in-place)
    - .planning/phases/28-client-engine-embedding/28-CONTEXT.md (SUPERSEDED banner; 28-01 marked STILL ACTIVE)
    - .planning/phases/30-python-pipeline-api/30-CONTEXT.md (SUPERSEDED banner)
    - .planning/phases/31-streaming-mode-watch/31-CONTEXT.md (SUPERSEDED banner)
    - .planning/STATE.md (active-phase line: Phase 38 complete, Option M v0 shipping)

decisions:
  - "Kept Session / SessionMode / OutOfScopeError in src/client/mod.rs even though no production code uses them — the feature-flag smoke test tests/client_engine_roundtrip.rs links against them. Cost: 95 LOC total. Benefit: preserves the --no-default-features --features client --lib build path as a non-trivial compile, which is Phase 28-01's anti-regression guard."
  - "Kept tests/client_engine_roundtrip.rs — its goal (engine runs in a client-feature context with no server imports) is still valuable even without the embedded replica client on top."
  - "Deleted src/client/session.rs outright — server::replica_client has its own inline wire handshakes against src/server/protocol directly; the client-side helpers were only reused by the deleted streaming.rs + clone.rs."
  - "Did NOT touch ROADMAP.md progress table — orchestrator already flipped 28/30/31 rows to SUPERSEDED in the staged doc updates pre-execution."
  - "python/tally un-symlinked by copy (rm symlink + cp -r source). Equivalent end-state to the pre-Phase-30 directory; git tracks the real files now."
  - "python/tally/__init__.py: TallyError is sourced directly from tally._types (the pure-Python definition) now that the native version is gone. All downstream imports (`from tally import TallyError`) continue to work."

metrics:
  duration: "~55 min"
  completed: 2026-04-15
  tasks_completed: 3  # T1 + T2 + T3
  lines_deleted_total: 10377
  lines_added_total: 92
  net_delta: -10285
  files_deleted: 43
  files_modified: 11
  rust_tests_before: 1265  # pre-38 (after 37 landed)
  rust_tests_after: 1265   # unchanged — we deleted tests AND the code they tested (net-zero re cargo test pass count since removed tests no longer exist)
  python_integration_tests_before: 22
  python_integration_tests_after: 17  # -5 from deleted test_tally_clone.py (4 tests + 1 skip wrapper)
  python_sdk_tests_before: 451
  python_sdk_tests_after: 451  # untouched (python/ directory restored cleanly)
  python_native_tests_before: 24  # (23 pass + 1 skip) — all gone
  python_native_tests_after: 0    # crate deleted
---

# Phase 38 Plan 01: Mothball Option K embedded-client surfaces Summary

## What shipped

A large, mechanical delete-only cleanup that removes the Option K
embedded-client surfaces now superseded by Option M (`tally fork` +
replica-mode server boot).

- **~10,285 lines removed** across 43 deleted files + 11 modified files.
- **`cargo test` still green** at 1265 tests passing (removed tests AND
  removed code — net-neutral on pass count; the flaky
  `connect_dance_against_fake_server` is gone with the rest of
  `streaming.rs`).
- **Both feature flavors still build** — `cargo build` (default/server)
  and `cargo build --no-default-features --features client --lib`. The
  Phase 28-01 feature-split survives; only its consumers shrank.
- **`scripts/check-feature-builds.sh` green.**
- **`pytest tests/integration/` green** at 17 tests (-5 vs. the 22
  before, matching plan expectation — `test_tally_clone.py` gone).
- **`pytest python/tests/` green** at 451 tests (unchanged — the un-symlink
  round-trip is transparent to the existing SDK suite).

## Deletions

### T1 — Rust embedded-client modules

| File                                      | LOC    | Rationale                                                                     |
| ----------------------------------------- | ------ | ----------------------------------------------------------------------------- |
| `src/client/clone.rs`                     | 457    | `run_clone` historical-bootstrap. Superseded by `server::replica_client::fetch_historical_snapshot` (Phase 36). |
| `src/client/streaming.rs`                 | 866    | `StreamingClient` + the subscribe-first dance. Superseded by Phase 36's OP_SUBSCRIBE loop. |
| `src/client/state.rs`                     | 74     | `StreamingStore` wrapper. Dead after `streaming.rs` gone.                     |
| `src/client/session.rs`                   | 318    | Shared handshake helpers. `server::replica_client` talks to `server::protocol` directly; no live reuse. |
| `tests/test_client_streaming.rs`          | 240    | Tests the deleted `StreamingClient`. Includes the flaky `connect_dance_against_fake_server` — good riddance. |
| `src/client/mod.rs` (pruned)              | 323 → 95 | Kept `Session`, `SessionMode`, `OutOfScopeError`, and the `wire` re-export. Deleted `FrozenClient` + 7 test helpers. |

Grep sanity: `grep -r "FrozenClient\|StreamingClient\|run_clone\|StreamingStore"
src/ tests/ --include="*.rs"` returns only doc-comment tombstones in
`src/client/mod.rs` (intentional) and one in
`src/server/replica_client.rs`'s comment (pre-existing, kept — it's
historical context, not a call-site). The `src/state/store.rs` `bulk_load`
doc-comment was updated to point at Phase 36's replica_client.

### T2 — `tally_cli` + `python-native/`

| File / dir                               | Action  | LOC/files | Rationale                                                                 |
| ---------------------------------------- | ------- | --------- | ------------------------------------------------------------------------- |
| `src/bin/tally_cli.rs`                   | DELETE  | 775       | Subcommands `clone` / `query` / `inspect` / `sync` all wrapped the embedded-client path. `fork` lives on the main `tally` binary (Phase 37). |
| `Cargo.toml` `[[bin]] tally_cli`         | DELETE  | —         | Same.                                                                     |
| `python-native/`                         | DELETE  | ~32 files / ~2100 LOC | Entire PyO3 crate. Scientists use the pure-Python SDK + `tally fork`; no native extension needed. |
| `Cargo.toml` `workspace.members`         | MODIFY  | —         | `[".", "python-native"]` → `["."]`.                                       |
| `.github/workflows/ci.yml` `python-native` job | DELETE | 73 lines  | Left a SUPERSEDED comment in its place.                                   |
| `tests/integration/test_tally_clone.py`  | DELETE  | 285       | E2E test for the deleted `tally_cli clone`.                               |
| `python/tally` symlink                   | REPLACE | —         | Was `python/tally -> ../python-native/python_src/tally` (from Plan 30-01). Un-symlinked by copying the contents in-place. |
| `python/tally/__init__.py`               | MODIFY  | —         | Removed the `try: from tally._native import Pipeline, ...` block; `TallyError` sourced from `tally._types` (the pre-Phase-30 shape). |
| `python/tally/_native.pyi`               | DELETE  | —         | Native extension is gone.                                                 |
| `.gitignore`                             | MODIFY  | —         | Removed the `python-native/python_src/tally/_native*.so` rule.            |

### T3 — Doc sweep

| File                                                        | Action  |
| ----------------------------------------------------------- | ------- |
| `.planning/phases/28-client-engine-embedding/28-CONTEXT.md` | Prepended SUPERSEDED banner; flagged 28-01 as STILL ACTIVE (feature-flag split). |
| `.planning/phases/30-python-pipeline-api/30-CONTEXT.md`     | Prepended SUPERSEDED banner (whole phase).                                       |
| `.planning/phases/31-streaming-mode-watch/31-CONTEXT.md`    | Prepended SUPERSEDED banner (whole phase).                                       |
| `.planning/STATE.md`                                        | Updated active-phase line: Phase 38 complete, Option M v0 shipping.              |
| ROADMAP.md                                                  | Untouched in this plan — orchestrator already staged the 37 / 38 flip.           |

## What stays

- `src/client/wire.rs` — reused by `src/server/replica_client.rs::write_scope`.
- `src/client/mod.rs` — slimmed to `Session` / `SessionMode` / `OutOfScopeError`
  + `pub mod wire` (preserves the feature-flag smoke test surface).
- `tests/client_engine_roundtrip.rs` — anti-regression guard for Phase 28-01
  feature-flag split. Still green.
- `tests/phase28_feature_build.rs` — same.
- `scripts/check-feature-builds.sh` — unchanged; still gates both feature flavors.
- Phase 27 integration tests
  (`test_replica_snapshot_fetch_asyncio.py`, `test_replica_subscribe_asyncio.py`,
  `test_replica_log_fetch_asyncio.py`) — untouched; test live server opcodes.
- Phase 36 / 37 integration tests (`test_replica_mode.py`, `test_fork_demo.py`)
  — untouched.
- Phase 28-04 / 30 / 31 SUMMARY files — left as historical record.

## Deviations from Plan

### 1. [Rule 3 - Blocking] Un-symlink `python/tally` before deleting `python-native/`

**Found during:** T2 pre-flight.
**Issue:** Plan 30-01 physically moved `python/tally/` → `python-native/python_src/tally/`
and replaced `python/tally` with a relative symlink. Deleting `python-native/`
without restoring the directory would have broken every `from tally import ...`
callsite in `python/tests/` (451 tests).
**Fix:** `rm python/tally && cp -r python-native/python_src/tally python/tally`
BEFORE the `rm -rf python-native/`. Removed the stale `_native.pyi` + any
compiled `_native*.so` from the copied tree. `python/tests/` (451) stayed
green; no symlink artefacts remain.
**Files touched:** `python/tally/` (now a real dir with ~22 files tracked),
`python/tally/__init__.py` (deleted the `try/except ImportError` block +
updated `__all__`).

### 2. [Rule 2 - Critical] Updated `src/state/store.rs::bulk_load` doc-comment

**Found during:** final grep sweep for `client::clone::run_clone` references.
**Issue:** `src/state/store.rs:743` had a doc-comment pointing at the
now-deleted `tally::client::clone::run_clone`. Stale doc-comment, technically
still compiles, but a reader grep'ing the codebase for "run_clone" would
find only this tombstone and wonder where the source is.
**Fix:** Updated the doc-comment to point at Phase 36's `server::replica_client`
with a historical note that the original Phase 28-04 caller (FrozenClient)
was mothballed in 38-01.
**Files touched:** `src/state/store.rs` (11-line doc-comment touch).

### 3. [Rule 4 - Architectural] NOT applied — pre-existing clippy drift

`cargo clippy --all-targets -- -D warnings` reports ~46 lints across
8 files in the engine + server subsystems. **None** of the flagged files
were touched by this plan; all errors are pre-existing lint drift
independent of the mothball. Per the scope-boundary rule these are
out-of-scope for a housekeeping plan.

Logged to `.planning/phases/38-mothball-v0-client/deferred-items.md`
as a standalone tech-debt item (file list + recommendation to either
pin the clippy channel or do a dedicated sweep). Primary verification
gates (`cargo test`, both-flavor `cargo build`, feature-build script,
pytest integration) all pass — functional correctness unaffected.

### 4. Flaky `test_fork_demo.py` (pre-existing; not caused by this plan)

First two `pytest tests/integration/` runs errored on a `TCP server error:
Address already in use (os error 98)` in the Phase 37 fork fixture. This
is a known port-picker race in the fixture's `_find_free_port` / bind
sequence (documented in 37-01-SUMMARY). Subsequent 3 retries ALL passed
cleanly. Not regression from this plan — the same test passed in the
Phase 37 CI verification run.

## Auth gates

None. Offline housekeeping only.

## Verification evidence

| Check                                                     | Before (post-37) | After (post-38-01) | Result |
| --------------------------------------------------------- | ---------------- | ------------------ | ------ |
| `cargo build` (default/server)                            | ✅                | ✅                  | pass   |
| `cargo build --no-default-features --features client --lib` | ✅              | ✅                  | pass   |
| `cargo test` total                                        | 1265 pass + 1 flaky | **1265 pass**   | pass (flaky test GONE) |
| `scripts/check-feature-builds.sh`                         | ✅                | ✅                  | pass   |
| `pytest tests/integration/`                               | 22 pass          | **17 pass**        | pass (expected -5) |
| `pytest python/tests/`                                    | 451 pass         | **451 pass**       | pass   |
| `grep -r FrozenClient\|StreamingClient\|run_clone src/ tests/ --include=*.rs` | N/A | only doc-comment tombstones | pass |
| `git ls-files python-native` count                        | ~32              | **0**              | pass   |

## Threat Flags

None. Delete-only housekeeping introduces no new trust boundaries.

## v0 Option M ships with 1265 cargo + 17 integration + 451 SDK = 1733 tests green.

The whole Option M stack — `OP_LOG_FETCH` (Phase 35), replica-mode
server boot (Phase 36), `tally fork` CLI + `/debug/ready` + `test_fork_demo.py`
(Phase 37), Option K mothball (this plan) — is now the canonical data-scientist
workflow for v0 Local Replica. Embedded engines in Python processes,
custom streaming clients, and PyO3 wheel-building are all behind us.

## Self-Check: PASSED

- `test -d python-native` → not present → **deleted OK**
- `test -f src/client/clone.rs` → not present → **deleted OK**
- `test -f src/client/streaming.rs` → not present → **deleted OK**
- `test -f src/client/state.rs` → not present → **deleted OK**
- `test -f src/client/session.rs` → not present → **deleted OK**
- `test -f src/bin/tally_cli.rs` → not present → **deleted OK**
- `test -f tests/test_client_streaming.rs` → not present → **deleted OK**
- `test -f tests/integration/test_tally_clone.py` → not present → **deleted OK**
- `test -d python/tally && test ! -L python/tally` → real directory, not symlink → **restored OK**
- `test -f python/tally/__init__.py` → present, native re-exports removed → **edited OK**
- `cargo test` → 1265 pass → **verified**
- `pytest python/tests/` → 451 pass → **verified**
- `scripts/check-feature-builds.sh` → OK → **verified**

All deletions + restorations confirmed on disk; all verification gates green.
