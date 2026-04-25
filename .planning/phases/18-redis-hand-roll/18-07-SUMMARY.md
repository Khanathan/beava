---
phase: 18-redis-hand-roll
plan: "18-07"
subsystem: beava-server
tags: [feature-flag-removal, http-routes, push-and-get, phase-16-rename, python-sdk]
depends_on: [18-04]
dependency_graph:
  requires: [18-04]
  provides: [push-and-get-routes, upsert-delete-routes, x-runtime-headers, sdk-upsert-delete]
  affects: [beava-server, beava-bench, temporal_http, python-sdk]
tech_stack:
  added: []
  patterns:
    - "X-Runtime axum middleware for data-plane / admin-plane contract marking"
    - "atomic push-and-get: execute_push then state_tables.lock() — no await inside lock"
    - "EntityKey(Vec<(String, String)>) — group_key_name/value pairs for feature queries"
key_files:
  created:
    - crates/beava-server/src/push_and_get.rs
    - crates/beava-server/tests/phase18_07_no_tokio_dataplane_test.rs
    - crates/beava-server/tests/phase18_07_push_and_get_test.rs
    - crates/beava-server/tests/phase18_07_upsert_delete_rename_test.rs
    - python/tests/test_phase18_07_upsert_delete.py
  modified:
    - crates/beava-server/Cargo.toml
    - crates/beava-server/src/lib.rs
    - crates/beava-server/src/server.rs
    - crates/beava-server/src/http.rs
    - crates/beava-server/src/http_admin.rs
    - crates/beava-server/src/temporal_http.rs
    - crates/beava-server/tests/phase18_07_no_tokio_dataplane_test.rs
    - crates/beava-server/tests/phase11_5_temporal_smoke.rs
    - crates/beava-bench/src/bin/temporal_throughput.rs
    - python/beava/_app.py
    - .planning/phases/16-sdk-source-annotation/16-CONTEXT.md
decisions:
  - "Phase 12.5 GA1 compliance: execute_push completes fsync before state_tables.lock() — no await inside lock scope"
  - "Phase 16 D-09 hard break: /push-table and /delete-table removed with no deprecation aliases (pre-v0)"
  - "hand-rolled-runtime feature flag removed unconditionally; ServerV18 and http_admin always compiled"
  - "EntityKey is Vec<(String, String)> pairs not Vec<u8> — discovered during build, fixed per Rule 1"
metrics:
  duration_minutes: 120
  completed_date: "2026-04-25"
  tasks_completed: 6
  files_changed: 16
---

# Phase 18 Plan 07: Hot-Path Consolidation Summary

**One-liner:** Remove `hand-rolled-runtime` feature flag, add `/push-and-get` atomic routes (Phase 12.5), rename `/push-table` → `/upsert` + `/delete-table` → `/delete` (Phase 16 D-09 hard break), and add Python SDK `app.upsert` / `app.delete`.

---

## What Was Built

### Task 7.1 — Remove `hand-rolled-runtime` feature flag

Deleted `hand-rolled-runtime = []` from `beava-server/Cargo.toml`. Removed all
`#[cfg(feature = "hand-rolled-runtime")]` guards from `lib.rs`, `server.rs`, and
`http_admin.rs`. `ServerV18` and `http_admin` are now unconditionally compiled.
The `phase18_01_bind_v18` test had its `required-features` changed from
`["hand-rolled-runtime"]` to `["testing"]`.

### Task 7.2 — X-Runtime header middleware

Added `stamp_runtime_header` axum middleware in `http.rs` (data-plane:
`X-Runtime: hand-rolled`) and `stamp_tokio_header` in `http_admin.rs`
(admin-plane: `X-Runtime: tokio`). Both applied via `.layer(middleware::from_fn(...))`
on their respective routers.

### Task 7.3 — `/push-and-get` and `/push-sync-and-get` routes (Phase 12.5)

New file `push_and_get.rs` implements `execute_push_and_get`. Calls `execute_push`
(completing any WAL fsync before returning for `PerEvent` mode), then acquires
`state_tables.lock()` to query features — no `.await` inside the lock scope
(Phase 12.5 GA1 compliance / read-your-writes by construction).

Routes:
- `POST /push-and-get/:event_name` — `SyncMode::Periodic` (acks=1)
- `POST /push-sync-and-get/:event_name` — `SyncMode::PerEvent` (acks=all)

Feature query iterates `req.query.features`, resolves each via `registry.resolve_feature`,
builds `EntityKey(Vec<(String, String)>)` from the `entity_key` map, and returns
`{"ack_lsn", "registry_version", "features", "warnings"}`.

### Task 7.4 — Phase 16 D-09 route rename (hard break)

In `temporal_http.rs`:
- `push_table_handler` → `upsert_handler`; route `/push-table/:table_name` → `/upsert/:table_name`
- New `delete_handler` → route `/delete/:table_name` (retract semantics: scans
  `event_id_index` for most recent non-retracted `TableWrite` LSN for the key,
  then issues `store.retract()`)
- Old routes `/push-table` and `/delete-table` removed completely (pre-v0 hard break)

Migrated all callers: `phase11_5_temporal_smoke.rs` (3 sites) and
`temporal_throughput.rs` (3 sites) updated to `/upsert/`.

### Task 7.5 — Python SDK `app.upsert` / `app.delete`

Added `App.upsert(table_type, row_dict)` and `App.delete(table_type, *, key)` to
`python/beava/_app.py`. Both call `transport._client.post()` with JSON body and
`Content-Type: application/json`. No `push_table` / `delete_table` aliases added
(Phase 16 GA-2: hard break).

### Task 7.6 — Phase 16 CONTEXT.md absorb note

Updated `.planning/phases/16-sdk-source-annotation/16-CONTEXT.md` with an
"Absorb note (2026-04-24)" section recording that Plan 16-02 work was absorbed
by Plan 18-07. Requirements `SDK-UPSERT-01`, `SDK-UPSERT-02`, `SRV-WIRE-RENAME-01`
are satisfied. Plan 16-02 file is retained as design artifact; should be skipped
during Phase 16 execution.

---

## Test Results

| Suite | Tests | Result |
|-------|-------|--------|
| `phase18_07_no_tokio_dataplane_test` | 2 | PASS |
| `phase18_07_push_and_get_test` | 4 | PASS |
| `phase18_07_upsert_delete_rename_test` | 3 | PASS |
| `phase11_5_temporal_smoke` | 6 | PASS |
| `python/tests/test_phase18_07_upsert_delete.py` | 6 | PASS |
| `beava-server` total (excl. pre-existing cli_smoke) | 118 | PASS |

Pre-existing `cli_smoke` failures (`loads_valid_config_starts_and_prints_banner`,
`env_var_overrides_listen_addr`) are unrelated to Plan 18-07 — confirmed present
on HEAD before this plan's commits.

---

## Commits

| Hash | Message |
|------|---------|
| `db3d62b` | test(18-07): RED — assert hand-rolled-runtime feature is removed (Task 7.1) |
| `3caeae4` | feat(18-07): GREEN — remove hand-rolled-runtime feature flag (Task 7.1) |
| `8698324` | test(18-07): RED — only admin on tokio, /push-and-get routes, /upsert/delete rename, Python SDK (Tasks 7.2-7.5) |
| `6e03e52` | feat(18-07): GREEN — delete tokio data-plane; X-Runtime headers; /upsert /delete /push-and-get routes (Tasks 7.2-7.4) |
| `4efb36f` | feat(18-07): GREEN — Python SDK app.upsert/app.delete (Task 7.5) |
| `4b62832` | docs(18-07): update Phase 16 CONTEXT with Plan 16-02 absorb note (Task 7.6) |

---

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `EntityKey` type mismatch in `push_and_get.rs`**
- **Found during:** Task 7.3 GREEN
- **Issue:** Initial `build_entity_key` returned `Vec<u8>` (length-prefix encoded bytes)
  matching an older pattern. Compiler rejected it: `expected EntityKey, found Vec<u8>`.
- **Fix:** Inspected `feature_query.rs::parse_entity_key` to confirm `EntityKey` is
  `EntityKey(Vec<(String, String)>)` — pairs of `(group_key_name, value)`. Rewrote
  `build_entity_key` to return `EntityKey(pairs)`.
- **Files modified:** `crates/beava-server/src/push_and_get.rs`
- **Commit:** `6e03e52`

**2. [Rule 1 - Bug] `TestServer.client()` method does not exist**
- **Found during:** Task 7.4 RED/GREEN
- **Issue:** Test initially called `ts.client().get(url).send().await`. `TestServer` has
  no `client()` method.
- **Fix:** Used `ts.get_raw("/path").await` (existing helper) instead.
- **Files modified:** `crates/beava-server/tests/phase18_07_upsert_delete_rename_test.rs`
- **Commit:** `8698324`

**3. [Rule 3 - Blocker] `respx` not available for Python test mocking**
- **Found during:** Task 7.5 RED
- **Issue:** `import respx` failed — package not installed.
- **Fix:** Used `unittest.mock.patch.object(app._transport._client, "post", ...)` to
  mock `httpx.Client.post` directly.
- **Files modified:** `python/tests/test_phase18_07_upsert_delete.py`
- **Commit:** `8698324`

**4. [Rule 3 - Blocker] `@bv.source` not in SDK yet (Phase 16 Plan 01 deferred)**
- **Found during:** Task 7.5 RED
- **Issue:** Python test fixture used `@bv.source @bv.table(...)`. `bv.source` doesn't
  exist — Phase 16 Plan 01 hasn't landed.
- **Fix:** Used `@bv.table(key="user_id")` directly in test fixtures. The Phase 18-07
  plan only requires testing `upsert`/`delete` HTTP verb routing, not source marker
  enforcement.
- **Files modified:** `python/tests/test_phase18_07_upsert_delete.py`
- **Commit:** `8698324`

---

## Deferred Items

- `cli_smoke.rs::loads_valid_config_starts_and_prints_banner` and
  `env_var_overrides_listen_addr` — pre-existing flaky failures unrelated to this plan.
  Require binary compilation + subprocess start timing. Tracked separately.

---

## Known Stubs

None. All routes are fully wired with live server state.

---

## Self-Check: PASSED
