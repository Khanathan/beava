---
phase: 45-http-ingest-read-api
plan: 01
subsystem: server/http
tags: [http, axum, tower, auth, scaffolding, tdd-red]
dependency_graph:
  requires: []
  provides: [http_ingest_scaffold, auth_401, handle_push_core_ex_pub_crate, body_limit_layer, test_harness]
  affects: [src/server/http.rs, src/server/auth.rs, src/server/tcp.rs, tests/]
tech_stack:
  added:
    - "axum-extra 0.12.6 (json-lines feature)"
    - "tower-http 0.6.8 (limit, timeout features)"
    - "tower 0.5.3 (util feature) — promoted from dev-dep to runtime"
  patterns:
    - "ServiceBuilder stack: DefaultBodyLimit::disable() + RequestBodyLimitLayer::new(16 MiB) + TimeoutLayer::with_status_code(408, 30s)"
    - "register_ingest_routes() wired in build_router BEFORE .route_layer(auth) per Pitfall 15"
    - "501 Not Implemented stubs with {ok, error{code, message}} envelope"
key_files:
  created:
    - src/server/http_ingest.rs
    - tests/http_common.rs
    - tests/http_auth.rs
    - tests/test_http_push.rs
    - tests/test_http_ndjson.rs
    - tests/test_http_read.rs
    - tests/test_http_public_mode.rs
    - tests/test_http_body_limit.rs
    - tests/test_http_schema_parity.rs
  modified:
    - Cargo.toml
    - Cargo.lock
    - src/server/auth.rs
    - src/server/tcp.rs
    - src/server/mod.rs
    - src/server/http.rs
    - tests/test_admin_auth.rs
    - tests/test_debug_warnings_endpoint.rs
decisions:
  - "auth 403→401: require_loopback_or_token now returns 401 UNAUTHORIZED (HTTP-06, orchestrator decision A4)"
  - "TimeoutLayer::new() deprecated in tower-http 0.6; use with_status_code(REQUEST_TIMEOUT, duration) instead"
  - "DefaultBodyLimit::disable() must precede RequestBodyLimitLayer::new() to suppress axum's 2 MiB default cap"
  - "register_ingest_routes() called before .route_layer(auth) to prevent auth bypass on future route additions (Pitfall 15)"
metrics:
  duration_minutes: 35
  completed_date: "2026-04-17"
  tasks_completed: 3
  files_created: 9
  files_modified: 8
---

# Phase 45 Plan 01: Wave 0 Scaffolding Summary

**One-liner:** Added axum-extra 0.12 + tower-http 0.6 runtime deps, flipped auth from 403→401, created `http_ingest.rs` with 16 MiB body-limit + 30s timeout layers before auth gate, and scaffolded 8 TDD-RED test files with Wave 0 assertions passing.

## Commits

| Hash | Message |
|------|---------|
| `26c07b5` | `feat(45-01): add axum-extra+tower-http deps, promote handle_push_core_ex to pub(crate), auth 403→401 per HTTP-06` |
| `1acbad9` | `feat(45-01): scaffold src/server/http_ingest.rs with body-limit/timeout layer + 6 stubs` |
| `73f186e` | `test(45-01): add http_common harness + 7 integration test scaffolds (TDD RED for Waves 1-2)` |

## New Dependency Versions (from Cargo.lock)

| Crate | Version | Features |
|-------|---------|---------|
| axum-extra | 0.12.6 | json-lines |
| tower-http | 0.6.8 | limit, timeout |
| tower | 0.5.3 | util (promoted from dev-dep) |

## Task 1: Deps + auth 401 + pub(crate)

- `Cargo.toml` `[dependencies]`: added axum-extra, tower-http; moved tower from `[dev-dependencies]`
- `src/server/auth.rs`: `StatusCode::FORBIDDEN` → `StatusCode::UNAUTHORIZED` in `require_loopback_or_token`; updated doc comment
- `src/server/tcp.rs:1271`: `fn handle_push_core_ex` → `pub(crate) fn handle_push_core_ex`
- `tests/test_admin_auth.rs`: 5 assertions updated from `FORBIDDEN` → `UNAUTHORIZED`
- `tests/test_debug_warnings_endpoint.rs`: 1 assertion updated from `FORBIDDEN` → `UNAUTHORIZED`
- **Auth test blast radius:** 2 test files, 6 assertion sites total. Both test suites pass after update.

## Task 2: http_ingest.rs skeleton

- New file `src/server/http_ingest.rs` (148 lines):
  - `register_ingest_routes(public_router, admin_router, public_mode) -> (Router, Router)`
  - Middleware stack: `DefaultBodyLimit::disable()` + `RequestBodyLimitLayer::new(16 MiB)` + `TimeoutLayer::with_status_code(408, 30s)`
  - 3 write routes mounted on admin router (with layer): `POST /push/{stream}`, `POST /push-batch/{stream}`, `POST /push/{stream}/ndjson`
  - 3 read routes: `GET /features/{key}`, `GET /streams`, `GET /streams/{name}` — admin or public based on `public_mode`
  - 6 handler stubs returning `501 Not Implemented` with `{"ok": false, "error": {"code": "not_implemented", "message": "..."}}`
- `src/server/mod.rs`: added `pub mod http_ingest;`
- `src/server/http.rs`: `register_ingest_routes` called at line 1571, BEFORE `.route_layer(require_loopback_or_token)` at line 1577

**Smoke test result:** `curl -X POST http://127.0.0.1:6401/push/test -d '{}'` from loopback → 501 JSON with `"not_implemented"` ✓

## Task 3: TDD-RED test scaffolds

| File | Wave 0 passing | Ignored |
|------|---------------|---------|
| `tests/http_common.rs` | n/a (harness) | — |
| `tests/http_auth.rs` | 1 (full auth sweep) | 0 |
| `tests/test_http_body_limit.rs` | 3 (1 MiB, 15 MiB, 17 MiB) | 0 |
| `tests/test_http_push.rs` | 1 (413 test) | 4 |
| `tests/test_http_public_mode.rs` | 1 (writes_always_admin) | 2 |
| `tests/test_http_ndjson.rs` | 0 | 2 |
| `tests/test_http_read.rs` | 0 | 4 |
| `tests/test_http_schema_parity.rs` | 0 | 1 |

**Wave 0 assertions passing (6 total):**
- Auth sweep: all 3 write routes 401 off-loopback; all 3 read routes open in public_mode; all 3 read routes 401 off-loopback in non-public_mode
- Body limit: 1 MiB → not 413, 15 MiB → not 413, 17 MiB → 413

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Deprecated API] tower-http 0.6 deprecated `TimeoutLayer::new`**
- **Found during:** Task 2 smoke build + clippy
- **Issue:** `tower_http::timeout::TimeoutLayer::new` is deprecated in tower-http 0.6; compiler warning `use of deprecated associated function: Use TimeoutLayer::with_status_code instead`
- **Fix:** Replaced with `TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, Duration::from_secs(30))`. Note: argument order is `(status_code, duration)` — confirmed from tower-http 0.6 source.
- **Files modified:** `src/server/http_ingest.rs`
- **Commit:** `1acbad9`

**2. [Rule 2 - Dead code suppression] Wave 0 stub struct fields trigger dead_code warnings**
- **Found during:** Task 2 clippy check
- **Issue:** `SyncQuery.sync` and `TableQuery.table` fields are `pub(crate)` but unread until Wave 1; clippy `-D warnings` would fail on these new files
- **Fix:** Added `#[allow(dead_code)]` on both structs — intentional Wave 0 stubs, Wave 1 will use the fields
- **Files modified:** `src/server/http_ingest.rs`
- **Commit:** `1acbad9`

**Pre-existing clippy errors (out of scope):** 11 pre-existing `-D warnings` failures in `tcp.rs`, `throughput.rs`, `shard_probe.rs` existed before this plan. Zero new ones introduced. Logged to deferred-items for INFRA-06.

## Known Stubs

| Stub | File | Handler | Reason |
|------|------|---------|--------|
| `push_single` returns 501 | `src/server/http_ingest.rs:101` | `http_push_single` | Wave 1 (45-02) will wire `handle_push_core_ex` |
| `push_batch` returns 501 | `src/server/http_ingest.rs:111` | `http_push_batch` | Wave 1 (45-02) will wire batch handler |
| `push_ndjson` returns 501 | `src/server/http_ingest.rs:121` | `http_push_ndjson` | Wave 2 (45-03) will wire `axum_extra::JsonLines` |
| `get_features` returns 501 | `src/server/http_ingest.rs:129` | `http_get_features` | Wave 2 (45-03) will wire state read |
| `list_streams` returns 501 | `src/server/http_ingest.rs:137` | `http_list_streams` | Wave 2 (45-03) will wire engine read |
| `get_stream` returns 501 | `src/server/http_ingest.rs:143` | `http_get_stream` | Wave 2 (45-03) will wire engine read |

These stubs are intentional — the plan goal is Wave 0 scaffolding only. 45-02 and 45-03 replace the bodies.

## Build + Test Gate Results

```
cargo build --release --bin beava   → OK (0 errors, 0 new warnings)
cargo test --lib --release          → 788 passed, 0 failed
cargo test --test test_admin_auth   → 10 passed
cargo test --test test_debug_warnings_endpoint → 10 passed
cargo test --test http_auth         → 1 passed
cargo test --test test_http_body_limit → 3 passed
cargo test --test test_http_push    → 1 passed, 4 ignored
cargo test --test test_http_ndjson  → 2 ignored
cargo test --test test_http_read    → 4 ignored
cargo test --test test_http_public_mode → 1 passed, 2 ignored
cargo test --test test_http_schema_parity → 1 ignored
```

## Self-Check: PASSED

Files verified to exist:
- `src/server/http_ingest.rs` ✓
- `src/server/mod.rs` (contains `pub mod http_ingest`) ✓
- `tests/http_common.rs` ✓
- `tests/http_auth.rs` ✓
- `tests/test_http_push.rs` ✓
- `tests/test_http_ndjson.rs` ✓
- `tests/test_http_read.rs` ✓
- `tests/test_http_public_mode.rs` ✓
- `tests/test_http_body_limit.rs` ✓
- `tests/test_http_schema_parity.rs` ✓

Commits verified: `26c07b5`, `1acbad9`, `73f186e` all present in `git log`.
