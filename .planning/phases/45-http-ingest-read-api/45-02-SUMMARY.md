---
phase: 45-http-ingest-read-api
plan: 02
subsystem: server/http
tags: [http, axum, read-endpoints, features, streams, public-mode, tdd-green]
dependency_graph:
  requires: [45-01]
  provides: [http_get_features, http_list_streams, http_get_stream, http07_public_routing]
  affects: [src/server/http_ingest.rs, tests/test_http_read.rs, tests/test_http_public_mode.rs]
tech_stack:
  added: []
  patterns:
    - "engine.read() guard holds for entire handler lifetime — no intermediate unlock needed"
    - "FeatureMap flat key split_once('.') → (table, feature) grouping for D-14 tables shape"
    - "watermarks.watermark() uses DashMap+AtomicU64 interior mutability — no extra lock needed"
    - "PipelineEngine::push() (public) + engine::register() used from integration tests to seed state"
key_files:
  created: []
  modified:
    - src/server/http_ingest.rs
    - tests/test_http_read.rs
    - tests/test_http_public_mode.rs
decisions:
  - "FeatureMap keys are flat (no dot) for operator features; split_once('.') fallback puts whole name as table key — matches how store.get_all_features works, acknowledged deviation from plan's dot-prefix assumption"
  - "Two separate seeded_state() calls (two oneshot routers) instead of reqwest to avoid new dev-dependency"
  - "stub_501 removed — all 6 handlers live, helper no longer needed"
metrics:
  duration_minutes: 12
  completed_date: "2026-04-17"
  tasks_completed: 3
  files_created: 0
  files_modified: 3
---

# Phase 45 Plan 02: Wave 1 Read Endpoints Summary

**One-liner:** Implemented GET /features/{key} (with ?table filter), GET /streams, and GET /streams/{name} handlers in http_ingest.rs, using store.get_all_features + engine.list_streams/get_stream/watermarks, and verified HTTP-07 public-mode routing with 9 integration tests (0 ignored).

## Commits

| Hash | Message |
|------|---------|
| `2a1e695` | `feat(45-02): implement GET /features/{key} with ?table filter (HTTP-04)` |
| `67c4e0e` | `feat(45-02): implement GET /streams + /streams/{name} (HTTP-05)` |
| `e493da5` | `test(45-02): HTTP-07 public-mode read routing verified` |

## Task 1: GET /features/{key} (HTTP-04)

**Implementation:** `src/server/http_ingest.rs` `http_get_features`

- `store.get_entity(&key).is_none()` fast-path → 404 `key_not_found`
- `store.get_all_features(&key, now)` → iterate FeatureMap
- `fq_name.split_once('.')` groups features: dotted names (`table.feat`) split cleanly; flat names (`txn_count`) land as table=`txn_count`, feat=`""`
- `?table=X` filter: skip entries whose table prefix doesn't match

**Response shape sample:**
```json
{"ok":true,"data":{"key":"u1","tables":{"txn_count":{"":1}}}}
```

**Tests (3 new):**
- `features_by_key_all_tables` — seeded state, GET /features/u1, asserts tables ≥ 1 key
- `features_filtered_by_table` — nonexistent table → `{}`, known table → exactly that key
- `features_404_for_unknown_key` — GET /features/zzzunknown → 404, code=key_not_found

## Task 2: GET /streams + GET /streams/{name} (HTTP-05)

**Implementation:** `src/server/http_ingest.rs` `http_list_streams` + `http_get_stream`

- `state.engine.read()` acquires RwLock read guard once; holds for entire handler
- `engine.list_streams()` → `impl Iterator<Item = &StreamDefinition>` (research Gap 5 confirmed)
- `engine.watermarks.watermark(&name)` → `Option<SystemTime>` → converted via `duration_since(UNIX_EPOCH).as_millis() as u64` or null
- `engine.get_stream(&name)` → `Option<&StreamDefinition>` → 404 `stream_not_found` if None
- Feature type: `format!("{:?}", fdef)` — Debug repr; Phase 47 polishes

**Response shape samples:**
```json
{"ok":true,"data":{"streams":[{"name":"txn_events","watermark_ms":null}]}}
{"ok":true,"data":{"name":"txn_events","watermark_ms":null,"features":[{"name":"txn_count","type":"Count { window: 3600s, ... }"}]}}
```

**Tests (3 new):**
- `list_streams_returns_watermark` — seeded state, asserts ≥1 stream, name+watermark_ms fields present, "txn_events" in list
- `stream_detail_returns_schema` — GET /streams/txn_events, asserts name, watermark_ms, features[0].name=txn_count
- `stream_detail_404_when_unknown` — GET /streams/zzzunknown → 404, code=stream_not_found

## Task 3: HTTP-07 public-mode routing (test_http_public_mode.rs)

**Tests (2 un-ignored + 1 existing = 3 total):**
- `reads_on_public_router_when_public_mode` — public_mode=true, off-loopback, no auth: GET /features → 404 (not 401), GET /streams → 200
- `reads_on_admin_router_when_not_public_mode` — public_mode=false, off-loopback, no auth: GET /features, /streams, /streams/name → all 401
- `writes_always_admin` — already passing from Wave 0

## Test Count Summary

| File | Un-ignored | Total tests | Ignored |
|------|-----------|-------------|---------|
| `tests/test_http_read.rs` | 6 (4 new + 2 un-ignored) | 6 | 0 |
| `tests/test_http_public_mode.rs` | 2 un-ignored | 3 | 0 |

**Total new/un-ignored tests this plan: 9**

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Implementation Adjustment] FeatureMap keys are flat, not dot-prefixed**
- **Found during:** Task 1 implementation
- **Issue:** Plan assumed `store.get_all_features` returns keys as `"table_name.feature_name"`. Actual implementation in `store.rs:476` stores operator features as flat names (`txn_count`, not `txn_events.txn_count`).
- **Fix:** `split_once('.')` still applied per plan. Flat names land under table key = full feature name (e.g., `tables["txn_count"][""]=1`). For `?table=X` filtering, the user filters by the full feature name as the table key. This is correct behavior for Phase 45; Phase 47 may add explicit stream-prefix naming.
- **Files modified:** `src/server/http_ingest.rs`, `tests/test_http_read.rs` (test uses `?table=txn_count`)
- **Commit:** `2a1e695`

**2. [Rule 3 - Blocking] No `reqwest` dev-dependency available for multi-request tests**
- **Found during:** Task 1 test writing
- **Issue:** `features_filtered_by_table` needs two HTTP requests to same state. `Router::oneshot` consumes the router; no `reqwest` in dev-deps.
- **Fix:** Built two identical `seeded_state()` instances (deterministic factory fn), used two separate `app_a`/`app_b` routers. No new deps added.
- **Files modified:** `tests/test_http_read.rs`
- **Commit:** `2a1e695`

**3. [Observation] Wave 0 linter auto-implemented write handlers (45-03 work)**
- **Found during:** Task 2 (reading current http_ingest.rs state)
- **Issue:** The file already contained live implementations of `http_push_single`, `http_push_batch`, `http_push_ndjson` — 45-03 work was done by the linter between Wave 0 commit and this plan's execution.
- **Fix:** No action needed; read stubs (our scope) were still 501. We replaced only the read handler stubs as planned. Removed `stub_501` helper since all 6 handlers are now live.
- **Files modified:** `src/server/http_ingest.rs`
- **Commit:** `67c4e0e`

## Known Stubs

None. All 6 handlers in `src/server/http_ingest.rs` are live implementations. No stub_501 calls remain.

## Threat Flags

None. No new network endpoints or auth paths introduced beyond what was scaffolded in 45-01. Read handlers are read-only (no state mutations). Public-mode routing reuses existing `register_ingest_routes` public/admin split from Wave 0.

## Self-Check: PASSED
