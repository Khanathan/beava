---
phase: 45-http-ingest-read-api
plan: "04"
subsystem: http-ingest
tags: [metrics, auth, observability, proto-label]
dependency_graph:
  requires: [45-01, 45-02, 45-03]
  provides: [beava_events_total{proto} dual-emit, HTTP-06 exhaustive auth coverage]
  affects: [src/server/tcp.rs, src/server/http.rs, src/server/http_ingest.rs, tests/http_auth.rs, tests/test_http_metrics.rs]
tech_stack:
  added: []
  patterns:
    - AtomicU64 per-protocol counters alongside existing unlabeled total (A5 dual-emit strategy)
    - Tower oneshot pattern for multi-request integration tests without reqwest
key_files:
  created:
    - tests/test_http_metrics.rs
  modified:
    - src/server/tcp.rs
    - src/server/http_ingest.rs
    - src/server/http.rs
    - tests/http_auth.rs
decisions:
  - "A5 dual-emit: keep unlabeled beava_events_total for backward compat; add proto-labeled series side-by-side; Phase 47 removes unlabeled"
  - "events_tcp bumped at call sites in tcp.rs (not inside handle_push_batch) so HTTP callers of handle_push_batch don't accidentally bump events_tcp"
  - "Replica path (replica_ingest_batch) intentionally excluded from events_tcp — replica events originate on the upstream node, not this node's TCP interface"
metrics:
  duration_minutes: 35
  completed_date: "2026-04-17"
  tasks_completed: 3
  tasks_total: 3
  files_modified: 5
---

# Phase 45 Plan 04: Wave 2 — Exhaustive auth sweep + proto-labeled metrics Summary

**One-liner:** Dual-emit `beava_events_total{proto="http"|"tcp"}` AtomicU64 counters alongside existing unlabeled total (A5 transition), plus exhaustive 7-function per-route auth sweep replacing the Wave 0 smoke test.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add events_http + events_tcp counters (A5 metric transition) | 1f0a14e | tcp.rs, http_ingest.rs, http.rs |
| 2 | Expand tests/http_auth.rs to exhaustive sweep | bfee8e9 | tests/http_auth.rs |
| 3 | /metrics proto-label integration test | 03fcc4d | tests/test_http_metrics.rs |

## What Was Built

### Task 1: A5 Dual-Emit Metrics

Added two new `AtomicU64` fields to `ConcurrentAppState` in `src/server/tcp.rs`:
- `pub events_http: AtomicU64` — bumped by every successful HTTP push handler
- `pub events_tcp: AtomicU64` — bumped at every TCP push call site

**TCP bump sites (4 total):**
1. `flush_batch_to_drain` helper — TCP async-batch path via ConnAccumulator
2. `handle_connection` MSET batch path (first site, ~line 656)
3. `handle_connection` async-batch path (second site, ~line 827)
4. `handle_sync_command` / OP_PUSH sync single-event path (~line 2010)

**HTTP bump sites (3 total in http_ingest.rs):**
1. `http_push_single` — after successful `handle_push_core_ex` Ok(_)
2. `http_push_batch` — after tallying `accepted` from `handle_push_batch`
3. `http_push_ndjson` — after final `flush_batch!()` accumulates `accepted`

Note: `handle_push_batch` and `handle_push_core_ex` already bump `events_total` internally. HTTP handlers bump only `events_http` (not `events_total` again) to avoid double-counting.

**`/metrics` now emits three lines per A5:**
```
# HELP beava_events_total Total events ingested (sum of all protocols; unlabeled for backward compat; labeled series will replace in v1.1)
# TYPE beava_events_total counter
beava_events_total <N_total>           // TODO(Phase 47): remove unlabeled beava_events_total emission
beava_events_total{proto="http"} <N_http>
beava_events_total{proto="tcp"} <N_tcp>
```

### Task 2: Exhaustive Auth Sweep

Replaced the Wave 0 smoke test (`test_auth_sweep_all_ingest_routes`) with 7 dedicated test functions:

| Test Function | Routes | Case |
|---|---|---|
| `test_writes_reject_offloopback_noauth` | 3 write | off-loopback, no token → 401 |
| `test_writes_allow_loopback_noauth` | 3 write | loopback, no token → NOT 401 |
| `test_writes_allow_offloopback_withtoken` | 3 write | off-loopback + Bearer → NOT 401 |
| `test_reads_reject_offloopback_noauth_when_not_public` | 3 read | admin-mode off-loopback → 401 |
| `test_reads_allow_offloopback_noauth_when_public` | 3 read | public_mode off-loopback → NOT 401 |
| `test_reads_allow_loopback_even_when_not_public` | 3 read | loopback in admin-mode → NOT 401 |
| `test_writes_always_admin_even_when_public` | 3 write | public_mode off-loopback → 401 |

**Pitfall-15 regression guard:** `test_writes_reject_offloopback_noauth` is the canonical guard. If a future refactor places any write route below `.route_layer(require_loopback_or_token)`, this test will fail with a message like: `"route POST /push/s expected 401 (off-loopback, no token) got 200 OK"`.

### Task 3: /metrics Integration Test

Two tests in `tests/test_http_metrics.rs`:
1. `test_proto_labeled_events_total` — 5 HTTP + 3 simulated-TCP pushes → asserts `beava_events_total 8`, `{proto="http"} 5`, `{proto="tcp"} 3`
2. `test_pure_http_run_shows_zero_tcp` — 4 HTTP-only pushes → asserts `{proto="tcp"} 0`

## Verification Results

```
cargo test --test http_auth --test test_http_metrics --release

running 7 tests
test test_writes_reject_offloopback_noauth ... ok
test test_reads_reject_offloopback_noauth_when_not_public ... ok
test test_reads_allow_loopback_even_when_not_public ... ok
test test_reads_allow_offloopback_noauth_when_public ... ok
test test_writes_always_admin_even_when_public ... ok
test test_writes_allow_offloopback_withtoken ... ok
test test_writes_allow_loopback_noauth ... ok
test result: ok. 7 passed; 0 failed

running 2 tests
test test_pure_http_run_shows_zero_tcp ... ok
test test_proto_labeled_events_total ... ok
test result: ok. 2 passed; 0 failed
```

Full suite (`cargo test --release --tests`): all non-replica tests pass. `test_replica_subscribe` failures are pre-existing environment issues (OS cannot bind non-loopback addresses, code 49) unrelated to this plan.

## Acceptance Criteria Verification

- `grep -c 'events_http: std::sync::atomic::AtomicU64' src/server/tcp.rs` = 2 (field + init) ✓
- `grep -c 'events_tcp: std::sync::atomic::AtomicU64' src/server/tcp.rs` = 2 ✓
- `grep -c 'events_tcp\.fetch_add' src/server/tcp.rs` = 4 (≥2 required) ✓
- `grep -c 'events_http' src/server/http_ingest.rs` = 3 ✓
- `proto="http"` and `proto="tcp"` present in http.rs format string (escaped as `{{proto=\"http\"}}`) ✓
- `grep -c 'beava_events_total' src/server/http.rs` = 6 (≥3 required) ✓
- `grep -c '#\[tokio::test\]' tests/http_auth.rs` = 7 ✓

## Deviations from Plan

None — plan executed exactly as written. The only implementation note: since the test suite doesn't include `reqwest` as a dependency, Task 3 uses the tower `oneshot` pattern with `app.clone()` per request (same as all other Phase 45 integration tests) rather than a real bound server with HTTP client. This is equivalent for counter-accuracy tests and avoids adding a new dependency.

## Known Stubs

None — all three tasks implement concrete functionality.

## Threat Flags

None — no new network endpoints, auth paths, file access patterns, or schema changes introduced. The new AtomicU64 fields are read-only observability counters.

## Self-Check: PASSED
