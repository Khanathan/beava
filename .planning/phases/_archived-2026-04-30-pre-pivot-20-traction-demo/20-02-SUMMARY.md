---
phase: 20-traction-demo
plan: 02
subsystem: http-server, frontend
tags: [traction-demo, http, auth, middleware, metrics, frontend, security]
dependency_graph:
  requires:
    - phase-10.2 (RollingHistogram, ThroughputTracker)
    - phase-10 (rust-embed UI pipeline)
    - phase-14 (ConcurrentAppState)
  provides:
    - public_http_surface
    - admin_auth_gate
    - loopback_tcp_bind_default
    - public_demo_page
    - extended_prometheus_metrics
  affects:
    - phase-20-03 (deploy wiring — Caddyfile + smoke.sh reference these routes)
tech-stack:
  added: []
  patterns:
    - axum middleware::from_fn_with_state for admin gate
    - ConnectInfo<SocketAddr> via into_make_service_with_connect_info
    - router split (public.merge(admin)) with shared state
    - rust-embed debug-embed for runtime asset fetch
key-files:
  created:
    - src/server/auth.rs
    - src/server/ui/demo.html
    - src/server/ui/demo.css
    - src/server/ui/demo.js
    - tests/test_admin_auth.rs
    - tests/test_public_http.rs
    - tests/test_demo_page.rs
    - tests/test_tcp_bind.rs
  modified:
    - src/server/http.rs
    - src/server/tcp.rs
    - src/server/latency.rs
    - src/server/throughput.rs
    - src/server/mod.rs
    - src/main.rs
decisions:
  - Admin routes gated via route_layer + from_fn_with_state, public surface merged after
  - TCP listener default binds 127.0.0.1 (security-critical; raw protocol has no auth)
  - RecentEventsRing lives in AppState (capacity 100, O(1) push) — independent of Phase 6 event log
  - CORS header only on /public/* responses; admin/metrics unchanged
  - Extended /metrics p99 reported in seconds (Prometheus convention), /public/stats reports µs
metrics:
  duration_seconds: 620
  tasks_completed: 3
  tests_added: 23
  completed_date: 2026-04-14
---

# Phase 20 Plan 02: Public HTTP Surface + Admin Auth Summary

Read-only public HTTP surface (`/public/features/:key`, `/public/recent-events`, `/public/stats`) plus `require_loopback_or_token` middleware for admin routes, extended `/metrics` (events_total, current_eps, p99 PUSH latency), loopback-default TCP bind, and a 90-LOC vanilla-JS demo page served via rust-embed.

## What Shipped

### Task 1 — Admin auth middleware, TCP loopback default, state plumbing (commit `15700b9`)
- **`src/server/auth.rs`** — `require_loopback_or_token` middleware admits loopback peers unconditionally, non-loopback peers only with matching `Authorization: Bearer <admin_token>`. Pure `resolve_tcp_bind(env, cli, port)` helper isolates bind-address resolution for unit testing.
- **`src/server/http.rs`** — `build_router` split: `public_router` (health, metrics, `/public/*`, `/`, `/static/*`) merged with `admin_router` (pipelines, snapshot, `/debug/*`) wrapped in `middleware::from_fn_with_state(state.clone(), require_loopback_or_token)` via `route_layer`. Server now uses `into_make_service_with_connect_info::<SocketAddr>()` so `ConnectInfo` is populated at runtime.
- **`src/server/tcp.rs`** — `ConcurrentAppState` extended with `admin_token: Option<String>`, `started_at: Instant`, `recent_events: Mutex<RecentEventsRing>`, `public_mode: bool`. `RecentEventsRing` is a bounded `VecDeque<RecentEvent>` with capacity 100 — O(1) push, O(n) snapshot. Legacy `make_concurrent_state` preserved; new `make_concurrent_state_full` carries the extra fields. Recent-events ring is populated from both `handle_push_core_ex` (sync) and `handle_push_batch` (async/batch) so the feed works end-to-end regardless of which path ingests the event.
- **`src/server/latency.rs`** — `push_percentile_us(q, now)` exposes a single-value percentile read without leaking the histogram internals. Used by `/public/stats` and `/metrics`.
- **`src/server/throughput.rs`** — `eps_5s()` / `eps_60s()` sum per-stream EWMAs to a global EPS figure.
- **`src/main.rs`** — `arg_value("tcp-bind")` + `TALLY_TCP_BIND` feed `resolve_tcp_bind`; TCP listener defaults to `127.0.0.1`. `TALLY_ADMIN_TOKEN` and `--public-mode` / `TALLY_PUBLIC_MODE` flow into `make_concurrent_state_full`.
- **Tests** — `tests/test_admin_auth.rs` (10 cases: loopback × public × token × wrong-token × no-config on `/debug/memory`, `/pipelines`, `/snapshot`, `/metrics`, `/health`) and `tests/test_tcp_bind.rs` (2 cases: loopback default + precedence CLI>env>default).

### Task 2 — Public routes + extended metrics (commit `3bedb33`)
Handlers landed in Task 1's refactor; this commit added the 8-case integration test suite. Tests exercise the full router over real loopback HTTP so the middleware stack is covered end-to-end.

- `GET /public/features/{key}` → `{"key", "features": {…}}` with **only** `name → scalar` values (no `buckets`, `hll`, `operator_state`, `estimated_bytes`, `live_operators`). 404 on unknown key.
- `GET /public/recent-events?limit=N` → `{"events": [...]}` with `{ts, stream, key, payload_preview}`. Default 20, clamped to ring capacity 100.
- `GET /public/stats` → `{events_total, current_eps, p99_push_us, p50_push_us, uptime_seconds, keys_total}`. All six fields numeric.
- All `/public/*` responses carry `Access-Control-Allow-Origin: *` for cross-origin blog embeds.
- `GET /metrics` gains `tally_events_total`, `tally_push_latency_p99_seconds` (seconds per Prometheus convention), `tally_current_eps`.

### Task 3 — Demo page + rust-embed wiring (commit `a086079`)
- **`src/server/ui/demo.{html,css,js}`** — 90 LOC total (cap: 200). Three counter tiles, key-lookup form, recent-events feed, 2s polling. No external CDN, no build step.
- **`GET /`** dispatches on `state.public_mode`: demo.html when true, existing debug UI otherwise. Keeps `/static/*` unchanged — both pages load assets from the same embed root.
- **Tests** — `tests/test_demo_page.rs` (3 cases: public-mode serves demo, debug-mode does not, `demo.js` embedded and references `/public/stats`).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 — Bug] Pre-existing `server::http::tests` tests broke after router split**
- **Found during:** Task 1, full-suite regression after adding `ConnectInfo` middleware.
- **Issue:** Two existing unit tests (`test_snapshot_trigger_returns_409_when_in_progress`, `test_snapshot_trigger_returns_404_when_disabled`) used `oneshot` without injecting `ConnectInfo<SocketAddr>`. After the middleware wrap, axum's `ConnectInfo` extractor returned 500 instead of the expected 409/404.
- **Fix:** Added `inject_loopback` helper in the test module that inserts a `ConnectInfo(127.0.0.1:1)` extension into each request. Tests now pass through the gate (loopback bypass) and reach the handler.
- **File:** `src/server/http.rs`
- **Commit:** `15700b9`

**2. [Rule 2 — Missing critical functionality] Plan called for `CMD_PUSH` const from `latency.rs`**
- **Found during:** Task 2 planning.
- **Issue:** Plan's pseudocode referenced `LatencyTracker.command_histograms[CMD_PUSH]`, but `CMD_PUSH` does not exist — the real type is `enum CommandKind { Push=0, … }` and `command_histograms` is private.
- **Fix:** Added `LatencyTracker::push_percentile_us(q, now)` public accessor that snapshots the PUSH histogram and returns the requested percentile in microseconds. Zero new internals leaked.
- **File:** `src/server/latency.rs`
- **Commit:** `15700b9`

**3. [Rule 2 — Missing critical functionality] Plan called for `ThroughputTracker::eps_5s()` — didn't exist**
- **Found during:** Task 2 planning.
- **Issue:** Plan's `/public/stats` pseudocode used `s.throughput.eps_5s()`, but the existing `ThroughputTracker` only exposes per-stream EWMAs via `snapshot()`.
- **Fix:** Added `eps_5s()` / `eps_60s()` global aggregates that sum `ewma_5s` / `ewma_1m` across every tracked stream. Matches the semantic the plan intended: "events per second across the whole server."
- **File:** `src/server/throughput.rs`
- **Commit:** `15700b9`

**4. [Rule 3 — Blocking] Plan specified `tests/integration/test_*.rs` paths**
- **Issue:** Rust's cargo test runner expects flat `tests/*.rs`; existing convention in this repo (see `tests/test_debug_ui.rs`, `tests/test_server.rs`, etc.) is flat, not nested.
- **Fix:** Placed all four new test files at `tests/test_*.rs` (matching the repo convention and what `cargo test --test test_admin_auth` actually resolves).
- **Commits:** `15700b9`, `3bedb33`, `a086079`

**5. [Rule 1 — Bug] `rust-embed` compile-time scan cached stale demo.{html,css,js} list**
- **Found during:** Task 3 initial test run.
- **Issue:** First run of `test_demo_page` returned 404 for `/static/demo.js` and `/` because rust-embed's `debug-embed` feature had cached the embed tree from a previous compile that didn't yet include the new files.
- **Fix:** `cargo clean -p tally` forced a rebuild; subsequent runs are stable because the new files are part of the source tree. Not a runtime issue — only the developer's first build after adding files to `src/server/ui/` needs the clean.
- **Commit:** N/A (toolchain quirk, documented here so reviewers don't trip on it)

## Authentication Gates

None. All work is server-side Rust; no external services to authenticate against.

## Threat Flags

Every `/public/*` route was explicitly designed around the CONTEXT.md "No public PUSH/SET/MSET/REGISTER" constraint. Each response was scrubbed before shipping:

| Surface                   | Leak Check                                                                 | Status |
| ------------------------- | -------------------------------------------------------------------------- | ------ |
| `/public/features/{key}`  | `public_features_no_operator_state` asserts no `buckets`/`hll`/`operator_state`/`estimated_bytes`/`live_operators` appears | PASS |
| `/public/recent-events`   | `payload_preview` capped at 200 chars, UTF-8-boundary safe truncation        | PASS |
| `/public/stats`           | Exposes only aggregates — no per-key, per-stream, or operator detail       | PASS |
| `/metrics`                | Prometheus output already public; only added aggregates, no per-key data   | PASS |
| `/` in public mode        | Serves static `demo.html` — no server-side state interpolation             | PASS |
| Admin routes              | 403 from non-loopback without bearer token; 403 when no token configured   | PASS |

Tested end-to-end: `test_admin_auth::public_without_token_config_forbidden` proves the fail-closed default (no token → no non-loopback access).

## Key Links Verified

- `src/server/http.rs::build_router` → `require_loopback_or_token` via `route_layer(middleware::from_fn_with_state(state.clone(), require_loopback_or_token))` — matches plan's `admin_router.*layer` pattern.
- `src/server/ui/demo.js` → `fetch('/public/stats')` in `poll()`, `setInterval(poll, 2000)` — matches plan's `fetch\\(.*/public/stats` pattern.
- `src/server/http.rs::public_stats` → `state.latency.lock().push_percentile_us(99.0, now)` — matches plan's `percentile\\(99` pattern (via accessor, not direct index).
- `src/server/http.rs::public_stats` + `metrics_endpoint` → `state.throughput.lock().eps_5s()` — matches plan's `eps_5s|throughput` pattern.

## Test Coverage Added

| File | Cases | Focus |
|---|---|---|
| `tests/test_admin_auth.rs` | 10 | loopback pass, public deny, token pass, wrong-token deny, no-config deny, `/metrics` + `/health` always open |
| `tests/test_tcp_bind.rs` | 2 | loopback default (security-critical), CLI>env>default precedence |
| `tests/test_public_http.rs` | 8 | feature map shape, no-internals leak, 404 unknown key, limit clamp (default 20, max 100), stats shape, CORS, metrics extension |
| `tests/test_demo_page.rs` | 3 | public-mode serves demo, debug-mode preserves debug UI, demo.js embedded + references `/public/stats` |
| **Total** | **23** | |

Plus 2 existing `server::http::tests` unit tests fixed to inject `ConnectInfo` (loopback helper).

Full suite: **833 tests passing, 0 failing** (vs baseline 810 before the plan).

## LOC Budget

| Area | LOC Added | Budget | Note |
|---|---:|---:|---|
| Rust src/ | ~640 | ≤450 | Over — mostly docstrings + new `RecentEventsRing` struct (~70 LOC) + extended `/metrics` output. Every line documented. |
| Rust tests/ | ~600 | — | |
| demo.html+css+js | 90 | ≤200 | 55% under cap |

The Rust src overage is driven by (a) documentation comments (~40% of diff lines), (b) the `RecentEventsRing` type which the plan specified but didn't budget for, (c) preserving the legacy `make_concurrent_state` signature via a delegation wrapper rather than breaking callers. Not a regression risk — build + full test suite both pass.

## Manual Verification (ready for operator)

```bash
# Public mode
TALLY_ADMIN_TOKEN=secret TALLY_PUBLIC_MODE=1 cargo run --release -- --tcp-bind 127.0.0.1
# Visit http://localhost:6401/  -> demo tiles update every 2s

# Admin denied over LAN without token
curl -X DELETE http://<LAN-IP>:6401/pipelines/Transactions          # -> 403
curl -X DELETE -H "Authorization: Bearer secret" http://<LAN-IP>:6401/pipelines/Transactions  # -> 200/404

# Prometheus metrics
curl -s http://localhost:6401/metrics | grep -E "tally_(events_total|push_latency_p99_seconds|current_eps)"
```

## Hand-off Notes for Plan 20-03

- Admin token → Caddyfile should strip and forward `Authorization: Bearer` unchanged to `127.0.0.1:6401`.
- `--tcp-bind 127.0.0.1` is the default; `deploy/tally.service` can drop the explicit flag but should keep it for documentation. Smoke test invariant `! nc -z -w 2 $PUBLIC_IP 6400` already satisfied.
- `/public/*` + `/metrics` + `/health` should be proxied open; `/pipelines`, `/snapshot`, `/debug/*` should 403 at the edge regardless of token (defense in depth).
- Demo assets are embedded — no separate static hosting needed.

## Self-Check: PASSED

- [x] `src/server/auth.rs` exists (87 LOC)
- [x] `src/server/ui/demo.html` exists (40 LOC)
- [x] `src/server/ui/demo.css` exists (23 LOC)
- [x] `src/server/ui/demo.js` exists (27 LOC)
- [x] `tests/test_admin_auth.rs` exists (161 LOC, 10 cases)
- [x] `tests/test_public_http.rs` exists (287 LOC, 8 cases)
- [x] `tests/test_demo_page.rs` exists (117 LOC, 3 cases)
- [x] `tests/test_tcp_bind.rs` exists (36 LOC, 2 cases)
- [x] Commit `15700b9` present in `git log --all` (Task 1)
- [x] Commit `3bedb33` present in `git log --all` (Task 2)
- [x] Commit `a086079` present in `git log --all` (Task 3)
- [x] `cargo test` passes — 833 passed, 0 failed
- [x] Combined demo LOC 90 ≤ 200 cap

## Known Stubs

None. All wired to live data sources.
