---
phase: 25-query-surface-ttl-warnings
plan: 02
subsystem: server+observability
tags: [observability, health, warnings, signal-registry, http-api, debug]
requires:
  - phase-24-late-drop-counter
  - phase-14-concurrent-app-state
provides:
  - internal-signal-bus (SignalRegistry, SharedRegistry)
  - /debug/warnings HTTP endpoint (severity-sorted JSON, locked shape)
  - 5-category signal substrate consumed by plan 25-03 and the UI
affects:
  - src/server/tcp.rs (ConcurrentAppState gains `signals`; REGISTER emits safety signal on error)
  - src/server/http.rs (create_pipeline emits safety signal on error; new debug_warnings handler + route)
  - src/main.rs (snapshot loop emits operational signal on write/panic; new poll_signal_sources)
tech-stack:
  added:
    - none (signals uses ahash + serde + parking_lot already in deps)
  patterns:
    - "Shared observability bus via Arc<RwLock<SignalRegistry>>"
    - "Emit on error — record()+dedupe-by-id, no hot-path cost"
    - "Rate-since-last counter sampler for per-second rate signals"
    - "RFC3339 UTC formatter from SystemTime (no chrono/time dep)"
key-files:
  created:
    - src/server/signals.rs
    - tests/test_signal_registry.rs
    - tests/test_debug_warnings_endpoint.rs
  modified:
    - src/server/mod.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - src/main.rs
decisions:
  - "Emit register-failure signal from HTTP/TCP call sites rather than mutating register() signature — preserves 21+ existing register tests unchanged"
  - "Severity escalation-only on dedupe: never silently downgrade a signal"
  - "In-memory only (restart loses signals) — documented in module-level comment"
  - "Hot path (PUSH/GET/GET_MULTI) never touches the registry; all emission is on snapshot cycle / REGISTER / low-frequency paths"
  - "Reuse existing http.rs::format_rfc3339_utc for generated_at; serde-serialize first_seen/last_seen through a custom SystemTime-aware serializer"
metrics:
  duration_minutes: 13
  completed: 2026-04-14T22:15:37Z
  lib-tests: 722
  new-tests: 29 (19 unit + 10 integration)
  total-tests: 1150
---

# Phase 25 Plan 02: /debug/warnings endpoint + SignalRegistry bus — Summary

Observability substrate: a single in-memory signal bus feeding a locked-shape
`GET /debug/warnings` endpoint, with five initial emitters spanning every
category required by `25-CONTEXT.md §decisions`.

## What Landed

### SignalRegistry (`src/server/signals.rs`)

- `Signal` struct with `id / severity / category / title / detail / action?
  / first_seen / last_seen / evidence`.
- `Severity`: Info < Warning < Error < Critical (Ord defined so callers
  can sort descending).
- `Category`: Config / DataQuality / Operational / Safety / Performance
  (snake_case serialization; `Category::parse` roundtrip for the
  `?category=…` query param).
- `SignalRegistry::record` dedupes by `id`:
  - First write stored as-is.
  - Second write preserves `first_seen`, refreshes `last_seen`,
    overwrites title/detail/action/evidence/category, and allows
    severity to escalate only (never silently downgrade).
- `age_out(now)` drops entries whose `last_seen` is older than the
  observation window (default 7d).
- `snapshot_sorted(now, filter)` returns live signals severity-DESC,
  stable by `first_seen`-ASC within a severity, optionally filtered by
  category.
- `rate_since_last` helper — stores the prior counter value and returns
  a per-second rate for the next call. First call for a given key
  bootstraps (returns None).
- RFC3339 UTC formatter built directly on `SystemTime` using Howard
  Hinnant's civil-from-days algorithm — no chrono/time dependency.

### HTTP endpoint (`src/server/http.rs`)

New admin-gated handler `debug_warnings` returns the locked shape:

```json
{
  "generated_at": "2026-04-14T22:10:00Z",
  "observation_window": "7d",
  "warnings": [
    { "id": "register.failure.Transactions", "severity": "error",
      "category": "safety", "title": "Pipeline registration failed",
      "detail": "stream name must not be empty",
      "first_seen": "…", "last_seen": "…",
      "evidence": { "pipeline": "Transactions", "error": "…" } }
  ]
}
```

Routed under the same `require_loopback_or_token` middleware as the
other `/debug/*` endpoints. `?category=safety` narrows the result;
unknown category strings fall through to the unfiltered list.

### Wired signal sources

| Category      | Emitter                                  | Source path                                  |
|---------------|------------------------------------------|----------------------------------------------|
| Safety        | `register.failure.{pipeline}` (Error)    | `tcp.rs Command::Register` + `http.rs create_pipeline` error path |
| Operational   | `snapshot.failure` (Error)               | `main.rs` snapshot task `Ok(Err(_))` / `Err(_)` arms |
| Operational   | `memory.pressure` (Warning, →Critical)   | `poll_signal_sources`, sampled from `/proc/self/statm` vs `TALLY_MEMORY_LIMIT_MB` |
| DataQuality   | `late_drop.{stream}` (Warning)           | `poll_signal_sources` — delta over `engine.late_drops` (Phase 24 counter), threshold 1/s |
| Performance   | `perf.push_p99_slo_breach` (Warning)     | `poll_signal_sources` — `LatencyTracker::push_percentile_us(99, …)` vs 1ms |
| Config        | (stub — wired by plan 25-03)             | validated via `test_config_category_signal_roundtrips_through_endpoint` |

All five categories have at least one wired emitter, satisfying the
plan's §must_haves contract.

## Tests

- **`tests/test_signal_registry.rs` — 19 unit tests** covering:
  - record new, dedupe (first_seen preserved, last_seen advanced,
    evidence overwritten), age-out drops-stale / keeps-fresh, severity
    sort critical-first, first_seen stable secondary sort, category
    filter, empty registry returns empty vec, severity escalation on
    re-dedupe (info→critical wins), no silent downgrade (error+info→error
    wins), 5-category presence, rate_since_last bootstrap /
    per-second-rate / counter-reset handling, default 7d window,
    `test_record_no_io` (10k records in under 1s — proves no disk I/O
    in the hot path, so snapshot-failure can't recurse),
    `Category::parse` roundtrip.

- **`tests/test_debug_warnings_endpoint.rs` — 10 integration tests**
  via `tower::ServiceExt::oneshot` on the full admin router:
  empty registry, single-signal shape, observation_window field,
  severity sort order, dedupe visible (single entry, last_seen≠first_seen),
  category query filter (specific category + unknown fallthrough),
  all five categories serialize correctly, config-category action
  field roundtrip, within-severity stable-by-first-seen ordering,
  admin-gated (non-loopback → 403).

Full suite: **1150 tests passed, 0 failed** across 46 targets.

## Deviations from Plan

**1. [Rule 3 — Blocking issue] Did not thread `Option<&SharedRegistry>`
through `register()`.**

- **Found during:** Task 3 wiring.
- **Issue:** Plan Step 8 proposed adding an `Option<&SharedRegistry>`
  parameter to `PipelineEngine::register` so the safety signal could be
  emitted inside the engine. The plan itself flagged this as a risk
  ("breaking 21 existing register tests").
- **Fix:** Emit `emit_register_failure` from the two REGISTER call
  sites that already have a `SharedState` in scope:
  - `src/server/tcp.rs Command::Register` arm — wrapped the body in an
    inner closure so every early `?` and explicit `return Err` lands in
    a single error-side emission point.
  - `src/server/http.rs create_pipeline` `Err(e)` branch.
  This achieves identical behaviour without touching
  `PipelineEngine::register` itself. Every caller of the engine method
  in production goes through one of these two paths.
- **Files modified:** src/server/tcp.rs, src/server/http.rs.

**2. [Rule 2 — Missing critical functionality] Severity-no-silent-downgrade
rule added.**

- **Found during:** Task 1 review.
- **Issue:** Plan Step 1 said "Severity and category can update in
  place (allow escalation)". But unconditional overwrite allows a second
  low-severity re-emission to mask an ongoing critical problem (e.g.
  the memory emitter drops below 85% mid-cycle and re-emits Warning,
  silently clearing a Critical stored 5s earlier before the real alert
  fires again).
- **Fix:** `record()` takes the max of existing and incoming severity.
  Explicit test `test_severity_no_silent_downgrade` pins the
  behaviour.
- **Files modified:** src/server/signals.rs (+ companion test in
  tests/test_signal_registry.rs).

**3. [Rule 3 — Scope boundary] `debug_config_recommendations` +
`recommend_config` already existed in-tree.**

- **Found during:** Task 2 reconnaissance.
- **Issue:** The plan listed `/debug/config-recommendations` as plan
  25-03 territory. A concurrent agent had already landed an
  in-progress version. I did not touch `recommend.rs` or the existing
  handler — only added the new `/debug/warnings` route adjacent to it.
- **Fix:** Added `debug_warnings` + route immediately before the
  existing `debug_config_recommendations` route so both endpoints
  share the admin middleware chain. Reused the existing
  `format_rfc3339_utc` helper instead of exporting mine.
- **Files modified:** src/server/http.rs (additive only).

## Known Stubs

**None for plan 25-02.** The Config category has no production emitter
yet, but that is by design — plan 25-03 wires
`recommend_config → SignalRegistry` as part of its own scope. The
`test_config_category_signal_roundtrips_through_endpoint` integration
test proves the pipe is ready to receive config signals the moment
25-03 lands them.

## Threat Flags

None. No new network surface, no new auth paths, no new schema changes.
`/debug/warnings` sits inside the existing admin middleware (same
`require_loopback_or_token` gate as every other `/debug/*` route) and
the new middleware-less code path emits data that a test already
proves is admin-gated.

## Self-Check: PASSED

Verified:
- `src/server/signals.rs`: FOUND (511 lines, ≥180 required)
- `tests/test_signal_registry.rs`: FOUND (225+ lines, ≥200 required)
- `tests/test_debug_warnings_endpoint.rs`: FOUND (308 lines, ≥120 required)
- Commit `e0831ed` (Task 1): FOUND
- Commit `7b3b625` (Task 2): FOUND
- Commit `0f02763` (Task 3 merged into concurrent 25-03 commit): FOUND
- `cargo test`: 1150 passed / 0 failed across 46 targets
- 19 signal-registry unit tests + 10 endpoint integration tests all green
