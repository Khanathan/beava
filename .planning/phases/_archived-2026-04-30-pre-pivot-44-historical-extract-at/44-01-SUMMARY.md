---
phase: 44-historical-extract-at
plan: 01
subsystem: replica / historical-extraction
tags: [fork, replica, historical-extract, data-scientist-dx]
requires: [35-01, 36-01, 37-01, 39-01]
provides: [tl.fork-extract-at, /extracts-endpoint]
key-files:
  created:
    - python/tests/integration/test_fork_extract_history.py
  modified:
    - src/main.rs
    - src/server/replica_client.rs
    - src/server/tcp.rs
    - src/server/http.rs
    - python/tally/_fork.py
    - python/tests/test_fork_unit.py
    - docs/data-scientist-demo.md
decisions:
  - "Historical extraction is a single-replay cursor walk (not query-time time-travel). Snapshot state BEFORE applying any event whose ts crosses the next threshold; trailing thresholds snap against end-of-log state."
  - "Feature-map capture reuses StateStore::get_all_features(key, now) directly — no compute_features_for_key helper needed. Single call site, zero refactor."
  - "Inner DashMap<String, Value> was fine; no fallback to Mutex<HashMap> needed."
  - "E2E timing uses (before, after) wall-clock brackets around each PUSH so thresholds are GUARANTEED strictly between server-side ts_ms values. 1.5s gaps + 15s settle keeps 9/9 parametrized runs green deterministically."
metrics:
  duration_minutes: ~55
  commits: 4
  rust_tests_added: 4
  python_unit_tests_added: 3
  python_integration_tests_added: 3
completed: 2026-04-14
---

# Phase 44 Plan 01: Historical extraction at timestamps (one-pass replay) — Summary

**One-liner:** `tl.fork(extract_at=[T1, T2, T3])` captures feature state at each threshold during a single replay; `fork.extract_history()` returns `{iso_ts: {key: features}}`.

## What shipped

Four atomic commits (T1→T4):

1. **T1 — `feat(44-01)`** (`9a961ba`): `ReplicaBootConfig.extract_at_millis: Vec<u64>` (sorted), `--replica-extract-at` server flag, `--extract-at` fork CLI flag, `ConcurrentAppState.extracted_history: DashMap<u64, DashMap<String, Value>>`, replica-client historical-catchup cursor that snapshots BEFORE applying each event crossing a threshold (and after `END` for trailing thresholds).
2. **T2 — `feat(44-01)`** (`eca2440`): `GET /extracts` handler on the admin-gated router. Sorts by timestamp, formats keys as ISO-8601 UTC whole seconds (reuses `format_rfc3339_utc`), returns `{"extracts": {...}}`.
3. **T3 — `feat(44-01)`** (`6eccf50`): Python `tl.fork(extract_at=...)` kwarg accepting `datetime|int|str`, `ForkedReplica.extract_history()` HTTP fetch + parse, 3 new pytest cases.
4. **T4 — `test(44-01)`** (`3e5c827`): E2E integration test (parametrized 3x) + `docs/data-scientist-demo.md` Path C section.

## Verification

- `cargo test` → all green (1 pre-existing statistical HLL flake rerolled green; not related).
- `cargo test --bin tally fork_cli_tests` → 15/15 green (4 new).
- `scripts/check-feature-builds.sh` → green (every feature flavor builds).
- `pytest python/tests/` → **483 passed**.
- `pytest python/tests/integration/test_fork_extract_history.py` → **9/9** (3 parametrizations × 3 consecutive runs).
- `pytest python/tests/test_fork_unit.py` → **28/28** (3 new).

## Sample `extract_history()` output

From the E2E test at t0 ≈ 2026-04-14T…Z with three checkpoints:

```python
{
  "2026-04-14T21:37:12Z": {
    "u1": {"count": 1, "total": 10.0},
    "u2": {"count": 1, "total": 5.0},
  },
  "2026-04-14T21:37:15Z": {
    "u1": {"count": 2, "total": 30.0},
    "u2": {"count": 1, "total": 5.0},
  },
  "2026-04-14T21:37:20Z": {
    "u1": {"count": 3, "total": 60.0},
    "u2": {"count": 2, "total": 20.0},
  },
}
```

## Deviations from Plan

None of Rules 1-4 triggered. Plan executed as written with two small design confirmations:

1. **[Confirmation] `debug_key` extraction helper not needed.** `store.get_all_features(key, now)` is the clean reuse path — used by debug_key AND the new snapshot hook. No shared `compute_features_for_key` helper needed.
2. **[Confirmation] Inner `DashMap<String, Value>` fine.** No lifetime/serialization issues; outer+inner DashMap ships clean.

## Threat surface

No new network surface; `/extracts` sits behind the existing admin router (same `require_loopback_or_token` middleware as other `/debug/*` routes). No threat flags raised.

## Known Stubs

None.

## Self-Check: PASSED

- Created: `python/tests/integration/test_fork_extract_history.py` — FOUND
- Commits: `9a961ba`, `eca2440`, `6eccf50`, `3e5c827` — all FOUND in `git log`
