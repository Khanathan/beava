---
phase: 48-shard-hint-scaffolding
plan: 01
subsystem: routing
tags: [tpc, wave-0, shard-hint, ahash, tdd]
dependency_graph:
  requires: []
  provides: [shard_hint_for_event, routing module]
  affects: [src/server/tcp.rs, src/server/http_ingest.rs]
tech_stack:
  added: [rstest = "0.26"]
  patterns: [TDD red-green, ahash hash-as-u32, let _ discard pattern]
key_files:
  created:
    - src/routing/mod.rs
    - src/routing/shard_hint.rs
  modified:
    - src/lib.rs
    - Cargo.toml
    - src/server/tcp.rs
    - src/server/http_ingest.rs
decisions:
  - ahash::AHasher::default() used for hash; clippy::modulo_one suppressed in n1_modulo_always_zero test (intentional semantic doc)
metrics:
  duration: ~18 min
  completed: 2026-04-18
  tasks: 2
  files: 6
---

# Phase 48 Plan 01: Trait + Default Impl + TCP/HTTP Call-Site Wiring Summary

**One-liner:** `shard_hint_for_event` via ahash with 8 unit tests; 4 call-sites (2 TCP, 2 HTTP) all `let _shard_hint` discard — observationally inert at N=1.

## Files Created

| File | Purpose |
|------|---------|
| `src/routing/mod.rs` | Routing module root; re-exports `shard_hint_for_event` |
| `src/routing/shard_hint.rs` | `shard_hint_for_event` impl (ahash) + 8 unit tests |

## Files Modified

| File | Change |
|------|--------|
| `src/lib.rs` | Added `pub mod routing` declaration |
| `Cargo.toml` | Added `rstest = "0.26"` to `[dev-dependencies]` |
| `src/server/tcp.rs` | Call-site in `handle_push_core_ex` (line ~1294) and `handle_push_batch` (line ~1641) |
| `src/server/http_ingest.rs` | Call-site in `http_push_single` (line ~1171) and `http_push_batch` (line ~267) |

## Unit Tests (8/8 passing)

| Test | Behavior verified |
|------|------------------|
| `string_key_nonzero` | "alice" hashes to nonzero u32 |
| `numeric_key_graceful` | Non-string key returns 0, no panic |
| `composite_key_hashes_first_field` | "us-east" hashes to nonzero |
| `keyless_returns_zero` | None key_field returns 0 |
| `missing_field_returns_zero` | Absent field returns 0 |
| `n1_modulo_always_zero` | Any u32 % 1 == 0 (semantic doc) |
| `deterministic_same_key` | Same key → same hint across calls |
| `different_keys_likely_different` | "alice" ≠ "bob" hash |

## Call-Site Locations

| File | Function | Line (approx) | Pattern |
|------|----------|---------------|---------|
| `src/server/tcp.rs` | `handle_push_core_ex` | ~1294 | `let _shard_hint: u32 = { ... }` |
| `src/server/tcp.rs` | `handle_push_batch` | ~1641 | `let _shard_hint: u32 = ...` per-event |
| `src/server/http_ingest.rs` | `http_push_single` | ~1171 | `let _shard_hint: u32 = ...` |
| `src/server/http_ingest.rs` | `http_push_batch` | ~267 | `let _shard_hint: u32 = ...` per-event |

## D-01 Compliance

- Zero `shard_hint` fields on `Event`, `PendingAsync`, or any wire type
- `grep -rn "shard_hint" src/types.rs src/engine/ src/state/` → 0 matches
- All 4 call-sites use `let _shard_hint` (underscore prefix, value immediately discarded)

## Deviations from Plan

**1. [Rule 2 - Lint] Added `#[allow(clippy::modulo_one)]` to `n1_modulo_always_zero` test**
- **Found during:** clippy pass after GREEN phase
- **Issue:** Clippy flags `hint % 1 == 0` as "any number modulo 1 will be 0" — technically correct but the test is intentional semantic documentation
- **Fix:** Added `#[allow(clippy::modulo_one)]` with explanatory comment
- **Files modified:** `src/routing/shard_hint.rs`
- **Commit:** 003a0fd

## Self-Check: PASSED

- `src/routing/shard_hint.rs` — FOUND
- `src/routing/mod.rs` — FOUND
- Commit 003a0fd — FOUND
