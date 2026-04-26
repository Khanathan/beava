---
phase: 12
plan: 01
subsystem: engine+state
tags: [batch-primitives, async-push, coalescing, cascade, fan-out]
dependency-graph:
  requires: [v1.2 push_with_cascade_no_features, v1.2 event_log.append, v1.2 store.mark_dirty]
  provides: [event_log.append_many, store.mark_dirty_many, engine.push_batch_no_features, engine.push_batch_with_cascade_no_features]
  affects: [Phase 12 Wave 2 handle_push_batch, Phase 13 OP_PUSH_BATCH wire path]
tech-stack:
  added: []
  patterns: [batch amortization, per-event delegation under once-resolved metadata, fan-out filter precomputation]
key-files:
  created:
    - tests/test_batch_primitives.rs
  modified:
    - src/state/event_log.rs
    - src/state/store.rs
    - src/engine/pipeline.rs
decisions:
  - "Fan-out dispatch inlined in push_batch_with_cascade_no_features: mirrors TCP handler semantics (src/server/tcp.rs:364-398), not just push_with_cascade_no_features — the plan's fan-out test demanded it"
  - "Metadata resolution (get_stream, primary key_field, cascade_targets, filtered fan_out list) hoisted ONCE per call; per-event loop delegates to the existing single-event cascade worker for correctness first, fine-grained amortization later"
  - "Partial failures surface as Err in the per-event Vec slot in input order; no panic, no unwrap, no state corruption on bad input"
metrics:
  duration: ~25min
  tasks_completed: 3
  completed_date: 2026-04-11
requirements: [PERF-03]
---

# Phase 12 Plan 01: Batch Primitives Summary

Four batch-shaped building blocks now exist so Wave 2's `handle_push_batch` can amortize per-event fixed costs (lock acquire, event_log lookup, dirty-mark insert, stream metadata resolution) without having to invent new engine APIs under time pressure.

## One-liner

Added `event_log.append_many`, `store.mark_dirty_many`, `engine.push_batch_no_features`, and `engine.push_batch_with_cascade_no_features` — the cascade + fan-out aware variant mirrors the TCP handler's full push semantics under a once-per-call metadata lookup.

## What Shipped

### `EventLog::append_many(stream_name, &[&[u8]], SystemTime) -> io::Result<usize>`

Batch-append primitive. Performs a SINGLE `writers.get_mut` lookup then encodes and writes each event under that one writer handle. Returns `Ok(n)` for success, `Ok(0)` for empty input or unregistered stream (mirrors `append`'s `Ok(false)` contract). No errors on unregistered streams — the caller decides whether missing-stream is a failure.

File: `src/state/event_log.rs` (added after the existing `append` at line ~108).

### `StateStore::mark_dirty_many<I, S>(keys: I) where I: IntoIterator<Item=S>, S: Into<String>`

Batch dirty-mark primitive. Uses a single `extend` call into `dirty_keys: AHashSet<EntityKey>`. Idempotent. Does NOT touch `deleted_keys` — that would cross-mutate a set `mark_dirty` leaves alone and would silently change semantics under eviction races.

File: `src/state/store.rs` (added after `mark_dirty` at line ~207).

### `PipelineEngine::push_batch_no_features(stream_name, &[&Value], &mut StateStore, SystemTime) -> Vec<Result<FeatureMap, TallyError>>`

Primary-only batch push with no feature read. Resolves `get_stream` ONCE per call. Unknown stream returns `Vec<Err(Protocol)>` for every input event without touching state. For every other case, loops and delegates to `push_internal(_, _, _, _, false)` per event. Partial failures are captured in the returned Vec in input order; a failure at index `i` does NOT halt subsequent events.

Exposed for future consumers (Phase 13 wire path) that have already handled fan-out and cascade upstream. Phase 12's `handle_push_batch` will call `push_batch_with_cascade_no_features` instead.

File: `src/engine/pipeline.rs` (added after `push_no_features` at line ~368).

### `PipelineEngine::push_batch_with_cascade_no_features(stream_name, &[&Value], &mut StateStore, SystemTime) -> Vec<Result<FeatureMap, TallyError>>`

Cascade + fan-out aware batch push. Mirrors the TCP handler's full single-event push path:

**Once-per-call resolution (D-07):**
- `get_stream(stream_name)` — unknown primary short-circuits into per-event errors with zero state mutation
- primary `key_field`
- `get_cascade_targets(stream_name)` — full DAG cascade set
- `fan_out_targets()` filtered list excluding (a) the primary itself, (b) any target sharing the primary's key field, (c) any target already reached through cascade

**Per-event loop:**
1. Delegate to the existing single-event `push_with_cascade_no_features` worker for the primary + depends_on DAG cascade. Preserves v1.2 cascade semantics exactly.
2. If the primary succeeded, iterate the pre-filtered fan-out list and dispatch `push_no_features` to each target whose key field is present in the event. Mirrors `src/server/tcp.rs:364-398` fan-out semantics.

Returns `Vec<Result<FeatureMap, TallyError>>` in input order. Partial failures do not halt the batch. Feature maps are always empty (`no_features` mode skips the read + derive block).

The Phase 12 amortization win at the caller (`handle_push_batch`) is primarily **the AppState mutex is held once per batch**; fine-grained per-event metadata lookups inside the single-event worker are a Wave 3 follow-up if benches show they dominate.

File: `src/engine/pipeline.rs` (added after `push_with_cascade_no_features` at line ~611).

## Test Coverage — `tests/test_batch_primitives.rs`

17 tests across 4 modules:

- `append_many` (4): empty, 3 events roundtrip, unregistered stream returns `Ok(0)`, append+append_many interleave preserves write order.
- `mark_dirty_many` (3): empty iterator no-op, dedup across 5 keys → count 4, mirrors `mark_dirty` (does not scrub deleted set).
- `push_batch_no_features` (4): empty, 3 in-order success, partial failure preserves side effects on events 0 and 2, unknown stream errors all.
- `push_batch_with_cascade_no_features` (6): empty, unknown stream errors all, **cascade_equivalence_3_events** (engine A batch vs engine B sequential single-event pushes produce bit-identical feature state for every (stream, key) pair), **fan_out_single_update_per_event_on_target_key** (4 events sharing `merchant_id=m1` produce exactly 4 counts on MerchantActivity — not 1, not 16), error_order_preserved_on_partial_failure, unknown_stream_returns_errors_in_order_without_side_effects.

## Deviations from Plan

### [Rule 2 - Missing critical functionality] Fan-out dispatch inlined in `push_batch_with_cascade_no_features`

- **Found during:** Task 3 — the plan's `fan_out_single_update_per_event_on_target_key` test expected the batch primitive itself to apply fan-out to sibling keyed streams (`MerchantActivity`). My initial implementation delegated to `push_with_cascade_no_features`, which only walks the `depends_on` DAG. The test failed: `MerchantActivity.count_1h == None`.
- **Issue:** The single-event `push_with_cascade_no_features` does NOT perform fan-out; fan-out is a separate step in the TCP handler (`src/server/tcp.rs:364-398`). The plan's Task 3 description and load-bearing fan-out test conflicted on that point.
- **Fix:** Inlined the TCP handler's fan-out filter logic inside `push_batch_with_cascade_no_features`. The fan-out target list is resolved ONCE at entry (excluding self, same-key-field, and cascade overlap — mirroring the TCP handler's filter), then walked per-event to dispatch `push_no_features` to every target whose key field is present.
- **Files modified:** `src/engine/pipeline.rs`
- **Commit:** `ad75e47`

No other deviations. Plan executed as written for Tasks 1 and 2.

## Self-Check: PASSED

- `src/state/event_log.rs` — `append_many` present, 1 match via `grep -n "pub fn append_many"`
- `src/state/store.rs` — `mark_dirty_many` present, 1 match via `grep -n "pub fn mark_dirty_many"`
- `src/engine/pipeline.rs` — both `push_batch_no_features` and `push_batch_with_cascade_no_features` present, 1 match each
- `tests/test_batch_primitives.rs` — exists at 402 lines, well above the 200-line minimum
- Commits `70ae0e9`, `08b4ab9`, `ad75e47` all reachable from HEAD (`git log --oneline -5` confirms)
- `cargo test` full suite — 614 tests across 8 suites, all green
- No new Cargo.toml dependencies — `git diff Cargo.toml` is empty
- No new warnings introduced by this plan — the only warning (`handle_push_core` unused) is pre-existing from src/server/tcp.rs:251
