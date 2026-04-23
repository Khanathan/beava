---
phase: 06-wal-idempotency
plan: 02
status: complete
shipped: 2026-04-23
---

# Plan 06-02: Group-commit fsync worker + rotation + truncate_up_to

## Commits

- `674b1f7` — test(06-02): add fsync worker + rotation + truncate tests (RED)
- `ef78df1` — feat(06-02): WAL group-commit fsync worker + rotation + truncate_up_to (GREEN)

## Files created

| File | Role |
|---|---|
| `crates/beava-persistence/src/fsync_worker.rs` | WalSink + WalSinkConfig + worker_loop + flush_batch |
| `crates/beava-persistence/src/rotation.rs` | list_segments + truncate_up_to + rotate |
| `crates/beava-persistence/tests/fsync_worker.rs` | 5 tokio tests |
| `crates/beava-persistence/tests/rotation.rs` | 3 tokio tests |

## Tests

Plan 02 tests: 8/8 pass. Plan 01 tests still pass (7/7). Persistence crate total: 15/15.
Workspace test count: 395 (TestServer-based HTTP tests unchanged). Clippy + fmt clean.

## Decisions honored

- D-05 Group-commit strategy: background worker + tokio watch watermark + oneshot acks
- D-06 Default fsync coalesce: 2ms / 1 MiB
- D-13 Rotation: size-based 128 MiB segments (configurable)
- D-14 Truncation: closed-only; `next_start_lsn <= covered_lsn` test
- D-15 Phase 6 smoke injects snapshot-covered LSN (test handoff for Phase 7)

## Deviations

- **Inline fsync** (not spawn_blocking). Rationale: the tokio current_thread
  runtime used in tests has no blocking pool, and the production server runtime
  is multi-threaded so inline fsync in the worker task doesn't starve HTTP.
  Revisit in Phase 13 if bench shows fsync blocking is a P99 concern.
- `rotate()` drops the old `WalWriter` after calling `sync_data()` — its `Drop`
  impl flushes again which is a harmless no-op given the explicit sync_data.

## Handoff to Plan 03

- `WalSink::append_event(payload).await -> Lsn` is the single API the /push handler calls.
- `WalSink` is Clone — share across request handlers via `Arc<AppState>`.
- `WalSink::shutdown().await` must be invoked on graceful server shutdown (wired in Plan 03).
- `WalSink::truncate_up_to(covered_lsn)` exposes Phase 7's integration point.
