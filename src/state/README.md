# `src/state/`

Durable state — the WAL (write-ahead log), snapshots, eviction, and the
in-memory store that backs feature reads. Everything that must survive a crash
lives behind a surface in this module. The engine writes events; this module
ensures they persist and can be recovered.

## Files

- **`event_log.rs`** — `EventLog::append(entry)` is the hot WAL path: every
  ingested event is appended here before the engine processes it. Also contains
  `EventLog::append_with_fsync(entry)`, an async variant that awaits an fsync
  before returning (scaffolded in Phase 46 Plan 08; reserved for a future
  `?sync=1` HTTP flag that provides durable-ack semantics; not yet wired to any
  endpoint).
- **`snapshot.rs`** — base and delta snapshot cycle. Base snapshots are written
  every 5 minutes; deltas every 30 seconds (defaults). Recovery loads the
  latest base, applies every intervening delta, then replays the WAL from the
  resume cursor to reconstruct in-memory state with no data loss.
- **`eviction.rs`** — `evict_expired_stream_entries`; uses
  `engine.watermarks.observed_max(stream)` as the eviction clock source (the
  CORR-07 fix from Phase 46). Historical backfills therefore do not trip TTL
  prematurely because the watermark stays behind wall-clock during replay.
- **`eviction_tracker.rs`** — bookkeeping for eviction scheduling: tracks
  per-stream eviction cursors and deduplicates concurrent eviction calls.
- **`store.rs`** — the in-memory feature store: per-stream, per-key, per-feature
  slot map. Provides the read path for `/features/{stream}/{key}` queries.

## Contract

Anything the engine needs to survive a crash goes through this module's APIs.
If you are adding persistent state, do not write a one-off file — extend
`EventLog` or the snapshot format and preserve migration compatibility
(following the CORR-04 pattern from Phase 46).

## Read order

`event_log.rs` first — its `append` contract is the core durability promise.
Then `snapshot.rs` to understand crash recovery. `store.rs` for the read path.
`eviction.rs` last; it is a background maintenance task, not on the hot path.
