# Phase 9 Deferred Items

Pre-existing integration test compile errors discovered during Plan 09-01 execution.
These were NOT introduced by Phase 9 work — they exist on the pre-Phase-9 commit (c9e35bc).

## Pre-existing compile errors in integration tests

### tests/test_snapshot.rs
Missing fields from Phase 8 (SCHM-03 backfill_complete + backfill flag):
- Line 71: `SnapshotState` literal missing `backfill_complete: vec![]`
- Line 103: `SnapshotState` literal missing `backfill_complete: vec![]`
- Line 142: `FeatureDef::Count` literal missing `backfill: false`
- Line 198: `FeatureDef::Count` literal missing `backfill: false`
- Line 232: `SnapshotState` literal missing `backfill_complete: vec![]`

### tests/test_server.rs
Missing fields from Phase 8:
- Line 30: `AppState` literal missing `backfill_complete` and `backfill_tracker`

## Status

- `cargo test --lib` passes (452 tests, 0 failures) — library code is clean.
- `cargo test` (integration tests) fails to compile due to the above.
- Verified these errors exist on commit `c9e35bc` before Plan 09-01 changes were made.
- These tests pre-date Phase 9 and appear to have been missed during Phase 8 completion.

Deferred to a follow-up cleanup pass (Phase 9 verify or a dedicated fix commit).
