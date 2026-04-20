//! Phase 55 Wave 0 RED — SC-4 part 2: crash before cursor advance is idempotent.
//!
//! Contract (D-A3): Each source shard's primary event log tracks
//! `last_cascaded_lsn`. On crash BETWEEN primary-log fsync AND cursor
//! advance, boot replay re-drives the cascade for uncommitted LSNs. Because
//! `upsert_table_row` has full-replace semantics (D-B5), re-applying a
//! coalesced delta is idempotent — no double-count is possible.
//!
//! Wave 1 (plan 55-01) introduces `shard-N/cascade-cursor.postcard` +
//! boot-time replay. Wave 0 landing: `#[ignore = "55-W1"]`.
//!
//! Run:
//!   cargo test --release --test cross_shard_cascade_recovery -- --ignored

#![cfg(not(feature = "state-inmem"))]

#[path = "common/mod.rs"]
mod common;

#[allow(unused_imports)]
use common::cascade_harness::spawn_two_shards;

/// RED contract — crash window between cascade flush and cursor advance.
///
/// Scenario:
///   - N=2. Persist dir via `tempfile::tempdir()` so state survives a
///     simulated crash.
///   - Instrument the event log / cascade dispatcher: after primary-log
///     fsync but BEFORE `last_cascaded_lsn` advances, drop EngineState
///     without graceful shutdown (simulates SIGKILL mid-cursor-write).
///   - Restart the server against the same persist dir.
///   - Assertion A: boot replay re-runs cascade for every uncommitted
///     LSN (log records with `lsn > last_cascaded_lsn`).
///   - Assertion B: target table row has `count == 1` (NOT 2) — the
///     full-replace upsert absorbs the re-delivered delta as a no-op.
///   - Assertion C: after a clean shutdown, `last_cascaded_lsn` on disk
///     equals the primary event log's head LSN.
#[test]
#[ignore = "55-W1"]
fn crash_before_cursor_advance_is_idempotent_on_replay() {
    let _harness = spawn_two_shards(65_536);
    unimplemented!("Wave 1 — delivery-cursor crash-safety RED");
}
