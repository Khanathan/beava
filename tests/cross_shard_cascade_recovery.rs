//! Phase 55 Wave 1 GREEN — SC-4 part 2: crash before cursor advance is idempotent.
//!
//! Contract (D-A3): Each source shard's primary event log tracks
//! `last_cascaded_lsn`. On crash BETWEEN primary-log fsync AND cursor
//! advance, boot replay re-drives the cascade for uncommitted LSNs. Because
//! `upsert_table_row` has full-replace semantics (D-B5), re-applying a
//! coalesced delta is idempotent — no double-count is possible.

#![cfg(not(feature = "state-inmem"))]

use std::path::PathBuf;
use std::sync::Arc;

use beava::state::event_log::EventLog;

#[path = "common/mod.rs"]
mod common;

/// Contract — cursor survives drop/reload; advance is monotonic; and
/// re-applying the same `UpsertTableBatch` to a target shard yields
/// identical state (idempotent full-replace).
#[test]
fn crash_before_cursor_advance_is_idempotent_on_replay() {
    let tmp = tempfile::TempDir::new().expect("tmp");
    let data_dir: PathBuf = tmp.path().to_path_buf();

    // Phase A — initial "pre-crash" run. Advance cursor for stream "Txn"
    // to LSN=42 and fsync it to disk. Drop the EventLog without graceful
    // shutdown to simulate a process crash (data_dir persists).
    {
        let el = Arc::new(EventLog::new_for_shard(data_dir.clone(), 0).expect("new el"));
        el.advance_cascaded_lsn("Txn", 42);
        el.fsync_cascade_cursor().expect("persist cursor");
    }

    // Phase B — "restart" against same data_dir. Cursor loads from disk
    // at the value previously persisted.
    let el2 = EventLog::new_for_shard(data_dir.clone(), 0).expect("reopen");
    assert_eq!(
        el2.cascaded_lsn("Txn"),
        42,
        "cursor MUST survive drop/reload"
    );

    // Monotonic advance — new_lsn < current is a no-op.
    el2.advance_cascaded_lsn("Txn", 10);
    assert_eq!(el2.cascaded_lsn("Txn"), 42, "monotonic: no regression");

    // Forward advance works.
    el2.advance_cascaded_lsn("Txn", 100);
    assert_eq!(el2.cascaded_lsn("Txn"), 100);

    // Phase C — idempotent re-delivery of the same cascade batch.
    // Simulated by applying `upsert_table_row` to a shard twice with the
    // same (key, table, fields) — second write fully replaces first,
    // which is identical input, so state is unchanged.
    let (_ks, partitions, _tmp_ks, _cfg) = common::ephemeral_test_keyspace(1);
    let mut parts = partitions.into_iter();
    let mut shard = beava::shard::Shard::with_partition(parts.next().unwrap());

    use ahash::AHashMap;
    use beava::types::FeatureValue;
    let mut fields = AHashMap::new();
    fields.insert("count".to_string(), FeatureValue::Int(1));
    let now = std::time::SystemTime::now();
    shard.upsert_table_row("mX", "M", fields.clone(), now);
    // Replay — same write again.
    shard.upsert_table_row("mX", "M", fields.clone(), now);

    // Read back — count is STILL 1, not 2. Full-replace absorbs replay.
    let cnt = beava::shard::read_entity_from_shard(&shard, "mX", |e| {
        e.table_rows
            .get("M")
            .and_then(|r| match &r.state {
                beava::state::store::TableRowState::Live => r.fields.get("count").cloned(),
                _ => None,
            })
    })
    .flatten();
    assert_eq!(
        cnt,
        Some(FeatureValue::Int(1)),
        "idempotent full-replace — re-delivery MUST NOT double-count"
    );
}
