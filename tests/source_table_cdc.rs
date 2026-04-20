//! Phase 55 Wave 0 RED — SC-2 (source-table wire format) + SC-3 (delete semantics).
//!
//! Contract (TPC-SOURCE-01):
//!   - TCP opcodes OP_UPSERT_TABLE_ROW (0x14), OP_DELETE_TABLE_ROW (0x15),
//!     OP_UPSERT_TABLE_BATCH (0x16), OP_DELETE_TABLE_BATCH (0x17) carry
//!     `source_lsn: u64` echoed on ack.
//!   - HTTP routes POST /table/{name}, DELETE /table/{name}/{key},
//!     POST /table/{name}/batch, POST /table/{name}/batch/delete.
//!   - Full-replace UPSERT semantics (D-B5); DELETE writes a
//!     pending-retraction marker to the event log (D-B5) but hard-deletes
//!     state. Re-applying same UPSERT is idempotent.
//!   - D-B4 all-or-nothing batches: first validation failure aborts the
//!     whole batch with accepted_count=0.
//!   - D-B6: source-table writes do NOT fire cascade in Phase 55.
//!
//! Wave 2 (plan 55-02) lands the wire format + HTTP routes that flip
//! these tests GREEN. Wave 0 landing: `#[ignore = "55-W2"]`.
//!
//! Run:
//!   cargo test --release --test source_table_cdc -- --ignored

#![cfg(not(feature = "state-inmem"))]

#[path = "common/mod.rs"]
mod common;

#[allow(unused_imports)]
use common::cascade_harness::spawn_two_shards;

// =====================================================================
// SC-2: wire format — HTTP + TCP + batch
// =====================================================================

/// HTTP single-row upsert. POST /table/Countries with
/// `{"key":"US","fields":{"name":"United States","currency":"USD"},
///  "source_lsn":12345}` returns 200 with body
/// `{"accepted": true, "source_lsn": 12345}`. GET /features/US reflects
/// the row.
#[test]
#[ignore = "55-W2"]
fn http_post_table_name_upserts_and_echoes_source_lsn() {
    let _ = 12345u64; // source_lsn fixture
    unimplemented!("Wave 2 — HTTP UPSERT /table/{{name}} wire + source_lsn echo");
}

/// TCP opcode OP_UPSERT_TABLE_ROW (0x14). Frame:
///   [0x14][varint(table_name.len)][table_name]
///   [varint(key.len)][key][u64 LE source_lsn][u32 LE fields_json_len][fields_json]
/// Ack: [status=0x00][u64 LE echoed source_lsn].
#[test]
#[ignore = "55-W2"]
fn tcp_upsert_table_row_opcode_0x14_echoes_source_lsn() {
    let _ = 67890u64; // source_lsn fixture
    unimplemented!("Wave 2 — TCP opcode 0x14 wire + source_lsn echo");
}

/// HTTP batch upsert ≥10K rows. POST /table/Countries/batch with 10,000
/// records returns 200 + `{accepted_count: 10000, source_lsns: [...]}`
/// in input order.
#[test]
#[ignore = "55-W2"]
fn http_post_table_batch_accepts_10k_rows_with_source_lsn_vec() {
    let _n_rows = 10_000usize;
    unimplemented!("Wave 2 — batch UPSERT (≥10K rows per SC-2) with source_lsn Vec");
}

/// D-B4 all-or-nothing — any validation failure aborts the whole batch.
/// POST /table/Countries/batch with 3 rows where row[1].key is empty
/// returns 400 + `{accepted_count: 0, error: "..."}`. Row 0's key is NOT
/// written (rollback on validation failure, not partial-apply).
#[test]
#[ignore = "55-W2"]
fn http_post_table_batch_all_or_nothing_on_validation_failure() {
    unimplemented!("Wave 2 — D-B4 all-or-nothing batch semantics");
}

// =====================================================================
// SC-3: DELETE semantics + idempotence + no-cascade
// =====================================================================

/// D-B5 DELETE semantics — hard-delete state + pending-retraction marker
/// in event log. Sequence:
///   1. POST /table/Countries {key:"US", fields:{...}, source_lsn:1}
///   2. DELETE /table/Countries/US with body {source_lsn:2}
/// Assertions:
///   - 200 response with body {accepted: true, source_lsn: 2}.
///   - GET /features/US returns None (hard-delete, not soft-tombstone).
///   - Event log for Countries contains a LogEntry variant with
///     pending_retraction=true AND source_lsn=2 (Phase 57 consumes this
///     marker to drive downstream retraction).
#[test]
#[ignore = "55-W2"]
fn http_delete_table_row_hard_deletes_and_writes_pending_retraction_marker() {
    unimplemented!("Wave 2 — D-B5 DELETE hard-delete + pending-retraction marker (source_lsn=2)");
}

/// D-B5 full-replace idempotence. Re-applying the same UPSERT with byte-
/// identical fields is a no-op. Log shows one diff; final state
/// bit-identical to single-write.
#[test]
#[ignore = "55-W2"]
fn idempotent_re_upsert_same_fields_is_noop() {
    unimplemented!("Wave 2 — D-B5 full-replace idempotence (source_lsn per write)");
}

/// D-B6 — source-table writes do NOT fire cascade in Phase 55. Source
/// tables are passive enrichment targets; cascade firing is Phase 57.
/// Scenario: register pipeline Stream Txn enriches from source Countries.
/// UPSERT Countries row. Assert: no ShardOp fires to Txn's downstream;
/// `beava_cascade_cross_shard_total` unchanged for this write (no
/// source_lsn-driven cascade).
#[test]
#[ignore = "55-W2"]
fn source_table_write_does_not_fire_cascade_in_phase_55() {
    unimplemented!("Wave 2 — D-B6 no-cascade on source-table writes (source_lsn echoed only)");
}
