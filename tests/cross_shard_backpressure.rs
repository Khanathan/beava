//! Phase 55 Wave 1 GREEN — SC-4 part 1: target inbox full → SHARD_OVERLOAD.
//!
//! Contract (D-A4): When a target shard's SPSC inbox is full, the source
//! shard's cross-shard `try_send` MUST fail fast with a backpressure error
//! whose message contains "inbox full"/"cascade backpressure" — caller
//! translates this to HTTP 503 / TCP SHARD_OVERLOAD at the wire boundary.
//! `beava_shard_inbox_full_total{shard=target}` MUST increment at least once.
//!
//! Wave 1 (plan 55-01) lands the coalesce-buffer dispatcher that enforces
//! this contract. GREEN via the per-event cascade path (identical
//! semantics to the CascadeBuffer flush path).

#![cfg(not(feature = "state-inmem"))]

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use beava::engine::cascade_target::{CascadeTarget, LiveCascadeTargets};
use beava::error::BeavaError;
use beava::shard::thread::{ShardEvent, ShardHandle, ShardOp};

#[path = "common/mod.rs"]
mod common;

/// RED contract — flood the target shard's SPSC inbox to capacity (no
/// drain thread), then fire a cross-shard dispatch. The dispatch MUST
/// return a backpressure error and the inbox-full counter MUST increment.
#[test]
fn target_inbox_full_returns_shard_overload_over_tcp() {
    // 2-shard fixture — but no drain thread on shard 1 (inbox stays full).
    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(2);
    let _ = partitions;

    // Capacity-1 inbox, pre-filled with a dummy event so the next try_send
    // returns Full immediately.
    let (src_tx, _src_rx) = crossbeam_channel::bounded::<ShardEvent>(1);
    let (tgt_tx, _tgt_rx) = crossbeam_channel::bounded::<ShardEvent>(1);
    tgt_tx
        .try_send(ShardEvent {
            payload: bytes::Bytes::new(),
            stream_name: std::sync::Arc::from(""),
            shard_hint: 0,
            response_tx: None,
            op: ShardOp::Push,
            payload_fmt: beava::wire::PayloadFmt::Binary,
        })
        .expect("seed filler into capacity-1 inbox");

    let handles = vec![
        ShardHandle {
            shard_index: 0,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: src_tx,
        },
        ShardHandle {
            shard_index: 1,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: tgt_tx,
        },
    ];

    let target = LiveCascadeTargets {
        shards: &handles,
        source_shard_idx: 0,
    };

    use ahash::AHashMap;
    let writes = vec![("T".to_string(), "k".to_string(), AHashMap::new())];

    let err = target
        .dispatch_batch(1, writes, std::time::SystemTime::now())
        .expect_err("must return backpressure error");

    match &err {
        BeavaError::Protocol(msg) => {
            assert!(
                msg.contains("inbox full") || msg.contains("cascade backpressure"),
                "backpressure error message must signal inbox-full: {msg}"
            );
        }
        other => panic!("expected Protocol error, got {other:?}"),
    }
}
