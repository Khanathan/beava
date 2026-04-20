//! Phase 54-02 Task 2 — scatter-gather cross-shard TT cascade.
//!
//! Direct test of `PipelineEngine::cascade_table_upsert_on_shard`. Sets up
//! a two-shard test rig where:
//!   - Input Table `A` is keyed by `user_id` and lives on shard 0
//!   - Input Table `B` is keyed by `user_id` (same shard as A)
//!   - Output TableTableJoin `J` is keyed by `region` — hashing to shard 1
//!
//! When cascade fires for a primary event `{user_id: "u1", region: "r9"}`,
//! the derived J row MUST land on shard 1 (the region-owner), NOT on
//! shard 0 (the user_id-owner). This is the scatter-gather correctness
//! gate that distinguishes Pass B from the old same-key cascade path.
//!
//! Test design (no real shard thread spawn):
//!
//!   - Build `Shard` for shard 0 + a sibling-channel fake for shard 1.
//!   - Spawn a tiny drain thread on shard 1's fake: it reads the SPSC
//!     inbox, applies `UpsertTableRow` / `TombstoneTableRow` against a
//!     shard-1 `Shard`, replies `ShardResult::SetOk` on the oneshot. No
//!     real `shard_event_loop` — keeps the test focused on the scatter-
//!     gather contract.
//!   - Call `cascade_table_upsert_on_shard` on shard 0 with
//!     `sibling_shards = Some(&[shard0_handle, shard1_handle])`.
//!   - Join the drain thread, inspect shard 1's state via
//!     `read_entity_from_shard`, assert the output row landed on shard 1
//!     at key `r9` and that shard 0 does NOT carry that row.
//!
//! TT-cascade is SCATTER-GATHER per user decision 2026-04-19; the
//! register-time shard_key-constraint recommendation is rejected (see
//! `.planning/phases/54-legacy-engine-removal/54-RESEARCH.md` Open
//! Question 1 resolution).

#![cfg(not(feature = "state-inmem"))]

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime};

use ahash::AHashMap;

use beava::engine::pipeline::{FeatureDef, JoinType, PipelineEngine, StreamDefinition};
use beava::routing::shard_hint_for_event;
use beava::shard::thread::{ShardEvent, ShardHandle, ShardOp, ShardResult};
use beava::shard::{read_entity_from_shard, Shard, StoreView};
use beava::state::store::TableRowState;
use beava::types::FeatureValue;

#[path = "common/mod.rs"]
mod common;

// ---------------------------------------------------------------------------
// Tiny event-loop that services ONLY UpsertTableRow + TombstoneTableRow.
// Runs on its own OS thread + its own crossbeam inbox. Replies SetOk via
// the oneshot. Returns the drained `Shard` (with accumulated writes) to
// the caller when the inbox disconnects.
// ---------------------------------------------------------------------------

fn spawn_drain_thread(
    shard: Shard,
    rx: crossbeam_channel::Receiver<ShardEvent>,
) -> thread::JoinHandle<Shard> {
    thread::spawn(move || {
        let mut shard = shard;
        while let Ok(mut event) = rx.recv() {
            let op = std::mem::replace(&mut event.op, ShardOp::Push);
            match op {
                ShardOp::UpsertTableRow {
                    key,
                    table_name,
                    fields,
                    now,
                } => {
                    shard.upsert_table_row(&key, &table_name, fields, now);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                ShardOp::TombstoneTableRow {
                    key,
                    table_name,
                    now,
                } => {
                    shard.tombstone_table_row(&key, &table_name, now);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
                _ => {
                    // Other variants aren't exercised by cascade; drop ack.
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
            }
        }
        shard
    })
}

// ---------------------------------------------------------------------------
// Build the engine with A ⋈ B → J (TT-Inner join). In this fixture A and B
// are both keyed by `user_id`, and J is keyed by `region`. The engine
// doesn't enforce different key fields at register time (per user
// decision: NO register-time shard_key constraint) — that's exactly the
// scenario we're testing.
// ---------------------------------------------------------------------------

fn make_engine() -> PipelineEngine {
    let mut engine = PipelineEngine::new();

    // Input Tables A and B — keyed by user_id, hold {score} / {tier}.
    engine
        .register(StreamDefinition {
            name: "A".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: Vec::new(),
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "B".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: Vec::new(),
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    // Output J — TableTableJoin(A, B) INNER on user_id. Crucially, J's own
    // key_field is `region` — DIFFERENT from A/B's user_id. That's the
    // cross-shard hop we're verifying.
    engine
        .register(StreamDefinition {
            name: "J".into(),
            key_field: Some("region".into()),
            group_by_keys: None,
            features: vec![(
                "joined".into(),
                FeatureDef::TableTableJoin {
                    left_table: "A".into(),
                    right_table: "B".into(),
                    on: vec!["user_id".into()],
                    join_type: JoinType::Inner,
                    left_fields: vec!["score".into()],
                    right_fields: vec![("tier".into(), "tier".into())],
                },
            )],
            depends_on: Some(vec!["A".into(), "B".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: None,
        })
        .unwrap();

    engine
}

fn fields(pairs: &[(&str, FeatureValue)]) -> AHashMap<String, FeatureValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// Pick (user_id, region) that hash to DIFFERENT shards at N=2, so we
// exercise the scatter (cross-shard) path deterministically.
fn pick_split_keys(n: usize) -> (String, String, usize, usize) {
    // Exhaustively probe deterministic candidate strings until we find a
    // pair whose user_id-hash and region-hash differ mod n. At n=2 this is
    // a 50/50 pairing — the first few candidates always hit.
    for u in 0u32..256 {
        for r in 0u32..256 {
            let user_id = format!("u{:02}", u);
            let region = format!("r{:02}", r);
            let ev_u = serde_json::json!({ "user_id": user_id });
            let ev_r = serde_json::json!({ "region": region });
            let uidx =
                (shard_hint_for_event(&ev_u, Some("user_id")) as usize) % n.max(1);
            let ridx =
                (shard_hint_for_event(&ev_r, Some("region")) as usize) % n.max(1);
            if uidx != ridx {
                return (user_id, region, uidx, ridx);
            }
        }
    }
    panic!("failed to find split keys for n={}", n);
}

#[test]
fn cross_shard_tt_cascade_lands_output_on_region_shard() {
    // 2 shards is the minimum that proves scatter. Fresh fjall keyspace
    // owned by this test.
    let n_shards = 2usize;
    // Suppress the unused helper warning — pick_split_keys is called by
    // way of the more specific pair-finder below and kept for future
    // expansion.
    let _ = pick_split_keys;

    // Find (user_id, region) with user_shard=0 and region_shard=1
    // specifically so cascade dispatches to shard 1 (cross-shard hop).
    let (user_id, region) = {
        let mut found = None;
        'outer: for u in 0u32..1024 {
            for r in 0u32..1024 {
                let uu = format!("u{:03}", u);
                let rr = format!("r{:03}", r);
                let uidx = (shard_hint_for_event(
                    &serde_json::json!({ "user_id": uu }),
                    Some("user_id"),
                ) as usize)
                    % n_shards;
                let ridx = (shard_hint_for_event(
                    &serde_json::json!({ "region": rr }),
                    Some("region"),
                ) as usize)
                    % n_shards;
                if uidx == 0 && ridx == 1 {
                    found = Some((uu, rr));
                    break 'outer;
                }
            }
        }
        found.expect("could not find user_id/region pair with user_shard=0 region_shard=1")
    };

    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(n_shards);
    let mut parts = partitions.into_iter();
    let mut input_shard = Shard::with_partition(parts.next().unwrap());
    let sibling_shard = Shard::with_partition(parts.next().unwrap());

    let now = SystemTime::now();
    let a_row = fields(&[("score", FeatureValue::Int(42))]);
    let b_row = fields(&[("tier", FeatureValue::String("gold".into()))]);

    // Seed A and B rows on input_shard at user_id key.
    {
        let mut view = StoreView::Sharded(&mut input_shard);
        view.upsert_table_row(&user_id, "A", a_row.clone(), now);
        view.upsert_table_row(&user_id, "B", b_row.clone(), now);
    }

    // Build the handles slice. The input-shard handle needs a real
    // Sender/Receiver pair (else try_send fails with Disconnected) but we
    // don't actually dispatch to it — intra-shard writes go directly to
    // the &mut Shard. Still, construct a valid sender to keep the
    // handle[0] non-bogus.
    let (input_tx, _input_rx) = crossbeam_channel::bounded::<ShardEvent>(64);
    let (sibling_tx, sibling_rx) = crossbeam_channel::bounded::<ShardEvent>(64);

    let handles = vec![
        ShardHandle {
            shard_index: 0,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: input_tx.clone(),
        },
        ShardHandle {
            shard_index: 1,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: sibling_tx.clone(),
        },
    ];

    // Spawn drain thread for sibling shard.
    let drain_handle = spawn_drain_thread(sibling_shard, sibling_rx);

    // Build the engine and invoke scatter-gather cascade. Primary event
    // carries BOTH user_id (for intra-shard read) AND region (for output-
    // key extraction).
    let engine = make_engine();
    let primary_event = serde_json::json!({
        "user_id": user_id,
        "region": region,
    });

    // Input table is "A" — cascade walks TT edges referencing A, computes
    // the merged row, and routes it to the shard that owns region=<r..>.
    engine
        .cascade_table_upsert_on_shard(
            "A",
            &user_id,
            false,
            Some(&primary_event),
            &mut input_shard,
            0,
            Some(&handles),
            now,
        )
        .expect("cascade ok");

    // Close the sibling channel to let the drain thread exit, then wait
    // for it and recover the sibling Shard to inspect. `handles` owns
    // a clone of `sibling_tx` (and `input_tx`), so we must drop the
    // whole vec before dropping our local senders — otherwise the
    // handle clones keep the channel alive and `drain_handle.join()`
    // deadlocks on `rx.recv()`.
    drop(handles);
    drop(sibling_tx);
    drop(input_tx);
    let sibling_shard = drain_handle.join().expect("drain thread clean exit");

    // Assert: the J row lives on sibling_shard at key=region.
    let j_row = read_entity_from_shard(&sibling_shard, &region, |entity| {
        entity.table_rows.get("J").cloned()
    })
    .flatten();
    let j_row = j_row.expect("J row must exist on region-owner shard");
    assert!(
        matches!(j_row.state, TableRowState::Live),
        "J row must be Live, got {:?}",
        j_row.state
    );
    assert_eq!(
        j_row.fields.get("score"),
        Some(&FeatureValue::Int(42)),
        "J row must carry left_fields from A: {:?}",
        j_row.fields
    );
    assert_eq!(
        j_row.fields.get("tier"),
        Some(&FeatureValue::String("gold".into())),
        "J row must carry right_fields from B: {:?}",
        j_row.fields
    );

    // Assert: input_shard does NOT carry the J row at region (would
    // indicate we took the intra-shard same-key path by mistake).
    let input_j = read_entity_from_shard(&input_shard, &region, |entity| {
        entity.table_rows.get("J").cloned()
    })
    .flatten();
    assert!(
        input_j.is_none(),
        "J row at key=region must NOT land on the input shard (would indicate \
         intra-shard fallback — scatter-gather regression)"
    );

    // Also: the input-shard should not have a J row at user_id either
    // (that would be the old same-key cascade behavior, which we REPLACED
    // for split-key output tables).
    let input_j_at_user = read_entity_from_shard(&input_shard, &user_id, |entity| {
        entity.table_rows.get("J").cloned()
    })
    .flatten();
    assert!(
        input_j_at_user.is_none(),
        "J row must not be written on input shard at user_id key for a \
         split-key output table"
    );
}

// ---------------------------------------------------------------------------
// Backpressure: if the sibling shard's inbox is Full, cascade must return
// BeavaError::Protocol("shard inbox full — cascade backpressure…") — same
// shape as Wave 1 Task 1's HTTP-push backpressure. Verifies the fail-fast
// contract that prevents deadlock cycles (see the doc comment on
// `cascade_table_upsert_on_shard`).
// ---------------------------------------------------------------------------

#[test]
fn cross_shard_tt_cascade_backpressure_returns_protocol_error() {
    let n_shards = 2usize;
    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(n_shards);
    let mut parts = partitions.into_iter();
    let mut input_shard = Shard::with_partition(parts.next().unwrap());
    let _sibling_shard = Shard::with_partition(parts.next().unwrap());

    // Pick a user/region pair where user_shard=0, region_shard=1 so
    // cascade dispatches to shard 1.
    let (user_id, region) = {
        let mut found = None;
        'outer: for u in 0u32..1024 {
            for r in 0u32..1024 {
                let uu = format!("u{:03}", u);
                let rr = format!("r{:03}", r);
                let uidx = (shard_hint_for_event(
                    &serde_json::json!({ "user_id": uu }),
                    Some("user_id"),
                ) as usize)
                    % n_shards;
                let ridx = (shard_hint_for_event(
                    &serde_json::json!({ "region": rr }),
                    Some("region"),
                ) as usize)
                    % n_shards;
                if uidx == 0 && ridx == 1 {
                    found = Some((uu, rr));
                    break 'outer;
                }
            }
        }
        found.expect("could not find split pair")
    };

    // Seed input rows.
    let now = SystemTime::now();
    {
        let mut view = StoreView::Sharded(&mut input_shard);
        view.upsert_table_row(&user_id, "A", fields(&[("score", FeatureValue::Int(1))]), now);
        view.upsert_table_row(
            &user_id,
            "B",
            fields(&[("tier", FeatureValue::String("x".into()))]),
            now,
        );
    }

    // Capacity-1 inbox for sibling, PRE-FILLED with a bogus event so the
    // cascade's try_send returns Full immediately. No drain thread here —
    // we want the inbox to stay full.
    let (input_tx, _input_rx) = crossbeam_channel::bounded::<ShardEvent>(1);
    let (sibling_tx, _sibling_rx) = crossbeam_channel::bounded::<ShardEvent>(1);

    let filler = ShardEvent {
        payload: bytes::Bytes::new(),
        stream_name: std::sync::Arc::from(""),
        shard_hint: 0,
        response_tx: None,
        op: ShardOp::Push,
    };
    sibling_tx
        .try_send(filler)
        .expect("seed filler into capacity-1 inbox");

    let handles = vec![
        ShardHandle {
            shard_index: 0,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: input_tx,
        },
        ShardHandle {
            shard_index: 1,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: sibling_tx,
        },
    ];

    let engine = make_engine();
    let primary_event = serde_json::json!({
        "user_id": user_id,
        "region": region,
    });

    let err = engine
        .cascade_table_upsert_on_shard(
            "A",
            &user_id,
            false,
            Some(&primary_event),
            &mut input_shard,
            0,
            Some(&handles),
            now,
        )
        .expect_err("cascade must fail fast when sibling inbox is full");

    let msg = format!("{}", err);
    assert!(
        msg.contains("cascade backpressure") || msg.contains("inbox full"),
        "backpressure error must mention inbox/cascade: {}",
        msg
    );
    // Give the unused Duration import a home.
    let _ = Duration::from_millis(0);
}
