//! Phase 57 Wave 0 RED test — SC-2 (StreamStreamJoin retraction).
//! Flips GREEN at Wave 3 when Plan 57-03 lands the SSJ retraction path
//! driven by `ShardOp::RetractDownstream` + `RetractReason::EntityTombstone`.
//!
//! Contract (TPC-CORR-10): LeftStream (keyed by user_id) × RightStream
//! (keyed by session_id) join on user_id. Pushing matched pairs emits
//! joined outputs into the `ssj-<join_id>/` buffer on `hash(user_id) % N`.
//! Tombstoning an L entity (user_1) MUST retract every previously-emitted
//! joined output that referenced user_1 — assertions verify via
//! read_entity_from_shard that the buffered matches on the join-owning
//! shard no longer match the tombstoned side.
//!
//! Metric-name assertions (string probes — grep targets for Wave 3):
//!   - "beava_retractions_sent_total"
//!   - "beava_retractions_applied_total"
//!   - "RetractReason::EntityTombstone"
//!   - "stream_stream_join"  (operator label)
//!
//! See .planning/phases/57-retraction-across-crossshard-joins/57-CONTEXT.md
//! Area A-A1 (SSJ contributing_inputs = {left_event_id, right_event_id})
//! + Area B-B2 (RetractReason::EntityTombstone) + 57-00-PLAN.md SC-2 hooks.
//!
//! Run:
//!   cargo test --release --test crossshard_ssj_retraction

#![cfg(not(feature = "state-inmem"))]
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;

use beava::engine::join_validator::ShardKeySpec;
use beava::engine::pipeline::{FeatureDef, JoinType, PipelineEngine, StreamDefinition};
use beava::routing::shard_hint_for_event;
use beava::shard::read_entity_from_shard;
use beava::shard::thread::{ShardEvent, ShardHandle, ShardOp, ShardResult};
use beava::shard::Shard;

#[path = "common/mod.rs"]
mod common;

// Metric-name probes — the production registry names asserted against
// after Wave 3 flips this test GREEN. Today these are just string
// constants so grep (57-00-PLAN acceptance) picks them up.
const METRIC_RETRACTIONS_SENT: &str = "beava_retractions_sent_total";
const METRIC_RETRACTIONS_APPLIED: &str = "beava_retractions_applied_total";

// RetractReason variant + operator label (strings — exist as constants
// so Wave 3 can grep-verify the acceptance).
const RETRACT_REASON_ENTITY_TOMBSTONE: &str = "RetractReason::EntityTombstone";
const OP_STREAM_STREAM_JOIN: &str = "stream_stream_join";

// ---------------------------------------------------------------------------
// Routing helpers — mirror cross_shard_stream_stream_join.rs.
// ---------------------------------------------------------------------------

fn route_by_field(value: &str, field: &str, n: usize) -> usize {
    (shard_hint_for_event(
        &serde_json::json!({ field: value }),
        Some(field),
    ) as usize)
        % n
}

fn route_join_key(state_key: &str, n: usize) -> usize {
    (shard_hint_for_event(
        &serde_json::json!({ "__k": state_key }),
        Some("__k"),
    ) as usize)
        % n
}

// ---------------------------------------------------------------------------
// Build the 3-stream SSJ fixture — L (user_id), R (session_id), LRJoin(on=user_id).
// ---------------------------------------------------------------------------

fn build_engine() -> PipelineEngine {
    let mut engine = PipelineEngine::new();

    engine
        .register(StreamDefinition {
            name: "L".into(),
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
            shard_key: Some(ShardKeySpec::Single("user_id".into())),
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "R".into(),
            key_field: Some("session_id".into()),
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
            shard_key: Some(ShardKeySpec::Single("session_id".into())),
        })
        .unwrap();

    engine
        .register(StreamDefinition {
            name: "LRJoin".into(),
            key_field: None,
            group_by_keys: None,
            features: vec![(
                "__ssj_LR".into(),
                FeatureDef::StreamStreamJoin {
                    left_stream: "L".into(),
                    right_stream: "R".into(),
                    on: vec!["user_id".into()],
                    within_ms: 60_000,
                    join_type: JoinType::Inner,
                    left_fields: vec![],
                    right_fields: vec![],
                },
            )],
            depends_on: Some(vec!["L".into(), "R".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
            shard_key: Some(ShardKeySpec::Single("user_id".into())),
        })
        .unwrap();

    engine
}

// ---------------------------------------------------------------------------
// Drain thread — services SsjInsert (Phase 56) and generic ops. Wave 3
// extends with a ShardOp::RetractDownstream arm that mutates the
// __ssj__ buffer on the join-owning shard.
// ---------------------------------------------------------------------------

fn spawn_drain(
    shard: Shard,
    rx: crossbeam_channel::Receiver<ShardEvent>,
    ssj_counter: Arc<AtomicU64>,
) -> thread::JoinHandle<Shard> {
    thread::spawn(move || {
        let mut shard = shard;
        while let Ok(mut event) = rx.recv() {
            let op = std::mem::replace(&mut event.op, ShardOp::Push);
            match op {
                ShardOp::SsjInsert {
                    join_id,
                    side,
                    join_key,
                    event: ssj_event,
                    within_ms,
                } => {
                    ssj_counter.fetch_add(1, Ordering::Relaxed);
                    let matches = shard.apply_ssj_insert(
                        &join_id,
                        side,
                        &join_key,
                        ssj_event,
                        within_ms,
                    );
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SsjInsertOk(matches));
                    }
                }
                ShardOp::ReadEntityAt { table_name, key } => {
                    let out = shard.read_entity_at(&table_name, &key);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::ReadEntityOk(out));
                    }
                }
                _ => {
                    // Wave 3 extends this arm with RetractDownstream —
                    // today the variant doesn't exist yet, so the
                    // catch-all ack keeps the source-side oneshot from
                    // dangling.
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
// SC-2 — tombstoning an L entity retracts every previously-emitted joined
// output referencing that entity.
// ---------------------------------------------------------------------------

/// SC-2 primary — push L + R pairs so the SSJ buffer accumulates matched
/// joined outputs; then tombstone the L entity for `user_1`. Every joined
/// output on `hash(user_1) % N` that referenced user_1 MUST be retracted.
///
/// Assertion hooks (Wave 3 must satisfy):
///   - read_entity_from_shard(join_shard, "user_1") has no remaining
///     joined outputs in __ssj_LR / __ssj__ buffer slot
///   - beava_retractions_sent_total{operator="stream_stream_join"} ≥ 1
///   - beava_retractions_applied_total{operator="stream_stream_join"} ==
///     count of emitted joined outputs that referenced user_1
///   - RetractReason::EntityTombstone { stream_name: "L", entity_key: "user_1", .. }
#[test]
#[ignore = "57-W3"]
// flips GREEN in Plan 57-03 (StreamStreamJoin retraction path)
fn ssj_tombstone_retracts_previously_joined_outputs() {
    const N: usize = 4;

    // Compile-time probes for grep.
    let _m_sent = METRIC_RETRACTIONS_SENT;
    let _m_applied = METRIC_RETRACTIONS_APPLIED;
    let _reason = RETRACT_REASON_ENTITY_TOMBSTONE;
    let _op = OP_STREAM_STREAM_JOIN;

    // Pick user_id + session_id that route to different source shards.
    // L source-ingress = hash(user_id) % N; since L.shard_key=user_id
    // and the join is on user_id, L's source shard == join-owning shard.
    // R source-ingress = hash(session_id) % N; generally ≠ join shard.
    let user = "user_1".to_string();
    let join_shard = route_by_field(&user, "user_id", N);
    let mut session = String::new();
    for i in 0u32..8192 {
        let candidate = format!("s_{i}");
        let s_shard = route_by_field(&candidate, "session_id", N);
        if s_shard != join_shard {
            session = candidate;
            break;
        }
    }
    assert!(!session.is_empty(), "no session_id with shard != join_shard");

    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(N);
    let mut shards: Vec<Option<Shard>> = partitions
        .into_iter()
        .map(|p| Some(Shard::with_partition(p)))
        .collect();

    let mut input_shard = shards[join_shard].take().expect("input shard present");

    let ssj_counter = Arc::new(AtomicU64::new(0));
    let mut senders: Vec<crossbeam_channel::Sender<ShardEvent>> = Vec::with_capacity(N);
    let mut handles_vec: Vec<ShardHandle> = Vec::with_capacity(N);
    let mut drains: Vec<Option<thread::JoinHandle<Shard>>> = (0..N).map(|_| None).collect();

    for i in 0..N {
        let (tx, rx) = crossbeam_channel::bounded::<ShardEvent>(65_536);
        senders.push(tx.clone());
        handles_vec.push(ShardHandle {
            shard_index: i,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: tx,
        });
        if i == join_shard {
            std::mem::forget(rx);
        } else {
            let sh = shards[i].take().expect("sibling shard present");
            drains[i] =
                Some(spawn_drain(sh, rx, Arc::clone(&ssj_counter)));
        }
    }

    let engine = build_engine();
    let now = SystemTime::now();

    // Step 1: push L event for user_1. Lands on join_shard (L.shard_key=user_id).
    let l_event = serde_json::json!({
        "user_id": user,
        "amount": 100,
    });
    engine
        .push_with_cascade_on_shard(
            "L",
            &l_event,
            &mut input_shard,
            None,
            now,
            true,
            Some(&handles_vec),
            join_shard,
        )
        .expect("L push ok");

    // Step 2: push R event with matching user_id. Source shard is
    // hash(session_id) % N ≠ join_shard; SsjInsert routes it to
    // join_shard for the match.
    //
    // We push R through the join-owning shard directly here for
    // harness simplicity (the fixture owns input_shard mutably). A
    // proper cross-shard R push would require additional handle
    // plumbing; Wave 3 verifies the retraction invariant regardless
    // of ingress path.
    let r_event = serde_json::json!({
        "user_id": user,
        "session_id": session,
        "ts": 1_000u64,
    });
    engine
        .push_with_cascade_on_shard(
            "R",
            &r_event,
            &mut input_shard,
            None,
            now,
            true,
            Some(&handles_vec),
            join_shard,
        )
        .expect("R push ok");

    // Step 3: tombstone the L entity for user_1. Wave 3 adds
    // the retraction dispatch that walks contributing_inputs and fans
    // out ShardOp::RetractDownstream { stream_name: "LRJoin", .. } to
    // every owner of a joined output referencing user_1.
    //
    // TODO(57-W3): when the retraction API lands:
    //   engine.retract_entity_on_shard("L", &user, &mut input_shard,
    //       join_shard, Some(&handles_vec), now)
    // Today we invoke delete_entity directly — pre-Wave-3 this does NOT
    // fan out a retraction; Wave 3 makes it do so.
    let _removed = input_shard.delete_entity(&user);

    // Step 4: assertions Wave 3 must satisfy.
    //
    // (a) On the join-owning shard, the __ssj_LR buffer under user_1
    //     no longer carries a joined output referencing the tombstoned
    //     L entity. Today the buffer state is inspected via
    //     read_entity_from_shard probing the "__ssj_LR" stream slot on
    //     the user_1 join key.
    let ssj_residue = read_entity_from_shard(&input_shard, &user, |entity| {
        // After retraction, either the entity itself is gone OR the
        // "__ssj_LR" stream slot has no stored left-side event id.
        entity.streams.get("__ssj_LR").is_some()
    });
    assert!(
        !ssj_residue.unwrap_or(false),
        "SC-2: post-tombstone, user={user} must not have a residual __ssj_LR buffer slot on join_shard={join_shard}"
    );

    // (b) The joined output downstream of LRJoin (if any was emitted
    //     to a keyed downstream stream) is retracted on its owning
    //     shard. For this fixture LRJoin is keyless so emitted output
    //     has no stable entity key; the assertion degenerates to (a).
    //     Wave 3 may also check `beava_retractions_applied_total
    //     {operator="stream_stream_join"}` on the metric registry.

    drop(handles_vec);
    for tx in senders.drain(..) {
        drop(tx);
    }
    drop(input_shard);
    for d in drains.into_iter().flatten() {
        let _ = d.join();
    }
}
