//! Phase 56 SC-2 — StreamStreamJoin buffer lives on `hash(join.on) % N`
//! (TPC-CORR-09).
//!
//! Contract: `StreamStreamJoin` with mismatched left/right `shard_key=`
//! declarations MUST produce correct joined events by routing both sides to
//! the shard owning `hash(join.on) % N`. The buffer lives on the join-owning
//! shard under the synthetic `"__ssj__"` stream slot on the join_key's
//! EntityState (Wave 1 + 3 layout). Source-shard dispatch:
//! `PipelineEngine::ssj_insert_at_shard` dispatches `ShardOp::SsjInsert`
//! to the target shard; the target evaluates the match inline and returns
//! the matched counterparty events. The source shard emits the joined
//! output via the existing downstream cascade path.
//!
//! When `shard_key=join.on` on both sides (co-located), no relaxation applies
//! — `target_shard == input_shard_idx` and the helper short-circuits to
//! `shard.apply_ssj_insert` inline (zero SPSC hops).
//!
//! Phase 56 Wave 3 (56-03-PLAN): GREEN — StreamStreamJoin eval rewired
//! through `ssj_insert_at_shard`; both SC-2 tests pass at N=4.
//!
//! Run:
//!   cargo test --release --test cross_shard_stream_stream_join

#![cfg(not(feature = "state-inmem"))]

use ahash::AHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;

use beava::engine::join_validator::ShardKeySpec;
use beava::engine::operators::JoinSide;
use beava::engine::pipeline::{FeatureDef, JoinType, PipelineEngine, StreamDefinition};
use beava::routing::shard_hint_for_event;
use beava::shard::read_entity_from_shard;
use beava::shard::thread::{ShardEvent, ShardHandle, ShardOp, ShardResult};
use beava::shard::Shard;
use beava::state::snapshot::OperatorState;

#[path = "common/mod.rs"]
mod common;

// ---------------------------------------------------------------------------
// Shard routing helpers — must mirror the production `shard_hint_for_event`
// so harness choices agree with operator eval.
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn hash_to_shard(key: &str, n_shards: usize) -> usize {
    let mut h = AHasher::default();
    key.hash(&mut h);
    (h.finish() % n_shards as u64) as usize
}

fn route_by_field(value: &str, field: &str, n_shards: usize) -> usize {
    (shard_hint_for_event(
        &serde_json::json!({ field: value }),
        Some(field),
    ) as usize)
        % n_shards
}

// State_key routing — mirrors PipelineEngine::push_with_cascade_on_shard's
// target_shard computation in the SSJ eval path: `shard_hint_for_event(
// {"__k": state_key}, Some("__k")) % N`.
fn route_join_key(state_key: &str, n_shards: usize) -> usize {
    (shard_hint_for_event(
        &serde_json::json!({ "__k": state_key }),
        Some("__k"),
    ) as usize)
        % n_shards
}

// ---------------------------------------------------------------------------
// Drain thread — services SsjInsert (+ Push / others) against a local Shard.
// ---------------------------------------------------------------------------

fn spawn_ssj_drain(
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
                        &join_id, side, &join_key, ssj_event, within_ms,
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
// Engine builder — three streams: L, R, LRJoin.
// ---------------------------------------------------------------------------

fn build_engine_mismatched() -> PipelineEngine {
    let mut engine = PipelineEngine::new();

    // Stream L — keyed by user_id (both key_field and shard_key).
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

    // Stream R — keyed by session_id, shard_key=session_id. Mismatch vs L.
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

    // Downstream — StreamStreamJoin on user_id. The join is keyless
    // (no key_field on the join stream itself); the buffer is stored on
    // the join_key-owning shard. shard_key on the join stream is
    // cosmetic for the join-buffer placement (that is driven by state_key).
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

fn build_engine_colocated() -> PipelineEngine {
    let mut engine = PipelineEngine::new();

    // Both L and R share shard_key=user_id (D-B5).
    for name in ["L", "R"] {
        engine
            .register(StreamDefinition {
                name: name.into(),
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
    }

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
// Helper — verify a StreamJoinBuffer is present on a given shard under
// join_key. Returns true if at least one buffer entry exists under the
// synthetic "__ssj__" stream slot.
// ---------------------------------------------------------------------------

fn has_ssj_buffer(shard: &Shard, join_key: &str) -> bool {
    read_entity_from_shard(shard, join_key, |entity| {
        let stream = match entity.streams.get("__ssj__") {
            Some(s) => s,
            None => return false,
        };
        stream
            .operators
            .iter()
            .any(|(_, op)| matches!(op, OperatorState::StreamJoinBuffer(_)))
    })
    .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// SC-2 primary — L(shard_key=user_id) × R(shard_key=session_id), join on
// user_id. Both sides converge on shard J = hash(user_id) % N via
// `ssj_insert_at_shard`. The right-side push (which lands on shard S =
// hash(session_id) % N) MUST produce exactly ONE cross-shard SsjInsert
// dispatch to shard J. The SSJ buffer MUST live on shard J; NOT on S.
// ---------------------------------------------------------------------------

#[test]
fn stream_stream_join_routes_to_join_key_shard() {
    const N: usize = 4;

    // Pick (u, s) such that hash(u)%N != hash(s)%N under the production
    // routing.
    let mut user = String::new();
    let mut session = String::new();
    let mut j = 0usize;
    let mut s = 0usize;
    'outer: for i in 0u32..4096 {
        let u_candidate = format!("u_{i}");
        let ju = route_by_field(&u_candidate, "user_id", N);
        for k in 0u32..4096 {
            let s_candidate = format!("s_{k}");
            let ss = route_by_field(&s_candidate, "session_id", N);
            if ju != ss {
                user = u_candidate.clone();
                session = s_candidate;
                j = ju;
                s = ss;
                break 'outer;
            }
        }
    }
    assert!(
        !user.is_empty() && !session.is_empty(),
        "no distinct (user, session) shard pair found at N={N}"
    );
    assert_ne!(j, s, "test precondition: J={j} != S={s}");

    // Also verify the join_key route (state_key="u1") targets shard J
    // under the "__k" hash scheme that the SSJ eval uses.
    let j_by_state = route_join_key(&user, N);
    assert_eq!(
        j, j_by_state,
        "state_key routing MUST equal session-by-user_id routing for user={user}"
    );

    // Build N shards, ephemeral fjall.
    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(N);
    let parts: Vec<_> = partitions.into_iter().collect();
    let mut shards: Vec<Option<Shard>> = parts
        .into_iter()
        .map(|p| Some(Shard::with_partition(p)))
        .collect();

    // Per-shard SsjInsert counter arrays (index by shard id).
    let ssj_counters: Vec<Arc<AtomicU64>> =
        (0..N).map(|_| Arc::new(AtomicU64::new(0))).collect();

    // Two passes of fixture: (a) push L on shard J, (b) push R on shard S.
    // In pass (a), input_shard=J==target_shard so the fast path runs
    // inline (no dispatch). In pass (b), input_shard=S, target=J — a
    // cross-shard SsjInsert fires to shard J.

    // ---- Pass (a): push L event on shard J ----
    let mut input_shard_a = shards[j].take().expect("shard J for pass A");
    let mut senders_a: Vec<crossbeam_channel::Sender<ShardEvent>> = Vec::with_capacity(N);
    let mut handles_a: Vec<ShardHandle> = Vec::with_capacity(N);
    let mut drains_a: Vec<Option<thread::JoinHandle<Shard>>> = (0..N).map(|_| None).collect();

    for i in 0..N {
        let (tx, rx) = crossbeam_channel::bounded::<ShardEvent>(65_536);
        senders_a.push(tx.clone());
        handles_a.push(ShardHandle {
            shard_index: i,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: tx,
        });
        if i == j {
            std::mem::forget(rx);
        } else {
            let sh = shards[i].take().expect("sibling shard present (A)");
            drains_a[i] = Some(spawn_ssj_drain(sh, rx, Arc::clone(&ssj_counters[i])));
        }
    }

    let engine = build_engine_mismatched();
    let now = SystemTime::now();
    engine
        .push_with_cascade_on_shard(
            "L",
            &serde_json::json!({
                "user_id": user,
                "session_id": session,
                "payload": "left",
            }),
            &mut input_shard_a,
            None,
            now,
            true,
            Some(&handles_a),
            j,
        )
        .expect("L push ok");

    // After L, buffer must exist on shard J (input shard == target shard,
    // inline fast path).
    assert!(
        has_ssj_buffer(&input_shard_a, &user),
        "SSJ buffer for join_key='{user}' MUST live on shard J={j} after L push"
    );
    // No cross-shard SsjInsert hops for L (it was same-shard).
    let l_ssj_hops: u64 = ssj_counters
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != j)
        .map(|(_, c)| c.load(Ordering::Relaxed))
        .sum();
    assert_eq!(
        l_ssj_hops, 0,
        "SC-2: L push lands on shard J same as target — zero sibling SsjInsert hops expected; got {l_ssj_hops}"
    );

    // Tear down pass-A drains cleanly.
    drop(handles_a);
    for tx in senders_a.drain(..) {
        drop(tx);
    }
    // Recover all sibling shards from drains.
    for (i, d) in drains_a.into_iter().enumerate() {
        if let Some(handle) = d {
            let recovered = handle.join().expect("drain A join");
            shards[i] = Some(recovered);
        }
    }
    // Return input shard J.
    shards[j] = Some(input_shard_a);

    // ---- Pass (b): push R event on shard S; expect cross-shard dispatch to J ----
    let mut input_shard_b = shards[s].take().expect("shard S for pass B");
    let mut senders_b: Vec<crossbeam_channel::Sender<ShardEvent>> = Vec::with_capacity(N);
    let mut handles_b: Vec<ShardHandle> = Vec::with_capacity(N);
    let mut drains_b: Vec<Option<thread::JoinHandle<Shard>>> = (0..N).map(|_| None).collect();

    for i in 0..N {
        let (tx, rx) = crossbeam_channel::bounded::<ShardEvent>(65_536);
        senders_b.push(tx.clone());
        handles_b.push(ShardHandle {
            shard_index: i,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: tx,
        });
        if i == s {
            std::mem::forget(rx);
        } else {
            let sh = shards[i].take().expect("sibling shard present (B)");
            drains_b[i] = Some(spawn_ssj_drain(sh, rx, Arc::clone(&ssj_counters[i])));
        }
    }

    engine
        .push_with_cascade_on_shard(
            "R",
            &serde_json::json!({
                "user_id": user,
                "session_id": session,
                "payload": "right",
            }),
            &mut input_shard_b,
            None,
            now,
            true,
            Some(&handles_b),
            s,
        )
        .expect("R push ok");

    // After R, the cross-shard path fired exactly once — to shard J.
    let j_hops = ssj_counters[j].load(Ordering::Relaxed);
    assert_eq!(
        j_hops, 1,
        "SC-2: R push from shard S={s} MUST cross-shard-dispatch exactly one SsjInsert to shard J={j}; got {j_hops}"
    );
    // No side-traffic to any shard other than J.
    let other_hops: u64 = ssj_counters
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != j)
        .map(|(_, c)| c.load(Ordering::Relaxed))
        .sum();
    assert_eq!(
        other_hops, 0,
        "SC-2: zero SsjInsert dispatches to non-J shards; got {other_hops}"
    );

    // The input shard S MUST NOT have a buffer for join_key='u...'.
    // (It may have other state, but the SSJ buffer entity is only
    // present on J.)
    assert!(
        !has_ssj_buffer(&input_shard_b, &user),
        "SC-2: SSJ buffer MUST NOT leak to shard S={s} (session_id owner)"
    );

    // Clean up.
    drop(handles_b);
    for tx in senders_b.drain(..) {
        drop(tx);
    }
    drop(input_shard_b);
    for d in drains_b.into_iter().flatten() {
        let _ = d.join();
    }
}

// ---------------------------------------------------------------------------
// SC-2 corollary — co-located case. Both L and R declare shard_key=user_id.
// Every event (L or R) lands on shard J directly; the SSJ target == input
// shard short-circuits the helper; ZERO cross-shard SsjInsert hops.
// ---------------------------------------------------------------------------

#[test]
fn stream_stream_join_colocated_fast_path() {
    const N: usize = 4;

    let user = "u_0"; // deterministic
    let j = route_by_field(user, "user_id", N);

    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(N);
    let parts: Vec<_> = partitions.into_iter().collect();
    let mut shards: Vec<Option<Shard>> = parts
        .into_iter()
        .map(|p| Some(Shard::with_partition(p)))
        .collect();

    let ssj_counters: Vec<Arc<AtomicU64>> =
        (0..N).map(|_| Arc::new(AtomicU64::new(0))).collect();

    let mut input_shard = shards[j].take().expect("shard J");
    let mut senders: Vec<crossbeam_channel::Sender<ShardEvent>> = Vec::with_capacity(N);
    let mut handles: Vec<ShardHandle> = Vec::with_capacity(N);
    let mut drains: Vec<Option<thread::JoinHandle<Shard>>> = (0..N).map(|_| None).collect();
    for i in 0..N {
        let (tx, rx) = crossbeam_channel::bounded::<ShardEvent>(65_536);
        senders.push(tx.clone());
        handles.push(ShardHandle {
            shard_index: i,
            is_down: Arc::new(AtomicBool::new(false)),
            inbox_tx: tx,
        });
        if i == j {
            std::mem::forget(rx);
        } else {
            let sh = shards[i].take().expect("sibling shard present");
            drains[i] = Some(spawn_ssj_drain(sh, rx, Arc::clone(&ssj_counters[i])));
        }
    }

    let engine = build_engine_colocated();
    let now = SystemTime::now();
    engine
        .push_with_cascade_on_shard(
            "L",
            &serde_json::json!({
                "user_id": user,
                "session_id": "s_colo",
                "payload": "left",
            }),
            &mut input_shard,
            None,
            now,
            true,
            Some(&handles),
            j,
        )
        .expect("L colocated push ok");
    engine
        .push_with_cascade_on_shard(
            "R",
            &serde_json::json!({
                "user_id": user,
                "session_id": "s_colo",
                "payload": "right",
            }),
            &mut input_shard,
            None,
            now,
            true,
            Some(&handles),
            j,
        )
        .expect("R colocated push ok");

    // Zero cross-shard hops — every event landed on J directly, and the
    // helper short-circuited target==input fast path.
    let total_hops: u64 = ssj_counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    assert_eq!(
        total_hops, 0,
        "SC-2 colocated D-B5: zero SsjInsert hops expected when shard_key=join.on on both sides; got {total_hops}"
    );
    assert!(
        has_ssj_buffer(&input_shard, user),
        "SSJ buffer MUST exist on shard J={j} for user={user} after L+R colocated pushes"
    );

    // Clean up.
    drop(handles);
    for tx in senders.drain(..) {
        drop(tx);
    }
    drop(input_shard);
    for d in drains.into_iter().flatten() {
        let _ = d.join();
    }

    // Silence dead-code warnings for unused helpers if any.
    let _ = &JoinSide::Left;
}
