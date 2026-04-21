//! Phase 56 SC-1 — EnrichFromTable cross-shard read correctness (TPC-CORR-08).
//!
//! Contract: `EnrichFromTable` MUST return the correct enrichment regardless
//! of which shard the driving event lands on. When the right-side key hashes
//! to a different shard than the current shard, the operator dispatches
//! `ShardOp::ReadEntityAt { target_shard, table_name, key, reply }` (single
//! key) or `ShardOp::ReadEntityBatch { .. }` (per-target coalesced) and blocks
//! the source shard on the oneshot reply. When `hash(key) % N == current_shard`,
//! the operator reads directly from the local `PartitionHandle` (same-shard
//! fast path — zero inbox hop).
//!
//! Phase 56 Wave 2 (56-02-PLAN): GREEN — cross-shard EnrichFromTable read
//! wired via ShardOp::ReadEntityAt + ReadEntityBatch; same-shard fast path
//! preserved via direct local read in `read_entity_at_shard` helper.
//!
//! Run:
//!   cargo test --release --test cross_shard_enrich_from_table

#![cfg(not(feature = "state-inmem"))]

use ahash::{AHashMap, AHasher};
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;

use beava::engine::pipeline::{FeatureDef, JoinType, PipelineEngine, StreamDefinition};
use beava::routing::shard_hint_for_event;
use beava::shard::read_entity_from_shard;
use beava::shard::thread::{ShardEvent, ShardHandle, ShardOp, ShardResult, MAX_ENRICH_BATCH_KEYS};
use beava::shard::Shard;
use beava::state::snapshot::OperatorState;
use beava::types::FeatureValue;

/// Observe the EnrichedSnap `last_gdp_usd` operator state for a given
/// user_id by cloning the entity's stream state out via
/// `read_entity_from_shard` and invoking `op.read(now)` on the clone.
/// Returns `FeatureValue::Missing` if the entity or stream or operator
/// is absent.
fn read_last_gdp(shard: &Shard, user: &str, now: SystemTime) -> FeatureValue {
    read_entity_from_shard(shard, user, |entity| {
        let stream = match entity.streams.get("EnrichedSnap") {
            Some(s) => s,
            None => return FeatureValue::Missing,
        };
        for (name, op) in &stream.operators {
            if name == "last_gdp_usd" {
                let mut op_clone: OperatorState = op.clone();
                return op_clone.read(now);
            }
        }
        FeatureValue::Missing
    })
    .unwrap_or(FeatureValue::Missing)
}

#[path = "common/mod.rs"]
mod common;

/// Helper — deterministic shard assignment for a given string key.
/// Mirrors the production `shard_hint_for_event` hashing so harness-level
/// shard decisions match the operator's routing.
#[allow(dead_code)]
fn hash_to_shard(key: &str, n_shards: usize) -> usize {
    let mut h = AHasher::default();
    key.hash(&mut h);
    (h.finish() % n_shards as u64) as usize
}

/// Hash a right-side enrichment key the same way the operator does
/// internally — via `shard_hint_for_event({"__k": key}, Some("__k"))`.
/// This is the canonical routing used by `EnrichFromTable::eval` in
/// Wave 2.
fn route_right_key(key: &str, n_shards: usize) -> usize {
    (shard_hint_for_event(&serde_json::json!({ "__k": key }), Some("__k")) as usize) % n_shards
}

// ---------------------------------------------------------------------------
// N-shard drain harness. Each sibling drain thread services ReadEntityAt
// and ReadEntityBatch against its own local `Shard`, replying via the
// oneshot the source shard attached. Optionally pre-seeded with a table
// row before the drain loop starts.
// ---------------------------------------------------------------------------

/// Spawn a drain thread that services `ReadEntityAt` / `ReadEntityBatch`
/// (+ generic ack for any other op) against a local Shard. Mirrors the
/// real `shard_event_loop` arms added in Wave 1 but strips the outer
/// plumbing — matches the tests/cross_shard_tt_cascade.rs pattern.
fn spawn_read_drain(
    shard: Shard,
    rx: crossbeam_channel::Receiver<ShardEvent>,
) -> thread::JoinHandle<Shard> {
    thread::spawn(move || {
        let shard = shard;
        while let Ok(mut event) = rx.recv() {
            let op = std::mem::replace(&mut event.op, ShardOp::Push);
            match op {
                ShardOp::ReadEntityAt { table_name, key } => {
                    let out = shard.read_entity_at(&table_name, &key);
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::ReadEntityOk(out));
                    }
                }
                ShardOp::ReadEntityBatch { table_name, keys } => {
                    if keys.len() > MAX_ENRICH_BATCH_KEYS {
                        if let Some(tx) = event.response_tx {
                            let _ = tx.send(ShardResult::Err(
                                beava::shard::thread::ShardDispatchError::ProcessingError(
                                    format!(
                                        "enrich batch > {} keys ({})",
                                        MAX_ENRICH_BATCH_KEYS,
                                        keys.len()
                                    ),
                                ),
                            ));
                        }
                        continue;
                    }
                    let out: Vec<Option<_>> = keys
                        .iter()
                        .map(|k| shard.read_entity_at(&table_name, k))
                        .collect();
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::ReadEntityBatchOk(out));
                    }
                }
                _ => {
                    // Unreachable in this fixture (we only dispatch
                    // read-ops cross-shard); respond generic OK to avoid
                    // deadlocks if we're wrong.
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::SetOk);
                    }
                }
            }
        }
        shard
    })
}

/// Build the 4-stream engine used by both SC-1 tests:
///   - `Countries` source-table keyed by country_code.
///   - `Txns` stream keyed by user_id (shard_key=user_id implicitly).
///   - `Enriched` keyless stream depending on Txns with a single
///     EnrichFromTable(Countries, on=country_code) feature emitting
///     `gdp_usd` + `continent` into the enriched event.
///   - `EnrichedSnap` stream keyed by user_id depending on Enriched with
///     a `Last(gdp_usd)` feature — observable downstream of the cascade.
fn build_engine() -> PipelineEngine {
    use beava::engine::register::register_source_table;

    let mut engine = PipelineEngine::new();

    // Source-table for Countries — key=country_code.
    register_source_table(
        &mut engine,
        "Countries",
        vec!["country_code".to_string()],
        None,
    );

    // Txns stream — keyed by user_id.
    engine
        .register(StreamDefinition {
            name: "Txns".into(),
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

    // Enriched keyless stream — the EnrichFromTable target. Depends on Txns.
    // key_field=None so Enriched itself stores no entity state; its role
    // is purely to produce enriched events that downstream streams consume.
    engine
        .register(StreamDefinition {
            name: "Enriched".into(),
            key_field: None,
            group_by_keys: None,
            features: vec![(
                "__enrich_from_Countries".into(),
                FeatureDef::EnrichFromTable {
                    right_table: "Countries".into(),
                    on: vec!["country_code".into()],
                    join_type: JoinType::Left,
                    right_fields: vec![
                        ("gdp_usd".into(), "gdp_usd".into()),
                        ("continent".into(), "continent".into()),
                    ],
                },
            )],
            depends_on: Some(vec!["Txns".into()]),
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

    // EnrichedSnap — keyed by user_id. Observes the enriched event's
    // `gdp_usd` field via Last(). Post-push we assert against this
    // stream's computed features at the user's entity.
    engine
        .register(StreamDefinition {
            name: "EnrichedSnap".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![(
                "last_gdp_usd".into(),
                FeatureDef::Last {
                    field: "gdp_usd".into(),
                    optional: true,
                    backfill: false,
                },
            )],
            depends_on: Some(vec!["Enriched".into()]),
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

// ---------------------------------------------------------------------------
// SC-1 primary: right-side key on shard K, driving event on shard J, J≠K.
// The cross-shard ReadEntityAt path MUST resolve the enrichment row.
// ---------------------------------------------------------------------------

/// SC-1 primary — a Txn event on shard-J joined against a Countries source-
/// table row owned by shard-K (J≠K) MUST return an enriched output
/// containing the Country columns (gdp_usd=800_000).
///
/// Wave 2 assertions (flipped GREEN here):
///   - EnrichedSnap entity at user_id on shard J has `last_gdp_usd == 800_000`.
///   - Post-push, the `gdp_usd` field on the enriched downstream event is
///     observable via get_features_on_shard on J (the shard that ran the push).
#[test]
fn enrich_from_table_crosses_shard_boundary() {
    const N: usize = 4;

    // Find a country_code with a known shard assignment, and a user_id
    // whose shard is DIFFERENT — so the enrichment read crosses a shard
    // boundary.
    let country = "CH";
    let k = route_right_key(country, N);
    let mut user = String::new();
    for i in 0u32..8192 {
        let candidate = format!("u_{i}");
        let user_shard = (shard_hint_for_event(
            &serde_json::json!({ "user_id": candidate.clone() }),
            Some("user_id"),
        ) as usize)
            % N;
        if user_shard != k {
            user = candidate;
            break;
        }
    }
    assert!(!user.is_empty(), "no user_id found with shard != {k}");
    let j = (shard_hint_for_event(
        &serde_json::json!({ "user_id": user.clone() }),
        Some("user_id"),
    ) as usize)
        % N;
    assert_ne!(j, k, "test precondition: J != K");

    // Fresh fjall keyspace with N partitions. We run the push on shard J;
    // shard K hosts the Countries row and receives the cross-shard read
    // dispatch; the other two shards receive nothing but we still need
    // valid drain handles for the handles slice.
    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(N);
    let mut parts_iter = partitions.into_iter();
    // Collect all partitions into a Vec so we can index by shard slot.
    let parts: Vec<_> = parts_iter.by_ref().collect();

    // Build Shards per slot — input_shard (j) stays on this thread (we
    // hold &mut during push); siblings run on drain threads.
    let mut shards: Vec<Option<Shard>> = parts
        .into_iter()
        .map(|p| Some(Shard::with_partition(p)))
        .collect();

    // Seed the Countries["CH"] row on shard K BEFORE building handles —
    // the drain thread takes ownership of the shard.
    {
        let k_shard = shards[k].as_mut().expect("shard k still here");
        let mut fields: AHashMap<String, FeatureValue> = AHashMap::new();
        fields.insert("gdp_usd".into(), FeatureValue::Int(800_000));
        fields.insert("continent".into(), FeatureValue::String("EU".into()));
        k_shard.upsert_source_table_row(
            country,
            "Countries",
            fields,
            1, // source_lsn
            SystemTime::now(),
        );
    }

    // Take input shard (J) for the push thread.
    let mut input_shard = shards[j].take().expect("input shard present");

    // Build SPSC channels + drain threads for every non-J shard.
    // handles[i].inbox_tx feeds shard i's drain (for i==j the sender is
    // unused: intra-shard reads go via the &mut Shard directly).
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
        if i == j {
            // Don't spawn a drain for the input shard — the push
            // thread owns input_shard mutably and services its own
            // reads via &mut Shard.
            //
            // Keep the rx alive but unused — drop at end-of-test
            // together with senders. Parking the rx avoids the
            // send-to-self Disconnected error in case the operator
            // ever tries to dispatch to its own shard (shouldn't
            // happen with the same-shard fast path, but defense-in-
            // depth).
            std::mem::forget(rx);
        } else {
            let sh = shards[i].take().expect("sibling shard present");
            drains[i] = Some(spawn_read_drain(sh, rx));
        }
    }

    // Run the push on input_shard (shard J).
    let engine = build_engine();
    let primary_event = serde_json::json!({
        "user_id": user,
        "country_code": country,
        "amount": 100,
    });
    let now = SystemTime::now();
    engine
        .push_with_cascade_on_shard(
            "Txns",
            &primary_event,
            &mut input_shard,
            None, // event_log
            now,
            true, // read_features
            Some(&handles_vec),
            j, // input_shard_idx
        )
        .expect("push ok");

    // Observe: EnrichedSnap entity at user_id on shard J (since
    // downstream aggregation is a regular keyed push written to the
    // input shard's partition) must carry the enriched Country gdp_usd.
    // Inspect the stored LastOp value directly — `get_features_on_shard`
    // only surfaces static_features / table_rows / Derive, not live
    // operator state.
    let last_gdp = read_last_gdp(&input_shard, &user, now);
    assert_eq!(
        last_gdp,
        FeatureValue::Int(800_000),
        "SC-1: EnrichedSnap.last_gdp_usd MUST equal 800_000 after cross-shard \
         EnrichFromTable read (got {:?}); user={user} shard_j={j}, country={country} \
         shard_k={k}",
        last_gdp,
    );

    // Clean up — drop handles + senders so drain threads exit.
    drop(handles_vec);
    for tx in senders.drain(..) {
        drop(tx);
    }
    drop(input_shard);
    for d in drains.into_iter().flatten() {
        let _ = d.join();
    }
}

// ---------------------------------------------------------------------------
// SC-1 corollary: same-shard fast path — user_id and country_code on the
// same shard. The read_entity_at_shard helper MUST bypass SPSC entirely.
// ---------------------------------------------------------------------------

/// SC-1 corollary — when both user_id and country_code hash to the SAME
/// shard, the operator takes the local-read fast path. Observable via
/// (a) correct enrichment output and (b) zero SPSC messages to siblings.
#[test]
fn enrich_from_table_same_shard_fast_path() {
    const N: usize = 4;

    // Find a (user_id, country_code) pair where BOTH hash to the same
    // shard — exercises the same-shard fast path (D-A3).
    let country = "CH";
    let k = route_right_key(country, N);
    let mut user = String::new();
    for i in 0u32..8192 {
        let candidate = format!("u_{i}");
        let user_shard = (shard_hint_for_event(
            &serde_json::json!({ "user_id": candidate.clone() }),
            Some("user_id"),
        ) as usize)
            % N;
        if user_shard == k {
            user = candidate;
            break;
        }
    }
    assert!(!user.is_empty(), "no user_id found with shard == {k}");

    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(N);
    let parts: Vec<_> = partitions.into_iter().collect();
    let mut shards: Vec<Option<Shard>> = parts
        .into_iter()
        .map(|p| Some(Shard::with_partition(p)))
        .collect();

    // Seed Countries["CH"] on shard K. Note: K == J for this test, so
    // the seed lands on the input shard directly.
    {
        let k_shard = shards[k].as_mut().expect("shard k still here");
        let mut fields: AHashMap<String, FeatureValue> = AHashMap::new();
        fields.insert("gdp_usd".into(), FeatureValue::Int(800_000));
        fields.insert("continent".into(), FeatureValue::String("EU".into()));
        k_shard.upsert_source_table_row(
            country,
            "Countries",
            fields,
            1,
            SystemTime::now(),
        );
    }

    let j = k; // by construction
    let mut input_shard = shards[j].take().expect("input shard present");

    // Build handles — shared with a counting drain that records any
    // ReadEntityAt / ReadEntityBatch dispatches. If the fast path is
    // taken correctly, NONE of the sibling drains should see a read op.
    let received_reads = Arc::new(std::sync::atomic::AtomicU64::new(0));
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
        if i == j {
            std::mem::forget(rx);
        } else {
            let sh = shards[i].take().expect("sibling shard present");
            let counter = Arc::clone(&received_reads);
            drains[i] = Some(thread::spawn(move || {
                let shard = sh;
                while let Ok(mut event) = rx.recv() {
                    let op = std::mem::replace(&mut event.op, ShardOp::Push);
                    match op {
                        ShardOp::ReadEntityAt { table_name, key } => {
                            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let out = shard.read_entity_at(&table_name, &key);
                            if let Some(tx) = event.response_tx {
                                let _ = tx.send(ShardResult::ReadEntityOk(out));
                            }
                        }
                        ShardOp::ReadEntityBatch { table_name, keys } => {
                            counter.fetch_add(
                                keys.len() as u64,
                                std::sync::atomic::Ordering::Relaxed,
                            );
                            let out: Vec<Option<_>> = keys
                                .iter()
                                .map(|kk| shard.read_entity_at(&table_name, kk))
                                .collect();
                            if let Some(tx) = event.response_tx {
                                let _ = tx.send(ShardResult::ReadEntityBatchOk(out));
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
            }));
        }
    }

    let engine = build_engine();
    let primary_event = serde_json::json!({
        "user_id": user,
        "country_code": country,
        "amount": 100,
    });
    let now = SystemTime::now();
    engine
        .push_with_cascade_on_shard(
            "Txns",
            &primary_event,
            &mut input_shard,
            None,
            now,
            true,
            Some(&handles_vec),
            j,
        )
        .expect("push ok");

    // Same-shard fast path assertions:
    //   (a) enrichment populated on EnrichedSnap entity at user_id.
    let last_gdp = read_last_gdp(&input_shard, &user, now);
    assert_eq!(
        last_gdp,
        FeatureValue::Int(800_000),
        "same-shard fast path MUST still resolve gdp_usd for user={user} (got {:?})",
        last_gdp,
    );
    //   (b) NO sibling drain saw any read op — the dispatch was purely
    //       a local `shard.read_entity_at` call.
    let n_sibling_reads =
        received_reads.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        n_sibling_reads, 0,
        "SC-1 fast path contract: zero sibling SPSC reads expected \
         when user_id and country_code are co-located on shard {j}; \
         observed {n_sibling_reads}"
    );

    drop(handles_vec);
    for tx in senders.drain(..) {
        drop(tx);
    }
    drop(input_shard);
    for d in drains.into_iter().flatten() {
        let _ = d.join();
    }
}
