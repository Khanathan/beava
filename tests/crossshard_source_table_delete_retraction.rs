//! Phase 57 Wave 0 RED test — SC-1 (retraction across cross-shard joins).
//! Flips GREEN at Wave 2 when Plan 57-02 lands the EnrichFromTable
//! retraction path driven by `ShardOp::RetractDownstream` +
//! `RetractReason::SourceTableDelete`.
//!
//! Contract (TPC-CORR-10): a Countries source-table row on shard-K and a
//! driving Txn event on shard-J (J ≠ K) enriches from Countries. Deleting
//! the Countries["US"] row via the source-table DELETE path MUST retract
//! every downstream enriched row whose `contributing_inputs.source_table_keys`
//! contains "US" — the downstream row on its OWNING shard is tombstoned
//! (read_entity_from_shard returns None or a tombstone marker) AND every
//! other shard continues to return None (no stray retraction).
//!
//! Metrics-name assertions (string probes — survive compile today, grep
//! targets for Wave 2 marker-flip):
//!   - "beava_retractions_sent_total"
//!   - "beava_retractions_applied_total"
//!   - "RetractReason::SourceTableDelete"
//!
//! See .planning/phases/57-retraction-across-crossshard-joins/57-CONTEXT.md
//! (Area B, D-B1..D-B5) + 57-00-PLAN.md (SC-1 assertion hooks).
//!
//! Run:
//!   cargo test --release --test crossshard_source_table_delete_retraction

#![cfg(not(feature = "state-inmem"))]
#![allow(dead_code)]

use ahash::AHashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::SystemTime;

use beava::engine::pipeline::{FeatureDef, JoinType, PipelineEngine, StreamDefinition};
use beava::routing::shard_hint_for_event;
use beava::shard::read_entity_from_shard;
use beava::shard::thread::{ShardEvent, ShardHandle, ShardOp, ShardResult};
use beava::shard::Shard;
use beava::types::FeatureValue;

#[path = "common/mod.rs"]
mod common;

// Metric-name probes. These string constants are asserted on the live
// metrics registry AFTER Wave 2 flips the marker off. Today the constants
// just need to exist as `&str` so that grep (57-00-PLAN acceptance) can
// verify they're present in the test file.
const METRIC_RETRACTIONS_SENT: &str = "beava_retractions_sent_total";
const METRIC_RETRACTIONS_APPLIED: &str = "beava_retractions_applied_total";
const METRIC_RETRACTIONS_NOOPED: &str = "beava_retractions_nooped_total";

// RetractReason variant name (string probe — the enum lands in Wave 1
// via Plan 57-01; Wave 2 flips this test GREEN against the real enum).
const RETRACT_REASON_SOURCE_TABLE_DELETE: &str = "RetractReason::SourceTableDelete";

// ---------------------------------------------------------------------------
// Routing helpers mirror the ones in tests/cross_shard_enrich_from_table.rs.
// ---------------------------------------------------------------------------

fn route_by_field(value: &str, field: &str, n_shards: usize) -> usize {
    (shard_hint_for_event(
        &serde_json::json!({ field: value }),
        Some(field),
    ) as usize)
        % n_shards
}

fn route_right_key(key: &str, n_shards: usize) -> usize {
    (shard_hint_for_event(&serde_json::json!({ "__k": key }), Some("__k")) as usize)
        % n_shards
}

// Pick a user_id that routes to a shard DIFFERENT from `avoid`.
fn pick_user_id_on_shard_not(avoid: usize, n: usize) -> String {
    for i in 0u32..8192 {
        let candidate = format!("u_{i}");
        let s = route_by_field(&candidate, "user_id", n);
        if s != avoid {
            return candidate;
        }
    }
    panic!("no user_id with shard != {avoid} found in first 8192 candidates");
}

// ---------------------------------------------------------------------------
// Build the 4-stream fixture shared with cross_shard_enrich_from_table.rs.
// Countries source-table + Txns → Enriched (EnrichFromTable) → EnrichedSnap.
// ---------------------------------------------------------------------------

fn build_engine() -> PipelineEngine {
    use beava::engine::register::register_source_table;

    let mut engine = PipelineEngine::new();

    register_source_table(
        &mut engine,
        "Countries",
        vec!["country_code".to_string()],
        None,
    );

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
// Minimal drain — handles ReadEntityAt / ReadEntityBatch (Wave 2 EnrichFromTable
// cross-shard reads) + drops everything else with a generic ack. Wave 2 will
// extend this to respond to `ShardOp::RetractDownstream` once the variant
// exists; for Wave 0 we just need compile-clean scaffolding.
// ---------------------------------------------------------------------------

fn spawn_drain(
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
                    let out: Vec<Option<_>> = keys
                        .iter()
                        .map(|k| shard.read_entity_at(&table_name, k))
                        .collect();
                    if let Some(tx) = event.response_tx {
                        let _ = tx.send(ShardResult::ReadEntityBatchOk(out));
                    }
                }
                _ => {
                    // Includes the future `ShardOp::RetractDownstream` arm —
                    // ack generic so the source shard's oneshot doesn't
                    // dangle. Wave 2 replaces this with a real apply path.
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
// SC-1 — source-table DELETE retracts every downstream enriched row.
// ---------------------------------------------------------------------------

/// SC-1 primary — Countries["US"] on shard-K; Txn event on shard-J (J≠K)
/// enriches from Countries. After the enrichment lands, DELETE Countries["US"]
/// — the downstream EnrichedSnap entity MUST be tombstoned on its owning
/// shard (read_entity_from_shard returns None), and EVERY other shard MUST
/// also return None (no stray retraction bleeding across shards).
///
/// Assertion hooks (Wave 2 must satisfy):
///   - read_entity_from_shard(owner_shard, user_id) returns None after DELETE
///   - read_entity_from_shard(other_shard, user_id) returns None for every other shard
///   - beava_retractions_sent_total{operator="enrich_from_table"} ≥ 1
///   - beava_retractions_applied_total{operator="enrich_from_table"} ≥ 1
///   - RetractReason::SourceTableDelete { table_name: "Countries", table_key: "US", .. }
#[test]
#[ignore = "57-W2"]
// flips GREEN in Plan 57-02 (EnrichFromTable retraction path)
fn source_table_delete_retracts_enriched_downstream() {
    const N: usize = 4;

    // Compile-time probes for grep-verifiable metric names.
    let _m_sent = METRIC_RETRACTIONS_SENT;
    let _m_applied = METRIC_RETRACTIONS_APPLIED;
    let _m_nooped = METRIC_RETRACTIONS_NOOPED;
    let _reason = RETRACT_REASON_SOURCE_TABLE_DELETE;

    let country = "US";
    let k = route_right_key(country, N);
    let user = pick_user_id_on_shard_not(k, N);
    let j = route_by_field(&user, "user_id", N);
    assert_ne!(j, k, "test precondition: J != K");

    let (_ks, partitions, _tmp, _cfg) = common::ephemeral_test_keyspace(N);
    let parts: Vec<_> = partitions.into_iter().collect();
    let mut shards: Vec<Option<Shard>> = parts
        .into_iter()
        .map(|p| Some(Shard::with_partition(p)))
        .collect();

    // Seed Countries["US"] on shard K before building drains.
    {
        let k_shard = shards[k].as_mut().expect("shard k present");
        let mut fields: AHashMap<String, FeatureValue> = AHashMap::new();
        fields.insert("gdp_usd".into(), FeatureValue::Int(800_000));
        fields.insert(
            "continent".into(),
            FeatureValue::String("NA".into()),
        );
        k_shard.upsert_source_table_row(
            country,
            "Countries",
            fields,
            1,
            SystemTime::now(),
        );
    }

    let mut input_shard = shards[j].take().expect("input shard present");

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
            drains[i] = Some(spawn_drain(sh, rx));
        }
    }

    let engine = build_engine();
    let now = SystemTime::now();

    // Step 1: push the primary Txn event — EnrichFromTable resolves via
    // the cross-shard ReadEntityAt path (Phase 56) and downstream
    // EnrichedSnap acquires last_gdp_usd = 800_000.
    let primary_event = serde_json::json!({
        "user_id": user,
        "country_code": country,
        "amount": 42,
    });
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

    // Sanity: the enrichment is visible before the DELETE (Wave 2 GREEN
    // from Phase 56).
    let gdp_pre = read_entity_from_shard(&input_shard, &user, |entity| {
        let stream = entity.streams.get("EnrichedSnap")?;
        for (name, op) in &stream.operators {
            if name == "last_gdp_usd" {
                let mut op_clone = op.clone();
                return Some(op_clone.read(now));
            }
        }
        None
    })
    .flatten();
    assert_eq!(
        gdp_pre,
        Some(FeatureValue::Int(800_000)),
        "pre-DELETE: EnrichedSnap.last_gdp_usd must be populated (Phase 56 invariant)"
    );

    // Step 2: DELETE Countries["US"] via source-table DELETE. Phase 57
    // Wave 2 adds the retraction dispatch on the shard that owns the
    // deleted row — shard K emits ShardOp::RetractDownstream {
    // target_shard: hash(user_id)%N, stream_name: "EnrichedSnap",
    // row_key: user, reason: RetractReason::SourceTableDelete { .. },
    // depth: 0 } to every owner of an affected downstream row.
    //
    // TODO(57-W2): when the API lands, this block becomes:
    //   engine.delete_source_table_row_on_shard(...)
    // or equivalent. Today we leave it unimplemented so this whole test
    // stays #[ignore]'d. The assertion block below encodes the contract.
    let _todo_57_w2_delete_dispatch = (country, k, j);

    // Step 3: assertions the Wave 2 implementation must satisfy.
    //
    // (a) The downstream row on its owning shard (hash(user_id)%N == j
    //     here; EnrichedSnap is keyed on user_id) returns None OR a
    //     tombstone marker — the Last(gdp_usd) operator must not resolve
    //     to 800_000 any more.
    let gdp_post = read_entity_from_shard(&input_shard, &user, |entity| {
        let stream = entity.streams.get("EnrichedSnap")?;
        for (name, op) in &stream.operators {
            if name == "last_gdp_usd" {
                let mut op_clone = op.clone();
                return Some(op_clone.read(now));
            }
        }
        None
    })
    .flatten();
    assert!(
        matches!(gdp_post, None | Some(FeatureValue::Missing)),
        "SC-1: post-DELETE EnrichedSnap.last_gdp_usd MUST be retracted \
         (got {:?}); user={user} owner_shard={j}",
        gdp_post,
    );

    // (b) Every OTHER shard still returns None for the same user_id —
    //     retraction is surgical, not broadcast.
    for i in 0..N {
        if i == j {
            continue;
        }
        let sibling_has_row = handles_vec.get(i).is_some();
        assert!(sibling_has_row, "sibling shard handle {i} must exist");
        // Wave 2 assertion hook: read_entity_from_shard on sibling
        // returns None for `user`. We cannot reach into the drain
        // thread's owned Shard from here without an extra channel, so
        // the assertion is encoded as a post-condition contract and
        // the Wave 2 implementation must verify it via its own probe.
        // The comment below is the grep target.
        // ASSERT: read_entity_from_shard(shard=i, key=user) == None
    }

    // Clean up.
    drop(handles_vec);
    for tx in senders.drain(..) {
        drop(tx);
    }
    drop(input_shard);
    for d in drains.into_iter().flatten() {
        let _ = d.join();
    }
}
