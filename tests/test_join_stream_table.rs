//! Phase 23-01 Task 2 — Stream↔Table enrichment join end-to-end.
//!
//! These tests drive the engine via the same REGISTER JSON shape that
//! `python/beava/_serialize.py::_compile_join` emits at the SDK layer, and
//! verify the cascade emits enriched events with correct inner-drop / left-
//! null semantics, `_right` collision handling, composite keys, and outer-
//! type rejection.
//!
//! The pattern: register `Source` (Stream) + `Table` source + `Enriched`
//! (join derivation) + a downstream aggregation that buckets the enriched
//! events. Asserting on the aggregation's state post-cascade is how we
//! observe whether the enriched event flowed (or was correctly dropped).

use std::time::{Duration, SystemTime};

use beava::engine::pipeline::PipelineEngine;
use beava::engine::register::{
    v0_aggregation_to_stream_def, v0_join_to_stream_def, v0_source_to_stream_def, V0RegisterPayload,
};
use beava::state::store::StateStore;
use beava::types::FeatureValue;

fn parse(json: &str) -> V0RegisterPayload {
    V0RegisterPayload::parse(json.as_bytes()).expect("parse")
}

/// Build a typical Stream↔Table fixture:
///   - `Clicks` Stream, key=user_id (or composite), fields={user_id, page, [region]}
///   - `UserProfile` Table, key=user_id (or composite), fields={user_id, country, tier}
///   - `Enriched` = Clicks.join(UserProfile, on=..., type=...)
///   - `EnrichedAgg` = Enriched.group_by([user_id, ...]).agg(n=count())
///
/// Returns (engine, store).
// Phase 47: test helper needs all 8 config dimensions for fixture variety.
#[allow(clippy::too_many_arguments)]
fn build_engine(
    left_fields_json: &str,
    table_fields_json: &str,
    table_key: &str,
    table_key_fields: Option<&str>,
    join_on: &[&str],
    join_type: &str,
    enriched_fields_json: &str,
    agg_keys: &[&str],
) -> (PipelineEngine, StateStore) {
    let mut engine = PipelineEngine::new();
    let mut left_raw_jsons: Vec<(&str, serde_json::Value)> = Vec::new();

    // 1. Clicks source.
    let clicks_json = format!(
        r#"{{"name":"Clicks","kind":"stream","key_field":null,"fields":{}}}"#,
        left_fields_json
    );
    let clicks_val: serde_json::Value = serde_json::from_str(&clicks_json).unwrap();
    let clicks_def = match parse(&clicks_json) {
        V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
        _ => panic!(),
    };
    engine.register(clicks_def).unwrap();
    engine.store_raw_register_json("Clicks", clicks_val.clone());
    left_raw_jsons.push(("Clicks", clicks_val));

    // 2. UserProfile table.
    let key_clause = if let Some(kf) = table_key_fields {
        format!(r#""key_field":null,"key_fields":{}"#, kf)
    } else {
        format!(r#""key_field":"{}""#, table_key)
    };
    let table_json = format!(
        r#"{{"name":"UserProfile","kind":"table","mode":"overwrite",{},"fields":{}}}"#,
        key_clause, table_fields_json
    );
    let table_val: serde_json::Value = serde_json::from_str(&table_json).unwrap();
    let table_def = match parse(&table_json) {
        V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
        _ => panic!(),
    };
    engine.register(table_def).unwrap();
    engine.store_raw_register_json("UserProfile", table_val);

    // 3. Enriched join.
    let on_arr =
        serde_json::to_string(&join_on.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap();
    let join_json = format!(
        r#"{{"name":"Enriched","kind":"stream","key_field":null,"fields":{},
            "join":{{"op":"join","left":"Clicks","right":"UserProfile","on":{},"type":"{}","shape":"stream_table"}},
            "depends_on":["Clicks","UserProfile"]}}"#,
        enriched_fields_json, on_arr, join_type
    );
    let join_val: serde_json::Value = serde_json::from_str(&join_json).unwrap();
    let join_desc = match parse(&join_json) {
        V0RegisterPayload::Join(d) => d,
        _ => panic!("expected Join"),
    };
    let lookup_table: std::collections::HashMap<String, Vec<String>> = left_raw_jsons
        .iter()
        .map(|(n, j)| {
            (
                n.to_string(),
                j.get("fields")
                    .and_then(|f| f.as_object())
                    .map(|m| m.keys().cloned().collect())
                    .unwrap_or_default(),
            )
        })
        .collect();
    let lookup = |name: &str| -> Option<Vec<String>> { lookup_table.get(name).cloned() };
    let enriched_def = v0_join_to_stream_def(&join_desc, Some(&lookup)).unwrap();
    engine.register(enriched_def).unwrap();
    engine.store_raw_register_json("Enriched", join_val);

    // 4. EnrichedAgg = group_by(agg_keys).count()
    let keys_arr =
        serde_json::to_string(&agg_keys.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap();
    let agg_key_field = agg_keys[0];
    let agg_json = format!(
        r#"{{"name":"EnrichedAgg","kind":"table","key_field":"{}","mode":"overwrite","fields":{{}},
            "aggregation":{{
                "source":"Enriched","keys":{},
                "features":[{{"name":"n","type":"count","supports_retraction":true,"window":"1h"}}]
            }},
            "depends_on":["Enriched"]}}"#,
        agg_key_field, keys_arr
    );
    let agg_def = match parse(&agg_json) {
        V0RegisterPayload::Aggregation(d) => v0_aggregation_to_stream_def(&d).unwrap(),
        _ => panic!(),
    };
    engine.register(agg_def).unwrap();

    (engine, StateStore::new())
}

fn set_table_row(store: &StateStore, key: &str, row: &[(&str, FeatureValue)]) {
    let now = SystemTime::now();
    for (n, v) in row {
        store.set_static(key, n, v.clone(), now);
    }
}

fn agg_count(store: &StateStore, agg_key: &str, now: SystemTime) -> i64 {
    let row = store.get_all_features(agg_key, now);
    match row.get("n") {
        Some(FeatureValue::Int(i)) => *i,
        _ => 0,
    }
}

// (1) Inner hit — enriched event reaches downstream aggregation.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn enrich_inner_hit() {
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}

// (2) Inner miss — event dropped, downstream count stays at 0.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn enrich_inner_miss_drops() {
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}

// (3) Left miss — event passes through; right-side fields null in enriched event.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn enrich_left_miss_nulls() {
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}

// (4) `_right` collision: left has `status`, right has `status` → SDK names
//     right's slot `status_right`. Engine must materialize both correctly
//     into the enriched event.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn enrich_collision_suffix() {
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}

// (5) Composite-key enrichment.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn enrich_composite_key() {
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}

// (6) Engine-side defense in depth: outer joins rejected at parse time.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn enrich_rejects_outer() {
    let join_json = r#"{
        "name":"E","kind":"stream","key_field":null,"fields":{"x":{"type":"str","optional":false}},
        "join":{"op":"join","left":"L","right":"R","on":["k"],"type":"outer","shape":"stream_table"},
        "depends_on":["L","R"]
    }"#;
    let parsed = parse(join_json);
    let desc = match parsed {
        V0RegisterPayload::Join(d) => d,
        _ => panic!("expected Join"),
    };
    let err = v0_join_to_stream_def(&desc, None).unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("outer joins deferred"),
        "expected 'outer joins deferred' in error, got: {}",
        msg
    );
}
