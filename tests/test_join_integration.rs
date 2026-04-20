//! Phase 23-03 Task 2 — Cross-shape join integration tests.
//!
//! Exercises DAGs that combine MULTIPLE join shapes in a single pipeline to
//! ensure cascade ordering, effective-event propagation, and aggregation
//! composition work correctly end-to-end. These are the gate on "joins
//! compose with other joins and with Phase 22 aggregations without
//! surprises."
//!
//! Three shapes — all driven through the same REGISTER JSON path as the
//! per-shape unit tests in `test_join_stream_table.rs`,
//! `test_join_stream_stream.rs`, and `test_join_table_table.rs`:
//!
//!   1. `dag_enrich_then_aggregate` — Clicks → Enrich(UserProfile) → agg
//!   2. `dag_ss_join_then_enrich`  — Orders.ss_join(Payments) → Enrich(UP)
//!   3. `dag_tt_join_feeds_enrich` — (Smoke) TT-join output Table used as
//!      enrichment right side. Acceptance-level regression.

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

/// (1) Stream → Enrich(Table) → group_by(right-side field).agg(count).
///     Asserts enriched events feed into the downstream aggregation and
///     bucket by the joined field `country`.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn dag_enrich_then_aggregate() {
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}

/// (2) Stream↔Stream join then Enrich by a Table — stresses cascade
///     ordering when a join stream's output is consumed by an enrichment.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn dag_ss_join_then_enrich() {
    // Phase 54-04 Pass B: legacy push/cascade helper deleted. Body stubbed
    // pending Pass C on_shard rewrite.
    unimplemented!("54-04 Pass B: legacy helper deleted; rewrite via on_shard path in Pass C")
}

/// (3) Table↔Table output used as enrichment right side.
///     Acceptance-level smoke: registers TT-join and verifies translator
///     emits the expected StreamDefinition shape for the output Table.
#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn dag_tt_join_feeds_enrich() {
    let mut engine = PipelineEngine::new();

    for (name, fields) in [
        (
            "Profile",
            r#"{"user_id":{"type":"str","optional":false},"country":{"type":"str","optional":false}}"#,
        ),
        (
            "Risk",
            r#"{"user_id":{"type":"str","optional":false},"score":{"type":"int","optional":false}}"#,
        ),
    ] {
        let json = format!(
            r#"{{"name":"{}","kind":"table","mode":"overwrite","key_field":"user_id","fields":{}}}"#,
            name, fields
        );
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        let def = match parse(&json) {
            V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
            _ => panic!(),
        };
        engine.register(def).unwrap();
        engine.store_raw_register_json(name, val);
    }

    // ProfileRisk = Profile.tt_join(Risk)
    let pr = r#"{"name":"ProfileRisk","kind":"table","mode":"overwrite","key_field":"user_id",
        "fields":{"user_id":{"type":"str","optional":false},"country":{"type":"str","optional":false},"score":{"type":"int","optional":false}},
        "join":{"op":"join","left":"Profile","right":"Risk","on":["user_id"],"type":"inner","shape":"table_table"},
        "depends_on":["Profile","Risk"]}"#;
    let pr_desc = match parse(pr) {
        V0RegisterPayload::Join(d) => d,
        _ => panic!(),
    };
    let fl = |name: &str| -> Option<Vec<String>> {
        match name {
            "Profile" => Some(vec!["user_id".into(), "country".into()]),
            "Risk" => Some(vec!["user_id".into(), "score".into()]),
            _ => None,
        }
    };
    let pr_def = v0_join_to_stream_def(&pr_desc, Some(&fl)).unwrap();
    let pr_name = pr_def.name.clone();
    engine.register(pr_def).unwrap();
    // Smoke: the output Table was registered successfully and carries the
    // TableTableJoin FeatureDef. That's the cross-shape integration gate.
    let sd = engine.get_stream(&pr_name).expect("ProfileRisk registered");
    assert_eq!(sd.key_field.as_deref(), Some("user_id"));
    assert!(!sd.features.is_empty(), "TT-join feature registered");
}
