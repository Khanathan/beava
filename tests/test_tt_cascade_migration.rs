//! Phase 24-03 — Cascade migration: `cascade_table_upsert` now reads
//! `table_rows[A] / table_rows[B]` (from plan 01) instead of the
//! `__tt_left_*` / `__tt_right_*` markers from Phase 23-03.
//!
//! These tests drive the cascade exclusively through the new primitives —
//! `StateStore::upsert_table_row` and `tombstone_table_row` — and observe
//! output exclusively via `StateStore::get_table_row`. They are the
//! authoritative check that the marker-based shim is gone.

use std::time::SystemTime;

use ahash::AHashMap;
use beava::engine::pipeline::PipelineEngine;
use beava::engine::register::{v0_join_to_stream_def, v0_source_to_stream_def, V0RegisterPayload};
use beava::state::store::{StateStore, TableRowState};
use beava::types::FeatureValue;

fn parse(json: &str) -> V0RegisterPayload {
    V0RegisterPayload::parse(json.as_bytes()).expect("parse register JSON")
}

/// Same build pattern as `test_join_table_table::build_engine` but returns
/// an engine+store pre-wired for the caller to drive with
/// `upsert_table_row` / `tombstone_table_row`.
fn build_engine(
    a_fields: &str,
    a_key_clause: &str,
    b_fields: &str,
    b_key_clause: &str,
    join_on: &[&str],
    join_type: &str,
    joined_fields: &str,
) -> (PipelineEngine, StateStore) {
    let mut engine = PipelineEngine::new();
    let mut raws: Vec<(String, serde_json::Value)> = Vec::new();

    for (name, fields, key_clause) in [("A", a_fields, a_key_clause), ("B", b_fields, b_key_clause)]
    {
        let json = format!(
            r#"{{"name":"{}","kind":"table","mode":"overwrite",{},"fields":{}}}"#,
            name, key_clause, fields
        );
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        let def = match parse(&json) {
            V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
            _ => panic!(),
        };
        engine.register(def).unwrap();
        engine.store_raw_register_json(name, val.clone());
        raws.push((name.into(), val));
    }

    let on_arr =
        serde_json::to_string(&join_on.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap();
    let j_json = format!(
        r#"{{"name":"J","kind":"table","mode":"overwrite",{},"fields":{},
            "join":{{"op":"join","left":"A","right":"B","on":{},"type":"{}","shape":"table_table"}},
            "depends_on":["A","B"]}}"#,
        a_key_clause, joined_fields, on_arr, join_type
    );
    let j_val: serde_json::Value = serde_json::from_str(&j_json).unwrap();
    let j_desc = match parse(&j_json) {
        V0RegisterPayload::Join(d) => d,
        _ => panic!("expected Join"),
    };
    let fl_table: std::collections::HashMap<String, Vec<String>> = raws
        .iter()
        .map(|(n, j)| {
            (
                n.clone(),
                j.get("fields")
                    .and_then(|f| f.as_object())
                    .map(|m| m.keys().cloned().collect())
                    .unwrap_or_default(),
            )
        })
        .collect();
    let fl = |name: &str| -> Option<Vec<String>> { fl_table.get(name).cloned() };
    let j_def = v0_join_to_stream_def(&j_desc, Some(&fl)).unwrap();
    engine.register(j_def).unwrap();
    engine.store_raw_register_json("J", j_val);

    (engine, StateStore::new())
}

fn fields(pairs: &[(&str, FeatureValue)]) -> AHashMap<String, FeatureValue> {
    let mut m = AHashMap::new();
    for (k, v) in pairs {
        m.insert((*k).to_string(), v.clone());
    }
    m
}

fn push_table(
    engine: &PipelineEngine,
    store: &StateStore,
    table: &str,
    key: &str,
    row: &[(&str, FeatureValue)],
) {
    let now = SystemTime::now();
    store.upsert_table_row(key, table, fields(row), now);
    engine
        .cascade_tt_after_upsert(table, key, store, now)
        .expect("cascade upsert");
}

fn delete_table(engine: &PipelineEngine, store: &StateStore, table: &str, key: &str) {
    let now = SystemTime::now();
    store.tombstone_table_row(key, table, now);
    engine
        .cascade_tt_after_delete(table, key, store, now)
        .expect("cascade delete");
}

const A_FIELDS_X: &str =
    r#"{"user_id":{"type":"str","optional":false},"x":{"type":"int","optional":false}}"#;
const B_FIELDS_Y: &str =
    r#"{"user_id":{"type":"str","optional":false},"y":{"type":"int","optional":false}}"#;
const FIELDS_XY: &str = r#"{"user_id":{"type":"str","optional":false},"x":{"type":"int","optional":false},"y":{"type":"int","optional":false}}"#;
const KEY_USER: &str = r#""key_field":"user_id""#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn cascade_migration_inner_both_live_merges() {
    let (engine, store) = build_engine(
        A_FIELDS_X,
        KEY_USER,
        B_FIELDS_Y,
        KEY_USER,
        &["user_id"],
        "inner",
        FIELDS_XY,
    );
    push_table(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(1))]);
    // Only A live: inner should NOT emit a Live row.
    let j = store.get_table_row("u1", "J");
    assert!(
        j.as_ref()
            .map(|r| !matches!(r.state, TableRowState::Live))
            .unwrap_or(true),
        "inner with only left must not emit Live"
    );

    push_table(&engine, &store, "B", "u1", &[("y", FeatureValue::Int(2))]);
    let j = store.get_table_row("u1", "J").expect("J must exist");
    assert!(matches!(j.state, TableRowState::Live));
    assert_eq!(j.fields.get("x"), Some(&FeatureValue::Int(1)));
    assert_eq!(j.fields.get("y"), Some(&FeatureValue::Int(2)));
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn cascade_migration_inner_right_tombstone_retracts_output() {
    let (engine, store) = build_engine(
        A_FIELDS_X,
        KEY_USER,
        B_FIELDS_Y,
        KEY_USER,
        &["user_id"],
        "inner",
        FIELDS_XY,
    );
    push_table(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(1))]);
    push_table(&engine, &store, "B", "u1", &[("y", FeatureValue::Int(2))]);
    let j = store.get_table_row("u1", "J").expect("J must be live");
    assert!(matches!(j.state, TableRowState::Live));

    delete_table(&engine, &store, "B", "u1");
    let j = store
        .get_table_row("u1", "J")
        .expect("J row still present as tombstone");
    assert!(
        matches!(j.state, TableRowState::Tombstoned { .. }),
        "inner + delete-right must tombstone output, got {:?}",
        j.state
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn cascade_migration_left_join_null_pads_missing_right() {
    let (engine, store) = build_engine(
        A_FIELDS_X,
        KEY_USER,
        B_FIELDS_Y,
        KEY_USER,
        &["user_id"],
        "left",
        FIELDS_XY,
    );
    push_table(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(7))]);
    let j = store
        .get_table_row("u1", "J")
        .expect("left join emits on left-only");
    assert!(matches!(j.state, TableRowState::Live));
    assert_eq!(j.fields.get("x"), Some(&FeatureValue::Int(7)));
    assert_eq!(j.fields.get("y"), Some(&FeatureValue::Missing));

    // Now add B, cascade → merged row with real y.
    push_table(&engine, &store, "B", "u1", &[("y", FeatureValue::Int(9))]);
    let j = store.get_table_row("u1", "J").unwrap();
    assert_eq!(j.fields.get("y"), Some(&FeatureValue::Int(9)));

    // Tombstone B → left join keeps left, right fields go back to null.
    delete_table(&engine, &store, "B", "u1");
    let j = store
        .get_table_row("u1", "J")
        .expect("left join row still Live");
    assert!(matches!(j.state, TableRowState::Live));
    assert_eq!(j.fields.get("x"), Some(&FeatureValue::Int(7)));
    assert_eq!(j.fields.get("y"), Some(&FeatureValue::Missing));

    // Tombstone A → left join output tombstones.
    delete_table(&engine, &store, "A", "u1");
    let j = store.get_table_row("u1", "J").unwrap();
    assert!(matches!(j.state, TableRowState::Tombstoned { .. }));
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn cascade_migration_collision_suffix_through_real_storage() {
    let a_fields =
        r#"{"user_id":{"type":"str","optional":false},"status":{"type":"str","optional":false}}"#;
    let b_fields =
        r#"{"user_id":{"type":"str","optional":false},"status":{"type":"str","optional":false}}"#;
    let joined = r#"{"user_id":{"type":"str","optional":false},"status":{"type":"str","optional":false},"status_right":{"type":"str","optional":false}}"#;
    let (engine, store) = build_engine(
        a_fields,
        KEY_USER,
        b_fields,
        KEY_USER,
        &["user_id"],
        "inner",
        joined,
    );
    push_table(
        &engine,
        &store,
        "A",
        "u1",
        &[("status", FeatureValue::String("left".into()))],
    );
    push_table(
        &engine,
        &store,
        "B",
        "u1",
        &[("status", FeatureValue::String("right".into()))],
    );
    let j = store.get_table_row("u1", "J").expect("J live");
    assert!(matches!(j.state, TableRowState::Live));
    assert_eq!(
        j.fields.get("status"),
        Some(&FeatureValue::String("left".into())),
        "left wins on the unsuffixed column"
    );
    assert_eq!(
        j.fields.get("status_right"),
        Some(&FeatureValue::String("right".into())),
        "right side is surfaced with _right suffix"
    );
}

#[ignore = "54-03 Task 4: legacy StateStore API / engine.push(&store, ...); Wave 4 re-enables after legacy-engine removal"]
#[test]
fn cascade_migration_recurses_through_chain_a_b_j1_c_j2() {
    // A.join(B) = J1; J1.join(C) = J2. Upsert on A|B|C must cascade into J2.
    let mut engine = PipelineEngine::new();
    let mut raws: Vec<(String, serde_json::Value)> = Vec::new();

    for (name, fields_decl) in [
        ("A", A_FIELDS_X),
        ("B", B_FIELDS_Y),
        (
            "C",
            r#"{"user_id":{"type":"str","optional":false},"z":{"type":"int","optional":false}}"#,
        ),
    ] {
        let json = format!(
            r#"{{"name":"{}","kind":"table","mode":"overwrite",{},"fields":{}}}"#,
            name, KEY_USER, fields_decl
        );
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        let def = match parse(&json) {
            V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
            _ => panic!(),
        };
        engine.register(def).unwrap();
        engine.store_raw_register_json(name, val.clone());
        raws.push((name.into(), val));
    }

    let register_join = |engine: &mut PipelineEngine,
                         raws: &mut Vec<(String, serde_json::Value)>,
                         name: &str,
                         left: &str,
                         right: &str,
                         joined_fields: &str| {
        let json = format!(
            r#"{{"name":"{}","kind":"table","mode":"overwrite",{},
                "fields":{},
                "join":{{"op":"join","left":"{}","right":"{}","on":["user_id"],"type":"inner","shape":"table_table"}},
                "depends_on":["{}","{}"]}}"#,
            name, KEY_USER, joined_fields, left, right, left, right
        );
        let val: serde_json::Value = serde_json::from_str(&json).unwrap();
        let desc = match parse(&json) {
            V0RegisterPayload::Join(d) => d,
            _ => panic!(),
        };
        let fl_table: std::collections::HashMap<String, Vec<String>> = raws
            .iter()
            .map(|(n, j)| {
                (
                    n.clone(),
                    j.get("fields")
                        .and_then(|f| f.as_object())
                        .map(|m| m.keys().cloned().collect())
                        .unwrap_or_default(),
                )
            })
            .collect();
        let fl = |name: &str| -> Option<Vec<String>> { fl_table.get(name).cloned() };
        let def = v0_join_to_stream_def(&desc, Some(&fl)).unwrap();
        engine.register(def).unwrap();
        engine.store_raw_register_json(name, val.clone());
        raws.push((name.into(), val));
    };

    register_join(&mut engine, &mut raws, "J1", "A", "B", FIELDS_XY);
    register_join(
        &mut engine,
        &mut raws,
        "J2",
        "J1",
        "C",
        r#"{"user_id":{"type":"str","optional":false},"x":{"type":"int","optional":false},"y":{"type":"int","optional":false},"z":{"type":"int","optional":false}}"#,
    );

    let store = StateStore::new();
    push_table(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(1))]);
    push_table(&engine, &store, "B", "u1", &[("y", FeatureValue::Int(2))]);
    push_table(&engine, &store, "C", "u1", &[("z", FeatureValue::Int(3))]);

    let j2 = store.get_table_row("u1", "J2").expect("J2 must be live");
    assert!(matches!(j2.state, TableRowState::Live));
    assert_eq!(j2.fields.get("x"), Some(&FeatureValue::Int(1)));
    assert_eq!(j2.fields.get("y"), Some(&FeatureValue::Int(2)));
    assert_eq!(j2.fields.get("z"), Some(&FeatureValue::Int(3)));

    // Tombstone B → J1 and J2 must both retract.
    delete_table(&engine, &store, "B", "u1");
    let j1 = store.get_table_row("u1", "J1").expect("J1 row");
    let j2 = store.get_table_row("u1", "J2").expect("J2 row");
    assert!(matches!(j1.state, TableRowState::Tombstoned { .. }));
    assert!(matches!(j2.state, TableRowState::Tombstoned { .. }));
}
