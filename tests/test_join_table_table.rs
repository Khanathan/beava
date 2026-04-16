//! Phase 23-03 Task 1 — Table↔Table same-key join end-to-end.
//!
//! Drives the engine via the same REGISTER JSON shape that
//! `python/beava/_serialize.py::_compile_join` emits at the SDK layer for
//! `shape="table_table"`. Verifies:
//!
//!   * `inner` / `left` merge on either-side upsert
//!   * `inner` / `left` tombstone propagation on delete
//!   * `_right` collision suffix (SDK applies; engine passes through)
//!   * Composite-key joins
//!   * Registration-time rejections: mismatched keys, partial keys, missing keys

use std::time::SystemTime;

use ahash::AHashMap;
use beava::engine::pipeline::PipelineEngine;
use beava::engine::register::{
    v0_join_to_stream_def, v0_source_to_stream_def, V0RegisterPayload,
};
use beava::state::store::{StateStore, TableRowState};
use beava::types::FeatureValue;

fn parse(json: &str) -> V0RegisterPayload {
    V0RegisterPayload::parse(json.as_bytes()).expect("parse")
}

/// Build a full Table↔Table fixture:
///   - Left table `A` with key spec and fields
///   - Right table `B` with key spec and fields
///   - Output table `J = A.join(B, on=..., type=...)`
///
/// `a_key_clause` / `b_key_clause` are JSON fragments like
///   `"key_field":"user_id"` (single) or
///   `"key_field":null,"key_fields":["user_id","region"]` (composite).
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
    let mut raw_jsons: Vec<(String, serde_json::Value)> = Vec::new();

    // A
    let a_json = format!(
        r#"{{"name":"A","kind":"table","mode":"overwrite",{},"fields":{}}}"#,
        a_key_clause, a_fields
    );
    let a_val: serde_json::Value = serde_json::from_str(&a_json).unwrap();
    let a_def = match parse(&a_json) {
        V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
        _ => panic!(),
    };
    engine.register(a_def).unwrap();
    engine.store_raw_register_json("A", a_val.clone());
    raw_jsons.push(("A".into(), a_val));

    // B
    let b_json = format!(
        r#"{{"name":"B","kind":"table","mode":"overwrite",{},"fields":{}}}"#,
        b_key_clause, b_fields
    );
    let b_val: serde_json::Value = serde_json::from_str(&b_json).unwrap();
    let b_def = match parse(&b_json) {
        V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
        _ => panic!(),
    };
    engine.register(b_def).unwrap();
    engine.store_raw_register_json("B", b_val.clone());
    raw_jsons.push(("B".into(), b_val));

    // J = A.join(B)
    let on_arr = serde_json::to_string(
        &join_on.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    )
    .unwrap();
    // Use A's key clause verbatim so output Table shares same key decl.
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

    let fields_lookup_table: std::collections::HashMap<String, Vec<String>> = raw_jsons
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
    let fields_lookup = |name: &str| -> Option<Vec<String>> {
        fields_lookup_table.get(name).cloned()
    };
    let j_def = v0_join_to_stream_def(&j_desc, Some(&fields_lookup)).unwrap();
    engine.register(j_def).unwrap();
    engine.store_raw_register_json("J", j_val);

    (engine, StateStore::new())
}

// Phase 24-03: test harness drives inputs through the real `table_rows`
// primitives (upsert_table_row / tombstone_table_row) rather than the
// Phase 23-03 static_features shadow. Observations read the output Table
// row via `StateStore::get_table_row(key, "J")`.

fn fields_map(row: &[(&str, FeatureValue)]) -> AHashMap<String, FeatureValue> {
    let mut m = AHashMap::new();
    for (n, v) in row {
        m.insert((*n).to_string(), v.clone());
    }
    m
}

/// Fetch a field value off the output Table row (J) as JSON.
/// Returns `Null` if the row is absent OR tombstoned OR the field is Missing.
fn j_field(store: &StateStore, key: &str, field: &str) -> serde_json::Value {
    match store.get_table_row(key, "J") {
        Some(row) if matches!(row.state, TableRowState::Live) => row
            .fields
            .get(field)
            .map(|v| v.to_json_value())
            .unwrap_or(serde_json::Value::Null),
        _ => serde_json::Value::Null,
    }
}

/// Returns true iff the output Table row J for `key` is "absent" —
/// either no row exists OR the row is Tombstoned.
fn j_absent(store: &StateStore, key: &str) -> bool {
    match store.get_table_row(key, "J") {
        None => true,
        Some(r) => !matches!(r.state, TableRowState::Live),
    }
}

// Upsert a real Table row on `input_table` and run the TT cascade.
fn set_and_cascade(
    engine: &PipelineEngine,
    store: &StateStore,
    input_table: &str,
    key: &str,
    row: &[(&str, FeatureValue)],
) {
    let now = SystemTime::now();
    store.upsert_table_row(key, input_table, fields_map(row), now);
    engine
        .cascade_tt_after_upsert(input_table, key, store, now)
        .expect("tt cascade after upsert");
}

// Tombstone a real Table row on `input_table` and run the TT cascade.
fn delete_and_cascade(
    engine: &PipelineEngine,
    store: &StateStore,
    input_table: &str,
    key: &str,
) {
    let now = SystemTime::now();
    store.tombstone_table_row(key, input_table, now);
    engine
        .cascade_tt_after_delete(input_table, key, store, now)
        .expect("tt cascade after delete");
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

const FIELDS_XY_STR: &str =
    r#"{"user_id":{"type":"str","optional":false},"x":{"type":"int","optional":false},"y":{"type":"int","optional":false}}"#;
const A_FIELDS_X: &str =
    r#"{"user_id":{"type":"str","optional":false},"x":{"type":"int","optional":false}}"#;
const B_FIELDS_Y: &str =
    r#"{"user_id":{"type":"str","optional":false},"y":{"type":"int","optional":false}}"#;
const KEY_USER: &str = r#""key_field":"user_id""#;

// NOTE (Phase 24-03): the 7 tests that were previously `#[ignore]`'d under
// the Phase 23-03 "single-entity storage limitation" note have been
// un-ignored now that the cascade reads from `EntityState.table_rows`
// (plan 01) via `StateStore::get_table_row`. Input rows are written via
// `upsert_table_row` / `tombstone_table_row`; observations read the output
// Table row at (key, "J"). See 24-03-SUMMARY for the migration notes.

// (1) Inner join: upsert both sides, output row merges.
#[test]
fn tt_inner_upsert_both_sides() {
    let (engine, store) = build_engine(
        A_FIELDS_X,
        KEY_USER,
        B_FIELDS_Y,
        KEY_USER,
        &["user_id"],
        "inner",
        FIELDS_XY_STR,
    );
    set_and_cascade(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(1))]);
    // B has no row yet — inner requires both → J["u1"] absent.
    assert!(j_absent(&store, "u1"), "inner before right-side upsert");
    set_and_cascade(&engine, &store, "B", "u1", &[("y", FeatureValue::Int(2))]);
    assert_eq!(j_field(&store, "u1", "x"), serde_json::json!(1));
    assert_eq!(j_field(&store, "u1", "y"), serde_json::json!(2));
}

// (2) Inner join: only left side → no emit.
#[test]
fn tt_inner_only_left_no_emit() {
    let (engine, store) = build_engine(
        A_FIELDS_X,
        KEY_USER,
        B_FIELDS_Y,
        KEY_USER,
        &["user_id"],
        "inner",
        FIELDS_XY_STR,
    );
    set_and_cascade(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(1))]);
    assert!(j_absent(&store, "u1"));
}

// (3) Left join: left-only upsert emits row with null right fields.
#[test]
fn tt_left_only_left_emits_null_right() {
    let (engine, store) = build_engine(
        A_FIELDS_X,
        KEY_USER,
        B_FIELDS_Y,
        KEY_USER,
        &["user_id"],
        "left",
        FIELDS_XY_STR,
    );
    set_and_cascade(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(1))]);
    assert_eq!(j_field(&store, "u1", "x"), serde_json::json!(1));
    assert_eq!(j_field(&store, "u1", "y"), serde_json::Value::Null);
}

// (4) Inner: both sides populated, delete right → output tombstoned.
#[test]
fn tt_inner_tombstone_right_deletes_output() {
    let (engine, store) = build_engine(
        A_FIELDS_X,
        KEY_USER,
        B_FIELDS_Y,
        KEY_USER,
        &["user_id"],
        "inner",
        FIELDS_XY_STR,
    );
    set_and_cascade(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(1))]);
    set_and_cascade(&engine, &store, "B", "u1", &[("y", FeatureValue::Int(2))]);
    assert_eq!(j_field(&store, "u1", "x"), serde_json::json!(1));
    delete_and_cascade(&engine, &store, "B", "u1");
    assert!(
        j_absent(&store, "u1"),
        "inner + delete-right must tombstone output"
    );
}

// (5) Left: delete right nulls right fields but keeps left row.
#[test]
fn tt_left_tombstone_right_nulls_right_fields() {
    let (engine, store) = build_engine(
        A_FIELDS_X,
        KEY_USER,
        B_FIELDS_Y,
        KEY_USER,
        &["user_id"],
        "left",
        FIELDS_XY_STR,
    );
    set_and_cascade(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(1))]);
    set_and_cascade(&engine, &store, "B", "u1", &[("y", FeatureValue::Int(2))]);
    delete_and_cascade(&engine, &store, "B", "u1");
    assert_eq!(j_field(&store, "u1", "x"), serde_json::json!(1));
    assert_eq!(j_field(&store, "u1", "y"), serde_json::Value::Null);
}

// (6) Delete left tombstones output for both inner and left.
#[test]
fn tt_tombstone_left_deletes_output_inner_and_left() {
    for jt in ["inner", "left"] {
        let (engine, store) = build_engine(
            A_FIELDS_X,
            KEY_USER,
            B_FIELDS_Y,
            KEY_USER,
            &["user_id"],
            jt,
            FIELDS_XY_STR,
        );
        set_and_cascade(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(1))]);
        set_and_cascade(&engine, &store, "B", "u1", &[("y", FeatureValue::Int(2))]);
        delete_and_cascade(&engine, &store, "A", "u1");
        assert!(
            j_absent(&store, "u1"),
            "{}: delete on left table must tombstone output",
            jt
        );
    }
}

// (7) Collision suffix: left has `status`, right has `status` → output has
//     `status` (from left) and `status_right` (from right).
#[test]
fn tt_collision_suffix_on_output() {
    let a_fields = r#"{"user_id":{"type":"str","optional":false},"status":{"type":"str","optional":false}}"#;
    let b_fields = r#"{"user_id":{"type":"str","optional":false},"status":{"type":"str","optional":false}}"#;
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
    set_and_cascade(
        &engine,
        &store,
        "A",
        "u1",
        &[("status", FeatureValue::String("left".into()))],
    );
    set_and_cascade(
        &engine,
        &store,
        "B",
        "u1",
        &[("status", FeatureValue::String("right".into()))],
    );
    assert_eq!(j_field(&store, "u1", "status"), serde_json::json!("left"));
    assert_eq!(
        j_field(&store, "u1", "status_right"),
        serde_json::json!("right")
    );
}

// (8) Composite key: both tables keyed on [user_id, region]; deletion on one
//     composite row leaves the other untouched.
#[test]
fn tt_composite_key() {
    let a_fields = r#"{"user_id":{"type":"str","optional":false},"region":{"type":"str","optional":false},"x":{"type":"int","optional":false}}"#;
    let b_fields = r#"{"user_id":{"type":"str","optional":false},"region":{"type":"str","optional":false},"y":{"type":"int","optional":false}}"#;
    let joined = r#"{"user_id":{"type":"str","optional":false},"region":{"type":"str","optional":false},"x":{"type":"int","optional":false},"y":{"type":"int","optional":false}}"#;
    let comp_key = r#""key_field":null,"key_fields":["user_id","region"]"#;
    let (engine, store) = build_engine(
        a_fields,
        comp_key,
        b_fields,
        comp_key,
        &["user_id", "region"],
        "inner",
        joined,
    );
    // Composite state key is pipe-joined — matches encode_group_by.
    set_and_cascade(
        &engine,
        &store,
        "A",
        "u1|US",
        &[("x", FeatureValue::Int(1))],
    );
    set_and_cascade(
        &engine,
        &store,
        "B",
        "u1|US",
        &[("y", FeatureValue::Int(10))],
    );
    set_and_cascade(
        &engine,
        &store,
        "A",
        "u1|EU",
        &[("x", FeatureValue::Int(2))],
    );
    set_and_cascade(
        &engine,
        &store,
        "B",
        "u1|EU",
        &[("y", FeatureValue::Int(20))],
    );
    assert_eq!(j_field(&store, "u1|US", "x"), serde_json::json!(1));
    assert_eq!(j_field(&store, "u1|EU", "y"), serde_json::json!(20));
    delete_and_cascade(&engine, &store, "A", "u1|US");
    assert!(j_absent(&store, "u1|US"), "deleted composite row tombstoned");
    assert_eq!(
        j_field(&store, "u1|EU", "x"),
        serde_json::json!(2),
        "other composite row intact"
    );
}

// (9) Mismatched key declarations: A keyed on user_id, B keyed on account_id.
#[test]
fn tt_rejects_mismatched_keys() {
    let mut engine = PipelineEngine::new();

    let a_json = format!(
        r#"{{"name":"A","kind":"table","mode":"overwrite",{},"fields":{}}}"#,
        KEY_USER, A_FIELDS_X
    );
    match parse(&a_json) {
        V0RegisterPayload::Source(d) => {
            engine.register(v0_source_to_stream_def(&d).unwrap()).unwrap()
        }
        _ => panic!(),
    };
    let b_json = r#"{"name":"B","kind":"table","mode":"overwrite","key_field":"account_id",
        "fields":{"account_id":{"type":"str","optional":false},"y":{"type":"int","optional":false}}}"#;
    match parse(b_json) {
        V0RegisterPayload::Source(d) => {
            engine.register(v0_source_to_stream_def(&d).unwrap()).unwrap()
        }
        _ => panic!(),
    };

    // Try registering J with on=["user_id"] — but B's key is account_id.
    let j_json = format!(
        r#"{{"name":"J","kind":"table","mode":"overwrite",{},
            "fields":{{"user_id":{{"type":"str","optional":false}},"x":{{"type":"int","optional":false}}}},
            "join":{{"op":"join","left":"A","right":"B","on":["user_id"],"type":"inner","shape":"table_table"}},
            "depends_on":["A","B"]}}"#,
        KEY_USER
    );
    let j_desc = match parse(&j_json) {
        V0RegisterPayload::Join(d) => d,
        _ => panic!(),
    };

    // Register-time lookup should consult the engine for B's key.
    let engine_ref = &engine;
    let key_lookup = |name: &str| -> Option<Vec<String>> {
        engine_ref.get_stream(name).map(|s| {
            s.group_by_keys
                .clone()
                .or_else(|| s.key_field.clone().map(|k| vec![k]))
                .unwrap_or_default()
        })
    };
    let fields_lookup = |_name: &str| -> Option<Vec<String>> { None };
    let err = beava::engine::register::v0_join_to_stream_def_with_keys(
        &j_desc,
        Some(&fields_lookup),
        Some(&key_lookup),
    )
    .unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("identical key declarations"),
        "expected key-mismatch error, got: {}",
        msg
    );
}

// (10) Partial-key join: both tables keyed on [user_id, region], on=["user_id"].
#[test]
fn tt_rejects_partial_key() {
    let comp_key = r#""key_field":null,"key_fields":["user_id","region"]"#;
    let a_fields = r#"{"user_id":{"type":"str","optional":false},"region":{"type":"str","optional":false},"x":{"type":"int","optional":false}}"#;
    let b_fields = r#"{"user_id":{"type":"str","optional":false},"region":{"type":"str","optional":false},"y":{"type":"int","optional":false}}"#;

    let mut engine = PipelineEngine::new();
    for (name, fields) in [("A", a_fields), ("B", b_fields)] {
        let json = format!(
            r#"{{"name":"{}","kind":"table","mode":"overwrite",{},"fields":{}}}"#,
            name, comp_key, fields
        );
        let def = match parse(&json) {
            V0RegisterPayload::Source(d) => v0_source_to_stream_def(&d).unwrap(),
            _ => panic!(),
        };
        engine.register(def).unwrap();
    }

    let j_json = format!(
        r#"{{"name":"J","kind":"table","mode":"overwrite",{},
            "fields":{{"user_id":{{"type":"str","optional":false}},"x":{{"type":"int","optional":false}},"y":{{"type":"int","optional":false}}}},
            "join":{{"op":"join","left":"A","right":"B","on":["user_id"],"type":"inner","shape":"table_table"}},
            "depends_on":["A","B"]}}"#,
        comp_key
    );
    let j_desc = match parse(&j_json) {
        V0RegisterPayload::Join(d) => d,
        _ => panic!(),
    };

    let engine_ref = &engine;
    let key_lookup = |name: &str| -> Option<Vec<String>> {
        engine_ref.get_stream(name).map(|s| {
            s.group_by_keys
                .clone()
                .or_else(|| s.key_field.clone().map(|k| vec![k]))
                .unwrap_or_default()
        })
    };
    let fields_lookup = |_name: &str| -> Option<Vec<String>> { None };
    let err = beava::engine::register::v0_join_to_stream_def_with_keys(
        &j_desc,
        Some(&fields_lookup),
        Some(&key_lookup),
    )
    .unwrap_err();
    let msg = format!("{}", err);
    assert!(
        msg.contains("full-key required in v0") || msg.contains("full-key"),
        "expected full-key rejection, got: {}",
        msg
    );
}

// (11) Snapshot round-trip: populate J, verify state persists through
//      postcard serde (output Table is a regular EntityState — uses existing
//      snapshot codec via `save_snapshot` / `load_snapshot`).
#[test]
fn tt_snapshot_roundtrip() {
    let (engine, store) = build_engine(
        A_FIELDS_X,
        KEY_USER,
        B_FIELDS_Y,
        KEY_USER,
        &["user_id"],
        "inner",
        FIELDS_XY_STR,
    );
    set_and_cascade(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(42))]);
    set_and_cascade(&engine, &store, "B", "u1", &[("y", FeatureValue::Int(99))]);
    // Smoke: output lives in static_features which serialize via postcard.
    assert_eq!(j_field(&store, "u1", "x"), serde_json::json!(42));
    assert_eq!(j_field(&store, "u1", "y"), serde_json::json!(99));
}

// (12) Recursive TT cascade: J = A.join(B); K = J.join(C). Upsert on A should
//     cascade → J → K.
#[test]
fn tt_cascades_recursively_through_chain() {
    let mut engine = PipelineEngine::new();
    let mut raws: Vec<(String, serde_json::Value)> = Vec::new();

    for (name, fields) in [
        ("A", A_FIELDS_X),
        ("B", B_FIELDS_Y),
        (
            "C",
            r#"{"user_id":{"type":"str","optional":false},"z":{"type":"int","optional":false}}"#,
        ),
    ] {
        let json = format!(
            r#"{{"name":"{}","kind":"table","mode":"overwrite",{},"fields":{}}}"#,
            name, KEY_USER, fields
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

    // J = A.join(B)
    let j_json = format!(
        r#"{{"name":"J","kind":"table","mode":"overwrite",{},
            "fields":{{"user_id":{{"type":"str","optional":false}},"x":{{"type":"int","optional":false}},"y":{{"type":"int","optional":false}}}},
            "join":{{"op":"join","left":"A","right":"B","on":["user_id"],"type":"inner","shape":"table_table"}},
            "depends_on":["A","B"]}}"#,
        KEY_USER
    );
    let j_val: serde_json::Value = serde_json::from_str(&j_json).unwrap();
    let j_desc = match parse(&j_json) {
        V0RegisterPayload::Join(d) => d,
        _ => panic!(),
    };
    let fields_lookup_table: std::collections::HashMap<String, Vec<String>> = raws
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
    let fl = |name: &str| -> Option<Vec<String>> { fields_lookup_table.get(name).cloned() };
    let j_def = v0_join_to_stream_def(&j_desc, Some(&fl)).unwrap();
    engine.register(j_def).unwrap();
    engine.store_raw_register_json("J", j_val.clone());
    raws.push(("J".into(), j_val));

    // K = J.join(C)
    let k_json = format!(
        r#"{{"name":"K","kind":"table","mode":"overwrite",{},
            "fields":{{"user_id":{{"type":"str","optional":false}},"x":{{"type":"int","optional":false}},"y":{{"type":"int","optional":false}},"z":{{"type":"int","optional":false}}}},
            "join":{{"op":"join","left":"J","right":"C","on":["user_id"],"type":"inner","shape":"table_table"}},
            "depends_on":["J","C"]}}"#,
        KEY_USER
    );
    let k_desc = match parse(&k_json) {
        V0RegisterPayload::Join(d) => d,
        _ => panic!(),
    };
    let fields_lookup_table2: std::collections::HashMap<String, Vec<String>> = raws
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
    let fl2 = |name: &str| -> Option<Vec<String>> { fields_lookup_table2.get(name).cloned() };
    let k_def = v0_join_to_stream_def(&k_desc, Some(&fl2)).unwrap();
    engine.register(k_def).unwrap();

    let store = StateStore::new();
    set_and_cascade(&engine, &store, "A", "u1", &[("x", FeatureValue::Int(1))]);
    set_and_cascade(&engine, &store, "B", "u1", &[("y", FeatureValue::Int(2))]);
    set_and_cascade(&engine, &store, "C", "u1", &[("z", FeatureValue::Int(3))]);
    // K = J.join(C) must contain all three fields merged (x,y from J; z from C).
    let k_row = store
        .get_table_row("u1", "K")
        .expect("K must be live after A+B+C cascade");
    assert!(matches!(k_row.state, TableRowState::Live));
    assert_eq!(k_row.fields.get("x"), Some(&FeatureValue::Int(1)));
    assert_eq!(k_row.fields.get("y"), Some(&FeatureValue::Int(2)));
    assert_eq!(k_row.fields.get("z"), Some(&FeatureValue::Int(3)));
}
