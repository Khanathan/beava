//! Phase 18 Plan 09 — HTTP direct Row deserialize tests.
//!
//! Task 9.3: Row implements serde::Deserialize so that both serde_json and
//! rmp_serde can deserialize directly into Row without going through JsonValue.

use beava_core::row::{Row, Value};

/// Task 9.3 RED: assert serde_json::from_slice::<Row>(...) produces the expected
/// Row without going through JsonValue.
///
/// RED: Row does not implement Deserialize yet — this fails at compile time
/// (or at runtime if derive is present but incorrect).
#[test]
fn test_parse_json_directly_into_row() {
    let json_bytes = br#"{"user_id":"u1","amount":42.5,"count":7,"active":true}"#;

    let row: Row = serde_json::from_slice(json_bytes)
        .expect("serde_json::from_slice::<Row> should succeed");

    assert_eq!(row.get("user_id"), Some(&Value::Str("u1".to_string())));
    assert_eq!(row.get("count"), Some(&Value::I64(7)));
    // amount 42.5 → F64
    match row.get("amount") {
        Some(Value::F64(f)) => {
            assert!((f - 42.5).abs() < 1e-9, "amount should be ~42.5, got {f}");
        }
        other => panic!("expected F64 for amount, got {other:?}"),
    }
    assert_eq!(row.get("active"), Some(&Value::Bool(true)));
}

/// Task 9.3: msgpack deserialization into Row also works (rmp_serde uses serde traits).
#[test]
fn test_parse_msgpack_directly_into_row() {
    // Build a msgpack map: {user_id: "u1", amount: 42.5, count: 7, active: true}
    use serde::Serialize;
    #[derive(Serialize)]
    struct Body<'a> {
        user_id: &'a str,
        amount: f64,
        count: i64,
        active: bool,
    }
    let body = Body { user_id: "u1", amount: 42.5, count: 7, active: true };
    let msgpack_bytes = rmp_serde::to_vec_named(&body).expect("msgpack serialize");

    let row: Row = rmp_serde::from_slice(&msgpack_bytes)
        .expect("rmp_serde::from_slice::<Row> should succeed");

    assert_eq!(row.get("user_id"), Some(&Value::Str("u1".to_string())));
    assert_eq!(row.get("count"), Some(&Value::I64(7)));
    assert_eq!(row.get("active"), Some(&Value::Bool(true)));
    match row.get("amount") {
        Some(Value::F64(f)) => {
            assert!((f - 42.5).abs() < 1e-9, "amount should be ~42.5, got {f}");
        }
        other => panic!("expected F64 for amount, got {other:?}"),
    }
}

/// Task 9.3: integer values in JSON are parsed to I64 where possible.
#[test]
fn test_row_deserialize_integer_prefers_i64() {
    let json_bytes = br#"{"x": 100, "y": -5}"#;
    let row: Row = serde_json::from_slice(json_bytes).expect("deserialize");
    assert_eq!(row.get("x"), Some(&Value::I64(100)));
    assert_eq!(row.get("y"), Some(&Value::I64(-5)));
}

/// Task 9.3: null values deserialize as Value::Null.
#[test]
fn test_row_deserialize_null_value() {
    let json_bytes = br#"{"a": null, "b": "hello"}"#;
    let row: Row = serde_json::from_slice(json_bytes).expect("deserialize");
    assert_eq!(row.get("a"), Some(&Value::Null));
    assert_eq!(row.get("b"), Some(&Value::Str("hello".to_string())));
}

/// Task 9.3: empty object deserializes to empty Row.
#[test]
fn test_row_deserialize_empty_object() {
    let json_bytes = br#"{}"#;
    let row: Row = serde_json::from_slice(json_bytes).expect("deserialize");
    assert!(row.is_empty(), "empty JSON object should produce empty Row");
}
