// 49-04: shard_key serde round-trip + backward-compat tests.
// TDD RED phase: these tests define the contract for ShardKeySpec serialization.

use beava::engine::join_validator::ShardKeySpec;

#[test]
fn shard_key_single_round_trip() {
    let spec = ShardKeySpec::Single("user_id".to_string());
    let json = serde_json::to_string(&spec).unwrap();
    let back: ShardKeySpec = serde_json::from_str(&json).unwrap();
    assert_eq!(spec, back);
    // Single → JSON string (untagged)
    assert_eq!(json, r#""user_id""#);
}

#[test]
fn shard_key_tuple_round_trip() {
    let spec = ShardKeySpec::Tuple(vec!["region".into(), "user_id".into()]);
    let json = serde_json::to_string(&spec).unwrap();
    let back: ShardKeySpec = serde_json::from_str(&json).unwrap();
    assert_eq!(spec, back);
    // Tuple → JSON array (untagged)
    assert_eq!(json, r#"["region","user_id"]"#);
}

#[test]
fn shard_key_missing_field_deserializes_as_none() {
    use serde::Deserialize;
    // Wrapper verifies #[serde(default)] behavior for Option<ShardKeySpec>
    #[derive(Deserialize)]
    struct Wrapper {
        #[serde(default)]
        shard_key: Option<ShardKeySpec>,
    }
    let json = r#"{}"#;
    let w: Wrapper = serde_json::from_str(json).unwrap();
    assert!(w.shard_key.is_none(), "#[serde(default)] missing field → None");
}

#[test]
fn shard_key_null_deserializes_as_none() {
    use serde::Deserialize;
    #[derive(Deserialize)]
    struct Wrapper {
        #[serde(default)]
        shard_key: Option<ShardKeySpec>,
    }
    let json = r#"{"shard_key": null}"#;
    let w: Wrapper = serde_json::from_str(json).unwrap();
    assert!(w.shard_key.is_none());
}

#[test]
fn shard_key_single_deserializes_from_string() {
    use serde::Deserialize;
    #[derive(Deserialize)]
    struct Wrapper {
        #[serde(default)]
        shard_key: Option<ShardKeySpec>,
    }
    let json = r#"{"shard_key": "user_id"}"#;
    let w: Wrapper = serde_json::from_str(json).unwrap();
    assert_eq!(w.shard_key, Some(ShardKeySpec::Single("user_id".to_string())));
}

#[test]
fn shard_key_tuple_deserializes_from_array() {
    use serde::Deserialize;
    #[derive(Deserialize)]
    struct Wrapper {
        #[serde(default)]
        shard_key: Option<ShardKeySpec>,
    }
    let json = r#"{"shard_key": ["region", "user_id"]}"#;
    let w: Wrapper = serde_json::from_str(json).unwrap();
    assert_eq!(
        w.shard_key,
        Some(ShardKeySpec::Tuple(vec!["region".into(), "user_id".into()]))
    );
}

// SourceDescriptor backward-compat test: old JSON without shard_key → None
#[test]
fn source_descriptor_missing_shard_key_is_backward_compat() {
    use beava::engine::register::SourceDescriptor;
    let legacy_json = r#"{
        "name": "Txns",
        "kind": "stream",
        "key_field": "user_id",
        "fields": {}
    }"#;
    let desc: SourceDescriptor = serde_json::from_str(legacy_json).unwrap();
    assert!(
        desc.shard_key.is_none(),
        "SourceDescriptor.shard_key must default to None on absent field"
    );
}

// SourceDescriptor with shard_key → parses correctly
#[test]
fn source_descriptor_with_shard_key_parses_correctly() {
    use beava::engine::register::SourceDescriptor;
    let json = r#"{
        "name": "Txns",
        "kind": "stream",
        "key_field": "user_id",
        "fields": {},
        "shard_key": "user_id"
    }"#;
    let desc: SourceDescriptor = serde_json::from_str(json).unwrap();
    assert_eq!(
        desc.shard_key,
        Some(ShardKeySpec::Single("user_id".to_string()))
    );
}
