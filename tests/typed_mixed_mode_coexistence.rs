//! Phase 59.6 SC-7 — two streams registered, one typed, one untyped;
//! interoperate without interfering.
//!
//! Wave 5 flips this test RED → GREEN.
//!
//! Coverage: verify that the V11 snapshot format (`TypedStateSnapshotV11`)
//! can carry BOTH typed-row entities (for schema-registered streams) AND
//! Value-path entities (for unschema'd streams) in the same envelope —
//! the foundational mixed-mode invariant. Also verifies the fjall
//! typed-row keyspace (`TYPED_ROW_KEY_PREFIX=0xFF`) doesn't collide with
//! the Value-path entity-key keyspace on the same partition.

#![allow(unused_imports)]

use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use beava::shard::store_fjall::{get_entity_typed, put_entity_typed};
use beava::state::snapshot::{
    load_typed_state_v11, save_typed_state_v11, SerializableEntityState,
    TypedAggState, TypedStateSnapshotV11, V11_FORMAT,
};
use std::sync::Arc;

fn typed_schema_txns() -> Arc<RegisteredSchema> {
    let s = RegisteredSchema {
        schema_id: 10,
        name: "Txns".into(),
        fields: vec![
            FieldSpec {
                name: "user_id".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            },
            FieldSpec {
                name: "amount".into(),
                ty: FieldTy::F64,
                offset: 16,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 24,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

#[test]
fn mixed_typed_untyped_streams_coexist_via_enrich() {
    // 1. Snapshot-level: V11 carries typed + value entities side-by-side.
    let schema = typed_schema_txns();
    let mut row_typed = Row::zeroed(&schema);
    row_typed.schema_id = schema.schema_id;
    row_typed.write_inline_str(0, schema.inline_str_cap, "alice");
    row_typed.write_f64(16, 99.5);

    // Typed-path entity for the Txns stream.
    let typed_entity = (
        ("Txns".to_string(), "alice".to_string()),
        TypedAggState::from_row(&row_typed, &schema.name),
    );

    // Value-path entity for the Events stream (no typed schema).
    let value_entity = (
        "bob_events".to_string(),
        SerializableEntityState {
            streams: vec![],
            static_features: vec![],
            table_rows: vec![],
        },
    );

    let snap = TypedStateSnapshotV11 {
        typed_entities: vec![typed_entity.clone()],
        value_entities: vec![value_entity],
    };

    let bytes = save_typed_state_v11(&snap).expect("save");
    assert_eq!(bytes[0], V11_FORMAT);

    let decoded = load_typed_state_v11(&bytes).expect("load");
    assert_eq!(
        decoded.typed_entities.len(),
        1,
        "typed entity preserved"
    );
    assert_eq!(
        decoded.value_entities.len(),
        1,
        "value entity preserved"
    );
    assert_eq!(decoded.typed_entities[0].0 .1, "alice");
    assert_eq!(decoded.value_entities[0].0, "bob_events");
    // Typed body byte-identical.
    assert_eq!(decoded.typed_entities[0].1.payload, row_typed.payload);
    assert_eq!(decoded.typed_entities[0].1.arena, row_typed.arena);

    // 2. Fjall-level: typed keyspace (0xFF prefix) coexists with Value
    //    keyspace (UTF-8 prefix) on the same partition — no collision.
    use beava::shard::fjall_backend::{
        fjall_config_from_env, open_keyspace_from_env, open_shard_partition,
    };

    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
    let cfg = fjall_config_from_env(1);
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open ks");
    let partition = open_shard_partition(&ks, 0, &cfg).expect("open partition");

    // Write a typed row.
    put_entity_typed(&partition, "Txns", "alice", &row_typed).expect("put typed");

    // Write a Value-path entity at the same entity_key — stored under
    // the raw UTF-8 `entity_key.as_bytes()` key (no 0xFF prefix).
    // Collision-free because 0xFF is not a valid UTF-8 leading byte.
    partition
        .insert(b"alice" as &[u8], b"value_path_body" as &[u8])
        .expect("value-path insert");

    // Typed read still returns the typed row.
    let got_typed = get_entity_typed(&partition, "Txns", "alice")
        .expect("get ok")
        .expect("typed row present");
    assert_eq!(got_typed.payload, row_typed.payload);

    // Value-path read still returns the Value bytes (no collision).
    let got_value = partition
        .get(b"alice" as &[u8])
        .expect("get ok")
        .expect("value row present");
    assert_eq!(got_value.as_ref(), b"value_path_body" as &[u8]);
}
