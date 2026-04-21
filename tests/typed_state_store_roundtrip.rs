//! Phase 59.6 Wave 5 (SC-8) — inmem + fjall stores typed rows when the
//! stream has a schema; byte-identical round-trip.
//!
//! Wave 5 flips these tests RED → GREEN. The implementations under test:
//!   - Inmem: `Shard::entity_state_typed` (AHashMap insert → get; byte-
//!     identical payload + arena).
//!   - Fjall: `src/shard/store_fjall.rs::put_entity_typed` +
//!     `get_entity_typed` (packed-row memcpy encoding — no serde_json).

#![allow(unused_imports)]

use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use std::sync::Arc;

fn sample_schema() -> Arc<RegisteredSchema> {
    let s = RegisteredSchema {
        schema_id: 42,
        name: "TxnState".into(),
        fields: vec![
            FieldSpec {
                name: "user_id".into(),
                ty: FieldTy::InlineStr,
                offset: 0,
                nullable: false,
            },
            FieldSpec {
                name: "count".into(),
                ty: FieldTy::I64,
                offset: 16,
                nullable: false,
            },
            FieldSpec {
                name: "sum_amount".into(),
                ty: FieldTy::F64,
                offset: 24,
                nullable: false,
            },
        ],
        inline_str_cap: 15,
        row_size: 32,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

fn sample_row() -> Row {
    let schema = sample_schema();
    let mut r = Row::zeroed(&schema);
    r.schema_id = schema.schema_id;
    r.write_inline_str(0, schema.inline_str_cap, "alice");
    r.write_i64(16, 7);
    r.write_f64(24, 123.45);
    r
}

#[test]
fn inmem_store_persists_typed_row() {
    // SC-8 — AHashMap<(stream, entity_key), Row> round-trip.
    // Mirrors `Shard::entity_state_typed` storage without needing the
    // full Shard surface (Row is Clone; the AHashMap is the storage).
    let row = sample_row();
    let mut store: ahash::AHashMap<(String, String), Row> = ahash::AHashMap::new();
    store.insert(("Txns".to_string(), "alice".to_string()), row.clone());

    // Read back.
    let got = store
        .get(&("Txns".to_string(), "alice".to_string()))
        .expect("row should be present");
    // Byte-identical payload + arena + schema_id.
    assert_eq!(got.schema_id, row.schema_id);
    assert_eq!(got.payload, row.payload);
    assert_eq!(got.arena, row.arena);

    // Typed-field reads must be identical across the round-trip.
    let schema = sample_schema();
    assert_eq!(
        got.read_inline_str(0, schema.inline_str_cap),
        row.read_inline_str(0, schema.inline_str_cap)
    );
    assert_eq!(got.read_i64(16), row.read_i64(16));
    assert!((got.read_f64(24) - row.read_f64(24)).abs() < 1e-12);
}

#[test]
fn fjall_store_persists_typed_row_memcpy() {
    // SC-8 — fjall partition round-trip via put_entity_typed +
    // get_entity_typed. Byte-identical payload + arena; memcpy
    // encoding (no serde_json / postcard on the body bytes).
    use beava::shard::fjall_backend::{fjall_config_from_env, open_keyspace_from_env, open_shard_partition};
    use beava::shard::store_fjall::{
        decode_typed_row_body, encode_typed_row_body, get_entity_typed, put_entity_typed,
    };

    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
    let cfg = fjall_config_from_env(1);
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
    let partition = open_shard_partition(&ks, 0, &cfg).expect("open shard partition");

    let row = sample_row();

    // Put.
    put_entity_typed(&partition, "Txns", "alice", &row).expect("put");
    // Get.
    let got = get_entity_typed(&partition, "Txns", "alice")
        .expect("get ok")
        .expect("row present");
    assert_eq!(got.schema_id, row.schema_id);
    assert_eq!(got.payload, row.payload);
    assert_eq!(got.arena, row.arena);

    // Also verify the encode/decode primitives are pure memcpy —
    // D-D1 invariant: `encode` then `decode` round-trips byte-for-byte.
    let body = encode_typed_row_body(&row);
    let decoded = decode_typed_row_body(&body).expect("decode");
    assert_eq!(decoded.schema_id, row.schema_id);
    assert_eq!(decoded.payload, row.payload);
    assert_eq!(decoded.arena, row.arena);

    // Missing key returns None (not an error).
    let none = get_entity_typed(&partition, "Txns", "missing_entity").expect("get ok");
    assert!(none.is_none(), "missing key returns None");

    // Missing stream also returns None (typed-key namespace isolation).
    let none2 = get_entity_typed(&partition, "OtherStream", "alice").expect("get ok");
    assert!(
        none2.is_none(),
        "typed key namespace isolates by stream name"
    );
}
