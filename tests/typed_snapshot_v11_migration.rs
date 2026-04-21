//! Phase 59.6 Wave 5 (SC-10) — V10 snapshots load through the V11 reader;
//! V11 snapshots round-trip typed rows byte-identically.
//!
//! Wave 5 flips these two tests RED → GREEN.
//!
//! Coverage:
//! - Test 1 (`v10_snapshot_loads_into_v11_writer`): seed a V10 (actually V9
//!   outer byte — Phase 57 retained V9_FORMAT for the V10 schema bump, per
//!   `src/state/snapshot.rs` V10_SCHEMA_VERSION docs) snapshot body, feed
//!   it to the V11 reader (`load_typed_state_v11`), verify the Value-path
//!   entities are preserved and `typed_entities` is empty (transparent
//!   upgrade). Then write a V11 snapshot from the same data and verify the
//!   outer byte is `V11_FORMAT`.
//! - Test 2 (`v11_snapshot_round_trip_preserves_typed_rows`): construct a
//!   typed Row, encode as V11, decode, verify byte-identical payload +
//!   arena + schema_id.

#![allow(unused_imports)]

use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use beava::state::snapshot::{
    load_typed_state_v11, save_base_snapshot, save_typed_state_v11,
    BaseSnapshotState, SerializableEntityState, SnapshotHeader, SnapshotState, SnapshotType,
    TypedAggState, TypedStateSnapshotV11, V10_SCHEMA_VERSION, V11_FORMAT, V9_FORMAT,
};
use std::sync::Arc;

fn typed_schema() -> Arc<RegisteredSchema> {
    let s = RegisteredSchema {
        schema_id: 7,
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
        ],
        inline_str_cap: 15,
        row_size: 24,
    };
    s.validate_layout().expect("valid");
    Arc::new(s)
}

fn typed_row_with(user: &str, count: i64) -> Row {
    let schema = typed_schema();
    let mut r = Row::zeroed(&schema);
    r.schema_id = schema.schema_id;
    r.write_inline_str(0, schema.inline_str_cap, user);
    r.write_i64(16, count);
    r
}

#[test]
fn v10_snapshot_loads_into_v11_writer() {
    // 1. Seed a Value-path snapshot written by the pre-Wave-5 writer
    //    (V9 outer byte, schema_version = V10_SCHEMA_VERSION = 10).
    let entities: Vec<(String, SerializableEntityState)> = vec![(
        "alice".to_string(),
        SerializableEntityState {
            streams: vec![],
            static_features: vec![],
            table_rows: vec![],
        },
    )];
    let base_state = SnapshotState {
        entities: entities.clone(),
        pipelines: vec![],
        backfill_complete: vec![],
    };
    let v9_bytes =
        save_base_snapshot(&BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 0,
                schema_version: V10_SCHEMA_VERSION,
            },
            entities: base_state.entities.clone(),
            pipelines: base_state.pipelines.clone(),
            backfill_complete: base_state.backfill_complete.clone(),
        })
        .expect("save_base_snapshot");

    // Verify the seeded bytes' outer byte is V9_FORMAT (legacy V10 schema).
    assert_eq!(
        v9_bytes[0], V9_FORMAT,
        "legacy V10 snapshot carries V9_FORMAT outer byte"
    );

    // 2. Load via V11 reader — transparent fallback.
    let decoded = load_typed_state_v11(&v9_bytes)
        .expect("V11 reader accepts V9/V10 bytes via transparent fallback");
    assert!(
        decoded.typed_entities.is_empty(),
        "V9/V10 snapshot carries no typed rows"
    );
    assert_eq!(
        decoded.value_entities.len(),
        1,
        "V9/V10 value entities preserved through fallback"
    );
    assert_eq!(decoded.value_entities[0].0, "alice");

    // 3. Write the same state as V11 — outer byte MUST be V11_FORMAT.
    let v11_bytes =
        save_typed_state_v11(&decoded).expect("save_typed_state_v11");
    assert_eq!(
        v11_bytes[0], V11_FORMAT,
        "V11 writer emits V11_FORMAT outer byte"
    );

    // 4. Round-trip the V11 bytes through the V11 reader — lossless.
    let re = load_typed_state_v11(&v11_bytes).expect("V11 reader accepts its own output");
    assert_eq!(re.value_entities.len(), 1);
    assert_eq!(re.value_entities[0].0, "alice");
    assert!(re.typed_entities.is_empty());
}

#[test]
fn v11_snapshot_round_trip_preserves_typed_rows() {
    // 1. Build a V11 snapshot with typed rows.
    let schema = typed_schema();
    let row_alice = typed_row_with("alice", 11);
    let row_bob = typed_row_with("bob", 22);

    let snap = TypedStateSnapshotV11 {
        typed_entities: vec![
            (
                ("Txns".to_string(), "alice".to_string()),
                TypedAggState::from_row(&row_alice, &schema.name),
            ),
            (
                ("Txns".to_string(), "bob".to_string()),
                TypedAggState::from_row(&row_bob, &schema.name),
            ),
        ],
        value_entities: vec![],
    };

    // 2. Encode.
    let bytes = save_typed_state_v11(&snap).expect("save_typed_state_v11");
    assert_eq!(bytes[0], V11_FORMAT);

    // 3. Decode.
    let decoded = load_typed_state_v11(&bytes).expect("load_typed_state_v11");
    assert_eq!(decoded.typed_entities.len(), 2);

    // 4. Byte-identical payload + arena + schema_id + schema_name for
    //    every typed entity.
    let mut by_key: std::collections::HashMap<(String, String), TypedAggState> =
        std::collections::HashMap::new();
    for ((stream, key), st) in decoded.typed_entities {
        by_key.insert((stream, key), st);
    }
    let alice = by_key
        .get(&("Txns".to_string(), "alice".to_string()))
        .expect("alice present");
    assert_eq!(alice.schema_id, row_alice.schema_id);
    assert_eq!(alice.schema_name, schema.name);
    assert_eq!(alice.payload, row_alice.payload);
    assert_eq!(alice.arena, row_alice.arena);

    let bob = by_key
        .get(&("Txns".to_string(), "bob".to_string()))
        .expect("bob present");
    assert_eq!(bob.schema_id, row_bob.schema_id);
    assert_eq!(bob.payload, row_bob.payload);
    assert_eq!(bob.arena, row_bob.arena);

    // 5. Reconstruct Rows and verify typed-field reads match.
    let alice_row = alice.to_row();
    assert_eq!(
        alice_row.read_inline_str(0, schema.inline_str_cap),
        "alice"
    );
    assert_eq!(alice_row.read_i64(16), 11);
    let bob_row = bob.to_row();
    assert_eq!(bob_row.read_inline_str(0, schema.inline_str_cap), "bob");
    assert_eq!(bob_row.read_i64(16), 22);
}
