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

use beava::engine::operators_typed_aggs_windowed::{
    TypedRingBufferAvg, TypedRingBufferEnum, TypedRingBufferF64, TypedRingBufferI64,
    TypedRingBufferInlineStr, TypedRingBufferMaxF64, TypedRingBufferMaxI64,
    TypedRingBufferMinF64, TypedRingBufferMinI64, TypedRingBufferVariantHint,
};
use beava::engine::schema::{FieldSpec, FieldTy, RegisteredSchema, Row};
use beava::state::snapshot::{
    load_typed_state_v11, save_base_snapshot, save_typed_state_v11,
    BaseSnapshotState, SerializableEntityState, SnapshotHeader, SnapshotState, SnapshotType,
    TypedAggState, TypedStateSnapshotV11, V10_SCHEMA_VERSION, V11_FORMAT, V9_FORMAT,
};
use proptest::prelude::*;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
        typed_ringbuffers: vec![],
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

// ---------------------------------------------------------------------------
// Phase 59.7 Wave 2 — V11 snapshot extension: typed_ringbuffers round-trip.
// ---------------------------------------------------------------------------

fn ts(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

/// V10→V11 transparent upgrade gate: a snapshot with `typed_ringbuffers: vec![]`
/// must round-trip byte-identically. `#[serde(default)]` on the new field means
/// a pre-59.7 V11 snapshot (no ringbuffer bytes on the wire) still decodes
/// with `typed_ringbuffers = vec![]`.
#[test]
fn test_v11_roundtrip_empty_ringbuffers_preserves_v10_compat() {
    let snap = TypedStateSnapshotV11 {
        typed_entities: vec![],
        value_entities: vec![],
        typed_ringbuffers: vec![],
    };
    let bytes = save_typed_state_v11(&snap).expect("save");
    assert_eq!(bytes[0], V11_FORMAT);
    let decoded = load_typed_state_v11(&bytes).expect("load");
    assert!(decoded.typed_entities.is_empty());
    assert!(decoded.value_entities.is_empty());
    assert!(decoded.typed_ringbuffers.is_empty());
    // Idempotent re-save → byte-identical.
    let bytes2 = save_typed_state_v11(&decoded).expect("re-save");
    assert_eq!(bytes, bytes2, "empty V11 snapshot must re-serialize identically");
}

/// Byte-format regression: with `typed_ringbuffers = vec![]` the new field
/// adds one postcard length prefix (0-byte Vec) to the wire. Assert the
/// outer byte stays V11_FORMAT and the full bytes match a golden constant
/// so future `#[serde(default)]` additions can be caught.
#[test]
fn test_v11_byte_format_unchanged_with_default_typed_ringbuffers() {
    let snap = TypedStateSnapshotV11 {
        typed_entities: vec![],
        value_entities: vec![],
        typed_ringbuffers: vec![],
    };
    let bytes = save_typed_state_v11(&snap).expect("save");
    assert_eq!(bytes[0], V11_FORMAT);
    assert_eq!(bytes[1], 0x00, "TYPE_TAG_BASE");
    // Empty typed_entities + empty value_entities + empty typed_ringbuffers
    // → three 0-byte Vec length prefixes in postcard (varint 0 each = 1 byte).
    // Total body: 3 bytes. Total wire: [V11, TAG, 0x00, 0x00, 0x00].
    assert_eq!(bytes.len(), 5, "empty V11 snapshot should be 5 bytes");
    assert_eq!(&bytes[..], &[V11_FORMAT, 0x00, 0x00, 0x00, 0x00]);
}

#[test]
fn test_v11_roundtrip_with_populated_typed_ringbuffers() {
    // Mix one of each ring-buffer variant so the enum dispatch is exercised.
    let mut rb_i64 = TypedRingBufferI64::new(Duration::from_secs(5), Duration::from_secs(1));
    rb_i64.update_at_event_time(|b| *b += 3, ts(1000));
    rb_i64.update_at_event_time(|b| *b += 5, ts(1002));
    let mut rb_f64 = TypedRingBufferF64::new(Duration::from_secs(5), Duration::from_secs(1));
    rb_f64.update_at_event_time(|b| *b += 1.5, ts(1001));
    let mut rb_avg = TypedRingBufferAvg::new(Duration::from_secs(5), Duration::from_secs(1));
    rb_avg.update_at_event_time(
        |b| {
            b.0 += 10.0;
            b.1 += 1;
        },
        ts(1000),
    );
    let mut rb_min = TypedRingBufferMinI64::new(Duration::from_secs(5), Duration::from_secs(1));
    rb_min.update_at_event_time(7, ts(1000));
    rb_min.update_at_event_time(3, ts(1001));
    let mut rb_max = TypedRingBufferMaxF64::new(Duration::from_secs(5), Duration::from_secs(1));
    rb_max.update_at_event_time(1.25, ts(1000));
    rb_max.update_at_event_time(9.75, ts(1002));
    let mut rb_str = TypedRingBufferInlineStr::new(Duration::from_secs(5), Duration::from_secs(1));
    rb_str.update_at_event_time("alpha", ts(1000));
    rb_str.update_at_event_time("bravo", ts(1002));

    let rings = vec![
        (("Txns".to_string(), "u1".to_string(), 0u16), TypedRingBufferEnum::I64(rb_i64.clone())),
        (("Txns".to_string(), "u1".to_string(), 1u16), TypedRingBufferEnum::F64(rb_f64.clone())),
        (("Txns".to_string(), "u1".to_string(), 2u16), TypedRingBufferEnum::Avg(rb_avg.clone())),
        (("Txns".to_string(), "u2".to_string(), 3u16), TypedRingBufferEnum::MinI64(rb_min.clone())),
        (("Txns".to_string(), "u2".to_string(), 4u16), TypedRingBufferEnum::MaxF64(rb_max.clone())),
        (("Txns".to_string(), "u2".to_string(), 5u16), TypedRingBufferEnum::InlineStr(rb_str.clone())),
    ];

    let snap = TypedStateSnapshotV11 {
        typed_entities: vec![],
        value_entities: vec![],
        typed_ringbuffers: rings.clone(),
    };
    let bytes = save_typed_state_v11(&snap).expect("save");
    let decoded = load_typed_state_v11(&bytes).expect("load");

    assert_eq!(decoded.typed_ringbuffers.len(), 6);
    // Byte-identical structural equality via PartialEq derive.
    for (orig, got) in rings.iter().zip(decoded.typed_ringbuffers.iter()) {
        assert_eq!(orig.0, got.0, "key mismatch");
        assert_eq!(orig.1, got.1, "ring value mismatch");
    }
    // Idempotent re-save.
    let bytes2 = save_typed_state_v11(&decoded).expect("re-save");
    assert_eq!(bytes, bytes2, "V11 with ringbuffers must re-serialize identically");
}

/// Proptest: generate random (window, bucket, events) configurations +
/// random variant per ring, snapshot-save-load, assert equal.
proptest! {
    #![proptest_config(ProptestConfig { cases: 50, .. ProptestConfig::default() })]

    #[test]
    fn roundtrip_typed_ringbuffers(
        seed in 0u64..10_000u64,
        entity_count in 1usize..20,
        events_per_ring in 1usize..50,
        window_secs in 1u64..3600u64,
        bucket_secs in 1u64..60u64,
    ) {
        // Ensure bucket divides window at least once.
        let bucket_secs = bucket_secs.min(window_secs);
        let window = Duration::from_secs(window_secs);
        let bucket = Duration::from_secs(bucket_secs);

        // Deterministic pseudo-random generator.
        let mut state = seed;
        let mut next = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            state
        };

        let variants = [
            TypedRingBufferVariantHint::I64,
            TypedRingBufferVariantHint::F64,
            TypedRingBufferVariantHint::Avg,
            TypedRingBufferVariantHint::MinI64,
            TypedRingBufferVariantHint::MinF64,
            TypedRingBufferVariantHint::MaxI64,
            TypedRingBufferVariantHint::MaxF64,
            TypedRingBufferVariantHint::InlineStr,
        ];

        let mut rings = Vec::with_capacity(entity_count);
        for i in 0..entity_count {
            let variant = variants[(next() as usize) % variants.len()];
            let mut enm = variant.construct(window, bucket);
            let base_t = 1_000_000_000u64 + (next() % 1000);
            for j in 0..events_per_ring {
                let et = ts(base_t + (j as u64) % window_secs);
                match &mut enm {
                    TypedRingBufferEnum::I64(r) => r.update_at_event_time(|b| *b += (next() % 100) as i64, et),
                    TypedRingBufferEnum::F64(r) => r.update_at_event_time(|b| *b += (next() % 100) as f64, et),
                    TypedRingBufferEnum::Avg(r) => r.update_at_event_time(|b| { b.0 += (next() % 100) as f64; b.1 += 1; }, et),
                    TypedRingBufferEnum::MinI64(r) => r.update_at_event_time((next() % 1000) as i64, et),
                    TypedRingBufferEnum::MinF64(r) => r.update_at_event_time((next() % 1000) as f64, et),
                    TypedRingBufferEnum::MaxI64(r) => r.update_at_event_time((next() % 1000) as i64, et),
                    TypedRingBufferEnum::MaxF64(r) => r.update_at_event_time((next() % 1000) as f64, et),
                    TypedRingBufferEnum::InlineStr(r) => r.update_at_event_time(&format!("v{}", next() % 1000), et),
                }
            }
            rings.push(((
                format!("stream_{}", i),
                format!("entity_{}", i),
                i as u16,
            ), enm));
        }

        let snap = TypedStateSnapshotV11 {
            typed_entities: vec![],
            value_entities: vec![],
            typed_ringbuffers: rings.clone(),
        };
        let bytes = save_typed_state_v11(&snap).expect("save");
        let decoded = load_typed_state_v11(&bytes).expect("load");

        prop_assert_eq!(decoded.typed_ringbuffers.len(), rings.len());
        for (orig, got) in rings.iter().zip(decoded.typed_ringbuffers.iter()) {
            prop_assert_eq!(orig.0.clone(), got.0.clone());
            prop_assert_eq!(&orig.1, &got.1);
        }
    }
}
