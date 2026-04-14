//! Phase 24-01, Task 2: Snapshot codec v7 round-trip + v6 → v7 migration.
//!
//! Verifies:
//! 1. v7 snapshots round-trip entities with Live and Tombstoned table_rows.
//! 2. Tombstoned.since (SystemTime) is preserved byte-for-byte.
//! 3. v6 snapshots (encoded via the test helper) load under the v7 binary
//!    with table_rows initialized empty and no panic.
//! 4. Unknown version bytes return None cleanly.
//! 5. A loaded v7 snapshot containing Live + Tombstoned rows is
//!    `gc_tombstones`-friendly: only the expired tombstone is removed.

use std::time::{Duration, UNIX_EPOCH};

use tally::engine::operators::CountOp;
use tally::state::snapshot::{
    load_snapshot, save_base_snapshot_v6_for_test, save_snapshot, BaseSnapshotStateV6,
    OperatorState, SerializableEntityState, SerializableEntityStateV6,
    SerializableStreamEntityState, SnapshotHeader, SnapshotState, SnapshotType, LEGACY_V6_FORMAT,
    SNAPSHOT_FORMAT_VERSION,
};
use tally::state::store::{
    SerializableTableRow, StateStore, StaticFeature, TableRowState, TOMBSTONE_GRACE,
};
use tally::types::FeatureValue;

fn ts(secs: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn sample_entity_with_rows(
    now: std::time::SystemTime,
    tombstone_at: std::time::SystemTime,
) -> (String, SerializableEntityState) {
    let mut op = OperatorState::Count(CountOp::new(
        Duration::from_secs(3600),
        Duration::from_secs(60),
    ));
    op.push(&serde_json::json!({}), None, now).unwrap();

    (
        "u1".to_string(),
        SerializableEntityState {
            streams: vec![(
                "Transactions".to_string(),
                SerializableStreamEntityState {
                    operators: vec![("tx_count_1h".to_string(), op)],
                    last_event_at: Some(now),
                },
            )],
            static_features: vec![(
                "segment".to_string(),
                StaticFeature {
                    value: FeatureValue::String("premium".into()),
                    updated_at: now,
                },
            )],
            table_rows: vec![
                (
                    "UserProfile".to_string(),
                    SerializableTableRow {
                        fields: vec![
                            ("country".into(), FeatureValue::String("US".into())),
                            ("score".into(), FeatureValue::Int(42)),
                        ],
                        state: TableRowState::Live,
                        updated_at: now,
                    },
                ),
                (
                    "Session".to_string(),
                    SerializableTableRow {
                        fields: vec![],
                        state: TableRowState::Tombstoned {
                            since: tombstone_at,
                        },
                        updated_at: tombstone_at,
                    },
                ),
            ],
        },
    )
}

#[test]
fn v7_roundtrip_table_rows() {
    let now = ts(1_000_000);
    let entity = sample_entity_with_rows(now, now);
    let state = SnapshotState {
        entities: vec![entity.clone()],
        pipelines: vec![],
        backfill_complete: vec![],
    };

    let bytes = save_snapshot(&state).expect("save");
    assert_eq!(bytes[0], SNAPSHOT_FORMAT_VERSION);
    assert_eq!(bytes[0], 0x07);

    let restored = load_snapshot(&bytes).expect("load");
    assert_eq!(restored.entities.len(), 1);
    let rows = &restored.entities[0].1.table_rows;
    assert_eq!(rows.len(), 2);

    let up = rows.iter().find(|(n, _)| n == "UserProfile").unwrap();
    assert_eq!(up.1.state, TableRowState::Live);
    let fields: std::collections::HashMap<_, _> = up.1.fields.iter().cloned().collect();
    assert_eq!(
        fields.get("country"),
        Some(&FeatureValue::String("US".into()))
    );
    assert_eq!(fields.get("score"), Some(&FeatureValue::Int(42)));

    let sess = rows.iter().find(|(n, _)| n == "Session").unwrap();
    assert!(matches!(sess.1.state, TableRowState::Tombstoned { .. }));
}

#[test]
fn v7_roundtrip_tombstone_since_preserved() {
    let now = ts(1_000_000);
    let tombstone_at = ts(1_234_567);
    let entity = sample_entity_with_rows(now, tombstone_at);
    let state = SnapshotState {
        entities: vec![entity],
        pipelines: vec![],
        backfill_complete: vec![],
    };

    let bytes = save_snapshot(&state).expect("save");
    let restored = load_snapshot(&bytes).expect("load");
    let sess = restored.entities[0]
        .1
        .table_rows
        .iter()
        .find(|(n, _)| n == "Session")
        .unwrap();
    match sess.1.state {
        TableRowState::Tombstoned { since } => {
            assert_eq!(
                since, tombstone_at,
                "Tombstoned.since must survive round-trip byte-for-byte"
            );
        }
        _ => panic!("expected Tombstoned"),
    }
}

#[test]
fn v6_snapshot_loads_with_empty_table_rows() {
    let now = ts(1_000_000);
    let mut op = OperatorState::Count(CountOp::new(
        Duration::from_secs(3600),
        Duration::from_secs(60),
    ));
    op.push(&serde_json::json!({}), None, now).unwrap();
    op.push(&serde_json::json!({}), None, now).unwrap();

    let v6_base = BaseSnapshotStateV6 {
        header: SnapshotHeader {
            snapshot_type: SnapshotType::Base,
            sequence: 42,
        },
        entities: vec![(
            "u_legacy".to_string(),
            SerializableEntityStateV6 {
                streams: vec![(
                    "Transactions".to_string(),
                    SerializableStreamEntityState {
                        operators: vec![("tx_count_1h".to_string(), op)],
                        last_event_at: Some(now),
                    },
                )],
                static_features: vec![(
                    "tier".to_string(),
                    StaticFeature {
                        value: FeatureValue::String("gold".into()),
                        updated_at: now,
                    },
                )],
            },
        )],
        pipelines: vec![],
        backfill_complete: vec![],
    };

    let v6_bytes = save_base_snapshot_v6_for_test(&v6_base).expect("save v6");
    assert_eq!(v6_bytes[0], LEGACY_V6_FORMAT);
    assert_ne!(v6_bytes[0], SNAPSHOT_FORMAT_VERSION);

    let restored = load_snapshot(&v6_bytes).expect("v6 must load under v7 binary");
    assert_eq!(restored.entities.len(), 1);
    let (key, entity) = &restored.entities[0];
    assert_eq!(key, "u_legacy");
    // Streams preserved.
    assert_eq!(entity.streams.len(), 1);
    assert_eq!(entity.streams[0].0, "Transactions");
    assert_eq!(entity.streams[0].1.operators.len(), 1);
    // Static features preserved.
    assert_eq!(entity.static_features.len(), 1);
    assert_eq!(entity.static_features[0].0, "tier");
    // table_rows initialized empty — this is the migration contract.
    assert!(
        entity.table_rows.is_empty(),
        "v6 entities must load with empty table_rows"
    );

    // The operator state survives the v6→v7 migration.
    let mut op_restored = entity.streams[0].1.operators[0].1.clone();
    assert_eq!(op_restored.read(now), FeatureValue::Int(2));
}

#[test]
fn unknown_version_returns_none() {
    // Start from a valid v7 snapshot then tamper with the version byte.
    let state = SnapshotState {
        entities: vec![],
        pipelines: vec![],
        backfill_complete: vec![],
    };
    let mut bytes = save_snapshot(&state).expect("save");
    bytes[0] = 0xFE;
    assert!(
        load_snapshot(&bytes).is_none(),
        "unknown version byte must return None (no panic, no deserialization)"
    );
}

#[test]
fn v7_mixed_live_tombstoned_gc_friendly() {
    // Build a v7 snapshot with two entities: one carrying a Live table_row,
    // another carrying a tombstoned-at-t0 row. Save, load into a fresh
    // StateStore via restore_from_snapshot, then call gc_tombstones with
    // (t0 + TOMBSTONE_GRACE + 1s) and verify only the tombstoned row is gone.
    let t0 = ts(1_000_000);

    let live_entity = (
        "u_live".to_string(),
        SerializableEntityState {
            streams: vec![],
            static_features: vec![],
            table_rows: vec![(
                "UserProfile".to_string(),
                SerializableTableRow {
                    fields: vec![("country".into(), FeatureValue::String("US".into()))],
                    state: TableRowState::Live,
                    updated_at: t0,
                },
            )],
        },
    );
    let tomb_entity = (
        "u_tomb".to_string(),
        SerializableEntityState {
            streams: vec![],
            static_features: vec![],
            table_rows: vec![(
                "UserProfile".to_string(),
                SerializableTableRow {
                    fields: vec![],
                    state: TableRowState::Tombstoned { since: t0 },
                    updated_at: t0,
                },
            )],
        },
    );

    let state = SnapshotState {
        entities: vec![live_entity, tomb_entity],
        pipelines: vec![],
        backfill_complete: vec![],
    };
    let bytes = save_snapshot(&state).expect("save");
    let restored = load_snapshot(&bytes).expect("load");

    let store = StateStore::new();
    store.restore_from_snapshot(restored.entities);

    // Pre-GC: both rows visible.
    assert!(store.get_table_row("u_live", "UserProfile").is_some());
    assert!(store.get_table_row("u_tomb", "UserProfile").is_some());

    // GC past the grace window: tombstoned row gone, live untouched.
    let now = t0 + TOMBSTONE_GRACE + Duration::from_secs(1);
    let removed = store.gc_tombstones(now);
    assert_eq!(removed, 1, "exactly one tombstone must be gc'd");
    assert_eq!(
        store.get_table_row("u_live", "UserProfile").unwrap().state,
        TableRowState::Live
    );
    assert!(
        store.get_table_row("u_tomb", "UserProfile").is_none(),
        "expired tombstone must be gone after gc_tombstones"
    );
}
