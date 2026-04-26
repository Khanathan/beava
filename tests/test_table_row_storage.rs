//! Phase 24-01, Task 1: Tests for the TableRow storage primitive.
//!
//! Verifies the four StateStore methods (`upsert_table_row`,
//! `tombstone_table_row`, `get_table_row`, `gc_tombstones`) plus the
//! isolation guarantee between `table_rows` and `static_features`.

use ahash::AHashMap;
use std::time::{Duration, UNIX_EPOCH};

use tally::state::store::{StateStore, TableRowState, TOMBSTONE_GRACE};
use tally::types::FeatureValue;

fn ts(secs: u64) -> std::time::SystemTime {
    UNIX_EPOCH + Duration::from_secs(secs)
}

fn fields_ab() -> AHashMap<String, FeatureValue> {
    let mut m = AHashMap::new();
    m.insert("country".into(), FeatureValue::String("US".into()));
    m.insert("score".into(), FeatureValue::Int(42));
    m
}

#[test]
fn upsert_creates_live_row() {
    let store = StateStore::new();
    let now = ts(1_000_000);
    store.upsert_table_row("u1", "UserProfile", fields_ab(), now);

    let row = store
        .get_table_row("u1", "UserProfile")
        .expect("row must exist");
    assert_eq!(row.state, TableRowState::Live);
    assert_eq!(row.updated_at, now);
    assert_eq!(
        row.fields.get("country"),
        Some(&FeatureValue::String("US".into()))
    );
    assert_eq!(row.fields.get("score"), Some(&FeatureValue::Int(42)));
}

#[test]
fn tombstone_flips_live_to_tombstoned() {
    let store = StateStore::new();
    let t0 = ts(1_000_000);
    let t1 = ts(1_000_500);

    store.upsert_table_row("u1", "UserProfile", fields_ab(), t0);
    let prior_live = store.tombstone_table_row("u1", "UserProfile", t1);
    assert!(
        prior_live,
        "tombstone must report that a Live row existed prior"
    );

    let row = store
        .get_table_row("u1", "UserProfile")
        .expect("row must still be visible (within grace window)");
    match row.state {
        TableRowState::Tombstoned { since } => {
            assert_eq!(since, t1, "tombstone.since must equal the supplied now");
        }
        TableRowState::Live => panic!("expected Tombstoned, got Live"),
    }
    assert_eq!(row.updated_at, t1);
}

#[test]
fn tombstone_on_absent_creates_tombstone_only() {
    let store = StateStore::new();
    let now = ts(1_000_000);
    let prior_live = store.tombstone_table_row("ghost", "UserProfile", now);
    assert!(
        !prior_live,
        "tombstone on an absent row must report no prior Live row"
    );

    let row = store
        .get_table_row("ghost", "UserProfile")
        .expect("tombstone-only row must still be readable");
    assert!(matches!(row.state, TableRowState::Tombstoned { .. }));
    assert!(row.fields.is_empty());
}

#[test]
fn upsert_over_tombstone_resurrects() {
    let store = StateStore::new();
    let t0 = ts(1_000_000);
    let t1 = ts(1_000_500);
    let t2 = ts(1_001_000);

    store.upsert_table_row("u1", "UserProfile", fields_ab(), t0);
    store.tombstone_table_row("u1", "UserProfile", t1);

    let mut new_fields = AHashMap::new();
    new_fields.insert("country".into(), FeatureValue::String("UK".into()));
    store.upsert_table_row("u1", "UserProfile", new_fields, t2);

    let row = store.get_table_row("u1", "UserProfile").unwrap();
    assert_eq!(row.state, TableRowState::Live);
    assert_eq!(
        row.fields.get("country"),
        Some(&FeatureValue::String("UK".into()))
    );
    // The resurrected row must NOT retain old fields from the pre-tombstone
    // Live row (upsert replaces the whole field map).
    assert!(row.fields.get("score").is_none());
    assert_eq!(row.updated_at, t2);
}

#[test]
fn gc_tombstones_respects_7d_grace() {
    let store = StateStore::new();
    let t0 = ts(1_000_000);
    store.upsert_table_row("u1", "UserProfile", fields_ab(), t0);
    store.tombstone_table_row("u1", "UserProfile", t0);

    // Within the grace window (6 days in): must still be present.
    let sixty_days = Duration::from_secs(6 * 86400);
    let gc_before = store.gc_tombstones(t0 + sixty_days);
    assert_eq!(gc_before, 0, "no rows should be gc'd within 7d");
    assert!(
        store.get_table_row("u1", "UserProfile").is_some(),
        "tombstone must remain within grace"
    );

    // One second past 7d: removed.
    let after = t0 + TOMBSTONE_GRACE + Duration::from_secs(1);
    let gc_after = store.gc_tombstones(after);
    assert_eq!(gc_after, 1, "exactly one tombstone must be gc'd");
    assert!(
        store.get_table_row("u1", "UserProfile").is_none(),
        "expired tombstone must be gone"
    );
}

#[test]
fn gc_tombstones_leaves_live_rows_alone() {
    let store = StateStore::new();
    let t0 = ts(1_000_000);

    // Live row on u1
    store.upsert_table_row("u1", "UserProfile", fields_ab(), t0);
    // Tombstoned (expired) row on u2
    store.upsert_table_row("u2", "UserProfile", fields_ab(), t0);
    store.tombstone_table_row("u2", "UserProfile", t0);
    // Tombstoned but fresh row on u3
    store.upsert_table_row("u3", "UserProfile", fields_ab(), t0);
    store.tombstone_table_row("u3", "UserProfile", t0 + Duration::from_secs(86400));

    // Run GC a bit past the 7d grace window relative to t0.
    let now = t0 + TOMBSTONE_GRACE + Duration::from_secs(1);
    let removed = store.gc_tombstones(now);
    assert_eq!(
        removed, 1,
        "only the expired tombstone on u2 should be removed"
    );

    // u1 Live row untouched.
    assert_eq!(
        store.get_table_row("u1", "UserProfile").unwrap().state,
        TableRowState::Live
    );
    // u2 row gone.
    assert!(store.get_table_row("u2", "UserProfile").is_none());
    // u3 still within grace.
    assert!(matches!(
        store.get_table_row("u3", "UserProfile").unwrap().state,
        TableRowState::Tombstoned { .. }
    ));
}

#[test]
fn table_rows_independent_from_static_features() {
    let store = StateStore::new();
    let now = ts(1_000_000);

    // Write a table row named "X".
    let mut x_fields = AHashMap::new();
    x_fields.insert("v".into(), FeatureValue::Int(1));
    store.upsert_table_row("u1", "X", x_fields, now);

    // Entity should exist and have no static_features populated.
    {
        let ent = store.get_entity("u1").expect("entity must exist");
        assert!(
            ent.static_features.is_empty(),
            "upsert_table_row must not touch static_features"
        );
        assert_eq!(ent.table_rows.len(), 1);
    }

    // Write a static feature named "X" — must not collide with the table row.
    store.set_static("u1", "X", FeatureValue::String("static".into()), now);
    {
        let ent = store.get_entity("u1").unwrap();
        assert_eq!(ent.static_features.len(), 1);
        assert_eq!(ent.table_rows.len(), 1);
        let row = ent.table_rows.get("X").unwrap();
        assert_eq!(row.fields.get("v"), Some(&FeatureValue::Int(1)));
        let sf = ent.static_features.get("X").unwrap();
        assert_eq!(sf.value, FeatureValue::String("static".into()));
    }

    // And vice versa: tombstone the table row — static_features "X" untouched.
    store.tombstone_table_row("u1", "X", now);
    {
        let ent = store.get_entity("u1").unwrap();
        assert_eq!(ent.static_features.len(), 1);
        let sf = ent.static_features.get("X").unwrap();
        assert_eq!(sf.value, FeatureValue::String("static".into()));
    }
}
