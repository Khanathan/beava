//! Phase 54-02 Task 1 — widened `StoreView::Sharded` + `Shard` surface.
//!
//! Covers the 5 new `StoreView` methods (`delete_entity`,
//! `tombstone_static`, `upsert_table_row`, `tombstone_table_row`,
//! `mark_dirty`) and the 2 new `Shard` helpers (`take_dirty`,
//! `iter_entities`). Each test runs against a freshly-constructed
//! `Shard` so that both feature backends (default / fjall + dev-mode
//! `state-inmem` / AHashMap) exercise the same behaviors:
//!
//!   cargo test --release --test shard_storeview_widening
//!   cargo test --release --features state-inmem --test shard_storeview_widening
//!
//! Semantics under test:
//! - `upsert_table_row` round-trip: write then read via
//!   `read_entity_from_shard` returns the row verbatim.
//! - `tombstone_table_row`: flips a Live row to Tombstoned; a fresh
//!   tombstone on an absent row creates an empty-fields tombstone.
//! - `delete_entity`: removes the entity from storage —
//!   `read_entity_from_shard` returns `None`. This is a deliberate
//!   semantic divergence from legacy `StateStore::delete_entity` (an
//!   alias for `tombstone_static`); Wave 4 unifies on full-removal.
//! - `tombstone_static`: clears static_features, preserves table_rows;
//!   returns `true` iff the entity had static features.
//! - `mark_dirty`: inserts into the shard's `dirty_set`.
//! - `take_dirty`: returns prior contents and empties the set.
//! - `iter_entities`: yields every key inserted via `with_entity_mut`.

use std::time::SystemTime;

use ahash::AHashMap;

use beava::shard::{read_entity_from_shard, Shard, StoreView};
use beava::state::store::{StaticFeature, TableRowState};
use beava::types::FeatureValue;

// ---------------------------------------------------------------------------
// Backend-agnostic Shard factory. Under `state-inmem` this is `Shard::new()`;
// under the default (fjall) build we open a tempdir-backed keyspace +
// partition. Each test owns its tempdir so file-descriptors are released
// when `_guard` falls out of scope.
// ---------------------------------------------------------------------------

#[cfg(not(feature = "state-inmem"))]
struct FjallFixture {
    _tmp: tempfile::TempDir,
    // Keyspace must outlive the partition handle — drop order: Shard (in
    // the test) → FjallFixture → _tmp. We keep the keyspace pinned here.
    // `open_keyspace_from_env` returns `Arc<Keyspace>`.
    _keyspace: std::sync::Arc<fjall::Keyspace>,
}

#[cfg(not(feature = "state-inmem"))]
fn new_shard() -> (Shard, FjallFixture) {
    use beava::shard::fjall_backend::{
        fjall_config_from_env, open_keyspace_from_env, open_shard_partition,
    };

    // Deterministic: disable fsync, small cache.
    std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
    std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let cfg = fjall_config_from_env(1);
    let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
    let partition = open_shard_partition(&ks, 0, &cfg).expect("open shard-0 partition");
    let shard = Shard::with_partition(partition);
    (
        shard,
        FjallFixture {
            _tmp: tmp,
            _keyspace: ks,
        },
    )
}

#[cfg(feature = "state-inmem")]
struct InmemFixture;

#[cfg(feature = "state-inmem")]
fn new_shard() -> (Shard, InmemFixture) {
    (Shard::new(), InmemFixture)
}

// Helper — build a simple fields map.
fn fields(pairs: &[(&str, FeatureValue)]) -> AHashMap<String, FeatureValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

// ===========================================================================
// 1. upsert_table_row round-trip
// ===========================================================================

#[test]
fn upsert_table_row_round_trip_via_view() {
    let (mut shard, _fx) = new_shard();
    let now = SystemTime::now();

    let row_fields = fields(&[
        ("name", FeatureValue::String("Alice".into())),
        ("score", FeatureValue::Int(42)),
    ]);

    {
        let mut view = StoreView::Sharded(&mut shard);
        view.upsert_table_row("u1", "UserProfile", row_fields.clone(), now);
    }

    // Read back via read_entity_from_shard and check the row landed.
    let row = read_entity_from_shard(&shard, "u1", |entity| {
        entity.table_rows.get("UserProfile").cloned()
    })
    .expect("entity present after upsert")
    .expect("UserProfile row present");

    assert!(matches!(row.state, TableRowState::Live));
    assert_eq!(
        row.fields.get("name"),
        Some(&FeatureValue::String("Alice".into()))
    );
    assert_eq!(row.fields.get("score"), Some(&FeatureValue::Int(42)));
    assert_eq!(row.updated_at, now);

    // Dirty-set must contain the key.
    assert!(shard.dirty_set.contains("u1"));
}

// ===========================================================================
// 2. tombstone_table_row — flips Live → Tombstoned; absent → tombstone-only
// ===========================================================================

#[test]
fn tombstone_table_row_flips_live_to_tombstoned() {
    let (mut shard, _fx) = new_shard();
    let now = SystemTime::now();
    let t1 = now + std::time::Duration::from_secs(60);

    // Seed a Live row.
    {
        let mut view = StoreView::Sharded(&mut shard);
        view.upsert_table_row(
            "u1",
            "T",
            fields(&[("x", FeatureValue::Int(1))]),
            now,
        );
    }
    shard.dirty_set.clear(); // isolate the tombstone's dirty-mark.

    // Tombstone. Return must be `true` (prior Live row existed).
    let had_live = {
        let mut view = StoreView::Sharded(&mut shard);
        view.tombstone_table_row("u1", "T", t1)
    };
    assert!(had_live, "prior Live row must report had_live=true");

    let row = read_entity_from_shard(&shard, "u1", |e| e.table_rows.get("T").cloned())
        .expect("entity present")
        .expect("row present");
    match row.state {
        TableRowState::Tombstoned { since } => assert_eq!(since, t1),
        TableRowState::Live => panic!("row must be Tombstoned after tombstone_table_row"),
    }
    assert!(
        row.fields.is_empty(),
        "tombstoned row fields are cleared (plan §Shard impl)"
    );
    assert!(shard.dirty_set.contains("u1"));
}

#[test]
fn tombstone_table_row_creates_tombstone_when_absent() {
    let (mut shard, _fx) = new_shard();
    let now = SystemTime::now();

    // No prior row. had_live = false.
    let had_live = {
        let mut view = StoreView::Sharded(&mut shard);
        view.tombstone_table_row("u1", "Absent", now)
    };
    assert!(!had_live, "absent row must report had_live=false");

    let row = read_entity_from_shard(&shard, "u1", |e| e.table_rows.get("Absent").cloned())
        .expect("entity created")
        .expect("tombstone row created");
    assert!(matches!(row.state, TableRowState::Tombstoned { .. }));
}

// ===========================================================================
// 3. delete_entity — removes entity from storage
// ===========================================================================

#[test]
fn delete_entity_removes_entity_from_shard_state() {
    let (mut shard, _fx) = new_shard();
    let now = SystemTime::now();

    // Seed an entity with a table row.
    {
        let mut view = StoreView::Sharded(&mut shard);
        view.upsert_table_row("u1", "T", fields(&[("x", FeatureValue::Int(1))]), now);
    }
    assert!(
        read_entity_from_shard(&shard, "u1", |_| ()).is_some(),
        "entity must be present before delete"
    );

    // Delete — returns true (existed).
    let removed = {
        let mut view = StoreView::Sharded(&mut shard);
        view.delete_entity("u1")
    };
    assert!(removed, "delete_entity must return true when entity existed");

    // Entity must be gone (plan §test #3).
    assert!(
        read_entity_from_shard(&shard, "u1", |_| ()).is_none(),
        "read_entity_from_shard must return None after delete_entity"
    );

    // Dirty-set must NOT retain the deleted key (matches StateStore's
    // mark_deleted → dirty_keys.remove contract).
    assert!(
        !shard.dirty_set.contains("u1"),
        "deleted key must be removed from dirty_set"
    );

    // Deleting an absent key returns false, not panic.
    let removed_again = {
        let mut view = StoreView::Sharded(&mut shard);
        view.delete_entity("u1")
    };
    assert!(!removed_again, "second delete on absent key returns false");
}

// ===========================================================================
// 4. tombstone_static — clears static_features, preserves table_rows
// ===========================================================================

#[test]
fn tombstone_static_clears_static_features_only() {
    let (mut shard, _fx) = new_shard();
    let now = SystemTime::now();

    // Seed: one static feature + one table row.
    {
        let mut view = StoreView::Sharded(&mut shard);
        view.with_entity_mut("u1", |e| {
            e.static_features.insert(
                "country".into(),
                StaticFeature {
                    value: FeatureValue::String("US".into()),
                    updated_at: now,
                },
            );
        });
        view.upsert_table_row("u1", "T", fields(&[("k", FeatureValue::Int(1))]), now);
    }
    shard.dirty_set.clear();

    // Tombstone the static features.
    let had_static = {
        let mut view = StoreView::Sharded(&mut shard);
        view.tombstone_static("u1")
    };
    assert!(had_static, "had static features before the call");
    assert!(shard.dirty_set.contains("u1"));

    // static_features is now empty; table_rows is preserved.
    read_entity_from_shard(&shard, "u1", |e| {
        assert!(
            e.static_features.is_empty(),
            "static_features cleared by tombstone_static"
        );
        assert!(
            e.table_rows.contains_key("T"),
            "table_rows preserved across tombstone_static"
        );
    })
    .expect("entity present after tombstone_static");

    // Second tombstone is a no-op (returns false, no dirty-mark).
    shard.dirty_set.clear();
    let had_static2 = {
        let mut view = StoreView::Sharded(&mut shard);
        view.tombstone_static("u1")
    };
    assert!(!had_static2, "no-op when static_features already empty");
    assert!(
        !shard.dirty_set.contains("u1"),
        "no dirty-mark when tombstone is a no-op"
    );
}

// ===========================================================================
// 5. mark_dirty via StoreView::Sharded
// ===========================================================================

#[test]
fn mark_dirty_via_view_inserts_into_shard_dirty_set() {
    let (mut shard, _fx) = new_shard();
    {
        let mut view = StoreView::Sharded(&mut shard);
        view.mark_dirty("u1");
        view.mark_dirty("u2");
        view.mark_dirty("u1"); // idempotent
    }
    assert_eq!(shard.dirty_set.len(), 2);
    assert!(shard.dirty_set.contains("u1"));
    assert!(shard.dirty_set.contains("u2"));
}

// ===========================================================================
// 6. take_dirty
// ===========================================================================

#[test]
fn take_dirty_consumes_and_empties_the_set() {
    let (mut shard, _fx) = new_shard();
    shard.dirty_set.insert("a".to_string());
    shard.dirty_set.insert("b".to_string());
    shard.dirty_set.insert("c".to_string());

    let drained = shard.take_dirty();
    assert_eq!(drained.len(), 3);
    assert!(drained.contains("a"));
    assert!(drained.contains("b"));
    assert!(drained.contains("c"));

    // Second take returns an empty set — the first one drained it.
    assert!(
        shard.dirty_set.is_empty(),
        "take_dirty leaves the shard's dirty_set empty"
    );
    let again = shard.take_dirty();
    assert!(again.is_empty());
}

// ===========================================================================
// 7. iter_entities — yields every key inserted via with_entity_mut
// ===========================================================================

#[test]
fn iter_entities_yields_every_inserted_key() {
    let (mut shard, _fx) = new_shard();
    let now = SystemTime::now();

    let keys = ["alpha", "beta", "gamma", "delta"];
    for (i, k) in keys.iter().enumerate() {
        let mut view = StoreView::Sharded(&mut shard);
        view.with_entity_mut(*k, |e| {
            e.static_features.insert(
                "idx".to_string(),
                StaticFeature {
                    value: FeatureValue::Int(i as i64),
                    updated_at: now,
                },
            );
        });
    }

    let mut seen: Vec<String> =
        shard.iter_entities().into_iter().map(|(k, _)| k).collect();
    seen.sort();
    let mut expected: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
    expected.sort();
    assert_eq!(seen, expected);

    // Each yielded EntityState carries the static_feature we wrote.
    for (k, entity) in shard.iter_entities() {
        let idx = entity
            .static_features
            .get("idx")
            .expect("idx static feature present");
        // The encoded index must match the position in `keys`.
        let pos = keys.iter().position(|s| *s == k).expect("yielded key in set");
        assert_eq!(idx.value, FeatureValue::Int(pos as i64));
    }
}
