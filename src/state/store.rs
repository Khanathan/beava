//! In-memory state store: EntityState + StateStore.
//!
//! EntityState stores per-key features from streaming operators (live) and
//! direct writes (static). StateStore maps entity keys to EntityState using
//! AHashMap (not std HashMap) per locked decision.

use std::time::SystemTime;
use ahash::AHashMap;
use serde::{Serialize, Deserialize};
use crate::types::{EntityKey, FeatureValue, FeatureMap};
use crate::state::snapshot::{OperatorState, SerializableEntityState};

/// A directly-written feature value (from SET/MSET commands).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticFeature {
    pub value: FeatureValue,
    pub updated_at: SystemTime,
}

/// Per-entity state. Holds live features (from streaming operators)
/// and static features (from direct SET/MSET writes).
#[derive(Debug, Clone)]
pub struct EntityState {
    /// Features computed by streaming operators. Keyed by feature name.
    /// Uses OperatorState enum (not Box<dyn Operator>) for serialization support.
    pub live_operators: Vec<(String, OperatorState)>,
    /// Features from direct writes (SET/MSET). Bypass pipeline engine.
    pub static_features: AHashMap<String, StaticFeature>,
    /// Last event timestamp for TTL eviction (Phase 4).
    pub last_event_at: Option<SystemTime>,
}

impl Default for EntityState {
    fn default() -> Self {
        Self {
            live_operators: Vec::new(),
            static_features: AHashMap::new(),
            last_event_at: None,
        }
    }
}

impl EntityState {
    /// Create a new empty EntityState.
    pub fn new() -> Self {
        Self::default()
    }

    /// Update the last event timestamp.
    pub fn update_last_event(&mut self, now: SystemTime) {
        self.last_event_at = Some(now);
    }
}

/// The top-level state store. Maps entity keys to their state.
/// Uses AHashMap per STATE.md locked decision (not std HashMap).
#[derive(Debug)]
pub struct StateStore {
    entities: AHashMap<EntityKey, EntityState>,
}

impl Default for StateStore {
    fn default() -> Self {
        Self {
            entities: AHashMap::new(),
        }
    }
}

impl StateStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create an EntityState for the given key.
    /// Returns a mutable reference to the entity's state.
    pub fn get_or_create_entity(&mut self, key: &str) -> &mut EntityState {
        self.entities
            .entry(key.to_string())
            .or_insert_with(EntityState::new)
    }

    /// Read-only access to an entity's state. Returns None if key not found.
    pub fn get_entity(&self, key: &str) -> Option<&EntityState> {
        self.entities.get(key)
    }

    /// Mutable access to an entity's state. Returns None if key not found.
    pub fn get_entity_mut(&mut self, key: &str) -> Option<&mut EntityState> {
        self.entities.get_mut(key)
    }

    /// Write a static feature for an entity. Creates the entity if absent.
    /// Accepts an explicit `now` timestamp for determinism and testability (WR-05).
    pub fn set_static(&mut self, key: &str, feature_name: &str, value: FeatureValue, now: SystemTime) {
        let entity = self.get_or_create_entity(key);
        entity.static_features.insert(
            feature_name.to_string(),
            StaticFeature {
                value,
                updated_at: now,
            },
        );
    }

    /// Collect all feature values for an entity.
    /// Iterates live_operators calling read(now) (which advances time to expire
    /// stale buckets), then overlays static_features. Static features with the
    /// same name override live features (direct writes take precedence).
    /// Takes &mut self because operator read() requires mutable access.
    pub fn get_all_features(&mut self, key: &str, now: SystemTime) -> FeatureMap {
        let entity = match self.entities.get_mut(key) {
            Some(e) => e,
            None => return FeatureMap::default(),
        };

        let mut features = FeatureMap::new();

        // Collect live features from operators
        for (name, op) in entity.live_operators.iter_mut() {
            features.insert(name.clone(), op.read(now));
        }

        // Overlay static features (static takes precedence)
        for (name, sf) in &entity.static_features {
            features.insert(name.clone(), sf.value.clone());
        }

        features
    }

    /// Read a single feature value for an entity. Used by cross-key lookups.
    /// Returns Missing if entity or feature not found.
    /// Takes &mut self because operator read() requires mutable access.
    pub fn get_feature_value(&mut self, key: &str, feature_name: &str, now: SystemTime) -> FeatureValue {
        let entity = match self.entities.get_mut(key) {
            Some(e) => e,
            None => return FeatureValue::Missing,
        };
        // Check live operators first
        for (name, op) in entity.live_operators.iter_mut() {
            if name == feature_name {
                return op.read(now);
            }
        }
        // Check static features
        if let Some(sf) = entity.static_features.get(feature_name) {
            return sf.value.clone();
        }
        FeatureValue::Missing
    }

    /// Number of tracked entities.
    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }

    /// Clone full state for snapshot serialization.
    /// AHashMap is not directly serializable by postcard -- convert to Vec<(K, V)>.
    pub fn clone_for_snapshot(&self) -> Vec<(String, SerializableEntityState)> {
        self.entities.iter().map(|(key, entity)| {
            (key.clone(), SerializableEntityState {
                live_operators: entity.live_operators.clone(),
                static_features: entity.static_features.iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
                last_event_at: entity.last_event_at,
            })
        }).collect()
    }

    /// Restore state from a snapshot. Clears existing state first.
    pub fn restore_from_snapshot(&mut self, entities: Vec<(String, SerializableEntityState)>) {
        self.entities.clear();
        for (key, state) in entities {
            let entity = EntityState {
                live_operators: state.live_operators,
                static_features: state.static_features.into_iter().collect(),
                last_event_at: state.last_event_at,
            };
            self.entities.insert(key, entity);
        }
    }

    /// Remove entities whose last_event_at is strictly older than `ttl` from `now`.
    /// Entities exactly at TTL age are kept (evicted only after TTL has fully elapsed).
    /// Entities with `last_event_at = None` are not evicted (never received an event).
    /// Returns the count of evicted entities.
    pub fn remove_expired_entities(&mut self, now: SystemTime, ttl: std::time::Duration) -> usize {
        let before = self.entities.len();
        self.entities.retain(|_key, entity| {
            match entity.last_event_at {
                None => true, // Never pushed -- don't evict
                Some(last) => {
                    now.duration_since(last).unwrap_or(std::time::Duration::ZERO) <= ttl
                }
            }
        });
        before - self.entities.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};
    use crate::engine::operators::{CountOp, SumOp};
    use crate::state::snapshot::OperatorState;

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn test_new_store_is_empty() {
        let store = StateStore::new();
        assert_eq!(store.entity_count(), 0);
    }

    #[test]
    fn test_get_or_create_entity_creates_new() {
        let mut store = StateStore::new();
        let entity = store.get_or_create_entity("u123");
        assert!(entity.live_operators.is_empty());
        assert!(entity.static_features.is_empty());
        assert!(entity.last_event_at.is_none());
        assert_eq!(store.entity_count(), 1);
    }

    #[test]
    fn test_get_or_create_entity_returns_existing() {
        let mut store = StateStore::new();
        // First call creates
        store.get_or_create_entity("u123");
        // Mutate the entity so we can verify it's the same one
        store.get_or_create_entity("u123").update_last_event(ts(1000));
        assert_eq!(store.entity_count(), 1); // Still only 1 entity
        let entity = store.get_entity("u123").unwrap();
        assert_eq!(entity.last_event_at, Some(ts(1000)));
    }

    #[test]
    fn test_entity_state_stores_live_operators() {
        let mut entity = EntityState::new();
        let op = OperatorState::Count(CountOp::new(Duration::from_secs(3600), Duration::from_secs(60)));
        entity.live_operators.push(("tx_count_1h".to_string(), op));
        assert_eq!(entity.live_operators.len(), 1);
        assert_eq!(entity.live_operators[0].0, "tx_count_1h");
    }

    #[test]
    fn test_entity_state_stores_static_features() {
        let mut store = StateStore::new();
        store.set_static("u123", "lifetime_value", FeatureValue::Float(4500.0), ts(1000));
        let entity = store.get_entity("u123").unwrap();
        assert_eq!(entity.static_features.len(), 1);
        assert_eq!(
            entity.static_features.get("lifetime_value").unwrap().value,
            FeatureValue::Float(4500.0)
        );
    }

    #[test]
    fn test_get_all_features_merges_live_and_static() {
        let mut store = StateStore::new();
        let now = ts(60_000);

        // Add a live operator
        {
            let entity = store.get_or_create_entity("u123");
            let mut op = OperatorState::Count(CountOp::new(Duration::from_secs(3600), Duration::from_secs(60)));
            op.push(&serde_json::json!({}), now).unwrap();
            entity.live_operators.push(("tx_count".to_string(), op));
        }

        // Add a static feature
        store.set_static("u123", "segment", FeatureValue::String("high_value".into()), ts(1000));

        let features = store.get_all_features("u123", now);
        assert_eq!(features.get("tx_count"), Some(&FeatureValue::Int(1)));
        assert_eq!(features.get("segment"), Some(&FeatureValue::String("high_value".into())));
    }

    #[test]
    fn test_static_feature_overrides_live_feature_same_name() {
        let mut store = StateStore::new();
        let now = ts(60_000);

        // Add a live operator named "score"
        {
            let entity = store.get_or_create_entity("u123");
            let mut op = OperatorState::Sum(SumOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false));
            op.push(&serde_json::json!({"amount": 100.0}), now).unwrap();
            entity.live_operators.push(("score".to_string(), op));
        }

        // Write a static feature with the same name
        store.set_static("u123", "score", FeatureValue::Float(999.0), ts(1000));

        let features = store.get_all_features("u123", now);
        // Static takes precedence
        assert_eq!(features.get("score"), Some(&FeatureValue::Float(999.0)));
    }

    #[test]
    fn test_last_event_at_updated_on_push() {
        let mut entity = EntityState::new();
        assert!(entity.last_event_at.is_none());
        let now = ts(1000);
        entity.update_last_event(now);
        assert_eq!(entity.last_event_at, Some(now));
    }

    #[test]
    fn test_get_all_features_unknown_key_returns_empty() {
        let mut store = StateStore::new();
        let features = store.get_all_features("nonexistent", ts(1000));
        assert!(features.is_empty());
    }

    // ======================== clone_for_snapshot / restore_from_snapshot Tests ========================

    #[test]
    fn test_clone_for_snapshot_preserves_state() {
        let mut store = StateStore::new();
        let now = ts(60_000);

        // Add an entity with a live operator and static feature
        {
            let entity = store.get_or_create_entity("u123");
            let mut op = OperatorState::Count(CountOp::new(Duration::from_secs(3600), Duration::from_secs(60)));
            op.push(&serde_json::json!({}), now).unwrap();
            op.push(&serde_json::json!({}), now).unwrap();
            entity.live_operators.push(("tx_count".to_string(), op));
            entity.update_last_event(now);
        }
        store.set_static("u123", "segment", FeatureValue::String("premium".into()), now);

        let snapshot = store.clone_for_snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].0, "u123");
        assert_eq!(snapshot[0].1.live_operators.len(), 1);
        assert_eq!(snapshot[0].1.static_features.len(), 1);
        assert_eq!(snapshot[0].1.last_event_at, Some(now));

        // Verify operator state preserved
        let mut op = snapshot[0].1.live_operators[0].1.clone();
        assert_eq!(op.read(now), FeatureValue::Int(2));
    }

    #[test]
    fn test_restore_from_snapshot() {
        let mut store = StateStore::new();
        let now = ts(60_000);

        let mut op = OperatorState::Count(CountOp::new(Duration::from_secs(3600), Duration::from_secs(60)));
        op.push(&serde_json::json!({}), now).unwrap();

        let snapshot_entities = vec![(
            "u456".to_string(),
            crate::state::snapshot::SerializableEntityState {
                live_operators: vec![("count".to_string(), op)],
                static_features: vec![(
                    "tier".to_string(),
                    StaticFeature {
                        value: FeatureValue::String("gold".into()),
                        updated_at: now,
                    },
                )],
                last_event_at: Some(now),
            },
        )];

        store.restore_from_snapshot(snapshot_entities);
        assert_eq!(store.entity_count(), 1);
        let entity = store.get_entity("u456").unwrap();
        assert_eq!(entity.live_operators.len(), 1);
        assert_eq!(entity.static_features.len(), 1);
        assert_eq!(entity.last_event_at, Some(now));
    }

    // ======================== Phase 5 Plan 03: get_feature_value Tests ========================

    #[test]
    fn test_get_feature_value_returns_live_operator_value() {
        let mut store = StateStore::new();
        let now = ts(60_000);

        let entity = store.get_or_create_entity("u123");
        let mut op = OperatorState::Count(CountOp::new(Duration::from_secs(3600), Duration::from_secs(60)));
        op.push(&serde_json::json!({}), now).unwrap();
        op.push(&serde_json::json!({}), now).unwrap();
        entity.live_operators.push(("tx_count".to_string(), op));

        let val = store.get_feature_value("u123", "tx_count", now);
        assert_eq!(val, FeatureValue::Int(2));
    }

    #[test]
    fn test_get_feature_value_returns_static_feature() {
        let mut store = StateStore::new();
        let now = ts(60_000);
        store.set_static("u123", "segment", FeatureValue::String("premium".into()), now);

        let val = store.get_feature_value("u123", "segment", now);
        assert_eq!(val, FeatureValue::String("premium".into()));
    }

    #[test]
    fn test_get_feature_value_returns_missing_for_unknown_entity() {
        let mut store = StateStore::new();
        let val = store.get_feature_value("nonexistent", "anything", ts(60_000));
        assert_eq!(val, FeatureValue::Missing);
    }

    #[test]
    fn test_get_feature_value_returns_missing_for_unknown_feature() {
        let mut store = StateStore::new();
        store.get_or_create_entity("u123");
        let val = store.get_feature_value("u123", "nonexistent_feature", ts(60_000));
        assert_eq!(val, FeatureValue::Missing);
    }

    // ======================== remove_expired_entities Tests ========================

    #[test]
    fn test_remove_expired_entities() {
        let mut store = StateStore::new();
        let now = ts(100_000);
        let ttl = Duration::from_secs(3600); // 1 hour TTL

        // Entity with old last_event_at (should be evicted)
        {
            let entity = store.get_or_create_entity("old_user");
            entity.update_last_event(ts(1000)); // Very old
        }

        // Entity with recent last_event_at (should be kept)
        {
            let entity = store.get_or_create_entity("recent_user");
            entity.update_last_event(ts(99_000)); // Recent
        }

        // Entity with no last_event_at (should be kept)
        store.get_or_create_entity("no_event_user");

        assert_eq!(store.entity_count(), 3);
        let evicted = store.remove_expired_entities(now, ttl);
        assert_eq!(evicted, 1);
        assert_eq!(store.entity_count(), 2);
        assert!(store.get_entity("old_user").is_none());
        assert!(store.get_entity("recent_user").is_some());
        assert!(store.get_entity("no_event_user").is_some());
    }
}
