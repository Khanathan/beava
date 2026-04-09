//! In-memory state store: EntityState + StateStore.
//!
//! EntityState stores per-key features from streaming operators (live) and
//! direct writes (static). StateStore maps entity keys to EntityState using
//! AHashMap (not std HashMap) per locked decision.

use std::time::SystemTime;
use ahash::AHashMap;
use serde::{Serialize, Deserialize};
use crate::types::{EntityKey, FeatureValue, FeatureMap};
use crate::engine::operators::Operator;

/// A directly-written feature value (from SET/MSET commands).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaticFeature {
    pub value: FeatureValue,
    pub updated_at: SystemTime,
}

/// Per-entity state. Holds live features (from streaming operators)
/// and static features (from direct SET/MSET writes).
#[derive(Debug)]
pub struct EntityState {
    /// Features computed by streaming operators. Keyed by feature name.
    /// The value is the operator instance itself (holds ring buffer state).
    /// Not serializable via serde (trait objects) -- Phase 4 will use enum wrapper.
    pub live_operators: Vec<(String, Box<dyn Operator>)>,
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

    /// Number of tracked entities.
    pub fn entity_count(&self) -> usize {
        self.entities.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};
    use crate::engine::operators::{CountOp, SumOp};

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
        let op = CountOp::new(Duration::from_secs(3600), Duration::from_secs(60));
        entity.live_operators.push(("tx_count_1h".to_string(), Box::new(op)));
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
            let mut op = CountOp::new(Duration::from_secs(3600), Duration::from_secs(60));
            op.push(&serde_json::json!({}), now).unwrap();
            entity.live_operators.push(("tx_count".to_string(), Box::new(op)));
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
            let mut op = SumOp::new("amount", Duration::from_secs(3600), Duration::from_secs(60), false);
            op.push(&serde_json::json!({"amount": 100.0}), now).unwrap();
            entity.live_operators.push(("score".to_string(), Box::new(op)));
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
}
