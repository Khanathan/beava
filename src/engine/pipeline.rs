//! Pipeline engine: stream definitions and push-through orchestration.
//!
//! PipelineEngine holds registered stream definitions and coordinates the
//! synchronous push-through flow: event -> extract key -> update operators
//! -> evaluate derives -> return feature map.

use std::time::{Duration, SystemTime};
use ahash::AHashMap;
use crate::types::{FeatureValue, FeatureMap};
use crate::error::TallyError;
use crate::state::store::StateStore;
use super::operators::{CountOp, SumOp, AvgOp};
use crate::state::snapshot::OperatorState;
use super::expression::{Expr, EvalContext, eval};

/// Definition of a single feature within a stream.
#[derive(Debug, Clone)]
pub enum FeatureDef {
    Count {
        window: Duration,
        bucket: Duration,
    },
    Sum {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
    },
    Avg {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
    },
    Derive {
        expr: Expr, // Parsed at registration time
    },
}

/// A stream definition: a named stream with a key field and a list of named features.
#[derive(Debug, Clone)]
pub struct StreamDefinition {
    pub name: String,
    pub key_field: String,
    pub features: Vec<(String, FeatureDef)>, // (feature_name, definition)
}

/// The pipeline engine. Holds registered stream definitions and coordinates
/// the push-through flow.
#[derive(Debug)]
pub struct PipelineEngine {
    streams: AHashMap<String, StreamDefinition>,
    /// Raw register JSON strings for each stream, keyed by stream name.
    /// Stored on REGISTER so snapshots can persist pipeline definitions
    /// without serializing the Expr AST.
    raw_register_jsons: AHashMap<String, serde_json::Value>,
}

/// Create an operator instance from a FeatureDef (non-derive only).
/// Returns OperatorState enum (not Box<dyn Operator>) for serialization support.
fn create_operator(def: &FeatureDef) -> Option<OperatorState> {
    match def {
        FeatureDef::Count { window, bucket } => {
            Some(OperatorState::Count(CountOp::new(*window, *bucket)))
        }
        FeatureDef::Sum { field, window, bucket, optional } => {
            Some(OperatorState::Sum(SumOp::new(field.clone(), *window, *bucket, *optional)))
        }
        FeatureDef::Avg { field, window, bucket, optional } => {
            Some(OperatorState::Avg(AvgOp::new(field.clone(), *window, *bucket, *optional)))
        }
        FeatureDef::Derive { .. } => None, // Derives have no operator state
    }
}

impl PipelineEngine {
    /// Create engine with no registered streams.
    pub fn new() -> Self {
        Self {
            streams: AHashMap::new(),
            raw_register_jsons: AHashMap::new(),
        }
    }

    /// Register a stream definition. Validates derive expressions are parseable.
    /// Duplicate registration replaces the previous definition (idempotent).
    /// Stream names must be non-empty (T-01-14 mitigation).
    pub fn register(&mut self, stream: StreamDefinition) -> Result<(), TallyError> {
        if stream.name.is_empty() {
            return Err(TallyError::Protocol("stream name must not be empty".into()));
        }
        // Derive expressions should already be parsed in the StreamDefinition,
        // but verify they exist
        for (name, def) in &stream.features {
            if let FeatureDef::Derive { expr: _ } = def {
                // Expression is already parsed -- valid
                let _ = name;
            }
        }
        self.streams.insert(stream.name.clone(), stream);
        Ok(())
    }

    /// Synchronous push-through flow:
    /// 1. Look up stream definition by name
    /// 2. Extract entity key from event JSON
    /// 3. Get or create EntityState
    /// 4. For each operator feature: find or create operator, call push
    /// 5. Collect all feature values: read operators + evaluate derives
    /// 6. Update last_event_at
    /// 7. Return complete FeatureMap
    pub fn push(
        &self,
        stream_name: &str,
        event: &serde_json::Value,
        store: &mut StateStore,
        now: SystemTime,
    ) -> Result<FeatureMap, TallyError> {
        // 1. Look up stream definition
        let stream = self.streams.get(stream_name).ok_or_else(|| {
            TallyError::Protocol(format!("unknown stream: {}", stream_name))
        })?;

        // 2. Extract entity key from event JSON (T-01-11 mitigation)
        let key = match event.get(&stream.key_field) {
            Some(serde_json::Value::String(s)) => {
                if s.is_empty() {
                    return Err(TallyError::Protocol(
                        format!("empty key field '{}'", stream.key_field),
                    ));
                }
                s.clone()
            }
            Some(other) => {
                return Err(TallyError::Type {
                    field: stream.key_field.clone(),
                    expected: "string".into(),
                    got: format!("{}", other),
                });
            }
            None => {
                return Err(TallyError::Type {
                    field: stream.key_field.clone(),
                    expected: "string".into(),
                    got: "absent".into(),
                });
            }
        };

        // 3. Get or create EntityState
        let entity = store.get_or_create_entity(&key);

        // 4. Initialize or reconcile operators with current stream definition.
        // On first push: create operators. On re-registration: rebuild if feature
        // count changed (WR-04 fix: old code only checked is_empty, silently
        // ignoring stream definition changes).
        let expected_op_count = stream.features.iter()
            .filter(|(_, def)| !matches!(def, FeatureDef::Derive { .. }))
            .count();
        if entity.live_operators.len() != expected_op_count {
            entity.live_operators.clear();
            for (name, def) in &stream.features {
                if let Some(op) = create_operator(def) {
                    entity.live_operators.push((name.clone(), op));
                }
            }
        }

        // Push event to all operators
        for (_, op) in entity.live_operators.iter_mut() {
            op.push(event, now)?;
        }

        // 5. Collect feature values
        let mut features = FeatureMap::new();

        // Read all operator values
        for (name, op) in entity.live_operators.iter_mut() {
            features.insert(name.clone(), op.read(now));
        }

        // Overlay static features
        for (name, sf) in &entity.static_features {
            features.insert(name.clone(), sf.value.clone());
        }

        // Evaluate derive expressions (collect first to avoid borrow conflict)
        let derived: Vec<(String, FeatureValue)> = {
            let ctx = EvalContext {
                features: &features,
                event: Some(event),
            };
            stream.features.iter()
                .filter_map(|(name, def)| {
                    if let FeatureDef::Derive { expr } = def {
                        Some((name.clone(), eval(expr, &ctx)))
                    } else {
                        None
                    }
                })
                .collect()
        };
        for (name, value) in derived {
            features.insert(name, value);
        }

        // 6. Update last_event_at
        entity.update_last_event(now);

        // 7. Return features
        Ok(features)
    }

    /// Feature retrieval for GET path.
    /// Calls store.get_all_features (which reads operators with &mut self to
    /// advance time and expire stale buckets), then evaluates derive expressions
    /// for any registered streams.
    pub fn get_features(
        &self,
        key: &str,
        store: &mut StateStore,
        now: SystemTime,
    ) -> FeatureMap {
        let mut features = store.get_all_features(key, now);

        // Evaluate derives from all registered streams
        let ctx = EvalContext {
            features: &features,
            event: None,
        };
        // Collect derives first to avoid borrow issues
        let mut derived: Vec<(String, FeatureValue)> = Vec::new();
        for stream in self.streams.values() {
            for (name, def) in &stream.features {
                if let FeatureDef::Derive { expr } = def {
                    let value = eval(expr, &ctx);
                    derived.push((name.clone(), value));
                }
            }
        }
        for (name, value) in derived {
            features.insert(name, value);
        }

        features
    }

    /// Get a registered stream definition by name.
    pub fn get_stream(&self, name: &str) -> Option<&StreamDefinition> {
        self.streams.get(name)
    }

    /// Number of registered streams.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Return the maximum window duration across all registered streams.
    /// Returns Duration::ZERO if no streams are registered.
    pub fn max_window_duration(&self) -> Duration {
        self.streams.values()
            .flat_map(|s| s.features.iter())
            .filter_map(|(_, def)| match def {
                FeatureDef::Count { window, .. } => Some(*window),
                FeatureDef::Sum { window, .. } => Some(*window),
                FeatureDef::Avg { window, .. } => Some(*window),
                FeatureDef::Derive { .. } => None,
            })
            .max()
            .unwrap_or(Duration::ZERO)
    }

    /// Iterate over all registered stream definitions.
    pub fn list_streams(&self) -> impl Iterator<Item = &StreamDefinition> {
        self.streams.values()
    }

    /// Remove a stream definition by name. Returns true if found and removed.
    pub fn remove_stream(&mut self, name: &str) -> bool {
        self.raw_register_jsons.remove(name);
        self.streams.remove(name).is_some()
    }

    /// Store the raw register JSON for a stream (called during REGISTER command processing).
    pub fn store_raw_register_json(&mut self, name: &str, json: serde_json::Value) {
        self.raw_register_jsons.insert(name.to_string(), json);
    }

    /// Get the raw register JSON for a stream. Returns None if not found.
    pub fn get_raw_register_json(&self, name: &str) -> Option<&serde_json::Value> {
        self.raw_register_jsons.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn make_tx_stream() -> StreamDefinition {
        StreamDefinition {
            name: "Transactions".into(),
            key_field: "user_id".into(),
            features: vec![
                ("tx_count_1h".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                }),
                ("tx_sum_1h".into(), FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                }),
                ("avg_amount_1h".into(), FeatureDef::Avg {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                }),
            ],
        }
    }

    #[test]
    fn test_register_stream() {
        let mut engine = PipelineEngine::new();
        let stream = make_tx_stream();
        engine.register(stream).unwrap();
        assert_eq!(engine.stream_count(), 1);
        assert!(engine.get_stream("Transactions").is_some());
    }

    #[test]
    fn test_register_empty_name_rejected() {
        let mut engine = PipelineEngine::new();
        let stream = StreamDefinition {
            name: "".into(),
            key_field: "user_id".into(),
            features: vec![],
        };
        assert!(engine.register(stream).is_err());
    }

    #[test]
    fn test_push_updates_all_operators() {
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        engine.register(make_tx_stream()).unwrap();

        let now = ts(60_000);
        let event = serde_json::json!({
            "user_id": "u123",
            "amount": 50.0
        });

        let features = engine.push("Transactions", &event, &mut store, now).unwrap();
        assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
        assert_eq!(features.get("tx_sum_1h"), Some(&FeatureValue::Float(50.0)));
        assert_eq!(features.get("avg_amount_1h"), Some(&FeatureValue::Float(50.0)));
    }

    #[test]
    fn test_push_missing_key_field_returns_error() {
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        engine.register(make_tx_stream()).unwrap();

        let event = serde_json::json!({"amount": 50.0});
        let result = engine.push("Transactions", &event, &mut store, ts(60_000));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_empty_key_rejected() {
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        engine.register(make_tx_stream()).unwrap();

        let event = serde_json::json!({"user_id": "", "amount": 50.0});
        let result = engine.push("Transactions", &event, &mut store, ts(60_000));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_unknown_stream_returns_error() {
        let engine = PipelineEngine::new();
        let mut store = StateStore::new();
        let event = serde_json::json!({"user_id": "u123"});
        let result = engine.push("NonExistent", &event, &mut store, ts(60_000));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_3_events_verify_aggregates() {
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        engine.register(make_tx_stream()).unwrap();

        let now = ts(60_000);
        for amount in [10.0, 20.0, 30.0] {
            let event = serde_json::json!({
                "user_id": "u123",
                "amount": amount
            });
            engine.push("Transactions", &event, &mut store, now).unwrap();
        }

        let features = store.get_all_features("u123", now);
        assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(3)));
        assert_eq!(features.get("tx_sum_1h"), Some(&FeatureValue::Float(60.0)));
        assert_eq!(features.get("avg_amount_1h"), Some(&FeatureValue::Float(20.0)));
    }

    // ======================== max_window_duration Tests ========================

    #[test]
    fn test_max_window_duration() {
        let mut engine = PipelineEngine::new();
        engine.register(StreamDefinition {
            name: "stream1".into(),
            key_field: "id".into(),
            features: vec![
                ("c1".into(), FeatureDef::Count {
                    window: Duration::from_secs(1800), // 30m
                    bucket: Duration::from_secs(60),
                }),
                ("s1".into(), FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600), // 1h -- largest
                    bucket: Duration::from_secs(60),
                    optional: false,
                }),
            ],
        }).unwrap();
        engine.register(StreamDefinition {
            name: "stream2".into(),
            key_field: "id".into(),
            features: vec![
                ("c2".into(), FeatureDef::Count {
                    window: Duration::from_secs(900), // 15m
                    bucket: Duration::from_secs(60),
                }),
            ],
        }).unwrap();
        assert_eq!(engine.max_window_duration(), Duration::from_secs(3600));
    }

    #[test]
    fn test_max_window_duration_no_streams() {
        let engine = PipelineEngine::new();
        assert_eq!(engine.max_window_duration(), Duration::ZERO);
    }

    #[test]
    fn test_max_window_duration_derive_only_returns_zero() {
        let mut engine = PipelineEngine::new();
        engine.register(StreamDefinition {
            name: "derived".into(),
            key_field: "id".into(),
            features: vec![
                ("ratio".into(), FeatureDef::Derive {
                    expr: crate::engine::expression::parse_expr("1 + 1").unwrap(),
                }),
            ],
        }).unwrap();
        assert_eq!(engine.max_window_duration(), Duration::ZERO);
    }

    // ======================== list_streams / remove_stream Tests ========================

    #[test]
    fn test_list_streams() {
        let mut engine = PipelineEngine::new();
        engine.register(make_tx_stream()).unwrap();
        let streams: Vec<_> = engine.list_streams().collect();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].name, "Transactions");
    }

    #[test]
    fn test_remove_stream() {
        let mut engine = PipelineEngine::new();
        engine.register(make_tx_stream()).unwrap();
        assert_eq!(engine.stream_count(), 1);
        assert!(engine.remove_stream("Transactions"));
        assert_eq!(engine.stream_count(), 0);
        assert!(!engine.remove_stream("Transactions")); // Already removed
    }

    // ======================== raw_register_json Tests ========================

    #[test]
    fn test_get_raw_register_json_returns_some_for_registered() {
        let mut engine = PipelineEngine::new();
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [{"name": "tx_count_1h", "type": "count", "window": "1h"}]
        });
        engine.store_raw_register_json("Transactions", json.clone());
        let result = engine.get_raw_register_json("Transactions");
        assert!(result.is_some());
        assert_eq!(result.unwrap(), &json);
    }

    #[test]
    fn test_get_raw_register_json_returns_none_for_unknown() {
        let engine = PipelineEngine::new();
        assert!(engine.get_raw_register_json("NonExistent").is_none());
    }

    #[test]
    fn test_remove_stream_also_removes_raw_json() {
        let mut engine = PipelineEngine::new();
        engine.register(make_tx_stream()).unwrap();
        engine.store_raw_register_json("Transactions", serde_json::json!({"test": true}));
        assert!(engine.get_raw_register_json("Transactions").is_some());
        engine.remove_stream("Transactions");
        assert!(engine.get_raw_register_json("Transactions").is_none());
    }
}
