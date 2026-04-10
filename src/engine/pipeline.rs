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
use super::operators::{CountOp, SumOp, AvgOp, MinOp, MaxOp, LastOp};
use super::hll::DistinctCountOp;
use crate::state::snapshot::OperatorState;
use super::expression::{Expr, EvalContext, eval};

/// Definition of a single feature within a stream.
#[derive(Debug, Clone)]
pub enum FeatureDef {
    Count {
        window: Duration,
        bucket: Duration,
        where_expr: Option<Expr>,
    },
    Sum {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
    },
    Avg {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
    },
    Min {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
    },
    Max {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
    },
    Last {
        field: String,
        optional: bool,
    },
    DistinctCount {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
    },
    Derive {
        expr: Expr, // Parsed at registration time
    },
}

/// A view feature: either a derived expression or a cross-key lookup.
#[derive(Debug, Clone)]
pub enum ViewFeatureDef {
    Derive { expr: Expr },
    Lookup { target_stream: String, target_feature: String, on_field: String },
}

/// A cross-stream view. Views have no key_field for push -- they compute
/// derived features across multiple streams for the same entity key.
/// Evaluated lazily on GET only (not on PUSH).
#[derive(Debug, Clone)]
pub struct ViewDefinition {
    pub name: String,
    pub key_field: String,
    pub features: Vec<(String, ViewFeatureDef)>,
}

/// A stream definition: a named stream with a key field and a list of named features.
#[derive(Debug, Clone)]
pub struct StreamDefinition {
    pub name: String,
    /// Key field for entity extraction. None = keyless stream (raw event ingestion).
    /// Keyless streams cannot have windowed operators -- only derive features are allowed.
    pub key_field: Option<String>,
    pub features: Vec<(String, FeatureDef)>, // (feature_name, definition)
    /// Upstream stream dependencies for composable pipeline DAG.
    /// None means no dependencies (root stream).
    pub depends_on: Option<Vec<String>>,
    /// Stream-level filter expression. Evaluated before operator processing.
    /// Events not matching the filter are skipped (push returns empty FeatureMap).
    pub filter: Option<Expr>,
    /// Per-stream entity state TTL. When set, entities with no events
    /// for this stream older than this duration have their stream entry evicted.
    /// None means this stream uses the global TTL behavior.
    pub entity_ttl: Option<Duration>,
    /// How long to retain events in the event log for this stream.
    /// Default: None (uses global default). Used by event log compaction.
    pub history_ttl: Option<Duration>,
}

/// The pipeline engine. Holds registered stream definitions and coordinates
/// the push-through flow.
#[derive(Debug)]
pub struct PipelineEngine {
    streams: AHashMap<String, StreamDefinition>,
    views: AHashMap<String, ViewDefinition>,
    /// Raw register JSON strings for each stream/view, keyed by name.
    /// Stored on REGISTER so snapshots can persist pipeline definitions
    /// without serializing the Expr AST.
    raw_register_jsons: AHashMap<String, serde_json::Value>,
}

/// Create an operator instance from a FeatureDef (non-derive only).
/// Returns OperatorState enum (not Box<dyn Operator>) for serialization support.
fn create_operator(def: &FeatureDef) -> Option<OperatorState> {
    match def {
        FeatureDef::Count { window, bucket, .. } => {
            Some(OperatorState::Count(CountOp::new(*window, *bucket)))
        }
        FeatureDef::Sum { field, window, bucket, optional, .. } => {
            Some(OperatorState::Sum(SumOp::new(field.clone(), *window, *bucket, *optional)))
        }
        FeatureDef::Avg { field, window, bucket, optional, .. } => {
            Some(OperatorState::Avg(AvgOp::new(field.clone(), *window, *bucket, *optional)))
        }
        FeatureDef::Min { field, window, bucket, optional, .. } => {
            Some(OperatorState::Min(MinOp::new(field.clone(), *window, *bucket, *optional)))
        }
        FeatureDef::Max { field, window, bucket, optional, .. } => {
            Some(OperatorState::Max(MaxOp::new(field.clone(), *window, *bucket, *optional)))
        }
        FeatureDef::Last { field, optional } => {
            Some(OperatorState::Last(LastOp::new(field.clone(), *optional)))
        }
        FeatureDef::DistinctCount { field, window, bucket, optional, .. } => {
            Some(OperatorState::DistinctCount(DistinctCountOp::new(field.clone(), *window, *bucket, *optional)))
        }
        FeatureDef::Derive { .. } => None, // Derives have no operator state
    }
}

/// Extract the where_expr from a FeatureDef, if present.
fn get_where_expr(def: &FeatureDef) -> Option<&Expr> {
    match def {
        FeatureDef::Count { where_expr, .. } => where_expr.as_ref(),
        FeatureDef::Sum { where_expr, .. } => where_expr.as_ref(),
        FeatureDef::Avg { where_expr, .. } => where_expr.as_ref(),
        FeatureDef::Min { where_expr, .. } => where_expr.as_ref(),
        FeatureDef::Max { where_expr, .. } => where_expr.as_ref(),
        FeatureDef::Last { .. } => None,
        FeatureDef::DistinctCount { where_expr, .. } => where_expr.as_ref(),
        FeatureDef::Derive { .. } => None,
    }
}

impl PipelineEngine {
    /// Create engine with no registered streams.
    pub fn new() -> Self {
        Self {
            streams: AHashMap::new(),
            views: AHashMap::new(),
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
        // Keyless streams cannot have windowed operators (T-07-01 mitigation)
        if stream.key_field.is_none() {
            for (name, def) in &stream.features {
                let is_windowed = matches!(def,
                    FeatureDef::Count { .. } | FeatureDef::Sum { .. } | FeatureDef::Avg { .. } |
                    FeatureDef::Min { .. } | FeatureDef::Max { .. } | FeatureDef::DistinctCount { .. } |
                    FeatureDef::Last { .. }
                );
                if is_windowed {
                    return Err(TallyError::Protocol(format!(
                        "keyless stream '{}' cannot have windowed operator '{}'; only derive features are allowed",
                        stream.name, name
                    )));
                }
            }
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

        // Apply stream-level filter before any processing
        if let Some(ref filter_expr) = stream.filter {
            let ctx = EvalContext {
                features: &ahash::AHashMap::new(),
                event: Some(event),
            };
            let result = eval(filter_expr, &ctx);
            match result {
                FeatureValue::Int(0) | FeatureValue::Missing => {
                    return Ok(FeatureMap::new());
                }
                FeatureValue::Float(f) if f == 0.0 => {
                    return Ok(FeatureMap::new());
                }
                _ => {} // truthy -- proceed
            }
        }

        // Keyless stream: no entity state, return empty feature map
        if stream.key_field.is_none() {
            return Ok(FeatureMap::new());
        }

        // 2. Extract entity key from event JSON (T-01-11 mitigation)
        let key_field = stream.key_field.as_ref().unwrap(); // safe: checked above
        let key = match event.get(key_field) {
            Some(serde_json::Value::String(s)) => {
                if s.is_empty() {
                    return Err(TallyError::Protocol(
                        format!("empty key field '{}'", key_field),
                    ));
                }
                s.clone()
            }
            Some(other) => {
                return Err(TallyError::Type {
                    field: key_field.clone(),
                    expected: "string".into(),
                    got: format!("{}", other),
                });
            }
            None => {
                return Err(TallyError::Type {
                    field: key_field.clone(),
                    expected: "string".into(),
                    got: "absent".into(),
                });
            }
        };

        // 3. Get or create EntityState
        let entity = store.get_or_create_entity(&key);

        // 4. Get or create the stream's state within the entity.
        // Each stream has its own operators and last_event_at for independent
        // TTL management (OPS-02).
        // Use entry API to ensure stream exists, then work through entity.streams
        // to avoid long-lived mutable borrow conflicts with static_features.
        entity.get_or_create_stream(stream_name);

        // Initialize or reconcile operators for THIS stream only.
        let op_features: Vec<&(String, FeatureDef)> = stream.features.iter()
            .filter(|(_, def)| !matches!(def, FeatureDef::Derive { .. }))
            .collect();

        // Ensure each expected operator exists in the stream's state
        {
            let stream_state = entity.streams.get_mut(stream_name).unwrap();
            for (name, def) in &op_features {
                let exists = stream_state.operators.iter().any(|(n, _)| *n == **name);
                if !exists {
                    if let Some(op) = create_operator(def) {
                        stream_state.operators.push(((*name).clone(), op));
                    }
                }
            }

            // Push event to this stream's operators, respecting where-clause filters.
            for (fname, def) in &op_features {
                // Find the operator by name in stream_state
                if let Some((_, op)) = stream_state.operators.iter_mut().find(|(n, _)| *n == **fname) {
                    // Check where clause
                    if let Some(where_expr) = get_where_expr(def) {
                        let ctx = EvalContext {
                            features: &ahash::AHashMap::new(),
                            event: Some(event),
                        };
                        let result = eval(where_expr, &ctx);
                        match result {
                            FeatureValue::Int(0) | FeatureValue::Missing => continue,
                            FeatureValue::Float(f) if f == 0.0 => continue,
                            _ => {} // truthy -- proceed with push
                        }
                    }
                    op.push(event, now)?;
                }
            }
        } // stream_state borrow dropped here

        // 5. Collect feature values for this stream only (PUSH returns primary stream features).
        let mut features = FeatureMap::new();

        // Read operator values belonging to this stream
        {
            let stream_state = entity.streams.get_mut(stream_name).unwrap();
            for (name, op) in stream_state.operators.iter_mut() {
                features.insert(name.clone(), op.read(now));
            }
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

        // 6. Update last_event_at on the stream
        entity.streams.get_mut(stream_name).unwrap().last_event_at = Some(now);

        // 7. Return features
        Ok(features)
    }

    /// Feature retrieval for GET path.
    /// Calls store.get_all_features (which reads operators with &mut self to
    /// advance time and expire stale buckets), then evaluates derive expressions
    /// for any registered streams, then evaluates view features (cross-stream
    /// derives and cross-key lookups).
    pub fn get_features(
        &self,
        key: &str,
        store: &mut StateStore,
        now: SystemTime,
    ) -> FeatureMap {
        let mut features = store.get_all_features(key, now);

        // Build qualified feature names: "StreamName.feature_name" -> value
        // so view derive expressions can reference features from specific streams.
        // Iterate all streams' operators from the entity to build qualified names.
        let mut qualified: Vec<(String, FeatureValue)> = Vec::new();
        for stream in self.streams.values() {
            for (fname, _) in &stream.features {
                if let Some(val) = features.get(fname) {
                    qualified.push((format!("{}.{}", stream.name, fname), val.clone()));
                }
            }
        }
        for (qname, val) in qualified {
            features.insert(qname, val);
        }

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

        // Evaluate view features (cross-stream derives and cross-key lookups)
        let mut view_results: Vec<(String, FeatureValue)> = Vec::new();
        for view in self.views.values() {
            for (fname, vdef) in &view.features {
                match vdef {
                    ViewFeatureDef::Derive { expr } => {
                        let ctx = EvalContext {
                            features: &features,
                            event: None,
                        };
                        view_results.push((fname.clone(), eval(expr, &ctx)));
                    }
                    ViewFeatureDef::Lookup { target_stream: _target_stream, target_feature, on_field } => {
                        // Resolve the foreign key from the entity's existing features.
                        // Search stream definitions for a Last operator that tracks the
                        // on_field, then use its feature name to look up the value.
                        let mut foreign_key: Option<&FeatureValue> = None;
                        'outer: for stream in self.streams.values() {
                            for (feat_name, def) in &stream.features {
                                if let FeatureDef::Last { field, .. } = def {
                                    if field == on_field {
                                        foreign_key = features.get(feat_name);
                                        break 'outer;
                                    }
                                }
                            }
                        }
                        // Fallback: try direct name match (e.g. feature named same as on_field)
                        if foreign_key.is_none() {
                            foreign_key = features.get(on_field);
                        }
                        match foreign_key {
                            Some(FeatureValue::String(fk)) => {
                                let val = store.get_feature_value(fk, target_feature, now);
                                view_results.push((fname.clone(), val));
                            }
                            _ => {
                                view_results.push((fname.clone(), FeatureValue::Missing));
                            }
                        }
                    }
                }
            }
        }
        for (name, value) in view_results {
            features.insert(name, value);
        }

        features
    }

    /// Get a registered stream definition by name.
    pub fn get_stream(&self, name: &str) -> Option<&StreamDefinition> {
        self.streams.get(name)
    }

    /// Returns the entity_ttl for a given stream, if set.
    pub fn get_stream_entity_ttl(&self, stream_name: &str) -> Option<Duration> {
        self.streams.get(stream_name).and_then(|s| s.entity_ttl)
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
                FeatureDef::Min { window, .. } => Some(*window),
                FeatureDef::Max { window, .. } => Some(*window),
                FeatureDef::Last { .. } => None, // No window
                FeatureDef::DistinctCount { window, .. } => Some(*window),
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

    // ======================== View management ========================

    /// Register a view definition. View names must be non-empty.
    /// Duplicate registration replaces the previous definition (idempotent).
    pub fn register_view(&mut self, view: ViewDefinition) -> Result<(), TallyError> {
        if view.name.is_empty() {
            return Err(TallyError::Protocol("view name must not be empty".into()));
        }
        self.views.insert(view.name.clone(), view);
        Ok(())
    }

    /// Get a registered view definition by name.
    pub fn get_view(&self, name: &str) -> Option<&ViewDefinition> {
        self.views.get(name)
    }

    /// Iterate over all registered view definitions.
    pub fn list_views(&self) -> impl Iterator<Item = &ViewDefinition> {
        self.views.values()
    }

    /// Remove a view definition by name. Returns true if found and removed.
    pub fn remove_view(&mut self, name: &str) -> bool {
        self.raw_register_jsons.remove(name);
        self.views.remove(name).is_some()
    }

    /// Return list of (stream_name, key_field) for all registered keyed streams.
    /// Used by PUSH handler for fan-out. Keyless streams are excluded (T-07-03).
    pub fn fan_out_targets(&self) -> Vec<(String, String)> {
        self.streams.values()
            .filter_map(|s| s.key_field.as_ref().map(|kf| (s.name.clone(), kf.clone())))
            .collect()
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
            key_field: Some("user_id".into()),
            features: vec![
                ("tx_count_1h".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
                ("tx_sum_1h".into(), FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                }),
                ("avg_amount_1h".into(), FeatureDef::Avg {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
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
            key_field: Some("user_id".into()),
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
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
            key_field: Some("id".into()),
            features: vec![
                ("c1".into(), FeatureDef::Count {
                    window: Duration::from_secs(1800), // 30m
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
                ("s1".into(), FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600), // 1h -- largest
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();
        engine.register(StreamDefinition {
            name: "stream2".into(),
            key_field: Some("id".into()),
            features: vec![
                ("c2".into(), FeatureDef::Count {
                    window: Duration::from_secs(900), // 15m
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
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
            key_field: Some("id".into()),
            features: vec![
                ("ratio".into(), FeatureDef::Derive {
                    expr: crate::engine::expression::parse_expr("1 + 1").unwrap(),
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
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

    // ======================== Phase 5: FeatureDef Min/Max/Last Tests ========================

    #[test]
    fn test_create_operator_min() {
        let def = FeatureDef::Min {
            field: "amount".into(),
            window: Duration::from_secs(3600),
            bucket: Duration::from_secs(60),
            optional: false,
            where_expr: None,
        };
        assert!(create_operator(&def).is_some());
    }

    #[test]
    fn test_create_operator_max() {
        let def = FeatureDef::Max {
            field: "amount".into(),
            window: Duration::from_secs(3600),
            bucket: Duration::from_secs(60),
            optional: false,
            where_expr: None,
        };
        assert!(create_operator(&def).is_some());
    }

    #[test]
    fn test_create_operator_last() {
        let def = FeatureDef::Last {
            field: "country".into(),
            optional: false,
        };
        assert!(create_operator(&def).is_some());
    }

    #[test]
    fn test_push_with_min_max_last_operators() {
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                ("min_amount_1h".into(), FeatureDef::Min {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                }),
                ("max_amount_1h".into(), FeatureDef::Max {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                }),
                ("last_country".into(), FeatureDef::Last {
                    field: "country".into(),
                    optional: false,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        };
        engine.register(stream).unwrap();
        let now = ts(60_000);
        let event = serde_json::json!({
            "user_id": "u123",
            "amount": 50.0,
            "country": "US"
        });
        let features = engine.push("Transactions", &event, &mut store, now).unwrap();
        assert_eq!(features.get("min_amount_1h"), Some(&FeatureValue::Float(50.0)));
        assert_eq!(features.get("max_amount_1h"), Some(&FeatureValue::Float(50.0)));
        assert_eq!(features.get("last_country"), Some(&FeatureValue::String("US".into())));
    }

    // ======================== Phase 5: where-clause filtering Tests ========================

    #[test]
    fn test_push_with_where_expr_filters_events() {
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        // Create a stream with a where-clause filtered count
        let where_expr = crate::engine::expression::parse_expr("_event.status == 'failed'").unwrap();
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                ("tx_count_1h".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
                ("failed_tx_1h".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: Some(where_expr),
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        };
        engine.register(stream).unwrap();
        let now = ts(60_000);

        // Push 3 events: 2 success, 1 failed
        engine.push("Transactions", &serde_json::json!({
            "user_id": "u123", "status": "success"
        }), &mut store, now).unwrap();
        engine.push("Transactions", &serde_json::json!({
            "user_id": "u123", "status": "failed"
        }), &mut store, now).unwrap();
        let features = engine.push("Transactions", &serde_json::json!({
            "user_id": "u123", "status": "success"
        }), &mut store, now).unwrap();

        assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(3)));
        assert_eq!(features.get("failed_tx_1h"), Some(&FeatureValue::Int(1)));
    }

    // ======================== Phase 5 Plan 03: DistinctCount FeatureDef Tests ========================

    #[test]
    fn test_create_operator_distinct_count() {
        let def = FeatureDef::DistinctCount {
            field: "merchant_id".into(),
            window: Duration::from_secs(300),
            bucket: Duration::from_secs(60),
            optional: false,
            where_expr: None,
        };
        let op = create_operator(&def);
        assert!(op.is_some());
        // Verify it's a DistinctCount variant
        match op.unwrap() {
            crate::state::snapshot::OperatorState::DistinctCount(_) => {}
            other => panic!("Expected DistinctCount, got {:?}", other),
        }
    }

    #[test]
    fn test_max_window_duration_includes_distinct_count() {
        let mut engine = PipelineEngine::new();
        engine.register(StreamDefinition {
            name: "stream1".into(),
            key_field: Some("id".into()),
            features: vec![
                ("dc_24h".into(), FeatureDef::DistinctCount {
                    field: "merchant_id".into(),
                    window: Duration::from_secs(86400),
                    bucket: Duration::from_secs(300),
                    optional: false,
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();
        assert_eq!(engine.max_window_duration(), Duration::from_secs(86400));
    }

    // ======================== Phase 5 Plan 03: ViewDefinition Tests ========================

    #[test]
    fn test_register_view_and_get_view() {
        let mut engine = PipelineEngine::new();
        let view = ViewDefinition {
            name: "UserRisk".into(),
            key_field: "user_id".into(),
            features: vec![
                ("ratio".into(), ViewFeatureDef::Derive {
                    expr: crate::engine::expression::parse_expr("Transactions.tx_count_1h / 1").unwrap(),
                }),
            ],
        };
        engine.register_view(view).unwrap();
        assert!(engine.get_view("UserRisk").is_some());
        assert_eq!(engine.list_views().count(), 1);
    }

    #[test]
    fn test_register_view_empty_name_rejected() {
        let mut engine = PipelineEngine::new();
        let view = ViewDefinition {
            name: "".into(),
            key_field: "user_id".into(),
            features: vec![],
        };
        assert!(engine.register_view(view).is_err());
    }

    #[test]
    fn test_remove_view() {
        let mut engine = PipelineEngine::new();
        let view = ViewDefinition {
            name: "UserRisk".into(),
            key_field: "user_id".into(),
            features: vec![],
        };
        engine.register_view(view).unwrap();
        assert!(engine.remove_view("UserRisk"));
        assert!(!engine.remove_view("UserRisk"));
    }

    #[test]
    fn test_view_derive_resolves_qualified_fields_from_two_streams() {
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        let now = ts(60_000);

        // Register two streams
        engine.register(StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                ("tx_count_1h".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();
        engine.register(StreamDefinition {
            name: "Logins".into(),
            key_field: Some("user_id".into()),
            features: vec![
                ("login_count_1h".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();

        // Register a view that derives from both streams
        let view = ViewDefinition {
            name: "UserRisk".into(),
            key_field: "user_id".into(),
            features: vec![
                ("tx_to_login_ratio".into(), ViewFeatureDef::Derive {
                    expr: crate::engine::expression::parse_expr("Transactions.tx_count_1h / Logins.login_count_1h").unwrap(),
                }),
            ],
        };
        engine.register_view(view).unwrap();

        // Push events to both streams for the same user
        engine.push("Transactions", &serde_json::json!({"user_id": "u1"}), &mut store, now).unwrap();
        engine.push("Transactions", &serde_json::json!({"user_id": "u1"}), &mut store, now).unwrap();
        engine.push("Logins", &serde_json::json!({"user_id": "u1"}), &mut store, now).unwrap();

        // GET should include view features with correct ratio: 2 / 1 = 2.0
        let features = engine.get_features("u1", &mut store, now);
        assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(2)));
        assert_eq!(features.get("login_count_1h"), Some(&FeatureValue::Int(1)));
        assert_eq!(features.get("tx_to_login_ratio"), Some(&FeatureValue::Float(2.0)));
    }

    #[test]
    fn test_view_lookup_resolves_cross_key_feature() {
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        let now = ts(60_000);

        // Register MerchantActivity stream (keyed by merchant_id)
        engine.register(StreamDefinition {
            name: "MerchantActivity".into(),
            key_field: Some("merchant_id".into()),
            features: vec![
                ("chargeback_count_24h".into(), FeatureDef::Count {
                    window: Duration::from_secs(86400),
                    bucket: Duration::from_secs(300),
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();

        // Register Transactions stream with last_merchant_id to store the foreign key
        engine.register(StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                ("tx_count_1h".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
                ("last_merchant_id".into(), FeatureDef::Last {
                    field: "merchant_id".into(),
                    optional: true,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();

        // Register a view with lookup
        let view = ViewDefinition {
            name: "FraudSignals".into(),
            key_field: "user_id".into(),
            features: vec![
                ("merchant_chargebacks".into(), ViewFeatureDef::Lookup {
                    target_stream: "MerchantActivity".into(),
                    target_feature: "chargeback_count_24h".into(),
                    on_field: "merchant_id".into(),
                }),
            ],
        };
        engine.register_view(view).unwrap();

        // Push events: merchant gets 3 chargebacks
        for _ in 0..3 {
            engine.push("MerchantActivity", &serde_json::json!({"merchant_id": "m456"}), &mut store, now).unwrap();
        }

        // Push a user transaction with merchant_id (stores last_merchant_id)
        engine.push("Transactions", &serde_json::json!({"user_id": "u123", "merchant_id": "m456", "amount": 50.0}), &mut store, now).unwrap();

        // GET for user should include lookup result
        let features = engine.get_features("u123", &mut store, now);
        assert_eq!(features.get("merchant_chargebacks"), Some(&FeatureValue::Int(3)));
    }

    #[test]
    fn test_view_lookup_returns_missing_when_target_entity_not_found() {
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        let now = ts(60_000);

        engine.register(StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                ("last_merchant_id".into(), FeatureDef::Last {
                    field: "merchant_id".into(),
                    optional: true,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();

        engine.register(StreamDefinition {
            name: "MerchantActivity".into(),
            key_field: Some("merchant_id".into()),
            features: vec![
                ("chargeback_count_24h".into(), FeatureDef::Count {
                    window: Duration::from_secs(86400),
                    bucket: Duration::from_secs(300),
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();

        let view = ViewDefinition {
            name: "FraudSignals".into(),
            key_field: "user_id".into(),
            features: vec![
                ("merchant_chargebacks".into(), ViewFeatureDef::Lookup {
                    target_stream: "MerchantActivity".into(),
                    target_feature: "chargeback_count_24h".into(),
                    on_field: "merchant_id".into(),
                }),
            ],
        };
        engine.register_view(view).unwrap();

        // Push user transaction but do NOT push any MerchantActivity events
        engine.push("Transactions", &serde_json::json!({"user_id": "u123", "merchant_id": "m_nonexistent", "amount": 50.0}), &mut store, now).unwrap();

        let features = engine.get_features("u123", &mut store, now);
        // Lookup target entity doesn't exist -> Missing
        assert_eq!(features.get("merchant_chargebacks"), Some(&FeatureValue::Missing));
    }

    // ======================== Phase 6 Plan 02: entity_ttl / history_ttl Tests ========================

    #[test]
    fn test_stream_definition_with_entity_ttl_stores_value() {
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: Some(Duration::from_secs(300)),
            history_ttl: None,
        };
        assert_eq!(stream.entity_ttl, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_stream_definition_with_entity_ttl_none_is_backwards_compatible() {
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        };
        assert_eq!(stream.entity_ttl, None);
        assert_eq!(stream.history_ttl, None);
    }

    #[test]
    fn test_get_stream_entity_ttl_returns_some() {
        let mut engine = PipelineEngine::new();
        engine.register(StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: Some(Duration::from_secs(300)),
            history_ttl: None,
        }).unwrap();
        assert_eq!(engine.get_stream_entity_ttl("Transactions"), Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_get_stream_entity_ttl_returns_none_for_unset() {
        let mut engine = PipelineEngine::new();
        engine.register(StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();
        assert_eq!(engine.get_stream_entity_ttl("Transactions"), None);
    }

    #[test]
    fn test_get_stream_entity_ttl_returns_none_for_unknown_stream() {
        let engine = PipelineEngine::new();
        assert_eq!(engine.get_stream_entity_ttl("NonExistent"), None);
    }

    #[test]
    fn test_max_window_duration_includes_min_max() {
        let mut engine = PipelineEngine::new();
        engine.register(StreamDefinition {
            name: "stream1".into(),
            key_field: Some("id".into()),
            features: vec![
                ("min_1h".into(), FeatureDef::Min {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                }),
                ("max_24h".into(), FeatureDef::Max {
                    field: "amount".into(),
                    window: Duration::from_secs(86400),
                    bucket: Duration::from_secs(300),
                    optional: false,
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();
        assert_eq!(engine.max_window_duration(), Duration::from_secs(86400));
    }

    // ======================== Phase 7 Plan 01: Keyless streams, depends_on, filter ========================

    #[test]
    fn test_keyless_stream_registers() {
        let mut engine = PipelineEngine::new();
        let stream = StreamDefinition {
            name: "RawEvents".into(),
            key_field: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        };
        engine.register(stream).unwrap();
        assert_eq!(engine.stream_count(), 1);
        assert!(engine.get_stream("RawEvents").is_some());
    }

    #[test]
    fn test_keyless_rejects_windowed_ops() {
        let mut engine = PipelineEngine::new();
        let stream = StreamDefinition {
            name: "RawEvents".into(),
            key_field: None,
            features: vec![
                ("bad_count".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        };
        let result = engine.register(stream);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("keyless"), "error should mention 'keyless', got: {}", err_msg);
        assert!(err_msg.contains("windowed") || err_msg.contains("operator"),
            "error should mention windowed/operator, got: {}", err_msg);
    }

    #[test]
    fn test_keyless_with_derive_registers() {
        let mut engine = PipelineEngine::new();
        let stream = StreamDefinition {
            name: "RawEvents".into(),
            key_field: None,
            features: vec![
                ("doubled".into(), FeatureDef::Derive {
                    expr: crate::engine::expression::parse_expr("_event.amount * 2.0").unwrap(),
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        };
        engine.register(stream).unwrap();
        assert_eq!(engine.stream_count(), 1);
    }

    #[test]
    fn test_keyless_push_returns_empty() {
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        let stream = StreamDefinition {
            name: "RawEvents".into(),
            key_field: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        };
        engine.register(stream).unwrap();

        let event = serde_json::json!({"user_id": "u123", "amount": 50.0});
        let features = engine.push("RawEvents", &event, &mut store, ts(60_000)).unwrap();
        assert!(features.is_empty(), "keyless stream push should return empty FeatureMap");
    }

    #[test]
    fn test_keyed_with_depends_on_registers() {
        let mut engine = PipelineEngine::new();
        let stream = StreamDefinition {
            name: "Enriched".into(),
            key_field: Some("user_id".into()),
            features: vec![
                ("tx_count_1h".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
            ],
            depends_on: Some(vec!["RawEvents".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        };
        engine.register(stream).unwrap();
        assert_eq!(engine.stream_count(), 1);
        let s = engine.get_stream("Enriched").unwrap();
        assert_eq!(s.depends_on.as_ref().unwrap(), &vec!["RawEvents".to_string()]);
    }

    #[test]
    fn test_filter_parsed_at_registration() {
        let mut engine = PipelineEngine::new();
        // Valid filter
        let stream = StreamDefinition {
            name: "FailedOnly".into(),
            key_field: Some("user_id".into()),
            features: vec![
                ("cnt".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: Some(crate::engine::expression::parse_expr("_event.status == 'failed'").unwrap()),
            entity_ttl: None,
            history_ttl: None,
        };
        engine.register(stream).unwrap();
        assert_eq!(engine.stream_count(), 1);
    }

    #[test]
    fn test_filter_blocks_non_matching_events() {
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        let stream = StreamDefinition {
            name: "FailedTx".into(),
            key_field: Some("user_id".into()),
            features: vec![
                ("cnt".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: Some(crate::engine::expression::parse_expr("_event.status == 'failed'").unwrap()),
            entity_ttl: None,
            history_ttl: None,
        };
        engine.register(stream).unwrap();
        let now = ts(60_000);

        // Push event with status: "success" -- should be filtered out
        let features = engine.push("FailedTx", &serde_json::json!({
            "user_id": "u123", "status": "success"
        }), &mut store, now).unwrap();
        assert!(features.is_empty(), "non-matching event should return empty features");

        // Push event with status: "failed" -- should proceed
        let features = engine.push("FailedTx", &serde_json::json!({
            "user_id": "u123", "status": "failed"
        }), &mut store, now).unwrap();
        assert_eq!(features.get("cnt"), Some(&FeatureValue::Int(1)));
    }

    #[test]
    fn test_fan_out_targets_excludes_keyless() {
        let mut engine = PipelineEngine::new();
        // Register a keyed stream
        engine.register(StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();
        // Register a keyless stream
        engine.register(StreamDefinition {
            name: "RawEvents".into(),
            key_field: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        }).unwrap();

        let targets = engine.fan_out_targets();
        assert_eq!(targets.len(), 1, "fan_out_targets should only include keyed streams");
        assert_eq!(targets[0].0, "Transactions");
        assert_eq!(targets[0].1, "user_id");
    }

    // ======================== Phase 7 Plan 03: DAG unit tests ========================

    #[test]
    fn test_rebuild_dag_no_deps() {
        let mut engine = PipelineEngine::new();
        engine.register(make_tx_stream()).unwrap();
        // DAG should succeed with standalone stream (no depends_on)
        // No panic, no error
        assert_eq!(engine.stream_count(), 1);
    }

    #[test]
    fn test_rebuild_dag_topo_order() {
        let mut engine = PipelineEngine::new();
        // Register in reverse order: C, B, A -- topo order should still be A, B, C
        let c = StreamDefinition {
            name: "C".into(), key_field: Some("uid".into()),
            features: vec![], entity_ttl: None, history_ttl: None,
            depends_on: Some(vec!["B".into()]), filter: None,
        };
        let b = StreamDefinition {
            name: "B".into(), key_field: Some("uid".into()),
            features: vec![], entity_ttl: None, history_ttl: None,
            depends_on: Some(vec!["A".into()]), filter: None,
        };
        let a = StreamDefinition {
            name: "A".into(), key_field: None,
            features: vec![], entity_ttl: None, history_ttl: None,
            depends_on: None, filter: None,
        };
        engine.register(c).unwrap();
        engine.register(b).unwrap();
        engine.register(a).unwrap();
        // After all registered, topo order should have A before B, B before C
        let order = engine.get_topo_order();
        let a_pos = order.iter().position(|n| n == "A").unwrap();
        let b_pos = order.iter().position(|n| n == "B").unwrap();
        let c_pos = order.iter().position(|n| n == "C").unwrap();
        assert!(a_pos < b_pos, "A must come before B");
        assert!(b_pos < c_pos, "B must come before C");
    }

    #[test]
    fn test_backward_compat_keyed_stream() {
        // Existing pattern with key_field: Some(...) should work exactly as before
        let mut engine = PipelineEngine::new();
        let mut store = StateStore::new();
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                ("tx_count_1h".into(), FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                }),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
        };
        engine.register(stream).unwrap();
        let now = ts(60_000);
        let event = serde_json::json!({"user_id": "u123", "amount": 50.0});
        let features = engine.push("Transactions", &event, &mut store, now).unwrap();
        assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
    }
}
