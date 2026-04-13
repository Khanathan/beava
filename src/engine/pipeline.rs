//! Pipeline engine: stream definitions and push-through orchestration.
//!
//! PipelineEngine holds registered stream definitions and coordinates the
//! synchronous push-through flow: event -> extract key -> update operators
//! -> evaluate derives -> return feature map.

use super::expression::{eval, EvalContext, Expr};
use super::hll::DistinctCountOp;
use super::operators::{
    AvgOp, CountOp, EmaOp, ExactMaxOp, ExactMinOp, FirstOp, LagOp, LastNOp, LastOp, MaxOp, MinOp,
    PercentileOp, StddevOp, SumOp,
};
use crate::error::TallyError;
use crate::state::snapshot::OperatorState;
use crate::state::store::StateStore;
use crate::types::{FeatureMap, FeatureValue};
use ahash::{AHashMap, AHashSet};
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use std::time::{Duration, SystemTime};

/// Definition of a single feature within a stream.
#[derive(Debug, Clone)]
pub enum FeatureDef {
    Count {
        window: Duration,
        bucket: Duration,
        where_expr: Option<Expr>,
        backfill: bool,
    },
    Sum {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
        backfill: bool,
    },
    Avg {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
        backfill: bool,
    },
    Min {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
        backfill: bool,
    },
    Max {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
        backfill: bool,
    },
    Last {
        field: String,
        optional: bool,
        backfill: bool,
    },
    DistinctCount {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
        backfill: bool,
    },
    Stddev {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
        backfill: bool,
    },
    Percentile {
        field: String,
        quantile: f64,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
        backfill: bool,
    },
    Derive {
        expr: Expr, // Parsed at registration time
    },
    Lag {
        field: String,
        n: usize,
        optional: bool,
        backfill: bool,
    },
    Ema {
        field: String,
        half_life_secs: f64,
        optional: bool,
        backfill: bool,
    },
    LastN {
        field: String,
        n: usize,
        optional: bool,
        backfill: bool,
    },
    First {
        field: String,
        optional: bool,
        backfill: bool,
    },
    ExactMin {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
        backfill: bool,
    },
    ExactMax {
        field: String,
        window: Duration,
        bucket: Duration,
        optional: bool,
        where_expr: Option<Expr>,
        backfill: bool,
    },
}

/// Schema diff result from re-registering a stream.
/// Classifies features as added, removed, unchanged, or backfilling.
#[derive(Debug, Clone)]
pub struct SchemaDiff {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub unchanged: Vec<String>,
    pub backfilling: Vec<String>,
}

/// Check if two FeatureDef variants are the same operator type
/// (using std::mem::discriminant to compare enum variant identity).
fn same_operator_type(a: &FeatureDef, b: &FeatureDef) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

/// Extract the backfill flag from a FeatureDef. Returns false for Derive (no state).
pub fn get_backfill_flag(def: &FeatureDef) -> bool {
    match def {
        FeatureDef::Count { backfill, .. } => *backfill,
        FeatureDef::Sum { backfill, .. } => *backfill,
        FeatureDef::Avg { backfill, .. } => *backfill,
        FeatureDef::Min { backfill, .. } => *backfill,
        FeatureDef::Max { backfill, .. } => *backfill,
        FeatureDef::Last { backfill, .. } => *backfill,
        FeatureDef::DistinctCount { backfill, .. } => *backfill,
        FeatureDef::Stddev { backfill, .. } => *backfill,
        FeatureDef::Percentile { backfill, .. } => *backfill,
        FeatureDef::Derive { .. } => false,
        FeatureDef::Lag { backfill, .. } => *backfill,
        FeatureDef::Ema { backfill, .. } => *backfill,
        FeatureDef::LastN { backfill, .. } => *backfill,
        FeatureDef::First { backfill, .. } => *backfill,
        FeatureDef::ExactMin { backfill, .. } => *backfill,
        FeatureDef::ExactMax { backfill, .. } => *backfill,
    }
}

/// Compute the schema diff between old and new feature lists.
/// Returns error if a feature name exists in both but with a different operator type.
fn diff_features(
    old: &[(String, FeatureDef)],
    new: &[(String, FeatureDef)],
) -> Result<SchemaDiff, TallyError> {
    let old_map: AHashMap<&str, &FeatureDef> =
        old.iter().map(|(name, def)| (name.as_str(), def)).collect();
    let new_map: AHashMap<&str, &FeatureDef> =
        new.iter().map(|(name, def)| (name.as_str(), def)).collect();

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut unchanged = Vec::new();
    let mut backfilling = Vec::new();

    // Check features in new definition
    for (name, new_def) in &new_map {
        if let Some(old_def) = old_map.get(name) {
            // Feature exists in both -- check type compatibility
            if !same_operator_type(old_def, new_def) {
                return Err(TallyError::Protocol(format!(
                    "feature '{}' type changed: cannot change operator type on re-registration; remove and re-add with a new name",
                    name
                )));
            }
            unchanged.push(name.to_string());
        } else {
            // New feature
            added.push(name.to_string());
            if get_backfill_flag(new_def) {
                backfilling.push(name.to_string());
            }
        }
    }

    // Check features removed (in old but not new)
    for (name, _) in &old_map {
        if !new_map.contains_key(name) {
            removed.push(name.to_string());
        }
    }

    Ok(SchemaDiff {
        added,
        removed,
        unchanged,
        backfilling,
    })
}

/// A view feature: either a derived expression or a cross-key lookup.
#[derive(Debug, Clone)]
pub enum ViewFeatureDef {
    Derive {
        expr: Expr,
    },
    Lookup {
        target_stream: String,
        target_feature: String,
        on_field: String,
    },
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

/// Feature projection: filters the FeatureMap before returning from push/get.
/// Applied AFTER derives evaluate (so derives can reference any feature), but
/// BEFORE the response is sent to the client.
#[derive(Debug, Clone)]
pub enum Projection {
    /// Only return features whose names are in this set.
    Select(AHashSet<String>),
    /// Return all features EXCEPT those whose names are in this set.
    Drop(AHashSet<String>),
}

impl Projection {
    /// Filter `features` in-place according to this projection.
    pub fn apply(&self, features: &mut FeatureMap) {
        match self {
            Projection::Select(allowed) => {
                features.retain(|k, _| allowed.contains(k));
            }
            Projection::Drop(excluded) => {
                features.retain(|k, _| !excluded.contains(k));
            }
        }
    }
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
    /// Feature projection: filters the response FeatureMap after derive evaluation.
    pub projection: Option<Projection>,
    /// Whether this pipeline is ephemeral (schema-only, no runtime enforcement yet).
    pub ephemeral: Option<bool>,
    /// Pipeline-level TTL (schema-only, no runtime enforcement yet).
    pub pipeline_ttl: Option<Duration>,
    /// Maximum number of entity keys for this stream (schema-only, no runtime enforcement yet).
    pub max_keys: Option<u64>,
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
    // DAG for cascade execution (composable pipeline)
    dag: DiGraph<String, ()>,
    node_indices: AHashMap<String, NodeIndex>,
    topo_order: Vec<String>,
    /// Pre-computed: for each stream, which streams are directly downstream.
    downstream_map: AHashMap<String, Vec<String>>,
}

/// Create an operator instance from a FeatureDef (non-derive only).
/// Returns OperatorState enum (not Box<dyn Operator>) for serialization support.
fn create_operator(def: &FeatureDef) -> Option<OperatorState> {
    match def {
        FeatureDef::Count { window, bucket, .. } => {
            Some(OperatorState::Count(CountOp::new(*window, *bucket)))
        }
        FeatureDef::Sum {
            field,
            window,
            bucket,
            optional,
            ..
        } => Some(OperatorState::Sum(SumOp::new(
            field.clone(),
            *window,
            *bucket,
            *optional,
        ))),
        FeatureDef::Avg {
            field,
            window,
            bucket,
            optional,
            ..
        } => Some(OperatorState::Avg(AvgOp::new(
            field.clone(),
            *window,
            *bucket,
            *optional,
        ))),
        FeatureDef::Min {
            field,
            window,
            bucket,
            optional,
            ..
        } => Some(OperatorState::Min(MinOp::new(
            field.clone(),
            *window,
            *bucket,
            *optional,
        ))),
        FeatureDef::Max {
            field,
            window,
            bucket,
            optional,
            ..
        } => Some(OperatorState::Max(MaxOp::new(
            field.clone(),
            *window,
            *bucket,
            *optional,
        ))),
        FeatureDef::Last {
            field, optional, ..
        } => Some(OperatorState::Last(LastOp::new(field.clone(), *optional))),
        FeatureDef::DistinctCount {
            field,
            window,
            bucket,
            optional,
            ..
        } => Some(OperatorState::DistinctCount(DistinctCountOp::new(
            field.clone(),
            *window,
            *bucket,
            *optional,
        ))),
        FeatureDef::Stddev {
            field,
            window,
            bucket,
            optional,
            ..
        } => Some(OperatorState::Stddev(StddevOp::new(
            field.clone(),
            *window,
            *bucket,
            *optional,
        ))),
        FeatureDef::Percentile {
            field,
            quantile,
            window,
            bucket,
            optional,
            ..
        } => Some(OperatorState::Percentile(PercentileOp::new(
            field.clone(),
            *quantile,
            *window,
            *bucket,
            *optional,
        ))),
        FeatureDef::Derive { .. } => None, // Derives have no operator state
        FeatureDef::Lag {
            field, n, optional, ..
        } => Some(OperatorState::Lag(LagOp::new(field.clone(), *n, *optional))),
        FeatureDef::Ema {
            field,
            half_life_secs,
            optional,
            ..
        } => Some(OperatorState::Ema(EmaOp::new(
            field.clone(),
            *half_life_secs,
            *optional,
        ))),
        FeatureDef::LastN {
            field, n, optional, ..
        } => Some(OperatorState::LastN(LastNOp::new(
            field.clone(),
            *n,
            *optional,
        ))),
        FeatureDef::First {
            field, optional, ..
        } => Some(OperatorState::First(FirstOp::new(field.clone(), *optional))),
        FeatureDef::ExactMin {
            field,
            window,
            bucket,
            optional,
            ..
        } => Some(OperatorState::ExactMin(ExactMinOp::new(
            field.clone(),
            *window,
            *bucket,
            *optional,
        ))),
        FeatureDef::ExactMax {
            field,
            window,
            bucket,
            optional,
            ..
        } => Some(OperatorState::ExactMax(ExactMaxOp::new(
            field.clone(),
            *window,
            *bucket,
            *optional,
        ))),
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
        FeatureDef::Stddev { where_expr, .. } => where_expr.as_ref(),
        FeatureDef::Percentile { where_expr, .. } => where_expr.as_ref(),
        FeatureDef::Derive { .. } => None,
        FeatureDef::Lag { .. } => None,
        FeatureDef::Ema { .. } => None,
        FeatureDef::LastN { .. } => None,
        FeatureDef::First { .. } => None,
        FeatureDef::ExactMin { where_expr, .. } => where_expr.as_ref(),
        FeatureDef::ExactMax { where_expr, .. } => where_expr.as_ref(),
    }
}

impl Default for PipelineEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineEngine {
    /// Create engine with no registered streams.
    pub fn new() -> Self {
        Self {
            streams: AHashMap::new(),
            views: AHashMap::new(),
            raw_register_jsons: AHashMap::new(),
            dag: DiGraph::new(),
            node_indices: AHashMap::new(),
            topo_order: Vec::new(),
            downstream_map: AHashMap::new(),
        }
    }

    /// Register a stream definition. Validates derive expressions are parseable.
    /// Duplicate registration replaces the previous definition (idempotent).
    /// Returns a SchemaDiff describing what changed (added/removed/unchanged features).
    /// Stream names must be non-empty (T-01-14 mitigation).
    pub fn register(&mut self, stream: StreamDefinition) -> Result<SchemaDiff, TallyError> {
        if stream.name.is_empty() {
            return Err(TallyError::Protocol("stream name must not be empty".into()));
        }
        // Keyless streams cannot have windowed operators (T-07-01 mitigation)
        if stream.key_field.is_none() {
            for (name, def) in &stream.features {
                let is_windowed = matches!(
                    def,
                    FeatureDef::Count { .. }
                        | FeatureDef::Sum { .. }
                        | FeatureDef::Avg { .. }
                        | FeatureDef::Min { .. }
                        | FeatureDef::Max { .. }
                        | FeatureDef::DistinctCount { .. }
                        | FeatureDef::Last { .. }
                        | FeatureDef::Stddev { .. }
                        | FeatureDef::Percentile { .. }
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

        // Compute schema diff before replacing the definition
        let diff = if let Some(old_stream) = self.streams.get(&stream.name) {
            diff_features(&old_stream.features, &stream.features)?
        } else {
            // First registration: all features are "added"
            let added: Vec<String> = stream.features.iter().map(|(n, _)| n.clone()).collect();
            let backfilling: Vec<String> = stream
                .features
                .iter()
                .filter(|(_, def)| get_backfill_flag(def))
                .map(|(n, _)| n.clone())
                .collect();
            SchemaDiff {
                added,
                removed: Vec::new(),
                unchanged: Vec::new(),
                backfilling,
            }
        };

        let name_clone = stream.name.clone();
        self.streams.insert(name_clone.clone(), stream);
        // Rebuild DAG and validate (cycle detection)
        if let Err(e) = self.rebuild_dag() {
            // Registration failed due to cycle -- remove the stream we just added
            self.streams.remove(&name_clone);
            return Err(e);
        }
        Ok(diff)
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
        store: &StateStore,
        now: SystemTime,
    ) -> Result<FeatureMap, TallyError> {
        self.push_internal(stream_name, event, None, None, store, now, true)
    }

    /// Async-mode push: identical to `push` but skips the feature read + derive
    /// evaluation at the end. Returns an empty `FeatureMap`.
    ///
    /// Used by `handle_push_async` (OP_PUSH_ASYNC) where the caller discards
    /// the feature map anyway. Skipping the read loop avoids the O(m) cost of
    /// `DistinctCountOp::read` — the HLL read scans all 16384 registers across
    /// every bucket, which measured at ~300µs per HLL operator. On a pipeline
    /// with 3 HLLs (like `bench.py large`), skipping this block recovers
    /// ~140x throughput on the async hot path.
    pub fn push_no_features(
        &self,
        stream_name: &str,
        event: &serde_json::Value,
        store: &StateStore,
        now: SystemTime,
    ) -> Result<FeatureMap, TallyError> {
        self.push_internal(stream_name, event, None, None, store, now, false)
    }

    /// Batch primary-only push with no feature read.
    ///
    /// Iterates events under a **single** `get_stream` lookup and calls the
    /// existing `push_internal(_, _, _, _, false)` per event. This primitive
    /// does NOT perform cascade or fan-out — the caller is responsible for
    /// any cross-stream updates. For the async coalescing hot path
    /// (Phase 12), use [`push_batch_with_cascade_no_features`] instead.
    ///
    /// Returns a `Vec` of per-event `Result`s in **input order**. An error
    /// at index `i` does NOT halt the batch; subsequent events continue to
    /// apply their operator mutations.
    ///
    /// Empty slice → `Vec::new()`. Unknown stream name → a Vec of
    /// `Err(Protocol)` for every input event (no partial state mutation).
    pub fn push_batch_no_features(
        &self,
        stream_name: &str,
        events: &[&serde_json::Value],
        store: &StateStore,
        now: SystemTime,
    ) -> Vec<Result<FeatureMap, TallyError>> {
        if events.is_empty() {
            return Vec::new();
        }
        // Resolve the primary stream ONCE per call (D-07). If the stream is
        // unknown, surface an error for every input event without touching
        // state.
        if self.get_stream(stream_name).is_none() {
            return events
                .iter()
                .map(|_| {
                    Err(TallyError::Protocol(format!(
                        "unknown stream: {}",
                        stream_name
                    )))
                })
                .collect();
        }
        let mut out = Vec::with_capacity(events.len());
        for event in events {
            out.push(self.push_internal(stream_name, event, None, None, store, now, false));
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    fn push_internal(
        &self,
        stream_name: &str,
        event: &serde_json::Value,
        enrichment_json: Option<&ahash::AHashMap<String, serde_json::Value>>,
        enrichment_fv: Option<&ahash::AHashMap<String, FeatureValue>>,
        store: &StateStore,
        now: SystemTime,
        read_features: bool,
    ) -> Result<FeatureMap, TallyError> {
        // 1. Look up stream definition
        let stream = self
            .streams
            .get(stream_name)
            .ok_or_else(|| TallyError::Protocol(format!("unknown stream: {}", stream_name)))?;

        // Apply stream-level filter before any processing
        if let Some(ref filter_expr) = stream.filter {
            let ctx = EvalContext {
                features: &ahash::AHashMap::new(),
                event: Some(event),
                enrichment: enrichment_fv,
            };
            let result = eval(filter_expr, &ctx);
            match result {
                FeatureValue::Int(0) | FeatureValue::Missing => {
                    return Ok(FeatureMap::new());
                }
                FeatureValue::Float(0.0) => {
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
                    return Err(TallyError::Protocol(format!(
                        "empty key field '{}'",
                        key_field
                    )));
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
        let mut entity = store.get_or_create_entity(&key);

        // 4. Get or create the stream's state within the entity.
        // Each stream has its own operators and last_event_at for independent
        // TTL management (OPS-02).
        // Use entry API to ensure stream exists, then work through entity.streams
        // to avoid long-lived mutable borrow conflicts with static_features.
        entity.get_or_create_stream(stream_name);

        // Initialize or reconcile operators for THIS stream only.
        let op_features: Vec<&(String, FeatureDef)> = stream
            .features
            .iter()
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
                if let Some((_, op)) = stream_state
                    .operators
                    .iter_mut()
                    .find(|(n, _)| *n == **fname)
                {
                    // Check where clause
                    if let Some(where_expr) = get_where_expr(def) {
                        let ctx = EvalContext {
                            features: &ahash::AHashMap::new(),
                            event: Some(event),
                            enrichment: enrichment_fv,
                        };
                        let result = eval(where_expr, &ctx);
                        match result {
                            FeatureValue::Int(0) | FeatureValue::Missing => continue,
                            FeatureValue::Float(0.0) => continue,
                            _ => {} // truthy -- proceed with push
                        }
                    }
                    op.push(event, enrichment_json, now)?;
                }
            }
        } // stream_state borrow dropped here

        // 5. Collect feature values for this stream only (PUSH returns primary stream features).
        // PERF fast path: when called from the async push path (OP_PUSH_ASYNC),
        // `read_features` is false and we skip the entire read + derive block.
        // The HLL read alone can be ~300µs per operator, which dominates the
        // async hot path on large pipelines. Still update `last_event_at`.
        if !read_features {
            entity.streams.get_mut(stream_name).unwrap().last_event_at = Some(now);
            return Ok(FeatureMap::new());
        }

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
                enrichment: enrichment_fv,
            };
            stream
                .features
                .iter()
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

        // 7. Apply projection filter (after derives, before return)
        if let Some(ref proj) = stream.projection {
            proj.apply(&mut features);
        }

        // 8. Return features
        Ok(features)
    }

    /// Rebuild the DAG from all registered streams. Called after each registration.
    /// Detects circular dependencies via topological sort.
    fn rebuild_dag(&mut self) -> Result<(), TallyError> {
        let mut dag = DiGraph::new();
        let mut indices = AHashMap::new();

        // Add all streams as nodes
        for name in self.streams.keys() {
            let idx = dag.add_node(name.clone());
            indices.insert(name.clone(), idx);
        }

        // Add edges for depends_on relationships
        for stream in self.streams.values() {
            if let Some(ref deps) = stream.depends_on {
                let downstream_idx = indices[&stream.name];
                for dep in deps {
                    if let Some(&upstream_idx) = indices.get(dep) {
                        // Edge: upstream -> downstream (data flows this direction)
                        dag.add_edge(upstream_idx, downstream_idx, ());
                    }
                    // If dep not registered yet, skip -- deferred resolution
                }
            }
        }

        // Topological sort -- detects cycles
        let order = toposort(&dag, None).map_err(|cycle| {
            let node = &dag[cycle.node_id()];
            TallyError::Protocol(format!(
                "circular dependency detected involving stream '{}'",
                node
            ))
        })?;

        self.topo_order = order.iter().map(|idx| dag[*idx].clone()).collect();

        // Build downstream map: for each stream, which streams directly depend on it
        let mut downstream_map: AHashMap<String, Vec<String>> = AHashMap::new();
        for stream in self.streams.values() {
            if let Some(ref deps) = stream.depends_on {
                for dep in deps {
                    downstream_map
                        .entry(dep.clone())
                        .or_default()
                        .push(stream.name.clone());
                }
            }
        }

        self.dag = dag;
        self.node_indices = indices;
        self.downstream_map = downstream_map;
        Ok(())
    }

    /// Push event to a stream and cascade through all downstream streams
    /// in topological order. Returns features from the primary (origin) stream.
    pub fn push_with_cascade(
        &self,
        stream_name: &str,
        event: &serde_json::Value,
        store: &StateStore,
        now: SystemTime,
    ) -> Result<FeatureMap, TallyError> {
        self.push_with_cascade_internal(stream_name, event, store, now, true)
    }

    /// Async-mode cascade push: skips feature read + derive evaluation for
    /// primary AND cascade targets. Returns empty FeatureMap. See
    /// `push_no_features` for details on why this matters.
    pub fn push_with_cascade_no_features(
        &self,
        stream_name: &str,
        event: &serde_json::Value,
        store: &StateStore,
        now: SystemTime,
    ) -> Result<FeatureMap, TallyError> {
        self.push_with_cascade_internal(stream_name, event, store, now, false)
    }

    /// Batch cascade-aware push with no feature read. This is the hot-path
    /// primitive used by `handle_push_batch` (Phase 12) under the coalescer.
    ///
    /// Per-event semantics are **identical** to `push_with_cascade_no_features`:
    /// same filter eval, same key extraction, same operator mutation on both
    /// primary and cascade targets, same fan-out dispatch. Critically, this
    /// honors D-06 / D-07 of Phase 12's CONTEXT.md — no silent reduction of
    /// cascade scope and no silent drop of fan-out.
    ///
    /// Amortization commitments at the method boundary (D-07):
    ///   - `get_stream(stream_name)` is resolved ONCE per call; unknown stream
    ///     short-circuits into `Vec<Err(Protocol)>` for every input event.
    ///   - `fan_out_targets()` is resolved ONCE per call (the returned Vec is
    ///     not re-walked inside the per-event delegation today, but the
    ///     HashMap lookup + allocation happens only once at entry — leaving
    ///     headroom for a Wave 3 micro-refactor that inlines the loop body).
    ///
    /// The body delegates to the existing single-event
    /// `push_with_cascade_no_features` worker per event. The Phase 12 win at
    /// the caller (`handle_push_batch`) is that the AppState mutex is held
    /// exactly once for the whole batch — correctness first, fine-grained
    /// amortization second. If Wave 3 benches show that the per-event
    /// re-resolution of metadata inside the single-event worker dominates, a
    /// follow-up extraction of
    /// `push_with_cascade_no_features_inner(primary: &StreamDefinition, ...)`
    /// is the next optimization, but it is NOT required for correctness.
    ///
    /// Returns a `Vec` of per-event `Result<FeatureMap, TallyError>` in
    /// **input order** (the `FeatureMap` is always empty — `no_features`
    /// mode skips the read). An error at index `i` does NOT halt the batch.
    pub fn push_batch_with_cascade_no_features(
        &self,
        stream_name: &str,
        events: &[&serde_json::Value],
        store: &StateStore,
        now: SystemTime,
    ) -> Vec<Result<FeatureMap, TallyError>> {
        if events.is_empty() {
            return Vec::new();
        }

        // Resolve primary stream definition ONCE (D-07). Unknown primary
        // short-circuits with an error per input event; zero state mutation.
        if self.get_stream(stream_name).is_none() {
            return events
                .iter()
                .map(|_| {
                    Err(TallyError::Protocol(format!(
                        "unknown stream: {}",
                        stream_name
                    )))
                })
                .collect();
        }

        // Resolve fan-out targets ONCE (D-07) and compute the filtered list
        // of targets this primary should actually fan out to. The TCP
        // handler's per-event fan-out loop (src/server/tcp.rs:364+) skips:
        //   (a) the primary stream itself,
        //   (b) any target sharing the primary's key_field,
        //   (c) any target already reached through the cascade DAG.
        // We mirror that filter here so batch semantics match what
        // handle_push and handle_push_async do for a single event.
        let primary_key_field = self
            .get_stream(stream_name)
            .and_then(|s| s.key_field.clone());
        let cascade_targets = self.get_cascade_targets(stream_name);
        let fan_out_all = self.fan_out_targets();
        let fan_out: Vec<(String, String)> = fan_out_all
            .into_iter()
            .filter(|(target_name, target_key_field)| {
                if target_name == stream_name {
                    return false;
                }
                if primary_key_field.as_deref() == Some(target_key_field.as_str()) {
                    return false;
                }
                if cascade_targets.iter().any(|ct| ct == target_name) {
                    return false;
                }
                true
            })
            .collect();

        let mut out = Vec::with_capacity(events.len());
        for event in events {
            // 1. Primary + cascade via the existing single-event worker.
            //    Preserves depends_on DAG cascade semantics EXACTLY (D-06).
            let res = self.push_with_cascade_no_features(stream_name, event, store, now);

            // 2. Fan-out dispatch mirrors the TCP handler's per-event fan-out
            //    block. Each qualifying target receives exactly ONE push per
            //    event — matching v1.2 semantics for async pushes.
            if res.is_ok() {
                for (target_name, target_key_field) in &fan_out {
                    if let Some(serde_json::Value::String(key_val)) =
                        event.get(target_key_field.as_str())
                    {
                        if !key_val.is_empty() {
                            let _ = self.push_no_features(target_name, event, store, now);
                        }
                    }
                }
            }

            out.push(res);
        }
        out
    }

    fn push_with_cascade_internal(
        &self,
        stream_name: &str,
        event: &serde_json::Value,
        store: &StateStore,
        now: SystemTime,
        read_features: bool,
    ) -> Result<FeatureMap, TallyError> {
        // Determine if downstream cascade exists
        let has_downstream = self.downstream_map.contains_key(stream_name);

        if !has_downstream {
            // No cascade -- skip enrichment overhead entirely
            return self.push_internal(stream_name, event, None, None, store, now, read_features);
        }

        // Stack-local enrichment accumulators (C-5: never enter DashMap)
        let mut enrichment_json: AHashMap<String, serde_json::Value> = AHashMap::new();
        let mut enrichment_fv: AHashMap<String, FeatureValue> = AHashMap::new();

        // Primary push -- MUST read features when downstream exists (Pitfall 5)
        // even if outer caller requested read_features=false (async mode)
        let primary_features =
            self.push_internal(stream_name, event, None, None, store, now, true)?;

        // Populate enrichment from primary stream results
        for (name, value) in &primary_features {
            let qualified = format!("{}.{}", stream_name, name);
            enrichment_json.insert(qualified.clone(), value.to_json_value());
            enrichment_json.insert(name.clone(), value.to_json_value()); // unqualified (last-writer-wins)
            enrichment_fv.insert(qualified, value.clone());
            enrichment_fv.insert(name.clone(), value.clone()); // unqualified
        }

        // BFS to find all reachable downstream streams
        let mut visited = AHashSet::new();
        visited.insert(stream_name.to_string());
        let mut to_visit = Vec::new();

        if let Some(direct_downstream) = self.downstream_map.get(stream_name) {
            for ds in direct_downstream {
                if visited.insert(ds.clone()) {
                    to_visit.push(ds.clone());
                }
            }
        }

        let mut i = 0;
        while i < to_visit.len() {
            let current = to_visit[i].clone();
            if let Some(next_downstream) = self.downstream_map.get(&current) {
                for ds in next_downstream {
                    if visited.insert(ds.clone()) {
                        to_visit.push(ds.clone());
                    }
                }
            }
            i += 1;
        }

        // Execute in topological order with enrichment
        for stream_in_order in &self.topo_order {
            if !to_visit.contains(stream_in_order) {
                continue;
            }
            let downstream_def = match self.streams.get(stream_in_order) {
                Some(d) => d,
                None => continue,
            };

            // Check if this downstream stream has further downstream (for read_features decision)
            let has_further_downstream = self.downstream_map.contains_key(stream_in_order.as_str());
            // Must read features if: caller wants them, OR further downstream needs enrichment
            let ds_read_features = read_features || has_further_downstream;

            // For keyed downstream: check if key_field exists in event
            if let Some(ref key_field) = downstream_def.key_field {
                match event.get(key_field) {
                    Some(serde_json::Value::String(k)) if !k.is_empty() => {
                        let ds_features = self.push_internal(
                            stream_in_order,
                            event,
                            Some(&enrichment_json),
                            Some(&enrichment_fv),
                            store,
                            now,
                            ds_read_features,
                        )?;

                        // Accumulate this stream's results for further downstream
                        if has_further_downstream {
                            for (name, value) in &ds_features {
                                let qualified = format!("{}.{}", stream_in_order, name);
                                enrichment_json.insert(qualified.clone(), value.to_json_value());
                                enrichment_json.insert(name.clone(), value.to_json_value());
                                enrichment_fv.insert(qualified, value.clone());
                                enrichment_fv.insert(name.clone(), value.clone());
                            }
                        }
                    }
                    _ => continue, // Key missing -- skip (LEFT JOIN semantics)
                }
            } else {
                // Keyless downstream
                let ds_features = self.push_internal(
                    stream_in_order,
                    event,
                    Some(&enrichment_json),
                    Some(&enrichment_fv),
                    store,
                    now,
                    ds_read_features,
                )?;

                if has_further_downstream {
                    for (name, value) in &ds_features {
                        let qualified = format!("{}.{}", stream_in_order, name);
                        enrichment_json.insert(qualified.clone(), value.to_json_value());
                        enrichment_json.insert(name.clone(), value.to_json_value());
                        enrichment_fv.insert(qualified, value.clone());
                        enrichment_fv.insert(name.clone(), value.clone());
                    }
                }
            }
        }

        // Return primary features (or empty if read_features=false for outer caller)
        if read_features {
            Ok(primary_features)
        } else {
            Ok(FeatureMap::new())
        }
    }

    /// Push an event to only the specified backfill operators, using the provided
    /// event timestamp instead of wall clock. Used during backfill replay.
    /// Does NOT evaluate derives (they auto-resolve on read).
    /// Does NOT update last_event_at (backfill is not a "live" event).
    pub fn push_for_backfill(
        &self,
        stream_name: &str,
        event: &serde_json::Value,
        store: &StateStore,
        event_time: SystemTime,
        backfill_features: &[String],
    ) -> Result<(), TallyError> {
        let stream = self
            .streams
            .get(stream_name)
            .ok_or_else(|| TallyError::Protocol(format!("unknown stream: {}", stream_name)))?;

        // Apply stream-level filter (same as push)
        if let Some(ref filter_expr) = stream.filter {
            let ctx = EvalContext {
                features: &ahash::AHashMap::new(),
                event: Some(event),
                enrichment: None,
            };
            let result = eval(filter_expr, &ctx);
            match result {
                FeatureValue::Int(0) | FeatureValue::Missing => return Ok(()),
                FeatureValue::Float(0.0) => return Ok(()),
                _ => {}
            }
        }

        // Keyless stream: nothing to backfill
        if stream.key_field.is_none() {
            return Ok(());
        }

        // Extract key
        let key_field = stream.key_field.as_ref().unwrap();
        let key = match event.get(key_field) {
            Some(serde_json::Value::String(s)) if !s.is_empty() => s.clone(),
            _ => return Ok(()), // Skip events without valid key (defensive)
        };

        let mut entity = store.get_or_create_entity(&key);
        entity.get_or_create_stream(stream_name);

        // Only push to backfill operators
        let op_features: Vec<&(String, FeatureDef)> = stream
            .features
            .iter()
            .filter(|(name, def)| {
                !matches!(def, FeatureDef::Derive { .. }) && backfill_features.contains(name)
            })
            .collect();

        let stream_state = entity.streams.get_mut(stream_name).unwrap();

        // Ensure backfill operators exist
        for (name, def) in &op_features {
            let exists = stream_state.operators.iter().any(|(n, _)| *n == **name);
            if !exists {
                if let Some(op) = create_operator(def) {
                    stream_state.operators.push(((*name).clone(), op));
                }
            }
        }

        // Push with event_time (not wall clock)
        for (fname, def) in &op_features {
            if let Some((_, op)) = stream_state
                .operators
                .iter_mut()
                .find(|(n, _)| *n == **fname)
            {
                // Check where clause
                if let Some(where_expr) = get_where_expr(def) {
                    let ctx = EvalContext {
                        features: &ahash::AHashMap::new(),
                        event: Some(event),
                        enrichment: None,
                    };
                    let result = eval(where_expr, &ctx);
                    match result {
                        FeatureValue::Int(0) | FeatureValue::Missing => continue,
                        FeatureValue::Float(0.0) => continue,
                        _ => {}
                    }
                }
                let _ = op.push(event, None, event_time); // Use event timestamp!
            }
        }

        Ok(())
    }

    /// Return the current topological order (for testing/debugging).
    pub fn get_topo_order(&self) -> &[String] {
        &self.topo_order
    }

    /// Feature retrieval for GET path.
    /// Calls store.get_all_features (which reads operators with &mut self to
    /// advance time and expire stale buckets), then evaluates derive expressions
    /// for any registered streams, then evaluates view features (cross-stream
    /// derives and cross-key lookups).
    pub fn get_features(&self, key: &str, store: &StateStore, now: SystemTime) -> FeatureMap {
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
            enrichment: None,
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

        // Apply per-stream projections to filter response features
        // (after derives are evaluated -- they need all features)
        for stream in self.streams.values() {
            if let Some(ref proj) = stream.projection {
                proj.apply(&mut features);
            }
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
                            enrichment: None,
                        };
                        view_results.push((fname.clone(), eval(expr, &ctx)));
                    }
                    ViewFeatureDef::Lookup {
                        target_stream: _target_stream,
                        target_feature,
                        on_field,
                    } => {
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
        self.streams
            .values()
            .flat_map(|s| s.features.iter())
            .filter_map(|(_, def)| match def {
                FeatureDef::Count { window, .. } => Some(*window),
                FeatureDef::Sum { window, .. } => Some(*window),
                FeatureDef::Avg { window, .. } => Some(*window),
                FeatureDef::Min { window, .. } => Some(*window),
                FeatureDef::Max { window, .. } => Some(*window),
                FeatureDef::Last { .. } => None, // No window
                FeatureDef::DistinctCount { window, .. } => Some(*window),
                FeatureDef::Stddev { window, .. } => Some(*window),
                FeatureDef::Percentile { window, .. } => Some(*window),
                FeatureDef::Derive { .. } => None,
                FeatureDef::Lag { .. } => None, // No window (event-count-based)
                FeatureDef::Ema { .. } => None, // No window (decaying)
                FeatureDef::LastN { .. } => None, // No window (event-count-based)
                FeatureDef::First { .. } => None, // No window
                FeatureDef::ExactMin { window, .. } => Some(*window),
                FeatureDef::ExactMax { window, .. } => Some(*window),
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
        let removed = self.streams.remove(name).is_some();
        if removed {
            // Rebuild DAG after removal (cannot fail -- removing nodes cannot create cycles)
            let _ = self.rebuild_dag();
        }
        removed
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

    /// Build a map of stream_name -> Vec<feature_name> for all stateful (non-Derive)
    /// features in each registered stream. Used by clone_for_snapshot_with_gc to
    /// determine which operators are still valid.
    pub fn valid_features_map(&self) -> AHashMap<String, Vec<String>> {
        self.streams
            .iter()
            .map(|(name, def)| {
                let feature_names: Vec<String> = def
                    .features
                    .iter()
                    .filter(|(_, fd)| !matches!(fd, FeatureDef::Derive { .. }))
                    .map(|(n, _)| n.clone())
                    .collect();
                (name.clone(), feature_names)
            })
            .collect()
    }

    /// Return list of (stream_name, key_field) for all registered keyed streams.
    /// Used by PUSH handler for fan-out. Keyless streams are excluded (T-07-03).
    pub fn fan_out_targets(&self) -> Vec<(String, String)> {
        self.streams
            .values()
            .filter_map(|s| s.key_field.as_ref().map(|kf| (s.name.clone(), kf.clone())))
            .collect()
    }

    /// Return all streams downstream of the given stream (for event log and fan-out isolation).
    /// Uses BFS through the downstream_map to find all reachable streams.
    pub fn get_cascade_targets(&self, stream_name: &str) -> Vec<String> {
        let mut targets = Vec::new();
        let mut visited = AHashSet::new();
        let mut queue = Vec::new();
        if let Some(direct) = self.downstream_map.get(stream_name) {
            queue.extend(direct.iter().cloned());
        }
        while let Some(current) = queue.pop() {
            if visited.insert(current.clone()) {
                targets.push(current.clone());
                if let Some(next) = self.downstream_map.get(&current) {
                    queue.extend(next.iter().cloned());
                }
            }
        }
        targets
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
                (
                    "tx_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "tx_sum_1h".into(),
                    FeatureDef::Sum {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "avg_amount_1h".into(),
                    FeatureDef::Avg {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
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
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        assert!(engine.register(stream).is_err());
    }

    #[test]
    fn test_push_updates_all_operators() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        engine.register(make_tx_stream()).unwrap();

        let now = ts(60_000);
        let event = serde_json::json!({
            "user_id": "u123",
            "amount": 50.0
        });

        let features = engine
            .push("Transactions", &event, &store, now)
            .unwrap();
        assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
        assert_eq!(features.get("tx_sum_1h"), Some(&FeatureValue::Float(50.0)));
        assert_eq!(
            features.get("avg_amount_1h"),
            Some(&FeatureValue::Float(50.0))
        );
    }

    #[test]
    fn test_push_missing_key_field_returns_error() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        engine.register(make_tx_stream()).unwrap();

        let event = serde_json::json!({"amount": 50.0});
        let result = engine.push("Transactions", &event, &store, ts(60_000));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_empty_key_rejected() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        engine.register(make_tx_stream()).unwrap();

        let event = serde_json::json!({"user_id": "", "amount": 50.0});
        let result = engine.push("Transactions", &event, &store, ts(60_000));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_unknown_stream_returns_error() {
        let engine = PipelineEngine::new();
        let store = StateStore::new();
        let event = serde_json::json!({"user_id": "u123"});
        let result = engine.push("NonExistent", &event, &store, ts(60_000));
        assert!(result.is_err());
    }

    #[test]
    fn test_push_3_events_verify_aggregates() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        engine.register(make_tx_stream()).unwrap();

        let now = ts(60_000);
        for amount in [10.0, 20.0, 30.0] {
            let event = serde_json::json!({
                "user_id": "u123",
                "amount": amount
            });
            engine
                .push("Transactions", &event, &store, now)
                .unwrap();
        }

        let features = store.get_all_features("u123", now);
        assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(3)));
        assert_eq!(features.get("tx_sum_1h"), Some(&FeatureValue::Float(60.0)));
        assert_eq!(
            features.get("avg_amount_1h"),
            Some(&FeatureValue::Float(20.0))
        );
    }

    // ======================== max_window_duration Tests ========================

    #[test]
    fn test_max_window_duration() {
        let mut engine = PipelineEngine::new();
        engine
            .register(StreamDefinition {
                name: "stream1".into(),
                key_field: Some("id".into()),
                features: vec![
                    (
                        "c1".into(),
                        FeatureDef::Count {
                            window: Duration::from_secs(1800), // 30m
                            bucket: Duration::from_secs(60),
                            where_expr: None,
                            backfill: false,
                        },
                    ),
                    (
                        "s1".into(),
                        FeatureDef::Sum {
                            field: "amount".into(),
                            window: Duration::from_secs(3600), // 1h -- largest
                            bucket: Duration::from_secs(60),
                            optional: false,
                            where_expr: None,
                            backfill: false,
                        },
                    ),
                ],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();
        engine
            .register(StreamDefinition {
                name: "stream2".into(),
                key_field: Some("id".into()),
                features: vec![(
                    "c2".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(900), // 15m
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                )],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();
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
        engine
            .register(StreamDefinition {
                name: "derived".into(),
                key_field: Some("id".into()),
                features: vec![(
                    "ratio".into(),
                    FeatureDef::Derive {
                        expr: crate::engine::expression::parse_expr("1 + 1").unwrap(),
                    },
                )],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();
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
            backfill: false,
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
            backfill: false,
        };
        assert!(create_operator(&def).is_some());
    }

    #[test]
    fn test_create_operator_last() {
        let def = FeatureDef::Last {
            field: "country".into(),
            optional: false,
            backfill: false,
        };
        assert!(create_operator(&def).is_some());
    }

    #[test]
    fn test_push_with_min_max_last_operators() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                (
                    "min_amount_1h".into(),
                    FeatureDef::Min {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "max_amount_1h".into(),
                    FeatureDef::Max {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "last_country".into(),
                    FeatureDef::Last {
                        field: "country".into(),
                        optional: false,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream).unwrap();
        let now = ts(60_000);
        let event = serde_json::json!({
            "user_id": "u123",
            "amount": 50.0,
            "country": "US"
        });
        let features = engine
            .push("Transactions", &event, &store, now)
            .unwrap();
        assert_eq!(
            features.get("min_amount_1h"),
            Some(&FeatureValue::Float(50.0))
        );
        assert_eq!(
            features.get("max_amount_1h"),
            Some(&FeatureValue::Float(50.0))
        );
        assert_eq!(
            features.get("last_country"),
            Some(&FeatureValue::String("US".into()))
        );
    }

    // ======================== Phase 5: where-clause filtering Tests ========================

    #[test]
    fn test_push_with_where_expr_filters_events() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        // Create a stream with a where-clause filtered count
        let where_expr =
            crate::engine::expression::parse_expr("_event.status == 'failed'").unwrap();
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                (
                    "tx_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "failed_tx_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: Some(where_expr),
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream).unwrap();
        let now = ts(60_000);

        // Push 3 events: 2 success, 1 failed
        engine
            .push(
                "Transactions",
                &serde_json::json!({
                    "user_id": "u123", "status": "success"
                }),
                &store,
                now,
            )
            .unwrap();
        engine
            .push(
                "Transactions",
                &serde_json::json!({
                    "user_id": "u123", "status": "failed"
                }),
                &store,
                now,
            )
            .unwrap();
        let features = engine
            .push(
                "Transactions",
                &serde_json::json!({
                    "user_id": "u123", "status": "success"
                }),
                &store,
                now,
            )
            .unwrap();

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
            backfill: false,
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
        engine
            .register(StreamDefinition {
                name: "stream1".into(),
                key_field: Some("id".into()),
                features: vec![(
                    "dc_24h".into(),
                    FeatureDef::DistinctCount {
                        field: "merchant_id".into(),
                        window: Duration::from_secs(86400),
                        bucket: Duration::from_secs(300),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                )],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();
        assert_eq!(engine.max_window_duration(), Duration::from_secs(86400));
    }

    // ======================== Phase 5 Plan 03: ViewDefinition Tests ========================

    #[test]
    fn test_register_view_and_get_view() {
        let mut engine = PipelineEngine::new();
        let view = ViewDefinition {
            name: "UserRisk".into(),
            key_field: "user_id".into(),
            features: vec![(
                "ratio".into(),
                ViewFeatureDef::Derive {
                    expr: crate::engine::expression::parse_expr("Transactions.tx_count_1h / 1")
                        .unwrap(),
                },
            )],
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
        let store = StateStore::new();
        let now = ts(60_000);

        // Register two streams
        engine
            .register(StreamDefinition {
                name: "Transactions".into(),
                key_field: Some("user_id".into()),
                features: vec![(
                    "tx_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                )],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();
        engine
            .register(StreamDefinition {
                name: "Logins".into(),
                key_field: Some("user_id".into()),
                features: vec![(
                    "login_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                )],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();

        // Register a view that derives from both streams
        let view = ViewDefinition {
            name: "UserRisk".into(),
            key_field: "user_id".into(),
            features: vec![(
                "tx_to_login_ratio".into(),
                ViewFeatureDef::Derive {
                    expr: crate::engine::expression::parse_expr(
                        "Transactions.tx_count_1h / Logins.login_count_1h",
                    )
                    .unwrap(),
                },
            )],
        };
        engine.register_view(view).unwrap();

        // Push events to both streams for the same user
        engine
            .push(
                "Transactions",
                &serde_json::json!({"user_id": "u1"}),
                &store,
                now,
            )
            .unwrap();
        engine
            .push(
                "Transactions",
                &serde_json::json!({"user_id": "u1"}),
                &store,
                now,
            )
            .unwrap();
        engine
            .push(
                "Logins",
                &serde_json::json!({"user_id": "u1"}),
                &store,
                now,
            )
            .unwrap();

        // GET should include view features with correct ratio: 2 / 1 = 2.0
        let features = engine.get_features("u1", &store, now);
        assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(2)));
        assert_eq!(features.get("login_count_1h"), Some(&FeatureValue::Int(1)));
        assert_eq!(
            features.get("tx_to_login_ratio"),
            Some(&FeatureValue::Float(2.0))
        );
    }

    #[test]
    fn test_view_lookup_resolves_cross_key_feature() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        let now = ts(60_000);

        // Register MerchantActivity stream (keyed by merchant_id)
        engine
            .register(StreamDefinition {
                name: "MerchantActivity".into(),
                key_field: Some("merchant_id".into()),
                features: vec![(
                    "chargeback_count_24h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(86400),
                        bucket: Duration::from_secs(300),
                        where_expr: None,
                        backfill: false,
                    },
                )],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();

        // Register Transactions stream with last_merchant_id to store the foreign key
        engine
            .register(StreamDefinition {
                name: "Transactions".into(),
                key_field: Some("user_id".into()),
                features: vec![
                    (
                        "tx_count_1h".into(),
                        FeatureDef::Count {
                            window: Duration::from_secs(3600),
                            bucket: Duration::from_secs(60),
                            where_expr: None,
                            backfill: false,
                        },
                    ),
                    (
                        "last_merchant_id".into(),
                        FeatureDef::Last {
                            field: "merchant_id".into(),
                            optional: true,
                            backfill: false,
                        },
                    ),
                ],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();

        // Register a view with lookup
        let view = ViewDefinition {
            name: "FraudSignals".into(),
            key_field: "user_id".into(),
            features: vec![(
                "merchant_chargebacks".into(),
                ViewFeatureDef::Lookup {
                    target_stream: "MerchantActivity".into(),
                    target_feature: "chargeback_count_24h".into(),
                    on_field: "merchant_id".into(),
                },
            )],
        };
        engine.register_view(view).unwrap();

        // Push events: merchant gets 3 chargebacks
        for _ in 0..3 {
            engine
                .push(
                    "MerchantActivity",
                    &serde_json::json!({"merchant_id": "m456"}),
                    &store,
                    now,
                )
                .unwrap();
        }

        // Push a user transaction with merchant_id (stores last_merchant_id)
        engine
            .push(
                "Transactions",
                &serde_json::json!({"user_id": "u123", "merchant_id": "m456", "amount": 50.0}),
                &store,
                now,
            )
            .unwrap();

        // GET for user should include lookup result
        let features = engine.get_features("u123", &store, now);
        assert_eq!(
            features.get("merchant_chargebacks"),
            Some(&FeatureValue::Int(3))
        );
    }

    #[test]
    fn test_view_lookup_returns_missing_when_target_entity_not_found() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        let now = ts(60_000);

        engine
            .register(StreamDefinition {
                name: "Transactions".into(),
                key_field: Some("user_id".into()),
                features: vec![(
                    "last_merchant_id".into(),
                    FeatureDef::Last {
                        field: "merchant_id".into(),
                        optional: true,
                        backfill: false,
                    },
                )],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();

        engine
            .register(StreamDefinition {
                name: "MerchantActivity".into(),
                key_field: Some("merchant_id".into()),
                features: vec![(
                    "chargeback_count_24h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(86400),
                        bucket: Duration::from_secs(300),
                        where_expr: None,
                        backfill: false,
                    },
                )],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();

        let view = ViewDefinition {
            name: "FraudSignals".into(),
            key_field: "user_id".into(),
            features: vec![(
                "merchant_chargebacks".into(),
                ViewFeatureDef::Lookup {
                    target_stream: "MerchantActivity".into(),
                    target_feature: "chargeback_count_24h".into(),
                    on_field: "merchant_id".into(),
                },
            )],
        };
        engine.register_view(view).unwrap();

        // Push user transaction but do NOT push any MerchantActivity events
        engine.push("Transactions", &serde_json::json!({"user_id": "u123", "merchant_id": "m_nonexistent", "amount": 50.0}), &store, now).unwrap();

        let features = engine.get_features("u123", &store, now);
        // Lookup target entity doesn't exist -> Missing
        assert_eq!(
            features.get("merchant_chargebacks"),
            Some(&FeatureValue::Missing)
        );
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
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
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
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        assert_eq!(stream.entity_ttl, None);
        assert_eq!(stream.history_ttl, None);
    }

    #[test]
    fn test_get_stream_entity_ttl_returns_some() {
        let mut engine = PipelineEngine::new();
        engine
            .register(StreamDefinition {
                name: "Transactions".into(),
                key_field: Some("user_id".into()),
                features: vec![],
                depends_on: None,
                filter: None,
                entity_ttl: Some(Duration::from_secs(300)),
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();
        assert_eq!(
            engine.get_stream_entity_ttl("Transactions"),
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn test_get_stream_entity_ttl_returns_none_for_unset() {
        let mut engine = PipelineEngine::new();
        engine
            .register(StreamDefinition {
                name: "Transactions".into(),
                key_field: Some("user_id".into()),
                features: vec![],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();
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
        engine
            .register(StreamDefinition {
                name: "stream1".into(),
                key_field: Some("id".into()),
                features: vec![
                    (
                        "min_1h".into(),
                        FeatureDef::Min {
                            field: "amount".into(),
                            window: Duration::from_secs(3600),
                            bucket: Duration::from_secs(60),
                            optional: false,
                            where_expr: None,
                            backfill: false,
                        },
                    ),
                    (
                        "max_24h".into(),
                        FeatureDef::Max {
                            field: "amount".into(),
                            window: Duration::from_secs(86400),
                            bucket: Duration::from_secs(300),
                            optional: false,
                            where_expr: None,
                            backfill: false,
                        },
                    ),
                ],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();
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
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
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
            features: vec![(
                "bad_count".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        let result = engine.register(stream);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("keyless"),
            "error should mention 'keyless', got: {}",
            err_msg
        );
        assert!(
            err_msg.contains("windowed") || err_msg.contains("operator"),
            "error should mention windowed/operator, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_keyless_with_derive_registers() {
        let mut engine = PipelineEngine::new();
        let stream = StreamDefinition {
            name: "RawEvents".into(),
            key_field: None,
            features: vec![(
                "doubled".into(),
                FeatureDef::Derive {
                    expr: crate::engine::expression::parse_expr("_event.amount * 2.0").unwrap(),
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream).unwrap();
        assert_eq!(engine.stream_count(), 1);
    }

    #[test]
    fn test_keyless_push_returns_empty() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        let stream = StreamDefinition {
            name: "RawEvents".into(),
            key_field: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream).unwrap();

        let event = serde_json::json!({"user_id": "u123", "amount": 50.0});
        let features = engine
            .push("RawEvents", &event, &store, ts(60_000))
            .unwrap();
        assert!(
            features.is_empty(),
            "keyless stream push should return empty FeatureMap"
        );
    }

    #[test]
    fn test_keyed_with_depends_on_registers() {
        let mut engine = PipelineEngine::new();
        let stream = StreamDefinition {
            name: "Enriched".into(),
            key_field: Some("user_id".into()),
            features: vec![(
                "tx_count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: Some(vec!["RawEvents".into()]),
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream).unwrap();
        assert_eq!(engine.stream_count(), 1);
        let s = engine.get_stream("Enriched").unwrap();
        assert_eq!(
            s.depends_on.as_ref().unwrap(),
            &vec!["RawEvents".to_string()]
        );
    }

    #[test]
    fn test_filter_parsed_at_registration() {
        let mut engine = PipelineEngine::new();
        // Valid filter
        let stream = StreamDefinition {
            name: "FailedOnly".into(),
            key_field: Some("user_id".into()),
            features: vec![(
                "cnt".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: Some(
                crate::engine::expression::parse_expr("_event.status == 'failed'").unwrap(),
            ),
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream).unwrap();
        assert_eq!(engine.stream_count(), 1);
    }

    #[test]
    fn test_filter_blocks_non_matching_events() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        let stream = StreamDefinition {
            name: "FailedTx".into(),
            key_field: Some("user_id".into()),
            features: vec![(
                "cnt".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: Some(
                crate::engine::expression::parse_expr("_event.status == 'failed'").unwrap(),
            ),
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream).unwrap();
        let now = ts(60_000);

        // Push event with status: "success" -- should be filtered out
        let features = engine
            .push(
                "FailedTx",
                &serde_json::json!({
                    "user_id": "u123", "status": "success"
                }),
                &store,
                now,
            )
            .unwrap();
        assert!(
            features.is_empty(),
            "non-matching event should return empty features"
        );

        // Push event with status: "failed" -- should proceed
        let features = engine
            .push(
                "FailedTx",
                &serde_json::json!({
                    "user_id": "u123", "status": "failed"
                }),
                &store,
                now,
            )
            .unwrap();
        assert_eq!(features.get("cnt"), Some(&FeatureValue::Int(1)));
    }

    #[test]
    fn test_fan_out_targets_excludes_keyless() {
        let mut engine = PipelineEngine::new();
        // Register a keyed stream
        engine
            .register(StreamDefinition {
                name: "Transactions".into(),
                key_field: Some("user_id".into()),
                features: vec![],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();
        // Register a keyless stream
        engine
            .register(StreamDefinition {
                name: "RawEvents".into(),
                key_field: None,
                features: vec![],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();

        let targets = engine.fan_out_targets();
        assert_eq!(
            targets.len(),
            1,
            "fan_out_targets should only include keyed streams"
        );
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
            name: "C".into(),
            key_field: Some("uid".into()),
            features: vec![],
            entity_ttl: None,
            history_ttl: None,
            depends_on: Some(vec!["B".into()]),
            filter: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        let b = StreamDefinition {
            name: "B".into(),
            key_field: Some("uid".into()),
            features: vec![],
            entity_ttl: None,
            history_ttl: None,
            depends_on: Some(vec!["A".into()]),
            filter: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        let a = StreamDefinition {
            name: "A".into(),
            key_field: None,
            features: vec![],
            entity_ttl: None,
            history_ttl: None,
            depends_on: None,
            filter: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
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
        let store = StateStore::new();
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![(
                "tx_count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream).unwrap();
        let now = ts(60_000);
        let event = serde_json::json!({"user_id": "u123", "amount": 50.0});
        let features = engine
            .push("Transactions", &event, &store, now)
            .unwrap();
        assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(1)));
    }

    // ======================== Phase 8 Plan 01: Schema Diff Tests ========================

    #[test]
    fn test_schema_diff_add_feature() {
        let mut engine = PipelineEngine::new();
        let stream1 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![(
                "tx_count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        let diff1 = engine.register(stream1).unwrap();
        assert!(diff1.added.contains(&"tx_count_1h".to_string()));
        assert!(diff1.removed.is_empty());

        // Re-register with added feature
        let stream2 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                (
                    "tx_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "tx_sum_1h".into(),
                    FeatureDef::Sum {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        let diff2 = engine.register(stream2).unwrap();
        assert!(diff2.added.contains(&"tx_sum_1h".to_string()));
        assert!(diff2.unchanged.contains(&"tx_count_1h".to_string()));
        assert!(diff2.removed.is_empty());
    }

    #[test]
    fn test_schema_diff_remove_feature() {
        let mut engine = PipelineEngine::new();
        let stream1 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                (
                    "tx_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "tx_sum_1h".into(),
                    FeatureDef::Sum {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream1).unwrap();

        // Re-register with removed feature
        let stream2 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![(
                "tx_count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        let diff = engine.register(stream2).unwrap();
        assert!(diff.removed.contains(&"tx_sum_1h".to_string()));
        assert!(diff.unchanged.contains(&"tx_count_1h".to_string()));
        assert!(diff.added.is_empty());
    }

    #[test]
    fn test_schema_diff_type_change_rejected() {
        let mut engine = PipelineEngine::new();
        let stream1 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![(
                "f1".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream1).unwrap();

        // Re-register with different type for same name
        let stream2 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![(
                "f1".into(),
                FeatureDef::Sum {
                    field: "amount".into(),
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    optional: false,
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        let result = engine.register(stream2);
        assert!(result.is_err());
        let err = format!("{}", result.unwrap_err());
        assert!(
            err.contains("type changed"),
            "Error should contain 'type changed': {}",
            err
        );
    }

    #[test]
    fn test_schema_diff_first_registration() {
        let mut engine = PipelineEngine::new();
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                (
                    "tx_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "tx_sum_1h".into(),
                    FeatureDef::Sum {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        let diff = engine.register(stream).unwrap();
        assert_eq!(diff.added.len(), 2);
        assert!(diff.removed.is_empty());
        assert!(diff.unchanged.is_empty());
    }

    #[test]
    fn test_backfill_flag_parsed() {
        let def = FeatureDef::Count {
            window: Duration::from_secs(3600),
            bucket: Duration::from_secs(60),
            where_expr: None,
            backfill: true,
        };
        assert!(get_backfill_flag(&def));

        let def_false = FeatureDef::Count {
            window: Duration::from_secs(3600),
            bucket: Duration::from_secs(60),
            where_expr: None,
            backfill: false,
        };
        assert!(!get_backfill_flag(&def_false));

        // Derive should always return false
        let derive_def = FeatureDef::Derive {
            expr: crate::engine::expression::parse_expr("1 + 1").unwrap(),
        };
        assert!(!get_backfill_flag(&derive_def));
    }

    #[test]
    fn test_reregister_preserves_state() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        let now = ts(60_000);

        // Register stream with count feature
        let stream1 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![(
                "tx_count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: false,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream1).unwrap();

        // Push 5 events
        for _ in 0..5 {
            engine
                .push(
                    "Transactions",
                    &serde_json::json!({
                        "user_id": "u123", "amount": 10.0
                    }),
                    &store,
                    now,
                )
                .unwrap();
        }

        // Re-register with an added feature
        let stream2 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                (
                    "tx_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "avg_amount_1h".into(),
                    FeatureDef::Avg {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: false,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream2).unwrap();

        // Push 1 more event
        let features = engine
            .push(
                "Transactions",
                &serde_json::json!({
                    "user_id": "u123", "amount": 10.0
                }),
                &store,
                now,
            )
            .unwrap();

        // Original feature count should be 6 (not reset)
        assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(6)));
        // New feature should have count=1
        assert_eq!(
            features.get("avg_amount_1h"),
            Some(&FeatureValue::Float(10.0))
        );
    }

    #[test]
    fn test_valid_features_map() {
        let mut engine = PipelineEngine::new();
        engine
            .register(StreamDefinition {
                name: "Transactions".into(),
                key_field: Some("user_id".into()),
                features: vec![
                    (
                        "tx_count_1h".into(),
                        FeatureDef::Count {
                            window: Duration::from_secs(3600),
                            bucket: Duration::from_secs(60),
                            where_expr: None,
                            backfill: false,
                        },
                    ),
                    (
                        "ratio".into(),
                        FeatureDef::Derive {
                            expr: crate::engine::expression::parse_expr("1 + 1").unwrap(),
                        },
                    ),
                ],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();
        engine
            .register(StreamDefinition {
                name: "Logins".into(),
                key_field: Some("user_id".into()),
                features: vec![(
                    "login_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                )],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
            .unwrap();

        let vfm = engine.valid_features_map();
        assert_eq!(vfm.len(), 2);
        // Transactions should only have tx_count_1h (Derive excluded)
        let tx_features = vfm.get("Transactions").unwrap();
        assert_eq!(tx_features.len(), 1);
        assert!(tx_features.contains(&"tx_count_1h".to_string()));
        // Logins should have login_count_1h
        let login_features = vfm.get("Logins").unwrap();
        assert_eq!(login_features.len(), 1);
        assert!(login_features.contains(&"login_count_1h".to_string()));
    }

    // ======================== Phase 8 Plan 02: push_for_backfill Tests ========================

    #[test]
    fn test_push_for_backfill_targets_only_specified_features() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();
        let now = ts(60_000);

        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![
                (
                    "tx_count_1h".into(),
                    FeatureDef::Count {
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        where_expr: None,
                        backfill: false,
                    },
                ),
                (
                    "tx_sum_1h".into(),
                    FeatureDef::Sum {
                        field: "amount".into(),
                        window: Duration::from_secs(3600),
                        bucket: Duration::from_secs(60),
                        optional: false,
                        where_expr: None,
                        backfill: true,
                    },
                ),
            ],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream).unwrap();

        // push_for_backfill targeting ONLY "tx_sum_1h"
        let event = serde_json::json!({"user_id": "u1", "amount": 42.0});
        engine
            .push_for_backfill(
                "Transactions",
                &event,
                &store,
                now,
                &["tx_sum_1h".into()],
            )
            .unwrap();

        // Verify: tx_sum_1h should have the event, tx_count_1h should NOT have been pushed
        let entity = store.get_entity("u1").unwrap();
        let stream_state = entity.streams.get("Transactions").unwrap();
        // tx_sum_1h should exist and have a value
        let sum_op = stream_state
            .operators
            .iter()
            .find(|(n, _)| n == "tx_sum_1h");
        assert!(
            sum_op.is_some(),
            "tx_sum_1h operator should exist after backfill push"
        );
        // tx_count_1h should NOT exist (not in backfill_features list)
        let count_op = stream_state
            .operators
            .iter()
            .find(|(n, _)| n == "tx_count_1h");
        assert!(
            count_op.is_none(),
            "tx_count_1h operator should NOT exist -- not in backfill list"
        );
    }

    #[test]
    fn test_push_for_backfill_uses_event_timestamp() {
        let mut engine = PipelineEngine::new();
        let store = StateStore::new();

        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            features: vec![(
                "tx_count_1h".into(),
                FeatureDef::Count {
                    window: Duration::from_secs(3600),
                    bucket: Duration::from_secs(60),
                    where_expr: None,
                    backfill: true,
                },
            )],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
        };
        engine.register(stream).unwrap();

        // Push at time T=60000
        let t = ts(60_000);
        let event = serde_json::json!({"user_id": "u1"});
        engine
            .push_for_backfill(
                "Transactions",
                &event,
                &store,
                t,
                &["tx_count_1h".into()],
            )
            .unwrap();

        // Read at time T=60000 should show count=1
        let features = store.get_all_features("u1", t);
        assert_eq!(features.get("tx_count_1h"), Some(&FeatureValue::Int(1)));

        // Read at time T=60000 + 7200 (2h later, outside 1h window) should show count expired
        let t_future = ts(60_000 + 7200);
        let features_future = store.get_all_features("u1", t_future);
        // Count should be 0 or Missing (expired beyond 1h window)
        let val = features_future.get("tx_count_1h");
        assert!(
            val == Some(&FeatureValue::Missing) || val == Some(&FeatureValue::Int(0)),
            "Count at T+2h should be expired, got {:?}",
            val
        );
    }

    // ======================== Phase 18 Plan 01: Projection Tests ========================

    #[test]
    fn test_projection_select_filters_to_allowed_keys() {
        let mut map: FeatureMap = AHashMap::new();
        map.insert("a".into(), FeatureValue::Int(1));
        map.insert("b".into(), FeatureValue::Int(2));
        map.insert("c".into(), FeatureValue::Int(3));

        let proj = Projection::Select(AHashSet::from_iter(["a".into(), "b".into()]));
        proj.apply(&mut map);

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("a"), Some(&FeatureValue::Int(1)));
        assert_eq!(map.get("b"), Some(&FeatureValue::Int(2)));
        assert!(map.get("c").is_none());
    }

    #[test]
    fn test_projection_drop_removes_excluded_keys() {
        let mut map: FeatureMap = AHashMap::new();
        map.insert("a".into(), FeatureValue::Int(1));
        map.insert("b".into(), FeatureValue::Int(2));
        map.insert("c".into(), FeatureValue::Int(3));

        let proj = Projection::Drop(AHashSet::from_iter(["c".into()]));
        proj.apply(&mut map);

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("a"), Some(&FeatureValue::Int(1)));
        assert_eq!(map.get("b"), Some(&FeatureValue::Int(2)));
        assert!(map.get("c").is_none());
    }

    #[test]
    fn test_projection_apply_on_empty_map() {
        let mut map: FeatureMap = AHashMap::new();

        let proj = Projection::Select(AHashSet::from_iter(["a".into()]));
        proj.apply(&mut map);
        assert!(map.is_empty());

        let proj2 = Projection::Drop(AHashSet::from_iter(["a".into()]));
        proj2.apply(&mut map);
        assert!(map.is_empty());
    }
}
