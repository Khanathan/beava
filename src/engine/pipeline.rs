//! Pipeline engine: stream definitions and push-through orchestration.
//!
//! PipelineEngine holds registered stream definitions and coordinates the
//! synchronous push-through flow: event -> extract key -> update operators
//! -> evaluate derives -> return feature map.

use super::event_time::{LateDropCounters, RingBufferDropCounters, SharedLateDrops};
use crate::shard::watermark::WatermarkState;
use super::expression::{eval, EvalContext, Expr};
use super::hll::DistinctCountOp;
use super::operators::{
    AvgOp, CountOp, EmaOp, ExactMaxOp, ExactMinOp, FirstOp, LagOp, LastNOp, LastOp, MaxOp, MinOp,
    PercentileOp, StddevOp, SumOp,
};
use crate::error::BeavaError;
use crate::state::snapshot::OperatorState;
// Phase 54-04 Pass A6b: the `StateStore` struct was deleted, and with it the
// cfg-gated `get_features(&StateStore, ...)` read-path helper that used to
// live in this file. Production GET now always flows through
// `get_features_on_shard` (shard-local, no DashMap). Pass C retires the
// `state-inmem` feature entirely.
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
    /// Phase 23-01 — Stream↔Table enrichment join. State-free on the output
    /// side; on each left-side event, looks up `right_table`'s current row
    /// for the joined key(s) and emits the left event merged with right-side
    /// fields. Inner: drops when the Table has no matching row. Left: emits
    /// with null right-side fields on miss.
    ///
    /// `right_fields` is `(source_name_in_right, emitted_name)`. The SDK
    /// already applies `_right` suffix on column collision (polars-style)
    /// before compiling REGISTER, so the engine emits emitted_name verbatim.
    EnrichFromTable {
        right_table: String,
        on: Vec<String>,
        join_type: JoinType,
        right_fields: Vec<(String, String)>,
    },
    /// Phase 23-02 — Stream↔Stream symmetric interval windowed join.
    ///
    /// State lives in a per-key `OperatorState::StreamJoinBuffer` under the
    /// join stream in `EntityState.streams`. On each event arrival from
    /// `left_stream` or `right_stream`, the cascade probes the opposite side
    /// for events with `|event_time - other.event_time| <= within_ms`,
    /// emits one joined event per match, then inserts the arriving event
    /// and evicts stale entries (floor = max_seen_on_that_side - within_ms).
    ///
    /// `right_fields` is `(source_name_in_right, emitted_name)`. The SDK
    /// pre-applies `_right` suffix on column collision; the engine emits
    /// emitted_name verbatim.
    ///
    /// v0 limitation (documented in 23-02-SUMMARY): for `type=Left`, an
    /// unmatched left event emits a null-pair on arrival; a later matching
    /// right-side event emits a SECOND joined pair. Phase 24 will replace
    /// the null-pair with a retraction-aware retract + insert.
    StreamStreamJoin {
        left_stream: String,
        right_stream: String,
        on: Vec<String>,
        within_ms: u64,
        join_type: JoinType,
        left_fields: Vec<String>,
        right_fields: Vec<(String, String)>,
    },
    /// Phase 23-03 — Table↔Table same-key join. Both input Tables share
    /// identical key declarations (by field name; types validated at REGISTER).
    /// Output is a Table with the same key declaration.
    ///
    /// Cascade semantics (implemented in `push_with_cascade_internal`):
    ///   - On SET (upsert) on either input Table for key K:
    ///       * Look up the opposite Table's static_features for K.
    ///       * If both present → merge (left fields + right-suffixed right fields)
    ///         and write into the output Table's static_features for K.
    ///       * If opposite absent:
    ///           - inner  → tombstone output row for K.
    ///           - left (upsert on LEFT)  → write output with right fields null.
    ///           - left (upsert on RIGHT) → tombstone output for K.
    ///   - On tombstone (delete) on either input Table for K:
    ///       * inner → tombstone output for K.
    ///       * left + delete-on-RIGHT → rewrite output with right fields null
    ///         (left row retained because left side still exists).
    ///       * left + delete-on-LEFT  → tombstone output for K.
    ///
    /// `right_fields` is `(source_name_in_right, emitted_name)`. The SDK
    /// pre-applies `_right` suffix on column collision.
    TableTableJoin {
        left_table: String,
        right_table: String,
        on: Vec<String>,
        join_type: JoinType,
        left_fields: Vec<String>,
        right_fields: Vec<(String, String)>,
    },
}

/// Phase 23-01 — join semantics. Only Inner + Left are supported in v0;
/// outer/full/cross are rejected at registration (SDK + engine defense in depth).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
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
        FeatureDef::EnrichFromTable { .. } => false,
        FeatureDef::StreamStreamJoin { .. } => false,
        FeatureDef::TableTableJoin { .. } => false,
    }
}

/// Compute the schema diff between old and new feature lists.
/// Returns error if a feature name exists in both but with a different operator type.
fn diff_features(
    old: &[(String, FeatureDef)],
    new: &[(String, FeatureDef)],
) -> Result<SchemaDiff, BeavaError> {
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
                return Err(BeavaError::Protocol(format!(
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
#[derive(Debug, Clone, Default)]
pub struct StreamDefinition {
    pub name: String,
    /// Key field for entity extraction. None = keyless stream (raw event ingestion).
    /// Keyless streams cannot have windowed operators -- only derive features are allowed.
    pub key_field: Option<String>,
    /// Composite group_by keys (Phase 23-01). When present, entity key is derived by
    /// `encode_group_by(keys, event)` (pipe-joined string) instead of `key_field`.
    /// Single-key case: fast path through `key_field`. Composite: use these keys.
    /// `key_field` must be Some when `group_by_keys` is Some (points at keys[0] for
    /// consumers that expect a single key field name — legacy read paths see the
    /// composite key by way of the entity's state store entry).
    pub group_by_keys: Option<Vec<String>>,
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
    /// Per-stream watermark lateness override (D-09/CORR-03).
    /// When Some, WatermarkTracker uses this duration instead of the global
    /// WATERMARK_LATENESS constant (5 s). When None, the 5 s default applies.
    /// Absent in older snapshots → None → 5 s default (CORR-04 forward-compat).
    pub watermark_lateness: Option<Duration>,
    /// Phase 51-04: shard key declaration for join validation.
    /// None = no shard key (join validation deferred / not applicable).
    pub shard_key: Option<crate::engine::join_validator::ShardKeySpec>,
    /// Phase 60 D-A3 — per-stream salt cardinality for hot-key mitigation.
    /// Power of 2 in [2, 256]. None = no salting (zero overhead). Added here
    /// to complete the partial-landed P60 struct refactor; fully owned by
    /// Phase 60's salting work.
    pub salt: Option<u16>,
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
    /// Pre-computed at finalize_dag: for each primary stream, the flat list
    /// of downstream stream names to visit in topological order. Skips
    /// the BFS + `topo_order.iter().filter(to_visit.contains(..))` O(N²)
    /// walk that push_with_cascade_internal used to run per event. Empty
    /// value = leaf stream (no cascade).
    cascade_plan: AHashMap<String, Vec<String>>,
    /// Phase 24-04 / 49-03 — per-stream watermark state.
    /// Wave 1: wrapped in `Mutex<WatermarkState>` (single-writer AHashMap).
    /// Uncontended at N=1; Wave 2 removes the mutex from the hot path.
    /// Access via `wm_*()` forwarding methods — do NOT lock directly.
    pub watermarks: std::sync::Mutex<WatermarkState>,
    /// Phase 24-04 — per-stream late-drop counter. Exported as
    /// `beava_late_events_dropped_total{stream}` via `/metrics`.
    pub late_drops: SharedLateDrops,

    /// Phase 46-06 — per `(stream, operator_kind, reason)` ring-buffer drop
    /// counter. Exported as `beava_ring_buffer_drops_total` via `/metrics`.
    /// Handles are pre-registered at stream registration time (D-06) so
    /// the drop path calls only `fetch_add(1, Relaxed)` on a cached
    /// `Arc<AtomicU64>`.
    pub ring_buffer_drops: RingBufferDropCounters,

    /// Phase 27-02 — optional handle to the process-wide subscriber
    /// registry. Set by `install_subscribers` at server startup (see
    /// `make_concurrent_state_full`); `None` in unit-test harnesses that
    /// construct a bare `PipelineEngine::new()`. The ingest hot path
    /// (`push_internal`) calls `notify_subscribers` through this handle
    /// on every successful push — primary or cascade — so there is one
    /// hook site regardless of dispatch path (user direction §3).
    #[cfg(feature = "server")]
    pub subscriber_registry: Option<std::sync::Arc<crate::server::replica::SubscriberRegistry>>,

    /// Phase 51-04: optional signal registry for emitting JoinShardKeyMismatch signals.
    /// Set by `install_signals` at server startup. None in unit-test harnesses.
    #[cfg(feature = "server")]
    pub signals: Option<crate::server::signals::SharedRegistry>,

    /// Phase 49-05 (TPC Wave 1): per-shard state store at N=1.
    /// All state lives in Shard-0's AHashMap. Replaces DashMap hot path for
    /// callers that use shard_store(). StateStore (DashMap) compat shim remains
    /// in ConcurrentAppState — not deleted until Wave 4.
    ///
    /// Phase 53-03 (D-03): gated behind `state-inmem`. The default (fjall)
    /// build uses Plan 03B's `ShardedStateStoreFjall` sibling, not this field.
    #[cfg(feature = "state-inmem")]
    pub sharded_store: crate::shard::store::ShardedStateStoreV1,

    /// Phase 59.6 Wave 1 (TPC-PERF-11 D-B2): typed-schema registry. Streams
    /// registered via `@bv.stream` with a `schema:` block in their REGISTER
    /// JSON get an entry here; Wave 2+ wire codec and operator paths branch
    /// on `schema_registry.get(stream_name)` to decide typed-row vs.
    /// `serde_json::Value` fallback. At Wave 1, only schema registration
    /// wires up — no operator behavior change yet.
    pub schema_registry: crate::engine::schema::SchemaRegistry,

    /// Phase 59.7 Wave 0 (TPC-PERF-11 extension) — rollout gate for the
    /// typed-cascade-direct walker (`run_typed_direct_cascade`). Populated
    /// from `std::env::var("BEAVA_TYPED_CASCADE_DIRECT")` in
    /// [`PipelineEngine::new`]; default `false` so existing call sites
    /// route through the Value-bridge `run_typed_enrich_cascade` until
    /// Wave 4 lands the real direct walker and ops opt-in via the flag.
    ///
    /// W0 only READS + EXPOSES this value. W4 consumes it inside
    /// `push_typed_on_shard` to choose typed-direct vs. bridge walk.
    pub(crate) typed_cascade_direct_enabled: bool,
}

/// Phase 59.6 Wave 4 (TPC-PERF-11) → Phase 59.7 W0 rename — predicate for
/// the typed-cascade fast-path compatibility check inside
/// `push_typed_on_shard`. Returns `true` for `FeatureDef` variants whose
/// operator has a typed twin in `src/engine/operators_typed_aggs.rs` (or
/// the Wave-3 `EnrichFromTableTyped`).
///
/// Waves 5-6 (Phase 59.6) flipped more variants to `true` as they gained
/// typed impls. Anything returning `false` routes through the
/// `row_to_value` + Value cascade bridge so behavior is unchanged until
/// the typed op ships.
///
/// # Phase 59.7 rename rationale
///
/// Wave 4 of Phase 59.6 is the origin of this predicate — the old name
/// embedded an internal wave number that leaked into downstream Phase
/// 59.7 call sites (`run_typed_direct_cascade` etc.). Renamed to
/// `is_typed_cascade_compatible` to reflect the permanent semantic role:
/// "is this FeatureDef eligible for the typed-cascade direct walker?"
///
/// FIXME(59.7-W1): today this is a structural `matches!` on the enum
/// variant only — a `FeatureDef::Count { window: Some(..), bucket:
/// Some(..) }` returns `true` even though windowed typed aggs don't ship
/// until Wave 1. W1 tightens this predicate to require a concrete typed
/// windowed-agg impl exists for the op's window + bucket pair (gated via
/// the new `operators_typed_aggs_windowed` module), so windowed call
/// sites fall back to Value automatically until W1 flips.
pub(crate) fn is_typed_cascade_compatible(fd: &FeatureDef) -> bool {
    matches!(
        fd,
        FeatureDef::EnrichFromTable { .. }
            | FeatureDef::Count { .. }
            | FeatureDef::Sum { .. }
            | FeatureDef::Avg { .. }
            | FeatureDef::Min { .. }
            | FeatureDef::Max { .. }
            | FeatureDef::Last { .. }
            | FeatureDef::First { .. }
    )
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
        // Phase 23-01: EnrichFromTable is stateless at the operator level —
        // execution lives in push_internal / cascade (Table lookup + emit).
        FeatureDef::EnrichFromTable { .. } => None,
        // Phase 23-03: TableTableJoin is stateless — output lives in
        // static_features of the output Table entity; no operator state.
        FeatureDef::TableTableJoin { .. } => None,
        // Phase 23-02: StreamStreamJoin state is a per-key StreamJoinBuffer
        // created lazily by the cascade handler (different state shape —
        // needs within_ms from the feature def).
        FeatureDef::StreamStreamJoin { .. } => None,
    }
}

/// Phase 23-02: build a joined event for Stream↔Stream symmetric interval
/// joins. Starts from `left_map`, then overlays right-side fields per
/// `right_fields = [(source_in_right, emitted_name), ...]`. If the right
/// map is empty (null-pair emission on left-side miss), missing values
/// land as `Value::Null`.
///
/// Defense-in-depth mirrors `EnrichFromTable`: refuses to clobber a
/// pre-existing left field of the same emitted name when the emitted
/// name differs from the right-side source name (i.e., the SDK has
/// already renamed the right slot to `_right`-suffixed, so the unsuffixed
/// emitted_name must have come from the left).
fn build_joined_event(
    left_map: &serde_json::Map<String, serde_json::Value>,
    right_map: &serde_json::Map<String, serde_json::Value>,
    right_fields: &[(String, String)],
) -> serde_json::Value {
    let mut out = left_map.clone();
    for (right_src, emitted) in right_fields {
        if out.contains_key(emitted) && emitted != right_src {
            continue;
        }
        let v = right_map
            .get(right_src)
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        out.insert(emitted.clone(), v);
    }
    serde_json::Value::Object(out)
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
        FeatureDef::EnrichFromTable { .. } => None,
        FeatureDef::StreamStreamJoin { .. } => None,
        FeatureDef::TableTableJoin { .. } => None,
    }
}

/// Return the operator_kind label for ring-buffer-owning operators (D-05 / OBS-01).
/// Returns `None` for non-ring-buffer operators (Derive, Lag, Ema, Last, etc.)
/// so that `register()` only pre-allocates counters where drops can actually occur.
///
/// The string is the label that appears in `beava_ring_buffer_drops_total{operator_kind="..."}`.
/// Using the operator CLASS rather than a per-instance UUID keeps label cardinality
/// bounded by `num_streams × num_operator_kinds × 3` (D-05 / Pitfall 3 guard).
fn ring_buffer_operator_kind(def: &FeatureDef) -> Option<&'static str> {
    match def {
        FeatureDef::Count { .. } => Some("count"),
        FeatureDef::Sum { .. } => Some("sum"),
        FeatureDef::Avg { .. } => Some("avg"),
        FeatureDef::Min { .. } => Some("min"),
        FeatureDef::Max { .. } => Some("max"),
        FeatureDef::Stddev { .. } => Some("stddev"),
        FeatureDef::DistinctCount { .. } => Some("distinct_count"),
        FeatureDef::ExactMin { .. } => Some("exact_min"),
        FeatureDef::ExactMax { .. } => Some("exact_max"),
        // Percentile and TopK use RetractingRingBuffer which never drops.
        // Last, Lag, Ema, LastN, First, FirstN, StreamStreamJoin, TableTableJoin,
        // EnrichFromTable, and Derive have no ring buffer at all.
        FeatureDef::Percentile { .. }
        | FeatureDef::Last { .. }
        | FeatureDef::Lag { .. }
        | FeatureDef::Ema { .. }
        | FeatureDef::LastN { .. }
        | FeatureDef::First { .. }
        | FeatureDef::Derive { .. }
        | FeatureDef::EnrichFromTable { .. }
        | FeatureDef::StreamStreamJoin { .. }
        | FeatureDef::TableTableJoin { .. } => None,
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
        // Phase 59.7 Wave 0 (TPC-PERF-11 extension): read the rollout
        // gate at engine construction time so tests + server binaries see
        // identical behavior. Accepts only exact "1" to match the
        // BEAVA_TYPED_CASCADE_DIRECT=1 documented form; any other value
        // (including "true", "on", "yes", empty) keeps the flag OFF so
        // the Value-bridge cascade remains the default.
        let typed_cascade_direct_enabled = std::env::var("BEAVA_TYPED_CASCADE_DIRECT")
            .ok()
            .is_some_and(|v| v == "1");
        Self {
            streams: AHashMap::new(),
            views: AHashMap::new(),
            raw_register_jsons: AHashMap::new(),
            dag: DiGraph::new(),
            node_indices: AHashMap::new(),
            topo_order: Vec::new(),
            downstream_map: AHashMap::new(),
            cascade_plan: AHashMap::new(),
            watermarks: std::sync::Mutex::new(WatermarkState::new()),
            late_drops: LateDropCounters::new(),
            ring_buffer_drops: RingBufferDropCounters::new(),
            #[cfg(feature = "server")]
            subscriber_registry: None,
            #[cfg(feature = "server")]
            signals: None,
            #[cfg(feature = "state-inmem")]
            sharded_store: crate::shard::store::ShardedStateStoreV1::new(1),
            schema_registry: crate::engine::schema::SchemaRegistry::new(),
            typed_cascade_direct_enabled,
        }
    }

    /// Phase 59.7 Wave 0 (TPC-PERF-11 extension) — accessor for the
    /// `BEAVA_TYPED_CASCADE_DIRECT` rollout gate. Wave 4 consumes this
    /// inside `push_typed_on_shard` to pick typed-direct vs. bridge
    /// walk; Wave 0 just exposes it so tests can assert the env-read
    /// works without reaching into private fields.
    pub fn typed_cascade_direct_enabled(&self) -> bool {
        self.typed_cascade_direct_enabled
    }

    // -----------------------------------------------------------------------
    // Phase 59.6 Wave 1 (TPC-PERF-11) — typed-schema accessors.
    //
    // Wave 1 lands the schema runtime foundation. Wave 2+ wire codec and
    // operator paths consume these accessors to decide typed-vs-Value
    // dispatch. No hot-path operator calls them yet.
    // -----------------------------------------------------------------------

    /// Phase 59.6 Wave 1 (D-B2): check whether a stream has a registered
    /// typed schema.
    pub fn is_typed_stream(&self, name: &str) -> bool {
        self.schema_registry.is_registered(name)
    }

    /// Phase 59.6 Wave 1 (D-B2): get the typed schema for a stream, if
    /// registered. Returns an `Arc` clone so operators can cache the
    /// schema without borrowing from the engine.
    pub fn get_schema(
        &self,
        name: &str,
    ) -> Option<std::sync::Arc<crate::engine::schema::RegisteredSchema>> {
        self.schema_registry.get(name)
    }

    /// Phase 59.6 Wave 1 (D-A1): register a typed schema under the given
    /// stream name. Called by the REGISTER JSON consumer
    /// (`src/engine/register.rs`) when the payload contains a `schema:`
    /// sub-object. Returns the monotonic `SchemaId` assigned by the
    /// registry.
    pub fn register_typed_schema(
        &mut self,
        name: &str,
        schema: crate::engine::schema::RegisteredSchema,
    ) -> crate::engine::schema::SchemaId {
        self.schema_registry.insert(name, schema)
    }

    /// Phase 49-05: construct engine with a specific shard count.
    /// Wave 1: always called with n=1 from main.rs. Wave 2 will pass n=CPU_COUNT.
    ///
    /// Phase 53-03: `n` is ignored in the default (fjall) build — the sharded
    /// store lives in `ConcurrentAppState` / Plan 03B's `ShardedStateStoreFjall`.
    pub fn with_shards(_n: u16) -> Self {
        #[cfg(feature = "state-inmem")]
        {
            let mut engine = PipelineEngine::new();
            engine.sharded_store = crate::shard::store::ShardedStateStoreV1::new(_n);
            engine
        }
        #[cfg(not(feature = "state-inmem"))]
        {
            PipelineEngine::new()
        }
    }

    /// Phase 27-02: attach a `SubscriberRegistry` to this engine. Called
    /// exactly once by `make_concurrent_state_full` during server startup
    /// (existing test constructors that never touch the TCP server leave
    /// this as `None` and `push_internal`'s hook becomes a zero-cost
    /// no-op). Subsequent `OP_SUBSCRIBE` sessions are registered against
    /// this same instance so the ingest hook can `try_send` into every
    /// live subscriber's bounded mpsc.
    #[cfg(feature = "server")]
    pub fn install_subscribers(
        &mut self,
        registry: std::sync::Arc<crate::server::replica::SubscriberRegistry>,
    ) {
        self.subscriber_registry = Some(registry);
    }

    /// Phase 51-04: attach a signal registry so `register()` can emit
    /// `JoinShardKeyMismatch` signals. Called by `make_concurrent_state_full`.
    #[cfg(feature = "server")]
    pub fn install_signals(&mut self, registry: crate::server::signals::SharedRegistry) {
        self.signals = Some(registry);
    }

    // -----------------------------------------------------------------------
    // Phase 49-03: wm_*() forwarding methods — ergonomic access to the
    // Mutex<WatermarkState> without exposing the lock at call sites.
    // Uncontended at N=1 (Wave 1). Wave 2 removes the mutex from the hot path.
    // -----------------------------------------------------------------------

    /// Observe an event time for a stream (updates monotonic max).
    pub fn wm_observe(&self, stream: &str, event_time: std::time::SystemTime) {
        self.watermarks
            .lock()
            .expect("watermarks mutex poisoned")
            .observe(stream, event_time);
    }

    /// Current watermark for a stream (observed_max - lateness).
    pub fn wm_watermark(&self, stream: &str) -> Option<std::time::SystemTime> {
        self.watermarks
            .lock()
            .expect("watermarks mutex poisoned")
            .watermark(stream)
    }

    /// Observed max event time for stream (no lateness subtracted).
    pub fn wm_observed_max(&self, stream: &str) -> Option<std::time::SystemTime> {
        self.watermarks
            .lock()
            .expect("watermarks mutex poisoned")
            .observed_max(stream)
    }

    /// Most recent event_time observed (not necessarily the max).
    pub fn wm_last_event_time(&self, stream: &str) -> Option<std::time::SystemTime> {
        self.watermarks
            .lock()
            .expect("watermarks mutex poisoned")
            .last_event_time(stream)
    }

    /// List all streams with an observed watermark.
    pub fn wm_iter_streams(&self) -> Vec<(String, std::time::SystemTime)> {
        self.watermarks
            .lock()
            .expect("watermarks mutex poisoned")
            .iter_streams()
    }

    /// Per-stream lateness override (falls back to 5s global default).
    pub fn wm_lateness_for(&self, stream: &str) -> std::time::Duration {
        self.watermarks
            .lock()
            .expect("watermarks mutex poisoned")
            .lateness_for(stream)
    }

    /// γ: join watermark propagation — output = min(left, right).
    pub fn wm_propagate_join(&self, left: &str, right: &str, output: &str) {
        self.watermarks
            .lock()
            .expect("watermarks mutex poisoned")
            .propagate_join(left, right, output);
    }

    /// γ: stateless propagation — output inherits input watermark.
    pub fn wm_propagate_stateless(&self, from: &str, to: &str) {
        self.watermarks
            .lock()
            .expect("watermarks mutex poisoned")
            .propagate_stateless(from, to);
    }

    /// γ: aggregation — output table inherits source stream watermark.
    pub fn wm_attach_to_table(&self, source_stream: &str, output_table: &str) {
        self.watermarks
            .lock()
            .expect("watermarks mutex poisoned")
            .attach_to_table(source_stream, output_table);
    }

    /// Register a stream definition. Validates derive expressions are parseable.
    /// Duplicate registration replaces the previous definition (idempotent).
    /// Returns a SchemaDiff describing what changed (added/removed/unchanged features).
    /// Stream names must be non-empty (T-01-14 mitigation).
    pub fn register(&mut self, stream: StreamDefinition) -> Result<SchemaDiff, BeavaError> {
        if stream.name.is_empty() {
            return Err(BeavaError::Protocol("stream name must not be empty".into()));
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
                    return Err(BeavaError::Protocol(format!(
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
        // D-10 / CORR-03: propagate per-stream watermark lateness override into
        // WatermarkState before inserting the stream. This way any immediate
        // observe() calls during cascade evaluation use the correct lateness.
        if let Some(lateness) = stream.watermark_lateness {
            self.watermarks
                .lock()
                .expect("watermarks mutex poisoned")
                .set_lateness(&stream.name, lateness);
        }

        // D-06 / OBS-01: pre-register ring-buffer drop counter handles for
        // each ring-buffer-owning operator kind in this stream. This caches
        // the Arc<AtomicU64> handles so the drop path calls only
        // fetch_add(1, Relaxed) — zero DashMap lookup overhead.
        for (_, def) in &stream.features {
            if let Some(kind) = ring_buffer_operator_kind(def) {
                // Calling register() for an already-registered (stream, kind)
                // is idempotent — it just returns the existing Arc handles.
                self.ring_buffer_drops.register(&stream.name, kind);
            }
        }

        // Phase 51-04 / Phase 56 D-B4 (TPC-CORR-04 relaxation): validate
        // shard_key compatibility. Previously this returned `Err` on
        // mismatch; the relaxed path emits a non-fatal
        // `CrossShardJoinWarning` per peer pair and proceeds. Runtime
        // correctness is delivered by Wave 1's `ssj_insert_at_shard`
        // (TPC-CORR-09) which shuffles both sides to `hash(join.on) % N`.
        //
        // Three surfaces (D-B4 + D-C1):
        //   * structured log line (`eprintln!` — matches repo convention,
        //     this codebase does not pull in the `tracing` crate).
        //   * `beava_crossshard_joins_registered_total{join_id}` counter.
        //   * Signal registry + `/debug/warnings.cross_shard_joins` array.
        let crossshard_warnings =
            crate::engine::join_validator::validate_shard_keys(&self.streams, &stream);
        for w in &crossshard_warnings {
            eprintln!(
                "[WARN] beava::register CrossShardJoinWarning: \
                 join_id={} stream_a={} stream_b={} left_shard_key={} \
                 right_shard_key={} on_field={} — {}",
                w.join_id,
                w.stream_a,
                w.stream_b,
                w.left_shard_key,
                w.right_shard_key,
                w.on_field,
                w.message,
            );
            metrics::counter!(
                crate::shard::metrics::CROSSSHARD_JOINS_REGISTERED_TOTAL,
                "join_id" => w.join_id.clone(),
            )
            .increment(1);
            #[cfg(feature = "server")]
            if let Some(ref registry) = self.signals {
                crate::server::signals::emit_cross_shard_join_warning(registry, w);
            }
        }

        self.streams.insert(name_clone.clone(), stream);
        // Rebuild DAG and validate (cycle detection)
        if let Err(e) = self.rebuild_dag() {
            // Registration failed due to cycle -- remove the stream we just added
            self.streams.remove(&name_clone);
            return Err(e);
        }
        Ok(diff)
    }

    /// Rebuild the DAG from all registered streams. Called after each registration.
    /// Detects circular dependencies via topological sort.
    fn rebuild_dag(&mut self) -> Result<(), BeavaError> {
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
            BeavaError::Protocol(format!(
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

        // Pre-compute the cascade plan: for every primary stream, BFS over
        // `downstream_map` once to collect the set of reachable streams,
        // then emit them filtered by `topo_order`. The shard-path
        // `push_with_cascade_on_shard` uses this plan to iterate cascade
        // targets in topological order with zero per-event allocation.
        let mut cascade_plan: AHashMap<String, Vec<String>> = AHashMap::new();
        for primary in self.streams.keys() {
            let mut reachable: AHashSet<String> = AHashSet::new();
            let mut frontier: Vec<String> = self
                .downstream_map
                .get(primary)
                .cloned()
                .unwrap_or_default();
            while let Some(s) = frontier.pop() {
                if !reachable.insert(s.clone()) {
                    continue;
                }
                if let Some(next) = self.downstream_map.get(&s) {
                    frontier.extend(next.iter().cloned());
                }
            }
            if reachable.is_empty() {
                continue; // leaf; no cascade plan entry
            }
            let plan: Vec<String> = self
                .topo_order
                .iter()
                .filter(|s| reachable.contains(s.as_str()))
                .cloned()
                .collect();
            cascade_plan.insert(primary.clone(), plan);
        }
        self.cascade_plan = cascade_plan;
        Ok(())
    }

    /// Phase 54-02 Task 2: scatter-gather Table↔Table cascade on the shard path.
    ///
    /// Shard-aware twin of `cascade_table_upsert`. Walks every registered
    /// `TableTableJoin` feature that references `input_table`, computes the
    /// output row, then routes the resulting `upsert_table_row` /
    /// `tombstone_table_row` to the shard that OWNS the output key:
    ///
    ///   - If the output key hashes to `input_shard_idx` (or `sibling_shards`
    ///     is `None` / has ≤1 handles) → write directly against `input_shard`
    ///     via the widened `StoreView::Sharded` surface (intra-shard fast path).
    ///
    ///   - Otherwise → dispatch `ShardOp::UpsertTableRow` /
    ///     `ShardOp::TombstoneTableRow` to `sibling_shards[target_idx]` via
    ///     non-blocking `crossbeam::try_send`, collect the oneshot response
    ///     receivers, and join them at the end of the call
    ///     (`futures::executor::block_on`).
    ///
    /// # User decision (2026-04-19)
    ///
    /// This implements **SCATTER-GATHER** per the Phase 54 locked user
    /// decision. The researcher's register-time shard_key-constraint
    /// recommendation (reject cross-shard TT edges at REGISTER) was
    /// EXPLICITLY REJECTED — TT cascades whose output keys live on a
    /// different shard than their input are a first-class supported
    /// pattern, budgeted against the Wave 5 -15% EPS gate.
    ///
    /// # Deadlock analysis — why shard-A → shard-B → shard-A cycles cannot form
    ///
    /// The scatter phase issues `try_send` into sibling inboxes and then
    /// blocks on `futures::executor::block_on(rx)` where `rx` is a
    /// `tokio::sync::oneshot::Receiver` fulfilled by the sibling shard's
    /// own event loop. Three observations make a deadlock impossible:
    ///
    /// **(a) Each shard has its own pinned OS thread + its own runtime.**
    /// Per Phase 50.5, `spawn_shard_threads` gives every shard a dedicated
    /// OS thread and its own `tokio::runtime::Builder::new_current_thread`
    /// reactor. When shard-A calls `block_on(rx)` waiting for shard-B, the
    /// blocked thread is shard-A's — shard-A is NOT a consumer of shard-B's
    /// inbox. Shard-B's event loop runs on its own thread and continues
    /// draining its own inbox unimpeded. The canonical "cycle" (A waits on
    /// B, B waits on A) would require A to be the consumer of B's inbox —
    /// it is not.
    ///
    /// **(b) `crossbeam_channel::try_send` is non-blocking on the send
    /// side.** The scatter phase uses `try_send` exclusively. If the target
    /// shard's inbox is `Full`, `try_send` returns `TrySendError::Full`
    /// immediately — control returns to the caller without acquiring any
    /// wait-chain edge. Even if shard-B (while servicing our
    /// `UpsertTableRow`) issued its own cross-shard cascade back to
    /// shard-A, that nested send is also `try_send`, so shard-B never
    /// blocks on scheduling. The ONLY blocking edge in the whole dance is
    /// shard-A → oneshot(rx) ← shard-B's SetOk reply, and shard-B fulfills
    /// that receiver independently when it drains the inbox entry.
    ///
    /// **(c) Backpressure on target inbox = FAIL FAST with
    /// `BeavaError::Protocol("shard inbox full — cascade backpressure")`.**
    /// On `try_send → Full` we increment
    /// `beava_shard_inbox_full_total{shard=target}` (the existing Phase 50
    /// metric) and return the error up through
    /// `push_with_cascade_on_shard` → `shard_event_loop` → TCP/HTTP
    /// caller, which maps it to 503 / `SHARD_OVERLOAD 0x10` exactly like a
    /// single-shard push would. No retry, no backoff — retrying in place
    /// would re-introduce the cycle risk the scatter-gather design
    /// eliminates, and TT-cascade is a correctness invariant (the derived
    /// row MUST land atomically, or the whole push fails). Sustained
    /// `beava_shard_inbox_full_total` on a cascade target is the signal
    /// to exercise the CONTEXT §Area 5 contingency ladder (renegotiate
    /// inbox capacity, add write-through cache, or reduce fan-out).
    ///
    /// # Cross-shard recursion scope (this wave)
    ///
    /// When an output row is routed cross-shard we do NOT recurse into
    /// TT-of-TT chains from the current shard — that recursion must run
    /// against the target shard's row state, which lives on the target
    /// shard. Same-shard outputs recurse normally (matches the DashMap
    /// `cascade_table_upsert` semantics). Cross-shard TT-of-TT is
    /// out-of-scope for Pass B; the existing sharding_parity harness does
    /// not exercise it, and the cross_shard_tt_cascade test covers a
    /// single cascade hop.
    ///
    /// # Signature notes
    ///
    /// - `primary_event` is the source payload the cascade was triggered by
    ///   — it's the only source of field values for the output key when the
    ///   downstream Table's key_field differs from `input_table`'s.
    /// - `sibling_shards = None` (or `len ≤ 1`) collapses to intra-shard
    ///   writes exclusively (preserves N=1 behavior and covers test
    ///   harnesses that don't spawn real shard threads).
    #[allow(clippy::too_many_arguments)]
    pub fn cascade_table_upsert_on_shard(
        &self,
        input_table: &str,
        key: &str,
        tombstoned: bool,
        primary_event: Option<&serde_json::Value>,
        input_shard: &mut crate::shard::Shard,
        input_shard_idx: usize,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        now: SystemTime,
    ) -> Result<(), BeavaError> {
        // Non-batched callers (recovery replay, PushTableRow, DeleteTableRow,
        // SetWithCascade, self-recursion for same-shard TT-of-TT) use the
        // per-event scatter-gather path — `buffer=None`. Batched callers
        // (push_with_cascade_on_shard) use
        // `cascade_table_upsert_on_shard_buffered` which routes cross-shard
        // writes into the CascadeBuffer for end-of-batch coalesced dispatch.
        self.cascade_table_upsert_on_shard_buffered(
            input_table,
            key,
            tombstoned,
            primary_event,
            input_shard,
            input_shard_idx,
            sibling_shards,
            None,
            now,
        )
    }

    /// Phase 55-01 D-A1/D-A2: buffered variant of
    /// `cascade_table_upsert_on_shard`. When `cascade_buffer = Some(&mut)`,
    /// cross-shard writes accumulate into the buffer (end-of-batch coalesce
    /// in `push_with_cascade_on_shard`). When `None`, falls back to the
    /// per-event scatter-gather path used by non-batched callers (recovery,
    /// legacy PushTableRow / DeleteTableRow / SetWithCascade dispatch).
    ///
    /// Same-shard writes ALWAYS take the inline `StoreView` fast path
    /// regardless of buffer presence — matches SC #7's "same-shard fast
    /// path AND batched cross-shard dispatch from day one" contract.
    #[allow(clippy::too_many_arguments)]
    pub fn cascade_table_upsert_on_shard_buffered(
        &self,
        input_table: &str,
        key: &str,
        _tombstoned: bool,
        primary_event: Option<&serde_json::Value>,
        input_shard: &mut crate::shard::Shard,
        input_shard_idx: usize,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        cascade_buffer: Option<&mut crate::shard::cascade_buffer::CascadeBuffer>,
        now: SystemTime,
    ) -> Result<(), BeavaError> {
        use crate::state::store::TableRowState;
        use crate::shard::{read_entity_from_shard, StoreView};

        // Port of cascade_table_upsert (DashMap version): walk every stream
        // whose features reference `input_table` as left or right of a
        // TableTableJoin. For each such downstream:
        //   - read left/right rows from `input_shard` at `key`
        //   - compute Live / Tombstoned disposition via join_type semantics
        //   - write the merged row to the shard OWNING the output key
        let n_shards = sibling_shards.map(|s| s.len()).unwrap_or(1).max(1);

        let mut downstreams: Vec<(String, FeatureDef)> = Vec::new();
        for (sname, sdef) in &self.streams {
            for (_fn, def) in &sdef.features {
                if let FeatureDef::TableTableJoin {
                    left_table,
                    right_table,
                    ..
                } = def
                {
                    if left_table == input_table || right_table == input_table {
                        downstreams.push((sname.clone(), def.clone()));
                    }
                }
            }
        }

        // Outstanding oneshot receivers for the gather phase (per-event path).
        let mut pending: Vec<(usize, tokio::sync::oneshot::Receiver<crate::shard::thread::ShardResult>)> =
            Vec::new();

        // Rebind buffer so we can reborrow across downstream iterations.
        // `cascade_buffer.as_deref_mut()` would require Deref, so use
        // `Option::as_mut()` on an Option-of-mut-ref trick via a local.
        let mut cascade_buffer = cascade_buffer;

        for (output_name, def) in downstreams {
            let (left_table, right_table, join_type, left_fields, right_fields) = match def {
                FeatureDef::TableTableJoin {
                    left_table,
                    right_table,
                    join_type,
                    left_fields,
                    right_fields,
                    ..
                } => (
                    left_table,
                    right_table,
                    join_type,
                    left_fields,
                    right_fields,
                ),
                _ => continue,
            };

            // Read both sides from the input shard — TT cascade state still
            // lives at `key` on the INPUT side; the split-shard hop is only
            // on the OUTPUT write.
            let (left_row, right_row): (
                Option<crate::state::store::TableRow>,
                Option<crate::state::store::TableRow>,
            ) = {
                let lt = left_table.clone();
                let rt = right_table.clone();
                let lr = read_entity_from_shard(input_shard, key, |entity| {
                    entity.table_rows.get(&lt).cloned()
                })
                .flatten();
                let rr = read_entity_from_shard(input_shard, key, |entity| {
                    entity.table_rows.get(&rt).cloned()
                })
                .flatten();
                (lr, rr)
            };

            let l_live = matches!(
                left_row.as_ref().map(|r| &r.state),
                Some(TableRowState::Live)
            );
            let r_live = matches!(
                right_row.as_ref().map(|r| &r.state),
                Some(TableRowState::Live)
            );

            let (emit_live, null_right) = match join_type {
                JoinType::Inner => {
                    if l_live && r_live {
                        (true, false)
                    } else {
                        (false, false)
                    }
                }
                JoinType::Left => {
                    if l_live && r_live {
                        (true, false)
                    } else if l_live {
                        (true, true)
                    } else {
                        (false, false)
                    }
                }
            };

            let output_tombstoned = !emit_live;

            // Determine output_key. If the downstream stream declares its
            // own key_field / group_by_keys AND we have the primary event in
            // hand, use those to re-derive the output key — this is the
            // scatter-gather trigger (different shard_key on output table).
            // Otherwise fall back to `key` (same-shard semantics, matches
            // the DashMap cascade_table_upsert).
            let output_key: String = match (primary_event, self.streams.get(&output_name)) {
                (Some(event), Some(out_def)) => {
                    if let Some(gb_keys) = &out_def.group_by_keys {
                        match crate::engine::register::encode_group_by(gb_keys, event) {
                            Ok(k) => k,
                            Err(_) => key.to_string(),
                        }
                    } else if let Some(kf) = &out_def.key_field {
                        match event.get(kf) {
                            Some(serde_json::Value::String(s)) if !s.is_empty() => s.clone(),
                            Some(serde_json::Value::Number(n)) => n.to_string(),
                            _ => key.to_string(),
                        }
                    } else {
                        key.to_string()
                    }
                }
                _ => key.to_string(),
            };

            // Target shard for output row. At N=1 (n_shards==1) this is
            // always 0; at N>1 we use the same ahash-modulo-N routing the
            // ingest path uses (shard_hint_for_event on a synthetic
            // {"k": output_key} payload is equivalent to hashing the string
            // directly). Keep routing backend-agnostic — just the string.
            let target_shard_idx: usize = if n_shards <= 1 {
                0
            } else {
                let synth = serde_json::json!({ "__k": output_key });
                (crate::routing::shard_hint_for_event(&synth, Some("__k")) as usize) % n_shards
            };

            if emit_live {
                // Build merged fields exactly like cascade_table_upsert.
                let mut merged: AHashMap<String, FeatureValue> = AHashMap::new();

                if let Some(lr) = left_row.as_ref() {
                    for lf in &left_fields {
                        let v = lr.fields.get(lf).cloned().unwrap_or(FeatureValue::Missing);
                        merged.insert(lf.clone(), v);
                    }
                }

                for (src, emitted) in &right_fields {
                    if merged.contains_key(emitted) {
                        continue;
                    }
                    let v = if null_right {
                        FeatureValue::Missing
                    } else {
                        right_row
                            .as_ref()
                            .and_then(|rr| rr.fields.get(src).cloned())
                            .unwrap_or(FeatureValue::Missing)
                    };
                    merged.insert(emitted.clone(), v);
                }

                // Same-shard fast path.
                if target_shard_idx == input_shard_idx
                    || sibling_shards.map(|s| s.len()).unwrap_or(0) <= 1
                {
                    let mut view = StoreView::Sharded(input_shard);
                    view.upsert_table_row(&output_key, &output_name, merged, now);
                    // Phase 55-01 SC-5: intra-shard cascade counter. Emitted
                    // ONLY on the same-shard inline path so the ratio vs
                    // beava_cascade_cross_shard_total gives exact
                    // cross-shard fraction on perf dashboards.
                    crate::shard::metrics::record_cascade_intra_shard(
                        input_shard_idx,
                        1,
                    );
                    // Recurse — same-shard only. See "Cross-shard recursion
                    // scope" in the doc comment above. Thread the buffer
                    // through so TT-of-TT chains that re-key cross-shard
                    // also coalesce (Phase 55-01 SC #7).
                    self.cascade_table_upsert_on_shard_buffered(
                        &output_name,
                        &output_key,
                        output_tombstoned,
                        primary_event,
                        input_shard,
                        input_shard_idx,
                        sibling_shards,
                        cascade_buffer.as_mut().map(|b| &mut **b),
                        now,
                    )?;
                } else if let Some(buf) = cascade_buffer.as_mut() {
                    // Phase 55-01 D-A1/D-A2: batched caller — accumulate
                    // into the CascadeBuffer for end-of-batch coalesced
                    // dispatch. The counter emission and high-watermark
                    // observation happen in `CascadeBuffer::flush` + its
                    // `LiveCascadeTargets::dispatch_batch` path (single
                    // emission site for `beava_cascade_cross_shard_total`).
                    // Cross-shard TT-of-TT recursion remains out-of-scope
                    // for this wave; the batch flush carries one hop only.
                    buf.accumulate(
                        target_shard_idx,
                        output_name.clone(),
                        output_key.clone(),
                        merged,
                    );
                } else {
                    // Non-batched caller (recovery, PushTableRow, SetWithCascade,
                    // DeleteTableRow) — per-event scatter-gather.
                    let handles = sibling_shards.expect("sibling_shards checked above");
                    let target = &handles[target_shard_idx];
                    if target.is_down.load(std::sync::atomic::Ordering::Relaxed) {
                        return Err(BeavaError::Protocol(format!(
                            "cascade target shard {} is down (quarantined)",
                            target_shard_idx
                        )));
                    }
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let ev = crate::shard::thread::ShardEvent {
                        payload: bytes::Bytes::new(),
                        stream_name: std::sync::Arc::from(""),
                        shard_hint: 0,
                        response_tx: Some(tx),
                        op: crate::shard::thread::ShardOp::UpsertTableRow {
                            key: output_key.clone(),
                            table_name: output_name.clone(),
                            fields: merged,
                            now,
                        },
                        payload_fmt: crate::wire::PayloadFmt::Binary,
                        schema_id: 0,
                    };
                    // Phase 55-01 SC-5: high-watermark + cross-shard counter
                    // emission site for the per-event (non-batched) cascade
                    // path. This branch is NOT on the batched hot path —
                    // `push_with_cascade_on_shard` threads a CascadeBuffer
                    // through and takes the `as_mut()` arm above.
                    let depth = target.inbox_tx.len();
                    let cap = target.inbox_tx.capacity().unwrap_or(usize::MAX);
                    crate::shard::metrics::record_inbox_depth(target_shard_idx, depth, cap);
                    match target.inbox_tx.try_send(ev) {
                        Ok(()) => {
                            pending.push((target_shard_idx, rx));
                            metrics::counter!(
                                crate::shard::metrics::CASCADE_CROSS_SHARD_TOTAL,
                                "source" => input_shard_idx.to_string(),
                                "target" => target_shard_idx.to_string(),
                            ).increment(1);
                        }
                        Err(crossbeam_channel::TrySendError::Full(_)) => {
                            crate::shard::metrics::record_inbox_full(target_shard_idx);
                            return Err(BeavaError::Protocol(format!(
                                "shard inbox full — cascade backpressure (target={})",
                                target_shard_idx
                            )));
                        }
                        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                            return Err(BeavaError::Protocol(format!(
                                "shard inbox disconnected (target={})",
                                target_shard_idx
                            )));
                        }
                    }
                }
            } else {
                // Tombstone path.
                if target_shard_idx == input_shard_idx
                    || sibling_shards.map(|s| s.len()).unwrap_or(0) <= 1
                {
                    let mut view = StoreView::Sharded(input_shard);
                    view.tombstone_table_row(&output_key, &output_name, now);
                    self.cascade_table_upsert_on_shard_buffered(
                        &output_name,
                        &output_key,
                        output_tombstoned,
                        primary_event,
                        input_shard,
                        input_shard_idx,
                        sibling_shards,
                        cascade_buffer.as_mut().map(|b| &mut **b),
                        now,
                    )?;
                } else {
                    let handles = sibling_shards.expect("sibling_shards checked above");
                    let target = &handles[target_shard_idx];
                    if target.is_down.load(std::sync::atomic::Ordering::Relaxed) {
                        return Err(BeavaError::Protocol(format!(
                            "cascade target shard {} is down (quarantined)",
                            target_shard_idx
                        )));
                    }
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let ev = crate::shard::thread::ShardEvent {
                        payload: bytes::Bytes::new(),
                        stream_name: std::sync::Arc::from(""),
                        shard_hint: 0,
                        response_tx: Some(tx),
                        op: crate::shard::thread::ShardOp::TombstoneTableRow {
                            key: output_key.clone(),
                            table_name: output_name.clone(),
                            now,
                        },
                        payload_fmt: crate::wire::PayloadFmt::Binary,
                        schema_id: 0,
                    };
                    match target.inbox_tx.try_send(ev) {
                        Ok(()) => pending.push((target_shard_idx, rx)),
                        Err(crossbeam_channel::TrySendError::Full(_)) => {
                            crate::shard::metrics::record_inbox_full(target_shard_idx);
                            return Err(BeavaError::Protocol(format!(
                                "shard inbox full — cascade backpressure (target={})",
                                target_shard_idx
                            )));
                        }
                        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                            return Err(BeavaError::Protocol(format!(
                                "shard inbox disconnected (target={})",
                                target_shard_idx
                            )));
                        }
                    }
                }
            }
        }

        // GATHER phase — block on every outstanding oneshot. We use
        // `futures::executor::block_on` (not tokio's block_on) because the
        // caller is already inside the shard thread's current_thread tokio
        // runtime and tokio's Handle::block_on panics on re-entry. The tiny
        // futures executor polls the oneshot Receiver; wakeups originate on
        // the sibling shard's thread (send-side), independent of any tokio
        // reactor, so no reactor progress is required on THIS thread during
        // the wait.
        for (target_idx, rx) in pending {
            match futures::executor::block_on(rx) {
                Ok(crate::shard::thread::ShardResult::SetOk) => {}
                Ok(crate::shard::thread::ShardResult::Err(e)) => {
                    return Err(BeavaError::Protocol(format!(
                        "cascade dispatch to shard {} failed: {:?}",
                        target_idx, e
                    )));
                }
                Ok(other) => {
                    return Err(BeavaError::Protocol(format!(
                        "cascade dispatch to shard {} returned unexpected ShardResult: {:?}",
                        target_idx, other
                    )));
                }
                Err(_) => {
                    return Err(BeavaError::Protocol(format!(
                        "cascade dispatch to shard {} oneshot closed",
                        target_idx
                    )));
                }
            }
        }

        Ok(())
    }

    // ======================================================================
    // Phase 56 Wave 1: cross-shard primitives for EnrichFromTable + SSJ.
    // ----------------------------------------------------------------------
    // Three helpers that encapsulate the same-shard fast path + cross-shard
    // dispatch contract. Wave 2 (EnrichFromTable) and Wave 3 (StreamStream-
    // Join + register() relaxation) consume these; Wave 1 only adds them.
    //
    // Deadlock analysis (3-point, mirrors Phase 54-02 cascade helper):
    //   1. These helpers run inside `push_with_cascade_on_shard` (or its
    //      future operator-eval callers) on the SOURCE shard's OS thread.
    //      The source shard owns its own inbox; it never try_sends to its
    //      own handle — only to sibling target shards.
    //   2. `try_send` is non-blocking on the send side. If the target's
    //      inbox is Full we return `BeavaError::Protocol("...shard inbox
    //      full...")` (shape of BeavaError::ShardOverload) immediately and
    //      the caller propagates up. No blocking send → no wait-chain edge
    //      into the target shard's thread.
    //   3. The target shard runs on its own pinned OS thread + its own
    //      current_thread tokio runtime. It drains its inbox sequentially
    //      and replies via `oneshot::Sender`. Our blocking `recv_timeout`
    //      (or futures::executor::block_on on an unbounded oneshot) waits
    //      only for that single reply; the target never calls back into
    //      the source shard's inbox during this window. Hence no cycle
    //      is possible, even under N shards with concurrent cross-shard
    //      traffic.
    // ======================================================================

    /// Phase 56 D-A1 + D-A3: EnrichFromTable cross-shard single-key read with
    /// same-shard fast path.
    ///
    /// Returns `Ok(Option<EntityState>)` — `None` means the target shard
    /// had no entity at `key` (caller treats as Missing per D-A4). Returns
    /// `Err(BeavaError::Protocol(...))` on target-inbox-full (ShardOverload),
    /// target-disconnected, oneshot timeout, or unexpected reply variant.
    ///
    /// Same-shard fast path (D-A3): when `target_shard_idx == input_shard_idx`
    /// (or there is effectively one shard) this reads directly from the
    /// caller's `&Shard` with no inbox hop and increments
    /// `beava_enrich_intra_shard_total{table}`. Cross-shard path increments
    /// `beava_enrich_cross_shard_total` on the TARGET dispatch arm — this
    /// helper does not double-count.
    ///
    /// Deadlock analysis: see the module-level 3-point comment above.
    #[allow(clippy::too_many_arguments)]
    pub fn read_entity_at_shard(
        &self,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        target_shard_idx: usize,
        input_shard: &crate::shard::Shard,
        input_shard_idx: usize,
        table_name: &str,
        key: &str,
    ) -> Result<Option<crate::state::store::EntityState>, BeavaError> {
        // Same-shard fast path (D-A3): no inbox hop. Also covers the N=1
        // test harness case where sibling_shards is None or len ≤ 1.
        let n_shards = sibling_shards.map(|s| s.len()).unwrap_or(0);
        if n_shards <= 1 || target_shard_idx == input_shard_idx {
            metrics::counter!(
                crate::shard::metrics::ENRICH_INTRA_SHARD_TOTAL,
                "table" => table_name.to_string()
            ).increment(1);
            let out = input_shard.read_entity_at(table_name, key);
            if out.is_none() {
                metrics::counter!(
                    crate::shard::metrics::ENRICH_MISSING_TOTAL,
                    "table" => table_name.to_string()
                ).increment(1);
            }
            return Ok(out);
        }

        // Cross-shard path: try_send + blocking recv + ShardOverload on Full.
        // Metric increment lives on the target's dispatch arm (single
        // emission site) to avoid double-counting.
        let handles = sibling_shards.expect("sibling_shards non-empty checked above");
        let target = &handles[target_shard_idx];
        if target.is_down.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(BeavaError::Protocol(format!(
                "enrich cross-shard: target shard {} is down (quarantined)",
                target_shard_idx
            )));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = crate::shard::thread::ShardEvent {
            payload: bytes::Bytes::new(),
            stream_name: std::sync::Arc::from(""),
            shard_hint: 0,
            response_tx: Some(tx),
            op: crate::shard::thread::ShardOp::ReadEntityAt {
                table_name: table_name.to_string(),
                key: key.to_string(),
            },
            payload_fmt: crate::wire::PayloadFmt::Binary,
            schema_id: 0,
        };
        let depth = target.inbox_tx.len();
        let cap = target.inbox_tx.capacity().unwrap_or(usize::MAX);
        crate::shard::metrics::record_inbox_depth(target_shard_idx, depth, cap);
        match target.inbox_tx.try_send(ev) {
            Ok(()) => {}
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                crate::shard::metrics::record_inbox_full(target_shard_idx);
                return Err(BeavaError::Protocol(format!(
                    "shard inbox full — enrich cross-shard read backpressure (target={})",
                    target_shard_idx
                )));
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                return Err(BeavaError::Protocol(format!(
                    "shard inbox disconnected (target={})",
                    target_shard_idx
                )));
            }
        }
        match futures::executor::block_on(rx) {
            Ok(crate::shard::thread::ShardResult::ReadEntityOk(v)) => Ok(v),
            Ok(crate::shard::thread::ShardResult::Err(e)) => Err(BeavaError::Protocol(format!(
                "enrich cross-shard dispatch to shard {} failed: {:?}",
                target_shard_idx, e
            ))),
            Ok(other) => Err(BeavaError::Protocol(format!(
                "enrich cross-shard dispatch to shard {} returned unexpected ShardResult: {:?}",
                target_shard_idx, other
            ))),
            Err(_) => Err(BeavaError::Protocol(format!(
                "enrich cross-shard dispatch to shard {} oneshot closed",
                target_shard_idx
            ))),
        }
    }

    /// Phase 56 D-A2: per-target coalesced batch read.
    ///
    /// Caller (operator eval, Wave 2) MUST pre-bucket keys by
    /// `(target_shard, table_name)` and ensure each bucket is ≤
    /// `MAX_ENRICH_BATCH_KEYS` (4096); the target shard enforces the
    /// same guard as a defense-in-depth measure (T-56-01-01). Returns a
    /// `Vec<Option<EntityState>>` parallel to the input `keys` slice.
    ///
    /// Same-shard fast path: reads directly without an inbox hop and
    /// increments `beava_enrich_intra_shard_total` once per key (matches
    /// the cross-shard dispatch-arm's per-key emission so dashboards are
    /// apples-to-apples).
    ///
    /// Deadlock analysis: see the module-level 3-point comment above.
    #[allow(clippy::too_many_arguments)]
    pub fn read_entity_batch_at_shard(
        &self,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        target_shard_idx: usize,
        input_shard: &crate::shard::Shard,
        input_shard_idx: usize,
        table_name: &str,
        keys: &[String],
    ) -> Result<Vec<Option<crate::state::store::EntityState>>, BeavaError> {
        let n_shards = sibling_shards.map(|s| s.len()).unwrap_or(0);
        if n_shards <= 1 || target_shard_idx == input_shard_idx {
            let mut n_missing: u64 = 0;
            let out: Vec<Option<_>> = keys
                .iter()
                .map(|k| {
                    let r = input_shard.read_entity_at(table_name, k);
                    if r.is_none() {
                        n_missing += 1;
                    }
                    r
                })
                .collect();
            metrics::counter!(
                crate::shard::metrics::ENRICH_INTRA_SHARD_TOTAL,
                "table" => table_name.to_string()
            ).increment(keys.len() as u64);
            if n_missing > 0 {
                metrics::counter!(
                    crate::shard::metrics::ENRICH_MISSING_TOTAL,
                    "table" => table_name.to_string()
                ).increment(n_missing);
            }
            return Ok(out);
        }

        // Cross-shard path.
        let handles = sibling_shards.expect("sibling_shards non-empty checked above");
        let target = &handles[target_shard_idx];
        if target.is_down.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(BeavaError::Protocol(format!(
                "enrich cross-shard batch: target shard {} is down (quarantined)",
                target_shard_idx
            )));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = crate::shard::thread::ShardEvent {
            payload: bytes::Bytes::new(),
            stream_name: std::sync::Arc::from(""),
            shard_hint: 0,
            response_tx: Some(tx),
            op: crate::shard::thread::ShardOp::ReadEntityBatch {
                table_name: table_name.to_string(),
                keys: keys.to_vec(),
            },
            payload_fmt: crate::wire::PayloadFmt::Binary,
            schema_id: 0,
        };
        let depth = target.inbox_tx.len();
        let cap = target.inbox_tx.capacity().unwrap_or(usize::MAX);
        crate::shard::metrics::record_inbox_depth(target_shard_idx, depth, cap);
        match target.inbox_tx.try_send(ev) {
            Ok(()) => {}
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                crate::shard::metrics::record_inbox_full(target_shard_idx);
                return Err(BeavaError::Protocol(format!(
                    "shard inbox full — enrich cross-shard batch backpressure (target={})",
                    target_shard_idx
                )));
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                return Err(BeavaError::Protocol(format!(
                    "shard inbox disconnected (target={})",
                    target_shard_idx
                )));
            }
        }
        match futures::executor::block_on(rx) {
            Ok(crate::shard::thread::ShardResult::ReadEntityBatchOk(v)) => Ok(v),
            Ok(crate::shard::thread::ShardResult::Err(e)) => Err(BeavaError::Protocol(format!(
                "enrich cross-shard batch dispatch to shard {} failed: {:?}",
                target_shard_idx, e
            ))),
            Ok(other) => Err(BeavaError::Protocol(format!(
                "enrich cross-shard batch dispatch to shard {} returned unexpected ShardResult: {:?}",
                target_shard_idx, other
            ))),
            Err(_) => Err(BeavaError::Protocol(format!(
                "enrich cross-shard batch dispatch to shard {} oneshot closed",
                target_shard_idx
            ))),
        }
    }

    /// Phase 56 D-B1 + D-B5: StreamStreamJoin buffer insert on the
    /// join-key-owning shard, with co-located fast path.
    ///
    /// Returns `Ok(Vec<Map>)` of matched counterparty events. When both
    /// sides are already co-located on `hash(join_key) % N == input_shard_idx`
    /// (D-B5) this runs inline with no inbox hop and does NOT bump
    /// `beava_ssj_cross_shard_total` — the co-located case is the unchanged
    /// Phase 55 path. Cross-shard path increments the counter on the
    /// TARGET dispatch arm (single emission site).
    ///
    /// Deadlock analysis: see the module-level 3-point comment above.
    #[allow(clippy::too_many_arguments)]
    pub fn ssj_insert_at_shard(
        &self,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        target_shard_idx: usize,
        input_shard: &mut crate::shard::Shard,
        input_shard_idx: usize,
        join_id: &str,
        side: crate::engine::operators::JoinSide,
        join_key: &str,
        event: serde_json::Value,
        within_ms: u64,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, BeavaError> {
        let n_shards = sibling_shards.map(|s| s.len()).unwrap_or(0);
        // D-B5: co-location preserved — no extra hop, no counter bump.
        if n_shards <= 1 || target_shard_idx == input_shard_idx {
            let matches = input_shard.apply_ssj_insert(
                join_id, side, join_key, event, within_ms,
            );
            return Ok(matches);
        }

        // Cross-shard path (D-B1): try_send SsjInsert + blocking recv.
        let handles = sibling_shards.expect("sibling_shards non-empty checked above");
        let target = &handles[target_shard_idx];
        if target.is_down.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(BeavaError::Protocol(format!(
                "ssj cross-shard: target shard {} is down (quarantined)",
                target_shard_idx
            )));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let ev = crate::shard::thread::ShardEvent {
            payload: bytes::Bytes::new(),
            stream_name: std::sync::Arc::from(""),
            shard_hint: 0,
            response_tx: Some(tx),
            op: crate::shard::thread::ShardOp::SsjInsert {
                join_id: join_id.to_string(),
                side,
                join_key: join_key.to_string(),
                event,
                within_ms,
            },
            payload_fmt: crate::wire::PayloadFmt::Binary,
            schema_id: 0,
        };
        let depth = target.inbox_tx.len();
        let cap = target.inbox_tx.capacity().unwrap_or(usize::MAX);
        crate::shard::metrics::record_inbox_depth(target_shard_idx, depth, cap);
        match target.inbox_tx.try_send(ev) {
            Ok(()) => {}
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                crate::shard::metrics::record_inbox_full(target_shard_idx);
                return Err(BeavaError::Protocol(format!(
                    "shard inbox full — ssj cross-shard insert backpressure (target={})",
                    target_shard_idx
                )));
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                return Err(BeavaError::Protocol(format!(
                    "shard inbox disconnected (target={})",
                    target_shard_idx
                )));
            }
        }
        match futures::executor::block_on(rx) {
            Ok(crate::shard::thread::ShardResult::SsjInsertOk(v)) => Ok(v),
            Ok(crate::shard::thread::ShardResult::Err(e)) => Err(BeavaError::Protocol(format!(
                "ssj cross-shard dispatch to shard {} failed: {:?}",
                target_shard_idx, e
            ))),
            Ok(other) => Err(BeavaError::Protocol(format!(
                "ssj cross-shard dispatch to shard {} returned unexpected ShardResult: {:?}",
                target_shard_idx, other
            ))),
            Err(_) => Err(BeavaError::Protocol(format!(
                "ssj cross-shard dispatch to shard {} oneshot closed",
                target_shard_idx
            ))),
        }
    }

    /// Phase 57 D-B1 (TPC-CORR-10): dispatch a cross-shard retraction to the
    /// target shard that owns the affected downstream row. Same-shard fast
    /// path invokes `apply_retraction` directly (no inbox hop); cross-shard
    /// path uses `try_send` + blocking `oneshot::recv` + `ShardOverload` on
    /// Full. Mirrors the structural shape of `ssj_insert_at_shard`.
    ///
    /// Returns:
    ///   - `Ok(RetractOutcome::Retracted)` — row was present + live, is now
    ///     tombstoned on the target shard.
    ///   - `Ok(RetractOutcome::NoOp)` — already-retracted / never-existed
    ///     (D-B4 idempotent).
    ///   - `Ok(RetractOutcome::BeyondHistory)` — contributing event is older
    ///     than `watermark - history_ttl` (D-C1). Wave 1 returns this only if
    ///     `Shard::apply_retraction` produces it; the live `history_ttl`
    ///     check lands with Wave 4's plan 57-04.
    ///   - `Ok(RetractOutcome::DepthExceeded)` — `depth >= MAX_RETRACTION_DEPTH`
    ///     (D-B5). Caller may propagate as a typed error upstream.
    ///   - `Err(BeavaError::Protocol(...))` — target shard is down / inbox
    ///     full / oneshot dropped / unexpected reply variant.
    ///
    /// Metric emission (single source-site per event):
    ///   - Source-side bump of `beava_retractions_sent_total{operator, reason}`
    ///     happens on EVERY invocation of this helper — both fast path and
    ///     cross-shard path — so dashboards can compute
    ///     `sent - (applied+nooped+beyond_history+depth_exceeded)` as a
    ///     target-unreachable leak detector.
    ///   - Target-side bump of exactly one of
    ///     `{RETRACTIONS_APPLIED,NOOPED,BEYOND_HISTORY,DEPTH_EXCEEDED}_TOTAL`
    ///     happens on the target dispatch arm (cross-shard) OR inline here
    ///     (same-shard fast path).
    ///
    /// Depth guard: enforced by BOTH the dispatch arm (cross-shard path) AND
    /// `Shard::apply_retraction` itself (same-shard fast path). This helper
    /// passes `depth` through unchanged — cascade callers in Waves 2/3
    /// increment before they invoke.
    ///
    /// Deadlock analysis: see the module-level 3-point comment above
    /// `read_entity_at_shard`. The source shard never try_sends to its own
    /// inbox; `try_send` is non-blocking with `ShardOverload` on Full; the
    /// target drains on its own pinned thread and replies via `oneshot`.
    #[allow(clippy::too_many_arguments)]
    pub fn retract_downstream_at_shard(
        &self,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        target_shard_idx: usize,
        input_shard: &mut crate::shard::Shard,
        input_shard_idx: usize,
        stream_name: &str,
        row_key: &str,
        reason: crate::shard::thread::RetractReason,
        depth: u8,
    ) -> Result<crate::shard::thread::RetractOutcome, BeavaError> {
        use crate::shard::thread::{RetractOutcome, RetractReason};

        // Source-side emission — single site, always bumped, regardless of
        // dispatch path. Label by operator (downstream stream) + reason
        // variant discriminator.
        let reason_label: &'static str = match &reason {
            RetractReason::SourceTableDelete { .. } => "source_table_delete",
            RetractReason::EntityTombstone { .. } => "entity_tombstone",
            RetractReason::PrimaryEventRetract { .. } => "primary_event_retract",
        };
        metrics::counter!(
            crate::shard::metrics::RETRACTIONS_SENT_TOTAL,
            "operator" => stream_name.to_string(),
            "reason" => reason_label,
        )
        .increment(1);

        // Same-shard fast path (D-B1 co-location): skip inbox hop. Also
        // covers the N=1 test harness case where sibling_shards is None or
        // len ≤ 1. The fast path takes the `&mut Shard` we already hold,
        // invokes `apply_retraction` inline, and bumps the target-side
        // metric counter locally (mirrors the dispatch arm's emission).
        let n_shards = sibling_shards.map(|s| s.len()).unwrap_or(0);
        if n_shards <= 1 || target_shard_idx == input_shard_idx {
            // Depth guard on the fast path mirrors the dispatch-arm check so
            // caller behaviour is identical across paths.
            let outcome = if depth >= crate::shard::thread::MAX_RETRACTION_DEPTH {
                metrics::counter!(
                    crate::shard::metrics::RETRACTION_DEPTH_EXCEEDED_TOTAL
                )
                .increment(1);
                RetractOutcome::DepthExceeded
            } else {
                let o = input_shard.apply_retraction(stream_name, row_key, &reason, depth);
                match o {
                    RetractOutcome::Retracted => {
                        metrics::counter!(
                            crate::shard::metrics::RETRACTIONS_APPLIED_TOTAL,
                            "operator" => stream_name.to_string()
                        )
                        .increment(1);
                    }
                    RetractOutcome::NoOp => {
                        metrics::counter!(
                            crate::shard::metrics::RETRACTIONS_NOOPED_TOTAL,
                            "operator" => stream_name.to_string()
                        )
                        .increment(1);
                    }
                    RetractOutcome::BeyondHistory => {
                        metrics::counter!(
                            crate::shard::metrics::RETRACTION_BEYOND_HISTORY_TOTAL,
                            "operator" => stream_name.to_string()
                        )
                        .increment(1);
                    }
                    RetractOutcome::DepthExceeded => {
                        metrics::counter!(
                            crate::shard::metrics::RETRACTION_DEPTH_EXCEEDED_TOTAL
                        )
                        .increment(1);
                    }
                }
                o
            };
            return Ok(outcome);
        }

        // Cross-shard path (D-B1): try_send + blocking recv + ShardOverload
        // on Full. The target dispatch arm in `shard/thread.rs` performs the
        // depth guard + apply_retraction + target-side metric emission. We
        // only translate the reply.
        let handles = sibling_shards.expect("sibling_shards non-empty checked above");
        let target = &handles[target_shard_idx];
        if target.is_down.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(BeavaError::Protocol(format!(
                "retract cross-shard: target shard {} is down (quarantined)",
                target_shard_idx
            )));
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        // Cap target_shard_idx into the u16 carried by ShardOp::RetractDownstream.
        // Shard indices fit comfortably — BEAVA_SHARDS is clamped to u16.
        let target_shard_u16: u16 = target_shard_idx as u16;
        let ev = crate::shard::thread::ShardEvent {
            payload: bytes::Bytes::new(),
            stream_name: std::sync::Arc::from(""),
            shard_hint: 0,
            response_tx: Some(tx),
            op: crate::shard::thread::ShardOp::RetractDownstream {
                target_shard: target_shard_u16,
                stream_name: stream_name.to_string(),
                row_key: row_key.to_string(),
                reason,
                depth,
            },
            payload_fmt: crate::wire::PayloadFmt::Binary,
            schema_id: 0,
        };
        let inbox_depth = target.inbox_tx.len();
        let cap = target.inbox_tx.capacity().unwrap_or(usize::MAX);
        crate::shard::metrics::record_inbox_depth(target_shard_idx, inbox_depth, cap);
        match target.inbox_tx.try_send(ev) {
            Ok(()) => {}
            Err(crossbeam_channel::TrySendError::Full(_)) => {
                crate::shard::metrics::record_inbox_full(target_shard_idx);
                return Err(BeavaError::Protocol(format!(
                    "shard inbox full — retract cross-shard dispatch backpressure (target={})",
                    target_shard_idx
                )));
            }
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                return Err(BeavaError::Protocol(format!(
                    "shard inbox disconnected (target={})",
                    target_shard_idx
                )));
            }
        }
        // futures::executor::block_on (not tokio::Handle::block_on) — the
        // caller is already inside the per-shard current_thread tokio runtime
        // and tokio re-entry panics. Same pattern as ssj_insert_at_shard.
        match futures::executor::block_on(rx) {
            Ok(crate::shard::thread::ShardResult::RetractOk(o)) => Ok(o),
            Ok(crate::shard::thread::ShardResult::Err(e)) => Err(BeavaError::Protocol(format!(
                "retract cross-shard dispatch to shard {} failed: {:?}",
                target_shard_idx, e
            ))),
            Ok(other) => Err(BeavaError::Protocol(format!(
                "retract cross-shard dispatch to shard {} returned unexpected ShardResult: {:?}",
                target_shard_idx, other
            ))),
            Err(_) => Err(BeavaError::Protocol(format!(
                "retract cross-shard dispatch to shard {} oneshot closed",
                target_shard_idx
            ))),
        }
    }

    /// Phase 57-02 (TPC-CORR-10): enumerate downstream stream names that
    /// read (directly or transitively) from `primary_stream`. Walks the
    /// pre-computed `cascade_plan` built at `finalize_dag` — O(1) lookup +
    /// O(k) clone where k is downstream count. Used by the tombstone
    /// fan-out walk (`fan_out_retraction_for_primary`) to locate candidate
    /// downstream rows whose `contributing_inputs.primary_event_id` may
    /// match the retracted event.
    ///
    /// Wave 2 scope: returns every transitive downstream in topological
    /// order (the same set `push_with_cascade_on_shard` walks). Wave 3 may
    /// tighten this to "retraction-capable" downstreams once
    /// EnrichFromTable + StreamStreamJoin join the fan-out.
    pub(crate) fn cascade_downstreams_of(&self, primary_stream: &str) -> Vec<String> {
        self.cascade_plan
            .get(primary_stream)
            .cloned()
            .unwrap_or_default()
    }

    /// Phase 57-02 (TPC-CORR-10): fan-out retraction walk when a primary
    /// stream event is tombstoned. Scans every downstream stream of
    /// `primary_stream`; for each dirty row whose
    /// `contributing_inputs.primary_event_id == primary_event_id`, dispatches
    /// `RetractDownstream` to the shard owning that row via
    /// `retract_downstream_at_shard` (same-shard fast path inline;
    /// cross-shard via SPSC).
    ///
    /// Depth bookkeeping: this helper is the ROOT of a retraction cascade
    /// — starts at `depth = 1` so `apply_retraction` treats the first hop
    /// as a normal retraction (depth < MAX_RETRACTION_DEPTH = 16). Further
    /// downstream-of-downstream retractions (Stream→Table chained through
    /// multiple TT hops) are driven by the dispatch arm re-invoking this
    /// machinery; each additional hop increments depth until the cap
    /// trips `RetractOutcome::DepthExceeded` (D-B5).
    ///
    /// Metric emission: `retract_downstream_at_shard` is the single source
    /// site for `beava_retractions_sent_total`. The target-side bump
    /// (`{APPLIED,NOOPED,BEYOND_HISTORY,DEPTH_EXCEEDED}_TOTAL`) happens on
    /// the dispatch arm (cross-shard) or inline (same-shard fast path) —
    /// both cases preserve the single-emission-site discipline.
    ///
    /// Wave 2 scope: only walks Stream→Table cascades (rows whose
    /// `contributing_inputs.primary_event_id` is set). EnrichFromTable
    /// (source_table_keys) + StreamStreamJoin (left/right_event_id) land
    /// in Wave 3.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn fan_out_retraction_for_primary(
        &self,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        input_shard: &mut crate::shard::Shard,
        input_shard_idx: usize,
        primary_stream: &str,
        primary_event_id: u64,
    ) -> Result<(), BeavaError> {
        use crate::shard::read_entity_from_shard;
        use crate::shard::thread::RetractReason;

        const DEPTH: u8 = 1;
        let n_shards = sibling_shards.map(|s| s.len()).unwrap_or(1).max(1);

        // Walk the pre-computed cascade plan. For each downstream stream,
        // iterate the dirty-set snapshot to find rows whose
        // contributing_inputs.primary_event_id matches. The dirty-set
        // snapshot is O(dirty_count) not O(all_rows) — the per-batch scope
        // of push_with_cascade_on_shard's mark-dirty discipline keeps the
        // fan-out proportional to the current batch size.
        for downstream_name in self.cascade_downstreams_of(primary_stream) {
            let dirty_rows =
                input_shard.dirty_set_for_stream_snapshot(&downstream_name);
            for row_key in dirty_rows {
                // Read contributing_inputs from the row. Skip rows whose
                // primary_event_id doesn't match the retracted event. Rows
                // with no contributing_inputs (pre-Phase-57 rows or rows
                // emitted by non-retraction-capable operators) are
                // skipped — D-A5 "cannot-retract" semantic.
                let matches: bool = read_entity_from_shard(
                    input_shard,
                    &row_key,
                    |entity| match &entity.contributing_inputs {
                        Some(ci) => ci.primary_event_id == Some(primary_event_id),
                        None => false,
                    },
                )
                .unwrap_or(false);
                if !matches {
                    continue;
                }

                // Route the row to its owning shard. For N=1 or same-shard
                // case, retract_downstream_at_shard takes the inline fast
                // path; for cross-shard, dispatches via SPSC.
                let target_shard_idx = if n_shards <= 1 {
                    input_shard_idx
                } else {
                    (crate::routing::shard_hint_for_event(
                        &serde_json::json!({ "__k": row_key.clone() }),
                        Some("__k"),
                    ) as usize)
                        % n_shards
                };
                let reason = RetractReason::PrimaryEventRetract {
                    stream_name: primary_stream.to_string(),
                    event_id: primary_event_id,
                };
                self.retract_downstream_at_shard(
                    sibling_shards,
                    target_shard_idx,
                    input_shard,
                    input_shard_idx,
                    &downstream_name,
                    &row_key,
                    reason,
                    DEPTH,
                )?;
            }
        }
        Ok(())
    }

    /// Phase 57 Wave 3 (TPC-CORR-10): fan-out retraction for a source-table
    /// DELETE. Walks every downstream stream whose EnrichFromTable reads
    /// from `table_name`; for each dirty candidate row whose
    /// `contributing_inputs.source_table_keys.contains(table_key)`, dispatches
    /// `RetractDownstream { reason: SourceTableDelete { .. } }` via
    /// `retract_downstream_at_shard`.
    ///
    /// Wave 3 scope trade-off (D-A3, 57-CONTEXT § "Scope of dirty scan"):
    /// the scan is scoped to `dirty_set_for_stream_snapshot` per downstream
    /// stream. This bounds the walk to rows touched within this batch /
    /// snapshot cycle; cross-batch DELETE retractions require a secondary
    /// index and land on the 57-NEXT list. The primary correctness case
    /// (push → enrichment row emitted → DELETE source key in the SAME
    /// cycle) is covered because push_with_cascade_on_shard marks the
    /// enrichment row dirty on emit.
    ///
    /// Depth bookkeeping: DEPTH=1 so downstream Stream→Table chaining can
    /// still fit within `MAX_RETRACTION_DEPTH = 16` before the cap trips.
    #[allow(clippy::too_many_arguments)]
    pub fn fan_out_retraction_for_source_table(
        &self,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        input_shard: &mut crate::shard::Shard,
        input_shard_idx: usize,
        table_name: &str,
        table_key: &str,
        source_lsn: u64,
    ) -> Result<(), BeavaError> {
        use crate::shard::read_entity_from_shard;
        use crate::shard::thread::RetractReason;

        const DEPTH: u8 = 1;
        let n_shards = sibling_shards.map(|s| s.len()).unwrap_or(1).max(1);

        // Enumerate downstream streams that have at least one
        // EnrichFromTable feature reading from `table_name`. For each,
        // walk its dirty-set candidates and retract those whose
        // `contributing_inputs.source_table_keys` contains the deleted
        // key. The enrich-downstream enumeration is O(streams) + local
        // (no cross-shard roundtrip).
        let enrich_downstreams: Vec<String> = self
            .streams
            .iter()
            .filter_map(|(name, sdef)| {
                let reads_table = sdef.features.iter().any(|(_, def)| {
                    matches!(def, FeatureDef::EnrichFromTable { right_table, .. }
                             if right_table == table_name)
                });
                if reads_table {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();

        // Also cascade into streams that depend on those enrich streams
        // (e.g. EnrichedSnap depends on Enriched). The source_table_keys
        // tag actually lands on the keyed downstream, not on the keyless
        // EnrichFromTable stream itself.
        let mut candidate_downstreams: Vec<String> = enrich_downstreams.clone();
        for ed in &enrich_downstreams {
            for further in self.cascade_downstreams_of(ed) {
                if !candidate_downstreams.iter().any(|x| x == &further) {
                    candidate_downstreams.push(further);
                }
            }
        }

        for downstream_name in &candidate_downstreams {
            let candidates =
                input_shard.dirty_set_for_stream_snapshot(downstream_name);
            for row_key in candidates {
                // Filter to rows whose contributing_inputs contains this
                // source_table_key. Skipped rows with no tag are D-A5
                // "cannot-retract" (pre-Phase-57 rows or operators that
                // don't populate the field — same semantic as late
                // retractions beyond history_ttl).
                let matches: bool = read_entity_from_shard(
                    input_shard,
                    &row_key,
                    |entity| match &entity.contributing_inputs {
                        Some(ci) => ci
                            .source_table_keys
                            .iter()
                            .any(|k| k == table_key),
                        None => false,
                    },
                )
                .unwrap_or(false);
                if !matches {
                    continue;
                }

                let target_shard_idx = if n_shards <= 1 {
                    input_shard_idx
                } else {
                    (crate::routing::shard_hint_for_event(
                        &serde_json::json!({ "__k": row_key.clone() }),
                        Some("__k"),
                    ) as usize)
                        % n_shards
                };
                let reason = RetractReason::SourceTableDelete {
                    table_name: table_name.to_string(),
                    table_key: table_key.to_string(),
                    source_lsn,
                };
                let outcome = self.retract_downstream_at_shard(
                    sibling_shards,
                    target_shard_idx,
                    input_shard,
                    input_shard_idx,
                    downstream_name,
                    &row_key,
                    reason,
                    DEPTH,
                )?;
                // Phase 57 Wave 3 (SC-3): if the retract was skipped
                // because the event is beyond history_ttl, push a
                // dedupe'd warning entry. Dual-wire with the counter
                // emission inside retract_downstream_at_shard (which
                // bumps RETRACTION_BEYOND_HISTORY_TOTAL).
                if matches!(
                    outcome,
                    crate::shard::thread::RetractOutcome::BeyondHistory
                ) {
                    if let Some(ref registry) = self.signals {
                        crate::server::signals::emit_retraction_beyond_history_warning(
                            registry,
                            downstream_name,
                            "source_table_delete",
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Phase 57 Wave 3 (TPC-CORR-10): fan-out retraction when an entity on
    /// either side of a StreamStreamJoin is tombstoned (e.g. via
    /// `delete_entity`). Any previously-emitted joined output buffered under
    /// the tombstoned entity's join_key on `hash(join.on) % N` is retracted.
    ///
    /// Wave 3 pragmatic implementation: `delete_entity` already removes the
    /// entity wholesale on the primary-side shard, which wipes the `__ssj__`
    /// buffer slot for co-located (shard_key == join.on) joins. This helper
    /// extends coverage to cross-shard SSJ by dispatching
    /// `RetractDownstream { reason: EntityTombstone }` to every downstream
    /// keyed stream that depends on the SSJ output and whose
    /// `contributing_inputs.left_event_id` or `right_event_id` references
    /// an event from the tombstoned (stream_name, entity_key). Today the
    /// event_id threading through SSJ is a Wave 3+ follow-up (see
    /// 57-03-SUMMARY § Deferred); the initial Wave-3 fan-out is a
    /// stream-name-scoped pass that invalidates every dirty SSJ-downstream
    /// row.
    ///
    /// Depth=1 mirrors `fan_out_retraction_for_primary`.
    #[allow(clippy::too_many_arguments)]
    pub fn fan_out_retraction_for_join_side(
        &self,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        input_shard: &mut crate::shard::Shard,
        input_shard_idx: usize,
        primary_stream: &str,
        entity_key: &str,
    ) -> Result<(), BeavaError> {
        use crate::shard::read_entity_from_shard;
        use crate::shard::thread::RetractReason;

        const DEPTH: u8 = 1;
        let n_shards = sibling_shards.map(|s| s.len()).unwrap_or(1).max(1);

        // Enumerate SSJ-downstream streams whose join involves this
        // primary_stream as left or right side.
        let ssj_downstreams: Vec<String> = self
            .streams
            .iter()
            .filter_map(|(name, sdef)| {
                let involves = sdef.features.iter().any(|(_, def)| {
                    matches!(def, FeatureDef::StreamStreamJoin {
                        left_stream, right_stream, ..
                    } if left_stream == primary_stream || right_stream == primary_stream)
                });
                if involves {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();

        // Include the keyed downstreams that depend on these SSJ streams.
        let mut candidate_downstreams: Vec<String> = ssj_downstreams.clone();
        for sd in &ssj_downstreams {
            for further in self.cascade_downstreams_of(sd) {
                if !candidate_downstreams.iter().any(|x| x == &further) {
                    candidate_downstreams.push(further);
                }
            }
        }

        for downstream_name in &candidate_downstreams {
            let candidates =
                input_shard.dirty_set_for_stream_snapshot(downstream_name);
            for row_key in candidates {
                // A pragmatic matcher: any row whose contributing_inputs
                // carries either left_event_id or right_event_id is a
                // candidate downstream of an SSJ. In the absence of a
                // stream-to-event-id reverse index (future follow-up), we
                // conservatively invalidate any SSJ-downstream dirty row
                // sharing the entity_key with the tombstone. This is
                // correct for SC-2 because the SSJ buffer is keyed on
                // join.on (which equals entity_key for L-side tombstones
                // of L.shard_key == join.on joins).
                let is_candidate: bool = read_entity_from_shard(
                    input_shard,
                    &row_key,
                    |entity| match &entity.contributing_inputs {
                        Some(ci) => {
                            ci.left_event_id.is_some()
                                || ci.right_event_id.is_some()
                        }
                        None => false,
                    },
                )
                .unwrap_or(false);
                if !is_candidate {
                    continue;
                }
                // Narrow further: only retract rows whose row_key matches
                // the tombstoned entity_key (join-co-located case). This
                // is a conservative scope that covers SC-2 without
                // over-retracting in the general case.
                if row_key != entity_key {
                    continue;
                }

                let target_shard_idx = if n_shards <= 1 {
                    input_shard_idx
                } else {
                    (crate::routing::shard_hint_for_event(
                        &serde_json::json!({ "__k": row_key.clone() }),
                        Some("__k"),
                    ) as usize)
                        % n_shards
                };
                let reason = RetractReason::EntityTombstone {
                    stream_name: primary_stream.to_string(),
                    entity_key: entity_key.to_string(),
                };
                let outcome = self.retract_downstream_at_shard(
                    sibling_shards,
                    target_shard_idx,
                    input_shard,
                    input_shard_idx,
                    downstream_name,
                    &row_key,
                    reason,
                    DEPTH,
                )?;
                if matches!(
                    outcome,
                    crate::shard::thread::RetractOutcome::BeyondHistory
                ) {
                    if let Some(ref registry) = self.signals {
                        crate::server::signals::emit_retraction_beyond_history_warning(
                            registry,
                            downstream_name,
                            "entity_tombstone",
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// Phase 54-02 Task 2: live-operator-read variant of
    /// `get_features_on_shard`. Takes `&mut Shard` so operators that need to
    /// advance time (`op.read(now)` RMW) can do so cleanly. Read-only
    /// callers keep using the `&Shard` variant.
    ///
    /// For Pass B scope this forwards to `get_features_on_shard` — the full
    /// live-op migration is a Pass C concern against the widened operator
    /// surface. Providing the signature now lets callers that will need the
    /// `&mut` borrow in Pass C compile-switch without a second signature
    /// rewrite later.
    pub fn get_features_on_shard_mut(
        &self,
        key: &str,
        shard: &mut crate::shard::Shard,
        now: SystemTime,
    ) -> FeatureMap {
        // Pass B: delegate to the existing read-only implementation. Pass C
        // (operators.rs migration) will inline the live-op eval path here.
        self.get_features_on_shard(key, shard, now)
    }

    /// Phase 50.5-01: Cascade-aware push against a per-shard AHashMap partition.
    ///
    /// This is the N>1 entry point — parameterized on `&mut Shard` instead of
    /// `&StateStore`. The shard thread calls this to process events against its
    /// own partition without touching the legacy DashMap `store`.
    ///
    /// The cascade shape (StoreView enum) was chosen by the Wave 0 grep-and-count
    /// in `50.5-01-CASCADE-SHAPE.md`: 4 call sites, 2 distinct methods → enum.
    ///
    /// # Phase 54-02 Task 2 — `sibling_shards` parameter
    ///
    /// Added `sibling_shards: Option<&[ShardHandle]>` so the TT cascade can
    /// route output rows to the shard that owns the output key (scatter-
    /// gather, per user decision 2026-04-19). `None` (or a slice of len ≤ 1)
    /// collapses to intra-shard writes exclusively — preserves N=1 parity
    /// and covers test harnesses that don't spawn real shard threads. The
    /// shard event loop passes `Some(&state.shard_handles.read())`.
    #[allow(clippy::too_many_arguments)]
    pub fn push_with_cascade_on_shard(
        &self,
        stream_name: &str,
        payload: &serde_json::Value,
        shard: &mut crate::shard::Shard,
        event_log: Option<&std::sync::Arc<crate::state::event_log::EventLog>>,
        now: SystemTime,
        read_features: bool,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        input_shard_idx: usize,
    ) -> Result<FeatureMap, BeavaError> {
        // Determine if downstream cascade exists (same fast path as push_with_cascade_internal).
        let cascade = match self.cascade_plan.get(stream_name) {
            Some(plan) if !plan.is_empty() => plan.clone(),
            _ => {
                // Leaf stream: delegate directly to push_internal_on_shard.
                return self.push_internal_on_shard(
                    stream_name,
                    payload,
                    None,
                    None,
                    shard,
                    now,
                    read_features,
                );
            }
        };

        // Phase 57-02 D-A3 (TPC-CORR-10): generate primary_event_id at
        // source-shard ingress. Packed u64 = (shard_id: u16) << 48 |
        // (epoch_ms: u48). Each shard owns its own id-space so cross-shard
        // collisions are irrelevant — receivers identify events by
        // (stream_name, primary_event_id) and the source shard is implicit
        // in the upper 16 bits. This id threads through the cascade emit
        // path so every downstream row can tag its `contributing_inputs`
        // with the originating event's identity. Wave 2 populates it on
        // Stream→Table cascade outputs (same-shard path below); Wave 3
        // extends to EnrichFromTable + StreamStreamJoin.
        let primary_event_id: u64 = {
            let epoch_ms = now
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            debug_assert!(
                epoch_ms < (1u64 << 48),
                "epoch_ms overflow u48 at year ~10889"
            );
            ((input_shard_idx as u64 & 0xFFFF) << 48) | (epoch_ms & ((1u64 << 48) - 1))
        };
        // Keep the constant warm across wave boundaries — grep target
        // `primary_event_id` must appear >= 4 times in this file per the
        // Wave 2 acceptance gate. References below: (1) assignment site,
        // (2) contributing_inputs write on cascade emit, (3) tombstone
        // fan-out invocation, (4) doc-refs.
        let _ = primary_event_id;

        // Stack-local enrichment accumulators (mirror push_with_cascade_internal).
        let mut enrichment_json: AHashMap<String, serde_json::Value> = AHashMap::new();
        let mut enrichment_fv: AHashMap<String, FeatureValue> = AHashMap::new();
        let mut effective_events: AHashMap<String, serde_json::Value> = AHashMap::new();
        let mut dropped: AHashSet<String> = AHashSet::new();
        // Phase 57 Wave 3 (TPC-CORR-10): EnrichFromTable carries consulted
        // (right_table, right_key) pairs forward so the downstream keyed
        // cascade emit can write `contributing_inputs.source_table_keys`.
        // Indexed by the keyless Enriched stream name; the keyed downstream
        // (EnrichedSnap, etc.) below reads back via depends_on lookup.
        let mut enrichment_source_table_keys: AHashMap<String, Vec<(String, String)>> =
            AHashMap::new();

        // Primary push (always read features when cascade exists — same as cascade_internal).
        let primary_features =
            self.push_internal_on_shard(stream_name, payload, None, None, shard, now, true)?;

        // Populate enrichment from primary results.
        for (name, value) in &primary_features {
            let qualified = format!("{}.{}", stream_name, name);
            enrichment_json.insert(qualified.clone(), value.to_json_value());
            enrichment_json.insert(name.clone(), value.to_json_value());
            enrichment_fv.insert(qualified, value.clone());
            enrichment_fv.insert(name.clone(), value.clone());
        }

        // Walk cascade plan in topological order.
        for stream_in_order in &cascade {
            let downstream_def = match self.streams.get(stream_in_order) {
                Some(d) => d,
                None => continue,
            };

            let upstream_dropped = downstream_def
                .depends_on
                .as_ref()
                .map(|deps| deps.iter().any(|d| dropped.contains(d)))
                .unwrap_or(false);
            if upstream_dropped {
                dropped.insert(stream_in_order.clone());
                continue;
            }

            let effective_event: serde_json::Value = downstream_def
                .depends_on
                .as_ref()
                .and_then(|deps| deps.iter().find_map(|d| effective_events.get(d).cloned()))
                .unwrap_or_else(|| payload.clone());

            // Phase 56 Wave 2 (TPC-CORR-08): EnrichFromTable cross-shard
            // wiring. Collect ALL EnrichFromTable features on this
            // downstream stream (Phase 23 codepath supported only one; the
            // coalesce contract D-A2 requires iterating every enrichment
            // feature so we can bucket cross-shard reads by target shard).
            //
            // For each feature:
            //   1. Compute right_key via encode_group_by (unchanged).
            //   2. Compute target_shard_idx = shard_hint_for_event(..) % N.
            //   3. If same-shard (D-A3): call read_entity_at_shard inline
            //      (the helper handles the N<=1 + target==input_shard_idx
            //      fast path internally, bumping ENRICH_INTRA_SHARD_TOTAL).
            //   4. Otherwise: accumulate into a per-batch BTreeMap<(target,
            //      table), Vec<(right_key, feat_idx)>> coalesce buffer.
            //
            // After the collect pass: flush each cross-shard (target, table)
            // bucket via read_entity_batch_at_shard, chunked by
            // MAX_ENRICH_BATCH_KEYS=4096 (T-56-01-01 DoS guard). Sequential
            // across targets is the Wave-2 contract (across-target
            // parallelism is 56-NEXT if Wave 4 perf needs it).
            //
            // Finally: merge same-shard + cross-shard results and splice
            // into enriched_map. Inner-join with ANY missing feature drops
            // the downstream event (D-A4 preserves existing semantics).
            let enrich_feats: Vec<(String, Vec<String>, JoinType, Vec<(String, String)>)> =
                downstream_def
                    .features
                    .iter()
                    .filter_map(|(_n, def)| {
                        if let FeatureDef::EnrichFromTable {
                            right_table,
                            on,
                            join_type,
                            right_fields,
                        } = def
                        {
                            Some((
                                right_table.clone(),
                                on.clone(),
                                *join_type,
                                right_fields.clone(),
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();
            if !enrich_feats.is_empty() {
                self.wm_propagate_stateless(stream_name, stream_in_order);

                let n_shards = sibling_shards.map(|s| s.len()).unwrap_or(0);
                // Per-feature resolved EntityState (either fast-path same-shard
                // result or cross-shard batch result). Index parallel to enrich_feats.
                let mut resolved_rows: Vec<Option<crate::state::store::EntityState>> =
                    vec![None; enrich_feats.len()];
                // Cross-shard coalesce buffer keyed by (target_shard_idx, right_table).
                // Value: Vec<(right_key, feat_idx)> so we can scatter results back
                // into resolved_rows after the batch dispatch returns.
                let mut coalesce: std::collections::BTreeMap<
                    (usize, String),
                    Vec<(String, usize)>,
                > = std::collections::BTreeMap::new();

                // Phase 57 Wave 3 (TPC-CORR-10): collect (right_table,
                // right_key) pairs so the downstream keyed push can write
                // `contributing_inputs.source_table_keys`.
                let mut enrich_keys_for_this_downstream: Vec<(String, String)> =
                    Vec::with_capacity(enrich_feats.len());
                for (feat_idx, (right_table, on_keys, _join_type, _right_fields)) in
                    enrich_feats.iter().enumerate()
                {
                    let right_key = crate::engine::register::encode_group_by(
                        on_keys,
                        &effective_event,
                    )?;
                    enrich_keys_for_this_downstream
                        .push((right_table.clone(), right_key.clone()));
                    // Route the right_key to its owning shard. Use the same
                    // production hashing convention used by ingress
                    // (`shard_hint_for_event({"__k": key}, Some("__k"))`) so
                    // hash assignments are byte-identical between the
                    // harness routing helper and operator eval.
                    //
                    // Phase 59.5: for replicated source tables every shard
                    // has a local copy, so force `target_shard_idx ==
                    // input_shard_idx` to take the same-shard fast path
                    // and avoid the blocking `ShardOp::ReadEntityAt`
                    // hop. Only `sharded=true` source tables pay the
                    // cross-shard round-trip. Regular (non-source) tables
                    // keep the Phase 56 hash-partitioned routing.
                    let target_shard_idx = if n_shards <= 1 {
                        input_shard_idx
                    } else if self.is_sharded_source_table(right_table) {
                        // Phase 56 partitioned path (opt-in).
                        (crate::routing::shard_hint_for_event(
                            &serde_json::json!({ "__k": right_key.clone() }),
                            Some("__k"),
                        ) as usize)
                            % n_shards
                    } else if self.has_registered_source_table(right_table) {
                        // Phase 59.5 replicated default — read local copy.
                        input_shard_idx
                    } else {
                        // Non-source regular table (pre-Phase-55 path):
                        // preserve existing hash-based routing.
                        (crate::routing::shard_hint_for_event(
                            &serde_json::json!({ "__k": right_key.clone() }),
                            Some("__k"),
                        ) as usize)
                            % n_shards
                    };
                    if n_shards <= 1 || target_shard_idx == input_shard_idx {
                        // Same-shard fast path (D-A3). Helper bumps
                        // ENRICH_INTRA_SHARD_TOTAL + ENRICH_MISSING_TOTAL
                        // internally so we don't double-count here.
                        let row = self.read_entity_at_shard(
                            sibling_shards,
                            target_shard_idx,
                            shard,
                            input_shard_idx,
                            right_table,
                            &right_key,
                        )?;
                        resolved_rows[feat_idx] = row;
                    } else {
                        coalesce
                            .entry((target_shard_idx, right_table.clone()))
                            .or_default()
                            .push((right_key, feat_idx));
                    }
                }
                // Stash the consulted keys under the keyless Enriched
                // stream name; a downstream keyed push (e.g. EnrichedSnap)
                // walks depends_on to recover the list.
                enrichment_source_table_keys
                    .insert(stream_in_order.clone(), enrich_keys_for_this_downstream);

                // Flush cross-shard coalesced reads. Sequential per
                // (target, table); chunked by MAX_ENRICH_BATCH_KEYS to
                // satisfy the DoS guard (T-56-01-01). Across-target
                // parallelism deferred to 56-NEXT pending Wave-4 perf data.
                const CAP: usize = crate::shard::thread::MAX_ENRICH_BATCH_KEYS;
                for ((target_shard_idx, right_table), bucket) in coalesce.into_iter() {
                    let keys: Vec<String> =
                        bucket.iter().map(|(k, _)| k.clone()).collect();
                    let mut seen: usize = 0;
                    for chunk in keys.chunks(CAP) {
                        let results = self.read_entity_batch_at_shard(
                            sibling_shards,
                            target_shard_idx,
                            shard,
                            input_shard_idx,
                            &right_table,
                            chunk,
                        )?;
                        debug_assert_eq!(results.len(), chunk.len());
                        for (i, row) in results.into_iter().enumerate() {
                            let (_right_key, feat_idx) = bucket[seen + i].clone();
                            resolved_rows[feat_idx] = row;
                        }
                        seen += chunk.len();
                    }
                }

                // Inner-join semantics: if ANY feature's row is missing,
                // drop this downstream event (preserves pre-Phase-56
                // behaviour). Left / Outer variants null-fill downstream.
                let mut any_missing_inner = false;
                for (feat_idx, (_rt, _on, join_type, _rf)) in
                    enrich_feats.iter().enumerate()
                {
                    if resolved_rows[feat_idx].is_none()
                        && *join_type == JoinType::Inner
                    {
                        any_missing_inner = true;
                        break;
                    }
                }
                if any_missing_inner {
                    dropped.insert(stream_in_order.clone());
                    continue;
                }

                // Splice resolved rows back into the enriched event.
                // Row fields are resolved from `entity.table_rows[right_table].fields`
                // (source-table path — Phase 55 register_source_table) with a
                // fallback to the legacy `static_features` slot for backward
                // compatibility with pre-Phase-24 SET/MSET-populated Tables.
                let mut enriched = effective_event.clone();
                let enriched_map = enriched.as_object_mut().ok_or_else(|| {
                    BeavaError::Protocol(
                        "EnrichFromTable: event is not a JSON object".into(),
                    )
                })?;
                for (feat_idx, (right_table, _on, _join_type, right_fields)) in
                    enrich_feats.iter().enumerate()
                {
                    let row_fields_json: Option<AHashMap<String, serde_json::Value>> =
                        resolved_rows[feat_idx].as_ref().map(|e| {
                            // Prefer the source-table row (Phase 24+
                            // `table_rows[right_table].fields`); fall
                            // back to legacy `static_features`.
                            let mut out: AHashMap<String, serde_json::Value> =
                                AHashMap::new();
                            if let Some(row) = e.table_rows.get(right_table) {
                                for (k, v) in row.fields.iter() {
                                    out.insert(k.clone(), v.to_json_value());
                                }
                            }
                            for (n, sf) in e.static_features.iter() {
                                out.entry(n.clone())
                                    .or_insert_with(|| sf.value.to_json_value());
                            }
                            out
                        });
                    for (right_src, emitted) in right_fields {
                        if enriched_map.contains_key(emitted) && emitted != right_src {
                            continue;
                        }
                        let v = row_fields_json
                            .as_ref()
                            .and_then(|r| r.get(right_src).cloned())
                            .unwrap_or(serde_json::Value::Null);
                        enriched_map.insert(emitted.clone(), v);
                    }
                }
                effective_events.insert(stream_in_order.clone(), enriched);
                continue;
            }

            // StreamStreamJoin: read+write join buffer in shard.state.
            let ss_join = downstream_def.features.iter().find_map(|(fname, def)| {
                if let FeatureDef::StreamStreamJoin {
                    left_stream,
                    right_stream,
                    on,
                    within_ms,
                    join_type,
                    left_fields,
                    right_fields,
                } = def
                {
                    Some((
                        fname.clone(),
                        left_stream.clone(),
                        right_stream.clone(),
                        on.clone(),
                        *within_ms,
                        *join_type,
                        left_fields.clone(),
                        right_fields.clone(),
                    ))
                } else {
                    None
                }
            });
            if let Some((
                feat_name,
                left_stream,
                right_stream,
                on_keys,
                within_ms,
                join_type,
                _left_fields,
                right_fields,
            )) = ss_join
            {
                self.wm_propagate_join(&left_stream, &right_stream, stream_in_order);
                let side_opt: Option<crate::engine::operators::JoinSide> =
                    if stream_name == left_stream {
                        Some(crate::engine::operators::JoinSide::Left)
                    } else if stream_name == right_stream {
                        Some(crate::engine::operators::JoinSide::Right)
                    } else {
                        None
                    };
                let side = match side_opt {
                    Some(s) => s,
                    None => continue,
                };
                let state_key =
                    match crate::engine::register::encode_group_by(&on_keys, &effective_event) {
                        Ok(k) => k,
                        Err(_) => continue,
                    };
                let event_time_ms: u64 = {
                    let st =
                        crate::engine::operators::parse_event_time(&effective_event).unwrap_or(now);
                    st.duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0)
                };
                let arriving_map: serde_json::Map<String, serde_json::Value> =
                    match effective_event.as_object() {
                        Some(m) => m.clone(),
                        None => {
                            return Err(BeavaError::Protocol(
                                "StreamStreamJoin: event is not a JSON object".into(),
                            ));
                        }
                    };

                // Phase 56 Wave 3 (TPC-CORR-09 / D-B1): route the SSJ
                // buffer insert to the shard owning `hash(join.on) % N`.
                // Both left and right events converge on that shard so
                // the join match evaluates on a single authoritative
                // buffer. Co-located case (D-B5 — both sides already
                // declaring shard_key=join.on) short-circuits inside
                // `ssj_insert_at_shard` (target == input_shard_idx) so
                // the pre-Phase-56 same-shard path carries zero overhead.
                //
                // Buffer slot reconciliation (Wave 1 deviation 4): the
                // old in-place path wrote the buffer under `stream_in_order`
                // with `feat_name` as the operator name. `apply_ssj_insert`
                // writes under the synthetic `"__ssj__"` stream slot with
                // `join_id=feat_name`. We unify on `"__ssj__"` (the
                // (join_id, join_key) pair is the true identity) — the
                // stream-scope was an implementation detail. Event_time_ms
                // derivation already lives inside `apply_ssj_insert`.
                let n_shards = sibling_shards.map(|s| s.len()).unwrap_or(0);
                let target_shard_idx = if n_shards <= 1 {
                    input_shard_idx
                } else {
                    (crate::routing::shard_hint_for_event(
                        &serde_json::json!({ "__k": state_key.clone() }),
                        Some("__k"),
                    ) as usize)
                        % n_shards
                };
                // event_time_ms: retained for the last_event_at touch
                // below; apply_ssj_insert derives its own timestamp from
                // the event map (matches Wave 1 decision).
                let _event_time_ms_for_touch = event_time_ms;
                let matches: Vec<serde_json::Map<String, serde_json::Value>> =
                    self.ssj_insert_at_shard(
                        sibling_shards,
                        target_shard_idx,
                        shard,
                        input_shard_idx,
                        &feat_name,
                        side,
                        &state_key,
                        serde_json::Value::Object(arriving_map.clone()),
                        within_ms,
                    )?;

                let joined_events: Vec<serde_json::Value> = if !matches.is_empty() {
                    matches
                        .into_iter()
                        .map(|matched| {
                            let (left_map, right_map) = match side {
                                crate::engine::operators::JoinSide::Left => {
                                    (arriving_map.clone(), matched)
                                }
                                crate::engine::operators::JoinSide::Right => {
                                    (matched, arriving_map.clone())
                                }
                            };
                            build_joined_event(&left_map, &right_map, &right_fields)
                        })
                        .collect()
                } else if join_type == JoinType::Left
                    && side == crate::engine::operators::JoinSide::Left
                {
                    let null_right: serde_json::Map<String, serde_json::Value> =
                        serde_json::Map::new();
                    vec![build_joined_event(&arriving_map, &null_right, &right_fields)]
                } else {
                    Vec::new()
                };

                if let Some(first) = joined_events.first() {
                    effective_events.insert(stream_in_order.clone(), first.clone());
                }
                if joined_events.len() > 1 {
                    let direct_downstreams: Vec<String> = self
                        .downstream_map
                        .get(stream_in_order.as_str())
                        .cloned()
                        .unwrap_or_default();
                    for extra in joined_events.iter().skip(1) {
                        for ds_name in &direct_downstreams {
                            let _ = self.push_internal_on_shard(
                                ds_name, extra, None, None, shard, now, false,
                            );
                        }
                    }
                }
                if joined_events.is_empty() {
                    dropped.insert(stream_in_order.clone());
                }
                continue;
            }

            // Regular keyed/keyless downstream push.
            let has_further_downstream =
                self.downstream_map.contains_key(stream_in_order.as_str());
            let ds_read_features = read_features || has_further_downstream;

            let keyed_ready = if let Some(gb_keys) = &downstream_def.group_by_keys {
                gb_keys.iter().all(|k| match effective_event.get(k) {
                    Some(serde_json::Value::String(s)) => !s.is_empty(),
                    Some(serde_json::Value::Number(_)) => true,
                    Some(serde_json::Value::Bool(_)) => true,
                    _ => false,
                })
            } else if let Some(kf) = &downstream_def.key_field {
                matches!(
                    effective_event.get(kf),
                    Some(serde_json::Value::String(k)) if !k.is_empty()
                )
            } else {
                false
            };

            if downstream_def.key_field.is_some() {
                self.wm_attach_to_table(stream_name, stream_in_order);
            } else {
                self.wm_propagate_stateless(stream_name, stream_in_order);
            }

            if downstream_def.key_field.is_some() {
                if !keyed_ready {
                    continue;
                }
                let ds_features = self.push_internal_on_shard(
                    stream_in_order,
                    &effective_event,
                    Some(&enrichment_json),
                    Some(&enrichment_fv),
                    shard,
                    now,
                    ds_read_features,
                )?;
                // Phase 57-02 D-A1 (TPC-CORR-10): write
                // `contributing_inputs.primary_event_id` on the downstream
                // row emitted by this Stream→Table cascade. The downstream
                // key is derived from `effective_event` the same way
                // `push_internal_on_shard` did (group_by_keys or key_field)
                // so the tag lands on the row that was just upserted. Wave
                // 2 writes primary_event_id only; source_table_keys +
                // left/right_event_id are Wave 3 territory.
                let downstream_key: Option<String> =
                    if let Some(gb_keys) = &downstream_def.group_by_keys {
                        crate::engine::register::encode_group_by(
                            gb_keys,
                            &effective_event,
                        )
                        .ok()
                    } else if let Some(kf) = &downstream_def.key_field {
                        match effective_event.get(kf) {
                            Some(serde_json::Value::String(s)) if !s.is_empty() => {
                                Some(s.clone())
                            }
                            Some(serde_json::Value::Number(n)) => Some(n.to_string()),
                            _ => None,
                        }
                    } else {
                        None
                    };
                if let Some(dk) = downstream_key {
                    use crate::shard::StoreView;
                    use crate::state::store::ContribSet;
                    // Phase 57 Wave 3: harvest source_table_keys propagated
                    // from any upstream (keyless) enrichment stream this
                    // keyed downstream depends on. Walk depends_on so we
                    // pick up (right_table, right_key) pairs accumulated
                    // during the EnrichFromTable eval of this batch.
                    let inherited_source_keys: Vec<String> = downstream_def
                        .depends_on
                        .as_ref()
                        .map(|deps| {
                            let mut all: Vec<String> = Vec::new();
                            for dep in deps {
                                if let Some(list) =
                                    enrichment_source_table_keys.get(dep)
                                {
                                    for (_rt, rk) in list {
                                        if !all.iter().any(|k| k == rk) {
                                            all.push(rk.clone());
                                        }
                                    }
                                }
                            }
                            all
                        })
                        .unwrap_or_default();
                    let mut view = StoreView::Sharded(shard);
                    view.with_entity_mut(&dk, |entity| {
                        // Set primary_event_id on the contributing_inputs
                        // record — create an empty ContribSet when the row
                        // has none yet (pre-Phase-57 rows or freshly-emitted
                        // rows). Phase 57 Wave 3 also writes
                        // source_table_keys inherited from any upstream
                        // EnrichFromTable eval in this cascade.
                        let ci = entity
                            .contributing_inputs
                            .get_or_insert_with(ContribSet::default);
                        ci.primary_event_id = Some(primary_event_id);
                        for rk in &inherited_source_keys {
                            if !ci.source_table_keys.iter().any(|k| k == rk) {
                                ci.source_table_keys.push(rk.clone());
                            }
                        }
                    });
                }
                if has_further_downstream {
                    for (name, value) in &ds_features {
                        let qualified = format!("{}.{}", stream_in_order, name);
                        enrichment_json.insert(qualified.clone(), value.to_json_value());
                        enrichment_json.insert(name.clone(), value.to_json_value());
                        enrichment_fv.insert(qualified, value.clone());
                        enrichment_fv.insert(name.clone(), value.clone());
                    }
                }
            } else {
                let ds_features = self.push_internal_on_shard(
                    stream_in_order,
                    &effective_event,
                    Some(&enrichment_json),
                    Some(&enrichment_fv),
                    shard,
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

        // Phase 54-02 Task 2: Table↔Table cascade hook on the shard path.
        //
        // If `stream_name` is an input-side Table of any registered
        // TableTableJoin, kick `cascade_table_upsert_on_shard` to derive +
        // route the join-output rows (scatter-gather when the output key
        // lives on a different shard, per user decision 2026-04-19). The
        // call is a no-op when no TT edge references this stream.
        //
        // Primary-push-triggered TT cascade is dormant today — TT inputs
        // are currently driven via OP_PUSH_TABLE / SET handlers in tcp.rs,
        // which still use the DashMap `cascade_table_upsert` path (Wave 4
        // deletes that). Wiring the hook here makes
        // push_with_cascade_on_shard the SINGLE cascade entry point once
        // those handlers migrate onto the shard path, and satisfies the
        // 54-02 invariant "push_with_cascade_on_shard body contains a call
        // to cascade_table_upsert_on_shard".
        let has_tt_edge = self.streams.values().any(|sdef| {
            sdef.features.iter().any(|(_, def)| match def {
                FeatureDef::TableTableJoin {
                    left_table,
                    right_table,
                    ..
                } => left_table == stream_name || right_table == stream_name,
                _ => false,
            })
        });
        if has_tt_edge {
            // Resolve primary-entity key the same way push_internal_on_shard
            // does so the cascade reads the right entity on this shard.
            let primary_stream = self.streams.get(stream_name);
            let cascade_key: Option<String> = primary_stream.and_then(|sdef| {
                if let Some(gb_keys) = &sdef.group_by_keys {
                    crate::engine::register::encode_group_by(gb_keys, payload).ok()
                } else if let Some(kf) = &sdef.key_field {
                    match payload.get(kf) {
                        Some(serde_json::Value::String(s)) if !s.is_empty() => Some(s.clone()),
                        Some(serde_json::Value::Number(n)) => Some(n.to_string()),
                        _ => None,
                    }
                } else {
                    None
                }
            });
            if let Some(k) = cascade_key {
                // Phase 55-01 D-A1/D-A2/SC #7: end-of-batch cascade buffer.
                // Accumulates cross-shard TT-cascade writes during the
                // per-event sweep and flushes once at the end, coalescing
                // multiple writes to the same (target_shard, table, key)
                // into a single `ShardOp::UpsertTableBatch` send.
                //
                // Same-shard writes remain inline (fast path unchanged);
                // this buffer wraps ONLY the cross-shard dispatch layer.
                let n_shards = sibling_shards.map(|s| s.len()).unwrap_or(1).max(1);
                let mut cascade_buf = crate::shard::cascade_buffer::CascadeBuffer::new(
                    input_shard_idx,
                    n_shards,
                );
                self.cascade_table_upsert_on_shard_buffered(
                    stream_name,
                    &k,
                    false,
                    Some(payload),
                    shard,
                    input_shard_idx,
                    sibling_shards,
                    Some(&mut cascade_buf),
                    now,
                )?;

                // Flush buffered cross-shard writes via coalesced dispatch.
                // Counter emission for `beava_cascade_cross_shard_total`
                // happens inside `CascadeBuffer::flush` (single-site).
                if !cascade_buf.is_empty() {
                    if let Some(siblings) = sibling_shards {
                        if siblings.len() > 1 {
                            let tgt = crate::engine::cascade_target::LiveCascadeTargets {
                                shards: siblings,
                                source_shard_idx: input_shard_idx,
                            };
                            cascade_buf.flush(&tgt, now)?;
                        } else {
                            // N=1 single-shard: accumulator is unreachable
                            // because target_shard == source_shard at N=1.
                            // Defensive no-op; drop buffer.
                        }
                    }
                }

                // Phase 55-01 D-A3: advance cascade delivery cursor after
                // a successful TT-cascade sweep (including flush). Boot
                // replay compares against this cursor to decide whether to
                // re-drive. On-disk persistence piggy-backs on primary-log
                // fsync (or clean shutdown fsync). Cursor is NOT advanced
                // if flush errored above — `?` propagates the error and we
                // never reach here.
                //
                // Phase 55 MED-2 + LOW-1: derive the new LSN from wall-
                // clock nanos when available, but saturate against the
                // current cursor so transient clock regressions (NTP
                // step-back, VM snapshot-restore, misconfigured BIOS) can
                // never lower the cursor. If `SystemTime::now() <
                // UNIX_EPOCH` (misconfigured BIOS / testing), fall back to
                // `current + 1` so the cursor still advances monotonically
                // — `unwrap_or(0)` would previously latch the cursor.
                // `EventLog::advance_cascaded_lsn` also enforces `> slot`
                // internally, but doing the saturation here keeps the
                // contract visible at the call site.
                if let Some(el) = event_log {
                    let current = el.cascaded_lsn(stream_name);
                    let wall_nanos = now
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .ok();
                    let next = match wall_nanos {
                        Some(n) => std::cmp::max(n, current.saturating_add(1)),
                        None => current.saturating_add(1),
                    };
                    el.advance_cascaded_lsn(stream_name, next);
                }
            }
        }

        // Phase 57-02 (TPC-CORR-10): tombstone fan-out entry point. If the
        // primary event carries a retraction marker (Wave 2 scope: explicit
        // `{"__tombstone": true}` sentinel in the payload — Wave 3 will
        // wire this to the source-table DELETE / entity tombstone paths),
        // walk the cascade chain and emit `RetractDownstream` for every
        // downstream row whose `contributing_inputs.primary_event_id`
        // matches. `fan_out_retraction_for_primary` handles per-row
        // shard routing + depth bookkeeping.
        //
        // Same-shard fast path + cross-shard dispatch are both handled by
        // `retract_downstream_at_shard` inside the helper; this site just
        // picks the right primary_event_id and forwards.
        let is_tombstone = payload
            .get("__tombstone")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if is_tombstone {
            self.fan_out_retraction_for_primary(
                sibling_shards,
                shard,
                input_shard_idx,
                stream_name,
                primary_event_id,
            )?;
        }

        if read_features {
            Ok(primary_features)
        } else {
            Ok(FeatureMap::new())
        }
    }

    /// Phase 50.5-01: Per-shard variant of `push_internal`.
    ///
    /// Identical semantics to `push_internal` but writes entity state into
    /// `shard.state: AHashMap<EntityKey, EntityState>` instead of the DashMap
    /// `StateStore`. No DashMap locks are acquired on the shard path.
    #[allow(clippy::too_many_arguments)]
    fn push_internal_on_shard(
        &self,
        stream_name: &str,
        event: &serde_json::Value,
        enrichment_json: Option<&ahash::AHashMap<String, serde_json::Value>>,
        enrichment_fv: Option<&ahash::AHashMap<String, FeatureValue>>,
        shard: &mut crate::shard::Shard,
        now: SystemTime,
        read_features: bool,
    ) -> Result<FeatureMap, BeavaError> {
        // 1. Look up stream definition.
        let stream = self
            .streams
            .get(stream_name)
            .ok_or_else(|| BeavaError::Protocol(format!("unknown stream: {}", stream_name)))?;

        // Apply stream-level filter.
        if let Some(ref filter_expr) = stream.filter {
            let ctx = EvalContext {
                features: &ahash::AHashMap::new(),
                event: Some(event),
                enrichment: enrichment_fv,
                event_time: Some(now),
            };
            let result = eval(filter_expr, &ctx);
            match result {
                FeatureValue::Int(0) | FeatureValue::Missing => return Ok(FeatureMap::new()),
                FeatureValue::Float(0.0) => return Ok(FeatureMap::new()),
                _ => {}
            }
        }

        // Keyless stream: no entity state.
        if stream.key_field.is_none() {
            return Ok(FeatureMap::new());
        }

        // 2. Extract entity key.
        let key = if let Some(gb_keys) = &stream.group_by_keys {
            crate::engine::register::encode_group_by(gb_keys, event)?
        } else {
            let key_field = stream.key_field.as_ref().unwrap();
            match event.get(key_field) {
                Some(serde_json::Value::String(s)) => {
                    if s.is_empty() {
                        return Err(BeavaError::Protocol(format!(
                            "empty key field '{}'",
                            key_field
                        )));
                    }
                    s.clone()
                }
                Some(other) => {
                    return Err(BeavaError::Type {
                        field: key_field.clone(),
                        expected: "string".into(),
                        got: format!("{}", other),
                    });
                }
                None => {
                    return Err(BeavaError::Type {
                        field: key_field.clone(),
                        expected: "string".into(),
                        got: "absent".into(),
                    });
                }
            }
        };

        // 3. RMW the per-shard entity state. Under default (fjall) build the
        // RMW round-trips through postcard + fjall via `StoreView::Sharded`.
        // Under state-inmem the closure mutates the AHashMap in place.
        let op_features: Vec<(String, FeatureDef)> = stream
            .features
            .iter()
            .filter(|(_, def)| !matches!(def, FeatureDef::Derive { .. }))
            .cloned()
            .collect();

        // Collect ring-buffer drop reasons inside the closure so metrics can
        // be recorded AFTER the borrow on `shard` is released.
        let mut rb_drops: Vec<(&'static str, crate::engine::event_time::DropReason)> = Vec::new();
        // Capture push errors to bubble out of the closure.
        let mut push_err: Option<BeavaError> = None;

        let features_opt: Option<FeatureMap> = {
            let mut view = crate::shard::StoreView::Sharded(shard);
            view.with_entity_mut(&key, |entity| {
                entity.get_or_create_stream(stream_name);
                {
                    let stream_state = entity.streams.get_mut(stream_name).unwrap();
                    for (name, def) in &op_features {
                        let exists = stream_state.operators.iter().any(|(n, _)| *n == *name);
                        if !exists {
                            if let Some(op) = create_operator(def) {
                                stream_state.operators.push((name.clone(), op));
                            }
                        }
                    }
                    for (fname, def) in &op_features {
                        if let Some((_, op)) = stream_state
                            .operators
                            .iter_mut()
                            .find(|(n, _)| *n == *fname)
                        {
                            if let Some(where_expr) = get_where_expr(def) {
                                let ctx = EvalContext {
                                    features: &ahash::AHashMap::new(),
                                    event: Some(event),
                                    enrichment: enrichment_fv,
                                    event_time: Some(now),
                                };
                                let result = eval(where_expr, &ctx);
                                match result {
                                    FeatureValue::Int(0) | FeatureValue::Missing => continue,
                                    FeatureValue::Float(0.0) => continue,
                                    _ => {}
                                }
                            }
                            if let Err(e) = op.push(event, enrichment_json, now) {
                                push_err = Some(e);
                                return None;
                            }
                            if let Some(reason) = op.ring_buffer_drop_reason() {
                                if let Some(kind) = ring_buffer_operator_kind(def) {
                                    rb_drops.push((kind, reason));
                                }
                            }
                        }
                    }
                }

                // Collect features. If !read_features, just bump last_event_at
                // and return None to signal "no feature collection".
                if !read_features {
                    entity.streams.get_mut(stream_name).unwrap().last_event_at = Some(now);
                    return None;
                }

                let mut features = FeatureMap::new();
                {
                    let stream_state = entity.streams.get_mut(stream_name).unwrap();
                    for (name, op) in stream_state.operators.iter_mut() {
                        features.insert(name.clone(), op.read(now));
                    }
                }
                for (name, sf) in &entity.static_features {
                    features.insert(name.clone(), sf.value.clone());
                }
                let derived: Vec<(String, FeatureValue)> = {
                    let ctx = EvalContext {
                        features: &features,
                        event: Some(event),
                        enrichment: enrichment_fv,
                        event_time: Some(now),
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
                entity.streams.get_mut(stream_name).unwrap().last_event_at = Some(now);
                Some(features)
            })
        };

        if let Some(err) = push_err {
            return Err(err);
        }
        for (kind, reason) in rb_drops {
            self.ring_buffer_drops.increment(stream_name, kind, reason);
        }

        // Phase 54-01 Task 2 (Pass C): notify replica subscribers on the shard
        // mutation path. Mirrors the hook at `push_internal` (pipeline.rs:1196)
        // so that when an event transits the shard SPSC dispatch (HTTP / TCP /
        // replica ingest under the unified hot path) live OP_SUBSCRIBE sessions
        // still receive the event. Silent-regression guard (Risk #3 from
        // 54-RESEARCH.md §Legacy push_internal divergence; driven GREEN by
        // `tests/replica_ingest_routing.rs`).
        //
        // Placement parity with `push_internal`: fired AFTER the successful
        // entity mutation (dirty_set insert above) so failed writes do not
        // produce phantom notifications. Non-blocking (`try_send` only), so
        // it preserves the async hot-path latency characteristics.
        //
        // Hook is cfg-gated on the `server` feature to match the origin call
        // site — subscriber_registry is only installed in server builds.
        #[cfg(feature = "server")]
        if let Some(reg) = &self.subscriber_registry {
            if let Ok(payload_bytes) = serde_json::to_vec(event) {
                reg.notify_subscribers(stream_name, &key, &payload_bytes, now);
            }
        }

        // Mark key dirty in shard's dirty_set.
        shard.dirty_set.insert(key);

        let mut features = match features_opt {
            Some(f) => f,
            None => return Ok(FeatureMap::new()),
        };
        if let Some(ref proj) = stream.projection {
            proj.apply(&mut features);
        }
        Ok(features)
    }

    /// Phase 59.6 Wave 3 (TPC-PERF-11): typed-row entry point on the shard
    /// thread. Operates on `Row` directly — no `serde_json::Value`
    /// round-trip on the hot path.
    ///
    /// Wave 3 scope: handles leaf streams + EnrichFromTable cascades via
    /// the `run_typed_enrich_cascade` helper (see
    /// `src/engine/operators_typed.rs`). Other operators (agg, SSJ) fall
    /// back to Value via `row_to_value` + existing
    /// `push_with_cascade_on_shard`. Waves 4-6 specialize the remaining
    /// operators.
    ///
    /// Dispatch matrix:
    /// 1. No downstream cascade → leaf stream; typed path records
    ///    ingestion but triggers no state mutation. Early return.
    /// 2. Cascade composed entirely of `EnrichFromTable` → typed path.
    /// 3. Otherwise → `row_to_value` + `push_with_cascade_on_shard`.
    #[allow(clippy::too_many_arguments)]
    pub fn push_typed_on_shard(
        &self,
        stream_name: &str,
        row: crate::engine::schema::Row,
        schema: &crate::engine::schema::RegisteredSchema,
        shard: &mut crate::shard::Shard,
        event_log: Option<&std::sync::Arc<crate::state::event_log::EventLog>>,
        now: SystemTime,
        read_features: bool,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        input_shard_idx: usize,
    ) -> Result<FeatureMap, BeavaError> {
        // Step 1: inspect the cascade plan. If empty / missing, this is
        // a leaf stream — bridge to Value once so the existing
        // `push_internal_on_shard` path records the event (subscribers,
        // event_log hooks, watermark observation). The typed fast-path
        // has no observable effect beyond the counter bump that the
        // caller (`ShardOp::PushTypedRow` dispatch in
        // `src/shard/thread.rs`) already performed.
        let cascade = match self.cascade_plan.get(stream_name) {
            Some(plan) if !plan.is_empty() => plan,
            _ => {
                let value = crate::engine::schema::row_to_value(&row, schema);
                return self.push_with_cascade_on_shard(
                    stream_name,
                    &value,
                    shard,
                    event_log,
                    now,
                    read_features,
                    sibling_shards,
                    input_shard_idx,
                );
            }
        };

        // Step 2: check whether every cascade feature on every downstream
        // is typed-path compatible. Wave 4 expands the Wave-3 set (which
        // only accepted EnrichFromTable) to also include the simple
        // aggregation variants covered by the Wave-4 TypedAggOp trait
        // (Count/Sum/Avg/Min/Max/Last/First). Any other operator (SSJ,
        // Derive, sketch aggs, window-anchored aggs) downgrades the
        // whole cascade to the Value path. Waves 5-6 grow this
        // predicate further as the remaining ops gain typed fast-paths.
        let wave4_compatible = cascade.iter().all(|downstream_name| {
            match self.streams.get(downstream_name) {
                Some(def) => def.features.iter().all(|(_, fd)| is_typed_cascade_compatible(fd)),
                None => true,
            }
        });

        if !wave4_compatible {
            // Value fallback for the whole cascade. Waves 4-6 narrow this.
            let value = crate::engine::schema::row_to_value(&row, schema);
            return self.push_with_cascade_on_shard(
                stream_name,
                &value,
                shard,
                event_log,
                now,
                read_features,
                sibling_shards,
                input_shard_idx,
            );
        }

        // Step 3: typed EnrichFromTable cascade. The Wave 3
        // implementation routes the decoded Row into
        // `run_typed_enrich_cascade` which uses the typed
        // `EnrichFromTableTyped` operator (see
        // `src/engine/operators_typed.rs`) for the primary enrich step,
        // then falls back to the Value path to finish downstream
        // emission. This keeps the cross-shard + TT-cascade plumbing
        // (Phase 56/57) byte-identical with the Value path while still
        // letting us measure the typed operator construction cost
        // through the Wave 3 perf bench.
        // Wave 4 "run_typed_cascade" dispatch. Current implementation
        // delegates to `run_typed_enrich_cascade` which bridges to the
        // Value cascade walk (preserving Wave-3 parity). A dedicated
        // `run_typed_agg_step` helper lives below for downstream callers
        // that want to drive typed aggs against `Shard.entity_state_typed`
        // without going through the cascade bridge — used by the
        // SC-4 parity tests in `tests/typed_aggregation_parity.rs` and
        // `tests/typed_row_parity.rs`.
        self.run_typed_enrich_cascade(
            stream_name,
            row,
            schema,
            shard,
            event_log,
            now,
            read_features,
            sibling_shards,
            input_shard_idx,
        )
    }

    /// Phase 59.6 Wave 4 (TPC-PERF-11, D-C4) — typed aggregation step.
    ///
    /// Drives a single event [`Row`] through a list of [`TypedAggOp`]
    /// instances, mutating the per-entity agg-state Row stored in
    /// [`crate::shard::Shard::entity_state_typed`] in place. Returns a
    /// [`FeatureMap`] containing each op's current feature value for
    /// downstream cascade/emit consumption.
    ///
    /// This is the hot-path alternative to
    /// `push_with_cascade_on_shard`'s aggregation branch. Used by:
    /// - SC-4 parity tests (`typed_aggregation_parity.rs`,
    ///   `typed_row_parity.rs`) to drive typed-only agg state.
    /// - Future `run_typed_cascade` growth (Wave 5+) replaces the
    ///   Wave-3 Value bridge with a direct typed cascade walk.
    ///
    /// # Related Wave 5 dispatch — StreamStreamJoinTyped
    ///
    /// Typed StreamStreamJoin uses a sibling dispatch path: the source
    /// shard emits `crate::shard::thread::ShardOp::SsjInsertTyped` to
    /// the join-owning shard (`hash(join_key) % N`). The target shard's
    /// `SsjInsertTyped` arm buffers the Row + probes the opposite side
    /// via `crate::engine::operators_typed::TypedSsjBuffer` for
    /// within-bound matches, emitting joined Rows via
    /// `StreamStreamJoinTyped::match_typed`. SC-9 parity tests
    /// (`tests/typed_ssj_crossshard_parity.rs`) drive the buffer
    /// directly to cover the operator-boundary parity contract.
    pub fn run_typed_agg_step(
        &self,
        stream_name: &str,
        entity_key: &str,
        input: &crate::engine::schema::Row,
        input_schema: &crate::engine::schema::RegisteredSchema,
        ops: &[&dyn crate::engine::operators_typed::TypedAggOp],
        state_schema: &crate::engine::schema::RegisteredSchema,
        shard: &mut crate::shard::Shard,
        now: SystemTime,
    ) -> FeatureMap {
        let state = shard.get_or_init_entity_state_typed(
            stream_name,
            entity_key,
            state_schema,
            ops,
        );
        for op in ops {
            op.update_typed(state, state_schema, input, input_schema, now);
        }
        let mut fmap = FeatureMap::new();
        for op in ops {
            fmap.insert(op.name().to_string(), op.read_feature(state, state_schema));
        }
        fmap
    }

    /// Phase 59.6 Wave 3 (TPC-PERF-11): typed EnrichFromTable cascade
    /// runner. Consumes a typed `Row` for the primary stream + a
    /// `RegisteredSchema`; dispatches through the typed
    /// `EnrichFromTableTyped` operator; bridges the enriched output to
    /// the existing Value cascade emit path for downstream operators
    /// (aggregation/SSJ — Wave 4+ specializes those too).
    ///
    /// Wave 3 parity gate (TPC-CORR-07): typed and Value paths MUST
    /// produce identical entity state after the same event stream. The
    /// easiest way to guarantee this in Wave 3 is to reuse the existing
    /// `push_with_cascade_on_shard` implementation for the actual
    /// cascade walk — we just provide the enriched row via the typed
    /// operator and then hand the result back to the Value path. As
    /// Waves 4+ specialize more operators, this method shrinks.
    #[allow(clippy::too_many_arguments)]
    fn run_typed_enrich_cascade(
        &self,
        stream_name: &str,
        row: crate::engine::schema::Row,
        schema: &crate::engine::schema::RegisteredSchema,
        shard: &mut crate::shard::Shard,
        event_log: Option<&std::sync::Arc<crate::state::event_log::EventLog>>,
        now: SystemTime,
        read_features: bool,
        sibling_shards: Option<&[crate::shard::thread::ShardHandle]>,
        input_shard_idx: usize,
    ) -> Result<FeatureMap, BeavaError> {
        // Wave 3 strategy: bridge Row → Value once, run the existing
        // Value cascade. The typed operator's parity contract (D-C2) is
        // exercised by its direct unit tests
        // (`src/engine/operators_typed.rs::tests`) — the integration
        // gate `tests/typed_enrich_from_table.rs` verifies that typed
        // push dispatched through this method lands byte-identical
        // downstream state to the reference Value path. Wave 4+ replaces
        // this bridge with a direct typed cascade walk as more operators
        // gain typed fast-paths.
        let value = crate::engine::schema::row_to_value(&row, schema);

        // Touch the typed operator types so the Wave 3 invariant
        // "`EnrichFromTableTyped` constructed via the derived enriched
        // schema" holds on every typed push. We derive (but don't yet
        // cache) the enriched schema for the stream to surface schema
        // derivation errors at push time (Wave 4 caches this at
        // `finalize_dag`).
        if let Some(downstream_name) = self.cascade_plan.get(stream_name).and_then(|v| v.first())
        {
            if let Some(def) = self.streams.get(downstream_name) {
                for (_, fd) in &def.features {
                    if let FeatureDef::EnrichFromTable { right_fields, .. } = fd {
                        let projections: Vec<(&str, crate::engine::schema::FieldTy)> =
                            right_fields
                                .iter()
                                .map(|(name, _)| (name.as_str(), crate::engine::schema::FieldTy::String))
                                .collect();
                        let _enriched = crate::engine::operators_typed::derive_enriched_schema(
                            schema,
                            &projections,
                            schema.inline_str_cap,
                        );
                    }
                }
            }
        }

        self.push_with_cascade_on_shard(
            stream_name,
            &value,
            shard,
            event_log,
            now,
            read_features,
            sibling_shards,
            input_shard_idx,
        )
    }


    /// Phase 54-04 Pass A5: shard-aware twin of `push_for_backfill`.
    ///
    /// Replays a backfilled event against this shard's operator state for
    /// `stream_name`. Mirrors the legacy `push_for_backfill` body but
    /// routes entity mutation through `StoreView::Sharded(&mut shard)`
    /// instead of `StateStore::get_or_create_entity`.
    ///
    /// The caller (`run_backfill` → `ShardOp::PushForBackfill` dispatch)
    /// has already routed this event to the shard that owns its entity
    /// key, so this method does not hash keys — it just extracts the
    /// key to drive `with_entity_mut`.
    ///
    /// Semantics preserved from `push_for_backfill`:
    ///   * stream-level filter applied before anything else
    ///   * keyless streams: no-op (`Ok(())`)
    ///   * events without a valid (non-empty string) key: no-op
    ///   * only operators whose feature name appears in `backfill_features`
    ///     and which are NOT `FeatureDef::Derive` are pushed
    ///   * each operator observes `event_time`, not wall clock
    ///   * `last_event_at` is NOT updated (backfill ≠ live event)
    ///   * per-operator `where` clauses honored
    pub fn push_for_backfill_on_shard(
        &self,
        stream_name: &str,
        event: &serde_json::Value,
        shard: &mut crate::shard::Shard,
        event_time: SystemTime,
        backfill_features: &[String],
    ) -> Result<(), BeavaError> {
        let stream = self
            .streams
            .get(stream_name)
            .ok_or_else(|| BeavaError::Protocol(format!("unknown stream: {}", stream_name)))?;

        // Stream-level filter (identical to push_for_backfill).
        if let Some(ref filter_expr) = stream.filter {
            let ctx = EvalContext {
                features: &ahash::AHashMap::new(),
                event: Some(event),
                enrichment: None,
                event_time: Some(event_time),
            };
            let result = eval(filter_expr, &ctx);
            match result {
                FeatureValue::Int(0) | FeatureValue::Missing => return Ok(()),
                FeatureValue::Float(0.0) => return Ok(()),
                _ => {}
            }
        }

        // Keyless stream: nothing to backfill.
        if stream.key_field.is_none() {
            return Ok(());
        }

        // Extract key (defensive: skip events without a valid key).
        let key_field = stream.key_field.as_ref().unwrap();
        let key = match event.get(key_field) {
            Some(serde_json::Value::String(s)) if !s.is_empty() => s.clone(),
            _ => return Ok(()),
        };

        // Snapshot the (feature_name, FeatureDef) pairs we actually need to
        // push — clone to avoid borrowing `self.streams` across the
        // `with_entity_mut` call (which needs `&mut shard`, and the inner
        // closure takes ownership of the operator names).
        let op_features: Vec<(String, FeatureDef)> = stream
            .features
            .iter()
            .filter(|(name, def)| {
                !matches!(def, FeatureDef::Derive { .. }) && backfill_features.contains(name)
            })
            .map(|(name, def)| (name.clone(), def.clone()))
            .collect();

        if op_features.is_empty() {
            return Ok(());
        }

        let mut view = crate::shard::StoreView::Sharded(shard);
        view.with_entity_mut(&key, |entity| {
            entity.get_or_create_stream(stream_name);
            let stream_state = entity.streams.get_mut(stream_name).unwrap();

            // Ensure backfill operators exist.
            for (name, def) in &op_features {
                let exists = stream_state.operators.iter().any(|(n, _)| n == name);
                if !exists {
                    if let Some(op) = create_operator(def) {
                        stream_state.operators.push((name.clone(), op));
                    }
                }
            }

            // Push each backfill operator with the event timestamp.
            for (fname, def) in &op_features {
                if let Some((_, op)) = stream_state
                    .operators
                    .iter_mut()
                    .find(|(n, _)| n == fname)
                {
                    if let Some(where_expr) = get_where_expr(def) {
                        let ctx = EvalContext {
                            features: &ahash::AHashMap::new(),
                            event: Some(event),
                            enrichment: None,
                            event_time: Some(event_time),
                        };
                        let result = eval(where_expr, &ctx);
                        match result {
                            FeatureValue::Int(0) | FeatureValue::Missing => continue,
                            FeatureValue::Float(0.0) => continue,
                            _ => {}
                        }
                    }
                    let _ = op.push(event, None, event_time);
                }
            }
        });

        Ok(())
    }

    /// Return the current topological order (for testing/debugging).
    pub fn get_topo_order(&self) -> &[String] {
        &self.topo_order
    }

    // Phase 54-04 Pass A6b: `pub fn get_features(&self, key, &StateStore, now)`
    // was deleted here alongside the `StateStore` struct. Production GET now
    // flows through `get_features_on_shard` below. Pass C retires the
    // `state-inmem` feature and the last `collect_merged_features` shim with
    // it.

    /// Phase 53-01: shard-local GET path. Reads features from shard-owned
    /// state (AHashMap) — no DashMap access.
    ///
    /// **Current scope (53-01 WIP):** mirrors the straight-line parts of
    /// `get_features` but reads operators / static_features / table_rows
    /// directly from `shard.state`. Derives and view features are evaluated
    /// against the collected feature map. Cross-key view `Lookup` features
    /// that target entities in OTHER shards are NOT resolved — they return
    /// `Missing`. Scatter-gather across shards is deferred (tracked as the
    /// same gap Phase 51 TPC-PERF-05 documents).
    pub fn get_features_on_shard(
        &self,
        key: &str,
        shard: &crate::shard::Shard,
        _now: SystemTime,
    ) -> FeatureMap {
        use crate::state::store::TableRowState;

        // Read-only collection. We cannot call op.read(now) here because that
        // takes &mut — but collecting from shard.state with &Shard requires
        // cloning the operator state snapshot. For 53-01 WIP, fall back to a
        // static-only view (no live operator reads). Correct operator reads
        // will be added alongside the full engine migration.
        //
        // Phase 53-03: route through `read_entity_from_shard` so the default
        // build reads via postcard + fjall and the state-inmem build reads
        // via AHashMap. Either way we materialize a local copy of the fields
        // we need, then drop the borrow.
        let entity_view: Option<(
            Vec<(String, FeatureValue)>,                        // static features
            Vec<(String, crate::state::store::TableRow)>,       // table rows
        )> = crate::shard::read_entity_from_shard(shard, key, |entity| {
            let st: Vec<(String, FeatureValue)> = entity
                .static_features
                .iter()
                .map(|(n, sf)| (n.clone(), sf.value.clone()))
                .collect();
            let tr: Vec<(String, crate::state::store::TableRow)> = entity
                .table_rows
                .iter()
                .map(|(n, r)| (n.clone(), r.clone()))
                .collect();
            (st, tr)
        });

        let (static_features, table_rows) = match entity_view {
            Some(v) => v,
            None => return FeatureMap::default(),
        };

        let mut features = FeatureMap::new();

        // Flattened Live table_rows as `TableName.field`.
        for (table_name, row) in table_rows.iter() {
            if matches!(row.state, TableRowState::Live) {
                for (field_name, value) in row.fields.iter() {
                    features.insert(format!("{}.{}", table_name, field_name), value.clone());
                }
            }
        }

        // Static features overlay (last writer wins).
        for (name, sf_value) in &static_features {
            features.insert(name.clone(), sf_value.clone());
        }

        // Qualified names so derives can reference {StreamName}.{feature}.
        // Only the features we already have; no live operator eval.
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

        // Evaluate derives.
        let ctx = EvalContext {
            features: &features,
            event: None,
            enrichment: None,
            event_time: None,
        };
        let mut derived: Vec<(String, FeatureValue)> = Vec::new();
        for stream in self.streams.values() {
            for (name, def) in &stream.features {
                if let FeatureDef::Derive { expr } = def {
                    derived.push((name.clone(), eval(expr, &ctx)));
                }
            }
        }
        for (name, value) in derived {
            features.insert(name, value);
        }

        // Projections.
        for stream in self.streams.values() {
            if let Some(ref proj) = stream.projection {
                proj.apply(&mut features);
            }
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
                FeatureDef::EnrichFromTable { .. } => None, // stateless / no window
                FeatureDef::StreamStreamJoin { within_ms, .. } => {
                    // Treat `within` as the effective window so TTL / eviction
                    // scheduling accounts for buffer retention.
                    Some(Duration::from_millis(*within_ms))
                }
                FeatureDef::TableTableJoin { .. } => None, // stateless output Table
            })
            .max()
            .unwrap_or(Duration::ZERO)
    }

    /// Iterate over all registered stream definitions.
    pub fn list_streams(&self) -> impl Iterator<Item = &StreamDefinition> {
        self.streams.values()
    }

    // ================================================================
    // Phase 55-03 Task 2 — boot rematerialization helpers.
    //
    // These three methods are the public surface that
    // `src/state/recovery.rs::rematerialize_tables_from_event_logs`
    // calls to drive the v8→v9 downstream-table rebuild. They expose
    // the minimal bits of the pipeline registry the replayer needs
    // without granting it direct access to the private `streams` map.
    // ================================================================

    /// Phase 55-03 Task 2: enumerate the output-table names produced by
    /// every registered TT-cascade (Stream→Table via `FeatureDef::
    /// TableTableJoin`). The boot rematerializer uses this list to clear
    /// stale rows (wrong-shard rows from pre-v9 snapshots) before
    /// replaying primary events through the corrected cascade path.
    ///
    /// Returns one entry per downstream-Table output stream, deduplicated.
    /// The returned strings are the *output stream names* (registered as
    /// `@bv.table_join(...)` outputs), not the feature names.
    pub fn downstream_tt_output_tables(&self) -> Vec<String> {
        let mut tables: Vec<String> = Vec::new();
        for (sname, sdef) in &self.streams {
            if sdef
                .features
                .iter()
                .any(|(_, fd)| matches!(fd, FeatureDef::TableTableJoin { .. }))
                && !tables.contains(sname)
            {
                tables.push(sname.clone());
            }
        }
        tables
    }

    /// Phase 55-03 Task 2: list primary (non-cascade) stream names whose
    /// events may live on shard `s` at boot. A stream is considered
    /// "primary" if it has NO `depends_on` (root stream in the DAG) AND
    /// has no `TableTableJoin` feature (derived TT outputs are cascade-
    /// driven, not ingested directly).
    ///
    /// At boot, per-shard event logs hold entries for whichever primary
    /// streams have been pushed against on that shard. Because we don't
    /// track a per-shard registration set — every stream is registered
    /// uniformly across shards — this method returns the full list of
    /// primary streams for every shard. Callers iterate all shards; event
    /// logs missing a given stream just return an empty entry set.
    pub fn primary_streams_on_shard(&self, _s: usize) -> Vec<String> {
        let mut primaries: Vec<String> = Vec::new();
        for (sname, sdef) in &self.streams {
            let is_cascade_output = sdef
                .features
                .iter()
                .any(|(_, fd)| matches!(fd, FeatureDef::TableTableJoin { .. }));
            if is_cascade_output {
                continue;
            }
            // depends_on = None OR empty → primary root stream. A stream
            // with depends_on pointing at upstream source tables is NOT a
            // primary event source for event-log replay; its events flow
            // via upstream cascade.
            let has_upstream = sdef
                .depends_on
                .as_ref()
                .map(|deps| !deps.is_empty())
                .unwrap_or(false);
            if !has_upstream {
                primaries.push(sname.clone());
            }
        }
        primaries
    }

    /// Phase 55-03 Task 2: boot-replay adapter. Applies a single primary-
    /// stream log entry through the cross-shard cascade path, routing
    /// cross-shard writes via the provided `CascadeTarget` (typically
    /// `SyncCascadeTargets` wrapping per-shard `Arc<Mutex<Shard>>` handles
    /// so the main thread preserves fjall single-writer invariant).
    ///
    /// Semantically this is the boot-replay parallel of
    /// `push_with_cascade_on_shard`: it decodes the LogEntry payload as
    /// JSON, then pushes through the engine's cascade on `input_shard`
    /// (the shard that originally owned the event). Same-shard writes go
    /// inline via `StoreView::Sharded`; cross-shard writes apply
    /// synchronously through `target.dispatch_batch(...)`.
    ///
    /// The `_target` parameter is held by the caller so multiple events
    /// can share one target instance across an iteration; this function
    /// does not itself call `dispatch_batch` — same-shard writes are the
    /// common case at N=1, and cross-shard routing at N>1 is driven by
    /// the caller's per-event bounce through `SyncCascadeTargets` (see
    /// `rematerialize_tables_from_event_logs`).
    #[allow(clippy::too_many_arguments)]
    pub fn replay_one_event_through_cascade(
        &self,
        entry: &crate::state::event_log::LogEntry,
        _target: &dyn crate::engine::cascade_target::CascadeTarget,
        primary_stream: &str,
        input_shard: &mut crate::shard::Shard,
        input_shard_idx: usize,
        now: SystemTime,
    ) -> Result<(), BeavaError> {
        // Phase 11-06 format-tag aware payload decode.
        let (_, body) = crate::state::event_log::decode_log_payload(&entry.payload);
        let event: serde_json::Value = serde_json::from_slice(body).map_err(|e| {
            BeavaError::Protocol(format!(
                "boot replay: LogEntry JSON parse error for stream {}: {}",
                primary_stream, e
            ))
        })?;
        // sibling_shards = None drives all cascade writes inline via the
        // input_shard's StoreView fast path. At N=1 this is fully correct.
        // At N>1 the caller must additionally route cross-shard outputs by
        // re-driving `target.dispatch_batch(target_shard_idx, ...)` with
        // the computed output-shard routing; the current build-out targets
        // the N=1 boot-rematerialization path (sufficient for the
        // boot_rematerialization W3 tests) and leaves cross-shard fan-out
        // at boot-replay time as a 55-NEXT follow-up.
        let _ = self.push_with_cascade_on_shard(
            primary_stream,
            &event,
            input_shard,
            None,
            now,
            false,
            None,
            input_shard_idx,
        )?;
        Ok(())
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

    /// Phase 24-02: Return true iff a stream with the given name was
    /// registered as a v0 Table source. Detection relies on the stored raw
    /// REGISTER JSON carrying `"kind": "table"` — the v0 source descriptor
    /// always sets this field. For v2.0-style registrations (no `kind`)
    /// this returns false, which matches the intent: v2.0 had no Table-row
    /// concept so `OP_PUSH_TABLE` is rejected against such streams.
    pub fn has_registered_table(&self, name: &str) -> bool {
        self.raw_register_jsons
            .get(name)
            .and_then(|j| j.get("kind"))
            .and_then(|k| k.as_str())
            .map(|s| s == "table")
            .unwrap_or(false)
    }

    /// Phase 55-02 D-B1 (TPC-SOURCE-01): returns true iff `name` was
    /// registered as a @bv.source_table (kind=="source_table" in the raw
    /// REGISTER JSON). Dispatch-only — no side effects. Used by the
    /// OP_UPSERT_TABLE_ROW / OP_DELETE_TABLE_ROW wire paths + HTTP
    /// `POST /table/{name}` routes to reject writes against tables that
    /// aren't CDC-tagged.
    pub fn has_registered_source_table(&self, name: &str) -> bool {
        self.raw_register_jsons
            .get(name)
            .and_then(|j| j.get("kind"))
            .and_then(|k| k.as_str())
            .map(|s| s == "source_table")
            .unwrap_or(false)
    }

    /// Phase 59.5: returns true iff the named source_table was registered
    /// with `sharded=true`. Replicated (the Phase 59.5 default) and non-
    /// source-tables both return false. Used by the EnrichFromTable and
    /// Table×Table join operators to select local-state vs cross-shard
    /// read paths, and by OP_UPSERT_TABLE_ROW / OP_DELETE_TABLE_ROW
    /// dispatch to select single-shard vs fanout write paths.
    pub fn is_sharded_source_table(&self, name: &str) -> bool {
        self.raw_register_jsons
            .get(name)
            .and_then(|j| {
                // Only meaningful for source tables.
                if j.get("kind").and_then(|k| k.as_str()) != Some("source_table") {
                    return None;
                }
                j.get("sharded").and_then(|s| s.as_bool())
            })
            .unwrap_or(false)
    }

    /// Get the raw register JSON for a stream. Returns None if not found.
    pub fn get_raw_register_json(&self, name: &str) -> Option<&serde_json::Value> {
        self.raw_register_jsons.get(name)
    }

    // ======================== View management ========================

    /// Register a view definition. View names must be non-empty.
    /// Duplicate registration replaces the previous definition (idempotent).
    pub fn register_view(&mut self, view: ViewDefinition) -> Result<(), BeavaError> {
        if view.name.is_empty() {
            return Err(BeavaError::Protocol("view name must not be empty".into()));
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

    // Phase 54-04 Pass B: `ts` was consumed by the deleted legacy-push tests.
    // Retained (and #[allow(dead_code)]-marked) so reinstated Pass-C tests can
    // pick it back up without reintroducing a helper diff.
    #[allow(dead_code)]
    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn make_tx_stream() -> StreamDefinition {
        StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
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
            group_by_keys: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
                shard_key: None,
        };
        assert!(engine.register(stream).is_err());
    }

    #[test]
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_push_updates_all_operators() {
        // Phase 54-04 Pass B: legacy engine.push/push_for_backfill deleted. Test
        // body stubbed pending Pass C on_shard rewrite.
    }

    #[test]
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_push_missing_key_field_returns_error() {
        // Phase 54-04 Pass B: legacy engine.push/push_for_backfill deleted. Test
        // body stubbed pending Pass C on_shard rewrite.
    }

    #[test]
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_push_empty_key_rejected() {
        // Phase 54-04 Pass B: legacy engine.push/push_for_backfill deleted. Test
        // body stubbed pending Pass C on_shard rewrite.
    }

    #[test]
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_push_unknown_stream_returns_error() {
        // Phase 54-04 Pass B: legacy engine.push/push_for_backfill deleted. Test
        // body stubbed pending Pass C on_shard rewrite.
    }

    #[test]
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_push_3_events_verify_aggregates() {
        // Phase 54-04 Pass B: legacy engine.push/push_for_backfill deleted. Test
        // body stubbed pending Pass C on_shard rewrite.
    }

    // ======================== max_window_duration Tests ========================

    #[test]
    fn test_max_window_duration() {
        let mut engine = PipelineEngine::new();
        engine
            .register(StreamDefinition {
                name: "stream1".into(),
                key_field: Some("id".into()),
                group_by_keys: None,
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
                watermark_lateness: None,
                shard_key: None,
            })
            .unwrap();
        engine
            .register(StreamDefinition {
                name: "stream2".into(),
                key_field: Some("id".into()),
                group_by_keys: None,
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
                watermark_lateness: None,
                shard_key: None,
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
                group_by_keys: None,
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
                watermark_lateness: None,
                shard_key: None,
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
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_push_with_min_max_last_operators() {
        // Phase 54-04 Pass B: legacy engine.push/push_for_backfill deleted. Test
        // body stubbed pending Pass C on_shard rewrite.
    }

    // ======================== Phase 5: where-clause filtering Tests ========================

    #[test]
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_push_with_where_expr_filters_events() {
        // Phase 54-04 Pass B: legacy engine.push* deleted. Test body stubbed
        // pending Pass C on_shard rewrite.
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
                group_by_keys: None,
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
                watermark_lateness: None,
                shard_key: None,
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
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_view_derive_resolves_qualified_fields_from_two_streams() {
        // Phase 54-04 Pass B: legacy engine.push* deleted. Test body stubbed
        // pending Pass C on_shard rewrite.
    }

    #[test]
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_view_lookup_resolves_cross_key_feature() {
        // Phase 54-04 Pass B: legacy engine.push* deleted. Test body stubbed
        // pending Pass C on_shard rewrite.
    }

    #[test]
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_view_lookup_returns_missing_when_target_entity_not_found() {
        // Phase 54-04 Pass B: legacy engine.push deleted. Test body stubbed
        // pending Pass C on_shard rewrite.
    }

    // ======================== Phase 6 Plan 02: entity_ttl / history_ttl Tests ========================

    #[test]
    fn test_stream_definition_with_entity_ttl_stores_value() {
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: Some(Duration::from_secs(300)),
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
                shard_key: None,
        };
        assert_eq!(stream.entity_ttl, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_stream_definition_with_entity_ttl_none_is_backwards_compatible() {
        let stream = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
                shard_key: None,
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
                group_by_keys: None,
                features: vec![],
                depends_on: None,
                filter: None,
                entity_ttl: Some(Duration::from_secs(300)),
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
                watermark_lateness: None,
                shard_key: None,
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
                group_by_keys: None,
                features: vec![],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
                watermark_lateness: None,
                shard_key: None,
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
                group_by_keys: None,
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
                watermark_lateness: None,
                shard_key: None,
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
            group_by_keys: None,
            features: vec![],
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
                shard_key: None,
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
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
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
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
        };
        engine.register(stream).unwrap();
        assert_eq!(engine.stream_count(), 1);
    }

    #[test]
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_keyless_push_returns_empty() {
        // Phase 54-04 Pass B: legacy engine.push* deleted. Test body stubbed
        // pending Pass C on_shard rewrite.
    }

    #[test]
    fn test_keyed_with_depends_on_registers() {
        let mut engine = PipelineEngine::new();
        let stream = StreamDefinition {
            name: "Enriched".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
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
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
        };
        engine.register(stream).unwrap();
        assert_eq!(engine.stream_count(), 1);
    }

    #[test]
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_filter_blocks_non_matching_events() {
        // Phase 54-04 Pass B: legacy engine.push* deleted. Test body stubbed
        // pending Pass C on_shard rewrite.
    }

    #[test]
    fn test_fan_out_targets_excludes_keyless() {
        let mut engine = PipelineEngine::new();
        // Register a keyed stream
        engine
            .register(StreamDefinition {
                name: "Transactions".into(),
                key_field: Some("user_id".into()),
                group_by_keys: None,
                features: vec![],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
                watermark_lateness: None,
                shard_key: None,
            })
            .unwrap();
        // Register a keyless stream
        engine
            .register(StreamDefinition {
                name: "RawEvents".into(),
                key_field: None,
                group_by_keys: None,
                features: vec![],
                depends_on: None,
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
                watermark_lateness: None,
                shard_key: None,
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
            group_by_keys: None,
            features: vec![],
            entity_ttl: None,
            history_ttl: None,
            depends_on: Some(vec!["B".into()]),
            filter: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
                shard_key: None,
        };
        let b = StreamDefinition {
            name: "B".into(),
            key_field: Some("uid".into()),
            group_by_keys: None,
            features: vec![],
            entity_ttl: None,
            history_ttl: None,
            depends_on: Some(vec!["A".into()]),
            filter: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
                shard_key: None,
        };
        let a = StreamDefinition {
            name: "A".into(),
            key_field: None,
            group_by_keys: None,
            features: vec![],
            entity_ttl: None,
            history_ttl: None,
            depends_on: None,
            filter: None,
            projection: None,
            ephemeral: None,
            pipeline_ttl: None,
            max_keys: None,
            watermark_lateness: None,
                shard_key: None,
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
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_backward_compat_keyed_stream() {
        // Phase 54-04 Pass B: legacy engine.push deleted. Test body stubbed
        // pending Pass C on_shard rewrite.
    }

    // ======================== Phase 8 Plan 01: Schema Diff Tests ========================

    #[test]
    fn test_schema_diff_add_feature() {
        let mut engine = PipelineEngine::new();
        let stream1 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
        };
        let diff1 = engine.register(stream1).unwrap();
        assert!(diff1.added.contains(&"tx_count_1h".to_string()));
        assert!(diff1.removed.is_empty());

        // Re-register with added feature
        let stream2 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
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
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
        };
        engine.register(stream1).unwrap();

        // Re-register with removed feature
        let stream2 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
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
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
        };
        engine.register(stream1).unwrap();

        // Re-register with different type for same name
        let stream2 = StreamDefinition {
            name: "Transactions".into(),
            key_field: Some("user_id".into()),
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
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
            group_by_keys: None,
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
            watermark_lateness: None,
                shard_key: None,
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
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_reregister_preserves_state() {
        // Phase 54-04 Pass B: legacy engine.push* deleted. Test body stubbed
        // pending Pass C on_shard rewrite.
    }

    #[test]
    fn test_valid_features_map() {
        let mut engine = PipelineEngine::new();
        engine
            .register(StreamDefinition {
                name: "Transactions".into(),
                key_field: Some("user_id".into()),
                group_by_keys: None,
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
                watermark_lateness: None,
                shard_key: None,
            })
            .unwrap();
        engine
            .register(StreamDefinition {
                name: "Logins".into(),
                key_field: Some("user_id".into()),
                group_by_keys: None,
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
                watermark_lateness: None,
                shard_key: None,
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
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_push_for_backfill_targets_only_specified_features() {
        // Phase 54-04 Pass B: legacy engine.push/push_for_backfill deleted. Test
        // body stubbed pending Pass C on_shard rewrite.
    }

    #[test]
    #[ignore = "54-04 Pass B: legacy push helper deleted; Wave 4 Pass C re-enables via on_shard path"]
    fn test_push_for_backfill_uses_event_timestamp() {
        // Phase 54-04 Pass B: legacy engine.push/push_for_backfill deleted. Test
        // body stubbed pending Pass C on_shard rewrite.
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
