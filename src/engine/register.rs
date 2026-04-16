//! Phase 22-01: v0 REGISTER JSON payload consumer.
//!
//! This module defines the serde-level shape of the REGISTER payload as
//! emitted by `python/beava/_serialize.py::compile_to_register_json` and
//! provides `build_operator()` that dispatches per-feature-type to the
//! correct `OperatorState` variant.
//!
//! **Scope boundary (per 22-01 plan):** This file is the *dispatch scaffold*.
//! Actual operator math lives in the operator structs themselves — most
//! already exist from the v2.0 engine (CountOp, SumOp, AvgOp, MinOp, MaxOp,
//! LastOp, FirstOp, DistinctCountOp, StddevOp, PercentileOp, EmaOp, LagOp,
//! LastNOp). The three new stubs (VarianceOp, TopKOp, FirstNOp) are
//! dispatch-reachable but return Missing — plans 22-02 and 22-03 fill them.
//!
//! ## Payload shapes
//!
//! See `python/beava/_serialize.py` module docstring for the canonical
//! shapes. Five descriptor kinds: Source (stream/table), StatelessChain,
//! Aggregation, Join, Union. 22-01 parses all five; execution is
//! implemented only for the Aggregation branch (plus `count` / `sum`
//! smoke path), with Join / Union / StatelessChain recorded but pushed
//! to NotImplemented on event.
//!
//! ## Key-encoding convention
//!
//! Composite group_by keys are joined with `'|'` (ASCII 0x7C). Example:
//! keys=["user_id","merchant_id"] on event {"user_id":"u1","merchant_id":"m9"}
//! produces entity-key `"u1|m9"`. Single-key falls through untouched.
//! Phase 22-02 / 23 match this encoding verbatim.

use crate::engine::hll::DistinctCountOp;
use crate::engine::operators::{
    AvgOp, CountOp, EmaOp, FirstNOp, FirstOp, LagOp, LastNOp, LastOp, PercentileOp, StddevOp,
    SumOp, TopKOp, VarianceOp,
};
use crate::error::BeavaError;
use crate::state::snapshot::OperatorState;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Phase 25-02: v0 TTL defaults — applied at REGISTER-time when the SDK did not
// emit an explicit value. Locked by v0-restructure-spec §7.2.
// ---------------------------------------------------------------------------

/// Default `entity_ttl` (aka Table `ttl`) applied when a Table source / derivation
/// is registered without an explicit override. 30 days per spec.
pub const DEFAULT_TABLE_TTL: &str = "30d";

/// Default `history_ttl` applied when a Stream source is registered without an
/// explicit override. 90 days per spec. The existing `event_log::DEFAULT_HISTORY_TTL`
/// (72h) is the *fallback* used by the event log when no stream-level ttl was
/// plumbed through; Phase 25-02 now always plumbs through the 90d default at
/// registration time, so the 72h fallback never fires in normal operation.
pub const DEFAULT_STREAM_HISTORY_TTL: &str = "90d";

// ---------------------------------------------------------------------------
// Payload structs — serde-shaped to match python/beava/_serialize.py verbatim.
// ---------------------------------------------------------------------------

/// Top-level REGISTER JSON payload. One of five descriptor kinds.
///
/// The shape uses `serde(untagged)` because `_serialize.py` writes several
/// flavors that share `kind` but differ on the presence of `aggregation`,
/// `join`, `union`, or `ops`. The first matching variant wins — ordering
/// here matters (aggregation / join / union must come before StatelessChain
/// because all of them may carry `kind`, `fields`, `depends_on`).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum V0RegisterPayload {
    /// TableDerivation carrying an AggregationSpec.
    Aggregation(AggregationDescriptor),
    /// Stream/Table derivation carrying a JoinSpec (stub — Phase 23).
    Join(JoinDescriptor),
    /// StreamDerivation carrying a UnionSpec (stub — Phase 22-03 / 23).
    Union(UnionDescriptor),
    /// Stateless op-chain derivation over Stream or Table.
    StatelessChain(StatelessChainDescriptor),
    /// StreamSource / TableSource.
    Source(SourceDescriptor),
}

/// StreamSource / TableSource.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SourceDescriptor {
    pub name: String,
    pub kind: String, // "stream" | "table"
    #[serde(default)]
    pub key_field: Option<String>,
    #[serde(default)]
    pub key_fields: Option<Vec<String>>,
    #[serde(default)]
    pub mode: Option<String>, // table mode ("overwrite" | "append")
    pub fields: serde_json::Value,
    #[serde(default)]
    pub history_ttl: Option<String>,
    #[serde(default)]
    pub entity_ttl: Option<String>,
}

/// Stream/Table derivation with a stateless `ops: [...]` chain.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StatelessChainDescriptor {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub key_field: Option<String>,
    #[serde(default)]
    pub key_fields: Option<Vec<String>>,
    #[serde(default)]
    pub mode: Option<String>,
    pub fields: serde_json::Value,
    pub ops: Vec<serde_json::Value>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub entity_ttl: Option<String>,
}

/// TableDerivation with an `aggregation: {...}` block.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggregationDescriptor {
    pub name: String,
    pub kind: String, // always "table"
    #[serde(default)]
    pub key_field: Option<String>,
    #[serde(default)]
    pub key_fields: Option<Vec<String>>,
    #[serde(default)]
    pub mode: Option<String>,
    pub fields: serde_json::Value,
    pub aggregation: AggregationSpec,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggregationSpec {
    pub source: String,
    pub keys: Vec<String>,
    pub features: Vec<AggregationFeature>,
}

/// Stream/Table derivation carrying a `join: {...}` block. Phase 23-01
/// consumes this via `v0_join_to_stream_def`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JoinDescriptor {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub key_field: Option<String>,
    #[serde(default)]
    pub key_fields: Option<Vec<String>>,
    #[serde(default)]
    pub mode: Option<String>,
    pub fields: serde_json::Value,
    pub join: JoinSpec,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// Typed shape of the `join: {...}` block. Mirrors
/// `python/beava/_join.py::JoinSpec._to_join_json()`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JoinSpec {
    #[serde(default)]
    pub op: String, // always "join" when emitted by the SDK; optional for test fixtures
    pub left: String,
    pub right: String,
    pub on: Vec<String>,
    #[serde(default)]
    pub within: Option<String>,
    #[serde(rename = "type")]
    pub type_: String,
    pub shape: String, // "stream_stream" | "stream_table" | "table_table"
}

/// StreamDerivation carrying a `union: {sources:[...]}` block. Stub.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UnionDescriptor {
    pub name: String,
    pub kind: String, // "stream"
    #[serde(default)]
    pub key_field: Option<String>,
    pub fields: serde_json::Value,
    pub union: serde_json::Value,
    #[serde(default)]
    pub depends_on: Vec<String>,
}

/// One feature inside an AggregationSpec.features[]. Fields match
/// `_agg_ops.AggOp.to_json()` — hybrid params are flattened at top level.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AggregationFeature {
    pub name: String,
    #[serde(rename = "type")]
    pub op_type: String,
    #[serde(default)]
    pub supports_retraction: bool,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub window: Option<String>,
    #[serde(default)]
    pub r#where: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    // Operator-specific extras.
    #[serde(default)]
    pub n: Option<usize>,
    #[serde(default)]
    pub quantile: Option<f64>,
    #[serde(default)]
    pub half_life: Option<String>,
    #[serde(default)]
    pub k: Option<usize>,
    // Hybrid sketch params (flattened per the SDK contract).
    #[serde(default)]
    pub exact_threshold: Option<usize>,
    #[serde(default)]
    pub hybrid_alpha: Option<f64>,
    #[serde(default)]
    pub hybrid_precision: Option<u8>,
    #[serde(default)]
    pub hybrid_width: Option<usize>,
    #[serde(default)]
    pub hybrid_depth: Option<usize>,
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

impl V0RegisterPayload {
    /// Parse a REGISTER JSON payload. Wraps serde_json errors into
    /// `BeavaError::Protocol` with a named reason for observability.
    pub fn parse(bytes: &[u8]) -> Result<Self, BeavaError> {
        // First, reject the pre-Phase-21 v2.0 shape up front: top-level
        // `features: [...]` without `aggregation`. The new shape always
        // carries `kind` and either `aggregation`, `ops`, `join`, or
        // `union` (or is a bare Source).
        let raw: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| {
            BeavaError::Protocol(format!("v0 REGISTER: invalid JSON: {}", e))
        })?;
        if raw.get("features").is_some() && raw.get("aggregation").is_none() {
            return Err(BeavaError::Protocol(
                "v0 REGISTER: legacy top-level 'features' array rejected — \
                 aggregation features now live under aggregation.features[] \
                 (Phase 21-03 contract)"
                    .into(),
            ));
        }
        if raw.get("kind").is_none() {
            return Err(BeavaError::Protocol(
                "v0 REGISTER: payload missing required 'kind' field".into(),
            ));
        }
        serde_json::from_value::<V0RegisterPayload>(raw).map_err(|e| {
            BeavaError::Protocol(format!("v0 REGISTER: payload shape mismatch: {}", e))
        })
    }

    /// Human-readable tag for logs / metrics.
    pub fn descriptor_kind(&self) -> &'static str {
        match self {
            V0RegisterPayload::Source(_) => "source",
            V0RegisterPayload::StatelessChain(_) => "op_chain",
            V0RegisterPayload::Aggregation(_) => "aggregation",
            V0RegisterPayload::Join(_) => "join",
            V0RegisterPayload::Union(_) => "union",
        }
    }

    /// Registered descriptor name.
    pub fn descriptor_name(&self) -> &str {
        match self {
            V0RegisterPayload::Source(d) => &d.name,
            V0RegisterPayload::StatelessChain(d) => &d.name,
            V0RegisterPayload::Aggregation(d) => &d.name,
            V0RegisterPayload::Join(d) => &d.name,
            V0RegisterPayload::Union(d) => &d.name,
        }
    }
}

// ---------------------------------------------------------------------------
// Window / bucket parsing
// ---------------------------------------------------------------------------

/// Parse a duration string like "30m" / "1h" / "24h" / "500ms" / "7d".
///
/// Matches the Python validator in `_agg_ops._validate_window` so the
/// engine accepts exactly what the SDK emits.
pub fn parse_window(s: &str, field_name: &str) -> Result<Duration, BeavaError> {
    if s.is_empty() {
        return Err(BeavaError::Protocol(format!(
            "v0 REGISTER: {} is empty",
            field_name
        )));
    }
    // Longest suffix first so "ms" matches before "s".
    let (num_str, suffix_secs): (&str, u64) = if let Some(n) = s.strip_suffix("ms") {
        // Milliseconds are sub-second: handle separately.
        let n: u64 = n.parse().map_err(|_| {
            BeavaError::Protocol(format!(
                "v0 REGISTER: {} has non-numeric prefix: {}",
                field_name, s
            ))
        })?;
        return Ok(Duration::from_millis(n));
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3600)
    } else if let Some(n) = s.strip_suffix('d') {
        (n, 86_400)
    } else {
        return Err(BeavaError::Protocol(format!(
            "v0 REGISTER: {} missing unit suffix (expected ms/s/m/h/d): {}",
            field_name, s
        )));
    };
    let n: u64 = num_str.parse().map_err(|_| {
        BeavaError::Protocol(format!(
            "v0 REGISTER: {} has non-numeric prefix: {}",
            field_name, s
        ))
    })?;
    Ok(Duration::from_secs(n * suffix_secs))
}

/// Default bucket granularity per 22-CONTEXT.md:
///   - 1 minute for windows >= 1h
///   - 1 second for windows < 1h
///
/// Honored only when the AggOp did not specify `bucket=` explicitly.
pub fn default_bucket(window: Duration) -> Duration {
    if window >= Duration::from_secs(3600) {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(1)
    }
}

/// Resolve (window, bucket) from an AggregationFeature, applying defaults.
/// Returns `None` for non-windowed operators (first/last/first_n/last_n/ema/lag).
pub fn resolve_window_bucket(
    feat: &AggregationFeature,
) -> Result<Option<(Duration, Duration)>, BeavaError> {
    match feat.window.as_deref() {
        None => Ok(None),
        Some(w) => {
            let window = parse_window(w, &format!("feature {}.window", feat.name))?;
            let bucket = match feat.bucket.as_deref() {
                Some(b) => parse_window(b, &format!("feature {}.bucket", feat.name))?,
                None => default_bucket(window),
            };
            Ok(Some((window, bucket)))
        }
    }
}

// ---------------------------------------------------------------------------
// Operator dispatch
// ---------------------------------------------------------------------------

/// Hard cap on `lag(n)` — per-entity memory budget enforcement.
/// Matches `python/beava/_agg_ops.py::_Lag` at the SDK layer; this is
/// the defense-in-depth ceiling on the engine side.
pub const LAG_N_CAP: usize = 10_000;

/// Build the `OperatorState` variant for a given aggregation feature,
/// rejecting `ema` / `lag` when the upstream is a Table source.
///
/// `source_kind` is `"stream"`, `"table"`, or (safest default) `"stream"`
/// if not known at call time. Phase 21 already rejects the
/// table-sourced ema/lag at SDK compile time; this is belt-and-suspenders.
pub fn build_operator_with_source_kind(
    feat: &AggregationFeature,
    source_kind: &str,
) -> Result<OperatorState, BeavaError> {
    // Defense-in-depth: ema / lag require Stream input.
    if matches!(feat.op_type.as_str(), "ema" | "lag") && source_kind == "table" {
        return Err(BeavaError::Protocol(format!(
            "v0 REGISTER: '{}' operator requires a Stream source; got a Table \
             upstream (redundant defense — Phase 21 rejects this at the SDK layer)",
            feat.op_type
        )));
    }
    build_operator(feat)
}

/// Build the `OperatorState` variant for a given aggregation feature.
///
/// For all 16 AggOp types this returns the correct variant wired to the
/// parsed window/bucket and operator-specific parameters. Unknown
/// `feat.type` values fail with `BeavaError::Protocol`.
///
/// Does not perform upstream-kind validation — use
/// [`build_operator_with_source_kind`] when the caller has the source
/// descriptor in scope (e.g. during REGISTER dispatch).
pub fn build_operator(feat: &AggregationFeature) -> Result<OperatorState, BeavaError> {
    let window_bucket = resolve_window_bucket(feat)?;

    let require_field = |ctx: &str| -> Result<String, BeavaError> {
        feat.field
            .clone()
            .ok_or_else(|| BeavaError::Protocol(format!("v0 REGISTER: {} requires 'field'", ctx)))
    };
    let require_window = |ctx: &str| -> Result<(Duration, Duration), BeavaError> {
        window_bucket.ok_or_else(|| {
            BeavaError::Protocol(format!("v0 REGISTER: {} requires 'window'", ctx))
        })
    };

    // Absent events: engine v0 treats missing fields as a soft skip here;
    // 21-03 validate already enforces schema, so the strict-mode flag on
    // v2.0 ops is set to `false` (optional=true) so stub/bodies don't
    // raise Type errors on retries.
    let optional = true;

    let state = match feat.op_type.as_str() {
        "count" => {
            let (w, b) = require_window("count")?;
            OperatorState::Count(CountOp::new(w, b))
        }
        "sum" => {
            let field = require_field("sum")?;
            let (w, b) = require_window("sum")?;
            OperatorState::Sum(SumOp::new(field, w, b, optional))
        }
        "avg" => {
            let field = require_field("avg")?;
            let (w, b) = require_window("avg")?;
            OperatorState::Avg(AvgOp::new(field, w, b, optional))
        }
        "min" => {
            let field = require_field("min")?;
            let (w, b) = require_window("min")?;
            // v0 uses the bucket-granular MinOp by default (not ExactMin),
            // matching 22-CONTEXT.md §"Linear operators". Retraction arrives
            // via bucket expiry only.
            OperatorState::Min(crate::engine::operators::MinOp::new(field, w, b, optional))
        }
        "max" => {
            let field = require_field("max")?;
            let (w, b) = require_window("max")?;
            OperatorState::Max(crate::engine::operators::MaxOp::new(field, w, b, optional))
        }
        "variance" => {
            let field = require_field("variance")?;
            let (w, b) = require_window("variance")?;
            OperatorState::Variance(VarianceOp::new(field, w, b, optional))
        }
        "stddev" => {
            let field = require_field("stddev")?;
            let (w, b) = require_window("stddev")?;
            OperatorState::Stddev(StddevOp::new(field, w, b, optional))
        }
        "percentile" => {
            let field = require_field("percentile")?;
            let (w, b) = require_window("percentile")?;
            let quantile = feat.quantile.ok_or_else(|| {
                BeavaError::Protocol("v0 REGISTER: percentile requires 'quantile'".into())
            })?;
            OperatorState::Percentile(PercentileOp::new(field, quantile, w, b, optional))
        }
        "count_distinct" => {
            let field = require_field("count_distinct")?;
            let (w, b) = require_window("count_distinct")?;
            OperatorState::DistinctCount(DistinctCountOp::new(field, w, b, optional))
        }
        "top_k" => {
            let field = require_field("top_k")?;
            let (w, b) = require_window("top_k")?;
            let k = feat
                .k
                .ok_or_else(|| BeavaError::Protocol("v0 REGISTER: top_k requires 'k'".into()))?;
            let exact_threshold = feat.exact_threshold.unwrap_or(1024);
            let hybrid_width = feat.hybrid_width.unwrap_or(2048);
            let hybrid_depth = feat.hybrid_depth.unwrap_or(4);
            OperatorState::TopK(TopKOp::new(
                field,
                k,
                w,
                b,
                exact_threshold,
                hybrid_width,
                hybrid_depth,
                optional,
            ))
        }
        "first" => {
            let field = require_field("first")?;
            OperatorState::First(FirstOp::new(field, optional))
        }
        "last" => {
            let field = require_field("last")?;
            OperatorState::Last(LastOp::new(field, optional))
        }
        "first_n" => {
            let field = require_field("first_n")?;
            let n = feat.n.ok_or_else(|| {
                BeavaError::Protocol("v0 REGISTER: first_n requires 'n'".into())
            })?;
            OperatorState::FirstN(FirstNOp::new(field, n, optional))
        }
        "last_n" => {
            let field = require_field("last_n")?;
            let n = feat
                .n
                .ok_or_else(|| BeavaError::Protocol("v0 REGISTER: last_n requires 'n'".into()))?;
            OperatorState::LastN(LastNOp::new(field, n, optional))
        }
        "ema" => {
            let field = require_field("ema")?;
            let half_life_str = feat.half_life.as_deref().ok_or_else(|| {
                BeavaError::Protocol("v0 REGISTER: ema requires 'half_life'".into())
            })?;
            let half_life = parse_window(half_life_str, "ema.half_life")?;
            OperatorState::Ema(EmaOp::new(field, half_life.as_secs_f64(), optional))
        }
        "lag" => {
            let field = require_field("lag")?;
            let n = feat
                .n
                .ok_or_else(|| BeavaError::Protocol("v0 REGISTER: lag requires 'n'".into()))?;
            if n == 0 || n > LAG_N_CAP {
                return Err(BeavaError::Protocol(format!(
                    "v0 REGISTER: lag.n={} out of range (1..={}); \
                     per-entity memory is bounded by this cap",
                    n, LAG_N_CAP
                )));
            }
            OperatorState::Lag(LagOp::new(field, n, optional))
        }
        other => {
            return Err(BeavaError::Protocol(format!(
                "v0 REGISTER: unknown aggregation op type: {}",
                other
            )));
        }
    };
    Ok(state)
}

// ---------------------------------------------------------------------------
// Key encoding
// ---------------------------------------------------------------------------

/// Encode the entity key for an aggregation output row.
///
/// Single-key: the raw field value (stringified). Composite: `'|'`-joined.
/// Returns `Err` if any group_by key is missing from the event.
pub fn encode_group_by(
    keys: &[String],
    event: &serde_json::Value,
) -> Result<String, BeavaError> {
    if keys.is_empty() {
        return Err(BeavaError::Protocol(
            "v0 REGISTER: aggregation.keys must be non-empty".into(),
        ));
    }
    let mut parts: Vec<String> = Vec::with_capacity(keys.len());
    for k in keys {
        match event.get(k) {
            Some(serde_json::Value::String(s)) => parts.push(s.clone()),
            Some(serde_json::Value::Number(n)) => parts.push(n.to_string()),
            Some(serde_json::Value::Bool(b)) => parts.push(b.to_string()),
            Some(serde_json::Value::Null) | None => {
                return Err(BeavaError::Type {
                    field: k.clone(),
                    expected: "group_by key value".into(),
                    got: "absent".into(),
                });
            }
            Some(other) => {
                return Err(BeavaError::Type {
                    field: k.clone(),
                    expected: "scalar (string|number|bool) group_by key".into(),
                    got: format!("{}", other),
                });
            }
        }
    }
    if parts.len() == 1 {
        Ok(parts.pop().unwrap())
    } else {
        Ok(parts.join("|"))
    }
}

// ---------------------------------------------------------------------------
// v0 → v2.0 translator (Plan 22-04 TCP wiring)
// ---------------------------------------------------------------------------

/// Translate a v0 `AggregationFeature` into a v2.0 `FeatureDef` so the
/// existing `PipelineEngine::register` / `push_with_cascade` machinery can
/// drive the new aggregation end-to-end without a parallel runtime.
///
/// Operators supported at the translation boundary: every linear op that v2.0
/// already supports (count / sum / avg / min / max / stddev / distinct_count
/// / last / first / lag / ema / last_n / percentile). Variance / top_k /
/// first_n are v0-only operators (22-02 / 22-03 landed the OperatorState
/// variants but no FeatureDef variant exists) — the translator returns
/// `BeavaError::Protocol` for these and 22-05 can add FeatureDef variants
/// when the operator set grows.
///
/// `where_expr` is translated straight through if present.
pub fn v0_feature_to_feature_def(
    feat: &AggregationFeature,
) -> Result<crate::engine::pipeline::FeatureDef, BeavaError> {
    use crate::engine::expression::parse_expr;
    use crate::engine::pipeline::FeatureDef;

    let window_bucket = resolve_window_bucket(feat)?;
    let require_field = |ctx: &str| -> Result<String, BeavaError> {
        feat.field
            .clone()
            .ok_or_else(|| BeavaError::Protocol(format!("v0→v2 xlate: {} requires 'field'", ctx)))
    };
    let require_window = |ctx: &str| -> Result<(Duration, Duration), BeavaError> {
        window_bucket.ok_or_else(|| {
            BeavaError::Protocol(format!("v0→v2 xlate: {} requires 'window'", ctx))
        })
    };
    let where_expr = match feat.r#where.as_deref() {
        Some(expr_str) if !expr_str.is_empty() => Some(parse_expr(expr_str).map_err(|e| {
            BeavaError::Protocol(format!("v0→v2 xlate: invalid where expr: {}", e))
        })?),
        _ => None,
    };
    let optional = true;
    let backfill = false;

    let def = match feat.op_type.as_str() {
        "count" => {
            let (window, bucket) = require_window("count")?;
            FeatureDef::Count {
                window,
                bucket,
                where_expr,
                backfill,
            }
        }
        "sum" => {
            let field = require_field("sum")?;
            let (window, bucket) = require_window("sum")?;
            FeatureDef::Sum {
                field,
                window,
                bucket,
                optional,
                where_expr,
                backfill,
            }
        }
        "avg" => {
            let field = require_field("avg")?;
            let (window, bucket) = require_window("avg")?;
            FeatureDef::Avg {
                field,
                window,
                bucket,
                optional,
                where_expr,
                backfill,
            }
        }
        "min" => {
            let field = require_field("min")?;
            let (window, bucket) = require_window("min")?;
            FeatureDef::Min {
                field,
                window,
                bucket,
                optional,
                where_expr,
                backfill,
            }
        }
        "max" => {
            let field = require_field("max")?;
            let (window, bucket) = require_window("max")?;
            FeatureDef::Max {
                field,
                window,
                bucket,
                optional,
                where_expr,
                backfill,
            }
        }
        "stddev" => {
            let field = require_field("stddev")?;
            let (window, bucket) = require_window("stddev")?;
            FeatureDef::Stddev {
                field,
                window,
                bucket,
                optional,
                where_expr,
                backfill,
            }
        }
        "count_distinct" => {
            let field = require_field("count_distinct")?;
            let (window, bucket) = require_window("count_distinct")?;
            FeatureDef::DistinctCount {
                field,
                window,
                bucket,
                optional,
                where_expr,
                backfill,
            }
        }
        "percentile" => {
            let field = require_field("percentile")?;
            let (window, bucket) = require_window("percentile")?;
            let quantile = feat.quantile.ok_or_else(|| {
                BeavaError::Protocol("v0→v2 xlate: percentile requires 'quantile'".into())
            })?;
            FeatureDef::Percentile {
                field,
                quantile,
                window,
                bucket,
                optional,
                where_expr,
                backfill,
            }
        }
        "first" => {
            let field = require_field("first")?;
            FeatureDef::First {
                field,
                optional,
                backfill,
            }
        }
        "last" => {
            let field = require_field("last")?;
            FeatureDef::Last {
                field,
                optional,
                backfill,
            }
        }
        "last_n" => {
            let field = require_field("last_n")?;
            let n = feat
                .n
                .ok_or_else(|| BeavaError::Protocol("v0→v2 xlate: last_n requires 'n'".into()))?;
            FeatureDef::LastN {
                field,
                n,
                optional,
                backfill,
            }
        }
        "ema" => {
            let field = require_field("ema")?;
            let hl = feat
                .half_life
                .as_deref()
                .ok_or_else(|| BeavaError::Protocol("v0→v2 xlate: ema requires 'half_life'".into()))?;
            let half_life_secs = parse_window(hl, "ema.half_life")?.as_secs_f64();
            FeatureDef::Ema {
                field,
                half_life_secs,
                optional,
                backfill,
            }
        }
        "lag" => {
            let field = require_field("lag")?;
            let n = feat
                .n
                .ok_or_else(|| BeavaError::Protocol("v0→v2 xlate: lag requires 'n'".into()))?;
            if n == 0 || n > LAG_N_CAP {
                return Err(BeavaError::Protocol(format!(
                    "v0→v2 xlate: lag.n={} out of range (1..={})",
                    n, LAG_N_CAP
                )));
            }
            FeatureDef::Lag {
                field,
                n,
                optional,
                backfill,
            }
        }
        // v0-only operators: no v2.0 FeatureDef variant yet.
        other @ ("variance" | "top_k" | "first_n") => {
            return Err(BeavaError::Protocol(format!(
                "v0→v2 xlate: op '{}' has no v2.0 FeatureDef equivalent; \
                 add a FeatureDef variant in pipeline.rs to enable end-to-end wiring",
                other
            )));
        }
        other => {
            return Err(BeavaError::Protocol(format!(
                "v0→v2 xlate: unknown aggregation op type: {}",
                other
            )));
        }
    };
    Ok(def)
}

/// Translate a v0 `AggregationDescriptor` into a v2.0 `StreamDefinition`.
/// Single-key group_by maps directly to `key_field`; composite keys error for
/// now (Plan 22-04 scope — composite keys land alongside joins in Phase 23).
pub fn v0_aggregation_to_stream_def(
    desc: &AggregationDescriptor,
) -> Result<crate::engine::pipeline::StreamDefinition, BeavaError> {
    use crate::engine::pipeline::StreamDefinition;

    if desc.aggregation.keys.is_empty() {
        return Err(BeavaError::Protocol(
            "v0→v2 xlate: aggregation.keys must be non-empty".into(),
        ));
    }
    // Phase 23-01: composite group_by keys lifted. `encode_group_by` composes
    // the entity key string from multiple event fields (`k1|k2|...`). The
    // single-key fast path is preserved — `encode_group_by` of a one-element
    // slice returns just the scalar string.
    let keys = &desc.aggregation.keys;
    let (key_field, group_by_keys) = if keys.len() == 1 {
        (Some(keys[0].clone()), None)
    } else {
        // Composite: `key_field` still points at keys[0] for downstream code
        // paths that want a representative field name, but `group_by_keys` is
        // what drives the actual entity-key encoding in `push_internal`.
        (Some(keys[0].clone()), Some(keys.clone()))
    };
    let mut features: Vec<(String, crate::engine::pipeline::FeatureDef)> =
        Vec::with_capacity(desc.aggregation.features.len());
    for feat in &desc.aggregation.features {
        let def = v0_feature_to_feature_def(feat)?;
        features.push((feat.name.clone(), def));
    }
    Ok(StreamDefinition {
        name: desc.name.clone(),
        key_field,
        group_by_keys,
        features,
        depends_on: Some(vec![desc.aggregation.source.clone()]),
        filter: None,
        entity_ttl: None,
        history_ttl: None,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
    })
}

/// Translate a v0 `SourceDescriptor` (stream kind) into a v2.0
/// `StreamDefinition` with no features — a raw ingestion stream. Downstream
/// aggregations use `depends_on` to fan out from the source.
pub fn v0_source_to_stream_def(
    desc: &SourceDescriptor,
) -> Result<crate::engine::pipeline::StreamDefinition, BeavaError> {
    use crate::engine::pipeline::StreamDefinition;

    if desc.kind != "stream" && desc.kind != "table" {
        return Err(BeavaError::Protocol(format!(
            "v0→v2 xlate: SourceDescriptor.kind must be 'stream' or 'table', got '{}'",
            desc.kind
        )));
    }
    // For kind=stream: a keyless ingestion stream with no features. Push
    // events flow through the cascade to any dependent aggregation streams.
    // For kind=table: a single-key target (direct writes via SET/MSET).
    // Phase 23-01: Table sources may declare a composite key via `key_fields`.
    // In that case, SET lookups / Stream↔Table enrichment must look the entity
    // up under the pipe-encoded composite key. Stash the field list on
    // `group_by_keys` so consumers have a consistent accessor.
    let (key_field, group_by_keys) = match (&desc.key_field, &desc.key_fields) {
        (Some(k), _) => (Some(k.clone()), None),
        (None, Some(ks)) if ks.len() == 1 => (Some(ks[0].clone()), None),
        (None, Some(ks)) if !ks.is_empty() => (Some(ks[0].clone()), Some(ks.clone())),
        _ => (None, None),
    };

    // Phase 25-02: apply v0 TTL defaults if the SDK did not specify one.
    // Table sources → DEFAULT_TABLE_TTL (30d).
    // Stream sources → DEFAULT_STREAM_HISTORY_TTL (90d) for event-log retention.
    // The SDK may emit "forever" / "0" — those flow through unchanged.
    use crate::duration::parse_duration_str;
    let entity_ttl = if desc.kind == "table" {
        let s = desc
            .entity_ttl
            .as_deref()
            .unwrap_or(DEFAULT_TABLE_TTL);
        Some(parse_duration_str(s)?)
    } else {
        // For stream sources, entity_ttl is orthogonal to history_ttl and
        // left unset so global TTL behavior applies.
        desc.entity_ttl
            .as_deref()
            .map(parse_duration_str)
            .transpose()?
    };
    let history_ttl = if desc.kind == "stream" {
        let s = desc
            .history_ttl
            .as_deref()
            .unwrap_or(DEFAULT_STREAM_HISTORY_TTL);
        Some(parse_duration_str(s)?)
    } else {
        // Tables don't have an event log history.
        desc.history_ttl
            .as_deref()
            .map(parse_duration_str)
            .transpose()?
    };

    Ok(StreamDefinition {
        name: desc.name.clone(),
        key_field,
        group_by_keys,
        features: Vec::new(),
        depends_on: None,
        filter: None,
        entity_ttl,
        history_ttl,
        projection: None,
        ephemeral: None,
        pipeline_ttl: None,
        max_keys: None,
    })
}

// ---------------------------------------------------------------------------
// Join translation — Phase 23-01
// ---------------------------------------------------------------------------

/// Translate a v0 `JoinDescriptor` into a v2.0 `StreamDefinition`.
///
/// Phase 23-01 implements `shape="stream_table"` (enrichment join). The
/// `stream_stream` and `table_table` shapes are deliberately stubbed here —
/// Plans 23-02 and 23-03 replace the stub with typed implementations.
///
/// `left_fields_lookup` is a closure that returns the left stream's own
/// field names (the raw source schema), so the translator can partition
/// `desc.fields` into left-side keys vs right-side keys. When `None`, the
/// translator falls back to a conservative heuristic: fields with a
/// `_right*` suffix are right-side; everything else is left-side (and thus
/// does NOT need to be materialized by the enrichment).
pub fn v0_join_to_stream_def(
    desc: &JoinDescriptor,
    left_fields_lookup: Option<&dyn Fn(&str) -> Option<Vec<String>>>,
) -> Result<crate::engine::pipeline::StreamDefinition, BeavaError> {
    // Phase 23-03: for table_table, re-enter the keyed variant with no source
    // meta (permissive — assumes SDK already validated key declarations).
    v0_join_to_stream_def_with_meta(desc, left_fields_lookup, None)
}

/// Phase 23-03 test-harness companion that accepts a separate `key_lookup`
/// closure returning the ordered key declaration for a named source. The
/// field-type dimension is omitted — tests that need key-type mismatch
/// validation use `v0_join_to_stream_def_with_meta` directly.
pub fn v0_join_to_stream_def_with_keys(
    desc: &JoinDescriptor,
    fields_lookup: Option<&dyn Fn(&str) -> Option<Vec<String>>>,
    key_lookup: Option<&dyn Fn(&str) -> Option<Vec<String>>>,
) -> Result<crate::engine::pipeline::StreamDefinition, BeavaError> {
    let meta_adapter = key_lookup.map(|kl| {
        move |name: &str| -> Option<(Vec<String>, Vec<(String, String)>)> {
            kl(name).map(|keys| (keys, Vec::new()))
        }
    });
    // Borrow the adapter as a trait object for the call.
    let meta_ref: Option<&dyn Fn(&str) -> Option<(Vec<String>, Vec<(String, String)>)>> =
        meta_adapter.as_ref().map(|f| f as &dyn Fn(&str) -> _);
    v0_join_to_stream_def_with_meta(desc, fields_lookup, meta_ref)
}

/// Phase 23-03 companion to `v0_join_to_stream_def` that accepts a richer
/// source-meta lookup used by the `table_table` branch to validate that
/// both input Tables declare identical key fields and (when the lookup
/// reveals schema types) identical key types.
///
/// `source_meta_lookup(name)` returns `(key_fields, fields)` for the named
/// source if registered. `key_fields` is the ordered key declaration (from
/// `key_field` single-key sources it is `[k]`); `fields` is an ordered list
/// of `(field_name, type_str)` tuples suitable for equality checks.
#[allow(clippy::type_complexity)]
pub fn v0_join_to_stream_def_with_meta(
    desc: &JoinDescriptor,
    left_fields_lookup: Option<&dyn Fn(&str) -> Option<Vec<String>>>,
    source_meta_lookup: Option<
        &dyn Fn(&str) -> Option<(Vec<String>, Vec<(String, String)>)>,
    >,
) -> Result<crate::engine::pipeline::StreamDefinition, BeavaError> {
    use crate::engine::pipeline::{FeatureDef, JoinType, StreamDefinition};

    // Registration-time rejections — T-23-04 (outer) and unknown types.
    let join_type = match desc.join.type_.as_str() {
        "inner" => JoinType::Inner,
        "left" => JoinType::Left,
        "outer" => {
            return Err(BeavaError::Protocol(
                "v0 REGISTER: outer joins deferred to v0.1; use two inner+left \
                 joins unioned as a workaround"
                    .into(),
            ));
        }
        other => {
            return Err(BeavaError::Protocol(format!(
                "v0 REGISTER: join type must be 'inner' or 'left', got '{}'",
                other
            )));
        }
    };

    if desc.join.on.is_empty() {
        return Err(BeavaError::Protocol(
            "v0 REGISTER: join.on must declare at least one key field".into(),
        ));
    }

    // Shape dispatch.
    match desc.join.shape.as_str() {
        "stream_table" => {
            // Identify the set of right-side fields to materialize from the
            // Table. Start with the full output schema, remove join keys,
            // remove left-side fields (if lookup provided), remove any name
            // already present on the left schema.
            let on_set: std::collections::HashSet<&str> =
                desc.join.on.iter().map(|s| s.as_str()).collect();
            let left_schema: Option<std::collections::HashSet<String>> =
                left_fields_lookup.and_then(|lookup| {
                    lookup(&desc.join.left).map(|v| v.into_iter().collect())
                });

            // `desc.fields` is a JSON object whose keys are output field
            // names. We iterate the keys in document order.
            let fields_obj = desc.fields.as_object().ok_or_else(|| {
                BeavaError::Protocol(
                    "v0 REGISTER: join.fields must be a JSON object".into(),
                )
            })?;
            let mut right_fields: Vec<(String, String)> = Vec::new();
            for (emitted_name, _spec) in fields_obj {
                if on_set.contains(emitted_name.as_str()) {
                    continue;
                }
                // `_right` suffixes mean an SDK-applied collision rename —
                // unambiguously a right-side field. Source = strip suffix.
                if let Some(base) = strip_right_suffix(emitted_name) {
                    right_fields.push((base, emitted_name.clone()));
                    continue;
                }
                // When the left schema is known, skip fields that belong to
                // the left. Otherwise conservatively include the field as a
                // right-side passthrough (source_name == emitted_name).
                if let Some(ls) = &left_schema {
                    if ls.contains(emitted_name) {
                        continue;
                    }
                    right_fields.push((emitted_name.clone(), emitted_name.clone()));
                } else {
                    // Without left-schema knowledge, skip bare names — they're
                    // most likely left-side fields already present on the
                    // event. (The `_right` suffix loop above still catches
                    // any renamed right-side field.)
                }
            }

            let single_feature_name = format!("__enrich_from_{}", desc.join.right);
            let feature = FeatureDef::EnrichFromTable {
                right_table: desc.join.right.clone(),
                on: desc.join.on.clone(),
                join_type,
                right_fields,
            };

            Ok(StreamDefinition {
                name: desc.name.clone(),
                // Enrichment output is a keyless stream — it cascades events
                // to downstream aggregations without storing its own state.
                key_field: None,
                group_by_keys: None,
                features: vec![(single_feature_name, feature)],
                depends_on: Some(desc.depends_on.clone()),
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
        }
        "stream_stream" => {
            // Phase 23-02: symmetric interval windowed join with per-key
            // event-time-indexed buffers on both sides.
            let within_str = desc.join.within.as_ref().ok_or_else(|| {
                BeavaError::Protocol(
                    "v0 REGISTER: stream_stream join requires within=<duration> \
                     (e.g. '30s' / '5m'); missing `within` field"
                        .into(),
                )
            })?;
            let within = parse_window(within_str, "join.within")?;
            let within_ms = within.as_millis() as u64;

            // Partition the output schema into left-side fields (passthrough
            // from the left event) and right-side fields (lifted from the
            // right event on match). Mirrors the Plan 23-01 stream_table
            // partitioning logic.
            let on_set: std::collections::HashSet<&str> =
                desc.join.on.iter().map(|s| s.as_str()).collect();
            let left_schema: Option<std::collections::HashSet<String>> =
                left_fields_lookup.and_then(|lookup| {
                    lookup(&desc.join.left).map(|v| v.into_iter().collect())
                });
            let right_schema: Option<std::collections::HashSet<String>> =
                left_fields_lookup.and_then(|lookup| {
                    lookup(&desc.join.right).map(|v| v.into_iter().collect())
                });

            let fields_obj = desc.fields.as_object().ok_or_else(|| {
                BeavaError::Protocol(
                    "v0 REGISTER: join.fields must be a JSON object".into(),
                )
            })?;

            let mut left_fields: Vec<String> = Vec::new();
            let mut right_fields: Vec<(String, String)> = Vec::new();
            for (emitted_name, _spec) in fields_obj {
                if on_set.contains(emitted_name.as_str()) {
                    // Join keys come from both sides (equal by definition).
                    // Record them as left-side passthrough.
                    left_fields.push(emitted_name.clone());
                    continue;
                }
                if let Some(base) = strip_right_suffix(emitted_name) {
                    right_fields.push((base, emitted_name.clone()));
                    continue;
                }
                // When schemas known: left-side if in left_schema; otherwise
                // right-side passthrough.
                if let Some(ls) = &left_schema {
                    if ls.contains(emitted_name) {
                        left_fields.push(emitted_name.clone());
                        continue;
                    }
                }
                if let Some(rs) = &right_schema {
                    if rs.contains(emitted_name) {
                        right_fields.push((emitted_name.clone(), emitted_name.clone()));
                        continue;
                    }
                }
                // Conservative fallback: passthrough from left (same policy
                // as stream_table; the `_right` suffix branch above already
                // catches the collision-renamed right-side slots).
                left_fields.push(emitted_name.clone());
            }

            let single_feature_name = format!(
                "__stream_join_{}_{}", desc.join.left, desc.join.right
            );
            let feature = FeatureDef::StreamStreamJoin {
                left_stream: desc.join.left.clone(),
                right_stream: desc.join.right.clone(),
                on: desc.join.on.clone(),
                within_ms,
                join_type,
                left_fields,
                right_fields,
            };

            Ok(StreamDefinition {
                name: desc.name.clone(),
                // Keyless — the join emits synthesized events. Buffer state
                // is keyed per composite key of `on` via the cascade.
                key_field: None,
                group_by_keys: Some(desc.join.on.clone()),
                features: vec![(single_feature_name, feature)],
                depends_on: Some(desc.depends_on.clone()),
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
        }
        "table_table" => {
            use crate::engine::pipeline::{FeatureDef, StreamDefinition};

            // Validate that both input Tables declare identical key fields.
            // When source meta is provided (TCP path), also validate key types
            // and partition output fields into left/right buckets.
            let (left_keys, left_fields_full): (
                Vec<String>,
                Option<Vec<(String, String)>>,
            ) = match source_meta_lookup.and_then(|l| l(&desc.join.left)) {
                Some((k, f)) => (k, Some(f)),
                None => (desc.join.on.clone(), None),
            };
            let (right_keys, right_fields_full): (
                Vec<String>,
                Option<Vec<(String, String)>>,
            ) = match source_meta_lookup.and_then(|l| l(&desc.join.right)) {
                Some((k, f)) => (k, Some(f)),
                None => (desc.join.on.clone(), None),
            };

            // Keys must be set-equal (same names, same count).
            if left_keys.len() != right_keys.len()
                || left_keys.iter().any(|k| !right_keys.contains(k))
                || right_keys.iter().any(|k| !left_keys.contains(k))
            {
                return Err(BeavaError::Protocol(format!(
                    "v0 REGISTER: Table↔Table join requires identical key declarations \
                     (both key field names must match); left='{}' keys={:?}, \
                     right='{}' keys={:?}",
                    desc.join.left, left_keys, desc.join.right, right_keys
                )));
            }

            // Full-key requirement: desc.join.on must be set-equal to both
            // tables' key declarations (no partial-key joins in v0).
            if desc.join.on.len() != left_keys.len()
                || desc.join.on.iter().any(|o| !left_keys.contains(o))
            {
                return Err(BeavaError::Protocol(format!(
                    "v0 REGISTER: Table↔Table join requires full-key match; \
                     v0 requires full-key required in v0 — on={:?} does not cover \
                     table key declaration {:?}",
                    desc.join.on, left_keys
                )));
            }

            // Type validation on key fields — only when both schemas are
            // available. Emits a schema_mismatch_error-style message.
            if let (Some(lf), Some(rf)) = (&left_fields_full, &right_fields_full) {
                for k in &left_keys {
                    let lt = lf.iter().find(|(n, _)| n == k).map(|(_, t)| t.as_str());
                    let rt = rf.iter().find(|(n, _)| n == k).map(|(_, t)| t.as_str());
                    if let (Some(a), Some(b)) = (lt, rt) {
                        if a != b {
                            return Err(BeavaError::Protocol(format!(
                                "v0 REGISTER: Table↔Table join schema_mismatch_error: \
                                 key field '{}' type differs between '{}' ({}) and \
                                 '{}' ({})",
                                k, desc.join.left, a, desc.join.right, b
                            )));
                        }
                    }
                }
            }

            // Cycle guard (defense-in-depth; SDK DAG build rejects cycles).
            if desc.depends_on.contains(&desc.name)
                || desc.join.left == desc.name
                || desc.join.right == desc.name
            {
                return Err(BeavaError::Protocol(
                    "v0 REGISTER: Table↔Table join would create a cycle (output \
                     references itself)"
                        .into(),
                ));
            }

            // Partition output schema into left / right buckets using the
            // same heuristic as stream_stream.
            let on_set: std::collections::HashSet<&str> =
                desc.join.on.iter().map(|s| s.as_str()).collect();
            let left_schema: Option<std::collections::HashSet<String>> =
                left_fields_lookup.and_then(|l| {
                    l(&desc.join.left).map(|v| v.into_iter().collect())
                });
            let right_schema: Option<std::collections::HashSet<String>> =
                left_fields_lookup.and_then(|l| {
                    l(&desc.join.right).map(|v| v.into_iter().collect())
                });

            let fields_obj = desc.fields.as_object().ok_or_else(|| {
                BeavaError::Protocol(
                    "v0 REGISTER: join.fields must be a JSON object".into(),
                )
            })?;

            let mut left_fields: Vec<String> = Vec::new();
            let mut right_fields: Vec<(String, String)> = Vec::new();
            for (emitted_name, _spec) in fields_obj {
                if on_set.contains(emitted_name.as_str()) {
                    left_fields.push(emitted_name.clone());
                    continue;
                }
                if let Some(base) = strip_right_suffix(emitted_name) {
                    right_fields.push((base, emitted_name.clone()));
                    continue;
                }
                if let Some(ls) = &left_schema {
                    if ls.contains(emitted_name) {
                        left_fields.push(emitted_name.clone());
                        continue;
                    }
                }
                if let Some(rs) = &right_schema {
                    if rs.contains(emitted_name) {
                        right_fields.push((
                            emitted_name.clone(),
                            emitted_name.clone(),
                        ));
                        continue;
                    }
                }
                // Conservative fallback — surface the column as a right-side
                // passthrough so the output contains every schema entry.
                right_fields.push((emitted_name.clone(), emitted_name.clone()));
            }

            let single_feature_name = format!(
                "__table_join_{}_{}",
                desc.join.left, desc.join.right
            );
            let feature = FeatureDef::TableTableJoin {
                left_table: desc.join.left.clone(),
                right_table: desc.join.right.clone(),
                on: desc.join.on.clone(),
                join_type,
                left_fields,
                right_fields,
            };

            // Output Table shares the same key declaration. For composite
            // keys, carry `group_by_keys` so reads / cascade encode the key
            // the same way as Stream↔Table / Stream↔Stream.
            let (key_field, group_by_keys) = if left_keys.len() == 1 {
                (Some(left_keys[0].clone()), None)
            } else {
                (Some(left_keys[0].clone()), Some(left_keys.clone()))
            };

            Ok(StreamDefinition {
                name: desc.name.clone(),
                key_field,
                group_by_keys,
                features: vec![(single_feature_name, feature)],
                depends_on: Some(desc.depends_on.clone()),
                filter: None,
                entity_ttl: None,
                history_ttl: None,
                projection: None,
                ephemeral: None,
                pipeline_ttl: None,
                max_keys: None,
            })
        }
        other => Err(BeavaError::Protocol(format!(
            "v0 REGISTER: unknown join shape '{}'; expected stream_table / stream_stream / table_table",
            other
        ))),
    }
}

/// Strip `_right` / `_right2` / `_right3` ... suffix from a field name.
/// Returns `Some(base)` if the suffix matched, `None` otherwise. Mirrors
/// `_join.py::compute_joined_schema`'s collision renaming loop.
fn strip_right_suffix(name: &str) -> Option<String> {
    if let Some(base) = name.strip_suffix("_right") {
        return Some(base.to_string());
    }
    // _right{N} for N>=2 — the SDK appends index starting at 2.
    if let Some(idx_start) = name.rfind("_right") {
        let tail = &name[idx_start + "_right".len()..];
        if !tail.is_empty() && tail.chars().all(|c| c.is_ascii_digit()) {
            let base = &name[..idx_start];
            return Some(base.to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_feat(op_type: &str) -> AggregationFeature {
        AggregationFeature {
            name: format!("f_{}", op_type),
            op_type: op_type.into(),
            supports_retraction: false,
            field: Some("amount".into()),
            window: Some("1h".into()),
            r#where: None,
            bucket: None,
            n: None,
            quantile: None,
            half_life: None,
            k: None,
            exact_threshold: None,
            hybrid_alpha: None,
            hybrid_precision: None,
            hybrid_width: None,
            hybrid_depth: None,
        }
    }

    #[test]
    fn parse_window_all_suffixes() {
        assert_eq!(parse_window("500ms", "w").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_window("30s", "w").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_window("15m", "w").unwrap(), Duration::from_secs(15 * 60));
        assert_eq!(parse_window("2h", "w").unwrap(), Duration::from_secs(2 * 3600));
        assert_eq!(parse_window("7d", "w").unwrap(), Duration::from_secs(7 * 86_400));
    }

    #[test]
    fn parse_window_rejects_no_suffix() {
        assert!(parse_window("30", "w").is_err());
    }

    #[test]
    fn default_bucket_rules() {
        assert_eq!(default_bucket(Duration::from_secs(30 * 60)), Duration::from_secs(1));
        assert_eq!(default_bucket(Duration::from_secs(3600)), Duration::from_secs(60));
        assert_eq!(default_bucket(Duration::from_secs(24 * 3600)), Duration::from_secs(60));
    }

    #[test]
    fn build_all_16_op_types_dispatches() {
        // count — no field
        let mut f = mk_feat("count");
        f.field = None;
        assert!(matches!(build_operator(&f).unwrap(), OperatorState::Count(_)));

        for op in ["sum", "avg", "min", "max", "variance", "stddev"] {
            assert!(
                build_operator(&mk_feat(op)).is_ok(),
                "failed for op {}",
                op
            );
        }

        // percentile
        let mut f = mk_feat("percentile");
        f.quantile = Some(0.95);
        assert!(matches!(build_operator(&f).unwrap(), OperatorState::Percentile(_)));

        // count_distinct
        assert!(matches!(
            build_operator(&mk_feat("count_distinct")).unwrap(),
            OperatorState::DistinctCount(_)
        ));

        // top_k
        let mut f = mk_feat("top_k");
        f.k = Some(10);
        assert!(matches!(build_operator(&f).unwrap(), OperatorState::TopK(_)));

        // first / last — no window
        let mut f = mk_feat("first");
        f.window = None;
        assert!(matches!(build_operator(&f).unwrap(), OperatorState::First(_)));
        let mut f = mk_feat("last");
        f.window = None;
        assert!(matches!(build_operator(&f).unwrap(), OperatorState::Last(_)));

        // first_n / last_n — no window, needs n
        let mut f = mk_feat("first_n");
        f.window = None;
        f.n = Some(5);
        assert!(matches!(build_operator(&f).unwrap(), OperatorState::FirstN(_)));
        let mut f = mk_feat("last_n");
        f.window = None;
        f.n = Some(5);
        assert!(matches!(build_operator(&f).unwrap(), OperatorState::LastN(_)));

        // ema
        let mut f = mk_feat("ema");
        f.window = None;
        f.half_life = Some("30m".into());
        assert!(matches!(build_operator(&f).unwrap(), OperatorState::Ema(_)));

        // lag
        let mut f = mk_feat("lag");
        f.window = None;
        f.n = Some(3);
        assert!(matches!(build_operator(&f).unwrap(), OperatorState::Lag(_)));
    }

    #[test]
    fn build_operator_rejects_unknown_type() {
        let mut f = mk_feat("unknown_op_xyz");
        f.field = None;
        let err = build_operator(&f).unwrap_err();
        assert!(matches!(err, BeavaError::Protocol(_)));
    }

    #[test]
    fn build_operator_requires_window_for_sum() {
        let mut f = mk_feat("sum");
        f.window = None;
        assert!(build_operator(&f).is_err());
    }

    #[test]
    fn build_operator_requires_field_for_sum() {
        let mut f = mk_feat("sum");
        f.field = None;
        assert!(build_operator(&f).is_err());
    }

    #[test]
    fn encode_group_by_single_key() {
        let ev = serde_json::json!({"user_id": "u1", "amount": 50});
        assert_eq!(
            encode_group_by(&["user_id".into()], &ev).unwrap(),
            "u1"
        );
    }

    #[test]
    fn encode_group_by_composite() {
        let ev = serde_json::json!({"user_id": "u1", "merchant_id": "m9"});
        assert_eq!(
            encode_group_by(&["user_id".into(), "merchant_id".into()], &ev).unwrap(),
            "u1|m9"
        );
    }

    #[test]
    fn encode_group_by_missing_field_errors() {
        let ev = serde_json::json!({"user_id": "u1"});
        assert!(encode_group_by(&["user_id".into(), "merchant_id".into()], &ev).is_err());
    }

    #[test]
    fn encode_group_by_numeric_key() {
        let ev = serde_json::json!({"uid": 42});
        assert_eq!(encode_group_by(&["uid".into()], &ev).unwrap(), "42");
    }

    // ---- V0RegisterPayload parsing ----

    #[test]
    fn parse_rejects_legacy_v2_shape() {
        let legacy = br#"{"name":"X","key_field":"user_id","features":[]}"#;
        let err = V0RegisterPayload::parse(legacy).unwrap_err();
        match err {
            BeavaError::Protocol(msg) => assert!(
                msg.contains("legacy top-level 'features'"),
                "unexpected msg: {}",
                msg
            ),
            _ => panic!("expected Protocol error"),
        }
    }

    #[test]
    fn parse_rejects_missing_kind() {
        let bad = br#"{"name":"X","fields":{}}"#;
        assert!(V0RegisterPayload::parse(bad).is_err());
    }

    #[test]
    fn parse_source_stream() {
        let json = br#"{"name":"Clicks","kind":"stream","key_field":null,"fields":{}}"#;
        let p = V0RegisterPayload::parse(json).unwrap();
        assert_eq!(p.descriptor_kind(), "source");
        assert_eq!(p.descriptor_name(), "Clicks");
    }

    #[test]
    fn parse_source_table_composite_key() {
        let json = br#"{
            "name":"UserCtx","kind":"table","mode":"overwrite",
            "key_field":null,"key_fields":["user_id","region"],"fields":{}
        }"#;
        let p = V0RegisterPayload::parse(json).unwrap();
        assert_eq!(p.descriptor_kind(), "source");
    }

    #[test]
    fn parse_aggregation() {
        let json = br#"{
            "name":"UserSpend","kind":"table","key_field":"user_id","mode":"overwrite",
            "fields":{},
            "aggregation":{
                "source":"Checkouts","keys":["user_id"],
                "features":[
                    {"name":"n","type":"count","supports_retraction":true,"window":"1h"},
                    {"name":"total","type":"sum","supports_retraction":true,"field":"amount","window":"1h"}
                ]
            },
            "depends_on":["Checkouts"]
        }"#;
        let p = V0RegisterPayload::parse(json).unwrap();
        assert_eq!(p.descriptor_kind(), "aggregation");
        if let V0RegisterPayload::Aggregation(d) = p {
            assert_eq!(d.aggregation.source, "Checkouts");
            assert_eq!(d.aggregation.features.len(), 2);
            assert_eq!(d.aggregation.features[0].op_type, "count");
            assert_eq!(d.aggregation.features[1].op_type, "sum");
        } else {
            panic!("expected Aggregation variant");
        }
    }

    #[test]
    fn parse_op_chain() {
        let json = br#"{
            "name":"Big","kind":"stream","key_field":null,"fields":{},
            "ops":[{"kind":"filter","expr":"amount > 100"}],
            "depends_on":["Checkouts"]
        }"#;
        let p = V0RegisterPayload::parse(json).unwrap();
        assert_eq!(p.descriptor_kind(), "op_chain");
    }

    #[test]
    fn parse_union() {
        let json = br#"{
            "name":"AllEvents","kind":"stream","key_field":null,"fields":{},
            "union":{"sources":["A","B"]},
            "depends_on":["A","B"]
        }"#;
        let p = V0RegisterPayload::parse(json).unwrap();
        assert_eq!(p.descriptor_kind(), "union");
    }

    // ---- Defense-in-depth for ema / lag ----

    #[test]
    fn ema_against_table_source_is_rejected() {
        let mut f = mk_feat("ema");
        f.window = None;
        f.half_life = Some("30m".into());
        let err = build_operator_with_source_kind(&f, "table").unwrap_err();
        match err {
            BeavaError::Protocol(msg) => {
                assert!(msg.contains("requires a Stream source"), "msg={}", msg)
            }
            _ => panic!("expected Protocol error"),
        }
    }

    #[test]
    fn lag_against_table_source_is_rejected() {
        let mut f = mk_feat("lag");
        f.window = None;
        f.n = Some(3);
        assert!(build_operator_with_source_kind(&f, "table").is_err());
    }

    #[test]
    fn ema_and_lag_against_stream_source_ok() {
        let mut f = mk_feat("ema");
        f.window = None;
        f.half_life = Some("30m".into());
        assert!(build_operator_with_source_kind(&f, "stream").is_ok());
        let mut f = mk_feat("lag");
        f.window = None;
        f.n = Some(5);
        assert!(build_operator_with_source_kind(&f, "stream").is_ok());
    }

    #[test]
    fn lag_with_n_over_cap_is_rejected() {
        let mut f = mk_feat("lag");
        f.window = None;
        f.n = Some(LAG_N_CAP + 1);
        let err = build_operator(&f).unwrap_err();
        match err {
            BeavaError::Protocol(msg) => assert!(msg.contains("out of range"), "msg={}", msg),
            _ => panic!("expected Protocol error"),
        }
    }

    #[test]
    fn lag_with_zero_n_is_rejected() {
        let mut f = mk_feat("lag");
        f.window = None;
        f.n = Some(0);
        assert!(build_operator(&f).is_err());
    }

    #[test]
    fn parse_join() {
        let json = br#"{
            "name":"Enriched","kind":"stream","key_field":null,"fields":{},
            "join":{"left":"Clicks","right":"Users","on":["user_id"],"type":"left","shape":"stream_table"},
            "depends_on":["Clicks","Users"]
        }"#;
        let p = V0RegisterPayload::parse(json).unwrap();
        assert_eq!(p.descriptor_kind(), "join");
    }
}
