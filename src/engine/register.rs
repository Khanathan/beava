//! Phase 22-01: v0 REGISTER JSON payload consumer.
//!
//! This module defines the serde-level shape of the REGISTER payload as
//! emitted by `python/tally/_serialize.py::compile_to_register_json` and
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
//! See `python/tally/_serialize.py` module docstring for the canonical
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
use crate::error::TallyError;
use crate::state::snapshot::OperatorState;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Payload structs — serde-shaped to match python/tally/_serialize.py verbatim.
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

/// Stream/Table derivation carrying a `join: {...}` block. Stub for 23.
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
    pub join: serde_json::Value,
    #[serde(default)]
    pub depends_on: Vec<String>,
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
    /// `TallyError::Protocol` with a named reason for observability.
    pub fn parse(bytes: &[u8]) -> Result<Self, TallyError> {
        // First, reject the pre-Phase-21 v2.0 shape up front: top-level
        // `features: [...]` without `aggregation`. The new shape always
        // carries `kind` and either `aggregation`, `ops`, `join`, or
        // `union` (or is a bare Source).
        let raw: serde_json::Value = serde_json::from_slice(bytes).map_err(|e| {
            TallyError::Protocol(format!("v0 REGISTER: invalid JSON: {}", e))
        })?;
        if raw.get("features").is_some() && raw.get("aggregation").is_none() {
            return Err(TallyError::Protocol(
                "v0 REGISTER: legacy top-level 'features' array rejected — \
                 aggregation features now live under aggregation.features[] \
                 (Phase 21-03 contract)"
                    .into(),
            ));
        }
        if raw.get("kind").is_none() {
            return Err(TallyError::Protocol(
                "v0 REGISTER: payload missing required 'kind' field".into(),
            ));
        }
        serde_json::from_value::<V0RegisterPayload>(raw).map_err(|e| {
            TallyError::Protocol(format!("v0 REGISTER: payload shape mismatch: {}", e))
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
pub fn parse_window(s: &str, field_name: &str) -> Result<Duration, TallyError> {
    if s.is_empty() {
        return Err(TallyError::Protocol(format!(
            "v0 REGISTER: {} is empty",
            field_name
        )));
    }
    // Longest suffix first so "ms" matches before "s".
    let (num_str, suffix_secs): (&str, u64) = if let Some(n) = s.strip_suffix("ms") {
        // Milliseconds are sub-second: handle separately.
        let n: u64 = n.parse().map_err(|_| {
            TallyError::Protocol(format!(
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
        return Err(TallyError::Protocol(format!(
            "v0 REGISTER: {} missing unit suffix (expected ms/s/m/h/d): {}",
            field_name, s
        )));
    };
    let n: u64 = num_str.parse().map_err(|_| {
        TallyError::Protocol(format!(
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
) -> Result<Option<(Duration, Duration)>, TallyError> {
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

/// Build the `OperatorState` variant for a given aggregation feature.
///
/// For all 16 AggOp types this returns the correct variant wired to the
/// parsed window/bucket and operator-specific parameters. Unknown
/// `feat.type` values fail with `TallyError::Protocol`.
///
/// Note on stubs: Variance / TopK / FirstN use the 22-01 stub operators
/// (no-op push, Missing read). Plans 22-02 and 22-03 replace the bodies.
pub fn build_operator(feat: &AggregationFeature) -> Result<OperatorState, TallyError> {
    let window_bucket = resolve_window_bucket(feat)?;

    let require_field = |ctx: &str| -> Result<String, TallyError> {
        feat.field
            .clone()
            .ok_or_else(|| TallyError::Protocol(format!("v0 REGISTER: {} requires 'field'", ctx)))
    };
    let require_window = |ctx: &str| -> Result<(Duration, Duration), TallyError> {
        window_bucket.ok_or_else(|| {
            TallyError::Protocol(format!("v0 REGISTER: {} requires 'window'", ctx))
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
                TallyError::Protocol("v0 REGISTER: percentile requires 'quantile'".into())
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
                .ok_or_else(|| TallyError::Protocol("v0 REGISTER: top_k requires 'k'".into()))?;
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
                TallyError::Protocol("v0 REGISTER: first_n requires 'n'".into())
            })?;
            OperatorState::FirstN(FirstNOp::new(field, n, optional))
        }
        "last_n" => {
            let field = require_field("last_n")?;
            let n = feat
                .n
                .ok_or_else(|| TallyError::Protocol("v0 REGISTER: last_n requires 'n'".into()))?;
            OperatorState::LastN(LastNOp::new(field, n, optional))
        }
        "ema" => {
            let field = require_field("ema")?;
            let half_life_str = feat.half_life.as_deref().ok_or_else(|| {
                TallyError::Protocol("v0 REGISTER: ema requires 'half_life'".into())
            })?;
            let half_life = parse_window(half_life_str, "ema.half_life")?;
            OperatorState::Ema(EmaOp::new(field, half_life.as_secs_f64(), optional))
        }
        "lag" => {
            let field = require_field("lag")?;
            let n = feat
                .n
                .ok_or_else(|| TallyError::Protocol("v0 REGISTER: lag requires 'n'".into()))?;
            OperatorState::Lag(LagOp::new(field, n, optional))
        }
        other => {
            return Err(TallyError::Protocol(format!(
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
) -> Result<String, TallyError> {
    if keys.is_empty() {
        return Err(TallyError::Protocol(
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
                return Err(TallyError::Type {
                    field: k.clone(),
                    expected: "group_by key value".into(),
                    got: "absent".into(),
                });
            }
            Some(other) => {
                return Err(TallyError::Type {
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
        assert!(matches!(err, TallyError::Protocol(_)));
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
            TallyError::Protocol(msg) => assert!(
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
