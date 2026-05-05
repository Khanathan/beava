//! Feature query helpers shared by the mio data-plane dispatch path.
//!
//! # GET /get/:feature/:key
//!
//! Single-feature lookup. Returns `{"value": <JSON>}` per the D-02 envelope
//! contract.
//!
//! - 200 `{"value": <JSON>}` — feature and key found
//! - 404 `{"error": {"code": "feature_not_found"}}` — unknown feature name
//! - 404 `{"error": {"code": "key_not_found"}}` — valid feature, key not seen
//!
//! # POST /get
//!
//! Batch lookup. Body: `{"keys": [...], "features": [...]}`. Returns the flat
//! per-entity dict `{key: {feature: value}}`.
//!
//! - 200 `{key: {feature: value}, ...}` — success (missing keys omitted, not null)
//! - 200 `{}` — cold-start (no entities matched)
//! - 400 `{"error": {"code": "feature_not_found", "missing": [...]}}`
//! - 400 `{"error": {"code": "batch_too_large"}}` — keys × features > 10_000
//!
//! Query time uses `max(event_time_ms observed)` or 0 — wall-clock is never
//! read here; a grep tripwire enforces that.

use beava_core::agg_state_table::EntityKey;
use beava_core::row::Value;

/// Parse a URL-encoded entity key into an `EntityKey`.
///
/// Multi-key group_bys use `|` as a separator (e.g. `"alice|merchant1"` →
/// `[("user_id", "alice"), ("merchant_id", "merchant1")]`). Pipe characters
/// inside key values must be percent-encoded as `%7C`.
///
/// Returns `None` when the segment count does not match `group_keys.len()`.
/// Callers must surface that as `key_parse_failure` so it stays
/// distinguishable from `key_not_found` (per WR-02).
///
/// Empty `group_keys` + empty `key_str` is the global-table sentinel: it
/// addresses the single-slot global state so an unkeyed pipeline can be
/// queried via `GET /get/:feature/`. Without this short-circuit
/// `"".split('|')` would produce one segment and fail the arity check.
pub(crate) fn parse_entity_key(key_str: &str, group_keys: &[String]) -> Option<EntityKey> {
    use beava_core::row::Value;
    use compact_str::CompactString;
    use smallvec::SmallVec;
    if group_keys.is_empty() && key_str.is_empty() {
        return Some(EntityKey(SmallVec::new()));
    }
    let segments: Vec<&str> = key_str.split('|').collect();
    if segments.len() != group_keys.len() {
        return None;
    }
    let pairs: SmallVec<[(CompactString, Value); 2]> = group_keys
        .iter()
        .zip(segments.iter())
        .map(|(k, v)| {
            let key: CompactString = k.as_str().into();
            let val: Value = Value::Str(CompactString::from(*v));
            (key, val)
        })
        .collect();
    Some(EntityKey(pairs))
}

/// Convert a `beava_core::row::Value` to a `serde_json::Value`.
///
/// Mirror of the helper in `registry_debug.rs`; duplicated to keep this
/// module self-contained. NaN floats serialise as JSON null.
pub(crate) fn value_to_json(v: Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::I64(n) => serde_json::Value::Number(n.into()),
        Value::F64(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Str(s) => serde_json::Value::String(s.into_string()),
        Value::Bytes(_) => serde_json::Value::Null,
        Value::Datetime(ms) => serde_json::Value::Number(ms.into()),
        Value::Json(j) => j,
        Value::List(items) => {
            serde_json::Value::Array(items.into_iter().map(value_to_json).collect())
        }
        Value::Map(m) => {
            serde_json::Value::Object(m.into_iter().map(|(k, v)| (k, value_to_json(v))).collect())
        }
    }
}

