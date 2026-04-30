//! Feature query endpoints — Plan 05-06.
//!
//! # GET /get/:feature/:key
//!
//! Single-feature lookup. Returns `{"value": <JSON>}` per D-02.
//!
//! - 200 `{"value": <JSON>}` — feature and key found
//! - 404 `{"error": {"code": "feature_not_found"}}` — unknown feature name
//! - 404 `{"error": {"code": "key_not_found"}}` — valid feature, key not seen
//!
//! # POST /get
//!
//! Batch lookup. Body: `{"keys": [...], "features": [...]}`.
//! Returns `{"result": {key: {feature: value}}}` per SRV-API-08.
//!
//! - 200 `{"result": {...}}` — success (missing keys are omitted, not null)
//! - 400 `{"error": {"code": "feature_not_found", "missing": [...]}}` — unknown feature
//! - 400 `{"error": {"code": "batch_too_large"}}` — keys × features > 10_000
//!
//! # D-02 compliance
//!
//! Response envelope is `{"value": ...}` ONLY. v0 ships no metadata envelope.
//! Grep guard test asserts the response wrapper key is exactly "value".
//!
//! # D-06 compliance
//!
//! Query time uses max(event_time_ms observed) or 0 — wall-clock is never read.
//! Grep guard test asserts clock functions are absent from this file's production code.
//!
//! # SDK-AGG-02

use beava_core::agg_state_table::EntityKey;
use beava_core::row::Value;

// ─── Helpers ──────────────────────────────────────────────────────────────────
//
// Plan 12.6-07: the legacy axum router + `feature_query_router` /
// `get_feature_handler` / `post_get_batch_handler` are deleted. The mio
// dispatch path (`runtime_core_glue::dispatch_get_*_sync`) calls
// `parse_entity_key` and `value_to_json` directly. The SRV-API-08 batch
// cap (10_000 cells) is enforced inline in `dispatch_get_batch`.

/// Parse a URL-encoded entity key into an `EntityKey`.
///
/// For multi-key group_by, the key string uses `|` as a separator:
/// e.g., `"alice|merchant1"` → `[("user_id", "alice"), ("merchant_id", "merchant1")]`.
///
/// Returns `None` when the number of pipe-separated segments does not match
/// `group_keys.len()`. Callers should return a `key_parse_failure` error code in
/// that case so it is distinguishable from `key_not_found` (WR-02).
///
/// **Limitation:** pipe characters inside key values must be percent-encoded as `%7C`
/// because `|` is the multi-key separator. Full URL-decoding of `%7C` → `|` is deferred
/// to Phase 12 API completion.
pub(crate) fn parse_entity_key(key_str: &str, group_keys: &[String]) -> Option<EntityKey> {
    use beava_core::row::Value;
    use compact_str::CompactString;
    use smallvec::SmallVec;
    let segments: Vec<&str> = key_str.split('|').collect();
    if segments.len() != group_keys.len() {
        return None;
    }
    // Plan 18-11 D-5: build EntityKey via SmallVec of (CompactString, Value::Str)
    // pairs. URL parameters are always strings — values are stored as Str.
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
/// Mirror of the helper in `registry_debug.rs` — duplicated here to keep
/// `feature_query.rs` self-contained (no dep on the debug module).
///
/// Phase 11 (D-01): `Value::List` → JSON array; `Value::Map` → JSON object.
/// Recursive on element values. NaN floats serialise as JSON null.
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

// ─── Tests ───────────────────────────────────────────────────────────────────
