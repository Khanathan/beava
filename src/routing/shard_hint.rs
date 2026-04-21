//! Shard-hint computation: deterministic routing slot from event primary key.
//!
//! # Design (Wave 0 — TPC-INFRA-01)
//! `shard_hint_for_event` is a pure, side-effect-free function that returns `hash(key) as u32`.
//! At `N_SHARDS=1`, the caller computes `hint % 1 == 0` — always shard 0, no routing change.
//! The value is computed at ingest and DISCARDED after use. It is never stored on `Event`,
//! `PendingAsync`, or any wire struct (see CONTEXT.md D-01).
//!
//! # Hash function
//! Uses `ahash::AHasher` (re-exported via existing `ahash = "0.8"` dep). Adequate for
//! non-adversarial key distributions. Cross-arch determinism is a Wave 4 concern (D-05).

use std::hash::{Hash, Hasher};

/// Random shard hint for streams without a key_field or declared shard_key.
/// Phase 56-NEXT #7: events from such streams distribute pseudorandomly
/// across shards; aggregation operators downstream reshuffle to the owner
/// shard via cross-shard ShardOps (Phase 55/56/57), so correctness holds.
///
/// Uses `rand::thread_rng()` (per-thread ChaCha12 PRNG, zero cross-core
/// contention). A global atomic counter was rejected because at high EPS
/// across 8+ cores it would bounce a single cache line on every ingest.
pub fn random_shard_hint() -> u32 {
    use rand::Rng;
    rand::thread_rng().gen::<u32>()
}

/// Hash a single event field by name. Returns `None` if the field is absent
/// or its value is not a non-empty JSON string.
fn hash_event_field(event: &serde_json::Value, field: &str) -> Option<u32> {
    let serde_json::Value::String(s) = event.get(field)? else {
        return None;
    };
    if s.is_empty() {
        return None;
    }
    let mut hasher = ahash::AHasher::default();
    s.hash(&mut hasher);
    Some(hasher.finish() as u32)
}

/// Hash a tuple of event fields in declared order. Any missing/non-string
/// field short-circuits to `None` (caller can fall back to round-robin).
fn hash_event_tuple(event: &serde_json::Value, fields: &[&str]) -> Option<u32> {
    if fields.is_empty() {
        return None;
    }
    let mut hasher = ahash::AHasher::default();
    for f in fields {
        let serde_json::Value::String(s) = event.get(*f)? else {
            return None;
        };
        if s.is_empty() {
            return None;
        }
        s.hash(&mut hasher);
    }
    Some(hasher.finish() as u32)
}

/// Compute a shard hint from a REGISTER payload's declared `shard_key` JSON
/// value. Accepts either a JSON string (scalar shard key) or a JSON array of
/// strings (composite shard key). Returns `None` if the shape is wrong or if
/// any declared field is missing from the event.
///
/// This mirrors the `_serialize.py` emission contract:
///   - `d["shard_key"] = "user_id"`        → Single
///   - `d["shard_key"] = ["region", "uid"]` → Tuple
pub fn shard_hint_from_shard_key_json(
    event: &serde_json::Value,
    shard_key: &serde_json::Value,
) -> Option<u32> {
    match shard_key {
        serde_json::Value::String(field) => hash_event_field(event, field),
        serde_json::Value::Array(arr) => {
            let fields: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            if fields.len() != arr.len() {
                return None; // Some element wasn't a string.
            }
            hash_event_tuple(event, &fields)
        }
        _ => None,
    }
}

/// Full ingest routing: compute a shard hint using the declared priority order.
///
/// Priority:
///   1. `key_field` (stream's primary key) — existing Phase 48+ behavior.
///   2. `shard_key` from the stream's REGISTER payload (scalar or tuple).
///   3. Round-robin counter — when neither declaration exists or the event
///      is missing the declared fields.
///
/// Callers pass `raw_shard_key` as the JSON value at `raw_register_json["shard_key"]`
/// (or `None` if absent). This avoids a circular dependency between
/// `routing` and `engine::pipeline`.
pub fn compute_ingest_shard_hint(
    event: &serde_json::Value,
    key_field: Option<&str>,
    raw_shard_key: Option<&serde_json::Value>,
) -> u32 {
    if key_field.is_some() {
        return shard_hint_for_event(event, key_field);
    }
    if let Some(sk) = raw_shard_key {
        if let Some(hint) = shard_hint_from_shard_key_json(event, sk) {
            return hint;
        }
    }
    random_shard_hint()
}

/// Compute the shard hint for a single event.
///
/// Returns the lower 32 bits of an ahash of the primary-key string value.
/// Returns `0` for keyless streams, missing key fields, or non-string key values.
///
/// # Arguments
/// - `event`: JSON object representing the event.
/// - `key_field`: The primary key field name for this stream (`StreamDefinition::key_field`).
///   Pass `None` for keyless streams.
pub fn shard_hint_for_event(event: &serde_json::Value, key_field: Option<&str>) -> u32 {
    let Some(field) = key_field else {
        return 0; // keyless stream
    };
    let Some(serde_json::Value::String(key_str)) = event.get(field) else {
        return 0; // missing field or non-string value
    };
    if key_str.is_empty() {
        return 0;
    }
    // Use ahash (existing dep). AHasher is !Send but we only use it locally.
    // Per D-05: no cross-arch determinism required at Wave 0.
    let mut hasher = ahash::AHasher::default();
    key_str.hash(&mut hasher);
    hasher.finish() as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn string_key_nonzero() {
        // "alice" must hash to a nonzero value (probability of collision is 1/2^32).
        let hint = shard_hint_for_event(&json!({"user_id": "alice"}), Some("user_id"));
        assert_ne!(hint, 0, "string key should hash to nonzero for 'alice'");
    }

    #[test]
    fn numeric_key_graceful() {
        // Non-string key value → graceful fallback to 0, no panic.
        let hint = shard_hint_for_event(&json!({"id": 42}), Some("id"));
        assert_eq!(hint, 0, "numeric key value returns 0 gracefully");
    }

    #[test]
    fn composite_key_hashes_first_field() {
        // Tuple key scenario: key_field = "region" (keys[0]). Must hash "us-east".
        let hint = shard_hint_for_event(
            &json!({"region": "us-east", "user_id": "bob"}),
            Some("region"),
        );
        assert_ne!(hint, 0, "composite key first field hashes to nonzero");
    }

    #[test]
    fn keyless_returns_zero() {
        let hint = shard_hint_for_event(&json!({"value": 99}), None);
        assert_eq!(hint, 0, "keyless stream always returns 0");
    }

    #[test]
    fn missing_field_returns_zero() {
        let hint = shard_hint_for_event(&json!({"other": "x"}), Some("user_id"));
        assert_eq!(hint, 0, "missing key field returns 0 gracefully");
    }

    #[test]
    #[allow(clippy::modulo_one)]
    fn n1_modulo_always_zero() {
        // Semantic documentation: at N=1, routing is always shard 0.
        // The `% 1` is intentional — documents that any u32 mod 1 == 0,
        // so N=1 always selects shard 0 regardless of the hash value.
        let hint = shard_hint_for_event(&json!({"user_id": "charlie"}), Some("user_id"));
        assert_eq!(hint % 1, 0, "any u32 % 1 is 0 — N=1 is always shard 0");
    }

    #[test]
    fn deterministic_same_key() {
        // Same key must produce the same hint across two calls (no random salt).
        let a = shard_hint_for_event(&json!({"user_id": "diana"}), Some("user_id"));
        let b = shard_hint_for_event(&json!({"user_id": "diana"}), Some("user_id"));
        assert_eq!(a, b, "shard_hint is deterministic for identical keys");
    }

    #[test]
    fn different_keys_likely_different() {
        // "alice" and "bob" should (very likely) hash to different values.
        let a = shard_hint_for_event(&json!({"user_id": "alice"}), Some("user_id"));
        let b = shard_hint_for_event(&json!({"user_id": "bob"}), Some("user_id"));
        assert_ne!(a, b, "distinct keys hash to distinct values");
    }
}
