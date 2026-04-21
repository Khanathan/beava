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
