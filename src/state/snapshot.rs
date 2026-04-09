//! Snapshot persistence: OperatorState enum, serializable state types,
//! save/load functions with versioning.
//!
//! OperatorState replaces Box<dyn Operator> throughout the codebase,
//! making EntityState fully serializable with serde/postcard.

use serde::{Serialize, Deserialize};
use std::time::SystemTime;
use crate::engine::operators::{CountOp, SumOp, AvgOp, Operator};
use crate::state::store::StaticFeature;
use crate::types::FeatureValue;
use crate::error::TallyError;

/// Snapshot format version byte. Prepended to serialized data.
/// If the version doesn't match on load, return None (clean startup from empty state).
const SNAPSHOT_FORMAT_VERSION: u8 = 1;

/// Serializable enum wrapping all operator types.
/// Replaces Box<dyn Operator> so EntityState can be serialized.
/// Phase 5 adds: Min(MinOp), Max(MaxOp), DistinctCount(DistinctCountOp), Last(LastOp)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperatorState {
    Count(CountOp),
    Sum(SumOp),
    Avg(AvgOp),
}

impl OperatorState {
    pub fn push(&mut self, event: &serde_json::Value, now: SystemTime) -> Result<(), TallyError> {
        match self {
            Self::Count(op) => op.push(event, now),
            Self::Sum(op) => op.push(event, now),
            Self::Avg(op) => op.push(event, now),
        }
    }

    pub fn read(&mut self, now: SystemTime) -> FeatureValue {
        match self {
            Self::Count(op) => op.read(now),
            Self::Sum(op) => op.read(now),
            Self::Avg(op) => op.read(now),
        }
    }
}

/// Serializable pipeline definition for snapshot persistence.
/// Stores the raw RegisterRequest JSON as a String so pipelines can be re-parsed on load.
/// Uses String (not serde_json::Value) because postcard cannot serialize serde_json::Value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializablePipeline {
    pub name: String,
    pub key_field: String,
    /// Raw JSON string from the RegisterRequest. Re-parsed via convert_register_request on load.
    pub raw_register_json: String,
}

/// Serializable entity state for snapshot persistence.
/// Mirrors EntityState but uses Vec instead of AHashMap for static_features
/// (AHashMap is not directly serializable by postcard).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEntityState {
    pub live_operators: Vec<(String, OperatorState)>,
    pub static_features: Vec<(String, StaticFeature)>,
    pub last_event_at: Option<SystemTime>,
}

/// Top-level serializable snapshot state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotState {
    pub entities: Vec<(String, SerializableEntityState)>,
    pub pipelines: Vec<SerializablePipeline>,
}

/// Serialize a SnapshotState to bytes with a version prefix.
/// Format: [1 byte version][postcard-encoded SnapshotState]
pub fn save_snapshot(data: &SnapshotState) -> Vec<u8> {
    let mut buf = vec![SNAPSHOT_FORMAT_VERSION];
    buf.extend_from_slice(&postcard::to_stdvec(data).expect("snapshot serialization failed"));
    buf
}

/// Deserialize a SnapshotState from bytes.
/// Returns None if:
/// - bytes is empty
/// - version byte doesn't match SNAPSHOT_FORMAT_VERSION
/// - postcard deserialization fails (corrupt data)
pub fn load_snapshot(bytes: &[u8]) -> Option<SnapshotState> {
    if bytes.is_empty() {
        return None;
    }
    let version = bytes[0];
    if version != SNAPSHOT_FORMAT_VERSION {
        eprintln!(
            "Snapshot version mismatch: found {}, expected {}. Starting fresh.",
            version, SNAPSHOT_FORMAT_VERSION
        );
        return None;
    }
    postcard::from_bytes(&bytes[1..]).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};
    use serde_json::json;

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    // ======================== OperatorState Tests ========================

    #[test]
    fn test_operator_state_count_push_read() {
        let mut op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        let now = ts(60_000);
        op.push(&json!({}), now).unwrap();
        op.push(&json!({}), now).unwrap();
        op.push(&json!({}), now).unwrap();
        assert_eq!(op.read(now), FeatureValue::Int(3));
    }

    #[test]
    fn test_operator_state_sum_push_read() {
        let mut op = OperatorState::Sum(SumOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"amount": 50.0}), now).unwrap();
        assert_eq!(op.read(now), FeatureValue::Float(50.0));
    }

    #[test]
    fn test_operator_state_avg_push_read() {
        let mut op = OperatorState::Avg(AvgOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"amount": 10.0}), now).unwrap();
        op.push(&json!({"amount": 20.0}), now).unwrap();
        assert_eq!(op.read(now), FeatureValue::Float(15.0));
    }

    // ======================== Postcard Round-Trip Tests ========================

    #[test]
    fn test_operator_state_count_roundtrip_postcard() {
        let mut op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        let now = ts(60_000);
        op.push(&json!({}), now).unwrap();
        op.push(&json!({}), now).unwrap();
        op.push(&json!({}), now).unwrap();

        let bytes = postcard::to_stdvec(&op).expect("serialize");
        let mut restored: OperatorState = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(restored.read(now), FeatureValue::Int(3));
    }

    #[test]
    fn test_operator_state_sum_roundtrip_postcard() {
        let mut op = OperatorState::Sum(SumOp::new(
            "amount",
            Duration::from_secs(3600),
            Duration::from_secs(60),
            false,
        ));
        let now = ts(60_000);
        op.push(&json!({"amount": 42.5}), now).unwrap();
        op.push(&json!({"amount": 7.5}), now).unwrap();

        let bytes = postcard::to_stdvec(&op).expect("serialize");
        let mut restored: OperatorState = postcard::from_bytes(&bytes).expect("deserialize");
        assert_eq!(restored.read(now), FeatureValue::Float(50.0));
    }

    // ======================== SnapshotState Tests ========================

    #[test]
    fn test_snapshot_state_roundtrip() {
        let now = ts(60_000);
        let mut count_op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        count_op.push(&json!({}), now).unwrap();
        count_op.push(&json!({}), now).unwrap();
        count_op.push(&json!({}), now).unwrap();

        let state = SnapshotState {
            entities: vec![(
                "u123".to_string(),
                SerializableEntityState {
                    live_operators: vec![("tx_count_1h".to_string(), count_op)],
                    static_features: vec![(
                        "segment".to_string(),
                        StaticFeature {
                            value: FeatureValue::String("premium".to_string()),
                            updated_at: now,
                        },
                    )],
                    last_event_at: Some(now),
                },
            )],
            pipelines: vec![SerializablePipeline {
                name: "Transactions".to_string(),
                key_field: "user_id".to_string(),
                raw_register_json: r#"{"name":"Transactions","key_field":"user_id","features":[{"name":"tx_count_1h","type":"count","window":"1h"}]}"#.to_string(),
            }],
        };

        let bytes = postcard::to_stdvec(&state).expect("serialize");
        let restored: SnapshotState = postcard::from_bytes(&bytes).expect("deserialize");

        assert_eq!(restored.entities.len(), 1);
        assert_eq!(restored.entities[0].0, "u123");
        assert_eq!(restored.entities[0].1.live_operators.len(), 1);
        assert_eq!(restored.entities[0].1.static_features.len(), 1);
        assert_eq!(restored.entities[0].1.last_event_at, Some(now));
        assert_eq!(restored.pipelines.len(), 1);
        assert_eq!(restored.pipelines[0].name, "Transactions");

        // Verify operator state preserved
        let mut restored_op = restored.entities[0].1.live_operators[0].1.clone();
        assert_eq!(restored_op.read(now), FeatureValue::Int(3));
    }

    // ======================== save_snapshot / load_snapshot Tests ========================

    #[test]
    fn test_save_snapshot_starts_with_version_byte() {
        let state = SnapshotState {
            entities: vec![],
            pipelines: vec![],
        };
        let bytes = save_snapshot(&state);
        assert_eq!(bytes[0], SNAPSHOT_FORMAT_VERSION);
        assert_eq!(bytes[0], 0x01);
    }

    #[test]
    fn test_load_snapshot_correct_version() {
        let now = ts(60_000);
        let mut count_op = OperatorState::Count(CountOp::new(
            Duration::from_secs(3600),
            Duration::from_secs(60),
        ));
        count_op.push(&json!({}), now).unwrap();
        count_op.push(&json!({}), now).unwrap();
        count_op.push(&json!({}), now).unwrap();

        let state = SnapshotState {
            entities: vec![(
                "u123".to_string(),
                SerializableEntityState {
                    live_operators: vec![("tx_count_1h".to_string(), count_op)],
                    static_features: vec![],
                    last_event_at: Some(now),
                },
            )],
            pipelines: vec![],
        };

        let bytes = save_snapshot(&state);
        let restored = load_snapshot(&bytes);
        assert!(restored.is_some());

        let restored = restored.unwrap();
        assert_eq!(restored.entities.len(), 1);
        let mut restored_op = restored.entities[0].1.live_operators[0].1.clone();
        assert_eq!(restored_op.read(now), FeatureValue::Int(3));
    }

    #[test]
    fn test_load_snapshot_wrong_version_returns_none() {
        let state = SnapshotState {
            entities: vec![],
            pipelines: vec![],
        };
        let mut bytes = save_snapshot(&state);
        // Tamper with version byte
        bytes[0] = 0xFF;
        assert!(load_snapshot(&bytes).is_none());
    }

    #[test]
    fn test_load_snapshot_empty_bytes_returns_none() {
        assert!(load_snapshot(&[]).is_none());
    }

    #[test]
    fn test_load_snapshot_corrupt_data_returns_none() {
        let mut bytes = vec![SNAPSHOT_FORMAT_VERSION];
        bytes.extend_from_slice(b"this is not valid postcard data!!!");
        assert!(load_snapshot(&bytes).is_none());
    }
}
