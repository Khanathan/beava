//! Snapshot persistence: OperatorState enum, serializable state types,
//! save/load functions with versioning.
//!
//! OperatorState replaces Box<dyn Operator> throughout the codebase,
//! making EntityState fully serializable with serde/postcard.

use serde::{Serialize, Deserialize};
use std::time::SystemTime;
use crate::engine::operators::{CountOp, SumOp, AvgOp, Operator};
use crate::types::FeatureValue;
use crate::error::TallyError;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};
    use serde_json::json;

    fn ts(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

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
}
