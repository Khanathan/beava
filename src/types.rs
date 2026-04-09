use std::time::SystemTime;
use serde::{Serialize, Deserialize};

/// Type alias for entity keys (e.g., "user_id:u123").
pub type EntityKey = String;

/// Type alias for timestamps. Uses SystemTime (not Instant) because
/// client-supplied Unix timestamps must be comparable.
pub type Timestamp = SystemTime;

/// Core value type for all features. Variants per CONTEXT.md locked decision:
/// Float(f64), Int(i64), String(String), Missing.
/// No Bool variant -- boolean results use Int(0)/Int(1) per Redis convention.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FeatureValue {
    Float(f64),
    Int(i64),
    String(String),
    Missing,
}

impl FeatureValue {
    /// Extract as f64, promoting Int to Float per CONTEXT.md:
    /// "No implicit type coercion beyond Int+Float->Float in arithmetic expressions."
    /// Returns None for String/Missing.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FeatureValue::Float(f) => Some(*f),
            FeatureValue::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Check if this value is Missing.
    pub fn is_missing(&self) -> bool {
        matches!(self, FeatureValue::Missing)
    }
}

/// A map of feature name to feature value.
pub type FeatureMap = ahash::AHashMap<String, FeatureValue>;
