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

    /// Convert to a plain serde_json::Value (untagged).
    /// Float(1.5) -> 1.5, Int(42) -> 42, String("ok") -> "ok", Missing -> null.
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            FeatureValue::Float(f) => serde_json::Value::from(*f),
            FeatureValue::Int(i) => serde_json::Value::from(*i),
            FeatureValue::String(s) => serde_json::Value::String(s.clone()),
            FeatureValue::Missing => serde_json::Value::Null,
        }
    }
}

/// Convert a FeatureMap to JSON bytes (untagged values).
pub fn feature_map_to_json(features: &FeatureMap) -> Vec<u8> {
    let map: serde_json::Map<String, serde_json::Value> = features
        .iter()
        .map(|(k, v)| (k.clone(), v.to_json_value()))
        .collect();
    serde_json::to_vec(&serde_json::Value::Object(map)).unwrap()
}

/// A map of feature name to feature value.
pub type FeatureMap = ahash::AHashMap<String, FeatureValue>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_value_float_to_json() {
        assert_eq!(
            FeatureValue::Float(1.5).to_json_value(),
            serde_json::Value::from(1.5)
        );
    }

    #[test]
    fn test_feature_value_int_to_json() {
        assert_eq!(
            FeatureValue::Int(42).to_json_value(),
            serde_json::Value::from(42)
        );
    }

    #[test]
    fn test_feature_value_string_to_json() {
        assert_eq!(
            FeatureValue::String("ok".into()).to_json_value(),
            serde_json::Value::String("ok".into())
        );
    }

    #[test]
    fn test_feature_value_missing_to_json() {
        assert_eq!(
            FeatureValue::Missing.to_json_value(),
            serde_json::Value::Null
        );
    }

    #[test]
    fn test_feature_map_to_json() {
        let mut map = FeatureMap::new();
        map.insert("a".into(), FeatureValue::Float(1.5));
        map.insert("b".into(), FeatureValue::Int(2));
        let bytes = feature_map_to_json(&map);
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["a"], 1.5);
        assert_eq!(parsed["b"], 2);
    }

    // --- G-09: as_f64 and is_missing exhaustive variant tests ---

    #[test]
    fn test_as_f64_float() {
        assert_eq!(FeatureValue::Float(3.14).as_f64(), Some(3.14));
    }

    #[test]
    fn test_as_f64_int() {
        assert_eq!(FeatureValue::Int(42).as_f64(), Some(42.0));
    }

    #[test]
    fn test_as_f64_string_returns_none() {
        assert_eq!(FeatureValue::String("hi".into()).as_f64(), None);
    }

    #[test]
    fn test_as_f64_missing_returns_none() {
        assert_eq!(FeatureValue::Missing.as_f64(), None);
    }

    #[test]
    fn test_is_missing_true() {
        assert!(FeatureValue::Missing.is_missing());
    }

    #[test]
    fn test_is_missing_false_float() {
        assert!(!FeatureValue::Float(1.0).is_missing());
    }

    #[test]
    fn test_is_missing_false_int() {
        assert!(!FeatureValue::Int(0).is_missing());
    }

    #[test]
    fn test_is_missing_false_string() {
        assert!(!FeatureValue::String("".into()).is_missing());
    }

    // --- G-10: feature_map_to_json with Missing and String values ---

    #[test]
    fn test_feature_map_to_json_with_missing() {
        let mut map = FeatureMap::new();
        map.insert("x".into(), FeatureValue::Missing);
        let bytes = feature_map_to_json(&map);
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["x"], serde_json::Value::Null);
    }

    #[test]
    fn test_feature_map_to_json_with_string() {
        let mut map = FeatureMap::new();
        map.insert("name".into(), FeatureValue::String("alice".into()));
        let bytes = feature_map_to_json(&map);
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["name"], "alice");
    }

    #[test]
    fn test_feature_map_to_json_all_variants() {
        let mut map = FeatureMap::new();
        map.insert("f".into(), FeatureValue::Float(1.5));
        map.insert("i".into(), FeatureValue::Int(42));
        map.insert("s".into(), FeatureValue::String("ok".into()));
        map.insert("m".into(), FeatureValue::Missing);
        let bytes = feature_map_to_json(&map);
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["f"], 1.5);
        assert_eq!(parsed["i"], 42);
        assert_eq!(parsed["s"], "ok");
        assert_eq!(parsed["m"], serde_json::Value::Null);
    }
}
