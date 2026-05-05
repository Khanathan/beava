//! Schema primitives: `FieldType`, `EventSchema`, `TableSchema`,
//! `DerivedSchema`, descriptor name validation.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use thiserror::Error;

/// The scalar field types supported by Beava v0 schemas.
/// Serializes to/from lowercase strings (e.g., `"str"`, `"f64"`, `"i64"`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    Str,
    F64,
    I64,
    Bool,
    Bytes,
    Datetime,
    /// Structured JSON output — used by sketch operators that return lists/objects.
    Json,
}

/// Schema attached to an event descriptor.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSchema {
    pub fields: BTreeMap<String, FieldType>,
    #[serde(default)]
    pub optional_fields: Vec<String>,
}

/// Schema attached to a table descriptor.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub fields: BTreeMap<String, FieldType>,
    #[serde(default)]
    pub optional_fields: Vec<String>,
}

/// Schema attached to a derivation descriptor.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedSchema {
    pub fields: BTreeMap<String, FieldType>,
    #[serde(default)]
    pub optional_fields: Vec<String>,
}

const MAX_NAME_LEN: usize = 128;
const RESERVED_PREFIX: &str = "_beava_";

/// Errors returned by [`validate_descriptor_name`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DescriptorNameError {
    #[error("descriptor name must not be empty")]
    Empty,

    #[error(
        "descriptor name '{0}' is invalid: must match [A-Za-z_][A-Za-z0-9_]* (no hyphens, spaces, or other characters)"
    )]
    BadPattern(String),

    #[error("descriptor name '{0}' uses reserved prefix '_beava_'")]
    ReservedPrefix(String),

    #[error("descriptor name is too long ({len} chars); maximum is {max}", max = MAX_NAME_LEN)]
    TooLong { len: usize },
}

/// Validate a descriptor name against Beava naming rules:
/// - Non-empty
/// - Length ≤ 128 characters
/// - Matches `[A-Za-z_][A-Za-z0-9_]*` (no hyphens, spaces, leading digits)
/// - Does not start with reserved prefix `_beava_`
pub fn validate_descriptor_name(name: &str) -> Result<(), DescriptorNameError> {
    if name.is_empty() {
        return Err(DescriptorNameError::Empty);
    }
    if name.len() > MAX_NAME_LEN {
        return Err(DescriptorNameError::TooLong { len: name.len() });
    }
    if name.starts_with(RESERVED_PREFIX) {
        return Err(DescriptorNameError::ReservedPrefix(name.to_string()));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap(); // non-empty checked above
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(DescriptorNameError::BadPattern(name.to_string()));
    }
    for ch in chars {
        if !ch.is_ascii_alphanumeric() && ch != '_' {
            return Err(DescriptorNameError::BadPattern(name.to_string()));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn field_type_serde_round_trip_all_variants() {
        let cases = [
            (FieldType::Str, "\"str\""),
            (FieldType::F64, "\"f64\""),
            (FieldType::I64, "\"i64\""),
            (FieldType::Bool, "\"bool\""),
            (FieldType::Bytes, "\"bytes\""),
            (FieldType::Datetime, "\"datetime\""),
        ];
        for (variant, expected_json) in &cases {
            let serialized = serde_json::to_string(variant).unwrap();
            assert_eq!(
                &serialized, expected_json,
                "serialization mismatch for {variant:?}"
            );
            let deserialized: FieldType = serde_json::from_str(expected_json).unwrap();
            assert_eq!(
                deserialized, *variant,
                "round-trip mismatch for {expected_json}"
            );
        }
    }

    #[test]
    fn field_type_json_round_trips() {
        use crate::schema::FieldType;
        let s = serde_json::to_string(&FieldType::Json).unwrap();
        assert_eq!(s, "\"json\"");
        let r: FieldType = serde_json::from_str("\"json\"").unwrap();
        assert_eq!(r, FieldType::Json);
    }

    #[test]
    fn field_type_unknown_string_returns_err() {
        let result: Result<FieldType, _> = serde_json::from_str("\"int\"");
        assert!(result.is_err(), "expected Err for unknown field type 'int'");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("unknown variant") || msg.contains("int"),
            "error message should mention 'int' or 'unknown variant', got: {msg}"
        );
    }

    #[test]
    fn event_schema_round_trip() {
        let schema = EventSchema {
            fields: {
                let mut m = BTreeMap::new();
                m.insert("a".to_string(), FieldType::Str);
                m.insert("event_time".to_string(), FieldType::I64);
                m
            },
            optional_fields: vec!["a".to_string()],
        };
        let json = serde_json::to_string_pretty(&schema).unwrap();
        let deserialized: EventSchema = serde_json::from_str(&json).unwrap();
        let re_serialized = serde_json::to_string_pretty(&deserialized).unwrap();
        assert_eq!(
            json, re_serialized,
            "EventSchema re-serialization must be byte-identical"
        );
        assert_eq!(schema, deserialized);
    }

    #[test]
    fn validate_descriptor_name_cases() {
        assert_eq!(validate_descriptor_name("Transaction"), Ok(()));
        assert_eq!(validate_descriptor_name("_private"), Ok(()));
        assert_eq!(validate_descriptor_name("a"), Ok(()));
        assert_eq!(validate_descriptor_name("A1_b2"), Ok(()));

        assert_eq!(
            validate_descriptor_name(""),
            Err(DescriptorNameError::Empty)
        );

        assert!(matches!(
            validate_descriptor_name("1abc"),
            Err(DescriptorNameError::BadPattern(_))
        ));

        assert!(matches!(
            validate_descriptor_name("_beava_internal"),
            Err(DescriptorNameError::ReservedPrefix(_))
        ));

        let long_name = "a".repeat(129);
        assert!(matches!(
            validate_descriptor_name(&long_name),
            Err(DescriptorNameError::TooLong { len: 129 })
        ));

        let max_name = "a".repeat(128);
        assert_eq!(validate_descriptor_name(&max_name), Ok(()));

        assert!(matches!(
            validate_descriptor_name("foo-bar"),
            Err(DescriptorNameError::BadPattern(_))
        ));
    }

    #[test]
    fn table_and_derived_schema_round_trip() {
        let table_json = r#"{"fields": {"k": "str"}, "optional_fields": []}"#;
        let table: TableSchema = serde_json::from_str(table_json).unwrap();
        assert_eq!(table.fields.get("k"), Some(&FieldType::Str));
        assert!(table.optional_fields.is_empty());

        let derived_json = r#"{"fields": {"k": "str"}, "optional_fields": []}"#;
        let derived: DerivedSchema = serde_json::from_str(derived_json).unwrap();
        assert_eq!(derived.fields.get("k"), Some(&FieldType::Str));
        assert!(derived.optional_fields.is_empty());

        let ts2: TableSchema =
            serde_json::from_str(&serde_json::to_string(&table).unwrap()).unwrap();
        assert_eq!(table, ts2);

        let ds2: DerivedSchema =
            serde_json::from_str(&serde_json::to_string(&derived).unwrap()).unwrap();
        assert_eq!(derived, ds2);
    }
}
