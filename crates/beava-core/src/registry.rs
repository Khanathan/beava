//! Registry data model: descriptor structs, OutputKind, TableMode, RegistryInner,
//! and the parking_lot::RwLock-guarded Registry wrapper.

use crate::op_node::OpNode;
use crate::schema::{DerivedSchema, EventSchema, TableSchema};
use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ─── Enums ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputKind {
    Event,
    Table,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableMode {
    Append,
}

// ─── Descriptor structs ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventDescriptor {
    pub name: String,
    pub schema: EventSchema,
    pub event_time_field: String,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub idempotency_ttl_ms: Option<u64>,
    #[serde(default)]
    pub history_ttl_ms: Option<u64>,
    #[serde(default)]
    pub watermark_lateness_ms: Option<u64>,
    /// Assigned server-side; ignored (defaulted to 0) when deserializing from client JSON.
    #[serde(default)]
    pub registered_at_version: u64,
}

impl EventDescriptor {
    /// Compare two descriptors field-by-field, EXCLUDING `registered_at_version`.
    /// Used by the diff engine (Plan 03) to detect conflicts without false positives
    /// from version stamps.
    pub fn equiv_ignoring_version(&self, other: &Self) -> bool {
        self.name == other.name
            && self.schema == other.schema
            && self.event_time_field == other.event_time_field
            && self.idempotency_key == other.idempotency_key
            && self.idempotency_ttl_ms == other.idempotency_ttl_ms
            && self.history_ttl_ms == other.history_ttl_ms
            && self.watermark_lateness_ms == other.watermark_lateness_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableDescriptor {
    pub name: String,
    pub primary_key: Vec<String>,
    pub schema: TableSchema,
    #[serde(default)]
    pub ttl_ms: Option<u64>,
    pub mode: TableMode,
    /// Assigned server-side; ignored (defaulted to 0) when deserializing from client JSON.
    #[serde(default)]
    pub registered_at_version: u64,
}

impl TableDescriptor {
    /// Compare two descriptors field-by-field, EXCLUDING `registered_at_version`.
    pub fn equiv_ignoring_version(&self, other: &Self) -> bool {
        self.name == other.name
            && self.primary_key == other.primary_key
            && self.schema == other.schema
            && self.ttl_ms == other.ttl_ms
            && self.mode == other.mode
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DerivationDescriptor {
    pub name: String,
    pub output_kind: OutputKind,
    pub upstreams: Vec<String>,
    /// Strongly-typed op pipeline. Plan 02-02 swapped this from Vec<serde_json::Value>.
    #[serde(default)]
    pub ops: Vec<OpNode>,
    pub schema: DerivedSchema,
    #[serde(default)]
    pub table_primary_key: Option<Vec<String>>,
    /// Assigned server-side; ignored (defaulted to 0) when deserializing from client JSON.
    #[serde(default)]
    pub registered_at_version: u64,
}

impl DerivationDescriptor {
    /// Compare two descriptors field-by-field, EXCLUDING `registered_at_version`.
    pub fn equiv_ignoring_version(&self, other: &Self) -> bool {
        self.name == other.name
            && self.output_kind == other.output_kind
            && self.upstreams == other.upstreams
            && self.ops == other.ops
            && self.schema == other.schema
            && self.table_primary_key == other.table_primary_key
    }
}

// ─── Registry types ───────────────────────────────────────────────────────────

#[derive(Debug, Default, Clone)]
pub struct RegistryInner {
    pub version: u64,
    pub events: BTreeMap<String, EventDescriptor>,
    pub tables: BTreeMap<String, TableDescriptor>,
    pub derivations: BTreeMap<String, DerivationDescriptor>,
}

#[derive(Debug, Default)]
pub struct Registry {
    inner: RwLock<RegistryInner>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn version(&self) -> u64 {
        self.inner.read().version
    }

    pub fn read(&self) -> RwLockReadGuard<'_, RegistryInner> {
        self.inner.read()
    }

    pub fn snapshot(&self) -> RegistryInner {
        self.inner.read().clone()
    }

    /// Install descriptors into the registry under a write lock. Monotonically
    /// bumps the version to `new_version`. Panics in debug if `new_version` is
    /// not strictly greater than the current version.
    ///
    /// NOTE: this is a low-level helper. Plan 05 adds `apply_registration` on top
    /// which handles the PayloadNode dispatch and skips already-present descriptors.
    pub fn install_descriptors(
        &self,
        new_version: u64,
        events: Vec<EventDescriptor>,
        tables: Vec<TableDescriptor>,
        derivations: Vec<DerivationDescriptor>,
    ) {
        let mut w = self.inner.write();
        debug_assert!(
            new_version > w.version,
            "install_descriptors: new_version ({new_version}) must be > current version ({})",
            w.version
        );
        for mut e in events {
            e.registered_at_version = new_version;
            w.events.insert(e.name.clone(), e);
        }
        for mut t in tables {
            t.registered_at_version = new_version;
            w.tables.insert(t.name.clone(), t);
        }
        for mut d in derivations {
            d.registered_at_version = new_version;
            w.derivations.insert(d.name.clone(), d);
        }
        w.version = new_version;
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{EventSchema, FieldType};
    use std::collections::BTreeMap;

    fn make_event_schema() -> EventSchema {
        let mut fields = BTreeMap::new();
        fields.insert("card_id".to_string(), FieldType::Str);
        fields.insert("amount".to_string(), FieldType::F64);
        fields.insert("merchant_id".to_string(), FieldType::Str);
        fields.insert("event_time".to_string(), FieldType::I64);
        EventSchema {
            fields,
            optional_fields: vec![],
        }
    }

    // Test 1: EventDescriptor JSON round-trip (Transaction from 02-CONTEXT.md)
    #[test]
    fn event_descriptor_json_round_trip() {
        let json = r#"{
            "name": "Transaction",
            "schema": {
                "fields": {
                    "card_id": "str",
                    "amount": "f64",
                    "merchant_id": "str",
                    "event_time": "i64"
                },
                "optional_fields": []
            },
            "event_time_field": "event_time",
            "idempotency_key": "request_id",
            "idempotency_ttl_ms": 86400000,
            "history_ttl_ms": 604800000,
            "watermark_lateness_ms": 5000
        }"#;

        let desc: EventDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(desc.name, "Transaction");
        assert_eq!(desc.event_time_field, "event_time");
        assert_eq!(desc.idempotency_key, Some("request_id".to_string()));
        assert_eq!(desc.idempotency_ttl_ms, Some(86_400_000));
        assert_eq!(desc.history_ttl_ms, Some(604_800_000));
        assert_eq!(desc.watermark_lateness_ms, Some(5000));
        assert_eq!(desc.registered_at_version, 0); // defaulted
        assert_eq!(desc.schema.fields.get("amount"), Some(&FieldType::F64));

        // Re-serialize and re-parse → must match
        let re_json = serde_json::to_string(&desc).unwrap();
        let desc2: EventDescriptor = serde_json::from_str(&re_json).unwrap();
        assert_eq!(desc, desc2);
    }

    // Test 2: TableDescriptor JSON round-trip (Merchant from 02-CONTEXT.md)
    #[test]
    fn table_descriptor_json_round_trip() {
        let json = r#"{
            "name": "Merchant",
            "primary_key": ["merchant_id"],
            "schema": {
                "fields": {
                    "merchant_id": "str",
                    "name": "str",
                    "category": "str"
                },
                "optional_fields": ["category"]
            },
            "ttl_ms": 2592000000,
            "mode": "append"
        }"#;

        let desc: TableDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(desc.name, "Merchant");
        assert_eq!(desc.primary_key, vec!["merchant_id".to_string()]);
        assert_eq!(desc.ttl_ms, Some(2_592_000_000));
        assert_eq!(desc.mode, TableMode::Append);
        assert_eq!(desc.schema.optional_fields, vec!["category".to_string()]);
        assert_eq!(desc.registered_at_version, 0);

        let re_json = serde_json::to_string(&desc).unwrap();
        let desc2: TableDescriptor = serde_json::from_str(&re_json).unwrap();
        assert_eq!(desc, desc2);
    }

    // Test 3: TableMode strict — unknown variant returns Err
    #[test]
    fn table_mode_unknown_variant_rejected() {
        let result: Result<TableMode, _> = serde_json::from_str("\"changelog\"");
        assert!(result.is_err(), "expected Err for 'changelog'");
        let msg = result.unwrap_err().to_string();
        // serde's built-in message includes the unknown variant name
        assert!(
            msg.contains("unknown variant") || msg.contains("changelog"),
            "error should mention unknown variant, got: {msg}"
        );
    }

    // Test 4: OutputKind — valid and invalid
    #[test]
    fn output_kind_serde() {
        let e: OutputKind = serde_json::from_str("\"event\"").unwrap();
        assert_eq!(e, OutputKind::Event);
        let t: OutputKind = serde_json::from_str("\"table\"").unwrap();
        assert_eq!(t, OutputKind::Table);

        let result: Result<OutputKind, _> = serde_json::from_str("\"derivation\"");
        assert!(result.is_err(), "expected Err for 'derivation'");
    }

    // Test 5: Registry new() starts at version 0 with empty maps
    #[test]
    fn registry_new_starts_empty() {
        let r = Registry::new();
        assert_eq!(r.version(), 0);
        {
            let inner = r.read();
            assert!(inner.events.is_empty());
            assert!(inner.tables.is_empty());
            assert!(inner.derivations.is_empty());
        }
        let snap = r.snapshot();
        assert_eq!(snap.version, 0);
        assert!(snap.events.is_empty());
    }

    // Test 6: registered_at_version is ignored in equiv_ignoring_version
    #[test]
    fn equality_ignores_registered_at_version() {
        let schema = make_event_schema();
        let a = EventDescriptor {
            name: "A".to_string(),
            schema: schema.clone(),
            event_time_field: "event_time".to_string(),
            idempotency_key: None,
            idempotency_ttl_ms: None,
            history_ttl_ms: None,
            watermark_lateness_ms: None,
            registered_at_version: 1,
        };
        let mut b = a.clone();
        b.registered_at_version = 99;

        // Derived PartialEq sees them as different (different version)
        assert_ne!(a, b, "derived PartialEq includes registered_at_version");

        // equiv_ignoring_version sees them as equal
        assert!(
            a.equiv_ignoring_version(&b),
            "equiv_ignoring_version must ignore registered_at_version"
        );
    }

    // Test 7: install_descriptors increments version + indexes by name
    #[test]
    fn install_descriptors_increments_version() {
        let r = Registry::new();
        let schema = make_event_schema();

        let event_a = EventDescriptor {
            name: "Transaction".to_string(),
            schema: schema.clone(),
            event_time_field: "event_time".to_string(),
            idempotency_key: None,
            idempotency_ttl_ms: None,
            history_ttl_ms: None,
            watermark_lateness_ms: None,
            registered_at_version: 0,
        };

        r.install_descriptors(1, vec![event_a], vec![], vec![]);
        assert_eq!(r.version(), 1);
        {
            let inner = r.read();
            assert!(inner.events.contains_key("Transaction"));
            assert_eq!(
                inner.events["Transaction"].registered_at_version, 1,
                "registered_at_version should be stamped with install version"
            );
        }

        // Install a derivation at v2
        let deriv = DerivationDescriptor {
            name: "BigTx".to_string(),
            output_kind: OutputKind::Event,
            upstreams: vec!["Transaction".to_string()],
            ops: vec![],
            schema: crate::schema::DerivedSchema {
                fields: {
                    let mut m = BTreeMap::new();
                    m.insert("amount".to_string(), FieldType::F64);
                    m
                },
                optional_fields: vec![],
            },
            table_primary_key: None,
            registered_at_version: 0,
        };
        r.install_descriptors(2, vec![], vec![], vec![deriv]);
        assert_eq!(r.version(), 2);
        let snap = r.snapshot();
        assert!(snap.events.contains_key("Transaction"));
        assert!(snap.derivations.contains_key("BigTx"));
        assert_eq!(snap.derivations["BigTx"].registered_at_version, 2);
    }

    // Test 8 (Plan 02-02): DerivationDescriptor with OpNode round-trip (BigTx)
    // NOTE: The outer JSON uses "kind" discrimination which is handled at the payload-parsing
    // layer (Plan 05). Here we test the inner descriptor shape directly without "kind".
    #[test]
    fn derivation_with_ops_round_trip() {
        let json = r#"{
            "name": "BigTx",
            "output_kind": "event",
            "upstreams": ["Transaction"],
            "ops": [{"op": "filter", "expr": "(amount > 500)"}],
            "schema": {
                "fields": {
                    "card_id": "str",
                    "amount": "f64",
                    "event_time": "i64"
                },
                "optional_fields": []
            }
        }"#;
        let d: DerivationDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(d.ops.len(), 1);
        assert_eq!(
            d.ops[0],
            crate::op_node::OpNode::Filter {
                expr: "(amount > 500)".to_string()
            }
        );
        // Round-trip
        let j2 = serde_json::to_string(&d).unwrap();
        let d2: DerivationDescriptor = serde_json::from_str(&j2).unwrap();
        assert_eq!(d.name, d2.name);
        assert_eq!(d.ops, d2.ops);
    }

    // Test 9 (Plan 02-02): Derivation with GroupBy op round-trip
    #[test]
    fn derivation_with_group_by_round_trip() {
        let json = r#"{
            "name": "UserTxCount",
            "output_kind": "table",
            "upstreams": ["Transaction"],
            "ops": [{"op": "group_by", "keys": ["card_id"], "agg": {"cnt": {"op": "count", "params": {}}}}],
            "schema": {"fields": {"card_id": "str", "cnt": "i64"}, "optional_fields": []},
            "table_primary_key": ["card_id"]
        }"#;
        let d: DerivationDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(d.output_kind, OutputKind::Table);
        assert_eq!(d.table_primary_key, Some(vec!["card_id".to_string()]));
        let j2 = serde_json::to_string(&d).unwrap();
        let d2: DerivationDescriptor = serde_json::from_str(&j2).unwrap();
        assert_eq!(d.ops, d2.ops);
    }

    // Test 10 (Plan 02-02): equiv_ignoring_version still works with OpNode ops
    #[test]
    fn derivation_equiv_ignoring_version_with_ops() {
        let schema = crate::schema::DerivedSchema {
            fields: {
                let mut m = BTreeMap::new();
                m.insert("amount".to_string(), FieldType::F64);
                m
            },
            optional_fields: vec![],
        };
        let base = DerivationDescriptor {
            name: "D".to_string(),
            output_kind: OutputKind::Event,
            upstreams: vec!["A".to_string()],
            ops: vec![crate::op_node::OpNode::Filter {
                expr: "(amount > 1)".to_string(),
            }],
            schema: schema.clone(),
            table_primary_key: None,
            registered_at_version: 1,
        };

        // Same except version — equiv
        let mut same_diff_version = base.clone();
        same_diff_version.registered_at_version = 99;
        assert!(
            base.equiv_ignoring_version(&same_diff_version),
            "must be equiv when only version differs"
        );

        // Different ops — not equiv
        let mut diff_ops = base.clone();
        diff_ops.ops = vec![crate::op_node::OpNode::Filter {
            expr: "(amount > 999)".to_string(),
        }];
        assert!(
            !base.equiv_ignoring_version(&diff_ops),
            "must NOT be equiv when ops differ"
        );
    }
}
