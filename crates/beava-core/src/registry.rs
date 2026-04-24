//! Registry data model: descriptor structs, OutputKind, TableMode, RegistryInner,
//! and the parking_lot::RwLock-guarded Registry wrapper.

use crate::agg_descriptor::AggregationDescriptor;
use crate::op_chain::OpChain;
use crate::op_node::OpNode;
use crate::schema::{DerivedSchema, EventSchema, TableSchema};
use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

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
    Upsert,
}

// ─── Descriptor structs ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventDescriptor {
    pub name: String,
    pub schema: EventSchema,
    #[serde(default)]
    pub event_time_field: Option<String>,
    #[serde(default)]
    pub dedupe_key: Option<String>,
    #[serde(default)]
    pub dedupe_window_ms: Option<u64>,
    #[serde(default)]
    pub keep_events_for_ms: Option<u64>,
    #[serde(default)]
    pub tolerate_delay_ms: Option<u64>,
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
            && self.dedupe_key == other.dedupe_key
            && self.dedupe_window_ms == other.dedupe_window_ms
            && self.keep_events_for_ms == other.keep_events_for_ms
            && self.tolerate_delay_ms == other.tolerate_delay_ms
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

/// Runtime-only compiled op-chain cache. Parallel to `derivations` map.
/// Arc<OpChain> allows cheap sharing with the future push-path (Phase 6).
/// Not serialized — rebuilt from ops at register time.
#[derive(Debug, Default, Clone)]
pub struct RegistryInner {
    pub version: u64,
    pub events: BTreeMap<String, EventDescriptor>,
    pub tables: BTreeMap<String, TableDescriptor>,
    pub derivations: BTreeMap<String, DerivationDescriptor>,
    /// Phase 4: compiled op-chains keyed by derivation name.
    /// Populated by `apply_registration` when a derivation with ops is installed.
    pub compiled_chains: BTreeMap<String, Arc<OpChain>>,
    /// Phase 5 Plan 04: compiled aggregation descriptors keyed by derivation name.
    /// Populated by `apply_registration` when a derivation with GroupBy ops is installed.
    pub compiled_aggregations: BTreeMap<String, Arc<AggregationDescriptor>>,
    /// Phase 5 Plan 06: reverse index from feature name to (aggregation node_name, feature_index).
    /// Built at register time alongside compiled_aggregations.
    /// Enables O(1) feature-name → aggregation lookup at query time.
    pub feature_index: BTreeMap<String, (String, usize)>,
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

    /// Phase 4: Return the compiled OpChain for a derivation (if cached).
    /// Returns `None` if the derivation has no ops or was not yet registered.
    pub fn compiled_chain(&self, derivation_name: &str) -> Option<Arc<OpChain>> {
        self.inner
            .read()
            .compiled_chains
            .get(derivation_name)
            .cloned()
    }

    /// Phase 5 Plan 04: Return the compiled AggregationDescriptor for a derivation (if cached).
    /// Returns `None` if the derivation has no GroupBy ops or was not yet registered.
    pub fn compiled_aggregation(
        &self,
        derivation_name: &str,
    ) -> Option<Arc<AggregationDescriptor>> {
        self.inner
            .read()
            .compiled_aggregations
            .get(derivation_name)
            .cloned()
    }

    /// Phase 5 Plan 06: Return the (aggregation node_name, feature_index) for a
    /// feature name, or `None` if the feature name is not registered.
    /// O(1) reverse lookup into `feature_index`.
    pub fn resolve_feature(&self, feature_name: &str) -> Option<(String, usize)> {
        self.inner.read().feature_index.get(feature_name).cloned()
    }

    /// Phase 5 Plan 05: Return all compiled AggregationDescriptors whose
    /// `source_node_name` matches `source_name`.
    ///
    /// Used by `apply_event_to_aggregations` to route an incoming event to every
    /// aggregation that watches the event's source.
    pub fn compiled_aggregations_for_source(
        &self,
        source_name: &str,
    ) -> Vec<Arc<AggregationDescriptor>> {
        self.inner
            .read()
            .compiled_aggregations
            .values()
            .filter(|d| d.source_node_name == source_name)
            .cloned()
            .collect()
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

    /// Atomically install a batch of already-validated, non-conflicting PayloadNodes.
    /// Bumps version by 1 and stamps each NEW descriptor with `registered_at_version = new_version`.
    /// Existing (already_present) descriptors are left unchanged.
    ///
    /// Phase 4: Also installs compiled OpChains (`compiled_chains`) and overwrites the
    /// derivation schema for any derivation that has a server-propagated schema
    /// (`propagated_schemas`). Both lists come from `ValidatedPayload::into_parts()`.
    ///
    /// Phase 5 Plan 04: Also installs compiled AggregationDescriptors (`compiled_aggregations`).
    /// For aggregation derivations, the schema is overwritten with the server-authoritative
    /// aggregation output schema (D-05).
    ///
    /// Precondition: `nodes` has passed `validate_payload` and `compute_diff` yielded
    /// `changed = []` AND `added != []`. Caller (Plan 05 endpoint) enforces this.
    ///
    /// Returns the new version number.
    pub fn apply_registration(
        &self,
        nodes: Vec<crate::registry_diff::PayloadNode>,
        compiled_chains: Vec<(String, Arc<OpChain>)>,
        propagated_schemas: Vec<(String, crate::schema::DerivedSchema)>,
        compiled_aggregations: Vec<(String, Arc<AggregationDescriptor>)>,
    ) -> u64 {
        let mut w = self.inner.write();
        let new_version = w.version + 1;

        // Build lookup maps for propagated schemas, compiled chains, and
        // compiled aggregations so we can apply them alongside their descriptor
        // in the same write-lock pass.
        // Chains/aggregations are inserted ONLY when the derivation descriptor is
        // new — this prevents stale entries from accumulating if apply_registration
        // is ever called with a derivation that is already present (WR-01).
        let schema_map: std::collections::HashMap<String, crate::schema::DerivedSchema> =
            propagated_schemas.into_iter().collect();
        let mut chains_map: std::collections::HashMap<String, Arc<OpChain>> =
            compiled_chains.into_iter().collect();
        let mut agg_map: std::collections::HashMap<String, Arc<AggregationDescriptor>> =
            compiled_aggregations.into_iter().collect();

        // Track newly inserted aggregation node names for O(N_new) index update (WR-03).
        let mut newly_inserted_agg_names: Vec<String> = Vec::new();

        for n in nodes {
            match n {
                crate::registry_diff::PayloadNode::Event(mut e) => {
                    if !w.events.contains_key(&e.name) {
                        e.registered_at_version = new_version;
                        w.events.insert(e.name.clone(), e);
                    }
                }
                crate::registry_diff::PayloadNode::Table(mut t) => {
                    if !w.tables.contains_key(&t.name) {
                        t.registered_at_version = new_version;
                        w.tables.insert(t.name.clone(), t);
                    }
                }
                crate::registry_diff::PayloadNode::Derivation(mut d) => {
                    if !w.derivations.contains_key(&d.name) {
                        d.registered_at_version = new_version;
                        // Phase 4 (D-06): overwrite client-supplied schema with
                        // server-authoritative propagated schema, if available.
                        if let Some(propagated) = schema_map.get(&d.name) {
                            d.schema = propagated.clone();
                        }
                        // Install compiled chain alongside descriptor — only for
                        // new derivations, so stale chains never accumulate.
                        if let Some(chain) = chains_map.remove(&d.name) {
                            w.compiled_chains.insert(d.name.clone(), chain);
                        }
                        // Phase 5 Plan 04: install compiled aggregation descriptor.
                        if let Some(agg) = agg_map.remove(&d.name) {
                            newly_inserted_agg_names.push(d.name.clone());
                            w.compiled_aggregations.insert(d.name.clone(), agg);
                        }
                        w.derivations.insert(d.name.clone(), d);
                    }
                }
            }
        }

        // Phase 5 Plan 06: update feature_index for ONLY the newly inserted aggregation
        // nodes (WR-03: O(N_new) instead of O(N_total)).
        // Additive-only: existing entries are preserved via `entry().or_insert()`.
        // Collect new entries first to avoid simultaneous mutable + immutable borrows of `w`.
        let new_index_entries: Vec<(String, String, usize)> = newly_inserted_agg_names
            .iter()
            .filter_map(|node_name| {
                w.compiled_aggregations
                    .get(node_name)
                    .map(|d| (node_name, d))
            })
            .flat_map(|(node_name, agg_desc)| {
                agg_desc
                    .features
                    .iter()
                    .enumerate()
                    .map(|(idx, named_op)| (named_op.feature_name.clone(), node_name.clone(), idx))
                    .collect::<Vec<_>>()
            })
            .collect();
        for (feature_name, node_name, feature_idx) in new_index_entries {
            w.feature_index
                .entry(feature_name)
                .or_insert((node_name, feature_idx));
        }

        w.version = new_version;
        new_version
    }

    /// Phase 7 Plan 03: install descriptors loaded from a snapshot.
    ///
    /// Replaces the in-memory registry contents with the descriptor set carried
    /// by a `RegistryDescriptorsOnly` (the projection produced by
    /// `SnapshotBody::from_live`). Runtime caches (compiled chains, compiled
    /// aggregations, feature index) are NOT rebuilt here — recovery replays
    /// `RegistryBump` WAL records via `apply_registration` which compiles and
    /// caches them in normal flow. Cold start with snapshot only (no WAL
    /// records past the snapshot LSN): caches are empty until next register.
    ///
    /// Idempotent: calling twice with the same descriptors leaves the same
    /// state (modulo `version` which is overwritten). Caller MUST hold the
    /// invariant that this runs BEFORE any concurrent reader (Server::bind
    /// runs recovery before flipping readiness).
    pub fn install_from_descriptors(&self, body: &crate::snapshot_body::RegistryDescriptorsOnly) {
        let mut w = self.inner.write();
        w.version = body.version;
        w.events = body.events.clone();
        w.tables = body.tables.clone();
        w.derivations = body.derivations.clone();
        // Caches stay as they are; recovery's WAL replay re-applies any
        // registration via apply_registration, which populates them.
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
            "dedupe_key": "request_id",
            "dedupe_window_ms": 86400000,
            "keep_events_for_ms": 604800000,
            "tolerate_delay_ms": 5000
        }"#;

        let desc: EventDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(desc.name, "Transaction");
        assert_eq!(desc.event_time_field, Some("event_time".to_string()));
        assert_eq!(desc.dedupe_key, Some("request_id".to_string()));
        assert_eq!(desc.dedupe_window_ms, Some(86_400_000));
        assert_eq!(desc.keep_events_for_ms, Some(604_800_000));
        assert_eq!(desc.tolerate_delay_ms, Some(5000));
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
            "mode": "upsert"
        }"#;

        let desc: TableDescriptor = serde_json::from_str(json).unwrap();
        assert_eq!(desc.name, "Merchant");
        assert_eq!(desc.primary_key, vec!["merchant_id".to_string()]);
        assert_eq!(desc.ttl_ms, Some(2_592_000_000));
        assert_eq!(desc.mode, TableMode::Upsert);
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
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
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
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
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

    // Plan 02-05 tests: apply_registration

    #[test]
    fn apply_registration_installs_events() {
        use crate::registry_diff::PayloadNode;
        let r = Registry::new();
        let schema = make_event_schema();
        let event_a = EventDescriptor {
            name: "A".to_string(),
            schema,
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        };
        let new_version =
            r.apply_registration(vec![PayloadNode::Event(event_a)], vec![], vec![], vec![]);
        assert_eq!(new_version, 1);
        assert_eq!(r.version(), 1);
        let snap = r.snapshot();
        assert!(snap.events.contains_key("A"));
        assert_eq!(snap.events["A"].registered_at_version, 1);
    }

    #[test]
    fn apply_registration_bumps_version_linear() {
        use crate::registry_diff::PayloadNode;
        let r = Registry::new();

        let e1 = EventDescriptor {
            name: "E1".to_string(),
            schema: make_event_schema(),
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        };
        let v1 = r.apply_registration(vec![PayloadNode::Event(e1)], vec![], vec![], vec![]);
        assert_eq!(v1, 1);

        let e2 = EventDescriptor {
            name: "E2".to_string(),
            schema: make_event_schema(),
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        };
        let v2 = r.apply_registration(vec![PayloadNode::Event(e2)], vec![], vec![], vec![]);
        assert_eq!(v2, 2);

        let snap = r.snapshot();
        assert_eq!(snap.events["E1"].registered_at_version, 1);
        assert_eq!(snap.events["E2"].registered_at_version, 2);
    }

    #[test]
    fn apply_registration_skips_already_present() {
        use crate::registry_diff::PayloadNode;
        let r = Registry::new();

        // Seed EventA at v1
        let event_a = EventDescriptor {
            name: "A".to_string(),
            schema: make_event_schema(),
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        };
        r.apply_registration(
            vec![PayloadNode::Event(event_a.clone())],
            vec![],
            vec![],
            vec![],
        );
        assert_eq!(r.version(), 1);

        // Apply [EventA (identical), EventB (new)]
        let event_b = EventDescriptor {
            name: "B".to_string(),
            schema: make_event_schema(),
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        };
        let v2 = r.apply_registration(
            vec![PayloadNode::Event(event_a), PayloadNode::Event(event_b)],
            vec![],
            vec![],
            vec![],
        );
        assert_eq!(v2, 2);
        let snap = r.snapshot();
        // A's registered_at_version stays at 1 (not overwritten)
        assert_eq!(snap.events["A"].registered_at_version, 1);
        // B is stamped at v2
        assert_eq!(snap.events["B"].registered_at_version, 2);
    }

    // Plan 05-06 tests: feature_index + resolve_feature

    #[test]
    fn resolve_feature_after_register() {
        use crate::agg_descriptor::{AggregationDescriptor, NamedAggOp};
        use crate::agg_op::{AggKind, AggOpDescriptor};
        use crate::registry_diff::PayloadNode;

        let r = Registry::new();
        let event = EventDescriptor {
            name: "Txn".to_string(),
            schema: make_event_schema(),
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        };
        let agg_desc = AggregationDescriptor {
            node_name: "AggTable".to_string(),
            source_node_name: "Txn".to_string(),
            group_keys: vec!["card_id".to_string()],
            features: vec![
                NamedAggOp {
                    feature_name: "cnt".to_string(),
                    descriptor: AggOpDescriptor {
                        kind: AggKind::Count,
                        field: None,
                        window_ms: Some(300_000),
                        where_expr: None,
                        sketch_params: None,
                    },
                },
                NamedAggOp {
                    feature_name: "total".to_string(),
                    descriptor: AggOpDescriptor {
                        kind: AggKind::Sum,
                        field: Some("amount".to_string()),
                        window_ms: None,
                        where_expr: None,
                        sketch_params: None,
                    },
                },
            ],
        };
        let deriv = DerivationDescriptor {
            name: "AggTable".to_string(),
            output_kind: OutputKind::Table,
            upstreams: vec!["Txn".to_string()],
            ops: vec![],
            schema: crate::schema::DerivedSchema {
                fields: {
                    let mut m = BTreeMap::new();
                    m.insert("card_id".to_string(), crate::schema::FieldType::Str);
                    m.insert("cnt".to_string(), crate::schema::FieldType::I64);
                    m.insert("total".to_string(), crate::schema::FieldType::F64);
                    m
                },
                optional_fields: vec![],
            },
            table_primary_key: Some(vec!["card_id".to_string()]),
            registered_at_version: 0,
        };
        r.apply_registration(
            vec![PayloadNode::Event(event), PayloadNode::Derivation(deriv)],
            vec![],
            vec![],
            vec![("AggTable".to_string(), Arc::new(agg_desc))],
        );

        // cnt is at feature_index 0
        let cnt = r.resolve_feature("cnt");
        assert!(cnt.is_some(), "resolve_feature('cnt') must return Some");
        let (node, idx) = cnt.unwrap();
        assert_eq!(node, "AggTable");
        assert_eq!(idx, 0, "cnt is at feature_index 0");

        // total is at feature_index 1
        let total = r.resolve_feature("total");
        assert!(total.is_some(), "resolve_feature('total') must return Some");
        let (node2, idx2) = total.unwrap();
        assert_eq!(node2, "AggTable");
        assert_eq!(idx2, 1, "total is at feature_index 1");
    }

    #[test]
    fn resolve_feature_missing_returns_none() {
        let r = Registry::new();
        assert!(
            r.resolve_feature("unknown").is_none(),
            "resolve_feature('unknown') must return None on empty registry"
        );
    }

    #[test]
    fn resolve_feature_index_rebuilt_on_register() {
        use crate::agg_descriptor::{AggregationDescriptor, NamedAggOp};
        use crate::agg_op::{AggKind, AggOpDescriptor};
        use crate::registry_diff::PayloadNode;

        let r = Registry::new();

        // First registration: event + AggA with feature "cnt"
        let event_a = EventDescriptor {
            name: "EvA".to_string(),
            schema: make_event_schema(),
            event_time_field: None,
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        };
        let agg_a = Arc::new(AggregationDescriptor {
            node_name: "AggA".to_string(),
            source_node_name: "EvA".to_string(),
            group_keys: vec!["card_id".to_string()],
            features: vec![NamedAggOp {
                feature_name: "cnt".to_string(),
                descriptor: AggOpDescriptor {
                    kind: AggKind::Count,
                    field: None,
                    window_ms: None,
                    where_expr: None,
                    sketch_params: None,
                },
            }],
        });
        let deriv_a = DerivationDescriptor {
            name: "AggA".to_string(),
            output_kind: OutputKind::Table,
            upstreams: vec!["EvA".to_string()],
            ops: vec![],
            schema: crate::schema::DerivedSchema {
                fields: {
                    let mut m = BTreeMap::new();
                    m.insert("card_id".to_string(), crate::schema::FieldType::Str);
                    m.insert("cnt".to_string(), crate::schema::FieldType::I64);
                    m
                },
                optional_fields: vec![],
            },
            table_primary_key: Some(vec!["card_id".to_string()]),
            registered_at_version: 0,
        };
        r.apply_registration(
            vec![
                PayloadNode::Event(event_a),
                PayloadNode::Derivation(deriv_a),
            ],
            vec![],
            vec![],
            vec![("AggA".to_string(), agg_a)],
        );
        assert!(
            r.resolve_feature("cnt").is_some(),
            "cnt must be in index after first register"
        );

        // Second registration: event + AggB with feature "revenue"
        let event_b = EventDescriptor {
            name: "EvB".to_string(),
            schema: make_event_schema(),
            event_time_field: None,
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        };
        let agg_b = Arc::new(AggregationDescriptor {
            node_name: "AggB".to_string(),
            source_node_name: "EvB".to_string(),
            group_keys: vec!["card_id".to_string()],
            features: vec![NamedAggOp {
                feature_name: "revenue".to_string(),
                descriptor: AggOpDescriptor {
                    kind: AggKind::Sum,
                    field: Some("amount".to_string()),
                    window_ms: None,
                    where_expr: None,
                    sketch_params: None,
                },
            }],
        });
        let deriv_b = DerivationDescriptor {
            name: "AggB".to_string(),
            output_kind: OutputKind::Table,
            upstreams: vec!["EvB".to_string()],
            ops: vec![],
            schema: crate::schema::DerivedSchema {
                fields: {
                    let mut m = BTreeMap::new();
                    m.insert("card_id".to_string(), crate::schema::FieldType::Str);
                    m.insert("revenue".to_string(), crate::schema::FieldType::F64);
                    m
                },
                optional_fields: vec![],
            },
            table_primary_key: Some(vec!["card_id".to_string()]),
            registered_at_version: 0,
        };
        r.apply_registration(
            vec![
                PayloadNode::Event(event_b),
                PayloadNode::Derivation(deriv_b),
            ],
            vec![],
            vec![],
            vec![("AggB".to_string(), agg_b)],
        );

        // Both features must be in the index now
        assert!(
            r.resolve_feature("cnt").is_some(),
            "cnt must still be in index after second register"
        );
        assert!(
            r.resolve_feature("revenue").is_some(),
            "revenue must be in index after second register"
        );
        let (node, _) = r.resolve_feature("revenue").unwrap();
        assert_eq!(node, "AggB");
    }

    // Plan 05-04 test: compiled_aggregations cached after apply_registration
    #[test]
    fn compiled_aggregations_cached_after_apply_registration() {
        use crate::agg_descriptor::{AggregationDescriptor, NamedAggOp};
        use crate::agg_op::{AggKind, AggOpDescriptor};
        use crate::registry_diff::PayloadNode;

        let r = Registry::new();

        // Build a minimal event + aggregation derivation
        let event = EventDescriptor {
            name: "Txn".to_string(),
            schema: make_event_schema(),
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        };

        let agg_desc = AggregationDescriptor {
            node_name: "AggTable".to_string(),
            source_node_name: "Txn".to_string(),
            group_keys: vec!["card_id".to_string()],
            features: vec![NamedAggOp {
                feature_name: "cnt".to_string(),
                descriptor: AggOpDescriptor {
                    kind: AggKind::Count,
                    field: None,
                    window_ms: Some(300_000),
                    where_expr: None,
                    sketch_params: None,
                },
            }],
        };
        let agg_arc = Arc::new(agg_desc);

        let deriv = crate::registry::DerivationDescriptor {
            name: "AggTable".to_string(),
            output_kind: crate::registry::OutputKind::Table,
            upstreams: vec!["Txn".to_string()],
            ops: vec![],
            schema: crate::schema::DerivedSchema {
                fields: {
                    let mut m = BTreeMap::new();
                    m.insert("card_id".to_string(), crate::schema::FieldType::Str);
                    m.insert("cnt".to_string(), crate::schema::FieldType::I64);
                    m
                },
                optional_fields: vec![],
            },
            table_primary_key: Some(vec!["card_id".to_string()]),
            registered_at_version: 0,
        };

        r.apply_registration(
            vec![PayloadNode::Event(event), PayloadNode::Derivation(deriv)],
            vec![],
            vec![],
            vec![("AggTable".to_string(), agg_arc)],
        );

        let cached = r.compiled_aggregation("AggTable");
        assert!(
            cached.is_some(),
            "registry.compiled_aggregation('AggTable') must return Some after registration"
        );
        let cached = cached.unwrap();
        assert_eq!(cached.node_name, "AggTable");
        assert_eq!(cached.source_node_name, "Txn");
        assert_eq!(cached.group_keys, vec!["card_id"]);
        assert_eq!(cached.features.len(), 1);
        assert_eq!(cached.features[0].feature_name, "cnt");
    }
}
