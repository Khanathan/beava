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

/// Default for the `name_arc` field — populated server-side at registration,
/// so the deserialize default is just an empty Arc<str> placeholder. The
/// install_descriptors / apply_registration / install_from_descriptors paths
/// always overwrite this with `Arc::from(name.as_str())`.
fn default_event_name_arc() -> Arc<str> {
    Arc::from("")
}

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
    /// Plan 18-12: pre-allocated `Arc<str>` of `name`. The bookkeeping site in
    /// dispatch_push_sync clones this (refcount bump, ~5 ns) instead of
    /// calling `event_name.to_string()` (heap alloc, ~50-100 ns) on every push.
    /// Populated server-side at registration; client-supplied JSON omits it
    /// (skipped on serde, defaulted to `Arc::from("")` on deserialize, then
    /// overwritten to `Arc::from(name.as_str())` by the install/registration
    /// paths). Equality on Arc<str> is by `str` content, so derived PartialEq
    /// behaves intuitively even across different allocations.
    #[serde(skip, default = "default_event_name_arc")]
    pub name_arc: Arc<str>,
    /// Plan 19.2-01 (D-01): ordered list of distinct field names referenced by
    /// ALL aggregations that source from this event. Built as the union of all
    /// `AggregationDescriptor.field_names` lists across aggs for this source.
    /// Each `AggOpDescriptor.field_idx` is an index into this per-event list.
    /// The apply-loop pre-extracts `extracted[i] = row.get(apply_field_names[i])`
    /// once per event — O(distinct_fields) total — then each feature reads
    /// `extracted[feature.descriptor.field_idx]` in O(1).
    /// Populated by `Registry::apply_registration`; client JSON omits it.
    #[serde(skip, default)]
    pub apply_field_names: Vec<String>,
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
    /// Phase 11.5 (D-01): when `true`, the table is stored as an MVCC chain so
    /// `as_of=<lsn>` queries and `POST /retract` work. Defaults to `false` so
    /// pre-Phase-11.5 client payloads continue to deserialize as non-temporal.
    #[serde(default)]
    pub temporal: bool,
    /// Phase 11.5 (D-16): MVCC history-window in wall-clock milliseconds.
    /// Distinct from `ttl_ms` (per-row TTL): `retention_ms` bounds how far
    /// back `as_of` queries and retractions can reach. `None` means
    /// "unbounded retention" (use with care; memory grows with history).
    ///
    /// Note: `skip_serializing_if` is intentionally NOT used here — bincode's
    /// positional layout would then become asymmetric with decode. JSON clients
    /// can still omit the field (serde `default` handles the missing case).
    #[serde(default)]
    pub retention_ms: Option<u64>,
}

impl TableDescriptor {
    /// Compare two descriptors field-by-field, EXCLUDING `registered_at_version`.
    pub fn equiv_ignoring_version(&self, other: &Self) -> bool {
        self.name == other.name
            && self.primary_key == other.primary_key
            && self.schema == other.schema
            && self.ttl_ms == other.ttl_ms
            && self.mode == other.mode
            && self.temporal == other.temporal
            && self.retention_ms == other.retention_ms
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
    /// Plan 18-11 D-6: events stored as Arc so dispatch_push_sync can grab a
    /// cheap refcount-bump pointer instead of cloning the EventDescriptor on
    /// every push. snapshot/install paths convert via Arc::new and
    /// (*arc).clone() at the boundaries (cold paths).
    pub events: BTreeMap<String, Arc<EventDescriptor>>,
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
    /// Plan 18-11 D-7: precomputed per-source index. Maps a source event/table
    /// name to the list of compiled aggregations that watch it. Lookup is
    /// O(1) at apply time — replaces the prior linear scan over the
    /// compiled_aggregations BTreeMap. Built register-time alongside
    /// compiled_aggregations; tracked here so it survives Registry::clone.
    pub aggregations_by_source: std::collections::HashMap<String, Vec<Arc<AggregationDescriptor>>>,
    /// Plan 18-16: monotonic counter for stable u32 IDs assigned to each new
    /// aggregation at `apply_registration` time. Used as O(1) Vec index into
    /// `DevAggState.state_tables`. Increments by 1 per new aggregation; IDs
    /// are stable for process lifetime (additive-only registration).
    /// Default = 0; first aggregation gets ID 0.
    pub next_agg_id: u32,
    /// Plan 19.2-03 (D-04): maps a cluster-signature hash to a stable u32
    /// cluster_id. Aggregations sharing the same `group_keys` signature
    /// (declaration-order hash, NOT sorted-lex — see Warning 4 in PLAN.md)
    /// share a cluster_id so the apply loop builds EntityKey ONCE per cluster.
    pub cluster_id_by_signature: std::collections::HashMap<u64, u32>,
    /// Plan 19.2-03 (D-04): monotonic counter for cluster_id assignment.
    /// Default = 0; first unique cluster gets ID 0.
    pub next_cluster_id: u32,
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

    /// Plan 18-16 Task 16.2: read the registry's monotonic agg_id counter.
    /// Server-side register handlers call this after `apply_registration` to
    /// resize `DevAggState.state_tables` (a `Vec<AggStateTable>`) so apply hot
    /// path can index by `desc.agg_id` without bounds-issues.
    pub fn next_agg_id(&self) -> u32 {
        self.inner.read().next_agg_id
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

    /// Plan 18-11 D-6: O(1) Arc-backed event-descriptor lookup.
    ///
    /// Returns `None` if the event isn't registered. The returned `Arc` is a
    /// refcount bump on the registry-owned Arc — dispatch_push_sync can hold
    /// it for the duration of one push without cloning the EventDescriptor.
    pub fn get_event_descriptor(&self, name: &str) -> Option<Arc<EventDescriptor>> {
        self.inner.read().events.get(name).cloned()
    }

    /// Phase 5 Plan 06: Return the (aggregation node_name, feature_index) for a
    /// feature name, or `None` if the feature name is not registered.
    /// O(1) reverse lookup into `feature_index`.
    pub fn resolve_feature(&self, feature_name: &str) -> Option<(String, usize)> {
        self.inner.read().feature_index.get(feature_name).cloned()
    }

    /// Phase 5 Plan 05 + Plan 18-11 D-7: Return all compiled
    /// AggregationDescriptors whose `source_node_name` matches `source_name`.
    ///
    /// Used by `apply_event_to_aggregations` to route an incoming event to every
    /// aggregation that watches the event's source.
    ///
    /// **Plan 18-11 D-7:** O(1) HashMap lookup via the precomputed
    /// `aggregations_by_source` index. The returned Vec is cloned from the
    /// index — cheap because (a) it's a Vec of Arc, and (b) typical apps
    /// have 1-3 aggregations per source. Eliminates the prior
    /// `compiled_aggregations.values().filter(...).collect()` scan that
    /// allocated a fresh Vec on every push.
    pub fn compiled_aggregations_for_source(
        &self,
        source_name: &str,
    ) -> Vec<Arc<AggregationDescriptor>> {
        self.inner
            .read()
            .aggregations_by_source
            .get(source_name)
            .cloned()
            .unwrap_or_default()
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
            // Plan 18-12: pre-allocate the Arc<str> for the bookkeeping
            // hot path. Client-supplied descriptors deserialize with an
            // empty placeholder; we always overwrite it here.
            e.name_arc = Arc::from(e.name.as_str());
            w.events.insert(e.name.clone(), Arc::new(e));
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

        // Plan 19.4-04 (D-02): pre-compute the per-source field-union
        // (alphabetical-sorted distinct fields any incoming agg consumes) so
        // the EventDescriptor.apply_field_names can be set as the new event is
        // inserted. The union is the union of declared fields across all aggs
        // targeting the same `source_node_name`. BTreeSet's iteration order is
        // alphabetical — required for deterministic `field_idx_into_event_extracted`
        // resolution at register-time and snapshot replay.
        let mut new_union_per_source: std::collections::HashMap<
            String,
            std::collections::BTreeSet<String>,
        > = std::collections::HashMap::new();
        for (_, agg_arc) in compiled_aggregations.iter() {
            let entry = new_union_per_source
                .entry(agg_arc.source_node_name.clone())
                .or_default();
            for feat in agg_arc.features.iter() {
                if let Some(f) = &feat.descriptor.field {
                    entry.insert(f.clone());
                }
                if let Some(lat) = &feat.descriptor.ext.lat_field {
                    entry.insert(lat.clone());
                }
                if let Some(lon) = &feat.descriptor.ext.lon_field {
                    entry.insert(lon.clone());
                }
            }
        }

        let mut agg_map: std::collections::HashMap<String, Arc<AggregationDescriptor>> =
            compiled_aggregations.into_iter().collect();

        // Track newly inserted aggregation node names for O(N_new) index update (WR-03).
        let mut newly_inserted_agg_names: Vec<String> = Vec::new();

        for n in nodes {
            match n {
                crate::registry_diff::PayloadNode::Event(mut e) => {
                    if !w.events.contains_key(&e.name) {
                        e.registered_at_version = new_version;
                        // Plan 18-12: pre-allocate the Arc<str> for the
                        // bookkeeping hot path (refcount bump per push, no
                        // String alloc). See install_descriptors for the
                        // companion site.
                        e.name_arc = Arc::from(e.name.as_str());
                        // Plan 19.4-04 (D-02): seed apply_field_names from the
                        // alphabetical-sorted field union for any aggs in this
                        // batch targeting this source. If a future
                        // apply_registration adds aggs targeting an existing
                        // source, the post-loop union-extend pass below
                        // re-derives apply_field_names against ALL aggs (new +
                        // pre-existing) so the union stays consistent.
                        if let Some(union) = new_union_per_source.get(&e.name) {
                            e.apply_field_names = union.iter().cloned().collect();
                        }
                        w.events.insert(e.name.clone(), Arc::new(e));
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
                        // Phase 5 Plan 04 + Plan 18-11 D-7: install compiled
                        // aggregation descriptor and update the per-source
                        // index.
                        // Plan 18-16: assign a stable u32 agg_id from the
                        // monotonic counter and write it into the descriptor
                        // before inserting. We must clone+mutate since the
                        // caller passed Arc<Desc>.
                        if let Some(agg) = agg_map.remove(&d.name) {
                            newly_inserted_agg_names.push(d.name.clone());
                            // Assign the next available agg_id.
                            let mut agg_owned = (*agg).clone();
                            agg_owned.agg_id = w.next_agg_id;
                            w.next_agg_id += 1;

                            // Plan 19.2-03 (D-04): assign cluster_id — aggregations sharing
                            // the same group_keys signature (declaration-order hash) share a
                            // cluster_id so the apply loop builds EntityKey ONCE per cluster,
                            // not once per agg.  The signature is stable across restarts
                            // because it is computed from the group_keys in registration order
                            // (NOT sorted-lex) and uses 0u8 separators to avoid prefix
                            // collisions ("ab","c" ≠ "a","bc").
                            let sig = Self::cluster_signature(&agg_owned.group_keys);
                            agg_owned.cluster_id =
                                if let Some(&existing) = w.cluster_id_by_signature.get(&sig) {
                                    existing
                                } else {
                                    let id = w.next_cluster_id;
                                    w.next_cluster_id += 1;
                                    w.cluster_id_by_signature.insert(sig, id);
                                    id
                                };

                            // Plan 19.2-01 (D-01): resolve field indices at registration time.
                            // Look up the source event's schema to validate field references
                            // and populate field_idx on each feature descriptor, plus
                            // build agg.field_names (the per-agg distinct-fields list).
                            // `field_idx` indexes into `agg.field_names`; the apply loop
                            // pre-extracts by iterating `agg.field_names` once per event.
                            // Silently skip if the source event is not yet registered
                            // (register_validate enforces ordering before we reach here).
                            if let Some(src_event) = w.events.get(&agg_owned.source_node_name) {
                                let schema = src_event.schema.clone();
                                // Ignore errors: register_validate already checked field refs.
                                // Any remaining mismatch is a latent inconsistency — don't
                                // panic in the write path.
                                let _ = Self::resolve_field_indices_for_agg_mut_inner(
                                    &mut agg_owned,
                                    &schema,
                                );
                            }

                            let agg = Arc::new(agg_owned);
                            // Update aggregations_by_source for O(1) lookup at apply time.
                            let source_name = agg.source_node_name.clone();
                            w.aggregations_by_source
                                .entry(source_name)
                                .or_default()
                                .push(Arc::clone(&agg));
                            w.compiled_aggregations.insert(d.name.clone(), agg);
                        }
                        w.derivations.insert(d.name.clone(), d);
                    }
                }
            }
        }

        // Plan 19.4-04 (D-02) post-loop apply_field_names_post_pass: walk the
        // CURRENT registry's `compiled_aggregations` (post-insert) and rebuild
        // each affected source's `apply_field_names` as the union of declared
        // fields across ALL aggs targeting that source. This handles the
        // cross-batch case where an Event was registered in a prior call and
        // a new agg in this batch declares fields beyond what the prior union
        // covered. Cost: O(N_aggs × M_features) at register time only —
        // register-time is cold-path, the apply-loop reads the precomputed
        // `apply_field_names` slice.
        //
        // Determinism: the union is built via BTreeSet → Vec, so iteration
        // order is alphabetical. `field_idx_into_event_extracted` resolution
        // (Task 4.2.b) reads this same alphabetical ordering, ensuring
        // reproducibility across snapshot replay.
        let affected_sources: std::collections::BTreeSet<String> = newly_inserted_agg_names
            .iter()
            .filter_map(|node_name| {
                w.compiled_aggregations
                    .get(node_name)
                    .map(|a| a.source_node_name.clone())
            })
            .collect();
        for source_name in affected_sources {
            // Recompute the alphabetical-sorted union of declared fields across
            // every agg that targets this source.
            let mut union: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
            if let Some(aggs) = w.aggregations_by_source.get(&source_name) {
                for agg in aggs.iter() {
                    for feat in agg.features.iter() {
                        if let Some(f) = &feat.descriptor.field {
                            union.insert(f.clone());
                        }
                        if let Some(lat) = &feat.descriptor.ext.lat_field {
                            union.insert(lat.clone());
                        }
                        if let Some(lon) = &feat.descriptor.ext.lon_field {
                            union.insert(lon.clone());
                        }
                    }
                }
            }
            let new_fields: Vec<String> = union.into_iter().collect();
            // Update only if the union changed; avoids unnecessary Arc clones.
            if let Some(existing_arc) = w.events.get(&source_name) {
                if existing_arc.apply_field_names != new_fields {
                    let mut updated = (**existing_arc).clone();
                    updated.apply_field_names = new_fields;
                    w.events.insert(source_name, Arc::new(updated));
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

    /// Plan 19.2-01 (D-01): validate field references in `agg` against `schema`
    /// and return an error if any field is missing. Does NOT mutate the descriptor.
    /// Use `resolve_field_indices_for_agg_mut` for the in-place mutation path.
    ///
    /// Error message format:
    ///   `"aggregation '{node}': field '{fname}' referenced by feature '{feature}' is not in source schema for event '{source}'"`
    pub fn resolve_field_indices_for_agg(
        &self,
        agg: &crate::agg_descriptor::AggregationDescriptor,
        schema: &crate::schema::EventSchema,
    ) -> Result<(), String> {
        for feat in &agg.features {
            if let Some(fname) = &feat.descriptor.field {
                if !schema.fields.contains_key(fname.as_str()) {
                    return Err(format!(
                        "aggregation '{}': field '{}' referenced by feature '{}' is not in source schema for event '{}'",
                        agg.node_name, fname, feat.feature_name, agg.source_node_name
                    ));
                }
            }
            // Geo ops: validate ext.lat_field + ext.lon_field if present.
            if let Some(lat) = &feat.descriptor.ext.lat_field {
                if !schema.fields.contains_key(lat.as_str()) {
                    return Err(format!(
                        "aggregation '{}': geo lat_field '{}' referenced by feature '{}' is not in source schema for event '{}'",
                        agg.node_name, lat, feat.feature_name, agg.source_node_name
                    ));
                }
            }
            if let Some(lon) = &feat.descriptor.ext.lon_field {
                if !schema.fields.contains_key(lon.as_str()) {
                    return Err(format!(
                        "aggregation '{}': geo lon_field '{}' referenced by feature '{}' is not in source schema for event '{}'",
                        agg.node_name, lon, feat.feature_name, agg.source_node_name
                    ));
                }
            }
        }
        Ok(())
    }

    /// Plan 19.2-01 (D-01): resolve field indices in-place on `agg`.
    ///
    /// For each feature with `field: Some(fname)`:
    ///   - Validates that `fname` exists in `schema`. Returns `Err` if not.
    ///   - Assigns `feature.descriptor.field_idx` as the index into
    ///     `agg.field_names` (inserting if not already present).
    ///   - Two features referencing the same field get the same `field_idx`.
    ///
    /// Features with `field: None` keep `field_idx = FIELD_IDX_NONE`.
    ///
    /// Plan 19.4-03 (D-01): also resolves geo `ext.lat_idx`/`ext.lon_idx` from
    /// `ext.lat_field`/`ext.lon_field` against the same `field_names` list —
    /// engages the `update_at` fast path at agg_op.rs:933-960 for every geo
    /// feature whose lat/lon fields exist in schema. This completes the
    /// register-time index assignment that Plan 19.2-06 Task 3 left unfinished.
    ///
    /// Populates `agg.field_names` with the distinct field list in resolution order.
    pub fn resolve_field_indices_for_agg_mut(
        &self,
        agg: &mut crate::agg_descriptor::AggregationDescriptor,
        schema: &crate::schema::EventSchema,
    ) -> Result<(), String> {
        use crate::agg_op::FIELD_IDX_NONE;

        // First pass: validate all field references exist.
        self.resolve_field_indices_for_agg(agg, schema)?;

        // Second pass: build field_names and assign field_idx + lat_idx/lon_idx
        // to each feature. The same `field_names` list is referenced by
        // `field_idx` (single-field ops) and `lat_idx`/`lon_idx` (geo ops);
        // apply-loop pre-extraction populates one slot per `field_names` entry.
        let mut field_names: Vec<String> = Vec::new();

        for feat in &mut agg.features {
            if let Some(fname) = &feat.descriptor.field {
                let idx = if let Some(pos) = field_names.iter().position(|f| f == fname) {
                    pos
                } else {
                    let pos = field_names.len();
                    field_names.push(fname.clone());
                    pos
                };
                feat.descriptor.field_idx = idx as u8;
            } else {
                feat.descriptor.field_idx = FIELD_IDX_NONE;
            }

            // Plan 19.4-03 (D-01): resolve geo lat_idx/lon_idx alongside field_idx.
            // Engages the geo update_at fast path at agg_op.rs:933-960 — the
            // dispatch arms branch on `if lat_idx != FIELD_IDX_NONE`, falling
            // through to the slow `update()` row.get path when unresolved.
            match (
                &feat.descriptor.ext.lat_field,
                &feat.descriptor.ext.lon_field,
            ) {
                (Some(lat_name), Some(lon_name)) => {
                    let lat_idx = if let Some(pos) = field_names.iter().position(|f| f == lat_name)
                    {
                        pos
                    } else {
                        let pos = field_names.len();
                        field_names.push(lat_name.clone());
                        pos
                    };
                    let lon_idx = if let Some(pos) = field_names.iter().position(|f| f == lon_name)
                    {
                        pos
                    } else {
                        let pos = field_names.len();
                        field_names.push(lon_name.clone());
                        pos
                    };
                    feat.descriptor.ext.lat_idx = lat_idx as u8;
                    feat.descriptor.ext.lon_idx = lon_idx as u8;
                }
                _ => {
                    // Partial or absent geo declaration — keep sentinel; dispatch
                    // falls through to the slow update() path which reads by
                    // field-name (agg_op.rs:937-959). Partial resolution would
                    // be a bug because the dispatch only checks lat_idx.
                    feat.descriptor.ext.lat_idx = FIELD_IDX_NONE;
                    feat.descriptor.ext.lon_idx = FIELD_IDX_NONE;
                }
            }
        }

        agg.field_names = field_names;
        Ok(())
    }

    /// Plan 19.2-01 (D-01): static (no `&self`) version of
    /// `resolve_field_indices_for_agg_mut`, called inside the write-locked
    /// `apply_registration` closure where borrowing `self` is not possible.
    ///
    /// Same contract as `resolve_field_indices_for_agg_mut`:
    ///   - Validates field refs against `schema`. Returns `Err` on first missing field.
    ///   - Assigns `field_idx` (index into the per-agg `agg.field_names` list).
    ///   - Plan 19.4-03 (D-01): also resolves geo `ext.lat_idx`/`ext.lon_idx`
    ///     against the same `field_names` list — engages the `update_at` fast
    ///     path at agg_op.rs:933-960. Runtime apply path (registry.rs:458 →
    ///     `apply_registration` write-lock closure) calls THIS function, so the
    ///     lat_idx/lon_idx resolution must mirror the public version exactly.
    ///   - Populates `agg.field_names` with the distinct ordered field list.
    fn resolve_field_indices_for_agg_mut_inner(
        agg: &mut crate::agg_descriptor::AggregationDescriptor,
        schema: &crate::schema::EventSchema,
    ) -> Result<(), String> {
        use crate::agg_op::FIELD_IDX_NONE;

        // Validate all field references first.
        for feat in &agg.features {
            if let Some(fname) = &feat.descriptor.field {
                if !schema.fields.contains_key(fname.as_str()) {
                    return Err(format!(
                        "aggregation '{}': field '{}' referenced by feature '{}' is not in source schema for event '{}'",
                        agg.node_name, fname, feat.feature_name, agg.source_node_name
                    ));
                }
            }
            if let Some(lat) = &feat.descriptor.ext.lat_field {
                if !schema.fields.contains_key(lat.as_str()) {
                    return Err(format!(
                        "aggregation '{}': geo lat_field '{}' referenced by feature '{}' is not in source schema for event '{}'",
                        agg.node_name, lat, feat.feature_name, agg.source_node_name
                    ));
                }
            }
            if let Some(lon) = &feat.descriptor.ext.lon_field {
                if !schema.fields.contains_key(lon.as_str()) {
                    return Err(format!(
                        "aggregation '{}': geo lon_field '{}' referenced by feature '{}' is not in source schema for event '{}'",
                        agg.node_name, lon, feat.feature_name, agg.source_node_name
                    ));
                }
            }
        }

        // Build field_names and assign field_idx + lat_idx/lon_idx.
        // IDENTICAL logic to `resolve_field_indices_for_agg_mut`; both functions
        // produce the same field_names ordering for the same input agg/schema.
        let mut field_names: Vec<String> = Vec::new();
        for feat in &mut agg.features {
            if let Some(fname) = &feat.descriptor.field {
                let idx = if let Some(pos) = field_names.iter().position(|f| f == fname) {
                    pos
                } else {
                    let pos = field_names.len();
                    field_names.push(fname.clone());
                    pos
                };
                feat.descriptor.field_idx = idx as u8;
            } else {
                feat.descriptor.field_idx = FIELD_IDX_NONE;
            }

            // Plan 19.4-03 (D-01): geo lat_idx/lon_idx resolution. This is the
            // runtime-critical path: `apply_registration` invokes _inner from
            // its write-lock closure (registry.rs:458). Without this block
            // fraud-team's geo features stay on the slow `update()` arm.
            match (
                &feat.descriptor.ext.lat_field,
                &feat.descriptor.ext.lon_field,
            ) {
                (Some(lat_name), Some(lon_name)) => {
                    let lat_idx = if let Some(pos) = field_names.iter().position(|f| f == lat_name)
                    {
                        pos
                    } else {
                        let pos = field_names.len();
                        field_names.push(lat_name.clone());
                        pos
                    };
                    let lon_idx = if let Some(pos) = field_names.iter().position(|f| f == lon_name)
                    {
                        pos
                    } else {
                        let pos = field_names.len();
                        field_names.push(lon_name.clone());
                        pos
                    };
                    feat.descriptor.ext.lat_idx = lat_idx as u8;
                    feat.descriptor.ext.lon_idx = lon_idx as u8;
                }
                _ => {
                    feat.descriptor.ext.lat_idx = FIELD_IDX_NONE;
                    feat.descriptor.ext.lon_idx = FIELD_IDX_NONE;
                }
            }
        }
        agg.field_names = field_names;
        Ok(())
    }

    /// Plan 19.2-03 (D-03): validate that none of the aggregation's `group_keys`
    /// reference a float-typed column.
    ///
    /// Float group keys are rejected at register-time because NaN values are
    /// silently dropped at push time (they produce `None` from
    /// `EntityKeyShape::from_row`), which could cause confusing event drops.
    /// Users must cast float columns or ensure non-NaN before using as group key.
    ///
    /// Returns `Ok(())` if no float group keys are found. Returns `Err(String)`
    /// with a descriptive message if any group key has `FieldType::F64`.
    pub fn validate_group_keys_for_agg(
        &self,
        agg: &crate::agg_descriptor::AggregationDescriptor,
        schema: &crate::schema::EventSchema,
    ) -> Result<(), String> {
        for key in &agg.group_keys {
            if let Some(field_type) = schema.fields.get(key.as_str()) {
                if *field_type == crate::schema::FieldType::F64 {
                    return Err(format!(
                        "aggregation '{}': group_keys cannot include float field '{}' \
                         — float NaN values are rejected at push time. \
                         Use cast or ensure non-NaN.",
                        agg.node_name, key
                    ));
                }
            }
        }
        Ok(())
    }

    /// Plan 19.2-03 (D-04): compute a stable cluster signature hash for the
    /// given `group_keys` in DECLARATION ORDER (NOT sorted-lex).
    ///
    /// The existing `EntityKey::from_row` produces order-sensitive keys
    /// (column_name + value pairs in a SmallVec; Hash derived over the SmallVec).
    /// Sorting would create cluster_id collisions for aggs that produce different
    /// EntityKeys at runtime, breaking the shared-lookup invariant.
    ///
    /// A separator byte (0u8) is hashed between each key so that
    /// `["a","bc"]` and `["ab","c"]` produce different signatures.
    fn cluster_signature(group_keys: &[String]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut h = fxhash::FxHasher::default();
        for k in group_keys {
            k.as_str().hash(&mut h);
            0u8.hash(&mut h); // separator to prevent prefix collisions
        }
        h.finish()
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
        // Plan 18-11 D-6: snapshot body holds plain EventDescriptor; wrap in
        // Arc on install. snapshot writer (snapshot_body.rs::from_live)
        // unwraps the Arc into a plain map for serialization.
        // Plan 18-12: when reinstalling from a snapshot, populate name_arc
        // alongside the Arc<EventDescriptor> wrap. Snapshot bodies serialize
        // without name_arc (skipped on serde) so this re-derives it from the
        // descriptor's `name`.
        w.events = body
            .events
            .iter()
            .map(|(k, v)| {
                let mut ev = v.clone();
                ev.name_arc = Arc::from(ev.name.as_str());
                (k.clone(), Arc::new(ev))
            })
            .collect();
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

    // Phase 11.5: temporal table flag + retention_ms round-trip.
    // Verifies D-01 + D-16: TableDescriptor carries `temporal: bool` and
    // optional `retention_ms: u64` (MVCC history-window). Defaults to
    // (false, None) when absent so legacy tables continue to deserialize.
    #[test]
    fn temporal_table_descriptor_round_trips() {
        // Sub-assertion 1: explicit construction round-trips.
        let desc = TableDescriptor {
            name: "merch".to_string(),
            primary_key: vec!["mid".to_string()],
            schema: TableSchema {
                fields: [("mid".to_string(), FieldType::Str)].into_iter().collect(),
                optional_fields: vec![],
            },
            ttl_ms: None,
            mode: TableMode::Upsert,
            registered_at_version: 0,
            temporal: true,
            retention_ms: Some(7 * 86_400_000),
        };
        let s = serde_json::to_string(&desc).unwrap();
        let back: TableDescriptor = serde_json::from_str(&s).unwrap();
        assert!(back.temporal);
        assert_eq!(back.retention_ms, Some(604_800_000));

        // Sub-assertion 2: client-shape JSON parses with both new fields set.
        let json = r#"{
            "name": "merch",
            "primary_key": ["mid"],
            "schema": {"fields": {"mid": "str"}, "optional_fields": []},
            "mode": "upsert",
            "temporal": true,
            "retention_ms": 3600000
        }"#;
        let desc2: TableDescriptor = serde_json::from_str(json).unwrap();
        assert!(desc2.temporal);
        assert_eq!(desc2.retention_ms, Some(3_600_000));

        // Sub-assertion 3: backwards-compat — JSON without the new fields
        // defaults `temporal` to false and `retention_ms` to None.
        let legacy_json = r#"{
            "name": "u",
            "primary_key": ["k"],
            "schema": {"fields": {"k": "str"}, "optional_fields": []},
            "mode": "upsert"
        }"#;
        let desc3: TableDescriptor = serde_json::from_str(legacy_json).unwrap();
        assert!(!desc3.temporal);
        assert_eq!(desc3.retention_ms, None);
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
            name_arc: Arc::from(""),
            apply_field_names: vec![],
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
            name_arc: Arc::from(""),
            apply_field_names: vec![],
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
            name_arc: Arc::from(""),
            apply_field_names: vec![],
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
            name_arc: Arc::from(""),
            apply_field_names: vec![],
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
            name_arc: Arc::from(""),
            apply_field_names: vec![],
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
            name_arc: Arc::from(""),
            apply_field_names: vec![],
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
            name_arc: Arc::from(""),
            apply_field_names: vec![],
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
            name_arc: Arc::from(""),
            apply_field_names: vec![],
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
                        n: None,
                        half_life_ms: None,
                        sub_window_ms: None,
                        sigma: None,
                        sketch_params: None,
                        ext: Default::default(),
                        field_idx: crate::agg_op::FIELD_IDX_NONE,
                    },
                },
                NamedAggOp {
                    feature_name: "total".to_string(),
                    descriptor: AggOpDescriptor {
                        kind: AggKind::Sum,
                        field: Some("amount".to_string()),
                        window_ms: None,
                        where_expr: None,
                        n: None,
                        half_life_ms: None,
                        sub_window_ms: None,
                        sigma: None,
                        sketch_params: None,
                        ext: Default::default(),
                        field_idx: crate::agg_op::FIELD_IDX_NONE,
                    },
                },
            ],
            agg_id: 0, // placeholder; registry overwrites at apply_registration
            field_names: vec![],
            cluster_id: 0,
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
            name_arc: Arc::from(""),
            apply_field_names: vec![],
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
                    n: None,
                    half_life_ms: None,
                    sub_window_ms: None,
                    sigma: None,
                    sketch_params: None,
                    ext: Default::default(),
                    field_idx: crate::agg_op::FIELD_IDX_NONE,
                },
            }],
            agg_id: 0,
            field_names: vec![],
            cluster_id: 0,
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
            name_arc: Arc::from(""),
            apply_field_names: vec![],
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
                    n: None,
                    half_life_ms: None,
                    sub_window_ms: None,
                    sigma: None,
                    sketch_params: None,
                    ext: Default::default(),
                    field_idx: crate::agg_op::FIELD_IDX_NONE,
                },
            }],
            agg_id: 0,
            field_names: vec![],
            cluster_id: 0,
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
            name_arc: Arc::from(""),
            apply_field_names: vec![],
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
                    n: None,
                    half_life_ms: None,
                    sub_window_ms: None,
                    sigma: None,
                    sketch_params: None,
                    ext: Default::default(),
                    field_idx: crate::agg_op::FIELD_IDX_NONE,
                },
            }],
            agg_id: 0, // placeholder; registry overwrites at apply_registration
            field_names: vec![],
            cluster_id: 0,
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

    // Plan 18-12 RED: EventDescriptor gains a `name_arc: Arc<str>` field that
    // is populated to the descriptor's `name` at install/registration time.
    // The bookkeeping site in dispatch_push_sync uses
    // `descriptor.name_arc.clone()` (refcount bump) instead of
    // `event_name.to_string()` (heap alloc per push).
    //
    // This test pins three invariants:
    //   1. `name_arc.as_ref() == name` after install (population at register time)
    //   2. consecutive `get_event_descriptor` calls return Arcs whose
    //      `name_arc` field shares the same `Arc<str>` allocation
    //      (proves the Arc is registry-owned, not re-derived per-call)
    //   3. the EventDescriptor itself is also shared via Arc::ptr_eq
    //      (re-asserts Plan 18-11 D-6, here as the carrier for invariant 2)
    #[test]
    fn event_descriptor_has_name_arc_and_registry_shares_it() {
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
            name_arc: Arc::from(""), // server overwrites at install; value here is a placeholder
            apply_field_names: vec![],
        };
        r.install_descriptors(1, vec![event], vec![], vec![]);

        let d1 = r
            .get_event_descriptor("Txn")
            .expect("descriptor should be present after install");
        let d2 = r
            .get_event_descriptor("Txn")
            .expect("descriptor should be present on second lookup");

        // Invariant 1: name_arc carries the event's name post-install.
        assert_eq!(
            d1.name_arc.as_ref(),
            "Txn",
            "install_descriptors must populate name_arc to the descriptor's name"
        );

        // Invariant 2: name_arc shares the same allocation across lookups.
        assert!(
            Arc::ptr_eq(&d1.name_arc, &d2.name_arc),
            "consecutive get_event_descriptor calls must share the same name_arc allocation"
        );

        // Invariant 3: the carrier Arc<EventDescriptor> is also shared.
        assert!(
            Arc::ptr_eq(&d1, &d2),
            "consecutive get_event_descriptor calls must share the same EventDescriptor Arc"
        );
    }

    // Plan 18-12 RED (companion): apply_registration also populates name_arc.
    // Mirrors the install_descriptors path so both registry-entry sites stay
    // covered. This guards against accidentally regressing one path while
    // fixing the other.
    #[test]
    fn apply_registration_populates_name_arc() {
        use crate::registry_diff::PayloadNode;

        let r = Registry::new();
        let event = EventDescriptor {
            name: "Click".to_string(),
            schema: make_event_schema(),
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
            name_arc: Arc::from(""), // overwritten server-side at apply_registration
            apply_field_names: vec![],
        };
        r.apply_registration(vec![PayloadNode::Event(event)], vec![], vec![], vec![]);

        let d = r
            .get_event_descriptor("Click")
            .expect("descriptor should be present after apply_registration");
        assert_eq!(
            d.name_arc.as_ref(),
            "Click",
            "apply_registration must populate name_arc to the descriptor's name"
        );
    }

    // Plan 18-16 Task 16.1.a (RED): AggregationDescriptor.agg_id assigned monotonically.
    //
    // Registers two aggregations sequentially and asserts:
    // 1. agg_id values are 0 and 1 respectively (monotonic counter starting at 0).
    // 2. Re-registering the same aggregation name is a no-op (additive idempotence) —
    //    agg_id stays the same.
    //
    // This test FAILS before 16.1.b lands the `agg_id` field and `next_agg_id` counter.
    #[test]
    fn test_agg_id_assigned_monotonically() {
        use crate::agg_descriptor::{AggregationDescriptor, NamedAggOp};
        use crate::agg_op::{AggKind, AggOpDescriptor};
        use crate::registry_diff::PayloadNode;

        let r = Registry::new();

        // Helper: build a minimal AggregationDescriptor for a given source.
        let make_agg = |node_name: &str, source: &str| -> AggregationDescriptor {
            AggregationDescriptor {
                node_name: node_name.to_string(),
                source_node_name: source.to_string(),
                group_keys: vec!["user_id".to_string()],
                features: vec![NamedAggOp {
                    feature_name: "cnt".to_string(),
                    descriptor: AggOpDescriptor {
                        kind: AggKind::Count,
                        field: None,
                        window_ms: None,
                        where_expr: None,
                        n: None,
                        half_life_ms: None,
                        sub_window_ms: None,
                        sigma: None,
                        sketch_params: None,
                        ext: Default::default(),
                        field_idx: crate::agg_op::FIELD_IDX_NONE,
                    },
                }],
                agg_id: 0, // placeholder; registry overwrites at registration time
                field_names: vec![],
                cluster_id: 0,
            }
        };

        let make_event = |name: &str| -> EventDescriptor {
            EventDescriptor {
                name: name.to_string(),
                schema: make_event_schema(),
                event_time_field: None,
                dedupe_key: None,
                dedupe_window_ms: None,
                keep_events_for_ms: None,
                tolerate_delay_ms: None,
                registered_at_version: 0,
                name_arc: Arc::from(""),
                apply_field_names: vec![],
            }
        };

        let make_deriv = |name: &str, source: &str| -> DerivationDescriptor {
            DerivationDescriptor {
                name: name.to_string(),
                output_kind: OutputKind::Table,
                upstreams: vec![source.to_string()],
                ops: vec![],
                schema: crate::schema::DerivedSchema {
                    fields: {
                        let mut m = BTreeMap::new();
                        m.insert("user_id".to_string(), crate::schema::FieldType::Str);
                        m.insert("cnt".to_string(), crate::schema::FieldType::I64);
                        m
                    },
                    optional_fields: vec![],
                },
                table_primary_key: Some(vec!["user_id".to_string()]),
                registered_at_version: 0,
            }
        };

        // Register first aggregation: AggA (source=EvA) → must get agg_id=0.
        let agg_a = Arc::new(make_agg("AggA", "EvA"));
        r.apply_registration(
            vec![
                PayloadNode::Event(make_event("EvA")),
                PayloadNode::Derivation(make_deriv("AggA", "EvA")),
            ],
            vec![],
            vec![],
            vec![("AggA".to_string(), Arc::clone(&agg_a))],
        );

        let cached_a = r
            .compiled_aggregation("AggA")
            .expect("AggA must be in registry after registration");
        assert_eq!(
            cached_a.agg_id, 0,
            "first registered aggregation must get agg_id=0"
        );

        // Register second aggregation: AggB (source=EvB) → must get agg_id=1.
        let agg_b = Arc::new(make_agg("AggB", "EvB"));
        r.apply_registration(
            vec![
                PayloadNode::Event(make_event("EvB")),
                PayloadNode::Derivation(make_deriv("AggB", "EvB")),
            ],
            vec![],
            vec![],
            vec![("AggB".to_string(), Arc::clone(&agg_b))],
        );

        let cached_b = r
            .compiled_aggregation("AggB")
            .expect("AggB must be in registry after registration");
        assert_eq!(
            cached_b.agg_id, 1,
            "second registered aggregation must get agg_id=1"
        );

        // Idempotence: re-register AggA (already present) — agg_id must stay 0.
        let agg_a_again = Arc::new(make_agg("AggA", "EvA"));
        r.apply_registration(
            vec![PayloadNode::Derivation(make_deriv("AggA", "EvA"))],
            vec![],
            vec![],
            vec![("AggA".to_string(), Arc::clone(&agg_a_again))],
        );

        let cached_a_again = r
            .compiled_aggregation("AggA")
            .expect("AggA must still be in registry after re-registration");
        assert_eq!(
            cached_a_again.agg_id, 0,
            "re-registration must not change agg_id (additive idempotence)"
        );
    }

    // Plan 19.4-03 (D-01) Task 3.a RED → 3.b GREEN: lat_idx/lon_idx must be resolved
    // at register time so the existing geo update_at fast path (agg_op.rs:933-960)
    // engages on the apply hot path. Pre-19.4-03 they defaulted to FIELD_IDX_NONE
    // (agg_compile.rs:855-856) and the resolver was a no-op for those fields — see
    // 19.3-FLAMEGRAPH.md §2 row #8 (agg_geo::read_lat_lon = 2.86% self-time).
    #[test]
    fn geo_feature_resolves_lat_idx_and_lon_idx_at_register_time() {
        use crate::agg_descriptor::{AggregationDescriptor, NamedAggOp};
        use crate::agg_op::{AggExtParams, AggKind, AggOpDescriptor, FIELD_IDX_NONE};
        use crate::schema::{EventSchema, FieldType};

        let r = Registry::new();

        // Geo schema: lat, lon, plus a non-geo field for the union sanity check.
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("lat".to_string(), FieldType::F64);
        fields.insert("lon".to_string(), FieldType::F64);
        fields.insert("user_id".to_string(), FieldType::Str);
        let schema = EventSchema {
            fields,
            optional_fields: vec![],
        };

        let mut agg = AggregationDescriptor {
            node_name: "geo_test".to_string(),
            source_node_name: "Txn".to_string(),
            group_keys: vec!["user_id".to_string()],
            features: vec![NamedAggOp {
                feature_name: "max_kmh".to_string(),
                descriptor: AggOpDescriptor {
                    kind: AggKind::GeoVelocity,
                    field: None,
                    window_ms: None,
                    where_expr: None,
                    n: None,
                    half_life_ms: None,
                    sub_window_ms: None,
                    sigma: None,
                    sketch_params: None,
                    ext: AggExtParams {
                        lat_field: Some("lat".to_string()),
                        lon_field: Some("lon".to_string()),
                        ..Default::default()
                    },
                    field_idx: FIELD_IDX_NONE,
                },
            }],
            agg_id: 0,
            field_names: vec![],
            cluster_id: 0,
        };

        r.resolve_field_indices_for_agg_mut(&mut agg, &schema)
            .expect("registration must succeed for valid geo schema");

        let feat = &agg.features[0];
        assert_ne!(
            feat.descriptor.ext.lat_idx, FIELD_IDX_NONE,
            "lat_idx must be resolved at register time (was FIELD_IDX_NONE — Plan 19.2-06 Task 3 never landed; \
             agg_geo::read_lat_lon slow path stays engaged on hot path; see 19.3-FLAMEGRAPH §2 row #8)"
        );
        assert_ne!(
            feat.descriptor.ext.lon_idx, FIELD_IDX_NONE,
            "lon_idx must be resolved at register time (was FIELD_IDX_NONE)"
        );
        assert_ne!(
            feat.descriptor.ext.lat_idx, feat.descriptor.ext.lon_idx,
            "lat_idx and lon_idx must point at distinct ExtractedFields slots"
        );
        assert!(
            agg.field_names.iter().any(|n| n == "lat"),
            "lat must be in agg.field_names so apply-loop pre-extraction populates extracted[lat_idx]; got {:?}",
            agg.field_names
        );
        assert!(
            agg.field_names.iter().any(|n| n == "lon"),
            "lon must be in agg.field_names so apply-loop pre-extraction populates extracted[lon_idx]; got {:?}",
            agg.field_names
        );
    }

    // Plan 19.4-04 (D-02) Task 4.1.a RED: EventDescriptor.apply_field_names must be
    // populated at register-time as the field-union of all aggs on this source.
    // Uses the canonical Registry::apply_registration test pattern from line 1580's
    // compiled_aggregations_cached_after_apply_registration test — no fictional helper.
    #[test]
    fn event_descriptor_apply_field_names_is_populated_at_registration() {
        use crate::agg_descriptor::{AggregationDescriptor, NamedAggOp};
        use crate::agg_op::{AggKind, AggOpDescriptor, FIELD_IDX_NONE};
        use crate::registry_diff::PayloadNode;

        let r = Registry::new();

        // Event source with the make_event_schema() schema (card_id, amount,
        // merchant_id, event_time).
        let event_txn = EventDescriptor {
            name: "Txn".to_string(),
            schema: make_event_schema(),
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
            name_arc: Arc::from(""),
            apply_field_names: vec![], // <-- this is what the resolver must populate
        };

        // 2-feature aggregation on Txn source: Count (no field) + Sum(amount).
        let agg_desc = AggregationDescriptor {
            node_name: "AggTable".to_string(),
            source_node_name: "Txn".to_string(),
            group_keys: vec!["card_id".to_string()],
            features: vec![
                NamedAggOp {
                    feature_name: "txn_cnt".to_string(),
                    descriptor: AggOpDescriptor {
                        kind: AggKind::Count,
                        field: None,
                        window_ms: Some(300_000),
                        where_expr: None,
                        n: None,
                        half_life_ms: None,
                        sub_window_ms: None,
                        sigma: None,
                        sketch_params: None,
                        ext: Default::default(),
                        field_idx: FIELD_IDX_NONE,
                    },
                },
                NamedAggOp {
                    feature_name: "amount_sum".to_string(),
                    descriptor: AggOpDescriptor {
                        kind: AggKind::Sum,
                        field: Some("amount".to_string()),
                        window_ms: Some(300_000),
                        where_expr: None,
                        n: None,
                        half_life_ms: None,
                        sub_window_ms: None,
                        sigma: None,
                        sketch_params: None,
                        ext: Default::default(),
                        field_idx: FIELD_IDX_NONE,
                    },
                },
            ],
            agg_id: 0,
            field_names: vec![],
            cluster_id: 0,
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
                    m.insert("txn_cnt".to_string(), crate::schema::FieldType::I64);
                    m.insert("amount_sum".to_string(), crate::schema::FieldType::F64);
                    m
                },
                optional_fields: vec![],
            },
            table_primary_key: Some(vec!["card_id".to_string()]),
            registered_at_version: 0,
        };

        r.apply_registration(
            vec![
                PayloadNode::Event(event_txn),
                PayloadNode::Derivation(deriv),
            ],
            vec![],
            vec![],
            vec![("AggTable".to_string(), agg_arc)],
        );

        let txn_desc = r
            .get_event_descriptor("Txn")
            .expect("Txn must be registered");
        assert!(
            !txn_desc.apply_field_names.is_empty(),
            "EventDescriptor.apply_field_names is still vec![] post-registration; \
             Plan 19.4-04 D-02 requires register-time population (union of agg-declared fields)"
        );
        assert!(
            txn_desc.apply_field_names.iter().any(|n| n == "amount"),
            "amount field (consumed by amount_sum agg) must be in the union; got {:?}",
            txn_desc.apply_field_names
        );
        let schema_fields = make_event_schema().fields;
        for n in txn_desc.apply_field_names.iter() {
            assert!(
                schema_fields.contains_key(n.as_str()),
                "apply_field_names entry '{}' must resolve to a real schema field",
                n
            );
        }
    }

    // Plan 19.4-04 (D-02) Task 4.2.a RED: each agg feature has
    // field_idx_into_event_extracted mapping its declared fields to per-event
    // union indices. RED state: AggOpDescriptor does not yet carry the field
    // (compile error E0609 — added in Task 4.2.b).
    #[test]
    fn agg_field_idx_into_event_extracted_resolved_against_union() {
        use crate::agg_descriptor::{AggregationDescriptor, NamedAggOp};
        use crate::agg_op::{AggKind, AggOpDescriptor, FIELD_IDX_NONE};
        use crate::registry_diff::PayloadNode;

        let r = Registry::new();

        let event_txn = EventDescriptor {
            name: "Txn".to_string(),
            schema: make_event_schema(),
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
            name_arc: Arc::from(""),
            apply_field_names: vec![],
        };

        let agg_desc = AggregationDescriptor {
            node_name: "AggTable2".to_string(),
            source_node_name: "Txn".to_string(),
            group_keys: vec!["card_id".to_string()],
            features: vec![
                NamedAggOp {
                    feature_name: "txn_cnt".to_string(),
                    descriptor: AggOpDescriptor {
                        kind: AggKind::Count,
                        field: None,
                        window_ms: Some(300_000),
                        where_expr: None,
                        n: None,
                        half_life_ms: None,
                        sub_window_ms: None,
                        sigma: None,
                        sketch_params: None,
                        ext: Default::default(),
                        field_idx: FIELD_IDX_NONE,
                    },
                },
                NamedAggOp {
                    feature_name: "amount_sum".to_string(),
                    descriptor: AggOpDescriptor {
                        kind: AggKind::Sum,
                        field: Some("amount".to_string()),
                        window_ms: Some(300_000),
                        where_expr: None,
                        n: None,
                        half_life_ms: None,
                        sub_window_ms: None,
                        sigma: None,
                        sketch_params: None,
                        ext: Default::default(),
                        field_idx: FIELD_IDX_NONE,
                    },
                },
            ],
            agg_id: 0,
            field_names: vec![],
            cluster_id: 0,
        };
        let agg_arc = Arc::new(agg_desc);

        let deriv = crate::registry::DerivationDescriptor {
            name: "AggTable2".to_string(),
            output_kind: crate::registry::OutputKind::Table,
            upstreams: vec!["Txn".to_string()],
            ops: vec![],
            schema: crate::schema::DerivedSchema {
                fields: {
                    let mut m = BTreeMap::new();
                    m.insert("card_id".to_string(), crate::schema::FieldType::Str);
                    m.insert("txn_cnt".to_string(), crate::schema::FieldType::I64);
                    m.insert("amount_sum".to_string(), crate::schema::FieldType::F64);
                    m
                },
                optional_fields: vec![],
            },
            table_primary_key: Some(vec!["card_id".to_string()]),
            registered_at_version: 0,
        };

        r.apply_registration(
            vec![
                PayloadNode::Event(event_txn),
                PayloadNode::Derivation(deriv),
            ],
            vec![],
            vec![],
            vec![("AggTable2".to_string(), agg_arc)],
        );

        let cached = r.compiled_aggregation("AggTable2").expect("agg present");
        let txn_desc = r.get_event_descriptor("Txn").expect("event present");

        // Locate the index of "amount" in the alphabetical union.
        let union_amount_idx = txn_desc
            .apply_field_names
            .iter()
            .position(|n| n == "amount")
            .expect("amount must be in the union after Task 4.1.b")
            as u8;

        // Find features by name and assert mapping shape.
        let count_feat = cached
            .features
            .iter()
            .find(|f| f.feature_name == "txn_cnt")
            .expect("txn_cnt feature");
        let sum_feat = cached
            .features
            .iter()
            .find(|f| f.feature_name == "amount_sum")
            .expect("amount_sum feature");

        // RED: field_idx_into_event_extracted does not exist as a field; E0609.
        assert!(
            count_feat
                .descriptor
                .field_idx_into_event_extracted
                .is_empty(),
            "Count agg has no field; field_idx_into_event_extracted must be empty"
        );
        assert_eq!(
            sum_feat.descriptor.field_idx_into_event_extracted.len(),
            1,
            "Sum agg has 1 declared field; mapping len must be 1"
        );
        assert_eq!(
            sum_feat.descriptor.field_idx_into_event_extracted[0], union_amount_idx,
            "Sum's mapping[0] must equal 'amount'-position in the union"
        );
    }
}
