//! Pre-diff validation pass for `POST /register` payloads.
//!
//! Validates all 9 rules from 02-CONTEXT.md §Validation pass:
//! 1. Node uniqueness within payload
//! 2. Reserved names / pattern / length
//! 3. Event schema: non-empty; if event_time_field is Some, it must exist and be I64.
//!    If event_time_field is None, the server will stamp wall-clock time on push.
//! 4. Table schema: primary_key ≥ 1 and ≤ 4 fields, all in schema
//! 5. Derivation upstreams: each name resolves in payload OR current registry
//! 6. Derivation schema non-empty; output_kind=Table requires table_primary_key
//! 7. DAG acyclicity (DFS, reports first cycle)
//! 8. Topological order (upstreams-within-payload appear before dependents)
//! 9. Dedupe key: if present, must be in schema; dedupe_window_ms must be positive

use crate::registry::{EventDescriptor, RegistryInner, TableDescriptor};
use crate::registry_diff::PayloadNode;
use crate::schema::{validate_descriptor_name, DescriptorNameError, FieldType};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

// ─── Public types ─────────────────────────────────────────────────────────────

/// Machine-readable error code for each validation rule violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    InvalidRegistration,
    NameDuplicate,
    NameReservedPrefix,
    NameBadPattern,
    NameEmpty,
    NameTooLong,
    EventTimeFieldMissing,
    EventTimeFieldWrongType,
    EventSchemaEmpty,
    TablePrimaryKeyEmpty,
    TablePrimaryKeyTooLong,
    TablePrimaryKeyUnknownField,
    DerivationUpstreamUnknown,
    DerivationSchemaEmpty,
    RegistrationCycle,
    TopologicalOrderViolation,
    DedupeKeyUnknownField,
    DedupeWindowNonPositive,
    DerivationOutputKindTableMissingPrimaryKey,
}

/// A single structured validation error. `path` uses pseudo-JSON-pointer format
/// (e.g., `"nodes[2].upstreams[0]"`). `reason` is human-readable.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ValidationError {
    pub code: ErrorCode,
    pub path: String,
    pub reason: String,
}

/// Newtype wrapper: a `Vec<PayloadNode>` that has passed all validation rules.
/// `compute_diff` (Plan 03) accepts `&[PayloadNode]` via `as_slice()`.
/// The endpoint (Plan 05) extracts the inner vec via `into_inner()`.
#[derive(Debug)]
pub struct ValidatedPayload(pub(crate) Vec<PayloadNode>);

impl ValidatedPayload {
    pub fn as_slice(&self) -> &[PayloadNode] {
        &self.0
    }

    pub fn into_inner(self) -> Vec<PayloadNode> {
        self.0
    }
}

// ─── Main entry point ─────────────────────────────────────────────────────────

/// Validate a registration payload against the current registry state.
///
/// Returns `Ok(ValidatedPayload)` if all 9 rules pass.
/// Returns `Err(Vec<ValidationError>)` with ALL detected violations (fail-soft, not fail-fast),
/// except for the three cross-node rules (5, 7, 8) which are appended after per-node checks.
///
/// An empty payload is valid and results in a no-op at the endpoint.
pub fn validate_payload(
    current: &RegistryInner,
    payload: Vec<PayloadNode>,
) -> Result<ValidatedPayload, Vec<ValidationError>> {
    if payload.is_empty() {
        return Ok(ValidatedPayload(payload));
    }

    let mut errors: Vec<ValidationError> = Vec::new();

    // Rule 1: uniqueness within payload
    validate_uniqueness_within_payload(&payload, &mut errors);

    // Rules 2, 3, 4, 6, 9 — per node
    for (i, node) in payload.iter().enumerate() {
        validate_node_name(i, node, &mut errors);
        match node {
            PayloadNode::Event(e) => validate_event(i, e, &mut errors),
            PayloadNode::Table(t) => validate_table(i, t, &mut errors),
            PayloadNode::Derivation(d) => validate_derivation_struct(i, d, &mut errors),
        }
    }

    // Rule 5: upstream resolution
    validate_upstreams(&payload, current, &mut errors);

    // Rule 8: topological order (upstreams-within-payload must appear before dependents)
    validate_topological_order(&payload, &mut errors);

    // Rule 7: DAG acyclicity (across payload + current)
    validate_acyclicity(&payload, current, &mut errors);

    if errors.is_empty() {
        Ok(ValidatedPayload(payload))
    } else {
        Err(errors)
    }
}

// ─── Path helpers ─────────────────────────────────────────────────────────────

fn path_node(i: usize) -> String {
    format!("nodes[{i}]")
}

fn path_field(i: usize, suffix: &str) -> String {
    format!("nodes[{i}].{suffix}")
}

// ─── Rule 1: uniqueness within payload ────────────────────────────────────────

fn validate_uniqueness_within_payload(payload: &[PayloadNode], errors: &mut Vec<ValidationError>) {
    let mut seen: HashMap<&str, usize> = HashMap::new();
    for (i, node) in payload.iter().enumerate() {
        let name = node.name();
        if let Some(first_idx) = seen.get(name) {
            errors.push(ValidationError {
                code: ErrorCode::NameDuplicate,
                path: path_field(i, "name"),
                reason: format!(
                    "duplicate descriptor name '{name}'; first seen at nodes[{first_idx}]"
                ),
            });
        } else {
            seen.insert(name, i);
        }
    }
}

// ─── Rule 2: name validation ──────────────────────────────────────────────────

fn validate_node_name(i: usize, node: &PayloadNode, errors: &mut Vec<ValidationError>) {
    let name = node.name();
    match validate_descriptor_name(name) {
        Ok(()) => {}
        Err(DescriptorNameError::Empty) => errors.push(ValidationError {
            code: ErrorCode::NameEmpty,
            path: path_field(i, "name"),
            reason: "descriptor name must not be empty".to_string(),
        }),
        Err(DescriptorNameError::BadPattern(n)) => errors.push(ValidationError {
            code: ErrorCode::NameBadPattern,
            path: path_field(i, "name"),
            reason: format!(
                "descriptor name '{n}' must match [A-Za-z_][A-Za-z0-9_]* (no hyphens or leading digits)"
            ),
        }),
        Err(DescriptorNameError::ReservedPrefix(n)) => errors.push(ValidationError {
            code: ErrorCode::NameReservedPrefix,
            path: path_field(i, "name"),
            reason: format!("descriptor name '{n}' uses reserved prefix '_beava_'"),
        }),
        Err(DescriptorNameError::TooLong { len }) => errors.push(ValidationError {
            code: ErrorCode::NameTooLong,
            path: path_field(i, "name"),
            reason: format!("descriptor name is {len} chars; maximum is 128"),
        }),
    }
}

// ─── Rule 3: event schema validation ─────────────────────────────────────────

fn validate_event(i: usize, e: &EventDescriptor, errors: &mut Vec<ValidationError>) {
    // If event_time_field is Some, it must exist in schema with I64 type.
    // If None, server will stamp wall-clock time on push (skip existence+type checks).
    if let Some(ref etf) = e.event_time_field {
        match e.schema.fields.get(etf) {
            None => {
                errors.push(ValidationError {
                    code: ErrorCode::EventTimeFieldMissing,
                    path: path_field(i, &format!("schema.fields.{etf}")),
                    reason: format!("event_time_field '{etf}' does not exist in schema.fields"),
                });
            }
            Some(ft) if *ft != FieldType::I64 => {
                errors.push(ValidationError {
                    code: ErrorCode::EventTimeFieldWrongType,
                    path: path_field(i, &format!("schema.fields.{etf}")),
                    reason: format!("event_time_field '{etf}' must be type i64, got {ft:?}"),
                });
            }
            _ => {}
        }

        // When event_time_field is Some, schema must have ≥1 non-event_time field.
        let non_ts_count = e
            .schema
            .fields
            .keys()
            .filter(|k| k.as_str() != etf.as_str())
            .count();
        if non_ts_count == 0 {
            errors.push(ValidationError {
                code: ErrorCode::EventSchemaEmpty,
                path: path_field(i, "schema.fields"),
                reason: "event schema must have at least one field besides event_time_field"
                    .to_string(),
            });
        }
    } else {
        // No event_time_field → schema must be non-empty (any fields OK).
        if e.schema.fields.is_empty() {
            errors.push(ValidationError {
                code: ErrorCode::EventSchemaEmpty,
                path: path_field(i, "schema.fields"),
                reason: "event schema must have at least one field".to_string(),
            });
        }
    }

    // Rule 9: dedupe_key
    if let Some(ref key) = e.dedupe_key {
        if !e.schema.fields.contains_key(key) {
            errors.push(ValidationError {
                code: ErrorCode::DedupeKeyUnknownField,
                path: path_field(i, "dedupe_key"),
                reason: format!("dedupe_key '{key}' is not a field in schema"),
            });
        }
    }
    if let Some(ttl) = e.dedupe_window_ms {
        if ttl == 0 {
            errors.push(ValidationError {
                code: ErrorCode::DedupeWindowNonPositive,
                path: path_field(i, "dedupe_window_ms"),
                reason: "dedupe_window_ms must be positive (> 0)".to_string(),
            });
        }
    }
}

// ─── Rule 4: table schema validation ─────────────────────────────────────────

fn validate_table(i: usize, t: &TableDescriptor, errors: &mut Vec<ValidationError>) {
    if t.primary_key.is_empty() {
        errors.push(ValidationError {
            code: ErrorCode::TablePrimaryKeyEmpty,
            path: path_field(i, "primary_key"),
            reason: "primary_key must have at least 1 field".to_string(),
        });
        return; // don't check unknown fields if key is empty
    }
    if t.primary_key.len() > 4 {
        errors.push(ValidationError {
            code: ErrorCode::TablePrimaryKeyTooLong,
            path: path_field(i, "primary_key"),
            reason: format!(
                "primary_key has {} fields; maximum is 4",
                t.primary_key.len()
            ),
        });
    }
    for (j, key_field) in t.primary_key.iter().enumerate() {
        if !t.schema.fields.contains_key(key_field) {
            errors.push(ValidationError {
                code: ErrorCode::TablePrimaryKeyUnknownField,
                path: path_field(i, &format!("primary_key[{j}]")),
                reason: format!("primary_key field '{key_field}' does not exist in schema.fields"),
            });
        }
    }
}

// ─── Rule 6: derivation schema + output_kind=Table check ─────────────────────

fn validate_derivation_struct(
    i: usize,
    d: &crate::registry::DerivationDescriptor,
    errors: &mut Vec<ValidationError>,
) {
    if d.schema.fields.is_empty() {
        errors.push(ValidationError {
            code: ErrorCode::DerivationSchemaEmpty,
            path: path_field(i, "schema.fields"),
            reason: "derivation schema must have at least one field".to_string(),
        });
    }

    if d.output_kind == crate::registry::OutputKind::Table && d.table_primary_key.is_none() {
        errors.push(ValidationError {
            code: ErrorCode::DerivationOutputKindTableMissingPrimaryKey,
            path: path_field(i, "table_primary_key"),
            reason: "derivation with output_kind='table' must specify table_primary_key"
                .to_string(),
        });
    }
}

// ─── Rule 5: upstream resolution ─────────────────────────────────────────────

fn validate_upstreams(
    payload: &[PayloadNode],
    current: &RegistryInner,
    errors: &mut Vec<ValidationError>,
) {
    let payload_names: HashSet<&str> = payload.iter().map(|n| n.name()).collect();

    for (i, node) in payload.iter().enumerate() {
        if let PayloadNode::Derivation(d) = node {
            for (j, upstream) in d.upstreams.iter().enumerate() {
                let known_in_payload = payload_names.contains(upstream.as_str());
                let known_in_current = current.events.contains_key(upstream)
                    || current.tables.contains_key(upstream)
                    || current.derivations.contains_key(upstream);
                if !known_in_payload && !known_in_current {
                    errors.push(ValidationError {
                        code: ErrorCode::DerivationUpstreamUnknown,
                        path: path_field(i, &format!("upstreams[{j}]")),
                        reason: format!(
                            "upstream '{upstream}' is not declared in this payload or in the registry"
                        ),
                    });
                }
            }
        }
    }
}

// ─── Rule 8: topological order ────────────────────────────────────────────────

fn validate_topological_order(payload: &[PayloadNode], errors: &mut Vec<ValidationError>) {
    // Build index: name → position in payload
    let payload_index: HashMap<&str, usize> = payload
        .iter()
        .enumerate()
        .map(|(i, n)| (n.name(), i))
        .collect();

    for (i, node) in payload.iter().enumerate() {
        if let PayloadNode::Derivation(d) = node {
            for (j, upstream) in d.upstreams.iter().enumerate() {
                // Only check upstreams that appear in this payload (not registry-resolved ones)
                if let Some(&upstream_idx) = payload_index.get(upstream.as_str()) {
                    if upstream_idx > i {
                        errors.push(ValidationError {
                            code: ErrorCode::TopologicalOrderViolation,
                            path: path_field(i, &format!("upstreams[{j}]")),
                            reason: format!(
                                "upstream '{upstream}' appears later in payload at nodes[{upstream_idx}]"
                            ),
                        });
                    }
                }
            }
        }
    }
}

// ─── Rule 7: acyclicity (DFS, three-color) ────────────────────────────────────

fn validate_acyclicity(
    payload: &[PayloadNode],
    current: &RegistryInner,
    errors: &mut Vec<ValidationError>,
) {
    // Build adjacency: name → Vec<upstream_name>
    // Payload nodes shadow current nodes of the same name.
    let mut adj: HashMap<String, Vec<String>> = HashMap::new();

    // Start with current registry's derivations
    for (name, d) in &current.derivations {
        adj.insert(name.clone(), d.upstreams.clone());
    }

    // Overlay with payload (payload shadows current)
    for node in payload {
        if let PayloadNode::Derivation(d) = node {
            adj.insert(d.name.clone(), d.upstreams.clone());
        }
    }

    // Build payload index for error reporting
    let payload_index: HashMap<&str, usize> = payload
        .iter()
        .enumerate()
        .map(|(i, n)| (n.name(), i))
        .collect();

    // Collect all node names (only derivations can form cycles; events/tables have no upstreams)
    let all_names: Vec<String> = adj.keys().cloned().collect();

    // Three-color DFS
    // 0 = white (unvisited), 1 = gray (in stack), 2 = black (done)
    let mut color: HashMap<String, u8> = HashMap::new();
    let mut stack: Vec<String> = Vec::new();

    for start in &all_names {
        if color.get(start).copied().unwrap_or(0) == 0 {
            if let Some(cycle) = dfs_cycle(start, &adj, &mut color, &mut stack) {
                // Find which payload node is in the cycle for error path
                let cycle_str = cycle.join(" -> ");
                // Pick the first payload node that's part of the cycle for path
                let path = cycle
                    .iter()
                    .filter_map(|n| payload_index.get(n.as_str()))
                    .next()
                    .map(|idx| path_node(*idx))
                    .unwrap_or_else(|| "nodes".to_string());

                errors.push(ValidationError {
                    code: ErrorCode::RegistrationCycle,
                    path,
                    reason: format!("cycle detected: {cycle_str}"),
                });
                return; // report only the first cycle (CONTEXT.md: first-wins)
            }
        }
    }
}

fn dfs_cycle(
    node: &str,
    adj: &HashMap<String, Vec<String>>,
    color: &mut HashMap<String, u8>,
    stack: &mut Vec<String>,
) -> Option<Vec<String>> {
    color.insert(node.to_string(), 1); // gray
    stack.push(node.to_string());

    if let Some(neighbors) = adj.get(node) {
        for neighbor in neighbors {
            let c = color.get(neighbor).copied().unwrap_or(0);
            if c == 1 {
                // Back edge → cycle found; extract cycle from stack
                let cycle_start = stack.iter().position(|n| n == neighbor).unwrap_or(0);
                let mut cycle: Vec<String> = stack[cycle_start..].to_vec();
                cycle.push(neighbor.to_string()); // close the cycle
                return Some(cycle);
            }
            if c == 0 {
                if let Some(cycle) = dfs_cycle(neighbor, adj, color, stack) {
                    return Some(cycle);
                }
            }
        }
    }

    stack.pop();
    color.insert(node.to_string(), 2); // black
    None
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests_structural {
    use super::*;
    use crate::registry::{
        DerivationDescriptor, EventDescriptor, OutputKind, TableDescriptor, TableMode,
    };
    use crate::schema::{DerivedSchema, EventSchema, FieldType, TableSchema};
    use std::collections::BTreeMap;

    // ── Test helpers ──────────────────────────────────────────────────────────

    fn empty_current() -> RegistryInner {
        RegistryInner::default()
    }

    fn minimal_event(name: &str) -> PayloadNode {
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        PayloadNode::Event(EventDescriptor {
            name: name.to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        })
    }

    fn minimal_table(name: &str, pk: Vec<&str>) -> PayloadNode {
        let mut fields = BTreeMap::new();
        for k in &pk {
            fields.insert(k.to_string(), FieldType::Str);
        }
        fields.insert("extra".to_string(), FieldType::Str);
        PayloadNode::Table(TableDescriptor {
            name: name.to_string(),
            primary_key: pk.iter().map(|s| s.to_string()).collect(),
            schema: TableSchema {
                fields,
                optional_fields: vec![],
            },
            ttl_ms: None,
            mode: TableMode::Upsert,
            registered_at_version: 0,
        })
    }

    fn minimal_derivation(name: &str, upstreams: Vec<&str>) -> PayloadNode {
        let mut fields = BTreeMap::new();
        fields.insert("amount".to_string(), FieldType::F64);
        PayloadNode::Derivation(DerivationDescriptor {
            name: name.to_string(),
            output_kind: OutputKind::Event,
            upstreams: upstreams.iter().map(|s| s.to_string()).collect(),
            ops: vec![],
            schema: DerivedSchema {
                fields,
                optional_fields: vec![],
            },
            table_primary_key: None,
            registered_at_version: 0,
        })
    }

    fn assert_ok(payload: Vec<PayloadNode>) {
        match validate_payload(&empty_current(), payload) {
            Ok(_) => {}
            Err(errs) => panic!("expected Ok, got {} errors: {errs:#?}", errs.len()),
        }
    }

    fn assert_err_contains(
        payload: Vec<PayloadNode>,
        expected_code: ErrorCode,
        expected_path_contains: &str,
    ) {
        let errs = validate_payload(&empty_current(), payload).expect_err("expected Err");
        let found = errs
            .iter()
            .any(|e| e.code == expected_code && e.path.contains(expected_path_contains));
        assert!(
            found,
            "expected error code {expected_code:?} with path containing '{expected_path_contains}', got: {errs:#?}"
        );
    }

    fn assert_err_contains_with_current(
        current: &RegistryInner,
        payload: Vec<PayloadNode>,
        expected_code: ErrorCode,
        expected_path_contains: &str,
    ) {
        let errs = validate_payload(current, payload).expect_err("expected Err");
        let found = errs
            .iter()
            .any(|e| e.code == expected_code && e.path.contains(expected_path_contains));
        assert!(
            found,
            "expected error code {expected_code:?} with path containing '{expected_path_contains}', got: {errs:#?}"
        );
    }

    // ── Rule 1: Node uniqueness ───────────────────────────────────────────────

    #[test]
    fn rule1_pass_distinct_names() {
        assert_ok(vec![minimal_event("A"), minimal_table("B", vec!["extra"])]);
    }

    #[test]
    fn rule1_fail_duplicate_event() {
        assert_err_contains(
            vec![minimal_event("A"), minimal_event("A")],
            ErrorCode::NameDuplicate,
            "nodes[1].name",
        );
    }

    #[test]
    fn rule1_fail_duplicate_cross_kind() {
        assert_err_contains(
            vec![minimal_event("Foo"), minimal_table("Foo", vec!["extra"])],
            ErrorCode::NameDuplicate,
            "nodes[1].name",
        );
    }

    // ── Rule 2: Name validation ───────────────────────────────────────────────

    #[test]
    fn rule2_pass_valid_name() {
        assert_ok(vec![minimal_event("Transaction_1")]);
    }

    #[test]
    fn rule2_fail_empty_name() {
        // We can't construct a PayloadNode with "" name easily via minimal_event,
        // so build it directly.
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        let node = PayloadNode::Event(EventDescriptor {
            name: "".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        assert_err_contains(vec![node], ErrorCode::NameEmpty, "nodes[0].name");
    }

    #[test]
    fn rule2_fail_bad_pattern_leading_digit() {
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        let node = PayloadNode::Event(EventDescriptor {
            name: "1foo".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        assert_err_contains(vec![node], ErrorCode::NameBadPattern, "nodes[0].name");
    }

    #[test]
    fn rule2_fail_reserved_prefix() {
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        let node = PayloadNode::Event(EventDescriptor {
            name: "_beava_internal".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        assert_err_contains(vec![node], ErrorCode::NameReservedPrefix, "nodes[0].name");
    }

    #[test]
    fn rule2_fail_name_too_long() {
        let long_name = "a".repeat(129);
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        let node = PayloadNode::Event(EventDescriptor {
            name: long_name,
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        assert_err_contains(vec![node], ErrorCode::NameTooLong, "nodes[0].name");
    }

    // ── Rule 3: Event schema ──────────────────────────────────────────────────

    #[test]
    fn rule3_pass_valid_event() {
        assert_ok(vec![minimal_event("T")]);
    }

    #[test]
    fn rule3_fail_event_time_field_missing() {
        let mut fields = BTreeMap::new();
        fields.insert("x".to_string(), FieldType::F64);
        // event_time_field="ts" but no "ts" in schema
        let node = PayloadNode::Event(EventDescriptor {
            name: "T".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("ts".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        assert_err_contains(
            vec![node],
            ErrorCode::EventTimeFieldMissing,
            "nodes[0].schema.fields.ts",
        );
    }

    #[test]
    fn rule3_fail_event_time_field_wrong_type() {
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::Str); // wrong type
        fields.insert("x".to_string(), FieldType::F64);
        let node = PayloadNode::Event(EventDescriptor {
            name: "T".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        let errs = validate_payload(&empty_current(), vec![node]).expect_err("expected Err");
        let e = errs
            .iter()
            .find(|e| e.code == ErrorCode::EventTimeFieldWrongType)
            .unwrap();
        assert!(e.path.contains("schema.fields.event_time"));
        assert!(e.reason.to_lowercase().contains("i64") && e.reason.to_lowercase().contains("str"));
    }

    #[test]
    fn rule3_fail_event_schema_empty() {
        // Only event_time field — no non-event_time fields
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        let node = PayloadNode::Event(EventDescriptor {
            name: "T".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        assert_err_contains(
            vec![node],
            ErrorCode::EventSchemaEmpty,
            "nodes[0].schema.fields",
        );
    }

    #[test]
    fn rule3_pass_event_time_field_omitted() {
        // Event with NO event_time_field (server will stamp wall-clock on push)
        let current = RegistryInner::default();
        let payload = vec![PayloadNode::Event(EventDescriptor {
            name: "Heartbeat".to_string(),
            schema: EventSchema {
                fields: {
                    let mut m = BTreeMap::new();
                    m.insert("user_id".to_string(), FieldType::Str);
                    m
                },
                optional_fields: vec![],
            },
            event_time_field: None,
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        })];
        let result = validate_payload(&current, payload);
        assert!(
            result.is_ok(),
            "event without event_time_field should be valid"
        );
    }

    // ── Rule 4: Table schema ──────────────────────────────────────────────────

    #[test]
    fn rule4_pass_valid_table() {
        assert_ok(vec![minimal_table("M", vec!["extra"])]);
    }

    #[test]
    fn rule4_fail_primary_key_unknown_field() {
        let mut fields = BTreeMap::new();
        fields.insert("name".to_string(), FieldType::Str);
        let node = PayloadNode::Table(TableDescriptor {
            name: "M".to_string(),
            primary_key: vec!["id".to_string()], // "id" not in schema
            schema: TableSchema {
                fields,
                optional_fields: vec![],
            },
            ttl_ms: None,
            mode: TableMode::Upsert,
            registered_at_version: 0,
        });
        assert_err_contains(
            vec![node],
            ErrorCode::TablePrimaryKeyUnknownField,
            "nodes[0].primary_key[0]",
        );
    }

    #[test]
    fn rule4_fail_primary_key_empty() {
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), FieldType::Str);
        let node = PayloadNode::Table(TableDescriptor {
            name: "M".to_string(),
            primary_key: vec![], // empty
            schema: TableSchema {
                fields,
                optional_fields: vec![],
            },
            ttl_ms: None,
            mode: TableMode::Upsert,
            registered_at_version: 0,
        });
        assert_err_contains(
            vec![node],
            ErrorCode::TablePrimaryKeyEmpty,
            "nodes[0].primary_key",
        );
    }

    #[test]
    fn rule4_fail_primary_key_too_long() {
        let mut fields = BTreeMap::new();
        let pk: Vec<String> = (0..5).map(|i| format!("k{i}")).collect();
        for k in &pk {
            fields.insert(k.clone(), FieldType::Str);
        }
        let node = PayloadNode::Table(TableDescriptor {
            name: "M".to_string(),
            primary_key: pk,
            schema: TableSchema {
                fields,
                optional_fields: vec![],
            },
            ttl_ms: None,
            mode: TableMode::Upsert,
            registered_at_version: 0,
        });
        assert_err_contains(
            vec![node],
            ErrorCode::TablePrimaryKeyTooLong,
            "nodes[0].primary_key",
        );
    }

    // ── Rule 6: Derivation schema ─────────────────────────────────────────────

    #[test]
    fn rule6_pass_nonempty_schema() {
        assert_ok(vec![minimal_event("A"), minimal_derivation("D", vec!["A"])]);
    }

    #[test]
    fn rule6_fail_empty_schema() {
        let node = PayloadNode::Derivation(DerivationDescriptor {
            name: "D".to_string(),
            output_kind: OutputKind::Event,
            upstreams: vec![],
            ops: vec![],
            schema: DerivedSchema {
                fields: BTreeMap::new(), // empty
                optional_fields: vec![],
            },
            table_primary_key: None,
            registered_at_version: 0,
        });
        assert_err_contains(
            vec![node],
            ErrorCode::DerivationSchemaEmpty,
            "nodes[0].schema.fields",
        );
    }

    #[test]
    fn rule6b_pass_output_kind_table_with_primary_key() {
        let mut fields = BTreeMap::new();
        fields.insert("user".to_string(), FieldType::Str);
        fields.insert("count".to_string(), FieldType::I64);
        let node = PayloadNode::Derivation(DerivationDescriptor {
            name: "D".to_string(),
            output_kind: OutputKind::Table,
            upstreams: vec![],
            ops: vec![],
            schema: DerivedSchema {
                fields,
                optional_fields: vec![],
            },
            table_primary_key: Some(vec!["user".to_string()]),
            registered_at_version: 0,
        });
        // upstreams empty but that's a rule5 concern; test only rule6b here
        // We allow empty upstreams for this specific test by seeding them in current
        assert_ok(vec![node]);
    }

    #[test]
    fn rule6b_fail_output_kind_table_missing_primary_key() {
        let mut fields = BTreeMap::new();
        fields.insert("user".to_string(), FieldType::Str);
        let node = PayloadNode::Derivation(DerivationDescriptor {
            name: "D".to_string(),
            output_kind: OutputKind::Table,
            upstreams: vec![],
            ops: vec![],
            schema: DerivedSchema {
                fields,
                optional_fields: vec![],
            },
            table_primary_key: None, // missing
            registered_at_version: 0,
        });
        assert_err_contains(
            vec![node],
            ErrorCode::DerivationOutputKindTableMissingPrimaryKey,
            "nodes[0].table_primary_key",
        );
    }

    // ── Rule 9: Idempotency ───────────────────────────────────────────────────

    #[test]
    fn rule9_pass_valid_dedupe() {
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        fields.insert("request_id".to_string(), FieldType::Str);
        let node = PayloadNode::Event(EventDescriptor {
            name: "T".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: Some("request_id".to_string()),
            dedupe_window_ms: Some(1000),
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        assert_ok(vec![node]);
    }

    #[test]
    fn rule9_fail_dedupe_key_unknown_field() {
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        let node = PayloadNode::Event(EventDescriptor {
            name: "T".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: Some("missing".to_string()),
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        assert_err_contains(
            vec![node],
            ErrorCode::DedupeKeyUnknownField,
            "nodes[0].dedupe_key",
        );
    }

    #[test]
    fn rule9_fail_dedupe_window_zero() {
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        let node = PayloadNode::Event(EventDescriptor {
            name: "T".to_string(),
            schema: EventSchema {
                fields,
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: Some(0), // zero = non-positive
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        assert_err_contains(
            vec![node],
            ErrorCode::DedupeWindowNonPositive,
            "nodes[0].dedupe_window_ms",
        );
    }

    // ── Multi-error collection ────────────────────────────────────────────────

    #[test]
    fn collects_multiple_errors() {
        // 3 nodes each with independent errors:
        // node 0: bad name
        // node 1: bad event_time_field
        // node 2: empty derivation schema
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        let node0 = PayloadNode::Event(EventDescriptor {
            name: "1bad".to_string(), // bad pattern
            schema: EventSchema {
                fields: fields.clone(),
                optional_fields: vec![],
            },
            event_time_field: Some("event_time".to_string()),
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        let mut fields2 = BTreeMap::new();
        fields2.insert("x".to_string(), FieldType::F64);
        let node1 = PayloadNode::Event(EventDescriptor {
            name: "GoodName".to_string(),
            schema: EventSchema {
                fields: fields2,
                optional_fields: vec![],
            },
            event_time_field: Some("ts".to_string()), // missing
            dedupe_key: None,
            dedupe_window_ms: None,
            keep_events_for_ms: None,
            tolerate_delay_ms: None,
            registered_at_version: 0,
        });
        let node2 = PayloadNode::Derivation(DerivationDescriptor {
            name: "EmptyDeriv".to_string(),
            output_kind: OutputKind::Event,
            upstreams: vec![],
            ops: vec![],
            schema: DerivedSchema {
                fields: BTreeMap::new(),
                optional_fields: vec![],
            },
            table_primary_key: None,
            registered_at_version: 0,
        });
        let errs = validate_payload(&empty_current(), vec![node0, node1, node2])
            .expect_err("expected Err");
        assert!(
            errs.len() >= 3,
            "expected at least 3 errors (one per bad node), got {}: {errs:#?}",
            errs.len()
        );
        // Verify paths are distinct
        let paths: std::collections::HashSet<&str> = errs.iter().map(|e| e.path.as_str()).collect();
        assert!(paths.len() >= 3, "paths should be distinct, got: {paths:?}");
    }

    // ── Rule 5: Upstream resolution ───────────────────────────────────────────

    #[test]
    fn rule5_pass_upstream_in_payload() {
        assert_ok(vec![minimal_event("A"), minimal_derivation("D", vec!["A"])]);
    }

    #[test]
    fn rule5_pass_upstream_in_current_registry() {
        let mut current = RegistryInner::default();
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        current.events.insert(
            "A".to_string(),
            EventDescriptor {
                name: "A".to_string(),
                schema: EventSchema {
                    fields,
                    optional_fields: vec![],
                },
                event_time_field: Some("event_time".to_string()),
                dedupe_key: None,
                dedupe_window_ms: None,
                keep_events_for_ms: None,
                tolerate_delay_ms: None,
                registered_at_version: 1,
            },
        );
        current.version = 1;
        let result = validate_payload(&current, vec![minimal_derivation("D", vec!["A"])]);
        assert!(
            result.is_ok(),
            "upstream in current registry should pass: {result:?}"
        );
    }

    #[test]
    fn rule5_fail_upstream_unknown() {
        assert_err_contains(
            vec![minimal_derivation("D", vec!["Missing"])],
            ErrorCode::DerivationUpstreamUnknown,
            "nodes[0].upstreams[0]",
        );
    }

    #[test]
    fn rule5_fail_second_upstream_missing() {
        let mut current = RegistryInner::default();
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        current.events.insert(
            "KnownA".to_string(),
            EventDescriptor {
                name: "KnownA".to_string(),
                schema: EventSchema {
                    fields: fields.clone(),
                    optional_fields: vec![],
                },
                event_time_field: Some("event_time".to_string()),
                dedupe_key: None,
                dedupe_window_ms: None,
                keep_events_for_ms: None,
                tolerate_delay_ms: None,
                registered_at_version: 1,
            },
        );
        current.events.insert(
            "KnownB".to_string(),
            EventDescriptor {
                name: "KnownB".to_string(),
                schema: EventSchema {
                    fields,
                    optional_fields: vec![],
                },
                event_time_field: Some("event_time".to_string()),
                dedupe_key: None,
                dedupe_window_ms: None,
                keep_events_for_ms: None,
                tolerate_delay_ms: None,
                registered_at_version: 1,
            },
        );
        // D has 3 upstreams: KnownA (ok), KnownB (ok), Missing (bad)
        let errs = validate_payload(
            &current,
            vec![minimal_derivation("D", vec!["KnownA", "KnownB", "Missing"])],
        )
        .expect_err("expected Err");
        assert_eq!(errs.len(), 1, "only one error for one missing upstream");
        assert_eq!(errs[0].code, ErrorCode::DerivationUpstreamUnknown);
        assert!(errs[0].path.contains("upstreams[2]")); // index 2
    }

    // ── Rule 7: Acyclicity ────────────────────────────────────────────────────

    #[test]
    fn rule7_pass_linear_chain() {
        assert_ok(vec![
            minimal_event("A"),
            minimal_derivation("D1", vec!["A"]),
            minimal_derivation("D2", vec!["D1"]),
            minimal_derivation("D3", vec!["D2"]),
        ]);
    }

    #[test]
    fn rule7_fail_two_node_cycle() {
        let errs = validate_payload(
            &empty_current(),
            vec![
                minimal_derivation("D1", vec!["D2"]),
                minimal_derivation("D2", vec!["D1"]),
            ],
        )
        .expect_err("expected cycle error");
        let found = errs.iter().any(|e| e.code == ErrorCode::RegistrationCycle);
        assert!(found, "expected RegistrationCycle error, got: {errs:#?}");
        let cycle_err = errs
            .iter()
            .find(|e| e.code == ErrorCode::RegistrationCycle)
            .unwrap();
        assert!(
            cycle_err.reason.contains("D1") || cycle_err.reason.contains("D2"),
            "cycle reason should name the nodes: {}",
            cycle_err.reason
        );
    }

    #[test]
    fn rule7_fail_self_loop() {
        let errs = validate_payload(&empty_current(), vec![minimal_derivation("D1", vec!["D1"])])
            .expect_err("expected cycle error");
        assert!(errs.iter().any(|e| e.code == ErrorCode::RegistrationCycle));
    }

    #[test]
    fn rule7_fail_three_node_cycle() {
        // A → B → C → A (all in payload)
        let errs = validate_payload(
            &empty_current(),
            vec![
                minimal_derivation("A", vec!["C"]),
                minimal_derivation("B", vec!["A"]),
                minimal_derivation("C", vec!["B"]),
            ],
        )
        .expect_err("expected cycle");
        assert!(errs.iter().any(|e| e.code == ErrorCode::RegistrationCycle));
    }

    // ── Rule 8: Topological order ─────────────────────────────────────────────

    #[test]
    fn rule8_pass_correct_order() {
        assert_ok(vec![minimal_event("A"), minimal_derivation("D", vec!["A"])]);
    }

    #[test]
    fn rule8_fail_dependent_before_upstream() {
        let errs = validate_payload(
            &empty_current(),
            vec![
                // D appears at index 0, but its upstream A appears at index 1
                minimal_derivation("D", vec!["A"]),
                minimal_event("A"),
            ],
        )
        .expect_err("expected TopologicalOrderViolation");
        let found = errs
            .iter()
            .any(|e| e.code == ErrorCode::TopologicalOrderViolation);
        assert!(found, "expected TopologicalOrderViolation: {errs:#?}");
        let topo_err = errs
            .iter()
            .find(|e| e.code == ErrorCode::TopologicalOrderViolation)
            .unwrap();
        assert!(
            topo_err.reason.contains("A") && topo_err.reason.contains("nodes[1]"),
            "reason should mention 'A' and 'nodes[1]': {}",
            topo_err.reason
        );
    }

    // ── Rule 7+8 cooperate ────────────────────────────────────────────────────

    #[test]
    fn rule7_and_rule8_cooperate() {
        // D1 at index 0 depends on D2 (which is at index 1): both cycle AND topo violation
        let errs = validate_payload(
            &empty_current(),
            vec![
                minimal_derivation("D1", vec!["D2"]),
                minimal_derivation("D2", vec!["D1"]),
            ],
        )
        .expect_err("expected errors");
        let has_cycle = errs.iter().any(|e| e.code == ErrorCode::RegistrationCycle);
        let has_topo = errs
            .iter()
            .any(|e| e.code == ErrorCode::TopologicalOrderViolation);
        assert!(has_cycle, "expected RegistrationCycle");
        assert!(has_topo, "expected TopologicalOrderViolation");
    }

    #[test]
    fn multiple_upstreams_partial_missing() {
        // D with 3 upstreams: KnownA, KnownB, Missing
        let mut current = RegistryInner::default();
        let mut fields = BTreeMap::new();
        fields.insert("event_time".to_string(), FieldType::I64);
        fields.insert("x".to_string(), FieldType::F64);
        current.events.insert(
            "KnownA".to_string(),
            EventDescriptor {
                name: "KnownA".to_string(),
                schema: EventSchema {
                    fields: fields.clone(),
                    optional_fields: vec![],
                },
                event_time_field: Some("event_time".to_string()),
                dedupe_key: None,
                dedupe_window_ms: None,
                keep_events_for_ms: None,
                tolerate_delay_ms: None,
                registered_at_version: 1,
            },
        );
        current.events.insert(
            "KnownB".to_string(),
            EventDescriptor {
                name: "KnownB".to_string(),
                schema: EventSchema {
                    fields,
                    optional_fields: vec![],
                },
                event_time_field: Some("event_time".to_string()),
                dedupe_key: None,
                dedupe_window_ms: None,
                keep_events_for_ms: None,
                tolerate_delay_ms: None,
                registered_at_version: 1,
            },
        );
        assert_err_contains_with_current(
            &current,
            vec![minimal_derivation("D", vec!["KnownA", "KnownB", "Missing"])],
            ErrorCode::DerivationUpstreamUnknown,
            "upstreams[2]",
        );
    }
}
