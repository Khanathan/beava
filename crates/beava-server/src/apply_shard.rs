//! Single-writer apply shard — Phase 18 Plan 04.6 Task 4.6.1.
//!
//! `ApplyShard` wraps the shared `AppState` (behind `Arc<Mutex>`/parking_lot)
//! with a synchronous dispatch path for the hand-rolled mio event loop.
//!
//! # Design (D-1, D-2, D-3 from 18-04.6-PLAN.md)
//!
//! - D-1: keep `Arc<AppState>` (uncontended Mutex on apply thread).
//! - D-2: async-to-sync bridge REMOVED. `dispatch_wire_request_sync` is purely
//!   synchronous — no `.await`, no mpsc, no tokio dependency on the hot path.
//! - D-3: the legacy async `dispatch_wire_request` stays in `runtime_core_glue`
//!   for tests and admin callers. This file adds the NEW sync path.
//!
//! # Thread safety
//!
//! `ApplyShard` is `Send + Sync` because all interior mutability uses `Arc`.
//! In the serve loop, only the single apply thread calls `dispatch_wire_request_sync`;
//! the Mutex is uncontended → lock+unlock cost ~10–20 ns on macOS/Linux.

use crate::register::RegisterPayload;
use crate::runtime_core_glue::GlueResponse;
use crate::AppState;
use beava_core::row::Row;
use beava_runtime_core::wal_buffer::WalBufferRing;
use beava_runtime_core::wal_lsn::WalLsn;
use beava_runtime_core::wire_request::WireRequest;
use bytes::Bytes;
use std::sync::Arc;

/// Single-writer apply shard. Owns the apply path for the mio event loop.
///
/// Created once by `ServerV18::serve_with_dirs` and used exclusively from the
/// apply thread. The `AppState` Arc is shared with admin endpoints (read-only).
pub struct ApplyShard {
    state: Arc<AppState>,
    wal_ring: Arc<WalBufferRing>,
    #[allow(dead_code)]
    wal_lsn: Arc<WalLsn>,
}

impl ApplyShard {
    /// Create a new `ApplyShard`.
    ///
    /// `state` — shared application state (registry, aggregations, idem-cache).
    /// `wal_ring` — hand-rolled WAL ring buffer (lock-free append on apply thread).
    /// `wal_lsn` — four-watermark LSN tracker (committed/written/synced/acked).
    pub fn new(state: Arc<AppState>, wal_ring: Arc<WalBufferRing>, wal_lsn: Arc<WalLsn>) -> Self {
        Self {
            state,
            wal_ring,
            wal_lsn,
        }
    }

    /// Synchronous dispatch — the hot path for the apply thread.
    ///
    /// Processes one `WireRequest` and returns a `Vec<GlueResponse>` (almost
    /// always exactly one element; Vec used for future pipelining / batch
    /// expansion).
    ///
    /// No `.await`, no tokio, no mpsc. The WAL append uses
    /// `WalBufferRing::append` (lock-free memcpy + atomic position bump).
    pub fn dispatch_wire_request_sync(&self, req: WireRequest) -> Vec<GlueResponse> {
        vec![self.dispatch_one(req, None)]
    }

    /// Plan 18-04.8: dispatch with an optional pre-parsed Row.
    ///
    /// The IoPool worker thread (`read_and_parse_client` in server.rs) eagerly
    /// deserialises push-frame bodies into `Row` while it has the bytes hot in
    /// L1, then hands the result to the apply thread via the
    /// `MioClient.parsed_rows` side-channel. This method consumes that
    /// pre-parsed Row when present; the apply thread skips the redundant
    /// `from_slice::<Row>` call (saves ~190 ns per push at parallel=4/pd=64).
    ///
    /// `pre_parsed_row = None` is the fallback path — used when:
    /// - the request is not a push variant (Ping, Register, GetSingle …)
    /// - IoPool pre-parse failed (malformed body); apply path retries the
    ///   parse and emits `invalid_event` per the existing error path
    /// - test/legacy callers that don't run through the IoPool
    pub fn dispatch_wire_request_with_row(
        &self,
        req: WireRequest,
        pre_parsed_row: Option<Row>,
    ) -> Vec<GlueResponse> {
        vec![self.dispatch_one(req, pre_parsed_row)]
    }

    fn dispatch_one(&self, req: WireRequest, pre_parsed_row: Option<Row>) -> GlueResponse {
        match req {
            // ─── Ping ─────────────────────────────────────────────────────────
            WireRequest::Ping => GlueResponse::Pong {
                registry_version: self.state.dev_agg.registry.version() as u32,
            },

            // ─── Register ─────────────────────────────────────────────────────
            // Register is a cold path on the mio event loop. Routed here so the
            // apply thread owns all mutations. WAL RegistryBump durability is
            // deferred to Plan 18-06 (currently the legacy WalSink path handles
            // durability for /register; the mio path calls it without WAL for now).
            WireRequest::Register { payload } => {
                // Plan 12.6-04: JSON-prelude shim — intercept removed ops
                // (`{"op":"join"}` / `{"op":"union"}`) BEFORE strict
                // RegisterPayload deserialize so the rejection path is
                // independent of whether the OpNode variants still exist.
                // Per CONTEXT.md §Implementation Decisions / Bucket 5,
                // emits structured error codes (feature_removed_no_joins_v0 /
                // feature_removed_no_unions_v0) instead of opaque serde
                // "unknown variant" errors after the variant deletion.
                if let Ok(json_value) = serde_json::from_slice::<serde_json::Value>(&payload) {
                    if let Some(removed) =
                        beava_core::register_validate::pre_check_removed_ops(&json_value)
                    {
                        let body = serde_json::json!({
                            "error": {
                                "code": removed.code,
                                "path": removed.path,
                                "reason": removed.reason,
                            },
                            "registry_version": self.state.dev_agg.registry.version(),
                        });
                        return GlueResponse::Register {
                            http_status: 400,
                            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
                            tcp_op: beava_core::wire::OP_ERROR_RESPONSE,
                        };
                    }
                    // Plan 12.6-06: legacy event-time JSON-key strict-deny shim.
                    // Same JSON-prelude posture as Plan 04's removed-ops check
                    // — runs BEFORE strict RegisterPayload deserialize so legacy
                    // `event_time_field` / `tolerate_delay_ms` keys raise a
                    // structured error code (unknown_field_event_time_v0 /
                    // unknown_field_tolerate_delay_v0) per D-03 hard-rip.
                    if let Some(removed) =
                        beava_core::register_validate::pre_check_legacy_event_time_keys(&json_value)
                    {
                        let body = serde_json::json!({
                            "error": {
                                "code": removed.code,
                                "path": removed.path,
                                "reason": removed.reason,
                            },
                            "registry_version": self.state.dev_agg.registry.version(),
                        });
                        return GlueResponse::Register {
                            http_status: 400,
                            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
                            tcp_op: beava_core::wire::OP_ERROR_RESPONSE,
                        };
                    }
                    // Plan 12.7-01: events-only register-time enforcement.
                    // Third JSON-prelude shim — sits alongside Plan 12.6-04
                    // (joins/unions) and Plan 12.6-06 (event-time keys). Rejects
                    // payloads with `{"kind": "table", ...}` (or any other
                    // non-event/non-derivation kind) at the JSON layer BEFORE
                    // strict RegisterPayload deserialize, so the rejection path
                    // is independent of whether `OpNode::Table*` /
                    // `PayloadNode::Table` variants still exist in the enum.
                    // Per CONTEXT D-02 the structured error code is
                    // `unsupported_node_kind` (forward-looking) — v0 is the
                    // FIRST public release; tables were never available, so a
                    // retrospective code naming would confuse fresh users.
                    if let Some(removed) =
                        beava_core::register_validate::pre_check_unsupported_node_kind(&json_value)
                    {
                        let body = serde_json::json!({
                            "error": {
                                "code": removed.code,
                                "path": removed.path,
                                "reason": removed.reason,
                            },
                            "registry_version": self.state.dev_agg.registry.version(),
                        });
                        return GlueResponse::Register {
                            http_status: 400,
                            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
                            tcp_op: beava_core::wire::OP_ERROR_RESPONSE,
                        };
                    }
                    // Plan 12.8-01: 4th JSON-prelude shim — events-only memory
                    // governance. Rejects derivation payloads with windowless
                    // aggregation ops whose lifetime bound is Unbounded per
                    // `lifetime_bound_for_op_str`. Per CONTEXT D-03 hard-reject
                    // posture; per CONTEXT D-02 forward-looking framing
                    // (`unbounded_op_in_lifetime_mode`).
                    //
                    // **Wave 3 (Plan 12.8-06): env-gate default flipped OFF → ON.**
                    // Now defaults to ENABLED — every `/register` validates
                    // declarations against the per-op classification table from
                    // Plan 12.8-04. Explicit escape hatch:
                    // `BEAVA_MEMORY_GOV_ENFORCE=0` (production env) or
                    // `.memory_governance_enforce(false)` (test builder)
                    // disables enforcement.
                    //
                    // Phase 13.5.3 (D-04 mitigate): the per-call env-var read
                    // moved to a struct field on AppState resolved at boot
                    // (`ServerV18Config::from_env() -> AppState.memory_governance_enforce`).
                    // Removes a process-global env read from the cold register
                    // path that leaked enforcement state across parallel
                    // TestServers.
                    if self.state.memory_governance_enforce {
                        if let Some(removed) =
                            beava_core::register_validate::pre_check_unbounded_op_in_lifetime_mode(
                                &json_value,
                            )
                        {
                            let body = serde_json::json!({
                                "error": {
                                    "code": removed.code,
                                    "path": removed.path,
                                    "reason": removed.reason,
                                },
                                "registry_version": self.state.dev_agg.registry.version(),
                            });
                            return GlueResponse::Register {
                                http_status: 400,
                                body: bytes::Bytes::from(
                                    serde_json::to_vec(&body).unwrap_or_default(),
                                ),
                                tcp_op: beava_core::wire::OP_ERROR_RESPONSE,
                            };
                        }
                    }
                }
                // Plan 12.6-01: parse + dispatch on the apply thread, then
                // funnel the outcome through `register_outcome_to_glue`
                // so wire bytes match the legacy axum
                // `register::map_outcome_to_http` output exactly.
                let reg_payload: RegisterPayload = match serde_json::from_slice(&payload) {
                    Ok(p) => p,
                    Err(e) => {
                        let (path, reason) = crate::register::format_serde_error_public(&e);
                        let body = serde_json::json!({
                            "error": {
                                "code": "invalid_registration",
                                "path": path,
                                "reason": reason,
                            },
                            "registry_version": self.state.dev_agg.registry.version(),
                        });
                        return GlueResponse::Register {
                            http_status: 400,
                            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
                            tcp_op: beava_core::wire::OP_ERROR_RESPONSE,
                        };
                    }
                };

                // ─── Phase 13.4 Plan 06 — D-01 force=true + dry_run=true ──
                //
                // After the JSON-prelude shims (12.6-04 / 12.6-06 / 12.7-01 /
                // 12.8-01) and AFTER strict serde, but BEFORE the legacy
                // `compute_diff` conflict path: classify the diff under the
                // D-01 categorized-lists schema. The dry_run branch fires
                // FIRST (per CONTEXT scope item #6: "returns JSON without
                // applying"). The force gate is checked second; if
                // destructive entries exist without `force=true`, return
                // 409 + force_required. If `force=true` is set on a
                // destructive payload, eagerly remove the conflicting
                // descriptors so the subsequent `execute_register_with_wal`
                // treats the payload as additive (registry_version bumps,
                // WAL records the change).
                let (force, dry_run) = match serde_json::from_slice::<serde_json::Value>(&payload) {
                    Ok(v) => (
                        v.get("force").and_then(|x| x.as_bool()).unwrap_or(false),
                        v.get("dry_run").and_then(|x| x.as_bool()).unwrap_or(false),
                    ),
                    Err(_) => (false, false),
                };
                let prev_snapshot = self.state.dev_agg.registry.snapshot();
                let diff = beava_core::register_validate::classify_register_diff(
                    &prev_snapshot,
                    &reg_payload.nodes,
                );

                if dry_run {
                    // Phase 13.4 Plan 06 Task 6.d: dry_run branch — return 200
                    // + diff JSON without applying. The branch fires BEFORE
                    // the force gate so `dry_run=true, force=true` is treated
                    // as dry_run (matches Phase 13.5 SDK contract).
                    let body = serde_json::json!({
                        "diff": diff,
                        "would_apply": false,
                    });
                    return GlueResponse::Register {
                        http_status: 200,
                        body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
                        tcp_op: beava_core::wire::OP_REGISTER,
                    };
                }

                if let Err(force_err) =
                    beava_core::register_validate::register_check_force_required(&diff, force)
                {
                    let body = serde_json::json!({
                        "error": force_err,
                        "registry_version": self.state.dev_agg.registry.version(),
                    });
                    return GlueResponse::Register {
                        http_status: 409,
                        body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap_or_default()),
                        tcp_op: beava_core::wire::OP_ERROR_RESPONSE,
                    };
                }

                // force=true on a destructive payload: pre-remove the
                // conflicting descriptors so the apply path treats them as
                // new. Per Plan 06's must_haves: "force=true bumps
                // registry_version + WAL records the change". The
                // execute_register_with_wal path emits a RegistryBump
                // record that captures the new payload's nodes.
                if force && !diff.destructive.is_empty() {
                    let mut to_remove: Vec<String> = Vec::new();
                    for entry in &diff.destructive {
                        match entry {
                            beava_core::registry_diff::DiffEntry::Rename { from, .. } => {
                                to_remove.push(from.clone());
                            }
                            beava_core::registry_diff::DiffEntry::TypeChange { field, .. } => {
                                // field is "<descriptor>.<field>"; remove the
                                // owning descriptor so it can be re-installed.
                                if let Some((descriptor, _)) = field.split_once('.') {
                                    to_remove.push(descriptor.to_string());
                                }
                            }
                            beava_core::registry_diff::DiffEntry::OpRemoval { table, .. }
                            | beava_core::registry_diff::DiffEntry::AggRemoval { table, .. }
                            | beava_core::registry_diff::DiffEntry::KeyColsChange {
                                table, ..
                            } => {
                                to_remove.push(table.clone());
                            }
                            beava_core::registry_diff::DiffEntry::WindowChange { agg, .. } => {
                                if let Some((descriptor, _)) = agg.split_once('.') {
                                    to_remove.push(descriptor.to_string());
                                }
                            }
                            // Additive variants (sentinel-only when destructive list mixed)
                            // shouldn't appear here; the destructive list is destructive-only.
                            _ => {}
                        }
                    }
                    to_remove.sort();
                    to_remove.dedup();
                    self.state
                        .dev_agg
                        .registry
                        .force_remove_descriptors(&to_remove);
                }

                // Register is a cold path on the mio event loop.
                // Delegate to the async WAL-backed register function using a
                // temporary single-threaded tokio runtime (register is never
                // on the hot path; it's a one-shot admin operation).
                let state_clone = Arc::clone(&self.state);
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("temp tokio rt for register");
                let outcome = rt.block_on(crate::register::execute_register_with_wal(
                    &state_clone.dev_agg.registry,
                    reg_payload,
                    &state_clone.wal_sink,
                ));
                // Plan 18-16 Task 16.2: grow `state_tables` so apply hot path
                // can index by `desc.agg_id` without bounds issues. Cold path —
                // register is rare, so the lock + resize is fine here.
                let new_next_agg_id = state_clone.dev_agg.registry.next_agg_id() as usize;
                if new_next_agg_id > 0 {
                    let mut tables = state_clone.dev_agg.state_tables.lock();
                    beava_core::agg_state_table::ensure_capacity_for(&mut tables, new_next_agg_id);
                }
                let (http_status, body, tcp_op) =
                    crate::register::register_outcome_to_glue(outcome);
                GlueResponse::Register {
                    http_status,
                    body,
                    tcp_op,
                }
            }

            // ─── TCP push / HTTP push (periodic mode) ─────────────────────────
            WireRequest::TcpPush {
                event_name,
                body,
                body_format,
            }
            | WireRequest::HttpPush {
                event_name,
                body,
                body_format,
            } => self.dispatch_push_sync(&event_name, body, body_format, pre_parsed_row),

            // ─── HTTP push-sync (per-event / acks=all mode) ───────────────────
            // For the mio path we still do sync WAL append; the
            // wait-for-synced blocking call would stall the apply thread.
            // Per plan D-2 the full per-event path is a future refinement.
            // For now, treat identically to periodic push.
            WireRequest::HttpPushSync {
                event_name,
                body,
                body_format,
            } => self.dispatch_push_sync(&event_name, body, body_format, pre_parsed_row),

            WireRequest::HttpPushBatch {
                event_name,
                body,
                body_format,
            } => {
                // Batch push: treat as single push for scaffold correctness.
                self.dispatch_push_sync(&event_name, body, body_format, pre_parsed_row)
            }

            // ─── GET single ───────────────────────────────────────────────────
            WireRequest::HttpGetSingle { feature, key } => {
                fn trace_apply_enabled() -> bool {
                    use std::sync::OnceLock;
                    static FLAG: OnceLock<bool> = OnceLock::new();
                    *FLAG.get_or_init(|| {
                        std::env::var("BEAVA_TRACE_APPLY_TIMING").ok().as_deref() == Some("1")
                    })
                }
                let trace_apply = trace_apply_enabled();
                let t0 = if trace_apply {
                    Some(std::time::Instant::now())
                } else {
                    None
                };
                let resp = crate::runtime_core_glue::dispatch_get_single_sync(
                    &self.state,
                    &feature,
                    &key,
                    beava_core::wire::CT_JSON,
                );
                if let Some(t0_inst) = t0 {
                    let total = t0_inst.elapsed();
                    eprintln!(
                        "TRACE_APPLY ns get: feature_len={} key_len={} TOTAL={}",
                        feature.len(),
                        key.len(),
                        total.as_nanos()
                    );
                }
                resp
            }

            // ─── POST /get — Phase 13.4.1 D-01 verb-style single-row GET ──────
            // Body parses to `{"table": "<name>", "key": "<id>",
            // "features"?: [...]}`. Three-step ladder per PATTERNS.md
            // cross-cutting §1: (a) try strict-deserialise the new shape;
            // (b) on parse failure, attempt legacy-shape detection (`{keys,
            // features}` 2D-cell or `{feature, key}` single-feature) and
            // return D-05 `UnsupportedRequestShape`; (c) on no-match, fall
            // through to `InternalError` so genuinely-malformed JSON still
            // surfaces clearly.
            WireRequest::HttpGet { body } => {
                use beava_core::wire::CT_JSON;
                #[derive(serde::Deserialize)]
                struct HttpGetReq {
                    table: String,
                    key: String,
                    #[serde(default)]
                    features: Option<Vec<String>>,
                }
                match serde_json::from_slice::<HttpGetReq>(&body) {
                    Ok(req) => crate::runtime_core_glue::dispatch_get_single_verb_style_sync(
                        &self.state,
                        &req.table,
                        &req.key,
                        req.features.as_deref(),
                        CT_JSON,
                    ),
                    Err(parse_err) => {
                        // Plan 13.4.1 D-05 — attempt legacy-shape detection.
                        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&body) {
                            let is_legacy_2d =
                                value.get("keys").map(|v| v.is_array()).unwrap_or(false)
                                    && value.get("features").map(|v| v.is_array()).unwrap_or(false);
                            let is_legacy_single =
                                value.get("feature").map(|v| v.is_string()).unwrap_or(false)
                                    && value.get("key").map(|v| v.is_string()).unwrap_or(false);
                            if is_legacy_2d || is_legacy_single {
                                let hint = "POST /get expects {table, key, features?}; received legacy {keys, features} shape — see docs/http-api.md#post-get".to_string();
                                return GlueResponse::UnsupportedRequestShape { hint };
                            }
                        }
                        // Truly malformed JSON — surface the parse error.
                        GlueResponse::InternalError {
                            reason: parse_err.to_string(),
                        }
                    }
                }
            }

            // ─── TCP OP_GET — Phase 13.4.1 D-01 verb-style single-row GET ─────
            // Same body shape as POST /get; codec selected by the frame's
            // content_type byte (CT_JSON or CT_MSGPACK). Three-step ladder
            // mirrors the HTTP branch above; legacy-shape detection runs on
            // CT_JSON only (CT_MSGPACK clients are post-Plan 12-09 and
            // already verb-style aware).
            WireRequest::TcpGet { body, body_format } => {
                use beava_core::wire::{CT_JSON, CT_MSGPACK};
                #[derive(serde::Deserialize)]
                struct TcpGetReq {
                    table: String,
                    key: String,
                    #[serde(default)]
                    features: Option<Vec<String>>,
                }
                let parse_result: Result<TcpGetReq, String> = match body_format {
                    CT_JSON => serde_json::from_slice(&body).map_err(|e| e.to_string()),
                    CT_MSGPACK => rmp_serde::from_slice(&body).map_err(|e| e.to_string()),
                    other => {
                        return GlueResponse::InternalError {
                            reason: format!("unsupported content_type: {other:#04x}"),
                        };
                    }
                };
                match parse_result {
                    Ok(req) => crate::runtime_core_glue::dispatch_get_single_verb_style_sync(
                        &self.state,
                        &req.table,
                        &req.key,
                        req.features.as_deref(),
                        body_format,
                    ),
                    Err(parse_err) => {
                        // Plan 13.4.1 D-05 — legacy-shape detection on JSON only.
                        if body_format == CT_JSON {
                            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&body) {
                                let is_legacy_2d = value
                                    .get("keys")
                                    .map(|v| v.is_array())
                                    .unwrap_or(false)
                                    && value.get("features").map(|v| v.is_array()).unwrap_or(false);
                                let is_legacy_single =
                                    value.get("feature").map(|v| v.is_string()).unwrap_or(false)
                                        && value.get("key").map(|v| v.is_string()).unwrap_or(false);
                                if is_legacy_2d || is_legacy_single {
                                    let hint = "OP_GET expects {table, key, features?}; received legacy {keys, features} or {feature, key} shape — see docs/wire-spec.md#op_get-0x0020".to_string();
                                    return GlueResponse::UnsupportedRequestShape { hint };
                                }
                            }
                        }
                        GlueResponse::InternalError { reason: parse_err }
                    }
                }
            }

            // ─── TCP /mget (single feature, multi key) — Plan 12-07 Wave 3, Plan 12-09 Wave 4 ───
            // Body parses to {"feature": "<name>", "keys": [...]}. Materialise as
            // a batch with a single-feature list and reuse dispatch_get_batch_sync.
            //
            // TODO(12-10+): pass keys/features directly into a batch helper to skip
            // the re-serialise step on this path. The current form mirrors the
            // Plan 12-07 shape; this is suboptimal but correct.
            WireRequest::TcpMGet { body, body_format } => {
                use beava_core::wire::{CT_JSON, CT_MSGPACK};
                #[derive(serde::Deserialize)]
                struct TcpMGetReq {
                    feature: String,
                    keys: Vec<String>,
                }
                let req: TcpMGetReq = match body_format {
                    CT_JSON => match serde_json::from_slice(&body) {
                        Ok(r) => r,
                        Err(e) => {
                            return GlueResponse::InternalError {
                                reason: e.to_string(),
                            };
                        }
                    },
                    CT_MSGPACK => match rmp_serde::from_slice(&body) {
                        Ok(r) => r,
                        Err(e) => {
                            return GlueResponse::InternalError {
                                reason: e.to_string(),
                            };
                        }
                    },
                    other => {
                        return GlueResponse::InternalError {
                            reason: format!("unsupported content_type: {other:#04x}"),
                        };
                    }
                };
                let batch_body = serde_json::json!({
                    "keys": req.keys,
                    "features": [req.feature],
                });
                let batch_bytes = match body_format {
                    CT_JSON => match serde_json::to_vec(&batch_body) {
                        Ok(b) => bytes::Bytes::from(b),
                        Err(e) => {
                            return GlueResponse::InternalError {
                                reason: e.to_string(),
                            };
                        }
                    },
                    CT_MSGPACK => match rmp_serde::to_vec_named(&batch_body) {
                        Ok(b) => bytes::Bytes::from(b),
                        Err(e) => {
                            return GlueResponse::InternalError {
                                reason: e.to_string(),
                            };
                        }
                    },
                    _ => unreachable!("validated above"),
                };
                crate::runtime_core_glue::dispatch_get_batch_sync(
                    &self.state,
                    &batch_bytes,
                    body_format,
                )
            }

            // ─── TCP /get-multi (multi feature, multi key) — Plan 12-07 Wave 3, Plan 12-09 Wave 4 ──
            // Body shape mirrors HTTP /get: {"keys": [...], "features": [...]}.
            // Body_format selects the parse codec inside dispatch_get_batch_sync
            // — no re-serialize needed here.
            WireRequest::TcpGetMulti { body, body_format } => {
                crate::runtime_core_glue::dispatch_get_batch_sync(&self.state, &body, body_format)
            }

            // ─── OP_BATCH_GET / POST /batch_get — Plan 13.4-03 ─────────────────
            // Heterogeneous batched read. Walks the request list, calls per-entity
            // feature lookup for each (table, entity_id) tuple, aggregates results
            // with partial-failure semantics (unknown_table becomes a per-tuple
            // error inside `results` rather than a whole-batch 4xx).
            //
            // Per ADR-003, the empty-string entity_id is the global-table sentinel
            // and forwards as-is to `parse_entity_key`. Plan 13.4-09 wires the
            // register-time `key_cols: []` acceptance; until then, global-table
            // pipelines fail at registration (the test gating that case is
            // `#[ignore]`d in `tests/phase13_4_op_batch_get.rs`).
            WireRequest::TcpBatchGet { body, body_format } => {
                dispatch_batch_get_sync(&self.state, &body, body_format)
            }
            WireRequest::HttpBatchGet { body } => {
                dispatch_batch_get_sync(&self.state, &body, beava_core::wire::CT_JSON)
            }

            // ─── Plan 13.4-08 (D-03 USER-LOCKED) — OP_RESET / POST /reset ─────
            // Both transports route through a single dispatch_reset_sync.
            // The `body` payload is `{}` by spec but is intentionally NOT
            // parsed — reset is a no-arg operation; any body is tolerated.
            WireRequest::TcpReset { .. } | WireRequest::HttpReset { .. } => {
                dispatch_reset_sync(&self.state)
            }

            // Plan 12.6-14: 415 Unsupported Media Type — POST request with
            // wrong/missing Content-Type. Body matches legacy axum's
            // register handler `RegisterErrorBody` shape.
            WireRequest::HttpUnsupportedMediaType { received, path } => {
                GlueResponse::HttpUnsupportedMediaType { received, path }
            }

            // ─── /health (Plan 12-07 Wave 5.5) ────────────────────────────────
            // Inline shim — no AppState consult, no WAL recovery dependency.
            // read_bench.py polls /health with a 0.5s timeout per attempt and
            // a 10s total budget; gating on apply-thread responsiveness would
            // race against startup recovery on cold replicas. Returning OK
            // unconditionally matches the Kubernetes liveness contract:
            // "yes the process is up and accepting connections".
            WireRequest::HttpHealth => GlueResponse::HealthOk,

            // ─── POST /ping (Plan 13.4-04 — verb-style liveness) ──────────────
            // HTTP mirror of TCP `OP_PING (0x0000)`. Returns `200 {"status":"ok"}`
            // (same wire shape as `/health`) so verb-style fixtures can poll
            // either endpoint. No AppState consult; no apply-thread roundtrip
            // for the same reason `/health` is inline-shimmed.
            WireRequest::HttpPing => GlueResponse::HealthOk,

            // Plan 12.6-01: data-plane /ready and /registry shims for
            // back-compat with TestServer-using tests. /ready is a
            // constant-body shim (mirrors admin sidecar's /ready). /registry
            // serializes the live registry snapshot via
            // `registry_debug::build_registry_dump`.
            WireRequest::HttpReady => GlueResponse::ReadyOk,
            WireRequest::HttpRegistry => {
                // Plan 12.6-14: dev_endpoints gating — 404 unless flag set.
                if !self.state.dev_endpoints_enabled() {
                    GlueResponse::HttpRouteNotFound {
                        path: "/registry".to_owned(),
                    }
                } else {
                    let dump =
                        crate::registry_debug::build_registry_dump(&self.state.dev_agg.registry);
                    let body = serde_json::to_vec(&dump).unwrap_or_default();
                    GlueResponse::RegistrySnapshot {
                        body: bytes::Bytes::from(body),
                    }
                }
            }
            // Plan 12.6-01: route-level errors (unknown path / wrong method)
            // surface as 404 / 405 — same shape as the legacy axum
            // fallback. ParseError (wire-level decode failure) and Unknown
            // op (TCP) still map to 501 Unsupported below.
            WireRequest::HttpNotFound { path } => GlueResponse::HttpRouteNotFound { path },
            WireRequest::HttpMethodNotAllowed { method, path } => {
                GlueResponse::HttpMethodNotAllowed { method, path }
            }

            // Plan 12.6-15: known-but-deferred opcodes get rich op_not_implemented
            // error frames; truly unknown ones get unknown_op. Both keep the
            // connection open (criterion 5).
            WireRequest::Unknown { op } => {
                use beava_core::wire::OP_PUSH_SYNC;
                if op == OP_PUSH_SYNC {
                    GlueResponse::TcpError {
                        code: "op_not_implemented",
                        message: format!(
                            "opcode {op:#06x} (push_sync) is reserved for Phase 12 and not yet implemented",
                        ),
                        extras: serde_json::json!({"op": op}),
                    }
                } else {
                    GlueResponse::TcpError {
                        code: "unknown_op",
                        message: format!("opcode {op:#06x} is not recognised by this server"),
                        extras: serde_json::json!({"op": op}),
                    }
                }
            }
            // Plan 12.6-15: ParseError now distinguishes content-type rejections
            // (carrying the special prefix) from generic parse errors. Anything
            // matching the unsupported-content-type prefix is surfaced as a
            // dedicated TcpError so the criterion-bonus_msgpack test passes
            // (`error.code == "unsupported_content_type"`).
            //
            // Plan 13.4-04: verb-style `POST /push` body-parse failures
            // (`missing_event_name_in_body`, `invalid_json_body`) emitted by
            // `http_listener::parse_verb_push` surface as `PushError` with the
            // reason copied verbatim into the static error code so the HTTP
            // encoder returns 400 + `{"error":{"code":"missing_event_name_in_body"}}`.
            WireRequest::ParseError { reason } => {
                if reason.starts_with("unsupported content_type") {
                    GlueResponse::TcpError {
                        code: "unsupported_content_type",
                        message: reason,
                        extras: serde_json::json!({}),
                    }
                } else if reason == "missing_event_name_in_body" {
                    GlueResponse::PushError {
                        code: "missing_event_name_in_body",
                        registry_version: self.state.dev_agg.registry.version() as u32,
                    }
                } else if reason == "invalid_json_body" {
                    GlueResponse::PushError {
                        code: "invalid_json_body",
                        registry_version: self.state.dev_agg.registry.version() as u32,
                    }
                } else {
                    GlueResponse::Unsupported
                }
            }
        }
    }

    /// Synchronous push — the hot path.
    ///
    /// Plan 18-10 D-3: parse body directly into beava_core::row::Row via
    /// the `Row::Deserialize` impl (Plan 18-09 Task 9.3, rewritten in Plan
    /// 18-10 to walk MapAccess directly without serde_json::Value intermediate).
    /// No JsonValue allocation on the hot path.
    ///
    /// 1. Parse body → Row (sonic_rs::from_slice or rmp_serde::from_slice).
    /// 2. Look up event descriptor.
    /// 3. Schema validate.
    /// 4. Dedupe lookup.
    /// 5. Serialize WAL payload (body bytes pass through unchanged).
    /// 6. WalBufferRing::append.
    /// 7. apply_event_to_aggregations.
    /// 8. Build and return GlueResponse.
    fn dispatch_push_sync(
        &self,
        event_name: &str,
        body: Bytes,
        body_format: u8,
        pre_parsed_row: Option<Row>,
    ) -> GlueResponse {
        use beava_core::agg_apply::apply_event_to_aggregations;
        use beava_core::defaults::DEFAULT_DEDUPE_WINDOW_MS;
        use beava_core::wire::CT_MSGPACK;
        use std::sync::atomic::Ordering;
        use std::time::{Instant, SystemTime, UNIX_EPOCH};

        // SPIKE: per-stage apply-path timing (env-gated, success-path-only).
        // OnceLock cache: HashMap-on-env-vars lookup happens once per process,
        // not once per push. Saves ~200-500 ns per event when trace is OFF
        // (the common production case).
        fn trace_apply_enabled() -> bool {
            use std::sync::OnceLock;
            static FLAG: OnceLock<bool> = OnceLock::new();
            *FLAG.get_or_init(|| {
                std::env::var("BEAVA_TRACE_APPLY_TIMING").ok().as_deref() == Some("1")
            })
        }
        let trace_apply = trace_apply_enabled();
        let t0 = if trace_apply {
            Some(Instant::now())
        } else {
            None
        };
        // SPIKE: inter-event gap (time since previous push completed on this thread).
        thread_local! {
            static LAST_PUSH_END: std::cell::Cell<Option<std::time::Instant>> = const { std::cell::Cell::new(None) };
        }
        let gap = if trace_apply {
            LAST_PUSH_END.with(|cell| cell.take()).map(|t| t.elapsed())
        } else {
            None
        };

        let registry_version = self.state.dev_agg.registry.version() as u32;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // 1. Plan 18-04.8: prefer the pre-parsed Row from the IoPool worker
        //    when present. Falls back to body→Row inline (parse on apply
        //    thread) when the IoPool failed to pre-parse OR when the caller
        //    didn't use the IoPool path (tests, legacy admin).
        //
        //    Plan 18-10 D-3: Row::Deserialize walks MapAccess + visit_*
        //    primitives without allocating an intermediate JsonValue tree.
        //    Both serde_json and rmp_serde drive the same visitor.
        let row: Row = match pre_parsed_row {
            Some(r) => r,
            None => {
                if body_format == CT_MSGPACK {
                    match rmp_serde::from_slice::<Row>(&body) {
                        Ok(r) => r,
                        Err(_) => {
                            return GlueResponse::PushError {
                                code: "invalid_event",
                                registry_version,
                            };
                        }
                    }
                } else {
                    match sonic_rs::from_slice::<Row>(&body) {
                        Ok(r) => r,
                        Err(_) => {
                            return GlueResponse::PushError {
                                code: "invalid_event",
                                registry_version,
                            };
                        }
                    }
                }
            }
        };
        let t_parse = t0.map(|t| t.elapsed());

        // 2. Lookup event descriptor.
        // Plan 18-11 D-6: Arc-backed lookup — refcount bump only, no clone.
        let descriptor = match self.state.dev_agg.registry.get_event_descriptor(event_name) {
            Some(d) => d,
            None => {
                return GlueResponse::PushError {
                    code: "event_not_found",
                    registry_version,
                };
            }
        };
        let t_lookup = t0.map(|t| t.elapsed());

        // 3. Plan 12.6-06: strict-deny on legacy `event_time` / `event_time_ms`
        //    fields in the push body. Per CONTEXT D-03 hard-rip: the wire
        //    schema permanently does not accept event-time data; clients
        //    sending the legacy field get a structured 400 with code
        //    `unknown_field_event_time_v0` rather than silent-ignore.
        //
        //    The check runs against the *parsed Row* — Beava's Row is a
        //    generic key-value map (events have arbitrary user-defined
        //    schemas), so deny_unknown_fields on Row itself is wrong. The
        //    correct boundary is the EventDescriptor: any Row field absent
        //    from descriptor.schema.fields is a stale-fixture / forbidden
        //    field.
        for (field_name, _) in row.iter() {
            if !descriptor.schema.fields.contains_key(field_name)
                && !descriptor
                    .schema
                    .optional_fields
                    .iter()
                    .any(|f| f == field_name)
            {
                let code: &'static str =
                    if field_name == "event_time" || field_name == "event_time_ms" {
                        "unknown_field_event_time_v0"
                    } else {
                        "unknown_field_v0"
                    };
                return GlueResponse::PushError {
                    code,
                    registry_version,
                };
            }
        }

        // 4. Schema validate against Row.fields directly.
        if !validate_row_against_descriptor(&descriptor, &row) {
            return GlueResponse::PushError {
                code: "invalid_event",
                registry_version,
            };
        }

        // 4. Dedupe lookup against Row.fields.
        let dedupe_str = descriptor
            .dedupe_key
            .as_deref()
            .and_then(|k| extract_dedupe_str_from_row(&row, k));

        if let (Some(_), Some(ref key_str)) = (descriptor.dedupe_key.as_ref(), &dedupe_str) {
            if let Some((cached_ack_lsn, cached_body)) = self
                .state
                .idem_cache
                .get_with_ack_lsn(event_name, key_str, now_ms)
            {
                // Plan 12.6-15: byte-identical replay (HTTP success
                // criterion #2). HTTP encoder emits `cached_body` verbatim;
                // TCP encoder uses `ack_lsn` to build a `{ack_lsn,
                // idempotent_replay: true, …}` body (TCP has no replay
                // header — the body flag IS the discriminator).
                return GlueResponse::PushReplay {
                    registry_version,
                    ack_lsn: Some(cached_ack_lsn),
                    cached_body: Some(cached_body),
                };
            }
        }

        // 5. Plan 12.6-05 Path X: time source = server wall-clock at dispatch.
        //
        // Per `project_redis_shaped_no_event_time_ever` and CONTEXT D-03,
        // the apply path no longer reads `event_time` from the row body.
        // `now_ms` (computed above as the wall-clock at this dispatch) is
        // the single time source threaded into the operator surface — both
        // for windowed bucketing and for `query_time_ms.fetch_max`
        // below (which Plan 12.6-06 will rename to a now-aligned name once
        // the `event_time_ms` wire field + EventDescriptor.event_time_field
        // are deleted).
        //
        // Pre-Path-X this read `descriptor.event_time_field.read(row).unwrap_or(now_ms)`.
        let now_ms_i64: i64 = now_ms as i64;
        let t_validate = t0.map(|t| t.elapsed());

        // 6. Serialize WAL payload — v=2 binary format.
        //
        // Record format: [u8 v=2][u8 body_format][u32 rv BE][u64 et_ms BE]
        //                [u16 event_name_len BE][N bytes name][u32 body_len BE][M bytes body]
        //
        // Plan 18-10: the `body` Bytes is the EXACT raw client bytes passed
        // through from parse_msgpack_envelope / parse_json_envelope (zero-copy
        // from wire to disk). No re-serialise on this path.
        //
        // Plan 12.6-05 Path X: the `et_ms` byte slot continues to receive an
        // 8-byte BE u64 timestamp; post-Path-X this is the server arrival-time
        // `now_ms` rather than a body-derived event_time. Plan 12.6-06 will
        // formally rename the slot and bump the WAL schema version.
        let name_bytes = event_name.as_bytes();
        let name_len = name_bytes.len() as u16;
        let body_len = body.len() as u32;
        // Total: 1 + 1 + 4 + 8 + 2 + name_len + 4 + body_len
        let mut payload_bytes =
            Vec::with_capacity(1 + 1 + 4 + 8 + 2 + name_bytes.len() + 4 + body.len());
        payload_bytes.push(0x02u8); // v = 2
        payload_bytes.push(body_format); // body_format (CT_JSON=0x01 or CT_MSGPACK=0x02)
        payload_bytes.extend_from_slice(&registry_version.to_be_bytes()); // u32 rv
        payload_bytes.extend_from_slice(&now_ms.to_be_bytes()); // u64 et_ms (Path X = server now_ms)
        payload_bytes.extend_from_slice(&name_len.to_be_bytes()); // u16 name_len
        payload_bytes.extend_from_slice(name_bytes); // name bytes
        payload_bytes.extend_from_slice(&body_len.to_be_bytes()); // u32 body_len
        payload_bytes.extend_from_slice(&body); // body bytes — zero-copy passthrough
        let t_wal_build = t0.map(|t| t.elapsed());

        // 7. WAL append — lock-free on the hot path (no Mutex, no channel).
        let ack_lsn = self.wal_ring.append(&payload_bytes);
        let t_wal_append = t0.map(|t| t.elapsed());

        // 8. Apply to aggregations under the table lock (uncontended on apply thread).
        //
        // Plan 12.6-05 Path X: the i64 time-source threaded into
        // `apply_event_to_aggregations` is the server `now_ms_i64`, NOT a
        // body-derived event_time. This is the keystone of the windowed-op
        // arrival-time semantics swap (no event-time anywhere downstream).
        //
        // Plan 12.8-03 D-01/D-04: pass the source's `cold_after_ms` so the
        // apply path can run the per-event cold-TTL eviction check inline.
        // `descriptor` is an `Arc<EventDescriptor>` (refcount bump on the
        // `get_event_descriptor` call above), so this is just a `Copy` of
        // an `Option<u64>`.
        {
            let mut tables = self.state.dev_agg.state_tables.lock();
            apply_event_to_aggregations(
                event_name,
                &row,
                now_ms_i64,
                ack_lsn,
                &self.state.dev_agg.registry,
                &mut tables,
                descriptor.cold_after_ms,
            );
            // Plan 12.8-06: refresh the process-static
            // `beava_entity_count_resident` snapshot under the same lock the
            // apply path is already holding. O(N_tables) sum of three
            // HashMap.len() values — typically < 30 tables in production
            // (one per registered aggregation), so well under 100 ns even
            // with cache misses. Acceptable per CONTEXT D-04
            // "inline-cheap or amortized" for the apply hot path.
            let total_entities: usize = tables.iter().map(|t| t.entity_count()).sum();
            beava_core::agg_state::EntityCountResidentSnapshot::store(total_entities as u64);
        }
        let t_agg = t0.map(|t| t.elapsed());

        // 9. Bump monotonic event counters.
        //
        // Plan 12.6-05 Path X: `query_time_ms` is fed `now_ms` (server
        // wall-clock at apply) rather than a body-derived event_time. The
        // field name is misleading post-Path-X — Plan 12.6-06 renames it to
        // a now-aligned identifier once the EventDescriptor.event_time_field
        // and the WAL schema slot are formally retired. Keeping the write
        // means the GET path's `compute_query_time_ms` (and the equivalent
        // logic in `runtime_core_glue.rs`) continues to surface a meaningful
        // query_time for windowed-op queries; removing it would silently
        // break ~30 tests that depend on a non-zero query time.
        self.state
            .dev_agg
            .next_event_id
            .fetch_max(ack_lsn, Ordering::Relaxed);
        if now_ms > 0 {
            self.state
                .dev_agg
                .query_time_ms
                .fetch_max(now_ms, Ordering::Relaxed);
        }
        let t_bk_counters = t0.map(|t| t.elapsed());

        // Plan 12.7-04: step 10 (record event_id_index entry for retract
        // routing) deleted alongside the table/retract surface. Per
        // `project_v0_events_only_scope` (locked 2026-04-30) v0 ships
        // events-only — there is no retract path to populate the side-table.
        // `t_bk_evid` is fed by the same monotonic clock as the surrounding
        // stages so the trace stays well-formed; with the work gone the
        // delta `t_bk_evid - t_bk_counters` is the noise of two `Instant::now`
        // calls (~5 ns).
        let t_bk_evid = t0.map(|t| t.elapsed());

        // 11. Cache on dedupe path.
        if let Some(key_str) = dedupe_str {
            // Plan 12.6-07: legacy `crate::push::PushAck` deleted along with
            // the legacy axum router. Inline the wire shape here — same JSON
            // body as the legacy struct.
            let ack = serde_json::json!({
                "ack_lsn": ack_lsn,
                "idempotent_replay": false,
                "registry_version": registry_version,
            });
            let response_bytes = serde_json::to_vec(&ack)
                .map(Bytes::from)
                .unwrap_or_default();
            let window_ms = descriptor
                .dedupe_window_ms
                .unwrap_or(DEFAULT_DEDUPE_WINDOW_MS);
            self.state.idem_cache.put(
                event_name.to_string(),
                key_str,
                crate::idem_cache::CachedEntry {
                    response_bytes,
                    ack_lsn,
                    inserted_at_ms: now_ms,
                    expires_at_ms: now_ms.saturating_add(window_ms),
                },
            );
        }

        // SPIKE: per-stage timing eprintln (success path only).
        if let (
            Some(t0_inst),
            Some(parse),
            Some(lookup),
            Some(validate),
            Some(wal_b),
            Some(wal_a),
            Some(agg),
            Some(bk_counters),
            Some(bk_evid),
        ) = (
            t0,
            t_parse,
            t_lookup,
            t_validate,
            t_wal_build,
            t_wal_append,
            t_agg,
            t_bk_counters,
            t_bk_evid,
        ) {
            let total = t0_inst.elapsed();
            // gap_ns = nanoseconds since previous push completed; "first" for the very first push on this thread.
            let gap_str = match gap {
                Some(g) => format!("{}", g.as_nanos()),
                None => "first".to_string(),
            };
            eprintln!(
                "TRACE_APPLY ns push: gap={} parse={} lookup={} validate={} wal_build={} wal_append={} agg={} bk_counters={} bk_evid={} bk_dedupe={} bookkeeping={} TOTAL={}",
                gap_str,
                parse.as_nanos(),
                (lookup - parse).as_nanos(),
                (validate - lookup).as_nanos(),
                (wal_b - validate).as_nanos(),
                (wal_a - wal_b).as_nanos(),
                (agg - wal_a).as_nanos(),
                (bk_counters - agg).as_nanos(),
                (bk_evid - bk_counters).as_nanos(),
                (total - bk_evid).as_nanos(),
                (total - agg).as_nanos(),
                total.as_nanos()
            );
        }
        if trace_apply {
            LAST_PUSH_END.with(|cell| cell.set(Some(Instant::now())));
        }

        GlueResponse::PushAck {
            ack_lsn,
            registry_version,
        }
    }
}

// ─── Sync helpers (Plan 18-10 D-3: Row-based, no JsonValue) ───────────────────

/// Schema validation against a beava_core::row::Row directly. Replaces the
/// Plan 18-09 `validate_body_sync` which took `serde_json::Map<String, Value>`.
///
/// For each non-optional schema field, the row must contain a typed `Value`
/// compatible with the declared `FieldType`.
fn validate_row_against_descriptor(
    descriptor: &beava_core::registry::EventDescriptor,
    row: &beava_core::row::Row,
) -> bool {
    for (field_name, field_type) in &descriptor.schema.fields {
        if descriptor.schema.optional_fields.contains(field_name) {
            continue;
        }
        let val = match row.get(field_name) {
            Some(v) => v,
            None => return false,
        };
        if !value_type_compatible(val, field_type) {
            return false;
        }
    }
    true
}

/// FieldType ↔ Value compatibility. Numeric coercion (i64↔f64) is permitted
/// because the wire data may be either; the apply path consumes the typed
/// Value as-is.
fn value_type_compatible(val: &beava_core::row::Value, ft: &beava_core::schema::FieldType) -> bool {
    use beava_core::row::Value;
    use beava_core::schema::FieldType;
    match ft {
        FieldType::I64 | FieldType::F64 => matches!(val, Value::I64(_) | Value::F64(_)),
        FieldType::Str => matches!(val, Value::Str(_)),
        FieldType::Bool => matches!(val, Value::Bool(_)),
        // Bytes/Datetime/Json: accept any non-null value for forward compat.
        FieldType::Bytes | FieldType::Datetime | FieldType::Json => !matches!(val, Value::Null),
    }
}

/// Extract the dedupe key as a string from a Row field. Mirrors the Plan 18-09
/// behaviour where strings pass through and other types are stringified.
fn extract_dedupe_str_from_row(row: &beava_core::row::Row, key: &str) -> Option<String> {
    use beava_core::row::Value;
    row.get(key).map(|v| match v {
        Value::Str(s) => s.to_string(),
        Value::I64(i) => i.to_string(),
        Value::F64(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        Value::Bytes(b) => format!("{:?}", b),
        Value::Datetime(i) => i.to_string(),
        Value::Json(j) => j.to_string(),
        Value::List(l) => format!("{:?}", l),
        Value::Map(m) => format!("{:?}", m),
    })
}

// ─── Plan 13.4-03 / Plan 13.4.1 — OP_BATCH_GET (0x0024) dispatch ──────────────

/// Plan 13.4.1 — one entry in the OP_BATCH_GET request payload.
///
/// Verb-style fields per D-02:
/// - `table`: aggregation node name (lookup key in `registry.compiled_aggregation`).
/// - `key`: per-entity scoping value (parsed via `parse_entity_key`).
/// - `features`: optional per-entry filter; None = return all features
///   for the entity; Some(vec) = narrow row dict to those names
///   (omit-on-absent per D-06).
///
/// D-04 — `entity_id` is accepted as a legacy alias of `key` for one
/// release; alias removal scheduled in v0.0.x cleanup
/// (.planning/ideas/v0.1-deferrals.md). `from_alias` tracks whether the
/// source field was the legacy alias so the dispatch loop can emit a
/// WARN log only on alias use (no false-positive on canonical path).
struct BatchGetReqEntry {
    table: String,
    key: String,
    features: Option<Vec<String>>,
    /// Internal — NOT serialised; set by custom Deserialize impl.
    from_alias: bool,
}

// Plan 13.4.1 D-04 — custom Deserialize impl that tracks whether the source
// field was `key` (canonical) or `entity_id` (legacy alias). Emits a serde
// error on missing-both / both-present.
impl<'de> serde::Deserialize<'de> for BatchGetReqEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct EntryVisitor;
        impl<'de> Visitor<'de> for EntryVisitor {
            type Value = BatchGetReqEntry;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a BatchGetReqEntry map with {table, key|entity_id, features?}")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut table: Option<String> = None;
                let mut key: Option<String> = None;
                let mut features: Option<Vec<String>> = None;
                let mut from_alias = false;
                while let Some(field) = map.next_key::<String>()? {
                    match field.as_str() {
                        "table" => {
                            if table.is_some() {
                                return Err(de::Error::duplicate_field("table"));
                            }
                            table = Some(map.next_value()?);
                        }
                        "key" => {
                            if key.is_some() {
                                return Err(de::Error::duplicate_field("key"));
                            }
                            key = Some(map.next_value()?);
                            // Canonical path — leave from_alias = false unless
                            // entity_id was already seen (rejected below).
                        }
                        "entity_id" => {
                            if key.is_some() {
                                return Err(de::Error::custom(
                                    "BatchGetReqEntry: cannot specify both `key` and `entity_id` (entity_id is a deprecated alias)",
                                ));
                            }
                            key = Some(map.next_value()?);
                            from_alias = true;
                        }
                        "features" => {
                            if features.is_some() {
                                return Err(de::Error::duplicate_field("features"));
                            }
                            features = Some(map.next_value()?);
                        }
                        _ => {
                            // Tolerate forward-compat fields by skipping their values.
                            let _ignored: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(BatchGetReqEntry {
                    table: table.ok_or_else(|| de::Error::missing_field("table"))?,
                    key: key.ok_or_else(|| de::Error::missing_field("key"))?,
                    features,
                    from_alias,
                })
            }
        }

        deserializer.deserialize_map(EntryVisitor)
    }
}

/// OP_BATCH_GET dispatch — walks `{requests:[{table, entity_id}, ...]}`,
/// calls per-entity feature lookup for each tuple, aggregates results into
/// the canonical `{results: [...]}` shape with partial-failure semantics.
///
/// Per the plan must_haves:
/// - Per-tuple `{table, entity_id, features: <flat_dict>}` on success.
/// - Per-tuple `{table, entity_id, error: {code: "unknown_table", reason}}`
///   on unknown table — the rest of the batch still completes (no whole-frame
///   4xx).
/// - Empty `requests: []` returns `{"results": []}` HTTP 200.
///
/// Per ADR-003 the empty-string `entity_id` is the global-table sentinel and
/// is forwarded as-is to `parse_entity_key`; Plan 13.4-09 wires the
/// `key_cols: []` register-time acceptance and ensures the existing hashmap
/// machinery handles `""` as a regular key segment.
///
/// `body_format` is the wire content_type byte (`CT_JSON` or `CT_MSGPACK`).
/// HTTP path always passes `CT_JSON`; TCP path forwards the frame's
/// content_type. Response body is always JSON encoded — the response opcode
/// is `OP_GET_RESPONSE`, whose body shape contract is JSON in v0.
pub fn dispatch_batch_get_sync(
    app: &std::sync::Arc<crate::AppState>,
    body: &Bytes,
    body_format: u8,
) -> GlueResponse {
    use beava_core::wire::{CT_JSON, CT_MSGPACK};

    #[derive(serde::Deserialize)]
    struct BatchGetBody {
        requests: Vec<BatchGetReqEntry>,
    }

    // 1. Parse the batch body.
    let req: BatchGetBody = match body_format {
        CT_JSON => match serde_json::from_slice(body) {
            Ok(r) => r,
            Err(e) => {
                return GlueResponse::InternalError {
                    reason: e.to_string(),
                };
            }
        },
        CT_MSGPACK => match rmp_serde::from_slice(body) {
            Ok(r) => r,
            Err(e) => {
                return GlueResponse::InternalError {
                    reason: e.to_string(),
                };
            }
        },
        other => {
            return GlueResponse::InternalError {
                reason: format!("unsupported content_type: {other:#04x}"),
            };
        }
    };

    // 2. Empty batch → `{"results": []}` HTTP 200 (plan must_have).
    if req.requests.is_empty() {
        let body_bytes = serde_json::to_vec(&serde_json::json!({"results": []}))
            .unwrap_or_else(|_| b"{\"results\":[]}".to_vec());
        return GlueResponse::QueryResult {
            body: Bytes::from(body_bytes),
            format: CT_JSON,
        };
    }

    // Plan 13.4.1 D-04 — emit a tracing::warn! once per entry that used
    // the legacy `entity_id` alias (no false-positive on canonical `key`
    // path; STRICT detection per Plan 03 Test 4 requirement). Verbatim
    // message text per CONTEXT.md D-04 line 76.
    for entry in &req.requests {
        if entry.from_alias {
            tracing::warn!(
                kind = "batch_get.entity_id_alias",
                table = %entry.table,
                "BatchGetReqEntry: deprecated 'entity_id' field name; rename to 'key'; alias removed in v0.0.x"
            );
        }
    }

    // 3. Compute query_time_ms once (D-06 — never wall clock; mirrors
    //    `dispatch_get_batch`'s policy).
    let query_time_ms = {
        let raw = app
            .dev_agg
            .query_time_ms
            .load(std::sync::atomic::Ordering::Acquire);
        if raw == 0 {
            0i64
        } else {
            raw as i64
        }
    };

    let registry = &app.dev_agg.registry;

    // Plan 13.4.1 D-06 (first clause) — whole-batch reject on registry-typo.
    // Mirrors the precedent at runtime_core_glue.rs:382-396 and the
    // single-row verb-style dispatch (dispatch_get_single_verb_style_sync).
    //
    // For each entry whose `features` filter is Some, look up the descriptor
    // and check every requested feature name. If ANY entry has a feature name
    // not in its table descriptor, abort the whole batch with InternalError.
    //
    // Skipped (still per-tuple, NOT whole-batch):
    //   - unknown_table → Step 5's per-tuple `{"error":{"code":"unknown_table",...}}`
    //   - key_parse_failure → Step 6's per-tuple `{"error":{"code":"key_parse_failure",...}}`
    //   - per-entity sparsity (feature absent on entity) → omit silently in Step 4
    for entry in &req.requests {
        let Some(filter) = entry.features.as_deref() else {
            continue;
        };
        let descriptor = match registry.compiled_aggregation(&entry.table) {
            // Unknown table here is a per-tuple error (not D-06 first clause);
            // let the dispatch loop below handle it.
            Some(d) => d,
            None => continue,
        };
        let mut unknown: Vec<String> = Vec::new();
        for name in filter {
            if !descriptor.features.iter().any(|f| &f.feature_name == name) {
                unknown.push(name.clone());
            }
        }
        if !unknown.is_empty() {
            return GlueResponse::InternalError {
                reason: format!(
                    "feature_not_found: missing={:?} table={}",
                    unknown, entry.table
                ),
            };
        }
    }

    // 4. Acquire the state_tables lock once for the whole batch (matches
    //    `dispatch_get_batch`'s single-critical-section discipline).
    let tables = app.dev_agg.state_tables.lock();

    let mut results: Vec<serde_json::Value> = Vec::with_capacity(req.requests.len());

    for entry in &req.requests {
        // Look up the table by name in the registry as an aggregation node.
        let descriptor = match registry.compiled_aggregation(&entry.table) {
            Some(d) => d,
            None => {
                // Plan 13.4.1 D-03 — flat per-tuple error tuple (no
                // table/entity_id wrapping).
                results.push(serde_json::json!({
                    "error": {
                        "code": "unknown_table",
                        "message": format!(
                            "table '{}' is not a registered aggregation",
                            entry.table
                        ),
                    },
                }));
                continue;
            }
        };

        // Per ADR-003: forward `key` verbatim to parse_entity_key. For
        // `key_cols: []` (global table) the empty string parses to a 0-arity
        // key; Plan 13.4-09 wires the register-time path that makes empty
        // group_keys legal.
        let entity_key =
            match crate::feature_query::parse_entity_key(&entry.key, &descriptor.group_keys) {
                Some(k) => k,
                None => {
                    // Plan 13.4.1 D-03 — flat per-tuple error tuple.
                    results.push(serde_json::json!({
                        "error": {
                            "code": "key_parse_failure",
                            "message": format!(
                                "key '{}' does not match table '{}' group_keys arity ({})",
                                entry.key,
                                entry.table,
                                descriptor.group_keys.len()
                            ),
                        },
                    }));
                    continue;
                }
            };

        // Plan 13.4.1 D-03 + D-06 — FLAT row response with per-entry features
        // filter. Drop the `{table, entity_id, features:{...}}` envelope.
        let mut feature_map = serde_json::Map::new();
        if let Some(table) = tables.get(descriptor.agg_id as usize) {
            for (idx, named_op) in descriptor.features.iter().enumerate() {
                // D-06 features-filter narrowing pass.
                if let Some(filter) = entry.features.as_deref() {
                    if !filter.iter().any(|f| f == &named_op.feature_name) {
                        continue;
                    }
                }
                if let Some(v) = table.query_feature(&entity_key, idx, query_time_ms) {
                    feature_map.insert(
                        named_op.feature_name.clone(),
                        crate::feature_query::value_to_json(v),
                    );
                }
                // D-06 omit-on-absent: when query_feature returns None, do
                // NOT insert the key. Cold-start entity → empty feature_map.
            }
        }

        // FLAT row — feature_map IS the result. Cold-start = `{}` per the
        // wire-spec ("Per-entry cold-start is `{}`, not an error").
        results.push(serde_json::Value::Object(feature_map));
    }
    drop(tables);

    let body_json = serde_json::json!({"results": results});
    let body_bytes = match serde_json::to_vec(&body_json) {
        Ok(b) => b,
        Err(e) => {
            return GlueResponse::InternalError {
                reason: e.to_string(),
            };
        }
    };
    GlueResponse::QueryResult {
        body: Bytes::from(body_bytes),
        format: CT_JSON,
    }
}

// ─── Plan 13.4-08 (D-03 USER-LOCKED) — OP_RESET dispatch ──────────────────────
//
// Honors the boot-time `effective_test_mode` flag stamped on AppState. When
// the flag is FALSE (production-by-default boot) the dispatch returns a
// structured `reset_disabled_in_production` error — HTTP 403 / TCP
// OP_ERROR_RESPONSE (0xFFFF). When TRUE the dispatch:
//
// 1. Acquires the `state_tables` Mutex (single-writer apply discipline).
// 2. Empties EVERY per-entity aggregation state by clearing the `Vec`.
// 3. Drops every registered descriptor + every compiled chain/aggregation
//    via `Registry::clear()`. The clear() call bumps `registry_version` by
//    1 so any cached client `registry_version` becomes stale.
// 4. Resets `next_event_id` and `query_time_ms` to 0 (cold-start state).
// 5. Returns `GlueResponse::ResetOk { registry_version }` — the encoder
//    layer maps this to HTTP 200 + `{"reset": true, "registry_version": N}`
//    or TCP OP_GET_RESPONSE with the same body.
//
// Per-event WAL ring buffers and the legacy /register WAL sink are NOT
// touched here — the in-memory state is the source of truth for v0; on
// restart the disk-mode path replays the WAL and rebuilds, but a fresh
// reset followed by a re-register starts from a clean slate. Memory mode
// has no WAL by design.
//
// **Threat model coverage** (Plan 13.4-08 §threat_model):
// - T-13.4-08-01 (Tampering: client wipes prod state): the
//   `if !state.effective_test_mode` early-return IS the defense.
// - T-13.4-08-03 (Spoofing: env var read at runtime): the flag is set ONCE
//   at bind time and never re-read; runtime escalation impossible.
pub fn dispatch_reset_sync(state: &std::sync::Arc<crate::AppState>) -> GlueResponse {
    if !state.effective_test_mode {
        return GlueResponse::ResetForbidden;
    }

    // 1+2 — acquire state_tables lock + empty every per-entity table.
    {
        let mut tables = state.dev_agg.state_tables.lock();
        tables.clear();
    }

    // 3 — drop every descriptor + bump registry_version.
    state.dev_agg.registry.clear();

    // 4 — reset cold-start counters. event_id is cumulative for the apply
    // path; resetting to 0 means the next push starts from event_id=1
    // again. query_time_ms is the latest server-side wall-clock the apply
    // path saw; resetting to 0 makes the next GET fall back to the live
    // wall clock until the first post-reset push lands.
    state
        .dev_agg
        .next_event_id
        .store(0, std::sync::atomic::Ordering::Release);
    state
        .dev_agg
        .query_time_ms
        .store(0, std::sync::atomic::Ordering::Release);

    let registry_version = state.dev_agg.registry.version();
    tracing::info!(
        target: "beava.server",
        kind = "server.reset_completed",
        registry_version,
        "OP_RESET completed: state + registry cleared (D-03 USER-LOCKED)"
    );
    GlueResponse::ResetOk { registry_version }
}
