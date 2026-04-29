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

use crate::register::{RegisterOutcome, RegisterPayload};
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
                let reg_payload: RegisterPayload = match serde_json::from_slice(&payload) {
                    Ok(p) => p,
                    Err(e) => {
                        return GlueResponse::RegisterError {
                            code: "invalid_registration".to_owned(),
                            message: e.to_string(),
                        };
                    }
                };
                // Delegate to the async glue via a blocking thread so the apply thread
                // doesn't stall on the tokio WAL channel (register is cold path).
                // This is the correct approach: apply thread stays sync; register just
                // calls the standard glue dispatch which is allowed to be async for
                // admin/cold paths per D-3.
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
                match outcome {
                    RegisterOutcome::Success { version, .. } => {
                        GlueResponse::RegisterOk { version }
                    }
                    RegisterOutcome::EmptyPayload { version } => {
                        GlueResponse::RegisterOk { version }
                    }
                    RegisterOutcome::Noop { version, .. } => GlueResponse::RegisterOk { version },
                    RegisterOutcome::ValidationFailed {
                        first_error_path,
                        first_error_reason,
                        ..
                    } => GlueResponse::RegisterError {
                        code: "invalid_registration".to_owned(),
                        message: format!("{first_error_path}: {first_error_reason}"),
                    },
                    RegisterOutcome::Conflict { .. } => GlueResponse::RegisterError {
                        code: "registration_conflict".to_owned(),
                        message: "descriptor conflict".to_owned(),
                    },
                    RegisterOutcome::WalUnavailable { .. } => GlueResponse::RegisterError {
                        code: "wal_unavailable".to_owned(),
                        message: "WAL unavailable".to_owned(),
                    },
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
                let resp =
                    crate::runtime_core_glue::dispatch_get_single_sync(&self.state, &feature, &key);
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

            // ─── GET batch ────────────────────────────────────────────────────
            WireRequest::HttpGet { body } => {
                crate::runtime_core_glue::dispatch_get_batch_sync(&self.state, &body)
            }

            // ─── TCP /get (single) — Plan 12-07 Wave 3 ───────────────────────
            // Body parses to {"feature": "<name>", "key": "<key>"} (CT_JSON shape
            // for v0; CT_MSGPACK is reserved for Plan 12-08+ when MsgPack body
            // parsing for query lands).
            WireRequest::TcpGet {
                body,
                body_format: _,
            } => {
                #[derive(serde::Deserialize)]
                struct TcpGetReq {
                    feature: String,
                    key: String,
                }
                let req: TcpGetReq = match serde_json::from_slice(&body) {
                    Ok(r) => r,
                    Err(e) => {
                        return GlueResponse::InternalError {
                            reason: e.to_string(),
                        }
                    }
                };
                crate::runtime_core_glue::dispatch_get_single_sync(
                    &self.state,
                    &req.feature,
                    &req.key,
                )
            }

            // ─── TCP /mget (single feature, multi key) — Plan 12-07 Wave 3 ───
            // Body parses to {"feature": "<name>", "keys": [...]}. Materialise as
            // a batch with a single-feature list and reuse dispatch_get_batch_sync.
            WireRequest::TcpMGet {
                body,
                body_format: _,
            } => {
                #[derive(serde::Deserialize)]
                struct TcpMGetReq {
                    feature: String,
                    keys: Vec<String>,
                }
                let req: TcpMGetReq = match serde_json::from_slice(&body) {
                    Ok(r) => r,
                    Err(e) => {
                        return GlueResponse::InternalError {
                            reason: e.to_string(),
                        }
                    }
                };
                let batch_body = serde_json::json!({
                    "keys": req.keys,
                    "features": [req.feature],
                });
                let batch_bytes = match serde_json::to_vec(&batch_body) {
                    Ok(b) => bytes::Bytes::from(b),
                    Err(e) => {
                        return GlueResponse::InternalError {
                            reason: e.to_string(),
                        }
                    }
                };
                crate::runtime_core_glue::dispatch_get_batch_sync(&self.state, &batch_bytes)
            }

            // ─── TCP /get-multi (multi feature, multi key) — Plan 12-07 Wave 3 ──
            // Body shape mirrors HTTP /get: {"keys": [...], "features": [...]}.
            // Reuse dispatch_get_batch_sync directly.
            WireRequest::TcpGetMulti {
                body,
                body_format: _,
            } => crate::runtime_core_glue::dispatch_get_batch_sync(&self.state, &body),

            // ─── Upsert / delete / retract (table ops — not on hot path) ──────
            WireRequest::HttpUpsert { .. }
            | WireRequest::HttpDelete { .. }
            | WireRequest::HttpRetract { .. } => GlueResponse::Unsupported,

            WireRequest::Unknown { .. } | WireRequest::ParseError { .. } => {
                GlueResponse::Unsupported
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

        // 3. Schema validate against Row.fields directly.
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
            if let Some(_cached) = self.state.idem_cache.get(event_name, key_str, now_ms) {
                return GlueResponse::PushReplay { registry_version };
            }
        }

        // 5. Extract event_time_ms from Row.
        let event_time_ms = descriptor
            .event_time_field
            .as_deref()
            .and_then(|f| row.get(f))
            .and_then(|v| match v {
                beava_core::row::Value::I64(i) => Some(*i),
                beava_core::row::Value::Datetime(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(now_ms as i64);
        let t_validate = t0.map(|t| t.elapsed());

        // 6. Serialize WAL payload — v=2 binary format.
        //
        // Record format: [u8 v=2][u8 body_format][u32 rv BE][u64 et_ms BE]
        //                [u16 event_name_len BE][N bytes name][u32 body_len BE][M bytes body]
        //
        // Plan 18-10: the `body` Bytes is the EXACT raw client bytes passed
        // through from parse_msgpack_envelope / parse_json_envelope (zero-copy
        // from wire to disk). No re-serialise on this path.
        let name_bytes = event_name.as_bytes();
        let name_len = name_bytes.len() as u16;
        let body_len = body.len() as u32;
        // Total: 1 + 1 + 4 + 8 + 2 + name_len + 4 + body_len
        let mut payload_bytes =
            Vec::with_capacity(1 + 1 + 4 + 8 + 2 + name_bytes.len() + 4 + body.len());
        payload_bytes.push(0x02u8); // v = 2
        payload_bytes.push(body_format); // body_format (CT_JSON=0x01 or CT_MSGPACK=0x02)
        payload_bytes.extend_from_slice(&registry_version.to_be_bytes()); // u32 rv
        payload_bytes.extend_from_slice(&(event_time_ms as u64).to_be_bytes()); // u64 et_ms
        payload_bytes.extend_from_slice(&name_len.to_be_bytes()); // u16 name_len
        payload_bytes.extend_from_slice(name_bytes); // name bytes
        payload_bytes.extend_from_slice(&body_len.to_be_bytes()); // u32 body_len
        payload_bytes.extend_from_slice(&body); // body bytes — zero-copy passthrough
        let t_wal_build = t0.map(|t| t.elapsed());

        // 7. WAL append — lock-free on the hot path (no Mutex, no channel).
        let ack_lsn = self.wal_ring.append(&payload_bytes);
        let t_wal_append = t0.map(|t| t.elapsed());

        // 8. Apply to aggregations under the table lock (uncontended on apply thread).
        {
            let mut tables = self.state.dev_agg.state_tables.lock();
            apply_event_to_aggregations(
                event_name,
                &row,
                event_time_ms,
                ack_lsn,
                &self.state.dev_agg.registry,
                &mut tables,
            );
        }
        let t_agg = t0.map(|t| t.elapsed());

        // 9. Bump monotonic event counters.
        self.state
            .dev_agg
            .next_event_id
            .fetch_max(ack_lsn, Ordering::Relaxed);
        if event_time_ms > 0 {
            self.state
                .dev_agg
                .max_event_time_ms
                .fetch_max(event_time_ms as u64, Ordering::Relaxed);
        }
        let t_bk_counters = t0.map(|t| t.elapsed());

        // 10. Record event ID entry for retract routing.
        {
            use crate::registry_debug::EventIdEntry;
            let mut idx = self.state.dev_agg.event_id_index.lock();
            idx.insert(
                ack_lsn,
                EventIdEntry::Stream {
                    // Plan 18-12 D-3: refcount bump on the registry-resident
                    // Arc<str> — no per-push heap alloc. `descriptor` is the
                    // Arc<EventDescriptor> from the Plan 18-11 D-6 lookup at
                    // step 2; `name_arc` was populated at registration.
                    event_name: Arc::clone(&descriptor.name_arc),
                },
            );
        }
        let t_bk_evid = t0.map(|t| t.elapsed());

        // 11. Cache on dedupe path.
        if let Some(key_str) = dedupe_str {
            let ack = crate::push::PushAck {
                ack_lsn,
                idempotent_replay: false,
                registry_version,
            };
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
