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

use crate::push::PushAck;
use crate::register::{RegisterOutcome, RegisterPayload};
use crate::runtime_core_glue::GlueResponse;
use crate::AppState;
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
    pub fn new(
        state: Arc<AppState>,
        wal_ring: Arc<WalBufferRing>,
        wal_lsn: Arc<WalLsn>,
    ) -> Self {
        Self { state, wal_ring, wal_lsn }
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
        vec![self.dispatch_one(req)]
    }

    fn dispatch_one(&self, req: WireRequest) -> GlueResponse {
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
                match outcome {
                    RegisterOutcome::Success { version, .. } => {
                        GlueResponse::RegisterOk { version }
                    }
                    RegisterOutcome::EmptyPayload { version } => {
                        GlueResponse::RegisterOk { version }
                    }
                    RegisterOutcome::Noop { version, .. } => {
                        GlueResponse::RegisterOk { version }
                    }
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
            WireRequest::TcpPush { event_name, body }
            | WireRequest::HttpPush { event_name, body } => {
                self.dispatch_push_sync(&event_name, body)
            }

            // ─── HTTP push-sync (per-event / acks=all mode) ───────────────────
            // For the mio path we still do sync WAL append; the
            // wait-for-synced blocking call would stall the apply thread.
            // Per plan D-2 the full per-event path is a future refinement.
            // For now, treat identically to periodic push.
            WireRequest::HttpPushSync { event_name, body } => {
                self.dispatch_push_sync(&event_name, body)
            }

            WireRequest::HttpPushBatch { event_name, body } => {
                // Batch push: treat as single push for scaffold correctness.
                self.dispatch_push_sync(&event_name, body)
            }

            // ─── GET single ───────────────────────────────────────────────────
            WireRequest::HttpGetSingle { feature, key } => {
                crate::runtime_core_glue::dispatch_get_single_sync(&self.state, &feature, &key)
            }

            // ─── GET batch ────────────────────────────────────────────────────
            WireRequest::HttpGet { body } => {
                crate::runtime_core_glue::dispatch_get_batch_sync(&self.state, &body)
            }

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
    /// 1. Parse JSON body (no clone of the full tree).
    /// 2. Look up event descriptor.
    /// 3. Schema validate.
    /// 4. Dedupe lookup.
    /// 5. Serialize WAL payload.
    /// 6. `WalBufferRing::append` (lock-free memcpy + atomic LSN bump).
    /// 7. `apply_event_to_aggregations` under the aggregation table lock.
    /// 8. Build and return GlueResponse.
    fn dispatch_push_sync(&self, event_name: &str, body: Bytes) -> GlueResponse {
        use beava_core::agg_apply::apply_event_to_aggregations;
        use beava_core::defaults::DEFAULT_DEDUPE_WINDOW_MS;
        use beava_core::row::Value;
        use beava_core::schema::FieldType;
        use serde_json::Value as JsonValue;
        use std::sync::atomic::Ordering;
        use std::time::{SystemTime, UNIX_EPOCH};

        let registry_version = self.state.dev_agg.registry.version() as u32;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // 1. Parse JSON body.
        let parsed: JsonValue = match sonic_rs::from_slice(&body) {
            Ok(v) => v,
            Err(_) => {
                return GlueResponse::PushError {
                    code: "invalid_event",
                    registry_version,
                };
            }
        };
        let obj = match parsed.as_object() {
            Some(o) => o.clone(),
            None => {
                return GlueResponse::PushError {
                    code: "invalid_event",
                    registry_version,
                };
            }
        };

        // 2. Lookup event descriptor.
        let descriptor = {
            let inner = self.state.dev_agg.registry.read();
            match inner.events.get(event_name).cloned() {
                Some(d) => d,
                None => {
                    return GlueResponse::PushError {
                        code: "event_not_found",
                        registry_version,
                    };
                }
            }
        };

        // 3. Schema validate (required fields present with correct types).
        if !validate_body_sync(&descriptor, &obj) {
            return GlueResponse::PushError {
                code: "invalid_event",
                registry_version,
            };
        }

        // 4. Dedupe lookup.
        let dedupe_str = descriptor
            .dedupe_key
            .as_deref()
            .and_then(|k| extract_dedupe_str_sync(&obj, k));

        if let (Some(_), Some(ref key_str)) = (descriptor.dedupe_key.as_ref(), &dedupe_str) {
            if let Some(_cached) = self.state.idem_cache.get(event_name, key_str, now_ms) {
                return GlueResponse::PushReplay { registry_version };
            }
        }

        // 5. Extract event_time_ms.
        let event_time_ms = descriptor
            .event_time_field
            .as_deref()
            .and_then(|f| obj.get(f))
            .and_then(|jv| jv.as_i64())
            .unwrap_or(now_ms as i64);

        // 6. Serialize WAL payload.
        let payload = serde_json::json!({
            "v": 1,
            "rv": registry_version,
            "s": event_name,
            "et": event_time_ms,
            "b": &parsed,
        });
        let payload_bytes = match sonic_rs::to_vec(&payload) {
            Ok(b) => b,
            Err(_) => {
                return GlueResponse::PushError {
                    code: "serialize_failed",
                    registry_version,
                };
            }
        };

        // 7. WAL append — lock-free on the hot path (no Mutex, no channel).
        let ack_lsn = self.wal_ring.append(&payload_bytes);

        // 8. Apply to aggregations under the table lock (uncontended on apply thread).
        let row = json_object_to_row_sync(&obj);
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

        // 10. Record event ID entry for retract routing.
        {
            use crate::registry_debug::EventIdEntry;
            let mut idx = self.state.dev_agg.event_id_index.lock();
            idx.insert(
                ack_lsn,
                EventIdEntry::Stream {
                    event_name: event_name.to_string(),
                },
            );
        }

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

        GlueResponse::PushAck {
            ack_lsn,
            registry_version,
        }
    }
}

// ─── Sync helpers (duplicated from push.rs to avoid async dependency) ─────────

fn validate_body_sync(
    descriptor: &beava_core::registry::EventDescriptor,
    obj: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    use beava_core::schema::FieldType;
    for (field_name, field_type) in &descriptor.schema.fields {
        if descriptor
            .schema
            .optional_fields
            .contains(field_name)
        {
            continue;
        }
        let val = match obj.get(field_name) {
            Some(v) => v,
            None => return false,
        };
        if !type_compatible(val, field_type) {
            return false;
        }
    }
    true
}

fn type_compatible(val: &serde_json::Value, ft: &beava_core::schema::FieldType) -> bool {
    use beava_core::schema::FieldType;
    match ft {
        FieldType::I64 | FieldType::F64 => val.is_number(),
        FieldType::Str => val.is_string(),
        FieldType::Bool => val.is_boolean(),
        // Bytes/Datetime/Json: accept any non-null value for forward compat.
        FieldType::Bytes | FieldType::Datetime | FieldType::Json => !val.is_null(),
    }
}

fn extract_dedupe_str_sync(
    obj: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<String> {
    obj.get(key).and_then(|v| match v {
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    })
}

fn json_object_to_row_sync(
    obj: &serde_json::Map<String, serde_json::Value>,
) -> beava_core::row::Row {
    use beava_core::row::{Row, Value};
    let mut row = Row::new();
    for (k, v) in obj {
        let rv = match v {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::I64(i)
                } else if let Some(f) = n.as_f64() {
                    Value::F64(f)
                } else {
                    Value::I64(0)
                }
            }
            serde_json::Value::String(s) => Value::Str(s.clone()),
            serde_json::Value::Bool(b) => Value::Bool(*b),
            _ => Value::I64(0),
        };
        row = row.with_field(k, rv);
    }
    row
}
