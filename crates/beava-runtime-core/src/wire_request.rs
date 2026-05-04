//! Parsed wire requests produced by the TCP frame parser and HTTP parser.
//!
//! A `WireRequest` is the unified command type dispatched to the apply callback
//! regardless of which transport delivered it. This mirrors Redis's `client::argv`
//! (translation table entry #10) but uses an enum instead of a slice of robj*.

use bytes::Bytes;

/// A fully-parsed inbound request, ready for the apply thread.
///
/// Produced by `tcp_listener::parse_wire_request` (framed TCP) or by the HTTP
/// state machine (Task 1.3). The apply thread receives these via the dispatch
/// callback and never touches raw bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireRequest {
    /// OP_PING (0x0000) — health probe; no payload.
    Ping,
    /// OP_REGISTER (0x0001) — register a feature pipeline DAG.
    Register { payload: Bytes },
    /// OP_PUSH (0x0010) — fire-and-forget event push.
    /// `event_name` is the stream name; `body` is the raw payload bytes (JSON or MsgPack).
    /// `body_format` indicates the encoding: `beava_core::wire::CT_JSON` or `CT_MSGPACK`.
    TcpPush {
        event_name: String,
        body: Bytes,
        body_format: u8,
    },
    /// HTTP POST /push/:event — same semantic as TcpPush but via HTTP.
    /// Always `CT_JSON` format (HTTP path carries JSON bodies only).
    HttpPush {
        event_name: String,
        body: Bytes,
        body_format: u8,
    },
    /// HTTP POST /push-sync/:event — synchronous push (await fsync).
    HttpPushSync {
        event_name: String,
        body: Bytes,
        body_format: u8,
    },
    /// HTTP POST /push-batch/:event — batched push.
    HttpPushBatch {
        event_name: String,
        body: Bytes,
        body_format: u8,
    },
    /// HTTP POST /get — batch feature read.
    HttpGet { body: Bytes },
    /// HTTP GET /get/:feature/:key — single feature read.
    HttpGetSingle { feature: String, key: String },
    /// OP_GET (0x0020) — TCP single-key feature read (Plan 12-07).
    ///
    /// `body` is the JSON or MsgPack payload encoding `{feature, key}`;
    /// `body_format` is `beava_core::wire::CT_JSON` (0x01) or `CT_MSGPACK` (0x02).
    /// The actual deserialisation happens at dispatch time in
    /// `beava_server::runtime_core_glue::dispatch_get_*` so the wire crate
    /// stays serialiser-agnostic.
    TcpGet { body: Bytes, body_format: u8 },
    /// OP_MGET (0x0021) — TCP batched single-feature multi-key read (Plan 12-07).
    ///
    /// `body` payload encodes `{feature: <name>, keys: [...]}`; same content_type rules.
    TcpMGet { body: Bytes, body_format: u8 },
    /// OP_GET_MULTI (0x0022) — TCP batched multi-feature multi-key read (Plan 12-07).
    ///
    /// `body` payload encodes `{keys: [...], features: [...]}` (mirrors HTTP /get);
    /// same content_type rules.
    TcpGetMulti { body: Bytes, body_format: u8 },
    /// OP_BATCH_GET (0x0024) — TCP heterogeneous batched read (Plan 13.4-03).
    ///
    /// `body` payload encodes `{requests: [{table, entity_id}, ...]}`;
    /// `body_format` is `CT_JSON` (0x01) or `CT_MSGPACK` (0x02). Composes
    /// natively with the global-table empty-string entity_id sentinel
    /// (ADR-003 — Plan 13.4-09 wires register-time validation). The
    /// dispatch layer (`apply_shard.rs::dispatch_batch_get_sync`) walks the
    /// request list, calls per-entity feature lookup, and aggregates results
    /// with partial-failure semantics (unknown_table becomes a per-tuple
    /// error inside `results` rather than a whole-batch 4xx).
    TcpBatchGet { body: Bytes, body_format: u8 },
    /// HTTP POST /batch_get — same payload as TcpBatchGet, JSON-only.
    /// Plan 13.4-03.
    HttpBatchGet { body: Bytes },
    /// Plan 12.6-14: POST request whose Content-Type was not
    /// `application/json` (or absent). Encoded as 415 with the structured
    /// `unsupported_media_type` body shape used by legacy axum's register
    /// handler.  `received` is what the client sent (or the empty string
    /// when the header was absent); `path` is the HTTP request path.
    HttpUnsupportedMediaType { received: String, path: String },
    /// GET /health — liveness probe (Plan 12-07). No payload, no apply-thread
    /// roundtrip; the dispatch layer returns `GlueResponse::HealthOk` directly
    /// so /health stays responsive even before WAL recovery completes.
    HttpHealth,
    /// GET /ready — readiness probe on the data-plane port (Plan 12.6-01).
    /// Mirrors the admin sidecar's /ready for back-compat with the ~20
    /// TestServer-using test files that poll `base_url()` for readiness.
    HttpReady,
    /// GET /registry — registry snapshot dump on the data-plane port (Plan 12.6-01).
    /// Mirrors the admin sidecar's /registry for back-compat with the
    /// phase4/5/11.5 tests that GET `/registry` for schema-propagation
    /// assertions.
    HttpRegistry,
    /// Plan 12.6-01: HTTP request whose path didn't match the router table.
    /// Distinct from `ParseError` (which covers wire-level decode failures)
    /// so the encoder can return `404 Not Found` for unknown routes — the
    /// legacy axum behaviour that TestServer-using tests assert.
    HttpNotFound { path: String },
    /// Plan 12.6-01: HTTP request whose path matched a route but with the
    /// wrong method.  Encoder returns `405 Method Not Allowed` (the legacy
    /// axum default).
    HttpMethodNotAllowed { method: String, path: String },
    /// Unknown or reserved opcode; contains the raw opcode for error reporting.
    Unknown { op: u16 },
    /// Malformed frame (parse error).
    ParseError { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use beava_core::wire::{CT_JSON, CT_MSGPACK};

    /// Task 9.1 RED: TcpPush carries a body_format byte.
    ///
    /// Constructs a WireRequest::TcpPush with body_format=CT_MSGPACK and
    /// reads back the fields. This test is RED until body_format exists on
    /// the WireRequest variants.
    #[test]
    fn test_tcp_push_carries_body_format() {
        let body = Bytes::from_static(b"\x82\xa5event\xa3Txn\xa4body\x81\xa6amount\x63");
        let req = WireRequest::TcpPush {
            event_name: "Txn".to_string(),
            body: body.clone(),
            body_format: CT_MSGPACK,
        };
        match req {
            WireRequest::TcpPush {
                event_name,
                body: b,
                body_format,
            } => {
                assert_eq!(event_name, "Txn");
                assert_eq!(b, body);
                assert_eq!(body_format, CT_MSGPACK);
                assert_ne!(body_format, CT_JSON);
            }
            other => panic!("expected TcpPush, got {other:?}"),
        }
    }

    /// Task 9.1: HttpPush also carries body_format.
    #[test]
    fn test_http_push_carries_body_format() {
        let body = Bytes::from_static(b"{\"amount\":99}");
        let req = WireRequest::HttpPush {
            event_name: "Txn".to_string(),
            body: body.clone(),
            body_format: CT_JSON,
        };
        match req {
            WireRequest::HttpPush {
                event_name: _,
                body: _,
                body_format,
            } => {
                assert_eq!(body_format, CT_JSON);
            }
            other => panic!("expected HttpPush, got {other:?}"),
        }
    }

    // ─── Plan 12-07 Task 1.a (RED): TcpGet / TcpMGet / TcpGetMulti carry body_format ───

    /// Plan 12-07 Task 1.a: TcpGet (OP_GET, single-key feature read) carries a
    /// body_format byte so the dispatch layer knows whether the payload is JSON
    /// or MessagePack. RED until `WireRequest::TcpGet { body, body_format }` exists.
    #[test]
    fn test_tcp_get_carries_body_format() {
        let body = Bytes::from_static(br#"{"feature":"cnt","key":"alice"}"#);
        let req = WireRequest::TcpGet {
            body: body.clone(),
            body_format: CT_JSON,
        };
        match req {
            WireRequest::TcpGet {
                body: b,
                body_format,
            } => {
                assert_eq!(b, body);
                assert_eq!(body_format, CT_JSON);
                assert_ne!(body_format, CT_MSGPACK);
            }
            other => panic!("expected TcpGet, got {other:?}"),
        }
    }

    /// Plan 12-07 Task 1.a: TcpMGet (OP_MGET, batched single-feature multi-key
    /// read) carries body_format. RED until variant exists.
    #[test]
    fn test_tcp_mget_carries_body_format() {
        let body = Bytes::from_static(b"\x82\xa7feature\xa3cnt\xa4keys\x91\xa5alice");
        let req = WireRequest::TcpMGet {
            body: body.clone(),
            body_format: CT_MSGPACK,
        };
        match req {
            WireRequest::TcpMGet {
                body: b,
                body_format,
            } => {
                assert_eq!(b, body);
                assert_eq!(body_format, CT_MSGPACK);
                assert_ne!(body_format, CT_JSON);
            }
            other => panic!("expected TcpMGet, got {other:?}"),
        }
    }

    /// Plan 12-07 Task 1.a: TcpGetMulti (OP_GET_MULTI, multi-feature multi-key
    /// read) carries body_format. RED until variant exists.
    #[test]
    fn test_tcp_get_multi_carries_body_format() {
        let body = Bytes::from_static(br#"{"keys":["alice"],"features":["cnt"]}"#);
        let req = WireRequest::TcpGetMulti {
            body: body.clone(),
            body_format: CT_JSON,
        };
        match req {
            WireRequest::TcpGetMulti {
                body: b,
                body_format,
            } => {
                assert_eq!(b, body);
                assert_eq!(body_format, CT_JSON);
                assert_ne!(body_format, CT_MSGPACK);
            }
            other => panic!("expected TcpGetMulti, got {other:?}"),
        }
    }
}
