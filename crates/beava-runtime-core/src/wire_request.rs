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
    /// HTTP POST /upsert/:table — table upsert.
    HttpUpsert { table: String, body: Bytes },
    /// HTTP POST /delete/:table — table tombstone.
    HttpDelete { table: String, body: Bytes },
    /// HTTP POST /retract — retraction.
    HttpRetract { body: Bytes },
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
}
