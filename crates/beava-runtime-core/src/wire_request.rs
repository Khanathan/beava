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
    /// `event_name` is the stream name; `body` is the JSON (or future MsgPack) payload.
    TcpPush { event_name: String, body: Bytes },
    /// HTTP POST /push/:event — same semantic as TcpPush but via HTTP.
    HttpPush { event_name: String, body: Bytes },
    /// HTTP POST /push-sync/:event — synchronous push (await fsync).
    HttpPushSync { event_name: String, body: Bytes },
    /// HTTP POST /push-batch/:event — batched push.
    HttpPushBatch { event_name: String, body: Bytes },
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
