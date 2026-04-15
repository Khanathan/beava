//! Binary protocol: frame encoding/decoding, string protocol, command parsing,
//! response serialization. All functions are synchronous (pure byte manipulation).

use crate::engine::pipeline::{FeatureDef, StreamDefinition, ViewDefinition, ViewFeatureDef};
use crate::error::TallyError;
use serde::Deserialize;

// Command opcodes
pub const OP_PUSH: u8 = 0x01;
pub const OP_GET: u8 = 0x02;
pub const OP_SET: u8 = 0x03;
pub const OP_MSET: u8 = 0x04;
pub const OP_REGISTER: u8 = 0x05;
pub const OP_MGET: u8 = 0x06;
pub const OP_PUSH_ASYNC: u8 = 0x07;
pub const OP_FLUSH: u8 = 0x08;
pub const OP_PUSH_BATCH: u8 = 0x0A;
/// Phase 24-02: Upsert a row in a Table source.
///
/// Payload: `[u16 table_name_len][table_name utf-8][u16 key_len][key utf-8][JSON fields object]`
///
/// 0x09 was left as a gap after OP_FLUSH; keeping the new Table opcodes
/// contiguous after OP_PUSH_BATCH (0x0A) is cleaner than back-filling.
pub const OP_PUSH_TABLE: u8 = 0x0B;
/// Phase 24-02: Tombstone a row in a Table source.
///
/// Payload: `[u16 table_name_len][table_name utf-8][u16 key_len][key utf-8]`
pub const OP_DELETE_TABLE: u8 = 0x0C;

/// Phase 25-01: Multi-table feature-vector read.
///
/// Assembles N table rows for a single entity key in one TCP round-trip.
/// Purpose: ML inference paths need 5-20 Tables per prediction; issuing
/// N sequential GETs is a latency amplifier. Null-collapse: never-seen,
/// tombstoned (post-grace filtered at source), and registered-but-empty
/// (pending) all serialize as `null` — indistinguishable at the wire.
///
/// Payload: `[u16 count][count × u16-string table_name][u16-string key]`
/// Response (STATUS_OK): JSON object `{table_name: row_obj | null, ...}`,
/// keys serialized in request order.
///
/// Cardinality guards: `count == 0` is rejected; `count > 256` is rejected
/// (T-25-01-01 DoS mitigation — bounds per-request memory).
pub const OP_GET_MULTI: u8 = 0x0D;

/// Phase 25-01: Reserved opcodes for the v0.x query surface expansion.
///
/// The 0x10..=0x1F range is reserved per v0-restructure-spec §6.3. v0
/// parses these opcodes and returns a typed `NotImplemented` protocol
/// error; the connection stays open so clients can probe capabilities
/// without tearing down their session.
pub const OP_SCAN_RESERVED: u8 = 0x10;

/// Phase 27-02: live-subscribe opcode.
///
/// Request payload (mirrors `OP_SNAPSHOT_FETCH`'s shape exactly):
///   `[u16 BE token_len][token_bytes][Scope]`
///
/// Admin-token is length-prefixed in-band; scope follows and is decoded
/// by `read_scope`. The TCP handler takes ownership of the connection
/// for the lifetime of the subscription — **no other opcodes are
/// accepted on a socket that has sent SUBSCRIBE**. A successful
/// subscription's response is an open-ended stream of per-event frames
/// until the client disconnects or back-pressure drops the subscriber.
///
/// Per-event frame shape (one frame per delivered event — NO global seq):
///   `[u32 BE frame_len][u8 tag=0x03][u64 BE ts_secs][u32 BE ts_nanos]
///    [u32 BE payload_len][payload_bytes]`
///
/// where `payload_bytes` is the serialized event JSON the client
/// originally pushed. `tag=0x03` (`REPLICA_FRAME_TAG_EVENT`) is chosen
/// to disambiguate from `STATUS_ERROR=0x01` and
/// `REPLICA_FRAME_TAG_PAYLOAD=0x02` (user direction §7: pick a
/// non-conflicting tag for events; keep 27-01's 0x01/0x02 shapes
/// intact).
///
/// Auth failure / invalid scope: a single `STATUS_ERROR` frame is
/// emitted and the socket is closed; no registry entry is created.
pub const OP_SUBSCRIBE: u8 = 0x11;

/// Legacy alias retained for tests / clients that still reference the
/// old name. 27-02 promoted this opcode from reserved-stub to
/// live-subscribe; call sites should migrate to `OP_SUBSCRIBE`.
#[deprecated(note = "use OP_SUBSCRIBE — the opcode is no longer a reserved stub")]
pub const OP_SUBSCRIBE_RESERVED: u8 = OP_SUBSCRIBE;

/// Phase 27-01: Scope-aware snapshot fetch for replica clients.
///
/// Wire (request payload, after the frame header):
///   `[u16 BE token_len][token_bytes][Scope]`
///   — the admin bearer token is length-prefixed in-band (no
///     connection-level handshake state); scope bytes follow and are
///     decoded by `read_scope`.
///
/// Response on success: two frames, emitted in order on the same connection:
///   Header frame:  `[u32 BE frame_len=1+8+4][u8 tag=0x01][u64 BE ts_secs][u32 BE ts_nanos]`
///   Payload frame: `[u32 BE frame_len=1+N][u8 tag=0x02][postcard(BaseSnapshotState) bytes]`
///
/// Response on failure (auth / validation / snapshot I/O): a single
/// standard STATUS_ERROR frame (`[u32 len][0x01][error_message]`) — the
/// same shape every other TCP opcode emits on error — and no header or
/// payload frame follows. Clients can distinguish error vs success by the
/// first byte after the length prefix (`0x01` error status vs `0x01`
/// header tag — they collide numerically, so clients must peek the length
/// to decide: a 13-byte frame is a header; everything else on the first
/// post-request read is an error. See `tests/test_replica_snapshot_fetch.rs`).
///
/// `snapshot_taken_at` is the server's `SystemTime::now()` at the moment
/// the handler begins processing the request. It is **response-only** and
/// is never persisted. See `27-CONTEXT.md §snapshot_taken_at semantics`.
pub const OP_SNAPSHOT_FETCH: u8 = 0x12;

// Phase 27-01 snapshot-fetch response-frame tags (tag byte follows the
// u32 BE length prefix; same framing shape as opcode/status frames).
pub const REPLICA_FRAME_TAG_HEADER: u8 = 0x01;
pub const REPLICA_FRAME_TAG_PAYLOAD: u8 = 0x02;

/// Phase 27-02: per-event frame tag on an `OP_SUBSCRIBE` socket.
/// Distinct from `STATUS_ERROR=0x01` and `REPLICA_FRAME_TAG_PAYLOAD=0x02`
/// so SUBSCRIBE clients disambiguate by tag byte (user direction §7).
pub const REPLICA_FRAME_TAG_EVENT: u8 = 0x03;

// Response status codes
pub const STATUS_OK: u8 = 0x00;
pub const STATUS_ERROR: u8 = 0x01;

// Binary event payload type tags (PERF-02)
pub const TYPE_NULL: u8 = 0x00;
pub const TYPE_BOOL: u8 = 0x01;
pub const TYPE_I64: u8 = 0x02;
pub const TYPE_F64: u8 = 0x03;
pub const TYPE_STR: u8 = 0x04;

/// Parsed command from a protocol frame.
#[derive(Debug)]
pub enum Command {
    /// Synchronous push. `payload` is the decoded JSON view of the event,
    /// `raw_payload` is the original binary wire bytes (empty when
    /// synthesized by tests, populated by `parse_command`). The raw bytes
    /// are forwarded to the event log as-is to avoid a re-serialize on the
    /// hot path (Plan 11-06).
    Push {
        stream_name: String,
        payload: serde_json::Value,
        raw_payload: Vec<u8>,
    },
    Get {
        key: String,
    },
    Set {
        key: String,
        payload: serde_json::Value,
    },
    Mset {
        entries: Vec<(String, serde_json::Value)>,
    },
    Register {
        payload: serde_json::Value,
    },
    Mget {
        keys: Vec<String>,
    },
    /// Fire-and-forget async push. Carries raw_payload for the same reason
    /// as `Push` — see that variant's docs.
    PushAsync {
        stream_name: String,
        payload: serde_json::Value,
        raw_payload: Vec<u8>,
    },
    Flush,
    /// Client-side batch of events for one stream. Decoded into per-event
    /// (payload, raw_payload) pairs; converted to Vec<PendingAsync> at the
    /// dispatch site in handle_connection where the seq counter lives
    /// (Research Open Question 1 -- parser has no connection context).
    PushBatch {
        stream_name: String,
        batch_id: u32,
        events: Vec<(serde_json::Value, Vec<u8>)>,
    },
    /// Phase 24-02: Upsert a row in a Table source. `fields` is the JSON
    /// object decoded from the payload tail.
    PushTable {
        table_name: String,
        key: String,
        fields: serde_json::Value,
    },
    /// Phase 24-02: Tombstone a row in a Table source.
    DeleteTable {
        table_name: String,
        key: String,
    },
    /// Phase 25-01: Multi-table feature-vector read. `table_names` preserves
    /// request order so the handler can serialize the response keys in the
    /// same order the client asked for.
    GetMulti {
        table_names: Vec<String>,
        key: String,
    },
    /// Phase 25-01: Reserved opcode landed at the parser. Handler emits a
    /// typed `TallyError::NotImplemented` (STATUS_ERROR on the wire) WITHOUT
    /// tearing down the connection (T-25-01-04). `op_name` identifies which
    /// reserved opcode so the error message remains descriptive.
    ReservedNotImplemented { op_name: &'static str },
    /// Phase 27-01: scope-aware snapshot fetch. Decoded by `parse_command`
    /// from `OP_SNAPSHOT_FETCH` frames; the handler performs auth + scope
    /// validation itself before doing any snapshot I/O.
    ///
    /// `admin_token` is the bearer token the client presented in the frame
    /// payload (length-prefixed u16 before the scope bytes — see
    /// `OP_SNAPSHOT_FETCH` docs). The handler compares it against
    /// `ConcurrentAppState.admin_token`. An empty string means the client
    /// sent a zero-length token, which the handler rejects with
    /// STATUS_ERROR "unauthorized" unless the server also has `admin_token`
    /// set to `Some("")` (not a supported configuration).
    SnapshotFetch { admin_token: String, scope: Scope },
    /// Phase 27-02: open a live-subscribe stream. The wire shape matches
    /// `SnapshotFetch` — the same `[u16 token_len][token][Scope]` payload
    /// mirrored across both replica opcodes. The handler owns the socket
    /// for the subscription's lifetime; `handle_connection` returns after
    /// spawning the drain task, so SUBSCRIBE cannot mix with any other
    /// opcode on the same connection.
    Subscribe { admin_token: String, scope: Scope },
}

/// Phase 27-01: Scope describes the subset of entities a replica client wants.
///
/// Wire shape (big-endian throughout, reuses `read_string`/`write_string`):
///   [u16 n_streams][n_streams × u16-string]           // required, non-empty
///   [u8 has_keys]                                     // 0 or 1
///     if has_keys: [u32 n_keys][n_keys × u16-string]
///   [u8 has_prefix]                                   // 0 or 1
///     if has_prefix: [u16-string prefix]
///   [u16-string pull]                                 // "all" only in v0
///
/// Validation is run separately by `validate_scope` AFTER decoding: the
/// codec accepts any well-formed bytes and the validator rejects by semantic
/// rule. This mirrors the rest of the protocol (`parse_command` is
/// structural, handlers enforce semantics).
#[derive(Debug, Clone, PartialEq)]
pub struct Scope {
    pub streams: Vec<String>,
    pub keys: Option<Vec<String>>,
    pub key_prefix: Option<String>,
    pub pull: String,
}

/// Phase 27-01: structured rejection reasons for `validate_scope`.
///
/// Missing-auth is NOT a variant here — admin-token checks live in the
/// TCP dispatch layer and fire before the handler ever touches a Scope.
#[derive(Debug, Clone, PartialEq)]
pub enum ScopeError {
    EmptyStreams,
    UnknownStream(String),
    KeysAndPrefix,
    PullNotImplemented(String),
    TooManyKeys(usize),
    EmptyPrefix,
    EmptyKey,
}

impl std::fmt::Display for ScopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScopeError::EmptyStreams => write!(f, "scope.streams must be non-empty"),
            ScopeError::UnknownStream(s) => write!(f, "scope.streams references unknown stream: {}", s),
            ScopeError::KeysAndPrefix => write!(f, "scope.keys and scope.key_prefix are mutually exclusive"),
            ScopeError::PullNotImplemented(p) => write!(f, "scope.pull='{}' is not implemented in v0 (only 'all')", p),
            ScopeError::TooManyKeys(n) => write!(f, "scope.keys.len()={} exceeds cap of 10000", n),
            ScopeError::EmptyPrefix => write!(f, "scope.key_prefix must not be an empty string (omit instead)"),
            ScopeError::EmptyKey => write!(f, "scope.keys must not contain empty strings"),
        }
    }
}

/// Phase 27-01: append a Scope to `buf` using the shape documented on `Scope`.
pub fn write_scope(buf: &mut Vec<u8>, scope: &Scope) {
    assert!(
        scope.streams.len() <= u16::MAX as usize,
        "scope.streams too long for u16 len prefix"
    );
    buf.extend_from_slice(&(scope.streams.len() as u16).to_be_bytes());
    for s in &scope.streams {
        buf.extend_from_slice(&write_string(s));
    }
    match &scope.keys {
        Some(keys) => {
            buf.push(1u8);
            assert!(
                keys.len() <= u32::MAX as usize,
                "scope.keys too long for u32 len prefix"
            );
            buf.extend_from_slice(&(keys.len() as u32).to_be_bytes());
            for k in keys {
                buf.extend_from_slice(&write_string(k));
            }
        }
        None => buf.push(0u8),
    }
    match &scope.key_prefix {
        Some(p) => {
            buf.push(1u8);
            buf.extend_from_slice(&write_string(p));
        }
        None => buf.push(0u8),
    }
    buf.extend_from_slice(&write_string(&scope.pull));
}

/// Phase 27-01: decode a Scope from the wire. Advances `*buf` past consumed
/// bytes. Returns `TallyError::Protocol` on truncation / bad UTF-8.
pub fn read_scope(buf: &mut &[u8]) -> Result<Scope, TallyError> {
    if buf.len() < 2 {
        return Err(TallyError::Protocol(
            "scope header truncated: need 2 bytes for n_streams".into(),
        ));
    }
    let n_streams = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    *buf = &buf[2..];
    let mut streams = Vec::with_capacity(n_streams.min(buf.len() / 2));
    for _ in 0..n_streams {
        streams.push(read_string(buf)?);
    }
    if buf.is_empty() {
        return Err(TallyError::Protocol("scope truncated: need has_keys byte".into()));
    }
    let has_keys = buf[0];
    *buf = &buf[1..];
    let keys = match has_keys {
        0 => None,
        1 => {
            if buf.len() < 4 {
                return Err(TallyError::Protocol("scope.keys truncated: need 4 bytes for count".into()));
            }
            let n_keys = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            *buf = &buf[4..];
            // Cap the preallocation against remaining buffer to avoid u32-driven OOM
            let cap = n_keys.min(buf.len() / 2);
            let mut ks = Vec::with_capacity(cap);
            for _ in 0..n_keys {
                ks.push(read_string(buf)?);
            }
            Some(ks)
        }
        other => {
            return Err(TallyError::Protocol(format!(
                "scope has_keys byte must be 0 or 1, got {}",
                other
            )));
        }
    };
    if buf.is_empty() {
        return Err(TallyError::Protocol(
            "scope truncated: need has_prefix byte".into(),
        ));
    }
    let has_prefix = buf[0];
    *buf = &buf[1..];
    let key_prefix = match has_prefix {
        0 => None,
        1 => Some(read_string(buf)?),
        other => {
            return Err(TallyError::Protocol(format!(
                "scope has_prefix byte must be 0 or 1, got {}",
                other
            )));
        }
    };
    let pull = read_string(buf)?;
    Ok(Scope {
        streams,
        keys,
        key_prefix,
        pull,
    })
}

/// Phase 27-01: structural + semantic Scope validator.
///
/// Runs the locked rejection rules from `27-CONTEXT.md §Scope validation`
/// in order. Auth is NOT checked here — the TCP handler gates admin-token
/// BEFORE calling this. Returns the first rule that fires.
pub fn validate_scope(
    scope: &Scope,
    known_streams: &std::collections::HashSet<String>,
) -> Result<(), ScopeError> {
    if scope.streams.is_empty() {
        return Err(ScopeError::EmptyStreams);
    }
    for s in &scope.streams {
        if !known_streams.contains(s) {
            return Err(ScopeError::UnknownStream(s.clone()));
        }
    }
    if scope.keys.is_some() && scope.key_prefix.is_some() {
        return Err(ScopeError::KeysAndPrefix);
    }
    if scope.pull != "all" {
        return Err(ScopeError::PullNotImplemented(scope.pull.clone()));
    }
    if let Some(keys) = &scope.keys {
        if keys.len() > 10_000 {
            return Err(ScopeError::TooManyKeys(keys.len()));
        }
        for k in keys {
            if k.is_empty() {
                return Err(ScopeError::EmptyKey);
            }
        }
    }
    if let Some(prefix) = &scope.key_prefix {
        if prefix.is_empty() {
            return Err(ScopeError::EmptyPrefix);
        }
    }
    Ok(())
}

/// Encode a frame: [4-byte BE length][opcode][payload].
/// Length = 1 (opcode) + payload.len().
pub fn encode_frame(opcode: u8, payload: &[u8]) -> Vec<u8> {
    let length = 1u32 + payload.len() as u32;
    let mut buf = Vec::with_capacity(4 + length as usize);
    buf.extend_from_slice(&length.to_be_bytes());
    buf.push(opcode);
    buf.extend_from_slice(payload);
    buf
}

/// Parse a complete frame buffer: [4-byte BE length][opcode][payload].
/// Returns (opcode, payload slice). The 4-byte length covers opcode + payload.
pub fn parse_frame(data: &[u8]) -> Result<(u8, &[u8]), TallyError> {
    if data.len() < 5 {
        return Err(TallyError::Protocol(
            "frame too short: need at least 5 bytes".into(),
        ));
    }
    let length = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if length == 0 {
        return Err(TallyError::Protocol("frame length is zero".into()));
    }
    if data.len() < 4 + length {
        return Err(TallyError::Protocol(format!(
            "frame truncated: expected {} bytes after header, got {}",
            length,
            data.len() - 4
        )));
    }
    let opcode = data[4];
    let payload = &data[5..4 + length];
    Ok((opcode, payload))
}

/// Phase 27-02: encode one `OP_SUBSCRIBE` per-event frame.
///
/// Wire shape (see `OP_SUBSCRIBE` doc for rationale):
///   `[u32 BE frame_len][u8 tag=REPLICA_FRAME_TAG_EVENT]
///    [u64 BE ts_secs][u32 BE ts_nanos]
///    [u32 BE payload_len][payload_bytes]`
///
/// `timestamp` is converted via `UNIX_EPOCH`; clock-skew (SystemTime pre-
/// epoch) emits zero for both secs and nanos, matching the snapshot-fetch
/// header treatment. `payload` is the raw event JSON bytes — the caller
/// is responsible for serialization.
pub fn encode_event_frame(timestamp: std::time::SystemTime, payload: &[u8]) -> Vec<u8> {
    let (secs, nanos) = match timestamp.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => (d.as_secs(), d.subsec_nanos()),
        Err(_) => (0u64, 0u32),
    };
    // body = tag(1) + ts_secs(8) + ts_nanos(4) + payload_len(4) + payload
    let body_len = 1 + 8 + 4 + 4 + payload.len();
    let mut out = Vec::with_capacity(4 + body_len);
    out.extend_from_slice(&(body_len as u32).to_be_bytes());
    out.push(REPLICA_FRAME_TAG_EVENT);
    out.extend_from_slice(&secs.to_be_bytes());
    out.extend_from_slice(&nanos.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Encode a response: [4-byte BE length][status byte][payload].
/// Length = 1 (status) + payload.len().
pub fn encode_response(status: u8, payload: &[u8]) -> Vec<u8> {
    let length = 1u32 + payload.len() as u32;
    let mut buf = Vec::with_capacity(4 + length as usize);
    buf.extend_from_slice(&length.to_be_bytes());
    buf.push(status);
    buf.extend_from_slice(payload);
    buf
}

/// Read a protocol string: [u16 BE length][UTF-8 bytes].
/// Advances the slice past the consumed bytes.
/// Returns TallyError::Protocol on truncation or invalid UTF-8.
pub fn read_string(buf: &mut &[u8]) -> Result<String, TallyError> {
    if buf.len() < 2 {
        return Err(TallyError::Protocol(
            "string header truncated: need 2 bytes for length".into(),
        ));
    }
    let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    *buf = &buf[2..];
    if buf.len() < len {
        return Err(TallyError::Protocol(format!(
            "string data truncated: expected {} bytes, got {}",
            len,
            buf.len()
        )));
    }
    let s = std::str::from_utf8(&buf[..len])
        .map_err(|e| TallyError::Protocol(format!("invalid UTF-8 in string: {}", e)))?;
    let result = s.to_owned();
    *buf = &buf[len..];
    Ok(result)
}

/// Write a protocol string: [u16 BE length][UTF-8 bytes].
/// Panics if s.len() > u16::MAX (protocol limit).
pub fn write_string(s: &str) -> Vec<u8> {
    assert!(
        s.len() <= u16::MAX as usize,
        "string too long for protocol: {} bytes (max {})",
        s.len(),
        u16::MAX
    );
    let len = s.len() as u16;
    let mut buf = Vec::with_capacity(2 + s.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
    buf
}

/// Read remaining bytes in buffer as JSON.
/// Returns TallyError::Protocol on parse failure.
pub fn read_json_payload(buf: &mut &[u8]) -> Result<serde_json::Value, TallyError> {
    let value = serde_json::from_slice(buf)
        .map_err(|e| TallyError::Protocol(format!("invalid JSON payload: {}", e)))?;
    *buf = &buf[buf.len()..]; // consume all
    Ok(value)
}

/// Decode a binary event payload (PERF-02).
///
/// Wire format:
///   [u16 BE field_count]
///   field := [u16 BE key_len][key utf-8][u8 type_tag][value bytes]
///
/// Type tags:
///   TYPE_NULL (0x00) — 0 value bytes
///   TYPE_BOOL (0x01) — 1 value byte (0 = false, non-zero = true)
///   TYPE_I64  (0x02) — 8 value bytes (big-endian i64)
///   TYPE_F64  (0x03) — 8 value bytes (big-endian f64; NaN/Inf rejected)
///   TYPE_STR  (0x04) — [u16 BE len][utf-8 bytes]
///
/// Returns a `serde_json::Value::Object` with fields inserted in file order.
/// Advances `*buf` past all consumed bytes. Truncation, unknown tags,
/// and non-finite floats return `TallyError::Protocol`.
pub fn decode_event_binary(buf: &mut &[u8]) -> Result<serde_json::Value, TallyError> {
    if buf.len() < 2 {
        return Err(TallyError::Protocol(
            "field_count header truncated: need 2 bytes".into(),
        ));
    }
    let field_count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    *buf = &buf[2..];
    // L-2: bound the pre-allocation against the remaining buffer size.
    // Each field needs at least 4 bytes on the wire (u16 key_len + u8
    // type tag + at least 1 byte of key data), so `buf.len() / 4` is a
    // tight upper bound on the number of decodable fields and keeps
    // an attacker-controlled u16 from triggering a wasted ~1.5 MB map
    // reservation when the payload is actually empty/truncated.
    let cap = field_count.min(buf.len() / 4);
    let mut map = serde_json::Map::with_capacity(cap);
    for _ in 0..field_count {
        let key = read_string(buf)?;
        if buf.is_empty() {
            return Err(TallyError::Protocol("type tag truncated".into()));
        }
        let tag = buf[0];
        *buf = &buf[1..];
        let value = match tag {
            TYPE_NULL => serde_json::Value::Null,
            TYPE_BOOL => {
                if buf.is_empty() {
                    return Err(TallyError::Protocol("bool value truncated".into()));
                }
                let b = buf[0] != 0;
                *buf = &buf[1..];
                serde_json::Value::Bool(b)
            }
            TYPE_I64 => {
                if buf.len() < 8 {
                    return Err(TallyError::Protocol(
                        "i64 value truncated: need 8 bytes".into(),
                    ));
                }
                let i = i64::from_be_bytes(buf[..8].try_into().unwrap());
                *buf = &buf[8..];
                serde_json::Value::Number(i.into())
            }
            TYPE_F64 => {
                if buf.len() < 8 {
                    return Err(TallyError::Protocol(
                        "f64 value truncated: need 8 bytes".into(),
                    ));
                }
                let n = f64::from_be_bytes(buf[..8].try_into().unwrap());
                *buf = &buf[8..];
                if n.is_nan() || n.is_infinite() {
                    return Err(TallyError::Protocol(format!(
                        "f64 value is not finite: {}",
                        n
                    )));
                }
                let num = serde_json::Number::from_f64(n).ok_or_else(|| {
                    TallyError::Protocol("f64 could not be represented as JSON number".into())
                })?;
                serde_json::Value::Number(num)
            }
            TYPE_STR => {
                let s = read_string(buf)?;
                serde_json::Value::String(s)
            }
            _ => {
                return Err(TallyError::Protocol(format!(
                    "unknown type tag 0x{:02x}",
                    tag
                )));
            }
        };
        // I-1: duplicate field keys are silently last-wins. This matches
        // JSON object semantics (and the JSON PUSH path via
        // serde_json::from_slice behaves identically), so clients can
        // rely on the last occurrence of a repeated key. A duplicate is
        // not a protocol error.
        map.insert(key, value);
    }
    Ok(serde_json::Value::Object(map))
}

/// Parse a command from opcode + payload bytes.
///
/// - PUSH (0x01): read_string for stream_name, remaining bytes as JSON
/// - GET (0x02): read_string for key
/// - SET (0x03): read_string for key, remaining bytes as JSON
/// - MSET (0x04): u32 BE count, then for each: read_string key + u32 json_len + json_bytes
/// - REGISTER (0x05): entire payload as JSON
/// - Unknown: TallyError::Protocol
pub fn parse_command(opcode: u8, payload: &[u8]) -> Result<Command, TallyError> {
    let mut buf: &[u8] = payload;
    match opcode {
        OP_PUSH => {
            let stream_name = read_string(&mut buf)?;
            // Capture the raw binary payload bytes before decoding so we
            // can forward them verbatim to the event log (Plan 11-06, no
            // JSON re-serialize on the hot path).
            let raw_payload = buf.to_vec();
            let payload_value = decode_event_binary(&mut buf)?;
            Ok(Command::Push {
                stream_name,
                payload: payload_value,
                raw_payload,
            })
        }
        OP_PUSH_ASYNC => {
            let stream_name = read_string(&mut buf)?;
            let raw_payload = buf.to_vec();
            let payload_value = decode_event_binary(&mut buf)?;
            Ok(Command::PushAsync {
                stream_name,
                payload: payload_value,
                raw_payload,
            })
        }
        OP_FLUSH => Ok(Command::Flush),
        OP_GET => {
            let key = read_string(&mut buf)?;
            Ok(Command::Get { key })
        }
        OP_SET => {
            let key = read_string(&mut buf)?;
            let payload_value = read_json_payload(&mut buf)?;
            Ok(Command::Set {
                key,
                payload: payload_value,
            })
        }
        OP_MSET => {
            if buf.len() < 4 {
                return Err(TallyError::Protocol(
                    "MSET payload too short: need 4 bytes for count".into(),
                ));
            }
            let count = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            buf = &buf[4..];
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key = read_string(&mut buf)?;
                // Each entry has u32 json_len followed by json_bytes
                if buf.len() < 4 {
                    return Err(TallyError::Protocol(
                        "MSET entry truncated: need 4 bytes for json_len".into(),
                    ));
                }
                let json_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                buf = &buf[4..];
                if buf.len() < json_len {
                    return Err(TallyError::Protocol(format!(
                        "MSET entry JSON truncated: expected {} bytes, got {}",
                        json_len,
                        buf.len()
                    )));
                }
                let value: serde_json::Value = serde_json::from_slice(&buf[..json_len])
                    .map_err(|e| TallyError::Protocol(format!("MSET entry invalid JSON: {}", e)))?;
                buf = &buf[json_len..];
                entries.push((key, value));
            }
            Ok(Command::Mset { entries })
        }
        OP_REGISTER => {
            let payload_value = read_json_payload(&mut buf)?;
            Ok(Command::Register {
                payload: payload_value,
            })
        }
        OP_MGET => {
            if buf.len() < 4 {
                return Err(TallyError::Protocol(
                    "MGET payload too short: need 4 bytes for count".into(),
                ));
            }
            let count = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            buf = &buf[4..];
            let mut keys = Vec::with_capacity(count);
            for _ in 0..count {
                keys.push(read_string(&mut buf)?);
            }
            Ok(Command::Mget { keys })
        }
        OP_PUSH_BATCH => {
            let stream_name = read_string(&mut buf)?;
            if buf.len() < 8 {
                return Err(TallyError::Protocol(
                    "PUSH_BATCH header truncated: need 8 bytes for batch_id + count".into(),
                ));
            }
            let batch_id = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            let count = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
            buf = &buf[8..];

            if count > 16_384 {
                return Err(TallyError::Protocol("batch too large".into()));
            }

            let mut events = Vec::with_capacity(count.min(16_384));
            for _ in 0..count {
                if buf.len() < 4 {
                    return Err(TallyError::Protocol(
                        "PUSH_BATCH event length truncated".into(),
                    ));
                }
                let event_len = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
                buf = &buf[4..];
                if buf.len() < event_len {
                    return Err(TallyError::Protocol(format!(
                        "PUSH_BATCH event truncated: expected {} bytes, got {}",
                        event_len,
                        buf.len()
                    )));
                }
                let event_bytes = &buf[..event_len];
                let raw_payload = event_bytes.to_vec();
                let mut event_buf: &[u8] = event_bytes;
                let payload = decode_event_binary(&mut event_buf)?;
                buf = &buf[event_len..];
                events.push((payload, raw_payload));
            }
            Ok(Command::PushBatch {
                stream_name,
                batch_id,
                events,
            })
        }
        OP_PUSH_TABLE => {
            let table_name = read_string(&mut buf)?;
            let key = read_string(&mut buf)?;
            let fields = read_json_payload(&mut buf)?;
            if !fields.is_object() {
                return Err(TallyError::Protocol(
                    "OP_PUSH_TABLE fields payload must be a JSON object".into(),
                ));
            }
            Ok(Command::PushTable {
                table_name,
                key,
                fields,
            })
        }
        OP_DELETE_TABLE => {
            let table_name = read_string(&mut buf)?;
            let key = read_string(&mut buf)?;
            Ok(Command::DeleteTable { table_name, key })
        }
        OP_GET_MULTI => {
            // Wire: [u16 count][count × u16-string table_name][u16-string key]
            if buf.len() < 2 {
                return Err(TallyError::Protocol(
                    "GET_MULTI header truncated: need 2 bytes for count".into(),
                ));
            }
            let count = u16::from_be_bytes([buf[0], buf[1]]) as usize;
            buf = &buf[2..];
            // Cardinality guards (T-25-01-01): reject empty and oversized
            // requests BEFORE allocating per-request memory.
            if count == 0 {
                return Err(TallyError::Protocol(
                    "GET_MULTI requires at least one table_name".into(),
                ));
            }
            if count > 256 {
                return Err(TallyError::Protocol(format!(
                    "GET_MULTI table_names count exceeds 256: got {}",
                    count
                )));
            }
            let mut table_names = Vec::with_capacity(count);
            for _ in 0..count {
                table_names.push(read_string(&mut buf)?);
            }
            let key = read_string(&mut buf)?;
            Ok(Command::GetMulti { table_names, key })
        }
        // Reserved opcodes: we parse them successfully into a marker variant
        // so the frame-level dispatcher does NOT tear down the connection
        // (T-25-01-04). The error is emitted later at the handler boundary
        // where a STATUS_ERROR response + connection-keep-alive is the
        // established flow for logical errors (compare to `unknown table`).
        OP_SCAN_RESERVED => Ok(Command::ReservedNotImplemented { op_name: "SCAN" }),
        OP_SUBSCRIBE => {
            // Phase 27-02: mirrors OP_SNAPSHOT_FETCH's wire shape.
            // `[u16 token_len][token][Scope]`. The handler enforces
            // admin-auth and scope-validation semantically.
            let admin_token = read_string(&mut buf)?;
            let scope = read_scope(&mut buf)?;
            Ok(Command::Subscribe { admin_token, scope })
        }
        OP_SNAPSHOT_FETCH => {
            // Wire: [u16 token_len][token_bytes][scope_bytes]. See the doc
            // comment on `OP_SNAPSHOT_FETCH` for rationale.
            let admin_token = read_string(&mut buf)?;
            let scope = read_scope(&mut buf)?;
            Ok(Command::SnapshotFetch { admin_token, scope })
        }
        _ => Err(TallyError::Protocol(format!(
            "unknown opcode: 0x{:02x}",
            opcode
        ))),
    }
}

// ---------------------------------------------------------------------------
// Duration string parsing
// ---------------------------------------------------------------------------

// Phase 28-01: canonical definitions moved to `crate::duration` so engine
// and state modules can use them under `--features client` (where this
// `server::protocol` module is gated out). These re-exports preserve every
// existing `tally::server::protocol::{FOREVER_TTL, is_forever_ttl,
// parse_duration_str}` call site (including public tests).
pub use crate::duration::{is_forever_ttl, parse_duration_str, FOREVER_TTL};

// ---------------------------------------------------------------------------
// REGISTER DTO types
// ---------------------------------------------------------------------------

/// Intermediate deserialization type for the REGISTER command payload.
/// Uses a flat struct with `feature_type` field instead of an internally tagged enum
/// for simpler Python SDK production.
#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub name: String,
    #[serde(default)]
    pub key_field: Option<String>,
    #[serde(default, rename = "type")]
    pub definition_type: Option<String>, // "stream" (default) or "view"
    pub features: Vec<FeatureDefRequest>,
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub entity_ttl: Option<String>, // e.g., "5m", "1h"
    #[serde(default)]
    pub history_ttl: Option<String>, // e.g., "72h", "7d"
    #[serde(default)]
    pub projection: Option<ProjectionRequest>,
    #[serde(default)]
    pub ephemeral: Option<bool>,
    #[serde(default)]
    pub ttl: Option<String>, // pipeline-level TTL, e.g., "1h"
    #[serde(default)]
    pub max_keys: Option<u64>,
}

/// Projection configuration in the REGISTER JSON payload.
/// Exactly one of `select` or `drop` must be provided (mutual exclusion).
#[derive(Debug, Deserialize)]
pub struct ProjectionRequest {
    #[serde(default)]
    pub select: Option<Vec<String>>,
    #[serde(default)]
    pub drop: Option<Vec<String>>,
}

/// A single feature definition in the REGISTER JSON payload.
#[derive(Debug, Deserialize)]
pub struct FeatureDefRequest {
    pub name: String,
    #[serde(rename = "type")]
    pub feature_type: String,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub window: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub expr: Option<String>,
    #[serde(default)]
    pub optional: Option<bool>,
    #[serde(default, rename = "where")]
    pub where_clause: Option<String>,
    #[serde(default)]
    pub on: Option<String>, // For lookup (used in Plan 03)
    #[serde(default)]
    pub target: Option<String>, // For lookup (used in Plan 03)
    #[serde(default)]
    pub backfill: Option<bool>, // For schema evolution (SCHM-01/02)
    #[serde(default)]
    pub quantile: Option<f64>, // For percentile operator (e.g. 0.95, 0.99)
    #[serde(default)]
    pub n: Option<usize>, // For lag, last_n (count parameter)
    #[serde(default)]
    pub half_life: Option<String>, // For ema (duration string, e.g. "30m")
}

// ---------------------------------------------------------------------------
// DTO to domain conversion
// ---------------------------------------------------------------------------

/// Compute the default bucket granularity: window / 30, clamped to minimum 1 second.
fn default_bucket(window: std::time::Duration) -> std::time::Duration {
    let bucket_nanos = window.as_nanos() / 30;
    let min_bucket = std::time::Duration::from_secs(1);
    let bucket = std::time::Duration::from_nanos(bucket_nanos as u64);
    if bucket < min_bucket {
        min_bucket
    } else {
        bucket
    }
}

/// Convert a RegisterRequest DTO into a StreamDefinition with parsed expressions.
/// Validates name/key_field non-empty, parses duration strings, parses derive expressions.
pub fn convert_register_request(req: RegisterRequest) -> Result<StreamDefinition, TallyError> {
    if req.name.is_empty() {
        return Err(TallyError::Protocol("stream name must not be empty".into()));
    }
    // Validate key_field: if present, must not be empty
    if let Some(ref kf) = req.key_field {
        if kf.is_empty() {
            return Err(TallyError::Protocol("key_field must not be empty".into()));
        }
    }
    // Keyless streams cannot have windowed operators (T-07-01 mitigation)
    if req.key_field.is_none() {
        for f in &req.features {
            let is_windowed = matches!(
                f.feature_type.as_str(),
                "count"
                    | "sum"
                    | "avg"
                    | "min"
                    | "max"
                    | "distinct_count"
                    | "last"
                    | "stddev"
                    | "percentile"
                    | "lag"
                    | "ema"
                    | "last_n"
                    | "first"
                    | "exact_min"
                    | "exact_max"
            );
            if is_windowed {
                return Err(TallyError::Protocol(format!(
                    "keyless stream '{}' cannot have windowed operator '{}'; only derive features are allowed",
                    req.name, f.name
                )));
            }
        }
    }

    let mut features = Vec::with_capacity(req.features.len());
    for f in req.features {
        // Parse optional where clause (shared by all windowed operators)
        let where_expr = match &f.where_clause {
            Some(clause) => {
                let expr = crate::engine::expression::parse_expr(clause).map_err(|e| {
                    TallyError::Protocol(format!(
                        "feature '{}': invalid where expression: {}",
                        f.name, e
                    ))
                })?;
                Some(expr)
            }
            None => None,
        };

        let def = match f.feature_type.as_str() {
            "count" => {
                let window_str = f.window.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': count requires 'window' field",
                        f.name
                    ))
                })?;
                let window = parse_duration_str(&window_str)?;
                let bucket = match f.bucket {
                    Some(b) => parse_duration_str(&b)?,
                    None => default_bucket(window),
                };
                FeatureDef::Count {
                    window,
                    bucket,
                    where_expr,
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "sum" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!("feature '{}': sum requires 'field'", f.name))
                })?;
                let window_str = f.window.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': sum requires 'window' field",
                        f.name
                    ))
                })?;
                let window = parse_duration_str(&window_str)?;
                let bucket = match f.bucket {
                    Some(b) => parse_duration_str(&b)?,
                    None => default_bucket(window),
                };
                FeatureDef::Sum {
                    field,
                    window,
                    bucket,
                    optional: f.optional.unwrap_or(false),
                    where_expr,
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "avg" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!("feature '{}': avg requires 'field'", f.name))
                })?;
                let window_str = f.window.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': avg requires 'window' field",
                        f.name
                    ))
                })?;
                let window = parse_duration_str(&window_str)?;
                let bucket = match f.bucket {
                    Some(b) => parse_duration_str(&b)?,
                    None => default_bucket(window),
                };
                FeatureDef::Avg {
                    field,
                    window,
                    bucket,
                    optional: f.optional.unwrap_or(false),
                    where_expr,
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "min" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!("feature '{}': min requires 'field'", f.name))
                })?;
                let window_str = f.window.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': min requires 'window' field",
                        f.name
                    ))
                })?;
                let window = parse_duration_str(&window_str)?;
                let bucket = match f.bucket {
                    Some(b) => parse_duration_str(&b)?,
                    None => default_bucket(window),
                };
                FeatureDef::Min {
                    field,
                    window,
                    bucket,
                    optional: f.optional.unwrap_or(false),
                    where_expr,
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "max" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!("feature '{}': max requires 'field'", f.name))
                })?;
                let window_str = f.window.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': max requires 'window' field",
                        f.name
                    ))
                })?;
                let window = parse_duration_str(&window_str)?;
                let bucket = match f.bucket {
                    Some(b) => parse_duration_str(&b)?,
                    None => default_bucket(window),
                };
                FeatureDef::Max {
                    field,
                    window,
                    bucket,
                    optional: f.optional.unwrap_or(false),
                    where_expr,
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "last" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!("feature '{}': last requires 'field'", f.name))
                })?;
                FeatureDef::Last {
                    field,
                    optional: f.optional.unwrap_or(false),
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "distinct_count" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': distinct_count requires 'field'",
                        f.name
                    ))
                })?;
                let window_str = f.window.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': distinct_count requires 'window' field",
                        f.name
                    ))
                })?;
                let window = parse_duration_str(&window_str)?;
                let bucket = match f.bucket {
                    Some(b) => parse_duration_str(&b)?,
                    None => default_bucket(window),
                };
                FeatureDef::DistinctCount {
                    field,
                    window,
                    bucket,
                    optional: f.optional.unwrap_or(false),
                    where_expr,
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "stddev" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!("feature '{}': stddev requires 'field'", f.name))
                })?;
                let window_str = f.window.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': stddev requires 'window' field",
                        f.name
                    ))
                })?;
                let window = parse_duration_str(&window_str)?;
                let bucket = match f.bucket {
                    Some(b) => parse_duration_str(&b)?,
                    None => default_bucket(window),
                };
                FeatureDef::Stddev {
                    field,
                    window,
                    bucket,
                    optional: f.optional.unwrap_or(false),
                    where_expr,
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "percentile" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': percentile requires 'field'",
                        f.name
                    ))
                })?;
                let quantile = f.quantile.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': percentile requires 'quantile' field (e.g. 0.95)",
                        f.name
                    ))
                })?;
                if !(0.0..=1.0).contains(&quantile) {
                    return Err(TallyError::Protocol(format!(
                        "feature '{}': quantile must be between 0.0 and 1.0, got {}",
                        f.name, quantile
                    )));
                }
                let window_str = f.window.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': percentile requires 'window' field",
                        f.name
                    ))
                })?;
                let window = parse_duration_str(&window_str)?;
                let bucket = match f.bucket {
                    Some(b) => parse_duration_str(&b)?,
                    None => default_bucket(window),
                };
                FeatureDef::Percentile {
                    field,
                    quantile,
                    window,
                    bucket,
                    optional: f.optional.unwrap_or(false),
                    where_expr,
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "lag" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!("feature '{}': lag requires 'field'", f.name))
                })?;
                let n = f.n.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': lag requires 'n' (positive integer)",
                        f.name
                    ))
                })?;
                if n == 0 {
                    return Err(TallyError::Protocol(format!(
                        "feature '{}': lag 'n' must be >= 1",
                        f.name
                    )));
                }
                FeatureDef::Lag {
                    field,
                    n,
                    optional: f.optional.unwrap_or(false),
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "ema" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!("feature '{}': ema requires 'field'", f.name))
                })?;
                let half_life_str = f.half_life.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': ema requires 'half_life' (duration string, e.g. '30m')",
                        f.name
                    ))
                })?;
                let half_life_dur = parse_duration_str(&half_life_str)?;
                let half_life_secs = half_life_dur.as_secs_f64();
                if half_life_secs <= 0.0 {
                    return Err(TallyError::Protocol(format!(
                        "feature '{}': ema half_life must be positive",
                        f.name
                    )));
                }
                FeatureDef::Ema {
                    field,
                    half_life_secs,
                    optional: f.optional.unwrap_or(false),
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "last_n" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!("feature '{}': last_n requires 'field'", f.name))
                })?;
                let n = f.n.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': last_n requires 'n' (positive integer)",
                        f.name
                    ))
                })?;
                if n == 0 {
                    return Err(TallyError::Protocol(format!(
                        "feature '{}': last_n 'n' must be >= 1",
                        f.name
                    )));
                }
                FeatureDef::LastN {
                    field,
                    n,
                    optional: f.optional.unwrap_or(false),
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "first" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!("feature '{}': first requires 'field'", f.name))
                })?;
                FeatureDef::First {
                    field,
                    optional: f.optional.unwrap_or(false),
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "exact_min" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': exact_min requires 'field'",
                        f.name
                    ))
                })?;
                let window_str = f.window.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': exact_min requires 'window' field",
                        f.name
                    ))
                })?;
                let window = parse_duration_str(&window_str)?;
                let bucket = match f.bucket {
                    Some(b) => parse_duration_str(&b)?,
                    None => default_bucket(window),
                };
                FeatureDef::ExactMin {
                    field,
                    window,
                    bucket,
                    optional: f.optional.unwrap_or(false),
                    where_expr,
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "exact_max" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': exact_max requires 'field'",
                        f.name
                    ))
                })?;
                let window_str = f.window.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': exact_max requires 'window' field",
                        f.name
                    ))
                })?;
                let window = parse_duration_str(&window_str)?;
                let bucket = match f.bucket {
                    Some(b) => parse_duration_str(&b)?,
                    None => default_bucket(window),
                };
                FeatureDef::ExactMax {
                    field,
                    window,
                    bucket,
                    optional: f.optional.unwrap_or(false),
                    where_expr,
                    backfill: f.backfill.unwrap_or(false),
                }
            }
            "derive" => {
                let expr_str = f.expr.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': derive requires 'expr' field",
                        f.name
                    ))
                })?;
                let expr = crate::engine::expression::parse_expr(&expr_str).map_err(|e| {
                    TallyError::Protocol(format!("feature '{}': invalid expression: {}", f.name, e))
                })?;
                FeatureDef::Derive { expr }
            }
            unknown => {
                return Err(TallyError::Protocol(format!(
                    "unknown feature type: {}",
                    unknown
                )));
            }
        };
        features.push((f.name, def));
    }

    let filter = req
        .filter
        .map(|f| crate::engine::expression::parse_expr(&f))
        .transpose()
        .map_err(|e| TallyError::Protocol(format!("invalid filter expression: {}", e)))?;

    let entity_ttl = req.entity_ttl.map(|s| parse_duration_str(&s)).transpose()?;
    let history_ttl = req
        .history_ttl
        .map(|s| parse_duration_str(&s))
        .transpose()?;

    // Parse projection: validate select/drop mutual exclusion
    let projection = match req.projection {
        Some(pr) => match (pr.select, pr.drop) {
            (Some(sel), None) => Some(crate::engine::pipeline::Projection::Select(
                sel.into_iter().collect(),
            )),
            (None, Some(drp)) => Some(crate::engine::pipeline::Projection::Drop(
                drp.into_iter().collect(),
            )),
            (Some(_), Some(_)) => {
                return Err(TallyError::Protocol(
                    "projection: select and drop are mutually exclusive".into(),
                ));
            }
            (None, None) => None,
        },
        None => None,
    };

    // Parse pipeline-level TTL
    let pipeline_ttl = req.ttl.map(|s| parse_duration_str(&s)).transpose()?;

    Ok(StreamDefinition {
        name: req.name,
        key_field: req.key_field,
        group_by_keys: None,
        features,
        depends_on: req.depends_on,
        filter,
        entity_ttl,
        history_ttl,
        projection,
        ephemeral: req.ephemeral,
        pipeline_ttl,
        max_keys: req.max_keys,
    })
}

/// Convert a RegisterRequest DTO into a ViewDefinition.
/// Only "derive" and "lookup" feature types are allowed in views.
pub fn convert_view_register_request(req: RegisterRequest) -> Result<ViewDefinition, TallyError> {
    if req.name.is_empty() {
        return Err(TallyError::Protocol("view name must not be empty".into()));
    }
    let key_field = req
        .key_field
        .ok_or_else(|| TallyError::Protocol("view key_field must not be empty".into()))?;
    if key_field.is_empty() {
        return Err(TallyError::Protocol("key_field must not be empty".into()));
    }

    let mut features = Vec::with_capacity(req.features.len());
    for f in req.features {
        let vdef = match f.feature_type.as_str() {
            "derive" => {
                let expr_str = f.expr.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': derive requires 'expr' field",
                        f.name
                    ))
                })?;
                let expr = crate::engine::expression::parse_expr(&expr_str).map_err(|e| {
                    TallyError::Protocol(format!("feature '{}': invalid expression: {}", f.name, e))
                })?;
                ViewFeatureDef::Derive { expr }
            }
            "lookup" => {
                let target = f.target.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': lookup requires 'target' field (e.g. 'StreamName.feature_name')",
                        f.name
                    ))
                })?;
                let on_field = f.on.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': lookup requires 'on' field",
                        f.name
                    ))
                })?;
                // Parse "StreamName.feature_name" into stream and feature parts
                let parts: Vec<&str> = target.splitn(2, '.').collect();
                if parts.len() != 2 {
                    return Err(TallyError::Protocol(format!(
                        "feature '{}': lookup target must be 'StreamName.feature_name', got '{}'",
                        f.name, target
                    )));
                }
                ViewFeatureDef::Lookup {
                    target_stream: parts[0].to_string(),
                    target_feature: parts[1].to_string(),
                    on_field,
                }
            }
            other => {
                return Err(TallyError::Protocol(format!(
                    "view feature '{}': only supports 'derive' and 'lookup' types, got '{}'",
                    f.name, other
                )));
            }
        };
        features.push((f.name, vdef));
    }

    Ok(ViewDefinition {
        name: req.name,
        key_field,
        features,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Frame encoding/decoding tests ---

    #[test]
    fn test_encode_frame_push_hello() {
        // opcode 0x01, payload b"hello" -> [0,0,0,6, 0x01, h,e,l,l,o]
        let result = encode_frame(OP_PUSH, b"hello");
        assert_eq!(result, vec![0, 0, 0, 6, 0x01, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_parse_frame_push_hello() {
        // [0,0,0,6, 0x01, h,e,l,l,o] -> (opcode=0x01, payload=b"hello")
        let data = vec![0u8, 0, 0, 6, 0x01, b'h', b'e', b'l', b'l', b'o'];
        let (opcode, payload) = parse_frame(&data).unwrap();
        assert_eq!(opcode, OP_PUSH);
        assert_eq!(payload, b"hello");
    }

    #[test]
    fn test_encode_response_ok() {
        // status OK (0x00) and payload b"data" -> [0,0,0,5, 0x00, d,a,t,a]
        let result = encode_response(STATUS_OK, b"data");
        assert_eq!(result, vec![0, 0, 0, 5, 0x00, b'd', b'a', b't', b'a']);
    }

    #[test]
    fn test_encode_response_error() {
        // status Error (0x01) and payload b"fail" -> [0,0,0,5, 0x01, f,a,i,l]
        let result = encode_response(STATUS_ERROR, b"fail");
        assert_eq!(result, vec![0, 0, 0, 5, 0x01, b'f', b'a', b'i', b'l']);
    }

    #[test]
    fn test_frame_roundtrip() {
        let encoded = encode_frame(OP_SET, b"test_payload");
        let (opcode, payload) = parse_frame(&encoded).unwrap();
        assert_eq!(opcode, OP_SET);
        assert_eq!(payload, b"test_payload");
    }

    // --- String protocol tests ---

    #[test]
    fn test_read_string_hi() {
        let data: &[u8] = &[0, 2, 0x68, 0x69];
        let mut buf = data;
        let s = read_string(&mut buf).unwrap();
        assert_eq!(s, "hi");
        assert!(buf.is_empty()); // fully consumed
    }

    #[test]
    fn test_read_string_empty() {
        let data: &[u8] = &[0, 0];
        let mut buf = data;
        let s = read_string(&mut buf).unwrap();
        assert_eq!(s, "");
    }

    #[test]
    fn test_read_string_truncated() {
        // claims 5 bytes but only 3 available
        let data: &[u8] = &[0, 5, 0x68, 0x69, 0x70];
        let mut buf = data;
        let result = read_string(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_string_hi() {
        let result = write_string("hi");
        assert_eq!(result, vec![0, 2, 0x68, 0x69]);
    }

    #[test]
    fn test_string_roundtrip() {
        let original = "hello world";
        let encoded = write_string(original);
        let mut buf: &[u8] = &encoded;
        let decoded = read_string(&mut buf).unwrap();
        assert_eq!(decoded, original);
        assert!(buf.is_empty());
    }

    // --- Command parsing tests ---

    // --- Binary PUSH payload test helpers ---

    fn build_binary_push_payload(stream: &str, fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut buf = write_string(stream);
        buf.extend_from_slice(&(fields.len() as u16).to_be_bytes());
        for (k, v) in fields {
            buf.extend_from_slice(&write_string(k));
            match v {
                serde_json::Value::Null => buf.push(TYPE_NULL),
                serde_json::Value::Bool(b) => {
                    buf.push(TYPE_BOOL);
                    buf.push(if *b { 1 } else { 0 });
                }
                serde_json::Value::Number(n) if n.is_i64() => {
                    buf.push(TYPE_I64);
                    buf.extend_from_slice(&n.as_i64().unwrap().to_be_bytes());
                }
                serde_json::Value::Number(n) => {
                    buf.push(TYPE_F64);
                    buf.extend_from_slice(&n.as_f64().unwrap().to_be_bytes());
                }
                serde_json::Value::String(s) => {
                    buf.push(TYPE_STR);
                    buf.extend_from_slice(&write_string(s));
                }
                _ => panic!("unsupported test fixture type"),
            }
        }
        buf
    }

    fn build_event_only(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(fields.len() as u16).to_be_bytes());
        for (k, v) in fields {
            buf.extend_from_slice(&write_string(k));
            match v {
                serde_json::Value::Null => buf.push(TYPE_NULL),
                serde_json::Value::Bool(b) => {
                    buf.push(TYPE_BOOL);
                    buf.push(if *b { 1 } else { 0 });
                }
                serde_json::Value::Number(n) if n.is_i64() => {
                    buf.push(TYPE_I64);
                    buf.extend_from_slice(&n.as_i64().unwrap().to_be_bytes());
                }
                serde_json::Value::Number(n) => {
                    buf.push(TYPE_F64);
                    buf.extend_from_slice(&n.as_f64().unwrap().to_be_bytes());
                }
                serde_json::Value::String(s) => {
                    buf.push(TYPE_STR);
                    buf.extend_from_slice(&write_string(s));
                }
                _ => panic!("unsupported test fixture type"),
            }
        }
        buf
    }

    #[test]
    fn test_parse_command_push() {
        // PUSH uses binary payload format (Phase 11)
        let payload = build_binary_push_payload(
            "Transactions",
            &[
                ("user_id", serde_json::json!("u123")),
                ("amount", serde_json::json!(50.0)),
            ],
        );
        let cmd = parse_command(OP_PUSH, &payload).unwrap();
        match cmd {
            Command::Push {
                stream_name,
                payload,
                raw_payload,
            } => {
                assert_eq!(stream_name, "Transactions");
                assert_eq!(payload["user_id"], "u123");
                assert_eq!(payload["amount"], 50.0);
                assert!(!raw_payload.is_empty(), "raw binary bytes must be captured");
            }
            _ => panic!("expected Push command"),
        }
    }

    #[test]
    fn test_parse_command_push_binary() {
        let payload = build_binary_push_payload("tx", &[("user_id", serde_json::json!("u1"))]);
        let cmd = parse_command(OP_PUSH, &payload).unwrap();
        match cmd {
            Command::Push {
                stream_name,
                payload,
                raw_payload,
            } => {
                assert_eq!(stream_name, "tx");
                assert_eq!(payload["user_id"], "u1");
                assert!(!raw_payload.is_empty());
            }
            _ => panic!("expected Push"),
        }
    }

    #[test]
    fn test_parse_command_push_async_binary() {
        let payload = build_binary_push_payload("tx", &[("user_id", serde_json::json!("u1"))]);
        let cmd = parse_command(OP_PUSH_ASYNC, &payload).unwrap();
        match cmd {
            Command::PushAsync {
                stream_name,
                payload,
                raw_payload,
            } => {
                assert_eq!(stream_name, "tx");
                assert_eq!(payload["user_id"], "u1");
                assert!(!raw_payload.is_empty());
            }
            _ => panic!("expected PushAsync"),
        }
    }

    #[test]
    fn test_parse_command_flush() {
        let cmd = parse_command(OP_FLUSH, &[]).unwrap();
        assert!(matches!(cmd, Command::Flush));
    }

    #[test]
    fn test_parse_command_push_rejects_json() {
        // v1.1 JSON-style payload should now fail: the JSON '{' byte (0x7B)
        // is interpreted as a u16 field_count header, leading to truncation or unknown tag.
        let mut payload = write_string("tx");
        payload.extend_from_slice(b"{\"user_id\":\"u1\"}");
        let result = parse_command(OP_PUSH, &payload);
        assert!(
            result.is_err(),
            "v1.1 JSON payload must be rejected by the binary decoder"
        );
    }

    #[test]
    fn test_parse_command_unknown_opcode_still_errors() {
        let result = parse_command(0xFE, &[]);
        assert!(result.is_err());
        match result.unwrap_err() {
            TallyError::Protocol(msg) => assert!(msg.contains("unknown opcode")),
            _ => panic!("expected Protocol error"),
        }
    }

    // --- decode_event_binary tests ---

    #[test]
    fn test_decode_event_binary_empty() {
        let data = build_event_only(&[]);
        let mut buf: &[u8] = &data;
        let v = decode_event_binary(&mut buf).unwrap();
        assert_eq!(v, serde_json::json!({}));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_decode_event_binary_null() {
        let data = build_event_only(&[("x", serde_json::Value::Null)]);
        let mut buf: &[u8] = &data;
        let v = decode_event_binary(&mut buf).unwrap();
        assert_eq!(v["x"], serde_json::Value::Null);
    }

    #[test]
    fn test_decode_event_binary_bool_true() {
        let data = build_event_only(&[("x", serde_json::json!(true))]);
        let mut buf: &[u8] = &data;
        let v = decode_event_binary(&mut buf).unwrap();
        assert_eq!(v["x"], serde_json::Value::Bool(true));
    }

    #[test]
    fn test_decode_event_binary_bool_false() {
        let data = build_event_only(&[("x", serde_json::json!(false))]);
        let mut buf: &[u8] = &data;
        let v = decode_event_binary(&mut buf).unwrap();
        assert_eq!(v["x"], serde_json::Value::Bool(false));
    }

    #[test]
    fn test_decode_event_binary_i64_positive() {
        let data = build_event_only(&[("n", serde_json::json!(42_i64))]);
        let mut buf: &[u8] = &data;
        let v = decode_event_binary(&mut buf).unwrap();
        assert_eq!(v["n"].as_i64(), Some(42));
    }

    #[test]
    fn test_decode_event_binary_i64_negative() {
        let data = build_event_only(&[("n", serde_json::json!(-42_i64))]);
        let mut buf: &[u8] = &data;
        let v = decode_event_binary(&mut buf).unwrap();
        assert_eq!(v["n"].as_i64(), Some(-42));
    }

    #[test]
    #[allow(clippy::approx_constant)]
    fn test_decode_event_binary_f64() {
        let data = build_event_only(&[("f", serde_json::json!(3.14))]);
        let mut buf: &[u8] = &data;
        let v = decode_event_binary(&mut buf).unwrap();
        assert!((v["f"].as_f64().unwrap() - 3.14).abs() < 1e-9);
    }

    #[test]
    fn test_decode_event_binary_string() {
        let data = build_event_only(&[("s", serde_json::json!("hello"))]);
        let mut buf: &[u8] = &data;
        let v = decode_event_binary(&mut buf).unwrap();
        assert_eq!(v["s"], "hello");
    }

    #[test]
    fn test_decode_event_binary_mixed() {
        let data = build_event_only(&[
            ("a", serde_json::Value::Null),
            ("b", serde_json::json!(true)),
            ("c", serde_json::json!(7_i64)),
            ("d", serde_json::json!(1.5)),
            ("e", serde_json::json!("x")),
        ]);
        let mut buf: &[u8] = &data;
        let v = decode_event_binary(&mut buf).unwrap();
        assert_eq!(v["a"], serde_json::Value::Null);
        assert_eq!(v["b"], true);
        assert_eq!(v["c"].as_i64(), Some(7));
        assert!((v["d"].as_f64().unwrap() - 1.5).abs() < 1e-9);
        assert_eq!(v["e"], "x");
    }

    #[test]
    fn test_decode_event_binary_truncated_field_count() {
        let data: [u8; 0] = [];
        let mut buf: &[u8] = &data;
        assert!(decode_event_binary(&mut buf).is_err());
    }

    #[test]
    fn test_decode_event_binary_truncated_type_tag() {
        // field_count=1, key="k", then nothing
        let mut data = Vec::new();
        data.extend_from_slice(&1u16.to_be_bytes());
        data.extend_from_slice(&write_string("k"));
        let mut buf: &[u8] = &data;
        let r = decode_event_binary(&mut buf);
        assert!(r.is_err());
    }

    #[test]
    fn test_decode_event_binary_truncated_i64() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u16.to_be_bytes());
        data.extend_from_slice(&write_string("n"));
        data.push(TYPE_I64);
        data.extend_from_slice(&[0, 0, 0, 0]); // only 4 bytes
        let mut buf: &[u8] = &data;
        assert!(decode_event_binary(&mut buf).is_err());
    }

    #[test]
    fn test_decode_event_binary_f64_nan() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u16.to_be_bytes());
        data.extend_from_slice(&write_string("f"));
        data.push(TYPE_F64);
        data.extend_from_slice(&f64::NAN.to_be_bytes());
        let mut buf: &[u8] = &data;
        let r = decode_event_binary(&mut buf);
        assert!(r.is_err());
        let msg = format!("{:?}", r.unwrap_err());
        assert!(
            msg.contains("not finite"),
            "expected 'not finite' in {}",
            msg
        );
    }

    #[test]
    fn test_decode_event_binary_f64_inf() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u16.to_be_bytes());
        data.extend_from_slice(&write_string("f"));
        data.push(TYPE_F64);
        data.extend_from_slice(&f64::INFINITY.to_be_bytes());
        let mut buf: &[u8] = &data;
        assert!(decode_event_binary(&mut buf).is_err());
    }

    #[test]
    fn test_decode_event_binary_unknown_tag() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u16.to_be_bytes());
        data.extend_from_slice(&write_string("k"));
        data.push(0xFF);
        let mut buf: &[u8] = &data;
        let r = decode_event_binary(&mut buf);
        assert!(r.is_err());
        let msg = format!("{:?}", r.unwrap_err());
        assert!(msg.contains("unknown type tag"));
    }

    #[test]
    fn test_decode_event_binary_advances_buffer() {
        let data = build_event_only(&[
            ("a", serde_json::json!(1_i64)),
            ("b", serde_json::json!("z")),
        ]);
        let mut buf: &[u8] = &data;
        let _ = decode_event_binary(&mut buf).unwrap();
        assert!(buf.is_empty(), "decoder should consume all bytes");
    }

    #[test]
    fn test_parse_command_get() {
        // GET: key string
        let payload = write_string("u123");
        let cmd = parse_command(OP_GET, &payload).unwrap();
        match cmd {
            Command::Get { key } => assert_eq!(key, "u123"),
            _ => panic!("expected Get command"),
        }
    }

    #[test]
    fn test_parse_command_set() {
        // SET: key string + JSON payload
        let mut payload = Vec::new();
        payload.extend_from_slice(&write_string("u123"));
        payload.extend_from_slice(b"{\"score\":0.95}");
        let cmd = parse_command(OP_SET, &payload).unwrap();
        match cmd {
            Command::Set { key, payload } => {
                assert_eq!(key, "u123");
                assert_eq!(payload["score"], 0.95);
            }
            _ => panic!("expected Set command"),
        }
    }

    // ----- Phase 24-02: OP_PUSH_TABLE / OP_DELETE_TABLE parse_command tests -----

    #[test]
    fn op_push_table_roundtrip_via_parse_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&write_string("UserProfile"));
        payload.extend_from_slice(&write_string("u123"));
        payload.extend_from_slice(br#"{"country":"US","score":42}"#);
        let cmd = parse_command(OP_PUSH_TABLE, &payload).unwrap();
        match cmd {
            Command::PushTable {
                table_name,
                key,
                fields,
            } => {
                assert_eq!(table_name, "UserProfile");
                assert_eq!(key, "u123");
                assert_eq!(fields["country"], "US");
                assert_eq!(fields["score"], 42);
            }
            _ => panic!("expected PushTable command"),
        }
    }

    #[test]
    fn op_push_table_rejects_non_object_fields() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&write_string("T"));
        payload.extend_from_slice(&write_string("k"));
        payload.extend_from_slice(b"[1,2,3]");
        let err = parse_command(OP_PUSH_TABLE, &payload).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("JSON object"), "msg was {}", msg);
    }

    #[test]
    fn op_delete_table_roundtrip_via_parse_command() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&write_string("UserProfile"));
        payload.extend_from_slice(&write_string("u123"));
        let cmd = parse_command(OP_DELETE_TABLE, &payload).unwrap();
        match cmd {
            Command::DeleteTable { table_name, key } => {
                assert_eq!(table_name, "UserProfile");
                assert_eq!(key, "u123");
            }
            _ => panic!("expected DeleteTable command"),
        }
    }

    // ----- Phase 25-01: OP_GET_MULTI + reserved opcodes parse_command tests -----

    fn build_get_multi_payload(tables: &[&str], key: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(tables.len() as u16).to_be_bytes());
        for t in tables {
            buf.extend_from_slice(&write_string(t));
        }
        buf.extend_from_slice(&write_string(key));
        buf
    }

    #[test]
    fn op_get_multi_happy_path() {
        let payload = build_get_multi_payload(&["UserProfile", "RiskScore"], "u123");
        let cmd = parse_command(OP_GET_MULTI, &payload).unwrap();
        match cmd {
            Command::GetMulti { table_names, key } => {
                assert_eq!(table_names, vec!["UserProfile", "RiskScore"]);
                assert_eq!(key, "u123");
            }
            _ => panic!("expected GetMulti"),
        }
    }

    #[test]
    fn op_get_multi_rejects_zero_count() {
        let payload = build_get_multi_payload(&[], "u1");
        let err = parse_command(OP_GET_MULTI, &payload).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("at least one table_name"), "got: {}", msg);
    }

    #[test]
    fn op_get_multi_rejects_oversized_count() {
        // Build a hand-crafted payload with count=257 but only one short
        // name — parse_command must reject at the cardinality guard
        // BEFORE attempting to read 257 strings.
        let mut payload = Vec::new();
        payload.extend_from_slice(&257u16.to_be_bytes());
        payload.extend_from_slice(&write_string("T"));
        payload.extend_from_slice(&write_string("k"));
        let err = parse_command(OP_GET_MULTI, &payload).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("exceeds 256"), "got: {}", msg);
    }

    #[test]
    fn op_get_multi_truncated_payload_errors_without_panic() {
        // Empty payload: no count bytes.
        let err = parse_command(OP_GET_MULTI, &[]).unwrap_err();
        assert!(matches!(err, TallyError::Protocol(_)));

        // Count claims 2 but only one string follows.
        let mut payload = Vec::new();
        payload.extend_from_slice(&2u16.to_be_bytes());
        payload.extend_from_slice(&write_string("A"));
        // No second table string, no key — truncation.
        let err = parse_command(OP_GET_MULTI, &payload).unwrap_err();
        assert!(matches!(err, TallyError::Protocol(_)));
    }

    #[test]
    fn op_get_multi_accepts_max_count_256() {
        // Boundary: count=256 should succeed.
        let names: Vec<String> = (0..256).map(|i| format!("T{}", i)).collect();
        let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        let payload = build_get_multi_payload(&refs, "k");
        let cmd = parse_command(OP_GET_MULTI, &payload).unwrap();
        match cmd {
            Command::GetMulti { table_names, key } => {
                assert_eq!(table_names.len(), 256);
                assert_eq!(key, "k");
            }
            _ => panic!("expected GetMulti"),
        }
    }

    #[test]
    fn op_scan_reserved_parses_as_marker_variant() {
        // Reserved opcodes parse successfully — the handler boundary
        // converts the marker to NotImplemented to keep the connection alive.
        let cmd = parse_command(OP_SCAN_RESERVED, &[]).unwrap();
        match cmd {
            Command::ReservedNotImplemented { op_name } => {
                assert_eq!(op_name, "SCAN");
            }
            other => panic!("expected ReservedNotImplemented, got {:?}", other),
        }
    }

    #[test]
    fn op_subscribe_parses_token_and_scope() {
        // Phase 27-02: OP_SUBSCRIBE was promoted from reserved-stub to a
        // real opcode. Wire shape matches OP_SNAPSHOT_FETCH:
        //   [u16 token_len][token][Scope]
        let mut payload = Vec::new();
        payload.extend_from_slice(&write_string("tok"));
        let scope = Scope {
            streams: vec!["orders".into()],
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        };
        write_scope(&mut payload, &scope);
        let cmd = parse_command(OP_SUBSCRIBE, &payload).unwrap();
        match cmd {
            Command::Subscribe { admin_token, scope } => {
                assert_eq!(admin_token, "tok");
                assert_eq!(scope.streams, vec!["orders".to_string()]);
                assert_eq!(scope.pull, "all");
            }
            other => panic!("expected Subscribe, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_command_mset() {
        // MSET: u32 count, then [string key][u32 json_len][json_bytes] per entry
        let mut payload = Vec::new();
        let count: u32 = 2;
        payload.extend_from_slice(&count.to_be_bytes());

        // Entry 1: "u123" -> {"score": 0.95}
        payload.extend_from_slice(&write_string("u123"));
        let json1 = b"{\"score\":0.95}";
        payload.extend_from_slice(&(json1.len() as u32).to_be_bytes());
        payload.extend_from_slice(json1);

        // Entry 2: "u456" -> {"score": 0.5}
        payload.extend_from_slice(&write_string("u456"));
        let json2 = b"{\"score\":0.5}";
        payload.extend_from_slice(&(json2.len() as u32).to_be_bytes());
        payload.extend_from_slice(json2);

        let cmd = parse_command(OP_MSET, &payload).unwrap();
        match cmd {
            Command::Mset { entries } => {
                assert_eq!(entries.len(), 2);
                assert_eq!(entries[0].0, "u123");
                assert_eq!(entries[0].1["score"], 0.95);
                assert_eq!(entries[1].0, "u456");
                assert_eq!(entries[1].1["score"], 0.5);
            }
            _ => panic!("expected Mset command"),
        }
    }

    #[test]
    fn test_parse_command_register() {
        let json = b"{\"name\":\"Transactions\",\"key_field\":\"user_id\"}";
        let cmd = parse_command(OP_REGISTER, json).unwrap();
        match cmd {
            Command::Register { payload } => {
                assert_eq!(payload["name"], "Transactions");
                assert_eq!(payload["key_field"], "user_id");
            }
            _ => panic!("expected Register command"),
        }
    }

    #[test]
    fn test_parse_command_unknown_opcode() {
        let result = parse_command(0xFF, &[]);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("unknown opcode"));
    }

    #[test]
    fn test_parse_frame_zero_length_rejected() {
        // T-02-03 mitigation: length=0 is rejected
        let data = vec![0u8, 0, 0, 0, 0x01];
        let result = parse_frame(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_frame_truncated_rejected() {
        // Claims 100 bytes but only has 5
        let data = vec![0u8, 0, 0, 100, 0x01, 0x02, 0x03, 0x04, 0x05];
        let result = parse_frame(&data);
        assert!(result.is_err());
    }

    // --- Duration parsing tests ---

    #[test]
    fn test_parse_duration_30m() {
        let d = parse_duration_str("30m").unwrap();
        assert_eq!(d, std::time::Duration::from_secs(1800));
    }

    #[test]
    fn test_parse_duration_1h() {
        let d = parse_duration_str("1h").unwrap();
        assert_eq!(d, std::time::Duration::from_secs(3600));
    }

    #[test]
    fn test_parse_duration_24h() {
        let d = parse_duration_str("24h").unwrap();
        assert_eq!(d, std::time::Duration::from_secs(86400));
    }

    #[test]
    fn test_parse_duration_30s() {
        let d = parse_duration_str("30s").unwrap();
        assert_eq!(d, std::time::Duration::from_secs(30));
    }

    #[test]
    fn test_parse_duration_7d() {
        let d = parse_duration_str("7d").unwrap();
        assert_eq!(d, std::time::Duration::from_secs(604800));
    }

    #[test]
    fn test_parse_duration_500ms() {
        let d = parse_duration_str("500ms").unwrap();
        assert_eq!(d, std::time::Duration::from_millis(500));
    }

    #[test]
    fn test_parse_duration_empty() {
        assert!(parse_duration_str("").is_err());
    }

    #[test]
    fn test_parse_duration_abc() {
        assert!(parse_duration_str("abc").is_err());
    }

    // --- REGISTER DTO and conversion tests ---

    #[test]
    fn test_register_request_count_feature() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [{
                "name": "tx_count_1h",
                "type": "count",
                "window": "1h"
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        assert_eq!(stream.name, "Transactions");
        assert_eq!(stream.key_field, Some("user_id".into()));
        assert_eq!(stream.features.len(), 1);
        assert_eq!(stream.features[0].0, "tx_count_1h");
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::Count { window, bucket, .. } => {
                assert_eq!(*window, std::time::Duration::from_secs(3600));
                // Default bucket = window / 30 = 120s = 2m
                assert_eq!(*bucket, std::time::Duration::from_secs(120));
            }
            other => panic!("expected Count, got {:?}", other),
        }
    }

    #[test]
    fn test_default_bucket_clamped_to_1s_minimum() {
        // 30s window / 30 = 1s (exactly at minimum)
        let b = default_bucket(std::time::Duration::from_secs(30));
        assert_eq!(b, std::time::Duration::from_secs(1));
        // 10s window / 30 = 0.33s -> clamped to 1s
        let b2 = default_bucket(std::time::Duration::from_secs(10));
        assert_eq!(b2, std::time::Duration::from_secs(1));
    }

    #[test]
    fn test_register_request_sum_feature() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [{
                "name": "tx_sum_1h",
                "type": "sum",
                "field": "amount",
                "window": "1h"
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::Sum {
                field,
                window,
                bucket,
                optional,
                ..
            } => {
                assert_eq!(field, "amount");
                assert_eq!(*window, std::time::Duration::from_secs(3600));
                assert_eq!(*bucket, std::time::Duration::from_secs(120));
                assert!(!optional);
            }
            other => panic!("expected Sum, got {:?}", other),
        }
    }

    #[test]
    fn test_register_request_avg_feature() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [{
                "name": "avg_amount_1h",
                "type": "avg",
                "field": "amount",
                "window": "1h",
                "optional": true
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::Avg {
                field,
                window,
                bucket,
                optional,
                ..
            } => {
                assert_eq!(field, "amount");
                assert_eq!(*window, std::time::Duration::from_secs(3600));
                assert_eq!(*bucket, std::time::Duration::from_secs(120));
                assert!(*optional);
            }
            other => panic!("expected Avg, got {:?}", other),
        }
    }

    #[test]
    fn test_register_request_derive_feature() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [{
                "name": "failure_rate",
                "type": "derive",
                "expr": "failed_count_1h / count_1h"
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::Derive { expr: _ } => {
                // Expression was parsed successfully
            }
            other => panic!("expected Derive, got {:?}", other),
        }
    }

    #[test]
    fn test_register_request_invalid_expression_returns_error() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [{
                "name": "bad",
                "type": "derive",
                "expr": "+++"
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let result = convert_register_request(req);
        assert!(result.is_err());
    }

    #[test]
    fn test_register_request_missing_required_fields() {
        // Missing "name" field
        let json = serde_json::json!({
            "key_field": "user_id",
            "features": []
        });
        let result: Result<RegisterRequest, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_register_request_empty_stream_name() {
        let json = serde_json::json!({
            "name": "",
            "key_field": "user_id",
            "features": []
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let result = convert_register_request(req);
        assert!(result.is_err());
    }

    #[test]
    fn test_register_request_empty_key_field() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "",
            "features": []
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let result = convert_register_request(req);
        assert!(result.is_err());
    }

    #[test]
    fn test_convert_register_request_pipeline_engine_compatibility() {
        // End-to-end: convert DTO and register in PipelineEngine
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [
                {"name": "tx_count_1h", "type": "count", "window": "1h"},
                {"name": "tx_sum_1h", "type": "sum", "field": "amount", "window": "1h"},
                {"name": "rate", "type": "derive", "expr": "tx_sum_1h / tx_count_1h"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        let mut engine = crate::engine::pipeline::PipelineEngine::new();
        engine.register(stream).unwrap();
        assert_eq!(engine.stream_count(), 1);
        assert!(engine.get_stream("Transactions").is_some());
    }

    // --- G-02: write_string panic on oversized input ---

    #[test]
    #[should_panic(expected = "string too long for protocol")]
    fn test_write_string_panics_on_oversized() {
        let huge = "x".repeat(u16::MAX as usize + 1); // 65536 bytes
        write_string(&huge);
    }

    // --- G-04: read_string invalid UTF-8 ---

    #[test]
    fn test_read_string_invalid_utf8() {
        let bytes: &[u8] = &[0, 3, 0xFF, 0xFE, 0xFD];
        let mut buf: &[u8] = bytes;
        let result = read_string(&mut buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("invalid UTF-8 in string"),
            "got: {}",
            err_msg
        );
    }

    #[test]
    fn test_read_string_invalid_utf8_continuation() {
        let bytes: &[u8] = &[0, 2, 0xC0, 0x01]; // invalid continuation byte
        let mut buf: &[u8] = bytes;
        let result = read_string(&mut buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("invalid UTF-8 in string"),
            "got: {}",
            err_msg
        );
    }

    // --- G-05: unknown feature type ---

    #[test]
    fn test_register_request_unknown_feature_type_median() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: Some("id".into()),
            definition_type: None,
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            features: vec![FeatureDefRequest {
                name: "f1".into(),
                feature_type: "median".into(),
                field: None,
                window: None,
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown feature type: median"),
            "got: {}",
            err_msg
        );
    }

    #[test]
    fn test_register_request_unknown_feature_type_histogram() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: Some("id".into()),
            definition_type: None,
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            features: vec![FeatureDefRequest {
                name: "f1".into(),
                feature_type: "histogram".into(),
                field: None,
                window: None,
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unknown feature type: histogram"),
            "got: {}",
            err_msg
        );
    }

    // --- G-06: missing required fields per feature type ---

    #[test]
    fn test_register_request_count_missing_window() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: Some("id".into()),
            definition_type: None,
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            features: vec![FeatureDefRequest {
                name: "cnt".into(),
                feature_type: "count".into(),
                field: None,
                window: None,
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("count requires 'window'"),
            "got: {}",
            err_msg
        );
    }

    #[test]
    fn test_register_request_sum_missing_field() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: Some("id".into()),
            definition_type: None,
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            features: vec![FeatureDefRequest {
                name: "total".into(),
                feature_type: "sum".into(),
                field: None,
                window: Some("1h".into()),
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("sum requires 'field'"), "got: {}", err_msg);
    }

    #[test]
    fn test_register_request_sum_missing_window() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: Some("id".into()),
            definition_type: None,
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            features: vec![FeatureDefRequest {
                name: "total".into(),
                feature_type: "sum".into(),
                field: Some("amount".into()),
                window: None,
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("sum requires 'window'"),
            "got: {}",
            err_msg
        );
    }

    #[test]
    fn test_register_request_avg_missing_field() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: Some("id".into()),
            definition_type: None,
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            features: vec![FeatureDefRequest {
                name: "mean".into(),
                feature_type: "avg".into(),
                field: None,
                window: Some("1h".into()),
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("avg requires 'field'"), "got: {}", err_msg);
    }

    #[test]
    fn test_register_request_derive_missing_expr() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: Some("id".into()),
            definition_type: None,
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            features: vec![FeatureDefRequest {
                name: "ratio".into(),
                feature_type: "derive".into(),
                field: None,
                window: None,
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("derive requires 'expr'"),
            "got: {}",
            err_msg
        );
    }

    // ======================== Phase 5: min/max/last/where protocol tests ========================

    #[test]
    fn test_register_request_min_feature() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [{
                "name": "min_amount_1h",
                "type": "min",
                "field": "amount",
                "window": "1h"
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::Min {
                field,
                window,
                bucket,
                optional,
                where_expr,
                ..
            } => {
                assert_eq!(field, "amount");
                assert_eq!(*window, std::time::Duration::from_secs(3600));
                assert_eq!(*bucket, std::time::Duration::from_secs(120));
                assert!(!optional);
                assert!(where_expr.is_none());
            }
            other => panic!("expected Min, got {:?}", other),
        }
    }

    #[test]
    fn test_register_request_max_feature() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [{
                "name": "max_amount_24h",
                "type": "max",
                "field": "amount",
                "window": "24h",
                "optional": true
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::Max {
                field,
                window,
                optional,
                where_expr,
                ..
            } => {
                assert_eq!(field, "amount");
                assert_eq!(*window, std::time::Duration::from_secs(86400));
                assert!(*optional);
                assert!(where_expr.is_none());
            }
            other => panic!("expected Max, got {:?}", other),
        }
    }

    #[test]
    fn test_register_request_last_feature() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [{
                "name": "last_country",
                "type": "last",
                "field": "country"
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::Last {
                field, optional, ..
            } => {
                assert_eq!(field, "country");
                assert!(!optional);
            }
            other => panic!("expected Last, got {:?}", other),
        }
    }

    #[test]
    fn test_register_request_count_with_where_clause() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [{
                "name": "failed_tx_1h",
                "type": "count",
                "window": "1h",
                "where": "status == 'failed'"
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::Count {
                window, where_expr, ..
            } => {
                assert_eq!(*window, std::time::Duration::from_secs(3600));
                assert!(where_expr.is_some());
            }
            other => panic!("expected Count with where_expr, got {:?}", other),
        }
    }

    #[test]
    fn test_register_request_min_missing_field() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: Some("id".into()),
            definition_type: None,
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            features: vec![FeatureDefRequest {
                name: "f1".into(),
                feature_type: "min".into(),
                field: None,
                window: Some("1h".into()),
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("min requires 'field'"), "got: {}", err_msg);
    }

    #[test]
    fn test_register_request_last_missing_field() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: Some("id".into()),
            definition_type: None,
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            features: vec![FeatureDefRequest {
                name: "f1".into(),
                feature_type: "last".into(),
                field: None,
                window: None,
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("last requires 'field'"),
            "got: {}",
            err_msg
        );
    }

    // ======================== Phase 6 Plan 02: MGET protocol tests ========================

    #[test]
    fn test_parse_command_mget_two_keys() {
        let mut payload = Vec::new();
        let count: u32 = 2;
        payload.extend_from_slice(&count.to_be_bytes());
        payload.extend_from_slice(&write_string("k1"));
        payload.extend_from_slice(&write_string("k2"));

        let cmd = parse_command(OP_MGET, &payload).unwrap();
        match cmd {
            Command::Mget { keys } => {
                assert_eq!(keys, vec!["k1".to_string(), "k2".to_string()]);
            }
            _ => panic!("expected Mget command"),
        }
    }

    #[test]
    fn test_parse_command_mget_zero_keys() {
        let mut payload = Vec::new();
        let count: u32 = 0;
        payload.extend_from_slice(&count.to_be_bytes());

        let cmd = parse_command(OP_MGET, &payload).unwrap();
        match cmd {
            Command::Mget { keys } => {
                assert!(keys.is_empty());
            }
            _ => panic!("expected Mget command"),
        }
    }

    #[test]
    fn test_parse_command_mget_truncated_count() {
        // Only 2 bytes instead of 4 for count
        let payload = vec![0u8, 1];
        let result = parse_command(OP_MGET, &payload);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("MGET payload too short"),
            "got: {}",
            err_msg
        );
    }

    #[test]
    fn test_parse_command_mget_truncated_key_string() {
        let mut payload = Vec::new();
        let count: u32 = 1;
        payload.extend_from_slice(&count.to_be_bytes());
        // String header says 10 bytes but only 2 available
        payload.extend_from_slice(&[0u8, 10, 0x68, 0x69]);

        let result = parse_command(OP_MGET, &payload);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("truncated"), "got: {}", err_msg);
    }

    // ======================== Phase 6 Plan 02: entity_ttl / history_ttl parsing tests ========================

    #[test]
    fn test_register_request_with_entity_ttl_parsed() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "entity_ttl": "5m",
            "features": [
                {"name": "tx_count_1h", "type": "count", "window": "1h"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        assert_eq!(stream.entity_ttl, Some(std::time::Duration::from_secs(300)));
    }

    #[test]
    fn test_register_request_without_entity_ttl_is_none() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [
                {"name": "tx_count_1h", "type": "count", "window": "1h"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        assert_eq!(stream.entity_ttl, None);
        assert_eq!(stream.history_ttl, None);
    }

    #[test]
    fn test_register_request_with_history_ttl_parsed() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "history_ttl": "72h",
            "features": [
                {"name": "tx_count_1h", "type": "count", "window": "1h"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        assert_eq!(
            stream.history_ttl,
            Some(std::time::Duration::from_secs(259200))
        );
    }

    #[test]
    fn test_register_request_with_both_ttls_parsed() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "entity_ttl": "1h",
            "history_ttl": "7d",
            "features": []
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        assert_eq!(
            stream.entity_ttl,
            Some(std::time::Duration::from_secs(3600))
        );
        assert_eq!(
            stream.history_ttl,
            Some(std::time::Duration::from_secs(604800))
        );
    }

    // --- G-08: read_json_payload ---

    // ======================== Phase 5 Plan 03: distinct_count and view protocol tests ========================

    #[test]
    fn test_register_request_distinct_count_feature() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [{
                "name": "unique_merchants_24h",
                "type": "distinct_count",
                "field": "merchant_id",
                "window": "24h"
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::DistinctCount {
                field,
                window,
                bucket,
                optional,
                where_expr,
                ..
            } => {
                assert_eq!(field, "merchant_id");
                assert_eq!(*window, std::time::Duration::from_secs(86400));
                assert!(!optional);
                assert!(where_expr.is_none());
                // bucket should be default: 86400/30 = 2880s
                assert_eq!(*bucket, std::time::Duration::from_secs(2880));
            }
            other => panic!("expected DistinctCount, got {:?}", other),
        }
    }

    #[test]
    fn test_register_request_distinct_count_missing_field() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: Some("id".into()),
            definition_type: None,
            depends_on: None,
            filter: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            features: vec![FeatureDefRequest {
                name: "dc".into(),
                feature_type: "distinct_count".into(),
                field: None,
                window: Some("1h".into()),
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("distinct_count requires 'field'"),
            "got: {}",
            err_msg
        );
    }

    #[test]
    fn test_convert_view_register_request_derive() {
        let json = serde_json::json!({
            "name": "UserRisk",
            "key_field": "user_id",
            "type": "view",
            "features": [{
                "name": "tx_ratio",
                "type": "derive",
                "expr": "Transactions.tx_count_1h / 1"
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.definition_type.as_deref(), Some("view"));
        let view = convert_view_register_request(req).unwrap();
        assert_eq!(view.name, "UserRisk");
        assert_eq!(view.key_field, "user_id");
        assert_eq!(view.features.len(), 1);
        assert_eq!(view.features[0].0, "tx_ratio");
        match &view.features[0].1 {
            crate::engine::pipeline::ViewFeatureDef::Derive { .. } => {}
            other => panic!("expected ViewFeatureDef::Derive, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_view_register_request_lookup() {
        let json = serde_json::json!({
            "name": "FraudSignals",
            "key_field": "user_id",
            "type": "view",
            "features": [{
                "name": "merchant_chargebacks",
                "type": "lookup",
                "target": "MerchantActivity.chargeback_count_24h",
                "on": "merchant_id"
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let view = convert_view_register_request(req).unwrap();
        assert_eq!(view.features.len(), 1);
        match &view.features[0].1 {
            crate::engine::pipeline::ViewFeatureDef::Lookup {
                target_stream,
                target_feature,
                on_field,
            } => {
                assert_eq!(target_stream, "MerchantActivity");
                assert_eq!(target_feature, "chargeback_count_24h");
                assert_eq!(on_field, "merchant_id");
            }
            other => panic!("expected ViewFeatureDef::Lookup, got {:?}", other),
        }
    }

    #[test]
    fn test_convert_view_register_request_rejects_count_type() {
        let json = serde_json::json!({
            "name": "BadView",
            "key_field": "user_id",
            "type": "view",
            "features": [{
                "name": "cnt",
                "type": "count",
                "window": "1h"
            }]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let result = convert_view_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("only supports 'derive' and 'lookup'"),
            "got: {}",
            err_msg
        );
    }

    #[test]
    fn test_read_json_payload_valid() {
        let json_bytes = b"{\"a\":1}";
        let mut buf: &[u8] = json_bytes;
        let result = read_json_payload(&mut buf).unwrap();
        assert_eq!(result, serde_json::json!({"a": 1}));
        assert!(buf.is_empty(), "buf should be fully consumed");
    }

    #[test]
    fn test_read_json_payload_invalid() {
        let bad_bytes = b"not json";
        let mut buf: &[u8] = bad_bytes;
        let result = read_json_payload(&mut buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("invalid JSON payload"), "got: {}", err_msg);
    }

    #[test]
    fn test_read_json_payload_empty_buffer() {
        let empty: &[u8] = b"";
        let mut buf: &[u8] = empty;
        let result = read_json_payload(&mut buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("invalid JSON payload"), "got: {}", err_msg);
    }

    #[test]
    fn test_read_json_payload_advances_buffer() {
        let json_bytes = b"[1,2,3]";
        let mut buf: &[u8] = json_bytes;
        let result = read_json_payload(&mut buf).unwrap();
        assert_eq!(result, serde_json::json!([1, 2, 3]));
        assert_eq!(buf.len(), 0, "buffer should be empty after consuming JSON");
    }

    // ======================== Phase 7 Plan 01: Keyless streams, depends_on, filter ========================

    #[test]
    fn test_register_request_optional_key_field() {
        // key_field omitted from JSON -> should deserialize as None
        let json = serde_json::json!({
            "name": "RawEvents",
            "features": []
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        assert!(
            req.key_field.is_none(),
            "key_field should be None when omitted"
        );
    }

    #[test]
    fn test_register_request_with_depends_on() {
        let json = serde_json::json!({
            "name": "TX",
            "key_field": "user_id",
            "depends_on": ["Raw"],
            "features": []
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.depends_on, Some(vec!["Raw".to_string()]));
    }

    #[test]
    fn test_register_request_with_filter() {
        let json = serde_json::json!({
            "name": "Failed",
            "key_field": "user_id",
            "filter": "_event.status == 'failed'",
            "features": [{"name": "c", "type": "count", "window": "1h"}]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        assert!(req.filter.is_some(), "filter should be Some");
        assert_eq!(req.filter.unwrap(), "_event.status == 'failed'");
    }

    #[test]
    fn test_convert_keyless_register() {
        let req = RegisterRequest {
            name: "RawEvents".into(),
            key_field: None,
            definition_type: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            depends_on: None,
            filter: None,
            features: vec![],
        };
        let stream = convert_register_request(req).unwrap();
        assert_eq!(stream.name, "RawEvents");
        assert!(stream.key_field.is_none());
    }

    #[test]
    fn test_convert_keyless_rejects_windowed() {
        let req = RegisterRequest {
            name: "RawEvents".into(),
            key_field: None,
            definition_type: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            depends_on: None,
            filter: None,
            features: vec![FeatureDefRequest {
                name: "cnt".into(),
                feature_type: "count".into(),
                field: None,
                window: Some("1h".into()),
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("keyless"),
            "error should mention 'keyless', got: {}",
            err_msg
        );
    }

    #[test]
    fn test_convert_depends_on_and_filter() {
        let req = RegisterRequest {
            name: "FailedTx".into(),
            key_field: Some("user_id".into()),
            definition_type: None,
            entity_ttl: None,
            history_ttl: None,
            projection: None,
            ephemeral: None,
            ttl: None,
            max_keys: None,
            depends_on: Some(vec!["RawEvents".into()]),
            filter: Some("_event.status == 'failed'".into()),
            features: vec![FeatureDefRequest {
                name: "cnt".into(),
                feature_type: "count".into(),
                field: None,
                window: Some("1h".into()),
                bucket: None,
                expr: None,
                optional: None,
                where_clause: None,
                on: None,
                target: None,
                backfill: None,
                quantile: None,
                n: None,
                half_life: None,
            }],
        };
        let stream = convert_register_request(req).unwrap();
        assert_eq!(
            stream.depends_on.as_ref().unwrap(),
            &vec!["RawEvents".to_string()]
        );
        assert!(stream.filter.is_some());
    }

    // ======================== Phase 16: stddev and percentile protocol tests ========================

    #[test]
    fn test_register_stddev_via_json() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [
                {"name": "amount_stddev_1h", "type": "stddev", "field": "amount", "window": "1h"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        assert_eq!(stream.features.len(), 1);
        assert_eq!(stream.features[0].0, "amount_stddev_1h");
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::Stddev {
                field,
                window,
                bucket,
                optional,
                ..
            } => {
                assert_eq!(field, "amount");
                assert_eq!(*window, std::time::Duration::from_secs(3600));
                assert_eq!(*bucket, std::time::Duration::from_secs(120)); // default_bucket(1h) = 120s
                assert!(!optional);
            }
            other => panic!("expected Stddev, got {:?}", other),
        }
    }

    #[test]
    fn test_register_stddev_with_optional_and_where() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [
                {"name": "amount_stddev_1h", "type": "stddev", "field": "amount", "window": "1h", "optional": true, "where": "status == 'success'"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::Stddev {
                optional,
                where_expr,
                ..
            } => {
                assert!(*optional);
                assert!(where_expr.is_some());
            }
            other => panic!("expected Stddev, got {:?}", other),
        }
    }

    #[test]
    fn test_register_stddev_missing_field_errors() {
        let json = serde_json::json!({
            "name": "Test",
            "key_field": "id",
            "features": [
                {"name": "sd", "type": "stddev", "window": "1h"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let result = convert_register_request(req);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("stddev requires 'field'"));
    }

    #[test]
    fn test_register_percentile_via_json() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [
                {"name": "amount_p95_1h", "type": "percentile", "field": "amount", "quantile": 0.95, "window": "1h"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let stream = convert_register_request(req).unwrap();
        assert_eq!(stream.features.len(), 1);
        assert_eq!(stream.features[0].0, "amount_p95_1h");
        match &stream.features[0].1 {
            crate::engine::pipeline::FeatureDef::Percentile {
                field,
                quantile,
                window,
                bucket,
                optional,
                ..
            } => {
                assert_eq!(field, "amount");
                assert!((quantile - 0.95).abs() < f64::EPSILON);
                assert_eq!(*window, std::time::Duration::from_secs(3600));
                assert_eq!(*bucket, std::time::Duration::from_secs(120));
                assert!(!optional);
            }
            other => panic!("expected Percentile, got {:?}", other),
        }
    }

    #[test]
    fn test_register_percentile_missing_quantile_errors() {
        let json = serde_json::json!({
            "name": "Test",
            "key_field": "id",
            "features": [
                {"name": "p99", "type": "percentile", "field": "amount", "window": "1h"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let result = convert_register_request(req);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("percentile requires 'quantile'"));
    }

    #[test]
    fn test_register_percentile_invalid_quantile_errors() {
        let json = serde_json::json!({
            "name": "Test",
            "key_field": "id",
            "features": [
                {"name": "bad", "type": "percentile", "field": "amount", "quantile": 1.5, "window": "1h"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let result = convert_register_request(req);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("quantile must be between 0.0 and 1.0"));
    }

    #[test]
    fn test_register_percentile_missing_field_errors() {
        let json = serde_json::json!({
            "name": "Test",
            "key_field": "id",
            "features": [
                {"name": "p50", "type": "percentile", "quantile": 0.5, "window": "1h"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let result = convert_register_request(req);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("percentile requires 'field'"));
    }

    // ======================== Phase 18 Plan 01: Projection & Ephemeral Tests ========================

    #[test]
    fn test_register_request_backward_compat_no_new_fields() {
        // v1.3-format JSON: no projection, ephemeral, ttl, max_keys
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [
                {"name": "tx_count_1h", "type": "count", "window": "1h"}
            ]
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        assert!(req.projection.is_none());
        assert!(req.ephemeral.is_none());
        assert!(req.ttl.is_none());
        assert!(req.max_keys.is_none());
    }

    #[test]
    fn test_register_request_with_projection_select() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [
                {"name": "tx_count_1h", "type": "count", "window": "1h"}
            ],
            "projection": {"select": ["tx_count_1h"]}
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let proj = req.projection.unwrap();
        assert!(proj.select.is_some());
        assert!(proj.drop.is_none());
        assert_eq!(proj.select.unwrap(), vec!["tx_count_1h".to_string()]);
    }

    #[test]
    fn test_register_request_select_drop_mutual_exclusion() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [
                {"name": "tx_count_1h", "type": "count", "window": "1h"}
            ],
            "projection": {"select": ["tx_count_1h"], "drop": ["tx_sum_1h"]}
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        let result = convert_register_request(req);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("select"));
    }

    #[test]
    fn test_register_request_ephemeral_fields() {
        let json = serde_json::json!({
            "name": "Transactions",
            "key_field": "user_id",
            "features": [
                {"name": "tx_count_1h", "type": "count", "window": "1h"}
            ],
            "ephemeral": true,
            "ttl": "1h",
            "max_keys": 10000
        });
        let req: RegisterRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.ephemeral, Some(true));
        assert_eq!(req.ttl, Some("1h".to_string()));
        assert_eq!(req.max_keys, Some(10000));
    }

    // ===================================================================
    // Phase 27-01: Scope struct, wire codec, validator, parse_command.
    // ===================================================================

    fn known(streams: &[&str]) -> std::collections::HashSet<String> {
        streams.iter().map(|s| s.to_string()).collect()
    }

    fn roundtrip(scope: &Scope) -> Scope {
        let mut buf = Vec::new();
        write_scope(&mut buf, scope);
        let mut cursor: &[u8] = &buf;
        let decoded = read_scope(&mut cursor).expect("read_scope must succeed");
        assert!(cursor.is_empty(), "read_scope must consume all bytes");
        decoded
    }

    #[test]
    fn test_scope_codec_streams_only() {
        let scope = Scope {
            streams: vec!["orders".into(), "clicks".into()],
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        };
        assert_eq!(roundtrip(&scope), scope);
    }

    #[test]
    fn test_scope_codec_with_keys() {
        let scope = Scope {
            streams: vec!["orders".into()],
            keys: Some(vec!["k1".into(), "k2".into(), "k3".into()]),
            key_prefix: None,
            pull: "all".into(),
        };
        assert_eq!(roundtrip(&scope), scope);
    }

    #[test]
    fn test_scope_codec_with_prefix() {
        let scope = Scope {
            streams: vec!["orders".into()],
            keys: None,
            key_prefix: Some("user_".into()),
            pull: "all".into(),
        };
        assert_eq!(roundtrip(&scope), scope);
    }

    #[test]
    fn test_scope_codec_with_empty_keys_vec() {
        // Structurally valid — empty keys Vec (semantic validation is separate).
        let scope = Scope {
            streams: vec!["orders".into()],
            keys: Some(vec![]),
            key_prefix: None,
            pull: "all".into(),
        };
        assert_eq!(roundtrip(&scope), scope);
    }

    #[test]
    fn test_scope_codec_rejects_truncated_header() {
        let mut buf: &[u8] = &[0u8];
        assert!(read_scope(&mut buf).is_err());
    }

    #[test]
    fn test_scope_codec_rejects_bad_has_keys_byte() {
        // n_streams=1, one string "x", has_keys=7 (illegal)
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u16.to_be_bytes());
        buf.extend_from_slice(&write_string("x"));
        buf.push(7);
        let mut cursor: &[u8] = &buf;
        assert!(read_scope(&mut cursor).is_err());
    }

    #[test]
    fn test_validate_scope_accepts_valid() {
        let scope = Scope {
            streams: vec!["orders".into()],
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        };
        validate_scope(&scope, &known(&["orders", "clicks"])).unwrap();
    }

    #[test]
    fn test_validate_scope_rejects_empty_streams() {
        let scope = Scope {
            streams: vec![],
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        };
        assert_eq!(
            validate_scope(&scope, &known(&["orders"])).unwrap_err(),
            ScopeError::EmptyStreams
        );
    }

    #[test]
    fn test_validate_scope_rejects_unknown_stream() {
        let scope = Scope {
            streams: vec!["nope".into()],
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        };
        assert_eq!(
            validate_scope(&scope, &known(&["orders"])).unwrap_err(),
            ScopeError::UnknownStream("nope".into())
        );
    }

    #[test]
    fn test_validate_scope_rejects_keys_and_prefix() {
        let scope = Scope {
            streams: vec!["orders".into()],
            keys: Some(vec!["k1".into()]),
            key_prefix: Some("u".into()),
            pull: "all".into(),
        };
        assert_eq!(
            validate_scope(&scope, &known(&["orders"])).unwrap_err(),
            ScopeError::KeysAndPrefix
        );
    }

    #[test]
    fn test_validate_scope_rejects_pull_not_all() {
        let scope = Scope {
            streams: vec!["orders".into()],
            keys: None,
            key_prefix: None,
            pull: "historical".into(),
        };
        assert_eq!(
            validate_scope(&scope, &known(&["orders"])).unwrap_err(),
            ScopeError::PullNotImplemented("historical".into())
        );
    }

    #[test]
    fn test_validate_scope_rejects_too_many_keys() {
        let keys: Vec<String> = (0..10_001).map(|i| format!("k{}", i)).collect();
        let scope = Scope {
            streams: vec!["orders".into()],
            keys: Some(keys),
            key_prefix: None,
            pull: "all".into(),
        };
        assert_eq!(
            validate_scope(&scope, &known(&["orders"])).unwrap_err(),
            ScopeError::TooManyKeys(10_001)
        );
    }

    #[test]
    fn test_validate_scope_rejects_empty_prefix() {
        let scope = Scope {
            streams: vec!["orders".into()],
            keys: None,
            key_prefix: Some(String::new()),
            pull: "all".into(),
        };
        assert_eq!(
            validate_scope(&scope, &known(&["orders"])).unwrap_err(),
            ScopeError::EmptyPrefix
        );
    }

    #[test]
    fn test_validate_scope_rejects_empty_key() {
        let scope = Scope {
            streams: vec!["orders".into()],
            keys: Some(vec!["ok".into(), String::new()]),
            key_prefix: None,
            pull: "all".into(),
        };
        assert_eq!(
            validate_scope(&scope, &known(&["orders"])).unwrap_err(),
            ScopeError::EmptyKey
        );
    }

    #[test]
    fn test_scope_error_display_is_single_line() {
        for err in [
            ScopeError::EmptyStreams,
            ScopeError::UnknownStream("x".into()),
            ScopeError::KeysAndPrefix,
            ScopeError::PullNotImplemented("foo".into()),
            ScopeError::TooManyKeys(42),
            ScopeError::EmptyPrefix,
            ScopeError::EmptyKey,
        ] {
            let s = format!("{}", err);
            assert!(!s.is_empty());
            assert!(!s.contains('\n'), "Display must be single-line: {:?}", err);
        }
    }

    #[test]
    fn test_parse_command_snapshot_fetch_roundtrip() {
        let scope = Scope {
            streams: vec!["orders".into(), "clicks".into()],
            keys: Some(vec!["k1".into()]),
            key_prefix: None,
            pull: "all".into(),
        };
        let mut buf = Vec::new();
        buf.extend_from_slice(&write_string("shh-admin"));
        write_scope(&mut buf, &scope);
        let cmd = parse_command(OP_SNAPSHOT_FETCH, &buf).unwrap();
        match cmd {
            Command::SnapshotFetch {
                admin_token,
                scope: got,
            } => {
                assert_eq!(admin_token, "shh-admin");
                assert_eq!(got, scope);
            }
            other => panic!("expected SnapshotFetch, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_command_snapshot_fetch_empty_token() {
        let scope = Scope {
            streams: vec!["orders".into()],
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        };
        let mut buf = Vec::new();
        buf.extend_from_slice(&write_string(""));
        write_scope(&mut buf, &scope);
        let cmd = parse_command(OP_SNAPSHOT_FETCH, &buf).unwrap();
        match cmd {
            Command::SnapshotFetch { admin_token, .. } => assert_eq!(admin_token, ""),
            other => panic!("expected SnapshotFetch, got {:?}", other),
        }
    }
}
