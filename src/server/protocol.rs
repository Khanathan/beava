//! Binary protocol: frame encoding/decoding, string protocol, command parsing,
//! response serialization. All functions are synchronous (pure byte manipulation).

use serde::Deserialize;
use crate::error::TallyError;
use crate::engine::pipeline::{StreamDefinition, FeatureDef, ViewDefinition, ViewFeatureDef};

// Command opcodes
pub const OP_PUSH: u8 = 0x01;
pub const OP_GET: u8 = 0x02;
pub const OP_SET: u8 = 0x03;
pub const OP_MSET: u8 = 0x04;
pub const OP_REGISTER: u8 = 0x05;

// Response status codes
pub const STATUS_OK: u8 = 0x00;
pub const STATUS_ERROR: u8 = 0x01;

/// Parsed command from a protocol frame.
#[derive(Debug)]
pub enum Command {
    Push { stream_name: String, payload: serde_json::Value },
    Get { key: String },
    Set { key: String, payload: serde_json::Value },
    Mset { entries: Vec<(String, serde_json::Value)> },
    Register { payload: serde_json::Value },
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
        return Err(TallyError::Protocol("frame too short: need at least 5 bytes".into()));
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
        return Err(TallyError::Protocol("string header truncated: need 2 bytes for length".into()));
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
            let payload_value = read_json_payload(&mut buf)?;
            Ok(Command::Push { stream_name, payload: payload_value })
        }
        OP_GET => {
            let key = read_string(&mut buf)?;
            Ok(Command::Get { key })
        }
        OP_SET => {
            let key = read_string(&mut buf)?;
            let payload_value = read_json_payload(&mut buf)?;
            Ok(Command::Set { key, payload: payload_value })
        }
        OP_MSET => {
            if buf.len() < 4 {
                return Err(TallyError::Protocol("MSET payload too short: need 4 bytes for count".into()));
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
            Ok(Command::Register { payload: payload_value })
        }
        _ => Err(TallyError::Protocol(format!("unknown opcode: 0x{:02x}", opcode))),
    }
}

// ---------------------------------------------------------------------------
// Duration string parsing
// ---------------------------------------------------------------------------

/// Parse a human-readable duration string into std::time::Duration.
/// Supported suffixes: ms (milliseconds), s (seconds), m (minutes), h (hours), d (days).
pub fn parse_duration_str(s: &str) -> Result<std::time::Duration, TallyError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(TallyError::Protocol("empty duration string".into()));
    }
    // Check for "ms" suffix first (two-character suffix)
    if let Some(num_str) = s.strip_suffix("ms") {
        let millis: u64 = num_str
            .parse()
            .map_err(|_| TallyError::Protocol(format!("invalid duration number: {}", s)))?;
        return Ok(std::time::Duration::from_millis(millis));
    }
    // Single-character suffix
    let (num_str, multiplier) = match s.as_bytes().last() {
        Some(b's') => (&s[..s.len() - 1], 1u64),
        Some(b'm') => (&s[..s.len() - 1], 60u64),
        Some(b'h') => (&s[..s.len() - 1], 3600u64),
        Some(b'd') => (&s[..s.len() - 1], 86400u64),
        _ => {
            return Err(TallyError::Protocol(format!(
                "unknown duration suffix: {}",
                s
            )));
        }
    };
    let value: u64 = num_str
        .parse()
        .map_err(|_| TallyError::Protocol(format!("invalid duration number: {}", s)))?;
    Ok(std::time::Duration::from_secs(value * multiplier))
}

// ---------------------------------------------------------------------------
// REGISTER DTO types
// ---------------------------------------------------------------------------

/// Intermediate deserialization type for the REGISTER command payload.
/// Uses a flat struct with `feature_type` field instead of an internally tagged enum
/// for simpler Python SDK production.
#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub name: String,
    pub key_field: String,
    #[serde(default, rename = "type")]
    pub definition_type: Option<String>,  // "stream" (default) or "view"
    pub features: Vec<FeatureDefRequest>,
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
    pub on: Option<String>,       // For lookup (used in Plan 03)
    #[serde(default)]
    pub target: Option<String>,   // For lookup (used in Plan 03)
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
        return Err(TallyError::Protocol(
            "stream name must not be empty".into(),
        ));
    }
    if req.key_field.is_empty() {
        return Err(TallyError::Protocol(
            "key_field must not be empty".into(),
        ));
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
                FeatureDef::Count { window, bucket, where_expr }
            }
            "sum" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': sum requires 'field'",
                        f.name
                    ))
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
                }
            }
            "avg" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': avg requires 'field'",
                        f.name
                    ))
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
                }
            }
            "min" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': min requires 'field'",
                        f.name
                    ))
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
                }
            }
            "max" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': max requires 'field'",
                        f.name
                    ))
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
                }
            }
            "last" => {
                let field = f.field.ok_or_else(|| {
                    TallyError::Protocol(format!(
                        "feature '{}': last requires 'field'",
                        f.name
                    ))
                })?;
                FeatureDef::Last {
                    field,
                    optional: f.optional.unwrap_or(false),
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
                    TallyError::Protocol(format!(
                        "feature '{}': invalid expression: {}",
                        f.name, e
                    ))
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

    Ok(StreamDefinition {
        name: req.name,
        key_field: req.key_field,
        features,
    })
}

/// Convert a RegisterRequest DTO into a ViewDefinition.
/// Only "derive" and "lookup" feature types are allowed in views.
pub fn convert_view_register_request(req: RegisterRequest) -> Result<ViewDefinition, TallyError> {
    if req.name.is_empty() {
        return Err(TallyError::Protocol("view name must not be empty".into()));
    }
    if req.key_field.is_empty() {
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
                    TallyError::Protocol(format!(
                        "feature '{}': invalid expression: {}",
                        f.name, e
                    ))
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
        key_field: req.key_field,
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

    #[test]
    fn test_parse_command_push() {
        // PUSH: stream_name string + JSON payload
        let mut payload = Vec::new();
        payload.extend_from_slice(&write_string("Transactions"));
        payload.extend_from_slice(b"{\"user_id\":\"u123\",\"amount\":50.0}");
        let cmd = parse_command(OP_PUSH, &payload).unwrap();
        match cmd {
            Command::Push { stream_name, payload } => {
                assert_eq!(stream_name, "Transactions");
                assert_eq!(payload["user_id"], "u123");
                assert_eq!(payload["amount"], 50.0);
            }
            _ => panic!("expected Push command"),
        }
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
        assert_eq!(stream.key_field, "user_id");
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
            crate::engine::pipeline::FeatureDef::Sum { field, window, bucket, optional, .. } => {
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
            crate::engine::pipeline::FeatureDef::Avg { field, window, bucket, optional, .. } => {
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
        assert!(err_msg.contains("invalid UTF-8 in string"), "got: {}", err_msg);
    }

    #[test]
    fn test_read_string_invalid_utf8_continuation() {
        let bytes: &[u8] = &[0, 2, 0xC0, 0x01]; // invalid continuation byte
        let mut buf: &[u8] = bytes;
        let result = read_string(&mut buf);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("invalid UTF-8 in string"), "got: {}", err_msg);
    }

    // --- G-05: unknown feature type ---

    #[test]
    fn test_register_request_unknown_feature_type_median() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: "id".into(),
            definition_type: None,
            features: vec![FeatureDefRequest {
                name: "f1".into(),
                feature_type: "median".into(),
                field: None, window: None, bucket: None, expr: None, optional: None,
                where_clause: None, on: None, target: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unknown feature type: median"), "got: {}", err_msg);
    }

    #[test]
    fn test_register_request_unknown_feature_type_histogram() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: "id".into(),
            definition_type: None,
            features: vec![FeatureDefRequest {
                name: "f1".into(),
                feature_type: "histogram".into(),
                field: None, window: None, bucket: None, expr: None, optional: None,
                where_clause: None, on: None, target: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unknown feature type: histogram"), "got: {}", err_msg);
    }

    // --- G-06: missing required fields per feature type ---

    #[test]
    fn test_register_request_count_missing_window() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: "id".into(),
            definition_type: None,
            features: vec![FeatureDefRequest {
                name: "cnt".into(),
                feature_type: "count".into(),
                field: None, window: None, bucket: None, expr: None, optional: None,
                where_clause: None, on: None, target: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("count requires 'window'"), "got: {}", err_msg);
    }

    #[test]
    fn test_register_request_sum_missing_field() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: "id".into(),
            definition_type: None,
            features: vec![FeatureDefRequest {
                name: "total".into(),
                feature_type: "sum".into(),
                field: None, window: Some("1h".into()), bucket: None, expr: None, optional: None,
                where_clause: None, on: None, target: None,
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
            key_field: "id".into(),
            definition_type: None,
            features: vec![FeatureDefRequest {
                name: "total".into(),
                feature_type: "sum".into(),
                field: Some("amount".into()), window: None, bucket: None, expr: None, optional: None,
                where_clause: None, on: None, target: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("sum requires 'window'"), "got: {}", err_msg);
    }

    #[test]
    fn test_register_request_avg_missing_field() {
        let req = RegisterRequest {
            name: "Test".into(),
            key_field: "id".into(),
            definition_type: None,
            features: vec![FeatureDefRequest {
                name: "mean".into(),
                feature_type: "avg".into(),
                field: None, window: Some("1h".into()), bucket: None, expr: None, optional: None,
                where_clause: None, on: None, target: None,
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
            key_field: "id".into(),
            definition_type: None,
            features: vec![FeatureDefRequest {
                name: "ratio".into(),
                feature_type: "derive".into(),
                field: None, window: None, bucket: None, expr: None, optional: None,
                where_clause: None, on: None, target: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("derive requires 'expr'"), "got: {}", err_msg);
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
            crate::engine::pipeline::FeatureDef::Min { field, window, bucket, optional, where_expr } => {
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
            crate::engine::pipeline::FeatureDef::Max { field, window, optional, where_expr, .. } => {
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
            crate::engine::pipeline::FeatureDef::Last { field, optional } => {
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
            crate::engine::pipeline::FeatureDef::Count { window, where_expr, .. } => {
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
            key_field: "id".into(),
            definition_type: None,
            features: vec![FeatureDefRequest {
                name: "f1".into(),
                feature_type: "min".into(),
                field: None, window: Some("1h".into()), bucket: None, expr: None, optional: None,
                where_clause: None, on: None, target: None,
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
            key_field: "id".into(),
            definition_type: None,
            features: vec![FeatureDefRequest {
                name: "f1".into(),
                feature_type: "last".into(),
                field: None, window: None, bucket: None, expr: None, optional: None,
                where_clause: None, on: None, target: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("last requires 'field'"), "got: {}", err_msg);
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
            crate::engine::pipeline::FeatureDef::DistinctCount { field, window, bucket, optional, where_expr } => {
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
            key_field: "id".into(),
            definition_type: None,
            features: vec![FeatureDefRequest {
                name: "dc".into(),
                feature_type: "distinct_count".into(),
                field: None, window: Some("1h".into()), bucket: None, expr: None, optional: None,
                where_clause: None, on: None, target: None,
            }],
        };
        let result = convert_register_request(req);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("distinct_count requires 'field'"), "got: {}", err_msg);
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
            crate::engine::pipeline::ViewFeatureDef::Lookup { target_stream, target_feature, on_field } => {
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
        assert!(err_msg.contains("only supports 'derive' and 'lookup'"), "got: {}", err_msg);
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
}
