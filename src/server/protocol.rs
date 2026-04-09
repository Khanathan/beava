//! Binary protocol: frame encoding/decoding, string protocol, command parsing,
//! response serialization. All functions are synchronous (pure byte manipulation).

use crate::error::TallyError;

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
}
