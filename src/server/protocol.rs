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
pub enum Command {
    Push { stream_name: String, payload: serde_json::Value },
    Get { key: String },
    Set { key: String, payload: serde_json::Value },
    Mset { entries: Vec<(String, serde_json::Value)> },
    Register { payload: serde_json::Value },
}

// TODO: implement encode_frame, parse_frame, encode_response, read_string, write_string, read_json_payload, parse_command

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

    // --- FeatureValue JSON conversion tests (in types module) ---
    // (These are tested in types::tests below)

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
    fn test_parse_command_unknown_opcode() {
        let result = parse_command(0xFF, &[]);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("unknown opcode"));
    }
}
