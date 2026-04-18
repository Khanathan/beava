//! Phase 28-04: thin client-side wire surface for OP_SNAPSHOT_FETCH.
//!
//! **Provenance / policy.** The canonical definitions of `Scope`, `write_scope`,
//! and the `OP_SNAPSHOT_FETCH` / `REPLICA_FRAME_TAG_*` constants live in
//! `src/server/protocol.rs`. That module is gated `#[cfg(feature = "server")]`
//! (see `src/lib.rs`) so it is invisible to a `--no-default-features --features
//! client` build. Rather than hoist ~400 lines of server-owned codec into a
//! shared module (see Plan 28-01 executor note), we duplicate the minimum
//! wire surface the client needs here.
//!
//! **MUST stay in sync with `src/server/protocol.rs`** — specifically:
//!   - `pub const OP_SNAPSHOT_FETCH: u8`
//!   - `pub const REPLICA_FRAME_TAG_HEADER: u8`
//!   - `pub const REPLICA_FRAME_TAG_PAYLOAD: u8`
//!   - `pub struct Scope { streams, keys, key_prefix, pull }`
//!   - `fn write_scope(&mut Vec<u8>, &Scope)` byte layout
//!
//! The **default** (`server`) build runs a compile-time assertion (see
//! `assert_consts_match_server` below) that compares each duplicated const to
//! the server's authoritative value. A mismatch fails the build loudly; the
//! server wire layout never silently drifts.
//!
//! Scope bytes are also covered by the cross-language test
//! `tests/integration/test_replica_snapshot_fetch_asyncio.py`, which hand-rolls
//! the identical layout from Python. Any future divergence would break that
//! test first.
//!
//! NOTE: once Phase 29/31 re-plans the protocol module, collapse this
//! file and `src/server/protocol.rs`'s Scope/codec into a shared module.
//! Phase 47 audit: keep — intentional duplication; design note, not a bug.

/// Snapshot-fetch opcode. MUST equal `crate::server::protocol::OP_SNAPSHOT_FETCH`.
pub const OP_SNAPSHOT_FETCH: u8 = 0x12;

/// Response header frame tag. MUST equal `crate::server::protocol::REPLICA_FRAME_TAG_HEADER`.
pub const REPLICA_FRAME_TAG_HEADER: u8 = 0x01;

/// Response payload frame tag. MUST equal `crate::server::protocol::REPLICA_FRAME_TAG_PAYLOAD`.
pub const REPLICA_FRAME_TAG_PAYLOAD: u8 = 0x02;

/// Phase 27-02: per-event frame tag on an `OP_SUBSCRIBE` socket.
/// Also reused by Phase 35-01 `OP_LOG_FETCH` to carry historical events
/// (with a distinct body layout — single `u64 timestamp_ms` cursor).
/// MUST equal `crate::server::protocol::REPLICA_FRAME_TAG_EVENT`.
pub const REPLICA_FRAME_TAG_EVENT: u8 = 0x03;

/// Phase 35-01: terminal "caught up to tail" frame for `OP_LOG_FETCH`
/// responses. Emitted exactly once after the event stream; body is empty
/// (frame_len = 1, just the tag byte). MUST equal
/// `crate::server::protocol::REPLICA_FRAME_TAG_END`.
pub const REPLICA_FRAME_TAG_END: u8 = 0x04;

/// Phase 35-01: scoped historical-log fetch opcode.
///
/// Request payload shape:
///   `[u16 BE token_len][token][u64 BE from_ts_millis][Scope bytes]`
///
/// Response: zero or more event frames `[u32 frame_len][u8 tag=0x03]
/// [u64 BE timestamp_ms][u32 BE payload_len][payload]`, then a single
/// END frame `[u32 frame_len=1][u8 tag=0x04]`.
///
/// Mirrored here for client-side wire parity; no client helper consumes
/// it in this phase (v0 data-scientist path uses a Python asyncio script
/// that hand-rolls the frames — see `tests/integration/`). Future Rust
/// clients can build on this const + `Scope` + `write_scope`.
///
/// MUST equal `crate::server::protocol::OP_LOG_FETCH`.
pub const OP_LOG_FETCH: u8 = 0x13;

/// Structural replica of the Phase 27 `Scope`.
///
/// Wire layout (big-endian, mirrors `server::protocol::write_scope`):
///   `[u16 n_streams][n_streams × u16-string]
///    [u8 has_keys][if has_keys: u32 n_keys][n_keys × u16-string]
///    [u8 has_prefix][if has_prefix: u16-string prefix]
///    [u16-string pull]`
#[derive(Debug, Clone, PartialEq)]
pub struct Scope {
    pub streams: Vec<String>,
    pub keys: Option<Vec<String>>,
    pub key_prefix: Option<String>,
    pub pull: String,
}

fn write_u16_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    assert!(
        bytes.len() <= u16::MAX as usize,
        "string too long for u16 length prefix: {}",
        bytes.len()
    );
    buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    buf.extend_from_slice(bytes);
}

/// Append a `Scope` to `buf`. Byte-for-byte identical to
/// `crate::server::protocol::write_scope`.
pub fn write_scope(buf: &mut Vec<u8>, scope: &Scope) {
    assert!(
        scope.streams.len() <= u16::MAX as usize,
        "scope.streams too long for u16 len prefix"
    );
    buf.extend_from_slice(&(scope.streams.len() as u16).to_be_bytes());
    for s in &scope.streams {
        write_u16_string(buf, s);
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
                write_u16_string(buf, k);
            }
        }
        None => buf.push(0u8),
    }
    match &scope.key_prefix {
        Some(p) => {
            buf.push(1u8);
            write_u16_string(buf, p);
        }
        None => buf.push(0u8),
    }
    write_u16_string(buf, &scope.pull);
}

// Compile-time alignment check: when built with the server feature, this
// module sees the canonical protocol constants and asserts that the
// duplicated values here match byte-for-byte.
#[cfg(feature = "server")]
const _: () = {
    assert!(OP_SNAPSHOT_FETCH == crate::server::protocol::OP_SNAPSHOT_FETCH);
    assert!(REPLICA_FRAME_TAG_HEADER == crate::server::protocol::REPLICA_FRAME_TAG_HEADER);
    assert!(REPLICA_FRAME_TAG_PAYLOAD == crate::server::protocol::REPLICA_FRAME_TAG_PAYLOAD);
    // Phase 35-01: mirror-consts parity check.
    assert!(REPLICA_FRAME_TAG_EVENT == crate::server::protocol::REPLICA_FRAME_TAG_EVENT);
    assert!(REPLICA_FRAME_TAG_END == crate::server::protocol::REPLICA_FRAME_TAG_END);
    assert!(OP_LOG_FETCH == crate::server::protocol::OP_LOG_FETCH);
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_scope_roundtrip_streams_only() {
        // Byte-level cross-check: manually spelled out layout matches write_scope output.
        let scope = Scope {
            streams: vec!["orders".into(), "clicks".into()],
            keys: None,
            key_prefix: None,
            pull: "all".into(),
        };
        let mut buf = Vec::new();
        write_scope(&mut buf, &scope);
        let mut expected = Vec::new();
        expected.extend_from_slice(&2u16.to_be_bytes());
        expected.extend_from_slice(&6u16.to_be_bytes());
        expected.extend_from_slice(b"orders");
        expected.extend_from_slice(&6u16.to_be_bytes());
        expected.extend_from_slice(b"clicks");
        expected.push(0); // has_keys = 0
        expected.push(0); // has_prefix = 0
        expected.extend_from_slice(&3u16.to_be_bytes());
        expected.extend_from_slice(b"all");
        assert_eq!(buf, expected);
    }

    #[test]
    fn write_scope_with_keys() {
        let scope = Scope {
            streams: vec!["S".into()],
            keys: Some(vec!["a".into(), "b".into()]),
            key_prefix: None,
            pull: "all".into(),
        };
        let mut buf = Vec::new();
        write_scope(&mut buf, &scope);
        // header byte should include has_keys=1 + n_keys u32=2
        // Check prefix section that follows: should be [has_prefix=0][pull len=3][all]
        assert_eq!(&buf[buf.len() - 6..], &[0u8, 0, 3, b'a', b'l', b'l']);
    }

    #[test]
    fn write_scope_with_prefix() {
        let scope = Scope {
            streams: vec!["S".into()],
            keys: None,
            key_prefix: Some("usr_".into()),
            pull: "all".into(),
        };
        let mut buf = Vec::new();
        write_scope(&mut buf, &scope);
        // Last 5 bytes = [u16 len=3][a][l][l] for pull.
        assert_eq!(&buf[buf.len() - 5..], &[0, 3, b'a', b'l', b'l']);
        // Sanity: prefix string "usr_" appears verbatim in the middle.
        assert!(buf.windows(4).any(|w| w == b"usr_"));
    }

    // When built with --features server we have access to the server-side
    // writer — assert byte-for-byte parity at runtime.
    #[cfg(feature = "server")]
    #[test]
    fn write_scope_matches_server_byte_for_byte() {
        let cases = vec![
            Scope {
                streams: vec!["orders".into(), "clicks".into()],
                keys: None,
                key_prefix: None,
                pull: "all".into(),
            },
            Scope {
                streams: vec!["X".into()],
                keys: Some(vec!["k1".into(), "k2".into()]),
                key_prefix: None,
                pull: "all".into(),
            },
            Scope {
                streams: vec!["Y".into()],
                keys: None,
                key_prefix: Some("pre_".into()),
                pull: "all".into(),
            },
        ];
        for c in cases {
            let mut client_buf = Vec::new();
            write_scope(&mut client_buf, &c);
            let server_scope = crate::server::protocol::Scope {
                streams: c.streams.clone(),
                keys: c.keys.clone(),
                key_prefix: c.key_prefix.clone(),
                pull: c.pull.clone(),
            };
            let mut server_buf = Vec::new();
            crate::server::protocol::write_scope(&mut server_buf, &server_scope);
            assert_eq!(client_buf, server_buf, "mismatch for scope {:?}", c);
        }
    }
}
