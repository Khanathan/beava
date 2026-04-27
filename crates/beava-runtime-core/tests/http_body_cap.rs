//! Phase 19.1.1 Plan 01 — RED tests for HTTP buffer-cap split.
//!
//! Bug being fixed: `parse_http_request` rejects ANY HTTP request larger than
//! 8 KiB because `MAX_HEADER_BYTES` is checked against the whole buffer
//! (`crates/beava-runtime-core/src/http_listener.rs:69-74`). This file exists
//! to lock in the contract for the fix:
//!
//!   1. A 15 KiB POST body MUST be accepted (was rejected pre-fix; CRITICAL RED).
//!   2. A 9 KiB header bomb (no `\r\n\r\n` boundary) MUST still be rejected
//!      (regression guard — header-bomb defense preserved).
//!   3. A POST with `Content-Length: 5_000_000` (5 MiB > MAX_BODY_BYTES = 4 MiB)
//!      MUST be rejected with `ParseError::TooLarge` (regression guard — body
//!      cap stays enforced).
//!
//! See `.planning/phases/19.1.1-http-buffer-cap-fix/19.1.1-01-PLAN.md` for the
//! full plan and `.planning/phases/19.1.1-http-buffer-cap-fix/19.1.1-CONTEXT.md`
//! for D-01..D-08 design decisions.

use beava_runtime_core::http_listener::{parse_http_request, ParseError};
use bytes::BytesMut;

/// CRITICAL RED — was failing before the fix.
///
/// Synthesizes a `POST /register HTTP/1.1` with a 15 KiB JSON-shaped body
/// and asserts `parse_http_request` returns `Ok(Some(_))`. Pre-fix this
/// returned `Err(ParseError::TooLarge)` because the 8 KiB cap fired before
/// the `Content-Length` check at line 143 was reachable.
#[test]
fn test_15kib_body_accepted() {
    let body_len: usize = 15 * 1024; // 15 KiB
    let body = vec![b'x'; body_len];
    let header = format!(
        "POST /register HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {body_len}\r\n\
         \r\n"
    );

    let mut buf = BytesMut::new();
    buf.extend_from_slice(header.as_bytes());
    buf.extend_from_slice(&body);

    let result = parse_http_request(&mut buf);

    assert!(
        matches!(result, Ok(Some(_))),
        "expected Ok(Some(_)) for 15 KiB POST body, got {result:?}"
    );
}

/// REGRESSION GUARD — header-bomb defense must stay.
///
/// Synthesizes 9 KiB of header lines with no `\r\n\r\n` terminator yet.
/// Asserts `Err(ParseError::TooLarge)` — the `MAX_HEADER_BYTES = 8 KiB` cap
/// must fire on header bytes alone (no headers complete yet).
#[test]
fn test_9kib_header_bomb_rejected() {
    // Build a request line + ~9 KiB of `X-Filler: aaa...\r\n` lines with NO
    // `\r\n\r\n` terminator. The buffer should exceed 8 KiB before any
    // header_end boundary is found, so the parser must reject as TooLarge.
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"GET / HTTP/1.1\r\n");

    // Each filler line is `X-Filler-NNNN: ` + 200 bytes of payload + `\r\n`
    // = ~218 bytes. We need > 8 KiB (8192) total, so push ~50 lines.
    let target_total: usize = 9 * 1024; // 9 KiB
    let mut counter = 0;
    while buf.len() < target_total {
        let line = format!(
            "X-Filler-{counter:04}: {payload}\r\n",
            payload = "a".repeat(200),
        );
        buf.extend_from_slice(line.as_bytes());
        counter += 1;
    }
    // Crucially: do NOT terminate with `\r\n\r\n`. The headers are unfinished.
    debug_assert!(
        buf.len() > 8 * 1024,
        "header bomb must exceed MAX_HEADER_BYTES (8 KiB)"
    );
    debug_assert!(
        !buf.windows(4).any(|w| w == b"\r\n\r\n"),
        "header bomb must NOT contain `\\r\\n\\r\\n`"
    );

    let result = parse_http_request(&mut buf);

    assert!(
        matches!(result, Err(ParseError::TooLarge)),
        "expected Err(ParseError::TooLarge) for 9 KiB header bomb, got {result:?}"
    );
}

/// REGRESSION GUARD — body cap stays enforced via Content-Length.
///
/// Synthesizes a POST with `Content-Length: 5_000_000` (5 MiB > MAX_BODY_BYTES
/// = 4 MiB). Asserts `Err(ParseError::TooLarge)`. We do NOT need to actually
/// supply 5 MiB of body bytes — the Content-Length check at line 143 fires on
/// the declared length alone.
#[test]
fn test_body_exceeding_max_body_bytes_rejected() {
    let header = "POST /register HTTP/1.1\r\n\
                  Host: localhost\r\n\
                  Content-Type: application/json\r\n\
                  Content-Length: 5000000\r\n\
                  \r\n";

    let mut buf = BytesMut::new();
    buf.extend_from_slice(header.as_bytes());
    // No body bytes — Content-Length check should fire first.

    let result = parse_http_request(&mut buf);

    assert!(
        matches!(result, Err(ParseError::TooLarge)),
        "expected Err(ParseError::TooLarge) for Content-Length: 5_000_000, got {result:?}"
    );
}
