//! Phase 59.6 Wave 2 (TPC-PERF-11) — typed-row push batch decoder.
//!
//! Extends the Phase 59 binary wire codec with a schema-aware push path
//! used by `OP_PUSH_TYPED_BATCH` (0x19). The body shape consumed by
//! [`decode_typed_row_push_batch`] is (in wire order, starting AFTER the
//! stream-name header that the TCP dispatcher has already read):
//!
//! ```text
//! [schema_id: u32 BE]
//! [row_count: u32 BE]
//! [rows: row_size × row_count bytes]       // one row payload per row
//! [arena_total_len: u32 BE]
//! [arena_bytes: arena_total_len]           // shared across all rows in Wave 2
//! [ack_token: u64 BE]
//! ```
//!
//! Wave 2 design simplifications (resolved in later waves):
//!
//! * **Shared arena per batch (D-B1 simplification):** a single arena payload
//!   is cloned into every row. Wave 4 will split per-row arenas using the
//!   `(start, len)` pointers already in the packed payload.
//! * **schema_id = 0 shortcut:** clients that haven't plumbed the REGISTER
//!   ack's schema_id back can send `schema_id = 0`; the server trusts the
//!   stream-name lookup. Wave 6 removes this shortcut.
//!
//! See `.planning/phases/59.6-typed-pipeline-records/59.6-CONTEXT.md` D-B1.

use crate::engine::schema::{RegisteredSchema, Row};
use crate::error::BeavaError;

/// D-B1: decode an `OP_PUSH_TYPED_BATCH` body into a `Vec<Row>` using the
/// given registered schema.
///
/// The input `bytes` slice starts AFTER the opcode byte and AFTER the
/// stream-name (the TCP dispatcher has already consumed both via the
/// standard `read_string` helper). It begins at the `schema_id` prefix.
///
/// Returns `(rows, ack_token, bytes_consumed)`. `bytes_consumed` lets the
/// caller advance past the decoded body — useful when the body is
/// embedded in a larger frame or when the caller validates that no stray
/// bytes follow the ack token.
///
/// # Errors
///
/// * `BeavaError::Protocol` if the body is short at any boundary (schema_id,
///   row_count, packed rows, arena length/bytes, ack_token).
/// * `BeavaError::Protocol` if `schema_id` is non-zero and disagrees with
///   the registered schema's id. A `schema_id` of zero is a Wave-2 shortcut
///   — the server trusts the stream-name lookup.
/// * `BeavaError::Protocol` if `row_count * row_size` would overflow or
///   exceed the payload cap (DoS defense T-59.6-02-01).
pub fn decode_typed_row_push_batch(
    bytes: &[u8],
    schema: &RegisteredSchema,
) -> Result<(Vec<Row>, u64, usize), BeavaError> {
    let mut pos = 0usize;

    // [schema_id: u32 BE]
    if bytes.len() < pos + 4 {
        return Err(BeavaError::Protocol(
            "typed push: short schema_id header (need 4 bytes)".into(),
        ));
    }
    let schema_id = u32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap());
    pos += 4;
    // Wave 2 shortcut: schema_id=0 → trust the stream-name lookup the
    // caller performed. Wave 6 removes this once REGISTER ack carries the
    // schema_id back to the client.
    if schema_id != 0 && schema_id != schema.schema_id {
        return Err(BeavaError::Protocol(format!(
            "typed push: schema_id {} does not match registered schema for {:?} (id={})",
            schema_id, schema.name, schema.schema_id
        )));
    }

    // [row_count: u32 BE]
    if bytes.len() < pos + 4 {
        return Err(BeavaError::Protocol(
            "typed push: short row_count header (need 4 bytes)".into(),
        ));
    }
    let row_count = u32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;

    // T-59.6-02-01 DoS defense: bound total rows bytes against the payload
    // cap BEFORE the big allocation.
    let row_size = schema.row_size as usize;
    let rows_bytes = row_count
        .checked_mul(row_size)
        .ok_or_else(|| BeavaError::Protocol("typed push: row_count × row_size overflow".into()))?;
    let cap = crate::wire::max_payload_bytes_from_env();
    if rows_bytes > cap {
        return Err(BeavaError::Protocol(format!(
            "typed push: rows_bytes {} exceeds payload cap {}",
            rows_bytes, cap
        )));
    }
    if bytes.len() < pos + rows_bytes + 4 + 8 {
        return Err(BeavaError::Protocol(format!(
            "typed push: body too short ({} bytes remaining) for {} rows × {} bytes + arena_len + ack",
            bytes.len() - pos,
            row_count,
            row_size
        )));
    }

    // [rows: row_size × row_count bytes]
    let mut rows: Vec<Row> = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let payload: Vec<u8> = bytes[pos..pos + row_size].to_vec();
        pos += row_size;
        rows.push(Row {
            schema_id: schema.schema_id,
            payload,
            arena: Vec::new(),
        });
    }

    // [arena_total_len: u32 BE]
    let arena_len = u32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
    pos += 4;
    if arena_len > cap {
        return Err(BeavaError::Protocol(format!(
            "typed push: arena_len {} exceeds payload cap {}",
            arena_len, cap
        )));
    }
    if bytes.len() < pos + arena_len + 8 {
        return Err(BeavaError::Protocol(format!(
            "typed push: body short at arena ({} bytes needed for arena + ack, {} remaining)",
            arena_len + 8,
            bytes.len() - pos,
        )));
    }

    // Wave 2 simplification: clone the full arena into every row. Wave 4
    // will split per-row arenas using the (start, len) pointers already
    // in each packed payload.
    if arena_len > 0 {
        let arena_bytes: Vec<u8> = bytes[pos..pos + arena_len].to_vec();
        for row in &mut rows {
            row.arena = arena_bytes.clone();
        }
    }
    pos += arena_len;

    // [ack_token: u64 BE]
    let ack_token = u64::from_be_bytes(bytes[pos..pos + 8].try_into().unwrap());
    pos += 8;

    Ok((rows, ack_token, pos))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::schema::{FieldSpec, FieldTy};

    fn txns_schema(schema_id: u32) -> RegisteredSchema {
        // Layout matching the plan body: user_id InlineStr@0 (slot 16) + amount F64@16.
        RegisteredSchema {
            schema_id,
            name: "Txns".into(),
            fields: vec![
                FieldSpec {
                    name: "user_id".into(),
                    ty: FieldTy::InlineStr,
                    offset: 0,
                    nullable: false,
                },
                FieldSpec {
                    name: "amount".into(),
                    ty: FieldTy::F64,
                    offset: 16,
                    nullable: false,
                },
            ],
            inline_str_cap: 15,
            row_size: 24,
        }
    }

    fn build_frame(
        schema_id: u32,
        rows: &[(&str, f64)],
        schema: &RegisteredSchema,
        arena: &[u8],
        ack: u64,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&schema_id.to_be_bytes());
        buf.extend_from_slice(&(rows.len() as u32).to_be_bytes());
        for (uid, amount) in rows {
            let mut row = vec![0u8; schema.row_size as usize];
            let b = uid.as_bytes();
            let cap = schema.inline_str_cap as usize;
            let copy = b.len().min(cap);
            row[..copy].copy_from_slice(&b[..copy]);
            row[16..24].copy_from_slice(&amount.to_le_bytes());
            buf.extend_from_slice(&row);
        }
        buf.extend_from_slice(&(arena.len() as u32).to_be_bytes());
        buf.extend_from_slice(arena);
        buf.extend_from_slice(&ack.to_be_bytes());
        buf
    }

    #[test]
    fn decode_typed_row_push_batch_happy_path() {
        let schema = txns_schema(7);
        let frame = build_frame(
            7,
            &[("alice", 1.5), ("bob", 2.5), ("charlie", -3.25)],
            &schema,
            &[],
            0xDEAD_BEEF_CAFE_F00D,
        );
        let (rows, ack, consumed) =
            decode_typed_row_push_batch(&frame, &schema).expect("decode ok");
        assert_eq!(rows.len(), 3);
        assert_eq!(ack, 0xDEAD_BEEF_CAFE_F00D);
        assert_eq!(consumed, frame.len());
        assert_eq!(rows[0].read_inline_str(0, 15), "alice");
        assert_eq!(rows[0].read_f64(16), 1.5);
        assert_eq!(rows[1].read_inline_str(0, 15), "bob");
        assert_eq!(rows[1].read_f64(16), 2.5);
        assert_eq!(rows[2].read_inline_str(0, 15), "charlie");
        assert_eq!(rows[2].read_f64(16), -3.25);
        // Wave 2 invariant: arena empty when no arena was written.
        for row in &rows {
            assert!(row.arena.is_empty());
            assert_eq!(row.schema_id, 7);
        }
    }

    #[test]
    fn decode_typed_row_push_batch_zero_rows_is_valid() {
        let schema = txns_schema(1);
        let frame = build_frame(1, &[], &schema, &[], 42);
        let (rows, ack, consumed) =
            decode_typed_row_push_batch(&frame, &schema).expect("zero-row body ok");
        assert!(rows.is_empty());
        assert_eq!(ack, 42);
        assert_eq!(consumed, frame.len());
    }

    #[test]
    fn decode_typed_row_push_batch_rejects_mismatched_schema_id() {
        let schema = txns_schema(1);
        // Build a frame claiming schema_id=99.
        let frame = build_frame(99, &[("alice", 1.0)], &schema, &[], 0);
        let err = decode_typed_row_push_batch(&frame, &schema)
            .expect_err("mismatched schema_id must fail");
        let msg = format!("{}", err);
        assert!(msg.contains("schema_id 99"), "unexpected error: {msg}");
    }

    #[test]
    fn decode_typed_row_push_batch_accepts_schema_id_zero_as_shortcut() {
        // Wave 2 D-B1 convenience: schema_id = 0 means "trust stream_name
        // lookup". The decoder accepts it and uses the registered schema's
        // actual id for the decoded Row's `schema_id` field.
        let schema = txns_schema(42);
        let frame = build_frame(0, &[("ned", 7.5)], &schema, &[], 1);
        let (rows, _ack, _consumed) =
            decode_typed_row_push_batch(&frame, &schema).expect("schema_id=0 shortcut ok");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].schema_id, 42);
    }

    #[test]
    fn decode_typed_row_push_batch_rejects_truncated_body() {
        let schema = txns_schema(1);
        // 3 bytes — way too short for any field.
        let bytes = [0u8, 0, 0];
        let err = decode_typed_row_push_batch(&bytes, &schema)
            .expect_err("truncated body must fail");
        let msg = format!("{}", err);
        assert!(
            msg.contains("short schema_id")
                || msg.contains("schema_id")
                || msg.contains("too short"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn decode_typed_row_push_batch_rejects_truncated_rows_payload() {
        let schema = txns_schema(1);
        // schema_id(1) + row_count(2) + but only 24 bytes of rows (we need 48)
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_be_bytes());
        buf.extend_from_slice(&2u32.to_be_bytes());
        buf.extend_from_slice(&vec![0u8; 24]); // one row's worth
        // arena_len + ack missing entirely
        let err = decode_typed_row_push_batch(&buf, &schema)
            .expect_err("short rows payload must fail");
        let msg = format!("{}", err);
        assert!(msg.contains("body too short"), "unexpected error: {msg}");
    }

    #[test]
    fn decode_typed_row_push_batch_arena_is_cloned_into_every_row() {
        let schema = txns_schema(1);
        let arena = b"the-shared-arena-bytes".to_vec();
        let frame = build_frame(1, &[("a", 1.0), ("b", 2.0)], &schema, &arena, 0);
        let (rows, _ack, _consumed) =
            decode_typed_row_push_batch(&frame, &schema).expect("arena decode ok");
        assert_eq!(rows.len(), 2);
        // Wave 2 simplification: arena cloned into every row.
        for row in &rows {
            assert_eq!(row.arena, arena);
        }
    }
}
