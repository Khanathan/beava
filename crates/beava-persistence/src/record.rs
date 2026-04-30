//! WAL record frame encoding + decoding.
//!
//! Frame layout (little-endian):
//! ```text
//! [u32 length][u32 crc32c][u64 lsn][u8 record_type][payload]
//! ```
//! where `length` covers `[crc || lsn || record_type || payload]` (i.e. from
//! immediately after `length` to the end of the record body) and CRC32C is
//! computed over `[lsn || record_type || payload]`.

use std::io::Read;

use crate::error::PersistError;
use crate::{Lsn, RecordType, WalRecord};

/// Format version emitted by this implementation.
///
/// **Plan 12.6-06 (D-03 hard rip):** bumped 1 → 2 alongside the deletion of
/// the `event_time` byte slot in the per-record payload (apply_shard.rs's
/// hand-rolled v=2 binary records carry server `now_ms` instead of a
/// body-derived event timestamp). v1 records fail with the existing
/// `SchemaVersionMismatch` path on recovery — per CONTEXT D-03 there is no
/// migration shim; pre-pivot WALs are dev artifacts that operators clear
/// before booting the new binary.
pub const FORMAT_VERSION: u32 = 2;

/// Magic bytes at the head of every WAL segment file.
pub const MAGIC: [u8; 8] = *b"BEAVAWAL";

impl RecordType {
    pub fn from_u8(b: u8) -> Result<Self, PersistError> {
        match b {
            0x01 => Ok(RecordType::Event),
            0x02 => Ok(RecordType::RegistryBump),
            0x03 => Ok(RecordType::TableUpsert),
            0x04 => Ok(RecordType::TableDelete),
            0x05 => Ok(RecordType::Retract),
            other => Err(PersistError::UnknownRecordType(other)),
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Encode a record into `out` (append).
pub fn encode_record(rec: &WalRecord, out: &mut Vec<u8>) {
    // Build the body (lsn || type || payload) first so we can CRC it.
    let body_len = 8 + 1 + rec.payload.len();
    let mut body = Vec::with_capacity(body_len);
    body.extend_from_slice(&rec.lsn.to_le_bytes());
    body.push(rec.record_type.to_u8());
    body.extend_from_slice(&rec.payload);

    let crc = crc32c::crc32c(&body);
    let length: u32 = (4 + body_len) as u32; // crc(4) + body

    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&body);
}

/// Try to decode a single record from `r`.
///
/// * `Ok(Some(record))` — a valid record was decoded.
/// * `Ok(None)` — clean EOF OR the trailing bytes look like a torn write
///   (see module docs). Callers should treat as end-of-stream.
/// * `Err(_)` — CRC mismatch or structural corruption mid-stream.
///
/// The caller passes `current_offset` so CRC errors can be reported with
/// the offset where the bad record began.
pub fn decode_record<R: Read>(
    r: &mut R,
    current_offset: u64,
) -> Result<Option<WalRecord>, PersistError> {
    // Read length prefix. Partial reads (0..4 bytes) → clean EOF / torn tail.
    let mut len_buf = [0u8; 4];
    match read_exact_or_eof(r, &mut len_buf)? {
        ReadResult::Eof => return Ok(None),
        ReadResult::Partial => return Ok(None), // torn
        ReadResult::Full => {}
    }
    let length = u32::from_le_bytes(len_buf) as usize;

    // Read the body. We need at least 4 (crc) + 8 (lsn) + 1 (type) = 13 bytes.
    if length < 13 {
        return Err(PersistError::TornRecord {
            offset: current_offset,
            reason: format!("declared length {length} < minimum header 13"),
        });
    }

    let mut body = vec![0u8; length];
    match read_exact_or_eof(r, &mut body)? {
        ReadResult::Eof | ReadResult::Partial => return Ok(None), // torn tail
        ReadResult::Full => {}
    }

    let crc_expected = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
    let crc_payload = &body[4..];
    let crc_got = crc32c::crc32c(crc_payload);
    if crc_expected != crc_got {
        return Err(PersistError::CrcMismatch {
            offset: current_offset,
            expected: crc_expected,
            got: crc_got,
        });
    }

    // Parse body: lsn(8) + type(1) + payload
    let lsn = u64::from_le_bytes(crc_payload[0..8].try_into().expect("8 bytes from slice"));
    let record_type = RecordType::from_u8(crc_payload[8])?;
    let payload = crc_payload[9..].to_vec();

    Ok(Some(WalRecord {
        lsn: lsn as Lsn,
        record_type,
        payload,
    }))
}

enum ReadResult {
    Full,
    Partial,
    Eof,
}

fn read_exact_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<ReadResult, PersistError> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => {
                return Ok(if filled == 0 {
                    ReadResult::Eof
                } else {
                    ReadResult::Partial
                });
            }
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(PersistError::Io(e)),
        }
    }
    Ok(ReadResult::Full)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RecordType, WalRecord};

    /// Phase 11.5 Task 2 — round-trip the three new record types through the
    /// WAL codec. Verifies D-11/D-12: TableUpsert (0x03), TableDelete (0x04),
    /// and Retract (0x05) each encode → decode losslessly and that unknown
    /// discriminants continue to error cleanly.
    #[test]
    fn new_record_types_round_trip_through_codec() {
        for (byte, rt) in [
            (0x03u8, RecordType::TableUpsert),
            (0x04u8, RecordType::TableDelete),
            (0x05u8, RecordType::Retract),
        ] {
            let rec = WalRecord {
                lsn: 42,
                record_type: rt,
                payload: b"hello".to_vec(),
            };
            let mut buf = Vec::new();
            encode_record(&rec, &mut buf);
            let mut slice: &[u8] = &buf;
            let back = decode_record(&mut slice, 0)
                .expect("decode ok")
                .expect("record present");
            assert_eq!(back.lsn, 42);
            assert_eq!(back.record_type as u8, byte);
            assert_eq!(back.payload, b"hello");
        }

        // Unknown discriminant continues to surface cleanly.
        assert!(matches!(
            RecordType::from_u8(0x06),
            Err(PersistError::UnknownRecordType(0x06))
        ));
    }
}
