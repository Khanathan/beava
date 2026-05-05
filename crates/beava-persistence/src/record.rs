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
/// v0 ships at version=1 across WAL/snapshot/wire (events-only invariant —
/// see CLAUDE.md §"Events-Only Invariant"). Bumping this constant is a
/// breaking change for any on-disk segment. Operators upgrading from
/// pre-v0 dev binaries that carried `v=2` must clear `.beava/wal` +
/// `.beava/snapshots` before boot — there is no migration shim; readers
/// of an unrecognized version surface `PersistError::UnsupportedVersion`.
pub const FORMAT_VERSION: u32 = 1;

/// Magic bytes at the head of every WAL segment file.
pub const MAGIC: [u8; 8] = *b"BEAVAWAL";

impl RecordType {
    pub fn from_u8(b: u8) -> Result<Self, PersistError> {
        match b {
            0x01 => Ok(RecordType::Event),
            0x02 => Ok(RecordType::RegistryBump),
            // Bytes 0x03 / 0x04 / 0x05 (formerly table / retract record
            // types, removed per the events-only invariant) surface as
            // `UnknownRecordType` — the generic variant intentionally
            // covers any deleted discriminant.
            other => Err(PersistError::UnknownRecordType(other)),
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Encode a record into `out` (append).
pub fn encode_record(rec: &WalRecord, out: &mut Vec<u8>) {
    // CRC covers `[lsn || record_type || payload]`; build that span first.
    let body_len = 8 + 1 + rec.payload.len();
    let mut body = Vec::with_capacity(body_len);
    body.extend_from_slice(&rec.lsn.to_le_bytes());
    body.push(rec.record_type.to_u8());
    body.extend_from_slice(&rec.payload);

    let crc = crc32c::crc32c(&body);
    let length: u32 = (4 + body_len) as u32;

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
    // A partial length-prefix read (0..4 bytes) is a torn tail, not corruption —
    // the writer was interrupted between buffer flushes. Treat as clean EOF.
    let mut len_buf = [0u8; 4];
    match read_exact_or_eof(r, &mut len_buf)? {
        ReadResult::Eof => return Ok(None),
        ReadResult::Partial => return Ok(None),
        ReadResult::Full => {}
    }
    let length = u32::from_le_bytes(len_buf) as usize;

    // Minimum body = crc(4) + lsn(8) + type(1) = 13 bytes; anything shorter
    // is structural corruption, not a torn tail.
    if length < 13 {
        return Err(PersistError::TornRecord {
            offset: current_offset,
            reason: format!("declared length {length} < minimum header 13"),
        });
    }

    let mut body = vec![0u8; length];
    match read_exact_or_eof(r, &mut body)? {
        // Short body after a valid length prefix is also a torn tail.
        ReadResult::Eof | ReadResult::Partial => return Ok(None),
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

    #[test]
    fn surviving_record_types_round_trip_through_codec() {
        for (byte, rt) in [
            (0x01u8, RecordType::Event),
            (0x02u8, RecordType::RegistryBump),
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

        // Bytes 0x03 / 0x04 / 0x05 (formerly table / retract record types)
        // and any other unknown discriminant must surface as
        // `UnknownRecordType` — the events-only invariant.
        for b in [0x03u8, 0x04, 0x05, 0x06, 0xff] {
            assert!(matches!(
                RecordType::from_u8(b),
                Err(PersistError::UnknownRecordType(got)) if got == b
            ));
        }
    }
}
