//! WAL writer/reader round-trip + CRC corruption + tail-truncation tests.
//!
//! Per CONTEXT.md D-02: frame is `[u32 length][u32 crc32c][u64 lsn][u8 type][payload]`.
//! CRC32C covers `[lsn || type || payload]`. Segment header: magic `BEAVAWAL` +
//! u32 format_version=1 + u64 start_lsn + u32 registry_version.

use beava_persistence::{PersistError, RecordType, WalReader, WalRecord, WalWriter};
use std::io::{Seek, SeekFrom, Write};

fn sample_payload() -> Vec<u8> {
    b"{\"user_id\":\"alice\",\"amount\":5.0}".to_vec()
}

#[test]
fn round_trip_single_event() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut w = WalWriter::open(dir.path(), 100, 7).expect("open writer");
        w.append(&WalRecord {
            lsn: 100,
            record_type: RecordType::Event,
            payload: sample_payload(),
        })
        .expect("append");
        // Explicit drop flushes BufWriter on close
    }

    let path = dir.path().join(format!("wal-{:016x}.log", 100u64));
    let mut r = WalReader::open(&path).expect("open reader");
    assert_eq!(r.start_lsn(), 100);
    assert_eq!(r.registry_version(), 7);

    let rec = r.next().expect("one record").expect("no err");
    assert_eq!(rec.lsn, 100);
    assert_eq!(rec.record_type, RecordType::Event);
    assert_eq!(rec.payload, sample_payload());

    assert!(r.next().is_none(), "reader exhausted");
}

#[test]
fn round_trip_multiple_events() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut w = WalWriter::open(dir.path(), 100, 1).expect("open writer");
        for lsn in 100..103 {
            w.append(&WalRecord {
                lsn,
                record_type: RecordType::Event,
                payload: format!("{{\"i\":{}}}", lsn).into_bytes(),
            })
            .expect("append");
        }
    }

    let path = dir.path().join(format!("wal-{:016x}.log", 100u64));
    let r = WalReader::open(&path).expect("open");
    let recs: Vec<_> = r.collect::<Result<_, _>>().expect("all ok");
    assert_eq!(recs.len(), 3);
    assert_eq!(recs[0].lsn, 100);
    assert_eq!(recs[1].lsn, 101);
    assert_eq!(recs[2].lsn, 102);
}

#[test]
fn segment_header_magic_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal-deadbeef.log");
    let mut f = std::fs::File::create(&path).unwrap();
    // Write bogus magic + plausible rest of header
    f.write_all(b"WRONGMAG").unwrap();
    f.write_all(&1u32.to_le_bytes()).unwrap();
    f.write_all(&42u64.to_le_bytes()).unwrap();
    f.write_all(&7u32.to_le_bytes()).unwrap();
    drop(f);

    match WalReader::open(&path) {
        Err(PersistError::BadMagic { got }) => {
            assert_eq!(&got, b"WRONGMAG");
        }
        other => panic!("expected BadMagic, got {other:?}"),
    }
}

#[test]
fn segment_header_bad_version() {
    // Plan 12.7-05 D-01 hard rip RESET: FORMAT_VERSION reset 2 → 1. v=2
    // (and any other non-1 value) is now the "wrong version" path; pre-12.7
    // dev WALs that carried v=2 are operator-clear-then-boot artifacts per
    // CONTEXT D-01 ("no migration shim").
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal-feedface.log");
    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(b"BEAVAWAL").unwrap();
    f.write_all(&2u32.to_le_bytes()).unwrap(); // pre-12.7 v=2 (now unsupported)
    f.write_all(&42u64.to_le_bytes()).unwrap();
    f.write_all(&7u32.to_le_bytes()).unwrap();
    drop(f);

    match WalReader::open(&path) {
        Err(PersistError::UnsupportedVersion(v)) => assert_eq!(v, 2),
        other => panic!("expected UnsupportedVersion(2), got {other:?}"),
    }
}

#[test]
fn crc_mismatch_mid_stream() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut w = WalWriter::open(dir.path(), 100, 1).unwrap();
        for lsn in 100..103 {
            w.append(&WalRecord {
                lsn,
                record_type: RecordType::Event,
                payload: format!("payload-{}", lsn).into_bytes(),
            })
            .unwrap();
        }
    }

    let path = dir.path().join(format!("wal-{:016x}.log", 100u64));
    // Flip one byte inside the middle record's payload.
    // Header = 24 bytes. First record: len(4) + crc(4) + lsn(8) + type(1) + payload "payload-100"(11) = 28 body + 4 length prefix = 32 bytes total.
    // Middle record starts at 24 + 32 = 56. Inside its payload (past len/crc/lsn/type = 17 bytes into the record).
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    // flip a byte deep inside the middle record's payload area
    let flip_offset = 24 + 32 + 17 + 3; // third byte of middle payload
    f.seek(SeekFrom::Start(flip_offset)).unwrap();
    let mut b = [0u8; 1];
    use std::io::Read;
    f.read_exact(&mut b).unwrap();
    b[0] ^= 0xFF;
    f.seek(SeekFrom::Start(flip_offset)).unwrap();
    f.write_all(&b).unwrap();
    drop(f);

    let mut r = WalReader::open(&path).unwrap();
    let first = r.next().expect("first rec").expect("ok");
    assert_eq!(first.lsn, 100);
    match r.next() {
        Some(Err(PersistError::CrcMismatch { .. })) => {}
        other => panic!("expected CrcMismatch, got {other:?}"),
    }
    // Reader fuses after error
    assert!(r.next().is_none());
}

#[test]
fn torn_last_record_is_eof() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut w = WalWriter::open(dir.path(), 100, 1).unwrap();
        w.append(&WalRecord {
            lsn: 100,
            record_type: RecordType::Event,
            payload: b"first-ok".to_vec(),
        })
        .unwrap();
        w.append(&WalRecord {
            lsn: 101,
            record_type: RecordType::Event,
            payload: b"second-torn-me".to_vec(),
        })
        .unwrap();
    }

    let path = dir.path().join(format!("wal-{:016x}.log", 100u64));
    // Truncate last 3 bytes — simulates torn write
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let len = f.metadata().unwrap().len();
    f.set_len(len - 3).unwrap();
    drop(f);

    let mut r = WalReader::open(&path).unwrap();
    let first = r.next().expect("first").expect("ok");
    assert_eq!(first.lsn, 100);
    // Torn last record → Ok(None) = EOF
    assert!(
        r.next().is_none(),
        "torn last record should be treated as EOF"
    );
}

#[test]
fn unknown_record_type_errors() {
    // Plan 12.7-05 D-01 hard rip RESET: hand-written header uses
    // post-12.7-05 FORMAT_VERSION=1 so the reader proceeds past the header
    // and surfaces the bad record_type byte (0xFF here, but the same path
    // covers the now-deleted 0x03 / 0x04 / 0x05 bytes that pre-12.7 carried
    // table/retract records).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("wal-{:016x}.log", 100u64));

    // Hand-write a valid header + a record with record_type=0xFF
    let mut buf: Vec<u8> = Vec::new();
    buf.extend_from_slice(b"BEAVAWAL");
    buf.extend_from_slice(&1u32.to_le_bytes()); // post-12.7-05 FORMAT_VERSION
    buf.extend_from_slice(&100u64.to_le_bytes());
    buf.extend_from_slice(&1u32.to_le_bytes());

    // Build a record body: lsn(8) + type(1=0xFF) + payload
    let payload = b"oops";
    let mut body: Vec<u8> = Vec::new();
    body.extend_from_slice(&100u64.to_le_bytes());
    body.push(0xFF);
    body.extend_from_slice(payload);
    let crc = crc32c::crc32c(&body);

    // length covers: crc(4) + lsn(8) + type(1) + payload
    let length: u32 = (4 + 8 + 1 + payload.len()) as u32;
    buf.extend_from_slice(&length.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&body);

    std::fs::write(&path, &buf).unwrap();

    let mut r = WalReader::open(&path).unwrap();
    match r.next() {
        Some(Err(PersistError::UnknownRecordType(0xFF))) => {}
        other => panic!("expected UnknownRecordType(0xFF), got {other:?}"),
    }
}
