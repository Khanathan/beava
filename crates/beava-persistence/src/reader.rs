//! Append-only WAL segment reader.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::error::PersistError;
use crate::record::decode_record;
use crate::segment;
use crate::{Lsn, WalRecord};

#[derive(Debug)]
pub struct WalReader {
    file: BufReader<File>,
    /// Byte offset from start of file of the NEXT record to decode.
    pos: u64,
    start_lsn: Lsn,
    registry_version: u32,
    fused: bool,
}

impl WalReader {
    pub fn open(path: &Path) -> Result<Self, PersistError> {
        let file = File::open(path)?;
        let mut br = BufReader::new(file);
        let (start_lsn, registry_version) = segment::read_header(&mut br)?;
        Ok(Self {
            file: br,
            pos: segment::HEADER_SIZE,
            start_lsn,
            registry_version,
            fused: false,
        })
    }

    pub fn start_lsn(&self) -> Lsn {
        self.start_lsn
    }

    pub fn registry_version(&self) -> u32 {
        self.registry_version
    }
}

impl Iterator for WalReader {
    type Item = Result<WalRecord, PersistError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.fused {
            return None;
        }
        let record_offset = self.pos;
        match decode_record(&mut self.file, record_offset) {
            Ok(Some(rec)) => {
                // Advance pos: length(4) + length_value
                // We don't have direct access to the bytes read here; reuse
                // encoded length. Fortunately the encoded record size is
                // deterministic from (payload.len()).
                // length field is u32 that equals 4 (crc) + 8 (lsn) + 1 (type) + payload.len()
                let encoded_len = 4 + 4 + 8 + 1 + rec.payload.len() as u64;
                self.pos += encoded_len;
                Some(Ok(rec))
            }
            Ok(None) => {
                self.fused = true;
                None
            }
            Err(e) => {
                self.fused = true;
                Some(Err(e))
            }
        }
    }
}
