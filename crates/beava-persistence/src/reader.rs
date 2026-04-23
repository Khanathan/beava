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
    /// Open a single segment file OR a directory containing `wal-*.log` files.
    /// When passed a directory, uses the segment with the lowest start_lsn
    /// (equivalent to `open_dir`).
    pub fn open(path: &Path) -> Result<Self, PersistError> {
        if path.is_dir() {
            return Self::open_dir(path);
        }
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

    /// Open the lowest-LSN segment within a WAL directory (convenience for tests).
    /// For production replay, iterate all segments in sorted order.
    pub fn open_dir(dir: &Path) -> Result<Self, PersistError> {
        let mut entries: Vec<std::path::PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|r| r.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("log"))
            .collect();
        entries.sort();
        let first = entries.first().ok_or_else(|| {
            PersistError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "no wal-*.log segments in directory",
            ))
        })?;
        let file = File::open(first)?;
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

    /// Read every record across every segment in `dir` in ascending LSN order.
    /// Convenience for tests; production recovery (Phase 7) streams segment by
    /// segment.
    pub fn read_all(dir_or_file: &Path) -> Result<Vec<WalRecord>, PersistError> {
        if dir_or_file.is_file() {
            let reader = Self::open(dir_or_file)?;
            return reader.collect::<Result<Vec<_>, _>>();
        }
        let mut segments: Vec<std::path::PathBuf> = std::fs::read_dir(dir_or_file)?
            .filter_map(|r| r.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("log"))
            .collect();
        segments.sort();
        let mut out = Vec::new();
        for seg in segments {
            let reader = Self::open(&seg)?;
            for r in reader {
                out.push(r?);
            }
        }
        Ok(out)
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
