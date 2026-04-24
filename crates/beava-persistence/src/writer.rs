//! Append-only WAL segment writer.
//!
//! Plan 01 ships `append()` without any fsync — Plan 02 adds the group-commit
//! fsync worker on top.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::error::PersistError;
use crate::record::encode_record;
use crate::segment;
use crate::{Lsn, WalRecord};

/// An append-only WAL segment writer.
///
/// `open` creates a NEW segment file (errors if the target file already
/// exists); Plan 02's rotation logic handles opening subsequent segments.
pub struct WalWriter {
    file: BufWriter<File>,
    path: PathBuf,
    bytes_since_header: u64,
}

impl WalWriter {
    pub fn open(dir: &Path, start_lsn: Lsn, registry_version: u32) -> Result<Self, PersistError> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(segment::segment_filename(start_lsn));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        let mut bw = BufWriter::new(file);
        segment::write_header(&mut bw, start_lsn, registry_version)?;
        // Ensure header hits the file before any append — cheap given we
        // haven't fsynced yet; this also simplifies reader correctness.
        bw.flush()?;
        Ok(Self {
            file: bw,
            path,
            bytes_since_header: 0,
        })
    }

    /// Append a record. No fsync. Caller (Plan 02's fsync worker) invokes
    /// `sync_data` to make the write durable.
    pub fn append(&mut self, record: &WalRecord) -> Result<(), PersistError> {
        let mut buf = Vec::with_capacity(16 + record.payload.len());
        encode_record(record, &mut buf);
        self.file.write_all(&buf)?;
        self.bytes_since_header += buf.len() as u64;
        Ok(())
    }

    /// Bytes appended since the segment was opened (excludes header).
    pub fn bytes_written(&self) -> u64 {
        self.bytes_since_header
    }

    pub fn current_path(&self) -> &Path {
        &self.path
    }

    /// Flush the buffered writer AND `sync_data()` the underlying file.
    /// Plan 01 tests do not call this — Plan 02's fsync worker will.
    pub fn sync_data(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        self.file.get_mut().sync_data()
    }

    /// Phase 13.1: flush the in-memory buffer to the OS without invoking the
    /// blocking `sync_data()` syscall. Used by the async fsync worker which
    /// performs the durability syscall on a `spawn_blocking` thread via
    /// `try_clone_file()` so the runtime stays free.
    pub fn flush_buffer(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }

    /// Phase 13.1: produce an OS-level clone of the underlying file handle
    /// suitable for being moved into `tokio::task::spawn_blocking` to call
    /// `sync_data()` off the runtime thread. The clone shares the same
    /// kernel file description, so a fsync on the clone durably persists
    /// any bytes previously flushed via `flush_buffer()`.
    pub fn try_clone_file(&self) -> std::io::Result<File> {
        self.file.get_ref().try_clone()
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Best-effort flush on drop so Plan 01's read-after-write tests don't
        // have to explicitly sync. This does NOT sync_data — that's Plan 02.
        let _ = self.file.flush();
    }
}
