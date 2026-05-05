//! Append-only WAL segment writer.
//!
//! `append()` writes to the in-memory `BufWriter` only; durability is the
//! group-commit fsync worker's responsibility (`fsync_worker.rs`).

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
/// exists); rotation in `rotation::rotate` handles opening subsequent
/// segments.
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
        // Flush the header before any append so a reader opened concurrently
        // with the first record sees a complete segment header.
        bw.flush()?;
        Ok(Self {
            file: bw,
            path,
            bytes_since_header: 0,
        })
    }

    /// Append a record. No fsync. Caller (the group-commit fsync worker)
    /// invokes `sync_data` to make the write durable.
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
    pub fn sync_data(&mut self) -> std::io::Result<()> {
        self.file.flush()?;
        self.file.get_mut().sync_data()
    }

    /// Flush the in-memory buffer to the OS page cache without invoking the
    /// blocking `sync_data()` syscall. Pair with [`Self::try_clone_file`] +
    /// `spawn_blocking` to run the durability syscall off the runtime.
    pub fn flush_buffer(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }

    /// Produce an OS-level clone of the underlying file handle suitable for
    /// being moved into `tokio::task::spawn_blocking` to call `sync_data()`
    /// off the runtime thread. The clone shares the same kernel file
    /// description, so a fsync on the clone durably persists bytes
    /// previously flushed via [`Self::flush_buffer`].
    pub fn try_clone_file(&self) -> std::io::Result<File> {
        self.file.get_ref().try_clone()
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Best-effort flush on drop so read-after-write tests don't need
        // to explicitly sync. Does NOT sync_data — durability is the fsync
        // worker's responsibility.
        let _ = self.file.flush();
    }
}
