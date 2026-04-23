//! WAL segment rotation + truncate-past-snapshot-LSN support.

use std::path::{Path, PathBuf};

use crate::error::PersistError;
use crate::segment;
use crate::writer::WalWriter;
use crate::Lsn;

/// List every `wal-*.log` segment in `dir`, sorted by start_lsn ascending.
/// Returns (start_lsn, full path) pairs.
pub fn list_segments(dir: &Path) -> std::io::Result<Vec<(Lsn, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(s) = name.to_str() else { continue };
        if !s.starts_with("wal-") || !s.ends_with(".log") {
            continue;
        }
        let hex = &s[4..s.len() - 4];
        if let Ok(lsn) = u64::from_str_radix(hex, 16) {
            out.push((lsn as Lsn, entry.path()));
        }
    }
    out.sort_by_key(|(l, _)| *l);
    Ok(out)
}

/// Delete any closed segment whose LAST lsn is strictly less than `covered_lsn`.
///
/// Approximation: a segment's last_lsn is `(next_segment.start_lsn - 1)`.
/// So the segment is fully covered iff `next_segment.start_lsn <= covered_lsn`.
/// The current (open) segment — the one with start_lsn == `current_start_lsn` —
/// is never deleted.
///
/// Returns the count of segments deleted.
pub fn truncate_up_to(
    dir: &Path,
    current_start_lsn: Lsn,
    covered_lsn: Lsn,
) -> Result<u32, PersistError> {
    let segs = list_segments(dir)?;
    if segs.is_empty() {
        return Ok(0);
    }
    let mut count = 0u32;
    for i in 0..segs.len() {
        let (start_lsn, path) = &segs[i];
        if *start_lsn == current_start_lsn {
            // Current open segment — skip.
            continue;
        }
        // next_start_lsn = start_lsn of the next segment (by ascending order),
        // or current_start_lsn if this is the last closed segment before the
        // current one. If this segment has no successor, treat it as uncovered
        // (it's the current segment — but we already filtered that above).
        let next_start_lsn = if i + 1 < segs.len() {
            segs[i + 1].0
        } else {
            // Should not happen: current segment always exists at the end,
            // so a segment with no successor must BE the current one and
            // was skipped above. If we're here with no successor, bail out.
            continue;
        };
        if next_start_lsn <= covered_lsn {
            std::fs::remove_file(path)?;
            count += 1;
        }
    }
    Ok(count)
}

/// Close the current writer and open a new segment starting at `next_start_lsn`.
pub fn rotate(
    writer: &mut WalWriter,
    dir: &Path,
    next_start_lsn: Lsn,
    registry_version: u32,
) -> Result<(), PersistError> {
    // Flush + sync_data the current segment before rotating.
    writer.sync_data()?;

    // Open new segment. `WalWriter::open` uses create_new — if the file
    // already exists (shouldn't, since next_start_lsn is the next free LSN),
    // this errors loudly.
    let new_writer = WalWriter::open(dir, next_start_lsn, registry_version)?;
    // Swap in the new writer. We drop the old one — its Drop impl flushes,
    // but we already synced above, so data is durable.
    *writer = new_writer;
    let _ = segment::HEADER_SIZE; // silence "unused import" warnings in this module
    Ok(())
}
