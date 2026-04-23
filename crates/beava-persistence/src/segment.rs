//! Segment file naming + header I/O.
//!
//! Segment header (24 bytes, little-endian):
//! ```text
//! magic (8 bytes = b"BEAVAWAL") ||
//! format_version (u32) ||
//! start_lsn (u64) ||
//! registry_version (u32)
//! ```

use std::io::{Read, Write};

use crate::error::PersistError;
use crate::record::{FORMAT_VERSION, MAGIC};
use crate::Lsn;

pub const HEADER_SIZE: u64 = 24;

pub fn segment_filename(start_lsn: Lsn) -> String {
    format!("wal-{:016x}.log", start_lsn)
}

pub fn write_header<W: Write>(
    w: &mut W,
    start_lsn: Lsn,
    registry_version: u32,
) -> std::io::Result<()> {
    w.write_all(&MAGIC)?;
    w.write_all(&FORMAT_VERSION.to_le_bytes())?;
    w.write_all(&start_lsn.to_le_bytes())?;
    w.write_all(&registry_version.to_le_bytes())?;
    Ok(())
}

pub fn read_header<R: Read>(r: &mut R) -> Result<(Lsn, u32), PersistError> {
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if magic != MAGIC {
        return Err(PersistError::BadMagic { got: magic });
    }
    let mut version_buf = [0u8; 4];
    r.read_exact(&mut version_buf)?;
    let version = u32::from_le_bytes(version_buf);
    if version != FORMAT_VERSION {
        return Err(PersistError::UnsupportedVersion(version));
    }
    let mut lsn_buf = [0u8; 8];
    r.read_exact(&mut lsn_buf)?;
    let start_lsn = u64::from_le_bytes(lsn_buf) as Lsn;
    let mut rv_buf = [0u8; 4];
    r.read_exact(&mut rv_buf)?;
    let registry_version = u32::from_le_bytes(rv_buf);
    Ok((start_lsn, registry_version))
}
