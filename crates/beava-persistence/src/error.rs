//! Persistence error types.

use thiserror::Error;

/// Errors returned by the WAL writer/reader and related persistence APIs.
#[derive(Debug, Error)]
pub enum PersistError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("crc mismatch at offset {offset}: expected {expected:#010x}, got {got:#010x}")]
    CrcMismatch {
        offset: u64,
        expected: u32,
        got: u32,
    },

    #[error("bad magic: expected BEAVAWAL, got {got:?}")]
    BadMagic { got: [u8; 8] },

    #[error("unsupported format_version {0}")]
    UnsupportedVersion(u32),

    #[error("unknown record_type {0:#04x}")]
    UnknownRecordType(u8),

    #[error("torn record at offset {offset}: {reason}")]
    TornRecord { offset: u64, reason: String },
}
