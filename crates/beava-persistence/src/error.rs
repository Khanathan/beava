//! Persistence error types.

use thiserror::Error;

/// Snapshot-format-specific errors (Phase 7 Plan 01).
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("bad snapshot magic: expected BEAVASNP, got {got:?}")]
    BadMagic { got: [u8; 8] },

    #[error("unsupported snapshot format_version {0}")]
    UnsupportedVersion(u16),

    #[error("snapshot header crc mismatch: expected {expected:#010x}, got {got:#010x}")]
    HeaderCrcMismatch { expected: u32, got: u32 },

    #[error("snapshot body crc mismatch: expected {expected:#010x}, got {got:#010x}")]
    BodyCrcMismatch { expected: u32, got: u32 },

    #[error("truncated snapshot: expected {expected} body bytes, got {got}")]
    Truncated { expected: u64, got: u64 },
}

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

    #[error("snapshot: {0}")]
    Snapshot(#[from] SnapshotError),
}
