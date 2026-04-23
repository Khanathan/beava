//! Beava v2 persistence crate — WAL + snapshot (syscall-bearing, outside beava-core).
//!
//! Preserves the beava-core WASM-portability invariant: all filesystem/fsync
//! logic lives here so core stays syscall-free.

mod error;
mod fsync_worker;
mod reader;
mod record;
mod rotation;
mod segment;
mod writer;

/// Log Sequence Number — monotonic event identifier assigned by the WAL sink.
pub type Lsn = u64;

/// WAL record type discriminant.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    /// A pushed event (SRV-DUR-03 record kind).
    Event = 0x01,
    /// Reserved for Phase 7 — registry version bump records.
    RegistryBump = 0x02,
}

/// A single WAL record as stored on disk.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub record_type: RecordType,
    pub payload: Vec<u8>,
}

pub use error::PersistError;
pub use fsync_worker::{WalSink, WalSinkConfig};
pub use reader::WalReader;
pub use writer::WalWriter;
