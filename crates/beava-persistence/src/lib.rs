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
mod snapshot;
mod snapshot_header;
mod writer;

/// Log Sequence Number — monotonic event identifier assigned by the WAL sink.
pub type Lsn = u64;

/// WAL record type discriminant.
///
/// **Plan 12.7-05 (D-01 hard rip):** v0 ships events-only per
/// `project_v0_events_only_scope` (locked 2026-04-30). The Phase 11.5
/// table / retraction record types (formerly bytes 0x03 / 0x04 / 0x05)
/// are deleted here; those bytes now fall through to the existing
/// `PersistError::UnknownRecordType` arm in `record::RecordType::from_u8`
/// per CONTEXT D-02 ("not supported in v0", NOT "feature removed"). v0's
/// surface = pushed events (0x01) + registry bumps (0x02). Tables /
/// retraction return in v0.1+ alongside joins / aggregation if/when
/// justified by demand.
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

/// Phase 13.4 Plan 07 (D-02 USER-LOCKED) — persistence mode discriminator.
///
/// `Memory` is **pure RAM**: no WAL writer thread, no snapshot writer, no
/// recovery on boot. State lives in RAM only; on process restart the state
/// is gone (clean slate). Snapshot is a no-op (no file I/O at all).
///
/// `Disk { .. }` is the existing production path — WAL + snapshot + recovery.
/// Existing callers passing the disk-config flat continue to work via the
/// back-compat wrapper in `ServerV18::bind`.
///
/// Why memory mode exists: embed mode (`bv.App()` no-URL) is single-process
/// anyway; recovery has no value if the user restarts the binary. Tests +
/// notebook usage are the use case; production stays on disk.
#[derive(Debug, Clone)]
pub enum Persistence {
    /// In-RAM only — no WAL, no snapshot, no recovery.
    Memory,
    /// On-disk persistence — WAL + snapshot + recovery on boot.
    Disk {
        /// Directory for WAL segments (`.log` / `.wal` files).
        wal_dir: std::path::PathBuf,
        /// Directory for snapshot files (`.bvs`).
        snapshot_dir: std::path::PathBuf,
        /// Sync semantics for the legacy `WalSink::append_event` path.
        sync_mode: fsync_worker::SyncMode,
    },
}

impl Default for Persistence {
    /// Production default: disk persistence rooted at `.beava/wal` and
    /// `.beava/snapshots`. Memory mode is opt-in.
    fn default() -> Self {
        Persistence::Disk {
            wal_dir: std::path::PathBuf::from(".beava/wal"),
            snapshot_dir: std::path::PathBuf::from(".beava/snapshots"),
            sync_mode: fsync_worker::SyncMode::default(),
        }
    }
}

pub use error::{PersistError, SnapshotError};
pub use fsync_worker::{SyncMode, WalSink, WalSinkConfig};
pub use reader::WalReader;
pub use record::FORMAT_VERSION;
pub use snapshot::{list_snapshots, prune_old_snapshots, SnapshotReader, SnapshotWriter};
pub use snapshot_header::{
    SnapshotHeader, SNAPSHOT_EXT, SNAPSHOT_FORMAT_VERSION, SNAPSHOT_HEADER_SIZE, SNAPSHOT_MAGIC,
};
pub use writer::WalWriter;
