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
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    /// A pushed event (SRV-DUR-03 record kind).
    Event = 0x01,
    /// Reserved for Phase 7 — registry version bump records.
    RegistryBump = 0x02,
    /// Phase 11.5 — a table upsert (MVCC chain append for temporal tables,
    /// plain upsert otherwise). Payload shape documented in
    /// `beava_server::push_table`.
    TableUpsert = 0x03,
    /// Phase 11.5 — a table delete (tombstone insertion on MVCC; otherwise
    /// key removal). Symmetric with TableUpsert.
    TableDelete = 0x04,
    /// Phase 11.5 — an `app.retract(event_id)` directive targeting a prior
    /// TableUpsert/TableDelete record. Stream retraction is NOT valid in v0
    /// (the retract handler returns 501) but the record type is reserved now
    /// so stream retraction can land additively in v1 without a WAL format
    /// break.
    Retract = 0x05,
}

/// A single WAL record as stored on disk.
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub lsn: Lsn,
    pub record_type: RecordType,
    pub payload: Vec<u8>,
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
