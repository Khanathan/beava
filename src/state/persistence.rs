//! PersistenceBackend trait: abstraction over snapshot persistence backends.
//!
//! The hot path (DashMap push/get) is untouched. This trait only governs how
//! state is serialized to durable storage for crash recovery.
//!
//! Built-in implementations:
//! - `SnapshotFileBackend`: existing bincode/postcard file-based snapshots (default)
//! - `SlateBackend` (feature `slatedb-backend`): SlateDB LSM on local object store

use std::path::{Path, PathBuf};

use crate::state::snapshot::{
    BaseSnapshotState, DeltaSnapshotState, SerializableEntityState, SerializablePipeline,
    save_base_snapshot, save_delta_snapshot, load_snapshot_file, SnapshotFile,
};

/// Errors from persistence operations.
#[derive(Debug)]
pub enum PersistenceError {
    Io(std::io::Error),
    Serialization(String),
    Other(String),
}

impl std::fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {}", e),
            Self::Serialization(e) => write!(f, "Serialization error: {}", e),
            Self::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for PersistenceError {}

impl From<std::io::Error> for PersistenceError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<postcard::Error> for PersistenceError {
    fn from(e: postcard::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}

/// Restored state from a persistence backend. Mirrors `SnapshotState` but
/// also carries the next sequence number so the caller can resume the
/// monotonic counter.
pub struct RestoredState {
    pub entities: Vec<(String, SerializableEntityState)>,
    pub pipelines: Vec<SerializablePipeline>,
    pub backfill_complete: Vec<(String, String)>,
    /// Next sequence number to use (max_loaded + 1).
    pub next_seq: u64,
    /// Sequence number of the base snapshot that was loaded (for delta chain tracking).
    pub loaded_base_seq: u64,
}

/// Abstraction over persistence backends. Both the existing file-based
/// snapshot system and SlateDB implement this trait.
///
/// All methods are synchronous (called from `spawn_blocking` or blocking
/// context). The snapshot timer already runs serialization on a blocking
/// thread pool, so async is not needed here.
pub trait PersistenceBackend: Send + Sync {
    /// Persist a full base snapshot (all entities + pipelines + backfill markers).
    fn persist_base(
        &self,
        base: &BaseSnapshotState,
        snap_dir: &Path,
    ) -> Result<usize, PersistenceError>;

    /// Persist a delta snapshot (changed entities + deleted keys).
    fn persist_delta(
        &self,
        delta: &DeltaSnapshotState,
        snap_dir: &Path,
    ) -> Result<usize, PersistenceError>;

    /// Restore all state on startup. Returns None if no snapshot found.
    fn restore(
        &self,
        snap_dir: &Path,
        legacy_path: &Path,
    ) -> Option<RestoredState>;

    /// Clean up old snapshot files/data below the given base sequence.
    fn cleanup(&self, snap_dir: &Path, current_base_seq: u64);

    /// Backend name for logging.
    fn name(&self) -> &str;
}

// ==========================================================================
// SnapshotFileBackend: wraps existing file-based snapshot logic
// ==========================================================================

/// Default persistence backend using the existing v6 postcard file format.
/// Base snapshots: `tally.snapshot.base.{seq:010}`
/// Delta snapshots: `tally.snapshot.delta.{seq:010}`
pub struct SnapshotFileBackend;

impl SnapshotFileBackend {
    pub fn new() -> Self {
        Self
    }
}

impl PersistenceBackend for SnapshotFileBackend {
    fn persist_base(
        &self,
        base: &BaseSnapshotState,
        snap_dir: &Path,
    ) -> Result<usize, PersistenceError> {
        let bytes = save_base_snapshot(base)?;
        let seq = base.header.sequence;
        let filename = format!("tally.snapshot.base.{:010}", seq);
        write_atomic(snap_dir, &filename, &bytes)?;
        Ok(bytes.len())
    }

    fn persist_delta(
        &self,
        delta: &DeltaSnapshotState,
        snap_dir: &Path,
    ) -> Result<usize, PersistenceError> {
        let bytes = save_delta_snapshot(delta)?;
        let seq = delta.header.sequence;
        let filename = format!("tally.snapshot.delta.{:010}", seq);
        write_atomic(snap_dir, &filename, &bytes)?;
        Ok(bytes.len())
    }

    fn restore(
        &self,
        snap_dir: &Path,
        legacy_path: &Path,
    ) -> Option<RestoredState> {
        // Scan for base + delta files, same logic as load_incremental_snapshots.
        let mut bases: Vec<(u64, PathBuf)> = Vec::new();
        let mut deltas: Vec<(u64, PathBuf)> = Vec::new();

        if let Ok(entries) = std::fs::read_dir(snap_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy().into_owned();
                if let Some(seq_str) = name_str.strip_prefix("tally.snapshot.base.") {
                    if let Ok(seq) = seq_str.parse::<u64>() {
                        bases.push((seq, entry.path()));
                    }
                } else if let Some(seq_str) = name_str.strip_prefix("tally.snapshot.delta.") {
                    if let Ok(seq) = seq_str.parse::<u64>() {
                        deltas.push((seq, entry.path()));
                    }
                }
            }
        }

        bases.sort_by_key(|(seq, _)| *seq);

        // Try to load the latest base.
        let loaded = bases.iter().rev().find_map(|(seq, path)| {
            let bytes = std::fs::read(path).ok()?;
            match load_snapshot_file(&bytes)? {
                SnapshotFile::Base(b) => Some((*seq, b)),
                _ => None,
            }
        });

        if let Some((base_seq, base)) = loaded {
            // Apply deltas on top.
            let store = crate::state::store::StateStore::new();
            store.restore_from_snapshot(base.entities.clone());

            let mut applicable: Vec<(u64, PathBuf)> = deltas
                .into_iter()
                .filter(|(seq, _)| *seq > base_seq)
                .collect();
            applicable.sort_by_key(|(seq, _)| *seq);

            let mut max_seq = base_seq;
            for (seq, delta_path) in &applicable {
                let bytes = match std::fs::read(delta_path) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                match load_snapshot_file(&bytes) {
                    Some(SnapshotFile::Delta(delta)) => {
                        store.apply_delta(delta.changed_entities, delta.deleted_keys);
                        if *seq > max_seq {
                            max_seq = *seq;
                        }
                    }
                    _ => continue,
                }
            }

            let entities = store.clone_for_snapshot();
            return Some(RestoredState {
                entities,
                pipelines: base.pipelines,
                backfill_complete: base.backfill_complete,
                next_seq: max_seq + 1,
                loaded_base_seq: base_seq,
            });
        }

        // Try legacy v5 path.
        if legacy_path.exists() {
            let bytes = std::fs::read(legacy_path).ok()?;
            let legacy = crate::state::snapshot::load_legacy_v5(&bytes)?;
            eprintln!("Loaded legacy v5 snapshot from {}", legacy_path.display());
            return Some(RestoredState {
                entities: legacy.entities,
                pipelines: legacy.pipelines,
                backfill_complete: legacy.backfill_complete,
                next_seq: 1,
                loaded_base_seq: 0,
            });
        }

        None
    }

    fn cleanup(&self, snap_dir: &Path, current_base_seq: u64) {
        let entries = match std::fs::read_dir(snap_dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            let seq_opt = name_str
                .strip_prefix("tally.snapshot.base.")
                .or_else(|| name_str.strip_prefix("tally.snapshot.delta."));
            if let Some(seq_str) = seq_opt {
                if let Ok(seq) = seq_str.parse::<u64>() {
                    if seq < current_base_seq {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
    }

    fn name(&self) -> &str {
        "snapshot-file"
    }
}

/// Write bytes atomically: write to .tmp, fsync, rename.
fn write_atomic(dir: &Path, filename: &str, bytes: &[u8]) -> Result<(), std::io::Error> {
    use std::fs::OpenOptions;
    use std::io::Write;

    let file_path = dir.join(filename);
    let tmp_path = dir.join(format!("{}.tmp", filename));
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp_path, &file_path)?;
    if let Ok(dir_file) = std::fs::File::open(dir) {
        let _ = dir_file.sync_all();
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::snapshot::{
        BaseSnapshotState, DeltaSnapshotState, SerializableEntityState,
        SnapshotHeader, SnapshotType,
    };
    use crate::state::store::StaticFeature;
    use crate::types::FeatureValue;
    use std::time::{Duration, UNIX_EPOCH};

    fn make_test_entity(name: &str) -> (String, SerializableEntityState) {
        (
            name.to_string(),
            SerializableEntityState {
                streams: vec![],
                static_features: vec![(
                    "f1".to_string(),
                    StaticFeature {
                        value: FeatureValue::Float(42.0),
                        updated_at: UNIX_EPOCH + Duration::from_secs(1000),
                    },
                )],
            },
        )
    }

    #[test]
    fn test_snapshot_file_backend_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SnapshotFileBackend::new();

        // Persist a base snapshot.
        let base = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 0,
            },
            entities: vec![make_test_entity("key1")],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        let size = backend.persist_base(&base, dir.path()).unwrap();
        assert!(size > 0);

        // Persist a delta.
        let delta = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 0 },
                sequence: 1,
            },
            changed_entities: vec![make_test_entity("key2")],
            deleted_keys: vec![],
        };
        backend.persist_delta(&delta, dir.path()).unwrap();

        // Restore and verify both entities present.
        let legacy_path = dir.path().join("tally.snapshot");
        let restored = backend.restore(dir.path(), &legacy_path).unwrap();
        assert_eq!(restored.entities.len(), 2);
        assert_eq!(restored.next_seq, 2);
        assert_eq!(restored.loaded_base_seq, 0);

        // Verify entity keys.
        let keys: Vec<&str> = restored.entities.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"key1"));
        assert!(keys.contains(&"key2"));
    }

    #[test]
    fn test_snapshot_file_backend_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SnapshotFileBackend::new();

        // Create base at seq 0 and delta at seq 1.
        let base0 = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 0,
            },
            entities: vec![make_test_entity("old")],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        backend.persist_base(&base0, dir.path()).unwrap();

        let delta1 = DeltaSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Delta { base_seq: 0 },
                sequence: 1,
            },
            changed_entities: vec![],
            deleted_keys: vec![],
        };
        backend.persist_delta(&delta1, dir.path()).unwrap();

        // Create new base at seq 5.
        let base5 = BaseSnapshotState {
            header: SnapshotHeader {
                snapshot_type: SnapshotType::Base,
                sequence: 5,
            },
            entities: vec![make_test_entity("new")],
            pipelines: vec![],
            backfill_complete: vec![],
        };
        backend.persist_base(&base5, dir.path()).unwrap();

        // Cleanup: remove everything below seq 5.
        backend.cleanup(dir.path(), 5);

        // Only the seq=5 base should remain.
        let files: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(files.len(), 1);
        assert!(files[0].contains("base.0000000005"));
    }

    #[test]
    fn test_snapshot_file_backend_empty_restore() {
        let dir = tempfile::tempdir().unwrap();
        let backend = SnapshotFileBackend::new();
        let legacy_path = dir.path().join("tally.snapshot");
        assert!(backend.restore(dir.path(), &legacy_path).is_none());
    }
}
