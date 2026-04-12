//! SlateDB persistence backend.
//!
//! Uses SlateDB (cloud-native LSM on object storage) as an alternative to
//! the file-based snapshot system. Enabled via `slatedb-backend` cargo feature.
//!
//! Config:
//! - `TALLY_BACKEND=slatedb` selects this backend
//! - `TALLY_SLATEDB_PATH` sets the data directory (default: `{TALLY_DATA_DIR}/slatedb`)
//!
//! Each entity key maps to a SlateDB key; the value is postcard-serialized
//! `SerializableEntityState`. Pipeline definitions and backfill markers are
//! stored under reserved prefix keys (`__meta/pipeline/{name}`,
//! `__meta/backfill_index`, etc.).
//!
//! Restore uses a full-table scan via `db.scan(..)` to enumerate all entity
//! keys (those not starting with `__meta/`), plus index keys for pipelines
//! and backfill markers.

use std::path::Path;
use std::sync::Arc;

use slatedb::Db;
use slatedb::object_store::local::LocalFileSystem;

use crate::state::persistence::{PersistenceBackend, PersistenceError, RestoredState};
use crate::state::snapshot::{
    BaseSnapshotState, DeltaSnapshotState, SerializableEntityState, SerializablePipeline,
};

/// Key prefix for pipeline metadata entries.
const META_PIPELINE_PREFIX: &str = "__meta/pipeline/";
/// Sentinel key that stores the latest sequence number.
const META_SEQ_KEY: &[u8] = b"__meta/seq";
/// Key for the pipeline name index.
const META_PIPELINE_INDEX: &[u8] = b"__meta/pipeline_index";
/// Key for the backfill-complete index.
const META_BACKFILL_INDEX: &[u8] = b"__meta/backfill_index";
/// Prefix used by all metadata keys. Entity keys must not start with this.
const META_PREFIX: &str = "__meta/";

/// SlateDB persistence backend. Stores entity state as individual key-value
/// pairs for efficient incremental updates.
pub struct SlateBackend {
    db: Db,
    /// Tokio runtime handle for running async SlateDB ops from sync context.
    rt: tokio::runtime::Handle,
}

impl SlateBackend {
    /// Open (or create) a SlateDB database at the given path.
    /// Must be called from an async context.
    pub async fn open(path: &str) -> Result<Self, PersistenceError> {
        // Ensure the directory exists.
        std::fs::create_dir_all(path)
            .map_err(|e| PersistenceError::Io(e))?;

        let store = Arc::new(
            LocalFileSystem::new_with_prefix(path)
                .map_err(|e| PersistenceError::Other(format!("object_store init: {}", e)))?,
        );
        let db = Db::open("/", store)
            .await
            .map_err(|e| PersistenceError::Other(format!("slatedb open: {}", e)))?;
        let rt = tokio::runtime::Handle::current();
        Ok(Self { db, rt })
    }

    /// Put a key-value pair, blocking on the async SlateDB API.
    fn put_blocking(&self, key: &[u8], value: &[u8]) -> Result<(), PersistenceError> {
        self.rt.block_on(async {
            self.db
                .put(key, value)
                .await
                .map_err(|e| PersistenceError::Other(format!("slatedb put: {}", e)))?;
            Ok(())
        })
    }

    /// Delete a key, blocking on the async SlateDB API.
    fn delete_blocking(&self, key: &[u8]) -> Result<(), PersistenceError> {
        self.rt.block_on(async {
            self.db
                .delete(key)
                .await
                .map_err(|e| PersistenceError::Other(format!("slatedb delete: {}", e)))?;
            Ok(())
        })
    }

    /// Get a value by key, blocking on the async SlateDB API.
    fn get_blocking(&self, key: &[u8]) -> Result<Option<bytes::Bytes>, PersistenceError> {
        self.rt.block_on(async {
            self.db
                .get(key)
                .await
                .map_err(|e| PersistenceError::Other(format!("slatedb get: {}", e)))
        })
    }

    /// Flush WAL to durable storage, blocking.
    fn flush_blocking(&self) -> Result<(), PersistenceError> {
        self.rt.block_on(async {
            self.db
                .flush()
                .await
                .map_err(|e| PersistenceError::Other(format!("slatedb flush: {}", e)))
        })
    }

    /// Store pipeline metadata.
    fn put_pipeline(&self, pipeline: &SerializablePipeline) -> Result<(), PersistenceError> {
        let meta_key = format!("{}{}", META_PIPELINE_PREFIX, pipeline.name);
        let value = postcard::to_stdvec(pipeline)?;
        self.put_blocking(meta_key.as_bytes(), &value)
    }

    /// Store current sequence number.
    fn put_seq(&self, seq: u64) -> Result<(), PersistenceError> {
        self.put_blocking(META_SEQ_KEY, &seq.to_be_bytes())
    }

    /// Read current sequence number.
    fn get_seq(&self) -> Result<u64, PersistenceError> {
        match self.get_blocking(META_SEQ_KEY)? {
            Some(bytes) if bytes.len() == 8 => {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&bytes);
                Ok(u64::from_be_bytes(buf))
            }
            _ => Ok(0),
        }
    }

    /// Write index keys used by the restore path.
    fn write_indexes(&self, base: &BaseSnapshotState) -> Result<(), PersistenceError> {
        // Pipeline name index.
        let pipeline_names: Vec<String> =
            base.pipelines.iter().map(|p| p.name.clone()).collect();
        let pipeline_index = postcard::to_stdvec(&pipeline_names)?;
        self.put_blocking(META_PIPELINE_INDEX, &pipeline_index)?;

        // Backfill index.
        let backfill_index = postcard::to_stdvec(&base.backfill_complete)?;
        self.put_blocking(META_BACKFILL_INDEX, &backfill_index)?;

        Ok(())
    }

    /// Close the SlateDB database gracefully.
    pub async fn close(self) {
        let _ = self.db.close().await;
    }
}

impl PersistenceBackend for SlateBackend {
    fn persist_base(
        &self,
        base: &BaseSnapshotState,
        _snap_dir: &Path,
    ) -> Result<usize, PersistenceError> {
        let mut total_bytes = 0usize;

        // Write indexes for restore path.
        self.write_indexes(base)?;

        // Write all entities.
        for (key, entity) in &base.entities {
            let value = postcard::to_stdvec(entity)?;
            total_bytes += key.len() + value.len();
            self.put_blocking(key.as_bytes(), &value)?;
        }

        // Write pipeline metadata.
        for pipeline in &base.pipelines {
            self.put_pipeline(pipeline)?;
        }

        // Update sequence.
        self.put_seq(base.header.sequence)?;

        // Flush to durable storage.
        self.flush_blocking()?;

        Ok(total_bytes)
    }

    fn persist_delta(
        &self,
        delta: &DeltaSnapshotState,
        _snap_dir: &Path,
    ) -> Result<usize, PersistenceError> {
        let mut total_bytes = 0usize;

        // Delete removed keys.
        for key in &delta.deleted_keys {
            self.delete_blocking(key.as_bytes())?;
        }

        // Write changed entities.
        for (key, entity) in &delta.changed_entities {
            let value = postcard::to_stdvec(entity)?;
            total_bytes += key.len() + value.len();
            self.put_blocking(key.as_bytes(), &value)?;
        }

        // Update sequence.
        self.put_seq(delta.header.sequence)?;

        // Flush.
        self.flush_blocking()?;

        Ok(total_bytes)
    }

    fn restore(
        &self,
        _snap_dir: &Path,
        _legacy_path: &Path,
    ) -> Option<RestoredState> {
        // Use SlateDB scan to enumerate all keys, filtering out __meta/ prefixed keys
        // for entity data, and reading indexes for pipelines/backfill.
        let entities = self.rt.block_on(async {
            let mut iter = self.db.scan::<&[u8], _>(..).await.ok()?;
            let mut entities = Vec::new();
            while let Some(kv) = iter.next().await.ok()? {
                let key_str = std::str::from_utf8(&kv.key).ok()?;
                if key_str.starts_with(META_PREFIX) {
                    continue; // Skip metadata keys.
                }
                if let Ok(entity) = postcard::from_bytes::<SerializableEntityState>(&kv.value) {
                    entities.push((key_str.to_string(), entity));
                }
            }
            Some(entities)
        })?;

        // If no entities found and no seq, this is a fresh DB.
        let seq = self.get_seq().unwrap_or(0);
        if entities.is_empty() && seq == 0 {
            return None;
        }

        // Restore pipeline metadata from index.
        let pipelines = match self.get_blocking(META_PIPELINE_INDEX).ok()? {
            Some(index_bytes) => {
                let pipeline_names: Vec<String> =
                    postcard::from_bytes(&index_bytes).unwrap_or_default();
                let mut pipelines = Vec::new();
                for name in &pipeline_names {
                    let meta_key = format!("{}{}", META_PIPELINE_PREFIX, name);
                    if let Ok(Some(value_bytes)) = self.get_blocking(meta_key.as_bytes()) {
                        if let Ok(pipeline) =
                            postcard::from_bytes::<SerializablePipeline>(&value_bytes)
                        {
                            pipelines.push(pipeline);
                        }
                    }
                }
                pipelines
            }
            None => Vec::new(),
        };

        // Restore backfill markers from index.
        let backfill_complete: Vec<(String, String)> = self
            .get_blocking(META_BACKFILL_INDEX)
            .ok()
            .flatten()
            .and_then(|b| postcard::from_bytes(&b).ok())
            .unwrap_or_default();

        Some(RestoredState {
            entities,
            pipelines,
            backfill_complete,
            next_seq: seq + 1,
            loaded_base_seq: seq,
        })
    }

    fn cleanup(&self, _snap_dir: &Path, _current_base_seq: u64) {
        // SlateDB handles compaction internally via its LSM tree.
        // No manual cleanup needed.
    }

    fn name(&self) -> &str {
        "slatedb"
    }
}
