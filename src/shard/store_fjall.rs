//! `ShardedStateStoreFjall` — fjall-backed per-shard state store (Phase 53-03B).
//!
//! Sibling of the now-state-inmem-gated `ShardedStateStoreV1`. This module is
//! compiled only under the default (fjall) build; `#[cfg(not(feature =
//! "state-inmem"))]` in `src/shard/mod.rs` gates its registration.
//!
//! ## D-01 layout
//!
//! One `fjall::Keyspace` rooted at `data/fjall/` (opened by the boot path via
//! `fjall_backend::open_keyspace_from_env`). Each shard owns one
//! `fjall::PartitionHandle` named `shard-{index}` within that keyspace
//! (`fjall_backend::open_shard_partition`). `ShardedStateStoreFjall` wraps the
//! N partitions into N `Shard` structs via `Shard::with_partition` (Plan 03).
//!
//! ## Single-writer invariant
//!
//! See the module-level note on `src/shard/mod.rs`: `PartitionHandle` ops take
//! `&self`, so the type system does not enforce single-writer. The convention
//! is that the shard thread that owns `Shard` is the only thread that mutates
//! its partition via `StoreView::Sharded`. Cross-shard reads through cloned
//! handles are fine; cross-shard writes are NOT.
//!
//! ## Plan 03B trust boundary (T-53-03B-01)
//!
//! `shard_index_for_event` returns `(shard_hint_for_event % n)`; because `n`
//! is asserted `>= 1 && <= 256` at construction time, the returned index is
//! arithmetically always in-bounds for `self.shards[idx]`.

use std::sync::Arc;

use fjall::Keyspace;

use crate::routing::shard_hint::shard_hint_for_event;
use crate::shard::fjall_backend::{open_shard_partition, FjallConfig};
use crate::shard::traits::ShardedStateStore;
use crate::shard::Shard;

/// Fjall-backed implementation of `ShardedStateStore`.
///
/// Wraps an `Arc<Keyspace>` + `Vec<Shard>` where each `Shard` owns one
/// `PartitionHandle`. The keyspace is shared-owned (cheap `Arc::clone`) so the
/// boot path may hand the same pointer to `ConcurrentAppState.fjall_keyspace`.
pub struct ShardedStateStoreFjall {
    keyspace: Arc<Keyspace>,
    shards: Vec<Shard>,
}

impl std::fmt::Debug for ShardedStateStoreFjall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedStateStoreFjall")
            .field("shard_count", &self.shards.len())
            .finish()
    }
}

/// Phase 59.6 Wave 5 (TPC-PERF-11, D-D1) — key prefix reserving the
/// typed-row keyspace on a fjall `PartitionHandle` from the Value-path
/// `entity_key` keys. The V9/V10 Value-path writer stores
/// `key = entity_key.as_bytes()`; the typed-row path stores
/// `key = [TYPED_ROW_KEY_PREFIX, stream..., 0x00, entity_key...]` so
/// the two paths coexist on the same partition without collisions (SC-7).
pub const TYPED_ROW_KEY_PREFIX: u8 = 0xFF;

/// Phase 59.6 Wave 5 (TPC-PERF-11, D-D1) — build the fjall key for a
/// typed entity state row. The layout is:
/// `[TYPED_ROW_KEY_PREFIX=0xFF][stream_name_bytes][0x00][entity_key_bytes]`
///
/// The `0xFF` prefix reserves the typed namespace (no Value-path key
/// starts with 0xFF because entity_key strings are UTF-8; the 0x00
/// separator is harmless for UTF-8 data). The 0x00 separator splits
/// the stream name from the entity key unambiguously — no escaping
/// needed because UTF-8 strings don't contain NUL bytes. Any `\0` in
/// a stream name would be rejected at register time (schema validation).
pub fn make_typed_fjall_key(stream: &str, entity_key: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + stream.len() + 1 + entity_key.len());
    k.push(TYPED_ROW_KEY_PREFIX);
    k.extend_from_slice(stream.as_bytes());
    k.push(0x00);
    k.extend_from_slice(entity_key.as_bytes());
    k
}

/// Phase 59.6 Wave 5 (TPC-PERF-11, D-D1) — pure memcpy encoder for typed
/// rows. Produces a byte vector in the packed layout:
/// `[schema_id: u32 BE][payload_len: u32 BE][payload][arena_len: u32 BE][arena]`
///
/// No serde_json. No postcard. Direct byte concatenation — target
/// write cost is `payload_len + arena_len + 12` bytes / memcpy time,
/// which is the ~10-50× improvement called out in D-D1 over the
/// Value-path `postcard(SerializableEntityState)` encoder.
pub fn encode_typed_row_body(row: &crate::engine::schema::Row) -> Vec<u8> {
    let mut buf = Vec::with_capacity(row.payload.len() + row.arena.len() + 12);
    buf.extend_from_slice(&row.schema_id.to_be_bytes());
    buf.extend_from_slice(&(row.payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&row.payload);
    buf.extend_from_slice(&(row.arena.len() as u32).to_be_bytes());
    buf.extend_from_slice(&row.arena);
    buf
}

/// Phase 59.6 Wave 5 (TPC-PERF-11, D-D1) — pure memcpy decoder for
/// typed rows. Inverse of [`encode_typed_row_body`]. Validates every
/// offset boundary before slicing so corrupt bytes cannot trigger
/// out-of-bounds reads (threat `T-59.6-05-01`).
pub fn decode_typed_row_body(
    bytes: &[u8],
) -> Result<crate::engine::schema::Row, crate::error::BeavaError> {
    use crate::error::BeavaError;
    if bytes.len() < 12 {
        return Err(BeavaError::Protocol(format!(
            "decode_typed_row_body: short body ({} bytes, need >= 12)",
            bytes.len()
        )));
    }
    let schema_id = u32::from_be_bytes(bytes[0..4].try_into().unwrap());
    let payload_len = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if bytes.len() < 8 + payload_len + 4 {
        return Err(BeavaError::Protocol(format!(
            "decode_typed_row_body: payload out of bounds (need {}, have {})",
            8 + payload_len + 4,
            bytes.len()
        )));
    }
    let payload = bytes[8..8 + payload_len].to_vec();
    let arena_len_off = 8 + payload_len;
    let arena_len = u32::from_be_bytes(
        bytes[arena_len_off..arena_len_off + 4].try_into().unwrap(),
    ) as usize;
    let arena_off = arena_len_off + 4;
    if bytes.len() < arena_off + arena_len {
        return Err(BeavaError::Protocol(format!(
            "decode_typed_row_body: arena out of bounds (need {}, have {})",
            arena_off + arena_len,
            bytes.len()
        )));
    }
    let arena = bytes[arena_off..arena_off + arena_len].to_vec();
    Ok(crate::engine::schema::Row {
        schema_id,
        payload,
        arena,
    })
}

/// Phase 59.6 Wave 5 (TPC-PERF-11, D-D1) — put a typed entity-state
/// row into a fjall partition. Encodes via [`encode_typed_row_body`]
/// under the `[0xFF stream \0 entity_key]` key. Direct
/// `PartitionHandle::insert`; the caller (shard thread) holds the
/// single-writer invariant for the partition.
pub fn put_entity_typed(
    partition: &fjall::PartitionHandle,
    stream: &str,
    entity_key: &str,
    row: &crate::engine::schema::Row,
) -> Result<(), crate::error::BeavaError> {
    use crate::error::BeavaError;
    let key = make_typed_fjall_key(stream, entity_key);
    let body = encode_typed_row_body(row);
    partition
        .insert(key, body)
        .map_err(|e| BeavaError::Protocol(format!("fjall put_entity_typed: {e}")))?;
    Ok(())
}

/// Phase 59.6 Wave 5 (TPC-PERF-11, D-D1) — read a typed entity-state
/// row back from a fjall partition. Returns `None` if the key is
/// absent (fresh entity / evicted state). Decodes via
/// [`decode_typed_row_body`]; propagates any corruption / OOB errors.
pub fn get_entity_typed(
    partition: &fjall::PartitionHandle,
    stream: &str,
    entity_key: &str,
) -> Result<Option<crate::engine::schema::Row>, crate::error::BeavaError> {
    use crate::error::BeavaError;
    let key = make_typed_fjall_key(stream, entity_key);
    let Some(bytes) = partition
        .get(&key)
        .map_err(|e| BeavaError::Protocol(format!("fjall get_entity_typed: {e}")))?
    else {
        return Ok(None);
    };
    let row = decode_typed_row_body(bytes.as_ref())?;
    Ok(Some(row))
}

impl ShardedStateStoreFjall {
    /// Open (or create) N partitions inside `ks` and wrap each in a
    /// `Shard::with_partition`.
    ///
    /// # Panics
    /// Asserts `1 <= n <= 256` (T-53-03B-01 mitigation; matches
    /// `ShardedStateStoreV1::new`'s bound so the invariant is identical under
    /// both backends).
    ///
    /// # Errors
    /// Propagates `fjall::Error` from `open_shard_partition` on IO failure.
    pub fn new(n: u16, ks: Arc<Keyspace>, cfg: &FjallConfig) -> fjall::Result<Self> {
        assert!(n >= 1 && n <= 256, "shard count must be 1..=256");
        let mut shards = Vec::with_capacity(n as usize);
        for i in 0..n as usize {
            let partition = open_shard_partition(&ks, i, cfg)?;
            shards.push(Shard::with_partition(partition));
        }
        Ok(Self {
            keyspace: ks,
            shards,
        })
    }

    /// Return the shard index for a given event's routing key.
    ///
    /// At N=1 always returns 0 (fast-path); otherwise
    /// `(shard_hint_for_event(event, key_field) as usize) % self.shards.len()`.
    /// Identical contract to `ShardedStateStoreV1::shard_index_for_event` so
    /// routing is backend-agnostic.
    pub fn shard_index_for_event(
        &self,
        event: &serde_json::Value,
        key_field: Option<&str>,
    ) -> usize {
        let n = self.shards.len();
        if n == 1 {
            return 0;
        }
        (shard_hint_for_event(event, key_field) as usize) % n
    }

    /// Shared read access to shard `idx`.
    pub fn shard_at(&self, idx: usize) -> &Shard {
        &self.shards[idx]
    }

    /// Mutable read/write access to shard `idx`.
    pub fn shard_at_mut(&mut self, idx: usize) -> &mut Shard {
        &mut self.shards[idx]
    }

    /// Shared handle to the underlying keyspace. The boot path stashes a clone
    /// of this `Arc` inside `ConcurrentAppState.fjall_keyspace` so shutdown
    /// code (e.g. SIGTERM handlers) can call `keyspace.persist(SyncAll)`.
    pub fn keyspace(&self) -> &Arc<Keyspace> {
        &self.keyspace
    }
}

impl ShardedStateStore for ShardedStateStoreFjall {
    fn shard_count(&self) -> u16 {
        self.shards.len() as u16
    }

    fn for_each_shard<F: FnMut(&Shard)>(&self, mut f: F) {
        for s in &self.shards {
            f(s);
        }
    }

    fn for_each_shard_mut<F: FnMut(&mut Shard)>(&mut self, mut f: F) {
        for s in &mut self.shards {
            f(s);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::fjall_backend::{fjall_config_from_env, open_keyspace_from_env};
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn build_store(n: u16) -> (ShardedStateStoreFjall, tempfile::TempDir) {
        std::env::set_var("BEAVA_FJALL_FSYNC_DISABLE", "1");
        std::env::set_var("BEAVA_FJALL_CACHE_MB", "32");
        let cfg = fjall_config_from_env(n);
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let ks = open_keyspace_from_env(tmp.path(), &cfg).expect("open keyspace");
        let store = ShardedStateStoreFjall::new(n, ks, &cfg).expect("open store");
        (store, tmp)
    }

    #[test]
    fn new_allocates_n_shards() {
        let _g = env_lock().lock().unwrap();
        let (store, _tmp) = build_store(4);
        assert_eq!(store.shard_count(), 4);
    }

    #[test]
    fn shard_index_for_event_at_n1_is_zero() {
        let _g = env_lock().lock().unwrap();
        let (store, _tmp) = build_store(1);
        let idx = store.shard_index_for_event(&serde_json::json!({"k": "x"}), Some("k"));
        assert_eq!(idx, 0);
    }
}
