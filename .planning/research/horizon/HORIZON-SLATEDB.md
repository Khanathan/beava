# Horizon Research -- SlateDB as Alternative State Backend

**Date:** 2026-04-12
**Scope:** Evaluate SlateDB as a durable state backend for Tally. Three integration options, concrete code sketches, performance expectations.
**Confidence scale:** HIGH = docs/API verified; MED = inferred from architecture; LOW = speculation.

---

## 1. What is SlateDB?

SlateDB is a Rust embedded key-value store built as an LSM-tree that writes to object storage via the `object_store` crate. CNCF sandbox project, Apache 2.0 licensed.

- **GitHub:** https://github.com/slatedb/slatedb
- **Latest version:** ~0.9.x (April 2026, releases every 2 months)
- **Storage model:** WAL + MemTable -> L0 SSTs -> compacted sorted runs, all on object storage
- **Backend options:** S3, GCS, Azure Blob, MinIO, Tigris, `LocalFileSystem`, `InMemory`
- **Key API:** `Db::open()`, `db.put()`, `db.get()`, `db.delete()`, `db.scan()`, `db.flush()`, `db.close()`
- **Async:** All operations are async (tokio-based)
- **Re-exports:** `slatedb::object_store` (no version-pinning headaches)

### 1.1 Core API Surface

```rust
use slatedb::{Db, Error};
use slatedb::object_store::local::LocalFileSystem;
use std::sync::Arc;

let store = Arc::new(LocalFileSystem::new("/data/tally/slatedb"));
let db = Db::open("/data/tally/slatedb", store).await?;

// Write
db.put(b"user:u123", serialized_bytes).await?;

// Read
let val: Option<Bytes> = db.get(b"user:u123").await?;

// Delete
db.delete(b"user:u123").await?;

// Scan prefix
let iter = db.scan_prefix(b"user:").await?;

// Batch write (atomic)
let mut batch = WriteBatch::new();
batch.put(b"k1", b"v1");
batch.put(b"k2", b"v2");
db.write(batch).await?;

db.close().await?;
```

### 1.2 Performance Characteristics

| Metric | SlateDB (local FS) | SlateDB (S3) | Tally DashMap |
|--------|-------------------|--------------|---------------|
| Read (hot/cached) | < 1ms | 50-100ms | < 1us |
| Read (cold) | ~1-5ms (disk) | 50-100ms | N/A (all hot) |
| Write (put) | ~1-5ms (flush) | 50-100ms | < 1us |
| Write (buffered) | ~1-50us (memtable) | ~1-50us | < 1us |
| Scan | ms range | ms-sec range | N/A |
| Throughput | ~50-200K ops/s | ~3.5K PUTs/s (S3 limit) | 1M+ ops/s |

**Key insight:** SlateDB's `put()` by default blocks until data reaches the object store (WAL flush). With `WriteOptions` that disable await-on-durable, writes land in the MemTable at microsecond speed -- but durability is deferred to the next flush interval. (MED)

### 1.3 Dependencies

SlateDB pulls in `object_store`, `bytes`, `tokio`, `flatbuffers`, `log`, `thiserror`, plus compaction/bloom filter internals. Expect ~80-120 transitive crates. This is significant but manageable -- Tally already depends on tokio. (MED)

---

## 2. Integration Options

### Option A: Replace Snapshot Persistence (Recommended First)

Keep DashMap as the hot-path state store. Use SlateDB as a durable checkpoint backend instead of bincode snapshot files.

**Architecture:**

```
Hot path (unchanged):
  PUSH -> DashMap.get_mut() -> update operators -> return features  [< 100us]

Background persistence (new):
  Every 30s: iterate dirty keys -> db.put(key, serialize(entity)) -> clear dirty set
  On startup: db.scan_prefix("entity:") -> populate DashMap
```

**Advantages:**
- Zero impact on hot-path latency (DashMap untouched)
- Replaces complex base+delta snapshot format with simple KV writes
- Incremental by nature: only dirty keys written each cycle
- No "big bang" snapshot serialization -- writes amortized over time
- Natural TTL via `db.delete()` on eviction (vs tracking deleted_keys set)
- Scan-based restore simpler than file-format versioning
- Path to S3-backed durability without code changes (swap LocalFileSystem for S3)

**Disadvantages:**
- Restore potentially slower: scan all keys vs load one file
- Additional disk I/O during steady state (LSM compaction)
- More dependencies in the binary
- SlateDB's compaction runs background threads (memory/CPU overhead)

**Code sketch -- persistence loop:**

```rust
// src/state/slate_backend.rs

use slatedb::{Db, object_store::local::LocalFileSystem};
use std::sync::Arc;

pub struct SlateBackend {
    db: Db,
}

impl SlateBackend {
    pub async fn open(path: &str) -> Result<Self, slatedb::Error> {
        let store = Arc::new(LocalFileSystem::new(path));
        let db = Db::open(path, store).await?;
        Ok(Self { db })
    }

    /// Persist dirty entities from the StateStore.
    pub async fn persist_dirty(
        &self,
        store: &StateStore,
    ) -> Result<usize, slatedb::Error> {
        let dirty = store.take_dirty_keys();
        let mut count = 0;

        for key in &dirty {
            if let Some(entity) = store.get_entity(key) {
                let serializable = entity.to_serializable();
                let bytes = postcard::to_allocvec(&serializable)
                    .expect("serialization should not fail");
                let db_key = format!("entity:{}", key);
                self.db.put(db_key.as_bytes(), &bytes).await?;
                count += 1;
            }
        }

        // Persist deleted keys
        let deleted = store.take_deleted_keys();
        for key in &deleted {
            let db_key = format!("entity:{}", key);
            self.db.delete(db_key.as_bytes()).await?;
        }

        Ok(count)
    }

    /// Restore all entities into a StateStore on startup.
    pub async fn restore(&self) -> Result<StateStore, slatedb::Error> {
        let store = StateStore::new();
        let mut iter = self.db.scan_prefix(b"entity:").await?;

        while let Some(kv) = iter.next().await {
            let kv = kv?;
            let entity_key = std::str::from_utf8(&kv.key[7..]) // skip "entity:"
                .expect("keys are utf8")
                .to_string();
            let entity: SerializableEntityState = postcard::from_bytes(&kv.value)
                .expect("deserialization should not fail");
            store.insert_from_snapshot(entity_key, entity);
        }

        Ok(store)
    }

    /// Persist pipeline definitions.
    pub async fn persist_pipelines(
        &self,
        pipelines: &[SerializablePipeline],
    ) -> Result<(), slatedb::Error> {
        let bytes = postcard::to_allocvec(pipelines)
            .expect("serialization should not fail");
        self.db.put(b"meta:pipelines", &bytes).await?;
        Ok(())
    }

    pub async fn close(self) -> Result<(), slatedb::Error> {
        self.db.close().await
    }
}
```

**Estimated effort:** 3-4 days. Trait abstraction + SlateDB impl + config flag + restore path + tests.

### Option B: Replace DashMap Entirely (Not Recommended for v1)

Use SlateDB as the primary state store. Every PUSH does `db.get()` + `db.put()`.

**Why this does not work for Tally:**
- SlateDB `get()` on hot data (memtable hit): ~1-50us. Acceptable in isolation.
- But: `get()` returns `Bytes`, requiring deserialization of the full `EntityState` on every read.
- Then: modify operators in memory, re-serialize, `put()` back.
- Round-trip: deserialize (~5-20us) + operator update (~1us) + serialize (~5-20us) + put (~1-50us) = **12-90us best case**.
- This blows the <100us p99 budget on serialization alone, before any operator logic.
- Under load with compaction running, expect p99 to spike to 200-500us.
- DashMap: get_mut returns a direct mutable reference. Zero ser/de. ~200ns.

**Verdict:** Option B is a 100-500x regression on the hot path. Do not pursue. (HIGH confidence)

### Option C: Tiered Storage -- Hot DashMap + Cold SlateDB

Hot entities live in DashMap. When eviction removes a key, write it to SlateDB instead of dropping it. On access for an evicted key, pull from SlateDB back into DashMap.

**Architecture:**

```
PUSH for key "u123":
  1. DashMap.get_mut("u123")
  2. If present -> update operators, return features  [< 100us, common case]
  3. If absent -> db.get("entity:u123").await
     a. If found -> deserialize, insert into DashMap, update, return  [~50-200us, rare]
     b. If not found -> create fresh entity, proceed normally

Eviction (background):
  For each evicted key:
    serialize entity -> db.put("entity:{key}", bytes) -> DashMap.remove(key)
```

**Advantages:**
- Hot path unchanged for active entities (DashMap)
- Evicted entities survive restarts without needing snapshot of everything
- Memory bounded by DashMap size, cold data on disk/S3
- Graceful degradation: rarely-accessed keys have higher latency but still work

**Disadvantages:**
- Cold-path PUSH becomes async (needs `.await` for SlateDB get)
- Complicates the PUSH handler: two code paths depending on hot/cold
- Eviction becomes a write instead of a drop (I/O pressure)
- Need to handle races: what if an entity is being restored from SlateDB while a PUSH arrives?

**Estimated effort:** 5-7 days. More complex than Option A due to hot/cold transition logic and async considerations in the PUSH path.

---

## 3. Recommended Implementation Plan (Phase 19)

### Phase 19: SlateDB Durable Backend (Option A)

Ship Option A first. It is the lowest-risk integration that delivers real value (simpler persistence, path to S3 durability) without touching the hot path.

**Step 1: Backend trait abstraction (1 day)**

```rust
// src/state/backend.rs

use crate::state::store::StateStore;
use crate::state::snapshot::SerializablePipeline;

#[async_trait::async_trait]
pub trait PersistenceBackend: Send + Sync {
    /// Persist dirty entities from the store.
    async fn persist_dirty(&self, store: &StateStore) -> Result<usize, anyhow::Error>;

    /// Restore all state on startup.
    async fn restore(&self) -> Result<(StateStore, Vec<SerializablePipeline>), anyhow::Error>;

    /// Persist pipeline definitions.
    async fn persist_pipelines(&self, pipelines: &[SerializablePipeline]) -> Result<(), anyhow::Error>;

    /// Shutdown cleanly.
    async fn close(&self) -> Result<(), anyhow::Error>;
}
```

**Step 2: Wrap existing snapshot logic behind the trait (0.5 day)**

Implement `PersistenceBackend` for `SnapshotBackend` that delegates to the current `save_base_snapshot` / `load_snapshot` functions. This ensures zero regression.

**Step 3: Implement SlateDB backend (1.5 days)**

```toml
# Cargo.toml addition
[dependencies]
slatedb = { version = "0.9", optional = true }

[features]
default = []
slatedb-backend = ["slatedb"]
```

```rust
// src/state/slate_backend.rs
#[cfg(feature = "slatedb-backend")]
pub struct SlateBackend { db: slatedb::Db }

#[cfg(feature = "slatedb-backend")]
#[async_trait::async_trait]
impl PersistenceBackend for SlateBackend {
    async fn persist_dirty(&self, store: &StateStore) -> Result<usize, anyhow::Error> {
        // iterate dirty keys, serialize with postcard, db.put()
        // iterate deleted keys, db.delete()
        // use WriteBatch for atomicity per flush cycle
    }

    async fn restore(&self) -> Result<(StateStore, Vec<SerializablePipeline>), anyhow::Error> {
        // db.scan_prefix("entity:") -> deserialize each -> populate StateStore
        // db.get("meta:pipelines") -> deserialize pipeline list
    }
    // ...
}
```

**Step 4: Config flag + startup wiring (0.5 day)**

```
TALLY_BACKEND=snapshot   # default, existing behavior
TALLY_BACKEND=slatedb    # new, requires --features slatedb-backend
TALLY_SLATEDB_PATH=/data/tally/state  # SlateDB data directory
```

**Step 5: Tests + benchmarks (1 day)**

- Unit test: persist 1000 entities -> restore -> verify equality
- Integration test: PUSH events, trigger persist, kill process, restart, verify state
- Benchmark: persist cycle time for 10K/100K/1M dirty keys
- Benchmark: restore time for 10K/100K/1M entities
- Compare with existing snapshot: file size, write time, restore time

### Milestone gates

| Gate | Target | Rationale |
|------|--------|-----------|
| Persist 100K dirty keys | < 5s | Must complete within 2x snapshot interval |
| Restore 1M entities | < 10s | Parity with current snapshot restore |
| Hot-path latency delta | 0us | Option A must not touch the hot path |
| Binary size delta | < 5MB | SlateDB + object_store overhead |

---

## 4. Option C Follow-up (Phase 19b, if needed)

Only pursue Option C if memory pressure becomes a real problem (i.e., users report OOM with millions of entities that mostly sit idle). Option C adds:

- `SlateEvictionSink` that writes to SlateDB instead of dropping
- `SlateRestoreSource` in the PUSH handler that checks SlateDB on DashMap miss
- LRU or TTL-based promotion/demotion policy
- Async PUSH handler for the cold path (requires careful design)

**Estimated effort:** 5-7 additional days on top of Option A.

---

## 5. Key Risks and Mitigations

| Risk | Severity | Mitigation |
|------|----------|------------|
| SlateDB compaction CPU spikes | MED | Run on LocalFileSystem (no S3 latency), tune `l0_max_ssts` |
| SlateDB panics on local FS edge cases | MED | Known issue (#607). Pin version, test on target OS. |
| object_store crate version conflicts | LOW | SlateDB re-exports `object_store`; use their version |
| Dependency bloat | LOW | Feature-gate behind `slatedb-backend`; not compiled by default |
| Restore slower than snapshot file | MED | Benchmark early. SlateDB scan is sequential; may need parallel prefetch. |
| SlateDB < 1.0 API stability | MED | Pin exact version, wrap behind trait for easy swap |

---

## 6. What SlateDB Unlocks Long-Term

If Option A works well, SlateDB opens several future paths:

1. **S3 durability** -- swap `LocalFileSystem` for `S3` object store. State survives disk loss. Zero code changes in the persistence layer.
2. **Multi-node warm standby** -- a second Tally instance reads SlateDB from the same S3 bucket for disaster recovery.
3. **Cold entity queries** -- serve GET requests for evicted entities directly from SlateDB without restoring to memory (Option C lite).
4. **Checkpoint/restore for upgrades** -- flush all state to SlateDB, upgrade binary, restore. Simpler than snapshot format migration.

---

## 7. Alternatives Considered

| Alternative | Pros | Cons | Verdict |
|-------------|------|------|---------|
| RocksDB (rust-rocksdb) | Battle-tested, fast local reads | C++ dependency, complex build, no object storage path | Too heavy for Tally's "one binary" philosophy |
| sled | Pure Rust, embedded | Maintenance uncertain, no object storage | Risk of abandonment |
| redb | Pure Rust, simple API, ACID | No object storage path, B-tree (not LSM) | Good for local-only but no S3 future |
| fjall/lsm-tree | Pure Rust LSM | Newer, less proven | Worth watching but SlateDB has more momentum |
| Keep snapshots | Zero new deps | No S3 path, format versioning burden | Current default, works fine |

**SlateDB wins on:** Rust-native, object_store abstraction (local + S3), active maintenance (CNCF sandbox), clean async API, and alignment with Tally's future S3 durability goals.
