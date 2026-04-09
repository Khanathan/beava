# Phase 6: Foundation - Research

**Researched:** 2026-04-09
**Domain:** EntityState restructuring, SSD event log, per-stream TTL, MGET command
**Confidence:** HIGH

## Summary

Phase 6 is a foundational restructuring phase that touches nearly every layer of the Tally codebase. It accomplishes three distinct goals: (1) restructure `EntityState` from a flat feature map to per-stream isolated groups so that per-stream entity TTL is possible and Phase 7+ composable pipeline features have clean boundaries; (2) introduce an append-only SSD event log using per-stream log files with `BufWriter<File>` + periodic `fdatasync` to avoid blocking the hot path; and (3) add the MGET batch-read command as an operational improvement.

The EntityState restructure is the most structurally invasive change -- it modifies `store.rs`, `snapshot.rs`, `eviction.rs`, `pipeline.rs`, and `tcp.rs`. The event log is additive (new `src/state/event_log.rs` module) but requires integration into the PUSH hot path. MGET is the simplest change: a new opcode (0x06) in protocol.rs and a handler in tcp.rs.

**Primary recommendation:** Implement EntityState restructure first (it changes data structures everything depends on), then event log (new module with hot-path integration), then MGET and per-stream TTL (smaller changes building on the new structure).

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Per-stream log files (one file per registered stream) for independent compaction and TTL management
- Length-prefixed postcard serialization for log entries (consistent with snapshot format, compact binary)
- Timer-based background compaction every 60 seconds with cooperative yielding (like MSET chunking)
- Default history_ttl of 72 hours (3 days) when not configured per stream
- Opcode 0x06 for MGET (next sequential after REGISTER=0x05)
- Response format: nested JSON map `{ "key1": { "feat": val }, "key2": { "feat": val } }`
- Missing keys return empty map `{}` in response (consistent with GET behavior)
- No hard limit on keys per MGET request (server trusts client, like MSET)
- EntityState uses `HashMap<StreamName, StreamEntityState>` for per-stream isolation (StreamEntityState holds operators + last_event_at)
- Snapshot format bumped to v4 (clean break from v3, no migration)
- GET response merges all streams' features into flat map (simple API, no nesting)
- Per-stream entity TTL via `entity_ttl` field on StreamDefinition (set at registration time via Python SDK)

### Claude's Discretion
- Event log directory structure and naming conventions
- Compaction implementation details (rewrite vs truncate)
- BufWriter buffer sizing and fdatasync interval
- MGET wire format details (string encoding for multiple keys)

### Deferred Ideas (OUT OF SCOPE)
None -- discussion stayed within phase scope.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| ELOG-01 | Keyless streams persist events as append-only log on local SSD | EventLog module design with per-stream files; keyless streams are purely append-only, never compacted |
| ELOG-02 | Keyed streams persist events as append-only log that gets compacted | Same EventLog module; keyed stream logs are eligible for compaction (TTL-based deletion of expired entries) |
| ELOG-03 | Event log writes do not block the hot path (buffered async writes) | BufWriter<File>::write is ~100-300ns memcpy; fdatasync in background timer, not on hot path |
| ELOG-04 | User can configure history TTL per stream controlling event retention | `history_ttl` field on StreamDefinition, passed through Python SDK, stored in EventLog config |
| ELOG-05 | Background compaction removes events older than history TTL | Timer-based compaction every 60s with cooperative yielding; rewrites log file excluding expired entries |
| OPS-01 | User can fetch features for multiple keys in a single MGET call | New MGET opcode 0x06 in protocol, handler iterates keys calling existing get_all_features |
| OPS-02 | User can configure entity state TTL per dataset/stream | `entity_ttl` on StreamDefinition; EntityState restructure enables per-stream last_event_at tracking and independent expiry |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.51 | Async runtime, timers for fsync/compaction | Already in use; current_thread flavor, timers for periodic tasks [VERIFIED: Cargo.toml] |
| postcard | 1.1.3 | Binary serialization for log entries | Already in use for snapshots; compact, fast, no RUSTSEC issues [VERIFIED: Cargo.toml] |
| serde | 1.0.228 | Derive serialization for new types | Already in use throughout [VERIFIED: Cargo.toml] |
| ahash | 0.8.12 | HashMap for per-stream state grouping | Already in use; locked decision from v1.0 [VERIFIED: Cargo.toml] |
| serde_json | 1.0.149 | JSON response for MGET | Already in use [VERIFIED: Cargo.toml] |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| tempfile | 3.27 | Test-only temp directories for event log tests | Already a dev-dependency [VERIFIED: Cargo.toml] |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| postcard for log entries | Raw bytes/custom format | More complex parsing; postcard is already proven in snapshot path |
| std::fs::File + BufWriter | tokio::fs::File | tokio `fs` feature not enabled and not needed; BufWriter::write is non-blocking (memcpy to buffer), fsync via spawn_blocking |
| Per-stream log files | Single unified log file | Single file simpler but makes per-stream compaction/TTL impossible without complex offset tracking |

**Installation:**
No new dependencies required. All libraries are already in Cargo.toml.

## Architecture Patterns

### Recommended Project Structure
```
src/
├── state/
│   ├── store.rs          # MODIFIED: EntityState -> per-stream grouping
│   ├── snapshot.rs        # MODIFIED: v4 format with StreamEntityState
│   ├── eviction.rs        # MODIFIED: per-stream TTL instead of global
│   └── event_log.rs       # NEW: append-only SSD event log
├── engine/
│   └── pipeline.rs        # MODIFIED: history_ttl, entity_ttl on StreamDefinition
├── server/
│   ├── protocol.rs        # MODIFIED: MGET opcode 0x06
│   └── tcp.rs             # MODIFIED: MGET handler, event log write on PUSH
└── main.rs                # MODIFIED: EventLog init, fsync timer, compaction timer
```

### Event Log Directory Structure (Claude's Discretion)
```
{data_dir}/events/
├── Transactions.log      # One file per stream
├── Logins.log
└── MerchantActivity.log
```

**Recommendation:** Use `{TALLY_DATA_DIR}/events/{stream_name}.log` where `TALLY_DATA_DIR` defaults to the current working directory. Stream names are already validated as non-empty strings, but should be sanitized for filesystem safety (replace `/`, `\`, NUL bytes). [ASSUMED]

### Pattern 1: EntityState Per-Stream Restructure

**What:** Transform the flat `EntityState` into a per-stream grouped structure where each stream's operators and last_event_at are isolated.

**Current structure (v1.0):**
```rust
// CURRENT: flat, all streams share one entity
struct EntityState {
    live_operators: Vec<(String, OperatorState)>,  // all streams mixed
    static_features: AHashMap<String, StaticFeature>,
    last_event_at: Option<SystemTime>,  // global, not per-stream
}
```

**New structure (v1.1):**
```rust
// NEW: per-stream isolation
struct StreamEntityState {
    operators: Vec<(String, OperatorState)>,  // only this stream's operators
    last_event_at: Option<SystemTime>,        // per-stream timestamp
}

struct EntityState {
    streams: AHashMap<String, StreamEntityState>,  // keyed by stream name
    static_features: AHashMap<String, StaticFeature>,  // unchanged
}
```

**Why per-stream grouping matters:**
1. Per-stream entity TTL: A short-TTL stream (e.g., 5-minute session tracking) can expire independently of a long-TTL stream (e.g., 24-hour transaction history) for the same entity key
2. Clean operator isolation: No ambiguity about which operators belong to which stream (currently disambiguated by feature name matching, which is fragile)
3. Foundation for Phase 7: Composable pipeline needs clear stream boundaries for DAG execution
4. Foundation for Phase 8: Schema evolution needs per-stream operator management

**Impact on existing code:**
- `push()` in pipeline.rs: Instead of searching `entity.live_operators` by feature name, access `entity.streams.get_or_insert(stream_name).operators` directly
- `get_all_features()` in store.rs: Iterate all `entity.streams` values and merge operators, then overlay static_features
- `get_feature_value()` in store.rs: Search across all streams' operators for the feature name
- `clone_for_snapshot()` / `restore_from_snapshot()`: New SerializableEntityState format with per-stream grouping
- `remove_expired_entities()` in eviction.rs: Now per-stream -- remove individual stream entries, only remove the whole entity when all streams are expired

[VERIFIED: codebase analysis of src/state/store.rs, src/engine/pipeline.rs]

### Pattern 2: Event Log with Buffered Writes

**What:** Append raw event bytes to per-stream log files using `BufWriter<File>`, with periodic `fdatasync` via a background timer.

**Log entry format (postcard-serialized):**
```rust
#[derive(Serialize, Deserialize)]
struct LogEntry {
    timestamp: SystemTime,
    payload: Vec<u8>,  // raw JSON bytes from PUSH
}
```

Each entry is length-prefixed on disk: `[u32 BE entry_len][postcard bytes]`. This matches the snapshot format convention and allows sequential scanning.

**Write path (on PUSH hot path):**
```rust
// In handle_sync_command, after engine.push() succeeds:
if let Some(writer) = event_log.get_writer(&stream_name) {
    let entry = LogEntry { timestamp: now, payload: raw_event_bytes };
    let encoded = postcard::to_stdvec(&entry)?;
    let len_bytes = (encoded.len() as u32).to_be_bytes();
    writer.write_all(&len_bytes)?;   // ~50ns (memcpy into BufWriter buffer)
    writer.write_all(&encoded)?;     // ~100-200ns (memcpy)
}
```

**Fsync path (background timer):**
```rust
// Every 1 second (Claude's discretion: 1s is Redis's "everysec" default)
for writer in event_log.writers.values_mut() {
    // fdatasync flushes data to disk without updating metadata
    writer.flush()?;
    writer.get_ref().sync_data()?;
}
```

**Why BufWriter + std::fs, not tokio::fs:**
- tokio `fs` feature is not enabled in Cargo.toml [VERIFIED: Cargo.toml + cargo tree]
- BufWriter::write_all is synchronous but effectively a memcpy (~100-300ns for typical event payloads)
- The single-threaded model means there is no contention; the write is just copying bytes into the BufWriter's internal buffer
- fdatasync is done via spawn_blocking on the background timer, same pattern as snapshot writes

[VERIFIED: codebase pattern from snapshot writes in main.rs; CITED: Redis AOF everysec pattern]

### Pattern 3: MGET Command

**What:** Batch GET for multiple keys in a single round trip.

**Wire format (Claude's discretion):**
```
MGET (0x06):
  [u32 BE key_count]
  [u16-string key_1]
  [u16-string key_2]
  ...
  [u16-string key_n]
```

This mirrors the MSET pattern: count prefix followed by repeated string-encoded keys. Simpler than MSET because there is no per-key payload.

**Response format (locked decision):**
```json
{
  "key1": {"tx_count_1h": 5, "tx_sum_1h": 250.0},
  "key2": {"tx_count_1h": 3},
  "key3": {}
}
```

**Implementation:**
```rust
// In handle_sync_command:
Command::Mget { keys } => {
    let mut app = state.lock().unwrap_or_else(|e| e.into_inner());
    let mut result = serde_json::Map::new();
    for key in &keys {
        let features = app.engine.get_features(key, &mut app.store, now);
        let feature_json: serde_json::Map<String, serde_json::Value> = features
            .iter()
            .map(|(k, v)| (k.clone(), v.to_json_value()))
            .collect();
        result.insert(key.clone(), serde_json::Value::Object(feature_json));
    }
    Ok(serde_json::to_vec(&serde_json::Value::Object(result)).unwrap())
}
```

Note: MGET is synchronous (not chunked like MSET) because GET is read-only and fast. Each get_all_features call is O(operators) per entity, and the response serialization is bounded. Unlike MSET which writes state for potentially 100K keys, MGET reads are non-destructive and unlikely to exceed a few hundred keys in practice. [VERIFIED: existing GET handler is synchronous in tcp.rs]

### Pattern 4: Compaction (Rewrite Strategy)

**What:** Background task that rewrites per-stream log files, excluding entries older than the stream's `history_ttl`.

**Recommendation (Claude's discretion): Rewrite to temp file, then rename.**

This is the same atomic-replace pattern used for snapshots:
1. Read entries from `Transactions.log`, filtering out expired entries
2. Write surviving entries to `Transactions.log.tmp`
3. `rename("Transactions.log.tmp", "Transactions.log")`

**Why rewrite, not truncate:**
- Truncate only works for removing from the beginning (not supported on most filesystems without rewriting)
- Expired entries can be interleaved with valid entries (entity A's event from 4 days ago, entity B's event from 1 hour ago)
- Rewrite is what Redis AOF rewrite does [CITED: Redis AOF persistence docs]
- Atomic rename means no corruption window

**Cooperative yielding pattern:**
```rust
// Process entries in chunks of 1024
for chunk in entries.chunks(1024) {
    for entry in chunk {
        if !is_expired(entry, history_ttl) {
            write_entry(&mut tmp_writer, entry)?;
        }
    }
    tokio::task::yield_now().await;
}
```

For keyed streams (ELOG-02): Compaction both removes expired entries AND can be extended later (v1.2 ELOG-F1) to merge/deduplicate.

For keyless streams (ELOG-01): Keyless streams are append-only. Compaction still removes expired entries (those older than history_ttl), but there is no merge/dedup since there are no keys.

[VERIFIED: MSET cooperative yielding pattern in tcp.rs; VERIFIED: snapshot atomic rename in main.rs]

### Anti-Patterns to Avoid
- **Calling fdatasync on the hot path:** Even one fdatasync call adds 0.5-10ms latency (depending on SSD), destroying the <100us p99 target. Always use buffered writes and background fsync. [ASSUMED: standard SSD fsync latency]
- **Single global event log file:** Makes per-stream TTL and compaction extremely complex (need to track byte offsets per stream within a shared file). Per-stream files are simpler. [VERIFIED: locked decision]
- **Holding Mutex across fdatasync:** The fdatasync must happen outside the AppState lock. Clone or flush the BufWriter buffer, release the lock, then fdatasync in spawn_blocking. [VERIFIED: snapshot pattern in main.rs]
- **Mixing event log with snapshot:** The event log and snapshot serve different purposes. Snapshots capture operator state; the event log captures raw events for replay. They must remain independent. [CITED: ARCHITECTURE.md research]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Binary serialization for log entries | Custom byte packing | postcard (already used) | Battle-tested, serde-compatible, compact |
| Cooperative yielding for compaction | Custom work scheduler | tokio::task::yield_now() + chunks | Already proven in MSET handler |
| Atomic file replacement | Manual write-then-delete | std::fs::rename (tmp file) | Already proven in snapshot path |
| Hash maps for per-stream state | Custom array search | AHashMap (already used) | O(1) lookup, locked decision from v1.0 |

**Key insight:** Nearly every pattern needed in Phase 6 has an existing codebase precedent. The MSET chunking pattern becomes compaction yielding. The snapshot atomic-rename pattern becomes log file compaction. The snapshot serialize pattern becomes log entry serialization. Follow existing patterns.

## Common Pitfalls

### Pitfall 1: EntityState Migration Breaks Snapshot Loading
**What goes wrong:** After restructuring EntityState, the v3 snapshot format no longer matches the new v4 structure. If someone upgrades Tally mid-flight, their snapshot loads nothing.
**Why it happens:** Postcard deserialization is strict -- structural changes produce deserialization failures.
**How to avoid:** Bump SNAPSHOT_FORMAT_VERSION to 4. load_snapshot already handles version mismatches gracefully (returns None, prints warning, starts fresh). This is explicitly the locked decision: clean break, no migration.
**Warning signs:** Tests that construct SerializableEntityState directly will fail to compile after the restructure -- which is correct and desired.
[VERIFIED: load_snapshot in snapshot.rs handles version mismatch by returning None]

### Pitfall 2: Per-Stream Eviction Removing Entire Entity Too Early
**What goes wrong:** When implementing per-stream TTL, removing a stream entry for one expired stream inadvertently removes the entire entity (including other streams' state).
**Why it happens:** Confusing "remove this stream's entry from the entity" with "remove the entire entity key from the store."
**How to avoid:** Two-phase eviction: (1) iterate entity's streams, remove expired stream entries; (2) if entity has zero remaining stream entries AND zero static_features, remove the entity entirely.
**Warning signs:** After evicting one stream's state, other streams' features for the same key return Missing.

### Pitfall 3: BufWriter Not Flushed Before Compaction Rename
**What goes wrong:** The BufWriter has unflushed bytes when compaction renames the log file. The old file handle still has buffered writes that get lost.
**Why it happens:** BufWriter keeps an internal buffer. If you replace the underlying file (via rename) while the BufWriter is still holding the old file descriptor, you lose the buffered bytes.
**How to avoid:** Before compaction: flush the BufWriter, close the file handle, do the compaction rewrite, then reopen a new BufWriter for the stream.
**Warning signs:** Missing events at compaction boundaries; event counts decrease after compaction by more than the expired count.

### Pitfall 4: MGET Response Includes Qualified Feature Names
**What goes wrong:** get_features in pipeline.rs inserts qualified names like `"Transactions.tx_count_1h"` alongside unqualified `"tx_count_1h"`. MGET returns both, making the response unexpectedly large and confusing.
**Why it happens:** get_features adds qualified names for view expression evaluation. These are internal and should not appear in the response.
**How to avoid:** Either strip qualified names before serialization, or use get_all_features (which does not add qualified names) for MGET. If MGET should behave identically to GET, use get_features and strip. If MGET should be simpler (no view evaluation), use get_all_features.
**Warning signs:** Feature names containing dots in MGET responses.
[VERIFIED: get_features in pipeline.rs lines 330-341 adds qualified names]

### Pitfall 5: Event Log File Handles Leak on Stream Deregistration
**What goes wrong:** When a stream is deleted via DELETE /pipelines/:name, the event log writer for that stream is not closed.
**Why it happens:** Stream removal only touches PipelineEngine, not EventLog.
**How to avoid:** When removing a stream, also close and remove the corresponding event log writer.
**Warning signs:** File descriptor count grows over time with repeated register/deregister cycles.

### Pitfall 6: Compaction Timer and PUSH Race on File Handle
**What goes wrong:** Compaction is rewriting the log file while a concurrent PUSH tries to append to the same file's BufWriter.
**Why it happens:** Both compaction and PUSH access the same per-stream writer.
**How to avoid:** Since Tally is single-threaded (current_thread runtime), the Mutex serializes access. But compaction should be an async task that acquires the lock, detaches the writer, releases the lock, does the rewrite outside the lock, then re-acquires the lock to install the new writer. The key insight is that during compaction of one stream, PUSHes to OTHER streams can continue -- only PUSHes to the stream being compacted need to be buffered or skip logging temporarily.
**Warning signs:** Corrupt log entries, panics in BufWriter::write_all.

## Code Examples

### New EntityState Structure
```rust
// Source: derived from CONTEXT.md locked decisions + existing store.rs patterns
use std::time::SystemTime;
use ahash::AHashMap;
use serde::{Serialize, Deserialize};
use crate::state::snapshot::OperatorState;

/// Per-stream state within an entity. Isolates operators and last_event_at
/// per stream for independent TTL management.
#[derive(Debug, Clone)]
pub struct StreamEntityState {
    pub operators: Vec<(String, OperatorState)>,
    pub last_event_at: Option<SystemTime>,
}

/// Per-entity state. Now groups live features by stream name.
#[derive(Debug, Clone)]
pub struct EntityState {
    pub streams: AHashMap<String, StreamEntityState>,
    pub static_features: AHashMap<String, StaticFeature>,
}
```

### Serializable Snapshot v4 Structure
```rust
// Source: derived from existing snapshot.rs patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableStreamEntityState {
    pub operators: Vec<(String, OperatorState)>,
    pub last_event_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableEntityState {
    pub streams: Vec<(String, SerializableStreamEntityState)>,
    pub static_features: Vec<(String, StaticFeature)>,
}
```

### EventLog Module
```rust
// Source: derived from ARCHITECTURE.md research + snapshot pattern in main.rs
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use ahash::AHashMap;
use serde::{Serialize, Deserialize};

/// A single log entry: timestamp + raw event payload.
#[derive(Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: SystemTime,
    pub payload: Vec<u8>,
}

pub struct EventLog {
    log_dir: PathBuf,
    writers: AHashMap<String, BufWriter<File>>,
    /// Per-stream history TTL. Streams not in this map are not logged.
    history_ttls: AHashMap<String, Duration>,
}

impl EventLog {
    pub fn new(log_dir: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&log_dir)?;
        Ok(Self {
            log_dir,
            writers: AHashMap::new(),
            history_ttls: AHashMap::new(),
        })
    }

    /// Register a stream for event logging with its history TTL.
    pub fn register_stream(&mut self, stream_name: &str, history_ttl: Duration) -> std::io::Result<()> {
        self.history_ttls.insert(stream_name.to_string(), history_ttl);
        if !self.writers.contains_key(stream_name) {
            let path = self.log_dir.join(format!("{}.log", stream_name));
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            self.writers.insert(stream_name.to_string(), BufWriter::new(file));
        }
        Ok(())
    }

    /// Append an event to the stream's log. Returns Ok(false) if stream is not logged.
    pub fn append(&mut self, stream_name: &str, event_bytes: &[u8], now: SystemTime) -> std::io::Result<bool> {
        let writer = match self.writers.get_mut(stream_name) {
            Some(w) => w,
            None => return Ok(false),  // Stream not registered for logging
        };
        let entry = LogEntry { timestamp: now, payload: event_bytes.to_vec() };
        let encoded = postcard::to_stdvec(&entry)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        let len_bytes = (encoded.len() as u32).to_be_bytes();
        writer.write_all(&len_bytes)?;
        writer.write_all(&encoded)?;
        Ok(true)
    }

    /// Flush all writers and call sync_data (fdatasync).
    pub fn fsync_all(&mut self) -> std::io::Result<()> {
        for writer in self.writers.values_mut() {
            writer.flush()?;
            writer.get_ref().sync_data()?;
        }
        Ok(())
    }
}
```

### MGET Protocol Encoding/Parsing
```rust
// Source: derived from existing MSET pattern in protocol.rs
pub const OP_MGET: u8 = 0x06;

// In parse_command:
OP_MGET => {
    if buf.len() < 4 {
        return Err(TallyError::Protocol("MGET payload too short: need 4 bytes for count".into()));
    }
    let count = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    buf = &buf[4..];
    let mut keys = Vec::with_capacity(count);
    for _ in 0..count {
        keys.push(read_string(&mut buf)?);
    }
    Ok(Command::Mget { keys })
}
```

### Python SDK MGET
```python
# Source: derived from existing mset pattern in _protocol.py and _app.py
OP_MGET: int = 0x06

def encode_mget(keys: list[str]) -> bytes:
    """Encode MGET payload: [u32 count][u16-string key_1]...[u16-string key_n]."""
    parts = bytearray()
    parts.extend(struct.pack(">I", len(keys)))
    for key in keys:
        parts.extend(encode_string(key))
    return bytes(parts)

# In App class:
def mget(self, keys: list[str]) -> dict[str, FeatureResult]:
    """Fetch features for multiple keys in a single round trip."""
    payload = encode_mget(keys)
    resp = self._send(OP_MGET, payload)
    data = json.loads(resp) if resp else {}
    return {k: FeatureResult(v) for k, v in data.items()}
```

### Per-Stream Entity Eviction
```rust
// Source: derived from existing eviction.rs pattern + per-stream TTL requirement
pub fn evict_expired_stream_entries(
    store: &mut StateStore,
    engine: &PipelineEngine,
    now: SystemTime,
) -> usize {
    let mut total_evicted = 0;
    // For each entity, check each stream's last_event_at against that stream's entity_ttl
    for entity in store.entities_mut() {
        let streams_to_remove: Vec<String> = entity.streams.iter()
            .filter(|(stream_name, stream_state)| {
                if let Some(stream_def) = engine.get_stream(stream_name) {
                    if let Some(ttl) = stream_def.entity_ttl {
                        if let Some(last) = stream_state.last_event_at {
                            return now.duration_since(last).unwrap_or(Duration::ZERO) > ttl;
                        }
                    }
                }
                false
            })
            .map(|(name, _)| name.clone())
            .collect();
        for name in &streams_to_remove {
            entity.streams.remove(name);
            total_evicted += 1;
        }
    }
    // Remove entities with no remaining streams and no static features
    store.remove_empty_entities();
    total_evicted
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Global entity TTL (2x max window) | Per-stream entity TTL | This phase | Streams with different time horizons can coexist |
| Flat EntityState (all streams mixed) | Per-stream grouped EntityState | This phase | Clean operator isolation, prerequisite for composable pipeline |
| Snapshot-only persistence | Snapshot + event log | This phase | Enables backfill in Phase 8 |
| GET only (one key at a time) | GET + MGET (batch) | This phase | Reduced round trips for multi-key reads |
| Snapshot v3 | Snapshot v4 | This phase | No migration needed (clean break, locked decision) |

## Assumptions Log

> List all claims tagged [ASSUMED] in this research. The planner and discuss-phase use this
> section to identify decisions that need user confirmation before execution.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Stream names should be sanitized for filesystem safety when used as log filenames | Architecture Patterns | Could create files with invalid names or path traversal issues |
| A2 | SSD fdatasync latency is 0.5-10ms | Common Pitfalls | If higher, background fsync timer may need adjustment |
| A3 | BufWriter default buffer size (8KB) is sufficient for event log writes | Architecture Patterns | May need tuning if events are large; 8KB holds ~20-30 typical events |

## Open Questions

1. **Event log behavior on stream re-registration**
   - What we know: Locked decision says per-stream log files. When a stream is re-registered (updated definition), the log file should persist.
   - What's unclear: Should the log file be rotated on re-register? Or just continue appending?
   - Recommendation: Continue appending. The log captures raw events regardless of stream definition changes. Phase 8 (schema evolution) will need the full history.

2. **EventLog ownership within AppState**
   - What we know: EventLog holds file handles (BufWriter<File>). It needs to be accessible from the PUSH handler and the background fsync/compaction timers.
   - What's unclear: Should EventLog be inside AppState (behind the Mutex) or managed separately?
   - Recommendation: Put EventLog inside AppState. The BufWriter::write_all is ~200ns and does not benefit from being outside the lock. The fsync timer can clone the necessary data and call fdatasync via spawn_blocking (same as snapshot pattern). This keeps the architecture simple.

3. **MGET and view feature evaluation**
   - What we know: GET calls engine.get_features which evaluates view derives and lookups. MGET should be consistent with GET.
   - What's unclear: Should MGET evaluate views for each key? This could be expensive for cross-key lookups.
   - Recommendation: Yes, MGET should call get_features (same as GET) for consistency. Cross-key lookups are cheap (single HashMap lookup per feature). Strip qualified names from the response.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Rust (cargo) | Build/test | Yes | stable (aarch64-apple-darwin) | -- |
| Python 3 | SDK tests | Yes | 3.13.2 | -- |
| Local filesystem | Event log persistence | Yes | -- | -- |

**Missing dependencies with no fallback:**
- None

**Missing dependencies with fallback:**
- None

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in test framework (cargo test) + Python pytest |
| Config file | None (Rust uses default; Python uses conftest.py in python/tests/) |
| Quick run command | `~/.cargo/bin/cargo test --lib` |
| Full suite command | `~/.cargo/bin/cargo test` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| ELOG-01 | Keyless stream events written to append-only log | unit + integration | `~/.cargo/bin/cargo test event_log` | No -- Wave 0 |
| ELOG-02 | Keyed stream events written to log, compacted | unit + integration | `~/.cargo/bin/cargo test event_log` | No -- Wave 0 |
| ELOG-03 | Log writes do not block hot path (<100us p99) | integration | `~/.cargo/bin/cargo test --test test_server event_log_latency` | No -- Wave 0 |
| ELOG-04 | history_ttl configurable per stream | unit | `~/.cargo/bin/cargo test event_log::tests::history_ttl` | No -- Wave 0 |
| ELOG-05 | Background compaction removes expired entries | unit | `~/.cargo/bin/cargo test event_log::tests::compaction` | No -- Wave 0 |
| OPS-01 | MGET returns features for multiple keys | unit + integration | `~/.cargo/bin/cargo test mget` | No -- Wave 0 |
| OPS-02 | Per-stream entity TTL | unit | `~/.cargo/bin/cargo test eviction::tests::per_stream` | No -- Wave 0 |

### Sampling Rate
- **Per task commit:** `~/.cargo/bin/cargo test --lib`
- **Per wave merge:** `~/.cargo/bin/cargo test`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `src/state/event_log.rs` -- new module with unit tests for append, read, compaction, TTL
- [ ] Updates to `src/state/store.rs` tests -- new EntityState structure tests
- [ ] Updates to `src/state/snapshot.rs` tests -- v4 format round-trip tests
- [ ] Updates to `src/state/eviction.rs` tests -- per-stream TTL tests
- [ ] `tests/test_server.rs` additions -- MGET protocol tests, event log integration
- [ ] `python/tests/test_protocol.py` additions -- MGET encoding tests
- [ ] `python/tests/test_app.py` additions -- app.mget() tests

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | N/A (no auth in v1) |
| V3 Session Management | No | N/A |
| V4 Access Control | No | N/A (single-tenant by design) |
| V5 Input Validation | Yes | Validate stream names for filesystem safety; validate MGET key count bounds |
| V6 Cryptography | No | N/A |

### Known Threat Patterns for this Phase

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Path traversal via stream name in log filename | Tampering | Sanitize stream name: reject or replace `/`, `\`, `..`, NUL bytes before using as filename |
| Disk exhaustion via unbounded event log | Denial of Service | history_ttl with compaction; log size monitoring via HTTP /metrics |
| MGET with huge key list consuming CPU | Denial of Service | No hard limit per locked decision; rely on existing 64MB frame limit as natural bound |

## Sources

### Primary (HIGH confidence)
- Codebase analysis: `src/state/store.rs`, `src/state/snapshot.rs`, `src/state/eviction.rs`, `src/engine/pipeline.rs`, `src/server/tcp.rs`, `src/server/protocol.rs`, `src/main.rs` -- all read in full
- `.planning/research/ARCHITECTURE.md` -- v1.1 architecture design with EventLog, EntityState restructure patterns
- `.planning/phases/06-foundation/06-CONTEXT.md` -- locked decisions from discuss phase
- `.planning/REQUIREMENTS.md` -- requirement definitions ELOG-01 through ELOG-05, OPS-01, OPS-02

### Secondary (MEDIUM confidence)
- Redis AOF persistence pattern (everysec fsync, rewrite compaction) -- well-established industry pattern
- BufWriter<File>::write_all latency (~100-300ns for small writes) -- based on memcpy to kernel page cache

### Tertiary (LOW confidence)
- SSD fdatasync latency estimates (0.5-10ms) -- varies by hardware and workload

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new dependencies, all patterns from existing codebase
- Architecture: HIGH -- EntityState restructure is clearly specified in CONTEXT.md; event log design well-researched in ARCHITECTURE.md
- Pitfalls: HIGH -- identified from codebase analysis of actual data structures and existing patterns
- Event log performance: MEDIUM -- BufWriter latency is well-understood, but fdatasync behavior depends on hardware

**Research date:** 2026-04-09
**Valid until:** 2026-05-09 (stable Rust ecosystem, no fast-moving dependencies)
