# Stack Research: v1.1 Additions

**Domain:** Real-time feature server -- new capabilities for composable pipeline, SSD event log, incremental snapshots, debug UI
**Researched:** 2026-04-09
**Confidence:** HIGH (event log, DAG execution, debug UI), MEDIUM (incremental snapshots)

## Existing Stack (DO NOT CHANGE)

These are validated from v1.0 and remain unchanged:

| Technology | Version | Purpose |
|------------|---------|---------|
| tokio | 1.50+ | Async runtime (current_thread) |
| serde + serde_json | 1.0 | Serialization framework + JSON |
| postcard | 1.1 | Binary snapshot persistence |
| axum | 0.8 | HTTP management API |
| bytes | 1.11 | TCP frame buffering |
| ahash | 0.8 | Fast HashMap |
| winnow | 1.0 | Expression parsing |
| thiserror | 2.0 | Error types |

## New Stack Additions for v1.1

### 1. SSD Append-Only Event Log

**Recommendation: Hand-roll the event log. Do NOT use the `commitlog` crate.**

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| std::fs + std::io::BufWriter | stdlib | Append-only segment files | The event log is simple: append postcard-serialized events to segment files, rotate on size threshold. The `commitlog` crate (v0.1.1) is minimally maintained, last significant activity was years ago, and introduces mmap'd index files -- unnecessary complexity when Tally's single-threaded event loop can maintain an in-memory offset index. Using stdlib BufWriter with periodic flush/fsync gives full control over the write path and latency budget. |
| crc32fast | 1.5 | Per-record integrity checksums | SIMD-accelerated CRC32 (IEEE) checksums. At 100K events/sec with ~300 byte records, checksum computation must be negligible. crc32fast uses SSE4.2/PCLMULQDQ on x86 and NEON on ARM, processing at multi-GB/s rates. One dependency, zero unsafe in the public API, 335M+ downloads. Use CRC32 rather than a full cryptographic hash because the purpose is corruption detection, not authentication. |
| postcard | 1.1 (existing) | Event record serialization | Already in the stack. Each event record in the log is: `[4-byte length][postcard-encoded EventRecord][4-byte CRC32]`. Postcard's compact variable-length encoding keeps per-record overhead minimal (~5-15 bytes framing vs ~30+ for JSON). |

**Event log wire format (on-disk):**
```
Segment file: events_{segment_id}.log
Per-record: [u32 length][postcard bytes][u32 crc32]
Segment header: [u8 version][u64 base_offset][u64 created_at_unix_ms]
Rotate at: configurable max segment size (default 256MB)
```

**Why NOT commitlog:**
- v0.1.1, barely maintained, limited community adoption
- Uses memory-mapped index files -- adds mmap complexity and potential segfault risk in unsafe code
- Tally needs per-stream segmentation and configurable per-stream TTL; commitlog has a single-stream model
- The append-only pattern is <200 lines of Rust with BufWriter + fsync; the "library" adds more complexity than it removes
- Full control over cooperative yielding during compaction (critical for single-threaded architecture)

**Fsync strategy:**
- `BufWriter` with 64KB buffer for batching
- `fsync` every N milliseconds (configurable, default 100ms) or on buffer flush
- Amortized cost: ~100-300ns per event at 100K events/sec (well within the <100us PUSH latency budget)
- Lost events on crash: up to one fsync interval of events (acceptable per existing snapshot model)

### 2. DAG-Based Pipeline Execution

**Recommendation: Use `petgraph` for DAG construction and topological sort.**

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| petgraph | 0.8 | Directed graph + topological sort | The de facto graph library in Rust (2M+ downloads, actively maintained, used in the Rust compiler toolchain). Provides `DiGraph` for directed graphs and `toposort()` for O(V+E) topological ordering. The composable pipeline needs: (1) represent stream/view dependencies as a DAG, (2) detect cycles at registration time, (3) determine execution order. petgraph does all three. Minimal API surface needed -- just DiGraph, add_node, add_edge, toposort. Default features include everything needed; disable `rayon` (unnecessary for single-threaded architecture). |

**Why petgraph instead of hand-rolling:**
- Topological sort with cycle detection is ~50 lines to implement correctly, but cycle error reporting (which node caused it) is where bugs hide. petgraph returns `Cycle(NodeIndex)` with the offending node.
- Future features (visualizing the DAG in the debug UI, finding all ancestors of a stream for targeted backfill) come free with petgraph's traversal algorithms.
- One dependency with zero unsafe in the public API. Compiles fast.

**Usage pattern:**
```rust
use petgraph::graph::DiGraph;
use petgraph::algo::toposort;

// At pipeline registration time:
let mut dag = DiGraph::<String, ()>::new();
let tx_node = dag.add_node("Transactions".into());
let login_node = dag.add_node("Logins".into());
let risk_node = dag.add_node("UserRisk".into());
dag.add_edge(tx_node, risk_node, ());   // UserRisk depends on Transactions
dag.add_edge(login_node, risk_node, ()); // UserRisk depends on Logins

let order = toposort(&dag, None).expect("cycle detected");
// order: [Transactions, Logins, UserRisk] -- process in this order
```

**What petgraph replaces:**
- The current v1.0 code evaluates views independently from streams. Views are evaluated lazily on GET. With the composable pipeline, the DAG determines execution order for both streams and views during PUSH, enabling views to be evaluated eagerly as part of the push-through flow.

### 3. Debug Web UI

**Recommendation: Use `rust-embed` to compile a minimal HTML/JS/CSS UI into the binary. Use axum SSE for real-time updates.**

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| rust-embed | 8.11 | Embed static UI assets in binary | Compile HTML/JS/CSS into the single binary at build time. In debug mode, serves from filesystem (hot reload during development). In release, everything is embedded -- preserving the "single binary, zero ops" promise. Has native axum integration via optional `axum` feature. 23M+ downloads, actively maintained. |
| axum SSE (built-in) | 0.8 (existing) | Real-time streaming to debug UI | Axum includes `axum::response::sse` module with `Sse`, `Event`, and `KeepAlive` types. No additional dependency needed. SSE is simpler than WebSocket for the debug UI use case because the data flow is server-to-client only (metrics, stream events, memory stats). The debug UI polls or subscribes; it never sends commands. |
| tower-http | 0.6 | CORS headers for debug UI development | When developing the UI with a separate dev server (e.g., opening a local HTML file), CORS headers are needed. tower-http's `CorsLayer` integrates directly with axum. Also provides `CompressionLayer` for compressing embedded assets. Only needed for development convenience -- not required in production since UI and API are same-origin. |

**Why NOT a separate frontend framework or SPA:**
- The debug UI is a developer tool, not a customer-facing product. A single `index.html` with vanilla JS + CSS is sufficient.
- No build toolchain (no node, no npm, no webpack). The HTML/JS/CSS files are authored directly and embedded at Rust compile time.
- Total UI asset size target: <100KB (HTML + CSS + JS). Embedded in the binary with negligible size impact.
- If the UI grows complex later, can migrate to a lightweight framework (e.g., Preact) with a build step, but start simple.

**Debug UI architecture:**
```
axum router on port 6401 (existing HTTP management API)
  /debug/ui/*       -> rust-embed serves static files
  /debug/sse/events -> SSE stream of recent events
  /debug/sse/metrics -> SSE stream of throughput/latency/memory
  /debug/key/:key   -> existing JSON endpoint (already built)
  /debug/memory     -> existing JSON endpoint (already built)
```

**SSE channel pattern:**
```rust
// In the engine, broadcast events to an optional SSE channel
use tokio::sync::broadcast;

let (tx, _) = broadcast::channel::<DebugEvent>(256);
// On each PUSH, send to broadcast (if any subscribers exist)
// SSE handler subscribes: BroadcastStream::new(tx.subscribe())
```

Note: `tokio::sync::broadcast` is already available -- just add `"sync"` to tokio's feature list (currently not in Cargo.toml but trivially added).

### 4. Incremental Snapshot Serialization

**Recommendation: No new library needed. Implement dirty-key tracking with the existing postcard stack.**

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| postcard | 1.1 (existing) | Serialize only changed entities | The current clone-then-serialize approach copies all entity state on every snapshot. For incremental snapshots, track which entity keys have been modified since the last snapshot (a `HashSet<EntityKey>` "dirty set"). On snapshot, serialize only dirty keys as delta records appended to a snapshot log file. Periodic full compaction merges the base + deltas. No new serialization library needed -- postcard handles individual entity serialization identically to full-state serialization. |

**Incremental snapshot design:**
```
Base snapshot: full_snapshot_{timestamp}.dat  (full state, same as v1.0)
Delta files:   delta_{sequence}.dat           (only dirty keys since last base)
Compaction:    merge base + deltas -> new base (triggered on delta count or size threshold)

Delta record format:
  [u8 op: 0x01=upsert, 0x02=delete][u16 key_len][key bytes][postcard entity bytes]
```

**Why this approach:**
- The v1.0 snapshot already works by serializing `Vec<(String, SerializableEntityState)>`. Incremental just writes a subset.
- At 1M keys with 10 features each, a full snapshot is ~5GB of serialized state. If only 1% of keys change between snapshots (30 second interval), incremental writes ~50MB instead.
- Dirty tracking is O(1) per PUSH/SET (insert key into a HashSet). Zero overhead on the read path.
- No new dependencies. The existing `postcard::to_stdvec` works for individual entity serialization.

## Updated Cargo.toml Dependencies

```toml
[dependencies]
# Existing (unchanged)
ahash = "0.8"
winnow = "1.0"
thiserror = "2.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
postcard = { version = "1.1", features = ["use-std", "alloc"] }
bytes = "1.11"

# Existing (updated features)
tokio = { version = "1.50", features = ["rt", "net", "io-util", "macros", "time", "sync"] }
axum = "0.8"

# NEW for v1.1
petgraph = { version = "0.8", default-features = false, features = ["stable_graph"] }
crc32fast = "1.5"
rust-embed = { version = "8.11", features = ["axum"] }
tower-http = { version = "0.6", features = ["cors"] }

[dev-dependencies]
tempfile = "3"
```

**Total new dependencies: 4 crates (petgraph, crc32fast, rust-embed, tower-http)**

## Alternatives Considered

| Recommended | Alternative | Why Not |
|-------------|-------------|---------|
| Hand-rolled event log | `commitlog` crate (v0.1.1) | Minimally maintained, mmap complexity, single-stream model doesn't fit per-stream TTL/history needs. Rolling our own is <200 LOC with full control over fsync, rotation, and cooperative yielding. |
| Hand-rolled event log | `sled` (embedded DB as log) | sled is a full B-tree KV store -- massive overkill for append-only writes. Would add latency to the hot path and couple the log to a specific storage engine. |
| petgraph | Hand-rolled toposort | petgraph gives cycle detection with node identification for free. Future DAG visualization and traversal algorithms also come free. The 0.8 release is stable and actively maintained. |
| petgraph | `daggy` crate | daggy is a thin wrapper over petgraph anyway. Using petgraph directly avoids the indirection and gives access to the full algorithm library. |
| rust-embed | tower-http ServeDir | ServeDir serves from filesystem at runtime -- requires shipping a separate `static/` directory alongside the binary. Violates the "single binary, zero ops" constraint. |
| rust-embed | `include_dir!` macro | Lower-level than rust-embed -- requires manual MIME type handling and no dev-mode filesystem fallback. rust-embed's axum integration handles both automatically. |
| axum SSE | WebSocket | WebSocket is bidirectional but the debug UI only needs server-to-client streaming. SSE is simpler, requires no upgrade handshake complexity, works over standard HTTP, and is built into axum with no extra dependency. |
| axum SSE | Polling from UI | Polling adds latency (up to the poll interval) and unnecessary request overhead. SSE gives true real-time updates with a single long-lived HTTP connection. |
| crc32fast | `crc` crate | `crc` runs at ~0.5 GB/s vs crc32fast's multi-GB/s with SIMD. At 100K events/sec, the difference matters for keeping checksums off the critical path. |
| crc32fast | No checksums | Skipping integrity checks means silent corruption in the event log goes undetected until replay produces wrong results. CRC32 is cheap insurance. |
| Dirty-key HashSet (incremental snapshot) | `xxhash` content-addressable diffing | Content hashing every entity on every snapshot to detect changes is more expensive than tracking dirty keys at write time (O(1) insert). Over-engineered for this use case. |

## What NOT to Add

| Avoid | Why | Do Instead |
|-------|-----|------------|
| RocksDB / sled / redb | Embedded KV stores add latency to the hot path and introduce complex compaction behavior. The event log is append-only; it does not need random-access writes. | Hand-rolled append-only segment files with BufWriter. |
| mmap for event log | Memory-mapped files add complexity (SIGBUS handling on disk full, platform-specific behavior, difficult to control fsync timing) without significant benefit for sequential append workloads. | Standard buffered I/O with explicit fsync. |
| tokio-uring / io_uring | io_uring is Linux-only and adds significant complexity. The event log fsync is amortized and not on the critical latency path. Standard buffered I/O is sufficient. | `std::fs::File` + `BufWriter` + periodic `fsync`. |
| React / Vue / Svelte for debug UI | Adds a Node.js build toolchain dependency. The debug UI is a developer tool showing metrics and event streams -- vanilla HTML/JS/CSS is sufficient. | Single `index.html` with vanilla JS, embedded via rust-embed. |
| `serde_cbor` or `rmp-serde` for event log | Adding another serialization format alongside postcard increases cognitive load and testing surface. Postcard is already proven in the snapshot path. | Use postcard for event log records (same as snapshots). |
| `tower-http` fs/ServeDir features | ServeDir requires a filesystem directory at runtime, which breaks the single-binary deployment model. | Use rust-embed for compile-time asset embedding. |
| `dashmap` or concurrent data structures | The architecture is single-threaded. Concurrent data structures add overhead (atomic operations) with zero benefit. | Continue with plain `AHashMap` and no locking on the hot path. |

## Tokio Feature Flag Update

The current `Cargo.toml` has:
```toml
tokio = { version = "1.50", features = ["rt", "net", "io-util", "macros", "time"] }
```

Add `"sync"` for `broadcast::channel` (SSE event streaming) and `"fs"` for async file operations if needed for event log management:
```toml
tokio = { version = "1.50", features = ["rt", "net", "io-util", "macros", "time", "sync"] }
```

Note: The event log itself should use `std::fs` (synchronous I/O in the single-threaded loop is fine for buffered appends), but `tokio::sync::broadcast` is needed for the SSE debug event stream.

## Version Compatibility

| Package | Compatible With | Notes |
|---------|-----------------|-------|
| petgraph 0.8 | Rust 1.64+ | No async, no runtime dependency. Pure data structures + algorithms. |
| crc32fast 1.5 | Rust 1.56+ | Auto-detects SIMD at runtime. Falls back to software implementation. No feature flags needed. |
| rust-embed 8.11 | axum 0.8 | The `axum` feature flag enables `axum::response::IntoResponse` impl for embedded files. Must match axum major version. |
| tower-http 0.6 | axum 0.8, tower 0.5 | axum 0.8 uses tower 0.5 and hyper 1.x. tower-http 0.6 is compatible. Do NOT use tower-http 0.5 with axum 0.8. |
| tokio sync feature | tokio 1.50+ | `broadcast::channel` has been stable since tokio 1.0. No version concerns. |

## Sources

- [petgraph crates.io](https://crates.io/crates/petgraph) -- v0.8.2 confirmed, actively maintained (HIGH confidence)
- [petgraph docs.rs toposort](https://docs.rs/petgraph/latest/petgraph/algo/fn.toposort.html) -- O(V+E) with Cycle error return (HIGH confidence)
- [crc32fast crates.io](https://crates.io/crates/crc32fast) -- v1.5.0, 335M+ downloads, SIMD-accelerated (HIGH confidence)
- [rust-embed crates.io](https://crates.io/crates/rust-embed) -- v8.11.0, axum 0.8 feature flag confirmed (HIGH confidence)
- [axum SSE docs.rs](https://docs.rs/axum/latest/axum/response/sse/) -- built-in module in axum 0.8, no feature flag needed (HIGH confidence)
- [tower-http crates.io](https://crates.io/crates/tower-http) -- v0.6.8, CORS layer available (HIGH confidence)
- [commitlog GitHub](https://github.com/zowens/commitlog) -- v0.1.1, minimally maintained, mmap-based (MEDIUM confidence -- recommend against)
- [segmented log in Rust blog](https://arindas.github.io/blog/segmented-log-rust/) -- validates hand-rolled approach (MEDIUM confidence)
- [postcard docs.rs](https://docs.rs/postcard/latest/postcard/) -- v1.1, stable wire format, flavor system for custom serialization (HIGH confidence)

---
*Stack research for: Tally v1.1 -- composable pipeline, SSD event log, incremental snapshots, debug UI*
*Researched: 2026-04-09*
