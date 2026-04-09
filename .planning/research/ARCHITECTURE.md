# Architecture Research

**Domain:** Single-threaded in-memory real-time feature server (Rust/tokio)
**Researched:** 2026-04-09
**Confidence:** HIGH

## Standard Architecture

### System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Tally Server Process                          │
│                                                                     │
│  ┌──────────────────────┐    ┌────────────────────────────────────┐ │
│  │  TCP Listener        │    │  HTTP Management Listener          │ │
│  │  port 6400           │    │  port 6401 (axum)                  │ │
│  │  tokio::net::TcpListener  │  /health /metrics /pipelines       │ │
│  └──────────┬───────────┘    └────────────────────────────────────┘ │
│             │ accept()                                               │
│  ┌──────────▼───────────────────────────────────────────────────┐   │
│  │  Connection Handler (spawn_local per connection)             │   │
│  │  Connection { stream: BufWriter<TcpStream>, buf: BytesMut }  │   │
│  │  read_frame() → parse opcode → dispatch command              │   │
│  └──────────┬───────────────────────────────────────────────────┘   │
│             │ &mut Engine (via Rc<RefCell<>>)                       │
│  ┌──────────▼───────────────────────────────────────────────────┐   │
│  │  Command Dispatcher                                           │   │
│  │  PUSH → PipelineEngine::process_event()                      │   │
│  │  GET  → StateStore::get_all_features()                       │   │
│  │  SET  → StateStore::set_static()                             │   │
│  │  MSET → StateStore::bulk_set() [chunked with yield_now]      │   │
│  │  REGISTER → PipelineRegistry::register()                     │   │
│  └──────────┬───────────────────────────────────────────────────┘   │
│             │                                                        │
│  ┌──────────▼───────────────────────────────────────────────────┐   │
│  │  Pipeline Engine                                              │   │
│  │  - PipelineRegistry: HashMap<StreamName, StreamDef>          │   │
│  │  - process_event(): fan-out → update operators → derives     │   │
│  │  - resolve_views(): cross-stream derive evaluation           │   │
│  │  - resolve_lookups(): cross-key HashMap reads                │   │
│  └──────────┬───────────────────────────────────────────────────┘   │
│             │                                                        │
│  ┌──────────▼───────────────────────────────────────────────────┐   │
│  │  State Store (in-memory, owned by Engine)                    │   │
│  │  HashMap<EntityKey, EntityState>                             │   │
│  │    EntityState {                                             │   │
│  │      live:   HashMap<FeatureName, LiveFeature>               │   │
│  │      static: HashMap<FeatureName, StaticFeature>            │   │
│  │      last_event_at: Instant                                  │   │
│  │    }                                                         │   │
│  └──────────┬───────────────────────────────────────────────────┘   │
│             │                                                        │
│  ┌──────────▼───────────────────────────────────────────────────┐   │
│  │  Snapshot Task (background, spawn_local)                     │   │
│  │  tokio::time::interval(30s) → snapshot_state_to_disk()       │   │
│  │  bincode serialize → atomic rename to snapshot file          │   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Responsibility | Implementation Pattern |
|-----------|----------------|------------------------|
| TCP Listener | Accept connections, spawn per-connection task | `tokio::net::TcpListener::accept()` loop, `spawn_local` |
| Connection | Frame parsing, buffer management, command dispatch | `Connection { BufWriter<TcpStream>, BytesMut }` |
| Command Dispatcher | Route opcodes to engine methods, serialize responses | Match on opcode byte, call engine, write response frame |
| Pipeline Registry | Store stream/view definitions, validate on registration | `HashMap<String, StreamDef>`, validated at register time |
| Pipeline Engine | Process events: fan-out, operator update, derive evaluation | Owns `StateStore`, calls operators in sequence |
| State Store | In-memory entity state, live + static features | `HashMap<String, EntityState>` — single owner, no locks |
| Operator State | Per-key windowed aggregation (ring buffer) | Enum variants per operator type, bucketed ring buffer |
| Expression Evaluator | Parse and evaluate derive/where expressions | Pratt parser → AST at registration; tree-walk at runtime |
| Snapshot Task | Periodic full serialization to disk, load on startup | `spawn_local` + `interval`, `spawn_blocking` for disk I/O |
| HTTP API | Pipeline CRUD, health, metrics, debug endpoints | `axum` on separate port, shares `Arc` clone of registry |
| TTL Eviction | Remove inactive keys to bound memory | Background task scanning `last_event_at`, evict expired |

## Recommended Project Structure

```
tally/
├── Cargo.toml
├── src/
│   ├── main.rs              # Entry point: parse config, build runtime, start server
│   ├── config.rs            # Configuration struct (ports, snapshot interval, bucket size)
│   ├── types.rs             # Value, Timestamp, FeatureMap, EntityKey, FeatureName
│   ├── server/
│   │   ├── mod.rs           # Re-exports, Server struct that owns engine + listeners
│   │   ├── tcp.rs           # TcpListener loop, connection accept, spawn_local
│   │   ├── connection.rs    # Connection struct, read_frame, write_frame, command loop
│   │   ├── protocol.rs      # Frame encode/decode: opcodes, length prefix, string encoding
│   │   └── http.rs          # axum router, pipeline CRUD, /health, /metrics, /debug
│   ├── engine/
│   │   ├── mod.rs           # Engine struct owning StateStore + PipelineRegistry
│   │   ├── pipeline.rs      # StreamDef, ViewDef, PipelineRegistry, REGISTER handling
│   │   ├── dispatch.rs      # process_event(): fan-out, operator update, derive, lookup
│   │   ├── operators.rs     # OperatorState enum + update/read for each variant
│   │   ├── window.rs        # BucketedWindow<T>: ring buffer, bucket advance, sum
│   │   ├── expression.rs    # Lexer, Pratt parser, Expr AST, eval(env: &FeatureMap)
│   │   ├── hll.rs           # HyperLogLog (or thin wrapper over hyperloglog-rs crate)
│   │   └── view.rs          # Cross-stream view resolution, cross-key lookup dispatch
│   ├── state/
│   │   ├── mod.rs
│   │   ├── store.rs         # StateStore: HashMap<EntityKey, EntityState>, get/set/evict
│   │   ├── snapshot.rs      # serialize_to_bytes(), deserialize_from_bytes(), load/save
│   │   └── eviction.rs      # run_eviction_loop(): interval scan, remove stale keys
│   └── metrics.rs           # Prometheus counters/histograms, register_metrics()
├── python/
│   ├── pyproject.toml
│   └── streamlet/           # (or tally/ after rename)
│       ├── __init__.py
│       ├── client.py        # TCP client, connection pool, frame encode/decode
│       ├── stream.py        # @st.stream decorator, collects operator definitions
│       ├── view.py          # @st.view decorator
│       ├── operators.py     # st.count, st.sum, st.avg, st.derive, st.lookup, etc.
│       └── types.py         # FeatureResult, typed attribute access
├── tests/
│   ├── test_operators.rs
│   ├── test_window.rs
│   ├── test_expression.rs
│   ├── test_protocol.rs
│   ├── test_pipeline.rs
│   └── test_snapshot.rs
└── benches/
    ├── throughput.rs
    └── latency.rs
```

### Structure Rationale

- **server/**: Everything that touches the network. `tcp.rs` only accepts and spawns — no business logic. `connection.rs` handles the frame loop and calls into `engine/`. `protocol.rs` is a pure encode/decode module with no side effects, making it fully unit-testable.
- **engine/**: All computation. `dispatch.rs` is the hot path — it sequences fan-out, operator updates, and derive resolution. `expression.rs` is split: parse at registration time (slow path), evaluate at event time (hot path, no allocation).
- **state/**: Persistence layer is fully isolated. `store.rs` knows nothing about snapshots. `snapshot.rs` knows nothing about live event processing. Keeps both testable independently.
- **types.rs at root**: Shared value types imported by all modules — `Value`, `Timestamp`, `FeatureMap`. Avoids circular imports between `engine/` and `state/`.

## Architectural Patterns

### Pattern 1: Single-Threaded Ownership with tokio::task::LocalSet

**What:** Run the entire event loop on one thread using `tokio::runtime::Builder::new_current_thread()` and `tokio::task::LocalSet`. All tasks use `spawn_local`. State is owned by the `Engine` struct, passed as `Rc<RefCell<Engine>>` to connection tasks — no `Arc`, no `Mutex`, no atomic overhead on the hot path.

**When to use:** Exactly this use case: Redis-inspired servers where keys are independent, the workload is I/O-bound with short CPU bursts, and single-thread throughput meets requirements (100K+ events/sec is achievable on one core with zero lock contention).

**Trade-offs:**
- Pro: Zero lock contention. `RefCell` borrow panics if misused, but correct code never holds a borrow across `.await` — enforce this as a rule.
- Pro: Can use `Rc`, `Cell`, `RefCell` freely — zero atomic overhead.
- Con: `spawn_blocking` required for any disk I/O (snapshot writes) to avoid blocking the thread.
- Con: Cannot use standard `tokio::spawn` — all tasks must be `spawn_local`.

**Example:**
```rust
fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, async {
        let engine = Rc::new(RefCell::new(Engine::new(config)));
        // spawn snapshot background task
        let snap_engine = engine.clone();
        spawn_local(async move { run_snapshot_loop(snap_engine).await });
        // accept TCP connections
        run_tcp_server(engine).await;
    });
}
```

### Pattern 2: Frame-First Connection Loop (Connection Struct)

**What:** Each accepted TCP connection gets a `Connection` struct wrapping `BufWriter<TcpStream>` + `BytesMut` read buffer. The connection task runs a loop: `read_frame()` → dispatch command → `write_frame()`. Frame parsing is a two-phase check (boundary detection → deserialization) against the buffer, never blocking the event loop.

**When to use:** Any binary protocol over persistent TCP. This is the canonical pattern from the tokio tutorial and mini-redis.

**Trade-offs:**
- Pro: Clean separation — `Connection` handles I/O, `Engine` handles logic. Can swap protocol without touching engine.
- Pro: `BufWriter` coalesces small writes into fewer syscalls. Response frames for simple GET/SET are small — batching matters.
- Con: Each connection holds a buffer allocation (typically 4KB). For 10K concurrent connections that is 40MB just for buffers — acceptable.

**Example:**
```rust
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                return Ok(None); // EOF
            }
        }
    }
}
```

### Pattern 3: Register-Once, Evaluate-Many for Expressions

**What:** Parse derive/where expressions into an AST at pipeline registration time (on the REGISTER command, which is rare). At event time (PUSH), evaluate the pre-compiled AST by walking the tree — no parsing, no allocation per event.

**When to use:** Any situation where expressions are registered once but evaluated millions of times. This is what keeps Python off the hot path.

**Trade-offs:**
- Pro: Expression evaluation adds negligible overhead per event — tree walk over a small AST is cache-friendly.
- Pro: Syntax errors are caught at registration time, not silently at runtime.
- Con: Expression language must be intentionally limited — `derive` and `where` are not Turing-complete. This is a feature, not a bug.

**Example AST:**
```rust
pub enum Expr {
    Literal(f64),
    Bool(bool),
    Field(FieldRef),          // field_name, StreamName.field, _event.field
    BinOp { op: Op, lhs: Box<Expr>, rhs: Box<Expr> },
    UnaryOp { op: UnaryOp, operand: Box<Expr> },
    Call { name: String, args: Vec<Expr> },
}

// Evaluated at event time: no heap allocation for simple expressions
pub fn eval(expr: &Expr, env: &EvalEnv) -> Result<Value> { ... }
```

Use a Pratt parser for infix precedence — it handles arithmetic/comparison/boolean in ~200 lines and is significantly simpler than a recursive descent parser with full precedence climbing.

### Pattern 4: Bucketed Ring Buffer for Sliding Windows

**What:** Each windowed operator (count, sum, avg, min, max) maintains a fixed-size ring buffer of buckets. Bucket granularity is configurable (e.g., 1-minute buckets for a 30-minute window = 30 buckets). On event arrival: add to the current bucket (determined by `now % window_size`). On read: sum all non-expired buckets. Bucket advancement is lazy — expire old buckets when the current time has moved past them.

**When to use:** Any windowed streaming aggregation with bounded memory requirements. This is the approach used by Redis TimeSeries and Arroyo's sliding window implementation.

**Trade-offs:**
- Pro: O(1) per event update. O(W/B) read (W = window duration, B = bucket size) — typically 30-120 buckets, very fast.
- Pro: Strictly bounded memory per operator per key.
- Con: Introduces a maximum error of one bucket duration in window boundary precision. Configurable tradeoff.

**Example:**
```rust
pub struct BucketedWindow {
    buckets: Vec<f64>,       // ring buffer, len = num_buckets
    bucket_counts: Vec<u64>, // parallel count for avg computation
    head: usize,             // current bucket index
    last_advance: Instant,   // when we last moved head
    bucket_duration: Duration,
    num_buckets: usize,
}

impl BucketedWindow {
    pub fn advance_to_now(&mut self) {
        let elapsed = self.last_advance.elapsed();
        let buckets_to_advance = (elapsed.as_secs_f64()
            / self.bucket_duration.as_secs_f64()) as usize;
        for _ in 0..buckets_to_advance.min(self.num_buckets) {
            self.head = (self.head + 1) % self.num_buckets;
            self.buckets[self.head] = 0.0;
            self.bucket_counts[self.head] = 0;
        }
        self.last_advance += self.bucket_duration * buckets_to_advance as u32;
    }

    pub fn add(&mut self, value: f64) {
        self.advance_to_now();
        self.buckets[self.head] += value;
        self.bucket_counts[self.head] += 1;
    }

    pub fn sum(&mut self) -> f64 {
        self.advance_to_now();
        self.buckets.iter().sum()
    }
}
```

### Pattern 5: Cooperative Yielding for MSET Chunks

**What:** Large MSET operations (100K keys) are processed in chunks of ~1024 keys. After each chunk, call `tokio::task::yield_now().await` to give the event loop a chance to process pending PUSH and GET requests. This prevents MSET from starving the hot path.

**When to use:** Any bulk operation that would otherwise run for tens of milliseconds on the hot-path thread. The same pattern applies to snapshot serialization (handled differently — see snapshot pattern below).

**Trade-offs:**
- Pro: PUSH/GET remain responsive even during large batch ingestion.
- Pro: No need for separate threads or channels for MSET.
- Con: MSET throughput is slightly lower than naive sequential processing due to yield overhead (negligible in practice — yield is a scheduler hint, not a sleep).

**Example:**
```rust
async fn process_mset(engine: &mut Engine, entries: Vec<(String, FeatureMap)>) {
    const CHUNK_SIZE: usize = 1024;
    for chunk in entries.chunks(CHUNK_SIZE) {
        for (key, features) in chunk {
            engine.state.set_static(key, features);
        }
        tokio::task::yield_now().await;
    }
}
```

### Pattern 6: Snapshot via spawn_blocking

**What:** Snapshot writes are CPU and I/O intensive (serialize a large HashMap to bytes, then write to disk). Running this synchronously on the event loop thread would block all connections for hundreds of milliseconds. The pattern: serialize the state to a `Vec<u8>` synchronously inside `spawn_blocking`, then atomically rename the temp file to the snapshot path.

**Critical constraint:** The state must be cloned or serialized while holding no other borrows. On a single-threaded runtime with `RefCell`, the safe approach is: take a borrow, clone the data needed for serialization, release the borrow, then `spawn_blocking` the serialization.

**When to use:** Any periodic full-state serialization in an async context.

**Trade-offs:**
- Pro: Event loop is unblocked during disk I/O.
- Con: State clone for snapshot introduces a memory spike (up to 2x peak memory during snapshot). Mitigated by designing `EntityState` to be efficiently cloneable and compactly serializable.
- Con: Cannot serialize a `RefCell` borrow across a thread boundary — must clone first.

**Example:**
```rust
async fn run_snapshot_loop(engine: Rc<RefCell<Engine>>, path: PathBuf) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    loop {
        interval.tick().await;
        // Clone state while on event loop thread
        let snapshot_data = {
            let eng = engine.borrow();
            eng.state.clone_for_snapshot() // returns SnapshotData (cheaply serializable)
        };
        // Serialize + write on blocking thread pool
        let path = path.clone();
        tokio::task::spawn_blocking(move || {
            let bytes = bincode::serialize(&snapshot_data).unwrap();
            let tmp = path.with_extension("tmp");
            std::fs::write(&tmp, &bytes).unwrap();
            std::fs::rename(&tmp, &path).unwrap(); // atomic on POSIX
        }).await.unwrap();
    }
}
```

## Data Flow

### PUSH Event Flow (Hot Path)

```
Client TCP connection
    |
    | [4-byte len][0x01 opcode][stream_name][payload JSON]
    v
Connection::read_frame()
    | parse binary frame from BytesMut buffer
    v
Command::Push { stream, payload }
    |
    v
Engine::process_event(stream, payload)
    |
    +---> PipelineRegistry::get_stream(stream)
    |         returns: StreamDef { key_field, operators, derives }
    |
    +---> extract entity_key = payload[key_field]
    |
    +---> fan-out: find all streams keyed by fields in payload
    |         for each matched stream:
    |           StateStore::get_or_create(entity_key)
    |           update each LiveFeature's OperatorState
    |
    +---> evaluate derives: for each derive expr in StreamDef
    |         Expr::eval(env = current feature values + _event fields)
    |
    +---> resolve views: any ViewDef that depends on this stream
    |         re-evaluate cross-stream derive expressions
    |
    +---> resolve lookups: any lookup(Stream.feature, on=field)
    |         StateStore::get(payload[field])?.get_feature(feature)
    |
    +---> collect FeatureMap { feature_name -> Value }
    |
    v
Connection::write_frame()
    | serialize FeatureMap as JSON, prefix with 4-byte length
    v
Client receives response
```

### State Ownership Flow

```
Engine (owns StateStore via direct field)
    |
    +-- StateStore owns HashMap<EntityKey, EntityState>
    |       EntityState owns all operator state (ring buffers, HLL, etc.)
    |
    +-- PipelineRegistry owns HashMap<StreamName, StreamDef>
            StreamDef owns Vec<OperatorDef> + Vec<ExprAST>
            (ASTs allocated once at REGISTER time, never again)

Rc<RefCell<Engine>> shared across:
    - Connection tasks (borrow for PUSH/GET/SET/MSET duration)
    - Snapshot task (borrow briefly to clone state, then release)
    - Eviction task (borrow briefly to scan + remove stale keys)
    - HTTP handler (Arc<PipelineRegistry> clone, NOT RefCell borrow)
```

### HTTP Management API Data Flow

```
axum router (separate tokio task, separate port)
    |
    | POST /pipelines → deserialize PipelineDefinition JSON
    |                    → validate (expression parse, field refs)
    |                    → send Msg::Register over tokio::sync::mpsc channel
    |                         (mpsc channel bridges HTTP task to engine task)
    | GET /debug/key/:key → send Msg::DebugGet, await response
    | GET /metrics → read Arc<Metrics> counters (lock-free reads via atomics)
    v
Channel receiver in Engine's select! loop
    | process registration message
    | update PipelineRegistry
    v
HTTP handler awaits channel response
```

Note: The HTTP handler must communicate with the engine via a channel rather than shared `RefCell` because axum runs on a potentially different `spawn_local` task. The recommended pattern is a `tokio::sync::mpsc::channel` for requests and `tokio::sync::oneshot` for responses (request-reply over channels).

### Key Data Flows

1. **REGISTER command flow:** Python SDK serializes stream class to JSON → sends REGISTER frame → Server parses pipeline definition → validates expression syntax (Pratt parse to AST) → validates field references against stream key → stores in PipelineRegistry. ASTs stored per operator/derive — never re-parsed.

2. **GET command flow:** Read entity_key from frame → StateStore::get(key) → collect all live features (call `operator.read()` which advances expired buckets and returns current value) + all static features → serialize as JSON FeatureMap → write frame. No computation beyond bucket advancement.

3. **TTL eviction flow:** Background task wakes on interval (every 60s default) → borrows StateStore → iterates entity keys → removes any where `last_event_at` is older than `2 × largest_window`. TTL eviction is the only place keys are removed, keeping the store simple.

## Scaling Considerations

| Scale | Architecture Adjustments |
|-------|--------------------------|
| <10K events/sec | Current single-threaded design is optimal. No changes. |
| 10K-100K events/sec | Single-threaded target. Profile before changing anything. Likely still fine. |
| 100K-500K events/sec | Tune bucket sizes, reduce HLL precision if memory-bound. Consider connection limit tuning. |
| >500K events/sec | Key-partitioned multi-threading: shard `EntityKey` space across N engines on N threads. No locks between shards. Drop-in upgrade path from current design. |

### Scaling Priorities

1. **First bottleneck: CPU on derive evaluation.** Complex derive expressions with many field lookups per event. Fix: profile with `cargo flamegraph`, optimize hot expression paths. Expression evaluator should be JIT-compiled in a future version if this becomes a bottleneck.

2. **Second bottleneck: Memory for distinct_count.** HyperLogLog at 14-bit precision is ~12KB per operator per key. 1M keys × 3 distinct_count operators = 36GB. Fix: reduce HLL precision (10-bit = ~1KB, ~2% error), or add TTL eviction more aggressively.

3. **Third bottleneck: Snapshot write time.** Cloning 1M EntityState structs and serializing to bincode. Fix: incremental snapshots (post-v1 scope), or compress snapshot output with LZ4 before write.

## Anti-Patterns

### Anti-Pattern 1: Holding RefCell Borrow Across .await

**What people do:** Borrow the `RefCell<Engine>` at the start of a connection handler and hold it across `read_frame().await` or `write_frame().await`.

**Why it's wrong:** `RefCell` is not `Send`. Holding a borrow across an `.await` point on a `LocalSet` will either panic at runtime (second borrow while first is held during yield) or simply prevent any other task from borrowing the engine — effectively making the server single-connection.

**Do this instead:** Borrow, do computation synchronously, collect result, drop borrow, then `.await` the write. The engine should never be borrowed while waiting for I/O.

```rust
// Wrong
let guard = engine.borrow_mut(); // held across await below
let frame = conn.read_frame().await?; // PANIC if another task tries to borrow
guard.process(...);

// Correct
let frame = conn.read_frame().await?; // await with no borrow held
let result = engine.borrow_mut().process(frame); // brief borrow, no await
conn.write_frame(result).await?; // await with no borrow held
```

### Anti-Pattern 2: Using tokio::sync::Mutex on the Hot Path

**What people do:** Wrap state in `tokio::sync::Mutex<Engine>` because it is "async-safe." This is the correct choice for multi-threaded runtimes — but it adds unnecessary overhead on the single-threaded current_thread runtime where contention is impossible.

**Why it's wrong:** `tokio::sync::Mutex::lock().await` involves a future and a potential task yield even when uncontended. At 100K events/sec, this is measurable overhead. `std::sync::Mutex` is faster when uncontended (no async overhead), and `RefCell` is faster still (no atomic CAS, just a counter check).

**Do this instead:** Use `Rc<RefCell<Engine>>` with `spawn_local` tasks. Never hold the borrow across `.await`.

### Anti-Pattern 3: Allocating in the Hot Path Expression Evaluator

**What people do:** Re-parse derive expressions on every PUSH event, or build an intermediate `HashMap` of field values for each evaluation.

**Why it's wrong:** The expression evaluator is called for every derive and every `where` clause on every PUSH event. Allocation on every call balloons GC pressure (even in Rust, `Box` allocations under high load are measurable). Parsing on every call adds milliseconds of latency.

**Do this instead:** Parse to AST once at REGISTER time. During evaluation, pass a borrowed reference to the feature map as `&FeatureMap` — no cloning, no allocation unless the expression produces a new string (rare).

### Anti-Pattern 4: Doing Disk I/O on the Event Loop Thread

**What people do:** Call `std::fs::write(snapshot_path, bytes)` directly in an async function, without `spawn_blocking`.

**Why it's wrong:** On a single-threaded runtime, any blocking syscall (disk write, DNS lookup, file stat) blocks the entire event loop. A 100MB snapshot write could stall all connections for 50-500ms.

**Do this instead:** Always use `tokio::task::spawn_blocking` for disk I/O. Clone state before spawning — the blocking thread cannot borrow from `Rc<RefCell<>>` because `Rc` is not `Send`.

### Anti-Pattern 5: Recomputing All Derives on Every Operator Update

**What people do:** After updating any operator, recompute all derive expressions across all views — even those that don't reference the updated stream.

**Why it's wrong:** If a user has a `UserRisk` view that references both `Transactions` and `Logins`, and the event was for `Transactions`, there is no need to recompute `Logins`-only derives. At scale, unnecessary derive recomputation doubles or triples compute per event.

**Do this instead:** At REGISTER time, build a dependency graph: which views/derives reference which streams. On PUSH, only recompute derives that depend on the stream being updated.

## Integration Points

### Internal Boundaries

| Boundary | Communication | Notes |
|----------|---------------|-------|
| Connection → Engine | Direct method call via `Rc<RefCell<Engine>>` borrow | Never hold borrow across `.await` |
| Engine → StateStore | Direct field access — Engine owns StateStore | No indirection; they're always in the same task |
| Engine → PipelineRegistry | Direct field access — Engine owns Registry | ASTs owned by Registry, borrowed by eval |
| HTTP Handler → Engine | `tokio::sync::mpsc` + `oneshot` channel (request-reply) | HTTP runs in separate `spawn_local`, cannot share `RefCell` borrow |
| Snapshot Task → Engine | Clone state via `RefCell` borrow, then `spawn_blocking` | Clone before spawning — `Rc` is not `Send` |
| Eviction Task → Engine | `RefCell` borrow, scan, remove stale keys, release | Short borrow, no `.await` while held |
| TCP Server → HTTP Server | Shared `Arc<Metrics>` (atomic counters only) | Metrics are write-once-per-event, read by HTTP |

### External Boundaries

| Service | Integration Pattern | Notes |
|---------|---------------------|-------|
| Python SDK (client) | TCP persistent connection, binary protocol | Connection pool with configurable size |
| Prometheus/Grafana | HTTP scrape of `/metrics` endpoint | Use `metrics` + `metrics-exporter-prometheus` crates |
| Disk (snapshot) | Write-only periodic, read on startup | Bincode format, versioned with a header byte |
| OS signals | `tokio::signal::ctrl_c()` for graceful shutdown | Trigger final snapshot before exit |

## Build Order (Dependency Graph)

Build in this order to avoid blocked dependencies:

```
1. types.rs          — Value, Timestamp, FeatureMap, EntityKey (no deps)
2. engine/window.rs  — BucketedWindow<T> (depends on types)
3. engine/hll.rs     — HyperLogLog wrapper (depends on types)
4. engine/operators.rs — OperatorState enum, update/read (depends on window, hll)
5. engine/expression.rs — Lexer, Pratt parser, Expr AST, eval (depends on types)
6. engine/pipeline.rs — StreamDef, ViewDef, PipelineRegistry (depends on expression, operators)
7. state/store.rs    — StateStore, EntityState (depends on operators, types)
8. state/snapshot.rs — serialize/deserialize (depends on store)
9. state/eviction.rs — eviction loop (depends on store)
10. engine/dispatch.rs — process_event() hot path (depends on store, pipeline, expression)
11. engine/view.rs   — cross-stream, cross-key resolution (depends on dispatch, pipeline)
12. server/protocol.rs — Frame encode/decode, opcode constants (depends on types)
13. server/connection.rs — Connection struct, read/write frame (depends on protocol)
14. server/tcp.rs    — listener loop, spawn_local (depends on connection, engine)
15. server/http.rs   — axum router (depends on pipeline registry, metrics)
16. main.rs          — wire everything together (depends on all)
17. python/          — SDK (depends on stable protocol)
```

The expression evaluator (step 5) and operator implementations (step 4) are the most self-contained and can be developed + unit-tested without a running server. Start there.

## Sources

- [Tokio tutorial: Shared State](https://tokio.rs/tokio/tutorial/shared-state) — `std::sync::Mutex` recommendation, Arc pattern
- [Tokio tutorial: Framing](https://tokio.rs/tokio/tutorial/framing) — Connection struct, BufWriter, BytesMut read loop
- [Tokio: Reducing tail latencies with cooperative task yielding](https://tokio.rs/blog/2020-04-preemption) — yield_now, operation budget, spawn_blocking
- [tokio-rs/mini-redis](https://github.com/tokio-rs/mini-redis) — canonical Redis-like server architecture in Rust
- [tokio::task::LocalSet docs](https://docs.rs/tokio/latest/tokio/task/struct.LocalSet.html) — spawn_local, Rc/RefCell patterns
- [Arroyo: 10x faster sliding windows](https://www.arroyo.dev/blog/how-arroyo-beats-flink-at-sliding-windows/) — bucketed ring buffer approach for windowed aggregation
- [Cloudflare: Building fast interpreters in Rust](https://blog.cloudflare.com/building-fast-interpreters-in-rust/) — AST evaluation performance patterns
- [tokio::task::spawn_blocking docs](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html) — blocking I/O off the event loop
- [hyperloglog-rs crate](https://docs.rs/hyperloglog-rs/latest/hyperloglog_rs/) — HyperLogLog implementation for distinct_count
- [Pierre Zemb: Tokio Hidden Gems](https://pierrezemb.fr/posts/tokio-hidden-gems/) — LocalSet, deterministic single-thread execution

---
*Architecture research for: Single-threaded in-memory real-time feature server*
*Researched: 2026-04-09*
