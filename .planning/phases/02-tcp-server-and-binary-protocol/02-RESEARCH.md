# Phase 2: TCP Server and Binary Protocol - Research

**Researched:** 2026-04-09
**Domain:** Tokio async TCP server, binary protocol, HTTP management API (Rust)
**Confidence:** HIGH

## Summary

Phase 2 wraps the Phase 1 engine behind a running TCP server with a custom binary protocol and a secondary HTTP management API. The core stack is tokio (single-threaded `current_thread` runtime) for async I/O, axum for the HTTP management endpoint, and manual byte parsing for the binary protocol. All state (PipelineEngine + StateStore) lives behind `Arc<std::sync::Mutex<>>` -- not Rc<RefCell<>> -- because axum's `serve()` internally calls `tokio::spawn` which requires `Send`. On a single-threaded runtime, the Mutex never contends, so overhead is negligible.

The critical integration challenge is the REGISTER command: it receives JSON from the Python SDK and must convert it into the internal `StreamDefinition` + `FeatureDef` types. Since `StreamDefinition` and `FeatureDef` do NOT derive `serde::Deserialize`, and `FeatureDef::Derive` contains a pre-parsed `Expr` AST, the server needs a DTO layer (e.g., `RegisterRequest` with string expressions + string durations) that validates and converts to domain types, parsing expressions via `parse_expr()` at registration time.

**Primary recommendation:** Use `tokio::spawn` + `Arc<std::sync::Mutex<>>` for all shared state. Keep the binary protocol parser as manual byte reads (no framework). Use axum for HTTP. Implement MSET cooperative yielding via `tokio::task::yield_now()` after each 1024-key chunk.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- REGISTER allowed on both TCP (opcode 0x05) and HTTP -- per CLAUDE.md spec
- No hard connection limit (OS default) -- v1 single-threaded, connections naturally serialize
- Malformed frames: send error response, then close the connection -- prevents cascading parse failures from corrupted stream
- TCP keepalive: tokio defaults (no custom keepalive/timeout) -- v1 simplicity
- Response format: status byte (0x00 = OK, 0x01 = Error) followed by payload. OK payload = feature map JSON bytes; Error payload = UTF-8 error message string
- Plain text error messages only (no structured error codes) -- v1 simplicity
- PUSH to unregistered stream: return error "unknown stream: {name}"
- GET for key with no state: return empty JSON object `{}`
- MSET cooperative yielding: 1024 keys per chunk, yield to event loop between chunks per CLAUDE.md
- Tokio runtime: single-threaded `current_thread` per "single-threaded core (v1)" design principle
- HTTP management API: same binary, same tokio runtime, separate listener on port 6401
- HTTP framework: axum -- lightweight, tokio-native. Only health endpoint in Phase 2; full CRUD in Phase 4

### Claude's Discretion
- Internal buffer sizes for frame reading
- Exact task structure within the tokio runtime (single select loop vs separate spawn_local tasks)
- Frame parsing implementation details (nom/winnow vs manual byte parsing)
- Test helper design for TCP client simulation

### Deferred Ideas (OUT OF SCOPE)
- Connection authentication/TLS -- post-v1 security hardening
- Protocol versioning in frame header -- could add version byte later if needed
- Backpressure/rate limiting -- not needed for v1 single-threaded model
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| SRV-01 | TCP server accepts persistent connections on configurable port (default 6400) | tokio TcpListener with current_thread runtime; configurable via server config struct |
| SRV-02 | Binary protocol uses length-prefixed frames (4-byte u32 BE length + 1-byte opcode + payload) | Manual byte parsing with tokio::io::AsyncReadExt/AsyncWriteExt; no framework needed |
| SRV-03 | PUSH command ingests event to a stream and returns updated features synchronously | Wire up to PipelineEngine::push(); serialize FeatureMap to JSON via conversion function |
| SRV-04 | GET command returns all current features for an entity key | Wire up to PipelineEngine::get_features(); return empty `{}` for unknown keys |
| SRV-05 | SET command writes static feature values for a key | Wire up to StateStore::set_static() for each key-value pair in payload |
| SRV-06 | MSET command bulk-writes with cooperative yielding (chunked, non-blocking) | Process 1024 keys per chunk, tokio::task::yield_now() between chunks |
| SRV-07 | REGISTER command accepts pipeline definitions as JSON | DTO deserialization -> validation -> conversion to StreamDefinition/FeatureDef |
| SRV-08 | HTTP management API serves health on separate port (default 6401) | axum Router with GET /health endpoint; full CRUD deferred to Phase 4 |
</phase_requirements>

## Project Constraints (from CLAUDE.md)

- **Naming:** "Tally" everywhere (not "Streamlet") -- per approved rename
- **AHashMap** (not std HashMap) per locked decision
- **SystemTime** for timestamps (not Instant) -- client-supplied Unix timestamps must be comparable
- **postcard** for snapshot serialization (not bincode) -- RUSTSEC-2025-0141
- **winnow** for expression parsing
- **thiserror** for error types
- **Single-threaded core (v1)** -- like Redis, one thread, no locks, no contention
- **Edition 2021** for compatibility
- **TDD / Contract-First:** Define contracts and write tests before implementation (user memory directive)

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tokio | 1.50 | Async runtime (current_thread), TCP listener, I/O | De facto Rust async runtime; current_thread flavor gives Redis-like single-threaded model [VERIFIED: web search, docs.rs] |
| axum | 0.8.8 | HTTP management API (health endpoint) | Tokio-native, lightweight, ergonomic Router API [VERIFIED: web search, crates.io] |
| bytes | 1.11 | Efficient byte buffer for protocol parsing | Zero-copy byte manipulation, Buf/BufMut traits [VERIFIED: web search, docs.rs] |

### Already Present (from Phase 1)
| Library | Version | Purpose |
|---------|---------|---------|
| ahash | 0.8 | Fast hashing for AHashMap |
| winnow | 1.0 | Expression parser |
| thiserror | 2.0 | Error types |
| serde | 1.0 | Serialization framework |
| serde_json | 1.0 | JSON serialization/deserialization |
| postcard | 1.1 | Binary serialization (Phase 4 snapshots) |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Manual byte parsing | tokio-codec / winnow for frames | Manual is simpler for this fixed format; codec adds abstraction layer we don't need |
| axum | warp / actix-web | axum is tokio-native with minimal footprint; warp and actix add unnecessary complexity for 1 endpoint |
| bytes crate | raw Vec<u8> | bytes provides Buf/BufMut traits that simplify protocol reading; minimal cost, good ergonomics |
| Arc<std::sync::Mutex> | Rc<RefCell> | Rc<RefCell> requires spawn_local + LocalSet; but axum::serve uses tokio::spawn internally requiring Send. Arc<Mutex> works everywhere with zero contention on current_thread |

**Installation (additions to Cargo.toml):**
```toml
[dependencies]
tokio = { version = "1.50", features = ["rt", "net", "io-util", "macros", "time"] }
axum = "0.8"
bytes = "1.11"
```

**Tokio features breakdown:** [VERIFIED: docs.rs/tokio feature flags]
- `rt` -- enables current_thread runtime and tokio::spawn
- `net` -- enables TcpListener, TcpStream
- `io-util` -- enables AsyncReadExt, AsyncWriteExt (read_u32, read_exact, write_all)
- `macros` -- enables #[tokio::main] attribute
- `time` -- enables tokio::time (needed for potential timeouts, cooperative scheduling)

**Note:** Do NOT enable `rt-multi-thread`. The v1 design is explicitly single-threaded.

## Architecture Patterns

### Recommended Project Structure
```
src/
  server/
    mod.rs            # pub mod tcp; pub mod protocol; pub mod http;
    tcp.rs            # TcpListener, connection loop, frame dispatch
    protocol.rs       # Frame parsing (read/write), command encoding/decoding
    http.rs           # axum Router, health endpoint
  engine/             # (existing from Phase 1)
  state/              # (existing from Phase 1)
  types.rs            # (existing) + FeatureValue->serde_json::Value conversion
  error.rs            # (existing) + new Protocol variants if needed
  main.rs             # tokio::main entry point, start TCP + HTTP
  lib.rs              # add pub mod server
```

### Pattern 1: Shared State via Arc<Mutex<>>
**What:** PipelineEngine and StateStore wrapped in Arc<std::sync::Mutex<>> for sharing between TCP connections and HTTP handlers.
**When to use:** Always in this architecture (axum requires Send).
**Why std::sync::Mutex not tokio::sync::Mutex:** Lock is never held across .await points. Each command handler locks, processes synchronously (engine operations are sub-microsecond), and unlocks before any I/O. std::sync::Mutex has lower overhead. [ASSUMED]

```rust
// Source: Pattern derived from tokio shared-state tutorial + project constraints
use std::sync::{Arc, Mutex};

struct AppState {
    engine: PipelineEngine,
    store: StateStore,
}

type SharedState = Arc<Mutex<AppState>>;
```

### Pattern 2: Connection Handler Loop
**What:** Each TCP connection spawned as a separate tokio task that reads frames in a loop until disconnect or error.
**When to use:** For every accepted TCP connection.

```rust
// Source: Tokio tutorial pattern adapted for binary protocol
async fn handle_connection(stream: TcpStream, state: SharedState) {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    loop {
        // Read frame: 4 bytes length + 1 byte opcode + payload
        let len = match reader.read_u32().await {
            Ok(len) => len,
            Err(_) => break, // Connection closed
        };
        let opcode = reader.read_u8().await?;
        let mut payload = vec![0u8; (len - 1) as usize];
        reader.read_exact(&mut payload).await?;

        // Dispatch to command handler
        let response = match opcode {
            0x01 => handle_push(&payload, &state),
            0x02 => handle_get(&payload, &state),
            0x03 => handle_set(&payload, &state),
            0x04 => handle_mset(&payload, &state).await, // async: yields
            0x05 => handle_register(&payload, &state),
            _ => Err(TallyError::Protocol("unknown opcode".into())),
        };

        // Write response frame
        write_response(&mut writer, response).await?;
    }
}
```

### Pattern 3: MSET Cooperative Yielding
**What:** Large MSET operations processed in 1024-key chunks with `tokio::task::yield_now()` between chunks.
**When to use:** For MSET command handler only.
**Critical detail:** `yield_now()` returns control to the tokio scheduler, allowing pending PUSH/GET handlers on other connections to execute. On current_thread runtime, this is the only way to achieve fairness since there's one thread. [VERIFIED: tokio docs on yield_now]

```rust
// Source: CLAUDE.md MSET spec + tokio yield_now docs
async fn handle_mset(payload: &[u8], state: &SharedState) -> Result<(), TallyError> {
    let entries = parse_mset_payload(payload)?;
    let now = SystemTime::now();

    for chunk in entries.chunks(1024) {
        {
            let mut app = state.lock().unwrap();
            for (key, features) in chunk {
                for (name, value) in features {
                    app.store.set_static(key, name, value, now);
                }
            }
        } // Lock released before yield
        tokio::task::yield_now().await;
    }
    Ok(())
}
```

### Pattern 4: Response Wire Format
**What:** Length-prefixed response with status byte.
**Format per CONTEXT.md decision:**
```
[4 bytes: total response length u32 BE (includes status byte + payload)]
[1 byte: status (0x00 = OK, 0x01 = Error)]
[N bytes: payload]
  - OK + PUSH/GET: JSON bytes of feature map
  - OK + SET/MSET/REGISTER: empty (0 bytes)
  - Error: UTF-8 error message string
```

### Pattern 5: FeatureMap to JSON Conversion
**What:** Convert internal FeatureMap (AHashMap<String, FeatureValue>) to JSON bytes for responses.
**Why needed:** FeatureValue's default serde Serialize produces tagged JSON like `{"Float": 1.5}` instead of plain `1.5`. Adding `#[serde(untagged)]` would affect postcard snapshot serialization (Phase 4). [VERIFIED: serde docs on enum representations]

```rust
// Source: serde_json docs + project constraint analysis
impl FeatureValue {
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            FeatureValue::Float(f) => serde_json::Value::from(*f),
            FeatureValue::Int(i) => serde_json::Value::from(*i),
            FeatureValue::String(s) => serde_json::Value::String(s.clone()),
            FeatureValue::Missing => serde_json::Value::Null,
        }
    }
}

fn feature_map_to_json(features: &FeatureMap) -> Vec<u8> {
    let json_map: serde_json::Map<String, serde_json::Value> = features
        .iter()
        .map(|(k, v)| (k.clone(), v.to_json_value()))
        .collect();
    serde_json::to_vec(&serde_json::Value::Object(json_map)).unwrap()
}
```

### Pattern 6: REGISTER JSON DTO
**What:** Intermediate deserialization type for the REGISTER command payload.
**Why needed:** `StreamDefinition` and `FeatureDef` don't derive `Deserialize`. `FeatureDef::Derive` contains a pre-parsed `Expr` AST node (parsed from a string expression). The REGISTER payload must carry string expressions that get parsed server-side. [VERIFIED: codebase inspection -- StreamDefinition has no Deserialize]

```rust
// Source: Project analysis + serde best practices
#[derive(Deserialize)]
struct RegisterRequest {
    name: String,
    key_field: String,
    features: Vec<FeatureDefRequest>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum FeatureDefRequest {
    #[serde(rename = "count")]
    Count { window: String, #[serde(default = "default_bucket")] bucket: Option<String> },
    #[serde(rename = "sum")]
    Sum { field: String, window: String, #[serde(default)] optional: bool },
    #[serde(rename = "avg")]
    Avg { field: String, window: String, #[serde(default)] optional: bool },
    #[serde(rename = "derive")]
    Derive { expr: String },
}
```

The conversion function parses duration strings (e.g., "30m", "1h", "24h") and expression strings, returning `Result<StreamDefinition, TallyError>`.

### Pattern 7: Main Entry Point
**What:** Single tokio current_thread runtime driving both TCP and HTTP listeners.

```rust
// Source: tokio docs + axum docs
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let state = Arc::new(Mutex::new(AppState {
        engine: PipelineEngine::new(),
        store: StateStore::new(),
    }));

    let tcp_state = state.clone();
    let tcp_handle = tokio::spawn(async move {
        run_tcp_server("0.0.0.0:6400", tcp_state).await;
    });

    let http_state = state.clone();
    let http_handle = tokio::spawn(async move {
        run_http_server("0.0.0.0:6401", http_state).await;
    });

    tokio::select! {
        _ = tcp_handle => {},
        _ = http_handle => {},
    }
}
```

### Anti-Patterns to Avoid
- **Holding Mutex across .await:** Never hold the lock while awaiting I/O. Lock, process synchronously, unlock, then do I/O. This prevents deadlocks even though single-threaded runtime can't truly deadlock from contention.
- **Using tokio::sync::Mutex:** Unnecessary overhead. std::sync::Mutex is correct here because the lock is never held across .await points. [ASSUMED]
- **Buffering entire MSET in memory before processing:** Parse and process incrementally in chunks. The payload is already in memory from frame reading, but processing should be chunked for yielding.
- **Using rt-multi-thread feature:** Contradicts v1 single-threaded design principle. current_thread only.
- **Custom frame codec (tokio_util::codec):** Over-engineering for this simple fixed format. Manual read_u32/read_u8/read_exact is clearer and has less abstraction cost.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Async TCP server | Raw socket handling | tokio TcpListener/TcpStream | Handles epoll/kqueue, non-blocking I/O, task scheduling |
| HTTP server | Raw HTTP parsing | axum + tokio TcpListener | HTTP/1.1 is complex (chunked encoding, keep-alive, headers). axum handles it all |
| Byte buffer management | Manual Vec<u8> slicing | bytes crate Buf/BufMut | Provides cursor-based reading, prevents off-by-one errors |
| Duration string parsing | Regex-based parser | Simple match on suffix ("m", "h", "d", "s") | Fixed set of known suffixes; regex is overkill |

**Key insight:** The binary protocol format is simple enough to hand-parse (fixed header, length-prefixed). A codec framework would add indirection without benefit. But the HTTP protocol is too complex to hand-roll -- use axum.

## Common Pitfalls

### Pitfall 1: Frame Length Off-By-One
**What goes wrong:** The 4-byte length prefix counts either just the payload or the entire frame (including the length bytes themselves). Mismatched understanding between client and server causes parse failures.
**Why it happens:** CLAUDE.md says "message length" which is ambiguous. Does it include the 4 length bytes? The opcode?
**How to avoid:** Define clearly: the 4-byte length = number of bytes AFTER the length field (i.e., opcode + payload). This is the standard length-prefix convention (like Redis RESP bulk strings). Document in protocol.rs.
**Warning signs:** Tests pass with hand-crafted frames but fail with SDK-generated frames. Frame boundaries drift causing garbled reads.

### Pitfall 2: Lock Poisoning on Panic
**What goes wrong:** If a command handler panics while holding the Mutex, the mutex becomes poisoned and all subsequent operations fail.
**Why it happens:** std::sync::Mutex poisons on panic by design.
**How to avoid:** Never unwrap user input inside the lock. Parse and validate BEFORE acquiring the lock. If absolutely necessary, use `.lock().unwrap_or_else(|e| e.into_inner())` to recover from poisoned mutex -- acceptable in single-threaded context since no concurrent state corruption is possible.
**Warning signs:** "PoisonError" in logs after a malformed event causes a panic in the engine.

### Pitfall 3: FeatureValue JSON Serialization
**What goes wrong:** FeatureValue serializes as `{"Float": 1.5}` instead of `1.5` because serde's default enum serialization is externally tagged.
**Why it happens:** FeatureValue derives Serialize without `#[serde(untagged)]`.
**How to avoid:** Don't add `#[serde(untagged)]` (breaks postcard). Instead, implement explicit `FeatureValue -> serde_json::Value` conversion for protocol responses.
**Warning signs:** Python SDK receives `{"Float": 1.5}` instead of `1.5` in feature maps.

### Pitfall 4: MSET Yield_now Doesn't Guarantee Fairness
**What goes wrong:** `tokio::task::yield_now()` does not guarantee other tasks run. On current_thread, the scheduler may immediately re-poll the yielding task.
**Why it happens:** Tokio's documentation explicitly warns: "it is generally not guaranteed that the runtime behaves like you expect it to when deciding which task to schedule next after a call to yield_now()." [VERIFIED: tokio docs on yield_now]
**How to avoid:** For testing, verify interleaving by timing PUSH responses during MSET. In practice, yield_now is sufficient because pending I/O readiness (from other connections) will cause the scheduler to prefer those tasks. Accept that yield_now is best-effort cooperative yielding.
**Warning signs:** MSET appears to block all other operations despite yielding. Test with concurrent connections sending PUSH during MSET to verify.

### Pitfall 5: Connection Handler Cleanup
**What goes wrong:** When a connection is dropped unexpectedly (client crashes), the spawned task may leak or error spam.
**Why it happens:** read_u32() returns Err(UnexpectedEof) on clean disconnect, but other errors (ConnectionReset) may also occur.
**How to avoid:** Treat all read errors in the frame loop as "connection closed" -- break out of the loop, let the task complete cleanly. Log at debug level only.
**Warning signs:** Server logs filled with "connection reset by peer" errors during normal operation.

### Pitfall 6: REGISTER JSON Schema Mismatch
**What goes wrong:** The JSON format accepted by REGISTER doesn't match what the Python SDK sends.
**Why it happens:** The schema is defined in two places (Rust DTO and Python serializer) without a shared spec.
**How to avoid:** Define the JSON schema explicitly in the protocol documentation. Write tests that use the exact JSON format the SDK will produce. In Phase 3 (Python SDK), test against the same format.
**Warning signs:** SDK register() fails with cryptic deserialization errors.

## Code Examples

### Frame Reading (Verified Pattern)
```rust
// Source: tokio AsyncReadExt docs + CLAUDE.md wire format spec
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use bytes::{Buf, BytesMut};

/// Read a complete frame from the connection.
/// Returns (opcode, payload_bytes) or None on connection close.
async fn read_frame(reader: &mut (impl AsyncReadExt + Unpin))
    -> Result<Option<(u8, Vec<u8>)>, std::io::Error>
{
    // Read 4-byte length (u32 BE)
    let len = match reader.read_u32().await {
        Ok(len) => len as usize,
        Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    };

    if len == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "zero-length frame",
        ));
    }

    // Read opcode (1 byte)
    let opcode = reader.read_u8().await?;

    // Read payload (len - 1 bytes, since len includes opcode)
    let payload_len = len - 1;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        reader.read_exact(&mut payload).await?;
    }

    Ok(Some((opcode, payload)))
}

/// Write a response frame.
async fn write_response(
    writer: &mut (impl AsyncWriteExt + Unpin),
    status: u8,
    payload: &[u8],
) -> Result<(), std::io::Error> {
    let len = 1 + payload.len(); // status byte + payload
    writer.write_u32(len as u32).await?;
    writer.write_u8(status).await?;
    if !payload.is_empty() {
        writer.write_all(payload).await?;
    }
    writer.flush().await?;
    Ok(())
}
```

### String Reading in Protocol
```rust
// Source: CLAUDE.md string encoding spec
/// Read a length-prefixed string (u16 BE length + UTF-8 bytes).
fn read_string(buf: &mut &[u8]) -> Result<String, TallyError> {
    if buf.len() < 2 {
        return Err(TallyError::Protocol("unexpected end of frame".into()));
    }
    let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    *buf = &buf[2..];
    if buf.len() < len {
        return Err(TallyError::Protocol("string truncated".into()));
    }
    let s = std::str::from_utf8(&buf[..len])
        .map_err(|_| TallyError::Protocol("invalid UTF-8 in string".into()))?
        .to_string();
    *buf = &buf[len..];
    Ok(s)
}
```

### Duration String Parsing
```rust
// Source: Project-specific format for REGISTER JSON
fn parse_duration_str(s: &str) -> Result<Duration, TallyError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(TallyError::Protocol("empty duration string".into()));
    }
    let (num_str, suffix) = if s.ends_with("ms") {
        (&s[..s.len()-2], "ms")
    } else {
        (&s[..s.len()-1], &s[s.len()-1..])
    };
    let num: u64 = num_str.parse()
        .map_err(|_| TallyError::Protocol(format!("invalid duration: {}", s)))?;
    match suffix {
        "s" => Ok(Duration::from_secs(num)),
        "m" => Ok(Duration::from_secs(num * 60)),
        "h" => Ok(Duration::from_secs(num * 3600)),
        "d" => Ok(Duration::from_secs(num * 86400)),
        "ms" => Ok(Duration::from_millis(num)),
        _ => Err(TallyError::Protocol(format!("unknown duration suffix: {}", s))),
    }
}
```

### Axum Health Endpoint
```rust
// Source: axum 0.8 docs
use axum::{Router, routing::get, Json};

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

async fn run_http_server(addr: &str, state: SharedState) {
    let app = Router::new()
        .route("/health", get(health));
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| hyper::Server for HTTP | axum::serve(listener, app) | axum 0.8 (2024) | Simpler API, built-in TcpListener integration |
| tokio::spawn_blocking for CPU work | Inline sync work in async handlers | Ongoing | For sub-microsecond engine operations, spawn_blocking overhead > the work itself |
| tokio_util::codec::Framed | Manual AsyncReadExt | Preference | Codec adds abstraction; manual read is clearer for simple fixed-format protocols |

**Deprecated/outdated:**
- `hyper::Server::bind()`: Replaced by `axum::serve()` in axum 0.7+ [VERIFIED: axum docs]
- `tokio::runtime::Builder::new().basic_scheduler()`: Renamed to `new_current_thread()` in tokio 1.0 [VERIFIED: tokio docs]

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | Rust built-in #[test] + #[tokio::test] |
| Config file | None needed (Cargo native) |
| Quick run command | `cargo test` |
| Full suite command | `cargo test -- --test-threads=1` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| SRV-01 | TCP server accepts connections on port 6400 | integration | `cargo test --test test_server test_tcp_connect` | No -- Wave 0 |
| SRV-02 | Length-prefixed binary frames parsed correctly | unit | `cargo test protocol::tests` | No -- Wave 0 |
| SRV-03 | PUSH returns updated features synchronously | integration | `cargo test --test test_server test_push_returns_features` | No -- Wave 0 |
| SRV-04 | GET returns current features for key | integration | `cargo test --test test_server test_get_features` | No -- Wave 0 |
| SRV-05 | SET writes static features | integration | `cargo test --test test_server test_set_static` | No -- Wave 0 |
| SRV-06 | MSET with yielding doesn't starve PUSH/GET | integration | `cargo test --test test_server test_mset_cooperative` | No -- Wave 0 |
| SRV-07 | REGISTER accepts JSON pipeline definition | integration | `cargo test --test test_server test_register_pipeline` | No -- Wave 0 |
| SRV-08 | HTTP /health returns 200 OK | integration | `cargo test --test test_server test_health_endpoint` | No -- Wave 0 |

### Testing Strategy
- **Protocol unit tests:** Test frame parsing/serialization with byte arrays (no network)
- **Integration tests:** Start server on random port, connect with raw TcpStream, send/receive frames
- **Test helper:** A minimal TCP client struct that encodes/decodes the binary protocol for test assertions
- **#[tokio::test(flavor = "current_thread")]** for all async tests to match production runtime

### Sampling Rate
- **Per task commit:** `cargo test`
- **Per wave merge:** `cargo test -- --test-threads=1`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/test_server.rs` -- integration tests for all SRV-* requirements
- [ ] `src/server/protocol.rs` -- protocol unit tests (inline #[cfg(test)])
- [ ] Test helper: TCP client for binary protocol (in test_server.rs or a shared test utility)

## Assumptions Log

> List all claims tagged `[ASSUMED]` in this research. The planner and discuss-phase use this
> section to identify decisions that need user confirmation before execution.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | std::sync::Mutex is preferable to tokio::sync::Mutex because lock is never held across .await points | Architecture Patterns - Pattern 1 | If any future handler needs async work while holding state, would need refactoring to tokio::sync::Mutex. Low risk for Phase 2. |
| A2 | tokio::sync::Mutex has higher overhead than std::sync::Mutex for non-contended single-thread case | Anti-Patterns | Marginal difference in practice. Low risk. |

## Open Questions (RESOLVED)

1. **Frame length semantics: does length include the opcode byte?**
   - What we know: CLAUDE.md says `[4 bytes: message length][1 byte: opcode][payload]`. The most natural reading is that "message length" = opcode + payload (i.e., everything after the 4-byte length field).
   - What's unclear: "message length" could theoretically mean just the payload length. Need to pick one and document.
   - Recommendation: Length = opcode + payload bytes. This follows the standard length-prefix convention and matches Redis RESP. This is what the code examples above assume.

2. **REGISTER JSON schema: exact format for the Python SDK**
   - What we know: The SDK will serialize stream definitions as JSON. The server must accept that JSON.
   - What's unclear: Exact field names, duration format strings, feature type discriminator.
   - Recommendation: Define the schema in Phase 2 and have Phase 3 (Python SDK) conform to it. Use `{"type": "count", "window": "30m"}` style internally tagged enum serialization.

3. **SET command payload: JSON map or protocol-encoded key-value pairs?**
   - What we know: CLAUDE.md says `key: string, payload: JSON map of feature_name -> value`.
   - What's unclear: Whether SET payload is a JSON blob or protocol-encoded fields.
   - Recommendation: JSON blob (consistent with PUSH and REGISTER). The payload after the key string is raw JSON bytes.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Rust toolchain | Build | Yes | 1.94.1 (2026-03-25) | -- |
| cargo | Build/test | Yes | 1.94.1 | -- |
| tokio (crate) | Runtime | Via crates.io | 1.50.x | -- |
| axum (crate) | HTTP API | Via crates.io | 0.8.8 | -- |
| bytes (crate) | Protocol | Via crates.io | 1.11.x | -- |

**Missing dependencies with no fallback:** None.
**Missing dependencies with fallback:** None.

## Sources

### Primary (HIGH confidence)
- [tokio docs](https://docs.rs/tokio/latest/tokio/) - runtime flavors, TcpListener, yield_now, spawn_local
- [axum docs](https://docs.rs/axum/latest/axum/) - serve function, Router, State extractor
- [bytes docs](https://docs.rs/bytes/latest/bytes/) - Buf, BufMut traits
- [serde enum representations](https://serde.rs/enum-representations.html) - tagged vs untagged serialization
- [Codebase: src/engine/pipeline.rs] - PipelineEngine API (push, get_features, register)
- [Codebase: src/state/store.rs] - StateStore API (set_static, get_all_features)
- [Codebase: src/types.rs] - FeatureValue enum (no untagged attribute)
- [Codebase: src/engine/expression.rs] - Expr type (derives Serialize+Deserialize), parse_expr function

### Secondary (MEDIUM confidence)
- [tokio blog on cooperative yielding](https://tokio.rs/blog/2020-04-preemption) - yield_now behavior and caveats
- [tokio discussion on spawn vs spawn_local](https://users.rust-lang.org/t/tokio-spawn-vs-spawn-local/62047) - Send requirements
- [axum discussion on current_thread](https://github.com/tokio-rs/axum/discussions/2501) - single-threaded server setup

### Tertiary (LOW confidence)
- None -- all claims verified against docs or codebase.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - versions verified via web search against crates.io/docs.rs; APIs verified against official docs
- Architecture: HIGH - patterns derived from official tokio/axum documentation and codebase analysis of Phase 1 APIs
- Pitfalls: HIGH - derived from documented tokio behavior (yield_now caveats), serde docs (enum serialization), and std::sync::Mutex semantics

**Research date:** 2026-04-09
**Valid until:** 2026-05-09 (stable ecosystem; tokio 1.x and axum 0.8.x are mature)
