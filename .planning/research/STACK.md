# Stack Research

**Domain:** Real-time feature server (Rust binary + Python SDK)
**Researched:** 2026-04-09
**Confidence:** HIGH (core Rust stack), MEDIUM (HyperLogLog crate choice, Python TCP pooling)

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| tokio | 1.51.0 | Async runtime, TCP server, timers | The standard async runtime for Rust. `current_thread` flavor maps directly to the Redis-like single-threaded design: one event loop, no locks, no contention. All TCP I/O and timer-based snapshot scheduling run here. |
| serde | 1.0.228 | Serialization framework | Universal Rust serialization trait. Required by every serialization crate in the ecosystem. Derive macros cover all state structs with zero boilerplate. |
| serde_json | 1.0.149 | JSON for protocol payloads and pipeline registration | Event payloads arrive as JSON. Pipeline definitions are sent from Python as JSON. `serde_json::Value` covers dynamic event fields without a fixed schema. |
| postcard | 1.1.3 | Binary snapshot persistence to disk | Drop-in serde-compatible binary format. Actively maintained (7,000+ dependents, 16.8M downloads), stable wire format since v1.0.0, smaller output than bincode, used in embedded/constrained contexts — exactly the predictable footprint needed for periodic full-state snapshots. Replaces bincode which is now unmaintained (RUSTSEC-2025-0141). |
| axum | 0.8.x | HTTP management API (port 6401) | Built by the Tokio team, shares the same runtime, and is the dominant Rust web framework as of 2026. Management API is not on the hot path — axum's ergonomics and `tower` middleware integration are the right tradeoffs. Zero context-switching overhead since it runs on the same tokio current_thread executor. |
| bytes | 1.x | Byte buffer abstraction for TCP framing | The standard Tokio-ecosystem buffer. `BytesMut` handles length-prefixed frame reading without copies. Zero-cost slicing via reference counting. Required by tokio's `AsyncReadExt`/`AsyncWriteExt` patterns. |

### Supporting Libraries

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| ahash | 0.8.x | Fast hash function for HashMap | Replace default SipHash in all internal `HashMap` instances (entity state store, pipeline registry, feature maps). AES-based, DoS-resistant, 2-5x faster than SipHash on string keys. Use `AHashMap` type alias from ahash or enable the `ahash` feature on hashbrown. |
| thiserror | 2.0.17 | Typed error definitions | Use for all library-facing errors: protocol parse errors, expression evaluation errors, operator errors. Generates `Display` and `std::error::Error` impls from derive macros. Keeps error types explicit and matchable. |
| anyhow | 1.x | Contextual error propagation in binaries | Use in `main.rs` and server startup paths where specific error types don't matter — just propagating with context. Pair with `thiserror` for the engine/state modules. |
| tracing | 0.1.x | Structured logging and spans | Maintained by the Tokio team. Async-aware — integrates properly with tokio tasks without losing span context. Use `tracing-subscriber` with `EnvFilter` for configurable log levels. Critical for per-event latency tracing and PUSH throughput observability. |
| tracing-subscriber | 0.3.x | Tracing output (stdout/JSON) | Pair with tracing. `fmt::Subscriber` for development; JSON format for production. |
| prometheus-client | 0.22.x | Prometheus metrics exposition | The official Prometheus Rust client implementing the OpenMetrics spec. Expose on the HTTP management API (`/metrics`). Track: events/sec, GET p99 latency, state store key count, snapshot duration. |
| criterion | 0.7.0 | Statistical micro-benchmarking | The de facto Rust benchmark harness — runs on stable Rust, generates statistical confidence intervals. Use for the `benches/throughput.rs` and `benches/latency.rs` targets in Cargo.toml. |
| winnow | 0.6.x | Expression parser (derive/where clauses) | Parser combinator library that prioritizes performance and zero-copy parsing. Better fit than pest for this use case: the expression grammar is small and known at compile time, so PEG grammar files add unnecessary indirection. Winnow's functional style integrates naturally with Rust's type system for building the expression AST. |

### Python SDK Libraries

| Library | Version | Purpose | Notes |
|---------|---------|---------|-------|
| Python stdlib `socket` | 3.10+ | TCP client transport | No dependencies. The Python SDK is a thin client — raw socket + `struct.pack` for binary framing is sufficient and keeps the SDK zero-dependency. |
| Python stdlib `struct` | 3.10+ | Binary protocol encoding | Pack/unpack the length-prefixed frame format (u32 big-endian length + opcode + payload). Exact match to the wire format defined in CLAUDE.md. |
| Python stdlib `threading` | 3.10+ | Thread-safe connection pool | Use `threading.local()` or a pool with `threading.Lock` for connection reuse. The SDK is synchronous — one connection per thread is the simplest correct model. |

### Development Tools

| Tool | Purpose | Notes |
|------|---------|-------|
| cargo | Build, test, bench | Use workspaces if the Python SDK has a Rust extension later. For now, single crate. |
| cargo-watch | Auto-rebuild on file changes | Install with `cargo install cargo-watch`. Run `cargo-watch -x test` during development. |
| cargo-deny | Audit dependencies for advisories | Catches unmaintained crates like bincode early. Run in CI. |
| maturin | Build Python extension from Rust (future) | If the Python SDK later wraps a Rust extension (PyO3), maturin handles the build. Not needed for v1 pure-Python SDK. |

## Installation

```toml
# Cargo.toml [dependencies]
tokio = { version = "1", features = ["rt", "net", "io-util", "time", "macros", "sync"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
postcard = { version = "1", features = ["alloc"] }
axum = "0.8"
bytes = "1"
ahash = "0.8"
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
prometheus-client = "0.22"
winnow = "0.6"

# Cargo.toml [dev-dependencies]
criterion = { version = "0.7", features = ["html_reports"] }
tokio = { version = "1", features = ["rt", "macros", "test-util"] }  # test-util for time control
```

```toml
# tokio runtime configuration in main.rs
# Use current_thread builder for single-threaded Redis-like event loop:
# tokio::runtime::Builder::new_current_thread()
#   .enable_all()
#   .build()
```

```toml
# Python SDK (python/pyproject.toml) — zero Rust dependencies
[project]
name = "tally"
requires-python = ">=3.10"
dependencies = []  # pure stdlib only
```

## Alternatives Considered

| Recommended | Alternative | Why Not |
|-------------|-------------|---------|
| postcard | bincode | RUSTSEC-2025-0141: bincode is unmaintained. Maintainer published 3.0.0 as a tombstone (compiler error only). All bincode users are migrating. |
| postcard | rkyv | rkyv is faster for zero-copy deserialization but has a steeper API surface and requires unsafe for access. For periodic snapshots (not hot path), postcard's simpler serde API is the right tradeoff. |
| winnow | pest | pest requires an external `.pest` grammar file for a language simple enough to define inline. Adds a compile step and grammar file maintenance. Winnow is inline, typed, and fast. |
| winnow | nom | nom and winnow share the same lineage (winnow is the evolution of nom). Winnow has better error messages, improved combinator naming, and is the recommended replacement. |
| winnow | hand-written recursive descent | Viable for a grammar this small, but winnow produces equally fast code with better maintainability and built-in error recovery. |
| axum | actix-web | actix-web uses a multi-threaded runtime by default, which conflicts with the single-threaded tokio `current_thread` design. axum integrates with any tokio executor. |
| axum | hyper (direct) | hyper is the underlying HTTP library axum uses. Using it directly gains nothing for a management API that receives <1 req/sec, while axum's routing/extractor ergonomics are significantly better. |
| prometheus-client | prometheus (TiKV) | prometheus (TiKV fork) uses global state and thread-local storage, which doesn't integrate cleanly with single-threaded async. prometheus-client is the official OpenMetrics implementation without global state. |
| stdlib socket (Python) | PyO3 extension | PyO3 adds a Rust compilation step to the Python SDK install, which kills the "pip install tally" zero-ops story. Python's stdlib socket is fast enough: the protocol is simple and the bottleneck is the server, not the client encoding. |
| ahash | FxHash | FxHash has no DoS protection (non-keyed hash). If entity keys come from external events, an attacker can craft collisions. ahash is DoS-resistant and nearly as fast. |

## What NOT to Use

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| bincode (any version) | RUSTSEC-2025-0141 — maintainer left, 3.0.0 is a deliberate tombstone with a compile error | postcard |
| tokio multi_thread runtime | Violates the single-threaded v1 design; introduces shared state complexity; kills the Redis-style simplicity | `Builder::new_current_thread()` |
| Diesel / SQLx / any DB driver | No database. All state is in-memory HashMap. External DB would destroy the sub-millisecond latency target | In-memory HashMap + postcard snapshots |
| std::collections::HashMap | Default SipHash hasher is ~3x slower than ahash on string keys — significant on the hot path at 100K events/sec | ahash's `AHashMap` |
| tokio::sync::Mutex on hot path | Any lock on the event-processing path defeats single-threaded design and adds latency | No lock needed — single-threaded runtime means `Rc<RefCell<>>` or just plain ownership |
| log + env_logger | No async awareness — log spans don't propagate through tokio tasks correctly | tracing + tracing-subscriber |
| prost / protobuf | The custom binary protocol is simpler than protobuf and doesn't need schema evolution in v1; protobuf adds a codegen step and a .proto file per command | Custom framing with `bytes` + `serde_json` |

## Stack Patterns by Variant

**For the TCP hot path (PUSH/GET/SET):**
- Use `tokio::net::TcpListener` + `tokio::spawn` per connection
- Frame with `bytes::BytesMut` + `tokio::io::AsyncReadExt::read_exact`
- No allocations on the response path for GET (return borrowed data where possible)
- `current_thread` runtime: no `Send` bound required on state, use `Rc` freely

**For the HTTP management API:**
- Run axum on the same `current_thread` runtime (no separate thread needed)
- Use `axum::Router` with `Arc<AppState>` passed via `Extension`
- Expose `/metrics` with `prometheus-client`'s text format encoder

**For snapshot persistence:**
- Serialize with `postcard::to_allocvec(&state)` — returns `Vec<u8>`
- Write to a temp file, then `std::fs::rename` for atomicity
- Trigger via `tokio::time::interval` on the single-threaded runtime
- Cooperative yield between chunks using `tokio::task::yield_now()`

**For expression evaluation (derive/where):**
- Parse expression strings at pipeline registration time (REGISTER command)
- Store parsed AST alongside the pipeline definition
- Evaluate AST on every PUSH event — no reparse overhead on hot path

## Version Compatibility

| Package | Compatible With | Notes |
|---------|-----------------|-------|
| axum 0.8.x | tokio 1.x, hyper 1.x | axum 0.8 requires hyper 1.x (breaking change from axum 0.7). Do not mix axum 0.7 with hyper 1. |
| tokio 1.51 | tracing 0.1, bytes 1.x, axum 0.8 | All tokio-rs ecosystem crates track tokio 1.x. No version mismatches expected. |
| serde 1.x | serde_json 1.x, postcard 1.x | serde_derive is re-exported from the serde crate. Use `features = ["derive"]` on serde, not a separate serde_derive dependency. |
| postcard 1.x | serde 1.x | postcard 1.x has stable wire format. Snapshots written by postcard 1.0 are readable by 1.x — safe for long-lived snapshot files. |
| criterion 0.7 | stable Rust 1.70+ | Does not require nightly. `html_reports` feature requires gnuplot or plotters but is optional. |
| winnow 0.6 | stable Rust | No nightly features required. |

## HyperLogLog Decision

**Recommendation: Implement HyperLogLog directly in `src/engine/hll.rs` (no external crate).**

Rationale:
- `hyperloglog-rs` (latest: 0.1.56, Dec 2023) requires **nightly Rust** for const generics features. This is a build-time risk for a production binary.
- `hyperloglog` and `hyperloglogplus` crates are minimally maintained.
- `streaming_algorithms` is described as "work in progress."
- The HyperLogLog algorithm is well-understood and straightforward: 14-bit precision (16,384 registers) produces ~0.8% error with ~12KB fixed memory per key. A custom implementation is ~100 lines of Rust, gives full control over the bucket structure and serialization format, and eliminates the nightly dependency.
- The CLAUDE.md spec already specifies the implementation approach: `HyperLogLog per window` with `14-bit precision` and `~12KB fixed` size. This maps directly to a `[u8; 16384]` register array.

**Implementation sketch (in `hll.rs`):**
```rust
pub struct HyperLogLog {
    registers: Box<[u8; 16384]>,  // 14-bit precision = 2^14 registers
}
// count_distinct(value): hash(value), update register at index hash[0..14], set register to max(register, leading_zeros(hash[14..]))
// estimate(): harmonic mean of registers with bias correction
// serialize/deserialize: registers array is flat — postcard handles it
```

## Sources

- [tokio docs.rs 1.51.0](https://docs.rs/crate/tokio/latest) — version confirmed
- [bincode RUSTSEC advisory](https://docs.rs/crate/bincode/latest) — unmaintained status confirmed
- [postcard crates.io](https://crates.io/crates/postcard) — v1.1.3, stable wire format
- [axum 0.8 announcement](https://tokio.rs/blog/2025-01-01-announcing-axum-0-8-0) — v0.8 release confirmed
- [serde_json docs.rs 1.0.149](https://docs.rs/crate/serde_json/latest) — version confirmed
- [thiserror crates.io](https://crates.io/crates/thiserror) — v2.0.17 confirmed
- [criterion 0.7 crates.io](https://crates.io/crates/criterion) — v0.7.0 confirmed (HIGH confidence)
- [hyperloglog-rs lib.rs](https://lib.rs/crates/hyperloglog-rs) — nightly requirement, last release Dec 2023 (MEDIUM confidence — warrants custom implementation)
- [Rust serialization benchmark](https://github.com/djkoloski/rust_serialization_benchmark) — postcard vs bincode vs rkyv (MEDIUM confidence)
- [Rust web frameworks 2026 comparison](https://aarambhdevhub.medium.com/rust-web-frameworks-in-2026-axum-vs-actix-web-vs-rocket-vs-warp-vs-salvo-which-one-should-you-2db3792c79a2) — axum dominance confirmed (MEDIUM confidence)

---
*Stack research for: Tally real-time feature server (Rust + Python SDK)*
*Researched: 2026-04-09*
