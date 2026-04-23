---
phase: "01"
plan: "01-05"
subsystem: foundation
tags: [rust, workspace, axum, tokio, clap, tracing, testing]
dependency_graph:
  requires: []
  provides: [beava-binary, http-server, config-loading, json-logging, test-harness]
  affects: [all-future-phases]
tech_stack:
  added:
    - axum 0.7 (HTTP router)
    - tokio 1 current_thread runtime
    - clap 4 derive (CLI)
    - serde_yaml 0.9 (config parsing)
    - tracing + tracing-subscriber 0.3 json (structured logging)
    - once_cell (idempotent subscriber init)
    - anyhow + thiserror (error handling)
    - reqwest 0.12 rustls-tls (test HTTP client)
    - libc (SIGTERM in subprocess tests)
  patterns:
    - workspace with shared [workspace.dependencies]
    - lib+bin split for testability (beava-server has both)
    - feature-gated test harness (testing feature)
    - Arc<AtomicBool> readiness flag (swap-in point for Phase 5)
    - OS-allocated port 0 for test isolation
key_files:
  created:
    - Cargo.toml (workspace root)
    - rust-toolchain.toml
    - beava.example.yaml
    - crates/beava-core/Cargo.toml
    - crates/beava-core/src/lib.rs
    - crates/beava-core/src/config.rs
    - crates/beava-server/Cargo.toml
    - crates/beava-server/src/main.rs
    - crates/beava-server/src/lib.rs
    - crates/beava-server/src/cli.rs
    - crates/beava-server/src/logging.rs
    - crates/beava-server/src/http.rs
    - crates/beava-server/src/server.rs
    - crates/beava-server/src/shutdown.rs
    - crates/beava-server/src/testing.rs
    - crates/beava-server/src/bin/log_probe.rs
    - crates/beava-server/tests/cli_smoke.rs
    - crates/beava-server/tests/logging_smoke.rs
    - crates/beava-server/tests/foundation_smoke.rs
  modified:
    - .gitignore (added /target, !/Cargo.lock)
decisions:
  - "Mutex-based EnvGuard in config tests to serialize env-var mutation across concurrent test threads"
  - "Manual Debug impl for Server (TcpListener does not derive Debug)"
  - "cli_smoke tests updated to spawn+SIGTERM pattern because main.rs now starts a real HTTP server"
  - "foundation_smoke uses required-features=[testing] in [[test]] rather than relying on cfg(test) propagation to integration tests"
  - "libc dev-dep added for SIGTERM in subprocess CLI smoke tests"
metrics:
  duration: "~45 minutes"
  completed: "2026-04-22"
  tasks: 15
  files: 19
---

# Phase 1: Foundation Summary

**One-liner:** Cargo workspace + axum HTTP server + clap CLI + YAML config + JSON logging + TestServer harness; 36 tests all green, 1.6MB stripped release binary.

## What Shipped

### Binary
- `target/release/beava` — 1.6MB stripped, built with `lto=thin`, `codegen-units=1`, `strip=symbols`, `panic=abort`
- Starts in <200ms, binds configured HTTP port, emits JSON startup/bind log lines, exits cleanly on SIGTERM/SIGINT within 2s

### Crates
- `beava-core` — library crate with `Config` struct, `load_config`, `ConfigError`
- `beava-server` — binary+lib crate with CLI, logging, HTTP router, Server, TestServer harness

### Key Files Created (19 files)

```
Cargo.toml                                  workspace + shared deps + release profile
rust-toolchain.toml                         stable toolchain pin
beava.example.yaml                          reference config for users
crates/beava-core/src/config.rs             Config struct, load_config, BEAVA_* env overrides
crates/beava-server/src/cli.rs              Cli clap derive struct (--config flag)
crates/beava-server/src/logging.rs          logging::init() JSON subscriber (idempotent)
crates/beava-server/src/http.rs             axum Router: /health + /ready + ReadinessFlag
crates/beava-server/src/server.rs           Server::bind + Server::serve + graceful shutdown
crates/beava-server/src/shutdown.rs         shutdown_signal() SIGTERM+SIGINT future
crates/beava-server/src/testing.rs          TestServer + TestServerBuilder harness
crates/beava-server/src/bin/log_probe.rs    dev helper binary for JSON log format tests
crates/beava-server/tests/cli_smoke.rs      5 CLI subprocess integration tests
crates/beava-server/tests/logging_smoke.rs  2 JSON log format subprocess tests
crates/beava-server/tests/foundation_smoke.rs  Phase 1 acceptance gate (2 tests)
```

## How to Run

```bash
# Copy example config
cp beava.example.yaml beava.yaml

# Start the server
./target/release/beava --config ./beava.yaml

# In another terminal:
curl http://127.0.0.1:8080/health   # -> {"status":"ok"}
curl http://127.0.0.1:8080/ready    # -> {"status":"ready"}

# Graceful shutdown:
kill -TERM <pid>
```

## Test Evidence

```
cargo test --workspace
  beava-core lib:   8 passed (version_is_non_empty + 7 config tests)
  beava-server lib: 21 passed (banner + 3 cli + 3 logging + 5 http + 4 server + 5 testing)
  cli_smoke:        5 passed
  logging_smoke:    2 passed

cargo test -p beava-server --features testing --test foundation_smoke
  phase1_acceptance_lifecycle:       ok
  phase1_acceptance_ready_starts_at_503: ok

Total: 36 tests, 0 failures
```

```
cargo build --release  -> Finished in 33s (cold)
ls -lh target/release/beava  -> 1.6M
cargo fmt --check  -> clean
cargo clippy --workspace --all-targets -- -D warnings  -> 0 errors, 0 warnings
```

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Config tests race on env vars**
- **Found during:** Plan 02 Task 1
- **Issue:** `cargo test` runs tests concurrently; env vars are process-global, causing `env_var_overrides_*` tests to flake
- **Fix:** Added `static ENV_MUTEX: Mutex<()>` and `EnvGuard` holds the lock for its lifetime, serializing all env-mutating tests
- **Files modified:** `crates/beava-core/src/config.rs`

**2. [Rule 1 - Bug] Server struct lacks Debug, blocking unwrap_err() in tests**
- **Found during:** Plan 04, clippy check
- **Issue:** `TcpListener` does not implement `Debug` so `#[derive(Debug)]` is blocked; `unwrap_err()` requires `T: Debug`
- **Fix:** Manual `impl std::fmt::Debug for Server` using `finish_non_exhaustive()`
- **Files modified:** `crates/beava-server/src/server.rs`

**3. [Rule 1 - Bug] cli_smoke tests hang — binary now starts HTTP server**
- **Found during:** Plan 05, after wiring main.rs to run a real server
- **Issue:** Plan 02 cli_smoke tests used `Command::output()` (waits for exit) but the binary now blocks serving HTTP
- **Fix:** Updated tests to use `Command::spawn()` + brief sleep + SIGTERM + `wait_with_output()`; added `libc` dev-dep
- **Files modified:** `crates/beava-server/tests/cli_smoke.rs`

**4. [Rule 1 - Bug] integration tests cannot see testing module via cfg(test)**
- **Found during:** Plan 05, clippy check
- **Issue:** `cfg(test)` in the library crate is NOT propagated to integration tests in `tests/` (separate compilation unit); `use beava_server::testing::TestServer` failed to resolve
- **Fix:** Added `[[test]] name = "foundation_smoke" required-features = ["testing"]` to Cargo.toml; run with `cargo test --features testing`
- **Files modified:** `crates/beava-server/Cargo.toml`

## Known Stubs

- `Server::bind` 100ms readiness delay (`tokio::time::sleep(Duration::from_millis(100))`) — intentional Phase 1 stub. Phase 5 replaces with real WAL-replay completion signal via `ReadinessFlag::set_ready()`.

## Threat Flags

None — Phase 1 creates no auth paths, no user data handling, no network endpoints beyond the planned `/health` and `/ready` stubs. The HTTP listener is created but serves only static JSON responses.

## Handoff Notes for Phase 2

### Where to attach `/register`

```rust
// In crates/beava-server/src/http.rs, update router():
pub fn router(readiness: ReadinessFlag) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .merge(phase2_router())          // <-- Phase 2 adds this
        .with_state(readiness)
}
```

### Where Config grows

`crates/beava-core/src/config.rs` — add fields to `Config` struct and update `beava.example.yaml`. Remember to add `BEAVA_*` env overrides in `apply_env_overrides()` and validation in `validate()`.

### Where the operator trait lands

`crates/beava-core/src/` — Phase 2 creates `operator.rs` alongside `config.rs`.

### ReadinessFlag swap-in point (Phase 5)

`crates/beava-server/src/server.rs`, `Server::bind()`:
```rust
// Replace this:
tokio::spawn(async move {
    tokio::time::sleep(Duration::from_millis(100)).await;
    flag_clone.set_ready();
});
// With: pass readiness into recovery subsystem, call set_ready() after WAL replay
```

### TestServer harness usage (all future phase tests)

```toml
# In crates/beava-server/Cargo.toml [dev-dependencies]:
beava-server = { path = ".", features = ["testing"] }
```

```rust
use beava_server::testing::TestServer;

#[tokio::test]
async fn my_test() {
    let ts = TestServer::spawn().await.expect("spawn");
    // ts.base_url() -> "http://127.0.0.1:XXXXX"
    ts.shutdown().await.expect("shutdown");
}
```

## Self-Check: PASSED

| Check | Result |
|-------|--------|
| `target/release/beava` exists | FOUND |
| commit b100e51 (01-01) | FOUND |
| commit 96534e8 (01-02) | FOUND |
| commit 13f2202 (01-03) | FOUND |
| commit 7258126 (01-04) | FOUND |
| commit 66bbbc7 (01-05) | FOUND |
| `cargo test --workspace` | 36 passed, 0 failed |
| `cargo test --features testing --test foundation_smoke` | 2 passed, 0 failed |
| `cargo fmt --check` | clean |
| `cargo clippy --workspace --all-targets -- -D warnings` | 0 errors, 0 warnings |
