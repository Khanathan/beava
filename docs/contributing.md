# Contributing to Beava

Welcome! Beava is a real-time feature server: push events in, get features out, one Rust binary, zero infrastructure. See the [README](https://github.com/petrpan26/beava) for an overview.

## Development Setup

**Prerequisites:**

- Rust stable toolchain (install via [rustup](https://rustup.rs/))
- Python 3.10+
- pip

**Build and run:**

```bash
git clone https://github.com/petrpan26/beava.git
cd beava
cargo build
```

**Install the Python SDK (editable):**

```bash
cd python && pip install -e .
```

**Start the server (debug build):**

```bash
./target/debug/beava
# or
cargo run
```

The server listens on TCP port 6400 (protocol) and HTTP port 6401 (management) by default.

## Running Tests

**Rust tests:**

```bash
cargo test -- --test-threads=1
```

`--test-threads=1` is required because integration tests bind to fixed ports and will fail with port contention under parallel execution. CI uses this flag.

**Python tests:**

```bash
cd python && python -m pytest tests/ -q
```

Python integration tests build and start the Beava binary automatically, so run `cargo build` first.

**Linting and formatting:**

```bash
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

CI enforces all of the above. A PR that fails any check will not be merged.

## Project Structure

```
src/
  engine/       Pipeline engine, operators, expressions, HLL
  server/       TCP protocol, HTTP API, debug UI
  state/        In-memory store, snapshots, event log, eviction

python/
  beava/        Python SDK (client, dataset API, operators, protocol)
  tests/        Python SDK and integration tests

tests/          Rust integration tests
benchmark/      Performance benchmarks (fraud pipeline, throughput)
```

## Code Style

- **Rust:** Follow `rustfmt` defaults. Keep clippy clean (`-D warnings`).
- **Python:** Standard Python conventions.
- No unnecessary AI-generated comments or docstrings.
- Write tests for all new features.

## Pull Request Process

1. Fork the repo and create a feature branch.
2. Implement your changes with tests.
3. Ensure CI passes locally: `cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test -- --test-threads=1`
4. Open a PR against `main`.
5. Describe **what** changed and **why** in the PR description.

## Reporting Issues

Use [GitHub Issues](https://github.com/petrpan26/beava/issues).

- **Bug reports:** Include steps to reproduce, expected behavior, and actual behavior.
- **Feature requests:** Describe the use case and why it matters.

## License

Contributions are licensed under Apache 2.0.
