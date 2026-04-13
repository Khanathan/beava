# Installation

## From Source

Requires the Rust stable toolchain. Install it via [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Clone and build:

```bash
git clone https://github.com/petrpan26/tally.git
cd tally
cargo build --release
```

The binary is at `./target/release/tally`.

Start the server:

```bash
./target/release/tally
```

## Docker

```bash
# Coming soon
```

## Python SDK

Requires Python 3.10+.

Install from the repository (editable mode):

```bash
cd tally/python
pip install -e .
```

The SDK has no external dependencies -- it uses only the Python standard library.

## Configuration

Tally is configured through environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `TALLY_TCP_PORT` | `6400` | TCP protocol port |
| `TALLY_HTTP_PORT` | `6401` | HTTP management API port |
| `TALLY_WORKER_THREADS` | `4` | Tokio worker threads |
| `TALLY_SNAPSHOT` | `true` | Enable periodic snapshots to disk |
| `TALLY_EVENT_LOG` | `true` | Enable SSD event log |

Example:

```bash
TALLY_TCP_PORT=7000 TALLY_HTTP_PORT=7001 ./target/release/tally
```

## Verifying the Installation

Once the server is running, check the health endpoint:

```bash
curl http://localhost:6401/health
```

A healthy server responds with HTTP 200. You can also check registered pipelines:

```bash
curl http://localhost:6401/pipelines
```

To verify the Python SDK can connect:

```python
import tally as tl

app = tl.App("localhost:6400")
# If this returns without error, the connection is working
```
