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

### System dependencies

On Debian/Ubuntu you may need:

```bash
sudo apt install build-essential pkg-config libssl-dev
```

On macOS:

```bash
brew install openssl@3
```

## Docker

A Dockerfile and `docker-compose.yml` ship with the repo. No Rust toolchain required.

```bash
git clone https://github.com/petrpan26/tally.git
cd tally
docker compose up -d
```

The compose file mounts `./data` for snapshot and event log persistence. State survives container restarts.

To stop:

```bash
docker compose down
```

A published Docker image on Docker Hub is on the roadmap.

## Python SDK

Requires Python 3.10+.

From the repo root (after cloning):

```bash
cd python
pip install -e .
```

The SDK has no external dependencies beyond the Python standard library.

Verify:

```bash
python -c "import tally; print('SDK ready')"
```

## Configuration

Tally is configured through environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `TALLY_TCP_PORT` | `6400` | TCP protocol port |
| `TALLY_HTTP_PORT` | `6401` | HTTP management API port |
| `TALLY_WORKER_THREADS` | `4` | Tokio worker threads |
| `TALLY_SNAPSHOT` | `true` | Enable periodic snapshots to disk |
| `TALLY_SNAPSHOT_PATH` | `tally.snapshot` | Path prefix for snapshot files |
| `TALLY_EVENT_LOG` | `true` | Enable append-only event log (WAL) |
| `TALLY_DATA_DIR` | `.` | Base directory for event log files (`./events/` by default) |

Example:

```bash
TALLY_TCP_PORT=7000 \
TALLY_HTTP_PORT=7001 \
TALLY_SNAPSHOT_PATH=/var/lib/tally/tally.snapshot \
TALLY_DATA_DIR=/var/lib/tally \
./target/release/tally
```

For Docker, these are set in `docker-compose.yml` to point into the `/data` mount:

```yaml
environment:
  - TALLY_SNAPSHOT_PATH=/data/tally.snapshot
  - TALLY_DATA_DIR=/data
```

## Verifying the Installation

Once the server is running, check the health endpoint:

```bash
curl http://localhost:6401/health
```

Expected output:

```json
{"status":"ok"}
```

List registered pipelines (should be empty on first start):

```bash
curl http://localhost:6401/pipelines
```

Verify the Python SDK can connect:

```python
import tally as tl

app = tl.App("localhost:6400")
# If this returns without error, the connection is working
app.close()
```
