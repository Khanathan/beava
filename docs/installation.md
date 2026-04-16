# Installation

## From Source

Requires the Rust stable toolchain. Install it via [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Clone and build:

```bash
git clone https://github.com/petrpan26/beava.git
cd beava
cargo build --release
```

The binary is at `./target/release/beava`.

Start the server:

```bash
./target/release/beava
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
git clone https://github.com/petrpan26/beava.git
cd beava
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
python -c "import beava; print('SDK ready')"
```

## Configuration

Beava is configured through environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BEAVA_TCP_PORT` | `6400` | TCP protocol port |
| `BEAVA_HTTP_PORT` | `6401` | HTTP management API port |
| `BEAVA_WORKER_THREADS` | `4` | Tokio worker threads |
| `BEAVA_SNAPSHOT` | `true` | Enable periodic snapshots to disk |
| `BEAVA_SNAPSHOT_PATH` | `beava.snapshot` | Path prefix for snapshot files |
| `BEAVA_EVENT_LOG` | `true` | Enable append-only event log (WAL) |
| `BEAVA_DATA_DIR` | `.` | Base directory. Event log files are written to `{BEAVA_DATA_DIR}/events/`. |

Example:

```bash
BEAVA_TCP_PORT=7000 \
BEAVA_HTTP_PORT=7001 \
BEAVA_SNAPSHOT_PATH=/var/lib/beava/beava.snapshot \
BEAVA_DATA_DIR=/var/lib/beava \
./target/release/beava
```

For Docker, these are set in `docker-compose.yml` to point into the `/data` mount:

```yaml
environment:
  - BEAVA_SNAPSHOT_PATH=/data/beava.snapshot
  - BEAVA_DATA_DIR=/data
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
import beava as bv

app = bv.App("localhost:6400")
# If this returns without error, the connection is working
app.close()
```
