# Quick Start

Get Beava running and push your first event in under 5 minutes.

## Prerequisites

Pick one:
- **Docker** (fastest path) -- install [Docker Desktop](https://docs.docker.com/get-docker/)
- **Rust toolchain** (stable) -- install via [rustup](https://rustup.rs/)

Plus:
- **Python 3.10+** with pip

## 1. Start the Server

### Option A: Docker

```bash
git clone https://github.com/petrpan26/beava.git
cd beava
docker compose up -d
```

### Option B: From source

```bash
git clone https://github.com/petrpan26/beava.git
cd beava
cargo build --release
./target/release/beava &
```

Either path starts Beava on TCP port 6400 (protocol) and HTTP port 6401 (management).

Verify it is running:

```bash
curl http://localhost:6401/health
```

Expected output:

```json
{"status":"ok"}
```

## 2. Install the Python SDK

From the repo root (`cd beava` if you're not already there):

```bash
cd python
pip install -e .
cd ..
```

Verify:

```bash
python -c "import beava; print('SDK ready')"
```

## 3. Define a Pipeline

Create a file called `demo.py` at the repo root:

```python
import beava as bv

# Declare an event stream
@bv.stream
class Transactions:
    user_id: str
    amount: float
    merchant_id: str

# Define a keyed table with features
@bv.table(key="user_id")
def UserFeatures(txs: Transactions) -> bv.Table:
    return txs.group_by("user_id").agg(
        tx_count_1h=bv.count(window="1h"),
        tx_sum_1h=bv.sum("amount", window="1h"),
        avg_amount_24h=bv.avg("amount", window="24h"),
        unique_merchants=bv.count_distinct("merchant_id", window="24h"),
    )

# Connect and register
app = bv.App("localhost:6400")
app.register(Transactions, UserFeatures)

# Push events (fire-and-forget, fast)
app.push(Transactions, {"user_id": "u123", "amount": 50.0, "merchant_id": "m456"})
app.push(Transactions, {"user_id": "u123", "amount": 120.0, "merchant_id": "m789"})
app.push(Transactions, {"user_id": "u123", "amount": 25.0, "merchant_id": "m456"})
app.flush()

# Read computed results (instant, from in-memory state)
features = app.get("u123")
print(f"tx_count_1h:      {features.tx_count_1h}")
print(f"tx_sum_1h:        {features.tx_sum_1h}")
print(f"avg_amount_24h:   {features.avg_amount_24h}")
print(f"unique_merchants: {features.unique_merchants}")
```

## 4. Run It

```bash
python demo.py
```

Expected output:

```
tx_count_1h:      3
tx_sum_1h:        195.0
avg_amount_24h:   65.0
unique_merchants: 2
```

Push more events and watch the counts and sums update.

## 5. Inspect What's Happening

Beava ships with a management API for debugging:

```bash
# Memory usage breakdown
curl http://localhost:6401/debug/memory | python -m json.tool

# All features for a specific entity
curl http://localhost:6401/debug/key/u123 | python -m json.tool

# Pipeline topology
curl http://localhost:6401/debug/topology | python -m json.tool
```

## Troubleshooting

### Server won't start: "address already in use"

Something is using port 6400 or 6401. Either stop it or change Beava's port:

```bash
BEAVA_TCP_PORT=7400 BEAVA_HTTP_PORT=7401 ./target/release/beava
```

### `cargo build` fails

On Linux, you may need build essentials and OpenSSL dev headers:

```bash
# Debian/Ubuntu
sudo apt install build-essential pkg-config libssl-dev

# macOS
brew install openssl@3
```

### `ConnectionError` when running Python

The server isn't running, or it's on a different port. Check:

```bash
curl http://localhost:6401/health
```

If no response, start the server. If it's on a different port, pass it explicitly:

```python
app = bv.App("localhost:7400")  # match BEAVA_TCP_PORT
```

### `pip install -e .` fails

Make sure you're in the `python/` subdirectory, not the repo root:

```bash
pwd              # should end in .../beava/python
ls pyproject.toml   # should exist
```

## Next Steps

- **[Python SDK Guide](python-sdk.md)** -- Full API reference: sources, datasets, cascades, projections, and validation.
- **[Operators Reference](operators.md)** -- All 16 built-in operators with parameters, window behavior, and examples.
- **[Architecture](architecture.md)** -- How Beava works under the hood.
- **`/beava` Claude Code skill** -- Type `/beava` in [Claude Code](https://claude.ai/claude-code) for a guided setup with pipeline generation, realistic test data, and capacity planning.
