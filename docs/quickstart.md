# Quick Start

Get Tally running and push your first event in under 5 minutes.

## Prerequisites

- **Rust toolchain** (stable) -- install via [rustup](https://rustup.rs/), OR
- **Docker** (coming soon)
- **Python 3.10+** with pip

## 1. Start the Server

### From source

```bash
git clone https://github.com/petrpan26/tally.git
cd tally
cargo build --release
./target/release/tally
```

The server starts on TCP port 6400 (protocol) and HTTP port 6401 (management).

Verify it is running:

```bash
curl http://localhost:6401/health
```

## 2. Install the Python SDK

In a separate terminal:

```bash
cd tally/python
pip install -e .
```

## 3. Define a Pipeline

Create a file called `demo.py`:

```python
import tally as tl

# Declare an event source
@tl.source
class Transactions:
    pass

# Define a dataset with features
@tl.dataset(depends_on=[Transactions])
class UserFeatures:
    features = tl.group_by("user_id").agg(
        tx_count_1h=tl.count(window="1h"),
        tx_sum_1h=tl.sum("amount", window="1h"),
        avg_amount_24h=tl.avg("amount", window="24h"),
    )

# Connect and register
app = tl.App("localhost:6400")
app.register(Transactions, UserFeatures)

# Push an event -- get updated features in the response
features = app.push(Transactions, {
    "user_id": "u123",
    "amount": 50.0,
    "merchant_id": "m456",
})

print(f"tx_count_1h:    {features.tx_count_1h}")
print(f"tx_sum_1h:      {features.tx_sum_1h}")
print(f"avg_amount_24h: {features.avg_amount_24h}")
```

## 4. Run It

```bash
python demo.py
```

You should see the computed feature values printed. Push more events and watch the counts and sums update in real time.

## 5. Read Features Later

You can also read the current feature values for any entity key without pushing a new event:

```python
all_features = app.get("u123")
print(all_features)
```

## Next Steps

- **[Python SDK Guide](python-sdk.md)** -- Full API reference: sources, datasets, cascades, views, projections, and validation.
- **[Operators Reference](operators.md)** -- All 16 built-in operators with parameters, window behavior, and examples.
- **[Architecture](architecture.md)** -- How Tally works under the hood.
