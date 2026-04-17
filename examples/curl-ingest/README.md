# curl-ingest example

Demonstrates all six Phase 45 HTTP endpoints against a running Beava server
using only `curl` and the Python SDK. Exit code 0 = full end-to-end smoke pass.

## What this demonstrates

| Step | Endpoint                              | What it shows                                  |
| ---- | ------------------------------------- | ---------------------------------------------- |
| 1    | Python SDK `register_remote`          | Registering a stream + table over HTTP         |
| 2    | `POST /push/Transactions`             | Single-event ingest with `_event_time`         |
| 3    | `POST /push-batch/Transactions?sync=1`| Batch ingest with in-memory drain wait         |
| 4    | `POST /push/Transactions/ndjson`      | NDJSON streaming ingest (5 events, one per line)|
| 5    | `GET /features/alice`                 | Read back all computed features for a key      |
| 6    | `GET /features/alice?table=Transactions` | Filter features to a single table           |
| 7    | `GET /streams`                        | List all registered streams + watermarks       |
| 8    | `GET /streams/Transactions`           | Stream detail: name, watermark, feature schema |

## Prerequisites

1. **Beava server running** on `localhost:6401` (or set `PORT` env var):

   ```bash
   cargo build --release
   ./target/release/beava serve
   ```

   When Docker ships in Phase 47, you can use:

   ```bash
   docker run -p 6401:6401 beava/beava:latest
   ```

2. **Python SDK installed**:

   ```bash
   pip install -e python/   # from repo root
   ```

3. **`curl` and `jq`** available on PATH (most systems have `curl`; `jq` is
   optional — assertions use `grep`).

4. **`BEAVA_ADMIN_TOKEN`** exported if your server requires a token:

   ```bash
   export BEAVA_ADMIN_TOKEN=your-token-here
   ```

   Requests from `127.0.0.1` are automatically authenticated (loopback bypass),
   so you can omit the token when running locally against `localhost`.

## Running

```bash
# From repo root
bash examples/curl-ingest/run.sh

# With a custom port and token
PORT=7001 BEAVA_ADMIN_TOKEN=secret bash examples/curl-ingest/run.sh
```

Expected output (truncated):

```
== 0. Wait for server on :6401 ==
   server ready
== 1. Register pipeline (requires Python SDK installed) ==
registered Transactions + txn_summary on localhost:6401
== 2. POST /push/Transactions (single event) ==
{"ok":true}
   PASS
== 3. POST /push-batch/Transactions?sync=1 (3-event batch) ==
{"ok":true,"data":{"accepted":3,"rejected":0,"first_error":null}}
   PASS
...
============================================
  ALL GREEN (HTTP-08) — 8/8 steps passed
============================================
```

Expected run time: **under 5 seconds** on localhost.

## Sample pipeline

`sample-pipeline.py` registers:
- `Transactions` stream with `user: str` and `amount: float` fields
- `txn_summary` table that computes `count` and `total` grouped by `user`

After running, features for key `alice` will look like:

```json
{
  "ok": true,
  "data": {
    "key": "alice",
    "tables": {
      "Transactions": {
        "txn_summary.count": 5,
        "txn_summary.total": 24.5
      }
    }
  }
}
```

## Troubleshooting

**"server not ready after 30s"** — check that `beava serve` is running on the
correct port. Use `curl http://localhost:6401/health` to test manually.

**"python3: No module named tally"** — the Python SDK is not installed.
Run `pip install -e python/` from the repo root.

**401 responses** — the server requires a token. Export `BEAVA_ADMIN_TOKEN`
matching the value the server was started with, or run on loopback (127.0.0.1).
