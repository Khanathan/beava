# Session features — keyed-stream example

The simplest working Beava pipeline. One `Click` stream, one `SessionFeatures`
table. Four aggregations: `clicks_5m`, `total_duration_5m`, `last_3_pages`,
`first_page`. TTL 30 minutes.

**Start here if you are new to Beava.** After this example, see
[../fraud-scoring/](../fraud-scoring/) for a multi-stream, multi-table pipeline.

## Prerequisites

- Docker (pulls `beavadb/beava:latest` automatically)
- Python 3.10+

## Run

```bash
bash run.sh
```

`run.sh` will:

1. Start a Beava container on ports 6900 (HTTP) and 6400 (TCP) if not already
   running.
2. Install the `requests` library.
3. Register the Click stream + SessionFeatures table.
4. Push 1000 synthetic click events via HTTP `/push-batch/Click`.
5. Print features for session `session-001`.

## What it demonstrates

- Single-stream, single-table pipeline (the minimal Beava unit).
- Windowed aggregations: `count(window="5m")`, `sum("duration_ms", window="5m")`.
- Ordinal (stateful list) aggregation: `last_n("page", n=3)` — the last 3 pages
  visited, updated on every event.
- Point-in-time aggregation: `first("page")` — the very first page in the session.
- TTL-based eviction: sessions idle for 30 minutes are removed from RAM.
- HTTP push via `/push-batch/Click`.
- HTTP feature read via `/features/{session_id}`.

## Expected output

```json
{
  "SessionFeatures": {
    "clicks_5m": 47,
    "total_duration_5m": 113420,
    "last_3_pages": ["/checkout", "/cart", "/product/42"],
    "first_page": "/home"
  }
}
```

Exact numbers vary because the synthetic events are randomly generated.

## Explore further

```bash
# Read features for any session
curl http://localhost:6900/features/session-005

# List registered streams
curl http://localhost:6900/streams

# See the pipeline files
cat pipeline.py      # stream + table definitions (~55 lines)
cat push.py          # HTTP ingest script (~40 lines)
```

## Pipeline structure

```
Click ──► SessionFeatures
         (key=session_id, ttl=30m)
         ├── clicks_5m          count over 5-minute window
         ├── total_duration_5m  sum(duration_ms) over 5-minute window
         ├── last_3_pages       last_n(page, n=3) ordinal
         └── first_page         first(page) ordinal
```

## Cleanup

```bash
docker stop beava-session
```
