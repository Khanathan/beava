# curl-ingest — zero-SDK HTTP demo

Exercises the full Beava HTTP surface using only curl and bash — no Python
required for ingest. Useful as a smoke test against a fresh container and as
a reference for the shape of each HTTP endpoint.

## What it demonstrates

| Step | Endpoint                                   | What it shows                                  |
| ---- | ------------------------------------------ | ---------------------------------------------- |
| 1    | Python stdlib `urllib` + `POST /pipelines` | Registering a stream via the HTTP admin API    |
| 2    | `POST /push/Transactions`                  | Single-event ingest with `_event_time`         |
| 3    | `POST /push-batch/Transactions?sync=1`     | Batch ingest with in-memory drain wait         |
| 4    | `POST /push/Transactions/ndjson`           | NDJSON streaming ingest (5 events)             |
| 5    | `GET /features/alice`                      | Read back all computed features for a key      |
| 6    | `GET /features/alice?table=Transactions`   | Filter features to a single table              |
| 7    | `GET /streams`                             | List all registered streams + watermarks       |
| 8    | `GET /streams/Transactions`                | Stream detail: name, watermark, feature schema |

All 8 steps pass with exit code 0 = full HTTP smoke pass.

## Prerequisites

- Docker (`beavadb/beava:latest`)
- `curl` (present on most systems)
- `python3` with only the stdlib (no extra packages needed)

## Run

```bash
# Start Beava
docker run -d --rm -p 6900:6900 -p 6400:6400 --name beava beavadb/beava:latest

# Run the smoke test
PORT=6900 bash examples/curl-ingest/run.sh

# Or let run.sh wait for the server (it polls /health)
PORT=6900 BEAVA_ADMIN_TOKEN=test-admin bash examples/curl-ingest/run.sh
```

The default port is `6401`. Set `PORT=6900` when using the Docker image
(which exposes HTTP on 6900).

Expected output:

```
== 0. Wait for server on :6900 ==
   server ready
== 1. Register Transactions stream via HTTP /pipelines ==
registered Transactions on localhost:6900: ...
   PASS
== 2. POST /push/Transactions (single event) ==
{"ok":true}
   PASS
...
============================================
  ALL GREEN (HTTP-08) — 8/8 steps passed
============================================
```

Expected run time: under 5 seconds on localhost.

## Cleanup

```bash
docker stop beava    # if you started it above
```

## Troubleshooting

**"server not ready after 30s"** — check that the container is running:
`docker ps | grep beava`. Try `curl http://localhost:6900/health` manually.

**"401 Unauthorized"** — export `BEAVA_ADMIN_TOKEN` matching your server token,
or connect from loopback (127.0.0.1) where the admin bypass applies.

**"python3: command not found"** — install Python 3 (stdlib only; no pip needed).

## See also

- [../session-features/](../session-features/) — simplest full pipeline: one
  stream, one table, last-N + count + sum. Start here to understand the data model.
- [../fraud-scoring/](../fraud-scoring/) — multi-stream pipeline with Python SDK
  and HTTP ingest at scale.
- [../../docs/http-api.md](../../docs/http-api.md) — full HTTP API reference.
