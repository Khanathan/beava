# Getting started with Beava

**Goal:** take you from zero to a live feature read in under 60 seconds on a
fresh machine. You need Docker and `curl` — nothing else.

## Prerequisites

- Docker 20+ (or `podman` aliased to `docker`)
- `curl`
- One free port: **6900** (HTTP)

## Step 1 — Run Beava (5 seconds)

```bash
docker run -d --rm -p 6900:6900 --name beava beavadb/beava:latest
```

Verify it is alive:

```bash
curl http://localhost:6900/health
# → {"status":"ok"}
```

## Step 2 — Push an event (10 seconds)

Beava auto-registers a stream the first time you push to it. No schema
declaration needed for the zero-SDK path.

```bash
curl -X POST http://localhost:6900/push/clicks \
  -H 'Content-Type: application/json' \
  -d "{\"user\":\"alice\",\"page\":\"/home\",\"_event_time\":$(date +%s)000}"
# → {"ok":true}
```

`_event_time` is milliseconds since epoch. Omit it and Beava falls back to
wall-clock receipt time. See [docs/event-time.md](event-time.md) for why
event-time bucketing matters.

## Step 3 — Read the feature (5 seconds)

```bash
curl "http://localhost:6900/features/alice?table=clicks"
# → {"ok":true,"data":{"key":"alice","tables":{"clicks":{...}}}}
```

You have ingested an event and read it back. That is the 60-second path.

## Step 4 — Python SDK (optional, ~30 seconds more)

The Python SDK wires named pipelines with windowed aggregations:

```bash
pip install beava
```

```python
import beava as bv

@bv.stream
class Click:
    user: str
    page: str

@bv.table(key="user")
def UserClicks(c: Click) -> bv.Table:
    return c.group_by("user").agg(
        count_10m=bv.count(window="10m"),
        last_page=bv.last(c.page),
    )

app = bv.App("localhost:6400")   # TCP port for SDK
app.register(Click, UserClicks)
app.push(Click, {"user": "alice", "page": "/home"})
app.flush()

features = app.get("alice")
print(features.count_10m)   # → 1
print(features.last_page)   # → "/home"
```

Full SDK reference: [docs/python-sdk.md](python-sdk.md).

## What just happened

1. **Write path:** Beava appended the event to the per-stream WAL before
   acknowledging it (TCP path: fsync before ack; HTTP path: batched-fsync group
   commit — see [Operations § Durability](operations.md#durability)).
2. **Event-time bucketing:** the `_event_time` value — not the server wall-clock
   — determined which sliding-window bucket received the event. See
   [docs/event-time.md](event-time.md).
3. **Operator update:** registered operators (count, last, etc.) applied to the
   event in one pipeline pass. The feature was available for read immediately
   after the push drained the in-memory queue.

## Persistent data (no data loss on restart)

The `docker run --rm` command above discards data when the container stops. For
a persistent dev setup, use
[`examples/docker-compose.yml`](../examples/docker-compose.yml), which mounts a
named Docker volume at `/data`:

```bash
cd examples
docker compose up -d
```

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `Failed to connect to localhost port 6900` | Docker is not running or the port is taken. Check `docker ps` and `lsof -i :6900`. |
| `{"ok":false,"error":{"code":"stream_not_registered",...}}` | Stream name in the path must match the one used on registration. Check `GET /streams`. |
| `{"ok":true,"data":{"tables":{}}}` on `/features` | No events indexed for that key yet, or the key field name differs. |
| Push returns 413 | Body exceeds the 2 MiB default limit. Raise via `BEAVA_HTTP_MAX_BODY`. |

## Stop and clean up

```bash
docker stop beava   # container was --rm, so it self-destructs on stop
```

## Next steps

- [Concepts](concepts.md) — what streams, tables, operators, fork, and
  watermarks are.
- [HTTP API](http-api.md) — full reference for all endpoints, auth, body limits,
  and observability.
- [Operations](operations.md) — sizing, durability, crash-recovery, and tuning
  before you deploy for real.
- [Event time](event-time.md) — the correctness semantics you will want to
  understand before running in production.
- [Examples](../examples/) — working projects: fraud-scoring, session-features,
  curl-ingest.
