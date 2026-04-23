# Beava

Real-time feature server for ML. Single binary, event-time semantics, HTTP + CDC fork.

[![CI](https://github.com/petrpan26/beava/actions/workflows/ci.yml/badge.svg)](https://github.com/petrpan26/beava/actions/workflows/ci.yml)
[![Docker Pulls](https://img.shields.io/docker/pulls/beavadb/beava.svg)](https://hub.docker.com/r/beavadb/beava)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

---

## 60-second quickstart

```bash
docker run -p 6900:6900 beavadb/beava:latest
curl -X POST http://localhost:6900/push/clicks -d '{"user":"alice","page":"/home"}'
curl http://localhost:6900/features/alice
```

Beava ingested the event, bucketed by event-time, and served the feature vector keyed by
`alice`. No broker, no ETL, one binary.

Full walkthrough: [docs/getting-started.md](docs/getting-started.md).

## Iterate features against live prod — fork

```bash
tally fork --remote prod.example.com --streams transactions --pipeline-file pipeline.py
```

A scoped replica of live prod state. Define a new feature, see the value it produces from
real production history, close the context — production never sees your reads.

Details: [docs/architecture.md](docs/architecture.md) § Fork model.

## Why Beava

Replaces Postgres triggers + Redis counters + the cron job that heals drift. Same pipeline
from laptop to production. 315K EPS single-binary TCP push; 100K+ EPS over HTTP. See
[benchmark/README.md](benchmark/README.md) for reproducible numbers and
[docs/comparison.md](docs/comparison.md) for honest tradeoffs vs Flink / Feast.

## TCP wire protocol

Beava binds two listeners by default:

- **HTTP/JSON on `127.0.0.1:7379`** — debugging-friendly, curl-compatible. See [docs/http-api.md](docs/http-api.md).
- **Binary-framed TCP on `127.0.0.1:7380`** — lower-overhead fast path. Same JSON bodies, wrapped in a length-prefixed frame.

### Frame format

```text
[u32 length BE][u16 op BE][u8 content_type][payload: length - 3 bytes]
```

`length` counts the bytes after itself (i.e., `op + content_type + payload`). Minimum valid length is 3. Multi-byte integers are big-endian.

### Opcode table (v0)

| Opcode   | Name             | Status                 |
|----------|------------------|------------------------|
| `0x0000` | `ping`           | implemented            |
| `0x0001` | `register`       | implemented            |
| `0x0010` | `push`           | reserved (Phase 6)     |
| `0x0011` | `push_sync`      | reserved (Phase 12)    |
| `0x0012` | `push_many`      | reserved (Phase 12)    |
| `0x0013` | `push_table`     | reserved (Phase 12)    |
| `0x0014` | `delete_table`   | reserved (Phase 12)    |
| `0x0020` | `get`            | reserved (Phase 12)    |
| `0x0021` | `mget`           | reserved (Phase 12)    |
| `0x0022` | `get_multi`      | reserved (Phase 12)    |
| `0x0030` | `set`            | reserved (Phase 12)    |
| `0x0031` | `mset`           | reserved (Phase 12)    |
| `0xFFFF` | `error_response` | implemented            |

Reserved opcodes return `error_response` with code `op_not_implemented`. Unknown opcodes return code `unknown_op`. In both cases the connection stays open.

### Content-type bytes

| Byte   | Name        | Status                      |
|--------|-------------|-----------------------------|
| `0x01` | JSON        | v0 implementation           |
| `0x02` | MessagePack | reserved (Phase 6 / 12)     |

Frames with unknown content-types return `unsupported_content_type`; the connection stays open.

### Connection model

- Strict FIFO per connection (Redis RESP style) — client sends N frames, server reads/dispatches/writes one at a time in order. No `request_id` field; order correlates requests to responses.
- Oversized frames (default max `4 MiB`) return `frame_too_large` and the connection closes.
- No TLS in v0 — terminate at nginx / Envoy / Cloudflare if needed.

### Config keys

| YAML key              | Env var                       | Default           |
|-----------------------|-------------------------------|-------------------|
| `tcp.enabled`         | `BEAVA_TCP_ENABLED`           | `true`            |
| `tcp.host`            | `BEAVA_TCP_HOST`              | `127.0.0.1`       |
| `tcp.port`            | `BEAVA_TCP_PORT`              | `7380`            |
| `tcp.max_frame_bytes` | `BEAVA_TCP_MAX_FRAME_BYTES`   | `4194304` (4 MiB) |

Disable with `tcp.enabled: false` or `BEAVA_TCP_ENABLED=0`.

## Learn more

- [Getting started](docs/getting-started.md) — 60-second path on a fresh machine
- [Concepts](docs/concepts.md) — streams, tables, operators, fork, event-time
- [HTTP API](docs/http-api.md) — curl examples for all endpoints
- [Architecture](docs/architecture.md) — single-node design, fork model, scaling posture
- [Operations](docs/operations.md) — sizing, durability, crash recovery
- [Event time](docs/event-time.md) — bucketing, watermarks, TTL semantics
- [Comparison](docs/comparison.md) — vs Feast, Flink+Redis, Redpanda
- [Examples](examples/) — fraud-scoring, session-features, curl-ingest

[Apache 2.0](LICENSE) · [CHANGELOG](CHANGELOG.md) · [GOVERNANCE](GOVERNANCE.md) · [SECURITY](SECURITY.md)
