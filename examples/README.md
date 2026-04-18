# Beava examples

Three working example projects, each runnable against a fresh
`docker run -p 6900:6900 -p 6400:6400 beavadb/beava:latest`.

## [`curl-ingest/`](curl-ingest/)

Zero-SDK HTTP demo. Just curl + bash (and Python stdlib — no pip). Exercises
all 8 HTTP endpoint shapes: register, push single, push batch, NDJSON, read
features, filter by table, list streams, stream detail.

**Start here if you want to understand the HTTP API before writing any code.**

```bash
PORT=6900 bash examples/curl-ingest/run.sh
```

## [`session-features/`](session-features/)

The simplest working pipeline — one `Click` stream, one `SessionFeatures`
table, last-N-click features + count + sum aggregations, 30-minute TTL.

**Start here if you are new to Beava and want to understand the data model.**

```bash
bash examples/session-features/run.sh
```

## [`fraud-scoring/`](fraud-scoring/)

3 streams (Transaction, Device, Login), 2 tables (UserFraudScore,
UserLoginPattern), 10k synthetic events pushed via HTTP `/push-batch`.
HTTP-first variant of the published `benchmark/fraud-pipeline/`.

**Start here if you are evaluating Beava for a real use case.**

```bash
bash examples/fraud-scoring/run.sh
```

---

## Recommended order for new users

1. **[curl-ingest/](curl-ingest/)** — understand the HTTP API shape (no SDK, no pipeline)
2. **[session-features/](session-features/)** — understand streams + tables + aggregations
3. **[fraud-scoring/](fraud-scoring/)** — see a realistic multi-stream, multi-table pipeline

All three examples self-start Beava via Docker if it is not already running.
Each has its own `README.md` with expected output and troubleshooting steps.

If you prefer the TCP-first Python-SDK pattern, see
[`benchmark/fraud-pipeline/`](../benchmark/fraud-pipeline/).
