# `beava` Go SDK — `github.com/beava-dev/beava/sdk/go`

> Communicate-only client for [Beava](https://github.com/beava-dev/beava). Push events, read features. Pipeline authoring is Python-only.

## Install

```bash
go get github.com/beava-dev/beava/sdk/go
```

Requires Go >= 1.22.

## Scope (Phase 13.6, 2026-05-03)

The Go SDK is a **wire-thin client**:

- `app.Register(ctx, descriptors, opts...)` — accepts pre-compiled JSON descriptors (authored via the Python SDK or hand-written)
- `app.Push(ctx, name, event)` — fire-and-forget push
- `app.PushSync(ctx, name, event)` — durable push (delegates to `Push` in v0; OP_PUSH_SYNC reserved for v0.1+)
- `app.Get(ctx, table, key)` per-entity / `app.GetGlobal(ctx, table)` global form per ADR-003
- `app.BatchGet(ctx, requests)`
- `app.Reset(ctx)` — gated on `test_mode` per Phase 13.4 D-03
- `app.Ping(ctx)` / `app.Close()`

**No pipeline DSL** lives here: there are no `events.go` / `col.go` / `agg.go` / `table.go` files. Descriptors are opaque `map[string]any` JSON blobs. Use the Python SDK to author pipelines and pass the compiled JSON over to Go runtimes.

## Docs

See [`docs/sdk-api/go.md`](https://github.com/beava-dev/beava/blob/main/docs/sdk-api/go.md) for the full method surface and wire-spec mapping.
