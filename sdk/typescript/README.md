# `@beava/sdk` — Beava real-time feature server (TypeScript SDK)

> Communicate-only client for [Beava](https://github.com/beava-dev/beava). Push events, read features. Pipeline authoring is Python-only.

## Install

```bash
npm install @beava/sdk
```

Requires Node.js >= 18 (LTS). ESM-only — `package.json "type": "module"`.

## Scope (Phase 13.6, 2026-05-03)

The TypeScript SDK is a **wire-thin client**:

- `app.register(descriptors)` — accepts pre-compiled JSON descriptors (authored via the Python SDK or hand-written)
- `app.push(name, event)` — fire-and-forget push
- `app.pushSync(name, event)` — durable push (delegates to `push` in v0; OP_PUSH_SYNC reserved for v0.1+)
- `app.get(table, key)` / `app.batchGet([...])` — feature read
- `app.reset()` — gated on `test_mode` per Phase 13.4 D-03
- `app.ping()` / `app.close()` — lifecycle

**No pipeline DSL** lives here: there is no `bv.event` / `bv.col` / `bv.count` / etc. Descriptors are opaque `Record<string, unknown>` JSON blobs. Use the Python SDK to author pipelines and pass the compiled JSON over to TypeScript runtimes.

## Docs

See [`docs/sdk-api/typescript.md`](https://github.com/beava-dev/beava/blob/main/docs/sdk-api/typescript.md) for the full method surface and wire-spec mapping.
