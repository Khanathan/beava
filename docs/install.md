# Install

> Pick one. They're the same binary.

## pip (recommended)

```bash
pip install beava
```

The Python SDK ships with the server binary embedded. `bv.App()` discovers and runs it on an ephemeral port. This is what you want for development, tests, and most production deployments.

## Docker

```bash
docker run -p 8080:8080 ghcr.io/beava-dev/beava:latest
```

Push and query against `:8080`. Mount a volume at `/data` to persist the WAL and snapshots.

## Homebrew (macOS, Linux)

```bash
brew install beava-dev/tap/beava
beava -c beava.yaml      # binds 127.0.0.1:8080 by default (admin :8090)
```

The binary takes a single YAML config (default `./beava.yaml`); ports come
from `listen_addr` / `admin_addr` keys, or env vars
`BEAVA_LISTEN_ADDR` / `BEAVA_ADMIN_ADDR`. There is no `serve` subcommand.

## Verify

```bash
$ beava --version
beava 0.0.0
```

Or from Python:

```python
import beava as bv
print(bv.__version__)
```

## What's next

[Quickstart](/docs/get-started/quickstart/) — first feature in 60 seconds.
