# Install

> Pick one. They're the same binary.

## pip (recommended)

```bash
pip install tally
```

> **Pre-release naming.** The PyPI package is currently published as `tally`
> (the project's repo codename). The `beava` package name is reserved for the
> v0.0.0 GA cut. Once GA ships, `pip install beava` becomes the canonical
> command. Until then, install `tally` and import as `import beava as bv`
> (the import name is already `beava`).

The Python SDK ships with the server binary embedded. `bv.App()` discovers and runs it on an ephemeral port. This is what you want for development, tests, and most production deployments.

## Docker

```bash
docker run -p 6400:6400 ghcr.io/beava-dev/beava:latest
```

Push and query against `:6400`. Mount a volume at `/data` to persist the WAL and snapshots.

## Homebrew (macOS, Linux)

```bash
brew install beava-dev/tap/beava
beava serve --port 6400
```

## Static binary

Download the prebuilt binary for your platform from the [releases page](https://github.com/beava-dev/beava/releases) — about 14 MB, no runtime dependencies.

```bash
curl -L https://github.com/beava-dev/beava/releases/latest/download/beava-$(uname -s)-$(uname -m) -o beava
chmod +x beava
./beava serve --port 6400
```

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
