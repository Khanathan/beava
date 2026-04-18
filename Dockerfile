# syntax=docker/dockerfile:1.7
#
# Multi-stage cargo-chef build → gcr.io/distroless/cc-debian12:nonroot runtime
#
# Decision locks (Phase 47, 47-CONTEXT.md):
#   D-01: base image = gcr.io/distroless/cc-debian12:nonroot (NOT Alpine — MUSL
#         allocator regresses push-batch 5-15%; see docs/PITFALLS.md Pitfall 14)
#   D-03: image tags: beavadb/beava:latest + beavadb/beava:0.1.0
#   D-04: auto-push deferred; see docs/docker-publish-runbook.md
#
# Build:  docker build -t beavadb/beava:latest .
# Run:    docker run -p 6900:6900 beavadb/beava:latest

# ─── Stage 1: chef base (cargo-chef installed on rust:bookworm) ───────────────
FROM rust:1.85-bookworm AS chef
WORKDIR /app
# Install cargo-chef for dependency-layer caching
RUN cargo install cargo-chef --locked

# ─── Stage 2: planner (compute dep recipe from manifests + source) ────────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ─── Stage 3: builder (cook deps once; rebuild only on source changes) ─────────
FROM chef AS builder
# Copy the recipe and pre-build all dependencies (cached layer)
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Copy the real source tree and compile the final binary
COPY . .
# Binary name is `beava` (Cargo.toml [[bin]] name = "beava"; rename deferred to v1.1)
# The `server` feature is required to compile the binary (required-features = ["server"])
RUN cargo build --release --bin beava

# ─── Stage 4: runtime (distroless/cc-debian12:nonroot; per D-01) ──────────────
# distroless/cc-debian12 provides glibc + libgcc, matching the bookworm builder.
# The `nonroot` variant runs as uid 65532 (nonroot) — no shell, no package manager.
FROM gcr.io/distroless/cc-debian12:nonroot

# /data is the event-log + snapshot directory; mount a volume here in production.
WORKDIR /data

# Copy the compiled binary from the builder stage
COPY --from=builder /app/target/release/beava /usr/local/bin/beava

# HTTP (push + read) and TCP (Python SDK / binary protocol) ports
EXPOSE 6900 6400

# Persist event log and snapshots to /data
VOLUME ["/data"]

# Environment defaults — override with `docker run -e KEY=value` or compose.
# BEAVA_ADMIN_TOKEN is intentionally NOT set here — callers must supply it
# explicitly for any protected endpoint (safer default per README configuration table).
ENV BEAVA_HTTP_PORT="6900" \
    BEAVA_TCP_PORT="6400" \
    BEAVA_DATA_DIR="/data" \
    BEAVA_TCP_BIND="0.0.0.0" \
    BEAVA_SNAPSHOT="true" \
    BEAVA_EVENT_LOG="true"

# distroless has no shell; use exec form to avoid /bin/sh dependency.
ENTRYPOINT ["/usr/local/bin/beava"]
