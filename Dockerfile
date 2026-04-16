# Stage 1: Build
FROM rust:1.83-slim AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first and build a dummy target to cache dependencies.
# This layer is only invalidated when Cargo.toml / Cargo.lock change,
# not when source code changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/beava target/release/deps/beava*

# Now copy the real source and rebuild. Only this layer rebuilds on
# source changes.
COPY src/ src/
RUN touch src/main.rs && cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for running Beava
RUN useradd -r -s /bin/false -u 1001 beava && \
    mkdir -p /data && chown beava:beava /data

WORKDIR /app

COPY --from=builder /build/target/release/beava .
RUN chown beava:beava /app/beava && chmod +x /app/beava

USER beava

EXPOSE 6400 6401

CMD ["./beava"]
