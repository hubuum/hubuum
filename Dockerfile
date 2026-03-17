# syntax=docker/dockerfile:1
FROM rust:slim-trixie AS builder

ARG CARGO_BUILD_FLAGS="--locked --release"

WORKDIR /usr/src/hubuum

# Install system dependencies and cargo-binstall in one layer
RUN apt-get update && \
    apt-get install -y libpq-dev libpq5 libssl3 libssl-dev curl && \
    rm -rf /var/lib/apt/lists/* && \
    curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

# Install diesel CLI using binstall (much faster)
RUN cargo binstall --no-confirm diesel_cli

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY migrations ./migrations

# Build dependencies only (creates dummy project)
# Use cache mounts to persist cargo registry/git between builds
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    mkdir -p src/bin && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn dummy() {}" > src/lib.rs && \
    echo "fn main() {}" > src/bin/admin.rs && \
    echo "fn main() {}" > src/bin/openapi.rs && \
    cargo build ${CARGO_BUILD_FLAGS} --bin hubuum-server --bin hubuum-admin && \
    rm -rf src

# Copy the actual source code
COPY . .

# Build the real application (dependencies are cached)
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/src/hubuum/target \
    cargo build ${CARGO_BUILD_FLAGS} --bin hubuum-server --bin hubuum-admin && \
    cp target/release/hubuum-server /tmp/ && \
    cp target/release/hubuum-admin /tmp/

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y libpq5 libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/hubuum-server /usr/local/bin/hubuum-server
COPY --from=builder /tmp/hubuum-admin /usr/local/bin/hubuum-admin
COPY --from=builder /usr/local/cargo/bin/diesel /usr/local/bin/diesel
COPY --from=builder /usr/src/hubuum/migrations /migrations

# Copy a start script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
