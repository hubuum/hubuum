# syntax=docker/dockerfile:1
FROM rust:slim-trixie AS builder

ARG CARGO_BUILD_FLAGS="--locked --release"

WORKDIR /usr/src/hubuum

# Install system dependencies and cargo-binstall in one layer
RUN apt-get update && \
    apt-get install -y pkg-config libpq-dev libpq5 libssl3 libssl-dev curl && \
    rm -rf /var/lib/apt/lists/* && \
    curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

# Install diesel CLI using binstall (much faster)
RUN cargo binstall --no-confirm diesel_cli

# Copy manifests first for better layer caching. Workspace member manifests are
# required for Cargo to load the workspace during the dependency-only build.
#
# NOTE: workspace members are listed explicitly (not auto-detected) so that only
# crate *manifests* — not their sources — enter the dependency-cache layer,
# keeping that layer valid across crate source edits. When you add a crate under
# crates/, update THREE places below: (1) add a COPY for its Cargo.toml here,
# (2) add a dummy src/lib.rs in the dependency-only build, and (3) add its src
# dir to that step's `rm -rf` cleanup.
COPY Cargo.toml Cargo.lock ./
COPY crates/hubuum-outbound-http/Cargo.toml ./crates/hubuum-outbound-http/Cargo.toml
COPY crates/hubuum-templates/Cargo.toml ./crates/hubuum-templates/Cargo.toml
COPY migrations ./migrations

# The production image only builds binaries. Strip benchmark targets so Cargo
# does not require benchmark target files in the Docker build context. Keep
# [dev-dependencies] intact so Cargo.lock stays in sync and --locked succeeds.
RUN awk ' \
    /^\[\[bench\]\]$/ { skip = 1; next } \
    /^\[\[/ { skip = 0 } \
    /^\[/ && !/^\[\[/ { skip = 0 } \
    !skip { print } \
    ' Cargo.toml > Cargo.toml.docker && mv Cargo.toml.docker Cargo.toml

# Build dependencies only (creates dummy project)
# Use cache mounts to persist cargo registry/git between builds
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    mkdir -p src/bin crates/hubuum-outbound-http/src crates/hubuum-templates/src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn dummy() {}" > src/lib.rs && \
    echo "fn main() {}" > src/bin/admin.rs && \
    echo "fn main() {}" > src/bin/openapi.rs && \
    echo "pub fn dummy() {}" > crates/hubuum-outbound-http/src/lib.rs && \
    echo "pub fn dummy() {}" > crates/hubuum-templates/src/lib.rs && \
    cargo build ${CARGO_BUILD_FLAGS} --bin hubuum-server --bin hubuum-admin && \
    rm -rf src crates/hubuum-outbound-http/src crates/hubuum-templates/src

# Copy the actual source code
COPY . .

# COPY restores the repository manifest; prune benchmark targets again before
# the real production build so benchmark-only targets stay out of the image
# build, while keeping [dev-dependencies] so --locked stays satisfied.
RUN awk ' \
    /^\[\[bench\]\]$/ { skip = 1; next } \
    /^\[\[/ { skip = 0 } \
    /^\[/ && !/^\[\[/ { skip = 0 } \
    !skip { print } \
    ' Cargo.toml > Cargo.toml.docker && mv Cargo.toml.docker Cargo.toml

# Build the real application (dependencies are cached)
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/src/hubuum/target \
    cargo build ${CARGO_BUILD_FLAGS} --bin hubuum-server --bin hubuum-admin && \
    cp target/release/hubuum-server /tmp/ && \
    cp target/release/hubuum-admin /tmp/

FROM debian:trixie-slim

RUN apt-get update && apt-get install -y libpq5 libssl3 postgresql-client && rm -rf /var/lib/apt/lists/*

COPY --from=builder /tmp/hubuum-server /usr/local/bin/hubuum-server
COPY --from=builder /tmp/hubuum-admin /usr/local/bin/hubuum-admin
COPY --from=builder /usr/local/cargo/bin/diesel /usr/local/bin/diesel
COPY --from=builder /usr/src/hubuum/migrations /migrations

# Copy a start script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
