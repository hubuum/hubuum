# syntax=docker/dockerfile:1
FROM docker.io/library/rust:1.97.0-alpine3.24@sha256:ec9c91e77119ce498cd1e87d96d77e0f75b2cee21655a29bc2bf75a51a2b20a4 AS builder

ARG CARGO_BUILD_FLAGS="-F tls-rustls -F tls-openssl --locked --release"

WORKDIR /usr/src/hubuum

# Rust's Alpine target produces static executables. pq-sys builds its bundled
# libpq source, and the static OpenSSL archives retain TLS support for both the
# server and embedded migration runner.
RUN apk add --no-cache build-base openssl-dev openssl-libs-static perl pkgconf

# Copy manifests first for dependency-layer caching. Keep this list in exact
# parity with the workspace members in Cargo.toml.
COPY Cargo.toml Cargo.lock ./
COPY crates/hubuum-auth-core/Cargo.toml ./crates/hubuum-auth-core/Cargo.toml
COPY crates/hubuum-auth-ldap/Cargo.toml ./crates/hubuum-auth-ldap/Cargo.toml
COPY crates/hubuum-event-sink-amqp/Cargo.toml ./crates/hubuum-event-sink-amqp/Cargo.toml
COPY crates/hubuum-event-sink-email/Cargo.toml ./crates/hubuum-event-sink-email/Cargo.toml
COPY crates/hubuum-event-sink-valkey/Cargo.toml ./crates/hubuum-event-sink-valkey/Cargo.toml
COPY crates/hubuum-event-sink-webhook/Cargo.toml ./crates/hubuum-event-sink-webhook/Cargo.toml
COPY crates/hubuum-event-sinks-common/Cargo.toml ./crates/hubuum-event-sinks-common/Cargo.toml
COPY crates/hubuum-events-core/Cargo.toml ./crates/hubuum-events-core/Cargo.toml
COPY crates/hubuum-outbound-http/Cargo.toml ./crates/hubuum-outbound-http/Cargo.toml
COPY crates/hubuum-query/Cargo.toml ./crates/hubuum-query/Cargo.toml
COPY crates/hubuum-templates/Cargo.toml ./crates/hubuum-templates/Cargo.toml

# Build dependencies against dummy targets. Benchmark targets are removed from
# the copied manifests because benchmark sources are not present in this layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/src/hubuum/target \
    find . -name Cargo.toml -exec sh -c ' \
    for manifest do \
        awk '\'' \
            /^\[\[bench\]\]$/ { skip = 1; next } \
            /^\[\[/ { skip = 0 } \
            /^\[/ && !/^\[\[/ { skip = 0 } \
            !skip { print } \
        '\'' "$manifest" > "$manifest.docker" && mv "$manifest.docker" "$manifest"; \
    done \
    ' sh {} + && \
    mkdir -p src/bin && \
    find crates -mindepth 2 -maxdepth 2 -name Cargo.toml \
        -exec sh -c 'mkdir -p "$(dirname "$1")/src" && echo "pub fn dummy() { }" > "$(dirname "$1")/src/lib.rs"' sh {} \; && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn dummy() {}" > src/lib.rs && \
    echo "fn main() {}" > src/bin/admin.rs && \
    echo "fn main() {}" > src/bin/openapi.rs && \
    cargo build ${CARGO_BUILD_FLAGS} --features embedded-migrations \
        --bin hubuum-server --bin hubuum-admin && \
    rm -rf src && \
    find crates -mindepth 2 -maxdepth 2 -type d -name src -exec rm -rf {} +

COPY . .

RUN find . -name Cargo.toml -exec sh -c ' \
    for manifest do \
        awk '\'' \
            /^\[\[bench\]\]$/ { skip = 1; next } \
            /^\[\[/ { skip = 0 } \
            /^\[/ && !/^\[\[/ { skip = 0 } \
            !skip { print } \
        '\'' "$manifest" > "$manifest.docker" && mv "$manifest.docker" "$manifest"; \
    done \
    ' sh {} +

ARG HUBUUM_BUILD_GIT_SHA="unknown"
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/src/hubuum/target \
    HUBUUM_BUILD_GIT_SHA="${HUBUUM_BUILD_GIT_SHA}" \
    find ./src ./crates -path '*/src/*' -type f -exec touch {} + && \
    cargo build ${CARGO_BUILD_FLAGS} --features embedded-migrations \
        --bin hubuum-server --bin hubuum-admin && \
    cp target/release/hubuum-server /tmp/ && \
    cp target/release/hubuum-admin /tmp/

RUN strip /tmp/hubuum-server /tmp/hubuum-admin

FROM scratch AS release-artifacts

COPY --from=builder /tmp/hubuum-server /hubuum-server
COPY --from=builder /tmp/hubuum-admin /hubuum-admin

FROM docker.io/library/alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

ARG HUBUUM_UID="10001"
ARG HUBUUM_GID="10001"

RUN apk add --no-cache ca-certificates && \
    addgroup -S -g "${HUBUUM_GID}" hubuum && \
    adduser -S -D -H -u "${HUBUUM_UID}" -G hubuum hubuum

COPY --from=builder /tmp/hubuum-server /usr/local/bin/hubuum-server
COPY --from=builder /tmp/hubuum-admin /usr/local/bin/hubuum-admin
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080

USER hubuum:hubuum

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD xargs -0 /entrypoint.sh --container-healthcheck < /proc/1/cmdline

ENTRYPOINT ["/entrypoint.sh"]
