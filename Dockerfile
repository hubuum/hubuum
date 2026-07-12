# syntax=docker/dockerfile:1
FROM docker.io/library/rust:1.96.0-slim-trixie@sha256:c37af730be4fd8104cbf9aedbd6ab259e51ca2d5437817a0f8680edf66ac6c28 AS builder

ARG CARGO_BUILD_FLAGS="--locked --release"
ARG CARGO_BINSTALL_VERSION="1.20.1"
ARG CARGO_BINSTALL_SHA256_AMD64="f12954bc382e1d0b2df3fbfb217a05d92c25570e4517841e0613499a24f4594e"
ARG CARGO_BINSTALL_SHA256_ARM64="23679581c4cfa1782953264a6e36965198aed995b3a5287550dd78a113ce2288"
ARG DIESEL_CLI_VERSION="2.3.11"
ARG TARGETARCH

WORKDIR /usr/src/hubuum

# Install build dependencies and a checksum-verified cargo-binstall release.
RUN apt-get update && \
    apt-get install -y --no-install-recommends pkg-config libpq-dev libpq5 libssl3 libssl-dev curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    case "${TARGETARCH}" in \
        amd64) binstall_target="x86_64-unknown-linux-musl"; binstall_sha256="${CARGO_BINSTALL_SHA256_AMD64}" ;; \
        arm64) binstall_target="aarch64-unknown-linux-musl"; binstall_sha256="${CARGO_BINSTALL_SHA256_ARM64}" ;; \
        *) echo "Unsupported build architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac && \
    curl -L --proto '=https' --tlsv1.2 -sSf \
        "https://github.com/cargo-bins/cargo-binstall/releases/download/v${CARGO_BINSTALL_VERSION}/cargo-binstall-${binstall_target}.tgz" \
        -o /tmp/cargo-binstall.tgz && \
    echo "${binstall_sha256}  /tmp/cargo-binstall.tgz" | sha256sum --check --strict && \
    tar -xzf /tmp/cargo-binstall.tgz -C /usr/local/cargo/bin cargo-binstall && \
    rm /tmp/cargo-binstall.tgz

# Install an explicit Diesel CLI release using binstall.
RUN cargo binstall --no-confirm --disable-telemetry "diesel_cli@${DIESEL_CLI_VERSION}"

# Copy manifests first for better layer caching. Workspace member manifests are
# required for Cargo to load the workspace during the dependency-only build.
#
# NOTE: workspace members are listed explicitly (not auto-detected) so that only
# crate manifests, not their sources, enter the dependency-cache layer. This
# keeps that layer valid across crate source edits. When you add a crate under
# crates/, add a COPY for its Cargo.toml here; the dependency-only build below
# creates and cleans up dummy source files for every copied crate manifest.
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
    mkdir -p src/bin && \
    find crates -mindepth 2 -maxdepth 2 -name Cargo.toml \
        -exec sh -c 'mkdir -p "$(dirname "$1")/src" && echo "pub fn dummy() { }" > "$(dirname "$1")/src/lib.rs"' sh {} \; && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn dummy() {}" > src/lib.rs && \
    echo "fn main() {}" > src/bin/admin.rs && \
    echo "fn main() {}" > src/bin/openapi.rs && \
    cargo build ${CARGO_BUILD_FLAGS} --bin hubuum-server --bin hubuum-admin && \
    rm -rf src && \
    find crates -mindepth 2 -maxdepth 2 -type d -name src -exec rm -rf {} +

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
ARG HUBUUM_BUILD_GIT_SHA="unknown"
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/usr/src/hubuum/target \
    HUBUUM_BUILD_GIT_SHA="${HUBUUM_BUILD_GIT_SHA}" \
    cargo build ${CARGO_BUILD_FLAGS} --bin hubuum-server --bin hubuum-admin && \
    cp target/release/hubuum-server /tmp/ && \
    cp target/release/hubuum-admin /tmp/

FROM docker.io/library/debian:trixie-slim@sha256:28de0877c2189802884ccd20f15ee41c203573bd87bb6b883f5f46362d24c5c2

ARG HUBUUM_UID="10001"
ARG HUBUUM_GID="10001"

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl libpq5 libssl3 && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid "${HUBUUM_GID}" hubuum && \
    useradd --uid "${HUBUUM_UID}" --gid hubuum --no-create-home \
        --home-dir /nonexistent --shell /usr/sbin/nologin hubuum

COPY --from=builder /tmp/hubuum-server /usr/local/bin/hubuum-server
COPY --from=builder /tmp/hubuum-admin /usr/local/bin/hubuum-admin
COPY --from=builder /usr/local/cargo/bin/diesel /usr/local/bin/diesel
COPY --from=builder /usr/src/hubuum/migrations /migrations

# Copy a start script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080

USER hubuum:hubuum

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl --fail --silent --show-error "http://127.0.0.1:${HUBUUM_BIND_PORT:-8080}/healthz" || exit 1

ENTRYPOINT ["/entrypoint.sh"]
