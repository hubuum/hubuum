FROM rust:bookworm as builder

WORKDIR /usr/src/hubuum

# Install diesel CLI
RUN cargo install diesel_cli --no-default-features --features postgres

# Copy the project files
COPY . .

# Build the application
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y libpq5 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/hubuum/target/release/hubuum /usr/local/bin/hubuum
COPY --from=builder /usr/local/cargo/bin/diesel /usr/local/bin/diesel
COPY --from=builder /usr/src/hubuum/migrations /migrations

# Copy a start script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]