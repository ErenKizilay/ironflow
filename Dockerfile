FROM rust:slim as builder
WORKDIR /usr/src/myapp

# Install dependencies required for OpenSSL
RUN apt-get update && apt-get install -y \
    pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo install --path .

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libssl3 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/cargo/bin/ironflow /usr/local/bin/ironflow
CMD ["ironflow"]