ARG TARGETPLATFORM
ARG TARGETARCH
FROM rust:1-bookworm AS builder
ARG TARGETARCH
WORKDIR /app
ENV CARGO_TARGET_DIR=/app/target-${TARGETARCH}
COPY Cargo.toml rust-toolchain.toml ./
COPY apps/loader/Cargo.toml apps/loader/Cargo.toml
COPY apps/query/Cargo.toml apps/query/Cargo.toml
COPY apps/server/Cargo.toml apps/server/Cargo.toml
COPY apps/sidecar/Cargo.toml apps/sidecar/Cargo.toml
COPY crates/processor-runtime/Cargo.toml crates/processor-runtime/Cargo.toml
COPY tools/loadtest/Cargo.toml tools/loadtest/Cargo.toml
COPY apps/loader/src apps/loader/src
COPY apps/query/src apps/query/src
COPY apps/server/src apps/server/src
COPY apps/sidecar/src apps/sidecar/src
COPY crates/processor-runtime/src crates/processor-runtime/src
COPY tools/loadtest/src tools/loadtest/src
RUN cargo build --release -p nanotrace-client

FROM debian:bookworm-slim
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target-${TARGETARCH}/release/nanotrace-client /usr/local/bin/nanotrace-client
ENV NANOTRACE_CLIENT_BIND=0.0.0.0:4319
EXPOSE 4319/udp
CMD ["/usr/local/bin/nanotrace-client"]
