# syntax=docker/dockerfile:1.7
ARG TARGETPLATFORM=linux/arm64
ARG TARGETARCH=arm64
FROM --platform=$TARGETPLATFORM rust:1-bookworm AS builder
ARG TARGETARCH
WORKDIR /app
ENV CARGO_TARGET_DIR=/app/target-${TARGETARCH}
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY apps/loader/Cargo.toml apps/loader/Cargo.toml
COPY apps/query/Cargo.toml apps/query/Cargo.toml
COPY apps/server/Cargo.toml apps/server/Cargo.toml
COPY apps/sidecar/Cargo.toml apps/sidecar/Cargo.toml
COPY crates/auth/Cargo.toml crates/auth/Cargo.toml
COPY crates/lakehouse/Cargo.toml crates/lakehouse/Cargo.toml
COPY crates/processor-runtime/Cargo.toml crates/processor-runtime/Cargo.toml
COPY tools/lakehouse-rebuild/Cargo.toml tools/lakehouse-rebuild/Cargo.toml
COPY tools/loadtest/Cargo.toml tools/loadtest/Cargo.toml
COPY apps/loader/src apps/loader/src
COPY apps/query/src apps/query/src
COPY apps/server/src apps/server/src
COPY apps/sidecar/src apps/sidecar/src
COPY crates/auth/src crates/auth/src
COPY crates/lakehouse/src crates/lakehouse/src
COPY crates/processor-runtime/src crates/processor-runtime/src
COPY tools/lakehouse-rebuild/src tools/lakehouse-rebuild/src
COPY tools/loadtest/src tools/loadtest/src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target-${TARGETARCH} \
    cargo build --release -p nanotrace-server -p nanotrace-loader -p nanotrace-query -p nanotrace-lakehouse-rebuild \
    && mkdir -p /app/build-output \
    && cp "${CARGO_TARGET_DIR}/release/nanotrace-server" /app/build-output/nanotrace-server \
    && cp "${CARGO_TARGET_DIR}/release/nanotrace-loader" /app/build-output/nanotrace-loader \
    && cp "${CARGO_TARGET_DIR}/release/nanotrace-query" /app/build-output/nanotrace-query \
    && cp "${CARGO_TARGET_DIR}/release/nanotrace-lakehouse-rebuild" /app/build-output/nanotrace-lakehouse-rebuild

FROM --platform=$TARGETPLATFORM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates python3 python3-pip \
    && pip3 install --break-system-packages boto3 modal \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/build-output/nanotrace-server /usr/local/bin/nanotrace-server
COPY --from=builder /app/build-output/nanotrace-loader /usr/local/bin/nanotrace-loader
COPY --from=builder /app/build-output/nanotrace-query /usr/local/bin/nanotrace-query
COPY --from=builder /app/build-output/nanotrace-lakehouse-rebuild /usr/local/bin/nanotrace-lakehouse-rebuild
COPY scripts/modal_processor_builder.py /usr/local/bin/modal_processor_builder.py
ENV PORT=18473
EXPOSE 18473
CMD ["/usr/local/bin/nanotrace-server"]
