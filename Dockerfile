ARG TARGETPLATFORM=linux/arm64
ARG TARGETARCH=arm64
FROM --platform=$TARGETPLATFORM rust:1-bookworm AS builder
ARG TARGETARCH
WORKDIR /app
ENV CARGO_TARGET_DIR=/app/target-${TARGETARCH}
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml rust-toolchain.toml ./
COPY apps/loader/Cargo.toml apps/loader/Cargo.toml
COPY apps/query/Cargo.toml apps/query/Cargo.toml
COPY apps/server/Cargo.toml apps/server/Cargo.toml
COPY apps/sidecar/Cargo.toml apps/sidecar/Cargo.toml
COPY crates/auth/Cargo.toml crates/auth/Cargo.toml
COPY crates/processor-runtime/Cargo.toml crates/processor-runtime/Cargo.toml
COPY tools/loadtest/Cargo.toml tools/loadtest/Cargo.toml
COPY apps/loader/src apps/loader/src
COPY apps/query/src apps/query/src
COPY apps/server/src apps/server/src
COPY apps/sidecar/src apps/sidecar/src
COPY crates/auth/src crates/auth/src
COPY crates/processor-runtime/src crates/processor-runtime/src
COPY tools/loadtest/src tools/loadtest/src
RUN cargo build --release -p nanotrace-server -p nanotrace-loader -p nanotrace-query

FROM --platform=$TARGETPLATFORM debian:bookworm-slim
ARG TARGETARCH
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates python3 python3-pip \
    && pip3 install --break-system-packages boto3 modal \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target-${TARGETARCH}/release/nanotrace-server /usr/local/bin/nanotrace-server
COPY --from=builder /app/target-${TARGETARCH}/release/nanotrace-loader /usr/local/bin/nanotrace-loader
COPY --from=builder /app/target-${TARGETARCH}/release/nanotrace-query /usr/local/bin/nanotrace-query
COPY scripts/modal_processor_builder.py /usr/local/bin/modal_processor_builder.py
ENV PORT=18473
EXPOSE 18473
CMD ["/usr/local/bin/nanotrace-server"]
