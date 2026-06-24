# syntax=docker/dockerfile:1.7
#
# kokedb MySQL-protocol server.
#
# pyo3 (python-udf) is built with the `auto-initialize` feature, so it links
# libpython dynamically — builder and runtime both use the python:3.12 image
# family so the linked libpython3.12 matches at run time.

# ── builder ────────────────────────────────────────────────────────────────
FROM python:3.12-bookworm AS builder

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

# Rust toolchain (the python:3.12-bookworm base already provides curl,
# build-essential, pkg-config and git via buildpack-deps).
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal

# Extra native deps for ring / *-sys crates.
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake clang perl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Build just the server binary. BuildKit cache mounts keep the cargo registry
# and target dir warm across rebuilds; the binary is copied out of the cached
# target within the same layer (cache mounts are not persisted in the image).
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release -p kokedb-server && \
    cp /app/target/release/kokedb-server /usr/local/bin/kokedb-server

# ── runtime ──────────────────────────────────────────────────────────────────
FROM python:3.12-slim-bookworm AS runtime

# libpython3.12.so.1.0 lives in /usr/local/lib in this image.
ENV LD_LIBRARY_PATH=/usr/local/lib \
    PG_META_DSN=postgresql://postgres:123456@postgres:5432/kokedb \
    RESULT_CACHE_DIR=/data/cache \
    RUST_LOG=info

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /data/cache

COPY --from=builder /usr/local/bin/kokedb-server /usr/local/bin/kokedb-server

# 3306 = MySQL wire, 5433 = PostgreSQL wire, 9090 = Prometheus metrics.
EXPOSE 3306 5433 9090
ENTRYPOINT ["/usr/local/bin/kokedb-server"]
