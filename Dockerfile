FROM node:22-alpine AS frontend
WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# cargo-chef splits the build into "compile dependencies" (cached until
# Cargo.toml/Cargo.lock change — this is the ~13min librdkafka+openssl cmake
# build) and "compile our crate" (~1-2min, reruns on source changes).
FROM rust:slim-bookworm AS chef
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake g++ make perl pkg-config libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install cargo-chef --locked
WORKDIR /build

# The planner layer reruns on any backend change, but its output (recipe.json)
# only encodes the dependency graph — so the expensive cook layer below stays
# cache-hit as long as the manifests are unchanged.
FROM chef AS planner
COPY backend/ backend/
RUN cd backend && cargo chef prepare --recipe-path recipe.json

FROM chef AS backend
COPY --from=planner /build/backend/recipe.json backend/recipe.json
RUN cd backend && cargo chef cook --release --recipe-path recipe.json
# The commit this image is built from, computed by the caller (CI or the
# justfile) because the build context excludes .git. After the cook on
# purpose: it changes every commit and must not invalidate the cached
# dependency build above.
ARG BUILD_VERSION=""
ENV ARNE_BUILD_VERSION=$BUILD_VERSION
COPY backend/ backend/
COPY --from=frontend /build/frontend/dist frontend/dist
RUN cd backend && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --system --uid 10001 arne
COPY --from=backend /build/backend/target/release/arne /usr/local/bin/arne
USER arne
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/arne"]
CMD ["/etc/arne/config.yaml"]
