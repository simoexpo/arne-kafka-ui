FROM node:22-alpine AS frontend
WORKDIR /build/frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM rust:slim-bookworm AS backend
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake g++ make perl pkg-config libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY backend/ backend/
COPY --from=frontend /build/frontend/dist frontend/dist
RUN cd backend && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --system --uid 10001 betrachtung
COPY --from=backend /build/backend/target/release/betrachtung /usr/local/bin/betrachtung
USER betrachtung
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/betrachtung"]
CMD ["/etc/betrachtung/config.yaml"]
