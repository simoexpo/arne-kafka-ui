# Betrachtung

A fast, trustworthy Kafka UI for developers. Read-only v1: topic monitoring
and message lookup, built to report what is actually in the
cluster, and to say when it does not know.

## Prerequisites

- Rust (stable toolchain)
- Node 20.19+
- Docker
- [`just`](https://github.com/casey/just) (optional — command runner; raw
  commands are listed below for anyone without it)

## Development

```
cp config.example.yaml config.yaml   # then point it at your cluster(s)
```

With `just`:

```
just dev              # backend on :8080 + frontend on :5173 (proxies /api)
just test             # backend + frontend test suites
just lint             # clippy + frontend build
just docker           # build the production image (betrachtung:dev)
just playground       # docker-compose Kafka + schema registry + demo producer
just playground-down  # tear the playground down
just smoke            # run the end-to-end smoke check
```

Without `just`, the equivalent raw commands:

```
# dev
(cd backend && cargo run -- ../config.yaml) &
(cd frontend && npm run dev) &
wait

# test
cd backend && cargo test
cd frontend && npx vitest run

# lint
cd backend && cargo clippy --all-targets -- -D warnings
cd frontend && npm run build

# docker
docker build -t betrachtung:dev .

# playground
docker compose -f docker-compose.dev.yml up -d
docker compose -f docker-compose.dev.yml down -v   # to tear down

# smoke
node scripts/smoke.mjs
```

The playground brings up a local Kafka broker, schema registry, and a demo
producer that continuously writes JSON messages to a `demo-orders` topic —
useful for pointing a locally-running Betrachtung at real data without a
real cluster. Point `config.yaml` at `localhost:9092` (see
`config.example.yaml`) to use it.

The smoke check expects `just dev` (or the raw backend+vite equivalent)
already running and drives it with Playwright via `channel: 'chrome'` —
Google Chrome must be installed locally (no browser binary is downloaded
by `npm install`).

## Deployment

Betrachtung ships as a single Docker image with the frontend embedded in
the backend binary. Mount your config and run — assuming your config is
at `./config.yaml`:

```
docker run -p 8080:8080 -v $(pwd)/config.yaml:/etc/betrachtung/config.yaml betrachtung:dev
```
