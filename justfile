# Betrachtung dev commands

default: test

# backend API on :8080 (uses config.yaml) + vite on :5173 proxying /api
dev:
    #!/usr/bin/env bash
    trap 'kill 0' EXIT
    (cd backend && cargo run -- ../config.yaml) &
    (cd frontend && npm run dev) &
    wait

test:
    cd backend && cargo test
    cd frontend && npx vitest run

lint:
    cd backend && cargo clippy --all-targets -- -D warnings
    cd frontend && npm run lint && npx tsc -b

docker:
    docker build -t betrachtung:dev .

# one-command playground: kafka + schema registry + demo producer
playground:
    docker compose -f docker-compose.dev.yml up -d

playground-down:
    docker compose -f docker-compose.dev.yml down -v

smoke:
    node scripts/smoke.mjs
