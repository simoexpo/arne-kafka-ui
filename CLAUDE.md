# Arne — Kafka UI

Fast, reliable Kafka UI for developers. Read-only v1: topic monitoring +
message lookup. Design spec: `docs/superpowers/specs/2026-08-09-arne-kafka-ui-design.md`.

## Stack

- Backend: Rust — axum, tokio, rust-rdkafka. Lives in `backend/`.
- Frontend: React 19 + TypeScript + Vite, TanStack Router/Query, Tailwind. Lives in `frontend/`.
- Single repo, single Docker image; frontend dist embedded in the binary via rust-embed.
- No database. State is in Kafka or in memory only.

## Non-negotiable working rules

- **Always TDD.** Test first, watch it fail, then implement. No exceptions, ever.
- **Git:** commands allowed, but **NEVER push**.
- **Commit messages:** short and clear, one line. No Co-Authored-By, no
  session links, no generated-with trailers, no long bodies.

## Product principles

- Speed and trustworthy data above all — the point of Arne is a Kafka UI whose
  numbers you can act on, and which says so when it does not know.
- Never show stale data silently: every metric carries its sample timestamp.
- Errors render in the failing panel; one broken cluster never blanks the page.
- Search must show real progress (known total up front), stream results, and
  be cancellable. No zombie scans.
- Decode failures are shown, never silently skipped.

## Dev commands

(justfile to be created during implementation: `just dev`, `just test`, `just docker`)
