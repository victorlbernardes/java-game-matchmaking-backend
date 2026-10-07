# 0004 — Why PostgreSQL (W5)

## Status
Accepted (2026-10-06)

## Context
After W3/W4 the ticket and pending-match state is hot and disposable: Redis with
TTL holds it, and it is allowed to disappear. But once a match is fully confirmed,
the fact "this match happened, with these players, at this time" must survive
Redis flushes and redeploys — it is the historical record queried by
`GET /v1/players/{playerId}/matches`.

## Options
1. **Keep everything in Redis** — fast, but TTL eviction and flushall discard history.
2. **JSON files / embedded store** — no concurrent writers, no real indexes, no cursor pagination.
3. **PostgreSQL** — real transactions, indexes, cursor pagination, the classic system of record.

## Decision
PostgreSQL is the source of truth for durable facts: `matches` and `match_players`.
The engine persists a `Match` (status `PENDING`) plus its `match_players` rows the
moment every player has confirmed; Redis keeps only hot, disposable state.

## How it works
- New module `common/common-db`: Flyway migrations (`V1__create_matches.sql`),
  `DatabaseConfig` (env-based), `MatchRepository` (async via vertx-jdbc-client).
- `matchmaking-engine` runs Flyway at boot (idempotent, advisory-locked) and
  persists `Match` + `MatchPlayer` rows on full confirmation.
- `matchmaking-api` runs the same Flyway check at boot and serves
  `GET /v1/players/{playerId}/matches?cursor=...&limit=...` with composite
  cursor `(created_at DESC, id DESC)` returned as an opaque base64url string.
- Response items follow the contract: `{matchId, gameMode, region, status,
  startedAt, finishedAt, team}`.

## Deviations accepted for the MVP
- Auth on the history endpoint uses the session JWT with `sub == playerId` instead
  of the contract's scoped data-token (`player.read:{playerId}`); data-tokens land at W22.
- No transaction wrapping match insert + match_players insert yet; transactional
  outbox (W12) will harden durability and event publication together.
- `startedAt` is NULL until the match actually starts (set by the orchestrator flow).

## How to test
- `mvn -B verify` — unit tests incl. cursor codec and pagination math.
- Manual: `docker compose up -d postgres`, run the engine + api, confirm a match
  via the WS flow, then
  `GET /v1/players/{playerId}/matches?limit=2` shows the row.

## Trade-offs
- Adds PostgreSQL as a hard dependency for history (already in docker-compose).
- TIMESTAMPTZ mapping goes through a tolerant Instant converter in the repository.

## Consequences
- Historical facts survive Redis eviction; hot state stays disposable.
- Future sprints (W12 outbox, W13 analytics) build on these tables.
