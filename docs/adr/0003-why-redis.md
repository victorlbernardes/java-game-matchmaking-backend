# 0003 — Why Redis for hot state (W3)

## Status
Accepted (2026-10-03)

## Context
Tickets are created via `POST /v1/matchmaking/tickets` and are polled/cancelled by
clients. Their state is hot: short-lived, frequently read/written, and disposable.
Until W3 they lived in an in-memory `ConcurrentHashMap`, which breaks with more
than one instance of `matchmaking-api` (each instance has its own map) and is lost
on restart.

## Options
1. **PostgreSQL for hot state** — durable, but not optimized for high-rate
   create/cancel churn with TTL semantics.
2. **In-memory map (previous state)** — works for one JVM only.
3. **Redis** — shared across instances, native TTL, native ordered structures.

## Decision
Redis holds the hot ticket state; PostgreSQL (W5) remains the source of truth for
durable history.

## How it works
Three key families, all with TTL (`ticket:ttl-seconds`, default 900s):

| Key | Type | Content | TTL |
|-----|------|---------|-----|
| `ticket:{ticketId}` | String (JSON of `MatchmakingTicket`) | Latest ticket state | 900s |
| `queue:{gameMode}:{region}` | Sorted Set (score = `queuedAt` epoch ms) | Ticket ids waiting in that queue | refreshed with members |
| `idem:{idempotencyKey}` | String → `ticketId` | Idempotency-Key deduplication | 900s |

Flows:
- Create → save ticket JSON, `ZADD` to the queue, `SET idem:` (idempotent replay
  returns the existing ticket without creating a new one).
- Cancel (`DELETE`) → validate ownership and `SEARCHING` state, transition the
  ticket to `CANCELLED` via `TicketStateMachine`, `ZREM` from the queue, keep the
  cancelled record until TTL expires.
- TTL expiration removes tickets that never got cancelled or matched; the engine
  (W4+) consumes `queue:*` sets in score order.

The `TicketRepository` port is async (Vert.x `Future`) so the Redis client never
blocks the event loop. `matchmaking-api` wires `RedisTicketRepository` from
`main`; tests keep using the in-memory adapter.

## How to test
- `mvn -B verify` (unit/integration tests keep the in-memory adapter green).
- Manual: `docker compose up -d redis`, start `auth-service` + `matchmaking-api`,
  run `requests/matchmaking-api/POST-v1-matchmaking-tickets.http`, then check
  `redis-cli KEYS '*'` and `ZRANGE queue:5v5:NA 0 -1`.

## Trade-offs
- Adds Redis as a runtime dependency for the hot path (single point of failure
  until replication/sentinel in a later sprint).
- Data in Redis is disposable by design — acceptable because a lost ticket only
  forces the client to re-enqueue.

## Consequences
- `matchmaking-api` no longer owns ticket state in-process; multiple instances
  share one hot state.
- The W3 deviation ("tickets in memory until W3") is resolved.
