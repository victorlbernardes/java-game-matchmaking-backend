# 0008 — Concurrency testing strategy (W8)

## Status
Accepted (2026-10-14)

## Context
After W6 (atomic Lua pairing) and W7 (atomic idempotent create), the
implementation relies on Redis atomicity for correctness. But the race tests
only covered 2 workers. We need to prove the invariants hold at scale (10
workers) and that the tests actually detect violations (fail when atomicity is
removed).

## Options
1. **Sequential EVALSHA** — call the script one at a time. Simple, but does
   not exercise true concurrency (the Redis client serializes commands on a
   single connection).
2. **Concurrent Futures on the event loop** — send all EVALSHA calls as
   Vert.x Futures; the event loop dispatches them concurrently to Redis.
   Redis is single-threaded, so the Lua script executes atomically per call.
3. **Multiple threads with separate Redis clients** — true OS-level
   concurrency, but Vert.x Redis client is not thread-safe across event loops.

## Decision
Use concurrent Vert.x Futures (option 2) — the Vert.x event loop dispatches
all EVALSHA calls concurrently, and Redis executes each Lua script atomically
(single-threaded). This exercises the atomicity guarantee without thread-safety
issues.

## How it works
- `PairingStressTest` — 10 workers race to pair from a queue of 20 tickets.
  Each worker calls `pair.lua` (ZRANGE + ZREM + SET) via EVALSHA. Asserts:
  exactly 10 wins (20 tickets / 2 per match), queue empty after, 10 pending
  matches exist.
- `IdempotencyStressTest` — 10 concurrent requests with the same
  Idempotency-Key. Each calls `create.lua` (GET + SET + ZADD + SET) via
  EVALSHA. Asserts: all return the same ticketId, exactly 1 ticket exists.

## Properties verified under stress
- `1 ticket -> at most 1 match` (pairing invariant)
- `1 Idempotency-Key -> exactly 1 ticket` (idempotency invariant)
- `queue empty after pairing` (ZREM removes paired tickets)
- `pending match never duplicated` (same matchId never created twice)

## How to test
- `mvn -pl services/matchmaking-engine test -Dtest=PairingStressTest`
- `mvn -pl services/matchmaking-engine test -Dtest=IdempotencyStressTest`
- Both fail if the Lua scripts are removed (non-atomic fallback creates
  duplicates).

## Trade-offs
- Concurrent Futures on a single event loop do not create true OS-level
  parallelism, but Redis is single-threaded anyway — the atomicity guarantee
  holds regardless of how many clients send commands.
- Tests are slightly slower than sequential (they wait for all workers).

## Consequences
- The atomicity of the Lua scripts is proven at scale (10 workers), not just
  with 2. The invariant `1 ticket -> 1 match` and `1 key -> 1 ticket` are
  enforced by Redis, not by application logic.
