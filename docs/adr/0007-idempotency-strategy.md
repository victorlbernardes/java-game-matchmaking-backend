# 0007 — Idempotency strategy: atomic Lua create (W7)

## Status
Accepted (2026-10-14)

## Context
The `CreateTicketUseCase` checked the idempotency index (`GET idem:{key}`) and
then created the ticket (`SET ticket:{id}` + `ZADD` + `SET idem:{key}`) as
separate commands. Under concurrency, two requests with the same
`Idempotency-Key` both see `null` on the GET, both create separate tickets, and
the last `SET idem:{key}` overwrites the first — leaving an orphan ticket and
violating the invariant `1 Idempotency-Key → 1 ticket`.

## Options
1. **Atomic Lua script** — move GET + create + save into one script that Redis
   executes atomically. No lock, no deadlock, one round-trip.
2. **SET NX on the idempotency key** — `SET idem:{key} ticketId NX EX 900`
   claims the key before creating. But between the claim and the ticket creation,
   another request can still create a second ticket (the claim and the create are
   not atomic together).

## Decision
Use an atomic Lua script (`create.lua`) that does everything in one execution:
1. `GET idem:{key}` — if set, return the existing ticketId (idempotent replay).
2. Otherwise: `SET ticket:{id}` + `ZADD queue` + `SET idem:{key}` — all atomic.
3. Returns the ticketId (existing or newly created).

If two requests with the same key race, the first script "wins" (creates the ticket
and sets the index). The second script sees the index already set and returns the
same ticketId — no duplicate.

## How it works
- `create.lua` lives in `common/common-redis/src/main/resources/create.lua`.
- `RedisTicketRepository.createIdempotent(key, ticket)` loads the script lazily
  (`SCRIPT LOAD` → SHA, cached in an `AtomicReference`) and calls `EVALSHA`.
- `CreateTicketUseCase` generates the ticketId, builds the ticket, and calls
  `createIdempotent`. If the returned ticketId equals the generated one → created
  (201). Otherwise → replayed (200), fetch the existing ticket by the returned id.
- `InMemoryTicketRepository` mirrors the logic for tests (index check + save).

## How to test
- `IdempotencyRaceTest` — two requests with the same Idempotency-Key; asserts
  exactly one ticket exists and both requests return the same ticketId.
- The test fails if the script is removed (GET + save separately → 2 tickets).

## Trade-offs
- The ticketId is generated in Java (UUID) and passed to the script — the script
  does not generate IDs.
- If the script returns a different ticketId (replay), the use case fetches the
  existing ticket by that id (one extra round-trip on replay only).
- Lua scripts are harder to debug than Java (no breakpoints inside Redis).

## Consequences
- `1 Idempotency-Key → 1 ticket` is enforced by Redis atomicity.
- Retries (timeout, network replay) return the same ticket instead of creating
  duplicates.
- The idempotency index and the ticket are always consistent.
