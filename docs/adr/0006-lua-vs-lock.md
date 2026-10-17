# 0006 — Why Redis Lua for atomic pairing (W6)

## Status
Accepted (2026-10-10)

## Context
The matchmaking engine pairs queue heads with a non-atomic sequence:
`SCAN` → `ZRANGE` → `findById` → `pair()` (save tickets + create pending match).
If two engine workers run simultaneously, both can read the same two tickets
before either creates a pending match — forming the same match twice and
violating the invariant `1 ticket → 1 match`.

## Options
1. **Distributed lock (SET NX + TTL)** — lock the queue before pairing.
   - Needs TTL to avoid deadlock if a worker dies.
   - Needs lock renewal for long operations.
   - Adds operational complexity (lock management, renewal, release).
2. **Atomic Lua script** — move the entire pairing (ZRANGE + ZREM + SET) into
   a single Lua script that Redis executes atomically.
   - No lock, no deadlock, no renewal.
   - One round-trip does everything.
   - Redis guarantees no other command runs during the script.

## Decision
Use an atomic Lua script (`pair.lua`) for pairing. The script:
1. `ZRANGE queueKey 0 1` — takes the first two tickets from the queue.
2. `ZREM queueKey t1 t2` — removes them atomically.
3. If `ZREM < 2` → another worker already removed them → return `nil`.
4. `SET match-pending:{matchId} {json} EX {timeout+grace}` — creates the pending match.
   The TTL **must be greater than the confirmation deadline** so the sweeper can
   still read the key, dissolve the match and requeue the tickets when the
   deadline passes. Storing the key with `EX {timeout}` made it expire exactly at
   the deadline, so the sweeper never saw it and the tickets stayed `CONFIRMING`
   forever (fixed in W11; engine constants `CONFIRMATION_TIMEOUT_SECONDS = 30`,
   `PENDING_KEY_GRACE_SECONDS = 60`).
5. Returns `matchId` (winner) or `nil` (loser).

If two workers race, the first worker's `ZREM` removes the tickets; the second
worker's `ZREM` returns 0 and the script returns `nil` — no duplicate match.

## How it works
- `pair.lua` lives in `matchmaking-engine/src/main/resources/pair.lua`.
- The engine loads it at startup (`SCRIPT LOAD`) and stores the SHA.
- `tryPairFromQueue` finds a pairable pair (ZRANGE + findById + canPair), then
  calls `EVALSHA pair.lua` with `(queueKey, matchId, pendingJson, ttl)`.
- If the script returns `matchId` → transition tickets to CONFIRMING + notify.
- If the script returns `nil` → another worker won; try again next cycle.

## How to test
- `PairingRaceTest` — two workers pair the same 2 tickets; asserts exactly one
  pending match exists and the queue is empty.
- The test fails if the script is removed (falls back to non-atomic ZRANGE+save,
  which allows duplicates).
- `PendingMatchTtlTest` — the pair script stores the pending key with
  `PENDING_KEY_TTL_SECONDS > CONFIRMATION_TIMEOUT_SECONDS`, so the confirmation
  timeout can actually be swept.

## Trade-offs
- Lua scripts are harder to debug than Java code (no IDE breakpoints inside Redis).
- The script must be self-contained (no external dependencies).
- `EVALSHA` requires the script to be loaded first (handled at startup).

## Consequences
- The `1 ticket → 1 match` invariant is enforced by Redis atomicity, not by
  application-level locking.
- Multiple engine workers can run safely without duplicate matches.
- The pairing logic is split between Java (find pairable pair) and Lua (atomic
  commit) — a deliberate separation of concerns.
