# ADR-0009 — Queue partitioning for variable match sizes (W9)

- **Status:** Accepted
- **Date:** 2026-10-16
- **Sprint:** W9
- **Post:** pending

## Context

The MVP explicitly supports both 1v1 and 5v5 matchmaking (01 - Visão Geral e Regras,
seção 2: "Matchmaking 1v1 e 5v5"). The queue is already partitioned by game mode
and region (`queue:{gameMode}:{region}`, see `RedisKeys.queueKey`), which is what
allows both modes to coexist.

However, the pairing code is hard-wired for 1v1:

- `pair.lua` always takes the first two tickets of the queue (`ZRANGE ... 0 1`).
- `PairingPolicy.canPair` accepts exactly two tickets.
- `PendingMatch` and the confirmation path assume two players (teams "A"/"B").

With this, any 5v5 queue can never produce a match: two leftover tickets block
the queue forever, exactly the failure described by the sprint ("a fila 1v1 não
vira fila 5v5").

Separately, `GameMode` exposes `teamSize()` (1 for `1v1`, 5 for `5v5`). That value
counts *teams*, not *players*, so it cannot drive pairing: a `5v5` match needs ten
players, not five.

## Problem

Pair the head of a queue with the number of players the game mode requires:

- `1v1` queues need exactly two players.
- `5v5` queues need exactly ten players.
- The candidate count must be validated, the selected tickets must be removed
  atomically, and the pending match must contain all of them.
- The same rule must scale: adding a new game mode must only add a constant.

Without this, the engine silently gives up on any queue whose size is not exactly
two.

## Options Considered

| Option | Pros | Cons | Cost |
| --- | --- | --- | --- |
| Keep 1v1 only (drop 5v5 support) | Smallest change | Violates the MVP scope; `POST` with `gameMode: "5v5"` can never match | High (product) |
| Expose `teamSize()` to pairing | Minimal diff | Encodes teams, not players (`5v5` would pair five players, not ten); wrong semantic | Medium |
| `playersPerMatch()` + exact-count pairing (chosen) | One constant per mode drives everything; one Lua script scales | Pairing logic knows the required count; queue head must contain enough tickets | Low |
| Lock-based grouping instead of Lua | Familiar | Locks are not needed: the pairing script is one short Redis call | High |

## Decision

Add `GameMode.playersPerMatch()` — the total number of players forming a match —
and drive pairing from it:

- `PairingPolicy.canPair(List<MatchmakingTicket>)` accepts a candidate list only
  when it contains exactly `playersPerMatch()` tickets, all `SEARCHING`, in the
  same game mode and region, with distinct players.
- The engine parses the queue key to learn the required count, pulls that many
  tickets, validates them with `PairingPolicy`, and calls `pair.lua` with that
  count.
- `pair.lua` atomically removes the first N tickets of the queue and creates the
  pending match containing all N players.
- When a match is confirmed, the players are split into two balanced teams
  ("A"/"B"). Each team holds {@code teamSize()} players; the current model
  always splits into two teams, and teamSize only sizes them.

Queue partitioning by game mode and region is unchanged: it already exists and is
what makes different match sizes possible.

## How it works

```
POST /v1/matchmaking/tickets { "gameMode": "5v5", "region": "NA", ... }
        │
        ▼
queue:5v5:NA   (ZSET; one entry per ticket)
        │
        ▼  engine loop (every 500 ms)
parse queue key → N = playersPerMatch("5v5") = 10
ZRANGE queue:5v5:NA 0 9          → 10 candidates
PairingPolicy.canPair(list)    → valid (exact count, distinct players)
EVALSHA pair.lua 1 queue:5v5:NA <matchId> <pendingJson> <ttl> 10
        │                            │
        │                            └── atomic: ZREM 10 tickets, SET match-pending:<id>
        ▼
players are notified; both confirm → match persisted
```

Key points:

- The count is derived once from the game mode; every other rule reads it.
- `pair.lua` receives the count as `ARGV[4]` and always operates on the first N
  entries of the queue, so two workers can never create overlapping matches
  (the atomic `ZREM` was already proven in W6).
- If fewer than N tickets are queued, no pair is created and the queue keeps
  waiting — the player is not penalized by losing their place.

## How to test

- `PairingPolicyTest`: `canPair` fails unless the candidate count equals
  `playersPerMatch()` (2 for `1v1`, 10 for `5v5`); it also fails on duplicate
  players, wrong mode or region, and non-`SEARCHING` tickets.
- `PairingFiveVsFiveTest` (new): ten `5v5` tickets are queued; the pairing script
  runs with count 10 and creates exactly one pending match containing ten
  players; the queue is then empty.
- The existing 1v1 tests (`PairingRaceTest`, `PairingStressTest`) keep passing:
  they must pass count 2, which they do after the script is extended.
- Measured number: **one pending match for ten tickets in a 5v5 queue** (the
  stress tests also confirm 1v1 queues still pair two tickets at a time).

## Trade-offs

- The pairing rules are split across Java (`PairingPolicy`) and Lua (`pair.lua`).
  The count is validated in Java, while the queue manipulation is atomic in
  Lua — two short scripts instead of one.
- Team assignment happens after confirmation and uses a simple split into two
  balanced teams ("A"/"B"); more than two teams per match are not modeled (the
  current MVP only ever splits into two).
- `pair.lua` takes the first N entries of the queue without pre-selecting them;
  if the queue head contains unsuitable tickets, pairing waits for better ones.

## Consequences

- [x] Queues of any game mode can produce a match of the right size.
- [x] Adding a new game mode only requires a new enum constant and its label.
- [ ] A player may still not join two matches at once (enforced by `SEARCHING`
      state, unchanged).
- [ ] Skill widening (W10) is intentionally out of scope here; it is applied on
      top of the structural rules introduced by this ADR.
- [ ] The two-team split heuristic is only reviewed when a game mode needs more
      than two teams.
