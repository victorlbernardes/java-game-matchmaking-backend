# ADR-0031 — Capacity-aware matching, and an allocation that cannot happen twice (W22)

## Status
Accepted (2026-10-20)

> The number follows the vault's original plan (`08 - Banco de Decisões e ADRs.md`, Bloco G), which
> reserved 0031 for capacity-aware matching before W20/W21 took 0027/0028/0039. The order it was
> written in is not the order it is numbered in; the reason is that a reader looking for "how does a
> match get a server" should find it where the plan said it would be.

## Context

W13 built the consuming half of the orchestrator: a consumer group that projects a confirmed match
into `match_allocations` exactly once (inbox, ADR 0038), so that a redelivery cannot apply it twice.
What the projection never answered is the question it exists for — **where does the match run?** The
row was written with `status = PENDING_ALLOCATION` and stayed there. Two consequences:

1. **The system confirmed matches it could not host.** The engine paired, the players confirmed, the
   match was announced and committed to Postgres — and if the region had no room, it also dissolved
   at the confirmation deadline. The player paid the wait *and* the match, and no counter said so.
2. **"Capacity" did not exist as a number.** There was no pool, no registration, no way for an
   operator to say "this region can host N matches right now", and no way for the engine to ask.

W22 has to answer both without breaking the deployments that never register a game server — the E2E,
the load harness and the backpressure scenario all run against a pool-less stack, and turning
"nobody registered a server" into "capacity is zero" would make every one of them stop matching.

## Options

### Where capacity comes from

**A. The engine asks the game servers directly (gRPC/HTTP per match).** Fresh and precise, and the
wrong dependency: it puts every game server in the pairing loop's critical path, so one hung server
delays or stalls the pairing of a whole (mode, region). It also gives the *engine* an opinion about
placement, which is the orchestrator's job.

**B. An event stream of server state (a `game-server-state` topic, projected into Redis).** Scales
and decouples, and buys a second copy of the truth plus a second thing to keep consistent — for a
number that changes on the order of seconds and is read by one process. Rejected by scope: the
moments this sprint needs (registration, heartbeat, drain, release) are request/response, not facts
the world must consume.

**C. The orchestrator owns the pool in Postgres; the engine reads a snapshot (chosen).** One table,
one writer of `allocated` (the allocator), one place a server announces itself. The engine polls the
snapshot once a second and reads it from memory; the staleness that buys is bounded and its only
consequence is a match that lands in `PENDING_ALLOCATION` instead of disappearing.

### Where the allocation happens

**D. In the engine, before publishing the event.** The engine would pick a server and put it in the
event. Rejected: the engine then needs the pool, a reservation protocol and a compensating action
for every match that dissolves before confirmation — a leak per dissolution, which is a worse failure
than a match that waits for a server.

**E. In the orchestrator, as a second step after the inbox insert.** Simple and wrong in exactly the
case the inbox exists for: a crash between the insert and the allocation leaves a confirmed match the
pool never counted, and a redelivery finds the event already seen and skips the allocation. The
match is then confirmed, projected and invisible.

**F. In the orchestrator, inside the inbox transaction (chosen).** The projection row, the slot it
takes and the `processed_events` insert commit together or not at all. A redelivery cannot take a
second slot (the inbox insert decides), and two confirmations arriving together cannot take the same
last slot (`SELECT … FOR UPDATE SKIP LOCKED` + `UPDATE … WHERE allocated < capacity`).

### What "no pool" means

**G. No servers registered = zero capacity.** Consistent and unacceptable: every existing deployment
and every existing scenario would stop forming matches, and the sprint would be a breaking change
disguised as a feature.

**H. No servers registered = unknown (chosen).** The region's capacity is *authoritative* only when
at least one server has ever registered there (the row may be stale or draining; the region is still
a region with a pool). Unknown constrains nothing, which is precisely the W1–W21 behaviour. A region
whose pool is full is a different answer: zero, and the engine does not pair.

## Decision

The **orchestrator owns a game server pool** (Postgres table `game_servers`, one row per server:
region, game modes, `capacity` in concurrent matches, `allocated` bookkeeping, `draining`,
`last_seen`), exposes it on an **internal HTTP API** (`ORCHESTRATOR_PORT`, default 8085, service token
`GAME_SERVER_TOKEN`, scrape 9093), and **allocates inside the confirmed-match transaction** with a
bin-packing pick (`ORDER BY (capacity - allocated) ASC, allocated DESC, server_id`) that fills the
tightest server first and survives the last slot being taken by a concurrent confirmation.

The **engine reads the snapshot once a second** and gates pairing on it: unknown (no pool) leaves the
historical behaviour; zero free slots skip the (mode, region) and count
`matchmaking_pairing_suspended_total`; N free slots cap the batch at N, so a tick cannot form eight
matches against a pool with one slot.

Eligibility is a **query predicate**, not a state: `last_seen > now() - SERVER_TTL_SECONDS` and
`NOT draining`. There is no sweep and no `STALE` column; `READY`/`STALE`/`DRAINING` are derived at
read. A registration is a **full statement of intent** and a heartbeat is the same call, so an agent
has one thing to do and a missed renewal needs no repair.

## How it works (files)

- `common/common-db/src/main/resources/db/migration/V4__create_game_servers.sql` — the table
  (`CHECK (allocated <= capacity)`), the eligibility index, and `server_id`/`allocated_at` on
  `match_allocations`.
- `common/common-db/.../GameServerRepository.java` — `register`, `find`, `list`, `drain`, `snapshot`,
  `reserve(SqlConnection, …)`, `release`; `Registration`, `GameServer`, `RegionCapacity`,
  `CapacityBelowAllocated`. `reserve` is the two-step `SELECT … FOR UPDATE SKIP LOCKED` + `UPDATE …
  WHERE allocated < capacity` (a `WITH … UPDATE … RETURNING` hands back no rows through the Vert.x
  JDBC client — runbook item 16).
- `common/common-domain/.../domain/match/AllocationStatus.java` — `PENDING_ALLOCATION`, `ALLOCATED`,
  `RELEASED` (moved here: two services name it now).
- `services/match-orchestrator/.../presentation/GameServerRoutes.java` — `PUT`/`DELETE`
  `/v1/game-servers/:serverId`, `POST …/allocations/:matchId/release`, `GET /v1/game-servers`,
  `GET /v1/capacity`, `GET /health`; token check, `400/401/404/409` shapes.
- `services/match-orchestrator/.../infrastructure/PostgresConfirmedMatchInbox.java` — the allocation
  joins the event's transaction, and the outcome is counted after the commit
  (`matchmaking_allocations_total{result="allocated"|"no_capacity"}`).
- `services/matchmaking-engine/.../application/Capacity.java` — the port (`OptionalInt free(mode,
  region)`, `UNLIMITED`).
- `services/matchmaking-engine/.../infrastructure/PostgresCapacity.java` — the 1 s poll, the snapshot
  in memory, and the gauges for the regions this engine serves.
- `services/matchmaking-engine/.../MatchmakingEngineApplication.java` — `pairingBudget(...)` and the
  gate in `pairAcrossQueues`.
- `common/common-metrics/.../MetricNames.java` — `matchmaking.allocations`,
  `matchmaking.capacity.free`, `matchmaking.pairing.suspended`.

## Measured numbers (local stack, 2026-10-20)

`bash requests/gameservers/verify_capacity.sh` — **58 checks, 0 failures in 103 s**, with the scenario's own
API (8098), engine (metrics 9101) and orchestrator (8086, metrics 9094, `SERVER_TTL_SECONDS=10`).

**An empty pool keeps the old behaviour.** `GET /v1/capacity` without the token → **401**; with it →
`authoritative: false, free: 0`. The pair still matched, confirmed, and stayed `PENDING_ALLOCATION`
(and `matchmaking_allocations_total{result="no_capacity"}` moved 0 → 1).

**A full pool holds the queue.** `capacity: 0` → `READY, free: 0, servers: 1`; two arrivals stayed in
the queue (depth 2), both tickets `SEARCHING`, **no** `match-pending:*` key, `capacity_free == 0`, and
`pairing_suspended_total` went **7 → 25** across the wait.

**A slot is one match.** `capacity: 1` → the waiting pair left the queue within one poll; on
confirmation `match_allocations` for `match-9dc32ae9` got `ALLOCATED` with `server_id = gs-w22-one`
and the pool read `allocated 1, free 0`.

**Release returns the slot exactly once.** `200` (`allocated 0, free 1`), then `404
allocation_not_found` on the repeat, and `free` still **1** — the second call cannot return a second
slot because it has no row to flip. The waiting pair took the freed slot and was allocated again.

**Two confirmations, one slot.** Two matches formed against a two-slot pool, the pool shrank to one,
both confirmations arrived together: `match-90198069 → ALLOCATED`, `match-9da20a1c →
PENDING_ALLOCATION`, server at `allocated 1 / capacity 1`. One slot, one winner, nothing over-committed.

**TTL, with no sweep running.** Twelve seconds without a heartbeat took the fresh server out of the
pool (`free 1 → 0`), the listing derived `STALE`, and a new pair waited — and paired the moment the
server spoke again.

**A drain takes the slot away and a heartbeat gives it back.** `DELETE` → `state DRAINING`, `free 0`
with the region still authoritative; the next pair waited (`pending 0`, depth 2) instead of being
refused, and the heartbeat that cleared the drain formed it — allocated to `gs-w22-drain`, then
released (allocated 0).

**Shrinking is refused, not clamped.** `PUT capacity: 0` on a server running a match → `409
capacity_below_allocated`, row untouched (`capacity 1 / allocated 1`). The database agrees with the
API: no server has `allocated > capacity`, and `sum(allocated)` equals this run's `ALLOCATED` rows.

**The engine's own property test** (`CapacityAwarePairingTest`, 4 tests): no slot → no match and the
queue waits (with the suspension counter); one slot → exactly **one** match from four compatible
players; unknown capacity → pairs as before; a freed slot → the players who waited take it.
`GameServerRepositoryTest` (8 tests) forces the race with two open transactions and asserts exactly
one winner; `AllocationOnConfirmationTest` (5 tests) asserts the redelivery, the full pool, the race
through the inbox and the release.

## How to test

```bash
# by hand: the pool's whole contract, then the gate, the allocation and the TTL
bash requests/gameservers/verify_capacity.sh

# the properties, without a running stack (Postgres + Redis)
mvn -o -B -pl common/common-db test -Dtest=GameServerRepositoryTest
mvn -o -B -pl services/match-orchestrator test -Dtest=AllocationOnConfirmationTest
mvn -o -B -pl services/matchmaking-engine test -Dtest=CapacityAwarePairingTest
```

The guards were checked by **removing** them (a property test that passes with the guard gone proves
nothing): with `pairingBudget` always returning the batch, `CapacityAwarePairingTest` fails **2 of 4**
(the zero-slot case — "the reason is counted, not silent" — and "one slot, four players, one match");
with both `allocated < capacity` predicates gone from `reserve`,
`GameServerRepositoryTest.allocationTakesASlotUntilTheServerIsFull` fails with
`game_servers_allocated_within_capacity` violated. That is the two lines of defence in order: the
predicates make the full pool the ordinary answer, and the constraint makes an over-allocation a
database error rather than a truth nobody notices.

By hand, without the script: register `curl -X PUT localhost:8085/v1/game-servers/gs-1 -H
"authorization: Bearer $GAME_SERVER_TOKEN" -d '{"region":"NA","gameModes":["1v1"],"capacity":1,
"endpoint":"10.0.0.7:7777"}'`, read `curl -s localhost:8085/v1/capacity?gameMode=1v1&region=NA -H
"authorization: Bearer $GAME_SERVER_TOKEN"`, then `SELECT status, server_id FROM match_allocations`.
The collection's `gameservers-w22` folder is the same path request by request.

## Trade-offs

- **The engine's gate is a snapshot, up to one second stale.** Two engines (or one engine and one
  confirmation) can believe the same slot is free. The consequence is bounded on purpose: the match
  forms, the allocator refuses the slot, and the row stays `PENDING_ALLOCATION` — W23's timeout works
  through it. The alternative (reserving at pairing time) leaks a slot on every dissolution before
  confirmation, which loses matches instead of delaying them.
- **`SERVER_TTL_SECONDS` must agree between the orchestrator and the engine.** Both read the same
  table with their own env value; a mismatch means the engine forms matches the allocator considers
  stale. It degrades safely (the match waits) but it is capacity nobody uses, and boot does not check
  it — the same shape as `SERVED_REGIONS` in W21.
- **`allocated` is bookkeeping, not fact.** It is incremented by the allocator and decremented by
  `release`; a match whose release never arrives (the game server dies) keeps its slot until somebody
  releases it. W23/W24 own the reconciliation (the timeout sweep is the natural place); the number is
  never used to *allow* anything — the `CHECK (allocated <= capacity)` and the `allocated < capacity`
  predicate are what keep the pool honest.
- **The internal API is a shared bearer token.** Right for a local stack and for a first deployment,
  wrong for a hostile network: a real deployment replaces it with mTLS. The endpoints are on their own
  port, never exposed through the gateway, and the token is not the player's JWT — a game server has
  no player identity to present.
- **Bin-packing fills the tightest server first.** Good for cost and for draining (empty servers can
  be removed), and it concentrates a region's matches on few servers — a blast radius a spread policy
  would not have. It is a policy choice inside one query, easy to change, and the query is the place
  to change it.
- **The pool is one table, so the orchestrator is a single writer.** It scales vertically and by
  consumer count (the inbox makes N orchestrators safe for events), but the pool's row locks serialize
  allocations per server. At the scale this project measures the lock is not the bottleneck; a pool
  spread over shards is the next question, not this sprint's.

## Consequences

- A confirmed match has a destination (`ALLOCATED`, with `server_id`) or an honest state
  (`PENDING_ALLOCATION`) instead of silently having neither.
- The engine stops forming matches a region cannot host, and says so with a counter — a queue that is
  held is now distinguishable from a queue that is broken.
- A deployment with no game server behaves exactly as it did before W22; the E2E, the load harness and
  the backpressure scenario needed no change to keep passing.
- `match_allocations` became the join between the matchmaker and the game servers: W23's timeout reads
  `PENDING_ALLOCATION`, W24's reconciliation reads `ALLOCATED` without a release.
