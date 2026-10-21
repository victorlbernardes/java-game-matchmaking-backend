# ADR-0032 — The allocation lifecycle, and a player who goes back to the queue where he was (W23)

## Status
Accepted (2026-10-21)

## Context

W22 gave a confirmed match a destination when a slot was free at the instant of the confirmation. It
also named what it left open: a match that lands in `PENDING_ALLOCATION` **stays there forever** —
nothing retries, nothing gives up, nobody puts the players back in the queue; an `ALLOCATED` match
whose server dies keeps its slot until somebody calls `release` by hand (the `allocated` bookkeeping
had no reconciliation); and "the match started" was a sentence the system could not say, because the
game server had no way to report it.

Three holes, one shape: **the lifecycle had no clock and no way back.** A confirmed match could only
move forward, and a match that could not move forward was an orphan that also held a player's wait
and a server's slot.

The decision this ADR records is the *whole* loop: retry, deadline, rejection with a reason, and a
requeue that gives the players back **the position they had**, not the end of the line. The token and
the no-show are ADRs 0033 and 0034; they are separate because they answer different questions (how a
player gets in, and what happens when he does not).

## Options

### Who owns the clock

**A. The engine, in the pairing loop.** It already runs a timer and owns the queue. Rejected: the
engine would need the pool, the deadlines and the meaning of "the server is dead" — the W22 decision
put the pool in the orchestrator precisely so the pairing loop stays a loop over queues. Two writers
of `match_allocations` would also mean two places to keep the `allocated` bookkeeping honest.

**B. A separate scheduler service.** Correct at a scale this project does not have, and a third
process to deploy, monitor and make idempotent for a sweep that is five SQL statements.

**C. A watchdog inside the orchestrator (chosen).** The orchestrator already owns the pool, the
allocation transaction and the rejection vocabulary, and it already runs next to the database it
decides on. One timer, one process, one writer.

### Who puts the players back

**D. The orchestrator requeues, directly into Redis.** The decision and the act in one place, and the
wrong ownership: the queue is the engine's (`RedisKeys`, the pairing script, the partition hash), and
an orchestrator that writes it would need every rule the engine has — the partition, the score, the
TTL, the ticket state machine.

**E. The engine polls the rejected rows and requeues (chosen).** The orchestrator writes the
*decision* (`status = REJECTED`, `rejection_reason`, `rejected_at`), and the engine — the only process
that owns the queue — reads those rows every second and acts. The table is the channel, so a rejection
that happens while the engine is down is not lost: it is a row nobody has marked yet.

### How the requeue finds the queue entry

**F. By player id.** The queue's member is the ticket id, not the player id, so this needs a lookup
per player and still does not say *which* ticket the match was formed from (a player can have a new
ticket by then).

**G. A `match_roster` table carrying each player's `ticket_id` (chosen).** The event gains a
`ticket_id` per player (`MatchPlayerState.ticket_id = 4`, an additive change the contract gate
accepts), and the inbox writes the roster in the same transaction as the allocation. The requeue then
does exactly what the queue understands: `ZADD` that member with its original score.

### What the score is

**H. `now()`.** The queue is a priority queue ordered by arrival; a requeue with the current time is
not a requeue, it is a player sent to the back of a line he had already waited in — and every test
that only counts members passes. Rejected: it silently punishes the player for the system's failure.

**I. The ticket's own `queuedAt` (chosen).** The ticket keeps the instant it was created; the requeue
uses it as the score and re-adds the member to the partition the jump hash gives that ticket (W20),
so the position is the one he had.

### The order of the two writes

**J. Mark the row, then requeue.** One crash between the two loses the players of every match the
engine was marking — the exact failure the loop exists to prevent.

**K. Requeue, then mark (chosen).** A crash in between repeats the requeue on the next tick, and the
repeat is idempotent (`requeue.lua` answers `UNCHANGED` for a ticket that is already `SEARCHING`).
The marker is therefore the *last* write, and it means "this rejection was acted on", not "players
came back".

## Decision

`match_allocations` gains the timestamps of the states (`started_at`, `rejected_at`,
`rejection_reason` — a `CHECK` against the domain's `RejectionReason` vocabulary, so a wrong reason is
a database error — and `requeued_at`), and a new `match_roster` table holds the durable roster with
each player's `ticket_id`, `team`, `skill_at_match`, `connected_at` and `no_show_at`.

The **orchestrator's `AllocationWatchdog`** runs every second and asks five questions in this order:
retry every pending match against the pool; reject the ones past `ALLOCATION_TIMEOUT_SECONDS`
(`no_capacity`); reject the allocated ones past `START_TIMEOUT_SECONDS` (`start_timeout`) and give the
slot back; reject the ones whose server left the pool (`server_failed`); and, past
`CONNECT_WINDOW_SECONDS`, mark the silent players (ADR 0034).

The **engine's `MatchRequeueSweeper`** runs every second, reads the `REJECTED` rows of its own region
with `requeued_at IS NULL`, requeues each ticket with `requeue.lua` (original score, own partition, TTL
renewed, `SEARCHING` again) and marks the row last.

The **release** learned a second state: `ALLOCATED` **or** `STARTED` → `RELEASED`. Before this, a
match that had started could not be finished — the game server's call answered `404
allocation_not_found` and the slot stayed taken until the TTL decided the server was dead, a slot
leaked by the normal path.

## How it works (files)

- `common/common-db/src/main/resources/db/migration/V5__allocation_lifecycle.sql` — the four columns,
  the reason `CHECK`, `match_roster`, and the two partial indexes (`PENDING_ALLOCATION` by
  `confirmed_at`, `requeued_at IS NULL` by `rejected_at`).
- `common/common-events/src/main/proto/.../match_confirmed.proto` — `MatchPlayerState.ticket_id = 4`.
- `services/match-orchestrator/.../application/AllocationWatchdog.java` — the five questions, `BATCH =
  64` per tick, one tick at a time (`AtomicBoolean`), `ALLOCATION_TIMEOUT_SECONDS` (30),
  `START_TIMEOUT_SECONDS` (60), `CONNECT_WINDOW_SECONDS` (90) and the shared `SERVER_TTL_SECONDS`.
- `common/common-db/.../AllocationRepository.java` — `pendingForRetry`, `reject`, `start`, `connect`,
  `noShows`, `allNoShow`, `staleServers`, and the `Transition` enum (`APPLIED`/`ALREADY`/`NOT_FOUND`/
  `INVALID`/`PLAYER_NOT_IN_MATCH`) the HTTP layer renders.
- `common/common-db/.../RejectedMatchRepository.java` — `pendingRequeue(region, limit)` and the
  conditional `markRequeued`. The roster join is a **`LEFT JOIN`**: see "the starvation" below.
- `common/common-db/.../GameServerRepository.java` — `RELEASE_ALLOCATION` now flips `ALLOCATED` or
  `STARTED`, and `giveSlotBack(connection, serverId)` is the one way a slot comes back (release and
  rejection use the same statement, in the transaction that flips the row).
- `common/common-redis/.../requeue.lua` + `TicketRepository.requeue` — the state machine and the
  `RequeueOutcome` (`REQUEUED`/`UNCHANGED`/`NOT_SEARCHABLE`).
- `services/matchmaking-engine/.../application/MatchRequeueSweeper.java` — the sweep, `REQUEUE_BATCH =
  32` per region, 1 s timer; it counts a match as requeued only when a player actually came back.
- `services/match-orchestrator/.../presentation/GameServerRoutes.java` — `POST …/started` and
  `POST …/players/:playerId/connected`, both idempotent (`ALREADY`), `404 player_not_in_match`,
  `409 invalid_transition`.
- `common/common-metrics/.../MetricNames.java` — `matchmaking.match.rejections{reason}`,
  `matchmaking.no.shows`, `matchmaking.allocation.retries{result}`,
  `matchmaking.matches.requeued`, `matchmaking.players.requeued`,
  `matchmaking.requeue.delay.seconds`, `matchmaking.requeue.skipped{reason}`.

## Measured numbers (local stack, 2026-10-21)

`bash requests/lifecycle/verify_lifecycle.sh` — **75 checks, 0 failures** across 10 steps, with the
scenario's own API (8099, scrape 9201), engine (metrics 9102) and orchestrator (8087, scrape 9096),
`ALLOCATION_TIMEOUT=8 START_TIMEOUT=20 CONNECT_WINDOW=10 SERVER_TTL_SECONDS=120`.

**The deadline fires and the players come back where they were.** A pair formed against a free slot,
the slot was taken away before the confirmation (the W22 "capacity is a snapshot" property, used on
purpose), both players confirmed: the match waited in `PENDING_ALLOCATION`, the connection endpoint
answered `409 not_allocated`, and at the **8 s** deadline the watchdog wrote `REJECTED` with
`no_capacity` and `rejected_at`. The engine's sweep put both tickets back **with the epoch-millis
score they were created with** (`1792582460102`/`1792582460103`, read from the ZSET), the tickets
`SEARCHING` again, `requeued_at` set, `matchmaking_matches_requeued_total = 1`,
`matchmaking_players_requeued_total = 2`. **The requeue landed 0.666 s after the rejection**
(`matchmaking_requeue_delay_seconds`, the engine's own timer, not the scenario's stopwatch).

**A slot that frees before the deadline is a match, not a rejection.** The same pair, paired again the
moment a slot existed, confirmed with the slot taken away, landed in `PENDING_ALLOCATION`, and the
slot came back with 7 s of the deadline left: the retry allocated it **1.40 s** after the slot
returned, `matchmaking_allocation_retries_total{result="allocated"}` 0 → 1, and the deadline never
fired.

**The server that took a match and never started it gives the slot back.** `ALLOCATED`, no `started`
report: at the **20 s** start deadline the watchdog wrote `REJECTED` with `start_timeout`, the pool
read `allocated 0` and the roster was requeued.

**The server that stops heartbeating is the release that never came.** `gs-w23-b` was backdated past
its TTL: the match was rejected with `server_failed`, the slot came back (`allocated 0`) although the
server never called `release`, and the players were requeued.

**A running match is releasable.** `STARTED` → `POST …/release` → `200`, `allocated 0`, and the
connection endpoint answered `409 match_finished` from then on. (This is the fix the scenario found:
before it, the release answered `404 allocation_not_found` for a started match.)

**The starvation, found by this scenario on a database with history.** The first run rejected matches
and the sweep never marked them: the query was `… JOIN match_roster`, so a rejected match with **no
roster** (a projection written before W23, or an event that carried no players) produced no rows, was
never marked, and — because the batch is the oldest `LIMIT 32` — stayed at the head of every pass. The
local database had **18 930** such rows and the sweep marked **none** of the new ones. With the join
made `LEFT` and the marker unconditional, the same backlog **drained at 32 matches/s** (the batch, one
tick per second, one `UPDATE` per row) — 18 930 rows in ~10 minutes — and the new rejection was
reached immediately afterwards. `RequeuePreservesScoreTest.aRejectionWithNoRosterDoesNotStarveTheSweep`
is the property test: it fails (`but it is marked, so it leaves the head of the batch ==> expected:
not <null>`) with the `JOIN` restored.

**The engine's own property test** (`RequeuePreservesScoreTest`, 6 tests): the score is the original
`queuedAt`; a sweep repeated after a crash does not duplicate the member; a ticket that stopped
waiting is left alone; an expired ticket is counted (`requeue.skipped{reason=ticket_expired}`) and an
old event's player is counted (`reason=no_ticket`); another region's rejection is not this engine's;
and a rosterless rejection does not starve the sweep.

**The orchestrator's** `AllocationTimeoutTest` (7) drives the same deadlines with a real Postgres and
a fake clock: the retry wins before the deadline, the deadline rejects with the right reason, the slot
comes back exactly once, and a match already released is not rejected twice. `NoShowTest` (6) is ADR
0034.

## How to test

```bash
# by hand: the whole lifecycle, 10 steps (needs the stack, ~4 min)
bash requests/lifecycle/verify_lifecycle.sh

# the properties, without a running stack (Postgres + Redis)
mvn -o -B -pl services/match-orchestrator test -Dtest=AllocationTimeoutTest
mvn -o -B -pl services/matchmaking-engine test -Dtest=RequeuePreservesScoreTest
mvn -o -B -pl common/common-db test -Dtest=GameServerRepositoryTest
```

The guards were checked by **removing** them. With the roster join back to `JOIN`, the starvation test
fails as quoted above. With `RELEASE_ALLOCATION` back to `status = 'ALLOCATED'`,
`GameServerRepositoryTest.releaseFinishesARunningMatchToo` fails with `NoSuchElementException` (the
release found no row) — the slot leak by the normal path. The requeue script's state check is what
makes the repeat harmless: without it, `aSweepRepeatedAfterACrashDoesNotDuplicateTheMember` would see
two members.

By hand: `SELECT status, rejection_reason, rejected_at, requeued_at FROM match_allocations ORDER BY
confirmed_at DESC LIMIT 5;` after `curl -s localhost:8087/v1/game-servers` shows the pool, and
`ZRANGE queue:1v1:NA 0 3 WITHSCORES` shows the requeued members with their old scores. The
collection's `lifecycle-w23` folder is the same path request by request.

## Trade-offs

- **The decision and the act are two processes and one table.** A rejection is visible in
  `match_allocations` before the players are back (measured: 0.666 s, and a `REJECTED` row with
  `requeued_at` null is a state an operator can read and a crash can repeat). The alternative — the
  orchestrator writing the queue — trades that window for a second owner of the queue's rules.
- **The retry has no budget.** A pool that stays full makes a pair form → confirm → reject → requeue
  → pair again, once per `ALLOCATION_TIMEOUT` cycle. Each cycle costs a pairing and a confirmation, so
  the loop is bounded by the queue's admission cap (W17) and by the confirmation window, not by a
  per-ticket counter. That counter is a product decision (`MelhoriasFutura.md`), and the honest
  statement today is: the system prefers a player waiting in a loop over a player silently dropped.
- **The sweep is FIFO by `rejected_at` with a fixed batch.** Correct for the steady state (one
  rejection is reached within a second), and a backlog delays new rejections behind it: 18 930 rows at
  32/s is ~10 minutes of a *new* rejection waiting, one time, after the fix deploys. A bigger batch
  drains faster and puts more pressure on the database; the batch is a constant, not an env, because
  no measurement yet says where the knee is.
- **The roster is a second copy of the players.** `match_players` (the finished match's record, W13)
  and `match_roster` (the allocator's working set) hold the same people at different moments, written
  by different consumers. They are not merged because a rejected match never becomes a `matches` row
  at all, and the allocator needs its roster before the match is over.
- **`ticket_id` is nullable.** An event produced before W23 carries no ticket, so a replayed history
  still projects and a match from it can be allocated — but it cannot be requeued, and it says so
  (`requeue.skipped{reason=no_ticket}`) instead of guessing.
- **The deadlines are wall-clock and per-row.** `confirmed_at`/`started_at` come from `now()` in
  Postgres, and the watchdog compares them with its own `now()`. Two clocks (the database's and the
  orchestrator's) must agree; they do in one deployment, and a skewed orchestrator would reject early
  or late. A single-clock design would put the deadline in the row's own query (`WHERE confirmed_at <
  now() - interval`), which is what the SQL already does — the value in Java is only the interval.
- **`release` and the watchdog can both give a slot back.** `giveSlotBack` is `UPDATE … WHERE
  allocated > 0` and the release is `WHERE status IN (ALLOCATED, STARTED)`, so the two cannot both
  count the same slot; the row's status is the arbiter.

## Consequences

- A confirmed match is never an orphan: it runs, or it is rejected with a reason and its players are
  back in the queue where they were, typically within a second of the deadline.
- A slot is never leaked by the normal path: a match that never started, a server that died and a
  match that finished all give it back the same way, in the transaction that flips the row.
- `match_allocations` is now the whole lifecycle (`PENDING_ALLOCATION` → `ALLOCATED` → `STARTED` →
  `RELEASED`/`REJECTED`) and the durable hand-off between the two services: W24's `MATCH_FINISHED` and
  the skill application read the same rows.
- A database with pre-W23 history drains its unmarked rejections at the batch rate instead of
  starving — the property is a test, not a hope.
- Two test-hygiene bugs were fixed on the way, both the same shape (a test that inherits the previous
  run's state and answers for it): `PartitionedQueueTest` now clears its `idem:w20-*` keys, and
  `DbBackpressureTest` clears the `match-confirmed:match-w17-*` counter and claim, which outlive the
  pending match and made the next run's confirmations duplicates (`12 expected, 0 written`).
