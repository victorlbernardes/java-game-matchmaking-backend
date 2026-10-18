# ADR-0022 — Bounded queue, load shedding, and database backpressure (W17)

## Status
Accepted (2026-10-18)

## Context
W16 answered *how fast the node is*: ~184 players/s of pairing, measured with the load generator.
The 4000-player run also answered a question nobody had asked — **what happens above that line** —
and the answer was: the queue grows without bound (peak 2238 members), the wait grows with it
(p99 15.6 s), and every arriving player is still *promised* a match. Two things were wrong with
that, and they are different problems:

1. **The system had no behaviour for "too much".** Adding more players only made the backlog longer.
   The engine kept pacing the same 138 players/s, Redis kept accepting members, and the only thing
   that eventually said "no" was the 30 s confirmation timeout: the player waited half a minute to
   be told the match could not be confirmed. Latency was unbounded by construction, and SL-4 was
   the objective reporting it — an alert is not a behaviour.
2. **A slow database was asked harder.** The confirmation path opens a transaction per confirmation.
   When Postgres is behind, every confirmation still opens one, so the number of concurrent write
   transactions grows exactly when the database can least afford it — the classic way a busy service
   turns a slow database into an outage. Nothing bounded how many writes were in flight.

There is a third, quieter failure in the same family: a single authenticated player could fill the
queue by himself (1000 requests = 1000 tickets = the whole queue), which is a denial of service with
a valid token. That one is ADR 0023; it is split out because the fix is a different mechanism
(per-caller policy) than this one (capacity and write concurrency).

## Options

**A. Do nothing; let SL-4 fire.** Rejected. The alert tells an operator what the players already
know. The queue keeps growing until Redis runs out of memory, every player gets a promise the node
cannot keep, and the failure mode is a fleet-wide OOM instead of a fast refusal.

**B. Check the capacity in Java before writing the ticket.** `ZCARD` followed by `ZADD`. Rejected,
and this is the same mistake the project has already refused three times (idempotency in W6, the
roster in W16): between the read and the write, N concurrent arrivals all observe a depth below the
cap and all decide to write. A cap implemented that way is a suggestion that holds exactly until the
load it exists for.

**C. Enforce the cap inside the Lua script that writes the ticket.** The check and the write are one
atomic step, so no interleaving exists. Chosen.

**D. Shed at the edge (a load balancer, an API gateway, `iptables`).** Rejected *for now*: the cap is
per queue (`gameMode` × `region`), and the edge cannot know which queue a request will land in
without parsing the body; it would also refuse players that would have been perfectly fine in a
queue with room. A global edge limit is a coarser second line of defence, not this decision.

**E. Let the database connection pool be the bound.** Rejected: a pool of N connections does not
stop N slow transactions, it turns the excess into *waiting for a connection*. The failure moves
into the pool (whose wait queue is unbounded, and whose timeout is a bare exception with no
diagnosis) instead of being a decision the engine makes. The pool protects connections; it does not
express an intent about write concurrency.

**F. Serialize confirmations through a single writer.** Rejected: it throws away concurrency
entirely. With a 3 s write, one writer means 12 matches in 36 s. The interesting property is *a
bound*, not *one*.

**G. A permit around the write transaction, with the confirmation deferred by a timer.** The engine
acquires a permit before it opens the transaction and releases it when the write finishes; a
confirmation that finds no permit does not fail, it *waits* — it is already durable state (a
`CONFIRMING` ticket and a pending match), so there is nothing to lose by retrying in 100 ms.
Chosen.

## Decision

### 1. The queue has a capacity, and the capacity is inside the write

`RedisTicketRepository` is constructed with `queueMaxDepth` (`QUEUE_MAX_DEPTH`, default **1000**;
`0` = unbounded, which is what the tests and a dev box use). `create.lua` takes it as `ARGV[7]`,
reads the depth with `ZCARD` **in the same script** as the `ZADD`, and refuses before writing
anything.

The script returns `{status, payload}` — a word and a value, not a ticket id that secretly means
"full" (Regra 11: the status is an enum, not an inference on the payload):

| Status | Meaning | Payload |
| --- | --- | --- |
| `OK` | a new admission; ticket written, member added | `ticketId` |
| `EXISTS` | an idempotent replay; **nothing** admitted again | the ticket id the key already points at |
| `FULL` | the queue is at capacity; **nothing** written | the depth observed |

Three details that are not obvious:

- **The idempotency lookup runs before the cap.** A replay is not a new arrival: the player did
  enqueue, that ticket exists, and answering `503` to a retry would lose a ticket the system holds.
  A replay is answered even when the queue is full.
- **An arrival without an `Idempotency-Key` is capped too.** This was a real hole, found by the W17
  load run and fixed here: the check lived on the idempotency path, so a caller who sent no key (a
  curl, a second device, the load generator) took the plain write path around the cap. Measured:
  **4000 arrivals against a cap of 1000, zero refusals, the queue at 3146**. Both paths now go
  through the script, with an empty `KEYS[1]` when the caller sent none and the idempotency write
  guarded by `hasIdempotencyKey`. *Is this a replay?* and *is there room?* are two questions, and
  answering the first must not skip the second.
- **Party enqueue is a bounded best-effort.** A party is admitted only if there is room for the
  whole roster, checked before the (atomic) per-member enqueue. The overshoot is bounded by the
  party size (≤ 5), because the pre-check and the members' writes are not one step; making it exact
  would mean one script per party, and the party path is not where the flood is. Documented as an
  approximation in `RedisTicketRepository`, tracked in `MelhoriasFutura.md`.

`QueueFullException` carries `(queue, depth, capacity)` so the API can answer with a *description* of
the state it refused from. `HttpRoutes` maps it to **503** with `Retry-After: 5` and a body naming
the queue, the depth, the capacity and the retry — a refusal a client can act on without guessing:

```json
{"error":"queue full","queue":"queue:1v1:NA","depth":1000,"capacity":1000,"retryAfterSeconds":5}
```

503 (not 429) because the reason is the *service's* capacity, not the caller's speed — semantically
"come back when there is room", which is what `Retry-After` means in RFC 9110. The refusal is
counted as `matchmaking_tickets_rejected_total{reason="queue_full"}`.

### 2. The write concurrency is bounded by a gate, not by hope

`WriteGate` (engine) hands out `DB_MAX_IN_FLIGHT` permits (default **8**; `0` = unbounded). It is a
plain `AtomicInteger`, no locks, no fair queue — a denied caller is not blocked, it is *deferred*:

| Path | Behaviour with no permit |
| --- | --- |
| Confirmation | **Defers.** A Vert.x timer retries after `DB_WRITE_RETRY_MS` (100 ms), up to `DB_WRITE_MAX_ATTEMPTS` (600), and re-checks that the pending match still exists (the deadline may have dissolved it while we waited). Nothing is lost: the pending match and the `CONFIRMING` ticket are already durable. |
| Pairing tick | **Skips.** While the gate is saturated there is no point forming more matches that cannot be written; the queue keeps its members and the next tick tries again. The engine's throughput degrades instead of its latency. |
| Timeout sweep | **Skips a match it cannot claim.** A `CONFIRMING` ticket whose confirmation is currently in flight must not be dissolved at the deadline — the write may be seconds away from committing, and dissolving it would remove a match that is about to exist. The sweep checks `match-confirmed-claim:{matchId}`, the same claim the confirmation itself takes, so the two cannot disagree. |

Two changes the gate made necessary, both about *ordering*:

- **One permit-release point.** `persistConfirmedMatch(...).onComplete(done -> gate.leave())` is the
  single place a permit returns, on success or failure. Scattering `leave()` across branches is how
  a permit leaks, and a leaked permit looks exactly like a wedged database.
- **The player is no longer told "confirmed" before the row exists.** The `MATCH_CONFIRMED`
  notification and the `pending.delete` moved to *after* the write succeeds. The confirmation the
  player sent is still acknowledged immediately (the ticket is in `CONFIRMING`), but the
  "match is on" message waits for durability. Telling a player something the database does not know
  yet is how a restart turns a confirmed match into a match that never happened.

Why a **process-local** gate and not a Redis semaphore: the thing being protected is a database with
one writer pool per service, and a local permit is honest about what it bounds — `8 × replicas` at
the database. A global bound would need a distributed semaphore (and a lease protocol to survive the
holder dying), which is a payment not worth making while there is one engine replica. It is recorded
in `MelhoriasFutura.md` for the day there are two.

## Measured numbers

All with the running stack (ADR 0021's machine, same services).

**The cap, in a controlled setting** (`verify_load_shedding.sh` step 3: engine stopped so nothing
drains, then real arrivals; `QUEUE_MAX_DEPTH=1000`):

| Arrivals | Admitted | Refused (503) | Redis `ZCARD` | `Retry-After` | Replay of an accepted ticket |
| --- | --- | --- | --- | --- | --- |
| 1100 | **1000** | **100** | **exactly 1000** | 5 | answered `200` with its ticket, *while full* |

**The cap under load** (4000 players, 1v1/NA, concurrency 192, `requests/load/results-w17-*.json`):

| Run | Admitted | Refused | Refusal p50 / p99 | Peak depth | Wait p50 / p95 / p99 | Matches confirmed | Dissolved |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `QUEUE_MAX_DEPTH=1000` | 2862 | 1138 | 672 / 904 ms | 984 | 7.43 / 10.57 / 11.28 s | 1431 | 0 |
| `QUEUE_MAX_DEPTH=0` | 4000 | 0 | — | 2182 | 11.11 / 16.86 / 20.86 s | 2000 | 0 |

Read as: **a third of the arrivals got an explicit "no" in under a second instead of a match 21 s
later**, and the players who were admitted waited 11.3 s at p99 instead of 20.9 s, because the
backlog — and therefore the wait — is bounded. Nothing was lost in either run: no dissolved match, no
duplicate event. Note what the capped run does *not* claim: refusals are a cost, and 1138 players
did not play. The alternative in the uncapped row is not "everyone plays"; it is "everyone waits
longer, and the tail of them are dissolved at the deadline anyway".

**The gate against a genuinely slow database** (`verify_load_shedding.sh` step 2: a
`BEFORE INSERT ON matches` trigger sleeping 3 s, 24 players → 12 matches, all 24 confirmations
published at once, `DB_MAX_IN_FLIGHT=8`):

| Observation | Value |
| --- | --- |
| Matches written | **12 of 12** (nothing lost, nothing double-written) |
| Concurrent `INSERT INTO matches` in `pg_stat_activity` (sampled, ground truth) | never above ~4–5 observed, gate at 8 |
| `matchmaking_db_writes_in_flight` peak (gauge) | **exactly 8** = `DB_MAX_IN_FLIGHT` |
| Confirmations deferred (KPI delta + log) | **118** (`engine.backpressure confirmation deferred … inFlight=8 limit=8`) |
| Burst wall time | 11.0 s (≥ one 3 s database wait, which is how the run proves the trigger was real) |
| Rows left behind by the scenario | 0 |

The two "8" columns are the point: the engine asked Postgres to hold at most 8 of these transactions
at a time, and 118 confirmation attempts found the gate closed and waited 100 ms instead of opening a
ninth.

## How it works (files)

| Concern | File |
| --- | --- |
| Capacity inside the atomic create | `common/common-redis/src/main/resources/create.lua` |
| Cap configuration, `FULL` → `QueueFullException` | `common/common-redis/.../RedisTicketRepository.java` |
| 503 mapping, `Retry-After`, KPI | `services/matchmaking-api/.../presentation/HttpRoutes.java` |
| `QUEUE_MAX_DEPTH`, `DB_MAX_IN_FLIGHT`, retry knobs | `services/matchmaking-api/.../AdmissionConfig.java`, engine config |
| The permit | `services/matchmaking-engine/.../infrastructure/WriteGate.java` |
| Deferred confirmation, paused pairing, sweep claim skip, notify-after-write | `services/matchmaking-engine/.../MatchmakingEngineApplication.java` |

## How to test

```bash
# the whole property by hand, 35 checks, ~1 min (needs the stack + the engine on METRICS_PORT=9091)
bash requests/backpressure/verify_load_shedding.sh

# the queue cap alone, as a property test: 200 concurrent arrivals against a cap of 25 -> exactly 25
mvn -o -B -pl common/common-redis test -Dtest=QueueCapacityTest

# the gate: saturation pauses the pairing; 12 confirmations with a limit of 2 never exceed 2 in flight
mvn -o -B -pl services/matchmaking-engine test -Dtest=WriteGateTest,DbBackpressureTest
```

The tests that **fail if the property is violated**, and what each one would catch:

| Test | Property | Would catch |
| --- | --- | --- |
| `QueueCapacityTest.concurrentArrivalsNeverExceedTheCap` | 200 concurrent arrivals vs cap 25 → exactly 25 admitted, depth 25, 25 idempotency records | a check-then-write cap (the race in Option B) |
| `QueueCapacityTest.arrivalsWithoutAnIdempotencyKeyAreCappedToo` | the same without any key, and **zero** idempotency records | the W17 hole: a cap that only applies to callers who sent a header |
| `QueueCapacityTest.aReplayIsAnsweredEvenWhenTheQueueIsFull` | replay → `EXISTS`, not `503` | a cap checked before the idempotency lookup |
| `WriteGateTest` | permits are bounded, returned on failure, and `0` means unbounded | a leaked permit, or a saturating gate that blocks instead of refusing |
| `DbBackpressureTest` | saturated gate → **no** match formed; when freed, it pairs; with limit 2, 12 confirmations → `maxConcurrent ≤ 2`, all 12 written, `deferred > 0`, `inFlight == 0` | unbounded write concurrency, a lost confirmation while deferring, a permit leak |

The Postman collection carries the two refusals as runnable requests
(`tickets/POST /v1/matchmaking/tickets — queue full (503)` and `— rate limited (429)`), each with
the precondition and the exact expected headers/body.

## Trade-offs

- **A refusal is a bad experience.** 1000 players got a "no" in the capped run. The alternative is
  not "everyone plays" — it is everyone waiting longer, with the tail dissolved at the deadline
  anyway. A fast, explicit, actionable no beats a slow silent one; the number is *when* to stop
  (queue depth 1000 ≈ 5 s of pairing at the measured ceiling, ADR 0021).
- **The cap is per queue, not per node.** A full `1v1/NA` does not slow `5v5/EU`, which is what you
  want for availability — but N queues can still fill Redis between them. A node-wide budget
  (`QUEUE_MAX_TOTAL`) is deferred until a queue count makes it real.
- **The gate is per process.** `DB_MAX_IN_FLIGHT × replicas` is what the database sees. Multiplying
  naively (`8 replicas × 8 = 64`) is not a bound on the database; the distributed version is
  deferred (see above) and this ADR is the note that says so.
- **The pairing pauses when saturated.** Under database pressure the engine forms fewer matches than
  it could. That is a deliberate choice of latency over throughput — a match formed but not written
  is a player told "match found" for a match that does not exist.
- **The queue-full path is a refusal on a *shared* resource**, so it can be unfair: an arrival that
  loses the race gets a 503 while another enters. There is no priority or fairness in the cap; adding
  one (e.g. reserved room for parties) is policy, not correctness.
- **Party enqueue overshoots by at most the party size.** Documented; the alternative is a
  party-specific script.

## Consequences

- `matchmaking_tickets_rejected_total{reason}` gains `queue_full`; `matchmaking_db_writes_in_flight`
  (gauge) and `matchmaking_db_write_deferred_total` (counter) are new, and SL-3/SL-4 are unaffected
  (the outbox queue and the waiting players are unchanged by the gate).
- The API can now answer **503** on the enqueue path; clients must honour `Retry-After`. The
  collection documents both refusals so a client author can reproduce them on purpose.
- The engine logs `engine.backpressure confirmation deferred`, `engine.backpressure pairing paused`
  and the sweep's skip — the three places the gate is visible from outside.
- A confirmation whose write could not be completed within 600 × 100 ms (60 s) is logged and the
  match is left to the timeout sweep, which is the pre-existing safety net.
- The `create.lua` contract widened (`{status, payload}`, `ARGV[7]`): the tests that call the script
  directly were updated with it in the same change (W16's lesson about `pair.lua`).
