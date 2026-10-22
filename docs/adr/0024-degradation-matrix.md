# ADR-0024 — The degradation matrix: what happens when a dependency goes away (W18)

## Status
Accepted (2026-10-18)

## Context

W15–W17 gave the system numbers (KPIs, SLOs, a ceiling) and behaviours for *too much* (a bounded
queue, a rate limit, a bound on write concurrency). What none of them asked was the other half of the
question: **what does the system do when something it depends on stops answering?** Not the player
arriving too fast — the dependency on the other end of the wire.

The answer, measured on the local stack before this ADR (each row: stop the dependency, drive the
affected endpoint, read the state it leaves behind):

| Dependency | What the system said | What it left behind |
| --- | --- | --- |
| Redis, **restarted** (script cache **and** Pub/Sub gone) | two failures stacked. The script cache: `POST /v1/matchmaking/tickets` → **500 `NOSCRIPT No matching script. Please use EVAL.`**, permanently, and the matching loop stopped with it. The subscriptions: `RedisSubscriber` connected **once**, so the restart took `match_confirmations` (engine) and `player_notifications` (API → WebSocket) with the socket — **found by running this sprint's own scenario**: after `docker restart redis` the engine received **no confirmation at all**, logged nothing and looked healthy, and every match formed afterwards dissolved at its 30 s deadline (`match.dissolved … reason=timeout`); phases 3 and 4 of that first run failed for a reason that was not theirs | Two services that had to be restarted because a cache was — and, for confirmations, a silent 30 s timeout per match until somebody noticed. Restarting Redis is the most routine operation there is. |
| Redis, **down** | same enqueue → **500 `{"error":"Connection refused: localhost/127.0.0.1:6379"}`**; `GET /health` → **200 UP** | A wrong status code (the client is told it did something wrong), the internal address of the dependency in the body, and a liveness probe that is green while nothing the instance exists for can work. The rate limiter *silently failed open*. |
| Postgres, **down** | the confirmation answered "fine": both tickets went **`CONFIRMED` in 3 s**; `engine.error stage=persistMatchWithEvent` SEVERE | **0 rows** for a match the players were told existed, the pending match dissolved by the sweep at its deadline (~55 s), and both tickets stuck `CONFIRMED` forever — a state no code path repairs, because the sweep only requeues tickets that are still `CONFIRMING`. `GET /history` → 500 with the driver's message. And **neither the API nor the engine could start** (blocking Flyway migration, empty log, nothing listening). |
| Kafka, **down** | a confirmation still wrote the match… | **before W14** nothing: the publish was on the confirmation path. Since ADR 0018 the match and its event are one transaction and a relay publishes; the Kafka-down case had never been *exercised* end to end. |
| Schema Registry, **down** | — | W14's claim is that a registry outage cannot refuse a confirmation (the schema id is resolved at publish time). Never exercised. |

Three of those rows are the same mistake in different clothes: **the system did not distinguish "I am
broken" from "something I need is not there"**, and it did not distinguish *liveness* from
*readiness*. A `500` with the driver's message is both a wrong instruction to the client (do not
retry) and free reconnaissance for whoever reads it. An enqueue that fails because Redis restarted is
an outage wearing a maintenance window.

The fourth is worse because it is silent: **a subscription that dies with its connection leaves a
healthy-looking process receiving nothing.** There is no exception to catch and no 500 to read — the
evidence is the message that never arrives, and the cost is one dissolved match per 30 s window.

## Options

**A. Leave it: dependencies are the operator's problem.** Rejected. Two of the rows above ("cannot
start", "stranded tickets") are states that no amount of operator attention repairs from outside —
they are the service's own logic deciding the wrong thing.

**B. Retry everything / make every call tolerant.** Rejected as a *policy*: retrying into a dead
dependency multiplies the load on the thing that is already failing, and "tolerant" without a
definition of the failed state is just a longer timeout. Retries are used here only where the state
is already durable and the retry is bounded (the confirmation write, ADR 0022's gate; the outbox
relay, ADR 0018).

**C. A per-dependency degradation matrix: for each dependency, the state the system is allowed to
reach, the code it answers, and the recovery path.** Chosen. It is written down as a table in
`requests/README.md`, each row has a runnable scenario that fails when the row stops holding, and each
row has a test that fails when the *property* behind it regresses.

**D. Circuit breakers per dependency.** Deferred, not rejected: a breaker is an optimisation on top of
this matrix (stop asking for a while instead of asking and failing fast). It needs the matrix first —
what a breaker *opens onto* is exactly the behaviour this ADR defines. `MelhoriasFutura.md` item 11.

**E. Make Postgres a soft dependency** (serve from Redis, reconcile later). Rejected: the roster and
the match are the source of truth for money-adjacent facts (who played with whom). Serving a
confirmation the database has not accepted is the bug this ADR removes, not a feature to add
carefully.

## Decision

The matrix, as the system now behaves. Each row: what the dependency's absence is **allowed** to
affect, what it must **not** affect, and what the recovery is.

| Dependency | Code | Client-facing answer | Hot state | Source of truth | Recovery |
| --- | --- | --- | --- | --- | --- |
| Redis **restarted** (script cache and Pub/Sub gone) | transparent | normal (201/200) **and a confirmation still completes** | unchanged; the next script call reloads | n/a | none needed: the first `EVALSHA` after the restart reloads the script and retries **once**; the subscriber reconnects after 1 s and re-subscribes |
| Redis **down** | 503 `{"error":"hot state unavailable","dependency":"redis"}` + `Retry-After: 1` | enqueue/ticket/party/history-of-live-state: 503 in ms, no internal address | unwritable — and the system says so instead of pretending | Postgres endpoints unaffected (`/history` still served) | automatic on the next request; nothing to replay |
| Postgres **down** (runtime) | confirmation keeps the players in `CONFIRMING`, no row, no `CONFIRMED`; `engine.error … confirmation write failed`; retried by the gate's timer | ticket reads still work (Redis); history 503 `{"dependency":"postgres"}` | **not** moved ahead of the durable fact | the write is retried until it lands (≤ 600 × 100 ms) or the deadline sweep requeues the players | the pending write completes when the database returns; otherwise the sweep |
| Postgres **down** (boot) | the process exits with `db.migrate.failed url=…` | nothing listening (a not-starting pod is honest) | n/a | n/a | the driver gives up after `connectTimeout=5` instead of hanging forever |
| Kafka **down** | confirmation still succeeds; `matchmaking_outbox_pending` grows | normal (the match exists); only the **announcement** is late | normal | match + event written in one transaction (`outbox_events`) | the relay publishes when the broker returns; `PUBLISHED` only after the ack |
| Schema Registry **down** | same as Kafka | normal | normal | same transaction | the schema id is resolved at publish time, so a registry outage can delay an event but never refuse a confirmation |
| **Self** (`/health`) | stays 200 while a dependency is down | — | — | — | liveness is about the *process*; killing it does not fix Redis |
| **Readiness** (`/health/ready`) | 503 + `{"checks":{"redis":"DOWN","postgres":"UP"}}` | — | — | — | the orchestrator's cue: stop routing here, do not restart here |

### 1. A script cache is not a resource the services own

`RedisScript` (common-redis) owns *one* script: it loads it with `SCRIPT LOAD`, remembers the SHA, and
on `EVALSHA` failure recognises `NOSCRIPT`, reloads, and retries **once**. The retry is once because
the second `NOSCRIPT` cannot be a lost cache (the script was just loaded) — it is a real error, and
retrying it would be a loop. Every script caller goes through it (`create.lua`, `ratelimit.lua`,
`pair.lua`), so there is no path that remembers a SHA without being able to rebuild it.

This is deliberately *not* "call `SCRIPT LOAD` on every request" (a round trip per request to save
nothing) and *not* "cache forever and hope" (the measured outage).

### 1b. A subscription is not a resource one connection owns

The scenario found the other half of the restart, one layer below the scripts: **Pub/Sub state lives
on the connection**. `RedisSubscriber` connected once and never looked again, so a restart, a failover
or an idle timeout left the process subscribed to nothing — and there is no exception to catch,
because the failure is a message that never arrives. The engine's `match_confirmations` and the API's
`player_notifications` (the fan-out to the WebSockets) were both in that state.

`RedisSubscriber` now repairs the subscription: on connection end, on a connection error or on a
failed `SUBSCRIBE`, it reconnects after 1 s and subscribes again, forever. Three details are the
design: the delay is **flat**, not a backoff sequence (an attempt is one TCP connect to a dependency
that is either back or has nothing else working either); an `AtomicBoolean` per connection makes the
three signals of one death (the response stream ending, the pending subscribe failing, an exception)
schedule **one** repair, not three subscriptions; and the log names the attempt
(`redis.subscribed channel=… attempt=N`), so "did it ever come back?" is answerable from the log.

What it cannot do is deliver what was published while it was down — neither Redis nor this class
buffers. That is the delivery half of W16's open deviation (`MelhoriasFutura.md` item 7, the
re-presentation contract); the lifetime half is what this section fixes, and it is the half that was
silently costing every match formed after a restart.

### 2. Honest failures at the edge

`HttpRoutes` classifies a failure into three, and the classification is the whole decision:

| Classification | Detection | Answer |
| --- | --- | --- |
| Capacity | `QueueFullException` (W17) | 503 `queue full` + `Retry-After: 5` + the queue's numbers |
| Dependency unreachable | `ConnectException` anywhere in the cause chain, **or** the client's own wording (`NOSCRIPT`, `refused`, `connection has been closed`, `connection is closed`, `connection reset`, `broken pipe`, `max wait queue size` — every entry observed in a real outage) | 503 `… unavailable` + `Retry-After: 1` + `dependency` — **never** the driver's message |
| Everything else | — | 500 `{"error":"internal error"}`; the details go to the log, not the body |

The message is never echoed to the client: it names hosts, ports and (for SQL) table shapes. The log
line (`http.dependency.unavailable dependency=redis path=… reason=…`) keeps it for the operator. Every
dependency refusal increments `matchmaking_dependency_errors_total{dependency=…}`, so "how often is
Redis the reason?" is a query and not an investigation.

`/health` stays liveness (the process is up) and `/health/ready` answers **readiness**: it probes each
dependency (`PING` for Redis, `SELECT 1` for Postgres) with a 1 s cap per check and reports each one
by name, 503 when any is down. Two probes, because "the process is alive" and "this instance can
serve" are different questions with different answers to the router — collapsing them is how a
Kubernetes deployment gets restarted for a Redis outage, or kept in the rotation during one.

### 3. The durable fact goes before the hot state

`persistConfirmedMatch` now writes the match and its event (`outbox_events`, one transaction) and only
**then** moves the tickets to `CONFIRMED`, deletes the pending match and notifies the players. A failed
write retries through the same gate and timer as a *deferred* write (ADR 0022), so a database that is
away for a moment costs the players nothing.

The ordering is the whole point: `CONFIRMED` is a *derived* state. Any order that writes the
derivation first creates a state that claims a fact the source of truth does not have, and no recoverer
can tell it apart from a real one. Written the other way, the failure leaves what it left: tickets
`CONFIRMING`, the pending match alive, and a sweep that requeues both players at the deadline — or a
retry that finishes the same match when the database returns.

### 4. The boot path fails fast and loudly

`DatabaseMigrator` adds the driver's `connectTimeout=5` when the JDBC URL does not carry one (a
black-holed host must not hang a rollout) and logs `db.migrate.failed url=…` before rethrowing. The
database is a *startup* dependency of both services; the honest behaviour is to not come up, not to
stay half-up with nothing listening.

## Measured numbers (local stack, 2026-10-18)

`requests/resilience/verify_degradation.sh`, 5 phases, the whole stack alive, with traffic flowing.

**Before** (the table in Context) vs **after**:

| Scenario | Before | After (measured) |
| --- | --- | --- |
| Redis restarted, then enqueue | 500 `NOSCRIPT No matching script. Please use EVAL.` — forever, both services needing a restart | `201` on the first request after the restart, the engine pairs again (`match-884c631e`), the confirmation is received and written **in 0 s** and both tickets reach `CONFIRMED` — with the API (pid 65469) and the engine (pid 65507) **unchanged** |
| Redis down, 20 arrivals | 500, body with `localhost/127.0.0.1:6379`, `/health` 200 UP | **20/20 × 503**, p50 **8.7 ms**, p99 **15.1 ms**, max **67.7 ms** (a refusal, not a wait), `Retry-After: 1`, body names `redis` and carries no address; `/health` 200, `/health/ready` **503 naming `redis`**; `matchmaking_dependency_errors_total{dependency="redis"}` **+21**; the first arrival after Redis returns is admitted |
| Postgres down during a confirmation | 2× `CONFIRMED` with **0 rows**, pending match swept, tickets stranded forever | both confirmations published, the engine **claims** the match, tickets stay `CONFIRMING`, **0 rows**, pending match alive, `engine.error … confirmation write failed matchId=…`; when Postgres returns the retry writes the row (**0 s** — it landed on the first attempt after the database was back) and only then do the tickets reach `CONFIRMED` |
| Postgres down, `/history` | 500 with `Connection to localhost:5432 refused…` | **503** `{"dependency":"postgres"}`, no address; readiness names `postgres` as `DOWN` |
| Postgres down at boot | no log, nothing listening, indistinguishable from a slow boot | `db.migrate.failed url=…` and the process exits (≤ ~5 s with the connect timeout) |
| Kafka down before a confirmation | publish on the confirmation path (**pre-W14**) | match written in Postgres, event `PENDING` in the outbox, ticket **`CONFIRMED`**; published **15 s** after the broker returned, **0 PARKED**, no duplicates |
| Schema Registry down at engine start | — | the engine starts (a publish-time dependency), the match is written, the event waits in the outbox and is published **6 s** after the registry returned |

`requests/resilience/verify_degradation.sh`: **45 checks, 0 failures**, ~5 min, whole stack alive.
The first run of the same script, before the subscriber and detection fixes above, was **33 passed /
7 failed** — every failure caused by the dead subscription (phases 3 and 4) or by the `500`-vs-`503`
detection, which is what turned "a scenario" into "the evidence for two defects".

## How it works (files)

| Concern | File |
| --- | --- |
| Reload-once script owner | `common/common-redis/.../RedisScript.java` |
| Callers moved onto it | `common/common-redis/.../RedisTicketRepository.java`, `RateLimiter.java`, `services/matchmaking-engine/.../MatchmakingEngineApplication.java` (`PAIR_SCRIPT`) |
| Self-repairing subscription | `common/common-redis/.../RedisSubscriber.java` |
| 503 mapping, generic 500, `/health/ready` route | `services/matchmaking-api/.../presentation/HttpRoutes.java` |
| Readiness probes | `services/matchmaking-api/.../presentation/Readiness.java`; wiring in `MatchmakingApiApplication` |
| `SELECT 1` probe | `common/common-db/.../MatchRepository.java` |
| Boot-time fail-fast | `common/common-db/.../DatabaseMigrator.java` |
| Durable-before-hot-state ordering + retry | `services/matchmaking-engine/.../MatchmakingEngineApplication.java` (`persistConfirmedMatch`, `confirm`) |
| The counter | `common/common-metrics/.../MetricNames.java` (`DEPENDENCY_ERRORS`, `TAG_DEPENDENCY`) |
| The matrix, by hand | `requests/resilience/verify_degradation.sh` (+ `degradation_probe.py`) |

## How to test

```bash
# the whole matrix, 5 phases, ~4 min (needs the full stack; each phase restarts its dependency)
bash requests/resilience/verify_degradation.sh
SKIP_SCHEMA_PHASE=1 bash requests/resilience/verify_degradation.sh   # the 4 fast phases

# the script cache alone: SCRIPT FLUSH, then the operations must still work
mvn -o -B -pl common/common-redis test -Dtest=RedisScriptReloadTest

# the subscription alone: kill the connection (what a restart does) and the next message must arrive
mvn -o -B -pl common/common-redis test -Dtest=RedisSubscriberReconnectTest

# the ordering alone: a source of truth that refuses every write
mvn -o -B -pl services/matchmaking-engine test -Dtest=ConfirmationOrderingTest

# the edge alone: 503 without the address, readiness naming the dependency, a bug still a 500
mvn -o -B -pl services/matchmaking-api test -Dtest=DependencyDegradationTest

# the boot path alone: the connect timeout is added, an unreachable database ends the boot
mvn -o -B -pl common/common-db test -Dtest=DatabaseMigratorTest
```

The tests that **fail if the property is violated**:

| Test | Property | Would catch |
| --- | --- | --- |
| `RedisScriptReloadTest.anEnqueueSurvivesTheServerForgettingTheScript` | `SCRIPT FLUSH` → the next enqueue is `201` | the measured `NOSCRIPT` outage (verified: the test errors with `NOSCRIPT` when the reload is removed) |
| `RedisScriptReloadTest.theRateLimiterStillLimitsAfterTheServerForgetsTheScript` | after a flush the bucket still refuses once spent | a limiter that "fails open" permanently |
| `RedisScriptReloadTest.theReloadIsARepairNotANewVersion` | the reloaded SHA equals the original | a reload that silently changes the script |
| `RedisSubscriberReconnectTest.aKilledSubscriptionIsRebuiltAndStillDelivers` | `CLIENT KILL TYPE pubsub` (what a restart does to the socket) → the next message still arrives, exactly once | the measured dead subscription (verified: the test times out at 20 s when the repair is removed) |
| `ConfirmationOrderingTest.aFailedDurableWriteLeavesTheTicketsConfirmingAndTheSweepRequeuesThem` | write refused → tickets `CONFIRMING`, no `CONFIRMED`, then the sweep → 2/2 back in the queue | the measured stranded `CONFIRMED` ticket (verified: fails with the old order) |
| `ConfirmationOrderingTest.aSuccessfulWriteMovesTheTicketsToConfirmedAfterTheFact` | the happy path still confirms, once, with the roster | an ordering fix that broke the normal path |
| `DependencyDegradationTest.anUnavailableHotStateIsA503WithRetryAfterAndNoInternalAddress` | 503 + `Retry-After: 1` + `dependency=redis` + no address + counted | the measured `500` with `localhost:6379` |
| `DependencyDegradationTest.readinessNamesTheDependencyThatIsDownAndLivenessStaysUp` | ready 503 naming redis, live 200 | a liveness probe that goes red with a dependency (and gets the pod killed) |
| `DependencyDegradationTest.aBugIsStillA500AndTheBodyDoesNotCarryTheDriverMessage` | a bug is 500, details only in the log | a catch-all that turns every bug into a dependency outage, or leaks SQL |
| `DependencyDegradationTest.aPooledConnectionTheServerClosedIsAlsoA503` | a pool that hands back a connection the server closed is `503`, not `500` | a detector that only reads "connection refused" (the live failure said `This connection has been closed.`) |
| `DatabaseMigratorTest.anUnreachableDatabaseFailsInsteadOfHanging` | boot fails instead of hanging | the measured "no log, no listener" |

## Trade-offs

- **503 for a dead dependency and 503 for a full queue are the same code with different bodies.** A
  client that only looks at the status cannot tell "come back in 1 s" from "come back in 5 s"; hence
  `retryAfterSeconds` in both bodies and the `dependency`/`queue` field that names the reason. Honest,
  but a client must read the body.
- **A retried confirmation is a write attempt that failed and will be tried again**, which means a
  database that is down for minutes sees up to 10 attempts/s per in-flight match. It is bounded by the
  gate (8 in flight) and by `DB_WRITE_MAX_ATTEMPTS`, but it is load aimed at a service that is already
  unwell. A circuit breaker (Option D) is the next step, and this ADR is what makes it safe to add.
- **The readiness probe calls two dependencies per request.** Cheap (`PING`, `SELECT 1`) but not free;
  it is a probe, meant to be called at probe frequency (seconds), not per player request.
- **The boot path is still a hard dependency on Postgres.** A service that cannot reach its schema
  does not start. That is a deliberate choice over "start and be useless", but it does mean a database
  outage during a deploy takes the deploy with it.
- **A message published while the subscription is down is lost.** Neither Redis nor `RedisSubscriber`
  buffers, so re-subscribing repairs the *lifetime* of the subscription, not the delivery of what was
  sent during the gap. For confirmations the cost is bounded (the match dissolves at its deadline and
  the players are requeued); for a player notification it is the open W16 deviation
  (`MelhoriasFutura.md` item 7). A durable per-player queue is the fix, and it is a contract change.
- **The reconnect delay is flat, and the repair is untested against a *partial* outage** (a Redis that
  answers `PING` but drops subscriptions). One second between attempts against a dependency that is
  either back or wholly down is the right trade here; a flapping server would see one connect/s from
  every service, which is worth revisiting with a breaker (Option D) rather than with a backoff
  sequence guessed at now.
- **`Returning`/SQL errors are hidden from the client.** Debugging from the client side becomes
  "500 internal error, check the log" — a real cost, paid to stop leaking hosts, ports and shapes.
- **The claim key still delays the sweep.** A confirmation that fails to write holds
  `match-confirmed-claim:{matchId}` until its TTL, so the sweep cannot requeue the players until it
  expires; the retry path (the common case) is what makes that invisible. Worth revisiting if the
  retry window and the claim's TTL ever disagree — `MelhoriasFutura.md` item 12.
- **Out of scope, deliberately:** re-presenting a lost match notification (the engine-side half needs
  the heartbeat/`GET` contract; `MelhoriasFutura.md` item 7), a distributed `WriteGate` (ADR 0022), and
  a **network partition** (split brain, `unhealthy` members that are still reachable) — that one needs
  more than one replica of a service, which W19–W21 bring.

## Consequences

- A Redis restart is now a non-event for the services — the scripts reload and the subscriptions are
  rebuilt, so nothing under `services/` needs restarting when the cache does. A Redis outage is a 503
  that says so, with the internal address removed from every response and counted per dependency.
- The engine and the API no longer go deaf silently: both subscribe through `RedisSubscriber`, so a
  dropped connection is a re-subscription one second later instead of a process that keeps running
  while receiving nothing (`redis.subscribed … attempt=N` in the log is the evidence).
- `matchmaking_dependency_errors_total{dependency}` is the new KPI; SL-1..SL-4 are unaffected (a
  dependency outage that stops matching shows up in the existing wait/queue gauges, which is the
  honest signal).
- The API gained `GET /health/ready`; deployments should route on it and keep killing on `/health`.
- A confirmation now retries a *failed* write as well as a deferred one, and the notification/delete
  happen after the row exists — so a player can no longer be told about a match the database never got.
- Both services fail loudly at boot when Postgres is unreachable (`mvn … exec:java` exits; CI and the
  runbook's start order already assume the database is up first).
- The matrix in `requests/README.md` is the contract document; a new dependency means a new row, a
  scenario phase and a test — the same rule the ADR index follows for new ADRs.
