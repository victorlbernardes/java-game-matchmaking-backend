# ADR-0039 — Regions as failure domains, and the placement that prices the distance (W21)

## Status
Accepted (2026-10-20)

## Context

Until W20 the system was one deployment: one queue per (mode, region) inside **one** Redis, one
Postgres, one Kafka. The `region` on a ticket was a **label** — a value used to build a key
(`queue:1v1:NA`) and a column in the durable log — never a boundary. Two consequences followed:

1. **A region could not fail alone.** Every region's hot state lived in the same server, so losing
   it lost every region. `matchmaking-api` could be scaled horizontally (ADR 0027) but only
   *around* one shared Redis, not *across* independent ones.
2. **Distance was nobody's business.** A player could ask to be queued in a region it cannot reach,
   and the system accepted it: `region` was a request, and the only thing that ever checked it was
   the client's own patience. A 1v1 match formed across an ocean is a match that will dissolve at
   the confirmation deadline, and nobody measured it as such.

W21 makes the region a **deployment boundary**: each region runs its own API, its own engine and its
own Redis; Postgres and Kafka stay shared, because the durable log and the event stream are global
by requirement (the white-paper asks for one history and one schema, and the orchestrator reads one
topic). The sprint then has to answer the question the boundary creates: **which region should take
this player**, with numbers the server produces and the client cannot lower.

## Options

### Failure domain

**A. One Redis for all regions, partitions only (W20's shape).** Scale the queue, keep it global.
Rejected as the failure domain: `matchmaking_queue_partition_depth` spreads *load* across keys, but
all keys live in one server — a Redis restart still takes every region down, which is the failure
this sprint exists to bound. It also makes "region" a naming convention with a runtime cost and no
blast-radius benefit.

**B. Per-region Redis, shared Postgres and Kafka (chosen).** A region's hot state is its own process;
the durable truth is not. Losing a region loses the queue that was waiting in it — which is honest,
because a queue is a claim about players who are *connected now*, and a player who is not connected
is not waiting. Losing a region does not lose a **match**: matches and their outbox events are
committed to Postgres before anything is published (ADR 0018), so the region's own history survives
it.

**C. Per-region Postgres as well.** A stronger boundary, and the wrong one: the durable log would
fragment, cross-region analytics and the orchestrator's projection would need a join that does not
exist, and the sprint's requirement (one match, one row, labelled by region) would become a shard
key. Rejected by scope; `MelhoriasFutura.md` keeps it as the next question if the shared database
ever becomes the bottleneck.

### Where the distance comes from

**D. Let the client declare it (`maxLatencyMs` only).** Already in the contract (W1) and free.
Rejected alone: a client that declares 5 000 ms removes the constraint, and a client that declares 5
ms cannot connect at all. It is a *preference*, not a measurement.

**E. Only the operator's matrix (`REGION_RTT_MS`).** Server-side by construction and needs nothing
from the client. Rejected alone: it is a floor, the best case two regions can be apart, and it says
nothing about *this* player behind *this* Wi-Fi. A player in `NA` on a bad link would be offered
`NA` — correctly — but a player whose `EU`→`NA` path is far worse than the declared 120 ms would be
admitted and dissolve at the deadline.

**F. `max(declared, measured)` (chosen).** The declared matrix is the floor the client cannot lower;
the player's measured RTT is the ceiling its own network imposes. Taking the larger means neither
side can talk the placement down, and a deployment with neither number configured behaves exactly as
it did before W21.

### Where the measurement happens

**G. Client-measured, reported in the ticket body.** Cheapest — the client already knows its RTT. It
is also the one number an attacker controls, and the sprint's premise is server-side latency
(`AGENTS.md`: "Server-side latency measurement — prevent client-side cheating").

**H. The WebSocket heartbeat (`03`, 60 s).** Tempting: the connection already measures itself. It is
*late* — placement happens before the player connects, so a player choosing a region would have to
connect first, then discover the region cannot take it. A `ws://` connection is also the most
expensive thing to open twice.

**I. A two-request probe (chosen).** `GET /v1/regions/{region}/ping` issues a nonce, `POST
/v1/regions/{region}/pong` consumes it; the server times the gap. Both requests go through the API
being measured, so the number is the path *to that region as served by that region*. The nonce is
single-use, 30 s, bound to (player, region), and **only consumed by a pong that matches** — an
honest client that answers at the wrong endpoint keeps the probe it holds.

### Failover

**J. A load balancer / proxy in front of the regions.** The client is redirected; it never learns
why. Rejected: the redirect moves the *socket*, not the player's queue, and the API would have to
expose its placement rule to an operator's config — a rule that depends on the player's measured
path cannot live in an L7 rule.

**K. A global queue that pairs players across regions.** The queue would place the match in the
region both players can reach. Rejected: it recreates option A's failure domain (one queue, one
Redis) for a property — "cross-region matches" — that the game does not want. A match across
regions is a match with a 100 ms ping for one of the two players.

**L. Client-driven failover (chosen).** The server refuses with the reason and the numbers
(`422 region_unsupported` naming the regions this endpoint serves, `422 latency_budget` with
`rttMs`/`declaredRttMs`/`measuredRttMs`/`maxLatencyMs`, `503 hot state unavailable` when the region's
Redis is down), and the **client** picks another region. It is the only option where the client's
own knowledge (a token from its home region, its own socket) is what decides the second attempt.

## Decision

1. **A region is a deployment**: `SERVED_REGIONS` (default: all) is read by both the API and the
   engine. An API refuses a ticket for a region it does not serve; an engine pairs only the queues
   of the regions it serves. A mismatch between the two is a configuration error with a visible
   symptom (arrivals nobody pairs), not a silent one.
2. **The home region is a token claim**, not a request body field: the auth service puts `region` in
   the JWT (login: `region`, default `NA`, unknown region → 400). A client cannot move its home by
   asking.
3. **Placement is `max(declared(home, target), measured(player, target))`,** compared against the
   ticket's `maxLatencyMs`. Null on both sides means "no constraint" — the pre-W21 behaviour.
4. **The declared matrix is symmetric and operator-owned** (`REGION_RTT_MS=EU-EU=20,EU-NA=120,…`).
   A malformed, unknown or contradictory entry **fails the boot**: a distance that silently
   disappears is a placement rule that silently disappears with it.
5. **The measured profile keeps the minimum** of the samples, with a 5-minute TTL renewed by every
   probe, in the Redis of the region that measured it. Minimum because a sample taken while the
   machine was busy must not cost a player a session; TTL because "measured 5 minutes ago" is the
   horizon that matters.
6. **A region's numbers are region-local.** `/v1/regions` shows `measuredRttMs` only for the regions
   the answering endpoint serves, and `/v1/regions/{region}/ping` refuses to measure a region it
   does not serve (`422 region_unsupported`): an endpoint measuring someone else's region measures
   *itself* and calls it the other region.
7. **A region's Redis runs without persistence** (`--save '' --appendonly no` in the scenario, and
   the requirement in this ADR). The queue is a claim about players who are queued **now**; a region
   that comes back holding yesterday's queue pairs ghosts, and a player who failed over while the
   region was down would be matched twice.
8. **Observability**: `matchmaking_player_rtt_ms{region}` (the sample just taken),
   `matchmaking_latency_probes_total{result=measured|invalid_nonce}`, and the two new reasons on
   `matchmaking_tickets_rejected_total{reason=region_unsupported|latency_budget}`.

## How it works (files)

- `common/common-domain/.../domain/ticket/ServedRegions.java` — `SERVED_REGIONS` parsing
  (`parse`, `of`, `all`, `contains`, `list`, `servesEveryRegion`); one value object for both services.
- `services/auth-service/.../SessionTokenIssuer.java`, `JwtSessionTokenIssuer.java`, `AuthRoutes.java`
  — the `region` claim, defaulted and validated at login.
- `services/matchmaking-api/.../application/RegionPlacement.java` — the matrix, `check`,
  `effectiveRttMs`, and the typed `Denial`/`Reason` the route renders.
- `services/matchmaking-api/.../application/PlayerLatency.java`,
  `infrastructure/RedisPlayerLatency.java` — the probe port and its Redis implementation.
- `services/matchmaking-api/src/main/resources/latency-probe.lua` — consume the nonce only for the
  player and region that asked (check and delete in one step).
- `services/matchmaking-api/src/main/resources/latency.lua` — record the sample, keep the minimum,
  renew the profile's TTL, in one step.
- `services/matchmaking-api/.../presentation/HttpRoutes.java` — `GET /v1/regions`,
  `GET /v1/regions/:region/ping`, `POST /v1/regions/:region/pong`, the admission helpers
  (`admitThenCreate` → `refuseUnsupportedRegion` → `refuseLatencyBudget`), and `regions` in
  `/health`.
- `services/matchmaking-engine/.../MatchmakingEngineApplication.java` — `SERVED_REGIONS`,
  `pairAcrossQueues(ServedRegions, …)`.
- `services/matchmaking-engine/.../infrastructure/QueueMetricsPoller.java` — the KPIs of the served
  regions only, so an engine does not publish a zero for a queue it cannot see.
- `common/common-redis/.../RedisKeys.java` — `latencyProbeKey`, `playerLatencyKey`,
  `allQueueKeys(partitions, servedRegions)`.

## Measured numbers (local stack, 2026-10-20)

`bash requests/multiregion/verify_multiregion.sh` — **54 checks, 0 failures**, two regions (NA on
8096/9197 with Redis 6380, EU on 8097/9199 with Redis 6381), a delay proxy adding 40 ms each way in
front of NA.

**The probe measures what the network does, not what the client says.** An EU player probing its own
region, three attempts: `[47, 10, 11]` → **10 ms** (the first sample is the cost of the first
request, which is why the store keeps the minimum). The same player probing NA through the proxy:
`[99, 105, 121]` → **99 ms**, against a declared `EU-NA=120`.

**The placement number is the larger of the two**: `max(declared 120, measured 99) = 120 ms` — the
matrix is the floor, so a lucky sample cannot buy a region the path cannot sustain.

**A budget is a distance, not a bias**: the same player, budget 60 ms, EU→NA → `422 latency_budget`
with `rttMs=120, maxLatencyMs=60`; a 30 ms budget **inside** NA (declared `NA-NA=25`) is admitted.

**A region this endpoint does not serve** is refused even with a 1 000 ms budget:
`422 region_unsupported`, body naming `served: ["NA"]`; a replayed nonce is `422 invalid_nonce`.

**Inside the region vs across it** (same engine, same policy):

| Path | RTT | Wait |
| --- | --- | --- |
| EU pair, in EU | 14 ms | 796.9 ms / 782.2 ms |
| EU pair, failed over to NA through the proxy | 106 ms | 983.4 ms |
| Second failover pair, NA | 108 ms | 532.2 ms |

**Isolation**: with EU's Redis stopped, EU's `/health/ready` is **503** naming `redis`, an arrival at
EU is **503 `hot state unavailable`** (not a 500, not a silent queue), and NA forms and confirms a
match with no error, its ticket and its expiry untouched.

**Returning does not match twice**: after EU's Redis restarts, EU has **0 queue keys and 0 ticket
keys**, no ticket in it belongs to the player who failed over, the API never had to be restarted
(readiness went 503 → 200 on its own), and Postgres holds **exactly one match** for the failed-over
player.

**One durable log, two regions**: 8 match-player rows for this run's players, labelled `NA` and `EU`
in the same `matches` table.

## How to test

- `bash requests/multiregion/verify_multiregion.sh` (54 checks, ~6 min): two regions, the directory,
  the probe near and far, the unsupported region, an in-region match, a failover match, the refused
  budget, isolation, the returning region, and the scrape/database agreement. It starts **its own**
  auth (8091), APIs (8096/8097, scrapes 9196/9198), engines (9197/9199) and two Redis containers —
  a shared service would be whatever build happened to be running.
- `mvn -o -B -pl services/matchmaking-api -Dtest=RegionPlacementTest test` (9 tests): the rule, the
  floor, the measurement, neither, the malformed matrix, the symmetric pair, the unsupported region.
- `mvn -o -B -pl common/common-domain -Dtest=ServedRegionsTest test` (5): parsing, `all`, unknown
  region, duplicates, the single-region deployment.
- `mvn -o -B -pl services/matchmaking-api -Dtest=RedisPlayerLatencyTest test` (5): the nonce is
  single-use, bound to player and region; the profile keeps the minimum; the TTL is renewed.
- `mvn -o -B -pl services/matchmaking-engine -Dtest=EngineRegionScopeTest test` (1): an engine that
  serves EU does not pair an NA queue — **it fails** ("expected 2 but was 0") if the scoping is
  replaced by `Region.values()`.

## Trade-offs

- **The region's queue is lost with the region.** A player waiting in EU when EU's Redis dies has to
  re-enqueue; there is no durable queue and this ADR does not add one. What is *not* lost is a
  confirmed match (Postgres + outbox) — the durable log outlives the hot state, which is the split
  W2 already made.
- **The measurement is a sample, and the store keeps the minimum.** A player whose link degrades
  after a good probe is admitted on the good number until the profile expires (5 min) or a worse
  sample arrives — but a worse sample never replaces a better one, so the profile can only say
  "better than reality", never "worse". The TTL is what bounds it.
- **One extra Redis read per ticket creation** (the profile `HGET`), in the same Redis the queue
  lives in, plus one `HGET`+`HSET` per probe. The placement decision itself is in-process; there is
  no cross-region call on the admission path.
- **The client is the one that fails over.** A client that treats `503`/`422` as terminal simply
  stops; a client that retries moves itself. The alternative — the server moving the player — needs
  either a global queue (option K) or a proxy that knows the placement rule (option J).
- **Postgres and Kafka are still shared**, so a database outage is still global: W21 bounds the
  *hot-state* failure domain, not the durable one. ADR 0024's matrix already says the durable
  dependencies are the deployment's, not a region's.
- **`SERVED_REGIONS` must agree between the API and the engine of the same region.** A mismatch is
  not detected at boot: the API would accept a ticket the engine never pairs. The scenario asserts
  the pairing covers each region (a NA match and an EU match), which is what makes the mistake
  visible; a stricter check (an engine reporting the API's configured set) would need a shared
  config the sprint does not introduce.
- **The latency profile is per region because the number is.** A player measured in EU cannot reuse
  that number for NA (a different path, and the EU Redis does not hold NA's measurements); the
  directory shows `measuredRttMs` only for the regions the answering endpoint serves.

## Consequences

- A region can be deployed, scaled and **lost** on its own: its Redis dying is a 503 naming a
  dependency for its players and nothing at all for another region's (measured above).
- Distance is a server number. The client declares a ceiling; the matrix and the probe decide the
  number it is compared against, and both new refusals carry the three numbers that produced them, so
  a client can choose another region instead of guessing.
- The default deployment (no `SERVED_REGIONS`, no `REGION_RTT_MS`, no probe) is byte-for-byte the
  pre-W21 behaviour: one region, no constraint, and every existing scenario and collection keeps
  working.
- The failure the sprint was written to prevent — a match formed in a region one of the players
  cannot reach, dissolving at the deadline — now has a refusal with a number in front of it:
  `422 latency_budget {rttMs: 120, maxLatencyMs: 60}`.
