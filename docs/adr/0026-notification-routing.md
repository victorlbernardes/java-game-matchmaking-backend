# ADR-0026 — Routing a notification to whichever instance holds the socket (W19)

## Status
Accepted (2026-10-20)

## Context

The WebSocket contract was written before W4 and has not changed since (`03 - Domínio e Contrato da
API`, "Contrato de conexão WebSocket"): one upgrade endpoint (`GET /v1/ws`), a handshake with the
token, a typed envelope, a 60 s heartbeat, a **local** registry `playerId → WebSocket` per instance,
and cross-instance delivery by publishing on the Redis channel `player_notifications` with a
`targetPlayerId` that every instance reads and only the holder delivers. One paragraph of that
contract was never implemented:

> Se o pub/sub falhar ou a instância de destino reiniciar, o heartbeat do jogador detecta o estado
> pendente e a notificação é reapresentada.

W4 built the easy half — a `Map` in memory, `PING → PONG`, and broadcast delivery — and stopped
there. The result is a service that *looks* stateless and is not, in three specific ways:

1. **A notification is delivered once and forgotten.** The engine publishes `MATCH_PENDING_CONFIRMATION`
   once, on Pub/Sub. If the instance holding the player's socket dies (a rollout, an OOM, a scale
   down), the socket dies with it, the player reconnects somewhere else — and nobody tells them what
   they missed. W16 measured the shape of that loss with a load client that dropped 2 % of the
   frames: **24 of 1024** matches dissolved at the deadline with a client that could not react. W18
   fixed the *lifetime* half of the subscription (it reconnects); the *delivery* half is this ADR.
2. **There is no instance identity and no presence.** Two API instances are indistinguishable in the
   logs and in the scrape, and Redis holds no `presence:{playerId}`. "Which instance is this player
   on?" had no answer that was not a guess, so "the service is stateless" was a claim about the code
   rather than an observation of the running system.
3. **A delivery that found no socket was invisible.** `deliver` did nothing when the player was not
   connected locally: no log, no counter. A notification published to the wrong instance and one
   never published at all looked identical from the outside.

The consequence is the sprint's title: the connection is pinned to the instance that accepted it, and
everything the connection was supposed to survive — the instance — takes the pending match with it.

Measured on the local stack before this ADR, by the scenario that now tests it (two instances sharing
one Redis, a match formed while one player has **no** socket, then the instance holding the other
player is killed with `SIGKILL`):

| Step | Before |
| --- | --- |
| Player C's heartbeat after reconnecting on the surviving instance | `PONG` only — **no re-presentation** |
| Player D — who never had a socket when the match formed — connects and heartbeats | `PONG` only; D never learns the `matchId` |
| The match | not confirmed; **dissolved at its 30 s deadline** (`matchmaking_matches_dissolved_total{reason="timeout"}` 0 → 1) |
| The reverse index | left behind (nothing points the player at a match that is gone, but nothing clears it either) |
| Scenario score | **18 passed / 8 failed** |

## Options

**A. Sticky sessions at the load balancer.** Keep the socket and the instance together for the life of
the connection. It solves nothing that is broken: the socket still dies with the instance, and a
rollout still loses every player on the instance being replaced. It also makes the load balancer a
routing table the service cannot see.

**B. A shared connection registry with targeted publish.** Redis holds `playerId → instanceId`, and
the engine publishes only to the instance that holds the socket. It removes the fan-out and adds a
second distributed state that can go stale: the registry is written by the instance that holds the
socket, so a dead instance's entries are exactly the ones nobody will fix — the same failure as
`presence`, with the delivery now depending on it. Broadcast is the failure-tolerant choice: the
answer "nobody has this socket" is not a guess about the registry.

**C. Broadcast + an explicit registry + presence + heartbeat re-presentation (chosen).** The socket
stays where it is; the *facts* become shared. Each instance keeps its local registry, publishes
presence to Redis with a TTL, counts what it delivered and what it could not, and answers a heartbeat
with the pending confirmation the player still owes. The state that matters (the pending match) already
lives in Redis — the missing piece was the reverse index and the willingness to ask.

**D. A durable per-player notification queue.** Every notification is appended to a per-player list and
acknowledged by the client; a reconnect replays it. It is the correct design for a system that must
not lose a message at any cost, and it is a new component with its own retention, ordering and ack
semantics — for a state that already has a TTL (the pending match) and a deadline (30 s). The
heartbeat reaches the same outcome with the state that exists.

**E. Do nothing.** The contract already promises the re-presentation; leaving it unimplemented means a
documented guarantee that is false, and the only repair path for a lost notification is the timeout
sweep.

## Decision

**Option C.** Concretely:

1. **Identity.** `INSTANCE_ID` (env, default `api-{port}`) is logged at boot, returned by
   `GET /health` as `instance`, and tagged on the socket metrics as `instance_id` — not `instance`,
   because Prometheus adds a label of that name at scrape time and would rename ours.
2. **Presence.** `presence:{playerId} → instanceId`, TTL **90 s** (one and a half heartbeats), written
   on connect and refreshed on every `PING`, removed on disconnect. The removal is **compare-and-clear**
   (`presence-release.lua`): a late close handler from instance A must not erase the entry instance B
   wrote when the player reconnected first. A dead instance runs no handlers at all, which is what the
   TTL is for.
3. **An honest registry.** The class that was `WsHub` is now `ConnectionRegistry`, and it counts:
   `matchmaking_ws_connections` (gauge, per instance), `matchmaking_ws_connections_replaced`, and
   `matchmaking_ws_notifications{result="delivered"|"no_local_connection"}`. The miss is the number
   that did not exist before: a broadcast notification the instance had no socket for. It is not an
   error — the player may be elsewhere — it is the cost of the fan-out, made visible.
4. **A reverse index.** The engine writes `player-pending:{playerId} → matchId` (TTL = the pending
   match's) for every member when it forms a match, next to the tickets that become `CONFIRMING`, and
   clears it when the match is confirmed or dissolved. `match-pending:{matchId}` is keyed the other way
   round; a reconnecting client only knows its player id.
5. **Re-presentation on heartbeat.** `PING` answers `PONG`, refreshes presence **and** looks up the
   player's pending match: if there is one, and it has no confirmed claim yet, the instance sends the
   same `MATCH_PENDING_CONFIRMATION` envelope it would have pushed. The client deduplicates by
   `matchId`, so a duplicate is harmless; a miss is the thing that cost matches.

## How it works (files)

| File | Role |
| --- | --- |
| `services/matchmaking-api/…/presentation/ConnectionRegistry.java` | Local registry, delivery counters, presence on connect/heartbeat/disconnect, re-presentation on `PING` |
| `services/matchmaking-api/…/application/PlayerPresence.java` | Port: `markOnline` / `offline`; `none()` for the in-memory wiring |
| `services/matchmaking-api/…/application/PendingConfirmationLookup.java` | Port: "what does this player owe?"; `none()` for the in-memory wiring |
| `services/matchmaking-api/…/infrastructure/RedisPlayerPresence.java` + `src/main/resources/presence-release.lua` | Presence with TTL and compare-and-clear |
| `services/matchmaking-api/…/infrastructure/RedisPendingConfirmationLookup.java` | Reverse index → pending match → roster, skipping a match that is already confirmed |
| `services/matchmaking-engine/…/application/PendingMatchStore.java` | `indexPlayers` / `clearPlayerIndex` |
| `services/matchmaking-engine/…/infrastructure/RedisPendingMatchStore.java` | Writes the index with the pending TTL; clears it in one `DEL` |
| `services/matchmaking-engine/…/MatchmakingEngineApplication.java` | `pair` indexes the roster; confirmation and dissolution clear it |
| `common/common-redis/…/RedisKeys.java` | `presence:{playerId}`, `player-pending:{playerId}` |
| `common/common-metrics/…/MetricNames.java` | `matchmaking.ws.*`, `TAG_INSTANCE_ID`, `TAG_RESULT` |
| `requests/routing/verify_routing.sh` + `routing_probe.py` | The two-instance scenario |

## Measured numbers (local stack, 2026-10-20)

Two instances (`api-a` on 8092, `api-b` on 8093, metrics 9192/9193) sharing one Redis and one engine.

| Measurement | Before | After |
| --- | --- | --- |
| Scenario | **27 passed / 8 failed** (18/8 in the probe), 1 match dissolved (`reason="timeout"`) | **26 passed / 0 failed** in the probe, **35 / 0** including the script |
| Notification delivered by the holder | 1 per instance | 1 per instance |
| Notification with no local socket | **not measurable** (no counter) | 1 per instance — each instance hears the other player's notification and counts the miss |
| Player C (socket killed mid-match) reconnects and heartbeats | `PONG` only | `MATCH_PENDING_CONFIRMATION` re-presented |
| Player D (never connected when the match formed) connects and heartbeats | `PONG` only, match unknowable | `MATCH_PENDING_CONFIRMATION` with the `matchId` — the only way D could learn it |
| First heartbeat → re-presentation | — | **5 ms** |
| Presence after the reconnect | — | `presence:routing-2 → api-b` (follows the player) |
| Match outcome | dissolved at the 30 s deadline | confirmed, `match-pending` gone, reverse index cleared |
| Engine counters | confirmed +1, dissolved +1 | confirmed +2, dissolved +0 |

Tests added: `RePresentationTest` (2, API — a heartbeat re-presents, and one with nothing owed does
not), `PresenceReleaseTest` (1, API — a late disconnect must not erase the new owner), `PendingIndexTest`
(1, engine — pairing indexes the roster and the cleanup removes the pointers). Full suite: **166 tests**.

## How to test

```bash
# the two-instance scenario (~3 min; needs auth on 8081, the engine with METRICS_PORT=9091,
# docker redis/postgres, and the working tree built)
bash requests/routing/verify_routing.sh

# the properties alone
mvn -o -B -pl services/matchmaking-api test -Dtest='RePresentationTest,PresenceReleaseTest'
mvn -o -B -pl services/matchmaking-engine test -Dtest='PendingIndexTest'
```

By hand: `GET /health` on each instance answers `{"status":"UP","instance":"api-a"}`;
`redis-cli get presence:<playerId>` names the instance holding the socket; killing that instance and
connecting the same token to the other one, then sending `{"type":"PING"}` over the socket, produces a
`MATCH_PENDING_CONFIRMATION` frame as long as the match is still pending.

## Trade-offs

- **The fan-out is O(instances) per notification.** Every instance receives every notification and
  discards the ones it has no socket for. With two instances that is one wasted frame per
  notification; the alternative (a shared routing table) trades a bounded, observable waste for a
  distributed state that is wrong exactly when an instance dies. The `no_local_connection` counter is
  what says how much is being wasted.
- **Presence is a hint, not a source of truth.** Its TTL bounds how long a dead instance can appear
  online (90 s); it is not used to route, only to observe and to answer "where is this player".
- **The reverse index is best effort.** It is written next to the `CONFIRMING` transitions, not inside
  `pair.lua`, so a crash in that window leaves a pending match without pointers. That window costs the
  same as the pre-W19 behaviour (the player is re-presented nothing and the match dissolves at the
  deadline); it does not cost correctness, and it keeps `pair.lua`'s KEYS/ARGV shape — and its four
  direct callers — unchanged.
- **The re-presentation depends on the client's heartbeat.** A client that never sends `PING` never
  recovers a lost notification, and the recovery latency is bounded by the heartbeat interval (60 s in
  the contract) — not by the 30 s confirmation deadline. That is the contract's own choice: the
  heartbeat is the only thing that runs again after a delivery is lost.
- **A client that reconnects to the instance it left** gets the same treatment: the lookup is by
  player, not by instance. Nothing is pinned.

## Consequences

- `MelhoriasFutura.md` item 7 is closed on both halves (W18 the subscription, W19 the delivery). What
  remains is the client-side deadline: a player who does not reconnect within the confirmation window
  still loses the match, by design (two-phase confirmation, ADR 0011).
- W20/W21 can scale the API horizontally without a sticky load balancer: any instance can serve any
  player, and the state a player needs is in Redis.
- Multi-region (W21) will have more than one Redis; the broadcast is per-Redis, so the channel becomes
  the partition key — out of scope here and tracked there.
- The socket-layer metrics are per instance and must be summed in dashboards; the tag is
  `instance_id` on purpose, so a Prometheus `instance` label from the scrape target still names the
  process that produced the sample.
