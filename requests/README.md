# Request Collection

Single Postman/Insomnia collection for the entire matchmaking backend.

## Import

1. **File → Import** → select `matchmaking.postman_collection.json`
2. **File → Import** → select `matchmaking.postman_environment.json`
3. Select the `matchmaking` environment (top-right dropdown)

## Collection structure

```
matchmaking/
├── auth-service/          # POST /v1/auth/login, GET /.well-known/jwks.json
└── matchmaking-api/
    ├── health/            # GET /health
    ├── tickets/           # POST/GET/DELETE /v1/matchmaking/tickets (+ error cases, + party enqueue)
    ├── parties/           # GET/POST /v1/parties, POST members, DELETE member, DELETE party (+ error cases)
    ├── history/           # GET /v1/players/{id}/matches (+ error cases)
    ├── websocket/         # WS /v1/ws (PING, MATCH_CONFIRM, MATCH_DECLINE)
    ├── e2e-w5/            # Full W5 E2E flow (login ×2 → WS → pair → confirm → history)
    └── e2e-w11/           # Full W11 party flow (create → join → enqueue → fill/cancel)
```

## Party flow (W11)

1. `parties` → `POST /v1/parties` (creates a party, stores the `partyId` env var)
2. `parties` → `POST /v1/parties/{partyId}/members` (joiner adds themselves)
3. `tickets` → `POST /v1/matchmaking/tickets — party enqueue` (atomic `partyId`+`members` enqueue)
4. `parties` → `GET /v1/parties/{partyId}`, then `DELETE /v1/parties/{partyId}/members/{playerId}` / `DELETE /v1/parties/{partyId}` to dissolve

## Start services (order matters)

Install the whole reactor **once before starting** any service: `mvn exec:java`
in a single module resolves the other modules from the local `~/.m2` repository,
so starting a service right after a source change would otherwise run against a
stale artifact (for example an old `common-domain` without `partyId()`).

```bash
mvn -B install -DskipTests   # 1. build + install every module
# 2. start the services (one terminal each), in this order:
mvn -pl services/auth-service exec:java -Dexec.mainClass=com.victor.matchmaking.auth.AuthServiceApplication
mvn -pl services/matchmaking-api exec:java -Dexec.mainClass=com.victor.matchmaking.api.MatchmakingApiApplication
mvn -pl services/matchmaking-engine exec:java -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication
```

Infrastructure (Redis + Postgres, and Kafka from W12) comes from
`docker-compose up -d`. Start the services **after** the build: recompiling
`target/classes` under a running JVM breaks that process.

## Automated tests

```bash
mvn -B clean test            # unit + integration + race + stress (65 tests)
```

The engine concurrency tests touch `queue:*` and `match-pending:*` in Redis
directly, so **stop the running services and flush Redis first** — a live engine
or leftover keys otherwise interfere with their assertions (a live engine can
even steal the tickets the test just queued):

```bash
docker exec game-matchmaking-backend-redis-1 redis-cli flushall
```

## Automated E2E (W9/W10/W11)

`requests/e2e/run_e2e.py` drives the running services over HTTP and asserts the
engine state through Redis (9 scenarios, 37 checks): 1v1 and 5v5 pairing,
party fill, party never split, party not fillable, atomic cancel / remove /
dissolve, progressive skill widening and the confirmation-timeout sweep.

```bash
python3 requests/e2e/run_e2e.py            # exits non-zero if any check fails
python3 requests/e2e/check_confirmation_sweep.py   # focused sweep timeline
```

## Typical flow

1. `auth-service` → `POST /v1/auth/login` (captures `token`)
2. `matchmaking-api` → `health` → `tickets` → `history` → `websocket`
3. For E2E: run the `e2e-w5` folder top to bottom
4. Party flow (W11): run the `e2e-w11` folder top to bottom
