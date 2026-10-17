# Request Collection

Single Postman/Insomnia collection for the entire matchmaking backend.

## Import

1. **File → Import** → select `matchmaking.postman_collection.json`
2. **File → Import** → select `matchmaking.postman_environment.json`
3. Select the `matchmaking` environment (top-right dropdown)

## Collection structure

```text
matchmaking/
├── auth-service/          # POST /v1/auth/login, GET /.well-known/jwks.json
├── matchmaking-api/
│   ├── health/            # GET /health
│   ├── tickets/           # POST/GET/DELETE /v1/matchmaking/tickets (+ error cases, + party enqueue)
│   ├── parties/           # GET/POST /v1/parties, POST members, DELETE member, DELETE party (+ error cases)
│   ├── history/           # GET /v1/players/{id}/matches (+ error cases)
│   ├── websocket/         # WS /v1/ws (PING, MATCH_CONFIRM, MATCH_DECLINE)
│   ├── e2e-w5/            # Full W5 E2E flow (login ×2 → WS → pair → confirm → history)
│   └── e2e-w11/           # Full W11 party flow (create → join → enqueue → fill/cancel)
└── infra/
    └── schema-registry/   # GET /config, /subjects, /schemas/types (W12)
```

Scripts next to the collection (same folder, runnable by hand):

```text
requests/
├── e2e/run_e2e.py                     # 11 scenarios, 52 checks over HTTP + Redis + Kafka + Postgres
├── e2e/check_confirmation_sweep.py    # focused confirmation-timeout timeline
├── kafka/check_kafka.sh               # infrastructure: containers, registry, log round-trip
├── kafka/check_contract_gate.sh       # contract: is this change compatible with its base?
├── kafka/verify_match_confirmed.sh    # event: MATCH_CONFIRMED decodes through the registry
├── kafka/verify_idempotent_consumer.sh # consumer: a replay must not apply anything twice (W13)
└── kafka/verify_dlq.sh                # consumer: a record that cannot be handled must not stall the partition (W13)
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
# 2. register the event contracts in the Schema Registry (see "Kafka + Schema Registry")
mvn -B -pl common/common-events exec:java -Dexec.mainClass=com.victor.matchmaking.events.SchemaRegistrar
# 3. start the services (one terminal each), in this order:
mvn -pl services/auth-service exec:java -Dexec.mainClass=com.victor.matchmaking.auth.AuthServiceApplication
mvn -pl services/matchmaking-api exec:java -Dexec.mainClass=com.victor.matchmaking.api.MatchmakingApiApplication
mvn -pl services/matchmaking-engine exec:java -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication
mvn -pl services/match-orchestrator exec:java -Dexec.mainClass=com.victor.matchmaking.orchestrator.MatchOrchestratorApplication
```

The order is not cosmetic: the API fetches the auth service's JWKS at startup, so
starting it before `auth-service` is *listening* leaves it failing to bind (the
`ConnectException` in `fetchPublicJwk`) while the auth service finishes booting. The
orchestrator has no such dependency — it needs Postgres and Kafka, nothing else.

Infrastructure (Redis + Postgres, and Kafka + Schema Registry from W12) comes from
`docker-compose up -d`. Start the services **after** the build: recompiling
`target/classes` under a running JVM breaks that process.

Step 2 is not optional: producers run with `auto.register.schemas=false`, so the
engine refuses to publish an event whose schema was never versioned (it fails loudly
at the first confirmation, `engine.error stage=persistOrPublishMatch`). That is
deliberate — a running pod must never invent a schema version.

## Automated tests

```bash
mvn -B clean test            # unit + integration + race + stress (87 tests)
```

The engine concurrency tests touch `queue:*` and `match-pending:*` in Redis
directly, so **stop the running services and flush Redis first** — a live engine
or leftover keys otherwise interfere with their assertions (a live engine can
even steal the tickets the test just queued):

```bash
docker exec game-matchmaking-backend-redis-1 redis-cli flushall
```

The orchestrator tests need Postgres as well (`docker compose up -d postgres`): the
consumer's idempotency is enforced by a unique index and a transaction, so a fake
database would only assert the fake. They create their own rows (`match-w13-…`, group
`w13-test`) and delete them afterwards; the CI `build` job runs a Postgres service for
the same reason.

## Automated E2E (W9/W10/W11/W12/W13)

`requests/e2e/run_e2e.py` drives the running services over HTTP and asserts the
engine state through Redis, the published event through Kafka and the orchestrator's
projection through Postgres (11 scenarios, 52 checks): 1v1 and 5v5 pairing, party
fill, party never split, party not fillable, atomic cancel / remove / dissolve,
progressive skill widening, the confirmation-timeout sweep, S10 — a confirmed match
whose `MATCH_CONFIRMED` decodes from the Kafka log with the registered schema (roster,
teams and skill snapshot included) — and S11, where the orchestrator projects that
match exactly once.

```bash
python3 requests/e2e/run_e2e.py            # exits non-zero if any check fails
python3 requests/e2e/check_confirmation_sweep.py   # focused sweep timeline
```

S10 confirms both players by publishing on the `match_confirmations` channel — the
exact message the API produces from a WS `MATCH_CONFIRM` (`WsHub`); the WebSocket leg
itself is covered by the `e2e-w5` folder of the Postman collection.

## Kafka + Schema Registry (W12)

```bash
docker compose up -d zookeeper kafka schema-registry   # ~55s until all healthy
bash requests/kafka/check_kafka.sh                     # 9 checks, non-zero exit on failure
```

`check_kafka.sh` asserts the three containers are healthy, that the registry answers on
the host port 8082 with compatibility `FULL`, that it reached the broker through the
INTERNAL listener (the `_schemas` topic it owns exists), and that 200 messages written
to the EXTERNAL listener come back in order and none lost. Measured on the dev machine
(1 broker, 1 partition; ranges from repeated runs):

| Metric | Value |
|--------|-------|
| `docker compose up` → all healthy | 54 s |
| Broker accept (write → record `CreateTime`) | p50 0–1 ms |
| Produce-ack, `acks=all`, 2000 × 128 B | p50 9–17 ms, p95 29–35 ms, p99 47–52 ms |
| Throughput, single producer | 2.2–3.1 k records/s |

The round-trip check also prints the `kafka-console-consumer` CLI floor (0.5–1 s): that
is the CLI's own output granularity, not a broker property. It is measured with a warm
producer/consumer pair, so JVM startup is not part of it. See
`docs/adr/0014-why-kafka.md` for the two-listener setup (a single advertised listener
would break the registry) and `MESSAGES=1000 bash requests/kafka/check_kafka.sh` to
re-run it with a different volume.

Kafka itself cannot be driven from Postman, so the registry REST API is the part of this
infrastructure that lives in the collection: `infra/schema-registry`.

### The MATCH_CONFIRMED event (W12 task 2)

The contracts are `.proto` files compiled at build time: the shared types
(`EventMetadata`, `GameMode`, `Region`) in `common-proto`, the event itself
(`MatchConfirmed`, `MatchPlayerState`) in `common-events`. The engine publishes a
`matchmaking.events.MatchConfirmed` on the `matchmaking.events.match-confirmed` topic,
keyed by `matchId` (so every event of one match keeps its order in one partition).

Register (version) the schemas — also the compatibility gate, since the registry is
`FULL` and rejects an incompatible version:

```bash
mvn -B -pl common/common-events exec:java -Dexec.mainClass=com.victor.matchmaking.events.SchemaRegistrar
# schema.registered subject=matchmaking.events.match-confirmed-value version=1 id=2 schemaType=PROTOBUF
```

Read the event back — bytes decoded through the registry into JSON:

```bash
requests/kafka/verify_match_confirmed.sh                   # any confirmed match
requests/kafka/verify_match_confirmed.sh match-551dc626     # one specific match
```

It decodes the topic with `kafka-protobuf-console-consumer`, asserts the envelope
(`eventType`, `schemaVersion`, `eventId`, `occurredAt`), the roster (player, team 1|2,
skill snapshot) and prints `EVENT_JSON` / `EVENT_CREATE_TIME_MS` as evidence — the
E2E uses exactly that output for its assertions. Re-run it with
`QUIET_WINDOW_MS=10000 .../verify_match_confirmed.sh` when the broker is slow.

Measured in E2E S10 (`EVENT_CREATE_TIME_MS` minus the moment both `CONFIRM`s were
published, 4 runs): **416–831 ms, median ~600 ms** — it covers Redis, the Postgres
insert and the produce-ack. The registry reports version 1 / id 2 for the subject, and
re-running the registrar with an unchanged schema keeps that version (it versions, it
does not churn). See `docs/adr/0016-one-topic-per-event.md`.

### The contract gate (W12 task 3)

```bash
bash requests/kafka/check_contract_gate.sh                  # this change vs origin/main
BASE_REF=HEAD bash requests/kafka/check_contract_gate.sh    # this change vs the last commit
```

The script answers one question: *does this change break a consumer of the
contracts?* It registers the contracts of the **merge base** (in a worktree, with that
revision's own registrar), then the contracts of the working tree, and lets the
registry decide. A compatible change passes (and the version moves forward); an
incompatible one makes the registrar exit non-zero, so the CI step goes red with the
registry's reason in the log:

```text
schema.registration.cause: RestClientException: Schema being registered is incompatible
  with an earlier schema for subject "matchmaking.events.match-confirmed-value",
  details: [{errorType:"FIELD_SCALAR_KIND_CHANGED", description:"... '#/MatchPlayerState/2' ..."}]
```

The merge base (not the tip of `main`) is the baseline on purpose: a branch that is
merely behind main must not look like a downgrade of the contract. Two negative
controls run every time, because a gate nobody has seen red is not a gate: a mutated
contract is submitted to the **read-only** compatibility endpoint (must answer
`is_compatible: false`) and the registrar is pointed at a dead registry (must exit
non-zero).

Two documented cases where the gate is not the last word:

- **the bootstrap**: the first version of a contract has no baseline, and the script
  says so (`bootstrap case: … This is not a bypass`) instead of passing silently;
- **what `FULL` does not catch** (measured on 7.7.1): scalar/kind changes are refused,
  but *renumbering*, *removing* and *renaming* a field are accepted. The compiler
  catches most of it for our own producer; explicit evolution rules are ADR 0017 (W13).

The CI job `contracts` runs exactly this script (a gate duplicated in the workflow YAML
would drift from the one you can run by hand), after `check_kafka.sh`, against the same
`docker-compose.yml` used here. Measured on the dev machine: gate 23 s in the bootstrap
case (one build + registrar + both controls); the baseline adds one more build (10 s)
and one more registrar run (6 s); `check_kafka.sh` 25 s. See
`docs/adr/0015-protobuf-vs-avro-ci-gate.md`.

### The idempotent consumer (W13 task 1)

```bash
bash requests/kafka/verify_idempotent_consumer.sh
```

Kafka delivers at least once, so this script reproduces the replay an operator does
after fixing a projection bug: it stops the orchestrator, rewinds its consumer group to
the beginning of the topic (`--reset-offsets --to-earliest`), starts it again and
compares the database before and after. Every record is delivered a second time and
nothing may be applied twice.

The evidence is the database, not the log — the log says what the consumer intended:

```text
group position before: offset=8 end=8 lag=0
...
match_allocations:  8 -> 8
processed_events:   8 -> 8
outcomes logged:    APPLIED=0 DUPLICATE=8
```

Measured: 8 records re-read, **8 `DUPLICATE` / 0 `APPLIED`**, both tables unchanged and
the group back at lag 0 in **9 s** after subscribing (rebalance + 8 records, one commit
per record). The count is the topic's, not a limit: re-running it with 10 records on the
topic replays 10 and every one of them is recognised as a duplicate. On the live path (`projected_at - confirmed_at` in `match_allocations`) the
projection lags **42 ms** when the consumer is warm and 1.6 s for the first match after
it idled; the values in the hours range are backfill — events produced before the
orchestrator existed, which is a different thing. The check needs the orchestrator
running and at least one event on the topic (E2E S11 leaves both), stops and restarts
the consumer itself, and leaves it running. See
`docs/adr/0038-consumer-idempotency.md`.

### Bounded retry and the dead letter queue (W13 task 2)

```bash
bash requests/kafka/verify_dlq.sh
```

An idempotent consumer is only half of the at-least-once problem: a record that *always*
fails (bytes that never decode, or an effect the database refuses) is never committed,
so it is re-read forever and **blocks its partition** — every event behind it waits. The
script drives both failure shapes against the real stack and asserts the same ending for
each: three attempts, the original record on
`matchmaking.events.match-confirmed-dlq` with the diagnosis in its headers, and the
offset committed past it.

1. **a payload that never decodes** — 22 bytes of text are produced onto the topic
   through the console producer. The dead letter carries `dlq.stage:DECODE`,
   `dlq.attempts:3`, the original topic/partition/offset, the group, and the payload
   **verbatim**; no `dlq.event-id`, because nothing ever decoded.
2. **an effect that cannot be applied** — the script drops the inbox row of the newest
   projected event (the projection row stays, as it would after a partial restore) and
   rewinds the group onto it. The event decodes, is no longer a duplicate, and its
   `INSERT` conflicts on `match_id`: `dlq.stage:APPLY`,
   `dlq.error-class:java.util.concurrent.CompletionException` with the
   `duplicate key value violates unique constraint "match_allocations_pkey"` message, and
   `dlq.event-id` naming the event.

Measured: **38 checks, 0 failures**; after both phases the committed offset had passed
the record that failed (`lag 0`), the projection was still 10 rows, the inbox unchanged,
and the dead-lettered payload **still decoded through the Schema Registry into the same
event** — which is the point of keeping the bytes. The script needs the orchestrator
running and something already projected (E2E S11 leaves both), creates the dead letter
topic if it is missing, and restores the inbox row it perturbed. See
`docs/adr/0019-dlq-retry.md`.

The dead letter topic is a deployment precondition, not something the service invents:

```bash
docker exec game-matchmaking-backend-kafka-1 kafka-topics --bootstrap-server localhost:29092 \
  --create --if-not-exists --topic matchmaking.events.match-confirmed-dlq \
  --partitions 1 --replication-factor 1
```

## Typical flow

1. `auth-service` → `POST /v1/auth/login` (captures `token`)
2. `matchmaking-api` → `health` → `tickets` → `history` → `websocket`
3. For E2E: run the `e2e-w5` folder top to bottom
4. Party flow (W11): run the `e2e-w11` folder top to bottom
5. Event infrastructure (W12): `docker compose up -d zookeeper kafka schema-registry`,
   then `bash requests/kafka/check_kafka.sh`, then register the event schemas
   (`SchemaRegistrar`), then the `infra/schema-registry` folder in the collection
6. After a confirmed match (E2E S10 or the `e2e-w5` folder), read the durable event:
   `requests/kafka/verify_match_confirmed.sh`
7. Changing a contract (W12 task 3): `bash requests/kafka/check_contract_gate.sh`
   before opening the PR — the same script the `contracts` CI job runs
8. Consumers (W13): start `match-orchestrator`, confirm a match (E2E S11) and watch the
   projection row appear; then `requests/kafka/verify_idempotent_consumer.sh` to rewind
   the group and prove a full replay applies nothing twice
9. Failure paths (W13): `requests/kafka/verify_dlq.sh` produces a poison record and a
   conflicting effect, and proves that both end on the dead letter topic with the offset
   committed instead of stalling the partition
