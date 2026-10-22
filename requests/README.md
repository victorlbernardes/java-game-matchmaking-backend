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
│   ├── health/            # GET /health (liveness), GET /health/ready (readiness, W18)
│   ├── tickets/           # POST/GET/DELETE /v1/matchmaking/tickets (+ error cases, + party enqueue)
│   ├── parties/           # GET/POST /v1/parties, POST members, DELETE member, DELETE party (+ error cases)
│   ├── history/           # GET /v1/players/{id}/matches (+ error cases)
│   ├── websocket/         # WS /v1/ws (PING, MATCH_CONFIRM, MATCH_DECLINE)
│   ├── e2e-w5/            # Full W5 E2E flow (login ×2 → WS → pair → confirm → history)
│   └── e2e-w11/           # Full W11 party flow (create → join → enqueue → fill/cancel)
├── routing-w19/           # two instances: routing, presence, heartbeat re-presentation (W19)
├── partitioning-w20/      # queue partitions: enqueue, per-partition depth, hotspot (W20)
├── multiregion-w21/       # regions: the directory, the ping/pong probe, the two refusals (W21)
├── gameservers-w22/       # the pool, the atomic allocation and the capacity gate (W22)
├── lifecycle-w23/         # allocation deadlines, requeue and the connection token (W23)
└── infra/
    └── schema-registry/   # GET /config, /subjects, /schemas/types (W12)
```

Scripts next to the collection (same folder, runnable by hand):

```text
requests/
├── mm.sh                              # the scenario harness: one command per question a script asks
├── engine/verify_queue_reaper.sh      # queue: a ghost member must not stall the mode/region (W16)
├── load/results-*.json                # one file per measured run (before/after, 400/2000/4000 players)
├── kafka/check_kafka.sh               # infrastructure: containers, registry, log round-trip
├── kafka/check_contract_gate.sh       # contract: is this change compatible with its base?
├── kafka/check_proto_evolution.sh     # contract: the rules the registry does not check (W13)
├── kafka/verify_match_confirmed.sh    # event: MATCH_CONFIRMED decodes through the registry
├── kafka/verify_idempotent_consumer.sh # consumer: a replay must not apply anything twice (W13)
├── kafka/verify_dlq.sh                # consumer: a record that cannot be handled must not stall the partition (W13)
├── kafka/verify_outbox.sh             # producer: an event survives its publisher being absent (W14)
├── observability/
│   ├── check_metrics.sh               # KPIs: exposition, the numbers moving, Prometheus + Grafana (W15)
│   └── check_slos.sh                  # SLOs: rules loaded, ratios agree with a hand count, alerts alive (W16)
├── backpressure/verify_load_shedding.sh  # the three refusals: 429, 503, and waiting on the database (W17)
├── resilience/verify_degradation.sh   # the degradation matrix, one dependency at a time (W18)
├── routing/verify_routing.sh          # two instances: routing, kill, re-presentation (W19)
├── partitioning/verify_partitioning.sh # queue: one key vs 8, the hotspot, cross-partition pairing (W20)
├── multiregion/verify_multiregion.sh  # two regions, two Redises, placement and failover (W21)
├── gameservers/verify_capacity.sh     # the pool, the allocation, the gate and the TTL (W22)
├── lifecycle/verify_lifecycle.sh      # deadlines, requeue, connection token, no-show (W23)
├── load/                             # the results of each load run (the harness itself is `mm.sh load`)
└── (the E2E and the multiregion probe are harness subcommands: `mm.sh e2e`, `mm.sh multiregion`)
```

### The harness is Java

The probes the scripts call are a Java module, `tools/scenarios`, reached through
`requests/mm.sh`. It is not a deployable and no service depends on it; it exists so a
scenario can ask its question using the product's own vocabulary — the same
`DeterministicSkill` the engine pairs with, the same `QueuePartitions` the API writes,
the same `GameMode` and `Region`. A second implementation of any of those would let a
harness bug look like a product bug.

```bash
requests/mm.sh queue players --count 16 --prefix w23-   # ids that share one skill
requests/mm.sh json /tmp/probe.json hotspot             # one field of a probe's answer
requests/mm.sh le 1.092 1.15                            # a comparison, by exit code
```

`mm.sh` rebuilds the jar when a `.java` under `common/` or `tools/` is newer than it, so a
script never runs against a stale probe (the same trap as starting a service before
`mvn install`). Bash keeps the orchestration — `curl`, `docker`, `psql`, `redis-cli`, killing
and starting services — because that is what a scenario *does*; the logic that reads an
answer moved to Java.

There is **no Python left** in `requests/`: the load harness is `mm.sh load`, and the parts the
Kafka scripts used to generate at runtime are subcommands too — the log reader
(`mm.sh kafka verify-log`), the dead letter parsers (`dlq-offset`, `dlq-checks`), the registry
verdict (`compat`) and the contract mutations (`mutate`). `find . -name '*.py'` answers nothing, and a search for the
interpreter answers nothing. ADR 0040 records why the boundary is exactly there:
the logic is Java, the orchestration (curl, docker, psql, redis-cli, kafka-*) stays in bash.

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
mvn -B clean test            # unit + integration + race + stress (162 tests, 12 modules)
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

`requests/mm.sh e2e` drives the running services over HTTP and asserts the
engine state through Redis, the published event through Kafka and the orchestrator's
projection through Postgres (11 scenarios, 52 checks): 1v1 and 5v5 pairing, party
fill, party never split, party not fillable, atomic cancel / remove / dissolve,
progressive skill widening, the confirmation-timeout sweep, S10 — a confirmed match
whose `MATCH_CONFIRMED` decodes from the Kafka log with the registered schema (roster,
teams and skill snapshot included) — and S11, where the orchestrator projects that
match exactly once.

```bash
requests/mm.sh e2e            # exits non-zero if any check fails
requests/mm.sh e2e sweep                             # focused sweep timeline
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
SCAN_RECORDS=0 requests/kafka/verify_match_confirmed.sh     # scan the whole log
```

It decodes the topic with `kafka-protobuf-console-consumer`, asserts the envelope
(`eventType`, `schemaVersion`, `eventId`, `occurredAt`), the roster (player, team 1|2,
skill snapshot) and prints `EVENT_JSON` / `EVENT_CREATE_TIME_MS` as evidence — the
E2E uses exactly that output for its assertions. Re-run it with
`QUIET_WINDOW_MS=10000 .../verify_match_confirmed.sh` when the broker is slow.

The scan starts at the end of the log minus `SCAN_RECORDS` (default **2000**) and prints
the window it read (`SCANNED_FROM`, `LOG_END_OFFSET`), because the topic is shared and only
grows: after the W16/W17 load runs it held **18 650 records**, and reading all of it is ~38
`docker exec` calls that each start a JVM — ~16 minutes for the check, bounded only by
`MAX_READS`. The records the scenarios care about are the recent ones. `SCAN_RECORDS=0`
examines everything, which is what the poison-record accounting of the dead-letter scenario
wants (the check then reads "every record in the scanned window was decoded or accounted
for", with the window printed).

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

### The evolution rules (W13 task 3)

```bash
bash requests/kafka/check_proto_evolution.sh                  # this change vs origin/main
BASE_REF=HEAD bash requests/kafka/check_proto_evolution.sh    # this change vs the last commit
```

The registry gate answers *"is this change compatible by the registry's rules?"*. This
script answers the question the registry cannot: **does this change break a reader of the
previous contract?** A Protobuf field is tracked by the registry by *name*, so a
renumbering, a removal and a rename all pass `FULL` (measured, ADR 0015) — and those are
exactly the breaks, because the wire is read by *number*.

How it runs, and why it needs no registry: both sides are compiled with `protoc` into a
`FileDescriptorSet` (`--include_imports`, so a change in `matchmaking/common/types.proto`
is compared too) and `ProtoContractDiff` compares the descriptors — the wire view.
Comments, formatting and field order are invisible to both, which is the point.

Each run prints what was compared and a verdict, and the CI step reads the verdict:

```text
contract.diff files=3 messages=4 fields=15 enums=2 enumValues=7 violations=0 advisories=0 verdict=compatible
FIELD_RENUMBERED MatchConfirmed.players: field number 5 became 6; the wire is read by number, so a
  reader of the old contract decodes whatever else took that number. Add a new field and stop writing
  the old one instead.
```

Two severities: a **violation** fails the build; an **advisory** is printed and does not
(`FIELD_NAME_NOT_RESERVED` — the number is reserved but the name is not;
`FIELD_PRESENCE_CHANGED` — a field became explicitly `optional`). Five mutations are fed
to the checker on every run — renumbering, removal without `reserved`, kind change,
renumbered enum value (in the imported types) — and it must refuse all of them; two
changes are fed to it and must be **accepted** (a field added with a free number, and a
removal done with `reserved <number>` + `reserved "<name>"`). A checker that refuses
everything gets bypassed the first time it is inconvenient.

See `docs/adr/0017-schema-evolution.md` for the rules themselves and
`common/common-events/README.md` for what a producer must do instead.

### The transactional outbox (W14)

```bash
bash requests/kafka/verify_outbox.sh
```

`docs/adr/0018-transactional-outbox.md`. The engine no longer publishes on the confirmation
path: the match, its roster and the event that announces it are one transaction, and a relay
publishes what the table holds. The script proves the property the old path could not have —
**an event whose publisher never ran is not lost** — and the one that keeps the queue honest: a
row no code can handle is parked with its reason and does not block the row behind it.

It stops the engine, rewinds the newest published row to `PENDING` (what a crash between the
commit and the publish leaves behind), watches it stay there with nobody running, starts the
engine again and requires the event back on the topic **with the same event id**, decoded through
the registry by `verify_match_confirmed.sh`. It then inserts a row whose topic does not exist and
requires it to end `PARKED`, with `last_error` saying why, while a good row behind it still goes
out. Preconditions: the docker stack plus at least one confirmed match in `outbox_events` (E2E
S11 or any confirmation); the script runs `mvn install` itself and leaves the engine running. 14
checks, ~2 min. The drain is measured from the engine's own `engine.started` line, not from the
launch: Maven + JVM + Flyway alone reached 15 s on a busy machine, and a check about the relay must
not fail because the JVM was slow.

Reading the queue by hand:

```sql
SELECT status, count(*) FROM outbox_events GROUP BY status;          -- backlog, log, park lot
SELECT event_id, partition_key, attempts, last_error, claimed_by
  FROM outbox_events WHERE status = 'PARKED';                        -- what needs a human
-- requeue a parked event (it will be published on the next pass):
UPDATE outbox_events SET status = 'PENDING', attempts = 0, next_attempt_at = NULL
 WHERE event_id = '<uuid>';
```

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

The dead letter topic is shared and only accumulates (one record per dead letter — **133**
at the time of writing), so the script reads its **tail** (`DLQ_SCAN`, default **200**)
rather than the oldest records. It used to ask for the oldest 100, and once the topic
passed 100 the letter the run had just produced was not in the dump at all: the "newest
of this stage" selection then answered with a stale letter from a previous run and every
header assertion failed against the wrong record (found in W18 — 8 failures, all against
a record the run did not produce).

The dead letter topic is a deployment precondition, not something the service invents:

```bash
docker exec game-matchmaking-backend-kafka-1 kafka-topics --bootstrap-server localhost:29092 \
  --create --if-not-exists --topic matchmaking.events.match-confirmed-dlq \
  --partitions 1 --replication-factor 1
```

## Observability — domain KPIs (W15)

Both services expose a Prometheus scrape on **a port of their own** (`MetricsEndpoint`, ADR
0020): `matchmaking-api` on **9090**, `matchmaking-engine` on **9091**. The port is never the
public one; in this local stack the endpoint binds every interface so the Prometheus container
can reach the host through `host.docker.internal`.

```bash
docker compose up -d prometheus grafana          # Prometheus :9095, Grafana :3000

# the API owns 9090 by default; the engine is started with its own port:
METRICS_PORT=9091 mvn -pl services/matchmaking-engine exec:java \
  -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication </dev/null \
  > /tmp/matchmaking-engine.log 2>&1 &

curl -s localhost:9090/metrics | grep matchmaking_tickets_enqueued_total
curl -s localhost:9091/metrics | grep -E 'matchmaking_(queue|outbox|matches)'
```

The numbers, and the question each one answers:

| Metric (Prometheus) | Kind | Question |
| --- | --- | --- |
| `matchmaking_tickets_enqueued_total{mode,region}` | counter (API) | How many players arrived, where |
| `matchmaking_queue_depth{mode,region}` | gauge from Redis | How many are waiting right now |
| `matchmaking_queue_oldest_wait_seconds{mode,region}` | gauge from Redis | How long the head has been waiting (the depth alone lies) |
| `matchmaking_match_wait_seconds_*` | histogram (engine) | Wait from enqueue to match: p50/p95/p99 |
| `matchmaking_matches_formed_total{mode,region}` | counter | Pairing throughput |
| `matchmaking_matches_confirmed_total{mode,region}` | counter | Matches that actually happened |
| `matchmaking_matches_dissolved_total{reason}` | counter | Matches that fell apart (`timeout` / `declined`) |
| `matchmaking_outbox_backlog{status}` | gauge from Postgres | `PENDING` = backlog, `PARKED` = needs a human |
| `matchmaking_outbox_oldest_pending_seconds` | gauge from Postgres | A stopped relay, not a busy one |
| `matchmaking_outbox_publish_lag_seconds` | gauge from Postgres | Fact → broker for the newest event |
| `matchmaking_outbox_published_total` / `_retried_total` / `_parked_total` | counters | What the relay is doing |
| `vertx_http_server_requests_total{code,method}`, `vertx_http_server_response_time_seconds`, `vertx_pool_*`, `vertx_redis_*`, `vertx_sql_*`, `jvm_*` | Vert.x / JVM | Transport and runtime health (event loop, pool, memory, HTTP codes) |

Two rules the dashboards depend on: **every tag is a closed set** (`mode`, `region`, `status`,
`reason` — never a player id or a free-form string), and the **state-derived gauges are refreshed
every 2 s** by a poller, because a Micrometer gauge is read synchronously at scrape time and Redis
and Postgres are not (the staleness bound is in ADR 0020).

The scenario drives one real match with two players and asserts the numbers moved:

```bash
bash requests/observability/check_metrics.sh
```

It reads the exposition directly, then the same series through Prometheus (`up == 1`, a query),
then Grafana's API, and cross-checks the outbox backlog against `SELECT count(*)` in Postgres —
so it fails if a service stops exposing, if Prometheus stops scraping, if the dashboard is
dropped from the provisioning, or if Grafana can no longer query the datasource. Grafana and
Prometheus are configured as code: `ops/prometheus/` and `ops/grafana/` (datasource, provider,
dashboard), so a fresh machine gets the panels without clicking.

Measured: **35 checks, 0 failures, ~25 s** (ADR 0020 has the numbers the run produced). If the
container images cannot be pulled (this machine could not reach the registry), the same
configuration runs from the binaries on the host and the scenario is unaffected — it checks
reachability, not what serves it:

```bash
prometheus --config.file=ops/prometheus/prometheus.host.yml --web.listen-address=:9095 \
           --storage.tsdb.path=/tmp/prometheus-w15        # targets localhost:9090/9091
grafana server                                        # provisioning: ops/grafana/
```

## The load test — where the ceiling is (W16)

```bash
requests/mm.sh load --players 400 --concurrency 64 --label after
```

It drives real players through the whole stack (auth → API → engine → Redis/Postgres/Kafka),
stands in for the players' clients and reports what the sprint is about: enqueue throughput and
client-side percentiles, the pairing rate (players out of the queue per second — **the ceiling**,
as a peak and as a window average), the wait the players actually felt (p50/p95/p99), the
confirmation rate, the resource cost (engine CPU, heap, Redis ops/s, Postgres tx/s), and the
invariant a ratio cannot see: **one match, one event** (the `matches` rows are counted against the
distinct `partition_key` in `outbox_events`, and a duplicate exits non-zero).

The client half is a plain socket speaking RESP — a subscriber on `player_notifications` and a
publisher on `match_confirmations` — not `docker exec redis-cli`: measured with the exec hop, 24
of 1024 matches were announced and never arrived (the attach stream in front of the client was the
lossy part), and each dissolved at its 30 s deadline. A harness that drops 2% of its own input
cannot measure a 99% objective.

Measured on the dev machine (1v1/NA, every player pairable, one confirmation per player; the
before row is `PAIR_BATCH_PER_TICK=1`, behaviourally the pre-W16 loop):

| Run | Players | Enqueue | Pairing (peak / window) | Formed → confirmed | Dissolved | Wait p50 / p95 / p99 |
| --- | --- | --- | --- | --- | --- | --- |
| before | 400 | 12/400 (388 refused: Redis pool) | — / 4.2 players/s | 200 → 302 | 2 | 30 / 30 / 30 s |
| after | 400 | 265/s | 89 / 62 players/s | 200 → 200 | 0 | 1.23 / 1.74 / 1.78 s |
| after | 2000 | 282/s | 168 / 125 players/s | 1000 → 1000 | 0 | 6.16 / 9.22 / 9.86 s |
| after | 4000 (arrival above capacity) | 269/s | 184 / 138 players/s | 2000 → 2000 | 0 | 9.41 / 14.97 / 15.59 s |

Every run writes `requests/load/results-<label>.json`; the committed files are the evidence. The
4000-player row is honest about the envelope: arrivals above the node's pairing rate grow the
queue, waits grow with it, and **SL-4** (not SL-1) is the objective that reports it.

## The SLOs — what "good enough" means (W16)

Four objectives live as code in `ops/prometheus/rules/matchmaking-slos.yml` (10 recording rules +
8 alerts), loaded by both Prometheus configs and mounted into the compose file:

| Id | Objective | Shape |
| --- | --- | --- |
| SL-1 | 99% of matched tickets wait ≤ 10 s | ratio, 1% error budget |
| SL-2 | 99% of formed matches are confirmed | ratio, 1% error budget |
| SL-3 | the outbox never ages past 5 s, and parks nothing | threshold |
| SL-4 | a waiting player never waits > 30 s while his queue has work | threshold |

```bash
bash requests/observability/check_slos.sh      # 36 checks, ~2 min
```

The scenario asserts the rules are loaded **and are this repository's file**, computes the SL-1 and
SL-2 ratios of its own traffic by hand from two scrapes and compares them with the recorded series,
checks the queue rules against the queue itself (a lone waiting player raises both, cancelling
brings them back), and proves the alert expressions are **alive**: an expression that never answers
anything never fires either, so each one is evaluated at a threshold that *must* trip it (and must
return a series) and at its real threshold (and must stay empty).

Two traps are worth knowing before writing another ratio rule: the objective's boundary must be a
**bucket edge** (`EngineKpis.LATENCY_SLO` — Micrometer's defaults jump from 8.59 s to 10.02 s, so a
`le="10"` rule once summed over a boundary that did not exist), and PromQL matches labels by exact
string while the client renders the edge `le="10.0"` (hence `le=~"10(\.0)?"`). A rule file that
loads nothing also looks healthy — a relative `rule_files` entry resolves against the config file's
directory — which is why the scenario asserts the groups exist and checks their path.

The Prometheus this runs against is **not** a quiet stack: it holds every load and chaos run of the
repository. Since W18 the scenario therefore treats a firing **burn** as evidence to verify rather
than noise to demand away — all four burns (fast and slow) are checked against their own condition —
and the SL-1/SL-2 ratio and budget checks accept a shortfall only while the recorded burn agrees with
it, so a rule that stopped measuring still fails. The window is in `(1 - ratio)`, and after the W18
chaos run the 1 h confirmation window held **46 formed / 19 confirmed**: a match formed while Redis or
Postgres is down is never confirmed, which is the objective measuring exactly what it describes.

## Backpressure — what the system says no to (W17)

```bash
bash requests/backpressure/verify_load_shedding.sh    # 35 checks, ~1 min
```

Three refusals, three reasons, and three different places to stop (ADR 0022, ADR 0023):

| Refusal | Where the decision is made | Answer | What it protects |
| --- | --- | --- | --- |
| Rate limit | `ratelimit.lua` in Redis, key `ratelimit:tickets:{playerId}` | `429` + `Retry-After` + `X-RateLimit-Limit` / `-Remaining` | One authenticated player cannot turn the queue into a weapon |
| Queue cap | `create.lua`, in the same atomic step as the write | `503` + `Retry-After: 5` + `{queue, depth, capacity}` | A queue deep enough to promise nobody a game admits nobody more |
| Database | `WriteGate` in the engine (a permit, not a lock) | the confirmation *waits*; the pairing pauses | A database that is behind is not asked harder |

The cap is inside the Lua script and not in Java for the reason the whole sprint exists: a `ZCARD`
followed by a `ZADD` is a check that N in-flight arrivals all pass at the same moment, so a queue
"capped" at M grows without bound under exactly the load the cap is for. The same argument put
idempotency (W6) and the roster (W16) in scripts.

**The hole the load run found.** The admission check lived on the idempotency path only, so an
arrival *without* an `Idempotency-Key` — a curl, a player's second device, the load generator —
took the plain write path and walked around the cap. Measured before the fix: **4000 arrivals
against a cap of 1000, zero refusals, the queue at 3146**. Both paths now go through the script
(with an empty key when the caller sent none), and `QueueCapacityTest.arrivalsWithoutAnIdempotencyKeyAreCappedToo`
is the test that fails if it comes back. Two questions that look like one — *is this a replay?* and
*is there room?* — are answered independently.

The scenario proves each refusal against the running stack:

1. **Rate limit** — 60 requests in a row from one player: the API advertises `X-RateLimit-Limit: 20`,
   ~18 are refused with `429` + `Retry-After`, the admitted count is checked against
   `burst + refill × elapsed` (not against a hard-coded number, so the check survives a
   reconfiguration), the refusal reports `X-RateLimit-Remaining: 0`, the KPI
   `matchmaking_tickets_rejected_total{reason="rate_limited"}` counts them, and after the
   `Retry-After` the same player is served again — the bucket refills. If the limiter's own store is
   unreachable the API **fails open, loudly**: the queue cap is the real guard, and a guard that
   becomes the outage it exists to prevent is worse than the load.
2. **Queue cap** — the engine is stopped (nothing drains, so the cap is what answers), 1100 real
   arrivals go in: **1000 admitted, 100 refused** with `capacity=1000`, `depth=1000`, `Retry-After: 5`,
   Redis's `ZCARD` at exactly 1000, and the replay of an already-accepted ticket still answered with
   its ticket while the queue is full. Then the queue is cleaned up, the engine restarted and a new
   arrival admitted — the refusal was temporary, not a closed door.
3. **Database** — Postgres is made genuinely slow with a `BEFORE INSERT ON matches` trigger that
   sleeps 3 s, and `mm.sh backpressure db` drives a real burst: 24 players → 12 matches → 24
   confirmations published at once. It samples `pg_stat_activity` for the sessions actually running
   the insert (the ground truth, not the engine's opinion) and the `matchmaking_db_writes_in_flight`
   gauge. Result: **12/12 written, 0 lost**; Postgres never ran more than a handful of inserts at a
   time and the gauge peaked at exactly `DB_MAX_IN_FLIGHT` (8); **118 confirmations were deferred**
   (KPI + `engine.backpressure confirmation deferred` in the log) and the burst took 11 s — longer
   than one database wait, which is what proves the trigger was real. The scenario leaves no rows
   behind.

Under load, the same cap at 4000 players (1v1/NA, concurrency 192, one build):

| Run | Admitted | Refused | Refusal p50 / p99 | Peak depth | Wait p50 / p95 / p99 | Matches confirmed | Dissolved |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `QUEUE_MAX_DEPTH=1000` | 2862 | 1138 | 672 / 904 ms | 984 | 7.43 / 10.57 / 11.28 s | 1431 | 0 |
| `QUEUE_MAX_DEPTH=0` (uncapped) | 4000 | 0 | — | 2182 | 11.11 / 16.86 / 20.86 s | 2000 | 0 |

The capped run is honest about what it bought: a third of the arrivals got a fast, explicit "no" in
under a second instead of a match 20 s later, and the players who *were* admitted waited much less
(the backlog is bounded, so the wait is bounded with it). Nothing was lost in either run — no
dissolved match, no duplicate event. `mm.sh load` counts the shed separately from errors for this
reason: a refusal on purpose is not a failure, but a run that hides it would be.

Two traps that cost time here, both about measuring under pressure: the harness's own probe
(`SELECT count(*) FROM pg_stat_activity WHERE query ILIKE '%insert into matches%'`) matches **its
own** statement, so it counted itself until it excluded `pg_backend_pid()` — and one `DEL` with N
keys is one round trip through the Docker daemon, while N `docker exec redis-cli del` calls are N
(2000 of them turned a 30 s cleanup into minutes).

## Degradation — what a dependency going away may do (W18)

```bash
bash requests/resilience/verify_degradation.sh     # 45 checks, 5 phases, ~5 min (ADR 0024)
```

The sprint is not about the player arriving too fast (W17) — it is about the dependency on the other
end of the wire stopping. Each phase stops (or restarts) one dependency with the stack alive and
traffic flowing, drives the affected flow, and asserts **both halves**: what the absence is allowed to
affect, and what it must not. The measured "before", on this stack:

| Phase | Dependency | Before |
| --- | --- | --- |
| 1 | Redis **restarted** | `POST /tickets` → **500 `NOSCRIPT`**, permanently; the matching loop stopped with it. Both services had to be restarted because a cache was. |
| 2 | Redis **down** | `500 {"error":"Connection refused: localhost/127.0.0.1:6379"}` — the wrong status *and* the internal address; `/health` → **200 UP** |
| 3 | Postgres **down** | the confirmation answered "fine": 2 tickets **`CONFIRMED`**, **0 rows**, the pending match swept, the tickets stranded forever; `/history` → 500 with the driver's message |
| 4 | Kafka **down** | (pre-W14) published on the confirmation path — a broker outage and a confirmation were the same failure |
| 5 | Schema Registry **down** | never exercised |

What the scenario asserts now:

1. **Redis restart.** An enqueue before and after the restart (`201` both), and — because a restart
   also kills the **Pub/Sub connections**, which is where the confirmations arrive — a full match
   driven to `CONFIRMED` after the restart, **with both service PIDs unchanged**. The scripts reload
   (`RedisScript`) and the subscription is rebuilt (`RedisSubscriber`).
2. **Redis down.** 20 arrivals → **20 × 503**, p50 **8.7 ms** / p99 **15.1 ms** / max **67.7 ms**
   (a refusal, not a wait), `Retry-After: 1`, a body naming `redis` and carrying **no internal
   address**; `GET /health` stays **200** while `GET /health/ready` turns **503 naming redis**; the
   refusals are counted in `matchmaking_dependency_errors_total{dependency="redis"}` (**+21**); the
   first arrival after Redis returns is admitted.
3. **Postgres down around a confirmation.** The match is formed and both tickets are `CONFIRMING`
   *before* the database is stopped; both confirmations are published and the engine **claims the
   match** (the assertion that the confirmation actually arrived); then: **no ticket reaches
   `CONFIRMED`, no row exists**, the pending match is still there. When Postgres returns, the same
   confirmation is **written** (the retry the gate already had) and only then do the tickets reach
   `CONFIRMED`. `/history` is a **503** that names the dependency and leaks no address.
4. **Kafka down.** The match is written in Postgres, the ticket is `CONFIRMED`, and the event sits
   `PENDING` in the outbox — a broker is not the source of truth. When the broker returns the event
   is published (**15 s**, the relay's backoff), **nothing parked, nothing duplicated**.
5. **Schema Registry down at engine start.** A fresh engine starts (the schema id is resolved at
   publish time, not at boot), the match is still written, the event waits in the outbox and is
   published when the registry returns (**6 s**).

Result of the run behind this section: **45 passed, 0 failed**. The first run of the same script was
**33 passed, 7 failed** — all seven caused by the dead subscription (phases 3 and 4) or by the
`500`/`503` detection, which is what turned a scenario into the evidence for two defects.

W18 touches code the earlier scenarios exercise, so they were re-run green afterwards: E2E
**52/52**, SLOs **36/36**, metrics **35/35**, backpressure **35/35**, dead letter **38/38**,
outbox **14/14**, queue reaper **10/10** — and the full suite, **162 tests, 0 failures**. Four of
those scenarios needed a fix of their own to get there (they were answering about the shared test
state instead of about the system); each one is documented where it lives (`requests/kafka` for the
two scans, `requests/kafka/verify_outbox.sh` for boot-vs-drain, `requests/engine` for the ghost race,
`requests/observability` for the burns).

The two traps this scenario paid for are worth more than the checks: **`flushall` does not clear the
script cache** (only `SCRIPT FLUSH` or a server restart does, so the phase restarts Redis), and **a
Redis restart silently kills the subscriptions** — the first run of this scenario had phases 3 and 4
fail for a reason that was not theirs: the engine had received no confirmation since phase 1, every
match dissolved at its deadline, and both processes looked healthy. That is the class of failure the
sprint exists for, found by running the thing (ADR 0024, `MelhoriasFutura.md` item 7).

## Routing — the socket is not pinned to an instance (W19)

```bash
bash requests/routing/verify_routing.sh     # 35 checks, 2 instances, ~3 min (ADR 0026)
```

The WebSocket contract (`03`) promises that a player whose notification was lost gets it back on the
next heartbeat. W4 implemented the local registry and the broadcast; W19 implements the rest. The
scenario starts **two** API instances (`api-a` on 8092, `api-b` on 8093, metrics 9192/9193) against
one Redis and one engine, and proves three things:

1. **Each notification is delivered by the instance that holds the socket.** Player A on A, player B
   on B: both are notified, and each instance also *counts* the notification it had no socket for
   (`matchmaking_ws_notifications_total{result="no_local_connection"}`, 1 per instance).
2. **A player who never received the notification still gets it.** Player D never connects; the match
   forms; instance A (holding player C's socket) is killed with `SIGKILL`. C and D connect to B and
   send `PING`: both receive `MATCH_PENDING_CONFIRMATION` with the same `matchId` — D for the first
   time. D had no other way to learn it.
3. **Any instance can finish the match.** Both confirm through B; the match is confirmed (claim
   present, `match-pending` gone, reverse index cleared, `dissolved` unchanged), and a later heartbeat
   re-presents nothing.

Measured "before" (the same scenario with the re-presentation removed): **27 passed / 8 failed** (18/8
in the probe), and the match **dissolved at its 30 s deadline** (`dissolved{reason="timeout"}` 0 → 1).
After: **26/0** in the probe, **35/0** with the script; first heartbeat → re-presentation **5 ms**;
presence `routing-2 → api-b` after the reconnect.

Presence, by hand: `redis-cli get presence:<playerId>` names the instance holding the socket (TTL 90 s,
refreshed on every heartbeat, cleared compare-and-set so a late disconnect from the old instance does
not erase the new one). `GET /health` answers `{"status":"UP","instance":"api-a"}`.

Two port traps, paid for here: the Schema Registry already holds host **8082** and Kafka **9092**, so
the second instance uses **8092/8093** and its scrapes **9192/9193**. The script frees both ports
before starting and kills only the instances it started (the 8080 API is untouched).

## Partitioning — one queue is one key (W20)

```bash
bash requests/partitioning/verify_partitioning.sh   # 27 checks, ~4 min (ADR 0027/0028)
```

A queue is `queue:{mode}:{region}`: one key, one Redis slot, one shard. Every arrival (`ZADD`) and
every pairing read (`ZRANGE`) of that (mode, region) lands on it, and in a Redis Cluster that key is
one shard no matter how many nodes are added. W20 gives the queue a **partition key**
(`QUEUE_PARTITIONS`, default 1) and makes the distribution visible. The scenario measures the same
2000 arrivals in both layouts, then proves that a partition is not a matching boundary.

1. **Before: one key.** 2000 arrivals → `{queue:1v1:NA: 2000}`, 100 % of the queue on one key —
   against an 8-way target that is a hotspot ratio of **8.0**.
2. **After: 8 keys.** `{0:246, 1:243, 2:256, 3:232, 4:239, 5:239, 6:273, 7:272}`, sum **2000**,
   deepest **13.65 %** of the queue, hotspot **1.092**. The key is the jump consistent hash of the
   enqueue key (`partyId` for a party, `playerId` otherwise), so a party stays whole in one
   partition.
3. **The scrape agrees with Redis.** Three 5v5 players (a queue that cannot pair, it needs ten) in
   partitions 4 and 5: `matchmaking_queue_partition_depth{mode="5v5",region="NA",partition=…}` =
   `[0,0,0,0,2,1,0,0]`, `matchmaking_queue_depth` **3**, `matchmaking_queue_hotspot_ratio` **5.333**
   = 2 ÷ (3/8), and the series sum to the depth.
4. **Two players in different partitions still pair** — the engine reads every partition's head,
   merges by wait score and calls `pair.lua` with all partitions as `KEYS`. Measured: partitions 3
   and 5 paired in **0.2 s**, both members left their own partition in the same script call.
5. **The cap is not multiplied**: `QUEUE_MAX_DEPTH=100`, `P=8` → 12 per partition → 96 admitted,
   104 refused, depth 96 ≤ 100 (the sum never exceeds the configured depth).

Ports: the scenario owns API **8094** (scrape 9195) and engine **9194**, and stops any engine it
finds before counting the queue — an engine left by a previous run pairs the arrivals mid-count.
By hand: `redis-cli zcard queue:1v1:NA:3` per partition, `redis-cli keys 'queue:1v1:NA*'`, and
`curl -s localhost:9194/metrics | grep queue_` for the gauges. With the default
`QUEUE_PARTITIONS=1` the key is still the historical `queue:1v1:NA`, and every older scenario is
unchanged.

## Regions — a failure domain per region, and distance as a number (W21)

```bash
bash requests/multiregion/verify_multiregion.sh     # 54 checks, ~6 min (ADR 0039)
```

A region used to be a **label**: a value that built a key (`queue:1v1:NA`) and a column, inside one
Redis. W21 makes it a deployment: each region runs its own API, its own engine and its own Redis,
while Postgres and Kafka stay shared (one durable log, one event stream). The scenario runs two
complete regions — NA on 8096/9197 with Redis 6380, EU on 8097/9199 with Redis 6381 — plus a delay
proxy (8099) that puts 40 ms each way in front of NA.

1. **The directory, before asking.** `GET /v1/regions` answers with the caller's home region (a
   **token claim**, not a request field), the regions this endpoint serves, and for each one the
   declared distance and — only if this endpoint measured it — the player's own RTT. EU→NA reads
   `declaredRttMs: 120`; `measuredRttMs` is absent until the player probes.
2. **The probe is the server's.** `GET /v1/regions/NA/ping` → `201 {nonce}`; `POST
   /v1/regions/NA/pong {nonce}` → `200 {region, rttMs}`. The nonce is single-use, 30 s, bound to
   (player, region). Measured: EU→EU **10 ms** (best of three), EU→NA through the proxy **99 ms**,
   and the placement number is `max(declared 120, measured 99) = 120`.
3. **A region this endpoint does not serve is refused**, even with a 1000 ms budget:
   `422 region_unsupported`, body naming `served: ["NA"]`. A replayed nonce is `422 invalid_nonce`.
4. **A budget is a distance, not a bias.** A 60 ms budget at the 120 ms region →
   `422 latency_budget {rttMs: 120, declaredRttMs: 120, measuredRttMs: 99, maxLatencyMs: 60}`; the
   same player with a 30 ms budget **inside** NA (declared 25 ms) is admitted.
5. **Inside the region vs across it.** An EU pair matched in EU in **796.9 ms** with a 14 ms RTT; the
   same pair, failed over to NA through the proxy, matched in **983.4 ms** with a 106 ms RTT.
6. **Isolation.** With EU's Redis stopped, EU's `/health/ready` is **503** naming `redis`, an arrival
   is **503 `hot state unavailable`**, and NA forms and confirms a match with no error.
7. **Returning does not match twice.** After EU's Redis restarts, EU has **0 queue keys and 0 ticket
   keys** (the region's Redis runs with `--save '' --appendonly no`: a queue is a claim about players
   who are queued *now*), no ticket in EU belongs to the player who left, the API never restarted,
   and Postgres holds **exactly one match** for that player.
8. **The scrape and the database agree with the decisions**:
   `matchmaking_tickets_rejected_total{reason="region_unsupported"|"latency_budget"}`,
   `matchmaking_latency_probes_total{result="measured"|"invalid_nonce"}`,
   `matchmaking_player_rtt_ms{region}` — and the matches of both regions in one `matches` table.

Ports: the scenario owns auth **8091**, APIs **8096/8097** (scrapes **9196/9198**) and engines
**9197/9199**, and starts its own Redis containers (`w21-redis-na`, `w21-redis-eu`). It starts its
**own** auth on purpose: a shared one may be an older build without the `region` claim, and the
scenario would then be measuring the wrong thing. By hand: `curl -s localhost:8096/v1/regions -H
"authorization: Bearer $TOKEN"`, `redis-cli -p 6380 keys 'latency*'`, and the collection's
`multiregion-w21` folder. The default deployment (`SERVED_REGIONS` unset, `REGION_RTT_MS` unset) is
the pre-W21 behaviour: one region, no constraint.

## Game servers — a confirmed match with nowhere to run (W22)

```bash
bash requests/gameservers/verify_capacity.sh       # 58 checks, 0 failures, ~103 s (ADR 0031)
```

Before W22 the system stopped at "the players confirmed". Which machine runs the match, and whether
one existed at all, was nobody's question — so a region at capacity confirmed matches that then had
nowhere to go and dissolved at the deadline, costing the wait **and** the match. W22 adds the pool
(the orchestrator's new internal HTTP API, `ORCHESTRATOR_PORT`, default **8085**, scrape **9093**)
and the two decisions that go with it: where a confirmed match runs, and whether forming one is
worth doing.

The scenario stops any engine and any orchestrator it finds (it needs to own the pairing loop and
the consumer group) and starts its own API (**8098**), engine (metrics **9101**) and orchestrator
(**8086**, metrics **9094**, `SERVER_TTL_SECONDS=10`). It plays the game servers too, re-registering
them every phase — which is what an agent's heartbeat is.

1. **No pool means no constraint.** `GET /v1/capacity` without the service token is **401**; with
   it and nothing registered, `authoritative: false` — *unknown*, not zero. The two players still
   pair and confirm exactly as in W21, and the match stays `PENDING_ALLOCATION` (W23 gives it a
   deadline).
2. **A pool with no room holds the queue.** A server registered with `capacity: 0` is `READY` and
   `free: 0` (full is not stale). Two arrivals are admitted, stay `SEARCHING`, no match is formed,
   `matchmaking_capacity_free{mode="1v1",region="NA"}` reads **0** and
   `matchmaking_pairing_suspended_total` climbs (measured 7 → 25 across the wait).
3. **A slot is one match.** Registering `capacity: 1` makes the waiting pair leave the queue within
   one poll; on confirmation the orchestrator writes `match_allocations` with `server_id =
   gs-w22-one` **and** takes the slot in the same transaction (`allocated 1, free 0`).
   `matchmaking_allocations_total{result="allocated"}` moves.
4. **The slot is consumed, and release gives it back once.** With the match running, the next pair
   waits. `POST …/allocations/{matchId}/release` answers `200` (`allocated 0, free 1`); a second
   call answers `404 allocation_not_found` and returns **no second slot**. The waiting pair takes
   the freed slot and is allocated again.
5. **Two confirmations cannot take the same last slot.** Two matches are formed against a pool with
   two slots, the pool then shrinks to **one**, and both confirmations arrive together:
   `match-90198069 → ALLOCATED`, `match-9da20a1c → PENDING_ALLOCATION`, `gs-w22-two` at
   `allocated 1 / capacity 1`. The row lock and `allocated < capacity` are what make this true.
6. **A silent server leaves by TTL, with no sweep.** Twelve seconds without a heartbeat drop the
   fresh server out of the pool (`free 1 → 0`), the listing derives `STALE`, a new pair waits — and
   pairs the moment the server speaks again.
7. **A drain is a decision, not an outage.** `DELETE` derives `DRAINING`, takes the slot out of the
   region (`free 0`, still authoritative) and the next pair waits instead of being refused; a
   heartbeat clears the drain and the pair is formed — on the server that came back, then released.
8. **Shrinking below what is running is refused** (`409 capacity_below_allocated`), and the row is
   left untouched (`capacity 1 / allocated 1`). The database closes the loop: no server has
   `allocated > capacity`, and `sum(allocated)` equals the `ALLOCATED` rows of this run.

`SERVER_TTL_SECONDS` must be the **same** in the orchestrator and the engine: the engine's gate reads
the same table. A mismatch is not fatal — the engine forms a match the allocator then refuses, so the
match is `PENDING_ALLOCATION` instead of lost — but it is capacity nobody uses. Ports: the folder
`gameservers-w22` in the collection is the by-hand version of the same path, against the default
`ORCHESTRATOR_PORT=8085` and `GAME_SERVER_TOKEN` (default `local-game-server-token`, replaced by mTLS
in a real deployment).

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
8. The same change (W13 task 3): `bash requests/kafka/check_proto_evolution.sh`
   refuses what the registry accepts (renumbering, removal without `reserved`, rename)
9. Publisher durability (W14): `bash requests/kafka/verify_outbox.sh` — a row with no engine
   running is still published after the restart, and a poison row is parked instead of stalling
10. Consumers (W13): start `match-orchestrator`, confirm a match (E2E S11) and watch the
   projection row appear; then `requests/kafka/verify_idempotent_consumer.sh` to rewind
   the group and prove a full replay applies nothing twice
11. Failure paths (W13): `requests/kafka/verify_dlq.sh` produces a poison record and a
   conflicting effect, and proves that both end on the dead letter topic with the offset
   committed instead of stalling the partition
12. KPIs (W15): `docker compose up -d prometheus grafana`, start both services with the
    metrics ports, `bash requests/observability/check_metrics.sh` (Prometheus:
    http://localhost:9095, Grafana: http://localhost:3000 → folder *Matchmaking*)
13. SLOs (W16): `bash requests/observability/check_slos.sh` — the rules are the repository's,
    the ratios match a hand count, and each alert expression is alive
14. Backpressure (W17): `bash requests/backpressure/verify_load_shedding.sh` — the rate limiter
    refuses a burst with `Retry-After`, the queue cap refuses at exactly its capacity, and a
    3 s-slow Postgres is asked to hold at most `DB_MAX_IN_FLIGHT` transactions at a time while
    every confirmed match is still written
15. Regions (W21): `bash requests/multiregion/verify_multiregion.sh` — two complete regions, the
    directory and the ping/pong probe, a budget refused with its numbers, one region's Redis dying
    while the other keeps serving, and the returning region not matching its failed-over player twice
16. Degradation (W18): `bash requests/resilience/verify_degradation.sh` — five phases stop and
    start Redis (restart and outage), Postgres, Kafka and the Schema Registry, and assert what
    each absence may and may not affect; `GET /health/ready` is the probe to route on (see the
    matrix below)
