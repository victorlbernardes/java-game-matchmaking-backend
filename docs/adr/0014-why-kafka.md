# ADR-0014 — Why Kafka (with a Schema Registry) for events (W12)

## Status
Accepted (2026-10-16)

## Context
Until W12 the only event path was `RedisPublisher` → Redis pub/sub: `matchmaking-engine`
publishes `MATCH_CONFIRMED` and each `matchmaking-api` instance pushes it to the
players' WebSockets. That is enough for a live push and for nothing else:

- **No persistence, no replay.** A subscriber that is down (deploy, restart, scaling
  up) simply misses the event; there is no log to re-read.
- **No consumer groups.** Every service needing the same event needs its own
  mechanism — no offset, no lag, no "read from the beginning".
- **No contract governance.** The payload is a hand-built JSON envelope
  (`Envelope.of("MATCH_CONFIRMED", …)`); renaming a field on the producer side compiles
  and breaks consumers at runtime.
- **Fan-out is bound to the instant of the publish.** `match-orchestrator` (W13+),
  telemetry and `skill-service` would all have to be online the moment a match is
  confirmed.

`04 - Engenharia Distribuída` fixes the target: Protobuf events, one topic per event
type, a Confluent Schema Registry as the formal contract, `FULL` compatibility and a CI
gate that fails the build on `INCOMPATIBLE`. W12 must build and **validate** that
infrastructure before the first event can be published (task 2) and gated (task 3).

The `zookeeper` / `kafka` / `schema-registry` services were declared in
`docker-compose.yml` since W1 and had **never been started**. Validating them is the
point of this task.

## Options
1. **Redis pub/sub (current state)** — lowest latency, no new infrastructure. No
   durability, no replay, no consumer groups, no schema registry, no contract gate. It
   stays the right tool for an ephemeral WebSocket push.
2. **RabbitMQ** — mature routing (exchanges, dead-lettering), good for command/work
   queues. Weaker as an *event log*: replaying history means redesigning around it,
   while this project needs a durable, replayable log with per-consumer offsets.
3. **Kafka + Confluent Schema Registry (Protobuf)** — durable partitioned log, consumer
   groups with offsets and lag, replay from the beginning, and a registry that versions
   schemas and rejects incompatible changes. Protobuf keeps a single IDL with the gRPC
   contracts already in `common-proto` (Avro would add a second one and a second codegen).

## Decision
Kafka is the event backbone between services; Redis pub/sub remains only the ephemeral
push to live WebSocket sessions. Contracts are Protobuf, one topic per event type,
registered with `FULL` compatibility and `auto.register.schemas=false` (the schema is
versioned at build time, never at runtime).

## How it works
Topology in `docker-compose.yml`:

| Container | Port (host) | Role |
|-----------|-------------|------|
| `zookeeper` (`cp-zookeeper:7.7.1`) | 2181 | Broker metadata and controller election |
| `kafka` (`cp-kafka:7.7.1`) | 9092 (EXTERNAL) | Single broker; clients on the host connect here |
| `schema-registry` (`cp-schema-registry:7.7.1`) | 8082 → 8081 | Schema versions, stored in the `_schemas` topic |

**Two listeners, not one.** A broker answers a client with the *advertised* address of
the listener that client connected to. With a single `PLAINTEXT://localhost:9092`, the
Schema Registry — itself a Kafka client, since it stores every schema version in the
`_schemas` topic — is told to dial `localhost:9092`, which inside its own container is
itself: the registry never becomes ready. Reproduced, not assumed: running that exact
W1 configuration in an isolated network (`KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092`,
registry bootstrapping `kafka:9092`) makes the registry loop on
`Connection to node 1 (localhost/127.0.0.1:9092) could not be established` and it never
serves HTTP — port 8081 refuses the connection. Which is precisely why this task
validates the stack instead of assuming it. Advertising `kafka:29092` instead would work
inside the Compose network and fail for anything on the host. So:

```
KAFKA_LISTENERS:            INTERNAL://0.0.0.0:29092,EXTERNAL://0.0.0.0:9092
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:29092,EXTERNAL://localhost:9092
KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
```
and the registry bootstraps against `kafka:29092`.

**Startup order is enforced by healthchecks**, not by `depends_on` alone: `depends_on`
waits for the container to *start*, not to be *usable*. Zookeeper reports ready with
`cub zk-ready localhost 2181`, Kafka with
`kafka-broker-api-versions --bootstrap-server localhost:29092`, the registry with
`curl -fsS http://localhost:8081/subjects`; each service depends on the previous one
with `condition: service_healthy`. The published registry port is **8082** on the host
because 8081 is already the `auth-service` port.

**Compatibility** is `FULL` via `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL=full` — the
server-side half of the CI gate. Topics use one schema per event type, subject
`matchmaking.events.<event>-value` (TopicNameStrategy, pinned by
`subject.name.strategy` on the client side in task 2).

Measured on the dev machine (single broker, single partition; ranges from repeated
`check_kafka.sh` runs):

| Metric | Value |
|--------|-------|
| `docker compose up -d zookeeper kafka schema-registry` → all healthy | 54 s |
| Broker accept (write → record `CreateTime`) | p50 0–1 ms |
| Produce-ack, `acks=all`, 2000 × 128 B | p50 9–17 ms, p95 29–35 ms, p99 47–52 ms |
| Throughput, single producer | 2.2–3.1 k records/s |
| `kafka-console-consumer` CLI output floor | 0.5–1 s (tool artifact — see trade-offs) |

The worst single sample seen for broker accept was 188 ms — a client-side
batching/scheduling outlier in one run, not a broker stall (the p50 stayed at 1 ms in
that same run). Hence ranges, not best-case numbers.

## How to test
```bash
docker compose up -d zookeeper kafka schema-registry   # ~55s to all healthy
bash requests/kafka/check_kafka.sh                     # 9 checks, non-zero exit on failure
```

The script asserts the three containers are healthy, that the registry answers on the
host port 8082 with compatibility `FULL` and supports `PROTOBUF`, that the registry
reached the broker over the INTERNAL listener (the `_schemas` topic it owns exists), and
that 200 messages written to the EXTERNAL listener come back in order and none lost.

The registry REST API is also in the Postman collection (`infra/schema-registry`):
`GET /config`, `GET /subjects`, `GET /schemas/types`.

## Trade-offs
- **Zookeeper is still there.** Newer brokers run in KRaft mode without it; this stack
  keeps the classic mode because W12 needs a working, validated broker, not a migration.
  KRaft removes one container and one startup dependency — a follow-up, not a blocker.
- **Kafka is a heavier dependency than Redis pub/sub**: a second stateful component the
  event path now depends on. Accepted because the pub/sub path stays for the live push,
  so a broker outage degrades history and consumers, not the match flow.
- **Protobuf is not self-describing**: consumers need the `.proto` plus the registry to
  resolve the schema id. A schema-less reader (notebooks, data lake) would need a
  projected JSON/Avro topic — deliberately out of scope, noted in `04`.
- **The CLI latency is not the broker's.** The 0.5–1 s measured with
  `kafka-console-consumer` is that tool's own output granularity, in 500 ms steps: in
  the very same run the record's `CreateTime` lands at p50 0–1 ms after the write (measured with
  a warm CLI producer and consumer, so JVM startup is excluded). This is why the check
  reports both numbers instead of a single misleading "publish → consume latency"; the
  consumer-side latency that matters is the Java client's, measured in task 2.

## Consequences
- Task 2 publishes `MATCH_CONFIRMED` as a typed `matchmaking.events.MatchConfirmed`
  Protobuf message on the `matchmaking.events.match-confirmed` topic (subject
  `matchmaking.events.match-confirmed-value`), retiring the hand-built JSON envelope;
  task 3 adds the CI gate that registers the schema and fails the build on
  `INCOMPATIBLE`.
- Any new consumer (match-orchestrator, telemetry, skill recalculation) needs only the
  subject — no compile-time dependency on the producer.
- `docker-compose.yml` becomes the single definition of the event infrastructure, and
  it is now exercised by a re-runnable check instead of being an untested declaration.
