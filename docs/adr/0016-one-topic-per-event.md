# ADR-0016 — One topic per event, with the envelope as a message (W12)

## Status
Accepted (2026-10-17)

## Context
ADR 0014 put a broker and a Schema Registry in place; this task publishes the first
event through them. Until now the only "contract" was a hand-built JSON envelope:
`Envelope.of("MATCH_CONFIRMED", new Payloads.MatchConfirmed(matchId))` written to a
Redis channel. That shape lived in code on both sides: renaming a field compiles and
breaks the consumer at runtime, there is no version, nothing tells a consumer what it
is reading, and nothing survives a restart of the process that reads it.

The fact being announced — *a match reached its confirmations and was persisted* — is
what `match-orchestrator` (W13), telemetry and skill recalculation all need. It has to
be durable, self-describing and governed.

Three decisions were open: how topics are laid out, how the envelope travels, and how
the producer obtains its schema.

## Options
1. **One topic for all events** (`matchmaking.events`) with a generic envelope — a
   single schema holding every event's fields, a `oneof`, or `Any`. Cheap to operate,
   but every consumer then resolves and evolves against the union of all events: a
   change to any event becomes a change to everyone's contract, and `FULL`
   compatibility starts rejecting changes that have nothing to do with the event a
   given service cares about. Rejected.
2. **One topic per event type** — each topic carries one self-contained schema, so a
   consumer subscribes to exactly what it needs, and its compatibility gate reacts
   only to its own event. Costs a topic per event type and a deliberate creation step.
3. **Keep JSON, versioned by a JSON Schema in the registry** — no codegen, but the
   contract stops being checkable at compile time (rule 11: types over `JsonObject`)
   and the project would carry a second IDL next to the Protobuf it already uses for
   gRPC.

For the transport client: `vertx-kafka-client` versus `kafka-clients` directly. For
the schema: auto-registration at runtime versus registration in the build.

## Decision
1. **One topic per event type**, subject `<topic>-value` (TopicNameStrategy), holding
   **one event per message** — the envelope is a *message* (`EventMetadata meta = 1`),
   not loose fields duplicated in every event nor an out-of-band header.
2. Contracts are Protobuf, compiled at build time. Types shared by every event
   (`EventMetadata`, `GameMode`, `Region`) live in `common-proto`; each event lives in
   `common-events`.
3. The engine publishes with the Confluent `KafkaProtobufSerializer` over
   **`kafka-clients` directly**, keyed by `matchId`, **after** the match row is
   persisted. Redis pub/sub keeps carrying the live WebSocket push (ADR 0014).
4. Services run with `auto.register.schemas=false`. Registration happens in the build
   (`SchemaRegistrar`), which is also where an incompatible change fails.

## How it works

### The contracts
| File | Package | Content |
|------|---------|---------|
| `common-proto/src/main/proto/matchmaking/common/types.proto` | `matchmaking.common` | `EventMetadata`; enums `GameMode`, `Region` |
| `common-events/src/main/proto/matchmaking/events/match_confirmed.proto` | `matchmaking.events` | `MatchConfirmed`, `MatchPlayerState` |

Shared types stay in one file so a second event reuses the same `GameMode`/`Region`
instead of defining a copy that can drift — duplicated enums would break `FULL`
compatibility for reasons unrelated to the event. `protoc` runs from
`protobuf-maven-plugin` with the platform classifier from `os-maven-plugin`, pinned to
the `protobuf-java` version the Confluent provider declares (`3.25.4`); generated code
is not committed. Each file sets `java_package`, `java_multiple_files` and an explicit
`java_outer_classname` (`MatchConfirmedEvent`, `CommonTypes`) — without it the outer
class of `match_confirmed.proto` would collide with its own `MatchConfirmed` message.
Proto3 enums keep a `*_UNSPECIFIED = 0` value because the language requires a zero;
the mapper never produces it, and a test enforces that.

### The envelope as a message
`EventMetadata` (field 1 of the event) carries:

| Field | Meaning |
|-------|---------|
| `event_id` | uuid, fresh per delivery — the basis of consumer-side idempotency |
| `event_type` | `MATCH_CONFIRMED` (mirrors the topic, not the Java class name) |
| `schema_version` | version of the **contract semantics**, carried in the payload |
| `occurred_at` | `google.protobuf.Timestamp` of the fact, not of the publish |
| `correlation_id` | groups the events of one match; the match id until OpenTelemetry lands (W15) |

`schema_version` is deliberately not the registry's subject version: the registry
derives a version from any schema change, including purely cosmetic ones, while this
field is bumped only when the *meaning* of a field changes.

### Mapping (pure, no infrastructure)
`MatchConfirmedMapper.toEvent(match, players, occurredAt, correlationId)` takes the
clock and the correlation id as parameters, so the mapping is reproducible and
testable without a broker. It translates the domain's team labels `A`/`B` to the
contract's `1`/`2` and **fails fast on an unknown label**: publishing `0` would reach
consumers as data that looks valid. `Timestamp` is built from
`Instant.getEpochSecond()/getNano()` — no `protobuf-java-util` dependency is needed for
that.

The event carries identifiers and a skill snapshot, never the domain entities: a
consumer must not need `Match`, `MatchmakingTicket` or the queue to act on it.

### The producer
`MatchEventPublisher` (port, `application/`) is implemented by
`KafkaMatchEventPublisher` (`infrastructure/`):

- **key = `matchId`** — every event of one match lands on the same partition, so a
  consumer reads that match's history in order.
- `acks=all` and `enable.idempotence=true` explicit: the engine reports the match as
  confirmed only once the broker acknowledged it.
- `max.block.ms=5000`: `send` can block while it fetches metadata or while its buffer
  is full. Bounded (see trade-offs).
- **Why not `vertx-kafka-client`:** the Confluent serializer is compiled against
  Confluent's `kafka-clients` (`7.7.1-ccs`, Kafka 3.7 base) while `vertx-kafka-client`
  5.2.0 resolves Kafka 4.x; two `kafka-clients` on one classpath is a
  `NoSuchMethodError` waiting for the worst moment. And it is not needed:
  `KafkaProducer.send` is already asynchronous, so the reactive integration is a
  callback-to-Vert.x-`Promise` bridge — no event loop blocked, no second client.
- **Publish after persist:** the event is produced inside the `compose` of
  `MatchRepository.save`, so an event never announces a match that is not in the
  database. Atomicity between the two writes is the Transactional Outbox (W14) — today
  a crash in between loses the event, which is exactly the gap W14 closes.
- **Redis stays:** the live push to WebSocket sessions keeps using
  `RedisPublisher`/`Envelope`; Kafka is the durable log (ADR 0014). The JSON envelope
  is no longer the event contract, only a push payload.

### Registration, and the gate that comes with it
With `auto.register.schemas=false`, the serializer looks the schema up by subject and
**fails** when it is not registered. `SchemaRegistrar` runs the real serializer with
registration enabled at build time, so the schema and its imports are registered
exactly as the runtime serializer resolves them. The registry is `FULL`, so an
incompatible change is rejected, the registrar exits non-zero and the build fails —
task 3 wires that into CI as the gate.

### Measured (dev machine, single broker/partition)
| Metric | Value |
|--------|-------|
| `mvn -B clean test` (whole reactor) | 72 tests, 0 failures (65 before this task) |
| Mapper tests (unit, no infrastructure) | 7 checks |
| E2E `python3 requests/e2e/run_e2e.py` | 47 checks (10 scenarios), 0 failed (37 before S10) |
| E2E S10: CONFIRM published → `MATCH_CONFIRMED` on the log | 416–831 ms over 4 runs (median ~600 ms) |
| Registry after registration | `matchmaking.events.match-confirmed-value` version 1, id 2, `PROTOBUF` |
| Registry imports resolved | `matchmaking/common/types.proto` registered as a reference of the event |
| Registrar re-run with an unchanged schema | stays version 1, id 2 — no version churn |

The 416–831 ms is `EVENT_CREATE_TIME_MS` (the broker's record time) minus the moment
the E2E published the two `CONFIRM`s: it covers both Redis round trips, the ticket
state writes, the match insert and the produce-ack, on the same host. It is a range
from repeated runs, not a best case — the spread is scheduling noise, and the largest
value came from the run right after a rebuild (cold JIT/class loading).

## How to test
```bash
mvn -B install -DskipTests                     # protos compile, services build
mvn -B clean test                              # 65 tests, incl. the mapper contract tests
# infrastructure + services (order matters: register before starting the engine)
docker compose up -d zookeeper kafka schema-registry
mvn -B -pl common/common-events exec:java -Dexec.mainClass=com.victor.matchmaking.events.SchemaRegistrar
mvn -pl services/matchmaking-engine exec:java -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication
# drive a confirmed match and read the event back
python3 requests/e2e/run_e2e.py                # S10 confirms and asserts the event
requests/kafka/verify_match_confirmed.sh <matchId>   # decodes the log through the registry
```

`verify_match_confirmed.sh` is the check that fails if the property is violated: a
producer that publishes a wrong shape, an unregistered schema, a lost event or a
missing field turns it red — the bytes are read back through the registry that a real
consumer would use. It exits 0 only when the subject is registered and every expected
record decodes.

## Trade-offs
- **Two publishes per confirmation (Kafka + Redis) until W14.** They have different
  purposes (durable log versus live push) but today they are two writes from the same
  process with no transaction. The outbox is the answer, and it is scheduled.
- **`send` can still block up to `max.block.ms`** on the first record (metadata fetch)
  or when the buffer is full. Bounded to 5 s, and W14 moves the produce off the
  confirmation path.
- **The Confluent artifacts are not on Maven Central.** The parent POM adds
  `packages.confluent.io/maven` (public, no credentials), so CI build time now depends
  on a second repository.
- **`protoc` is pinned to the provider's Protobuf version (`3.25.4`).** Upgrading the
  registry means re-checking that pin; a mismatch fails at runtime, not at build time.
- **`correlation_id` is the match id** until W15 adds tracing — it already groups the
  events of a match, but it is not a trace id.
- **`schema_version` is carried but read by nobody yet.** It exists so a consumer can
  detect a semantics change the registry cannot express; it costs one field.
- **The verifier reads partition 0** of the topic (broker default single partition).
  A multi-partition topic would need it to consume the whole topic.
- **The event is a snapshot of the moment.** A consumer needing the current roster must
  read the later events (or the API); this is by design, not an oversight.

## Consequences
- W12 task 3 wires `SchemaRegistrar` into CI: a PR that breaks `FULL` compatibility
  fails the build instead of failing in production (ADR 0015).
- Any consumer needs only the subject name; there is no compile-time dependency on the
  producer, and no consumer needs to be online when the match is confirmed.
- The E2E now covers the confirmation path end to end (S10), including the fact that
  the event, the match row and the tickets agree — a regression in any of them fails
  the suite.
- Adding the next event is a checklist: a `.proto` in `common-events`, a mapper, a
  topic constant, and a line in the registrar's contract list.
