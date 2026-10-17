# match-orchestrator

Consumes `MATCH_CONFIRMED` and keeps every confirmed match ready for a game server.

## What it does today (W13)

- **Consumer group `match-orchestrator`** on `matchmaking.events.match-confirmed`, with
  the Confluent Protobuf deserializer: the schema is resolved from the schema id at the
  start of the payload through the Schema Registry (symmetry with the producer, ADR 0016).
  The value is consumed as bytes and decoded one record at a time, so a payload that does
  not decode is attributable — and therefore dead-letterable.
- **Idempotent projection**: each event becomes one row in `match_allocations`
  (`PENDING_ALLOCATION`) and that row is written in the same transaction as the inbox row
  in `processed_events`. A redelivery — rebalance, restart, offset reset — is recognised
  by `EventMetadata.event_id` and applies nothing. See
  `docs/adr/0038-consumer-idempotency.md`.
- **Manual offset commits**, after the effect: at-least-once delivery, exactly-once
  effect.
- **Bounded retry and a dead letter queue**: 3 attempts with backoff (250 ms doubling),
  covering decoding and applying together; then the record goes to
  `matchmaking.events.match-confirmed-dlq` with its original bytes and diagnostic headers,
  and its offset is committed so the partition moves on. Nothing is committed if the dead
  letter cannot be written. See `docs/adr/0019-dlq-retry.md`.

## Layout

```text
domain/          AllocationRequest, ProcessedEvent, AllocationStatus, ProcessingOutcome
application/     ConfirmedMatchInbox, OffsetCommitter, MatchConfirmedDecoder,
                 DeadLetterQueue (ports), ProcessConfirmedMatch (use case), RetryPolicy
infrastructure/  KafkaMatchConfirmedConsumer, MatchConfirmedTranslator, ProtobufEventReader,
                 PostgresConfirmedMatchInbox, KafkaDeadLetterQueue
```

## Running it

The service consumes its topic on its own, but the **dead letter topic must exist before
the first failure**, because an undeliverable dead letter leaves the offset uncommitted:

```bash
docker exec game-matchmaking-backend-kafka-1 kafka-topics --bootstrap-server localhost:29092 \
  --create --if-not-exists --topic matchmaking.events.match-confirmed-dlq \
  --partitions 1 --replication-factor 1
```

See the start order in `requests/README.md`. It needs Postgres and Kafka only:

```bash
mvn -B install -DskipTests
mvn -pl services/match-orchestrator exec:java -Dexec.mainClass=com.victor.matchmaking.orchestrator.MatchOrchestratorApplication
```

Environment: `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`),
`SCHEMA_REGISTRY_URL` (default `http://localhost:8082`), `DB_JDBC_URL` / `DB_USER` /
`DB_PASSWORD` (defaults in `DatabaseConfig`), `CONSUMER_MAX_ATTEMPTS` (default 3),
`CONSUMER_FIRST_BACKOFF_MS` (default 250).

The failure paths are covered end to end by `requests/kafka/verify_dlq.sh` and the replay
by `requests/kafka/verify_idempotent_consumer.sh`.

## What is not here yet

- **Server allocation** (W22): the row stays `PENDING_ALLOCATION`; choosing a server,
  capacity awareness and the allocation timeout come with the orchestrator's real job.
- **Alerting on the dead letter queue** (W15): a record that reaches it is logged, but
  nothing watches the queue's depth or rate.
- **Replaying the dead letters**: the payload is preserved and still decodes (measured),
  but nothing consumes the DLQ — picking records and re-applying them is manual today.
