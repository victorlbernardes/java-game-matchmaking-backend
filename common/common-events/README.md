# common-events

The event contracts published to Kafka: the `.proto` definitions, the mappers from the
domain to an event, and the registration of those schemas in the Schema Registry.

Consumers only need the subject in the registry — no compile-time dependency on the
producer. See `docs/adr/0016-*.md` for why one topic per event type, and
`docs/adr/0014-why-kafka.md` for the infrastructure.

## Layout

```
src/main/proto/matchmaking/events/match_confirmed.proto   # the event + its player state
src/main/java/com/victor/matchmaking/events/
├── EventTopics.java              # topic names (one per event type)
├── EventSerialization.java       # registry URL + Protobuf serializer properties
├── MatchConfirmedMapper.java     # Match + MatchPlayer -> MatchConfirmed (pure)
└── SchemaRegistrar.java          # versions the schemas in the registry (build time)
```

## Contract rules

- **The `.proto` is the contract.** Field numbers are permanent; a published number is
  never reused or renumbered. `EventMetadata.schemaVersion` is bumped only when the
  *meaning* of a field changes (the registry version is derived from the schema itself).
- **Events carry identifiers and snapshots, not entities.** A consumer must not need
  `Match`, `MatchmakingTicket` or the queue to act on an event.
- **The mapper is pure** (no clock, no broker): the caller passes `occurredAt` and
  `correlationId`, which makes the mapping reproducible in tests. An unknown team label
  fails instead of being published as an empty team.
- **`correlationId` groups the events of one match**; until OpenTelemetry lands (W15)
  the engine passes the match id.

## Schemas are versioned at build time, never at runtime

Services publish with `auto.register.schemas=false`: an unregistered change fails at
the first publish instead of silently creating a new subject version from a running
pod. That makes this step mandatory before the engine can publish:

```bash
mvn -B -pl common/common-events exec:java -Dexec.mainClass=com.victor.matchmaking.events.SchemaRegistrar
# schema.registered subject=matchmaking.events.match-confirmed-value version=1 id=2 schemaType=PROTOBUF
```

It runs the real serializer with registration enabled, so the schema and its imports
(`matchmaking/common/types.proto`) are registered exactly as the runtime serializer
resolves them. The registry is `FULL`, so an incompatible change is rejected and the
registrar exits non-zero — and the cause chain is printed (`schema.registration.cause`),
so a red build says which field the registry refused instead of just "failed".

The CI gate is `requests/kafka/check_contract_gate.sh` (job `contracts`): it registers
the contracts of the merge base, then the contracts of the change, and fails when the
registry refuses. See `docs/adr/0015-protobuf-vs-avro-ci-gate.md` — including what
`FULL` does *not* catch (renumbering, removal and renaming of a field are accepted).

Reading an event back (bytes → registry → JSON): `requests/kafka/verify_match_confirmed.sh`.

## The dead letter topic is not part of this contract

`EventTopics.deadLetterOf(topic)` (`<topic>-dlq`) is where `match-orchestrator` parks a
record it could not handle (ADR 0019). Its payload is the **original bytes**, untouched,
and its value is deliberately **not** a schema-managed message: nothing may assume that it
decodes, because the case it exists for is exactly a payload that does not decode. The
diagnosis travels in the record headers (`dlq.stage`, `dlq.attempts`, `dlq.error-class`,
`dlq.original-*`). No subject is registered for it, and none should ever be.
