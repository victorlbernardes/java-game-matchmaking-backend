# ADR-0018 — Transactional outbox: the event is written with the fact it announces (W14)

## Status
Accepted (2026-10-17)

## Context
The engine's confirmation did two things in sequence and needed both to survive: it wrote the
match to Postgres and then published `MATCH_CONFIRMED` to Kafka. Two durable systems, no
distributed transaction, and a window between them:

- a crash (or a pod eviction, or a `kill -9` during a deploy) after the commit and before the
  publish left a **confirmed match that no consumer ever hears about** — the orchestrator never
  allocates a server, telemetry never counts it, and nothing in the system knows it is missing;
- the reverse order is no better: publishing first and committing after produces an event for a
  match that does not exist. The consumer cannot tell it apart from a real one.

Retrying the publish in a loop helps only while the process lives, and a broker that is down for
a minute turns every confirmation into a failure — the match is durable, so failing it would be
a lie. The engine was written this way since W12, and the W13 work (idempotent consumer, DLQ)
made the *consumer* side of at-least-once trustworthy; the producer side still had the gap.

## Options
1. **Publish inside the transaction (a Kafka transaction with `sendOffsets`/2PC at the application
   level)** — the standard "exactly once" story. Rejected for this shape: it is a Kafka
   *producer* transaction (`beginTransaction` … `commitTransaction`) that still has to be tied to
   the Postgres commit, and there is no way to commit both atomically without XA. Two-phase commit
   across Postgres and Kafka is a distributed-coordinator problem: a blocker, a recovery log, and
   an availability loss when either side is slow. The cost is far above the value of the property.
2. **Publish synchronously and block the confirmation on the broker** — that is what W12 did
   (the match was confirmed only after the ack). Rejected: it makes Kafka a hard dependency of
   the confirmation path and loses the event on a crash in between anyway.
3. **Transactional outbox** — the fact and the event are written in **one** Postgres transaction;
   a relay reads the table and publishes. Chosen.
4. **Change data capture (Debezium on the `matches` table)** — a real alternative, and a heavier
   one: a connector, a replication slot, another runtime to operate, and the event would be
   derived from the table rather than built by the domain. It also cannot express an event that
   is not a row (a confirmation, a dissolution). Recorded as deferred, not rejected
   (`MelhoriasFutura.md`).

## Decision
A confirmed match and the event that announces it are written in the same transaction, into
`outbox_events`; a relay in the engine publishes the pending rows and marks them published only
after the broker acknowledges them.

### The write side
`PostgresOutboxQueue.writeMatchWithEvent(match, players, event)` runs one
`pool.withTransaction`: the `matches` row, the `match_players` rows and the outbox row. Either
all three exist or none does. The match's own SQL is the same code the history endpoint already
used — `MatchRepository.saveOn(connection, …)`, now callable on a connection the caller owns, and
`MatchRepository.save` composes it inside a transaction of its own.

The event is built **at the moment the fact happens** (its `occurred_at` is the confirmation
time, its `event_id` is the idempotency key the consumer deduplicates by) and stored as the bare
Protobuf message:

- **without the Confluent wire framing**, so resolving the schema id is not part of the
  confirmation transaction. Framing there would make the Schema Registry a dependency of
  *persisting a match* — a registry that is down would refuse confirmations. It delays the event
  instead, which is what a queue is for;
- **as bytes, not as fields**, so the producer cannot rebuild a different event later: what goes
  out is what was written. `EventContracts.parse(topic, payload)` (common-events) turns the bytes
  back into the typed message, by topic — one topic per event type (ADR 0016) is the routing
  table this needs.

### The read side: a lease, not a lock
The relay claims a batch with `SELECT … FOR UPDATE SKIP LOCKED` and then stamps `claimed_at` /
`claimed_by`, both inside one transaction:

- **`SKIP LOCKED`** is what makes two relays safe: the second one skips the rows the first is
  working on instead of waiting behind them;
- **the rows stay `PENDING`**, with `attempts` and `next_attempt_at` on the row. An event that was
  never acknowledged is not published data, and the retry policy must survive the process that
  wrote it;
- **the claim is a lease, not a lock**: a relay that dies holding a row does not take the event
  with it. After 30 s the row is claimable again, which is exactly what the crash recovery of
  option 3 requires.

Publishing a batch is deliberately sequential, ordered by `created_at, event_id`: the events of
one match keep the order they were written in, and one row that fails does not hold up the rows
behind it in the batch.

### At-least-once, on purpose
A crash between the broker's ack and `markPublished` publishes the record again. The alternative
would be a two-phase commit across Postgres and Kafka (option 1). The duplicate is absorbed by
the consumer's inbox (ADR 0038), which is exactly the machinery W13 built — the outbox makes the
producer side of "at least once" honest, and the inbox makes it harmless. The row is only marked
`PUBLISHED` *after* the ack, never before and never optimistically.

### Failure policy, in the row
A failed publish is rescheduled: `attempts + 1`, `last_error`, and `next_attempt_at = now + min(1s
× 2^attempts, 30s)`. A broker outage therefore delays events (1 s, 2 s, 4 s, …) instead of burning
every attempt in a couple of seconds. After 10 attempts the row is **parked** with its reason:

- parked rows are visible in the table (and in W15's metrics), never deleted;
- parking does not block the queue — ordering is per batch, and the next batch does not wait;
- an operator can requeue one: `UPDATE outbox_events SET status = 'PENDING', attempts = 0,
  next_attempt_at = NULL WHERE event_id = …`;
- a payload no code can decode (or a topic this build does not know) is parked immediately
  instead of retried: no amount of waiting turns unknown bytes into an event, and the reason
  belongs in front of a human.

`PUBLISHED` rows are kept — an event that was published is a fact worth auditing. Purge is an ops
decision (`DELETE FROM outbox_events WHERE status = 'PUBLISHED' AND published_at < now() - interval '30 days'`),
documented rather than scheduled here.

### Where it runs
A Vert.x periodic task in the engine every 250 ms calls `OutboxRelay.drainOnce()`. The JDBC client
and the Kafka producer are both asynchronous, so this is not a thread and never blocks the event
loop. The relay is identified on every row it claims (`engine-<pid>`), which is what an operator
needs to read a stuck queue.

### Measured (dev machine, 2026-10-17)
- **The whole suite grew by 16 tests**: 8 unit tests on the relay's policy (`OutboxRelayTest`) and
  8 against real Postgres (`PostgresOutboxQueueTest`) — 46 in the engine module, 0 failures;
- **the atomicity property is asserted, not assumed**:
  `anEventThatCannotBeWrittenTakesTheMatchWithIt` makes the event insert fail on a duplicate
  `event_id` and requires the match row and its roster **not** to exist afterwards. Without the
  transaction the match would be committed and no event would ever announce it;
- **claim exclusivity and crash recovery**: with a live lease a second claim gets nothing
  (`SKIP LOCKED` working), and after the lease expires the row is handed out again;
- **`requests/kafka/verify_outbox.sh`: 14 checks, 0 failures, 92.8 s** (including the `mvn
  install`): a row taken out of the queue and left with no engine running is still `PENDING` 3 s
  later, and **is out on the topic 13 s after the engine comes back** — engine boot included
  (~9 s of that is Maven + JVM startup);
- **the E2E is green with the outbox in the path: 52 checks, 0 failures**, and the scenario's own
  measurement of `CONFIRM → MATCH_CONFIRMED on the log` came out at **658 ms** (poll interval
  250 ms);
- **steady state**: rows reached the topic with a lag of **91 ms** (`occurred_at → published_at`)
  in the two confirmations of that run; the first publish after a cold start paid **1.6 s** (the
  producer's metadata + schema lookup), which the outbox hides from the confirmation path
  entirely.

## How it works
1. `POST /v1/matchmaking/tickets/{id}/confirm` (or the WebSocket flow) increments the Redis
   confirmation counter; the last confirmation makes the engine call `confirm(…)`;
2. the engine builds `MatchConfirmed` (mapper, as before) and calls
   `writeMatchWithEvent(match, players, event)`: one transaction writes the match, the roster and
   the outbox row;
3. the relay's next pass (≤ 250 ms) claims the row, decodes the payload by topic, publishes it
   keyed by `matchId` (same partition per match, ADR 0014/0016) and marks it `PUBLISHED`;
4. if the broker refuses, the row is rescheduled with backoff; if the failure cannot be fixed by
   retrying, it is parked with the reason.

## How to test
```bash
# the relay's policy (no broker, no database)
mvn -B -pl services/matchmaking-engine -am test -Dtest=OutboxRelayTest

# the properties only Postgres can provide (needs docker compose up -d postgres)
mvn -B -pl services/matchmaking-engine -am test -Dtest=PostgresOutboxQueueTest

# the whole story by hand: a queue that survives its relay, a poison row that does not block it
bash requests/kafka/verify_outbox.sh
```

## Trade-offs
- **At-least-once, not exactly-once**: a crash between the ack and the mark duplicates a record.
  Accepted, because the alternative is a distributed transaction and the duplicate is already
  handled by the consumer (ADR 0038).
- **The event is in the confirmation transaction**: the transaction is a little larger and holds
  its locks a little longer (a `matches` insert, N roster rows, one outbox row).
- **Lag**: up to one poll interval (250 ms) plus the broker's own latency is added to
  `CONFIRM → event on the topic`. The measurements above say 91 ms typical. What is gained is that
  a broker outage no longer fails a confirmation.
- **Per-key ordering holds for one relay.** Two relays can publish two rows of the same key out of
  order, because each claims its own batch. Today the engine runs a single relay and the claim
  reads in insertion order; scaling the relay out (W20) needs the claim partitioned by
  `partition_key`, and that is the point where this trade-off has to be revisited.
- **The outbox is a queue in a table**: it needs an index (given), a purge policy (documented) and
  a person to read the parked rows (the operator path above). Debezium (option 4) would remove the
  relay and add a connector plus a replication slot; the balance is written down in
  `MelhoriasFutura.md`.

## Consequences
- A match can no longer exist without the event that announces it, and an event can no longer
  exist for a match that was not committed. Both directions are now one write.
- `KafkaEventPublisher` is generic (topic, key, message) instead of match-specific: the publisher
  cannot change the event on its way out, and a second event type needs no new client.
- The engine's `match.confirmed` log line says `outbox=PENDING` instead of reporting a broker ack:
  the confirmation no longer waits for Kafka, and the relay's own lines (`outbox.published`,
  `outbox.retry`, `outbox.parked`, with `lagMs`) are the place to look for what the topic has.
- The relay is the only publisher in the engine, so a future event type is added by writing a
  contract (ADR 0017), a row and a parser entry — not by touching the confirmation path.
- W15 (observability) inherits a countable backlog: `outbox_events` by `status`, the parked rows
  with their reasons, and the lag as a histogram.
