# ADR-0019 — Retry and dead letter queue: a record that cannot be handled must not stall the partition (W13)

## Status
Accepted (2026-10-17)

## Context
"At least once" (ADR 0016) means a consumer will eventually meet a record it cannot
handle. Two shapes, and they need different diagnostics but the same destination:

- **the payload never decodes** — a producer wrote something the registry cannot resolve,
  a schema id was mangled in flight, or the bytes are simply not a Confluent-framed
  Protobuf message at all;
- **the payload decodes but the effect cannot be applied** — a data conflict (the row is
  already there), a bug in the consumer, a database that refuses the write.

Without a policy the unhandled record is left uncommitted, and that is not a local
failure: `match-orchestrator` polls, fails on the head of the partition and never
advances, so **every event behind it is blocked**, and it will be re-read after every
restart and every rebalance. ADR 0038 stated this boundary explicitly when it chose the
inbox: the failing record was redelivered after a restart, was not retried within the
session, and blocked its partition. This sprint closes it.

Two questions were open: how many times to try (and how long to wait), and where a record
goes when the answer is "not now, and not with this code".

## Options
1. **Retry in place, forever** — never lose a record, and turn one bad record into a
   permanently stalled partition: the only lever left is to redeploy different code.
   Rejected: it converts a data problem into an outage.
2. **Skip the record** (log it, commit the offset) — the cheapest way to keep the
   partition moving, and the payload is gone with no copy anywhere. Rejected: silent data
   loss, the exact failure the inbox exists to prevent.
3. **Bounded retries, then dead letter queue, then commit** — chosen. The partition moves
   on, the record is preserved with its diagnosis and its original bytes, and the decision
   of what to do with it moves to a human (or a replay tool) who has the payload in hand.
4. **A retry topic** (park the record on `<topic>-retry`, re-consume it after a delay) —
   the standard version of this at volume, and what Spring Kafka's `@RetryableTopic`
   implements. Not chosen *now*: it needs a second consumer and a delay mechanism, which
   multiplies what has to be tested, while the budget here (a few attempts inside one
   second) fits in-process. It is the documented next step once the budget has to survive
   a consumer restart.
5. **Deduplicate by not producing bad events** — the producer's job, and it does not
   cover the effect side (a conflict is not a wire problem). Not a substitute.

## Decision
`match-orchestrator` retries a record a bounded number of times, then publishes it to the
dead letter topic of its source (`EventTopics.deadLetterOf(topic)` → `<topic>-dlq`) and
commits the offset.

- **3 attempts** (env `CONSUMER_MAX_ATTEMPTS`), **250 ms first backoff, doubling** (env
  `CONSUMER_FIRST_BACKOFF_MS`) — about 750 ms of retrying before giving up.
- The retry covers **decoding and applying together**, one loop around both stages.
- The dead letter carries the **original payload verbatim** — the raw bytes, through
  `ByteArraySerializer`. It is not re-serialized: a payload that does not decode has no
  schema to be serialized against, and re-encoding the others would be a second contract
  to maintain.
- The offset is committed **only after the dead letter is durable** (the publish is waited
  on). If the publish fails, nothing is committed and the record is delivered again.
- The consumer reads **values as bytes** and decodes per record, so a decode failure is
  attributable to the record it came from.

## How it works

### One loop around both stages
`ProcessConfirmedMatch.handle(record)` tries, per attempt: `decoder.decode(record)` and
then `inbox.applyOnce(...)`. Which stage failed is read from how far the attempt got, not
guessed from the exception type:

| Failure | `stage` | Why |
| --- | --- | --- |
| the payload never became an event | `DECODE` | producer/contract/registry side |
| the event was understood, the effect was not | `APPLY` | this consumer's data or database |

That distinction is the difference between "the registry is down, expect a burst" and
"one producer is writing garbage" for whoever drains the queue.

### The outcomes are three, and all three are normal
`ProcessingOutcome`: `APPLIED` (first delivery), `DUPLICATE` (ADR 0038 — a success),
`DEAD_LETTERED` (this ADR). A record that reaches the DLQ is not an exception to the
consumer: it is an outcome, logged as such, with the offset committed like any other.

### What the dead letter carries
Value: the source bytes, byte for byte. Key: the source key (the match id, so a
human-readable partition is preserved). Headers:

| Header | Example | Why |
| --- | --- | --- |
| `dlq.stage` | `DECODE` | which side owns the problem |
| `dlq.error-class` | `java.util.concurrent.CompletionException` | the exception type, verbatim |
| `dlq.error-message` | `... duplicate key value violates unique constraint ...` | truncated at 1 000 chars: headers are read by everyone on the topic |
| `dlq.attempts` | `3` | the budget that was spent |
| `dlq.consumer-group` | `match-orchestrator` | which consumer gave up |
| `dlq.original-topic`, `dlq.original-partition`, `dlq.original-offset` | `matchmaking.events.match-confirmed`, `0`, `9` | where it came from, for forensics and replay |
| `dlq.event-id` | the uuid | present when the payload decoded; absent when nothing was understood |
| `dlq.failed-at` | ISO-8601 | when, from the consumer's clock |

### Why decode failures are retried too
A payload that does not decode may be broken beyond repair, or the Schema Registry may be
briefly unreachable — and the second case deserves a retry. Giving decode failures their
own "straight to the DLQ" path would save 750 ms and would throw away valid events during
a registry blip. One loop for both; the `stage` header keeps them distinguishable
afterwards.

### The DLQ topic is created by the operator, not by the service
A service that invents topics also invents partition counts and retention. The topic has
to exist before the first failure, because an undeliverable dead letter means the offset
stays uncommitted — the stall this ADR removes coming back in a worse disguise. So the
topic is a deployment precondition (documented in the service README, created by
`requests/kafka/verify_dlq.sh` for the local stack) and, deliberately, **not** registered
in the Schema Registry: its payload is not required to be a valid message of any subject.

## Measured (dev machine, single broker/partition, Postgres 16 in Compose)
| What | Result |
| --- | --- |
| Attempts before dead-lettering | 3, ~750 ms of waiting (250 + 500) |
| Phase 1 — payload that never decodes | `stage=DECODE`, `attempts=3`, payload on the topic **verbatim** (22 bytes), committed past it (`offset 12`, lag 0), nothing projected, inbox unchanged |
| Phase 2 — effect that cannot be applied | `stage=APPLY`, `dlq.event-id` recorded, payload intact (135 bytes), projection still 10 rows (no duplicate effect), committed, lag 0 |
| The dead-lettered bytes are replayable | the DLQ record still decodes through the Schema Registry into the same event (`eventId` matched) |
| Orchestrator unit tests | 22, 0 failures (7 policy, 3 retry policy, 3 inbox against real Postgres, 9 translator) |
| End-to-end scenario | `requests/kafka/verify_dlq.sh`: 38 checks, 0 failures |
| Mutation: commit before publishing the dead letter | 3 tests red, including `commitsNothingWhenTheDeadLetterCannotBePublished` |

## How to test
```bash
# the policy, without a broker: retries, the DLQ contents, and the commit rule
mvn -B -f services/match-orchestrator/pom.xml test -Dtest=ProcessConfirmedMatchTest,RetryPolicyTest

# the two failure shapes end to end (needs the stack, the orchestrator running, and
# something already projected: run the E2E first)
python3 requests/e2e/run_e2e.py
bash requests/kafka/verify_dlq.sh
```
The script creates the dead letter topic if missing, produces a poisoned record, and then
rewinds the group onto an event whose projection row already exists. It asserts the
committed offset passed both, the headers of each dead letter, and that the applied side
did not change.

## Trade-offs
- **A dead letter is not a fix.** The record is out of the way and still wrong until
  someone acts on it; the queue needs an owner and an alert on its depth (W15).
- **An outage can fill the queue.** If the registry is unreachable, every record fails to
  decode, and after three attempts each, valid events land in the DLQ. The `stage` header
  makes that visible and the alternative (blocking the partition) is worse, but the honest
  answer is a health signal, not this ADR.
- **The retry budget does not survive a restart.** Attempts are per session; a crash
  mid-retry loses the remaining budget (the record is redelivered and starts over, so it
  is a delay, never a loss).
- **Backoff blocks the partition.** 750 ms per failing record is bounded and observable;
  at volume the retry belongs on a retry topic (option 4).
- **No automatic replay.** Nothing consumes the DLQ. The payload is recoverable — that is
  what the measurement shows — but recovering it is a manual action today.

## Consequences
- [x] A record that cannot be handled no longer blocks the partition (measured: dead
      lettered, `lag 0`, offset committed past it)
- [x] No offset is committed without a durable outcome: neither the effect nor a dead
      letter, proven by a failing mutation
- [x] The failure kind is recorded (`DECODE` vs `APPLY`) instead of being inferred later
- [ ] Alerting on dead letter rate and depth (W15)
- [ ] A retry topic, when the budget has to survive a consumer restart
- [ ] A replay path that reads the DLQ and re-applies selected records (not scheduled; the
      payload decodes, which is the prerequisite)

## Evidence
- `services/match-orchestrator/src/test/java/.../application/ProcessConfirmedMatchTest.java`
  — 7 tests, including the commit rules and the failure stages.
- `requests/kafka/verify_dlq.sh` — 38 checks against the real stack, both failure shapes.
- Measured dead letter (`kafka-console-consumer` with headers):
  `dlq.stage:DECODE,dlq.error-class:java.lang.IllegalArgumentException,dlq.error-message:payload does not decode as MatchConfirmed: Error deserializing Protobuf message for id -1,dlq.attempts:3,dlq.consumer-group:match-orchestrator,dlq.original-topic:matchmaking.events.match-confirmed,dlq.original-partition:0,dlq.original-offset:12`
  and, for the applied side,
  `dlq.stage:APPLY,...,dlq.error-class:java.util.concurrent.CompletionException,dlq.error-message:org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint "match_allocations_pkey"`.
