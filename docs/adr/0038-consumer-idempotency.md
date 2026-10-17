# ADR-0038 — Idempotent consumer: an inbox, so that "at least once" is enough (W13)

## Status
Accepted (2026-10-17)

## Context
Kafka delivers **at least once**. The producer already publishes with
`enable.idempotence=true` (ADR 0016), but that removes duplicates *inside the log* — it
does nothing about duplicates *in the delivery*. A rebalance after a slow poll, a
consumer restarting after a crash, or an operator rewinding the group's offsets (the
normal reaction to a projection bug) all hand the same record to the consumer again.
It is not a rare path: when this work started, the topic already held 8 records from
the W12 validation and the first honest replay re-delivered all 8.

`match-orchestrator` is the first consumer (W13): every confirmed match becomes a row
that the allocator (W22) will fill with a game server. Applying that twice is not a
duplicate row, it is a match that starts twice — and the same reasoning applies to any
consumer added later (skill recalculation, telemetry).

Three questions were open: where the deduplication key lives, how the "already applied"
fact is made as durable as the effect itself, and when the offset may be committed.

## Options
1. **Trust the delivery** — consume with `enable.auto.commit=true` and ignore
   duplicates. Zero code, and it works exactly until the first rebalance or replay, at
   which point the data is quietly wrong. Rejected: the failure is silent and lands in
   production, which is the whole reason this sprint exists.
2. **Deduplicate in memory** (a set of seen event ids in the consumer) — cheap, and
   wrong twice: it dies with the process — precisely when a redelivery is most likely —
   and it grows without bound because offsets are not compacted by a `Set`. Rejected.
3. **Make each effect idempotent by construction** (`INSERT ... ON CONFLICT DO NOTHING`
   on the projection) — the classic shortcut. It works for *this* projection, but it is
   a property of the effect and not of the consumer, so every future effect needs the
   same trick; and it hides a real conflict (two different events for one match, which
   should hurt) behind the same statement that absorbs a redelivery. Rejected as the
   general mechanism — and deliberately *not* used on the projection, so the tests
   cannot pass for the wrong reason.
4. **Inbox**: record the processed event and apply its effect in one transaction. The
   "have I seen this?" question and the "do it" action are the same write, so they
   cannot disagree. Chosen.
5. **Kafka exactly-once (transactions + `read_committed`)** — gives exactly-once for
   operations that live *inside* Kafka. The effect here is a database write, outside the
   transaction, so EOS would not cover it; it would add a transactional producer and
   `sendOffsetsToTransaction` for a guarantee we still have to build on the database
   side. Not sufficient, and not cheaper.

## Decision
- Deduplicate on `EventMetadata.event_id` — the uuid the publisher mints per delivery —
  through an **inbox table** (`processed_events`, primary key = `event_id`) inserted in
  the **same transaction** as the effect.
- Commit the offset **by hand, after** the effect is durable (`enable.auto.commit=false`).
- Keep a plain `INSERT` for the projection: a duplicate match id is a data conflict and
  must fail, not be absorbed.

## How it works

### The unit of work
One transaction, two statements:

```sql
INSERT INTO processed_events (event_id, consumer_group, topic, partition_id, record_offset, processed_at)
VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING RETURNING event_id;
-- only when the insert above reported a row:
INSERT INTO match_allocations (match_id, game_mode, region, player_count, status, confirmed_at) VALUES (?, ?, ?, ?, ?, ?);
```

`ON CONFLICT DO NOTHING` is what makes the check atomic: two consumers racing on the
same event cannot both read "not processed" — the loser finds the winner's row (even
uncommitted) and waits, then does nothing. This is a unique index, not application
logic, which is why the property survives a rebalance, a restart or a hand-rolled
offset reset.

The group is part of the record: the same event may be processed once by
`match-orchestrator` and once by a future `skill-service` consumer, each with its own
inbox and its own offsets — a shared dedupe table without the group would silently
starve the second consumer.

### Delivery semantics: the commit comes after the effect
`ProcessConfirmedMatch` applies the effect and only then commits
`offset + 1` (Kafka's way of saying "done with this record"). Committing first would
make a crash in between lose the event *silently*: from the broker's point of view it
was already handled, so it is never redelivered. The order is the guarantee, and it is
tested without a broker (a fake inbox that fails must leave the committer untouched).

### A duplicate is a success, not an error
The consumer distinguishes two outcomes: `APPLIED` (first delivery) and `DUPLICATE`
(this group already applied it). Both commit their offset — a duplicate that stayed
uncommitted would be redelivered forever — and both are logged, because "the replay did
nothing" is the result an operator needs to see, not to infer from silence.

### The projection fails loudly on a conflict
A second event id for a match that already has an allocation violates the primary key:
the transaction rolls back (including its inbox row), the failure is logged and the
offset is not committed. Two events for one match is a producer bug or a business
duplicate; neither is a redelivery, and treating it like one would be a lie the next
reader cannot detect.

### Vert.x, threads and the one measured trap
The consumer owns a platform thread: `KafkaConsumer` is blocking and not thread-safe,
so pretending it is reactive would only move the blocking somewhere less visible. The
database call it waits on is the Vert.x JDBC pool, so the JVM does not need a second
connection story.

Trap found while writing the tests, and worth knowing before writing the same statement
elsewhere: with the Vert.x JDBC client, `RowSet.size()` is **-1** for an `INSERT ...
RETURNING`; the number of rows lives in `rowCount()` (1 for the insert, 0 for the
`ON CONFLICT DO NOTHING` no-op). Written with `size() == 0`, every first delivery would
have looked like a duplicate — the tests caught it as a primary-key violation on the
*second* delivery.

### Known boundary (closed by W13 task 2)
A record whose effect keeps failing is left uncommitted, so it is redelivered after a
restart or rebalance, but it is **not** retried inside the running session and it blocks
its partition. Bounded retries and the dead letter queue are the next task of this
sprint (ADR 0019); the loop is written so that is a change of policy, not a rewrite.

## Measured (dev machine, single broker/partition, Postgres 16 in Compose)

| What | Number |
| --- | --- |
| Unit + integration tests (`match-orchestrator`) | 15 tests, 0 failures |
| Replay of the whole topic (`verify_idempotent_consumer.sh`) | 8 records re-read → **8 `DUPLICATE`, 0 `APPLIED`** (the topic held 8 that day; re-run with 10, the outcome is identical — all duplicates) |
| Projection and inbox after the replay | `match_allocations` 8 → **8**, `processed_events` 8 → **8** |
| Catch-up after subscribing (rebalance + 8 records, per-record commit) | **9 s** |
| Projection lag on the live path (`projected_at - confirmed_at`) | **42 ms** warm, 1.6 s for the first match after the consumer idled |
| Backfill lag (events produced before the consumer existed) | 10 min to 68 min — a property of backfill, not of the consumer |
| Test that fails when the property is violated | inbox without `ON CONFLICT` → 2 tests fail with `processed_events_pkey` |
| E2E | **52 checks, 0 failed** (47 before; +5 in S11) |

## How to test

```bash
docker compose up -d                       # kafka + postgres + registry
mvn -B install -DskipTests

# the property, against the database that enforces it
mvn -B -f services/match-orchestrator/pom.xml test

# start the services (order matters: auth must be listening before the API fetches its JWKS)
mvn -pl services/auth-service exec:java -Dexec.mainClass=com.victor.matchmaking.auth.AuthServiceApplication &
mvn -pl services/matchmaking-api exec:java -Dexec.mainClass=com.victor.matchmaking.api.MatchmakingApiApplication &
mvn -pl services/matchmaking-engine exec:java -Dexec.mainClass=com.victor.matchmaking.engine.MatchmakingEngineApplication &
mvn -pl services/match-orchestrator exec:java -Dexec.mainClass=com.victor.matchmaking.orchestrator.MatchOrchestratorApplication &

python3 requests/e2e/run_e2e.py             # S11: one projection row per confirmed match
bash requests/kafka/verify_idempotent_consumer.sh   # rewinds the group and replays everything
```

## Trade-offs
- **The inbox grows forever.** One row per event per group. Retention is a real
  question, and it is deliberately not answered here: the table is also the audit of
  what a consumer has seen, so pruning it is a policy decision (ADR 0025 territory),
  not a cleanup.
- **Per-record `commitSync`** costs a round trip per record and caps throughput well
  below what the broker can deliver. Correct first; batching is a change of one method.
- **The commit is not atomic with the database.** A crash after the effect and before
  the commit means the record is delivered again — and applied once, because of the
  inbox. That is the intended reading of "at least once": the delivery repeats, the
  effect does not.
- **A second consumer group processes the whole topic again.** Correct by design, but
  it means the inbox is per group and the storage cost is per group as well.

## Consequences
- Rewinding the offsets — after a bug, or to rebuild a projection — is a safe
  operation, verified end to end and repeatable by hand.
- `match-orchestrator` stopped being a stub: it owns a consumer group, a projection and
  a table, and W22 extends the same row with the server it allocates.
- The `build` CI job now runs Postgres next to Redis: the idempotency property is
  enforced by a unique index, so a fake database would assert the fake.
- The failure path is now the visible gap. A record that always fails blocks its
  partition and is not retried in-session; ADR 0019 (W13 task 2) bounds the retries and
  gives the record a destination.
