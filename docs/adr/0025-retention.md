# ADR-0025 — Retention: what may be forgotten, what must not (W15)

## Status
Accepted (2026-10-17)

## Context
ADR 0020 gave the queue a number that can go up (`matchmaking_queue_depth`,
`matchmaking_queue_oldest_wait_seconds`, the outbox backlog). A number that goes up asks a second
question — *and then what?* — and answering it forced the question nobody asks until a disk is
full: **how long does each thing live, and which store is allowed to forget?**

What exists today, and where:

| Store | What it holds | Lifetime today |
| --- | --- | --- |
| Redis | `queue:{mode}:{region}` (ZSET by `queuedAt`) | **none** — shrinks only when a member is cancelled or paired |
| Redis | `ticket:{id}` | TTL 900 s (15 min) |
| Redis | `party:{id}` | TTL 7 days |
| Redis | `match-pending:{id}`, `match-confirmed:{id}` | the 30 s confirmation window; the confirmed set is deleted on confirmation |
| Redis | `idem:{key}` (idempotency of the enqueue) | TTL, same family as the ticket |
| Postgres | `matches`, `match_players` | forever (no deletion path exists) |
| Postgres | `outbox_events` (ADR 0018) | `PENDING`/`PARKED` until published or fixed; `PUBLISHED` kept |
| Postgres | orchestrator `match_allocations`, `processed_events` (ADR 0038) | forever |
| Kafka | `matchmaking.events.match-confirmed`, `…-dlq` | broker default: **7 days, no size cap** (the topic has no dynamic retention config) |

Measured on the local stack (2026-10-17, after the W14/W15 work):

| Table / log | Rows | Size |
| --- | --- | --- |
| `matches` | 32 | 40 kB |
| `match_players` | 64 | 72 kB |
| `outbox_events` | 6 (all `PUBLISHED`) | 96 kB |
| `processed_events` (orchestrator) | 20 | 88 kB |
| `match_allocations` (orchestrator) | 20 | 56 kB |
| `matchmaking.events.match-confirmed` | — | 6,180 bytes, 1 partition |

So the reason for this ADR is **not** storage pressure — everything here is kilobytes. The reason
is that "no policy" is itself a policy with an incident attached: Redis runs
`maxmemory-policy noeviction`, so the day it fills, writes fail. And the audit while writing this
found a rule already broken (the finding at the end).

## Options
1. **Keep everything forever, decide when it hurts.** Cheapest today; guarantees the decision is
   made under pressure, by whoever is on call.
2. **TTL everything** — including `matches`, with a history window like "90 days". Simple and
   uniform, but it deletes the only durable record of a match that the player and the analytics
   both read, to save kilobytes.
3. **A policy per store**, by the role the data plays: hot state expires, facts are kept, and the
   event log may forget what the facts already prove.

## Decision
**Option 3.** Three classes, and a rule for each:

1. **Hot state (Redis) expires.** Every key either carries a TTL or is explicitly deleted — that is
   a rule, not a coincidence: ticket 900 s, party 7 days, pending match 30 s, confirmed set deleted
   on confirmation. A new key that has neither is a bug, and the TTLs are asserted where they are
   set (`RedisTicketRepository`, `RedisPartyStore`).
2. **Facts (Postgres `matches`, `match_players`) are kept.** They are what `GET
   /v1/players/{id}/matches` reads and the audit of what the matchmaker decided. Deleting them is a
   *product* decision (how long does a player see their history?), not a storage one, and it will
   arrive as `DELETE … WHERE created_at < now() - interval 'N days'` behind monthly partitions.
   Criterion to revisit: the history table passing ~1M rows, or a stated product retention window.
3. **Events (Kafka) may be forgotten; the facts may not.** The topic keeps the broker default (7
   days): losing a record there is acceptable because the fact is durable in `matches` and its
   envelope is durable in `outbox_events` (ADR 0018). Revisit when a consumer needs a history
   longer than the retention — then the topic gets `retention.ms`/`retention.bytes` set explicitly,
   and a full replay is taken from the outbox, not from the log.
4. **The outbox's `PUBLISHED` rows are kept for audit**, with the purge documented and deliberately
   *not* scheduled (ADR 0018):
   `DELETE FROM outbox_events WHERE status = 'PUBLISHED' AND published_at < now() - interval '30 days';`
   It is registered in `MelhoriasFutura.md` with the criterion: it gets an owner and a metric, not a
   cron someone forgets.
5. **The reprocessed-effect record (orchestrator) is kept.** `processed_events` and
   `match_allocations` *are* the idempotency proof (ADR 0038); deleting them re-opens the double
   effect on the next replay. Retention here is a correctness property, so it does not get a TTL.
6. **Redis stays `noeviction`.** With TTL'd tickets and non-TTL'd queues no eviction policy is
   safe: `allkeys-lru` would silently drop a queue (players disappearing with no error anywhere),
   while `noeviction` turns memory pressure into a failed write — a loud failure, and now a visible
   one (`jvm_memory_used_bytes`, `vertx_redis_processing_pending`). The queue's memory is bounded by
   the ticket TTL *once the finding below is fixed*.

### Finding: a queue member outlives its ticket and stalls the queue
Redis expires `ticket:{id}` after 900 s, but a ZSET **member** cannot have a TTL: the member stays
in `queue:{mode}:{region}` with its score. The pairing reads the head of the queue, loads its
ticket, does not find it, and pairs nobody — so a ghost at the head blocks every compatible player
in that mode/region behind it.

Measured on the local stack (2026-10-17): ticket `DEL`eted (member kept), then a second player with
the same deterministic skill enqueued. Six seconds later there was still no pending match, with
`matchmaking_queue_depth{mode="1v1",region="NA"} = 2` and
`matchmaking_queue_oldest_wait_seconds = 9` — a queue that is deep, old and completely stuck. This
stall existed since tickets got a TTL; it became *visible* only when W15 derived a number from the
queue itself. That is the value of the sprint, not a coincidence of it.

Recorded as a known deviation (AGENTS.md, `11 - Progresso.md`): the fix is to remove, in the
engine's sweep, the queue members whose ticket no longer exists — with a test that fails while the
stall exists (two compatible players, one ghosted ticket, the queue must still pair). It is the
first task of W16.

## How it works
The rules above are enforced by the shape of the code, not by a document:

- **Hot state**: `RedisTicketRepository` passes `EX 900` on every write of a ticket (including the
  state transitions, so an active ticket stays alive) and `ZREM`s the member when a ticket is
  cancelled; `RedisPartyStore` passes `EX 604800`. The queue's only automatic cleanup is the fix
  above.
- **Facts**: there is deliberately **no** delete path for `matches`/`match_players` in
  `MatchRepository` — nothing can forget them by accident.
- **Events**: `OutboxRelay` only moves rows between statuses; `PUBLISHED` is terminal and the row
  stays. Kafka's retention is the broker's business, and the fact that the topic may forget is why
  the outbox exists in the first place.
- **The number that made this visible**: the queue depth/oldest wait come from Redis every 2 s
  (ADR 0020), so a queue whose head never moves is now a flat line at a growing value instead of a
  silent absence.

## How to test
By hand, on the local stack:

```bash
# the TTLs that exist (each one is set explicitly in code)
docker exec game-matchmaking-backend-redis-1 redis-cli ttl ticket:<ticketId>     # <= 900
docker exec game-matchmaking-backend-redis-1 redis-cli ttl party:<partyId>       # <= 604800

# the sizes this decision is justified by
docker exec game-matchmaking-backend-postgres-1 psql -U matchmaking -d matchmaking -c "
  SELECT relname, n_live_tup AS rows, pg_size_pretty(pg_total_relation_size(relid)) AS size
  FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC;"

# the eviction policy Redis actually runs with (no TTL'd-key eviction: see decision 6)
docker exec game-matchmaking-backend-redis-1 redis-cli config get maxmemory-policy

# the topic's effective retention (empty output = the broker default, 7 days)
docker exec game-matchmaking-backend-kafka-1 kafka-configs --bootstrap-server localhost:29092 \
  --entity-type topics --entity-name matchmaking.events.match-confirmed --describe

# the purge that is documented but not scheduled
docker exec game-matchmaking-backend-postgres-1 psql -U matchmaking -d matchmaking -c \
  "DELETE FROM outbox_events WHERE status = 'PUBLISHED' AND published_at < now() - interval '30 days';"
```

And the scenario that reads the stores these rules cover:
`bash requests/observability/check_metrics.sh` (35 checks — it cross-checks the outbox gauges
against `SELECT count(*)` and the publish lag against the table).

## Trade-offs
- **Keeping facts forever costs (almost) nothing now and grows linearly.** Accepted: 40 kB for
  matches; the cost of deleting a player's history by accident is higher than the disk, and the
  moment it stops being true is measurable (the criterion in decision 2).
- **The Kafka topic forgetting before a consumer is done is a real risk.** Accepted for this stack
  because there is exactly one consumer family and the facts are durable next to the outbox;
  revisit with a second consumer or a legal retention need.
- **`noeviction` means an availability incident when Redis fills.** Accepted over silent data loss;
  the mitigation is the KPI, not the policy.
- **The fix for the ghost member is not in this ADR.** It is recorded, with a measurement, and
  scheduled; writing the policy first (this document) and the fix next is the project's rule 1.

## Consequences
- Every future store/key answers "which class is this?" before it is designed, and a key created
  without a TTL or a delete path is reviewable as a bug.
- The `outbox_events` table grows with confirmed matches (`PUBLISHED` forever) until the purge is
  scheduled; that growth is now *visible* (`matchmaking_outbox_backlog{status="PUBLISHED"}`) rather
  than implicit.
- The topic's retention is a deployment fact, not an application one: the app must keep working
  after the log forgets, and it does — the fact is in Postgres.
- The stall found here is documented in AGENTS.md and in `11 - Progresso.md` as a known deviation,
  with the reproduction (DEL the ticket, keep the member) and the expected fix; the W16 sprint
  starts from it.
