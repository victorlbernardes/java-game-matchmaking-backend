# ADR-0027 — Horizontal scalability: what is stateless, what the partition key unlocks, what is still single (W20)

## Status
Accepted (2026-10-20)

## Context

By W19 the API is horizontally safe: any instance accepts any player's request, a notification is
delivered by whichever instance holds the socket, presence names the holder, and the pending
confirmation is re-presented by the heartbeat (ADR 0026). That removed exactly one reason to run a
single API process. It did not answer the next question, which is what this ADR is about: **which
part of the system can grow with replicas, and which part is a single thing by construction?**

The inventory of the running system, before W20:

| Component | Growable? | What stopped it |
| --- | --- | --- |
| `matchmaking-api` | **yes** (W19) | nothing: state is Redis, notifications are routed, presence is shared |
| `matchmaking-engine` (pairing) | no | one process owns the whole pairing loop; two would pair the same head twice, and only the atomic `pair.lua` stops it — correctness is fine, but the loop itself is one timer |
| `matchmaking-engine` (outbox relay) | no | the claim reads rows in insertion order; two relays could publish two rows of the same key out of order (W14 note in `11 - Progresso`, "Ordem por chave vale para um relay") |
| `match-orchestrator` | **yes** | it is a Kafka consumer group with an idempotent inbox (W13) |
| Redis (hot state) | no | one instance; the queue for a (mode, region) was **one key**, and one key is one shard |
| PostgreSQL | no | single writer, one schema |

The interesting row is the last one. A matchmaking queue is the part of the system that receives
every arrival, so its key is the hottest key in the deployment, and in Redis Cluster a key is a
slot and a slot is a shard. Adding Redis nodes to a queue stored in one key adds nothing. The W16
saturation run is the number that describes the shape: 4000 arrivals, `p99` wait **15.6 s** against
a 10 s objective, and the queue reaching **3146** in flight — the arrival rate was not the problem
the system was good at surviving; the concentration was.

The second row is the pairing loop. It is Redis-only and therefore *cheap to replicate* in
principle, but a second engine would read the same queue and race the first on every pairing — the
roster is named and the script is atomic (W16), so the loser simply gets `nil`, which is safe and
wasteful. The reason to replicate the engine is not correctness, it is that one event loop has a
ceiling: measured on this machine with the W16 load harness, 400 players at a single-key queue
drained at **45 players/s** (22.5 matches/s) after a peak depth of 320; the pairing loop, not Redis,
was the ceiling.

## Options

**A. Vertical only.** A bigger machine buys a bigger event loop and a bigger Redis, and the ceiling
moves without changing shape. It is the correct answer for a system whose load is known and small;
it answers nothing about what happens at 10×, which is the question the project exists to ask.

**B. More API replicas.** Already possible (W19) and necessary, but the API is not where the
ceiling is: it does one Redis write per arrival and waits. Replicas increase admissions, not
pairings, and push the concentration problem faster into the queue.

**C. Stateless services + a shardable hot state (chosen).** Keep every process stateless — the facts
live in Redis and Postgres — and make the hot state *addressable in pieces*: partition the queue key
by a consistent hash of the enqueue key (ADR 0028), publish the per-partition depth, and make the
pairing read every partition and merge the heads. The queue then has `P` keys, each one small
enough to live on its own shard/worker, while the matching semantics stay global.

**D. Redis Cluster now, with hash tags to keep the queue on one slot.** Hash tags
(`{1v1:NA}`) force every partition onto the same slot — the opposite of what is needed: it keeps
the hotspot and adds the cluster's operational weight. Adopting the cluster is a deployment
decision that the partition key makes *possible*; doing it in the same sprint as the key would
mean measuring neither.

**E. Partition by region only (the roadmap's first sketch).** Regions already are a key dimension
(`queue:{mode}:{region}`), and they are not the hotspot: the load runs are single-region and
single-mode, and that one queue is the hot one. Region partitioning was already there before the
sprint; it is not the answer to a hotspot inside a region.

## Decision

**W20 delivers the unit that makes the hot state growable, and stops there.**

1. **The queue key is partitioned** (`queue:{mode}:{region}:{p}`, `QUEUE_PARTITIONS`, default 1 —
   ADR 0028) by a consistent hash of the enqueue key.
2. **The pairing stays global.** Each tick reads the head of every partition and merges by the wait
   score, so a partition is storage, never a matching boundary. This is what keeps the assignment
   unit for an engine worker at **(mode, region)** rather than at a partition: assigning partition 3
   to worker B would need worker A to stop pairing across it, which is exactly the starvation the
   merge avoids.
3. **The distribution is observable**: `matchmaking_queue_partition_depth{mode,region,partition}`
   and `matchmaking_queue_hotspot{mode,region}` (max/mean), next to the existing total
   `queue_depth`, which keeps its meaning (the sum).
4. **The outbox relay remains single**, and the reason is recorded: publishing in insertion order is
   what makes "rows of one key in order" true; a second relay needs the claim partitioned by
   `partition_key` (the outbox already stores it), which is the next sprint's work, not this one.
5. **The engine's replica story is written down as a precondition**, not implemented: N engine
   workers, each owning a disjoint set of (mode, region) pairs, with the distributed `WriteGate`
   (item 9 of `MelhoriasFutura.md`) — the local `WriteGate` counts per process, so `N` engines
   would multiply the database's in-flight writes by `N`.

The API's story is unchanged since W19: N replicas behind a plain load balancer.

## How it works (files)

- `common/common-redis/.../QueuePartitions.java` — jump consistent hash (ADR 0028), `QUEUE_PARTITIONS`.
- `common/common-redis/.../RedisKeys.java` — `queueKey(mode, region, partition, partitions)` and
  `queueKeys`, with the single-partition deployment keeping the historical key name.
- `common/common-redis/.../RedisTicketRepository.java` — enqueue key = `partyId` or `playerId`,
  per-partition admission cap `floor(C/P)`, `queueDepth` as the sum.
- `services/matchmaking-engine/.../MatchmakingEngineApplication.java` — `pairBatch` per
  (mode, region): read the heads of all partitions with `WITHSCORES`, merge by score, reap ghosts
  per partition, pair the roster with every partition as `KEYS`.
- `services/matchmaking-engine/.../infrastructure/QueueMetricsPoller.java` + `EngineKpis` — the
  per-partition gauges and the hotspot.
- `services/matchmaking-api/.../MatchmakingApiApplication.java` — the same `QUEUE_PARTITIONS` on
  the write side.

## Measured numbers (local stack, 2026-10-20)

Correctness under load with the W16/W19 harness (`requests/mm.sh load`, 400 players,
concurrency 64, 1v1/NA, every client confirmation driven over the real channels):

| Metric | `QUEUE_PARTITIONS=1` | `QUEUE_PARTITIONS=8` |
| --- | --- | --- |
| arrivals admitted | 400/400 | 400/400 |
| matches formed | 200 | 200 |
| matches confirmed | 200 | 200 |
| dissolved at the deadline | 0 | 0 |
| duplicate `MATCH_CONFIRMED` events | 0 | 0 |
| enqueue rate (same machine, same day) | 119.9 req/s | 123.1 req/s |
| enqueue p50 | 468 ms | 523 ms |
| pairing drain after the arrivals | 320 players in 7.1 s (45 players/s) | already drained during the arrivals (peak depth 0) |

The enqueue rate is the cost question and the answer is "no measurable cost" (123 vs 120 req/s is
inside the noise of a laptop running five JVMs). The drain column differs by 3×, and **it is not
claimed as a win of partitioning**: the two runs were not a controlled benchmark (JIT state and a
shared machine), the drain sampler missed the queue in the P=8 run because it emptied during the
enqueue phase, and a single-node Redis executes commands one at a time regardless of how many keys
there are. What the run does prove is the property that mattered: with eight partitions, **all 200
matches still formed** — a partition is not a matching boundary.

The distribution and the hotspot, which are this sprint's deliverable, are in ADR 0028 and in
`requests/partitioning/verify_partitioning.sh` (27 checks, 27 passed): one key holds 100 % of 2000
arrivals; eight keys hold 13.65 % at the deepest, hotspot 1.092, sum exactly 2000.

## How to test

- `bash requests/partitioning/verify_partitioning.sh` — before/after distribution, the scrape
  agreeing with Redis, a match across two partitions (0.2 s), and the cap not multiplied by `P`.
- `mvn -o -B -pl common/common-redis -Dtest=QueuePartitionsTest test` — stability, uniformity, remap.
- `mvn -o -B -pl services/matchmaking-engine -Dtest=PartitionedQueueTest test` — cross-partition
  pairing, party locality, depth sum, cap.
- `requests/mm.sh load --players 400 --concurrency 64 --label w20-p8` with both
  services started with `QUEUE_PARTITIONS=8` — the load-regression number above.

## Trade-offs

- **`QUEUE_PARTITIONS` is deploy-time configuration and both services must agree.** An API writing
  partition 3 while the engine reads one key loses every arrival silently — the same class of
  mistake as `DB_MAX_IN_FLIGHT` on only one engine. It is checked by the scenario, not by the type
  system; the default of 1 makes the mistake impossible to make by omission.
- **Changing `P` is a queue drain.** The keys change shape (`queue:1v1:NA` vs `queue:1v1:NA:0`), so
  a rollout with a different `P` must wait for the queue to be empty (seconds in practice, the
  admission and the pairing both continue to work; the members written under the old shape are the
  ones nobody reads).
- **A roster may span partitions, and the script pays for it.** `pair.lua` looks each member up in
  every partition (`ZSCORE` per key) instead of one `ZREM` on a known key: `P` extra reads per
  member, bounded and inside one atomic script.
- **The hotspot ratio is a per-queue fact and only means something with `P > 1`.** With `P = 1`
  it is 1.0 by construction; the sprint's before/after compares the single-key layout
  (100 % on one key, ratio 8 against an 8-way target) with the partitioned one.
- **The engine is still one process.** This ADR makes the state shardable and records the next
  step; it does not run two engines, because the write gate is per process (item 9) and because
  replicating the pairing without a distributed gate would move the bottleneck to Postgres.

## Consequences

- The queue has a **partition key**: `queue:{mode}:{region}:{p}`, `p` from the enqueue key — the
  unit a Redis Cluster slot or a future engine worker can own.
- The per-partition depth and the hotspot are **numbers on the scrape**, so "the queue is hot" is
  now an observation instead of an inference.
- A deployment can run N APIs today, and N engines once the write gate is distributed.
- The next bottlenecks are named: the outbox relay (per-key ordering, single process), the single
  Postgres writer, and the local `WriteGate`.
