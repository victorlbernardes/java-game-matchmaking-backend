# ADR-0028 — Consistent hashing and hotspots: how a queue is partitioned (W20)

## Status
Accepted (2026-10-20)

## Context

ADR 0027 decided that the queue should be addressable in pieces. This ADR is the *key*: which hash,
over which key, with how many pieces, and what the distribution actually looks like when measured
instead of asserted.

The constraint that shapes everything: **pairing must not become a partition boundary.** A player
who waits because a compatible partner landed in another key is a regression disguised as a
scaling improvement — the queue would be faster and wrong. So the partition key is a storage and
write-distribution decision only; the matching keeps reading the whole logical queue.

The second constraint: the cap. `QUEUE_MAX_DEPTH` is the admission boundary that absorbed the W17
saturation run (`matchmaking.tickets.rejected{reason="queue_full"}`); a partitioned queue that
admits `P × C` would quietly remove it.

Third: `P` is a deployment number that will change. The keys must be stable for a given `P`, and
changing `P` must move as few keys as possible — otherwise growing the queue reshuffles the
majority of the players, and every resharding is a drain with a cost proportional to the queue.

## Options

**A. Modulo of a hash (`hash(key) % P`).** Uniform for a fixed `P`, one multiplication, no state.
Fails the third constraint: going from 8 to 9 partitions moves **88.88 %** of the keys (measured
below). It is the right algorithm only when `P` never changes.

**B. A hash ring with virtual nodes.** Consistent: adding a node moves ~1/N. Costs a sorted ring,
a lookup, and the decision of how many virtual nodes per partition to stay uniform — state and
tuning for a benefit that a stateless algorithm also provides.

**C. Jump consistent hash (chosen).** Lamping & Veach (2014): O(log P) time, O(1) memory, no state
to keep or synchronise, and growing from `P` to `P+1` moves ~`1/(P+1)` of the keys. Two functions,
no data structure, identical in every process and every language (the port is 10 lines and is
asserted against the Java implementation by the scenario).

**D. Rendezvous (HRW) hashing.** Also consistent and stateless, but it needs a hash per bucket per
key — `P` hashes instead of `log P` — and its "score" is a different number per key; for a few
thousand keys per second it is fine, and it buys nothing over jump hashing here.

**E. Hash tags to pin a queue to one slot (`queue:{1v1:NA}:{p}`).** The opposite of the goal: it
guarantees every partition of a queue lives on the same shard, keeping the hotspot and adding the
cluster's operations.

## Decision

1. **Hash**: jump consistent hashing over an FNV-1a 64-bit hash of the **enqueue key**. FNV-1a
   rather than `String.hashCode()` because the latter is only 32 bits and not specified across JVMs;
   a partition key that two processes disagree on is a ticket that waits forever.
2. **Enqueue key**: `partyId` when the ticket belongs to a party, `playerId` otherwise. Every member
   of a party therefore lands in the same partition, so the atomic party enqueue (`createPartyTickets`,
   W6/W8) stays one key and a party is never split across partitions.
3. **Key shape**: `queue:{mode}:{region}:{partition}` when `P > 1`, and the historical
   `queue:{mode}:{region}` when `P == 1`. `QUEUE_PARTITIONS` defaults to **1**, so a deployment that
   does not partition is byte-identical to the previous one and the whole existing suite keeps its
   keys.
4. **Pairing reads all partitions**: per tick, one `ZRANGE ... WITHSCORES` per partition (in
   parallel), merged by the wait score, and the roster is paired with **every** partition as
   `KEYS` — `pair.lua` finds each member with `ZSCORE` across the keys and removes it from the one
   that holds it, all-or-nothing.
5. **Cap**: the per-partition cap is `floor(QUEUE_MAX_DEPTH / P)`, so the sum never exceeds the
   configured depth (`P·floor(C/P) ≤ C`); with `C` divisible by `P` it is exact. `C < P`
   degenerates to one per partition — the smallest cap that still refuses something.
6. **Observability**: `matchmaking_queue_partition_depth{mode,region,partition}` and
   `matchmaking_queue_hotspot{mode,region}` = deepest/mean (0 when empty). `queue_depth` stays the
   sum, so the SLOs and the dashboards keep their meaning.

## How it works (files)

- `common/common-redis/src/main/java/com/victor/matchmaking/redis/QueuePartitions.java` — FNV-1a 64
  and `partitionOf(key, partitions)`; `partitionsFromEnv()` clamps `[1, 1024]`.
- `common/common-redis/src/main/java/com/victor/matchmaking/redis/RedisTicketRepository.java` —
  computes the key from the ticket's party/player, divides the cap per partition, sums the depth.
- `services/matchmaking-engine/src/main/resources/pair.lua` — `KEYS[1..N]` are the partitions of one
  (mode, region); each member is located across them and removed from its own key.
- `services/matchmaking-engine/.../MatchmakingEngineApplication.java` — `readHeads` (parallel
  `ZRANGE WITHSCORES`), merge by score, `reapDead` per partition, `pair(roster, queueKeys, …)`.
- `services/matchmaking-engine/.../infrastructure/QueueMetricsPoller.java` — one series per
  partition, the sum, the hotspot, and the oldest head across partitions.

## Measured numbers (local stack, 2026-10-20)

**Distribution, 20 000 synthetic keys over 8 partitions** (`QueuePartitionsTest`, counts per
partition):

```
[2528, 2504, 2456, 2558, 2337, 2551, 2541, 2525]   mean 2500   max/mean 1.023   min 2337
```

**The same 20 000 keys hashed naively** (two characters of the id, the "obvious" partition key):
`[1112, 12222, 1111, 1111, 1111, 1111, 1111, 1111]` — **61 % of the queue in one partition**,
hotspot **4.89**. A partition key that is not uniform is a hotspot with a better name.

**Consistent growth, 8 → 9 partitions:**

| Hash | Keys that change partition |
| --- | --- |
| jump consistent hash | **10.93 %** (ideal 11.11 %) |
| `hash % P` | **88.88 %** |

**Real arrivals through the API** (`verify_partitioning.sh`, 2000 players, 1v1/NA, same ids in both
phases):

| Layout | Counts | Sum | Deepest | Hotspot | Max share |
| --- | --- | --- | --- | --- | --- |
| 1 partition (kernel: `queue:1v1:NA`) | `{NA: 2000}` | 2000 | 2000 | **8.0** against the 8-way target | **100 %** |
| 8 partitions | `{0:246, 1:243, 2:256, 3:232, 4:239, 5:239, 6:273, 7:272}` | 2000 | 273 | **1.092** | **13.65 %** |

**Cross-partition pairing**: two players hashed to partitions 3 and 5 paired in **0.2 s**, both
members left their own partition in the same script call, both tickets `CONFIRMING`.

**The scrape agrees with Redis**: three 5v5 players (a queue that cannot pair, it needs ten) in
partitions 4 and 5 → `partition_depth{0..7}` = `[0,0,0,0,2,1,0,0]`, `queue_depth` 3, hotspot
**5.333** = 2 ÷ (3/8), and the series sum to the depth.

**The cap is not multiplied**: `QUEUE_MAX_DEPTH=100`, `QUEUE_PARTITIONS=8` → 12 per partition,
**96 admitted**, 104 refused, depth 96 ≤ 100.

## How to test

- `bash requests/partitioning/verify_partitioning.sh` (27 checks): distribution before/after, the
  hotspot falling from 8.0 to 1.092, the scrape agreeing with Redis, cross-partition pairing, cap.
- `mvn -o -B -pl common/common-redis -Dtest=QueuePartitionsTest test` (5 tests): stable, uniform,
  consistent, and the modulo counter-example.
- `mvn -o -B -pl services/matchmaking-engine -Dtest=PartitionedQueueTest test` (4 tests): different
  partitions pair, a party stays in one partition, depth is the sum, the cap is not multiplied.

## Trade-offs

- **One extra configuration value both services must share** (`QUEUE_PARTITIONS`), enforced by the
  scenario rather than the compiler.
- **Changing `P` is a drain**: the key shape changes, so the queue must be empty when the value
  changes. With jump hashing the *rehash* is cheap (1/N of the keys move); with the key shape
  changing, the drain is about the deploy, not the hash.
- **`pair.lua` scans `P` keys per member** (`ZSCORE` per key) instead of one known key: `P × size`
  reads inside one atomic script — bounded, and cheaper than a cross-partition lock would be.
- **A party is placed by `partyId`, not by its members' ids**: one member's "natural" partition does
  not win. That is the price of the party being atomic. The party admission path keeps its W17
  shape (a pre-check, so a party can overshoot by its own size) and compares against the **total**
  `C` rather than the per-partition cap — which is why "the sum never exceeds `C`" is exact for solo
  arrivals and bounded by one party size otherwise, the same caveat W17 recorded.
- **The hotspot ratio needs `P > 1` to mean anything**; with `P = 1` it is 1.0 and the interesting
  comparison is against a target `P` (the scenario computes it: 100 % on one key = 8.0).
- **`P` is not adaptive.** Choosing it from the queue size would remap keys exactly when the queue
  is large. It is a capacity-planning number: with 8, a partition holds a queue of 10 000 as
  comfortably as 10.

## Consequences

- The queue's storage is spread by a hash the tests measure, not by intuition; the naive key is in
  the test as the counter-example (4.89 vs 1.02).
- Pairing behaviour is **unchanged by partitioning**, which is asserted twice: by the unit test
  (script across keys) and by the load run (200 of 200 matches at `P = 8`).
- The enforcement of "a partition is not a boundary" is one test and one scenario section; removing
  the merge makes the cross-partition case fail rather than degrade.
- The `QUEUE_PARTITIONS` default of 1 keeps every existing deployment, test and request collection
  working while the capability exists.
