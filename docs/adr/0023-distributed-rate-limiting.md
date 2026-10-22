# ADR-0023 — Distributed rate limiting: one token bucket per caller in Redis (W17)

## Status
Accepted (2026-10-18)

## Context
ADR 0022 bounds the *queue*. It does not bound a *caller*. Measured in W17 before this ADR: one
authenticated player, one token, **1000 `POST /v1/matchmaking/tickets` in a few seconds fills the
whole queue** — every other player in that mode/region then gets a `503` from the cap, with a
perfectly valid request and no idea why. Idempotency does not help (each request carries a fresh
`Idempotency-Key`, so they are 1000 distinct arrivals), and the queue cap does not help either: it
refuses everyone equally, which is the opposite of what is wanted when one caller is the problem.

What the limit has to be, to be worth having:

1. **Shared across instances.** Two API replicas each holding a limiter in a field serve twice the
   limit — the limit becomes a lie the moment you scale out.
2. **Survives a restart.** A limiter that forgets its state redeploys into a full burst all over
   again; "restart the pod" becomes a way to reset the debt.
3. **Cheap enough to be on the hot path.** One round trip, no matter how many callers.
4. **Not a new single point of failure.** Whatever stores the buckets is now on the enqueue path.
5. **Actionable.** A refusal has to say *when* to come back, or the client guesses — and a client
   that guesses is a client that hammers.

## Options

**A. In-process limiter (`Guava RateLimiter`, a bucket in a field).** Rejected: fails 1 and 2. Also
the most common way to *believe* you have a limit while running two instances.

**B. Fixed window counter in Redis (`INCR` + `EXPIRE`).** Cheap and shared. Rejected: the window
boundary doubles the limit (60 requests at 11:59:59.9 and 60 at 12:00:00.1 are two windows), and the
counter cannot answer "when may I try again" — it knows the *window* end, not the *token* arrival.
It is also unnecessarily harsh in the opposite direction: a caller quiet for 59 s still has an empty
window but no proof he was ever reasonable.

**C. Sliding window log in Redis (a ZSET of timestamps per caller).** Rejected: memory per caller
proportional to the limit, and each request reads/trims N entries. For 20/s per player it is fine;
the design does not survive being made stricter, which is when people make it stricter.

**D. Token bucket in a Lua script, state in Redis.** Two numbers per caller (`tokens`, `ts`), O(1)
per request, one round trip, atomic read-modify-write, refills continuously and answers `Retry-After`
exactly. Chosen.

**E. Limit at the edge (LB / gateway / WAF).** Rejected *for this decision*: the identity that
matters is the authenticated player, and the edge (here, and in general) sees an IP or a token it
cannot verify. An IP limit on a NAT'd crowd punishes the innocent and is bypassed by anyone with a
voucher. An edge limit remains useful as a gross second line, which is not what protects the queue.

**F. Fail closed when the bucket store is unreachable.** Rejected: Redis being briefly unavailable
would become a total inability to enqueue — a small outage turned into a large one. But failing open
*these* requests must be loud (a warning line per request, a counter is impossible by definition) and
must not be silent, and it is only acceptable because the queue cap is a second, independent guard.

## Decision

A **token bucket per caller, in Redis, evaluated by a Lua script** (`ratelimit.lua`), reached
through the `RateLimiter` port in `common-redis`.

- **Key**: `ratelimit:{scope}:{id}` — for the enqueue endpoint, `ratelimit:tickets:{playerId}`. The
  scope is part of the key so a second policy (a different endpoint, a different cost) is a second
  scope, not a second mechanism.
- **State**: a Redis hash of `tokens` and `ts` (the instant of the last refill), plus a TTL of
  `2 × capacity / refill` seconds. The TTL is what keeps idle callers from accumulating keys, and it
  must be longer than one full refill: a shorter one hands a *fresh, full* bucket to a caller the
  script just throttled — the limit would have a hole at exactly the TTL boundary.
- **The clock is Redis's** (`redis.call('TIME')`), not the caller's. The bucket is shared state, so
  the clock has to be the one they share; with a caller-supplied timestamp a single instance with a
  fast clock refills its own bucket faster than the others.
- **The fraction is preserved** (`string.format('%.4f', …)`): a Lua number used as a command
  argument is converted by Redis with a precision that drops the fraction, so a bucket that stored
  only integers refills too generously — the W6 trap, repeated here on purpose.
- **Return value**: `{allowed, remaining, retryAfterSeconds}` — the script decides, the caller
  reports. `retryAfterSeconds` is `ceil((cost - tokens) / refill)` and is `0` when allowed, so the
  header is always honest.

Policy, all from the environment (`AdmissionConfig`), defaults chosen to be invisible to a
well-behaved client (a player enqueues, occasionally cancels and re-enqueues):

| Knob | Default | Meaning |
| --- | --- | --- |
| `RATE_LIMIT_BURST` | **20** | tokens in the bucket (`X-RateLimit-Limit`); `0` disables the limiter |
| `RATE_LIMIT_REFILL_PER_SECOND` | **5.0** | sustained rate after the burst; must be positive, or a refused caller can never be told when to come back |

The refusal is `429` + `Retry-After` + `X-RateLimit-Limit` + `X-RateLimit-Remaining: 0`, counted as
`matchmaking_tickets_rejected_total{reason="rate_limited"}`, and applied **per authenticated player**
(never per IP). It is applied to `POST /v1/matchmaking/tickets` only — the write path into the shared
queue. `GET`/`DELETE` of a ticket the caller already owns are not limited: they are cheap, they are
not a way to fill a shared buffer, and throttling a cancel would trap a player in a queue.

**429 and 503 are different sentences, deliberately.** `429` is about *you*: "you are too fast, come
back in N s" — the caller can fix it by slowing down. `503` (ADR 0022) is about *us*: "this queue is
full, come back in 5 s" — slowing down does not help, the capacity is the problem. A client that
treats them the same will be wrong about one of them.

## Failed open, loudly

If the limiter's own store is unreachable, `HttpRoutes` logs a warning and lets the request through
(`http.ratelimit.unavailable player=… reason=…`). Two reasons, and the second is the important one:
the queue cap is an independent guard that still holds, and a limiter that becomes the outage it was
written to prevent has a negative value. The cost is honest and stated: while Redis is unreachable,
the per-caller limit does not exist, and the *only* thing standing between a flood and the engine is
the capacity check inside `create.lua` — which is exactly why that check is atomic and inside the
write rather than a second read in Java.

## Measured numbers

`RateLimiterConcurrencyTest` (real Redis, `mvn -o -B -pl common/common-redis test`, **3/3**):

| Test | Property | Result |
| --- | --- | --- |
| `admitsExactlyTheBurstAndThenRefuses` | 200 concurrent `acquire` calls from one caller vs a burst of 20: `allowed` in `[20, 21]`, every refusal carries `Retry-After ≥ 1` | **exactly 20 admitted**, the rest refused with a usable delay |
| `refillsAfterTheBurstIsSpent` | a spent bucket refills at `refillPerSecond` (1/s in the test) and serves again | admitted after the refill window, refused before |
| `oneCallerDoesNotSpendAnotherCallersBudget` | a noisy caller's refusals do not touch a quiet caller's tokens | quiet caller admitted while the noisy one is throttled |

The scenario (`verify_load_shedding.sh` step 1, on the running stack, two runs): **60 requests in a
row from one player**, `X-RateLimit-Limit: 20` advertised, **42–43 admitted, 17–18 refused with
`429`**, the admitted count checked against `burst + refill × elapsed` (5.1–5.5 s → a budget of ~47,
i.e. the check would fail on a leak even though the burst is small), `X-RateLimit-Remaining: 0` on
every refusal (`Retry-After` present on all), no `5xx` anywhere in the loop, the KPI equal to the
refusal count, and — the part that makes it a limiter and not a wall — **after the `Retry-After` the
same player is admitted again (`201`)**. That last check is what proves the bucket refills instead of
the caller being banned: a limiter that never lets you back in is an outage with better metrics.

The concurrency number is the one that matters: 200 requests at once against a bucket of 20 admit
**20**, not 200. A limiter that is not atomic admits whatever the race allows, which is how a "20/s
limit" becomes a 200/s limit under exactly the load it exists for.

## How it works (files)

| Concern | File |
| --- | --- |
| The bucket, its arithmetic, its TTL | `common/common-redis/src/main/resources/ratelimit.lua` |
| The port: script load/cache, `Decision` | `common/common-redis/.../RateLimiter.java` |
| Key naming | `common/common-redis/.../RedisKeys.rateLimitKey` |
| Policy from the environment | `services/matchmaking-api/.../AdmissionConfig.java` |
| Where it is applied, the headers, fail-open | `services/matchmaking-api/.../presentation/HttpRoutes.java` |

## How to test

```bash
# the property, against real Redis: 200 concurrent vs a burst of 20 -> exactly 20
mvn -o -B -pl common/common-redis test -Dtest=RateLimiterConcurrencyTest

# the behaviour against the running stack (the burst, the bound, the headers, the KPI, the refill)
bash requests/backpressure/verify_load_shedding.sh     # step 1 of 3; 35 checks in total

# by hand, 60 requests in a row with the same token: Postman `… — rate limited (429)`
# and, one line, the bucket itself:
docker exec game-matchmaking-backend-redis-1 redis-cli hgetall ratelimit:tickets:<playerId>
```

## Trade-offs

- **20 burst / 5 per second is a policy, not a law.** It is far above a human and far below a flood.
  The scenario deliberately does not hard-code it in its assertions: it adopts the advertised
  `X-RateLimit-Limit` and checks the *bound* (`burst + refill × elapsed`), so tuning the policy does
  not silently weaken the test — a test that hard-codes a policy it may be configured away from is
  testing its own constants.
- **Per caller, not per IP.** N accounts get N buckets. The cap (ADR 0022) is what bounds that
  attack; a per-IP or per-subnet limit would be a second mechanism with a different set of unfair
  outcomes (a shared NAT, a tournament venue). Deferred, noted in `MelhoriasFutura.md`.
- **One Redis round trip on the enqueue path.** ~0.5 ms locally on top of the enqueue (the ticket
  write is another round trip anyway). Measured effect on the load generator's client p50 at
  concurrency 64: unchanged within run-to-run variance.
- **The bucket is per player *and* per scope, not per player globally.** A player could spend 20
  tokens on tickets and 20 on some future scope. Fine today (one limiter exists); a global per-player
  budget would need a different key shape, and the "one key per policy" choice is what makes it
  visible rather than accidental.
- **Fail-open means the limit is not a security boundary.** It is a fairness and stability
  mechanism. Treating it as an access control would need fail-closed, which this ADR explicitly
  declines; if it is ever used to protect something that is not also protected by the queue cap,
  that decision has to be revisited.

## Consequences

- The API can answer `429` on the enqueue path, with `Retry-After`; both refusals (`429`, `503`) are
  in the Postman collection with their preconditions, and both count in
  `matchmaking_tickets_rejected_total{reason}`.
- The bucket keys (`ratelimit:tickets:*`) are visible state an operator can inspect — and, in the
  W16 spirit of "measure it against the thing it describes", the scenario cross-checks the API's own
  numbers against Redis's `ZCARD`/`HLEN` where a cheap ground truth exists.
- The concurrency test added the second `junit-jupiter` test dependency to `common-redis` (it drives
  real Redis through Vert.x futures, and a fake would only assert the fake).
- One more trap paid for and recorded: `Tuple.of(Object[])` and blocking `.get()` inside a Vert.x
  context were the traps of ADR 0022; here it is that the test helper must take the limiter as a
  parameter — a hard-coded static limiter hid a wrong-capacity bug until the test was made to
  construct its own.
