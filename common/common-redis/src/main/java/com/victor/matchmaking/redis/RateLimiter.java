package com.victor.matchmaking.redis;

import io.vertx.core.Future;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/**
 * A distributed token bucket (W17, ADR 0023).
 *
 * <p>Where the bucket lives is the decision: in Redis, not in the process. A limiter held in a
 * field would be per instance, so two API instances would serve twice the limit, and a restart
 * would forget the debt of whoever was throttled a moment before. The state is two numbers
 * (tokens, and the instant they were last refilled) and the arithmetic that moves one from the
 * other is a read-modify-write — from the caller it is a race with every other instance. It runs
 * as one Lua script, like {@code create.lua} and {@code pair.lua}.
 *
 * <p>A token bucket and not a fixed window: a window is either too generous (all your requests at
 * the edge of two windows) or too harsh (a player quiet for 59 s still gets nothing in the 60th).
 * The bucket admits a burst of {@code capacity} and then holds a steady {@code refillPerSecond},
 * which is the shape of the traffic this service sees: a player enqueues once, occasionally
 * cancels and re-enqueues, and a retry storm is a bug rather than a habit.
 */
public final class RateLimiter {

    private final Redis client;
    private final int capacity;
    private final double refillPerSecond;
    private final int bucketTtlSeconds;
    private final RedisScript script;

    /**
     * @param capacity         burst size in tokens (also the {@code X-RateLimit-Limit} advertised)
     * @param refillPerSecond  sustained rate; must be positive, or a refused caller can never be told
     *                         when to come back
     */
    public RateLimiter(Redis client, int capacity, double refillPerSecond) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        if (refillPerSecond <= 0) {
            throw new IllegalArgumentException("refillPerSecond must be positive: " + refillPerSecond);
        }
        this.client = client;
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.script = new RedisScript(client, "/ratelimit.lua");
        // The bucket has to outlive at least one full refill: a shorter TTL hands a full bucket to
        // a caller the script throttled a moment ago. Twice the refill time is the margin.
        this.bucketTtlSeconds = Math.max(1, (int) Math.ceil(2 * capacity / refillPerSecond));
    }

    /** What the caller may tell its client: admitted or not, and when to try again. */
    public record Decision(boolean allowed, int limit, long remaining, long retryAfterSeconds) {

        public static Decision of(boolean allowed, int limit, long remaining, long retryAfterSeconds) {
            return new Decision(allowed, limit, remaining, retryAfterSeconds);
        }
    }

    public int capacity() {
        return capacity;
    }

    /** One request against {@code scope}:{@code id}, costing one token. */
    public Future<Decision> acquire(String scope, String id) {
        return acquire(scope, id, 1);
    }

    /** One request against {@code scope}:{@code id}, costing {@code cost} tokens. */
    public Future<Decision> acquire(String scope, String id, int cost) {
        // The SHA is RedisScript's business: it reloads the bucket script when the server has
        // forgotten it (W18, ADR 0024). Without that, a Redis restart makes the limiter fail open
        // for good - it would keep admitting everything, quietly, which is a limiter that lies.
        return script.eval(sha -> Request.cmd(Command.EVALSHA).arg(sha)
                        .arg("1").arg(RedisKeys.rateLimitKey(scope, id))
                        .arg(String.valueOf(capacity))
                        .arg(String.valueOf(refillPerSecond))
                        .arg(String.valueOf(cost))
                        .arg(String.valueOf(bucketTtlSeconds)))
                .map(response -> {
                    boolean allowed = response.get(0).toLong() == 1L;
                    long remaining = response.get(1).toLong();
                    long retryAfter = response.get(2).toLong();
                    return Decision.of(allowed, capacity, remaining, retryAfter);
                });
    }
}
