package com.victor.matchmaking.redis;

import io.vertx.redis.client.RedisOptions;

/**
 * The Redis client options the services connect with.
 *
 * <p>Vert.x sizes a Redis client at 6 connections with a wait queue of 24 by default
 * ({@code PoolOptions.maxSize}/{@code maxWaiting}). Measured in W16 with 64 concurrent clients:
 * the API refused 388 of 400 enqueues with HTTP 500 and
 * {@code Connection pool reached max wait queue size of 24}, and each refusal waited the full
 * pool timeout (~2 s) instead of failing fast. The pool was the ceiling, not Redis: the same
 * requests need microseconds of Redis time.
 *
 * <p>The numbers are sized for the load the stack is validated with (a thousand players, tens of
 * concurrent clients): deep enough that a burst queues instead of being refused, bounded so a
 * real overload still becomes backpressure rather than unbounded memory. What happens *at* the
 * limit is a policy (W17): today a refused request is a 500, and it should be an explicit
 * shedding answer with a metric.
 */
public final class RedisClients {

    /** Connections the client may open; commands are microseconds of Redis time each. */
    public static final int POOL_SIZE = 32;
    /** How many commands may wait for a connection before the client refuses them. */
    public static final int POOL_WAITING = 2048;

    private RedisClients() {
    }

    /** Options for a client that issues commands (not the dedicated subscriber connection). */
    public static RedisOptions options(String redisUri) {
        return new RedisOptions()
                .setConnectionString(redisUri)
                .setMaxPoolSize(POOL_SIZE)
                .setMaxPoolWaiting(POOL_WAITING);
    }
}
