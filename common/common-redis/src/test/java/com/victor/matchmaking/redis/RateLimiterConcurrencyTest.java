package com.victor.matchmaking.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;

/**
 * Property: a token bucket admits exactly its burst under concurrency (W17, ADR 0023).
 *
 * <p>The naive implementation - read the tokens, decide, write them back from the caller - is a
 * read-modify-write that N callers perform at the same time, and every one of them reads the same
 * "20 tokens left" before any of them writes. It lets the whole burst through and the limit
 * becomes decorative. This test fails on that implementation and passes only because the decision
 * and the write are one Lua execution.
 *
 * <p>The acquisitions are issued together and awaited once: awaiting inside the loop would make
 * them sequential and there would be no race left to lose.
 */
class RateLimiterConcurrencyTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String SCOPE = "w17-test";
    private static final String CALLER_BURST = "player-burst";
    private static final String CALLER_NOISY = "player-noisy";
    private static final String CALLER_QUIET = "player-quiet";
    private static final String CALLER_REFILL = "player-refill";
    private static final int BURST = 10;
    private static final double REFILL_PER_SECOND = 1.0;
    private static final int CALLERS = 200;

    private static Vertx vertx;
    private static Redis redis;
    private static RateLimiter limiter;

    @BeforeAll
    static void setUp() throws Exception {
        vertx = Vertx.vertx();
        redis = Redis.createClient(vertx, RedisClients.options(REDIS_URI));
        limiter = new RateLimiter(redis, BURST, REFILL_PER_SECOND);
        // Load the script once, so the burst below measures the bucket and not 200 script loads.
        limiter.acquire("w17-warmup", "warmup").toCompletionStage().toCompletableFuture()
                .get(30, TimeUnit.SECONDS);
        deleteKeys();
    }

    @AfterAll
    static void tearDown() throws Exception {
        deleteKeys();
        redis.close();
        vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static void deleteKeys() throws Exception {
        Request del = Request.cmd(Command.DEL);
        for (String caller : List.of(CALLER_BURST, CALLER_NOISY, CALLER_QUIET, CALLER_REFILL)) {
            del = del.arg(RedisKeys.rateLimitKey(SCOPE, caller));
        }
        redis.send(del).toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private static RateLimiter.Decision decide(RateLimiter rateLimiter, String caller) throws Exception {
        return rateLimiter.acquire(SCOPE, caller).toCompletionStage().toCompletableFuture()
                .get(30, TimeUnit.SECONDS);
    }

    @Test
    void admitsExactlyTheBurstAndThenRefuses() throws Exception {
        List<Future<RateLimiter.Decision>> inFlight = new ArrayList<>();
        for (int i = 0; i < CALLERS; i++) {
            inFlight.add(limiter.acquire(SCOPE, CALLER_BURST));
        }
        int allowed = 0;
        List<Long> retryAfters = new ArrayList<>();
        // Every acquire is already in flight; waiting for them one by one waits for all of them,
        // and each wait is bounded so a stalled Redis fails the test instead of hanging it.
        for (Future<RateLimiter.Decision> decision : inFlight) {
            RateLimiter.Decision answered;
            try {
                answered = decision.toCompletionStage().toCompletableFuture().get(60, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new AssertionError("every acquire must answer", e);
            }
            if (answered.allowed()) {
                allowed++;
            } else {
                retryAfters.add(answered.retryAfterSeconds());
            }
        }

        // The burst is all a caller gets without waiting: the refill is 1 token/s and the whole
        // race runs in well under a second, so at most one extra token can be earned.
        assertTrue(allowed <= BURST + 1, "the bucket must admit its burst, not one per caller: allowed=" + allowed);
        assertTrue(allowed >= BURST, "every token of the burst must be usable: allowed=" + allowed);
        assertEquals(CALLERS - allowed, retryAfters.size(), "every refusal is exactly one refusal");

        // A refusal has to say when to come back: that number is the Retry-After the API sends.
        assertTrue(retryAfters.stream().allMatch(seconds -> seconds >= 1),
                "a refusal must carry a positive retry-after: " + retryAfters);
    }

    @Test
    void refillsAfterTheBurstIsSpent() throws Exception {
        RateLimiter refill = new RateLimiter(redis, 2, 2.0);
        // Warm the script so the three decisions below are about tokens, not about loading Lua.
        refill.acquire(SCOPE, "w17-warmup-2").toCompletionStage().toCompletableFuture()
                .get(30, TimeUnit.SECONDS);

        assertTrue(decide(refill, CALLER_REFILL).allowed(), "the first of the burst is admitted");
        assertTrue(decide(refill, CALLER_REFILL).allowed(), "the second of the burst is admitted");
        assertFalse(decide(refill, CALLER_REFILL).allowed(), "the burst is spent after two");

        Thread.sleep(600);
        assertTrue(decide(refill, CALLER_REFILL).allowed(),
                "after 600 ms at 2 tokens/s the bucket has a token again");
    }

    @Test
    void oneCallerDoesNotSpendAnotherCallersBudget() throws Exception {
        for (int i = 0; i < BURST + 5; i++) {
            decide(limiter, CALLER_NOISY);
        }
        assertFalse(decide(limiter, CALLER_NOISY).allowed(), "the noisy caller is throttled");

        assertTrue(decide(limiter, CALLER_QUIET).allowed(),
                "a player's first request must not be refused because another player is noisy");
    }
}
