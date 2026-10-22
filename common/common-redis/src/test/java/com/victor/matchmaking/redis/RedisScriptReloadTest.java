package com.victor.matchmaking.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/**
 * A Redis restart must not require restarting the services (W18, ADR 0024).
 *
 * <p>Redis keeps the script cache in memory next to the data and drops it on a restart, a failover
 * or a {@code SCRIPT FLUSH}. The services cached only the SHA, so after such a restart every
 * {@code EVALSHA} answered {@code NOSCRIPT} — measured on the local stack: {@code POST
 * /v1/matchmaking/tickets} returned {@code 500 NOSCRIPT No matching script} for every arrival and
 * the matching loop stopped, permanently, until the process was restarted. Restarting Redis is the
 * most routine operation there is; needing to restart every consumer with it is an outage wearing a
 * maintenance window.
 *
 * <p>These tests flush the server's script cache the way a restart does and require the operations
 * to keep working: the ticket admission script, the rate-limit bucket and the cache itself. Each one
 * fails against a caller that only remembers the SHA.
 */
class RedisScriptReloadTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");

    private static Vertx vertx;
    private static Redis redis;

    @BeforeAll
    static void connect() {
        vertx = Vertx.vertx();
        redis = Redis.createClient(vertx, RedisClients.options(REDIS_URI));
    }

    @AfterAll
    static void disconnect() throws Exception {
        vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void anEnqueueSurvivesTheServerForgettingTheScript() throws Exception {
        RedisTicketRepository tickets = new RedisTicketRepository(redis);
        String first = await(tickets.createIdempotent("script-reload-" + System.nanoTime() / 1_000_000,
                searching("ticket-w18-reload-1", "w18-reload-1")));
        assertNotNull(first, "the first enqueue loads the script and succeeds");

        // What a Redis restart does to the script cache (FLUSHALL does not touch it; the server
        // coming back with an empty cache does).
        await(redis.send(Request.cmd(Command.SCRIPT).arg("FLUSH")));

        String second = await(tickets.createIdempotent("script-reload-" + System.nanoTime() / 1_000_000,
                searching("ticket-w18-reload-2", "w18-reload-2")));
        assertEquals("ticket-w18-reload-2", second,
                "the repository reloads the script and the arrival is admitted, with nobody restarted");
        await(redis.send(Request.cmd(Command.DEL).arg("ticket:ticket-w18-reload-1")
                .arg("ticket:ticket-w18-reload-2").arg("queue:1v1:NA")));
    }

    @Test
    void theRateLimiterStillLimitsAfterTheServerForgetsTheScript() throws Exception {
        // Fail-open is the limiter's answer to an unreachable dependency, so "it did not throw" is
        // not the property: the property is that it still refuses once the burst is spent.
        RateLimiter limiter = new RateLimiter(redis, 3, 0.1);
        String caller = "w18-reload-limiter";
        await(redis.send(Request.cmd(Command.DEL).arg(RedisKeys.rateLimitKey("tickets", caller))));
        assertTrue(await(limiter.acquire("tickets", caller)).allowed(), "the first token is admitted");

        await(redis.send(Request.cmd(Command.SCRIPT).arg("FLUSH")));

        for (int i = 0; i < 3; i++) {
            await(limiter.acquire("tickets", caller));
        }
        assertTrue(!await(limiter.acquire("tickets", caller)).allowed(),
                "the bucket still refuses after the reload: a limiter that fails open forever is a limiter that lies");
        await(redis.send(Request.cmd(Command.DEL).arg(RedisKeys.rateLimitKey("tickets", caller))));
    }

    @Test
    void theReloadIsARepairNotANewVersion() throws Exception {
        String key = RedisKeys.rateLimitKey("tickets", "w18-reload-script");
        await(redis.send(Request.cmd(Command.DEL).arg(key)));
        RedisScript script = new RedisScript(redis, "/ratelimit.lua");
        String loaded = await(script.load());
        assertEquals(loaded, script.cachedSha(), "the SHA the server hands back is the one cached");

        await(redis.send(Request.cmd(Command.SCRIPT).arg("FLUSH")));
        assertEquals(loaded, script.cachedSha(), "the cache is the caller's, not the server's");

        Response first = await(script.eval(sha -> rateLimitRequest(sha, key)));
        assertEquals(1L, first.get(0).toLong(),
                "the first call after the flush reloads the script and still decides");
        assertEquals(loaded, script.cachedSha(),
                "the same script hashes to the same SHA: the reload is a repair, not a new version");
        Response second = await(script.eval(sha -> rateLimitRequest(sha, key)));
        assertEquals(1L, second.get(0).toLong(), "and the repaired cache serves the next call");
        await(redis.send(Request.cmd(Command.DEL).arg(key)));
    }

    private static Request rateLimitRequest(String sha, String key) {
        return Request.cmd(Command.EVALSHA).arg(sha)
                .arg("1").arg(key)
                .arg("5").arg("1").arg("1").arg("60");
    }

    private static MatchmakingTicket searching(String ticketId, String playerId) {
        return new MatchmakingTicket(ticketId, playerId, GameMode.ONE_V_ONE, Region.NA, 80, 50,
                Instant.now(), TicketState.SEARCHING, 0L, null);
    }

    private static <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
}
